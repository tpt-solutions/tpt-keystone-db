//! Heuristic query planning helpers: index-aware scan selection and
//! size-aware join build-side choice. This is intentionally not a full
//! cost-based optimizer (no cardinality estimation or plan enumeration) —
//! just cheap, safe heuristics layered on top of the existing direct-AST
//! executor.

use std::sync::Arc;

use super::eval::{self, RowContext, Value};
use super::{parse_rows, resolve_table_ref, CteContext};
use crate::geo::geometry::Geometry;
use crate::sql::ast::{BinOp, Expr, TableRef};
use crate::storage::database::Database;
use crate::storage::{StorageEngine, TableSchema};

/// Resolve a FROM clause's primary table, using a B-Tree index point lookup
/// instead of a full table scan when the query's WHERE clause has a
/// top-level equality predicate on an indexed column. The full WHERE clause
/// is still re-evaluated by the caller afterwards, so an imperfect (or
/// stale) predicate match here can only cost performance, never correctness.
pub fn resolve_primary_table(
    table: &TableRef,
    where_: &Option<Expr>,
    db: &Arc<Database>,
    cte_ctx: &mut CteContext,
    outer: &[eval::OuterRow],
    params: &[eval::Value],
) -> anyhow::Result<(Option<Arc<TableSchema>>, Vec<Vec<Option<Vec<u8>>>>)> {
    if table.subquery.is_none() && cte_ctx.get(&table.name).is_none() {
        if let Some(schema) = db.get_table(&table.name)? {
            if let Some((col, literal)) = extract_equality_predicate(where_, &schema) {
                if db.indexed_column(&table.name, &col) {
                    let hit = db.index_lookup(&table.name, &col, &literal)?;
                    let schema_arc = Arc::new(schema.clone());
                    let rows = match hit {
                        Some(kv) => parse_rows(std::slice::from_ref(&kv), &Some(schema)),
                        None => Vec::new(),
                    };
                    return Ok((Some(schema_arc), rows));
                }
            } else if let Some(sp) = extract_spatial_predicate(where_, &schema) {
                if db.indexed_column_spatial(&table.name, &sp.col) {
                    if let Some(hits) = db.spatial_query(&table.name, &sp.col, sp.lon, sp.lat, sp.radius_m, sp.time_range) {
                        let schema_arc = Arc::new(schema.clone());
                        let rows = parse_rows(&hits, &Some(schema));
                        return Ok((Some(schema_arc), rows));
                    }
                }
            } else if let Some(tp) = extract_time_bucket_predicate(where_, &schema) {
                if db.indexed_column_time(&table.name, &tp.col) {
                    if let Some(hits) = db.time_range_query(&table.name, &tp.col, tp.t0, tp.t1) {
                        let schema_arc = Arc::new(schema.clone());
                        let rows = parse_rows(&hits, &Some(schema));
                        return Ok((Some(schema_arc), rows));
                    }
                }
            }
        }
    }
    resolve_table_ref(table, db, cte_ctx, outer, params)
}

/// A `ST_DWithin(col, point, radius)` predicate, optionally AND-combined
/// with `ST_T(col) BETWEEN t0 AND t1` — Meridian's "near this point AND
/// within this time range" shape (`2meridianspec.txt`'s milestone query).
/// Both halves are answered by a single `Database::spatial_query` call
/// against one spatial index, since the index buckets by cell and time is
/// just another field filtered within the matched cells' entries.
struct SpatialPredicate {
    col: String,
    lon: f64,
    lat: f64,
    radius_m: f64,
    time_range: Option<(i64, i64)>,
}

/// Finds a top-level (AND-connected) `ST_DWithin(<geometry column>, <point
/// expr>, <radius expr>)` call, plus an optional AND-connected
/// `ST_T(<same column>) BETWEEN <low> AND <high>`, anywhere else at the top
/// level. The full WHERE clause is still re-evaluated by the caller
/// afterwards (same contract as `extract_equality_predicate`), so a partial
/// or imprecise match here only costs performance, never correctness.
fn extract_spatial_predicate(where_: &Option<Expr>, schema: &TableSchema) -> Option<SpatialPredicate> {
    let root = where_.as_ref()?;
    let mut conjuncts = Vec::new();
    flatten_and(root, &mut conjuncts);

    let mut spatial = None;
    for e in &conjuncts {
        if let Some(sp) = try_dwithin(e, schema) {
            spatial = Some(sp);
            break;
        }
    }
    let (col, lon, lat, radius_m) = spatial?;

    let mut time_range = None;
    for e in &conjuncts {
        if let Some(tr) = try_time_between(e, schema, &col) {
            time_range = Some(tr);
            break;
        }
    }

    Some(SpatialPredicate { col, lon, lat, radius_m, time_range })
}

fn flatten_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp { op: BinOp::And, lhs, rhs } => {
            flatten_and(lhs, out);
            flatten_and(rhs, out);
        }
        _ => out.push(expr),
    }
}

/// If `expr` names a column that exists in `schema`, returns its name
/// (regardless of declared type — the caller is responsible for checking
/// an index actually exists on it).
fn ident_col(expr: &Expr, schema: &TableSchema) -> Option<String> {
    let name = match expr {
        Expr::Ident(n) => n.clone(),
        Expr::QualifiedIdent(_, n) => n.clone(),
        _ => return None,
    };
    schema.columns.iter().any(|c| c.name == name).then_some(name)
}

fn try_dwithin(expr: &Expr, schema: &TableSchema) -> Option<(String, f64, f64, f64)> {
    let Expr::Function { name, args, .. } = expr else { return None };
    if !name.eq_ignore_ascii_case("st_dwithin") || args.len() != 3 {
        return None;
    }
    let (col, point_expr) = match ident_col(&args[0], schema) {
        Some(c) => (c, &args[1]),
        None => (ident_col(&args[1], schema)?, &args[0]),
    };
    let point = RowContext::empty().eval(point_expr).ok()?;
    let Value::Text(wkt) = point else { return None };
    let c = Geometry::from_wkt(&wkt).ok()?.representative_point();
    let radius = RowContext::empty().eval(&args[2]).ok()?.as_f64().ok()?;
    Some((col, c.x, c.y, radius))
}

fn try_time_between(expr: &Expr, schema: &TableSchema, col: &str) -> Option<(i64, i64)> {
    let Expr::Between { expr: inner, low, high, negated: false } = expr else { return None };
    let Expr::Function { name, args, .. } = inner.as_ref() else { return None };
    if !(name.eq_ignore_ascii_case("st_t") || name.eq_ignore_ascii_case("st_time")) || args.len() != 1 {
        return None;
    }
    if ident_col(&args[0], schema)?.as_str() != col {
        return None;
    }
    let t0 = match RowContext::empty().eval(low).ok()? {
        Value::Int(n) => n,
        Value::Float(f) => f as i64,
        _ => return None,
    };
    let t1 = match RowContext::empty().eval(high).ok()? {
        Value::Int(n) => n,
        Value::Float(f) => f as i64,
        _ => return None,
    };
    Some((t0, t1))
}

/// A Chronos time-range predicate on an indexed timestamp column —
/// `<col> BETWEEN t0 AND t1` or `time_bucket(<interval>, <col>) = <const>`
/// (the latter re-expanded to the bucket's `[start, start + interval)`
/// range). Answered by a single `Database::time_range_query` call against
/// one time index. As with `extract_equality_predicate`/
/// `extract_spatial_predicate`, the caller still re-evaluates the full WHERE
/// clause afterwards, so an imprecise match here only costs performance.
struct TimeBucketPredicate {
    col: String,
    t0: i64,
    t1: i64,
}

fn extract_time_bucket_predicate(where_: &Option<Expr>, schema: &TableSchema) -> Option<TimeBucketPredicate> {
    let root = where_.as_ref()?;
    let mut conjuncts = Vec::new();
    flatten_and(root, &mut conjuncts);

    for e in &conjuncts {
        if let Some(tp) = try_time_bucket_between(e, schema) {
            return Some(tp);
        }
        if let Some(tp) = try_time_bucket_eq(e, schema) {
            return Some(tp);
        }
    }
    None
}

fn try_time_bucket_between(expr: &Expr, schema: &TableSchema) -> Option<TimeBucketPredicate> {
    let Expr::Between { expr: inner, low, high, negated: false } = expr else { return None };
    let col = ident_col(inner, schema)?;
    let t0 = match RowContext::empty().eval(low).ok()? {
        Value::Int(n) => n,
        Value::Float(f) => f as i64,
        _ => return None,
    };
    let t1 = match RowContext::empty().eval(high).ok()? {
        Value::Int(n) => n,
        Value::Float(f) => f as i64,
        _ => return None,
    };
    Some(TimeBucketPredicate { col, t0, t1 })
}

fn try_time_bucket_eq(expr: &Expr, schema: &TableSchema) -> Option<TimeBucketPredicate> {
    let Expr::BinaryOp { op: BinOp::Eq, lhs, rhs } = expr else { return None };
    let (bucket_expr, value_expr) = match lhs.as_ref() {
        Expr::Function { name, .. } if name.eq_ignore_ascii_case("time_bucket") => (lhs.as_ref(), rhs.as_ref()),
        _ => (rhs.as_ref(), lhs.as_ref()),
    };
    let Expr::Function { name, args, .. } = bucket_expr else { return None };
    if !name.eq_ignore_ascii_case("time_bucket") || args.len() != 2 {
        return None;
    }
    let interval_lit = RowContext::empty().eval(&args[0]).ok()?;
    let Value::Text(interval_str) = interval_lit else { return None };
    let interval_ms = eval::parse_interval(&interval_str)?;
    let col = ident_col(&args[1], schema)?;
    let bucket_start = match RowContext::empty().eval(value_expr).ok()? {
        Value::Int(n) => n,
        Value::Float(f) => f as i64,
        _ => return None,
    };
    Some(TimeBucketPredicate { col, t0: bucket_start, t1: bucket_start + interval_ms - 1 })
}

/// Find a top-level (AND-connected) `column = literal` predicate whose
/// column exists in `schema`, so it can drive an index lookup.
fn extract_equality_predicate(where_: &Option<Expr>, schema: &TableSchema) -> Option<(String, Vec<u8>)> {
    extract_eq(where_.as_ref()?, schema)
}

fn extract_eq(expr: &Expr, schema: &TableSchema) -> Option<(String, Vec<u8>)> {
    match expr {
        Expr::BinaryOp { op: BinOp::And, lhs, rhs } => extract_eq(lhs, schema).or_else(|| extract_eq(rhs, schema)),
        Expr::BinaryOp { op: BinOp::Eq, lhs, rhs } => {
            try_col_literal(lhs, rhs, schema).or_else(|| try_col_literal(rhs, lhs, schema))
        }
        _ => None,
    }
}

fn try_col_literal(col_expr: &Expr, lit_expr: &Expr, schema: &TableSchema) -> Option<(String, Vec<u8>)> {
    let name = match col_expr {
        Expr::Ident(n) => n.clone(),
        Expr::QualifiedIdent(_, n) => n.clone(),
        _ => return None,
    };
    if !schema.columns.iter().any(|c| c.name == name) {
        return None;
    }
    let value = RowContext::empty().eval(lit_expr).ok()?;
    value.to_wire_bytes().map(|b| (name, b))
}
