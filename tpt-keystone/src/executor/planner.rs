//! Heuristic query planning helpers: index-aware scan selection and
//! size-aware join build-side choice. This is intentionally not a full
//! cost-based optimizer (no cardinality estimation or plan enumeration) —
//! just cheap, safe heuristics layered on top of the existing direct-AST
//! executor.

use std::collections::HashSet;
use std::sync::Arc;

use super::eval::{self, RowContext, Value};
use super::stats::table_row_count_estimate;
use super::{parse_rows, resolve_table_ref, CteContext};
use crate::geo::geometry::Geometry;
use crate::sql::ast::{BinOp, Expr, Join, JoinType, TableRef};
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
                    if let Some(hits) = db.spatial_query(
                        &table.name,
                        &sp.col,
                        sp.lon,
                        sp.lat,
                        sp.radius_m,
                        sp.time_range,
                    ) {
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
fn extract_spatial_predicate(
    where_: &Option<Expr>,
    schema: &TableSchema,
) -> Option<SpatialPredicate> {
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

    Some(SpatialPredicate {
        col,
        lon,
        lat,
        radius_m,
        time_range,
    })
}

fn flatten_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            op: BinOp::And,
            lhs,
            rhs,
        } => {
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
    schema
        .columns
        .iter()
        .any(|c| c.name == name)
        .then_some(name)
}

fn try_dwithin(expr: &Expr, schema: &TableSchema) -> Option<(String, f64, f64, f64)> {
    let Expr::Function { name, args, .. } = expr else {
        return None;
    };
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
    let Expr::Between {
        expr: inner,
        low,
        high,
        negated: false,
    } = expr
    else {
        return None;
    };
    let Expr::Function { name, args, .. } = inner.as_ref() else {
        return None;
    };
    if !(name.eq_ignore_ascii_case("st_t") || name.eq_ignore_ascii_case("st_time"))
        || args.len() != 1
    {
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

fn extract_time_bucket_predicate(
    where_: &Option<Expr>,
    schema: &TableSchema,
) -> Option<TimeBucketPredicate> {
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
    let Expr::Between {
        expr: inner,
        low,
        high,
        negated: false,
    } = expr
    else {
        return None;
    };
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
    let Expr::BinaryOp {
        op: BinOp::Eq,
        lhs,
        rhs,
    } = expr
    else {
        return None;
    };
    let (bucket_expr, value_expr) = match lhs.as_ref() {
        Expr::Function { name, .. } if name.eq_ignore_ascii_case("time_bucket") => {
            (lhs.as_ref(), rhs.as_ref())
        }
        _ => (rhs.as_ref(), lhs.as_ref()),
    };
    let Expr::Function { name, args, .. } = bucket_expr else {
        return None;
    };
    if !name.eq_ignore_ascii_case("time_bucket") || args.len() != 2 {
        return None;
    }
    let interval_lit = RowContext::empty().eval(&args[0]).ok()?;
    let Value::Text(interval_str) = interval_lit else {
        return None;
    };
    let interval_ms = eval::parse_interval(&interval_str)?;
    let col = ident_col(&args[1], schema)?;
    let bucket_start = match RowContext::empty().eval(value_expr).ok()? {
        Value::Int(n) => n,
        Value::Float(f) => f as i64,
        _ => return None,
    };
    Some(TimeBucketPredicate {
        col,
        t0: bucket_start,
        t1: bucket_start + interval_ms - 1,
    })
}

/// Find a top-level (AND-connected) `column = literal` predicate whose
/// column exists in `schema`, so it can drive an index lookup.
fn extract_equality_predicate(
    where_: &Option<Expr>,
    schema: &TableSchema,
) -> Option<(String, Vec<u8>)> {
    extract_eq(where_.as_ref()?, schema)
}

fn extract_eq(expr: &Expr, schema: &TableSchema) -> Option<(String, Vec<u8>)> {
    match expr {
        Expr::BinaryOp {
            op: BinOp::And,
            lhs,
            rhs,
        } => extract_eq(lhs, schema).or_else(|| extract_eq(rhs, schema)),
        Expr::BinaryOp {
            op: BinOp::Eq,
            lhs,
            rhs,
        } => try_col_literal(lhs, rhs, schema).or_else(|| try_col_literal(rhs, lhs, schema)),
        _ => None,
    }
}

fn try_col_literal(
    col_expr: &Expr,
    lit_expr: &Expr,
    schema: &TableSchema,
) -> Option<(String, Vec<u8>)> {
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

/// A join `ON`-clause spatial predicate recognized as a candidate for the
/// GPU broad-phase join path (`geo::gpu`). Only a single top-level predicate
/// is recognized (no compound `a.id = b.id AND ST_Intersects(...)` — that
/// falls through to the existing nested-loop join unchanged); this is a
/// deliberate v1 scope limit, not an oversight, mirroring the same
/// "narrowest honest slice first" approach as the rest of this codebase's
/// scope cuts. As with every other `extract_*_predicate` helper in this
/// file, a `None` here only costs performance (falls back to nested-loop),
/// never correctness — the full predicate is always still what the join
/// actually evaluates.
pub(crate) enum SpatialJoinPredicate {
    Intersects {
        left_col: String,
        right_col: String,
    },
    DWithin {
        left_col: String,
        right_col: String,
        radius_m: f64,
    },
}

/// Recognizes `ST_Intersects(a, b)` or `ST_DWithin(a, b, radius)` where one
/// argument resolves entirely against `left_schema` and the other entirely
/// against `right_schema` (mirrors `expr_only_refs`'s single-side-resolution
/// contract used by `split_join_keys` for equi-joins).
pub(crate) fn extract_spatial_join_predicate(
    on_expr: &Expr,
    left_schema: &Option<Arc<TableSchema>>,
    right_schema: &Option<Arc<TableSchema>>,
) -> Option<SpatialJoinPredicate> {
    let Expr::Function { name, args, .. } = on_expr else {
        return None;
    };

    if name.eq_ignore_ascii_case("st_intersects") && args.len() == 2 {
        let (left_col, right_col) =
            resolve_join_cols(&args[0], &args[1], left_schema, right_schema)?;
        return Some(SpatialJoinPredicate::Intersects {
            left_col,
            right_col,
        });
    }
    if name.eq_ignore_ascii_case("st_dwithin") && args.len() == 3 {
        let (left_col, right_col) =
            resolve_join_cols(&args[0], &args[1], left_schema, right_schema)?;
        let radius_m = RowContext::empty().eval(&args[2]).ok()?.as_f64().ok()?;
        return Some(SpatialJoinPredicate::DWithin {
            left_col,
            right_col,
            radius_m,
        });
    }
    None
}

/// Resolves two expressions to (left-side column, right-side column) names,
/// trying both orderings — mirrors `split_join_keys`'s `expr_only_refs`
/// check but for plain column identifiers rather than arbitrary expressions
/// (a join's spatial predicate operands are always bare geometry columns in
/// v1, not computed expressions).
fn resolve_join_cols(
    a: &Expr,
    b: &Expr,
    left_schema: &Option<Arc<TableSchema>>,
    right_schema: &Option<Arc<TableSchema>>,
) -> Option<(String, String)> {
    let col_in = |expr: &Expr, schema: &Option<Arc<TableSchema>>| -> Option<String> {
        let name = match expr {
            Expr::Ident(n) => n.clone(),
            Expr::QualifiedIdent(_, n) => n.clone(),
            _ => return None,
        };
        schema
            .as_ref()
            .filter(|s| s.columns.iter().any(|c| c.name == name))
            .map(|_| name)
    };
    if let (Some(l), Some(r)) = (col_in(a, left_schema), col_in(b, right_schema)) {
        return Some((l, r));
    }
    if let (Some(l), Some(r)) = (col_in(b, left_schema), col_in(a, right_schema)) {
        return Some((l, r));
    }
    None
}

/// Reorders a maximal *prefix* of `joins` that are all `Inner`/`Cross`
/// (stopping at the first `Left`/`Right`/`Full` — its semantics, and
/// anything after it that may rely on its null-extended columns, depend on
/// encounter order, so those are left untouched) so tables with a smaller
/// `ANALYZE`d row-count estimate are joined earlier. `INNER`/`CROSS` joins
/// are commutative/associative, so any dependency-respecting order produces
/// the same final rows — this only changes how big the intermediate row set
/// gets along the way, which matters because every join here fully
/// materializes rows rather than iterating a plan tree.
///
/// Returns a permutation of `0..joins.len()` to apply to `joins`. Safety net:
/// this only reorders when every join's `ON` clause can be fully attributed
/// to already-scheduled tables (no ambiguous or unrecognized column
/// references) *and* every involved table has a persisted stat
/// (`ANALYZE` was run) — any gap in that falls back to the identity
/// permutation (today's exact behavior), so a bug or blind spot in this
/// heuristic can only leave a query's plan unchanged, never make it wrong.
pub fn reorder_inner_joins(
    primary_alias: &str,
    primary_schema: &Option<Arc<TableSchema>>,
    joins: &[Join],
    db: &Database,
) -> Vec<usize> {
    let identity: Vec<usize> = (0..joins.len()).collect();

    let prefix_len = joins
        .iter()
        .take_while(|j| matches!(j.join_type, JoinType::Inner | JoinType::Cross))
        .count();
    if prefix_len < 2 {
        return identity;
    }

    struct Node {
        alias: String,
        schema: Option<Arc<TableSchema>>,
        est_rows: i64,
    }

    let mut nodes = Vec::with_capacity(prefix_len);
    for j in &joins[..prefix_len] {
        if j.table.subquery.is_some() || j.table.func_args.is_some() {
            return identity; // no persisted stats for a derived table or table-valued function
        }
        let Ok(Some(schema)) = db.get_table(&j.table.name) else {
            return identity;
        };
        let Some(est_rows) = table_row_count_estimate(db, &j.table.name) else {
            return identity;
        };
        let alias = j
            .table
            .alias
            .clone()
            .unwrap_or_else(|| j.table.name.clone());
        nodes.push(Node {
            alias,
            schema: Some(Arc::new(schema)),
            est_rows,
        });
    }

    // (alias, schema) for every table available to attribute a column
    // reference to — the primary plus every reorderable join.
    let mut all_tables: Vec<(String, Option<Arc<TableSchema>>)> =
        vec![(primary_alias.to_string(), primary_schema.clone())];
    all_tables.extend(nodes.iter().map(|n| (n.alias.clone(), n.schema.clone())));

    let mut deps = Vec::with_capacity(prefix_len);
    for (i, j) in joins[..prefix_len].iter().enumerate() {
        match &j.on {
            None => deps.push(HashSet::new()),
            Some(expr) => match expr_dependencies(expr, &all_tables) {
                // A join's own alias in its own ON clause isn't a real
                // dependency (e.g. `ON o.customer_id = c.id`'s `c.id` half)
                // — it becomes available the instant this join is
                // scheduled, so it must never block scheduling itself.
                Some(mut d) => {
                    d.remove(&nodes[i].alias);
                    deps.push(d);
                }
                None => return identity, // couldn't safely attribute every reference
            },
        }
    }

    // Greedy schedule: among joins whose dependencies are already satisfied,
    // pick the smallest estimated row count next; ties keep original order.
    let mut scheduled: HashSet<String> = [primary_alias.to_string()].into_iter().collect();
    let mut remaining: Vec<usize> = (0..prefix_len).collect();
    let mut order = Vec::with_capacity(prefix_len);

    while !remaining.is_empty() {
        let ready: Vec<usize> = remaining
            .iter()
            .copied()
            .filter(|&i| deps[i].is_subset(&scheduled))
            .collect();
        let Some(&next) = ready.iter().min_by_key(|&&i| (nodes[i].est_rows, i)) else {
            return identity; // a forward reference our current engine wouldn't run anyway
        };
        scheduled.insert(nodes[next].alias.clone());
        order.push(next);
        remaining.retain(|&i| i != next);
    }

    order.extend(prefix_len..joins.len());
    order
}

/// Every table alias an expression's column references resolve to, given
/// `tables` (alias, schema) pairs. `None` means some reference couldn't be
/// safely attributed (ambiguous across tables, unknown, or an expression
/// shape this conservative walk doesn't cover) — the caller treats that as
/// "don't reorder this query" rather than guessing.
fn expr_dependencies(
    expr: &Expr,
    tables: &[(String, Option<Arc<TableSchema>>)],
) -> Option<HashSet<String>> {
    match expr {
        Expr::Ident(name) => {
            let matches: Vec<&String> = tables
                .iter()
                .filter(|(_, s)| {
                    s.as_ref()
                        .is_some_and(|s| s.columns.iter().any(|c| &c.name == name))
                })
                .map(|(a, _)| a)
                .collect();
            if matches.len() == 1 {
                Some([matches[0].clone()].into_iter().collect())
            } else {
                None
            }
        }
        Expr::QualifiedIdent(alias, _) => tables
            .iter()
            .any(|(a, _)| a == alias)
            .then(|| [alias.clone()].into_iter().collect()),
        Expr::Literal(_) | Expr::Param(_) => Some(HashSet::new()),
        Expr::BinaryOp { lhs, rhs, .. } => {
            let mut d = expr_dependencies(lhs, tables)?;
            d.extend(expr_dependencies(rhs, tables)?);
            Some(d)
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } => expr_dependencies(expr, tables),
        Expr::Function { args, .. } => {
            let mut d = HashSet::new();
            for a in args {
                d.extend(expr_dependencies(a, tables)?);
            }
            Some(d)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let mut d = expr_dependencies(expr, tables)?;
            d.extend(expr_dependencies(low, tables)?);
            d.extend(expr_dependencies(high, tables)?);
            Some(d)
        }
        Expr::IsNull { expr, .. } | Expr::IsTrue { expr, .. } | Expr::IsFalse { expr, .. } => {
            expr_dependencies(expr, tables)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::execute_query;
    use crate::sql::ast::{SelectStmt, Stmt};
    use crate::storage::config::NodeRole;
    use crate::storage::lease::LeaseManager;
    use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
    use std::time::Duration;

    fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let lease = Arc::new(LeaseManager::new(
            store.clone(),
            "db",
            "node-1".into(),
            Duration::from_secs(30),
        ));
        lease.try_acquire().unwrap();
        let db = Arc::new(
            Database::open(
                local.path(),
                store,
                lease.handle(),
                NodeRole::Writer,
                Default::default(),
            )
            .unwrap(),
        );
        (db, bucket, local)
    }

    fn parse_select(sql: &str) -> SelectStmt {
        match crate::sql::parse(sql).unwrap() {
            Stmt::Select(s) => s,
            _ => panic!("expected a SELECT"),
        }
    }

    #[test]
    fn reorders_independent_joins_by_ascending_row_count() {
        let (db, _b, _l) = test_db();
        execute_query(
            "CREATE TABLE orders (id INT4, customer_id INT4, product_id INT4)",
            db.clone(),
        )
        .unwrap();
        execute_query("CREATE TABLE customers (id INT4, name TEXT)", db.clone()).unwrap();
        execute_query("CREATE TABLE products (id INT4, name TEXT)", db.clone()).unwrap();

        for i in 0..10 {
            execute_query(
                &format!("INSERT INTO customers VALUES ({i}, 'c{i}')"),
                db.clone(),
            )
            .unwrap();
        }
        for i in 0..3 {
            execute_query(
                &format!("INSERT INTO products VALUES ({i}, 'p{i}')"),
                db.clone(),
            )
            .unwrap();
        }
        for i in 0..5 {
            execute_query(
                &format!("INSERT INTO orders VALUES ({i}, {}, {})", i % 10, i % 3),
                db.clone(),
            )
            .unwrap();
        }
        execute_query("ANALYZE", db.clone()).unwrap();

        // Both joins depend only on `orders` (the primary), so they're
        // freely reorderable; `products` (3 rows) should be scheduled ahead
        // of `customers` (10 rows) even though it's listed second in SQL.
        let select = parse_select(
            "SELECT o.id FROM orders o \
             JOIN customers c ON o.customer_id = c.id \
             JOIN products p ON o.product_id = p.id",
        );
        let twj = select.from.unwrap();
        let order = reorder_inner_joins(
            &"o".to_string(),
            &Some(Arc::new(db.get_table("orders").unwrap().unwrap())),
            &twj.joins,
            &db,
        );
        assert_eq!(
            order,
            vec![1, 0],
            "products (3 rows) should be scheduled before customers (10 rows)"
        );
    }

    #[test]
    fn dependency_chain_is_respected_even_when_reordering() {
        let (db, _b, _l) = test_db();
        // `line_items` (many rows) -> `orders` (fewer) -> `customers` (fewest),
        // each join's ON clause referencing the *previous* join, not the
        // primary directly, so a naive size-only sort would try to schedule
        // `customers` before `orders` even though it depends on it.
        execute_query(
            "CREATE TABLE line_items (id INT4, order_id INT4)",
            db.clone(),
        )
        .unwrap();
        execute_query(
            "CREATE TABLE orders2 (id INT4, customer_id INT4)",
            db.clone(),
        )
        .unwrap();
        execute_query("CREATE TABLE customers2 (id INT4, name TEXT)", db.clone()).unwrap();

        for i in 0..2 {
            execute_query(
                &format!("INSERT INTO customers2 VALUES ({i}, 'c{i}')"),
                db.clone(),
            )
            .unwrap();
        }
        for i in 0..4 {
            execute_query(
                &format!("INSERT INTO orders2 VALUES ({i}, {})", i % 2),
                db.clone(),
            )
            .unwrap();
        }
        for i in 0..20 {
            execute_query(
                &format!("INSERT INTO line_items VALUES ({i}, {})", i % 4),
                db.clone(),
            )
            .unwrap();
        }
        execute_query("ANALYZE", db.clone()).unwrap();

        let select = parse_select(
            "SELECT li.id, c.name FROM line_items li \
             JOIN orders2 o ON li.order_id = o.id \
             JOIN customers2 c ON o.customer_id = c.id",
        );
        let twj = select.from.unwrap();
        let order = reorder_inner_joins(
            &"li".to_string(),
            &Some(Arc::new(db.get_table("line_items").unwrap().unwrap())),
            &twj.joins,
            &db,
        );
        // customers2 (fewest rows) can't move ahead of orders2 — it depends
        // on the alias `o`, which only exists once orders2 is scheduled.
        assert_eq!(order, vec![0, 1]);

        // And the actual query still returns correct results end-to-end.
        let result = execute_query(
            "SELECT li.id, c.name FROM line_items li \
             JOIN orders2 o ON li.order_id = o.id \
             JOIN customers2 c ON o.customer_id = c.id \
             ORDER BY li.id",
            db.clone(),
        )
        .unwrap();
        assert_eq!(result.rows.len(), 20);
        assert_eq!(result.rows[0][1], Some(b"c0".to_vec()));
    }

    #[test]
    fn missing_stats_leaves_order_unchanged() {
        let (db, _b, _l) = test_db();
        execute_query("CREATE TABLE t1 (id INT4)", db.clone()).unwrap();
        execute_query("CREATE TABLE t2 (id INT4)", db.clone()).unwrap();
        execute_query("CREATE TABLE t3 (id INT4)", db.clone()).unwrap();
        // No ANALYZE run — no persisted stats.

        let select =
            parse_select("SELECT * FROM t1 a JOIN t2 b ON a.id = b.id JOIN t3 c ON a.id = c.id");
        let twj = select.from.unwrap();
        let order = reorder_inner_joins(
            &"a".to_string(),
            &Some(Arc::new(db.get_table("t1").unwrap().unwrap())),
            &twj.joins,
            &db,
        );
        assert_eq!(order, vec![0, 1]);
    }
}
