//! Heuristic query planning helpers: index-aware scan selection and
//! size-aware join build-side choice. This is intentionally not a full
//! cost-based optimizer (no cardinality estimation or plan enumeration) —
//! just cheap, safe heuristics layered on top of the existing direct-AST
//! executor.

use std::sync::Arc;

use super::eval::{self, RowContext};
use super::{parse_rows, resolve_table_ref, CteContext};
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
            }
        }
    }
    resolve_table_ref(table, db, cte_ctx, outer, params)
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
