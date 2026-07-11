//! JOIN execution: hash-based equi-joins (build from whichever side is
//! smaller), a GPU-accelerated broad-phase path for recognized spatial join
//! predicates, and the nested-loop fallback for everything else.

use std::collections::HashMap;
use std::sync::Arc;

use super::eval::{RowContext, Value};
use super::merge_schema;
use super::planner;
use crate::geo::geometry::Geometry;
use crate::geo::gpu::{self, GpuBBox, GpuPoint};
use crate::sql::ast::{BinOp, Expr, Join, JoinType};
use crate::storage::TableSchema;

/// Whether `expr` only references columns present in `schema` (plus
/// literals/params) — used to test whether one side of an `=` predicate is
/// entirely evaluable against a single joined table, so it can drive a hash
/// join key instead of a full nested-loop scan.
fn expr_only_refs(expr: &Expr, schema: &Option<Arc<TableSchema>>) -> bool {
    let has_col = |name: &str| {
        schema
            .as_ref()
            .is_some_and(|s| s.columns.iter().any(|c| c.name == name))
    };
    match expr {
        Expr::Ident(name) => has_col(name),
        Expr::QualifiedIdent(_, name) => has_col(name),
        Expr::Literal(_) | Expr::Param(_) => true,
        Expr::BinaryOp { lhs, rhs, .. } => {
            expr_only_refs(lhs, schema) && expr_only_refs(rhs, schema)
        }
        Expr::UnaryOp { expr, .. } => expr_only_refs(expr, schema),
        Expr::Cast { expr, .. } => expr_only_refs(expr, schema),
        Expr::Function { args, .. } => args.iter().all(|a| expr_only_refs(a, schema)),
        _ => false,
    }
}

/// If `on_expr` is a top-level equality where one side resolves entirely
/// against `left_schema` and the other entirely against `right_schema`,
/// return `(left_key_expr, right_key_expr)` so the join can use a hash
/// table instead of a nested-loop scan. Returns `None` for anything else
/// (non-equality, multi-column AND, cross-table expressions), which falls
/// back to a nested-loop join evaluating the full predicate.
fn split_join_keys(
    on_expr: &Expr,
    left_schema: &Option<Arc<TableSchema>>,
    right_schema: &Option<Arc<TableSchema>>,
) -> Option<(Expr, Expr)> {
    let Expr::BinaryOp {
        op: BinOp::Eq,
        lhs,
        rhs,
    } = on_expr
    else {
        return None;
    };
    if expr_only_refs(lhs, left_schema) && expr_only_refs(rhs, right_schema) {
        return Some(((**lhs).clone(), (**rhs).clone()));
    }
    if expr_only_refs(rhs, left_schema) && expr_only_refs(lhs, right_schema) {
        return Some(((**rhs).clone(), (**lhs).clone()));
    }
    None
}

/// Evaluate a join's ON predicate against one combined (left ++ right) row,
/// with table scopes so qualified column references resolve to the correct
/// side even when both sides share a column name.
fn eval_join_predicate(
    on_expr: &Expr,
    left_row: &[Option<Vec<u8>>],
    right_row: &[Option<Vec<u8>>],
    left_schema: &Option<Arc<TableSchema>>,
    right_schema: &Option<Arc<TableSchema>>,
    left_scopes: &[(String, std::ops::Range<usize>)],
    right_alias: &str,
) -> bool {
    let mut combined = left_row.to_vec();
    combined.extend(right_row.iter().cloned());
    let merged = merge_schema(left_schema, right_schema);
    let right_len = right_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0);
    let mut scopes = left_scopes.to_vec();
    scopes.push((
        right_alias.to_string(),
        left_row.len()..left_row.len() + right_len,
    ));
    RowContext::new(combined, merged)
        .with_table_scopes(scopes)
        .eval(on_expr)
        .map(|v| v.is_truthy())
        .unwrap_or(false)
}

/// Below this row-pair-count product, the CPU nested-loop join is assumed
/// cheaper than a GPU dispatch/readback round trip. Unbenchmarked at scale —
/// see `executor/gpu_join_tests.rs`'s wall-clock comparison for this
/// environment's actual crossover point — so this default is tunable via
/// `TPT_GPU_JOIN_THRESHOLD` rather than hardcoded, since it's inherently
/// hardware/driver dependent.
const DEFAULT_GPU_JOIN_THRESHOLD: u64 = 1_000_000;

fn gpu_join_threshold() -> u64 {
    std::env::var("TPT_GPU_JOIN_THRESHOLD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_GPU_JOIN_THRESHOLD)
}

/// Extracts a row's geometry column as a parsed `Geometry`, evaluating the
/// column expression via the same `RowContext` pattern used elsewhere in
/// `apply_join`. Returns `None` on any eval/parse failure — the row is then
/// simply excluded from the GPU batch (never appears in any output pair),
/// matching the CPU nested-loop path's existing `unwrap_or(false)`
/// "unmatched on eval error" semantics.
fn row_geometry(
    row: &[Option<Vec<u8>>],
    schema: &Option<Arc<TableSchema>>,
    col: &str,
) -> Option<Geometry> {
    let ctx = RowContext::new(row.to_vec(), schema.clone());
    let Value::Text(wkt) = ctx.eval(&Expr::Ident(col.to_string())).ok()? else {
        return None;
    };
    Geometry::from_wkt(&wkt).ok()
}

/// Attempts a GPU-accelerated broad-phase spatial join for a recognized
/// `ST_Intersects`/`ST_DWithin` join predicate. Returns `None` (never an
/// error surfaced to the client) whenever GPU is disabled/unavailable, the
/// batch is below `gpu_join_threshold()`, or the GPU call itself fails for
/// any reason — in every `None` case the caller falls back to the existing,
/// correctness-preserving nested-loop path unchanged. GPU is strictly a
/// performance path here, never a correctness dependency.
pub(super) fn try_gpu_spatial_join(
    pred: &planner::SpatialJoinPredicate,
    left_rows: &[Vec<Option<Vec<u8>>>],
    right_rows: &[Vec<Option<Vec<u8>>>],
    left_schema: &Option<Arc<TableSchema>>,
    right_schema: &Option<Arc<TableSchema>>,
) -> Option<Vec<(u32, u32)>> {
    let pair_count = (left_rows.len() as u64).checked_mul(right_rows.len() as u64)?;
    if pair_count < gpu_join_threshold() {
        return None;
    }

    match pred {
        planner::SpatialJoinPredicate::Intersects {
            left_col,
            right_col,
        } => {
            let left: Vec<GpuBBox> = left_rows
                .iter()
                .filter_map(|r| row_geometry(r, left_schema, left_col))
                .map(|g| {
                    let b = g.bbox();
                    GpuBBox {
                        min_x: b.min_x as f32,
                        min_y: b.min_y as f32,
                        max_x: b.max_x as f32,
                        max_y: b.max_y as f32,
                    }
                })
                .collect();
            let right: Vec<GpuBBox> = right_rows
                .iter()
                .filter_map(|r| row_geometry(r, right_schema, right_col))
                .map(|g| {
                    let b = g.bbox();
                    GpuBBox {
                        min_x: b.min_x as f32,
                        min_y: b.min_y as f32,
                        max_x: b.max_x as f32,
                        max_y: b.max_y as f32,
                    }
                })
                .collect();
            gpu::gpu_bbox_overlap_pairs(&left, &right).ok()
        }
        planner::SpatialJoinPredicate::DWithin {
            left_col,
            right_col,
            radius_m,
        } => {
            let left: Vec<GpuPoint> = left_rows
                .iter()
                .filter_map(|r| row_geometry(r, left_schema, left_col))
                .map(|g| {
                    let c = g.representative_point();
                    GpuPoint {
                        x: c.x as f32,
                        y: c.y as f32,
                    }
                })
                .collect();
            let right: Vec<GpuPoint> = right_rows
                .iter()
                .filter_map(|r| row_geometry(r, right_schema, right_col))
                .map(|g| {
                    let c = g.representative_point();
                    GpuPoint {
                        x: c.x as f32,
                        y: c.y as f32,
                    }
                })
                .collect();
            gpu::gpu_dwithin_pairs(&left, &right, *radius_m as f32).ok()
        }
    }
}

/// Builds joined rows from GPU broad-phase `(left_idx, right_idx)` pairs,
/// optionally null-padding unmatched rows on either side — mirrors the
/// existing nested-loop branches' per-row `matched` bookkeeping, just driven
/// by an index-pair list instead of a predicate evaluated per row-pair.
/// `pad_left`/`pad_right` must match whatever the equivalent nested-loop
/// branch does for the same `JoinType` (including today's `Full`
/// simplification to inner-join semantics — see the `NOTE` on
/// `JoinType::Inner | JoinType::Full` below), so GPU and CPU paths always
/// produce identical row sets for the same query.
fn build_rows_from_pairs(
    pairs: Vec<(u32, u32)>,
    left_rows: &[Vec<Option<Vec<u8>>>],
    right_rows: &[Vec<Option<Vec<u8>>>],
    right_len: usize,
    left_len: usize,
    pad_left: bool,
    pad_right: bool,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let nulls = |n: usize| std::iter::repeat(None).take(n).collect::<Vec<_>>();
    let mut result = Vec::with_capacity(pairs.len());
    let mut matched_left: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut matched_right: std::collections::HashSet<u32> = std::collections::HashSet::new();

    for &(i, j) in &pairs {
        let mut combined = left_rows[i as usize].clone();
        combined.extend(right_rows[j as usize].iter().cloned());
        result.push(combined);
        matched_left.insert(i);
        matched_right.insert(j);
    }

    if pad_left {
        for (i, left_row) in left_rows.iter().enumerate() {
            if !matched_left.contains(&(i as u32)) {
                let mut combined = left_row.clone();
                combined.extend(nulls(right_len));
                result.push(combined);
            }
        }
    }
    if pad_right {
        for (j, right_row) in right_rows.iter().enumerate() {
            if !matched_right.contains(&(j as u32)) {
                let mut combined = nulls(left_len);
                combined.extend(right_row.iter().cloned());
                result.push(combined);
            }
        }
    }
    result
}

/// Apply a JOIN to the result rows. Equi-join conditions (`a.x = b.y`) use a
/// hash join, building from whichever side is smaller; anything else falls
/// back to a nested-loop scan evaluating the full predicate per pair.
pub(super) fn apply_join(
    left_rows: Vec<Vec<Option<Vec<u8>>>>,
    right_rows: Vec<Vec<Option<Vec<u8>>>>,
    join: &Join,
    left_schema: &Option<Arc<TableSchema>>,
    right_schema: &Option<Arc<TableSchema>>,
    left_scopes: &[(String, std::ops::Range<usize>)],
    right_alias: &str,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let right_len = right_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0);
    let left_len = left_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0);
    let nulls = |n: usize| std::iter::repeat(None).take(n).collect::<Vec<_>>();

    match join.join_type {
        JoinType::Cross => {
            let mut result = Vec::new();
            for left_row in &left_rows {
                for right_row in &right_rows {
                    let mut combined = left_row.clone();
                    combined.extend(right_row.iter().cloned());
                    result.push(combined);
                }
            }
            result
        }
        JoinType::Inner | JoinType::Full => {
            let Some(on_expr) = &join.on else {
                return apply_join(
                    left_rows,
                    right_rows,
                    &Join {
                        join_type: JoinType::Cross,
                        table: join.table.clone(),
                        on: None,
                    },
                    left_schema,
                    right_schema,
                    left_scopes,
                    right_alias,
                );
            };
            if let Some((left_key, right_key)) = split_join_keys(on_expr, left_schema, right_schema)
            {
                hash_equi_join(
                    left_rows,
                    right_rows,
                    &left_key,
                    &right_key,
                    left_schema,
                    right_schema,
                )
            } else if let Some(pairs) =
                planner::extract_spatial_join_predicate(on_expr, left_schema, right_schema)
                    .and_then(|pred| {
                        try_gpu_spatial_join(
                            &pred,
                            &left_rows,
                            &right_rows,
                            left_schema,
                            right_schema,
                        )
                    })
            {
                // Full is simplified to inner-join semantics here too (see NOTE
                // below), so no padding on either side — matches the nested-loop
                // fallback's behavior exactly.
                build_rows_from_pairs(
                    pairs,
                    &left_rows,
                    &right_rows,
                    right_len,
                    left_len,
                    false,
                    false,
                )
            } else {
                let mut result = Vec::new();
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        if eval_join_predicate(
                            on_expr,
                            left_row,
                            right_row,
                            left_schema,
                            right_schema,
                            left_scopes,
                            right_alias,
                        ) {
                            let mut combined = left_row.clone();
                            combined.extend(right_row.iter().cloned());
                            result.push(combined);
                        }
                    }
                }
                result
            }
            // NOTE: JoinType::Full is simplified to inner-join semantics for now
            // (unmatched rows from either side are dropped) — full outer join
            // is not yet implemented.
        }
        JoinType::Left => {
            let Some(on_expr) = &join.on else {
                let mut result = Vec::new();
                for left_row in left_rows {
                    if right_rows.is_empty() {
                        let mut combined = left_row.clone();
                        combined.extend(nulls(right_len));
                        result.push(combined);
                        continue;
                    }
                    for right_row in &right_rows {
                        let mut combined = left_row.clone();
                        combined.extend(right_row.iter().cloned());
                        result.push(combined);
                    }
                }
                return result;
            };
            if let Some((left_key, right_key)) = split_join_keys(on_expr, left_schema, right_schema)
            {
                let mut hash_table: HashMap<Vec<u8>, Vec<Vec<Option<Vec<u8>>>>> = HashMap::new();
                for row in &right_rows {
                    let ctx = RowContext::new(row.clone(), right_schema.clone());
                    if let Ok(key) = ctx.eval(&right_key) {
                        if let Some(bytes) = key.to_wire_bytes() {
                            hash_table.entry(bytes).or_default().push(row.clone());
                        }
                    }
                }
                let mut result = Vec::new();
                for left_row in left_rows {
                    let ctx = RowContext::new(left_row.clone(), left_schema.clone());
                    let matches = ctx
                        .eval(&left_key)
                        .ok()
                        .and_then(|k| k.to_wire_bytes())
                        .and_then(|b| hash_table.get(&b));
                    match matches {
                        Some(rows) if !rows.is_empty() => {
                            for right_row in rows {
                                let mut combined = left_row.clone();
                                combined.extend(right_row.iter().cloned());
                                result.push(combined);
                            }
                        }
                        _ => {
                            let mut combined = left_row.clone();
                            combined.extend(nulls(right_len));
                            result.push(combined);
                        }
                    }
                }
                result
            } else if let Some(pairs) =
                planner::extract_spatial_join_predicate(on_expr, left_schema, right_schema)
                    .and_then(|pred| {
                        try_gpu_spatial_join(
                            &pred,
                            &left_rows,
                            &right_rows,
                            left_schema,
                            right_schema,
                        )
                    })
            {
                build_rows_from_pairs(
                    pairs,
                    &left_rows,
                    &right_rows,
                    right_len,
                    left_len,
                    true,
                    false,
                )
            } else {
                let mut result = Vec::new();
                for left_row in &left_rows {
                    let mut matched = false;
                    for right_row in &right_rows {
                        if eval_join_predicate(
                            on_expr,
                            left_row,
                            right_row,
                            left_schema,
                            right_schema,
                            left_scopes,
                            right_alias,
                        ) {
                            matched = true;
                            let mut combined = left_row.clone();
                            combined.extend(right_row.iter().cloned());
                            result.push(combined);
                        }
                    }
                    if !matched {
                        let mut combined = left_row.clone();
                        combined.extend(nulls(right_len));
                        result.push(combined);
                    }
                }
                result
            }
        }
        JoinType::Right => {
            let Some(on_expr) = &join.on else {
                let mut result = Vec::new();
                for right_row in right_rows {
                    if left_rows.is_empty() {
                        let mut combined =
                            nulls(left_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0));
                        combined.extend(right_row.iter().cloned());
                        result.push(combined);
                        continue;
                    }
                    for left_row in &left_rows {
                        let mut combined = left_row.clone();
                        combined.extend(right_row.iter().cloned());
                        result.push(combined);
                    }
                }
                return result;
            };
            if let Some((left_key, right_key)) = split_join_keys(on_expr, left_schema, right_schema)
            {
                let mut hash_table: HashMap<Vec<u8>, Vec<Vec<Option<Vec<u8>>>>> = HashMap::new();
                for row in &left_rows {
                    let ctx = RowContext::new(row.clone(), left_schema.clone());
                    if let Ok(key) = ctx.eval(&left_key) {
                        if let Some(bytes) = key.to_wire_bytes() {
                            hash_table.entry(bytes).or_default().push(row.clone());
                        }
                    }
                }
                let mut result = Vec::new();
                for right_row in right_rows {
                    let ctx = RowContext::new(right_row.clone(), right_schema.clone());
                    let matches = ctx
                        .eval(&right_key)
                        .ok()
                        .and_then(|k| k.to_wire_bytes())
                        .and_then(|b| hash_table.get(&b));
                    match matches {
                        Some(rows) if !rows.is_empty() => {
                            for left_row in rows {
                                let mut combined = left_row.clone();
                                combined.extend(right_row.iter().cloned());
                                result.push(combined);
                            }
                        }
                        _ => {
                            let mut combined =
                                nulls(left_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0));
                            combined.extend(right_row.iter().cloned());
                            result.push(combined);
                        }
                    }
                }
                result
            } else if let Some(pairs) =
                planner::extract_spatial_join_predicate(on_expr, left_schema, right_schema)
                    .and_then(|pred| {
                        try_gpu_spatial_join(
                            &pred,
                            &left_rows,
                            &right_rows,
                            left_schema,
                            right_schema,
                        )
                    })
            {
                build_rows_from_pairs(
                    pairs,
                    &left_rows,
                    &right_rows,
                    right_len,
                    left_len,
                    false,
                    true,
                )
            } else {
                let mut result = Vec::new();
                for right_row in &right_rows {
                    let mut matched = false;
                    for left_row in &left_rows {
                        if eval_join_predicate(
                            on_expr,
                            left_row,
                            right_row,
                            left_schema,
                            right_schema,
                            left_scopes,
                            right_alias,
                        ) {
                            matched = true;
                            let mut combined = left_row.clone();
                            combined.extend(right_row.iter().cloned());
                            result.push(combined);
                        }
                    }
                    if !matched {
                        let mut combined =
                            nulls(left_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0));
                        combined.extend(right_row.iter().cloned());
                        result.push(combined);
                    }
                }
                result
            }
        }
    }
}

/// Hash-based equi-join: build from whichever side is smaller.
pub(super) fn hash_equi_join(
    left_rows: Vec<Vec<Option<Vec<u8>>>>,
    right_rows: Vec<Vec<Option<Vec<u8>>>>,
    left_key: &Expr,
    right_key: &Expr,
    left_schema: &Option<Arc<TableSchema>>,
    right_schema: &Option<Arc<TableSchema>>,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut result = Vec::new();
    if right_rows.len() <= left_rows.len() {
        let mut hash_table: HashMap<Vec<u8>, Vec<Vec<Option<Vec<u8>>>>> = HashMap::new();
        for row in &right_rows {
            let ctx = RowContext::new(row.clone(), right_schema.clone());
            if let Ok(key) = ctx.eval(right_key) {
                if let Some(bytes) = key.to_wire_bytes() {
                    hash_table.entry(bytes).or_default().push(row.clone());
                }
            }
        }
        for left_row in left_rows {
            let ctx = RowContext::new(left_row.clone(), left_schema.clone());
            if let Ok(key) = ctx.eval(left_key) {
                if let Some(bytes) = key.to_wire_bytes() {
                    if let Some(matches) = hash_table.get(&bytes) {
                        for right_row in matches {
                            let mut combined = left_row.clone();
                            combined.extend(right_row.iter().cloned());
                            result.push(combined);
                        }
                    }
                }
            }
        }
    } else {
        let mut hash_table: HashMap<Vec<u8>, Vec<Vec<Option<Vec<u8>>>>> = HashMap::new();
        for row in &left_rows {
            let ctx = RowContext::new(row.clone(), left_schema.clone());
            if let Ok(key) = ctx.eval(left_key) {
                if let Some(bytes) = key.to_wire_bytes() {
                    hash_table.entry(bytes).or_default().push(row.clone());
                }
            }
        }
        for right_row in right_rows {
            let ctx = RowContext::new(right_row.clone(), right_schema.clone());
            if let Ok(key) = ctx.eval(right_key) {
                if let Some(bytes) = key.to_wire_bytes() {
                    if let Some(matches) = hash_table.get(&bytes) {
                        for left_row in matches {
                            let mut combined = left_row.clone();
                            combined.extend(right_row.iter().cloned());
                            result.push(combined);
                        }
                    }
                }
            }
        }
    }
    result
}
