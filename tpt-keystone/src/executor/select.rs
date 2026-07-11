//! The SELECT execution pipeline: CTE materialization, FROM/JOIN resolution,
//! WHERE, GROUP BY/aggregation, window functions, ORDER BY, LIMIT/OFFSET,
//! and projection.

use std::collections::HashMap;
use std::sync::Arc;

use super::eval::{self, eval_expr, value_compare, OuterRow, RowContext, Value};
use super::{build_fields, infer_column_name, merge_schema, resolve_table_ref, schema_from_fields};
use super::{join, planner};
use super::{CteContext, QueryResult};
use crate::sql::ast::{Expr, FrameBound, FrameBoundType, InList, Projection, SelectStmt, UnionOp};
use crate::storage::database::Database;
use crate::storage::TableSchema;
use crate::wire::messages::FieldDescription;

/// Execute a SELECT statement with CTE, correlation, and parameter-binding context.
pub(super) fn execute_select_with_cte(
    select: SelectStmt,
    db: Arc<Database>,
    cte_ctx: &mut CteContext,
    outer: &[OuterRow],
    params: &[Value],
) -> anyhow::Result<QueryResult> {
    // Set operations (UNION / UNION ALL) are handled generically: evaluate
    // both sides fully (including their own nested CTEs/unions) and combine.
    if let Some((op, rhs)) = select.union.clone() {
        let mut left = select.clone();
        left.union = None;
        let left_result = execute_select_with_cte(left, db.clone(), cte_ctx, outer, params)?;
        let right_result = execute_select_with_cte(*rhs, db, cte_ctx, outer, params)?;
        let mut rows = left_result.rows;
        rows.extend(right_result.rows);
        if op == UnionOp::Union {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|r| seen.insert(format!("{r:?}")));
        }
        let row_count = rows.len();
        return Ok(QueryResult {
            fields: left_result.fields,
            rows,
            tag: format!("SELECT {row_count}"),
        });
    }

    for cte in &select.ctes {
        materialize_cte(cte, db.clone(), cte_ctx, outer, params)?;
    }

    let Some(table_with_joins) = &select.from else {
        return execute_projection_only(&select, db, params);
    };

    let (primary_schema, primary_rows) = planner::resolve_primary_table(
        &table_with_joins.primary,
        &select.where_,
        &db,
        cte_ctx,
        outer,
        params,
    )?;

    let primary_alias = table_with_joins
        .primary
        .alias
        .clone()
        .unwrap_or_else(|| table_with_joins.primary.name.clone());
    let primary_len = primary_schema
        .as_ref()
        .map(|s| s.columns.len())
        .unwrap_or(0);
    let mut table_scopes = vec![(primary_alias, 0..primary_len)];
    let mut scope_offset = primary_len;

    let mut result_rows = primary_rows;
    let mut result_schema = primary_schema;
    let join_order = planner::reorder_inner_joins(
        &table_scopes[0].0,
        &result_schema,
        &table_with_joins.joins,
        &db,
    );
    for &join_idx in &join_order {
        let jn = &table_with_joins.joins[join_idx];
        let (join_schema, join_rows) = resolve_table_ref(&jn.table, &db, cte_ctx, outer, params)?;
        let join_len = join_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0);
        let join_alias = jn
            .table
            .alias
            .clone()
            .unwrap_or_else(|| jn.table.name.clone());
        result_rows = join::apply_join(
            result_rows,
            join_rows,
            jn,
            &result_schema,
            &join_schema,
            &table_scopes,
            &join_alias,
        );
        result_schema = merge_schema(&result_schema, &join_schema);
        table_scopes.push((join_alias, scope_offset..scope_offset + join_len));
        scope_offset += join_len;
    }

    let fields = build_fields(&select.projections, &result_schema)?;

    // Build one RowContext per row, then apply WHERE.
    let mut rows: Vec<RowContext> = result_rows
        .into_iter()
        .map(|values| {
            RowContext::with_db(values, result_schema.clone(), db.clone())
                .with_outer(outer.to_vec())
                .with_params(params.to_vec())
                .with_table_scopes(table_scopes.clone())
        })
        .collect();

    if let Some(ref where_expr) = select.where_ {
        rows.retain(|ctx| ctx.eval(where_expr).map(|v| v.is_truthy()).unwrap_or(false));
    }

    // GROUP BY / aggregates: collapse `rows` into one representative
    // RowContext per group, with aggregate values pre-computed and attached.
    if is_aggregate_query(&select) {
        rows = execute_aggregation(&select, rows, &result_schema, &db)?;
    }

    apply_window_functions(&select, &mut rows)?;

    if !select.order_by.is_empty() {
        rows.sort_by(|a, b| {
            for order in &select.order_by {
                let va = a.eval(&order.expr).unwrap_or(Value::Null);
                let vb = b.eval(&order.expr).unwrap_or(Value::Null);
                let cmp = value_compare(&va, &vb).unwrap_or(0);
                if cmp != 0 {
                    return if order.asc == (cmp < 0) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(ref limit_expr) = select.limit {
        let limit = match eval_expr(limit_expr, params)? {
            Value::Int(n) => n.max(0) as usize,
            Value::Float(f) => f.max(0.0) as usize,
            _ => rows.len(),
        };
        rows.truncate(limit);
    }

    if let Some(ref offset_expr) = select.offset {
        let offset = match eval_expr(offset_expr, params)? {
            Value::Int(n) => n.max(0) as usize,
            Value::Float(f) => f.max(0.0) as usize,
            _ => 0,
        };
        rows = if offset < rows.len() {
            rows.split_off(offset)
        } else {
            Vec::new()
        };
    }

    let mut projected_rows = Vec::with_capacity(rows.len());
    for ctx in &rows {
        let mut cells = Vec::new();
        for proj in &select.projections {
            match proj {
                Projection::Wildcard => cells.extend(ctx.values.iter().cloned()),
                Projection::WildcardTable(_) => unreachable!("rejected in build_fields"),
                Projection::Expr { expr, .. } => {
                    cells.push(ctx.eval(expr).ok().and_then(|v| v.to_wire_bytes()))
                }
            }
        }
        projected_rows.push(cells);
    }

    let row_count = projected_rows.len();
    Ok(QueryResult {
        fields,
        rows: projected_rows,
        tag: format!("SELECT {row_count}"),
    })
}

/// A no-FROM SELECT: evaluate projections once against an empty row context.
fn execute_projection_only(
    select: &SelectStmt,
    db: Arc<Database>,
    params: &[Value],
) -> anyhow::Result<QueryResult> {
    let mut ctx = RowContext::empty().with_params(params.to_vec());
    ctx.db = Some(db);
    let mut fields = Vec::new();
    let mut row: Vec<Option<Vec<u8>>> = Vec::new();

    for proj in &select.projections {
        match proj {
            Projection::Wildcard => anyhow::bail!("SELECT * requires a FROM clause"),
            Projection::WildcardTable(_) => anyhow::bail!("SELECT table.* requires a FROM clause"),
            Projection::Expr { expr, alias } => {
                let value = ctx.eval(expr)?;
                let col_name = alias.clone().unwrap_or_else(|| infer_column_name(expr));
                fields.push(FieldDescription::simple(col_name, value.type_oid()));
                row.push(value.to_wire_bytes());
            }
        }
    }

    Ok(QueryResult {
        fields,
        rows: vec![row],
        tag: "SELECT 1".into(),
    })
}

/// Materialize a CTE's result (iteratively, if `RECURSIVE`) into `cte_ctx`.
pub(super) fn materialize_cte(
    cte: &crate::sql::ast::Cte,
    db: Arc<Database>,
    cte_ctx: &mut CteContext,
    outer: &[OuterRow],
    params: &[Value],
) -> anyhow::Result<()> {
    if cte.recursive {
        if let Some((_, ref recursive_term)) = cte.subquery.union {
            let mut base = cte.subquery.clone();
            base.union = None;
            let base_result = execute_select_with_cte(base, db.clone(), cte_ctx, outer, params)?;
            let schema = cte_schema(cte, &base_result.fields);

            let mut all_rows = base_result.rows.clone();
            let mut working = base_result.rows;
            let mut iterations = 0usize;
            loop {
                cte_ctx
                    .ctes
                    .insert(cte.name.clone(), (working.clone(), schema.clone()));
                let step = execute_select_with_cte(
                    (**recursive_term).clone(),
                    db.clone(),
                    cte_ctx,
                    outer,
                    params,
                )?;
                if step.rows.is_empty() {
                    break;
                }
                all_rows.extend(step.rows.clone());
                working = step.rows;
                iterations += 1;
                if iterations > 10_000 {
                    anyhow::bail!("recursive CTE \"{}\" exceeded iteration limit", cte.name);
                }
            }
            cte_ctx.ctes.insert(cte.name.clone(), (all_rows, schema));
            return Ok(());
        }
        // RECURSIVE declared but no UNION body — fall through as non-recursive.
    }
    let result = execute_select_with_cte(cte.subquery.clone(), db, cte_ctx, outer, params)?;
    let schema = cte_schema(cte, &result.fields);
    cte_ctx.ctes.insert(cte.name.clone(), (result.rows, schema));
    Ok(())
}

fn cte_schema(cte: &crate::sql::ast::Cte, fields: &[FieldDescription]) -> Arc<TableSchema> {
    if !cte.columns.is_empty() {
        let columns: Vec<crate::storage::ColumnDef> = cte
            .columns
            .iter()
            .map(|col_name| crate::storage::ColumnDef {
                name: col_name.clone(),
                col_type: crate::storage::ColumnType::Text,
                nullable: true,
                default: None,
                is_pk: false,
            })
            .collect();
        Arc::new(TableSchema {
            name: cte.name.clone(),
            columns,
            pk_columns: vec![],
            unique_groups: vec![],
            foreign_keys: vec![],
            json_schemas: vec![],
        })
    } else {
        Arc::new(schema_from_fields(&cte.name, fields))
    }
}

/// Whether a SELECT requires grouping/aggregation (GROUP BY present, or any
/// projection/HAVING expression contains an aggregate function call).
fn is_aggregate_query(select: &SelectStmt) -> bool {
    if !select.group_by.is_empty() {
        return true;
    }
    for proj in &select.projections {
        if let Projection::Expr { expr, .. } = proj {
            if contains_aggregate(expr) {
                return true;
            }
        }
    }
    select.having.as_ref().is_some_and(contains_aggregate)
}

fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. } => {
            eval::is_aggregate_name(name) || args.iter().any(contains_aggregate)
        }
        Expr::BinaryOp { lhs, rhs, .. } => contains_aggregate(lhs) || contains_aggregate(rhs),
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::IsNull { expr, .. } | Expr::IsTrue { expr, .. } | Expr::IsFalse { expr, .. } => {
            contains_aggregate(expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        Expr::Like { expr, pattern, .. } => contains_aggregate(expr) || contains_aggregate(pattern),
        Expr::In { expr, list, .. } => {
            contains_aggregate(expr)
                || matches!(list, InList::Exprs(v) if v.iter().any(contains_aggregate))
        }
        Expr::Cast { expr, .. } => contains_aggregate(expr),
        Expr::Case {
            operand,
            branches,
            else_,
        } => {
            operand.as_ref().is_some_and(|e| contains_aggregate(e))
                || branches
                    .iter()
                    .any(|(c, r)| contains_aggregate(c) || contains_aggregate(r))
                || else_.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        _ => false,
    }
}

fn collect_aggregate_exprs(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Function { name, args, .. } if eval::is_aggregate_name(name) => {
            out.push(expr.clone());
            let _ = args;
        }
        Expr::Function { args, .. } => {
            for a in args {
                collect_aggregate_exprs(a, out);
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_aggregate_exprs(lhs, out);
            collect_aggregate_exprs(rhs, out);
        }
        Expr::UnaryOp { expr, .. } => collect_aggregate_exprs(expr, out),
        Expr::IsNull { expr, .. } | Expr::IsTrue { expr, .. } | Expr::IsFalse { expr, .. } => {
            collect_aggregate_exprs(expr, out)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_aggregate_exprs(expr, out);
            collect_aggregate_exprs(low, out);
            collect_aggregate_exprs(high, out);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_aggregate_exprs(expr, out);
            collect_aggregate_exprs(pattern, out);
        }
        Expr::In { expr, list, .. } => {
            collect_aggregate_exprs(expr, out);
            if let InList::Exprs(v) = list {
                for e in v {
                    collect_aggregate_exprs(e, out);
                }
            }
        }
        Expr::Cast { expr, .. } => collect_aggregate_exprs(expr, out),
        Expr::Case {
            operand,
            branches,
            else_,
        } => {
            if let Some(e) = operand {
                collect_aggregate_exprs(e, out);
            }
            for (c, r) in branches {
                collect_aggregate_exprs(c, out);
                collect_aggregate_exprs(r, out);
            }
            if let Some(e) = else_ {
                collect_aggregate_exprs(e, out);
            }
        }
        _ => {}
    }
}

/// Collapse `rows` into one RowContext per GROUP BY group, with aggregate
/// function results pre-computed into each group's `computed` cache and
/// HAVING already applied.
fn execute_aggregation(
    select: &SelectStmt,
    rows: Vec<RowContext>,
    schema: &Option<Arc<TableSchema>>,
    db: &Arc<Database>,
) -> anyhow::Result<Vec<RowContext>> {
    let mut group_order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<RowContext>> = HashMap::new();

    if select.group_by.is_empty() {
        // A single implicit group over all (possibly zero) rows.
        group_order.push(String::new());
        groups.insert(String::new(), rows);
    } else {
        for ctx in rows {
            let key_vals: Vec<Value> = select
                .group_by
                .iter()
                .map(|e| ctx.eval(e))
                .collect::<anyhow::Result<_>>()?;
            let key = format!("{key_vals:?}");
            if !groups.contains_key(&key) {
                group_order.push(key.clone());
            }
            groups.entry(key).or_default().push(ctx);
        }
    }

    let mut agg_exprs: Vec<Expr> = Vec::new();
    for proj in &select.projections {
        if let Projection::Expr { expr, .. } = proj {
            collect_aggregate_exprs(expr, &mut agg_exprs);
        }
    }
    if let Some(h) = &select.having {
        collect_aggregate_exprs(h, &mut agg_exprs);
    }

    let mut output = Vec::new();
    for key in group_order {
        let members = groups.remove(&key).unwrap_or_default();
        let member_rows: Vec<Vec<Option<Vec<u8>>>> =
            members.iter().map(|m| m.values.clone()).collect();

        let representative = members.into_iter().next().unwrap_or_else(|| {
            let width = schema.as_ref().map(|s| s.columns.len()).unwrap_or(0);
            RowContext::with_db(vec![None; width], schema.clone(), db.clone())
        });

        let mut computed = HashMap::new();
        for agg_expr in &agg_exprs {
            let computed_key = format!("{agg_expr:?}");
            if computed.contains_key(&computed_key) {
                continue;
            }
            if let Expr::Function {
                name,
                args,
                distinct,
            } = agg_expr
            {
                let value = eval::eval_aggregate(
                    name,
                    args.first(),
                    *distinct,
                    &member_rows,
                    schema,
                    &Some(db.clone()),
                )?;
                computed.insert(computed_key, value);
            }
        }

        let group_ctx = representative.with_computed(Arc::new(computed));

        if let Some(h) = &select.having {
            if !group_ctx.eval(h)?.is_truthy() {
                continue;
            }
        }
        output.push(group_ctx);
    }

    Ok(output)
}

/// Evaluate any window function calls in projections/ORDER BY and attach
/// their per-row results into each row's `computed` cache.
fn apply_window_functions(select: &SelectStmt, rows: &mut [RowContext]) -> anyhow::Result<()> {
    let mut window_exprs: Vec<Expr> = Vec::new();
    for proj in &select.projections {
        if let Projection::Expr { expr, .. } = proj {
            collect_window_exprs(expr, &mut window_exprs);
        }
    }
    for ob in &select.order_by {
        collect_window_exprs(&ob.expr, &mut window_exprs);
    }
    if window_exprs.is_empty() || rows.is_empty() {
        return Ok(());
    }

    let mut extra: Vec<HashMap<String, Value>> = vec![HashMap::new(); rows.len()];

    for win in &window_exprs {
        let Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } = win
        else {
            continue;
        };
        let key = format!("{win:?}");

        let mut order: Vec<usize> = (0..rows.len()).collect();
        order.sort_by(|&a, &b| {
            for p in partition_by {
                let va = rows[a].eval(p).unwrap_or(Value::Null);
                let vb = rows[b].eval(p).unwrap_or(Value::Null);
                if let Ok(c) = value_compare(&va, &vb) {
                    if c != 0 {
                        return if c < 0 {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        };
                    }
                }
            }
            for ob in order_by {
                let va = rows[a].eval(&ob.expr).unwrap_or(Value::Null);
                let vb = rows[b].eval(&ob.expr).unwrap_or(Value::Null);
                if let Ok(c) = value_compare(&va, &vb) {
                    let c = if ob.asc { c } else { -c };
                    if c != 0 {
                        return if c < 0 {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        };
                    }
                }
            }
            std::cmp::Ordering::Equal
        });

        let same_partition = |rows: &[RowContext], i: usize, j: usize| -> bool {
            partition_by.iter().all(|p| {
                let vi = rows[order[i]].eval(p).unwrap_or(Value::Null);
                let vj = rows[order[j]].eval(p).unwrap_or(Value::Null);
                value_compare(&vi, &vj).is_ok_and(|c| c == 0)
            })
        };

        let mut ranges: Vec<(usize, usize)> = Vec::new();
        let mut start = 0usize;
        for i in 1..order.len() {
            if !same_partition(rows, i - 1, i) {
                ranges.push((start, i));
                start = i;
            }
        }
        ranges.push((start, order.len()));

        for (part_start, part_end) in ranges {
            for i in part_start..part_end {
                let pos = i - part_start;
                let value = compute_window_value(
                    func, args, frame, order_by, rows, &order, part_start, part_end, i, pos,
                )?;
                extra[order[i]].insert(key.clone(), value);
            }
        }
    }

    for (ctx, new_vals) in rows.iter_mut().zip(extra.into_iter()) {
        if new_vals.is_empty() {
            continue;
        }
        let mut merged = ctx
            .computed
            .as_ref()
            .map(|m| (**m).clone())
            .unwrap_or_default();
        merged.extend(new_vals);
        ctx.computed = Some(Arc::new(merged));
    }
    Ok(())
}

fn collect_window_exprs(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Window { .. } => out.push(expr.clone()),
        Expr::Function { args, .. } => {
            for a in args {
                collect_window_exprs(a, out);
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_window_exprs(lhs, out);
            collect_window_exprs(rhs, out);
        }
        Expr::UnaryOp { expr, .. } => collect_window_exprs(expr, out),
        Expr::IsNull { expr, .. } | Expr::IsTrue { expr, .. } | Expr::IsFalse { expr, .. } => {
            collect_window_exprs(expr, out)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_window_exprs(expr, out);
            collect_window_exprs(low, out);
            collect_window_exprs(high, out);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_window_exprs(expr, out);
            collect_window_exprs(pattern, out);
        }
        Expr::In { expr, list, .. } => {
            collect_window_exprs(expr, out);
            if let InList::Exprs(v) = list {
                for e in v {
                    collect_window_exprs(e, out);
                }
            }
        }
        Expr::Cast { expr, .. } => collect_window_exprs(expr, out),
        Expr::Case {
            operand,
            branches,
            else_,
        } => {
            if let Some(e) = operand {
                collect_window_exprs(e, out);
            }
            for (c, r) in branches {
                collect_window_exprs(c, out);
                collect_window_exprs(r, out);
            }
            if let Some(e) = else_ {
                collect_window_exprs(e, out);
            }
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn compute_window_value(
    func: &str,
    args: &[Expr],
    frame: &Option<crate::sql::ast::WindowFrame>,
    order_by: &[crate::sql::ast::OrderBy],
    rows: &[RowContext],
    order: &[usize],
    part_start: usize,
    part_end: usize,
    i: usize,
    pos: usize,
) -> anyhow::Result<Value> {
    let fname = func.to_lowercase();
    match fname.as_str() {
        "row_number" => Ok(Value::Int(pos as i64 + 1)),
        "rank" | "dense_rank" => {
            if order_by.is_empty() {
                return Ok(Value::Int(1));
            }
            let mut rank = 1i64;
            let mut dense = 1i64;
            for k in (part_start + 1)..=i {
                let is_peer = order_by.iter().all(|ob| {
                    let va = rows[order[k - 1]].eval(&ob.expr).unwrap_or(Value::Null);
                    let vb = rows[order[k]].eval(&ob.expr).unwrap_or(Value::Null);
                    value_compare(&va, &vb).is_ok_and(|c| c == 0)
                });
                if !is_peer {
                    rank = (k - part_start) as i64 + 1;
                    dense += 1;
                }
            }
            Ok(Value::Int(if fname == "rank" { rank } else { dense }))
        }
        "ntile" => {
            let n = match args.first() {
                Some(e) => match rows[order[i]].eval(e)? {
                    Value::Int(n) => n,
                    Value::Float(f) => f as i64,
                    _ => anyhow::bail!("ntile() requires an integer argument"),
                },
                None => anyhow::bail!("ntile() requires an argument"),
            };
            if n <= 0 {
                anyhow::bail!("ntile() argument must be positive");
            }
            let part_len = (part_end - part_start) as i64;
            let bucket = (pos as i64 * n / part_len.max(1)) + 1;
            Ok(Value::Int(bucket.min(n)))
        }
        "lag" | "lead" => {
            let arg = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("{fname}() requires an argument"))?;
            let offset = match args.get(1) {
                Some(e) => match rows[order[i]].eval(e)? {
                    Value::Int(n) => n,
                    Value::Float(f) => f as i64,
                    _ => 1,
                },
                None => 1,
            };
            let default = args.get(2);
            let target_pos = if fname == "lag" {
                pos as i64 - offset
            } else {
                pos as i64 + offset
            };
            if target_pos < 0 || target_pos as usize >= (part_end - part_start) {
                return match default {
                    Some(d) => rows[order[i]].eval(d),
                    None => Ok(Value::Null),
                };
            }
            let target_idx = order[part_start + target_pos as usize];
            rows[target_idx].eval(arg)
        }
        "sum" | "avg" | "count" | "min" | "max" => {
            let (fs, fe) =
                resolve_frame_bounds(frame, order_by, rows, order, part_start, part_end, pos)?;
            let member_rows: Vec<Vec<Option<Vec<u8>>>> = order[fs..fe]
                .iter()
                .map(|&idx| rows[idx].values.clone())
                .collect();
            let schema = rows[order[i]].schema.clone();
            let db = rows[order[i]].db.clone();
            let is_star = fname == "count"
                && (args.is_empty() || matches!(args.first(), Some(Expr::Ident(s)) if s == "*"));
            let arg = if is_star { None } else { args.first() };
            eval::eval_aggregate(&fname, arg, false, &member_rows, &schema, &db)
        }
        // Chronos: `interpolate(value)` — for a NULL `value` at this row,
        // linearly interpolates between the nearest non-null values before
        // and after it in partition/ORDER BY order (falls back to whichever
        // single neighbor exists at a partition edge). Non-null values pass
        // through unchanged. `gap_fill()` (materializing missing timestamp
        // rows entirely) is a documented scope cut — see TODO.md — since it
        // would need to insert rows into the result set, not just compute a
        // per-row value like every other window function here.
        "interpolate" => {
            let arg = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("interpolate() requires 1 argument"))?;
            let current = rows[order[i]].eval(arg)?;
            if !matches!(current, Value::Null) {
                return Ok(current);
            }
            let mut before: Option<(usize, f64)> = None;
            for k in (part_start..i).rev() {
                if let Ok(v) = rows[order[k]].eval(arg) {
                    if let Ok(f) = v.as_f64() {
                        before = Some((k, f));
                        break;
                    }
                }
            }
            let mut after: Option<(usize, f64)> = None;
            for k in (i + 1)..part_end {
                if let Ok(v) = rows[order[k]].eval(arg) {
                    if let Ok(f) = v.as_f64() {
                        after = Some((k, f));
                        break;
                    }
                }
            }
            match (before, after) {
                (Some((bk, bv)), Some((ak, av))) => {
                    let frac = (i - bk) as f64 / (ak - bk) as f64;
                    Ok(Value::Float(bv + (av - bv) * frac))
                }
                (Some((_, bv)), None) => Ok(Value::Float(bv)),
                (None, Some((_, av))) => Ok(Value::Float(av)),
                (None, None) => Ok(Value::Null),
            }
        }
        // Chronos: `moving_average(value, window_size)` — average of the
        // trailing `window_size` rows (this row inclusive) in partition/
        // ORDER BY order, clamped to the start of the partition. Defines
        // its own frame from `window_size` rather than requiring the caller
        // to spell out `ROWS BETWEEN n PRECEDING AND CURRENT ROW`.
        "moving_average" => {
            let value_arg = args.first().ok_or_else(|| {
                anyhow::anyhow!("moving_average() requires 2 arguments (value, window_size)")
            })?;
            let window_size = match args.get(1) {
                Some(e) => match rows[order[i]].eval(e)? {
                    Value::Int(n) => n.max(1) as usize,
                    Value::Float(f) => (f as i64).max(1) as usize,
                    _ => anyhow::bail!("moving_average() window_size must be an integer"),
                },
                None => anyhow::bail!("moving_average() requires 2 arguments (value, window_size)"),
            };
            let fs = part_start + pos.saturating_sub(window_size - 1);
            let fe = i + 1;
            let member_rows: Vec<Vec<Option<Vec<u8>>>> = order[fs..fe]
                .iter()
                .map(|&idx| rows[idx].values.clone())
                .collect();
            let schema = rows[order[i]].schema.clone();
            let db = rows[order[i]].db.clone();
            eval::eval_aggregate("avg", Some(value_arg), false, &member_rows, &schema, &db)
        }
        other => anyhow::bail!("window function \"{other}\" is not supported"),
    }
}

/// Resolve a window frame (default, ROWS, or RANGE/GROUPS — the latter two
/// currently treated the same as ROWS) to an absolute `[start, end)` slice
/// of `order` for the current row.
fn resolve_frame_bounds(
    frame: &Option<crate::sql::ast::WindowFrame>,
    order_by: &[crate::sql::ast::OrderBy],
    rows: &[RowContext],
    order: &[usize],
    part_start: usize,
    part_end: usize,
    pos: usize,
) -> anyhow::Result<(usize, usize)> {
    match frame {
        None => {
            if order_by.is_empty() {
                Ok((part_start, part_end))
            } else {
                let i = part_start + pos;
                let mut end = i;
                while end + 1 < part_end {
                    let is_peer = order_by.iter().all(|ob| {
                        let va = rows[order[end]].eval(&ob.expr).unwrap_or(Value::Null);
                        let vb = rows[order[end + 1]].eval(&ob.expr).unwrap_or(Value::Null);
                        value_compare(&va, &vb).is_ok_and(|c| c == 0)
                    });
                    if !is_peer {
                        break;
                    }
                    end += 1;
                }
                Ok((part_start, end + 1))
            }
        }
        Some(f) => {
            let part_len = (part_end - part_start) as i64;
            let start_rel = frame_bound_offset(&f.start, pos, part_len)?;
            let end_rel = match &f.end {
                Some(b) => frame_bound_offset(b, pos, part_len)?,
                None => 0,
            };
            let start_pos = (pos as i64 + start_rel).clamp(0, part_len - 1);
            let end_pos = (pos as i64 + end_rel).clamp(0, part_len - 1);
            let (lo, hi) = if start_pos <= end_pos {
                (start_pos, end_pos)
            } else {
                (end_pos, start_pos)
            };
            Ok((part_start + lo as usize, part_start + hi as usize + 1))
        }
    }
}

fn frame_bound_offset(bound: &FrameBound, pos: usize, part_len: i64) -> anyhow::Result<i64> {
    match bound.bound_type {
        FrameBoundType::UnboundedPreceding => Ok(-(pos as i64)),
        FrameBoundType::UnboundedFollowing => Ok(part_len - 1 - pos as i64),
        FrameBoundType::CurrentRow => Ok(0),
        FrameBoundType::Preceding => Ok(-frame_offset_int(&bound.offset)?),
        FrameBoundType::Following => Ok(frame_offset_int(&bound.offset)?),
    }
}

fn frame_offset_int(offset: &Option<Box<Expr>>) -> anyhow::Result<i64> {
    match offset {
        Some(e) => match eval_expr(e, &[])? {
            Value::Int(n) => Ok(n),
            Value::Float(f) => Ok(f as i64),
            _ => Ok(0),
        },
        None => Ok(0),
    }
}
