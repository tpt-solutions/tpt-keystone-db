mod catalog;
pub mod eval;
mod planner;

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql;
use crate::sql::ast::{BinOp, Expr, FrameBound, FrameBoundType, InList, Join, JoinType, Projection, SelectStmt, Stmt, TableRef, UnionOp};
use crate::storage::database::Database;
use crate::storage::{ColumnDef, ColumnType, StorageEngine, TableSchema};
use crate::wire::messages::{FieldDescription, oid};
use eval::{eval_expr, value_compare, OuterRow, RowContext, Value};

/// The result of executing a query.
pub struct QueryResult {
    pub fields: Vec<FieldDescription>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub tag: String,
}

/// Context for CTE execution, mapping CTE names to their materialized results.
pub struct CteContext {
    /// Maps CTE name to (rows, schema)
    pub ctes: HashMap<String, (Vec<Vec<Option<Vec<u8>>>>, Arc<TableSchema>)>,
}

impl CteContext {
    pub fn new() -> Self {
        Self { ctes: HashMap::new() }
    }

    pub fn get(&self, name: &str) -> Option<&(Vec<Vec<Option<Vec<u8>>>>, Arc<TableSchema>)> {
        self.ctes.get(name)
    }
}

/// Parse and execute a SQL statement, returning a QueryResult.
pub fn execute_query(sql_text: &str, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    execute_parsed(sql::parse(sql_text)?, db, &[])
}

/// Execute an already-parsed statement, with bound `$n` parameter values
/// (used by the extended query protocol's Bind/Execute).
pub fn execute_parsed(stmt: Stmt, db: Arc<Database>, params: &[Value]) -> anyhow::Result<QueryResult> {
    match stmt {
        Stmt::Select(select) => execute_select_with_cte(select, db, &mut CteContext::new(), &[], params),
        Stmt::Insert(insert) => execute_insert(insert, db, params),
        Stmt::Delete(delete) => execute_delete(delete, db, params),
        Stmt::Update(update) => execute_update(update, db, params),
        Stmt::CreateTable(ct) => execute_create_table(ct, db),
        Stmt::DropTable(dt) => execute_drop_table(dt, db),
        Stmt::CreateIndex(ci) => execute_create_index(ci, db),
        Stmt::Set(s) => {
            tracing::debug!("SET {} = {:?} (ignored)", s.name, s.value);
            Ok(QueryResult { fields: vec![], rows: vec![], tag: "SET".into() })
        }
        Stmt::Show(s) => {
            let field = FieldDescription::simple(&s.name, oid::TEXT);
            Ok(QueryResult {
                fields: vec![field],
                rows: vec![vec![Some(b"".to_vec())]],
                tag: "SHOW".into(),
            })
        }
        Stmt::Begin => Ok(QueryResult { fields: vec![], rows: vec![], tag: "BEGIN".into() }),
        Stmt::Commit => Ok(QueryResult { fields: vec![], rows: vec![], tag: "COMMIT".into() }),
        Stmt::Rollback => Ok(QueryResult { fields: vec![], rows: vec![], tag: "ROLLBACK".into() }),
        Stmt::AlterTable(_) => {
            // TODO: Implement ALTER TABLE
            Ok(QueryResult { fields: vec![], rows: vec![], tag: "ALTER TABLE".into() })
        }
        Stmt::DeclareCursor(_) | Stmt::Fetch(_) | Stmt::MoveCursor(_) | Stmt::CloseCursor(_) => {
            anyhow::bail!("cursor statements are only supported over the simple query protocol")
        }
        Stmt::Notify(channel, payload) => {
            db.notify(&channel, payload.as_deref().unwrap_or(""));
            Ok(QueryResult { fields: vec![], rows: vec![], tag: "NOTIFY".into() })
        }
        Stmt::Listen(_) | Stmt::Unlisten(_) => {
            anyhow::bail!("LISTEN/UNLISTEN are only supported over the simple query protocol")
        }
    }
}

/// Execute a SELECT statement with CTE, correlation, and parameter-binding context.
pub fn execute_select_with_cte(
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
        return Ok(QueryResult { fields: left_result.fields, rows, tag: format!("SELECT {row_count}") });
    }

    for cte in &select.ctes {
        materialize_cte(cte, db.clone(), cte_ctx, outer, params)?;
    }

    let Some(table_with_joins) = &select.from else {
        return execute_projection_only(&select, params);
    };

    let (primary_schema, primary_rows) = planner::resolve_primary_table(&table_with_joins.primary, &select.where_, &db, cte_ctx, outer, params)?;

    let primary_alias = table_with_joins.primary.alias.clone().unwrap_or_else(|| table_with_joins.primary.name.clone());
    let primary_len = primary_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0);
    let mut table_scopes = vec![(primary_alias, 0..primary_len)];
    let mut scope_offset = primary_len;

    let mut result_rows = primary_rows;
    let mut result_schema = primary_schema;
    for join in &table_with_joins.joins {
        let (join_schema, join_rows) = resolve_table_ref(&join.table, &db, cte_ctx, outer, params)?;
        let join_len = join_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0);
        let join_alias = join.table.alias.clone().unwrap_or_else(|| join.table.name.clone());
        result_rows = apply_join(result_rows, join_rows, join, &result_schema, &join_schema, &table_scopes, &join_alias);
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
                    return if order.asc == (cmp < 0) { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
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
        rows = if offset < rows.len() { rows.split_off(offset) } else { Vec::new() };
    }

    let mut projected_rows = Vec::with_capacity(rows.len());
    for ctx in &rows {
        let mut cells = Vec::new();
        for proj in &select.projections {
            match proj {
                Projection::Wildcard => cells.extend(ctx.values.iter().cloned()),
                Projection::WildcardTable(_) => unreachable!("rejected in build_fields"),
                Projection::Expr { expr, .. } => cells.push(ctx.eval(expr).ok().and_then(|v| v.to_wire_bytes())),
            }
        }
        projected_rows.push(cells);
    }

    let row_count = projected_rows.len();
    Ok(QueryResult { fields, rows: projected_rows, tag: format!("SELECT {row_count}") })
}

/// The highest `$n` parameter index referenced anywhere in a statement —
/// used to size the extended query protocol's `ParameterDescription`.
pub fn max_param_index(stmt: &Stmt) -> u32 {
    let mut max = 0u32;
    match stmt {
        Stmt::Select(s) => scan_select_params(s, &mut max),
        Stmt::Insert(i) => for row in &i.values { for e in row { scan_expr_params(e, &mut max); } },
        Stmt::Update(u) => {
            for (_, e) in &u.assignments { scan_expr_params(e, &mut max); }
            if let Some(w) = &u.where_ { scan_expr_params(w, &mut max); }
        }
        Stmt::Delete(d) => {
            if let Some(w) = &d.where_ { scan_expr_params(w, &mut max); }
        }
        _ => {}
    }
    max
}

fn scan_select_params(s: &SelectStmt, max: &mut u32) {
    for cte in &s.ctes { scan_select_params(&cte.subquery, max); }
    for p in &s.projections {
        if let Projection::Expr { expr, .. } = p { scan_expr_params(expr, max); }
    }
    if let Some(w) = &s.where_ { scan_expr_params(w, max); }
    for g in &s.group_by { scan_expr_params(g, max); }
    if let Some(h) = &s.having { scan_expr_params(h, max); }
    for o in &s.order_by { scan_expr_params(&o.expr, max); }
    if let Some(l) = &s.limit { scan_expr_params(l, max); }
    if let Some(o) = &s.offset { scan_expr_params(o, max); }
    if let Some((_, rhs)) = &s.union { scan_select_params(rhs, max); }
}

fn scan_expr_params(expr: &Expr, max: &mut u32) {
    match expr {
        Expr::Param(n) => { if *n > *max { *max = *n; } }
        Expr::BinaryOp { lhs, rhs, .. } => { scan_expr_params(lhs, max); scan_expr_params(rhs, max); }
        Expr::UnaryOp { expr, .. } => scan_expr_params(expr, max),
        Expr::IsNull { expr, .. } | Expr::IsTrue { expr, .. } | Expr::IsFalse { expr, .. } => scan_expr_params(expr, max),
        Expr::Between { expr, low, high, .. } => { scan_expr_params(expr, max); scan_expr_params(low, max); scan_expr_params(high, max); }
        Expr::Like { expr, pattern, .. } => { scan_expr_params(expr, max); scan_expr_params(pattern, max); }
        Expr::In { expr, list, .. } => {
            scan_expr_params(expr, max);
            match list {
                InList::Exprs(v) => for e in v { scan_expr_params(e, max); },
                InList::Subquery(sq) => scan_select_params(sq, max),
            }
        }
        Expr::Exists { subquery, .. } => scan_select_params(subquery, max),
        Expr::Cast { expr, .. } => scan_expr_params(expr, max),
        Expr::Function { args, .. } => for a in args { scan_expr_params(a, max); },
        Expr::Case { operand, branches, else_ } => {
            if let Some(e) = operand { scan_expr_params(e, max); }
            for (c, r) in branches { scan_expr_params(c, max); scan_expr_params(r, max); }
            if let Some(e) = else_ { scan_expr_params(e, max); }
        }
        Expr::Subquery(sq) => scan_select_params(sq, max),
        Expr::Window { args, partition_by, order_by, .. } => {
            for a in args { scan_expr_params(a, max); }
            for p in partition_by { scan_expr_params(p, max); }
            for o in order_by { scan_expr_params(&o.expr, max); }
        }
        _ => {}
    }
}

/// Compute the RowDescription fields a SELECT would produce, without
/// evaluating WHERE/aggregation over any rows. Used to answer the extended
/// query protocol's `Describe` message ahead of `Execute`.
pub fn describe_select(select: &SelectStmt, db: Arc<Database>) -> anyhow::Result<Vec<FieldDescription>> {
    let mut cte_ctx = CteContext::new();
    for cte in &select.ctes {
        materialize_cte(cte, db.clone(), &mut cte_ctx, &[], &[])?;
    }
    let schema = if let Some(twj) = &select.from {
        let (primary_schema, _) = resolve_table_ref(&twj.primary, &db, &mut cte_ctx, &[], &[])?;
        let mut result_schema = primary_schema;
        for join in &twj.joins {
            let (join_schema, _) = resolve_table_ref(&join.table, &db, &mut cte_ctx, &[], &[])?;
            result_schema = merge_schema(&result_schema, &join_schema);
        }
        result_schema
    } else {
        None
    };
    build_fields(&select.projections, &schema)
}

/// A no-FROM SELECT: evaluate projections once against an empty row context.
fn execute_projection_only(select: &SelectStmt, params: &[Value]) -> anyhow::Result<QueryResult> {
    let ctx = RowContext::empty().with_params(params.to_vec());
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

    Ok(QueryResult { fields, rows: vec![row], tag: "SELECT 1".into() })
}

/// Resolve a FROM/JOIN table reference to its schema and rows: a derived
/// (subquery) table, a CTE reference, or a regular stored table.
fn resolve_table_ref(
    table: &TableRef,
    db: &Arc<Database>,
    cte_ctx: &mut CteContext,
    outer: &[OuterRow],
    params: &[Value],
) -> anyhow::Result<(Option<Arc<TableSchema>>, Vec<Vec<Option<Vec<u8>>>>)> {
    if let Some(subquery) = &table.subquery {
        let result = execute_select_with_cte((**subquery).clone(), db.clone(), cte_ctx, outer, params)?;
        let schema = schema_from_fields(&table.name, &result.fields);
        return Ok((Some(Arc::new(schema)), result.rows));
    }
    if let Some((rows, schema)) = cte_ctx.get(&table.name) {
        return Ok((Some(schema.clone()), rows.clone()));
    }
    if let Some(result) = catalog::resolve_virtual_table(&table.name, db) {
        let (schema, rows) = result?;
        return Ok((Some(schema), rows));
    }
    let schema = db.get_table(&table.name)?;
    let rows = db.scan(&table.name)?;
    let schema_arc = schema.clone().map(Arc::new);
    Ok((schema_arc, parse_rows(&rows, &schema)))
}

/// Build a schema for a derived table / CTE result from its RowDescription fields.
fn schema_from_fields(name: &str, fields: &[FieldDescription]) -> TableSchema {
    let columns: Vec<ColumnDef> = fields.iter().map(|f| {
        let col_type = match f.type_oid {
            oid::INT8 => ColumnType::Int8,
            oid::FLOAT8 => ColumnType::Float8,
            oid::BOOL => ColumnType::Bool,
            _ => ColumnType::Text,
        };
        ColumnDef { name: f.name.clone(), col_type, nullable: true, default: None, is_pk: false }
    }).collect();
    TableSchema { name: name.to_string(), columns, pk_columns: vec![] }
}

/// Combine two table schemas (in row-layout order) so joined columns are
/// resolvable by name in WHERE/ORDER BY/projections.
fn merge_schema(left: &Option<Arc<TableSchema>>, right: &Option<Arc<TableSchema>>) -> Option<Arc<TableSchema>> {
    match (left, right) {
        (Some(l), Some(r)) => {
            let mut columns = l.columns.clone();
            columns.extend(r.columns.iter().cloned());
            Some(Arc::new(TableSchema { name: format!("{}_{}", l.name, r.name), columns, pk_columns: vec![] }))
        }
        (Some(l), None) => Some(l.clone()),
        (None, Some(r)) => Some(r.clone()),
        (None, None) => None,
    }
}

fn build_fields(projections: &[Projection], schema: &Option<Arc<TableSchema>>) -> anyhow::Result<Vec<FieldDescription>> {
    let mut fields = Vec::new();
    for proj in projections {
        match proj {
            Projection::Wildcard => {
                let s = schema.as_ref().ok_or_else(|| anyhow::anyhow!("SELECT * requires a FROM clause"))?;
                for col in &s.columns {
                    fields.push(FieldDescription::simple(&col.name, col.col_type.oid()));
                }
            }
            Projection::WildcardTable(_) => {
                anyhow::bail!("table.* projections are not yet supported");
            }
            Projection::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| infer_column_name(expr));
                fields.push(FieldDescription::simple(name, oid::TEXT));
            }
        }
    }
    Ok(fields)
}

/// Materialize a CTE's result (iteratively, if `RECURSIVE`) into `cte_ctx`.
fn materialize_cte(
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
                cte_ctx.ctes.insert(cte.name.clone(), (working.clone(), schema.clone()));
                let step = execute_select_with_cte((**recursive_term).clone(), db.clone(), cte_ctx, outer, params)?;
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
        let columns: Vec<ColumnDef> = cte.columns.iter().map(|col_name| ColumnDef {
            name: col_name.clone(),
            col_type: ColumnType::Text,
            nullable: true,
            default: None,
            is_pk: false,
        }).collect();
        Arc::new(TableSchema { name: cte.name.clone(), columns, pk_columns: vec![] })
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
        Expr::Function { name, args, .. } => eval::is_aggregate_name(name) || args.iter().any(contains_aggregate),
        Expr::BinaryOp { lhs, rhs, .. } => contains_aggregate(lhs) || contains_aggregate(rhs),
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::IsNull { expr, .. } | Expr::IsTrue { expr, .. } | Expr::IsFalse { expr, .. } => contains_aggregate(expr),
        Expr::Between { expr, low, high, .. } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        Expr::Like { expr, pattern, .. } => contains_aggregate(expr) || contains_aggregate(pattern),
        Expr::In { expr, list, .. } => {
            contains_aggregate(expr) || matches!(list, InList::Exprs(v) if v.iter().any(contains_aggregate))
        }
        Expr::Cast { expr, .. } => contains_aggregate(expr),
        Expr::Case { operand, branches, else_ } => {
            operand.as_ref().is_some_and(|e| contains_aggregate(e))
                || branches.iter().any(|(c, r)| contains_aggregate(c) || contains_aggregate(r))
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
        Expr::Function { args, .. } => for a in args { collect_aggregate_exprs(a, out); },
        Expr::BinaryOp { lhs, rhs, .. } => { collect_aggregate_exprs(lhs, out); collect_aggregate_exprs(rhs, out); }
        Expr::UnaryOp { expr, .. } => collect_aggregate_exprs(expr, out),
        Expr::IsNull { expr, .. } | Expr::IsTrue { expr, .. } | Expr::IsFalse { expr, .. } => collect_aggregate_exprs(expr, out),
        Expr::Between { expr, low, high, .. } => { collect_aggregate_exprs(expr, out); collect_aggregate_exprs(low, out); collect_aggregate_exprs(high, out); }
        Expr::Like { expr, pattern, .. } => { collect_aggregate_exprs(expr, out); collect_aggregate_exprs(pattern, out); }
        Expr::In { expr, list, .. } => {
            collect_aggregate_exprs(expr, out);
            if let InList::Exprs(v) = list { for e in v { collect_aggregate_exprs(e, out); } }
        }
        Expr::Cast { expr, .. } => collect_aggregate_exprs(expr, out),
        Expr::Case { operand, branches, else_ } => {
            if let Some(e) = operand { collect_aggregate_exprs(e, out); }
            for (c, r) in branches { collect_aggregate_exprs(c, out); collect_aggregate_exprs(r, out); }
            if let Some(e) = else_ { collect_aggregate_exprs(e, out); }
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
            let key_vals: Vec<Value> = select.group_by.iter().map(|e| ctx.eval(e)).collect::<anyhow::Result<_>>()?;
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
        let member_rows: Vec<Vec<Option<Vec<u8>>>> = members.iter().map(|m| m.values.clone()).collect();

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
            if let Expr::Function { name, args, distinct } = agg_expr {
                let value = eval::eval_aggregate(name, args.first(), *distinct, &member_rows, schema, &Some(db.clone()))?;
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
        let Expr::Window { func, args, partition_by, order_by, frame } = win else { continue };
        let key = format!("{win:?}");

        let mut order: Vec<usize> = (0..rows.len()).collect();
        order.sort_by(|&a, &b| {
            for p in partition_by {
                let va = rows[a].eval(p).unwrap_or(Value::Null);
                let vb = rows[b].eval(p).unwrap_or(Value::Null);
                if let Ok(c) = value_compare(&va, &vb) {
                    if c != 0 { return if c < 0 { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater }; }
                }
            }
            for ob in order_by {
                let va = rows[a].eval(&ob.expr).unwrap_or(Value::Null);
                let vb = rows[b].eval(&ob.expr).unwrap_or(Value::Null);
                if let Ok(c) = value_compare(&va, &vb) {
                    let c = if ob.asc { c } else { -c };
                    if c != 0 { return if c < 0 { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater }; }
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
                let value = compute_window_value(func, args, frame, order_by, rows, &order, part_start, part_end, i, pos)?;
                extra[order[i]].insert(key.clone(), value);
            }
        }
    }

    for (ctx, new_vals) in rows.iter_mut().zip(extra.into_iter()) {
        if new_vals.is_empty() {
            continue;
        }
        let mut merged = ctx.computed.as_ref().map(|m| (**m).clone()).unwrap_or_default();
        merged.extend(new_vals);
        ctx.computed = Some(Arc::new(merged));
    }
    Ok(())
}

fn collect_window_exprs(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Window { .. } => out.push(expr.clone()),
        Expr::Function { args, .. } => for a in args { collect_window_exprs(a, out); },
        Expr::BinaryOp { lhs, rhs, .. } => { collect_window_exprs(lhs, out); collect_window_exprs(rhs, out); }
        Expr::UnaryOp { expr, .. } => collect_window_exprs(expr, out),
        Expr::IsNull { expr, .. } | Expr::IsTrue { expr, .. } | Expr::IsFalse { expr, .. } => collect_window_exprs(expr, out),
        Expr::Between { expr, low, high, .. } => { collect_window_exprs(expr, out); collect_window_exprs(low, out); collect_window_exprs(high, out); }
        Expr::Like { expr, pattern, .. } => { collect_window_exprs(expr, out); collect_window_exprs(pattern, out); }
        Expr::In { expr, list, .. } => {
            collect_window_exprs(expr, out);
            if let InList::Exprs(v) = list { for e in v { collect_window_exprs(e, out); } }
        }
        Expr::Cast { expr, .. } => collect_window_exprs(expr, out),
        Expr::Case { operand, branches, else_ } => {
            if let Some(e) = operand { collect_window_exprs(e, out); }
            for (c, r) in branches { collect_window_exprs(c, out); collect_window_exprs(r, out); }
            if let Some(e) = else_ { collect_window_exprs(e, out); }
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
            let arg = args.first().ok_or_else(|| anyhow::anyhow!("{fname}() requires an argument"))?;
            let offset = match args.get(1) {
                Some(e) => match rows[order[i]].eval(e)? {
                    Value::Int(n) => n,
                    Value::Float(f) => f as i64,
                    _ => 1,
                },
                None => 1,
            };
            let default = args.get(2);
            let target_pos = if fname == "lag" { pos as i64 - offset } else { pos as i64 + offset };
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
            let (fs, fe) = resolve_frame_bounds(frame, order_by, rows, order, part_start, part_end, pos)?;
            let member_rows: Vec<Vec<Option<Vec<u8>>>> = order[fs..fe].iter().map(|&idx| rows[idx].values.clone()).collect();
            let schema = rows[order[i]].schema.clone();
            let db = rows[order[i]].db.clone();
            let is_star = fname == "count" && (args.is_empty() || matches!(args.first(), Some(Expr::Ident(s)) if s == "*"));
            let arg = if is_star { None } else { args.first() };
            eval::eval_aggregate(&fname, arg, false, &member_rows, &schema, &db)
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
            let (lo, hi) = if start_pos <= end_pos { (start_pos, end_pos) } else { (end_pos, start_pos) };
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

fn execute_create_table(ct: crate::sql::ast::CreateTableStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    let columns: Vec<ColumnDef> = ct.columns.iter().map(|c| {
        let col_type = ColumnType::from_name(&c.col_type).unwrap_or(ColumnType::Text);
        ColumnDef {
            name: c.name.clone(),
            col_type,
            nullable: c.nullable,
            default: None,
            is_pk: c.is_pk,
        }
    }).collect();

    db.create_table(&ct.table, &columns)?;
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE TABLE".into() })
}

fn execute_drop_table(_dt: crate::sql::ast::DropTableStmt, _db: Arc<Database>) -> anyhow::Result<QueryResult> {
    // For now, just return success
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "DROP TABLE".into() })
}

fn execute_create_index(ci: crate::sql::ast::CreateIndexStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    db.create_index(&ci.table, &ci.column)?;
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE INDEX".into() })
}

fn execute_insert(insert: crate::sql::ast::InsertStmt, db: Arc<Database>, params: &[Value]) -> anyhow::Result<QueryResult> {
    let schema = db.get_table(&insert.table)?
        .ok_or_else(|| anyhow::anyhow!("table \"{}\" does not exist", insert.table))?;

    let mut row_count = 0usize;
    for row_values in &insert.values {
        let pk_value = if !schema.pk_columns.is_empty() {
            let pk_idx = schema.pk_columns[0];
            let expr = &row_values[pk_idx.min(row_values.len() - 1)];
            let val = eval_expr(expr, params)?;
            val.to_wire_bytes().unwrap_or_default()
        } else {
            let val = eval_expr(&row_values[0], params)?;
            val.to_wire_bytes().unwrap_or_default()
        };

        let mut value_buf = Vec::new();
        for expr in row_values {
            let val = eval_expr(expr, params)?;
            let bytes = val.to_wire_bytes();
            match bytes {
                Some(data) => {
                    value_buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    value_buf.extend_from_slice(&data);
                }
                None => {
                    value_buf.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }

        db.write(&insert.table, &pk_value, &value_buf)?;
        row_count += 1;
    }

    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: format!("INSERT 0 {row_count}"),
    })
}

fn execute_delete(delete: crate::sql::ast::DeleteStmt, db: Arc<Database>, params: &[Value]) -> anyhow::Result<QueryResult> {
    let schema = db.get_table(&delete.table)?;
    let schema_arc = schema.clone().map(Arc::new);
    let raw_rows = db.scan(&delete.table)?;
    let parsed = parse_rows(&raw_rows, &schema);

    let mut deleted = 0usize;
    for (kv, row) in raw_rows.iter().zip(parsed.iter()) {
        let matches = match &delete.where_ {
            Some(expr) => {
                let ctx = RowContext::with_db(row.clone(), schema_arc.clone(), db.clone()).with_params(params.to_vec());
                ctx.eval(expr).map(|v| v.is_truthy()).unwrap_or(false)
            }
            None => true,
        };
        if matches {
            db.delete(&delete.table, &kv.key)?;
            deleted += 1;
        }
    }

    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: format!("DELETE {deleted}"),
    })
}

fn execute_update(update: crate::sql::ast::UpdateStmt, db: Arc<Database>, params: &[Value]) -> anyhow::Result<QueryResult> {
    let schema = db.get_table(&update.table)?
        .ok_or_else(|| anyhow::anyhow!("table \"{}\" does not exist", update.table))?;
    let schema_arc = Arc::new(schema.clone());
    let raw_rows = db.scan(&update.table)?;
    let parsed = parse_rows(&raw_rows, &Some(schema.clone()));

    let mut updated = 0usize;
    for (kv, row) in raw_rows.iter().zip(parsed.iter()) {
        let ctx = RowContext::with_db(row.clone(), Some(schema_arc.clone()), db.clone()).with_params(params.to_vec());
        let matches = match &update.where_ {
            Some(expr) => ctx.eval(expr).map(|v| v.is_truthy()).unwrap_or(false),
            None => true,
        };
        if !matches {
            continue;
        }

        let mut new_row = row.clone();
        for (col_name, expr) in &update.assignments {
            let idx = schema.columns.iter().position(|c| c.name == *col_name)
                .ok_or_else(|| anyhow::anyhow!("column \"{col_name}\" does not exist"))?;
            let val = ctx.eval(expr)?;
            new_row[idx] = val.to_wire_bytes();
        }

        let mut value_buf = Vec::new();
        for cell in &new_row {
            match cell {
                Some(data) => {
                    value_buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    value_buf.extend_from_slice(data);
                }
                None => value_buf.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        db.write(&update.table, &kv.key, &value_buf)?;
        updated += 1;
    }

    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: format!("UPDATE {updated}"),
    })
}

/// Parse rows from storage into Vec<Vec<Option<Vec<u8>>>>
fn parse_rows(rows: &[crate::storage::KeyValue], _schema: &Option<TableSchema>) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut result = Vec::new();
    for kv in rows {
        let mut row = Vec::new();
        let mut pos = 0;
        let data = &kv.value;
        while pos + 4 <= data.len() {
            let len = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            if len < 0 {
                row.push(None);
            } else {
                let end = pos + len as usize;
                if end <= data.len() {
                    row.push(Some(data[pos..end].to_vec()));
                    pos = end;
                } else {
                    row.push(None);
                    break;
                }
            }
        }
        result.push(row);
    }
    result
}

/// Whether `expr` only references columns present in `schema` (plus
/// literals/params) — used to test whether one side of an `=` predicate is
/// entirely evaluable against a single joined table, so it can drive a hash
/// join key instead of a full nested-loop scan.
fn expr_only_refs(expr: &Expr, schema: &Option<Arc<TableSchema>>) -> bool {
    let has_col = |name: &str| schema.as_ref().is_some_and(|s| s.columns.iter().any(|c| c.name == name));
    match expr {
        Expr::Ident(name) => has_col(name),
        Expr::QualifiedIdent(_, name) => has_col(name),
        Expr::Literal(_) | Expr::Param(_) => true,
        Expr::BinaryOp { lhs, rhs, .. } => expr_only_refs(lhs, schema) && expr_only_refs(rhs, schema),
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
fn split_join_keys(on_expr: &Expr, left_schema: &Option<Arc<TableSchema>>, right_schema: &Option<Arc<TableSchema>>) -> Option<(Expr, Expr)> {
    let Expr::BinaryOp { op: BinOp::Eq, lhs, rhs } = on_expr else { return None };
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
    scopes.push((right_alias.to_string(), left_row.len()..left_row.len() + right_len));
    RowContext::new(combined, merged)
        .with_table_scopes(scopes)
        .eval(on_expr)
        .map(|v| v.is_truthy())
        .unwrap_or(false)
}

/// Apply a JOIN to the result rows. Equi-join conditions (`a.x = b.y`) use a
/// hash join, building from whichever side is smaller; anything else falls
/// back to a nested-loop scan evaluating the full predicate per pair.
fn apply_join(
    left_rows: Vec<Vec<Option<Vec<u8>>>>,
    right_rows: Vec<Vec<Option<Vec<u8>>>>,
    join: &Join,
    left_schema: &Option<Arc<TableSchema>>,
    right_schema: &Option<Arc<TableSchema>>,
    left_scopes: &[(String, std::ops::Range<usize>)],
    right_alias: &str,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let right_len = right_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0);
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
                return apply_join(left_rows, right_rows, &Join { join_type: JoinType::Cross, table: join.table.clone(), on: None }, left_schema, right_schema, left_scopes, right_alias);
            };
            if let Some((left_key, right_key)) = split_join_keys(on_expr, left_schema, right_schema) {
                hash_equi_join(left_rows, right_rows, &left_key, &right_key, left_schema, right_schema)
            } else {
                let mut result = Vec::new();
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        if eval_join_predicate(on_expr, left_row, right_row, left_schema, right_schema, left_scopes, right_alias) {
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
            if let Some((left_key, right_key)) = split_join_keys(on_expr, left_schema, right_schema) {
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
                    let matches = ctx.eval(&left_key).ok().and_then(|k| k.to_wire_bytes()).and_then(|b| hash_table.get(&b));
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
            } else {
                let mut result = Vec::new();
                for left_row in &left_rows {
                    let mut matched = false;
                    for right_row in &right_rows {
                        if eval_join_predicate(on_expr, left_row, right_row, left_schema, right_schema, left_scopes, right_alias) {
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
                        let mut combined = nulls(left_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0));
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
            if let Some((left_key, right_key)) = split_join_keys(on_expr, left_schema, right_schema) {
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
                    let matches = ctx.eval(&right_key).ok().and_then(|k| k.to_wire_bytes()).and_then(|b| hash_table.get(&b));
                    match matches {
                        Some(rows) if !rows.is_empty() => {
                            for left_row in rows {
                                let mut combined = left_row.clone();
                                combined.extend(right_row.iter().cloned());
                                result.push(combined);
                            }
                        }
                        _ => {
                            let mut combined = nulls(left_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0));
                            combined.extend(right_row.iter().cloned());
                            result.push(combined);
                        }
                    }
                }
                result
            } else {
                let mut result = Vec::new();
                for right_row in &right_rows {
                    let mut matched = false;
                    for left_row in &left_rows {
                        if eval_join_predicate(on_expr, left_row, right_row, left_schema, right_schema, left_scopes, right_alias) {
                            matched = true;
                            let mut combined = left_row.clone();
                            combined.extend(right_row.iter().cloned());
                            result.push(combined);
                        }
                    }
                    if !matched {
                        let mut combined = nulls(left_schema.as_ref().map(|s| s.columns.len()).unwrap_or(0));
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
fn hash_equi_join(
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

/// Derive a display name for an expression when no alias is given.
fn infer_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Ident(name) => name.clone(),
        Expr::QualifiedIdent(_, col) => col.clone(),
        Expr::Function { name, .. } => name.to_lowercase(),
        Expr::Literal(_) => "?column?".into(),
        _ => "?column?".into(),
    }
}
