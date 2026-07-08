mod catalog;
pub mod copy;
pub mod eval;
mod planner;
mod udf;
#[cfg(test)]
mod canopy_tests;
#[cfg(test)]
mod chronos_tests;
#[cfg(test)]
mod flux_tests;
#[cfg(test)]
mod geo_tests;
#[cfg(test)]
mod phase4_tests;
#[cfg(test)]
mod pg_dump_tests;
#[cfg(test)]
mod plexus_tests;

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::ast::{BinOp, Expr, FrameBound, FrameBoundType, InList, Join, JoinType, Literal, Projection, SelectStmt, Stmt, TableRef, UnOp, UnionOp};
use crate::storage::database::Database;
use crate::storage::{ColumnDef, ColumnType, ForeignKey, StorageEngine, TableSchema};
use crate::wire::messages::{FieldDescription, oid};
use eval::{eval_expr, eval_expr_with_db, value_compare, OuterRow, RowContext, Value};

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

/// Parse and execute a SQL statement, returning a QueryResult. Parsing goes
/// through `Database`'s shared statement cache (`db.parse_cached`) so a
/// repeated query text — even from a different connection — skips
/// re-lexing/parsing.
pub fn execute_query(sql_text: &str, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    let stmt = db.parse_cached(sql_text)?;
    execute_parsed(stmt, db, &[])
}

/// Execute an already-parsed statement, with bound `$n` parameter values
/// (used by the extended query protocol's Bind/Execute).
#[tracing::instrument(skip_all)]
pub fn execute_parsed(stmt: Stmt, db: Arc<Database>, params: &[Value]) -> anyhow::Result<QueryResult> {
    let start = std::time::Instant::now();
    let result = execute_parsed_inner(stmt, db, params);
    crate::metrics::Metrics::global().record_query(start.elapsed(), result.is_err());
    result
}

fn execute_parsed_inner(stmt: Stmt, db: Arc<Database>, params: &[Value]) -> anyhow::Result<QueryResult> {
    match stmt {
        Stmt::Select(select) => execute_select_with_cte(select, db, &mut CteContext::new(), &[], params),
        Stmt::Insert(insert) => execute_insert(insert, db, params),
        Stmt::Delete(delete) => execute_delete(delete, db, params),
        Stmt::Update(update) => execute_update(update, db, params),
        Stmt::CreateTable(ct) => execute_create_table(ct, db),
        Stmt::DropTable(dt) => execute_drop_table(dt, db),
        Stmt::CreateIndex(ci) => execute_create_index(ci, db),
        Stmt::CreateFunction(cf) => execute_create_function(cf, db),
        Stmt::CreateSequence(cs) => execute_create_sequence(cs, db),
        Stmt::CreateTopic(ct) => execute_create_topic(ct, db),
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
        Stmt::AlterTable(at) => execute_alter_table(at, db),
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
        Stmt::CopyIn(_) | Stmt::CopyOut(_) => {
            anyhow::bail!("COPY is only supported over the simple query protocol")
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
        return execute_projection_only(&select, db, params);
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
fn execute_projection_only(select: &SelectStmt, db: Arc<Database>, params: &[Value]) -> anyhow::Result<QueryResult> {
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
    if let Some(args) = &table.func_args {
        return resolve_graph_function(&table.name, args, db, params);
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

/// Dispatch a table-valued function call in the FROM clause (`table.name`
/// with `table.func_args` set — see `TableRef` docs) to one of the Plexus
/// graph-traversal/algorithm entry points. Arguments are evaluated as
/// constants (`eval_expr`, no row context), consistent with how `CREATE
/// INDEX ... WITH (...)` options are constant-only elsewhere in this engine.
fn resolve_graph_function(
    name: &str,
    args: &[Expr],
    db: &Arc<Database>,
    params: &[Value],
) -> anyhow::Result<(Option<Arc<TableSchema>>, Vec<Vec<Option<Vec<u8>>>>)> {
    use crate::graph::Direction;

    let arg_val = |i: usize| -> anyhow::Result<Value> {
        args.get(i).map(|e| eval_expr(e, params)).transpose()?
            .ok_or_else(|| anyhow::anyhow!("{name}: missing argument {}", i + 1))
    };
    let arg_text = |i: usize| -> anyhow::Result<String> {
        match arg_val(i)? {
            Value::Text(s) => Ok(s),
            Value::Int(n) => Ok(n.to_string()),
            other => anyhow::bail!("{name}: expected text argument {}, got {}", i + 1, other.type_name()),
        }
    };
    let arg_bytes = |i: usize| -> anyhow::Result<Vec<u8>> {
        arg_val(i)?.to_wire_bytes().ok_or_else(|| anyhow::anyhow!("{name}: argument {} must not be NULL", i + 1))
    };
    let arg_usize_or = |i: usize, default: usize| -> anyhow::Result<usize> {
        match args.get(i) { Some(e) => Ok(eval_expr(e, params)?.as_f64()? as usize), None => Ok(default) }
    };
    let arg_f64_or = |i: usize, default: f64| -> anyhow::Result<f64> {
        match args.get(i) { Some(e) => eval_expr(e, params)?.as_f64(), None => Ok(default) }
    };
    let arg_direction_or = |i: usize, default: Direction| -> anyhow::Result<Direction> {
        match args.get(i) {
            Some(_) => {
                let s = arg_text(i)?;
                Direction::parse(&s).ok_or_else(|| anyhow::anyhow!("{name}: invalid direction \"{s}\" (expected out/in/both)"))
            }
            None => Ok(default),
        }
    };

    fn func_schema(name: &str, cols: &[&str]) -> Arc<TableSchema> {
        Arc::new(TableSchema {
            name: name.to_string(),
            columns: cols.iter().map(|c| ColumnDef { name: c.to_string(), col_type: ColumnType::Text, nullable: true, default: None, is_pk: false }).collect(),
            pk_columns: vec![],
            unique_groups: vec![],
            foreign_keys: vec![],
            json_schemas: vec![],
        })
    }
    fn cell(b: Vec<u8>) -> Option<Vec<u8>> { Some(b) }
    fn opt_cell(s: Option<String>) -> Option<Vec<u8>> { s.map(|s| s.into_bytes()) }
    fn num_cell(n: impl ToString) -> Option<Vec<u8>> { Some(n.to_string().into_bytes()) }

    match name.to_ascii_lowercase().as_str() {
        "graph_neighbors" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let key = arg_bytes(2)?;
            let dir = arg_direction_or(3, Direction::Out)?;
            let neighbors = db.graph_neighbors(&table, &from_col, &key, dir)
                .ok_or_else(|| anyhow::anyhow!("no graph index on {table}.{from_col} (or unknown vertex)"))?;
            let rows = neighbors.into_iter().map(|(k, rel)| vec![cell(k), opt_cell(rel)]).collect();
            Ok((Some(func_schema("graph_neighbors", &["neighbor", "rel_type"])), rows))
        }
        "graph_bfs" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let key = arg_bytes(2)?;
            let max_depth = arg_usize_or(3, 10)?;
            let dir = arg_direction_or(4, Direction::Out)?;
            let visited = db.graph_bfs(&table, &from_col, &key, max_depth, dir)
                .ok_or_else(|| anyhow::anyhow!("no graph index on {table}.{from_col} (or unknown vertex)"))?;
            let rows = visited.into_iter().map(|(k, depth)| vec![cell(k), num_cell(depth)]).collect();
            Ok((Some(func_schema("graph_bfs", &["vertex", "depth"])), rows))
        }
        "graph_shortest_path" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let start = arg_bytes(2)?;
            let end = arg_bytes(3)?;
            let dir = arg_direction_or(4, Direction::Both)?;
            let path = db.graph_shortest_path(&table, &from_col, &start, &end, dir)
                .ok_or_else(|| anyhow::anyhow!("no graph index on {table}.{from_col} (or unknown vertex)"))?;
            let rows = match path {
                Some(vertices) => vertices.into_iter().enumerate().map(|(i, k)| vec![num_cell(i), cell(k)]).collect(),
                None => Vec::new(),
            };
            Ok((Some(func_schema("graph_shortest_path", &["step", "vertex"])), rows))
        }
        "graph_connected_components" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let comps = db.graph_connected_components(&table, &from_col)
                .ok_or_else(|| anyhow::anyhow!("no graph index on {table}.{from_col}"))?;
            let rows = comps.into_iter().map(|(k, c)| vec![cell(k), num_cell(c)]).collect();
            Ok((Some(func_schema("graph_connected_components", &["vertex", "component"])), rows))
        }
        "graph_pagerank" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let iterations = arg_usize_or(2, 20)?;
            let damping = arg_f64_or(3, 0.85)?;
            let ranks = db.graph_pagerank(&table, &from_col, damping, iterations)
                .ok_or_else(|| anyhow::anyhow!("no graph index on {table}.{from_col}"))?;
            let rows = ranks.into_iter().map(|(k, r)| vec![cell(k), num_cell(r)]).collect();
            Ok((Some(func_schema("graph_pagerank", &["vertex", "score"])), rows))
        }
        "graph_triangle_count" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let counts = db.graph_triangle_count(&table, &from_col)
                .ok_or_else(|| anyhow::anyhow!("no graph index on {table}.{from_col}"))?;
            let rows = counts.into_iter().map(|(k, c)| vec![cell(k), num_cell(c)]).collect();
            Ok((Some(func_schema("graph_triangle_count", &["vertex", "triangles"])), rows))
        }
        "json_path_lookup" => {
            let table = arg_text(0)?;
            let column = arg_text(1)?;
            let value_text = arg_text(2)?;
            let keys = db.json_path_lookup(&table, &column, &value_text)
                .ok_or_else(|| anyhow::anyhow!("no JSONPATH index on {table}.{column}"))?;
            let rows = keys.into_iter().map(|k| vec![cell(k)]).collect();
            Ok((Some(func_schema("json_path_lookup", &["row_key"])), rows))
        }
        "json_text_search" => {
            let table = arg_text(0)?;
            let column = arg_text(1)?;
            let query = arg_text(2)?;
            let keys = db.fts_search(&table, &column, &query)
                .ok_or_else(|| anyhow::anyhow!("no full-text index on {table}.{column}"))?;
            let rows = keys.into_iter().map(|k| vec![cell(k)]).collect();
            Ok((Some(func_schema("json_text_search", &["row_key"])), rows))
        }
        // --- Flux (Phase 11) ---------------------------------------------
        "flux_time_travel" => {
            let table = arg_text(0)?;
            let cutoff_ms = arg_val(1)?.as_f64()? as i64;
            let topic = format!("__cdc_{table}");
            let records = db.flux_all(&topic, 0)
                .ok_or_else(|| anyhow::anyhow!("no CDC events for table \"{table}\" (topic \"{topic}\" doesn't exist yet — insert/update/delete something first)"))?;
            // Reconstructed state, `row_key` (hex-encoded, matching
            // `publish_cdc_event`) -> the row's `after` JSON as of the last
            // applicable event. A `BTreeMap` just for deterministic output
            // ordering, not because row keys are ordered data.
            let mut state: std::collections::BTreeMap<String, serde_json::Value> = std::collections::BTreeMap::new();
            for rec in &records {
                let event: serde_json::Value = match serde_json::from_slice(&rec.value) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let ts = event.get("ts").and_then(|v| v.as_i64()).unwrap_or(i64::MAX);
                if ts > cutoff_ms {
                    continue;
                }
                let row_key = event.get("row_key").and_then(|v| v.as_str()).unwrap_or("").to_string();
                if event.get("op").and_then(|v| v.as_str()) == Some("delete") {
                    state.remove(&row_key);
                } else if let Some(after) = event.get("after") {
                    state.insert(row_key, after.clone());
                }
            }
            // Generic `(row_key, data)` shape rather than the live table
            // schema: `after` is already a JSON object of column name ->
            // text value (see `publish_cdc_event`), and the table's schema
            // may itself have changed since the events being replayed were
            // recorded, so re-serializing to JSON is the only shape that's
            // always honest about what was actually captured.
            let rows = state.into_iter()
                .map(|(k, v)| vec![cell(k.into_bytes()), cell(v.to_string().into_bytes())])
                .collect();
            Ok((Some(func_schema("flux_time_travel", &["row_key", "data"])), rows))
        }
        "flux_window_tumbling" => {
            let topic = arg_text(0)?;
            let window_ms = arg_val(1)?.as_f64()? as i64;
            anyhow::ensure!(window_ms > 0, "flux_window_tumbling: window_ms must be positive");
            let n = db.flux_num_partitions(&topic).ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
            anyhow::ensure!(n == 1, "flux_window_tumbling only supports single-partition topics (multi-partition merge not implemented — see storage::flux module docs)");
            let records = db.flux_all(&topic, 0).unwrap_or_default();
            let mut buckets: std::collections::BTreeMap<i64, u64> = std::collections::BTreeMap::new();
            for rec in &records {
                let start = rec.timestamp_ms.div_euclid(window_ms) * window_ms;
                *buckets.entry(start).or_insert(0) += 1;
            }
            let rows = buckets.into_iter()
                .map(|(start, count)| vec![num_cell(start), num_cell(start + window_ms), num_cell(count)])
                .collect();
            Ok((Some(func_schema("flux_window_tumbling", &["window_start", "window_end", "count"])), rows))
        }
        "flux_window_session" => {
            let topic = arg_text(0)?;
            let gap_ms = arg_val(1)?.as_f64()? as i64;
            anyhow::ensure!(gap_ms > 0, "flux_window_session: gap_ms must be positive");
            let n = db.flux_num_partitions(&topic).ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
            anyhow::ensure!(n == 1, "flux_window_session only supports single-partition topics (multi-partition merge not implemented — see storage::flux module docs)");
            let mut records = db.flux_all(&topic, 0).unwrap_or_default();
            records.sort_by_key(|r| r.timestamp_ms);
            let mut rows = Vec::new();
            let mut current: Option<(i64, i64, u64)> = None; // (start, end, count)
            for rec in &records {
                current = Some(match current {
                    Some((start, end, count)) if rec.timestamp_ms - end <= gap_ms => (start, rec.timestamp_ms, count + 1),
                    Some((start, end, count)) => {
                        rows.push(vec![num_cell(start), num_cell(end), num_cell(count)]);
                        (rec.timestamp_ms, rec.timestamp_ms, 1)
                    }
                    None => (rec.timestamp_ms, rec.timestamp_ms, 1),
                });
            }
            if let Some((start, end, count)) = current {
                rows.push(vec![num_cell(start), num_cell(end), num_cell(count)]);
            }
            Ok((Some(func_schema("flux_window_session", &["window_start", "window_end", "count"])), rows))
        }
        "flux_window_sliding" => {
            let topic = arg_text(0)?;
            let window_size_ms = arg_val(1)?.as_f64()? as i64;
            let slide_ms = arg_val(2)?.as_f64()? as i64;
            anyhow::ensure!(window_size_ms > 0 && slide_ms > 0, "flux_window_sliding: window_size_ms and slide_ms must be positive");
            let n = db.flux_num_partitions(&topic).ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
            anyhow::ensure!(n == 1, "flux_window_sliding only supports single-partition topics (multi-partition merge not implemented — see storage::flux module docs)");
            let records = db.flux_all(&topic, 0).unwrap_or_default();
            let mut rows = Vec::new();
            if let (Some(min_ts), Some(max_ts)) = (records.iter().map(|r| r.timestamp_ms).min(), records.iter().map(|r| r.timestamp_ms).max()) {
                // One row per slide boundary >= the first record, each
                // covering the trailing `[boundary - window_size_ms,
                // boundary)` window — boundaries with zero records in range
                // are skipped rather than emitted as empty rows.
                let mut boundary = min_ts.div_euclid(slide_ms) * slide_ms + slide_ms;
                while boundary - window_size_ms <= max_ts {
                    let window_start = boundary - window_size_ms;
                    let count = records.iter().filter(|r| r.timestamp_ms >= window_start && r.timestamp_ms < boundary).count() as u64;
                    if count > 0 {
                        rows.push(vec![num_cell(window_start), num_cell(boundary), num_cell(count)]);
                    }
                    boundary += slide_ms;
                }
            }
            Ok((Some(func_schema("flux_window_sliding", &["window_start", "window_end", "count"])), rows))
        }
        other => anyhow::bail!("unknown table function \"{other}\""),
    }
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
    TableSchema { name: name.to_string(), columns, pk_columns: vec![], unique_groups: vec![], foreign_keys: vec![], json_schemas: vec![] }
}

/// Combine two table schemas (in row-layout order) so joined columns are
/// resolvable by name in WHERE/ORDER BY/projections.
fn merge_schema(left: &Option<Arc<TableSchema>>, right: &Option<Arc<TableSchema>>) -> Option<Arc<TableSchema>> {
    match (left, right) {
        (Some(l), Some(r)) => {
            let mut columns = l.columns.clone();
            columns.extend(r.columns.iter().cloned());
            Some(Arc::new(TableSchema { name: format!("{}_{}", l.name, r.name), columns, pk_columns: vec![], unique_groups: vec![], foreign_keys: vec![], json_schemas: vec![] }))
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
        Arc::new(TableSchema { name: cte.name.clone(), columns, pk_columns: vec![], unique_groups: vec![], foreign_keys: vec![], json_schemas: vec![] })
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
        // Chronos: `interpolate(value)` — for a NULL `value` at this row,
        // linearly interpolates between the nearest non-null values before
        // and after it in partition/ORDER BY order (falls back to whichever
        // single neighbor exists at a partition edge). Non-null values pass
        // through unchanged. `gap_fill()` (materializing missing timestamp
        // rows entirely) is a documented scope cut — see TODO.md — since it
        // would need to insert rows into the result set, not just compute a
        // per-row value like every other window function here.
        "interpolate" => {
            let arg = args.first().ok_or_else(|| anyhow::anyhow!("interpolate() requires 1 argument"))?;
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
            let value_arg = args.first().ok_or_else(|| anyhow::anyhow!("moving_average() requires 2 arguments (value, window_size)"))?;
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
            let member_rows: Vec<Vec<Option<Vec<u8>>>> = order[fs..fe].iter().map(|&idx| rows[idx].values.clone()).collect();
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

/// Serialize the small subset of expressions a column DEFAULT realistically
/// needs (literals, `nextval('seq')`-style function calls, and `::type`
/// casts like `pg_dump`'s `nextval(...)::regclass`) back to SQL text, so it
/// can be persisted in `storage::ColumnDef.default: Option<String>` (which
/// can't hold a parsed `Expr` directly — `Expr::Subquery` pulls in the
/// entire `SelectStmt` type graph, which isn't `Serialize`) and re-parsed
/// via `sql::parse_expr_text` at INSERT time. Anything more complex is a
/// `CREATE TABLE`/`ALTER TABLE` error rather than a silently dropped default.
fn default_expr_to_text(e: &Expr) -> anyhow::Result<String> {
    match e {
        Expr::Literal(Literal::Int(n)) => Ok(n.to_string()),
        Expr::Literal(Literal::Float(f)) => Ok(f.to_string()),
        Expr::Literal(Literal::Text(s)) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Expr::Literal(Literal::Bool(b)) => Ok(b.to_string()),
        Expr::Literal(Literal::Null) => Ok("NULL".to_string()),
        Expr::UnaryOp { op: UnOp::Neg, expr } => Ok(format!("-{}", default_expr_to_text(expr)?)),
        Expr::Function { name, args, .. } => {
            let arg_texts: Vec<String> = args.iter().map(default_expr_to_text).collect::<anyhow::Result<_>>()?;
            Ok(format!("{name}({})", arg_texts.join(", ")))
        }
        Expr::Cast { expr, ty } => Ok(format!("{}::{}", default_expr_to_text(expr)?, ty)),
        other => anyhow::bail!(
            "DEFAULT expression too complex to persist in this version (supported: literals, nextval()-style function calls): {other:?}"
        ),
    }
}

/// Resolve a table-level constraint's column-name list to indices into
/// `columns` (in declared position order), for `TableSchema.unique_groups`.
fn resolve_column_indices(columns: &[ColumnDef], names: &[String]) -> anyhow::Result<Vec<usize>> {
    names.iter().map(|n| {
        columns.iter().position(|c| &c.name == n).ok_or_else(|| anyhow::anyhow!("column \"{n}\" does not exist"))
    }).collect()
}

fn execute_create_table(ct: crate::sql::ast::CreateTableStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    if ct.if_not_exists && db.list_tables()?.iter().any(|name| name == &ct.table) {
        return Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE TABLE".into() });
    }

    let mut columns: Vec<ColumnDef> = Vec::with_capacity(ct.columns.len());
    let mut unique_groups: Vec<Vec<usize>> = Vec::new();
    let mut foreign_keys: Vec<ForeignKey> = Vec::new();
    // (column index, sequence name) for SERIAL columns — the sequence is
    // created after the table so its implicit default can name it.
    let mut serial_columns: Vec<(usize, String)> = Vec::new();

    for (i, c) in ct.columns.iter().enumerate() {
        let col_type = ColumnType::from_name(&c.col_type).unwrap_or(ColumnType::Text);
        let default = c.default.as_ref().map(default_expr_to_text).transpose()?;

        if c.is_unique {
            unique_groups.push(vec![i]);
        }
        if let Some((ref_table, ref_column)) = &c.references {
            foreign_keys.push(ForeignKey { column: i, ref_table: ref_table.clone(), ref_column: ref_column.clone() });
        }
        if c.is_serial {
            serial_columns.push((i, format!("{}_{}_seq", ct.table, c.name)));
        }

        columns.push(ColumnDef {
            name: c.name.clone(),
            col_type,
            nullable: c.nullable,
            default,
            is_pk: c.is_pk,
        });
    }

    for constraint in &ct.table_constraints {
        match constraint {
            crate::sql::ast::TableConstraint::Unique(names) => {
                unique_groups.push(resolve_column_indices(&columns, names)?);
            }
            crate::sql::ast::TableConstraint::ForeignKey { column, ref_table, ref_column } => {
                let idx = resolve_column_indices(&columns, std::slice::from_ref(column))?[0];
                foreign_keys.push(ForeignKey { column: idx, ref_table: ref_table.clone(), ref_column: ref_column.clone() });
            }
        }
    }

    db.create_table_with_constraints(&ct.table, &columns, unique_groups, foreign_keys)?;

    // Canopy (Phase 10): `WITH (json_schema_col = ..., json_schema = '...',
    // json_schema_mode = 'strict' | 'relaxed' | 'off')` attaches a JSON
    // Schema validation rule, enforced on INSERT/UPDATE by `validate_json_schemas`.
    let opt = |key: &str| ct.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
    if let Some(col) = opt("json_schema_col") {
        let schema_text = opt("json_schema")
            .ok_or_else(|| anyhow::anyhow!("json_schema_col requires WITH (json_schema = '<json schema text>')"))?;
        serde_json::from_str::<serde_json::Value>(schema_text)
            .map_err(|e| anyhow::anyhow!("invalid json_schema: {e}"))?;
        let mode = opt("json_schema_mode").unwrap_or("strict");
        crate::storage::json_schema::Mode::parse(mode)
            .ok_or_else(|| anyhow::anyhow!("invalid json_schema_mode \"{mode}\" (expected strict, relaxed, or off)"))?;
        db.set_json_schema(&ct.table, crate::storage::JsonSchemaRule {
            column: col.to_string(),
            mode: mode.to_string(),
            schema: schema_text.to_string(),
        })?;
    }

    if !serial_columns.is_empty() {
        for (idx, seq_name) in serial_columns {
            db.create_sequence(&seq_name, 1, 1)?;
            columns[idx].default = Some(format!("nextval('{seq_name}')"));
        }
        // Re-persist with the implicit SERIAL defaults now that the backing
        // sequences exist (create_table_with_constraints already wrote the
        // pre-SERIAL-default version).
        let schema = db.get_table(&ct.table)?.expect("just created");
        db.update_table_schema(TableSchema { columns, ..schema })?;
    }

    Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE TABLE".into() })
}

fn execute_drop_table(_dt: crate::sql::ast::DropTableStmt, _db: Arc<Database>) -> anyhow::Result<QueryResult> {
    // For now, just return success
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "DROP TABLE".into() })
}

fn execute_create_index(ci: crate::sql::ast::CreateIndexStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    match ci.using.as_deref() {
        None => db.create_index(&ci.table, &ci.column)?,
        Some(m) if m.eq_ignore_ascii_case("spatial") || m.eq_ignore_ascii_case("gist") => {
            // 1km sizing hint: no SQL syntax yet to tune the underlying grid
            // level per index, so pick a level that comfortably serves
            // typical "within N meters" queries without either bucketing
            // everything into one giant cell or fragmenting into millions.
            const DEFAULT_SPATIAL_RADIUS_HINT_M: f64 = 1000.0;
            db.create_spatial_index(&ci.table, &ci.column, DEFAULT_SPATIAL_RADIUS_HINT_M)?
        }
        Some(m) if m.eq_ignore_ascii_case("time") || m.eq_ignore_ascii_case("chronos") => {
            let opt = |key: &str| ci.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
            // Default 1-hour buckets — a reasonable middle ground between
            // hourly/daily/monthly partitioning without requiring `interval`
            // on every `CREATE INDEX ... USING TIME`.
            let granularity_ms = match opt("interval") {
                Some(s) => eval::parse_interval(s).ok_or_else(|| anyhow::anyhow!("invalid interval {s:?}"))?,
                None => 3_600_000,
            };
            let retention_ms = match opt("retention") {
                Some(s) => Some(eval::parse_interval(s).ok_or_else(|| anyhow::anyhow!("invalid retention {s:?}"))?),
                None => None,
            };
            let schema = db.get_table(&ci.table)?.ok_or_else(|| anyhow::anyhow!("table \"{}\" does not exist", ci.table))?;
            let value_column = match opt("value") {
                Some(v) => v.to_string(),
                // No explicit `value` option: use the first numeric column
                // that isn't the indexed timestamp column itself.
                None => schema.columns.iter()
                    .find(|c| c.name != ci.column && matches!(c.col_type, crate::storage::ColumnType::Int8 | crate::storage::ColumnType::Int4 | crate::storage::ColumnType::Int2 | crate::storage::ColumnType::Float8 | crate::storage::ColumnType::Float4))
                    .map(|c| c.name.clone())
                    .ok_or_else(|| anyhow::anyhow!("USING TIME requires a numeric value column; specify WITH (value = '<column>')"))?,
            };
            let policy = crate::storage::ts_index::TimeBucketPolicy { granularity_ms, retention_ms };
            db.create_time_index(&ci.table, &ci.column, &value_column, policy)?
        }
        Some(m) if m.eq_ignore_ascii_case("graph") || m.eq_ignore_ascii_case("plexus") => {
            let opt = |key: &str| ci.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
            let to_column = opt("to").ok_or_else(|| anyhow::anyhow!("USING GRAPH requires WITH (to = '<destination column>')"))?;
            let type_column = opt("type");
            db.create_graph_index(&ci.table, &ci.column, to_column, type_column)?
        }
        Some(m) if m.eq_ignore_ascii_case("jsonpath") => {
            let opt = |key: &str| ci.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
            let json_path = opt("path")
                .ok_or_else(|| anyhow::anyhow!("USING JSONPATH requires WITH (path = '<dot.separated.path>')"))?;
            db.create_json_path_index(&ci.table, &ci.column, json_path)?
        }
        Some(m) if m.eq_ignore_ascii_case("gin") || m.eq_ignore_ascii_case("fts") => {
            db.create_fts_index(&ci.table, &ci.column)?
        }
        Some(other) => anyhow::bail!("unsupported index method \"{other}\" (supported: default B-Tree, SPATIAL/GIST, TIME/CHRONOS, GRAPH/PLEXUS, JSONPATH, GIN/FTS)"),
    }
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE INDEX".into() })
}

fn execute_create_sequence(cs: crate::sql::ast::CreateSequenceStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    db.create_sequence(&cs.name, cs.start, cs.increment)?;
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE SEQUENCE".into() })
}

/// `CREATE TOPIC name WITH (partitions = n, retention = '<interval>',
/// retention_bytes = n)` (Flux). Mirrors `execute_create_index`'s structure:
/// pull known keys out of the generic `options` list, parse durations via
/// `eval::parse_interval` (same parser Chronos's `retention` index option
/// uses), then call the one `Database` method that does the real work.
fn execute_create_topic(ct: crate::sql::ast::CreateTopicStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    if ct.if_not_exists && db.list_topics().iter().any(|(name, _)| name == &ct.name) {
        return Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE TOPIC".into() });
    }
    let opt = |key: &str| ct.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
    let partitions = match opt("partitions") {
        Some(s) => s.parse::<u32>().map_err(|_| anyhow::anyhow!("invalid partitions value {s:?}"))?,
        None => 1,
    };
    let retention_ms = match opt("retention") {
        Some(s) => Some(eval::parse_interval(s).ok_or_else(|| anyhow::anyhow!("invalid retention {s:?}"))?),
        None => None,
    };
    let retention_bytes = match opt("retention_bytes") {
        Some(s) => Some(s.parse::<u64>().map_err(|_| anyhow::anyhow!("invalid retention_bytes value {s:?}"))?),
        None => None,
    };
    db.create_topic(&ct.name, partitions, retention_ms, retention_bytes)?;
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE TOPIC".into() })
}

/// Apply an `ALTER TABLE ... ALTER COLUMN ...` metadata-only action
/// (`SET`/`DROP DEFAULT`, `SET`/`DROP NOT NULL`) by mutating and
/// re-persisting the schema. `ADD`/`DROP COLUMN` would need to backfill
/// every existing row's encoding and are left as a pre-existing TODO.
fn execute_alter_table(at: crate::sql::ast::AlterTableStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    use crate::sql::ast::{AlterTableAction, ColumnAction};
    match at.action {
        AlterTableAction::AlterColumn { name, action } => {
            let mut schema = db.get_table(&at.table)?
                .ok_or_else(|| anyhow::anyhow!("table \"{}\" does not exist", at.table))?;
            let col = schema.columns.iter_mut().find(|c| c.name == name)
                .ok_or_else(|| anyhow::anyhow!("column \"{name}\" does not exist"))?;
            match action {
                ColumnAction::SetDefault(expr) => col.default = Some(default_expr_to_text(&expr)?),
                ColumnAction::DropDefault => col.default = None,
                ColumnAction::SetNotNull => col.nullable = false,
                ColumnAction::DropNotNull => col.nullable = true,
            }
            db.update_table_schema(schema)?;
        }
        AlterTableAction::AddColumn(_) | AlterTableAction::DropColumn(_) => {
            // Pre-existing gap (row width/encoding change needs a backfill
            // pass this version doesn't implement) — accepted syntactically,
            // no-op, same as before this change.
        }
    }
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "ALTER TABLE".into() })
}

/// Resolve a `CREATE FUNCTION` type name to the restricted set of types
/// WASM UDFs support — deliberately narrower than `ColumnType::from_name`
/// (which also accepts `text`/`int4`/etc.), since those would require a
/// linear-memory ABI this version doesn't implement.
fn udf_column_type(name: &str) -> anyhow::Result<ColumnType> {
    match name.to_lowercase().as_str() {
        "int8" | "bigint" => Ok(ColumnType::Int8),
        "float8" | "double" | "double precision" => Ok(ColumnType::Float8),
        "bool" | "boolean" => Ok(ColumnType::Bool),
        other => anyhow::bail!("WASM UDFs only support int8, float8, and bool argument/return types, got \"{other}\""),
    }
}

fn execute_create_function(cf: crate::sql::ast::CreateFunctionStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    if !cf.language.eq_ignore_ascii_case("wasm") {
        anyhow::bail!("unsupported CREATE FUNCTION language \"{}\" (only \"wasm\" is supported)", cf.language);
    }

    let arg_types: Vec<ColumnType> = cf.args.iter().map(|(_, ty)| udf_column_type(ty)).collect::<anyhow::Result<_>>()?;
    let return_type = udf_column_type(&cf.return_type)?;

    use base64::Engine as _;
    let wasm_bytes = base64::engine::general_purpose::STANDARD
        .decode(cf.body_base64.trim())
        .map_err(|e| anyhow::anyhow!("CREATE FUNCTION body is not valid base64: {e}"))?;

    udf::validate_module(&wasm_bytes, &cf.name, &arg_types, &return_type, db.udf_config().max_module_bytes)?;

    db.create_function(crate::storage::UserFunction {
        name: cf.name,
        arg_types,
        return_type,
        wasm_bytes,
    })?;
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE FUNCTION".into() })
}

/// Resolve one `VALUES (...)` row (positional or via an explicit column
/// list) to a full-width, schema-ordered row of already wire-encoded cells,
/// filling any column not mentioned in `insert_columns` from its DEFAULT
/// (re-parsed from `ColumnDef.default`'s persisted text — see
/// `default_expr_to_text`) or `NULL` if nullable, erroring on a NOT NULL
/// column with neither a provided value nor a default.
fn resolve_insert_row(
    schema: &TableSchema,
    insert_columns: &[String],
    row_values: &[Expr],
    db: &Arc<Database>,
    params: &[Value],
) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
    let mut cells: Vec<Option<Vec<u8>>> = vec![None; schema.columns.len()];
    let mut provided = vec![false; schema.columns.len()];

    if insert_columns.is_empty() {
        if row_values.len() != schema.columns.len() {
            anyhow::bail!(
                "INSERT has {} value(s) but table \"{}\" has {} column(s)",
                row_values.len(), schema.name, schema.columns.len()
            );
        }
        for (i, expr) in row_values.iter().enumerate() {
            cells[i] = eval_expr_with_db(expr, db.clone(), params)?.to_wire_bytes();
            provided[i] = true;
        }
    } else {
        if insert_columns.len() != row_values.len() {
            anyhow::bail!(
                "INSERT column list has {} entries but VALUES has {}",
                insert_columns.len(), row_values.len()
            );
        }
        for (col_name, expr) in insert_columns.iter().zip(row_values) {
            let idx = schema.columns.iter().position(|c| &c.name == col_name)
                .ok_or_else(|| anyhow::anyhow!("column \"{col_name}\" does not exist"))?;
            cells[idx] = eval_expr_with_db(expr, db.clone(), params)?.to_wire_bytes();
            provided[idx] = true;
        }
    }

    for (i, col) in schema.columns.iter().enumerate() {
        if provided[i] {
            continue;
        }
        if let Some(default_text) = &col.default {
            let expr = crate::sql::parse_expr_text(default_text)?;
            cells[i] = eval_expr_with_db(&expr, db.clone(), params)?.to_wire_bytes();
        } else if !col.nullable {
            anyhow::bail!("column \"{}\" is NOT NULL and has no default", col.name);
        }
    }

    Ok(cells)
}

/// Reject `cells` if any `UNIQUE` group collides with an existing row
/// (other than `exclude_key`, for `UPDATE`'s "don't compare a row against
/// itself"). NULLs never participate in uniqueness (Postgres semantics).
/// O(n) table scan per check — no index acceleration yet.
fn check_unique_constraints(
    schema: &TableSchema,
    db: &Arc<Database>,
    cells: &[Option<Vec<u8>>],
    exclude_key: Option<&[u8]>,
) -> anyhow::Result<()> {
    if schema.unique_groups.is_empty() {
        return Ok(());
    }
    let raw_rows = db.scan(&schema.name)?;
    let existing = parse_rows(&raw_rows, &Some(schema.clone()));
    for group in &schema.unique_groups {
        if group.iter().any(|&i| cells.get(i).cloned().flatten().is_none()) {
            continue;
        }
        for (kv, row) in raw_rows.iter().zip(existing.iter()) {
            if exclude_key == Some(kv.key.as_slice()) {
                continue;
            }
            if group.iter().all(|&i| row.get(i).cloned().flatten() == cells.get(i).cloned().flatten()) {
                let cols: Vec<&str> = group.iter().map(|&i| schema.columns[i].name.as_str()).collect();
                anyhow::bail!("duplicate value violates unique constraint on {}({})", schema.name, cols.join(", "));
            }
        }
    }
    Ok(())
}

/// Reject `cells` if any `FOREIGN KEY` column's value doesn't match an
/// existing row in the referenced table. NULL FK values are exempt
/// (Postgres semantics). `ON DELETE`/`ON UPDATE` actions and delete-time
/// `RESTRICT` are not enforced — documented scope cut.
fn check_foreign_keys(schema: &TableSchema, db: &Arc<Database>, cells: &[Option<Vec<u8>>]) -> anyhow::Result<()> {
    for fk in &schema.foreign_keys {
        let Some(val) = cells.get(fk.column).cloned().flatten() else { continue };
        let ref_schema = db.get_table(&fk.ref_table)?
            .ok_or_else(|| anyhow::anyhow!("referenced table \"{}\" does not exist", fk.ref_table))?;
        let ref_idx = ref_schema.columns.iter().position(|c| c.name == fk.ref_column)
            .ok_or_else(|| anyhow::anyhow!("referenced column \"{}\" does not exist", fk.ref_column))?;
        let raw_rows = db.scan(&fk.ref_table)?;
        let ref_rows = parse_rows(&raw_rows, &Some(ref_schema));
        let exists = ref_rows.iter().any(|r| r.get(ref_idx).cloned().flatten().as_deref() == Some(val.as_slice()));
        if !exists {
            let col_name = &schema.columns[fk.column].name;
            anyhow::bail!(
                "insert or update on table \"{}\" violates foreign key constraint: no row in \"{}\" with {} = {:?}",
                schema.name, fk.ref_table, fk.ref_column, String::from_utf8_lossy(&val)
            );
        }
    }
    Ok(())
}

/// Reject `cells` if any Canopy JSON Schema rule (`TableSchema::json_schemas`,
/// set by `CREATE TABLE ... WITH (json_schema_col = ...)`) is violated. A
/// NULL value in the validated column is exempt (nullability is enforced
/// separately by the column's own `nullable` flag).
fn check_json_schemas(schema: &TableSchema, cells: &[Option<Vec<u8>>]) -> anyhow::Result<()> {
    for rule in &schema.json_schemas {
        let Some(col_idx) = schema.columns.iter().position(|c| c.name == rule.column) else { continue };
        let Some(Some(raw)) = cells.get(col_idx) else { continue };
        let Ok(text) = String::from_utf8(raw.clone()) else { continue };
        let Ok(doc) = serde_json::from_str::<serde_json::Value>(&text) else {
            anyhow::bail!("column \"{}\" is not valid JSON", rule.column);
        };
        let Ok(schema_doc) = serde_json::from_str::<serde_json::Value>(&rule.schema) else { continue };
        let mode = crate::storage::json_schema::Mode::parse(&rule.mode).unwrap_or(crate::storage::json_schema::Mode::Strict);
        let errors = crate::storage::json_schema::validate(&schema_doc, &doc, mode);
        if !errors.is_empty() {
            anyhow::bail!("json_schema validation failed for column \"{}\": {}", rule.column, errors.join("; "));
        }
    }
    Ok(())
}

fn execute_insert(insert: crate::sql::ast::InsertStmt, db: Arc<Database>, params: &[Value]) -> anyhow::Result<QueryResult> {
    let schema = db.get_table(&insert.table)?
        .ok_or_else(|| anyhow::anyhow!("table \"{}\" does not exist", insert.table))?;

    let mut row_count = 0usize;
    for row_values in &insert.values {
        let cells = resolve_insert_row(&schema, &insert.columns, row_values, &db, params)?;
        check_unique_constraints(&schema, &db, &cells, None)?;
        check_foreign_keys(&schema, &db, &cells)?;
        check_json_schemas(&schema, &cells)?;

        let pk_idx = schema.pk_columns.first().copied().unwrap_or(0);
        let pk_value = cells.get(pk_idx).cloned().flatten().unwrap_or_default();

        let mut value_buf = Vec::new();
        for cell in &cells {
            match cell {
                Some(data) => {
                    value_buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    value_buf.extend_from_slice(data);
                }
                None => {
                    value_buf.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }

        db.write(&insert.table, &pk_value, &value_buf)?;
        publish_cdc_event(&db, &insert.table, "insert", &pk_value, None, Some(&cells), &schema);
        row_count += 1;
    }

    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: format!("INSERT 0 {row_count}"),
    })
}

/// Publish a CDC event to `__cdc_<table>` for every successful mutation
/// (native Change Data Capture, Flux Phase 11) — unconditional, no opt-in
/// flag. Column values are carried as their wire (Postgres text-format)
/// representation, the same encoding already used for row storage, so
/// `before`/`after` are JSON objects of column name -> JSON string (or JSON
/// null), never typed JSON numbers/booleans. Best-effort: a publish failure
/// is logged, not propagated — CDC must never fail the mutation it
/// describes.
fn publish_cdc_event(
    db: &Arc<Database>,
    table: &str,
    op: &str,
    row_key: &[u8],
    before: Option<&[Option<Vec<u8>>]>,
    after: Option<&[Option<Vec<u8>>]>,
    schema: &TableSchema,
) {
    let cells_to_json = |cells: &[Option<Vec<u8>>]| -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        for (col, cell) in schema.columns.iter().zip(cells) {
            let v = match cell {
                Some(bytes) => serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()),
                None => serde_json::Value::Null,
            };
            obj.insert(col.name.clone(), v);
        }
        serde_json::Value::Object(obj)
    };
    let event = serde_json::json!({
        "op": op,
        "table": table,
        "row_key": hex::encode(row_key),
        "before": before.map(cells_to_json),
        "after": after.map(cells_to_json),
        "ts": crate::storage::flux::now_ms(),
    });
    if let Err(e) = db.flux_publish_cdc(table, event) {
        tracing::warn!(table, error = %e, "failed to publish CDC event");
    }
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
            if let Some(schema) = &schema {
                publish_cdc_event(&db, &delete.table, "delete", &kv.key, Some(row), None, schema);
            }
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

        check_unique_constraints(&schema, &db, &new_row, Some(&kv.key))?;
        check_foreign_keys(&schema, &db, &new_row)?;
        check_json_schemas(&schema, &new_row)?;

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
        publish_cdc_event(&db, &update.table, "update", &kv.key, Some(row), Some(&new_row), &schema);
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
