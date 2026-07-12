mod canopy_aggregate;
#[cfg(test)]
mod canopy_tests;
mod catalog;
#[cfg(test)]
mod chronos_tests;
pub mod copy;
mod ddl;
mod dml;
pub mod eval;
#[cfg(test)]
mod flux_tests;
#[cfg(test)]
mod geo_tests;
#[cfg(test)]
mod gpu_join_tests;
mod gql;
mod graph_fn;
mod join;
#[cfg(test)]
mod mirror_tests;
#[cfg(test)]
mod ogc_conformance_tests;
#[cfg(test)]
mod pg_dump_tests;
#[cfg(test)]
mod phase4_tests;
mod planner;
pub mod rbac;
#[cfg(test)]
mod plexus_tests;
#[cfg(test)]
mod prism_tests;
#[cfg(test)]
mod rbac_tests;
#[cfg(test)]
mod regex_op_tests;
mod select;
mod stats;
#[cfg(test)]
mod synapse_tests;
mod udf;

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::ast::{Expr, InList, Projection, SelectStmt, Stmt, TableRef};
use crate::storage::database::Database;
use crate::storage::{ColumnDef, ColumnType, StorageEngine, TableSchema};
use crate::wire::messages::{oid, FieldDescription};
use eval::{OuterRow, Value};

// `select::execute_select_with_cte` is re-exported here (rather than only
// called via `select::`) because `eval.rs`'s scalar-subquery evaluation path
// reaches back into it via `super::execute_select_with_cte` — see that
// module's doc comment.
use select::execute_select_with_cte;

/// The result of executing a query.
#[derive(Debug)]
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
        Self {
            ctes: HashMap::new(),
        }
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
pub fn execute_parsed(
    stmt: Stmt,
    db: Arc<Database>,
    params: &[Value],
) -> anyhow::Result<QueryResult> {
    // Trusted/internal callers (in-process server code, catalog maintenance,
    // the test suite) run unrestricted: authorization is enforced at the wire
    // layer via `execute_parsed_as`, which supplies the authenticated `Actor`.
    execute_parsed_as(stmt, db, params, &rbac::Actor::unrestricted())
}

/// Execute an already-parsed statement on behalf of an authenticated
/// connection. Runs the RBAC `Actor::check` predicate first; on denial the
/// returned error downcasts to `rbac::InsufficientPrivilege` so the wire
/// layer can emit SQLSTATE `42501`.
#[tracing::instrument(skip_all)]
pub fn execute_parsed_as(
    stmt: Stmt,
    db: Arc<Database>,
    params: &[Value],
    actor: &rbac::Actor,
) -> anyhow::Result<QueryResult> {
    let start = std::time::Instant::now();
    let result = actor
        .check(&db, &stmt)
        .map_err(|e| anyhow::Error::new(e))
        .and_then(|()| execute_parsed_inner(stmt, db, params));
    crate::metrics::Metrics::global().record_query(start.elapsed(), result.is_err());
    result
}

fn execute_parsed_inner(
    stmt: Stmt,
    db: Arc<Database>,
    params: &[Value],
) -> anyhow::Result<QueryResult> {
    match stmt {
        Stmt::Select(select) => {
            execute_select_with_cte(select, db, &mut CteContext::new(), &[], params)
        }
        Stmt::Insert(insert) => dml::execute_insert(insert, db, params),
        Stmt::Delete(delete) => dml::execute_delete(delete, db, params),
        Stmt::Update(update) => dml::execute_update(update, db, params),
        Stmt::CreateTable(ct) => ddl::execute_create_table(ct, db),
        Stmt::DropTable(dt) => ddl::execute_drop_table(dt, db),
        Stmt::CreateIndex(ci) => ddl::execute_create_index(ci, db),
        Stmt::CreateFunction(cf) => ddl::execute_create_function(cf, db),
        Stmt::CreateSequence(cs) => ddl::execute_create_sequence(cs, db),
        Stmt::CreateTopic(ct) => ddl::execute_create_topic(ct, db),
        Stmt::Analyze(table) => stats::execute_analyze(table, db),
        Stmt::Match(m) => gql::execute_match(m, db),
        Stmt::Set(s) => {
            tracing::debug!("SET {} = {:?} (ignored)", s.name, s.value);
            Ok(QueryResult {
                fields: vec![],
                rows: vec![],
                tag: "SET".into(),
            })
        }
        Stmt::Show(s) => {
            let field = FieldDescription::simple(&s.name, oid::TEXT);
            Ok(QueryResult {
                fields: vec![field],
                rows: vec![vec![Some(b"".to_vec())]],
                tag: "SHOW".into(),
            })
        }
        Stmt::Begin => Ok(QueryResult {
            fields: vec![],
            rows: vec![],
            tag: "BEGIN".into(),
        }),
        Stmt::Commit => Ok(QueryResult {
            fields: vec![],
            rows: vec![],
            tag: "COMMIT".into(),
        }),
        Stmt::Rollback => Ok(QueryResult {
            fields: vec![],
            rows: vec![],
            tag: "ROLLBACK".into(),
        }),
        Stmt::AlterTable(at) => ddl::execute_alter_table(at, db),
        Stmt::DeclareCursor(_) | Stmt::Fetch(_) | Stmt::MoveCursor(_) | Stmt::CloseCursor(_) => {
            anyhow::bail!("cursor statements are only supported over the simple query protocol")
        }
        Stmt::Notify(channel, payload) => {
            db.notify(&channel, payload.as_deref().unwrap_or(""));
            Ok(QueryResult {
                fields: vec![],
                rows: vec![],
                tag: "NOTIFY".into(),
            })
        }
        Stmt::Listen(_) | Stmt::Unlisten(_) => {
            anyhow::bail!("LISTEN/UNLISTEN are only supported over the simple query protocol")
        }
        Stmt::CopyIn(_) | Stmt::CopyOut(_) => {
            anyhow::bail!("COPY is only supported over the simple query protocol")
        }
        Stmt::CreateRole(c) => rbac::execute_create_role(&db, &c).map(|_| ok_tag("CREATE ROLE")),
        Stmt::AlterRole(a) => rbac::execute_alter_role(&db, &a).map(|_| ok_tag("ALTER ROLE")),
        Stmt::DropRole(d) => rbac::execute_drop_role(&db, &d).map(|_| ok_tag("DROP ROLE")),
        Stmt::Grant(g) => rbac::execute_grant(&db, &g).map(|_| ok_tag("GRANT")),
        Stmt::Revoke(r) => rbac::execute_revoke(&db, &r).map(|_| ok_tag("REVOKE")),
    }
}

/// An empty `QueryResult` tagged with `tag` (used by statements that perform
/// an action but return no rows — RBAC DDL, etc.).
fn ok_tag(tag: &str) -> QueryResult {
    QueryResult {
        fields: vec![],
        rows: vec![],
        tag: tag.to_string(),
    }
}

/// The highest `$n` parameter index referenced anywhere in a statement —
/// used to size the extended query protocol's `ParameterDescription`.
pub fn max_param_index(stmt: &Stmt) -> u32 {
    let mut max = 0u32;
    match stmt {
        Stmt::Select(s) => scan_select_params(s, &mut max),
        Stmt::Insert(i) => {
            for row in &i.values {
                for e in row {
                    scan_expr_params(e, &mut max);
                }
            }
        }
        Stmt::Update(u) => {
            for (_, e) in &u.assignments {
                scan_expr_params(e, &mut max);
            }
            if let Some(w) = &u.where_ {
                scan_expr_params(w, &mut max);
            }
        }
        Stmt::Delete(d) => {
            if let Some(w) = &d.where_ {
                scan_expr_params(w, &mut max);
            }
        }
        _ => {}
    }
    max
}

fn scan_select_params(s: &SelectStmt, max: &mut u32) {
    for cte in &s.ctes {
        scan_select_params(&cte.subquery, max);
    }
    for p in &s.projections {
        if let Projection::Expr { expr, .. } = p {
            scan_expr_params(expr, max);
        }
    }
    if let Some(w) = &s.where_ {
        scan_expr_params(w, max);
    }
    for g in &s.group_by {
        scan_expr_params(g, max);
    }
    if let Some(h) = &s.having {
        scan_expr_params(h, max);
    }
    for o in &s.order_by {
        scan_expr_params(&o.expr, max);
    }
    if let Some(l) = &s.limit {
        scan_expr_params(l, max);
    }
    if let Some(o) = &s.offset {
        scan_expr_params(o, max);
    }
    if let Some((_, rhs)) = &s.union {
        scan_select_params(rhs, max);
    }
}

fn scan_expr_params(expr: &Expr, max: &mut u32) {
    match expr {
        Expr::Param(n) => {
            if *n > *max {
                *max = *n;
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            scan_expr_params(lhs, max);
            scan_expr_params(rhs, max);
        }
        Expr::UnaryOp { expr, .. } => scan_expr_params(expr, max),
        Expr::IsNull { expr, .. } | Expr::IsTrue { expr, .. } | Expr::IsFalse { expr, .. } => {
            scan_expr_params(expr, max)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            scan_expr_params(expr, max);
            scan_expr_params(low, max);
            scan_expr_params(high, max);
        }
        Expr::Like { expr, pattern, .. } => {
            scan_expr_params(expr, max);
            scan_expr_params(pattern, max);
        }
        Expr::In { expr, list, .. } => {
            scan_expr_params(expr, max);
            match list {
                InList::Exprs(v) => {
                    for e in v {
                        scan_expr_params(e, max);
                    }
                }
                InList::Subquery(sq) => scan_select_params(sq, max),
            }
        }
        Expr::Exists { subquery, .. } => scan_select_params(subquery, max),
        Expr::Cast { expr, .. } => scan_expr_params(expr, max),
        Expr::Function { args, .. } => {
            for a in args {
                scan_expr_params(a, max);
            }
        }
        Expr::Case {
            operand,
            branches,
            else_,
        } => {
            if let Some(e) = operand {
                scan_expr_params(e, max);
            }
            for (c, r) in branches {
                scan_expr_params(c, max);
                scan_expr_params(r, max);
            }
            if let Some(e) = else_ {
                scan_expr_params(e, max);
            }
        }
        Expr::Subquery(sq) => scan_select_params(sq, max),
        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for a in args {
                scan_expr_params(a, max);
            }
            for p in partition_by {
                scan_expr_params(p, max);
            }
            for o in order_by {
                scan_expr_params(&o.expr, max);
            }
        }
        _ => {}
    }
}

/// Compute the RowDescription fields a SELECT would produce, without
/// evaluating WHERE/aggregation over any rows. Used to answer the extended
/// query protocol's `Describe` message ahead of `Execute`.
pub fn describe_select(
    select: &SelectStmt,
    db: Arc<Database>,
) -> anyhow::Result<Vec<FieldDescription>> {
    let mut cte_ctx = CteContext::new();
    for cte in &select.ctes {
        select::materialize_cte(cte, db.clone(), &mut cte_ctx, &[], &[])?;
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
        let result =
            execute_select_with_cte((**subquery).clone(), db.clone(), cte_ctx, outer, params)?;
        let schema = schema_from_fields(&table.name, &result.fields);
        return Ok((Some(Arc::new(schema)), result.rows));
    }
    if let Some(args) = &table.func_args {
        return graph_fn::resolve_graph_function(&table.name, args, db, params);
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
    let columns: Vec<ColumnDef> = fields
        .iter()
        .map(|f| {
            let col_type = match f.type_oid {
                oid::INT8 => ColumnType::Int8,
                oid::FLOAT8 => ColumnType::Float8,
                oid::BOOL => ColumnType::Bool,
                _ => ColumnType::Text,
            };
            ColumnDef {
                name: f.name.clone(),
                col_type,
                nullable: true,
                default: None,
                is_pk: false,
            }
        })
        .collect();
    TableSchema {
        name: name.to_string(),
        columns,
        pk_columns: vec![],
        unique_groups: vec![],
        foreign_keys: vec![],
        json_schemas: vec![],
    }
}

/// Combine two table schemas (in row-layout order) so joined columns are
/// resolvable by name in WHERE/ORDER BY/projections.
fn merge_schema(
    left: &Option<Arc<TableSchema>>,
    right: &Option<Arc<TableSchema>>,
) -> Option<Arc<TableSchema>> {
    match (left, right) {
        (Some(l), Some(r)) => {
            let mut columns = l.columns.clone();
            columns.extend(r.columns.iter().cloned());
            Some(Arc::new(TableSchema {
                name: format!("{}_{}", l.name, r.name),
                columns,
                pk_columns: vec![],
                unique_groups: vec![],
                foreign_keys: vec![],
                json_schemas: vec![],
            }))
        }
        (Some(l), None) => Some(l.clone()),
        (None, Some(r)) => Some(r.clone()),
        (None, None) => None,
    }
}

fn build_fields(
    projections: &[Projection],
    schema: &Option<Arc<TableSchema>>,
) -> anyhow::Result<Vec<FieldDescription>> {
    let mut fields = Vec::new();
    for proj in projections {
        match proj {
            Projection::Wildcard => {
                let s = schema
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("SELECT * requires a FROM clause"))?;
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

/// Parse rows from storage into Vec<Vec<Option<Vec<u8>>>>
fn parse_rows(
    rows: &[crate::storage::KeyValue],
    _schema: &Option<TableSchema>,
) -> Vec<Vec<Option<Vec<u8>>>> {
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
                    let cell = &data[pos..end];
                    // Native-binary jsonb cells (Phase 10 storage format) are
                    // self-describing via a marker prefix; decode them back to
                    // canonical JSON text so everything downstream (projection,
                    // WHERE, ->/->>/@> operators) sees text, unchanged. Raw
                    // text cells pass through untouched.
                    match crate::storage::jsonb::decode_cell(cell) {
                        Some(text) => row.push(Some(text)),
                        None => row.push(Some(cell.to_vec())),
                    }
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
