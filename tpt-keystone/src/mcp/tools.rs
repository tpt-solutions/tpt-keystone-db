//! The 7 MCP tools (`tools()`, `columns()`, `schema()`, `query()`,
//! `mutate()`, `explain()`, `related()`), dispatched by name from
//! `protocol.rs`. Each reuses existing executor/storage plumbing rather than
//! re-implementing query execution or catalog lookups.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value as Json};

use crate::executor::eval::Value;
use crate::executor::rbac::Actor;
use crate::executor::{self, execute_parsed_as, QueryResult};
use crate::sql::ast::{SelectStmt, Stmt};
use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine, TableSchema};

/// Dispatch an MCP tool call. `actor` is the authenticated identity, threaded
/// into the SQL-executing tools (`query`/`mutate`) for per-table RBAC; the
/// pure catalog-introspection tools (`tables`/`columns`/`schema`/`related`)
/// read schema shape (an `X-TPT-Token`-gated surface) and don't execute
/// user-authored SQL against arbitrary tables.
pub fn call(db: &Arc<Database>, actor: &Actor, name: &str, args: &Json) -> Result<Json> {
    match name {
        "tables" => tables(db),
        "columns" => columns(db, args),
        "schema" => schema(db),
        "query" => query(db, actor, args),
        "mutate" => mutate(db, actor, args),
        "explain" => explain(db, args),
        "related" => related(db, actor, args),
        other => bail!("unknown tool: {other}"),
    }
}

fn arg_sql(args: &Json) -> Result<&str> {
    args.get("sql")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing required argument: sql"))
}

fn arg_table(args: &Json) -> Result<&str> {
    args.get("table")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing required argument: table"))
}

fn tables(db: &Arc<Database>) -> Result<Json> {
    Ok(json!(db.list_tables()?))
}

fn columns(db: &Arc<Database>, args: &Json) -> Result<Json> {
    let table = arg_table(args)?;
    let schema = db
        .get_table(table)?
        .ok_or_else(|| anyhow!("no such table: {table}"))?;
    Ok(json!(schema.columns))
}

/// Rows scanned per column histogram bucket beyond which the histogram is
/// skipped for that table (keeps introspection cheap on large tables).
const HISTOGRAM_ROW_CAP: i64 = 10_000;
/// Max distinct-value buckets returned per column histogram.
const HISTOGRAM_BUCKETS: usize = 10;

/// Full schema dump: every table's columns/foreign keys, which columns have
/// a B-Tree index, row-count and per-column value-distribution histograms,
/// and the FK relationship graph as machine-readable nodes/edges.
fn schema(db: &Arc<Database>) -> Result<Json> {
    let indexes = db.list_indexes();
    let mut tables_json = Vec::new();
    let mut graph_edges = Vec::new();
    for name in db.list_tables()? {
        if let Some(t) = db.get_table(&name)? {
            let indexed_columns: Vec<&String> = indexes
                .iter()
                .filter(|(tbl, _)| tbl == &name)
                .map(|(_, col)| col)
                .collect();
            let row_count = table_row_count(db, &name)?;
            let histograms = column_histograms(db, &name, &t, row_count);
            for fk in &t.foreign_keys {
                graph_edges.push(json!({
                    "from_table": name,
                    "from_column": t.columns[fk.column].name,
                    "to_table": fk.ref_table,
                    "to_column": fk.ref_column,
                }));
            }
            tables_json.push(json!({
                "name": t.name,
                "columns": t.columns,
                "foreign_keys": t.foreign_keys,
                "indexed_columns": indexed_columns,
                "row_count": row_count,
                "histograms": histograms,
            }));
        }
    }
    Ok(json!({
        "tables": tables_json,
        "relationship_graph": {"nodes": db.list_tables()?, "edges": graph_edges},
    }))
}

fn table_row_count(db: &Arc<Database>, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let result = executor::execute_query(&sql, db.clone())?;
    let count = result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|cell| cell.as_ref())
        .and_then(|bytes| String::from_utf8_lossy(bytes).parse::<i64>().ok())
        .unwrap_or(0);
    Ok(count)
}

/// Per-column top-N value/count histograms via `GROUP BY ... ORDER BY COUNT(*)
/// DESC LIMIT N` — skipped for `bytea`/`json` columns (not meaningfully
/// bucketable) and for tables over `HISTOGRAM_ROW_CAP` rows.
fn column_histograms(
    db: &Arc<Database>,
    table: &str,
    schema: &TableSchema,
    row_count: i64,
) -> Json {
    if row_count == 0 {
        return json!({});
    }
    if row_count > HISTOGRAM_ROW_CAP {
        return json!({"note": format!("skipped: {row_count} rows exceeds histogram cap of {HISTOGRAM_ROW_CAP}")});
    }
    let mut out = serde_json::Map::new();
    for col in &schema.columns {
        if matches!(col.col_type, ColumnType::Bytea | ColumnType::Json) {
            continue;
        }
        let sql = format!(
            "SELECT {col_name}, COUNT(*) AS cnt FROM {table} GROUP BY {col_name} ORDER BY cnt DESC LIMIT {HISTOGRAM_BUCKETS}",
            col_name = col.name
        );
        if let Ok(result) = executor::execute_query(&sql, db.clone()) {
            let buckets: Vec<Json> = result
                .rows
                .iter()
                .map(|row| {
                    let value = cell_text(row.first().unwrap_or(&None));
                    let count = row
                        .get(1)
                        .and_then(|c| c.as_ref())
                        .and_then(|b| String::from_utf8_lossy(b).parse::<i64>().ok())
                        .unwrap_or(0);
                    json!({"value": value, "count": count})
                })
                .collect();
            out.insert(col.name.clone(), json!(buckets));
        }
    }
    Json::Object(out)
}

/// Read-only execution: only `SELECT`/`SHOW` are accepted. Anything else
/// (including DDL) must go through `mutate`.
fn query(db: &Arc<Database>, actor: &Actor, args: &Json) -> Result<Json> {
    let sql = arg_sql(args)?;
    let stmt = db.parse_cached(sql)?;
    if !matches!(stmt, Stmt::Select(_) | Stmt::Show(_)) {
        bail!(
            "query() only accepts read-only statements (SELECT/SHOW) — use mutate() for writes/DDL"
        );
    }
    let result = execute_parsed_as(stmt, db.clone(), &[], actor)?;
    Ok(rows_to_json(&result))
}

fn mutate(db: &Arc<Database>, actor: &Actor, args: &Json) -> Result<Json> {
    let sql = arg_sql(args)?;
    let stmt = db.parse_cached(sql)?;
    let result = execute_parsed_as(stmt, db.clone(), &[], actor)?;
    // DML tags are "INSERT 0 <n>" / "UPDATE <n>" / "DELETE <n>"; DDL tags
    // (e.g. "CREATE TABLE") have no trailing count.
    let rows_affected = result
        .tag
        .rsplit(' ')
        .next()
        .and_then(|n| n.parse::<u64>().ok());
    Ok(json!({"tag": result.tag, "rows_affected": rows_affected}))
}

/// Structural summary of the parsed statement's shape — not a cost-based
/// EXPLAIN. The executor has no plan/cost estimation today, so a literal
/// `EXPLAIN` (row estimates, scan strategy) is out of scope until that
/// exists; this describes what was parsed instead.
fn explain(db: &Arc<Database>, args: &Json) -> Result<Json> {
    let sql = arg_sql(args)?;
    let stmt = db.parse_cached(sql)?;
    Ok(describe_stmt(&stmt))
}

/// Max `{subject, relation, object}` facts returned by `related()` — caps
/// response size for the agent regardless of `max_depth`/`limit`.
const RELATED_FACT_CAP: usize = 200;

/// Structured retrieval tool: walks the FK graph outward from one row (both
/// the FKs it declares and the FKs other tables declare against it) and
/// returns compact `{subject, relation, object}` triples with human-readable
/// labels — never raw rows or unfiltered subgraphs.
fn related(db: &Arc<Database>, actor: &Actor, args: &Json) -> Result<Json> {
    let table = arg_table(args)?.to_string();
    let id = match args.get("id") {
        Some(Json::String(s)) => s.clone(),
        Some(n @ Json::Number(_)) => n.to_string(),
        _ => bail!("missing required argument: id"),
    };
    let max_depth = args
        .get("max_depth")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
        .min(2);
    let per_hop_limit = args
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(20)
        .clamp(1, 100);

    let mut visited: HashSet<(String, String)> = HashSet::new();
    let mut frontier: Vec<(String, String)> = vec![(table, id)];
    let mut facts: Vec<Json> = Vec::new();
    let mut truncated = false;

    for _ in 0..=max_depth {
        if frontier.is_empty() || facts.len() >= RELATED_FACT_CAP {
            break;
        }
        let mut next_frontier = Vec::new();
        for (tbl, id_val) in frontier {
            if !visited.insert((tbl.clone(), id_val.clone())) {
                continue;
            }
            let Some(schema) = db.get_table(&tbl)? else {
                continue;
            };
            let pk_idx = schema.pk_columns.first().copied().unwrap_or(0);
            let Some(pk_col) = schema.columns.get(pk_idx) else {
                continue;
            };
            let pk_value = typed_value(&pk_col.col_type, &id_val);
            let Some(row) = select_one_by_column(db, actor, &tbl, &pk_col.name, pk_value)? else {
                continue;
            };
            let subject_label = label_for_row(&schema, &row);
            let subject = format!("{tbl}:{id_val}");

            for fk in &schema.foreign_keys {
                if facts.len() >= RELATED_FACT_CAP {
                    truncated = true;
                    break;
                }
                let Some(fk_val) = row.get(fk.column).and_then(cell_text) else {
                    continue;
                };
                let Some(ref_schema) = db.get_table(&fk.ref_table)? else {
                    continue;
                };
                let ref_col_type = ref_schema
                    .columns
                    .iter()
                    .find(|c| c.name == fk.ref_column)
                    .map(|c| c.col_type.clone())
                    .unwrap_or(ColumnType::Text);
                let ref_row = select_one_by_column(
                    db,
                    actor,
                    &fk.ref_table,
                    &fk.ref_column,
                    typed_value(&ref_col_type, &fk_val),
                )?;
                let object_label = ref_row
                    .as_ref()
                    .map(|r| label_for_row(&ref_schema, r))
                    .unwrap_or_else(|| fk_val.clone());
                facts.push(json!({
                    "subject": subject, "subject_label": subject_label,
                    "relation": schema.columns[fk.column].name,
                    "direction": "outgoing",
                    "object": format!("{}:{}", fk.ref_table, fk_val), "object_label": object_label,
                }));
                next_frontier.push((fk.ref_table.clone(), fk_val));
            }

            'incoming: for other_name in db.list_tables()? {
                let Some(other_schema) = db.get_table(&other_name)? else {
                    continue;
                };
                for fk in &other_schema.foreign_keys {
                    if fk.ref_table != tbl {
                        continue;
                    }
                    if facts.len() >= RELATED_FACT_CAP {
                        truncated = true;
                        break 'incoming;
                    }
                    let fk_col_name = other_schema.columns[fk.column].name.clone();
                    let rows = select_many_by_column(
                        db,
                        actor,
                        &other_name,
                        &fk_col_name,
                        typed_value(&pk_col.col_type, &id_val),
                        per_hop_limit,
                    )?;
                    let other_pk_idx = other_schema.pk_columns.first().copied().unwrap_or(0);
                    for r in rows {
                        if facts.len() >= RELATED_FACT_CAP {
                            truncated = true;
                            break;
                        }
                        let other_pk_text =
                            r.get(other_pk_idx).and_then(cell_text).unwrap_or_default();
                        let other_label = label_for_row(&other_schema, &r);
                        facts.push(json!({
                            "subject": format!("{other_name}:{other_pk_text}"), "subject_label": other_label,
                            "relation": fk_col_name,
                            "direction": "incoming",
                            "object": subject.clone(), "object_label": subject_label.clone(),
                        }));
                        next_frontier.push((other_name.clone(), other_pk_text));
                    }
                }
            }
        }
        frontier = next_frontier;
    }

    Ok(json!({"facts": facts, "truncated": truncated}))
}

fn typed_value(col_type: &ColumnType, text: &str) -> Value {
    match col_type {
        ColumnType::Int8 | ColumnType::Int4 | ColumnType::Int2 => text
            .parse::<i64>()
            .map(Value::Int)
            .unwrap_or_else(|_| Value::Text(text.to_string())),
        ColumnType::Float8 | ColumnType::Float4 => text
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or_else(|_| Value::Text(text.to_string())),
        ColumnType::Bool => match text {
            "t" | "true" => Value::Bool(true),
            "f" | "false" => Value::Bool(false),
            _ => Value::Text(text.to_string()),
        },
        _ => Value::Text(text.to_string()),
    }
}

fn cell_text(cell: &Option<Vec<u8>>) -> Option<String> {
    cell.as_ref()
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

/// Picks a human-readable label for a row: the first non-null `text` column
/// other than the primary key, else falls back to `pk_col=value`.
fn label_for_row(schema: &TableSchema, row: &[Option<Vec<u8>>]) -> String {
    let pk_idx = schema.pk_columns.first().copied().unwrap_or(0);
    for (i, col) in schema.columns.iter().enumerate() {
        if i == pk_idx || col.col_type != ColumnType::Text {
            continue;
        }
        if let Some(text) = row.get(i).and_then(cell_text) {
            if !text.is_empty() {
                return format!("{}={}", col.name, text);
            }
        }
    }
    let pk_name = schema
        .columns
        .get(pk_idx)
        .map(|c| c.name.as_str())
        .unwrap_or("id");
    let pk_text = row.get(pk_idx).and_then(cell_text).unwrap_or_default();
    format!("{pk_name}={pk_text}")
}

fn select_one_by_column(
    db: &Arc<Database>,
    actor: &Actor,
    table: &str,
    column: &str,
    value: Value,
) -> Result<Option<Vec<Option<Vec<u8>>>>> {
    let sql = format!("SELECT * FROM {table} WHERE {column} = $1 LIMIT 1");
    let stmt = db.parse_cached(&sql)?;
    let result = execute_parsed_as(stmt, db.clone(), &[value], actor)?;
    Ok(result.rows.into_iter().next())
}

fn select_many_by_column(
    db: &Arc<Database>,
    actor: &Actor,
    table: &str,
    column: &str,
    value: Value,
    limit: u64,
) -> Result<Vec<Vec<Option<Vec<u8>>>>> {
    let sql = format!("SELECT * FROM {table} WHERE {column} = $1 LIMIT {limit}");
    let stmt = db.parse_cached(&sql)?;
    let result = execute_parsed_as(stmt, db.clone(), &[value], actor)?;
    Ok(result.rows)
}

fn rows_to_json(result: &QueryResult) -> Json {
    let columns: Vec<&str> = result.fields.iter().map(|f| f.name.as_str()).collect();
    let rows: Vec<Json> = result
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (i, cell) in row.iter().enumerate() {
                let key = columns.get(i).copied().unwrap_or("?").to_string();
                let value = match cell {
                    Some(bytes) => Json::String(String::from_utf8_lossy(bytes).into_owned()),
                    None => Json::Null,
                };
                obj.insert(key, value);
            }
            Json::Object(obj)
        })
        .collect();
    json!({"columns": columns, "rows": rows, "row_count": result.rows.len()})
}

fn describe_stmt(stmt: &Stmt) -> Json {
    match stmt {
        Stmt::Select(s) => json!({
            "statement": "select",
            "tables": collect_tables(s),
            "has_where": s.where_.is_some(),
            "has_group_by": !s.group_by.is_empty(),
            "has_order_by": !s.order_by.is_empty(),
            "has_limit": s.limit.is_some(),
            "join_count": s.from.as_ref().map(|f| f.joins.len()).unwrap_or(0),
        }),
        Stmt::Insert(i) => json!({"statement": "insert", "table": i.table}),
        Stmt::Update(u) => {
            json!({"statement": "update", "table": u.table, "has_where": u.where_.is_some()})
        }
        Stmt::Delete(d) => {
            json!({"statement": "delete", "table": d.table, "has_where": d.where_.is_some()})
        }
        Stmt::CreateTable(c) => json!({"statement": "create_table", "table": c.table}),
        Stmt::DropTable(d) => json!({"statement": "drop_table", "table": d.table}),
        Stmt::CreateIndex(c) => {
            json!({"statement": "create_index", "table": c.table, "column": c.column})
        }
        other => json!({"statement": format!("{other:?}")
            .split('(')
            .next()
            .unwrap_or("unknown")
            .to_lowercase()}),
    }
}

fn collect_tables(s: &SelectStmt) -> Vec<String> {
    let mut names = Vec::new();
    if let Some(from) = &s.from {
        names.push(from.primary.name.clone());
        for j in &from.joins {
            names.push(j.table.name.clone());
        }
    }
    names
}
