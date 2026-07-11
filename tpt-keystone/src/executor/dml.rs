//! DML execution: INSERT/UPDATE/DELETE, their row-resolution and constraint
//! checks (UNIQUE, FOREIGN KEY, JSON Schema), and CDC event publication.

use std::sync::Arc;

use super::eval::{eval_expr_with_db, RowContext, Value};
use super::parse_rows;
use super::QueryResult;
use crate::sql::ast::Expr;
use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine, TableSchema};

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
                row_values.len(),
                schema.name,
                schema.columns.len()
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
                insert_columns.len(),
                row_values.len()
            );
        }
        for (col_name, expr) in insert_columns.iter().zip(row_values) {
            let idx = schema
                .columns
                .iter()
                .position(|c| &c.name == col_name)
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

    normalize_vector_cells(schema, &mut cells);
    Ok(cells)
}

/// Re-serializes any `VECTOR`-typed cell through `Vector::from_text`/
/// `to_text` so a stored value is always in canonical form (`"[1,2,3]"`)
/// regardless of how the literal was written (`"[1.0, 2.0, 3.0]"`) — same
/// "canonicalize on write" precedent as `jsonb_set` normalizing JSON text.
/// Malformed vector text is left as-is; `check_json_schemas`-style rejection
/// isn't attempted here since `VECTOR` has no schema-validation hook.
fn normalize_vector_cells(schema: &TableSchema, cells: &mut [Option<Vec<u8>>]) {
    for (i, col) in schema.columns.iter().enumerate() {
        if col.col_type != ColumnType::Vector {
            continue;
        }
        if let Some(Some(bytes)) = cells.get(i) {
            if let Ok(text) = std::str::from_utf8(bytes) {
                if let Ok(vector) = crate::vector::vector::Vector::from_text(text) {
                    cells[i] = Some(vector.to_text().into_bytes());
                }
            }
        }
    }
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
        if group
            .iter()
            .any(|&i| cells.get(i).cloned().flatten().is_none())
        {
            continue;
        }
        for (kv, row) in raw_rows.iter().zip(existing.iter()) {
            if exclude_key == Some(kv.key.as_slice()) {
                continue;
            }
            if group
                .iter()
                .all(|&i| row.get(i).cloned().flatten() == cells.get(i).cloned().flatten())
            {
                let cols: Vec<&str> = group
                    .iter()
                    .map(|&i| schema.columns[i].name.as_str())
                    .collect();
                anyhow::bail!(
                    "duplicate value violates unique constraint on {}({})",
                    schema.name,
                    cols.join(", ")
                );
            }
        }
    }
    Ok(())
}

/// Reject `cells` if any `FOREIGN KEY` column's value doesn't match an
/// existing row in the referenced table. NULL FK values are exempt
/// (Postgres semantics). `ON DELETE`/`ON UPDATE` actions and delete-time
/// `RESTRICT` are not enforced — documented scope cut.
fn check_foreign_keys(
    schema: &TableSchema,
    db: &Arc<Database>,
    cells: &[Option<Vec<u8>>],
) -> anyhow::Result<()> {
    for fk in &schema.foreign_keys {
        let Some(val) = cells.get(fk.column).cloned().flatten() else {
            continue;
        };
        let ref_schema = db.get_table(&fk.ref_table)?.ok_or_else(|| {
            anyhow::anyhow!("referenced table \"{}\" does not exist", fk.ref_table)
        })?;
        let ref_idx = ref_schema
            .columns
            .iter()
            .position(|c| c.name == fk.ref_column)
            .ok_or_else(|| {
                anyhow::anyhow!("referenced column \"{}\" does not exist", fk.ref_column)
            })?;
        let raw_rows = db.scan(&fk.ref_table)?;
        let ref_rows = parse_rows(&raw_rows, &Some(ref_schema));
        let exists = ref_rows
            .iter()
            .any(|r| r.get(ref_idx).cloned().flatten().as_deref() == Some(val.as_slice()));
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
        let Some(col_idx) = schema.columns.iter().position(|c| c.name == rule.column) else {
            continue;
        };
        let Some(Some(raw)) = cells.get(col_idx) else {
            continue;
        };
        let Ok(text) = String::from_utf8(raw.clone()) else {
            continue;
        };
        let Ok(doc) = serde_json::from_str::<serde_json::Value>(&text) else {
            anyhow::bail!("column \"{}\" is not valid JSON", rule.column);
        };
        let Ok(schema_doc) = serde_json::from_str::<serde_json::Value>(&rule.schema) else {
            continue;
        };
        let mode = crate::storage::json_schema::Mode::parse(&rule.mode)
            .unwrap_or(crate::storage::json_schema::Mode::Strict);
        let errors = crate::storage::json_schema::validate(&schema_doc, &doc, mode);
        if !errors.is_empty() {
            anyhow::bail!(
                "json_schema validation failed for column \"{}\": {}",
                rule.column,
                errors.join("; ")
            );
        }
    }
    Ok(())
}

pub(super) fn execute_insert(
    insert: crate::sql::ast::InsertStmt,
    db: Arc<Database>,
    params: &[Value],
) -> anyhow::Result<QueryResult> {
    let schema = db
        .get_table(&insert.table)?
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
        publish_cdc_event(
            &db,
            &insert.table,
            "insert",
            &pk_value,
            None,
            Some(&cells),
            &schema,
        );
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
                Some(bytes) => {
                    serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned())
                }
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

pub(super) fn execute_delete(
    delete: crate::sql::ast::DeleteStmt,
    db: Arc<Database>,
    params: &[Value],
) -> anyhow::Result<QueryResult> {
    let schema = db.get_table(&delete.table)?;
    let schema_arc = schema.clone().map(Arc::new);
    let raw_rows = db.scan(&delete.table)?;
    let parsed = parse_rows(&raw_rows, &schema);

    let mut deleted = 0usize;
    for (kv, row) in raw_rows.iter().zip(parsed.iter()) {
        let matches = match &delete.where_ {
            Some(expr) => {
                let ctx = RowContext::with_db(row.clone(), schema_arc.clone(), db.clone())
                    .with_params(params.to_vec());
                ctx.eval(expr).map(|v| v.is_truthy()).unwrap_or(false)
            }
            None => true,
        };
        if matches {
            db.delete(&delete.table, &kv.key)?;
            if let Some(schema) = &schema {
                publish_cdc_event(
                    &db,
                    &delete.table,
                    "delete",
                    &kv.key,
                    Some(row),
                    None,
                    schema,
                );
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

pub(super) fn execute_update(
    update: crate::sql::ast::UpdateStmt,
    db: Arc<Database>,
    params: &[Value],
) -> anyhow::Result<QueryResult> {
    let schema = db
        .get_table(&update.table)?
        .ok_or_else(|| anyhow::anyhow!("table \"{}\" does not exist", update.table))?;
    let schema_arc = Arc::new(schema.clone());
    let raw_rows = db.scan(&update.table)?;
    let parsed = parse_rows(&raw_rows, &Some(schema.clone()));

    let mut updated = 0usize;
    for (kv, row) in raw_rows.iter().zip(parsed.iter()) {
        let ctx = RowContext::with_db(row.clone(), Some(schema_arc.clone()), db.clone())
            .with_params(params.to_vec());
        let matches = match &update.where_ {
            Some(expr) => ctx.eval(expr).map(|v| v.is_truthy()).unwrap_or(false),
            None => true,
        };
        if !matches {
            continue;
        }

        let mut new_row = row.clone();
        for (col_name, expr) in &update.assignments {
            let idx = schema
                .columns
                .iter()
                .position(|c| c.name == *col_name)
                .ok_or_else(|| anyhow::anyhow!("column \"{col_name}\" does not exist"))?;
            let val = ctx.eval(expr)?;
            new_row[idx] = val.to_wire_bytes();
        }
        normalize_vector_cells(&schema, &mut new_row);

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
        publish_cdc_event(
            &db,
            &update.table,
            "update",
            &kv.key,
            Some(row),
            Some(&new_row),
            &schema,
        );
        updated += 1;
    }

    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: format!("UPDATE {updated}"),
    })
}
