mod eval;

use std::sync::Arc;

use crate::sql;
use crate::sql::ast::{ColumnDef as AstColumnDef, Projection, Stmt};
use crate::storage::database::Database;
use crate::storage::{ColumnDef, ColumnType, StorageEngine};
use crate::wire::messages::{FieldDescription, oid};
use eval::eval_expr;

/// The result of executing a query.
pub struct QueryResult {
    pub fields: Vec<FieldDescription>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub tag: String,
}

/// Parse and execute a SQL statement, returning a QueryResult.
pub fn execute_query(sql_text: &str, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    let stmt = sql::parse(sql_text)?;
    match stmt {
        Stmt::Select(select) => execute_select(select, db),
        Stmt::Insert(insert) => execute_insert(insert, db),
        Stmt::Delete(delete) => execute_delete(delete, db),
        Stmt::Update(update) => execute_update(update, db),
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

fn execute_drop_table(dt: crate::sql::ast::DropTableStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    // For now, just return success
    tracing::info!(table = %dt.table, "DROP TABLE (stub)");
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "DROP TABLE".into() })
}

fn execute_create_index(ci: crate::sql::ast::CreateIndexStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    db.create_index(&ci.table, &ci.column)?;
    Ok(QueryResult { fields: vec![], rows: vec![], tag: "CREATE INDEX".into() })
}

fn execute_insert(insert: crate::sql::ast::InsertStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    let schema = db.get_table(&insert.table)?
        .ok_or_else(|| anyhow::anyhow!("table \"{}\" does not exist", insert.table))?;

    let mut row_count = 0usize;
    for row_values in &insert.values {
        // Build a key from the first PK column value, or use a generated key
        let pk_value = if !schema.pk_columns.is_empty() {
            let pk_idx = schema.pk_columns[0];
            let expr = &row_values[pk_idx.min(row_values.len() - 1)];
            let val = eval_expr(expr)?;
            val.to_wire_bytes().unwrap_or_default()
        } else {
            // Generate a key from the first column
            let val = eval_expr(&row_values[0])?;
            val.to_wire_bytes().unwrap_or_default()
        };

        // Serialize all column values as a single value blob
        let mut value_buf = Vec::new();
        for expr in row_values {
            let val = eval_expr(expr)?;
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

fn execute_delete(delete: crate::sql::ast::DeleteStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    // Scan all rows and delete matching ones
    let rows = db.scan(&delete.table)?;
    let mut deleted = 0usize;

    for kv in &rows {
        // For MVP, delete all rows (WHERE clause not yet implemented for storage)
        db.delete(&delete.table, &kv.key)?;
        deleted += 1;
    }

    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: format!("DELETE {deleted}"),
    })
}

fn execute_update(update: crate::sql::ast::UpdateStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    let rows = db.scan(&update.table)?;
    let mut updated = 0usize;

    for kv in &rows {
        // For MVP, update all rows
        let mut new_value = kv.value.clone();
        // Apply assignments (simplified: just rewrite with first assignment value)
        if let Some((_, ref expr)) = update.assignments.first() {
            let val = eval_expr(expr)?;
            if let Some(bytes) = val.to_wire_bytes() {
                new_value = bytes;
            }
        }
        db.write(&update.table, &kv.key, &new_value)?;
        updated += 1;
    }

    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: format!("UPDATE {updated}"),
    })
}

fn execute_select(select: crate::sql::ast::SelectStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    // If there's a FROM clause, read from the table
    if let Some(ref from) = select.from {
        let schema = db.get_table(&from.name)?;
        let rows = db.scan(&from.name)?;

        let mut fields = Vec::new();
        let mut result_rows = Vec::new();

        // Build field descriptions from schema or data
        if let Some(ref s) = schema {
            for col in &s.columns {
                fields.push(FieldDescription::simple(&col.name, col.col_type.oid()));
            }
        }

        for kv in &rows {
            let mut row = Vec::new();
            // Parse the value blob back into columns
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
            result_rows.push(row);
        }

        // If no schema, use wildcard
        if fields.is_empty() {
            for proj in &select.projections {
                match proj {
                    Projection::Wildcard => {
                        fields.push(FieldDescription::simple("?column?", oid::TEXT));
                    }
                    Projection::Expr { expr, alias } => {
                        let val = eval_expr(expr)?;
                        let col_name = alias.clone().unwrap_or_else(|| infer_column_name(expr));
                        fields.push(FieldDescription::simple(col_name, val.type_oid()));
                    }
                }
            }
        }

        let row_count = result_rows.len();
        return Ok(QueryResult {
            fields,
            rows: result_rows,
            tag: format!("SELECT {row_count}"),
        });
    }

    // No FROM clause — expression-only SELECT
    let mut fields = Vec::new();
    let mut row: Vec<Option<Vec<u8>>> = Vec::new();

    for proj in &select.projections {
        match proj {
            Projection::Wildcard => {
                anyhow::bail!("SELECT * requires a FROM clause");
            }
            Projection::Expr { expr, alias } => {
                let value = eval_expr(expr)?;
                let col_name = alias.clone().unwrap_or_else(|| infer_column_name(expr));
                fields.push(FieldDescription::simple(col_name, value.type_oid()));
                row.push(value.to_wire_bytes());
            }
        }
    }

    let row_count = 1usize;
    Ok(QueryResult {
        fields,
        rows: vec![row],
        tag: format!("SELECT {row_count}"),
    })
}

/// Derive a display name for an expression when no alias is given.
fn infer_column_name(expr: &crate::sql::ast::Expr) -> String {
    use crate::sql::ast::Expr;
    match expr {
        Expr::Ident(name) => name.clone(),
        Expr::QualifiedIdent(_, col) => col.clone(),
        Expr::Function { name, .. } => name.to_lowercase(),
        Expr::Literal(_) => "?column?".into(),
        _ => "?column?".into(),
    }
}