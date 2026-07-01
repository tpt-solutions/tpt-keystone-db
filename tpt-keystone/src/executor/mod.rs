mod eval;

use std::sync::Arc;

use crate::sql;
use crate::sql::ast::{Expr, Projection, Stmt, JoinType};
use crate::storage::database::Database;
use crate::storage::{ColumnDef, ColumnType, StorageEngine, TableSchema};
use crate::wire::messages::{FieldDescription, oid};
use eval::eval_expr;

/// The result of executing a query.
pub struct QueryResult {
    pub fields: Vec<FieldDescription>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub tag: String,
}

/// A row with its column values for expression evaluation.
pub struct RowContext {
    pub values: Vec<Option<Vec<u8>>>,
    pub schema: Option<Arc<TableSchema>>,
    pub db: Option<Arc<Database>>,
}

impl RowContext {
    pub fn new(values: Vec<Option<Vec<u8>>>, schema: Option<Arc<TableSchema>>) -> Self {
        Self { values, schema, db: None }
    }

    pub fn with_db(values: Vec<Option<Vec<u8>>>, schema: Option<Arc<TableSchema>>, db: Arc<Database>) -> Self {
        Self { values, schema, db: Some(db) }
    }

    /// Evaluate an expression against this row.
    pub fn eval(&self, expr: &Expr) -> anyhow::Result<eval::Value> {
        match expr {
            Expr::Ident(name) => {
                let col_idx = self.find_column(name)?;
                self.get_value(col_idx)
            }
            Expr::QualifiedIdent(_table, col) => {
                // For now, ignore table qualifier
                let col_idx = self.find_column(col)?;
                self.get_value(col_idx)
            }
            Expr::Subquery(subquery) => {
                // Execute the subquery and return the first value
                if let Some(ref db) = self.db {
                    let result = execute_select_with_cte(*subquery.clone(), db.clone(), &mut CteContext::new())?;
                    // Return the first value from the first row
                    if let Some(first_row) = result.rows.first() {
                        if let Some(first_col) = first_row.first() {
                            if let Some(bytes) = first_col {
                                if let Ok(s) = std::str::from_utf8(bytes) {
                                    if let Ok(n) = s.parse::<i64>() {
                                        return Ok(eval::Value::Int(n));
                                    }
                                    if let Ok(f) = s.parse::<f64>() {
                                        return Ok(eval::Value::Float(f));
                                    }
                                    if s == "t" {
                                        return Ok(eval::Value::Bool(true));
                                    }
                                    if s == "f" {
                                        return Ok(eval::Value::Bool(false));
                                    }
                                    return Ok(eval::Value::Text(s.to_string()));
                                }
                            }
                        }
                    }
                    Ok(eval::Value::Null)
                } else {
                    anyhow::bail!("subqueries not yet supported (no database context)")
                }
            }
            _ => eval_expr(expr),
        }
    }

    fn find_column(&self, name: &str) -> anyhow::Result<usize> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            anyhow::anyhow!("no table context for column \"{name}\"")
        })?;
        schema.columns.iter().position(|c| c.name == name)
            .ok_or_else(|| anyhow::anyhow!("column \"{name}\" does not exist"))
    }

    fn get_value(&self, idx: usize) -> anyhow::Result<eval::Value> {
        let bytes = self.values.get(idx).and_then(|v| v.as_ref());
        match bytes {
            None => Ok(eval::Value::Null),
            Some(b) => {
                // Try to parse as int, float, or text
                if let Ok(s) = std::str::from_utf8(b) {
                    if let Ok(n) = s.parse::<i64>() {
                        return Ok(eval::Value::Int(n));
                    }
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(eval::Value::Float(f));
                    }
                    if s == "t" {
                        return Ok(eval::Value::Bool(true));
                    }
                    if s == "f" {
                        return Ok(eval::Value::Bool(false));
                    }
                }
                Ok(eval::Value::Text(String::from_utf8_lossy(b).to_string()))
            }
        }
    }
}

/// Context for CTE execution, mapping CTE names to their materialized results.
pub struct CteContext {
    /// Maps CTE name to (rows, schema)
    pub ctes: std::collections::HashMap<String, (Vec<Vec<Option<Vec<u8>>>>, Arc<TableSchema>)>,
}

impl CteContext {
    pub fn new() -> Self {
        Self { ctes: std::collections::HashMap::new() }
    }

    pub fn get(&self, name: &str) -> Option<&(Vec<Vec<Option<Vec<u8>>>>, Arc<TableSchema>)> {
        self.ctes.get(name)
    }
}

/// Parse and execute a SQL statement, returning a QueryResult.
pub fn execute_query(sql_text: &str, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    let stmt = sql::parse(sql_text)?;
    match stmt {
        Stmt::Select(select) => execute_select_with_cte(select, db, &mut CteContext::new()),
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
        Stmt::AlterTable(_) => {
            // TODO: Implement ALTER TABLE
            Ok(QueryResult { fields: vec![], rows: vec![], tag: "ALTER TABLE".into() })
        }
    }
}

/// Execute a SELECT statement with CTE context support.
pub fn execute_select_with_cte(
    select: crate::sql::ast::SelectStmt,
    db: Arc<Database>,
    cte_ctx: &mut CteContext,
) -> anyhow::Result<QueryResult> {
    // Execute CTEs first and materialize their results
    for cte in &select.ctes {
        // Execute the CTE subquery (recursively, with the same CTE context)
        let cte_result = execute_select_with_cte(cte.subquery.clone(), db.clone(), cte_ctx)?;
        
        // Build a schema for the CTE from the result fields
        let schema = if !cte.columns.is_empty() {
            // Use explicit column names from CTE definition
            let columns: Vec<ColumnDef> = cte.columns.iter().map(|col_name| {
                ColumnDef {
                    name: col_name.clone(),
                    col_type: ColumnType::Text, // Default to text for now
                    nullable: true,
                    default: None,
                    is_pk: false,
                }
            }).collect();
            Arc::new(TableSchema {
                name: cte.name.clone(),
                columns,
                pk_columns: vec![],
            })
        } else if !cte_result.fields.is_empty() {
            // Derive schema from the result fields
            let columns: Vec<ColumnDef> = cte_result.fields.iter().map(|f| {
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
            }).collect();
            Arc::new(TableSchema {
                name: cte.name.clone(),
                columns,
                pk_columns: vec![],
            })
        } else {
            anyhow::bail!("CTE \"{}\" has no columns", cte.name);
        };
        
        // Store the materialized CTE result
        cte_ctx.ctes.insert(cte.name.clone(), (cte_result.rows, schema));
    }

    // If there's a FROM clause, read from the table(s)
    if let Some(table_with_joins) = &select.from {
        // Get the primary table (check if it's a CTE first)
        let (primary_schema, primary_rows) = if let Some((rows, schema)) = cte_ctx.get(&table_with_joins.primary.name) {
            // It's a CTE reference
            (Some(Arc::new((*schema).clone())), rows.clone())
        } else {
            // It's a regular table
            let schema = db.get_table(&table_with_joins.primary.name)?;
            let rows = db.scan(&table_with_joins.primary.name)?;
            let schema_arc = schema.clone().map(|s| Arc::new(s));
            (schema_arc, parse_rows(&rows, &schema))
        };

        // Apply JOINs
        let mut result_rows = primary_rows;
        for join in &table_with_joins.joins {
            let (join_schema, join_rows) = if let Some((rows, schema)) = cte_ctx.get(&join.table.name) {
                // It's a CTE reference
                (Some(Arc::new((*schema).clone())), rows.clone())
            } else {
                // It's a regular table
                let schema = db.get_table(&join.table.name)?;
                let rows = db.scan(&join.table.name)?;
                let schema_arc = schema.clone().map(|s| Arc::new(s));
                (schema_arc, parse_rows(&rows, &schema))
            };
            result_rows = apply_join(result_rows, join_rows, join, &primary_schema, &join_schema);
        }

        // Build field descriptions
        let mut fields = Vec::new();
        if let Some(ref s) = primary_schema {
            for col in &s.columns {
                fields.push(FieldDescription::simple(&col.name, col.col_type.oid()));
            }
        }

        // Apply WHERE clause filtering
        if let Some(ref where_expr) = select.where_ {
            let schema_arc = primary_schema.as_ref().map(|s| Arc::new(s.clone()));
            let db_for_where = db.clone();
            result_rows = result_rows
                .into_iter()
                .filter(|row| {
                    let ctx = RowContext::with_db(row.clone(), schema_arc.clone(), db_for_where.clone());
                    ctx.eval(where_expr).map(|v| v.is_truthy()).unwrap_or(false)
                })
                .collect();
        }

        // Apply ORDER BY
        if !select.order_by.is_empty() {
            let schema_arc = primary_schema.as_ref().map(|s| Arc::new(s.clone()));
            let db_for_order = db.clone();
            result_rows.sort_by(|a, b| {
                for order in &select.order_by {
                    let ctx_a = RowContext::with_db(a.clone(), schema_arc.clone(), db_for_order.clone());
                    let ctx_b = RowContext::with_db(b.clone(), schema_arc.clone(), db_for_order.clone());
                    let va = ctx_a.eval(&order.expr).unwrap_or(eval::Value::Null);
                    let vb = ctx_b.eval(&order.expr).unwrap_or(eval::Value::Null);
                    let cmp = eval::value_compare(&va, &vb).unwrap_or(0);
                    if cmp < 0 {
                        return if order.asc { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
                    }
                    if cmp > 0 {
                        return if order.asc { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply LIMIT
        if let Some(ref limit_expr) = select.limit {
            let limit_val = eval_expr(limit_expr)?;
            let limit = match limit_val {
                eval::Value::Int(n) => n.max(0) as usize,
                eval::Value::Float(f) => f.max(0.0) as usize as usize,
                _ => result_rows.len(),
            };
            result_rows.truncate(limit);
        }

        // Apply OFFSET
        if let Some(ref offset_expr) = select.offset {
            let offset_val = eval_expr(offset_expr)?;
            let offset = match offset_val {
                eval::Value::Int(n) => n.max(0) as usize,
                eval::Value::Float(f) => f.max(0.0) as usize as usize,
                _ => 0,
            };
            if offset < result_rows.len() {
                result_rows = result_rows[offset..].to_vec();
            } else {
                result_rows.clear();
            }
        }

        // Apply projections (eval expressions)
        let schema_arc = primary_schema.as_ref().map(|s| Arc::new(s.clone()));
        let db_for_proj = db.clone();
        let projected_rows: Vec<Vec<Option<Vec<u8>>>> = result_rows
            .into_iter()
            .map(|row| {
                let ctx = RowContext::with_db(row, schema_arc.clone(), db_for_proj.clone());
                select.projections.iter().map(|proj| {
                    match proj {
                        Projection::Wildcard => {
                            // Wildcard not supported in projected output
                            None
                        }
                        Projection::WildcardTable(_) => {
                            None
                        }
                        Projection::Expr { expr, .. } => {
                            ctx.eval(expr).ok().and_then(|v| v.to_wire_bytes())
                        }
                    }
                }).collect()
            })
            .collect();

        let row_count = projected_rows.len();
        return Ok(QueryResult {
            fields,
            rows: projected_rows,
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
            Projection::WildcardTable(_) => {
                anyhow::bail!("SELECT table.* requires a FROM clause");
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

/// Parse rows from storage into Vec<Vec<Option<Vec<u8>>>>
fn parse_rows(_rows: &[crate::storage::KeyValue], _schema: &Option<crate::storage::TableSchema>) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut result = Vec::new();
    for kv in _rows {
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

/// Apply a JOIN to the result rows.
fn apply_join(
    left_rows: Vec<Vec<Option<Vec<u8>>>>,
    right_rows: Vec<Vec<Option<Vec<u8>>>>,
    join: &crate::sql::ast::Join,
    left_schema: &Option<Arc<TableSchema>>,
    right_schema: &Option<Arc<TableSchema>>,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let left_schema_arc = left_schema.clone();
    let right_schema_arc = right_schema.clone();

    match join.join_type {
        JoinType::Inner => {
            // Hash join: build hash table from right side, probe with left
            let mut hash_table: std::collections::HashMap<Vec<u8>, Vec<Vec<Option<Vec<u8>>>>> = 
                std::collections::HashMap::new();
            
            for row in &right_rows {
                if let Some(ref on_expr) = join.on {
                    let ctx = RowContext::new(row.clone(), right_schema_arc.clone());
                    if let Ok(key) = ctx.eval(on_expr) {
                        if let Some(bytes) = key.to_wire_bytes() {
                            hash_table.entry(bytes).or_default().push(row.clone());
                        }
                    }
                } else {
                    // Cross join
                    hash_table.entry(Vec::new()).or_default().push(row.clone());
                }
            }

            let mut result = Vec::new();
            for left_row in left_rows {
                if let Some(ref on_expr) = join.on {
                    let ctx = RowContext::new(left_row.clone(), left_schema_arc.clone());
                    if let Ok(key) = ctx.eval(on_expr) {
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
                } else {
                    // Cross join
                    for right_row in &right_rows {
                        let mut combined = left_row.clone();
                        combined.extend(right_row.iter().cloned());
                        result.push(combined);
                    }
                }
            }
            result
        }
        JoinType::Left => {
            // Left join: include all left rows, even if no match
            let mut hash_table: std::collections::HashMap<Vec<u8>, Vec<Vec<Option<Vec<u8>>>>> = 
                std::collections::HashMap::new();
            
            for row in &right_rows {
                if let Some(ref on_expr) = join.on {
                    let ctx = RowContext::new(row.clone(), right_schema_arc.clone());
                    if let Ok(key) = ctx.eval(on_expr) {
                        if let Some(bytes) = key.to_wire_bytes() {
                            hash_table.entry(bytes).or_default().push(row.clone());
                        }
                    }
                }
            }

            let mut result = Vec::new();
            for left_row in left_rows {
                if let Some(ref on_expr) = join.on {
                    let ctx = RowContext::new(left_row.clone(), left_schema_arc.clone());
                    if let Ok(key) = ctx.eval(on_expr) {
                        if let Some(bytes) = key.to_wire_bytes() {
                            if let Some(matches) = hash_table.get(&bytes) {
                                for right_row in matches {
                                    let mut combined = left_row.clone();
                                    combined.extend(right_row.iter().cloned());
                                    result.push(combined);
                                }
                            } else {
                                // No match - add left row with nulls for right columns
                                let mut combined = left_row.clone();
                                if let Some(ref s) = right_schema {
                                    for _ in &s.columns {
                                        combined.push(None);
                                    }
                                }
                                result.push(combined);
                            }
                        }
                    }
                }
            }
            result
        }
        JoinType::Right => {
            // Right join: swap and use left join logic
            let mut hash_table: std::collections::HashMap<Vec<u8>, Vec<Vec<Option<Vec<u8>>>>> = 
                std::collections::HashMap::new();
            
            for row in &left_rows {
                if let Some(ref on_expr) = join.on {
                    let ctx = RowContext::new(row.clone(), left_schema_arc.clone());
                    if let Ok(key) = ctx.eval(on_expr) {
                        if let Some(bytes) = key.to_wire_bytes() {
                            hash_table.entry(bytes).or_default().push(row.clone());
                        }
                    }
                }
            }

            let mut result = Vec::new();
            for right_row in right_rows {
                if let Some(ref on_expr) = join.on {
                    let ctx = RowContext::new(right_row.clone(), right_schema_arc.clone());
                    if let Ok(key) = ctx.eval(on_expr) {
                        if let Some(bytes) = key.to_wire_bytes() {
                            if let Some(matches) = hash_table.get(&bytes) {
                                for left_row in matches {
                                    let mut combined = left_row.clone();
                                    combined.extend(right_row.iter().cloned());
                                    result.push(combined);
                                }
                            } else {
                                // No match - add right row with nulls for left columns
                                let mut combined = Vec::new();
                                if let Some(ref s) = left_schema {
                                    for _ in &s.columns {
                                        combined.push(None);
                                    }
                                }
                                combined.extend(right_row.iter().cloned());
                                result.push(combined);
                            }
                        }
                    }
                }
            }
            result
        }
        JoinType::Full => {
            // Full outer join: include all rows from both sides
            // For simplicity, just do inner join for now
            apply_join(left_rows, right_rows, join, &left_schema.as_ref().map(|s| s.clone()), &right_schema.as_ref().map(|s| s.clone()))
        }
        JoinType::Cross => {
            // Cross join: Cartesian product
            let mut result = Vec::new();
            for left_row in left_rows {
                for right_row in &right_rows {
                    let mut combined = left_row.clone();
                    combined.extend(right_row.iter().cloned());
                    result.push(combined);
                }
            }
            result
        }
    }
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