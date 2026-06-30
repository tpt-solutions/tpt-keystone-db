mod eval;

use crate::sql;
use crate::sql::ast::{Projection, Stmt};
use crate::wire::messages::{FieldDescription, oid};
use eval::eval_expr;

/// The result of executing a query.
pub struct QueryResult {
    pub fields: Vec<FieldDescription>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub tag: String,
}

/// Parse and execute a SQL statement, returning a QueryResult.
pub fn execute_query(sql_text: &str) -> anyhow::Result<QueryResult> {
    let stmt = sql::parse(sql_text)?;
    match stmt {
        Stmt::Select(select) => execute_select(select),
        Stmt::Set(s) => {
            tracing::debug!("SET {} = {:?} (ignored)", s.name, s.value);
            Ok(QueryResult { fields: vec![], rows: vec![], tag: "SET".into() })
        }
        Stmt::Show(s) => {
            // Return an empty single-column result for SHOW.
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

fn execute_select(select: crate::sql::ast::SelectStmt) -> anyhow::Result<QueryResult> {
    // MVP: no FROM clause support — expression-only SELECT.
    if select.from.is_some() {
        anyhow::bail!("FROM clause is not supported in this version (no storage engine yet)");
    }

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
