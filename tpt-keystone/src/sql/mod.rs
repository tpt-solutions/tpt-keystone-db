pub mod ast;
pub mod cache;
#[cfg(test)]
mod cache_tests;
pub mod lexer;
pub mod parser;
#[cfg(test)]
mod parser_tests;

use anyhow::Result;
use ast::Stmt;
use lexer::Lexer;
use parser::Parser;

pub fn parse(sql: &str) -> Result<Stmt> {
    let tokens = Lexer::new(sql).tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse_stmt()
}

/// Parse a `;`-separated batch of statements — the simple-query protocol
/// (`wire::session::handle_simple_query`) executes every statement a client
/// sends in one `Query` message, not just the first.
pub fn parse_all(sql: &str) -> Result<Vec<Stmt>> {
    let tokens = Lexer::new(sql).tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse_all_stmts()
}

/// Parse a single standalone expression (e.g. a persisted column DEFAULT,
/// re-parsed from the text `executor::default_expr_to_text` produced).
pub fn parse_expr_text(text: &str) -> Result<ast::Expr> {
    let tokens = Lexer::new(text).tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse_expr(0)
}
