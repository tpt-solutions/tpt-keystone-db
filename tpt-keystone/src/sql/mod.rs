pub mod ast;
pub mod lexer;
pub mod parser;

use anyhow::Result;
use ast::Stmt;
use lexer::Lexer;
use parser::Parser;

pub fn parse(sql: &str) -> Result<Stmt> {
    let tokens = Lexer::new(sql).tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse_stmt()
}
