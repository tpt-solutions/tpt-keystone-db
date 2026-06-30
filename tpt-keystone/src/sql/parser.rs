use super::ast::*;
use super::lexer::Token;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn peek2(&self) -> &Token {
        self.tokens.get(self.pos + 1).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        let t = self.tokens.get(self.pos).unwrap_or(&Token::Eof);
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    fn expect(&mut self, expected: &Token) -> anyhow::Result<()> {
        let got = self.advance();
        if got != expected {
            anyhow::bail!("expected {:?}, got {:?}", expected, got);
        }
        Ok(())
    }

    fn eat(&mut self, tok: &Token) -> bool {
        if self.peek() == tok {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    pub fn parse_stmt(&mut self) -> anyhow::Result<Stmt> {
        // Skip leading semicolons
        while self.eat(&Token::Semicolon) {}

        let stmt = match self.peek().clone() {
            Token::Select | Token::Distinct => Ok(Stmt::Select(self.parse_select()?)),
            Token::Set => {
                self.advance();
                self.parse_set()
            }
            Token::Show => {
                self.advance();
                self.parse_show()
            }
            Token::Begin => { self.advance(); Ok(Stmt::Begin) }
            Token::Commit => { self.advance(); Ok(Stmt::Commit) }
            Token::Rollback => { self.advance(); Ok(Stmt::Rollback) }
            Token::Eof => anyhow::bail!("empty statement"),
            other => anyhow::bail!("unexpected token: {:?}", other),
        }?;

        // Consume optional trailing semicolon.
        self.eat(&Token::Semicolon);
        Ok(stmt)
    }

    fn parse_select(&mut self) -> anyhow::Result<SelectStmt> {
        self.expect(&Token::Select)?;

        let distinct = self.eat(&Token::Distinct);
        if !distinct {
            self.eat(&Token::All);
        }

        let projections = self.parse_projection_list()?;

        let from = if self.eat(&Token::From) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        let where_ = if self.eat(&Token::Where) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        let group_by = if self.peek() == &Token::Group && self.peek2() == &Token::By {
            self.advance(); self.advance();
            self.parse_expr_list()?
        } else {
            vec![]
        };

        let having = if self.eat(&Token::Having) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        let order_by = if self.peek() == &Token::Order && self.peek2() == &Token::By {
            self.advance(); self.advance();
            self.parse_order_by_list()?
        } else {
            vec![]
        };

        let limit = if self.eat(&Token::Limit) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        let offset = if self.eat(&Token::Offset) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        Ok(SelectStmt { distinct, projections, from, where_, group_by, having, order_by, limit, offset })
    }

    fn parse_projection_list(&mut self) -> anyhow::Result<Vec<Projection>> {
        let mut list = vec![self.parse_projection()?];
        while self.eat(&Token::Comma) {
            list.push(self.parse_projection()?);
        }
        Ok(list)
    }

    fn parse_projection(&mut self) -> anyhow::Result<Projection> {
        if self.eat(&Token::Star) {
            return Ok(Projection::Wildcard);
        }
        let expr = self.parse_expr(0)?;
        let alias = if self.eat(&Token::As) {
            Some(self.parse_ident_string()?)
        } else if let Token::Ident(_) = self.peek() {
            // implicit alias: SELECT 1 two
            Some(self.parse_ident_string()?)
        } else {
            None
        };
        Ok(Projection::Expr { expr, alias })
    }

    fn parse_table_ref(&mut self) -> anyhow::Result<TableRef> {
        let name = self.parse_ident_string()?;
        let alias = if self.eat(&Token::As) {
            Some(self.parse_ident_string()?)
        } else if let Token::Ident(_) = self.peek() {
            Some(self.parse_ident_string()?)
        } else {
            None
        };
        Ok(TableRef { name, alias })
    }

    fn parse_order_by_list(&mut self) -> anyhow::Result<Vec<OrderBy>> {
        let mut list = vec![self.parse_order_by()?];
        while self.eat(&Token::Comma) {
            list.push(self.parse_order_by()?);
        }
        Ok(list)
    }

    fn parse_order_by(&mut self) -> anyhow::Result<OrderBy> {
        let expr = self.parse_expr(0)?;
        let asc = if self.eat(&Token::Desc) {
            false
        } else {
            self.eat(&Token::Asc);
            true
        };
        Ok(OrderBy { expr, asc })
    }

    fn parse_set(&mut self) -> anyhow::Result<Stmt> {
        // SET name = value  OR  SET name TO value
        let name = self.parse_ident_string()?;
        if !self.eat(&Token::Eq) {
            // Try TO keyword
            if let Token::Ident(s) = self.peek().clone() {
                if s.to_uppercase() == "TO" {
                    self.advance();
                }
            }
        }
        // Consume the rest of the value as a raw string until ; or EOF
        let mut parts = Vec::new();
        loop {
            match self.peek().clone() {
                Token::Semicolon | Token::Eof => break,
                t => {
                    parts.push(format!("{:?}", t));
                    self.advance();
                }
            }
        }
        Ok(Stmt::Set(SetStmt { name, value: parts.join(" ") }))
    }

    fn parse_show(&mut self) -> anyhow::Result<Stmt> {
        let name = self.parse_ident_string()?;
        Ok(Stmt::Show(ShowStmt { name }))
    }

    fn parse_ident_string(&mut self) -> anyhow::Result<String> {
        match self.advance().clone() {
            Token::Ident(s) => Ok(s),
            // Allow keywords used as identifiers in some positions
            other => anyhow::bail!("expected identifier, got {:?}", other),
        }
    }

    fn parse_expr_list(&mut self) -> anyhow::Result<Vec<Expr>> {
        let mut list = vec![self.parse_expr(0)?];
        while self.eat(&Token::Comma) {
            list.push(self.parse_expr(0)?);
        }
        Ok(list)
    }

    /// Precedence-climbing expression parser.
    /// Minimum binding power: 0 = lowest (OR), higher = tighter binding.
    pub fn parse_expr(&mut self, min_bp: u8) -> anyhow::Result<Expr> {
        let mut lhs = self.parse_unary()?;

        loop {
            let (l_bp, r_bp, op) = match infix_bp(self.peek()) {
                Some(x) => x,
                None => break,
            };
            if l_bp < min_bp {
                break;
            }

            // Handle IS NULL / IS NOT NULL / IS TRUE / IS FALSE
            if self.peek() == &Token::Is {
                self.advance();
                let negated = self.eat(&Token::Not);
                match self.peek().clone() {
                    Token::Null => {
                        self.advance();
                        lhs = Expr::IsNull { expr: Box::new(lhs), negated };
                        continue;
                    }
                    Token::True => {
                        self.advance();
                        lhs = Expr::IsTrue { expr: Box::new(lhs), negated };
                        continue;
                    }
                    Token::False => {
                        self.advance();
                        lhs = Expr::IsFalse { expr: Box::new(lhs), negated };
                        continue;
                    }
                    _ => anyhow::bail!("expected NULL, TRUE, or FALSE after IS"),
                }
            }

            // Handle BETWEEN
            if self.peek() == &Token::Between || (self.peek() == &Token::Not && self.peek2() == &Token::Between) {
                let negated = self.eat(&Token::Not);
                self.expect(&Token::Between)?;
                let low = self.parse_expr(r_bp)?;
                // expect AND
                if self.peek() == &Token::And { self.advance(); }
                let high = self.parse_expr(r_bp)?;
                lhs = Expr::Between { expr: Box::new(lhs), low: Box::new(low), high: Box::new(high), negated };
                continue;
            }

            // Handle LIKE
            if self.peek() == &Token::Like || (self.peek() == &Token::Not && self.peek2() == &Token::Like) {
                let negated = self.eat(&Token::Not);
                self.expect(&Token::Like)?;
                let pattern = self.parse_expr(r_bp)?;
                lhs = Expr::Like { expr: Box::new(lhs), pattern: Box::new(pattern), negated };
                continue;
            }

            // Handle IN (...)
            if self.peek() == &Token::In || (self.peek() == &Token::Not && self.peek2() == &Token::In) {
                let negated = self.eat(&Token::Not);
                self.expect(&Token::In)?;
                self.expect(&Token::LParen)?;
                let list = self.parse_expr_list()?;
                self.expect(&Token::RParen)?;
                lhs = Expr::In { expr: Box::new(lhs), list, negated };
                continue;
            }

            self.advance(); // consume operator token
            let rhs = self.parse_expr(r_bp)?;
            lhs = Expr::BinaryOp { op, lhs: Box::new(lhs), rhs: Box::new(rhs) };

            // Handle CAST with ::
            // (already in parse_postfix via DoubleColon)
        }

        Ok(lhs)
    }

    fn parse_unary(&mut self) -> anyhow::Result<Expr> {
        match self.peek().clone() {
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp { op: UnOp::Neg, expr: Box::new(expr) })
            }
            Token::Plus => {
                self.advance();
                self.parse_unary()
            }
            Token::Not => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp { op: UnOp::Not, expr: Box::new(expr) })
            }
            _ => self.parse_postfix(),
        }
    }

    fn parse_postfix(&mut self) -> anyhow::Result<Expr> {
        let mut expr = self.parse_atom()?;

        loop {
            match self.peek().clone() {
                // Type cast: expr::type
                Token::DoubleColon => {
                    self.advance();
                    let ty = self.parse_ident_string()?;
                    expr = Expr::Cast { expr: Box::new(expr), ty };
                }
                // Field access on JSON: expr->field or expr->>field (already a BinOp)
                _ => break,
            }
        }

        Ok(expr)
    }

    fn parse_atom(&mut self) -> anyhow::Result<Expr> {
        match self.peek().clone() {
            Token::IntLiteral(n) => {
                self.advance();
                Ok(Expr::Literal(Literal::Int(n)))
            }
            Token::FloatLiteral(f) => {
                self.advance();
                Ok(Expr::Literal(Literal::Float(f)))
            }
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Expr::Literal(Literal::Text(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Bool(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Bool(false)))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }
            Token::Dollar(n) => {
                self.advance();
                Ok(Expr::Param(n))
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr(0)?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            // CASE expression
            Token::Case => {
                self.advance();
                self.parse_case()
            }
            // CAST(expr AS type)
            Token::Cast => {
                self.advance();
                self.expect(&Token::LParen)?;
                let inner = self.parse_expr(0)?;
                // expect AS
                if let Token::As = self.peek() { self.advance(); }
                let ty = self.parse_ident_string()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Cast { expr: Box::new(inner), ty })
            }
            // Identifier or function call
            Token::Ident(name) => {
                self.advance();
                let name = name.clone();

                // Function call
                if self.eat(&Token::LParen) {
                    let distinct = self.eat(&Token::Distinct);
                    let args = if self.peek() == &Token::Star {
                        self.advance();
                        vec![]
                    } else if self.peek() == &Token::RParen {
                        vec![]
                    } else {
                        self.parse_expr_list()?
                    };
                    self.expect(&Token::RParen)?;
                    return Ok(Expr::Function { name, args, distinct });
                }

                // Qualified ident: table.column
                if self.eat(&Token::Dot) {
                    let col = self.parse_ident_string()?;
                    return Ok(Expr::QualifiedIdent(name, col));
                }

                Ok(Expr::Ident(name))
            }
            other => anyhow::bail!("unexpected token in expression: {:?}", other),
        }
    }

    fn parse_case(&mut self) -> anyhow::Result<Expr> {
        let operand = if self.peek() != &Token::When {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        let mut branches = Vec::new();
        while self.eat(&Token::When) {
            let cond = self.parse_expr(0)?;
            self.expect(&Token::Then)?;
            let result = self.parse_expr(0)?;
            branches.push((cond, result));
        }

        let else_ = if self.eat(&Token::Else) {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        self.expect(&Token::End)?;
        Ok(Expr::Case { operand, branches, else_ })
    }
}

/// Returns (left_bp, right_bp, BinOp) for infix operators, or None if not infix.
fn infix_bp(tok: &Token) -> Option<(u8, u8, BinOp)> {
    let (l, r, op) = match tok {
        Token::Or    => (1, 2, BinOp::Or),
        Token::And   => (3, 4, BinOp::And),
        Token::Eq    => (5, 6, BinOp::Eq),
        Token::NotEq => (5, 6, BinOp::NotEq),
        Token::Lt    => (5, 6, BinOp::Lt),
        Token::Lte   => (5, 6, BinOp::Lte),
        Token::Gt    => (5, 6, BinOp::Gt),
        Token::Gte   => (5, 6, BinOp::Gte),
        Token::Concat   => (7, 8, BinOp::Concat),
        Token::Plus  => (9, 10, BinOp::Add),
        Token::Minus => (9, 10, BinOp::Sub),
        Token::Star  => (11, 12, BinOp::Mul),
        Token::Slash => (11, 12, BinOp::Div),
        Token::Percent => (11, 12, BinOp::Mod),
        Token::Arrow     => (13, 14, BinOp::Arrow),
        Token::LongArrow => (13, 14, BinOp::LongArrow),
        // IS, BETWEEN, LIKE, IN are handled as special cases in parse_expr
        Token::Is | Token::Between | Token::Like | Token::In | Token::Not => (5, 6, BinOp::Eq),
        _ => return None,
    };
    Some((l, r, op))
}
