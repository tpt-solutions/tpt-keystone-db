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
        while self.eat(&Token::Semicolon) {}

        let stmt = match self.peek().clone() {
            Token::With => {
                self.advance();
                let recursive = self.eat(&Token::Recursive);
                let ctes = self.parse_ctes(recursive)?;
                let mut select = self.parse_select()?;
                select.ctes = ctes;
                Ok(Stmt::Select(select))
            }
            Token::Select | Token::Distinct => Ok(Stmt::Select(self.parse_select()?)),
            Token::Insert => {
                self.advance();
                self.parse_insert()
            }
            Token::Delete => {
                self.advance();
                self.parse_delete()
            }
            Token::Update => {
                self.advance();
                self.parse_update()
            }
            Token::Create => {
                self.advance();
                self.parse_create()
            }
            Token::Drop => {
                self.advance();
                self.parse_drop()
            }
            Token::Alter => {
                self.advance();
                self.parse_alter_table()
            }
            Token::Set => {
                self.advance();
                self.parse_set()
            }
            Token::Show => {
                self.advance();
                self.parse_show()
            }
            Token::Begin => {
                self.advance();
                Ok(Stmt::Begin)
            }
            Token::Commit => {
                self.advance();
                Ok(Stmt::Commit)
            }
            Token::Rollback => {
                self.advance();
                Ok(Stmt::Rollback)
            }
            Token::Declare => {
                self.advance();
                self.parse_declare_cursor()
            }
            Token::Fetch => {
                self.advance();
                Ok(Stmt::Fetch(self.parse_fetch_stmt()?))
            }
            Token::Move => {
                self.advance();
                Ok(Stmt::MoveCursor(self.parse_fetch_stmt()?))
            }
            Token::Close => {
                self.advance();
                Ok(Stmt::CloseCursor(self.parse_ident_string()?))
            }
            Token::Listen => {
                self.advance();
                Ok(Stmt::Listen(self.parse_ident_string()?))
            }
            Token::Unlisten => {
                self.advance();
                let channel = if self.eat(&Token::Star) {
                    "*".to_string()
                } else {
                    self.parse_ident_string()?
                };
                Ok(Stmt::Unlisten(channel))
            }
            Token::Notify => {
                self.advance();
                let channel = self.parse_ident_string()?;
                let payload = if self.eat(&Token::Comma) {
                    match self.advance().clone() {
                        Token::StringLiteral(s) => Some(s),
                        other => anyhow::bail!("expected string literal payload, got {:?}", other),
                    }
                } else {
                    None
                };
                Ok(Stmt::Notify(channel, payload))
            }
            Token::Copy => {
                self.advance();
                self.parse_copy()
            }
            Token::Analyze => {
                self.advance();
                if matches!(self.peek(), Token::Eof | Token::Semicolon) {
                    Ok(Stmt::Analyze(None))
                } else {
                    Ok(Stmt::Analyze(Some(self.parse_object_name()?)))
                }
            }
            Token::Match => {
                self.advance();
                self.parse_match()
            }
            Token::Eof => anyhow::bail!("empty statement"),
            other => anyhow::bail!("unexpected token: {:?}", other),
        }?;

        self.eat(&Token::Semicolon);
        Ok(stmt)
    }

    fn parse_ctes(&mut self, recursive: bool) -> anyhow::Result<Vec<Cte>> {
        let mut ctes = vec![self.parse_cte(recursive)?];
        while self.eat(&Token::Comma) {
            ctes.push(self.parse_cte(recursive)?);
        }
        Ok(ctes)
    }

    fn parse_cte(&mut self, recursive: bool) -> anyhow::Result<Cte> {
        let name = self.parse_ident_string()?;
        let columns = if self.eat(&Token::LParen) {
            let mut cols = vec![self.parse_ident_string()?];
            while self.eat(&Token::Comma) {
                cols.push(self.parse_ident_string()?);
            }
            self.expect(&Token::RParen)?;
            cols
        } else {
            vec![]
        };
        self.expect(&Token::As)?;
        self.expect(&Token::LParen)?;
        let subquery = self.parse_select()?;
        self.expect(&Token::RParen)?;
        Ok(Cte {
            name,
            columns,
            subquery,
            recursive,
        })
    }

    fn parse_insert(&mut self) -> anyhow::Result<Stmt> {
        self.expect(&Token::Into)?;
        let table = self.parse_object_name()?;

        let columns = if self.eat(&Token::LParen) {
            let mut cols = vec![self.parse_ident_string()?];
            while self.eat(&Token::Comma) {
                cols.push(self.parse_ident_string()?);
            }
            self.expect(&Token::RParen)?;
            cols
        } else {
            vec![]
        };

        self.expect(&Token::Values)?;
        self.expect(&Token::LParen)?;
        let mut first_values = vec![self.parse_expr(0)?];
        while self.eat(&Token::Comma) {
            first_values.push(self.parse_expr(0)?);
        }
        self.expect(&Token::RParen)?;

        let mut values = vec![first_values];
        while self.peek() == &Token::Comma && self.peek2() == &Token::LParen {
            self.advance();
            self.expect(&Token::LParen)?;
            let mut row = vec![self.parse_expr(0)?];
            while self.eat(&Token::Comma) {
                row.push(self.parse_expr(0)?);
            }
            self.expect(&Token::RParen)?;
            values.push(row);
        }

        Ok(Stmt::Insert(InsertStmt {
            table,
            columns,
            values,
        }))
    }

    fn parse_delete(&mut self) -> anyhow::Result<Stmt> {
        self.expect(&Token::From)?;
        let table = self.parse_object_name()?;
        let where_ = if self.eat(&Token::Where) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        Ok(Stmt::Delete(DeleteStmt { table, where_ }))
    }

    fn parse_update(&mut self) -> anyhow::Result<Stmt> {
        let table = self.parse_object_name()?;
        self.expect(&Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let col = self.parse_ident_string()?;
            self.expect(&Token::Eq)?;
            let expr = self.parse_expr(0)?;
            assignments.push((col, expr));
            if !self.eat(&Token::Comma) {
                break;
            }
        }

        let where_ = if self.eat(&Token::Where) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        Ok(Stmt::Update(UpdateStmt {
            table,
            assignments,
            where_,
        }))
    }

    fn parse_create(&mut self) -> anyhow::Result<Stmt> {
        match self.peek().clone() {
            Token::Table => {
                self.advance();
                self.parse_create_table()
            }
            Token::Index => {
                self.advance();
                self.parse_create_index()
            }
            Token::Function => {
                self.advance();
                self.parse_create_function()
            }
            Token::Sequence => {
                self.advance();
                self.parse_create_sequence()
            }
            Token::Topic => {
                self.advance();
                self.parse_create_topic()
            }
            other => anyhow::bail!(
                "expected TABLE, INDEX, FUNCTION, SEQUENCE, or TOPIC after CREATE, got {:?}",
                other
            ),
        }
    }

    /// `CREATE FUNCTION name(arg type, ...) RETURNS type LANGUAGE wasm AS '<base64>'`
    fn parse_create_function(&mut self) -> anyhow::Result<Stmt> {
        let name = self.parse_ident_string()?;
        self.expect(&Token::LParen)?;
        let mut args = Vec::new();
        if self.peek() != &Token::RParen {
            loop {
                let arg_name = self.parse_ident_string()?;
                let arg_type = self.parse_ident_string()?;
                args.push((arg_name, arg_type));
                if !self.eat(&Token::Comma) {
                    break;
                }
            }
        }
        self.expect(&Token::RParen)?;
        self.expect(&Token::Returns)?;
        let return_type = self.parse_ident_string()?;
        self.expect(&Token::Language)?;
        let language = self.parse_ident_string()?;
        self.expect(&Token::As)?;
        let body_base64 = match self.advance().clone() {
            Token::StringLiteral(s) => s,
            other => anyhow::bail!("expected string literal function body, got {:?}", other),
        };
        Ok(Stmt::CreateFunction(CreateFunctionStmt {
            name,
            args,
            return_type,
            language,
            body_base64,
        }))
    }

    fn parse_create_table(&mut self) -> anyhow::Result<Stmt> {
        let if_not_exists = if self.peek() == &Token::If && self.peek2() == &Token::Not {
            self.advance();
            self.advance();
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };

        let table = self.parse_object_name()?;
        self.expect(&Token::LParen)?;

        let mut columns: Vec<ColumnDef> = Vec::new();
        let mut table_constraints = Vec::new();

        loop {
            // A leading constraint keyword here means we've reached the
            // table-constraint tail, not another column definition (matters
            // when parsing e.g. `CREATE TABLE t (a int, PRIMARY KEY (a))`).
            if matches!(self.peek(), Token::Primary | Token::Unique | Token::Foreign) {
                break;
            }

            let name = self.parse_ident_string()?;
            let mut col_type = self.parse_ident_string()?;
            let mut nullable = true;
            let mut is_pk = false;
            let mut is_unique = false;
            let mut references = None;
            let mut default = None;

            let is_serial = matches!(
                col_type.to_lowercase().as_str(),
                "serial" | "bigserial" | "smallserial"
            );
            if is_serial {
                col_type = match col_type.to_lowercase().as_str() {
                    "smallserial" => "int2".to_string(),
                    "bigserial" => "int8".to_string(),
                    _ => "int4".to_string(),
                };
                nullable = false;
            }

            loop {
                match self.peek().clone() {
                    Token::Primary => {
                        self.advance();
                        self.expect(&Token::Key)?;
                        is_pk = true;
                        nullable = false;
                    }
                    Token::Not => {
                        self.advance();
                        if self.eat(&Token::Null) {
                            nullable = false;
                        }
                    }
                    Token::Null => {
                        self.advance();
                        nullable = true;
                    }
                    Token::Default => {
                        self.advance();
                        default = Some(self.parse_expr(0)?);
                    }
                    Token::Unique => {
                        self.advance();
                        is_unique = true;
                    }
                    Token::References => {
                        self.advance();
                        let ref_table = self.parse_object_name()?;
                        self.expect(&Token::LParen)?;
                        let ref_column = self.parse_ident_string()?;
                        self.expect(&Token::RParen)?;
                        references = Some((ref_table, ref_column));
                        self.skip_referential_actions();
                    }
                    _ => break,
                }
            }

            columns.push(ColumnDef {
                name,
                col_type,
                nullable,
                default,
                is_pk,
                is_unique,
                references,
                is_serial,
            });

            if !self.eat(&Token::Comma) {
                break;
            }
        }

        // Table-level constraints.
        while !self.eat(&Token::RParen) {
            match self.peek().clone() {
                Token::Primary => {
                    self.advance();
                    self.expect(&Token::Key)?;
                    for col in self.parse_paren_ident_list()? {
                        if let Some(c) = columns.iter_mut().find(|c| c.name == col) {
                            c.is_pk = true;
                            c.nullable = false;
                        }
                    }
                }
                Token::Unique => {
                    self.advance();
                    table_constraints.push(TableConstraint::Unique(self.parse_paren_ident_list()?));
                }
                Token::Foreign => {
                    self.advance();
                    self.expect(&Token::Key)?;
                    let cols = self.parse_paren_ident_list()?;
                    let column = cols.into_iter().next().ok_or_else(|| {
                        anyhow::anyhow!("FOREIGN KEY requires at least one column")
                    })?;
                    self.expect(&Token::References)?;
                    let ref_table = self.parse_object_name()?;
                    self.expect(&Token::LParen)?;
                    let ref_column = self.parse_ident_string()?;
                    self.expect(&Token::RParen)?;
                    self.skip_referential_actions();
                    table_constraints.push(TableConstraint::ForeignKey {
                        column,
                        ref_table,
                        ref_column,
                    });
                }
                Token::Comma => {
                    self.advance();
                }
                Token::RParen => {
                    break;
                }
                // CHECK (...) and anything else unrecognized: skip a
                // balanced-paren group if present, else one token at a time.
                _ => {
                    self.advance();
                    if self.eat(&Token::LParen) {
                        let mut depth = 1;
                        while depth > 0 {
                            match self.advance().clone() {
                                Token::LParen => depth += 1,
                                Token::RParen => depth -= 1,
                                Token::Eof => break,
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        let options = if self.eat(&Token::With) {
            self.expect(&Token::LParen)?;
            let mut opts = vec![self.parse_kv_option()?];
            while self.eat(&Token::Comma) {
                opts.push(self.parse_kv_option()?);
            }
            self.expect(&Token::RParen)?;
            opts
        } else {
            Vec::new()
        };

        Ok(Stmt::CreateTable(CreateTableStmt {
            table,
            if_not_exists,
            columns,
            table_constraints,
            options,
        }))
    }

    /// `ALTER TABLE [ONLY] table ALTER [COLUMN] col SET DEFAULT expr |
    /// DROP DEFAULT | SET NOT NULL | DROP NOT NULL`. `ADD`/`DROP COLUMN` are
    /// accepted syntactically (consistent with the pre-existing
    /// `AlterTableAction` AST shape) but — like before this change — not
    /// actually applied by the executor, since altering row width safely
    /// requires backfilling every existing row; only the column-metadata-only
    /// actions (`SET`/`DROP DEFAULT`, `SET`/`DROP NOT NULL`) are executed.
    fn parse_alter_table(&mut self) -> anyhow::Result<Stmt> {
        self.expect(&Token::Table)?;
        if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("ONLY")) {
            self.advance(); // tolerate pg_dump's `ALTER TABLE ONLY`
        }
        let table = self.parse_object_name()?;

        let action = match self.peek().clone() {
            Token::Ident(ref s) if s.eq_ignore_ascii_case("ADD") => {
                self.advance();
                self.eat(&Token::Column);
                let name = self.parse_ident_string()?;
                let col_type = self.parse_ident_string()?;
                AlterTableAction::AddColumn(ColumnDef {
                    name,
                    col_type,
                    nullable: true,
                    default: None,
                    is_pk: false,
                    is_unique: false,
                    references: None,
                    is_serial: false,
                })
            }
            Token::Drop => {
                self.advance();
                self.eat(&Token::Column);
                AlterTableAction::DropColumn(self.parse_ident_string()?)
            }
            Token::Alter => {
                self.advance();
                self.eat(&Token::Column);
                let name = self.parse_ident_string()?;
                let is_set = if self.eat(&Token::Set) {
                    true
                } else {
                    self.expect(&Token::Drop)?;
                    false
                };
                let action = if is_set {
                    if self.eat(&Token::Default) {
                        ColumnAction::SetDefault(self.parse_expr(0)?)
                    } else {
                        self.expect(&Token::Not)?;
                        self.expect(&Token::Null)?;
                        ColumnAction::SetNotNull
                    }
                } else if self.eat(&Token::Default) {
                    ColumnAction::DropDefault
                } else {
                    self.expect(&Token::Not)?;
                    self.expect(&Token::Null)?;
                    ColumnAction::DropNotNull
                };
                AlterTableAction::AlterColumn { name, action }
            }
            other => anyhow::bail!(
                "expected ADD, DROP, or ALTER after ALTER TABLE {table}, got {:?}",
                other
            ),
        };

        Ok(Stmt::AlterTable(AlterTableStmt { table, action }))
    }

    /// `(ident, ident, ...)`.
    fn parse_paren_ident_list(&mut self) -> anyhow::Result<Vec<String>> {
        self.expect(&Token::LParen)?;
        let mut names = vec![self.parse_ident_string()?];
        while self.eat(&Token::Comma) {
            names.push(self.parse_ident_string()?);
        }
        self.expect(&Token::RParen)?;
        Ok(names)
    }

    /// Consume (and ignore) a trailing `ON DELETE <action>` / `ON UPDATE
    /// <action>` clause after `REFERENCES table(col)`, if present. The
    /// action itself (CASCADE/RESTRICT/SET NULL/...) is not enforced.
    fn skip_referential_actions(&mut self) {
        while self.peek() == &Token::On {
            self.advance(); // ON
            self.advance(); // DELETE | UPDATE
                            // Action: CASCADE | RESTRICT | (SET NULL | SET DEFAULT) | (NO ACTION)
            self.advance();
            if matches!(self.peek(), Token::Null | Token::Default) {
                self.advance();
            }
        }
    }

    /// `CREATE SEQUENCE [IF NOT EXISTS] name [START [WITH] n] [INCREMENT [BY] n]`,
    /// tolerating (and ignoring) any further clauses real `pg_dump` output
    /// emits (`NO MINVALUE`, `CACHE 1`, `OWNED BY ...`, etc.) up to the
    /// statement terminator.
    fn parse_create_sequence(&mut self) -> anyhow::Result<Stmt> {
        if self.peek() == &Token::If && self.peek2() == &Token::Not {
            self.advance();
            self.advance();
            self.expect(&Token::Exists)?;
        }
        let name = self.parse_object_name()?;
        let mut start = 1i64;
        let mut increment = 1i64;

        loop {
            match self.peek().clone() {
                Token::Start => {
                    self.advance();
                    self.eat(&Token::With);
                    start = self.parse_signed_int()?;
                }
                Token::Increment => {
                    self.advance();
                    self.eat(&Token::By);
                    increment = self.parse_signed_int()?;
                }
                Token::Semicolon | Token::Eof => break,
                _ => {
                    self.advance();
                }
            }
        }

        Ok(Stmt::CreateSequence(CreateSequenceStmt {
            name,
            start,
            increment,
        }))
    }

    /// `CREATE TOPIC [IF NOT EXISTS] name [WITH (key = 'value', ...)]`,
    /// mirroring `parse_create_sequence`'s `IF NOT EXISTS` handling and
    /// `parse_create_index`'s generic `WITH (...)` options grammar.
    fn parse_create_topic(&mut self) -> anyhow::Result<Stmt> {
        let if_not_exists = if self.peek() == &Token::If && self.peek2() == &Token::Not {
            self.advance();
            self.advance();
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = self.parse_object_name()?;
        let options = if self.eat(&Token::With) {
            self.expect(&Token::LParen)?;
            let mut opts = vec![self.parse_kv_option()?];
            while self.eat(&Token::Comma) {
                opts.push(self.parse_kv_option()?);
            }
            self.expect(&Token::RParen)?;
            opts
        } else {
            Vec::new()
        };
        Ok(Stmt::CreateTopic(CreateTopicStmt {
            name,
            if_not_exists,
            options,
        }))
    }

    fn parse_signed_int(&mut self) -> anyhow::Result<i64> {
        let neg = self.eat(&Token::Minus);
        match self.advance().clone() {
            Token::IntLiteral(n) => Ok(if neg { -n } else { n }),
            other => anyhow::bail!("expected integer, got {:?}", other),
        }
    }

    fn parse_drop(&mut self) -> anyhow::Result<Stmt> {
        match self.peek().clone() {
            Token::Table => {
                self.advance();
                let if_exists = self.peek() == &Token::If && self.peek2() == &Token::Exists;
                if if_exists {
                    self.advance();
                    self.advance();
                }
                let table = self.parse_object_name()?;
                Ok(Stmt::DropTable(DropTableStmt { table, if_exists }))
            }
            Token::Index => {
                self.advance();
                let _name = self.parse_ident_string()?;
                anyhow::bail!("DROP INDEX not yet supported")
            }
            other => anyhow::bail!("expected TABLE or INDEX after DROP, got {:?}", other),
        }
    }

    fn parse_create_index(&mut self) -> anyhow::Result<Stmt> {
        let _name = if self.peek() == &Token::On {
            None
        } else {
            Some(self.parse_ident_string()?)
        };
        self.expect(&Token::On)?;
        let table = self.parse_object_name()?;
        let using = if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("USING")) {
            self.advance();
            Some(self.parse_ident_string()?)
        } else {
            None
        };
        self.expect(&Token::LParen)?;
        let column = self.parse_ident_string()?;
        self.expect(&Token::RParen)?;
        let options = if self.eat(&Token::With) {
            self.expect(&Token::LParen)?;
            let mut opts = vec![self.parse_kv_option()?];
            while self.eat(&Token::Comma) {
                opts.push(self.parse_kv_option()?);
            }
            self.expect(&Token::RParen)?;
            opts
        } else {
            Vec::new()
        };
        Ok(Stmt::CreateIndex(CreateIndexStmt {
            table,
            column,
            name: _name,
            using,
            options,
        }))
    }

    /// One `key = 'value'` pair inside a `CREATE INDEX ... WITH (...)` or
    /// `CREATE TABLE ... WITH (...)` clause. The key is usually a plain
    /// identifier (`retention`, `value`, `json_schema`), but `interval`
    /// collides with the reserved `INTERVAL` keyword token (and Plexus's
    /// `to`/`type` options with `TO`/`TYPE`), so those are accepted too.
    fn parse_kv_option(&mut self) -> anyhow::Result<(String, String)> {
        let key = match self.advance().clone() {
            Token::Ident(s) => s,
            Token::Interval => "interval".to_string(),
            Token::To => "to".to_string(),
            other => anyhow::bail!("expected an index option name, got {:?}", other),
        };
        self.expect(&Token::Eq)?;
        let value = match self.advance().clone() {
            Token::StringLiteral(s) => s,
            Token::Ident(s) => s,
            other => anyhow::bail!(
                "expected a string literal for index option value, got {:?}",
                other
            ),
        };
        Ok((key, value))
    }

    fn parse_select(&mut self) -> anyhow::Result<SelectStmt> {
        self.expect(&Token::Select)?;
        let distinct = self.eat(&Token::Distinct);
        if !distinct {
            self.eat(&Token::All);
        }
        let projections = self.parse_projection_list()?;
        let from = if self.eat(&Token::From) {
            Some(self.parse_table_with_joins()?)
        } else {
            None
        };
        let where_ = if self.eat(&Token::Where) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        let group_by = if self.peek() == &Token::Group && self.peek2() == &Token::By {
            self.advance();
            self.advance();
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
            self.advance();
            self.advance();
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
        let union = if self.peek() == &Token::Union {
            self.advance();
            let op = if self.eat(&Token::All) {
                UnionOp::UnionAll
            } else {
                UnionOp::Union
            };
            let rhs = self.parse_select()?;
            Some((op, Box::new(rhs)))
        } else {
            None
        };
        Ok(SelectStmt {
            ctes: vec![],
            distinct,
            projections,
            from,
            where_,
            group_by,
            having,
            order_by,
            limit,
            offset,
            union,
        })
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
            // Check for table.*
            if self.eat(&Token::Dot) {
                let table = self.parse_ident_string()?;
                Ok(Projection::WildcardTable(table))
            } else {
                Ok(Projection::Wildcard)
            }
        } else {
            let expr = self.parse_expr(0)?;
            let alias = if self.eat(&Token::As) {
                Some(self.parse_ident_string()?)
            } else if let Token::Ident(_) = self.peek() {
                Some(self.parse_ident_string()?)
            } else {
                None
            };
            Ok(Projection::Expr { expr, alias })
        }
    }

    fn parse_table_with_joins(&mut self) -> anyhow::Result<TableWithJoins> {
        let primary = self.parse_table_ref()?;
        let mut joins = Vec::new();

        while self.peek() == &Token::Join
            || self.peek() == &Token::Left
            || self.peek() == &Token::Right
            || self.peek() == &Token::Full
            || self.peek() == &Token::Cross
        {
            let join_type = match self.peek().clone() {
                Token::Join => {
                    self.advance();
                    JoinType::Inner
                }
                Token::Left => {
                    self.advance();
                    if self.eat(&Token::Join) {
                        JoinType::Left
                    } else {
                        JoinType::Left
                    }
                }
                Token::Right => {
                    self.advance();
                    self.eat(&Token::Join);
                    JoinType::Right
                }
                Token::Full => {
                    self.advance();
                    self.eat(&Token::Join);
                    JoinType::Full
                }
                Token::Cross => {
                    self.advance();
                    self.expect(&Token::Join)?;
                    JoinType::Cross
                }
                _ => break,
            };

            let table = self.parse_table_ref()?;
            let on = if self.eat(&Token::On) {
                Some(self.parse_expr(0)?)
            } else {
                None
            };

            joins.push(Join {
                join_type,
                table,
                on,
            });
        }

        Ok(TableWithJoins { primary, joins })
    }

    fn parse_table_ref(&mut self) -> anyhow::Result<TableRef> {
        if self.peek() == &Token::LParen && self.peek2() == &Token::Select {
            self.advance();
            let subquery = self.parse_select()?;
            self.expect(&Token::RParen)?;
            let alias = if self.eat(&Token::As) {
                self.parse_ident_string()?
            } else {
                self.parse_ident_string()?
            };
            return Ok(TableRef {
                name: alias.clone(),
                alias: Some(alias),
                subquery: Some(Box::new(subquery)),
                func_args: None,
            });
        }
        let first = self.parse_ident_string()?;
        // A table-valued function call (e.g. `graph_bfs('edges', 'from_id',
        // '1', 3)`) — the Plexus graph-traversal SQL surface. Distinguished
        // from a plain table name by an immediately-following `(`.
        if self.peek() == &Token::LParen {
            self.advance();
            let mut args = Vec::new();
            if self.peek() != &Token::RParen {
                loop {
                    args.push(self.parse_expr(0)?);
                    if !self.eat(&Token::Comma) {
                        break;
                    }
                }
            }
            self.expect(&Token::RParen)?;
            let alias = if self.eat(&Token::As) {
                Some(self.parse_ident_string()?)
            } else if let Token::Ident(_) = self.peek() {
                Some(self.parse_ident_string()?)
            } else {
                None
            };
            return Ok(TableRef {
                name: first,
                alias,
                subquery: None,
                func_args: Some(args),
            });
        }
        // Keep the dotted form only for the virtual catalog schemas
        // (`executor::catalog` matches on it directly); a real table's
        // schema qualifier (`public.foo`, as real `pg_dump` output writes)
        // carries no information here — this engine has one implicit schema.
        let name = if self.eat(&Token::Dot) {
            let second = self.parse_ident_string()?;
            if first.eq_ignore_ascii_case("pg_catalog")
                || first.eq_ignore_ascii_case("information_schema")
            {
                format!("{first}.{second}")
            } else {
                second
            }
        } else {
            first
        };
        let alias = if self.eat(&Token::As) {
            Some(self.parse_ident_string()?)
        } else if let Token::Ident(_) = self.peek() {
            Some(self.parse_ident_string()?)
        } else {
            None
        };
        Ok(TableRef {
            name,
            alias,
            subquery: None,
            func_args: None,
        })
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
        let name = self.parse_ident_string()?;
        if !self.eat(&Token::Eq) {
            self.eat(&Token::To);
        }
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
        Ok(Stmt::Set(SetStmt {
            name,
            value: parts.join(" "),
        }))
    }

    fn parse_show(&mut self) -> anyhow::Result<Stmt> {
        let name = self.parse_ident_string()?;
        Ok(Stmt::Show(ShowStmt { name }))
    }

    /// `COPY table [(cols)] FROM STDIN` / `COPY table [(cols)] TO STDOUT`.
    fn parse_copy(&mut self) -> anyhow::Result<Stmt> {
        let table = self.parse_object_name()?;
        let columns = if self.eat(&Token::LParen) {
            let mut cols = vec![self.parse_ident_string()?];
            while self.eat(&Token::Comma) {
                cols.push(self.parse_ident_string()?);
            }
            self.expect(&Token::RParen)?;
            cols
        } else {
            vec![]
        };
        let stmt = if self.eat(&Token::From) {
            self.expect(&Token::Stdin)?;
            Stmt::CopyIn(CopyStmt { table, columns })
        } else if self.eat(&Token::To) {
            self.expect(&Token::Stdout)?;
            Stmt::CopyOut(CopyStmt { table, columns })
        } else {
            anyhow::bail!("expected FROM STDIN or TO STDOUT after COPY table");
        };
        Ok(stmt)
    }

    /// `DECLARE name CURSOR FOR <select>`
    fn parse_declare_cursor(&mut self) -> anyhow::Result<Stmt> {
        let name = self.parse_ident_string()?;
        self.expect(&Token::Cursor)?;
        self.expect(&Token::For)?;
        let query = self.parse_select()?;
        Ok(Stmt::DeclareCursor(DeclareCursorStmt { name, query }))
    }

    /// `FETCH|MOVE [ NEXT | ALL | count ] [ FROM | IN ] cursor`
    fn parse_fetch_stmt(&mut self) -> anyhow::Result<FetchStmt> {
        let count = match self.peek().clone() {
            Token::Next => {
                self.advance();
                FetchCount::Next
            }
            Token::All => {
                self.advance();
                FetchCount::All
            }
            Token::IntLiteral(n) => {
                self.advance();
                FetchCount::Count(n)
            }
            _ => FetchCount::Next,
        };
        self.eat(&Token::From);
        self.eat(&Token::In);
        let cursor = self.parse_ident_string()?;
        Ok(FetchStmt { cursor, count })
    }

    fn parse_ident_string(&mut self) -> anyhow::Result<String> {
        match self.advance().clone() {
            Token::Ident(s) => Ok(s),
            other => anyhow::bail!("expected identifier, got {:?}", other),
        }
    }

    /// `[:REL]` or `[]` (untyped edge) — the relationship-type bracket in a
    /// `MATCH` pattern hop, after the leading `-`/`<-` has already been
    /// consumed by the caller.
    fn parse_match_rel_bracket(&mut self) -> anyhow::Result<Option<String>> {
        self.expect(&Token::LBracket)?;
        let rel_type = if self.eat(&Token::Colon) {
            Some(self.parse_ident_string()?)
        } else {
            None
        };
        self.expect(&Token::RBracket)?;
        Ok(rel_type)
    }

    /// `MATCH (a)-[:REL]->(b)-[:REL2]-(c) ON table(from_column) [WHERE var
    /// = 'lit'] RETURN a, b [LIMIT n]` — see `ast::MatchStmt`'s doc comment
    /// for the honest scope of this GQL subset. `MATCH` itself has already
    /// been consumed by `parse_stmt`.
    fn parse_match(&mut self) -> anyhow::Result<Stmt> {
        self.expect(&Token::LParen)?;
        let mut nodes = vec![self.parse_ident_string()?];
        self.expect(&Token::RParen)?;

        let mut hops = Vec::new();
        loop {
            match self.peek() {
                Token::Minus => {
                    self.advance();
                    let rel_type = self.parse_match_rel_bracket()?;
                    // A closing `-` immediately followed by `>` (no space)
                    // already lexes as one `Arrow` token (same rule the `->`
                    // JSON operator uses), not `Minus` then `Gt` — so an
                    // out-directed hop (`-[...]->`) ends in `Arrow`, while an
                    // undirected hop (`-[...]-`) ends in a bare `Minus`.
                    let direction = if self.eat(&Token::Arrow) {
                        MatchDirection::Out
                    } else {
                        self.expect(&Token::Minus)?;
                        MatchDirection::Both
                    };
                    self.expect(&Token::LParen)?;
                    nodes.push(self.parse_ident_string()?);
                    self.expect(&Token::RParen)?;
                    hops.push(MatchHop { rel_type, direction });
                }
                Token::Lt => {
                    self.advance();
                    self.expect(&Token::Minus)?;
                    let rel_type = self.parse_match_rel_bracket()?;
                    self.expect(&Token::Minus)?;
                    self.expect(&Token::LParen)?;
                    nodes.push(self.parse_ident_string()?);
                    self.expect(&Token::RParen)?;
                    hops.push(MatchHop {
                        rel_type,
                        direction: MatchDirection::In,
                    });
                }
                _ => break,
            }
        }

        self.expect(&Token::On)?;
        let table = self.parse_ident_string()?;
        self.expect(&Token::LParen)?;
        let column = self.parse_ident_string()?;
        self.expect(&Token::RParen)?;

        let start_filter = if self.eat(&Token::Where) {
            let var = self.parse_ident_string()?;
            self.expect(&Token::Eq)?;
            let lit = match self.advance().clone() {
                Token::StringLiteral(s) => s,
                Token::IntLiteral(n) => n.to_string(),
                other => anyhow::bail!(
                    "MATCH WHERE only supports `<var> = '<literal>'`, got {:?}",
                    other
                ),
            };
            anyhow::ensure!(
                var == nodes[0],
                "MATCH WHERE currently only supports filtering the pattern's first variable (\"{}\"), got \"{var}\"",
                nodes[0]
            );
            Some(lit)
        } else {
            None
        };

        self.expect(&Token::Return)?;
        let mut returns = vec![self.parse_ident_string()?];
        while self.eat(&Token::Comma) {
            returns.push(self.parse_ident_string()?);
        }
        for r in &returns {
            anyhow::ensure!(
                nodes.contains(r),
                "MATCH RETURN references unknown pattern variable \"{r}\""
            );
        }

        let limit = if self.eat(&Token::Limit) {
            match self.advance().clone() {
                Token::IntLiteral(n) => Some(n as u64),
                other => anyhow::bail!("expected integer after LIMIT, got {:?}", other),
            }
        } else {
            None
        };

        Ok(Stmt::Match(MatchStmt {
            nodes,
            hops,
            table,
            column,
            start_filter,
            returns,
            limit,
        }))
    }

    /// Parse a DDL/DML object name (table, sequence, ...), discarding a
    /// leading schema qualifier (`public.foo` → `foo`) — this engine has
    /// one implicit schema, but real `pg_dump` output schema-qualifies
    /// almost everything.
    fn parse_object_name(&mut self) -> anyhow::Result<String> {
        let first = self.parse_ident_string()?;
        if self.eat(&Token::Dot) {
            self.parse_ident_string()
        } else {
            Ok(first)
        }
    }

    fn parse_expr_list(&mut self) -> anyhow::Result<Vec<Expr>> {
        let mut list = vec![self.parse_expr(0)?];
        while self.eat(&Token::Comma) {
            list.push(self.parse_expr(0)?);
        }
        Ok(list)
    }

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

            if self.peek() == &Token::Is {
                self.advance();
                let negated = self.eat(&Token::Not);
                match self.peek().clone() {
                    Token::Null => {
                        self.advance();
                        lhs = Expr::IsNull {
                            expr: Box::new(lhs),
                            negated,
                        };
                        continue;
                    }
                    Token::True => {
                        self.advance();
                        lhs = Expr::IsTrue {
                            expr: Box::new(lhs),
                            negated,
                        };
                        continue;
                    }
                    Token::False => {
                        self.advance();
                        lhs = Expr::IsFalse {
                            expr: Box::new(lhs),
                            negated,
                        };
                        continue;
                    }
                    _ => anyhow::bail!("expected NULL, TRUE, or FALSE after IS"),
                }
            }

            if self.peek() == &Token::Between
                || (self.peek() == &Token::Not && self.peek2() == &Token::Between)
            {
                let negated = self.eat(&Token::Not);
                self.expect(&Token::Between)?;
                let low = self.parse_expr(r_bp)?;
                if self.peek() == &Token::And {
                    self.advance();
                }
                let high = self.parse_expr(r_bp)?;
                lhs = Expr::Between {
                    expr: Box::new(lhs),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated,
                };
                continue;
            }

            if self.peek() == &Token::Like
                || (self.peek() == &Token::Not && self.peek2() == &Token::Like)
            {
                let negated = self.eat(&Token::Not);
                self.expect(&Token::Like)?;
                let pattern = self.parse_expr(r_bp)?;
                lhs = Expr::Like {
                    expr: Box::new(lhs),
                    pattern: Box::new(pattern),
                    negated,
                };
                continue;
            }

            if self.peek() == &Token::In
                || (self.peek() == &Token::Not && self.peek2() == &Token::In)
            {
                let negated = self.eat(&Token::Not);
                self.expect(&Token::In)?;
                self.expect(&Token::LParen)?;
                let list = if self.peek() == &Token::Select {
                    let subquery = self.parse_select()?;
                    InList::Subquery(Box::new(subquery))
                } else {
                    InList::Exprs(self.parse_expr_list()?)
                };
                self.expect(&Token::RParen)?;
                lhs = Expr::In {
                    expr: Box::new(lhs),
                    list,
                    negated,
                };
                continue;
            }

            // `COLLATE <name>` — psql appends this to string comparisons in its
            // meta-command queries. A single collation exists here, so the
            // clause is parsed and discarded (no effect on evaluation).
            if self.peek() == &Token::Collate {
                self.advance();
                // Optional schema qualifier (`pg_catalog.default`).
                if matches!(self.peek(), Token::Ident(_)) && self.peek2() == &Token::Dot {
                    self.advance();
                    self.advance();
                }
                if let Token::Ident(_) = self.peek() {
                    self.advance();
                }
                continue;
            }

            // `OPERATOR(schema.opname)` — schema-qualified infix operator
            // syntax (`OPERATOR(pg_catalog.~)`). Treated as a normal infix op.
            if self.peek() == &Token::Operator {
                self.advance();
                self.expect(&Token::LParen)?;
                if matches!(self.peek(), Token::Ident(_)) && self.peek2() == &Token::Dot {
                    self.advance();
                    self.advance();
                }
                let op_tok = self.peek().clone();
                let (l_bp, r_bp, binop) = infix_bp(&op_tok)
                    .ok_or_else(|| anyhow::anyhow!("unknown operator in OPERATOR(...)"))?;
                self.advance();
                self.expect(&Token::RParen)?;
                let rhs = self.parse_expr(r_bp)?;
                lhs = Expr::BinaryOp {
                    op: binop,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                };
                continue;
            }

            self.advance();
            let rhs = self.parse_expr(r_bp)?;
            lhs = Expr::BinaryOp {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_unary(&mut self) -> anyhow::Result<Expr> {
        match self.peek().clone() {
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp {
                    op: UnOp::Neg,
                    expr: Box::new(expr),
                })
            }
            Token::Plus => {
                self.advance();
                self.parse_unary()
            }
            Token::Not if self.peek2() == &Token::Exists => {
                self.advance();
                self.parse_exists(true)
            }
            Token::Not => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp {
                    op: UnOp::Not,
                    expr: Box::new(expr),
                })
            }
            Token::Exists => self.parse_exists(false),
            _ => self.parse_postfix(),
        }
    }

    fn parse_exists(&mut self, negated: bool) -> anyhow::Result<Expr> {
        self.expect(&Token::Exists)?;
        self.expect(&Token::LParen)?;
        let subquery = self.parse_select()?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Exists {
            subquery: Box::new(subquery),
            negated,
        })
    }

    fn parse_postfix(&mut self) -> anyhow::Result<Expr> {
        let mut expr = self.parse_atom()?;
        loop {
            match self.peek().clone() {
                Token::DoubleColon => {
                    self.advance();
                    let ty = self.parse_ident_string()?;
                    expr = Expr::Cast {
                        expr: Box::new(expr),
                        ty,
                    };
                }
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
            Token::LParen if self.peek2() == &Token::Select => {
                self.advance();
                let subquery = self.parse_select()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Subquery(Box::new(subquery)))
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr(0)?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Token::Case => {
                self.advance();
                self.parse_case()
            }
            Token::Cast => {
                self.advance();
                self.expect(&Token::LParen)?;
                let inner = self.parse_expr(0)?;
                if let Token::As = self.peek() {
                    self.advance();
                }
                let ty = self.parse_ident_string()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Cast {
                    expr: Box::new(inner),
                    ty,
                })
            }
            Token::Ident(name) => {
                self.advance();
                let name = name.clone();
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
                    if self.peek() == &Token::Over {
                        return self.parse_over_clause(name, args);
                    }
                    return Ok(Expr::Function {
                        name,
                        args,
                        distinct,
                    });
                }
                if self.eat(&Token::Dot) {
                    let col = self.parse_ident_string()?;
                    // A schema-qualified name directly followed by `(` is a
                    // function call (`pg_catalog.pg_get_userbyid(...)`), not a
                    // column reference. Keep the qualifier in the function name
                    // so the evaluator can match it.
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
                        if self.peek() == &Token::Over {
                            return self.parse_over_clause(format!("{name}.{col}"), args);
                        }
                        return Ok(Expr::Function {
                            name: format!("{name}.{col}"),
                            args,
                            distinct,
                        });
                    }
                    return Ok(Expr::QualifiedIdent(name, col));
                }
                Ok(Expr::Ident(name))
            }
            other => anyhow::bail!("unexpected token in expression: {:?}", other),
        }
    }

    fn parse_over_clause(&mut self, func: String, args: Vec<Expr>) -> anyhow::Result<Expr> {
        self.expect(&Token::Over)?;
        self.expect(&Token::LParen)?;
        let partition_by = if self.peek() == &Token::Partition {
            self.advance();
            self.expect(&Token::By)?;
            self.parse_expr_list()?
        } else {
            vec![]
        };
        let order_by = if self.peek() == &Token::Order && self.peek2() == &Token::By {
            self.advance();
            self.advance();
            self.parse_order_by_list()?
        } else {
            vec![]
        };
        let frame = if self.peek() == &Token::Rows || self.peek() == &Token::Range {
            Some(self.parse_window_frame()?)
        } else {
            None
        };
        self.expect(&Token::RParen)?;
        Ok(Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        })
    }

    fn parse_window_frame(&mut self) -> anyhow::Result<WindowFrame> {
        let frame_type = match self.advance().clone() {
            Token::Rows => FrameType::Rows,
            Token::Range => FrameType::Range,
            other => anyhow::bail!("expected ROWS or RANGE, got {:?}", other),
        };
        if self.eat(&Token::Between) {
            let start = self.parse_frame_bound()?;
            self.expect(&Token::And)?;
            let end = self.parse_frame_bound()?;
            Ok(WindowFrame {
                frame_type,
                start,
                end: Some(end),
            })
        } else {
            let start = self.parse_frame_bound()?;
            Ok(WindowFrame {
                frame_type,
                start,
                end: None,
            })
        }
    }

    fn parse_frame_bound(&mut self) -> anyhow::Result<FrameBound> {
        match self.peek().clone() {
            Token::Unbounded => {
                self.advance();
                match self.advance().clone() {
                    Token::Preceding => Ok(FrameBound {
                        bound_type: FrameBoundType::UnboundedPreceding,
                        offset: None,
                    }),
                    Token::Following => Ok(FrameBound {
                        bound_type: FrameBoundType::UnboundedFollowing,
                        offset: None,
                    }),
                    other => anyhow::bail!("expected PRECEDING or FOLLOWING, got {:?}", other),
                }
            }
            Token::Current => {
                self.advance();
                self.expect(&Token::Row)?;
                Ok(FrameBound {
                    bound_type: FrameBoundType::CurrentRow,
                    offset: None,
                })
            }
            _ => {
                let offset = self.parse_expr(0)?;
                match self.advance().clone() {
                    Token::Preceding => Ok(FrameBound {
                        bound_type: FrameBoundType::Preceding,
                        offset: Some(Box::new(offset)),
                    }),
                    Token::Following => Ok(FrameBound {
                        bound_type: FrameBoundType::Following,
                        offset: Some(Box::new(offset)),
                    }),
                    other => anyhow::bail!("expected PRECEDING or FOLLOWING, got {:?}", other),
                }
            }
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
        Ok(Expr::Case {
            operand,
            branches,
            else_,
        })
    }
}

fn infix_bp(tok: &Token) -> Option<(u8, u8, BinOp)> {
    let (l, r, op) = match tok {
        Token::Or => (1, 2, BinOp::Or),
        Token::And => (3, 4, BinOp::And),
        Token::Eq => (5, 6, BinOp::Eq),
        Token::NotEq => (5, 6, BinOp::NotEq),
        Token::Lt => (5, 6, BinOp::Lt),
        Token::Lte => (5, 6, BinOp::Lte),
        Token::Gt => (5, 6, BinOp::Gt),
        Token::Gte => (5, 6, BinOp::Gte),
        Token::Concat => (7, 8, BinOp::Concat),
        Token::Plus => (9, 10, BinOp::Add),
        Token::Minus => (9, 10, BinOp::Sub),
        Token::Star => (11, 12, BinOp::Mul),
        Token::Slash => (11, 12, BinOp::Div),
        Token::Percent => (11, 12, BinOp::Mod),
        Token::Arrow => (13, 14, BinOp::Arrow),
        Token::LongArrow => (13, 14, BinOp::LongArrow),
        Token::HashArrow => (13, 14, BinOp::HashArrow),
        Token::HashLongArrow => (13, 14, BinOp::HashLongArrow),
        Token::AtArrow => (5, 6, BinOp::Contains),
        Token::Tilde => (5, 6, BinOp::RegexMatch),
        Token::NotTilde => (5, 6, BinOp::RegexNotMatch),
        Token::TildeStar => (5, 6, BinOp::RegexMatchCI),
        Token::NotTildeStar => (5, 6, BinOp::RegexNotMatchCI),
        Token::Is | Token::Between | Token::Like | Token::In | Token::Not => (5, 6, BinOp::Eq),
        Token::Collate | Token::Operator => (5, 6, BinOp::Eq),
        _ => return None,
    };
    Some((l, r, op))
}
