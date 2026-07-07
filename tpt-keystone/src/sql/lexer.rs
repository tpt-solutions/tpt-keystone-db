/// SQL tokens produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Literals
    IntLiteral(i64),
    FloatLiteral(f64),
    StringLiteral(String),

    // Keywords
    Select, From, Where, As, And, Or, Not, Null, True, False,
    Set, Show, Begin, Commit, Rollback, Is, In, Like, Between,
    Limit, Offset, Order, By, Asc, Desc, Group, Having, Distinct, All,
    Case, When, Then, Else, End, Cast, Interval,
    Insert, Into, Values, Delete, Update, Create, Table, Drop, Index,
    If, Exists, Primary, Key, Default,
    Join, Left, Right, Full, Cross, On,
    With, Recursive, Union,
    Over, Partition, Rows, Range, Unbounded, Preceding, Following, Current, Row,
    Declare, Cursor, For, Fetch, Next, Move, Close,
    Listen, Notify, Unlisten,
    Copy, Stdin, Stdout, To,
    Function, Returns, Language,
    Alter, Column, Unique, References, Foreign, Sequence, Increment, Start,
    /// `CREATE TOPIC` (Flux, Phase 11).
    Topic,

    // Identifiers
    Ident(String),

    // Operators & punctuation
    Star, Plus, Minus, Slash, Percent, Eq, NotEq, Lt, Lte, Gt, Gte,
    Concat, Arrow, LongArrow, Comma, Dot, Semicolon, LParen, RParen,
    LBracket, RBracket, Colon, DoubleColon, Dollar(u32),
    /// `@>` — JSON/JSONB containment (Canopy, Phase 10).
    AtArrow,
    /// `#>` / `#>>` — JSON path extraction (object/array result vs text
    /// result), the array-path-literal counterpart of `->`/`->>`.
    HashArrow, HashLongArrow,

    Eof,
}

pub struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str) -> Self {
        Self { src: src.as_bytes(), pos: 0 }
    }

    pub fn tokenize(mut self) -> anyhow::Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            let done = tok == Token::Eof;
            tokens.push(tok);
            if done {
                break;
            }
        }
        Ok(tokens)
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn peek2(&self) -> Option<u8> {
        self.src.get(self.pos + 1).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let b = self.src.get(self.pos).copied();
        if b.is_some() {
            self.pos += 1;
        }
        b
    }

    fn skip_whitespace(&mut self) {
        while let Some(b) = self.peek() {
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else if b == b'-' && self.peek2() == Some(b'-') {
                while let Some(b) = self.advance() {
                    if b == b'\n' { break; }
                }
            } else if b == b'/' && self.peek2() == Some(b'*') {
                self.pos += 2;
                loop {
                    match self.advance() {
                        None => break,
                        Some(b'*') if self.peek() == Some(b'/') => { self.pos += 1; break; }
                        _ => {}
                    }
                }
            } else {
                break;
            }
        }
    }

    fn next_token(&mut self) -> anyhow::Result<Token> {
        self.skip_whitespace();

        let b = match self.peek() {
            None => return Ok(Token::Eof),
            Some(b) => b,
        };

        if b == b'\'' { return self.read_string(); }
        if b == b'"' { return self.read_quoted_ident(); }
        if b.is_ascii_digit() || (b == b'.' && self.peek2().map_or(false, |c| c.is_ascii_digit())) {
            return self.read_number();
        }
        if b == b'$' && self.peek2().map_or(false, |c| c.is_ascii_digit()) {
            self.pos += 1;
            let n = self.read_digits();
            return Ok(Token::Dollar(n.parse().unwrap_or(0)));
        }
        if b.is_ascii_alphabetic() || b == b'_' {
            return self.read_ident_or_keyword();
        }

        self.pos += 1;
        let tok = match b {
            b'*' => Token::Star, b'+' => Token::Plus, b'%' => Token::Percent,
            b',' => Token::Comma, b'.' => Token::Dot, b';' => Token::Semicolon,
            b'(' => Token::LParen, b')' => Token::RParen,
            b'[' => Token::LBracket, b']' => Token::RBracket,
            b'/' => Token::Slash, b'=' => Token::Eq,
            b'!' if self.peek() == Some(b'=') => { self.pos += 1; Token::NotEq }
            b'<' => {
                if self.peek() == Some(b'=') { self.pos += 1; Token::Lte }
                else if self.peek() == Some(b'>') { self.pos += 1; Token::NotEq }
                else { Token::Lt }
            }
            b'>' => {
                if self.peek() == Some(b'=') { self.pos += 1; Token::Gte }
                else { Token::Gt }
            }
            b'-' => {
                if self.peek() == Some(b'>') {
                    self.pos += 1;
                    if self.peek() == Some(b'>') { self.pos += 1; Token::LongArrow }
                    else { Token::Arrow }
                } else { Token::Minus }
            }
            b'|' if self.peek() == Some(b'|') => { self.pos += 1; Token::Concat }
            b':' => {
                if self.peek() == Some(b':') { self.pos += 1; Token::DoubleColon }
                else { Token::Colon }
            }
            b'@' if self.peek() == Some(b'>') => { self.pos += 1; Token::AtArrow }
            b'#' if self.peek() == Some(b'>') => {
                self.pos += 1;
                if self.peek() == Some(b'>') { self.pos += 1; Token::HashLongArrow }
                else { Token::HashArrow }
            }
            other => anyhow::bail!("unexpected character: {}", other as char),
        };
        Ok(tok)
    }

    fn read_string(&mut self) -> anyhow::Result<Token> {
        self.pos += 1;
        let mut s = String::new();
        loop {
            match self.advance() {
                None => anyhow::bail!("unterminated string literal"),
                Some(b'\'') => {
                    if self.peek() == Some(b'\'') { self.pos += 1; s.push('\''); }
                    else { break; }
                }
                Some(b) => s.push(b as char),
            }
        }
        Ok(Token::StringLiteral(s))
    }

    fn read_quoted_ident(&mut self) -> anyhow::Result<Token> {
        self.pos += 1;
        let mut s = String::new();
        loop {
            match self.advance() {
                None => anyhow::bail!("unterminated quoted identifier"),
                Some(b'"') => {
                    if self.peek() == Some(b'"') { self.pos += 1; s.push('"'); }
                    else { break; }
                }
                Some(b) => s.push(b as char),
            }
        }
        Ok(Token::Ident(s))
    }

    fn read_number(&mut self) -> anyhow::Result<Token> {
        let start = self.pos;
        let mut has_dot = false;
        let mut has_exp = false;
        while let Some(b) = self.peek() {
            if b.is_ascii_digit() { self.pos += 1; }
            else if b == b'.' && !has_dot && !has_exp { has_dot = true; self.pos += 1; }
            else if (b == b'e' || b == b'E') && !has_exp {
                has_exp = true; self.pos += 1;
                if self.peek() == Some(b'+') || self.peek() == Some(b'-') { self.pos += 1; }
            } else { break; }
        }
        let s = std::str::from_utf8(&self.src[start..self.pos])?;
        if has_dot || has_exp { Ok(Token::FloatLiteral(s.parse()?)) }
        else { Ok(Token::IntLiteral(s.parse()?)) }
    }

    fn read_digits(&mut self) -> String {
        let start = self.pos;
        while self.peek().map_or(false, |b| b.is_ascii_digit()) { self.pos += 1; }
        String::from_utf8_lossy(&self.src[start..self.pos]).into_owned()
    }

    fn read_ident_or_keyword(&mut self) -> anyhow::Result<Token> {
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b.is_ascii_alphanumeric() || b == b'_' || b == b'$' { self.pos += 1; }
            else { break; }
        }
        let s = std::str::from_utf8(&self.src[start..self.pos])?;
        Ok(keyword_or_ident(s))
    }
}

fn keyword_or_ident(s: &str) -> Token {
    match s.to_uppercase().as_str() {
        "SELECT" => Token::Select, "FROM" => Token::From, "WHERE" => Token::Where,
        "AS" => Token::As, "AND" => Token::And, "OR" => Token::Or, "NOT" => Token::Not,
        "NULL" => Token::Null, "TRUE" => Token::True, "FALSE" => Token::False,
        "SET" => Token::Set, "SHOW" => Token::Show,
        "BEGIN" => Token::Begin, "COMMIT" => Token::Commit, "ROLLBACK" => Token::Rollback,
        "IS" => Token::Is, "IN" => Token::In, "LIKE" => Token::Like, "BETWEEN" => Token::Between,
        "LIMIT" => Token::Limit, "OFFSET" => Token::Offset,
        "ORDER" => Token::Order, "BY" => Token::By, "ASC" => Token::Asc, "DESC" => Token::Desc,
        "GROUP" => Token::Group, "HAVING" => Token::Having,
        "DISTINCT" => Token::Distinct, "ALL" => Token::All,
        "CASE" => Token::Case, "WHEN" => Token::When, "THEN" => Token::Then,
        "ELSE" => Token::Else, "END" => Token::End, "CAST" => Token::Cast, "INTERVAL" => Token::Interval,
        "INSERT" => Token::Insert, "INTO" => Token::Into, "VALUES" => Token::Values,
        "DELETE" => Token::Delete, "UPDATE" => Token::Update,
        "CREATE" => Token::Create, "TABLE" => Token::Table, "DROP" => Token::Drop, "INDEX" => Token::Index,
        "IF" => Token::If, "EXISTS" => Token::Exists,
        "PRIMARY" => Token::Primary, "KEY" => Token::Key, "DEFAULT" => Token::Default,
        "JOIN" => Token::Join, "LEFT" => Token::Left, "RIGHT" => Token::Right, "FULL" => Token::Full,
        "CROSS" => Token::Cross, "ON" => Token::On,
        "WITH" => Token::With, "RECURSIVE" => Token::Recursive, "UNION" => Token::Union,
        "OVER" => Token::Over, "PARTITION" => Token::Partition,
        "ROWS" => Token::Rows, "RANGE" => Token::Range, "UNBOUNDED" => Token::Unbounded,
        "PRECEDING" => Token::Preceding, "FOLLOWING" => Token::Following,
        "CURRENT" => Token::Current, "ROW" => Token::Row,
        "DECLARE" => Token::Declare, "CURSOR" => Token::Cursor, "FOR" => Token::For,
        "FETCH" => Token::Fetch, "NEXT" => Token::Next, "MOVE" => Token::Move,
        "CLOSE" => Token::Close,
        "LISTEN" => Token::Listen, "NOTIFY" => Token::Notify, "UNLISTEN" => Token::Unlisten,
        "COPY" => Token::Copy, "STDIN" => Token::Stdin, "STDOUT" => Token::Stdout, "TO" => Token::To,
        "FUNCTION" => Token::Function, "RETURNS" => Token::Returns, "LANGUAGE" => Token::Language,
        "ALTER" => Token::Alter, "COLUMN" => Token::Column, "UNIQUE" => Token::Unique,
        "REFERENCES" => Token::References, "FOREIGN" => Token::Foreign, "SEQUENCE" => Token::Sequence,
        "INCREMENT" => Token::Increment, "START" => Token::Start, "TOPIC" => Token::Topic,
        _ => Token::Ident(s.to_string()),
    }
}
