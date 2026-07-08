//! Embedded/native async Keystone client — "SDK/Rust" section 2 of
//! `9sdkspec.txt`. Talks the same hand-written Postgres wire protocol v3
//! `tpt-keystone/src/wire` implements server-side (see [`wire`]'s module
//! doc); this is a plain TCP client, no `pgwire`/`tokio-postgres` crate.

pub mod blocking;
mod wire;

use crate::zerocopy::RowView;
use wire::{BackendMessage, Conn};

#[derive(Debug, thiserror::Error)]
pub enum KeystoneError {
    #[error("connection error: {0}")]
    Io(#[from] anyhow::Error),
    #[error("server error: {0}")]
    Server(String),
}

/// One decoded row. Cheap to construct — see [`RowView`] for a borrowed
/// alternative that avoids the per-cell `Vec<u8>` this owns.
#[derive(Debug, Clone)]
pub struct Row {
    columns: std::sync::Arc<Vec<String>>,
    cells: Vec<Option<Box<[u8]>>>,
}

impl Row {
    pub fn as_view(&self) -> RowView<'_> {
        RowView::new(&self.cells)
    }

    pub fn column_names(&self) -> &[String] {
        &self.columns
    }

    pub fn get(&self, i: usize) -> Option<&[u8]> {
        self.cells.get(i).and_then(|c| c.as_deref())
    }

    pub fn get_by_name(&self, name: &str) -> Option<&[u8]> {
        let i = self.columns.iter().position(|c| c == name)?;
        self.get(i)
    }

    pub fn get_str(&self, i: usize) -> Option<&str> {
        self.get(i).and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Parse column `i` (text-format wire representation) as [`Value`].
    /// The type OID isn't threaded through here, so this makes a best
    /// effort at the common scalar types rather than a full catalog-aware
    /// decode.
    pub fn get_value(&self, i: usize) -> Value {
        match self.get_str(i) {
            None => Value::Null,
            Some(s) => Value::from_text(s),
        }
    }
}

/// A type-erased scalar value, decoded from the wire's text format.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl Value {
    fn from_text(s: &str) -> Self {
        match s {
            "t" | "true" => Value::Bool(true),
            "f" | "false" => Value::Bool(false),
            _ => {
                if let Ok(i) = s.parse::<i64>() {
                    Value::Int(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    Value::Float(f)
                } else {
                    Value::Text(s.to_string())
                }
            }
        }
    }

    /// Encode a parameter for the extended query protocol's text format.
    pub fn to_param(&self) -> Option<Vec<u8>> {
        match self {
            Value::Null => None,
            Value::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
            Value::Int(i) => Some(i.to_string().into_bytes()),
            Value::Float(f) => Some(f.to_string().into_bytes()),
            Value::Text(s) => Some(s.clone().into_bytes()),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_string())
    }
}
impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}
impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}
impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub command_tag: Option<String>,
}

pub struct KeystoneClient {
    conn: Conn,
}

impl KeystoneClient {
    /// Connect to a Keystone node's Postgres-wire listener, e.g.
    /// `"127.0.0.1:5432"`. `tpt-keystone`'s startup handshake auto-approves
    /// (no auth), so any `user` param is accepted.
    pub async fn connect(addr: &str) -> Result<Self, KeystoneError> {
        Self::connect_with_params(addr, &[("user", "tpt_sdk")]).await
    }

    pub async fn connect_with_params(addr: &str, params: &[(&str, &str)]) -> Result<Self, KeystoneError> {
        let conn = Conn::connect(addr, params).await?;
        Ok(Self { conn })
    }

    /// Run `sql` over the simple query protocol. Supports multi-statement
    /// SQL text but only the last statement's rows are returned, matching
    /// the simple query protocol's semantics (each statement gets its own
    /// `CommandComplete`, only the final `ReadyForQuery` ends the exchange).
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult, KeystoneError> {
        self.conn.write_query(sql);
        self.conn.flush().await?;

        let mut columns: Vec<String> = Vec::new();
        let mut rows = Vec::new();
        let mut command_tag = None;
        let mut columns_arc = std::sync::Arc::new(Vec::new());

        loop {
            match self.conn.read_message().await? {
                BackendMessage::RowDescription(fields) => {
                    columns = fields.into_iter().map(|f| f.name).collect();
                    columns_arc = std::sync::Arc::new(columns.clone());
                    rows.clear();
                }
                BackendMessage::DataRow(cells) => {
                    rows.push(Row { columns: columns_arc.clone(), cells });
                }
                BackendMessage::CommandComplete(tag) => {
                    command_tag = Some(tag);
                }
                BackendMessage::EmptyQueryResponse => {}
                BackendMessage::ErrorResponse(msg) => return Err(KeystoneError::Server(msg)),
                BackendMessage::NoticeResponse(_) => {}
                BackendMessage::ReadyForQuery(_) => break,
                _ => {}
            }
        }

        Ok(QueryResult { columns, rows, command_tag })
    }

    /// Run a parameterized query over the extended query protocol
    /// (Parse/Bind/Describe/Execute/Sync), all params/results in text format.
    pub async fn query_params(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, KeystoneError> {
        let encoded: Vec<Option<Vec<u8>>> = params.iter().map(Value::to_param).collect();

        self.conn.write_parse("", sql, &[]);
        self.conn.write_bind("", "", &encoded);
        self.conn.write_describe_portal("");
        self.conn.write_execute("", 0);
        self.conn.write_sync();
        self.conn.flush().await?;

        let mut columns: Vec<String> = Vec::new();
        let mut columns_arc = std::sync::Arc::new(Vec::new());
        let mut rows = Vec::new();
        let mut command_tag = None;

        loop {
            match self.conn.read_message().await? {
                BackendMessage::ParseComplete
                | BackendMessage::BindComplete
                | BackendMessage::ParameterDescription(_)
                | BackendMessage::NoData => {}
                BackendMessage::RowDescription(fields) => {
                    columns = fields.into_iter().map(|f| f.name).collect();
                    columns_arc = std::sync::Arc::new(columns.clone());
                }
                BackendMessage::DataRow(cells) => {
                    rows.push(Row { columns: columns_arc.clone(), cells });
                }
                BackendMessage::CommandComplete(tag) => command_tag = Some(tag),
                BackendMessage::PortalSuspended => {}
                BackendMessage::ErrorResponse(msg) => {
                    // Drain to ReadyForQuery so the connection stays usable.
                    loop {
                        if let BackendMessage::ReadyForQuery(_) = self.conn.read_message().await? {
                            break;
                        }
                    }
                    return Err(KeystoneError::Server(msg));
                }
                BackendMessage::ReadyForQuery(_) => break,
                _ => {}
            }
        }

        Ok(QueryResult { columns, rows, command_tag })
    }

    pub async fn close(mut self) -> Result<(), KeystoneError> {
        self.conn.write_terminate();
        self.conn.flush().await?;
        Ok(())
    }
}
