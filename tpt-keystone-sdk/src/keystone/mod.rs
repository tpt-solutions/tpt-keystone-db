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
    /// Build a `Row` from owned column names and cell buffers. Handy for
    /// tests and for SDK consumers that decode rows out-of-band (e.g. the
    /// FFI layer) without going through [`KeystoneClient::query`].
    pub fn new(
        columns: impl IntoIterator<Item = impl Into<String>>,
        cells: impl IntoIterator<Item = Option<impl Into<Vec<u8>>>>,
    ) -> Self {
        Row {
            columns: std::sync::Arc::new(columns.into_iter().map(|c| c.into()).collect()),
            cells: cells
                .into_iter()
                .map(|c| c.map(|v| v.into().into_boxed_slice()))
                .collect(),
        }
    }

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

/// Encode one `Value` as a single COPY text-format cell (mirroring
/// `tpt-keystone`'s `executor::copy::encode_copy_line`): `\N` for NULL,
/// `\t`/`\n`/`\r`/`\\` backslash-escaped text, otherwise the same text
/// rendering used on the wire.
fn encode_copy_cell(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.extend_from_slice(b"\\N"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"t" } else { b"f" }),
        Value::Int(i) => out.extend_from_slice(i.to_string().as_bytes()),
        Value::Float(f) => out.extend_from_slice(f.to_string().as_bytes()),
        Value::Text(s) => {
            for c in s.chars() {
                match c {
                    '\t' => out.extend_from_slice(b"\\t"),
                    '\n' => out.extend_from_slice(b"\\n"),
                    '\r' => out.extend_from_slice(b"\\r"),
                    '\\' => out.extend_from_slice(b"\\\\"),
                    _ => {
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    }
                }
            }
        }
    }
}

/// Encode a row as a COPY text-format line: tab-delimited cells,
/// `\n`-terminated — exactly what `COPY table FROM STDIN` expects.
pub fn encode_copy_line(values: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        encode_copy_cell(&mut out, v);
    }
    out.push(b'\n');
    out
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub command_tag: Option<String>,
}

impl QueryResult {
    /// Build a [`QueryResult`]. Mostly useful for tests and for callers
    /// that assemble results without a live [`KeystoneClient`].
    pub fn new(columns: Vec<String>, rows: Vec<Row>, command_tag: Option<String>) -> Self {
        QueryResult { columns, rows, command_tag }
    }
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

    /// Bulk-load `rows` into `table` via the server's `COPY table FROM STDIN`
    /// path — one sub-protocol exchange for the whole batch instead of one
    /// `query`/`query_params` round trip per row. This is the high-throughput
    /// ingest path for time-series / high-frequency pipelines (e.g. a
    /// `tpt-fluxstream`-style consumer) where per-row round trips would
    /// otherwise dominate.
    ///
    /// `columns` lists the target columns in the order the `rows`' cells line
    /// up with; pass an empty slice to use every column in schema order. Each
    /// inner `Vec<Value>` must have one cell per named column.
    ///
    /// Returns the number of rows the server actually committed.
    pub async fn copy_in(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
    ) -> Result<u64, KeystoneError> {
        let col_list = if columns.is_empty() {
            String::new()
        } else {
            let list = columns
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ({list})")
        };
        let sql = format!("COPY {table}{col_list} FROM STDIN");
        self.conn.write_query(&sql);
        self.conn.flush().await?;

        // The server either accepts the COPY (CopyInResponse) or rejects it
        // up front with an ErrorResponse + ReadyForQuery.
        let expected_cols = columns.len();
        loop {
            match self.conn.read_message().await? {
                BackendMessage::CopyInResponse { columns: n } => {
                    if expected_cols != 0 && n != expected_cols {
                        return Err(KeystoneError::Server(format!(
                            "COPY expected {expected_cols} columns but server reported {n}"
                        )));
                    }
                    break;
                }
                BackendMessage::ErrorResponse(msg) => return Err(KeystoneError::Server(msg)),
                BackendMessage::NoticeResponse(_) => {}
                BackendMessage::ReadyForQuery(_) => {
                    return Err(KeystoneError::Server(
                        "COPY was not accepted by the server".into(),
                    ))
                }
                _ => {}
            }
        }

        // Stream every row as a text-format COPY line, then end the copy.
        for row in rows {
            let line = encode_copy_line(row);
            self.conn.write_copy_data(&line);
        }
        self.conn.write_copy_done();
        self.conn.flush().await?;

        // The server replies with CommandComplete ("COPY n") or an
        // ErrorResponse if any buffered row failed, then ReadyForQuery.
        let mut row_count = 0u64;
        loop {
            match self.conn.read_message().await? {
                BackendMessage::CommandComplete(tag) => {
                    if let Some(n) = tag.strip_prefix("COPY ") {
                        if let Ok(c) = n.trim().parse::<u64>() {
                            row_count = c;
                        }
                    }
                }
                BackendMessage::ErrorResponse(msg) => return Err(KeystoneError::Server(msg)),
                BackendMessage::ReadyForQuery(_) => break,
                _ => {}
            }
        }
        Ok(row_count)
    }

    pub async fn close(mut self) -> Result<(), KeystoneError> {
        self.conn.write_terminate();
        self.conn.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_from_text_decodes_common_scalars() {
        assert_eq!(Value::from_text("t"), Value::Bool(true));
        assert_eq!(Value::from_text("false"), Value::Bool(false));
        assert_eq!(Value::from_text("42"), Value::Int(42));
        assert_eq!(Value::from_text("-7"), Value::Int(-7));
        assert_eq!(Value::from_text("3.14"), Value::Float(3.14));
        assert_eq!(Value::from_text("hello"), Value::Text("hello".into()));
    }

    #[test]
    fn value_to_param_round_trips_into_wire_text() {
        assert_eq!(Value::Null.to_param(), None);
        assert_eq!(Value::Bool(true).to_param(), Some(b"t".to_vec()));
        assert_eq!(Value::Bool(false).to_param(), Some(b"f".to_vec()));
        assert_eq!(Value::Int(9).to_param(), Some(b"9".to_vec()));
        assert_eq!(Value::Float(1.5).to_param(), Some(b"1.5".to_vec()));
        assert_eq!(Value::Text("x".into()).to_param(), Some(b"x".to_vec()));
    }

    #[test]
    fn from_impls_build_typed_values() {
        assert_eq!(Value::from("s"), Value::Text("s".into()));
        assert_eq!(Value::from(1i64), Value::Int(1));
        assert_eq!(Value::from(2.0f64), Value::Float(2.0));
        assert_eq!(Value::from(true), Value::Bool(true));
    }

    #[test]
    fn row_accessors_index_and_lookup_by_name() {
        let row = Row::new(
            ["id", "name", "score"],
            [Some(b"1".to_vec()), Some(b"Ada".to_vec()), None],
        );
        assert_eq!(row.get_str(0), Some("1"));
        assert_eq!(row.get_str(1), Some("Ada"));
        assert_eq!(row.get_str(2), None);
        let ada: &[u8] = b"Ada";
        assert_eq!(row.get_by_name("name"), Some(ada));
        assert_eq!(row.get_by_name("missing"), None);
        assert_eq!(row.get_value(0), Value::Int(1));
        assert_eq!(row.get_value(2), Value::Null);
    }

    #[test]
    fn row_as_view_exposes_the_same_cells() {
        let row = Row::new(["id"], [Some(b"7".to_vec())]);
        let view = row.as_view();
        assert_eq!(view.get_str(0), Some("7"));
        assert_eq!(view.to_owned_row(), vec![Some(b"7".to_vec())]);
    }

    #[test]
    fn keystone_error_formats_human_readable() {
        assert_eq!(KeystoneError::Server("boom".into()).to_string(), "server error: boom");
    }

    #[test]
    fn encode_copy_line_escapes_and_round_trips() {
        // Mirrors `tpt-keystone`'s COPY text format: tab-delimited,
        // `\N` for NULL, `\t`/`\n`/`\r`/`\\` escaped.
        let row = vec![
            Value::Int(1),
            Value::Text("Ada".into()),
            Value::Null,
            Value::Text("tab\there\nand\\back".into()),
            Value::Float(2.5),
            Value::Bool(true),
        ];
        let line = String::from_utf8(encode_copy_line(&row)).unwrap();
        assert_eq!(
            line,
            "1\tAda\t\\N\ttab\\there\\nand\\\\back\t2.5\tt\n"
        );
    }

    #[test]
    fn encode_copy_line_single_cell() {
        assert_eq!(
            String::from_utf8(encode_copy_line(&[Value::Text("x".into())])).unwrap(),
            "x\n"
        );
    }
}
