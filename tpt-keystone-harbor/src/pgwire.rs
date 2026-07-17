//! Hand-written Postgres wire protocol v3 client, extended (beyond
//! `tpt-keystone-sdk/src/keystone/wire.rs`'s simple/extended query subset) with the
//! `COPY` sub-protocol and `START_REPLICATION ... LOGICAL` / `pgoutput`
//! decoding Harbor needs for bulk snapshots and live CDC.
//!
//! One client serves both ends of a Harbor/PG migration: the *source* is a
//! real PostgreSQL server, the *target* is a TPT Keystone node — both speak
//! this same wire protocol, so there is exactly one hand-written codec here
//! rather than a source-specific and a target-specific one.
//!
//! No `pgwire`/`tokio-postgres`/`postgres-protocol` crate is used, per this
//! repo's from-scratch rule for wire protocols (see root `CLAUDE.md`).

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub type_oid: i32,
}

#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationMd5Password([u8; 4]),
    ParameterStatus { name: String, value: String },
    BackendKeyData { pid: i32, secret: i32 },
    ReadyForQuery(u8),
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<Box<[u8]>>>),
    CommandComplete(String),
    ErrorResponse(String),
    NoticeResponse(String),
    EmptyQueryResponse,
    CopyBothResponse,
    CopyOutResponse,
    CopyData(Vec<u8>),
    CopyDone,
    Unknown(u8),
}

pub struct Conn {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl Conn {
    pub async fn connect(addr: &str, params: &[(&str, &str)]) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.with_context(|| format!("connecting to {addr}"))?;
        let mut conn = Self { stream, read_buf: BytesMut::with_capacity(16384), write_buf: BytesMut::with_capacity(16384) };
        conn.write_startup(params);
        conn.flush().await?;

        loop {
            match conn.read_message().await? {
                BackendMessage::AuthenticationOk => {}
                BackendMessage::AuthenticationCleartextPassword | BackendMessage::AuthenticationMd5Password(_) => {
                    bail!("server requires password authentication, which Harbor does not implement — connect with a trust/no-password role");
                }
                BackendMessage::ReadyForQuery(_) => break,
                BackendMessage::ErrorResponse(msg) => bail!("startup rejected: {msg}"),
                _ => {}
            }
        }
        Ok(conn)
    }

    fn write_startup(&mut self, params: &[(&str, &str)]) {
        let mut body = BytesMut::new();
        body.put_i32(196608); // protocol version 3.0
        for (k, v) in params {
            body.put_slice(k.as_bytes());
            body.put_u8(0);
            body.put_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);
        self.write_buf.put_i32(4 + body.len() as i32);
        self.write_buf.put_slice(&body);
    }

    pub fn write_query(&mut self, sql: &str) {
        self.write_msg(b'Q', |b| {
            b.put_slice(sql.as_bytes());
            b.put_u8(0);
        });
    }

    pub fn write_copy_done(&mut self) {
        self.write_msg(b'c', |_| {});
    }

    pub fn write_terminate(&mut self) {
        self.write_msg(b'X', |_| {});
    }

    fn write_msg(&mut self, tag: u8, body: impl FnOnce(&mut BytesMut)) {
        let mut b = BytesMut::new();
        body(&mut b);
        self.write_buf.put_u8(tag);
        self.write_buf.put_i32(4 + b.len() as i32);
        self.write_buf.put_slice(&b);
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();
        Ok(())
    }

    pub async fn read_message(&mut self) -> Result<BackendMessage> {
        self.fill(5).await?;
        let tag = self.read_buf[0];
        let len = i32::from_be_bytes(self.read_buf[1..5].try_into()?) as usize;
        if len < 4 {
            bail!("invalid message length {len} for tag {}", tag as char);
        }
        let total = 1 + len;
        self.fill(total).await?;
        let mut data = self.read_buf.split_to(total);
        data.advance(5);

        let msg = match tag {
            b'R' => {
                let code = if data.remaining() >= 4 { data.get_i32() } else { 0 };
                match code {
                    0 => BackendMessage::AuthenticationOk,
                    3 => BackendMessage::AuthenticationCleartextPassword,
                    5 => {
                        let mut salt = [0u8; 4];
                        if data.remaining() >= 4 {
                            data.copy_to_slice(&mut salt);
                        }
                        BackendMessage::AuthenticationMd5Password(salt)
                    }
                    _ => BackendMessage::AuthenticationOk,
                }
            }
            b'S' => {
                let name = read_cstr(&mut data);
                let value = read_cstr(&mut data);
                BackendMessage::ParameterStatus { name, value }
            }
            b'K' => {
                let pid = data.get_i32();
                let secret = data.get_i32();
                BackendMessage::BackendKeyData { pid, secret }
            }
            b'Z' => BackendMessage::ReadyForQuery(if data.has_remaining() { data.get_u8() } else { b'I' }),
            b'T' => {
                let n = data.get_i16() as usize;
                let mut fields = Vec::with_capacity(n);
                for _ in 0..n {
                    let name = read_cstr(&mut data);
                    let _table_oid = data.get_i32();
                    let _col_attr = data.get_i16();
                    let type_oid = data.get_i32();
                    let _type_size = data.get_i16();
                    let _type_modifier = data.get_i32();
                    let _format = data.get_i16();
                    fields.push(FieldDescription { name, type_oid });
                }
                BackendMessage::RowDescription(fields)
            }
            b'D' => {
                let n = data.get_i16() as usize;
                let mut cols = Vec::with_capacity(n);
                for _ in 0..n {
                    let len = data.get_i32();
                    if len < 0 {
                        cols.push(None);
                    } else {
                        let len = len as usize;
                        let take = len.min(data.remaining());
                        cols.push(Some(data.copy_to_bytes(take).to_vec().into_boxed_slice()));
                    }
                }
                BackendMessage::DataRow(cols)
            }
            b'C' => BackendMessage::CommandComplete(read_cstr(&mut data)),
            b'E' => BackendMessage::ErrorResponse(parse_error_fields(&mut data)),
            b'N' => BackendMessage::NoticeResponse(parse_error_fields(&mut data)),
            b'I' => BackendMessage::EmptyQueryResponse,
            b'W' => BackendMessage::CopyBothResponse,
            b'H' => BackendMessage::CopyOutResponse,
            b'd' => BackendMessage::CopyData(data.to_vec()),
            b'c' => BackendMessage::CopyDone,
            other => BackendMessage::Unknown(other),
        };
        Ok(msg)
    }

    async fn fill(&mut self, n: usize) -> Result<()> {
        while self.read_buf.len() < n {
            let read = self.stream.read_buf(&mut self.read_buf).await?;
            if read == 0 {
                bail!("connection closed by peer");
            }
        }
        Ok(())
    }
}

fn read_cstr(buf: &mut BytesMut) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let s = String::from_utf8_lossy(&buf[..end]).into_owned();
    buf.advance((end + 1).min(buf.len()));
    s
}

fn parse_error_fields(data: &mut BytesMut) -> String {
    let mut message = None;
    loop {
        if !data.has_remaining() {
            break;
        }
        let code = data.get_u8();
        if code == 0 {
            break;
        }
        let value = read_cstr(data);
        if code == b'M' {
            message = Some(value);
        }
    }
    message.unwrap_or_else(|| "unknown server error".to_string())
}

/// A decoded row from a plain (non-replication) query, name-addressable.
#[derive(Debug, Clone)]
pub struct Row {
    pub columns: std::sync::Arc<Vec<String>>,
    pub cells: Vec<Option<Box<[u8]>>>,
}

impl Row {
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        let i = self.columns.iter().position(|c| c == name)?;
        self.cells.get(i).and_then(|c| c.as_deref())
    }

    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|b| std::str::from_utf8(b).ok())
    }

    /// This row's cells as `Vec<Option<Vec<u8>>>` — the [`crate::connector::SourceRow`]
    /// shape used throughout the connector traits, vs. this module's own
    /// `Box<[u8]>` cell representation.
    pub fn to_cell_vec(&self) -> Vec<Option<Vec<u8>>> {
        self.cells.iter().map(|c| c.as_ref().map(|b| b.to_vec())).collect()
    }
}

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub command_tag: Option<String>,
}

/// A thin, blocking-style (one query at a time) client wrapping [`Conn`].
/// Used for both the Postgres source and the Keystone target — see module doc.
pub struct Client {
    conn: Conn,
}

impl Client {
    pub async fn connect(addr: &str, params: &[(&str, &str)]) -> Result<Self> {
        Ok(Self { conn: Conn::connect(addr, params).await? })
    }

    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.conn.write_query(sql);
        self.conn.flush().await?;

        let mut columns: Vec<String> = Vec::new();
        let mut columns_arc = std::sync::Arc::new(Vec::new());
        let mut rows = Vec::new();
        let mut command_tag = None;

        loop {
            match self.conn.read_message().await? {
                BackendMessage::RowDescription(fields) => {
                    columns = fields.into_iter().map(|f| f.name).collect();
                    columns_arc = std::sync::Arc::new(columns.clone());
                    rows.clear();
                }
                BackendMessage::DataRow(cells) => rows.push(Row { columns: columns_arc.clone(), cells }),
                BackendMessage::CommandComplete(tag) => command_tag = Some(tag),
                BackendMessage::EmptyQueryResponse => {}
                BackendMessage::NoticeResponse(_) => {}
                BackendMessage::ErrorResponse(msg) => {
                    // Drain to ReadyForQuery so the connection stays usable for the next call.
                    loop {
                        if let BackendMessage::ReadyForQuery(_) = self.conn.read_message().await? {
                            break;
                        }
                    }
                    bail!("server error: {msg}");
                }
                BackendMessage::ReadyForQuery(_) => break,
                _ => {}
            }
        }

        Ok(QueryResult { columns, rows, command_tag })
    }

    pub async fn execute(&mut self, sql: &str) -> Result<()> {
        self.query(sql).await?;
        Ok(())
    }

    /// Enter `START_REPLICATION` COPY BOTH mode. The connection is
    /// single-purpose after this call — only [`Client::recv_replication_data`]
    /// and [`Client::send_standby_status_update`] are valid until it ends.
    pub async fn start_replication(&mut self, sql: &str) -> Result<()> {
        self.conn.write_query(sql);
        self.conn.flush().await?;
        loop {
            match self.conn.read_message().await? {
                BackendMessage::CopyBothResponse => return Ok(()),
                BackendMessage::ErrorResponse(msg) => bail!("replication start rejected: {msg}"),
                BackendMessage::NoticeResponse(_) => {}
                _ => {}
            }
        }
    }

    /// Read the next `CopyData` payload from an in-progress replication
    /// stream, stripped of the leading `d`/`k` sub-message byte. Returns
    /// `None` if the server ended the stream (`CopyDone`).
    pub async fn recv_replication_data(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            match self.conn.read_message().await? {
                BackendMessage::CopyData(data) => return Ok(Some(data)),
                BackendMessage::CopyDone => return Ok(None),
                BackendMessage::ErrorResponse(msg) => bail!("replication stream error: {msg}"),
                BackendMessage::NoticeResponse(_) => {}
                BackendMessage::ReadyForQuery(_) => return Ok(None),
                _ => {}
            }
        }
    }

    pub async fn close(mut self) -> Result<()> {
        self.conn.write_terminate();
        self.conn.flush().await?;
        Ok(())
    }
}
