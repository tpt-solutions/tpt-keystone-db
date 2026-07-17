//! Client-side codec for the same hand-written Postgres wire protocol v3
//! `tpt-keystone/src/wire/codec.rs` implements server-side. This is the
//! mirror image: frontend (client -> server) messages are *encoded* here,
//! backend (server -> client) messages are *decoded* here — the reverse of
//! `tpt-keystone`'s `codec.rs`.
//!
//! Only the subset of the protocol this SDK needs is implemented: startup,
//! the simple query loop, and the extended query subset (Parse/Bind/
//! Describe/Execute/Sync) needed for parameterized queries. All formats are
//! text (format code 0) — there is no binary-format support.

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
    ParameterStatus { name: String, value: String },
    BackendKeyData { pid: i32, secret: i32 },
    ReadyForQuery(u8),
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<Box<[u8]>>>),
    CommandComplete(String),
    ErrorResponse(String),
    NoticeResponse(String),
    ParseComplete,
    BindComplete,
    CloseComplete,
    ParameterDescription(Vec<i32>),
    NoData,
    PortalSuspended,
    EmptyQueryResponse,
    /// Server is ready to receive `CopyData` for a `COPY ... FROM STDIN`.
    CopyInResponse { columns: usize },
    /// Server is about to stream `CopyData` for a `COPY ... TO STDOUT`.
    CopyOutResponse { columns: usize },
    Unknown(u8),
}

pub struct Conn {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl Conn {
    pub async fn connect(addr: &str, params: &[(&str, &str)]) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let mut conn = Self { stream, read_buf: BytesMut::with_capacity(8192), write_buf: BytesMut::with_capacity(8192) };
        conn.write_startup(params);
        conn.flush().await?;

        loop {
            match conn.read_message().await? {
                BackendMessage::AuthenticationOk => {}
                BackendMessage::ReadyForQuery(_) => break,
                BackendMessage::ErrorResponse(msg) => anyhow::bail!("startup rejected: {msg}"),
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

    pub fn write_parse(&mut self, name: &str, sql: &str, param_types: &[i32]) {
        self.write_msg(b'P', |b| {
            b.put_slice(name.as_bytes());
            b.put_u8(0);
            b.put_slice(sql.as_bytes());
            b.put_u8(0);
            b.put_i16(param_types.len() as i16);
            for ty in param_types {
                b.put_i32(*ty);
            }
        });
    }

    pub fn write_bind(&mut self, portal: &str, stmt: &str, params: &[Option<Vec<u8>>]) {
        self.write_msg(b'B', |b| {
            b.put_slice(portal.as_bytes());
            b.put_u8(0);
            b.put_slice(stmt.as_bytes());
            b.put_u8(0);
            b.put_i16(1);
            b.put_i16(0); // all params text format
            b.put_i16(params.len() as i16);
            for p in params {
                match p {
                    None => b.put_i32(-1),
                    Some(data) => {
                        b.put_i32(data.len() as i32);
                        b.put_slice(data);
                    }
                }
            }
            b.put_i16(1);
            b.put_i16(0); // all results text format
        });
    }

    pub fn write_describe_portal(&mut self, name: &str) {
        self.write_msg(b'D', |b| {
            b.put_u8(b'P');
            b.put_slice(name.as_bytes());
            b.put_u8(0);
        });
    }

    pub fn write_execute(&mut self, portal: &str, max_rows: i32) {
        self.write_msg(b'E', |b| {
            b.put_slice(portal.as_bytes());
            b.put_u8(0);
            b.put_i32(max_rows);
        });
    }

    pub fn write_sync(&mut self) {
        self.write_msg(b'S', |_| {});
    }

    pub fn write_terminate(&mut self) {
        self.write_msg(b'X', |_| {});
    }

    /// Drive the `COPY ... FROM STDIN` sub-protocol: one row of text-format
    /// COPY data (tab-delimited, `\n`-terminated) per call. The server keeps
    /// reading `CopyData` until it sees `CopyDone`.
    pub fn write_copy_data(&mut self, data: &[u8]) {
        self.write_msg(b'd', |b| b.put_slice(data));
    }

    /// End a `COPY ... FROM STDIN` stream, telling the server to commit the
    /// buffered rows.
    pub fn write_copy_done(&mut self) {
        self.write_msg(b'c', |_| {});
    }

    /// Abort a `COPY ... FROM STDIN` stream, asking the server to discard the
    /// buffered rows and report `msg` as the failure reason.
    pub fn write_copy_fail(&mut self, msg: &str) {
        self.write_msg(b'f', |b| {
            b.put_slice(msg.as_bytes());
            b.put_u8(0);
        });
    }

    fn write_msg(&mut self, tag: u8, body: impl FnOnce(&mut BytesMut)) {
        let mut b = BytesMut::new();
        body(&mut b);
        self.write_buf.put_u8(tag);
        self.write_buf.put_i32(4 + b.len() as i32);
        self.write_buf.put_slice(&b);
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();
        Ok(())
    }

    pub async fn read_message(&mut self) -> anyhow::Result<BackendMessage> {
        self.fill(5).await?;
        let tag = self.read_buf[0];
        let len = i32::from_be_bytes(self.read_buf[1..5].try_into()?) as usize;
        if len < 4 {
            anyhow::bail!("invalid message length {len} for tag {}", tag as char);
        }
        let total = 1 + len;
        self.fill(total).await?;
        let mut data = self.read_buf.split_to(total);
        data.advance(5);

        let msg = match tag {
            b'R' => {
                let _code = if data.remaining() >= 4 { data.get_i32() } else { 0 };
                BackendMessage::AuthenticationOk
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
            b'1' => BackendMessage::ParseComplete,
            b'2' => BackendMessage::BindComplete,
            b'3' => BackendMessage::CloseComplete,
            b't' => {
                let n = data.get_i16() as usize;
                let mut types = Vec::with_capacity(n);
                for _ in 0..n {
                    types.push(data.get_i32());
                }
                BackendMessage::ParameterDescription(types)
            }
            b'n' => BackendMessage::NoData,
            b's' => BackendMessage::PortalSuspended,
            b'I' => BackendMessage::EmptyQueryResponse,
            b'G' => BackendMessage::CopyInResponse {
                columns: read_copy_response(&mut data),
            },
            b'H' => BackendMessage::CopyOutResponse {
                columns: read_copy_response(&mut data),
            },
            other => BackendMessage::Unknown(other),
        };
        Ok(msg)
    }

    async fn fill(&mut self, n: usize) -> anyhow::Result<()> {
        while self.read_buf.len() < n {
            let read = self.stream.read_buf(&mut self.read_buf).await?;
            if read == 0 {
                anyhow::bail!("connection closed by server");
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

/// Error/notice fields are a sequence of `(u8 field_code, cstr value)` pairs
/// terminated by a nul byte; we only surface the human-readable message.
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

/// Decode a `CopyInResponse`/`CopyOutResponse` body: an overall format byte
/// (0 = text), a column count, and one format code per column. We only care
/// about the column count here (everything is text format).
fn read_copy_response(data: &mut BytesMut) -> usize {
    if !data.has_remaining() {
        return 0;
    }
    let _overall = data.get_u8();
    let columns = if data.remaining() >= 2 {
        data.get_i16() as usize
    } else {
        0
    };
    for _ in 0..columns {
        if data.remaining() >= 2 {
            let _fmt = data.get_i16();
        }
    }
    columns
}
