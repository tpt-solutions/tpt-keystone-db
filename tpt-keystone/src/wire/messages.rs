use bytes::{BufMut, BytesMut};

/// A backend (server → client) message ready to write to the wire.
#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationOk,
    ParameterStatus { name: String, value: String },
    BackendKeyData { pid: i32, secret: i32 },
    ReadyForQuery(TransactionStatus),
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<Vec<u8>>>),
    CommandComplete(String),
    ErrorResponse(ErrorInfo),
    EmptyQueryResponse,
    NoticeResponse(String),
    ParseComplete,
    BindComplete,
    CloseComplete,
    ParameterDescription(Vec<i32>),
    NoData,
    PortalSuspended,
    NotificationResponse { pid: i32, channel: String, payload: String },
    CopyInResponse { columns: usize },
    CopyOutResponse { columns: usize },
    CopyData(Vec<u8>),
    CopyDone,
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionStatus {
    Idle,
    InTransaction,
    Failed,
}

impl TransactionStatus {
    pub fn byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction => b'T',
            Self::Failed => b'E',
        }
    }
}

#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub col_attr: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}

impl FieldDescription {
    pub fn simple(name: impl Into<String>, type_oid: i32) -> Self {
        Self {
            name: name.into(),
            table_oid: 0,
            col_attr: 0,
            type_oid,
            type_size: -1,
            type_modifier: -1,
            format: 0,
        }
    }
}

/// Postgres type OIDs for the types we produce.
pub mod oid {
    pub const INT8: i32 = 20;
    pub const FLOAT8: i32 = 701;
    pub const TEXT: i32 = 25;
    pub const BOOL: i32 = 16;
}

#[derive(Debug)]
pub struct ErrorInfo {
    pub severity: String,
    pub code: String,
    pub message: String,
}

impl ErrorInfo {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            severity: "ERROR".into(),
            code: code.into(),
            message: message.into(),
        }
    }
    pub fn fatal(code: &str, message: impl Into<String>) -> Self {
        Self {
            severity: "FATAL".into(),
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Serialize a backend message into `buf`.
pub fn encode(msg: &BackendMessage, buf: &mut BytesMut) {
    match msg {
        BackendMessage::AuthenticationOk => {
            write_msg(buf, b'R', |b| b.put_i32(0));
        }
        BackendMessage::ParameterStatus { name, value } => {
            write_msg(buf, b'S', |b| {
                b.put_slice(name.as_bytes());
                b.put_u8(0);
                b.put_slice(value.as_bytes());
                b.put_u8(0);
            });
        }
        BackendMessage::BackendKeyData { pid, secret } => {
            write_msg(buf, b'K', |b| {
                b.put_i32(*pid);
                b.put_i32(*secret);
            });
        }
        BackendMessage::ReadyForQuery(status) => {
            write_msg(buf, b'Z', |b| b.put_u8(status.byte()));
        }
        BackendMessage::RowDescription(fields) => {
            write_msg(buf, b'T', |b| {
                b.put_i16(fields.len() as i16);
                for f in fields {
                    b.put_slice(f.name.as_bytes());
                    b.put_u8(0);
                    b.put_i32(f.table_oid);
                    b.put_i16(f.col_attr);
                    b.put_i32(f.type_oid);
                    b.put_i16(f.type_size);
                    b.put_i32(f.type_modifier);
                    b.put_i16(f.format);
                }
            });
        }
        BackendMessage::DataRow(cols) => {
            write_msg(buf, b'D', |b| {
                b.put_i16(cols.len() as i16);
                for col in cols {
                    match col {
                        None => b.put_i32(-1),
                        Some(data) => {
                            b.put_i32(data.len() as i32);
                            b.put_slice(data);
                        }
                    }
                }
            });
        }
        BackendMessage::CommandComplete(tag) => {
            write_msg(buf, b'C', |b| {
                b.put_slice(tag.as_bytes());
                b.put_u8(0);
            });
        }
        BackendMessage::ErrorResponse(e) => {
            write_msg(buf, b'E', |b| {
                b.put_u8(b'S'); b.put_slice(e.severity.as_bytes()); b.put_u8(0);
                b.put_u8(b'V'); b.put_slice(e.severity.as_bytes()); b.put_u8(0);
                b.put_u8(b'C'); b.put_slice(e.code.as_bytes()); b.put_u8(0);
                b.put_u8(b'M'); b.put_slice(e.message.as_bytes()); b.put_u8(0);
                b.put_u8(0); // terminator
            });
        }
        BackendMessage::EmptyQueryResponse => {
            write_msg(buf, b'I', |_| {});
        }
        BackendMessage::NoticeResponse(msg) => {
            write_msg(buf, b'N', |b| {
                b.put_u8(b'M'); b.put_slice(msg.as_bytes()); b.put_u8(0);
                b.put_u8(0);
            });
        }
        BackendMessage::ParseComplete => {
            write_msg(buf, b'1', |_| {});
        }
        BackendMessage::BindComplete => {
            write_msg(buf, b'2', |_| {});
        }
        BackendMessage::CloseComplete => {
            write_msg(buf, b'3', |_| {});
        }
        BackendMessage::ParameterDescription(types) => {
            write_msg(buf, b't', |b| {
                b.put_i16(types.len() as i16);
                for ty in types {
                    b.put_i32(*ty);
                }
            });
        }
        BackendMessage::NoData => {
            write_msg(buf, b'n', |_| {});
        }
        BackendMessage::PortalSuspended => {
            write_msg(buf, b's', |_| {});
        }
        BackendMessage::NotificationResponse { pid, channel, payload } => {
            write_msg(buf, b'A', |b| {
                b.put_i32(*pid);
                b.put_slice(channel.as_bytes());
                b.put_u8(0);
                b.put_slice(payload.as_bytes());
                b.put_u8(0);
            });
        }
        BackendMessage::CopyInResponse { columns } => write_copy_response(buf, b'G', *columns),
        BackendMessage::CopyOutResponse { columns } => write_copy_response(buf, b'H', *columns),
        BackendMessage::CopyData(data) => {
            write_msg(buf, b'd', |b| b.put_slice(data));
        }
        BackendMessage::CopyDone => {
            write_msg(buf, b'c', |_| {});
        }
    }
}

/// Shared body for `CopyInResponse`/`CopyOutResponse`: overall format code
/// (0 = text) plus one format code per column (also text).
fn write_copy_response(buf: &mut BytesMut, tag: u8, columns: usize) {
    write_msg(buf, tag, |b| {
        b.put_u8(0); // text format overall
        b.put_i16(columns as i16);
        for _ in 0..columns {
            b.put_i16(0); // text format per column
        }
    });
}

/// Write a framed message: type byte + i32 length (includes itself) + body.
fn write_msg<F: FnOnce(&mut BytesMut)>(buf: &mut BytesMut, tag: u8, f: F) {
    buf.put_u8(tag);
    let len_idx = buf.len();
    buf.put_i32(0); // placeholder
    f(buf);
    let body_len = (buf.len() - len_idx) as i32; // includes the 4 length bytes
    let slice = &mut buf[len_idx..len_idx + 4];
    slice.copy_from_slice(&body_len.to_be_bytes());
}
