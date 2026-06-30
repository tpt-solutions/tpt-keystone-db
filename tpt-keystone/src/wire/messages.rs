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

#[derive(Debug)]
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
    }
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
