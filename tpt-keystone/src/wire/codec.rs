use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::messages::{encode, BackendMessage, ErrorInfo};

/// A frontend (client → server) message after initial startup.
#[derive(Debug)]
pub enum FrontendMessage {
    Query(String),
    Parse { name: String, query: String, param_types: Vec<i32> },
    Bind {
        portal: String,
        stmt: String,
        params: Vec<Option<Vec<u8>>>,
        param_formats: Vec<i16>,
        result_formats: Vec<i16>,
    },
    Describe { kind: u8, name: String },
    Execute { portal: String, max_rows: i32 },
    Close { kind: u8, name: String },
    Sync,
    Flush,
    Terminate,
    CancelRequest,
    CopyData(Vec<u8>),
    CopyDone,
    CopyFail(String),
}

/// Startup message parameters sent by the client before the normal message loop.
#[derive(Debug)]
pub struct StartupParams {
    pub protocol_version: i32,
    pub params: Vec<(String, String)>,
}

pub struct Conn {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl Conn {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(8192),
        }
    }

    /// Read the initial startup message (special framing — no type byte).
    pub async fn read_startup(&mut self) -> anyhow::Result<StartupParams> {
        loop {
            // Read at least 4 bytes to get the length.
            self.fill(4).await?;
            let total_len = i32::from_be_bytes(self.read_buf[..4].try_into()?) as usize;
            if total_len < 8 || total_len > 65535 {
                anyhow::bail!("invalid startup message length: {total_len}");
            }
            self.fill(total_len).await?;
            let mut data = self.read_buf.split_to(total_len);
            data.advance(4); // skip length

            let protocol_version = data.get_i32();

            // SSLRequest — decline with 'N', then re-read the real startup.
            if protocol_version == 80877103 {
                self.stream.write_all(b"N").await?;
                self.stream.flush().await?;
                continue;
            }
            if protocol_version == 80877102 {
                anyhow::bail!("cancel request not supported");
            }

            let mut params = Vec::new();
            while data.has_remaining() {
                let key = read_cstr(&mut data);
                if key.is_empty() {
                    break;
                }
                let value = read_cstr(&mut data);
                params.push((key, value));
            }

            return Ok(StartupParams { protocol_version, params });
        }
    }

    /// Read the next normal (post-startup) frontend message.
    pub async fn read_message(&mut self) -> anyhow::Result<FrontendMessage> {
        // Need at least type byte + 4 byte length.
        self.fill(5).await?;
        let tag = self.read_buf[0];
        let len = i32::from_be_bytes(self.read_buf[1..5].try_into()?) as usize;
        if len < 4 {
            anyhow::bail!("invalid message length {len} for tag {tag}");
        }
        let total = 1 + len; // type byte is not included in length field
        self.fill(total).await?;
        let mut data = self.read_buf.split_to(total);
        data.advance(5); // skip tag + length

        let msg = match tag {
            b'Q' => {
                let query = read_cstr(&mut data);
                FrontendMessage::Query(query)
            }
            b'P' => {
                let name = read_cstr(&mut data);
                let query = read_cstr(&mut data);
                let n = if data.remaining() >= 2 { data.get_i16() as usize } else { 0 };
                let mut param_types = Vec::with_capacity(n);
                for _ in 0..n {
                    param_types.push(data.get_i32());
                }
                FrontendMessage::Parse { name, query, param_types }
            }
            b'B' => {
                let portal = read_cstr(&mut data);
                let stmt = read_cstr(&mut data);

                let n_param_formats = if data.remaining() >= 2 { data.get_i16() as usize } else { 0 };
                let mut param_formats = Vec::with_capacity(n_param_formats);
                for _ in 0..n_param_formats {
                    param_formats.push(if data.remaining() >= 2 { data.get_i16() } else { 0 });
                }

                let n_params = if data.remaining() >= 2 { data.get_i16() as usize } else { 0 };
                let mut params = Vec::with_capacity(n_params);
                for _ in 0..n_params {
                    let len = if data.remaining() >= 4 { data.get_i32() } else { -1 };
                    if len < 0 {
                        params.push(None);
                    } else {
                        let len = len as usize;
                        let take = len.min(data.remaining());
                        params.push(Some(data.copy_to_bytes(take).to_vec()));
                    }
                }

                let n_result_formats = if data.remaining() >= 2 { data.get_i16() as usize } else { 0 };
                let mut result_formats = Vec::with_capacity(n_result_formats);
                for _ in 0..n_result_formats {
                    result_formats.push(if data.remaining() >= 2 { data.get_i16() } else { 0 });
                }

                FrontendMessage::Bind { portal, stmt, params, param_formats, result_formats }
            }
            b'D' => {
                let kind = if data.has_remaining() { data.get_u8() } else { b'S' };
                let name = read_cstr(&mut data);
                FrontendMessage::Describe { kind, name }
            }
            b'E' => {
                let portal = read_cstr(&mut data);
                let max_rows = if data.remaining() >= 4 { data.get_i32() } else { 0 };
                FrontendMessage::Execute { portal, max_rows }
            }
            b'C' => {
                let kind = if data.has_remaining() { data.get_u8() } else { b'S' };
                let name = read_cstr(&mut data);
                FrontendMessage::Close { kind, name }
            }
            b'S' => FrontendMessage::Sync,
            b'H' => FrontendMessage::Flush,
            b'X' => FrontendMessage::Terminate,
            b'd' => FrontendMessage::CopyData(data.to_vec()),
            b'c' => FrontendMessage::CopyDone,
            b'f' => FrontendMessage::CopyFail(read_cstr(&mut data)),
            other => {
                tracing::warn!("unknown frontend message type: {}", other as char);
                FrontendMessage::Sync
            }
        };
        Ok(msg)
    }

    /// Queue a backend message for sending.
    pub fn send(&mut self, msg: &BackendMessage) {
        encode(msg, &mut self.write_buf);
    }

    /// Flush all queued messages to the client.
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();
        Ok(())
    }

    pub fn send_error(&mut self, e: ErrorInfo) {
        self.send(&BackendMessage::ErrorResponse(e));
    }

    /// Ensure `read_buf` has at least `n` bytes, reading from the socket if needed.
    async fn fill(&mut self, n: usize) -> anyhow::Result<()> {
        while self.read_buf.len() < n {
            let read = self.stream.read_buf(&mut self.read_buf).await?;
            if read == 0 {
                anyhow::bail!("connection closed by client");
            }
        }
        Ok(())
    }
}

fn read_cstr(buf: &mut BytesMut) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let s = String::from_utf8_lossy(&buf[..end]).into_owned();
    buf.advance(end + 1);
    s
}
