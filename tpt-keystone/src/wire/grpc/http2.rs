//! Hand-written HTTP/2 (RFC 7540) frame layer for the Flux gRPC endpoint — no
//! `h2`/`hyper` crate, consistent with this repo's from-scratch wire-protocol
//! rule (see `wire::codec` for the Postgres side, `wire::websocket` for the
//! RFC 6455 Flux bridge). Only the frame types gRPC-over-h2c actually needs are
//! modelled; everything else is read off the wire and ignored so framing stays
//! in sync.
//!
//! This is cleartext HTTP/2 (h2c) with *prior knowledge* (RFC 7540 §3.4): the
//! client opens with the connection preface and speaks HTTP/2 immediately, no
//! HTTP/1.1 Upgrade dance. That matches how gRPC clients connect over an
//! insecure channel, and mirrors the plaintext-by-default posture of this
//! node's other auxiliary listeners (MCP, Flux WS, Canvas HTTP). TLS/ALPN-based
//! `h2` is a documented scope cut — see the module docs in `wire::grpc`.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The 24-byte HTTP/2 connection preface a client sends first (RFC 7540 §3.5).
pub const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

// Frame type codes (RFC 7540 §6).
pub const FRAME_DATA: u8 = 0x0;
pub const FRAME_HEADERS: u8 = 0x1;
pub const FRAME_PRIORITY: u8 = 0x2;
pub const FRAME_RST_STREAM: u8 = 0x3;
pub const FRAME_SETTINGS: u8 = 0x4;
pub const FRAME_PUSH_PROMISE: u8 = 0x5;
pub const FRAME_PING: u8 = 0x6;
pub const FRAME_GOAWAY: u8 = 0x7;
pub const FRAME_WINDOW_UPDATE: u8 = 0x8;
pub const FRAME_CONTINUATION: u8 = 0x9;

// Frame flags.
pub const FLAG_END_STREAM: u8 = 0x1;
pub const FLAG_ACK: u8 = 0x1; // SETTINGS/PING ack (same bit as END_STREAM)
pub const FLAG_END_HEADERS: u8 = 0x4;
pub const FLAG_PADDED: u8 = 0x8;
pub const FLAG_PRIORITY: u8 = 0x20;

// SETTINGS parameter identifiers we care about (RFC 7540 §6.5.2).
pub const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 0x4;

// Error codes (RFC 7540 §7).
pub const ERR_NO_ERROR: u32 = 0x0;
pub const ERR_PROTOCOL_ERROR: u32 = 0x1;
pub const ERR_FLOW_CONTROL_ERROR: u32 = 0x3;

/// A single HTTP/2 frame, header fields plus its raw payload.
#[derive(Debug, Clone)]
pub struct Frame {
    pub frame_type: u8,
    pub flags: u8,
    pub stream_id: u32,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn flag(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

/// Reads one frame. Returns `Ok(None)` on a clean EOF at a frame boundary.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> anyhow::Result<Option<Frame>> {
    let mut header = [0u8; 9];
    match r.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    let frame_type = header[3];
    let flags = header[4];
    // Top bit of the stream id is a reserved bit; mask it off.
    let stream_id = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
    anyhow::ensure!(len <= 16 * 1024 * 1024, "http2: frame too large ({len})");
    let mut payload = vec![0u8; len];
    if len > 0 {
        r.read_exact(&mut payload).await?;
    }
    Ok(Some(Frame {
        frame_type,
        flags,
        stream_id,
        payload,
    }))
}

/// Serializes and writes one frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    frame_type: u8,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> anyhow::Result<()> {
    let len = payload.len();
    anyhow::ensure!(len <= 0xff_ffff, "http2: frame payload exceeds 24-bit length");
    let mut header = [0u8; 9];
    header[0] = (len >> 16) as u8;
    header[1] = (len >> 8) as u8;
    header[2] = len as u8;
    header[3] = frame_type;
    header[4] = flags;
    header[5..9].copy_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
    w.write_all(&header).await?;
    if !payload.is_empty() {
        w.write_all(payload).await?;
    }
    w.flush().await?;
    Ok(())
}

/// Reads and validates the 24-byte client connection preface.
pub async fn read_preface<R: AsyncRead + Unpin>(r: &mut R) -> anyhow::Result<()> {
    let mut buf = [0u8; 24];
    r.read_exact(&mut buf).await?;
    anyhow::ensure!(buf == PREFACE, "http2: invalid connection preface");
    Ok(())
}

/// Parses a SETTINGS frame payload into `(identifier, value)` pairs. A SETTINGS
/// frame is a sequence of 6-byte entries (2-byte id + 4-byte value).
pub fn parse_settings(payload: &[u8]) -> anyhow::Result<Vec<(u16, u32)>> {
    anyhow::ensure!(
        payload.len() % 6 == 0,
        "http2: SETTINGS payload not a multiple of 6"
    );
    let mut out = Vec::new();
    let mut i = 0;
    while i < payload.len() {
        let id = u16::from_be_bytes([payload[i], payload[i + 1]]);
        let val = u32::from_be_bytes([
            payload[i + 2],
            payload[i + 3],
            payload[i + 4],
            payload[i + 5],
        ]);
        out.push((id, val));
        i += 6;
    }
    Ok(out)
}

/// Strips HEADERS-frame padding and the optional priority field, returning just
/// the header block fragment (RFC 7540 §6.2).
pub fn headers_block_fragment(frame: &Frame) -> anyhow::Result<Vec<u8>> {
    let mut data = frame.payload.as_slice();
    let mut pad_len = 0usize;
    if frame.flag(FLAG_PADDED) {
        anyhow::ensure!(!data.is_empty(), "http2: padded HEADERS missing pad length");
        pad_len = data[0] as usize;
        data = &data[1..];
    }
    if frame.flag(FLAG_PRIORITY) {
        anyhow::ensure!(data.len() >= 5, "http2: HEADERS priority field truncated");
        data = &data[5..];
    }
    anyhow::ensure!(data.len() >= pad_len, "http2: HEADERS padding exceeds payload");
    Ok(data[..data.len() - pad_len].to_vec())
}

/// Strips DATA-frame padding, returning the payload bytes (RFC 7540 §6.1).
pub fn data_payload(frame: &Frame) -> anyhow::Result<Vec<u8>> {
    let mut data = frame.payload.as_slice();
    if frame.flag(FLAG_PADDED) {
        anyhow::ensure!(!data.is_empty(), "http2: padded DATA missing pad length");
        let pad_len = data[0] as usize;
        data = &data[1..];
        anyhow::ensure!(data.len() >= pad_len, "http2: DATA padding exceeds payload");
        data = &data[..data.len() - pad_len];
    }
    Ok(data.to_vec())
}

/// Encodes a SETTINGS payload from `(id, value)` pairs.
pub fn encode_settings(entries: &[(u16, u32)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(entries.len() * 6);
    for &(id, val) in entries {
        out.extend_from_slice(&id.to_be_bytes());
        out.extend_from_slice(&val.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn frame_round_trips_through_read_write() {
        let mut buf = Vec::new();
        write_frame(&mut buf, FRAME_DATA, FLAG_END_STREAM, 3, b"hello")
            .await
            .unwrap();
        let mut cur = Cursor::new(buf);
        let frame = read_frame(&mut cur).await.unwrap().unwrap();
        assert_eq!(frame.frame_type, FRAME_DATA);
        assert!(frame.flag(FLAG_END_STREAM));
        assert_eq!(frame.stream_id, 3);
        assert_eq!(frame.payload, b"hello");
    }

    #[test]
    fn settings_parse_round_trip() {
        let entries = vec![(SETTINGS_INITIAL_WINDOW_SIZE, 1_000_000), (0x3, 100)];
        let encoded = encode_settings(&entries);
        assert_eq!(parse_settings(&encoded).unwrap(), entries);
    }

    #[test]
    fn headers_fragment_strips_padding_and_priority() {
        // flags: PADDED | PRIORITY. payload = [pad_len=2][5-byte priority][block "AB"][pad "XX"]
        let frame = Frame {
            frame_type: FRAME_HEADERS,
            flags: FLAG_PADDED | FLAG_PRIORITY,
            stream_id: 1,
            payload: vec![2, 0, 0, 0, 0, 0, b'A', b'B', b'X', b'X'],
        };
        assert_eq!(headers_block_fragment(&frame).unwrap(), b"AB");
    }

    #[tokio::test]
    async fn eof_at_boundary_returns_none() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        assert!(read_frame(&mut cur).await.unwrap().is_none());
    }
}
