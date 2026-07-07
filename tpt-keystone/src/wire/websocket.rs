//! Flux (Phase 11) WebSocket streaming endpoint — hand-rolled RFC 6455 per
//! this project's from-scratch wire-protocol ethos (see `wire::codec` for
//! the Postgres side): a raw `TcpListener`, an HTTP/1.1 Upgrade handshake
//! parsed by hand, and a minimal frame codec. `sha1` is a dependency for the
//! handshake's `Sec-WebSocket-Accept` digest — hashing itself isn't the
//! "from scratch" boundary this project draws (see `sha2` already used for
//! `objectstore.rs`'s content hashes), only the wire/parsing layers are.
//!
//! Protocol, deliberately scoped down to exactly what a "watch a topic"
//! client needs: the client sends one `{"subscribe": "<topic>"}` text
//! frame, after which every record subsequently published to that topic
//! (via `Database::flux_publish`, including native CDC) is pushed as a
//! `{"offset":...,"key":...,"value":...,"ts":...}` text frame. A later
//! `subscribe` message on the same connection replaces the topic being
//! watched rather than adding a second subscription — one topic per
//! connection.
//!
//! Explicit scope cuts (matching this codebase's "real but scoped, not
//! stub-and-claim-done" discipline elsewhere): no message fragmentation (a
//! multi-frame message is rejected, not reassembled), no permessage-deflate
//! extension, no binary frames, no ping/pong keepalive — a dead TCP
//! connection is only noticed when a write to it fails. No backlog replay:
//! a client only sees records published *after* its `subscribe` frame is
//! processed, same "from the moment of subscription onward" semantics a
//! Postgres `LISTEN` has in this engine (`storage::database::Database`'s
//! `notify_bus`), not Kafka's "replay from offset 0" semantics (that's what
//! `flux_poll`/`flux_commit` over the Postgres wire protocol are for).

use std::sync::Arc;

use base64::Engine as _;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::storage::database::Database;

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Drive one client connection from the HTTP Upgrade handshake through the
/// subscribe/push loop until it disconnects.
pub async fn handle(stream: TcpStream, peer: std::net::SocketAddr, db: Arc<Database>) {
    if let Err(e) = run(stream, db).await {
        debug!(%peer, "flux websocket session ended: {e}");
    }
}

async fn run(mut stream: TcpStream, db: Arc<Database>) -> anyhow::Result<()> {
    let key = read_handshake(&mut stream).await?;
    let accept = accept_key(&key);
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(response.as_bytes()).await?;

    let mut subscribed_topic: Option<String> = None;
    let mut flux_rx = db.subscribe_flux();

    loop {
        tokio::select! {
            frame = read_frame(&mut stream) => {
                match frame? {
                    Some(Frame::Text(text)) => {
                        if let Ok(msg) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(topic) = msg.get("subscribe").and_then(|v| v.as_str()) {
                                subscribed_topic = Some(topic.to_string());
                            }
                        }
                    }
                    Some(Frame::Ignored) => {}
                    Some(Frame::Close) | None => return Ok(()),
                }
            }
            event = flux_rx.recv() => {
                let Ok((topic, record)) = event else { return Ok(()) };
                if subscribed_topic.as_deref() == Some(topic.as_str()) {
                    let payload = serde_json::json!({
                        "offset": record.offset,
                        "key": record.key.map(|k| String::from_utf8_lossy(&k).into_owned()),
                        "value": String::from_utf8_lossy(&record.value),
                        "ts": record.timestamp_ms,
                    });
                    write_text_frame(&mut stream, &payload.to_string()).await?;
                }
            }
        }
    }
}

/// Reads (byte-by-byte, since the request is small and there's no framing
/// to know its length in advance) the HTTP Upgrade request up to the blank
/// line terminating its headers, and extracts `Sec-WebSocket-Key`.
async fn read_handshake(stream: &mut TcpStream) -> anyhow::Result<String> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        anyhow::ensure!(n > 0, "connection closed during WebSocket handshake");
        buf.push(byte[0]);
        if buf.len() >= 4 && &buf[buf.len() - 4..] == b"\r\n\r\n" {
            break;
        }
        anyhow::ensure!(buf.len() <= 16_384, "WebSocket handshake request too large");
    }
    let request = String::from_utf8_lossy(&buf);
    request
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("sec-websocket-key:"))
        .and_then(|line| line.splitn(2, ':').nth(1))
        .map(|v| v.trim().to_string())
        .ok_or_else(|| anyhow::anyhow!("missing Sec-WebSocket-Key header"))
}

fn accept_key(client_key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(client_key.as_bytes());
    hasher.update(WS_GUID.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

enum Frame {
    Text(String),
    /// A control/opcode this endpoint doesn't act on (ping/pong/binary) —
    /// read off the wire and discarded so framing stays in sync, but no
    /// response or state change happens (see module docs: no ping/pong
    /// keepalive is implemented).
    Ignored,
    Close,
}

/// Reads one client->server frame (RFC 6455 §5.2), unmasking the payload
/// (client frames are always masked). Returns `Ok(None)` on a clean TCP
/// close. Fragmented messages (`FIN` unset) are rejected rather than
/// reassembled — see module docs.
async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Option<Frame>> {
    let mut header = [0u8; 2];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let fin = header[0] & 0x80 != 0;
    anyhow::ensure!(fin, "fragmented WebSocket messages are not supported");
    let opcode = header[0] & 0x0F;
    let masked = header[1] & 0x80 != 0;
    let mut len = (header[1] & 0x7F) as u64;
    if len == 126 {
        let mut ext = [0u8; 2];
        stream.read_exact(&mut ext).await?;
        len = u16::from_be_bytes(ext) as u64;
    } else if len == 127 {
        let mut ext = [0u8; 8];
        stream.read_exact(&mut ext).await?;
        len = u64::from_be_bytes(ext);
    }
    let mask_key = if masked {
        let mut m = [0u8; 4];
        stream.read_exact(&mut m).await?;
        Some(m)
    } else {
        None
    };
    let mut payload = vec![0u8; len as usize];
    if !payload.is_empty() {
        stream.read_exact(&mut payload).await?;
    }
    if let Some(m) = mask_key {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= m[i % 4];
        }
    }
    match opcode {
        0x1 => Ok(Some(Frame::Text(String::from_utf8_lossy(&payload).into_owned()))),
        0x8 => Ok(Some(Frame::Close)),
        _ => Ok(Some(Frame::Ignored)),
    }
}

/// Writes one unmasked server->client text frame (servers never mask,
/// RFC 6455 §5.1).
async fn write_text_frame(stream: &mut TcpStream, text: &str) -> anyhow::Result<()> {
    let payload = text.as_bytes();
    let mut frame = Vec::with_capacity(payload.len() + 10);
    frame.push(0x81); // FIN=1, opcode=1 (text)
    let len = payload.len();
    if len <= 125 {
        frame.push(len as u8);
    } else if len <= u16::MAX as usize {
        frame.push(126);
        frame.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        frame.push(127);
        frame.extend_from_slice(&(len as u64).to_be_bytes());
    }
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await?;
    Ok(())
}
