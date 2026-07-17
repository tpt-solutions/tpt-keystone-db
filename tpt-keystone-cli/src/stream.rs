//! `tpt stream <topic>` — tails Keystone's Flux WebSocket bridge
//! (`tpt-keystone/src/wire/websocket.rs`, default port 5434) in real time,
//! per `TODO.md`'s "tail a Flux event stream" checklist item.
//!
//! Hand-rolled RFC 6455 client (blocking `std::net::TcpStream`), mirroring
//! the server's hand-rolled implementation rather than pulling in
//! `tokio-tungstenite`/`tungstenite` — same from-scratch-wire-protocol
//! reasoning as everywhere else in this codebase (see the server module's
//! doc comment). Client frames must be masked per RFC 6455 §5.3; this only
//! ever sends one small text frame (the subscribe message) so masking with
//! a random key is a few lines, not a dependency.

use std::io::{Read, Write};
use std::net::TcpStream;

use base64::Engine as _;
use rand::RngCore;
use sha1::{Digest, Sha1};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub fn run(host: &str, flux_port: u16, topic: &str) -> anyhow::Result<()> {
    let addr = format!("{host}:{flux_port}");
    let mut stream = TcpStream::connect(&addr)?;

    let mut key_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut key_bytes);
    let key = base64::engine::general_purpose::STANDARD.encode(key_bytes);

    let request = format!(
        "GET / HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    stream.write_all(request.as_bytes())?;

    let expected_accept = accept_key(&key);
    let response_headers = read_http_headers(&mut stream)?;
    anyhow::ensure!(
        response_headers.contains(&expected_accept),
        "WebSocket handshake failed: unexpected or missing Sec-WebSocket-Accept"
    );

    write_masked_text_frame(&mut stream, &serde_json::json!({ "subscribe": topic }).to_string())?;

    eprintln!("tailing Flux topic '{topic}' on {addr} — Ctrl-C to stop");
    loop {
        match read_frame(&mut stream)? {
            Some(text) => println!("{text}"),
            None => {
                eprintln!("connection closed by server");
                break;
            }
        }
    }
    Ok(())
}

fn accept_key(client_key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(client_key.as_bytes());
    hasher.update(WS_GUID.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

fn read_http_headers(stream: &mut TcpStream) -> anyhow::Result<String> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte)?;
        anyhow::ensure!(n > 0, "connection closed during WebSocket handshake");
        buf.push(byte[0]);
        if buf.len() >= 4 && &buf[buf.len() - 4..] == b"\r\n\r\n" {
            break;
        }
        anyhow::ensure!(buf.len() <= 16_384, "WebSocket handshake response too large");
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Sends one client->server masked text frame (RFC 6455 §5.3 requires
/// client frames to be masked).
fn write_masked_text_frame(stream: &mut TcpStream, text: &str) -> anyhow::Result<()> {
    let payload = text.as_bytes();
    let mut mask = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut mask);

    let mut frame = Vec::with_capacity(payload.len() + 14);
    frame.push(0x81); // FIN=1, opcode=1 (text)
    let len = payload.len();
    if len <= 125 {
        frame.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        frame.push(0x80 | 127);
        frame.extend_from_slice(&(len as u64).to_be_bytes());
    }
    frame.extend_from_slice(&mask);
    for (i, b) in payload.iter().enumerate() {
        frame.push(b ^ mask[i % 4]);
    }
    stream.write_all(&frame)?;
    Ok(())
}

/// Reads one server->client frame (unmasked — RFC 6455 §5.1 forbids server
/// masking). Returns `Ok(None)` on clean close; non-text opcodes are
/// skipped, matching the server's "no ping/pong keepalive" scope cut.
fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Option<String>> {
    loop {
        let mut header = [0u8; 2];
        match stream.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let opcode = header[0] & 0x0F;
        let masked = header[1] & 0x80 != 0;
        let mut len = (header[1] & 0x7F) as u64;
        if len == 126 {
            let mut ext = [0u8; 2];
            stream.read_exact(&mut ext)?;
            len = u16::from_be_bytes(ext) as u64;
        } else if len == 127 {
            let mut ext = [0u8; 8];
            stream.read_exact(&mut ext)?;
            len = u64::from_be_bytes(ext);
        }
        let mask_key = if masked {
            let mut m = [0u8; 4];
            stream.read_exact(&mut m)?;
            Some(m)
        } else {
            None
        };
        let mut payload = vec![0u8; len as usize];
        if !payload.is_empty() {
            stream.read_exact(&mut payload)?;
        }
        if let Some(m) = mask_key {
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= m[i % 4];
            }
        }
        match opcode {
            0x1 => return Ok(Some(String::from_utf8_lossy(&payload).into_owned())),
            0x8 => return Ok(None),
            _ => continue, // ping/pong/binary — skip, keep reading
        }
    }
}
