//! Optional TLS for the Postgres wire listener. Skipped entirely (today's
//! plaintext-only behavior, byte-for-byte) unless `TPT_TLS_CERT_PATH`/
//! `TPT_TLS_KEY_PATH` are both set — see `storage::config::StorageConfig`.
//!
//! Negotiation follows the real Postgres `SSLRequest` handshake: an 8-byte
//! probe packet (length=8, code=80877103) sent before the real
//! `StartupMessage`, answered with a single `S`/`N` byte. This has to happen
//! on the raw `TcpStream` *before* a `wire::codec::Conn` is constructed,
//! since upgrading a live connection to TLS means swapping the underlying
//! stream for a `tokio_rustls` one — `Conn` only ever holds one boxed stream
//! for its whole lifetime, not a stream it can later re-wrap.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

use super::codec::BoxedStream;

const SSL_REQUEST_CODE: i32 = 80877103;

/// Loads a `rustls` server config from PEM cert/key files and wraps it as a
/// `TlsAcceptor`. Called once at startup, not per-connection.
pub fn load_acceptor(cert_path: &str, key_path: &str) -> anyhow::Result<TlsAcceptor> {
    let cert_bytes = std::fs::read(cert_path)?;
    let key_bytes = std::fs::read(key_path)?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_bytes[..]).collect::<Result<_, _>>()?;
    let key = rustls_pemfile::private_key(&mut &key_bytes[..])?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {key_path}"))?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Peeks the connection's first bytes for an `SSLRequest`; if present and
/// `acceptor` is configured, performs the TLS handshake and returns the
/// upgraded stream. Otherwise (no `SSLRequest`, or TLS not configured)
/// returns the original `TcpStream` unchanged — the client falls back to
/// sending its real `StartupMessage` in plaintext next, exactly as today.
pub async fn negotiate(mut stream: TcpStream, acceptor: Option<&TlsAcceptor>) -> anyhow::Result<BoxedStream> {
    let mut probe = [0u8; 8];
    let mut peeked = 0usize;
    while peeked < 8 {
        let n = stream.peek(&mut probe[peeked..]).await?;
        if n == 0 {
            // Fewer than 8 bytes ever arrive (short-lived/health-check
            // connection) — not an SSLRequest, hand back the stream as-is.
            return Ok(Box::new(stream));
        }
        peeked += n;
        if peeked < 8 {
            stream.readable().await?;
        }
    }

    let len = i32::from_be_bytes(probe[0..4].try_into().unwrap());
    let code = i32::from_be_bytes(probe[4..8].try_into().unwrap());
    if len != 8 || code != SSL_REQUEST_CODE {
        // Not an SSLRequest — leave the bytes unconsumed (this was a peek)
        // for `Conn::read_startup` to read as the real StartupMessage.
        return Ok(Box::new(stream));
    }

    // Consume the 8 probed bytes for real this time.
    let mut discard = [0u8; 8];
    stream.read_exact(&mut discard).await?;

    match acceptor {
        Some(acceptor) => {
            stream.write_all(b"S").await?;
            stream.flush().await?;
            let tls = acceptor.accept(stream).await?;
            Ok(Box::new(tls))
        }
        None => {
            stream.write_all(b"N").await?;
            stream.flush().await?;
            Ok(Box::new(stream))
        }
    }
}
