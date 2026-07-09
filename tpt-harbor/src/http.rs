//! Minimal HTTP/1.1 client over tokio TCP — shared by REST-API source
//! connectors (InfluxDB, Elasticsearch, Pinecone/Weaviate/Qdrant).
//!
//! Not a general-purpose HTTP client: only supports GET/POST with
//! keep-alive, no chunked transfer encoding, no TLS (same trust boundary
//! as `pgwire.rs`). Enough for talking to local/cluster-internal API
//! endpoints.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct HttpClient {
    stream: TcpStream,
    read_buf: Vec<u8>,
    keep_alive: bool,
}

pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.with_context(|| format!("connecting to {addr}"))?;
        Ok(Self {
            stream,
            read_buf: Vec::with_capacity(16384),
            keep_alive: true,
        })
    }

    pub async fn get(&mut self, path: &str) -> Result<HttpResponse> {
        self.send_request("GET", path, &[]).await
    }

    pub async fn get_with_headers(&mut self, path: &str, extra: &[(&str, &str)]) -> Result<HttpResponse> {
        self.send_request("GET", path, extra).await
    }

    pub async fn post(&mut self, path: &str, body: &[u8]) -> Result<HttpResponse> {
        self.send_request_body("POST", path, &[], body).await
    }

    pub async fn post_with_headers(&mut self, path: &str, extra: &[(&str, &str)], body: &[u8]) -> Result<HttpResponse> {
        self.send_request_body("POST", path, extra, body).await
    }

    async fn send_request(&mut self, method: &str, path: &str, extra_headers: &[(&str, &str)]) -> Result<HttpResponse> {
        self.send_request_body(method, path, extra_headers, &[]).await
    }

    async fn send_request_body(&mut self, method: &str, path: &str, extra_headers: &[(&str, &str)], body: &[u8]) -> Result<HttpResponse> {
        let conn_header = if self.keep_alive { "keep-alive" } else { "close" };
        let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: {conn_header}\r\n");
        for (k, v) in extra_headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        if !body.is_empty() {
            req.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        req.push_str("\r\n");

        self.stream.write_all(req.as_bytes()).await?;
        if !body.is_empty() {
            self.stream.write_all(body).await?;
        }
        self.stream.flush().await?;

        self.read_response().await
    }

    async fn read_response(&mut self) -> Result<HttpResponse> {
        self.read_buf.clear();
        // Read headers: look for \r\n\r\n
        let header_end;
        loop {
            let old_len = self.read_buf.len();
            let mut temp = [0u8; 4096];
            let n = self.stream.read(&mut temp).await?;
            if n == 0 {
                bail!("connection closed by peer before response");
            }
            self.read_buf.extend_from_slice(&temp[..n]);
            if let Some(pos) = self.read_buf[old_len..].windows(4).position(|w| w == b"\r\n\r\n") {
                header_end = old_len + pos;
                break;
            }
        }

        let header_bytes = self.read_buf[..header_end].to_vec();
        let header_str = std::str::from_utf8(&header_bytes).context("non-UTF8 response headers")?;

        // Parse status line
        let first_line = header_str.lines().next().unwrap_or("");
        let status: u16 = first_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Parse headers
        let mut headers = HashMap::new();
        for line in header_str.lines().skip(1) {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_lowercase(), v.trim().to_string());
            }
        }

        // Read body based on Content-Length or Transfer-Encoding
        let body_start = header_end + 4; // skip \r\n\r\n
        let mut body = self.read_buf[body_start..].to_vec();

        if let Some(len_str) = headers.get("content-length") {
            let expected: usize = len_str.parse().context("invalid Content-Length")?;
            while body.len() < expected {
                let mut temp = [0u8; 8192];
                let n = self.stream.read(&mut temp).await?;
                if n == 0 {
                    bail!("connection closed while reading body");
                }
                body.extend_from_slice(&temp[..n]);
            }
            body.truncate(expected);
        } else if let Some(enc) = headers.get("transfer-encoding") {
            if enc == "chunked" {
                // Minimal chunked decoding
                let mut decoded = Vec::new();
                let mut cursor = 0;
                loop {
                    // Find chunk size line
                    while cursor + 2 <= body.len() && body[cursor..cursor + 2] != *b"\r\n" {
                        cursor += 1;
                    }
                    if cursor + 2 > body.len() {
                        // Need more data
                        let old_len = body.len();
                        let mut temp = [0u8; 8192];
                        let n = self.stream.read(&mut temp).await?;
                        if n == 0 {
                            break;
                        }
                        body.extend_from_slice(&temp[..n]);
                        continue;
                    }
                    let size_line = std::str::from_utf8(&body[..cursor]).unwrap_or("0");
                    let chunk_size = usize::from_str_radix(size_line.trim(), 16).unwrap_or(0);
                    cursor += 2; // skip \r\n
                    if chunk_size == 0 {
                        break;
                    }
                    while body.len() < cursor + chunk_size + 2 {
                        let mut temp = [0u8; 8192];
                        let n = self.stream.read(&mut temp).await?;
                        if n == 0 {
                            bail!("connection closed mid-chunk");
                        }
                        body.extend_from_slice(&temp[..n]);
                    }
                    decoded.extend_from_slice(&body[cursor..cursor + chunk_size]);
                    cursor += chunk_size + 2; // skip chunk data + \r\n
                }
                body = decoded;
            }
        }

        Ok(HttpResponse { status, headers, body })
    }
}

impl HttpResponse {
    pub fn body_str(&self) -> &str {
        std::str::from_utf8(&self.body).unwrap_or("")
    }

    pub fn body_json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body).context("failed to parse JSON response")
    }
}
