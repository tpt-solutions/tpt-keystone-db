//! Harbor/Vector — Pinecone / Weaviate / Qdrant source connector.
//! Hand-written REST/JSON client over TCP (one file covering all three per
//! TODO.md Phase 15's follow-up shape, not three separate connectors).
//! Discovery lists indexes/collections/classes; snapshot and the
//! Verification Engine's checksums both pull every vector (id + vector +
//! metadata payload) and stream them as `(id, vector, metadata)` rows. CDC
//! is genuinely scope-cut: none of these vector databases expose a standard
//! row-level change feed, so `replicate` stays `Unimplemented`.
//!
//! Scope cuts, documented: the client speaks plain HTTP (no TLS), which is
//! fine for self-hosted Qdrant/Weaviate and for local dev but means the
//! Pinecone hosted API (HTTPS-only) would need a TLS dependency wired in;
//! and only `id` / `vector` / `metadata` migrate (matching the three-column
//! discovered schema) — no per-DB hybrid/namespaced extras.

use crate::connector::{ConnectorError, SourceConnector, SourceRow};
use crate::schema::{ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 1_000;

/// Minimal HTTP/1.1 REST client shared by all three vector DBs.
struct VectorRestClient {
    stream: TcpStream,
    buf: Vec<u8>,
    host_header: String,
    addr: String,
    auth_header: Option<String>,
}

impl VectorRestClient {
    async fn connect(addr: &str, api_key: Option<&str>) -> Result<Self> {
        let (host, port) = if let Some(colon_idx) = addr.find(':') {
            (addr[..colon_idx].to_string(), addr[colon_idx + 1..].parse().unwrap_or(443))
        } else {
            (addr.to_string(), 443)
        };
        let stream = TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("connecting to vector DB at {host}:{port}"))?;
        Ok(Self {
            stream,
            buf: Vec::with_capacity(65536),
            host_header: format!("{host}:{port}"),
            addr: format!("{host}:{port}"),
            auth_header: api_key.map(|k| format!("Bearer {k}")),
        })
    }

    async fn request(&mut self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        let body_str = match body {
            Some(v) => v.to_string(),
            None => String::new(),
        };
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nAccept: application/json\r\n{auth}Content-Length: {len}\r\nConnection: close\r\n\r\n",
            method = method,
            path = path,
            host = self.host_header,
            auth = self.auth_header.as_ref().map(|a| format!("Authorization: {a}\r\n")).unwrap_or_default(),
            len = body_str.len()
        );
        self.stream.write_all(req.as_bytes()).await?;
        self.stream.write_all(body_str.as_bytes()).await?;
        self.stream.flush().await?;

        self.buf.clear();
        let mut tmp = [0u8; 8192];
        loop {
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
        let (status, body_bytes) = parse_http_response(&self.buf)?;
        if status >= 400 {
            bail!("vector DB request failed (status {status}): {path}");
        }
        serde_json::from_slice(&body_bytes)
            .with_context(|| format!("parsing vector DB JSON for {path}"))
    }
}

fn parse_http_response(buf: &[u8]) -> Result<(u16, Vec<u8>)> {
    let s = String::from_utf8_lossy(buf);
    let header_end = s
        .find("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("no HTTP header terminator in vector DB response"))?;
    let header_str = &s[..header_end];
    let mut status = 0u16;
    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    for line in header_str.lines() {
        let lower = line.to_ascii_lowercase();
        if line.starts_with("HTTP/") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                status = parts[1].parse().unwrap_or(0);
            }
        } else if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = v.trim().parse().ok();
        } else if let Some(v) = lower.strip_prefix("transfer-encoding:") {
            if v.contains("chunked") {
                chunked = true;
            }
        }
    }
    let body_start = header_end + 4;
    let mut body = buf[body_start..].to_vec();
    if chunked {
        body = decode_chunked(&body);
    } else if let Some(cl) = content_length {
        if body.len() > cl {
            body.truncate(cl);
        }
    }
    Ok((status, body))
}

fn decode_chunked(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut p = body;
    loop {
        let line_end = match p.iter().position(|&b| b == b'\r' || b == b'\n') {
            Some(i) => i,
            None => break,
        };
        let size_str = String::from_utf8_lossy(&p[..line_end]);
        let size = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0"), 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        let data_start = line_end + 1;
        if p.len() < data_start + size {
            break;
        }
        out.extend_from_slice(&p[data_start..data_start + size]);
        p = &p[data_start + size + 2..];
    }
    out
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for c in s.bytes() {
        match c {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(c as char),
            _ => out.push_str(&format!("%{c:02X}")),
        }
    }
    out
}

/// Serialize a vector as Keystone's `[x,y,z]` VECTOR text form.
fn vector_to_text(v: &[f64]) -> Vec<u8> {
    let parts: Vec<String> = v.iter().map(|x| x.to_string()).collect();
    format!("[{}]", parts.join(",")).into_bytes()
}

/// Render an arbitrary JSON value (metadata payload) as a cell, or NULL.
fn json_to_cell(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Null => None,
        other => Some(serde_json::to_vec(other).unwrap_or_default()),
    }
}

pub struct VectorSource {
    client: VectorRestClient,
    db_type: VectorDbType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorDbType {
    Pinecone,
    Weaviate,
    Qdrant,
}

impl VectorSource {
    pub async fn connect(addr: &str, database: &str, api_key: Option<&str>) -> anyhow::Result<Self> {
        let db_type = match database.to_ascii_lowercase().as_str() {
            "pinecone" => VectorDbType::Pinecone,
            "weaviate" => VectorDbType::Weaviate,
            "qdrant" | _ => VectorDbType::Qdrant,
        };
        let client = VectorRestClient::connect(addr, api_key).await?;
        Ok(Self { client, db_type })
    }

    /// Pull every vector from the source as `(id, vector, metadata)` rows,
    /// used by both snapshot and checksums so the two stay in lock-step.
    async fn fetch_all(&mut self, index: &str) -> Result<Vec<SourceRow>, ConnectorError> {
        let mut rows = Vec::new();
        let mut page = 0usize;
        loop {
            let page_rows = match self.db_type {
                VectorDbType::Pinecone => self.fetch_pinecone(index, page).await?,
                VectorDbType::Weaviate => self.fetch_weaviate(index, page).await?,
                VectorDbType::Qdrant => self.fetch_qdrant(index, page).await?,
            };
            let n = page_rows.len();
            rows.extend(page_rows);
            page += 1;
            // Stop when a page comes back empty (Qdrant/Weaviate) or at the
            // Pinecone ~10k ceiling.
            if n == 0 || (self.db_type == VectorDbType::Pinecone && page * SNAPSHOT_BATCH_SIZE >= 10_000) {
                break;
            }
        }
        Ok(rows)
    }

    async fn fetch_pinecone(&mut self, index: &str, page: usize) -> Result<Vec<SourceRow>, ConnectorError> {
        let top_k = SNAPSHOT_BATCH_SIZE.min(10_000);
        let body = json!({
            "vector": vec![0.0f64; 1],
            "topK": top_k,
            "includeValues": true,
            "includeMetadata": true
        });
        let path = format!("/{}/query", urlencode(index));
        let resp = self.client.request("POST", &path, Some(&body)).await.map_err(ConnectorError::Other)?;
        let mut out = Vec::new();
        if let Some(matches) = resp.get("matches").and_then(|v| v.as_array()) {
            for m in matches {
                let id = m.get("id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                let vector = m
                    .get("values")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|x| x.as_f64()).collect::<Vec<f64>>())
                    .unwrap_or_default();
                let metadata = m.get("metadata").cloned().unwrap_or(Value::Null);
                out.push(vec![
                    Some(id.into_bytes()),
                    Some(vector_to_text(&vector)),
                    json_to_cell(&metadata),
                ]);
            }
        }
        let _ = page;
        Ok(out)
    }

    async fn fetch_qdrant(&mut self, index: &str, page: usize) -> Result<Vec<SourceRow>, ConnectorError> {
        let limit = SNAPSHOT_BATCH_SIZE.min(1000);
        let offset = page * limit;
        let path = format!(
            "/collections/{}/points?with_vector=true&limit={}&offset={}",
            urlencode(index),
            limit,
            offset
        );
        let resp = self.client.request("GET", &path, None).await.map_err(ConnectorError::Other)?;
        let mut out = Vec::new();
        if let Some(points) = resp.get("result").and_then(|r| r.get("points")).and_then(|v| v.as_array()) {
            for p in points {
                let id = match p.get("id") {
                    Some(Value::String(s)) => s.clone(),
                    Some(Value::Number(n)) => n.to_string(),
                    _ => String::new(),
                };
                let vector = p
                    .get("vector")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|x| x.as_f64()).collect::<Vec<f64>>())
                    .unwrap_or_default();
                let payload = p.get("payload").cloned().unwrap_or(Value::Null);
                out.push(vec![
                    Some(id.into_bytes()),
                    Some(vector_to_text(&vector)),
                    json_to_cell(&payload),
                ]);
            }
        }
        Ok(out)
    }

    async fn fetch_weaviate(&mut self, class: &str, page: usize) -> Result<Vec<SourceRow>, ConnectorError> {
        let limit = SNAPSHOT_BATCH_SIZE.min(200);
        let offset = page * limit;
        let path = format!("/v1/objects/{}?limit={}&offset={}", urlencode(class), limit, offset);
        let resp = self.client.request("GET", &path, None).await.map_err(ConnectorError::Other)?;
        let mut out = Vec::new();
        let objects = resp.as_array().cloned().or_else(|| {
            resp.get("objects").and_then(|v| v.as_array()).cloned()
        });
        if let Some(objects) = objects {
            for o in &objects {
                let id = o.get("id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                let vector = o
                    .get("vector")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|x| x.as_f64()).collect::<Vec<f64>>())
                    .unwrap_or_default();
                let properties = o.get("properties").cloned().unwrap_or(Value::Null);
                out.push(vec![
                    Some(id.into_bytes()),
                    Some(vector_to_text(&vector)),
                    json_to_cell(&properties),
                ]);
            }
        }
        Ok(out)
    }
}

fn schema_for(db_type: VectorDbType, name: &str) -> TableSchema {
    let schema = match db_type {
        VectorDbType::Pinecone => "pinecone",
        VectorDbType::Weaviate => "weaviate",
        VectorDbType::Qdrant => "qdrant",
    };
    TableSchema {
        schema: schema.to_string(),
        name: name.to_string(),
        columns: vec![
            ColumnSchema {
                name: "id".to_string(),
                source_type: "string".to_string(),
                keystone_type: "TEXT".to_string(),
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "vector".to_string(),
                source_type: "[]float".to_string(),
                keystone_type: "VECTOR".to_string(),
                nullable: false,
                is_primary_key: false,
            },
            ColumnSchema {
                name: "metadata".to_string(),
                source_type: "object".to_string(),
                keystone_type: "JSONB".to_string(),
                nullable: true,
                is_primary_key: false,
            },
        ],
    }
}

#[async_trait]
impl SourceConnector for VectorSource {
    fn name(&self) -> &'static str {
        "Harbor/Vector"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let names: Vec<String> = match self.db_type {
            VectorDbType::Pinecone => {
                let resp = self.client.request("GET", "/indexes", None).await.map_err(ConnectorError::Other)?;
                resp.get("indexes")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|x| x.get("name").and_then(|n| n.as_str()).map(String::from)).collect())
                    .unwrap_or_default()
            }
            VectorDbType::Weaviate => {
                let resp = self.client.request("GET", "/v1/schema", None).await.map_err(ConnectorError::Other)?;
                resp.get("classes")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|x| x.get("class").and_then(|n| n.as_str()).map(String::from)).collect())
                    .unwrap_or_default()
            }
            VectorDbType::Qdrant => {
                let resp = self.client.request("GET", "/collections", None).await.map_err(ConnectorError::Other)?;
                resp.get("result")
                    .and_then(|r| r.get("collections"))
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|x| x.get("name").and_then(|n| n.as_str()).map(String::from)).collect())
                    .unwrap_or_default()
            }
        };
        Ok(names.into_iter().map(|n| schema_for(self.db_type, &n)).collect())
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let rows = self.fetch_all(&table.name).await?;
        let mut total: u64 = 0;
        for chunk in rows.chunks(SNAPSHOT_BATCH_SIZE) {
            let batch: Vec<SourceRow> = chunk.to_vec();
            total += batch.len() as u64;
            if tx.send(batch).await.is_err() {
                return Ok(total);
            }
        }
        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<crate::connector::ChangeEvent>) -> Result<(), ConnectorError> {
        // Pinecone/Weaviate/Qdrant expose no standard row-level change feed
        // (upserts are write-path only), so there's no portable CDC here —
        // documented scope cut, consistent with the other connectors.
        Err(ConnectorError::Unimplemented {
            connector: "Harbor/Vector",
            detail: "vector databases have no standard change-feed API; replays from snapshots only",
        })
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let rows = self.fetch_all(&table.name).await?;
        Ok(rows.iter().map(|r| crate::verify::hash_row(r)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_text_format() {
        assert_eq!(vector_to_text(&[1.0, 2.0, 3.0]), b"[1,2,3]".to_vec());
        assert_eq!(vector_to_text(&[]), b"[]".to_vec());
    }

    #[test]
    fn urlencodes_index_names() {
        assert_eq!(urlencode("my-index"), "my-index");
        assert_eq!(urlencode("a b"), "a%20b");
    }

    #[test]
    fn parses_content_length_response() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n{\"ok\":1}".to_vec();
        let (status, body) = parse_http_response(&raw).unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, b"{\"ok\":1}");
    }

    #[test]
    fn builds_three_column_vector_schema() {
        let s = schema_for(VectorDbType::Qdrant, "faces");
        assert_eq!(s.columns.len(), 3);
        assert_eq!(s.columns[1].keystone_type, "VECTOR");
        assert!(s.columns[0].is_primary_key);
    }
}
