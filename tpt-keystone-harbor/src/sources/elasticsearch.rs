//! Harbor/Search — Elasticsearch source connector. Hand-written HTTP API
//! client using the _cat and _search endpoints. Discovery lists indices; each
//! becomes a fixed `(_id TEXT PK, doc JSON)` table, GIN-indexed, and
//! snapshot/checksums serialize each hit's whole `_source` to the `doc`
//! column rather than flattening mapping properties into typed columns —
//! consistent with Harbor/Mongo (`sources::mongodb`) and more faithful to a
//! source where mappings can include unindexed/dynamic fields no static
//! per-field schema captures. CDC is scope-cut — Elasticsearch lacks a
//! standard change-log API.

use crate::connector::{ChangeEvent, ConnectorError, SourceConnector, SourceRow};
use crate::schema::{ColumnSchema, IndexSpec, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 5_000;

/// Minimal Elasticsearch HTTP client.
struct EsClient {
    stream: TcpStream,
    write_buf: Vec<u8>,
    host: String,
    scroll_keep_alive: String,
}

impl EsClient {
    async fn connect(addr: &str) -> Result<Self> {
        let (host, port) = if let Some(colon_idx) = addr.find(':') {
            let host_part = addr[..colon_idx].to_string();
            (host_part, addr[colon_idx + 1..].parse().unwrap_or(9200))
        } else {
            (addr.to_string(), 9200)
        };

        let stream = TcpStream::connect(format!("{}:{}", host, port))
            .await
            .with_context(|| format!("connecting to Elasticsearch at {}:{}", host, port))?;

        Ok(Self {
            stream,
            write_buf: Vec::with_capacity(16384),
            host: format!("{}:{}", host, port),
            scroll_keep_alive: "5m".to_string(),
        })
    }

    async fn request(&mut self, method: &str, path: &str, body: &Value) -> Result<Value> {
        let body_str = if body.is_null() { String::new() } else { body.to_string() };
        
        self.write_buf.clear();
        self.write_buf.extend_from_slice(format!("{} {} HTTP/1.1\r\n", method, path).as_bytes());
        self.write_buf.extend_from_slice(b"Host: ");
        self.write_buf.extend_from_slice(self.host.as_bytes());
        self.write_buf.extend_from_slice(b"\r\n");
        self.write_buf.extend_from_slice(b"Content-Type: application/json\r\n");
        self.write_buf.extend_from_slice(b"Accept: application/json\r\n");
        if !body_str.is_empty() {
            self.write_buf.extend_from_slice(b"Content-Length: ");
            self.write_buf.extend_from_slice(body_str.len().to_string().as_bytes());
            self.write_buf.extend_from_slice(b"\r\n");
        }
        self.write_buf.extend_from_slice(b"\r\n");
        self.write_buf.extend_from_slice(body_str.as_bytes());

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;

        // Read HTTP response
        let mut status = 0u16;
        let mut response = String::new();
        
        loop {
            let mut buf = [0u8; 4096];
            let n = match self.stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => break,
            };
            
            let chunk = String::from_utf8_lossy(&buf[..n]);
            
            if let Some(idx) = chunk.find("\r\n\r\n") {
                let headers = &chunk[..idx];
                let body = &chunk[idx + 4..];
                for line in headers.lines() {
                    if line.starts_with("HTTP/") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            status = parts[1].parse().unwrap_or(0);
                        }
                    }
                }
                response.push_str(body);
                break;
            } else {
                response.push_str(&chunk);
            }
        }

        if status != 200 {
            bail!("Elasticsearch request failed with status {}: {}", status, response);
        }

        // Read any remaining content
        loop {
            let mut buf = [0u8; 4096];
            match self.stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => response.push_str(&String::from_utf8_lossy(&buf[..n])),
                Err(_) => break,
            }
        }

        serde_json::from_str(&response).map_err(Into::into)
    }

    async fn get_indices(&mut self) -> Result<Vec<String>> {
        // Use _cat/indices API for simpler parsing
        let resp = self.request("GET", "/_cat/indices?format=json&h=index", &json!(null)).await?;
        
        let mut indices = Vec::new();
        if let Some(arr) = resp.as_array() {
            for item in arr {
                if let Some(index) = item.get("index").and_then(|v| v.as_str()) {
                    if !index.starts_with('.') {
                        indices.push(index.to_string());
                    }
                }
            }
        }
        Ok(indices)
    }

    async fn create_scroll_search(&mut self, index: &str, body: &Value) -> Result<(String, Vec<Value>)> {
        let resp = self.request(
            "POST", 
            &format!("/{}/_search?scroll={}", index_urlencode(index), self.scroll_keep_alive),
            body
        ).await?;
        
        let scroll_id = resp.get("scroll_id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let hits = resp.get("hits")
            .and_then(|v| v.get("hits"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        
        Ok((scroll_id, hits))
    }

    async fn scroll_next(&mut self, scroll_id: &str) -> Result<Vec<Value>> {
        let body = json!({ "scroll": self.scroll_keep_alive, "scroll_id": scroll_id });
        let resp = self.request("POST", "/_search/scroll", &body).await?;
        
        let hits = resp.get("hits")
            .and_then(|v| v.get("hits"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        
        Ok(hits)
    }

    async fn clear_scroll(&mut self, scroll_id: &str) -> Result<()> {
        let body = json!({ "scroll_id": scroll_id });
        let _ = self.request("DELETE", "/_search/scroll", &body).await;
        Ok(())
    }
}

fn index_urlencode(s: &str) -> String {
    // Simple encoding for index names
    s.replace("/", "%2F").replace(" ", "%20")
}

pub struct ElasticsearchSource {
    client: EsClient,
}

impl ElasticsearchSource {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let client = EsClient::connect(addr).await?;
        Ok(Self { client })
    }
}

#[async_trait]
impl SourceConnector for ElasticsearchSource {
    fn name(&self) -> &'static str {
        "Harbor/Search"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let indices = self.client.get_indices().await.map_err(ConnectorError::Other)?;
        Ok(indices.into_iter().map(|index| document_table_schema(&index)).collect())
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        // Use scroll API to fetch all documents, `_source` included so the
        // whole hit body lands in the `doc` column.
        let query = json!({ "query": { "match_all": {} }, "size": SNAPSHOT_BATCH_SIZE as i64 });

        let (scroll_id, hits) = self.client.create_scroll_search(&table.name, &query).await.map_err(ConnectorError::Other)?;

        let mut total: u64 = 0;
        let mut all_hits = hits;

        loop {
            let batch: Vec<SourceRow> = all_hits.iter().map(hit_to_row).collect();

            total += batch.len() as u64;
            if !batch.is_empty() {
                if tx.send(batch).await.is_err() {
                    break;
                }
            }
            
            if all_hits.len() < SNAPSHOT_BATCH_SIZE {
                break;
            }
            
            all_hits = self.client.scroll_next(&scroll_id).await.map_err(ConnectorError::Other)?;
            if all_hits.is_empty() {
                break;
            }
        }
        
        self.client.clear_scroll(&scroll_id).await.ok();
        
        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        Err(ConnectorError::Unimplemented { connector: "Harbor/Search", detail: "Elasticsearch change feed not yet written" })
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        // `_source` included (unlike a prior id-only version of this query)
        // so the checksum actually covers document content, not just which
        // ids exist.
        let query = json!({ "query": { "match_all": {} }, "size": SNAPSHOT_BATCH_SIZE as i64 });
        let (scroll_id, hits) = self.client.create_scroll_search(&table.name, &query).await.map_err(ConnectorError::Other)?;

        let mut checksums = Vec::new();
        let mut all_hits = hits;

        loop {
            for hit in &all_hits {
                checksums.push(crate::verify::hash_row(&hit_to_row(hit)));
            }
            
            if all_hits.len() < SNAPSHOT_BATCH_SIZE {
                break;
            }
            
            all_hits = self.client.scroll_next(&scroll_id).await.map_err(ConnectorError::Other)?;
            if all_hits.is_empty() {
                break;
            }
        }
        
        self.client.clear_scroll(&scroll_id).await.ok();
        
        Ok(checksums)
    }
}

/// The fixed table shape every Elasticsearch index migrates to: the whole
/// hit `_source` preserved as JSON (see module doc), GIN-indexed.
fn document_table_schema(index: &str) -> TableSchema {
    TableSchema {
        schema: "elasticsearch".to_string(),
        name: index.to_string(),
        columns: vec![
            ColumnSchema { name: "_id".to_string(), source_type: "keyword".to_string(), keystone_type: "TEXT".to_string(), nullable: false, is_primary_key: true },
            ColumnSchema { name: "doc".to_string(), source_type: "object".to_string(), keystone_type: "JSONB".to_string(), nullable: false, is_primary_key: false },
        ],
        indexes: vec![IndexSpec { columns: vec!["doc".to_string()], using: "GIN".to_string(), with: vec![] }],
        topic_partitions: None,
    }
}

/// One `_search`/scroll hit → a `(_id, doc)` row, `doc` the hit's whole
/// `_source` serialized as JSON text.
fn hit_to_row(hit: &Value) -> SourceRow {
    let id = hit.get("_id").and_then(|v| v.as_str()).map(|s| s.as_bytes().to_vec());
    let body = hit.get("_source").map(|source| serde_json::to_vec(source).unwrap_or_default());
    vec![id, body]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_schema_is_id_plus_json_with_gin_index() {
        let t = document_table_schema("orders");
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[0].name, "_id");
        assert!(t.columns[0].is_primary_key);
        assert_eq!(t.columns[1].name, "doc");
        assert_eq!(t.columns[1].keystone_type, "JSONB");
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(t.indexes[0].using, "GIN");
    }

    #[test]
    fn hit_to_row_preserves_whole_source_as_json() {
        let hit = json!({ "_id": "abc123", "_source": { "name": "Ada", "age": 30 } });
        let row = hit_to_row(&hit);
        assert_eq!(row[0], Some(b"abc123".to_vec()));
        let body: Value = serde_json::from_slice(&row[1].clone().unwrap()).unwrap();
        assert_eq!(body["name"], "Ada");
        assert_eq!(body["age"], 30);
    }

    #[test]
    fn hit_to_row_handles_missing_source() {
        let hit = json!({ "_id": "abc123" });
        let row = hit_to_row(&hit);
        assert_eq!(row[0], Some(b"abc123".to_vec()));
        assert_eq!(row[1], None);
    }
}