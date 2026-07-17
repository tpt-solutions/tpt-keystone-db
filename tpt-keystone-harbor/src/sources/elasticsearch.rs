//! Harbor/Search — Elasticsearch source connector. Hand-written HTTP API
//! client using the _cat and _search endpoints. Discovery introspects indices
//! and their mappings, snapshot exports documents via scroll API, CDC is
//! scope-cut — Elasticsearch lacks a standard change-log API.

use crate::connector::{ChangeEvent, ConnectorError, SourceConnector, SourceRow};
use crate::schema::{ColumnSchema, TableSchema};
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

    async fn get_mapping(&mut self, index: &str) -> Result<Value> {
        let resp = self.request("GET", &format!("/{}/_mapping", index_urlencode(index)), &json!(null)).await?;
        
        // Extract properties from mapping
        Ok(resp)
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
        
        let mut tables = Vec::new();
        for index in indices {
            let mapping = self.client.get_mapping(&index).await.map_err(ConnectorError::Other)?;
            
            // Extract field definitions from mapping
            let mut columns = Vec::new();
            
            // _id is the primary key
            columns.push(ColumnSchema {
                name: "_id".to_string(),
                source_type: "keyword".to_string(),
                keystone_type: "TEXT".to_string(),
                nullable: false,
                is_primary_key: true,
            });
            
            // Parse mapping properties
            if let Some(props) = extract_mapping_properties(&mapping, &index) {
                for (field_name, field_type) in props {
                    columns.push(ColumnSchema {
                        name: field_name,
                        source_type: field_type.clone(),
                        keystone_type: map_es_type_to_keystone(&field_type),
                        nullable: true,
                        is_primary_key: false,
                    });
                }
            }
            
            if columns.len() > 1 {
                tables.push(TableSchema {
                    schema: "elasticsearch".to_string(),
                    name: index,
                    columns,
                });
            }
        }
        
        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        // Use scroll API to fetch all documents
        let query = json!({ "query": { "match_all": {} }, "size": SNAPSHOT_BATCH_SIZE as i64 });
        
        let (scroll_id, hits) = self.client.create_scroll_search(&table.name, &query).await.map_err(ConnectorError::Other)?;
        
        let mut total: u64 = 0;
        let mut all_hits = hits;
        
        loop {
            let batch: Vec<SourceRow> = all_hits.iter().map(|hit| {
                let mut row = Vec::new();
                // Add _id
                if let Some(id) = hit.get("_id").and_then(|v| v.as_str()) {
                    row.push(Some(id.as_bytes().to_vec()));
                }
                // Add _source fields
                if let Some(source) = hit.get("_source") {
                    for col in &table.columns {
                        if col.name != "_id" {
                            if let Some(val) = source.get(&col.name).or_else(|| source.get(col.name.trim_start_matches('_'))) {
                                row.push(Some(serde_json::to_vec(val).unwrap_or_default()));
                            } else {
                                row.push(None);
                            }
                        }
                    }
                }
                row
            }).collect();
            
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
        let query = json!({ "query": { "match_all": {} }, "size": SNAPSHOT_BATCH_SIZE as i64, "_source": false });
        let (scroll_id, hits) = self.client.create_scroll_search(&table.name, &query).await.map_err(ConnectorError::Other)?;
        
        let mut checksums = Vec::new();
        let mut all_hits = hits;
        
        loop {
            for hit in &all_hits {
                let row: SourceRow = if let Some(id) = hit.get("_id").and_then(|v| v.as_str()) {
                    vec![Some(id.as_bytes().to_vec())]
                } else {
                    vec![]
                };
                checksums.push(crate::verify::hash_row(&row));
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

fn extract_mapping_properties(mapping: &Value, index: &str) -> Option<Vec<(String, String)>> {
    let mut props = Vec::new();
    
    // Elasticsearch mapping structure: { index: { mappings: { properties: { field: { type: "text" } } } } }
    if let Some(props_val) = mapping.get(index)
        .and_then(|v| v.get("mappings"))
        .and_then(|v| v.get("properties"))
    {
        if let Some(obj) = props_val.as_object() {
            for (field_name, field_def) in obj {
                let field_type = field_def.get("type").and_then(|v| v.as_str()).unwrap_or("text").to_string();
                props.push((field_name.clone(), field_type));
            }
        }
    }
    
    Some(props)
}

fn map_es_type_to_keystone(es_type: &str) -> String {
    match es_type {
        "text" | "keyword" | "string" => "TEXT".to_string(),
        "long" => "BIGINT".to_string(),
        "integer" => "INTEGER".to_string(),
        "short" => "SMALLINT".to_string(),
        "double" | "float" | "half_float" | "scaled_float" => "DOUBLE PRECISION".to_string(),
        "boolean" => "BOOLEAN".to_string(),
        "date" => "BIGINT".to_string(), // Unix timestamp
        "object" => "JSONB".to_string(),
        "nested" => "JSONB".to_string(), // Simplified - would need array handling
        _ => "JSONB".to_string(), // Unknown types as JSON
    }.to_string()
}