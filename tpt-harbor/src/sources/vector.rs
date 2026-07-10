//! Harbor/Vector — Pinecone / Weaviate / Qdrant source connector.
//! HTTP/gRPC REST API client for vector database migration to Prism.
//! Discovery lists collections/indexes, snapshot exports vectors in batches.
//! CDC is scope-cut — vector databases typically lack change feeds.

use crate::connector::{ChangeEvent, ConnectorError, SourceConnector, SourceRow};
use crate::schema::{ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::BufMut;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 1_000;

/// Minimal HTTP client supporting REST APIs for Pinecone, Weaviate, Qdrant.
struct VectorRestClient {
    stream: TcpStream,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    host_header: String,
    base_path: String,
    auth_header: Option<String>,
}

impl VectorRestClient {
    async fn connect(pinecone_addr: &str, api_key: Option<&str>) -> Result<Self> {
        let (host, port) = if let Some(colon_idx) = pinecone_addr.find(':') {
            let host_part = pinecone_addr[..colon_idx].to_string();
            (host_part, pinecone_addr[colon_idx + 1..].parse().unwrap_or(443))
        } else {
            (pinecone_addr.to_string(), 443)
        };

        let stream = TcpStream::connect(format!("{}:{}", host, port))
            .await
            .with_context(|| format!("connecting to vector DB at {}:{}", host, port))?;

        Ok(Self {
            stream,
            read_buf: Vec::with_capacity(65536),
            write_buf: Vec::with_capacity(16384),
            host_header: host,
            base_path: "/".to_string(),
            auth_header: api_key.map(|k| format!("Bearer {}", k)),
        })
    }

    async fn request(&mut self, method: &str, path: &str, body: &Value) -> Result<Value> {
        self.write_buf.clear();
        let body_str = body.to_string();
        
        self.write_buf.extend_from_slice(format!("{} {} HTTP/1.1\r\n", method, path).as_bytes());
        self.write_buf.extend_from_slice(b"Host: ");
        self.write_buf.extend_from_slice(self.host_header.as_bytes());
        self.write_buf.extend_from_slice(b"\r\n");
        self.write_buf.extend_from_slice(b"Content-Type: application/json\r\n");
        self.write_buf.extend_from_slice(b"Accept: application/json\r\n");
        if let Some(ref auth) = self.auth_header {
            self.write_buf.extend_from_slice(b"Authorization: ");
            self.write_buf.extend_from_slice(auth.as_bytes());
            self.write_buf.extend_from_slice(b"\r\n");
        }
        self.write_buf.extend_from_slice(b"Content-Length: ");
        self.write_buf.extend_from_slice(body_str.len().to_string().as_bytes());
        self.write_buf.extend_from_slice(b"\r\n");
        self.write_buf.extend_from_slice(b"\r\n");
        self.write_buf.extend_from_slice(body_str.as_bytes());

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;

        // Read HTTP response
        let mut status = 0u16;
        let mut response = String::new();
        
        loop {
            let mut buf = [0u8; 4096];
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            
            let chunk = String::from_utf8_lossy(&buf[..n]);
            
            // Parse status line
            if !response.is_empty() {
                response.push_str(&chunk);
            } else {
                // First chunk contains status
                if let Some(idx) = chunk.find("\r\n\r\n") {
                    let headers = &chunk[..idx];
                    let body_part = &chunk[idx + 4..];
                    
                    for line in headers.lines() {
                        if line.starts_with("HTTP/") {
                            let parts: Vec<&str> = line.split_whitespace().collect();
                            if parts.len() >= 2 {
                                status = parts[1].parse().unwrap_or(0);
                            }
                        }
                    }
                    response.push_str(body_part);
                    response.push_str(&chunk[chunk.len()..]); // Any remaining
                } else {
                    response.push_str(&chunk);
                }
            }
        }

        if status != 200 {
            bail!("Vector DB request failed with status {}: {}", status, response);
        }

        serde_json::from_str(&response).map_err(Into::into)
    }

    async fn get_pinecone_indexes(&mut self) -> Result<Vec<String>> {
        let body = json!({});
        let resp = self.request("GET", "/indexes", &body).await?;
        
        let mut indexes = Vec::new();
        if let Some(indexes_arr) = resp.get("indexes").and_then(|v| v.as_array()) {
            for idx in indexes_arr {
                if let Some(name) = idx.get("name").and_then(|v| v.as_str()) {
                    indexes.push(name.to_string());
                }
            }
        }
        Ok(indexes)
    }

    async fn get_weaviate_schemas(&mut self) -> Result<Vec<String>> {
        let body = json!({});
        let resp = self.request("GET", "/v1/schema", &body).await?;
        
        let mut classes = Vec::new();
        if let Some(classes_arr) = resp.get("classes").and_then(|v| v.as_array()) {
            for cls in classes_arr {
                if let Some(name) = cls.get("class").and_then(|v| v.as_str()) {
                    classes.push(name.to_string());
                }
            }
        }
        Ok(classes)
    }

    async fn get_qdrant_collections(&mut self) -> Result<Vec<String>> {
        let body = json!({});
        let resp = self.request("GET", "/collections", &body).await?;
        
        let mut collections = Vec::new();
        if let Some(result) = resp.get("result").and_then(|v| v.as_object()) {
            if let Some(colls) = result.get("collections").and_then(|v| v.as_array()) {
                for c in colls {
                    if let Some(name) = c.get("name").and_then(|v| v.as_str()) {
                        collections.push(name.to_string());
                    }
                }
            }
        }
        Ok(collections)
    }

    async fn fetch_pinecone_vectors(&mut self, index_name: &str) -> Result<Vec<SourceRow>> {
        let body = json!({
            "vector": null,
            "topK": 1000,
            "includeValues": true,
            "includeMetadata": true
        });
        let resp = self.request("POST", &format!("/{}/query", index_name), &body).await?;
        
        let mut vectors = Vec::new();
        if let Some(matches) = resp.get("matches").and_then(|v| v.as_array()) {
            for m in matches {
                let mut row = Vec::new();
                // Add values as JSON string
                if let Some(values) = m.get("values") {
                    row.push(Some(serde_json::to_vec(values).unwrap_or_default()));
                }
                // Add metadata
                if let Some(metadata) = m.get("metadata") {
                    row.push(Some(serde_json::to_vec(metadata).unwrap_or_default()));
                }
                vectors.push(row);
            }
        }
        Ok(vectors)
    }
}

pub struct VectorSource {
    client: VectorRestClient,
    db_type: VectorDbType,
}

#[derive(Debug, Clone, Copy)]
enum VectorDbType {
    Pinecone,
    Weaviate,
    Qdrant,
}

impl VectorSource {
    pub async fn connect(addr: &str, database: &str, api_key: Option<&str>) -> anyhow::Result<Self> {
        let db_type = match database.to_lowercase().as_str() {
            "pinecone" => VectorDbType::Pinecone,
            "weaviate" => VectorDbType::Weaviate,
            "qdrant" | _ => VectorDbType::Qdrant,
        };
        
        let client = VectorRestClient::connect(addr, api_key).await?;
        Ok(Self { client, db_type })
    }
}

#[async_trait]
impl SourceConnector for VectorSource {
    fn name(&self) -> &'static str {
        "Harbor/Vector"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let names = match self.db_type {
            VectorDbType::Pinecone => self.client.get_pinecone_indexes().await,
            VectorDbType::Weaviate => self.client.get_weaviate_schemas().await,
            VectorDbType::Qdrant => self.client.get_qdrant_collections().await,
        }.map_err(ConnectorError::Other)?;

        let mut tables = Vec::new();
        for name in names {
            // Vector tables have: id, vector, metadata
            tables.push(TableSchema {
                schema: match self.db_type {
                    VectorDbType::Pinecone => "pinecone".to_string(),
                    VectorDbType::Weaviate => "weaviate".to_string(),
                    VectorDbType::Qdrant => "qdrant".to_string(),
                },
                name,
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
                ],
            });
        }

        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let rows = match self.db_type {
            VectorDbType::Pinecone => self.client.fetch_pinecone_vectors(&table.name).await,
            _ => {
                Err(anyhow::anyhow!("Vector DB snapshot requires additional format support"))?
            }
        };

        let mut total: u64 = 0;
        for chunk in rows.chunks(SNAPSHOT_BATCH_SIZE) {
            total += chunk.len() as u64;
            if tx.send(chunk.to_vec()).await.is_err() {
                break;
            }
        }

        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        Err(ConnectorError::Unimplemented { connector: "Harbor/Vector", detail: "Vector DB change feed not yet written" })
    }

    async fn row_checksums(&mut self, _table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        Err(ConnectorError::Unimplemented { connector: "Harbor/Vector", detail: "Vector DB checksums require snapshot implementation" })
    }
}