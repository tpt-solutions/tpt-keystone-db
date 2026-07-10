//! Harbor/TimeSeries — InfluxDB source connector. Hand-written InfluxDB
//! line protocol and HTTP API client over TCP/HTTPS. Discovery introspects
//! measurements via `/query` endpoint, snapshot reads via HTTP query, CDC is
//! scope-cut to `Unimplemented` — InfluxDB doesn't have a standard change-log
//! API like Postgres WAL.

use crate::connector::{ChangeEvent, ConnectorError, SourceConnector, SourceRow};
use crate::schema::{from_influx_type, ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::BufMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 5_000;

/// Minimal InfluxDB line protocol encoder for writes.
fn influx_encode_line(measurement: &str, tags: &[(&str, &str)], fields: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(measurement.as_bytes());
    for (k, v) in tags {
        out.push(b',');
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(v.as_bytes());
    }
    out.push(b' ');
    for (i, (k, v)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(v.as_bytes());
    }
    out.push(b'\n');
    out
}

/// Minimal InfluxDB HTTP client.
struct InfluxConn {
    stream: TcpStream,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    addr: String,
    database: String,
}

impl InfluxConn {
    async fn connect(addr: &str, database: &str) -> Result<Self> {
        // Parse addr - handle both http://host:port and host:port formats
        let (host, port) = if addr.starts_with("http://") {
            let rest = &addr[7..];
            let parts: Vec<&str> = rest.split(':').collect();
            if parts.len() == 2 {
                (parts[0].to_string(), parts[1].parse().unwrap_or(8086))
            } else {
                (rest.to_string(), 8086)
            }
        } else if let Some(colon_idx) = addr.find(':') {
            (addr[..colon_idx].to_string(), addr[colon_idx + 1..].parse().unwrap_or(8086))
        } else {
            (addr.to_string(), 8086)
        };

        let stream = TcpStream::connect(format!("{}:{}", host, port))
            .await
            .with_context(|| format!("connecting to InfluxDB at {}:{}", host, port))?;

        Ok(Self {
            stream,
            read_buf: Vec::with_capacity(65536),
            write_buf: Vec::with_capacity(16384),
            addr: format!("{}:{}", host, port),
            database: database.to_string(),
        })
    }

    async fn http_request(&mut self, method: &str, path: &str, body: &[u8]) -> Result<(u16, String)> {
        self.write_buf.clear();
        self.write_buf.extend_from_slice(format!("{} {} HTTP/1.1\r\n", method, path).as_bytes());
        self.write_buf.extend_from_slice(b"Host: ");
        self.write_buf.extend_from_slice(self.addr.as_bytes());
        self.write_buf.extend_from_slice(b"\r\n");
        if !body.is_empty() {
            self.write_buf.extend_from_slice(b"Content-Length: ");
            self.write_buf.extend_from_slice(body.len().to_string().as_bytes());
            self.write_buf.extend_from_slice(b"\r\n");
        }
        self.write_buf.extend_from_slice(b"Accept: application/json\r\n");
        self.write_buf.extend_from_slice(b"\r\n");
        self.write_buf.extend_from_slice(body);

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;

        // Read HTTP response
        self.read_buf.clear();
        let mut headers_done = false;
        let mut content_length: usize = 0;
        let mut status = 0u16;
        let mut response_body = String::new();

        while !headers_done || self.read_buf.len() < content_length {
            let mut buf = [0u8; 4096];
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            for b in buf[..n].iter() {
                if !headers_done {
                    self.read_buf.push(*b);
                } else {
                    response_body.push(*b as char);
                }
            }

            if !headers_done && self.read_buf.len() >= 4 {
                // Check for \r\n\r\n
                let end = self.read_buf.windows(4)
                    .position(|w| w == b"\r\n\r\n");
                if let Some(idx) = end {
                    headers_done = true;
                    content_length = self.parse_headers(&self.read_buf[..idx], &mut status);
                    response_body = String::from_utf8_lossy(&self.read_buf[idx + 4..]).to_string();
                    self.read_buf.clear();
                }
            }
        }

        if self.read_buf.len() < content_length {
            // Read remaining body in one go
            let remaining = content_length - response_body.len();
            let mut buf = vec![0u8; remaining];
            self.stream.read_exact(&mut buf).await?;
            response_body = String::from_utf8_lossy(&buf).to_string();
        }

        Ok((status, response_body))
    }

    fn parse_headers(&self, header_bytes: &[u8], status: &mut u16) -> usize {
        let header_str = String::from_utf8_lossy(header_bytes);
        let mut content_length = 0;
        for line in header_str.lines() {
            if line.starts_with("HTTP/") {
                // Parse status code from "HTTP/1.1 200 OK"
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    *status = parts[1].parse().unwrap_or(0);
                }
            } else if let Some(val) = line.strip_prefix("Content-Length:") {
                content_length = val.trim().parse().unwrap_or(0);
            }
        }
        content_length
    }

    async fn query_sql(&mut self, q: &str) -> Result<String> {
        let path = format!("/query?db={}&q={}", 
            urlencode(&self.database), 
            urlencode(q));
        let (status, body) = self.http_request("GET", &path, &[]).await?;
        if status != 200 {
            bail!("InfluxDB query failed with status {}", status);
        }
        Ok(body)
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(c),
            _ => {
                out.push('%');
                out.push_str(&hex_encode(c as u8));
            }
        }
    }
    out
}

fn hex_encode(b: u8) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    format!("{}{}", 
        (HEX[(b >> 4) as usize]) as char, 
        (HEX[(b & 0x0F) as usize]) as char)
}

pub struct InfluxDbSource {
    conn: InfluxConn,
}

impl InfluxDbSource {
    pub async fn connect(addr: &str, database: &str) -> anyhow::Result<Self> {
        let conn = InfluxConn::connect(addr, database).await?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl SourceConnector for InfluxDbSource {
    fn name(&self) -> &'static str {
        "Harbor/TimeSeries"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        // Query for measurements
        let body = self.conn.query_sql("SHOW MEASUREMENTS").await.map_err(ConnectorError::Other)?;
        
        let mut tables = Vec::new();
        
        // Parse JSON response to extract measurement names
        if let Some(measurements) = parse_json_string_array(&body, "measurements") {
            for measurement in measurements {
                // Get column info via SHOW FIELD KEYS and SHOW TAG KEYS
                let fields_body = self.conn.query_sql(&format!("SHOW FIELD KEYS FROM {}", measurement)).await.map_err(ConnectorError::Other)?;
                let tags_body = self.conn.query_sql(&format!("SHOW TAG KEYS FROM {}", measurement)).await.map_err(ConnectorError::Other)?;
                
                let mut columns = Vec::new();
                
                // time column is always present
                columns.push(ColumnSchema {
                    name: "time".to_string(),
                    source_type: "timestamp".to_string(),
                    keystone_type: "BIGINT".to_string(),
                    nullable: false,
                    is_primary_key: true,
                });
                
                // Parse field keys
                if let Some(fields) = parse_json_kv_array(&fields_body, "fields") {
                    for (name, _influx_type) in fields {
                        columns.push(ColumnSchema {
                            name,
                            source_type: "field".to_string(),
                            keystone_type: "DOUBLE PRECISION".to_string(), // Default for numeric
                            nullable: true,
                            is_primary_key: false,
                        });
                    }
                }
                
                // Parse tag keys
                if let Some(tags) = parse_json_string_array(&tags_body, "tags") {
                    for tag in tags {
                        columns.push(ColumnSchema {
                            name: tag,
                            source_type: "tag".to_string(),
                            keystone_type: "TEXT".to_string(),
                            nullable: true,
                            is_primary_key: false,
                        });
                    }
                }
                
                if columns.len() > 1 {
                    tables.push(TableSchema {
                        schema: self.conn.database.clone(),
                        name: measurement,
                        columns,
                    });
                }
            }
        }
        
        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let measurement = &table.name;
        
        // Query all data for this measurement
        let body = self.conn.query_sql(&format!("SELECT * FROM {}", measurement)).await.map_err(ConnectorError::Other)?;
        
        // Parse JSON response into rows
        let rows = parse_influx_query_results(&body);
        
        let mut total: u64 = 0;
        for chunk in rows.chunks(SNAPSHOT_BATCH_SIZE) {
            let batch: Vec<SourceRow> = chunk.iter().map(|row| {
                row.iter().map(|v| Some(v.clone())).collect()
            }).collect();
            total += batch.len() as u64;
            if tx.send(batch).await.is_err() {
                break;
            }
        }
        
        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        Err(ConnectorError::Unimplemented { connector: "Harbor/TimeSeries", detail: "InfluxDB change feed not yet written" })
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let body = self.conn.query_sql(&format!("SELECT * FROM {}", table.name)).await.map_err(ConnectorError::Other)?;
        let rows = parse_influx_query_results(&body);
        Ok(rows.iter().map(|row| crate::verify::hash_row(&row.iter().map(|v| Some(v.clone())).collect::<SourceRow>())).collect())
    }
}

fn parse_json_string_array(body: &str, key: &str) -> Option<Vec<String>> {
    let mut result = Vec::new();
    let key_pattern = format!("\"{}\":", key);
    if let Some(start) = body.find(&key_pattern) {
        let slice = &body[start + key_pattern.len()..];
        // Find array start
        if let Some(arr_start) = slice.find('[') {
            let slice2 = &slice[arr_start..];
            // Parse array elements
            let mut in_string = false;
            let mut current = String::new();
            for c in slice2.chars() {
                match c {
                    '"' => in_string = !in_string,
                    ',' | ']' if !in_string => {
                        if !current.is_empty() {
                            result.push(current.trim().to_string());
                            current.clear();
                        }
                    },
                    _ if in_string => current.push(c),
                    _ => {}
                }
            }
            if !current.is_empty() {
                result.push(current.trim().to_string());
            }
            return Some(result);
        }
    }
    None
}

fn parse_json_kv_array(body: &str, key: &str) -> Option<Vec<(String, String)>> {
    let mut result = Vec::new();
    let key_pattern = format!("\"{}\":", key);
    if let Some(start) = body.find(&key_pattern) {
        let slice = &body[start + key_pattern.len()..];
        if let Some(arr_start) = slice.find('[') {
            let slice2 = &slice[arr_start..];
            // Simple parsing for {"name":"col","type":"float"} pairs
            let mut pos = 0;
            while pos < slice2.len() {
                if let Some(obj_start) = slice2[pos..].find('{') {
                    let obj_slice = &slice2[pos + obj_start..];
                    if let Some(obj_end) = obj_slice.find('}') {
                        let obj = &obj_slice[..=obj_end];
                        let name = extract_json_string(obj, "name");
                        let _dtype = extract_json_string(obj, "type");
                        if let Some(n) = name {
                            result.push((n, _dtype.unwrap_or_default()));
                        }
                        pos += obj_start + obj_end + 1;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            return Some(result);
        }
    }
    None
}

fn extract_json_string(obj: &str, key: &str) -> Option<String> {
    let pattern = format!("\"{}\":", key);
    if let Some(start) = obj.find(&pattern) {
        let slice = &obj[start + pattern.len()..];
        if slice.starts_with('"') {
            let slice2 = &slice[1..];
            if let Some(end) = slice2.find('"') {
                return Some(slice2[..end].to_string());
            }
        }
    }
    None
}

fn parse_influx_query_results(body: &str) -> Vec<Vec<Vec<u8>>> {
    let mut rows = Vec::new();
    if let Some(results_start) = body.find("\"results\":") {
        let slice = &body[results_start..];
        if let Some(arr_start) = slice.find('[') {
            let slice2 = &slice[arr_start..];
            // Parse each result's series
            let mut pos = 0;
            while pos < slice2.len() {
                if let Some(ser_start) = slice2[pos..].find("\"series\":") {
                    let ser_slice = &slice2[pos + ser_start..];
                    if let Some(sarr_start) = ser_slice.find('[') {
                        let sarr = &ser_slice[sarr_start..];
                        // Parse values array
                        if let Some(vals_start) = sarr.find("\"values\":") {
                            let vals_slice = &sarr[vals_start..];
                            if let Some(varr_start) = vals_slice.find('[') {
                                let varr = &vals_slice[varr_start..];
                                // Parse rows
                                pos = parse_json_values_array(varr, &mut rows);
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        }
    }
    rows
}

fn parse_json_values_array(varr: &str, rows: &mut Vec<Vec<Vec<u8>>>) -> usize {
    let mut pos = 0;
    while pos < varr.len() {
        if let Some(arr_start) = varr[pos..].find('[') {
            let inner = &varr[pos + arr_start..];
            if let Some(arr_end) = inner.find(']') {
                let row_content = &inner[..=arr_end];
                let row = parse_json_row(row_content);
                if !row.is_empty() {
                    rows.push(row);
                }
                pos += arr_start + arr_end + 1;
            } else {
                break;
            }
        } else {
            break;
        }
    }
    pos
}

fn parse_json_row(content: &str) -> Vec<Vec<u8>> {
    let mut row = Vec::new();
    let mut in_string = false;
    let mut current = String::new();
    
    for c in content.chars() {
        match c {
            '"' => {
                if in_string && current.ends_with('\\') {
                    current.push(c);
                } else {
                    in_string = !in_string;
                }
            },
            ',' if !in_string => {
                if !current.is_empty() {
                    row.push(current.trim().as_bytes().to_vec());
                    current.clear();
                }
            },
            _ if in_string => current.push(c),
            _ => {}
        }
    }
    if !current.is_empty() {
        row.push(current.trim().as_bytes().to_vec());
    }
    
    row
}