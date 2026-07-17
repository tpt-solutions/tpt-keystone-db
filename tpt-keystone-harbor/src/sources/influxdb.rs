//! Harbor/TimeSeries — InfluxDB source connector. Hand-written InfluxDB
//! line protocol + HTTP API client over TCP (no `reqwest`/`curl` dependency,
//! per this repo's from-scratch rule). Discovery introspects measurements
//! via the `/query` endpoint (`SHOW MEASUREMENTS` / `SHOW FIELD KEYS` /
//! `SHOW TAG KEYS`); snapshot reads via `SELECT *` and streams rows; the
//! Verification Engine's checksums read the same query. CDC is genuinely
//! scope-cut: InfluxDB is a TSDB with no universal row-level change-feed
//! API (only per-subscription Telegraf output), so `replicate` stays
//! `Unimplemented` — see the Harbor/PG note for the honesty precedent.

use crate::connector::{ConnectorError, SourceConnector, SourceRow};
use crate::schema::{from_influx_type, ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 5_000;

/// Resolve `host:port` (or `http://host:port`), defaulting the port when
/// absent. Shared by every source connector's raw-socket client in this crate.
fn parse_addr(addr: &str, default_port: u16) -> (String, u16) {
    let addr = addr.strip_prefix("http://").unwrap_or(addr);
    let addr = addr.strip_prefix("https://").unwrap_or(addr);
    match addr.split_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().unwrap_or(default_port)),
        None => (addr.to_string(), default_port),
    }
}

/// Minimal HTTP/1.1 client for InfluxDB's `/query` API, over a raw TCP
/// stream. Uses `Connection: close` so the response ends at EOF, which
/// sidesteps chunked-transfer framing complexity (Content-Length is still
/// honoured when present).
struct InfluxConn {
    stream: TcpStream,
    buf: Vec<u8>,
    host_header: String,
    addr: String,
    database: String,
}

impl InfluxConn {
    async fn connect(addr: &str, database: &str) -> Result<Self> {
        let (host, port) = parse_addr(addr, 8086);
        let stream = TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("connecting to InfluxDB at {host}:{port}"))?;
        Ok(Self {
            stream,
            buf: Vec::with_capacity(65536),
            host_header: format!("{host}:{port}"),
            addr: format!("{host}:{port}"),
            database: database.to_string(),
        })
    }

    async fn query(&mut self, q: &str) -> Result<Value> {
        let path = format!(
            "/query?db={}&epoch=ms&q={}",
            urlencode(&self.database),
            urlencode(q)
        );
        let (status, body) = self.http_request("GET", &path, &[]).await?;
        if status != 200 {
            bail!("InfluxDB query failed (status {status}): {q}");
        }
        let v: Value = serde_json::from_slice(&body)
            .with_context(|| format!("parsing InfluxDB JSON response for {q:?}"))?;
        if let Some(results) = v.get("results").and_then(|r| r.as_array()) {
            for r in results {
                if let Some(e) = r.get("error") {
                    if !e.is_null() {
                        bail!("InfluxDB error: {e}");
                    }
                }
            }
        }
        Ok(v)
    }

    async fn http_request(&mut self, method: &str, path: &str, body: &[u8]) -> Result<(u16, Vec<u8>)> {
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n",
            method = method,
            path = path,
            host = self.host_header,
            len = body.len()
        );
        self.stream.write_all(req.as_bytes()).await?;
        self.stream.write_all(body).await?;
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
        parse_http_response(&self.buf)
    }
}

/// Split a raw HTTP/1.1 response into `(status_code, body_bytes)`,
/// handling Content-Length and chunked transfer-encoding.
fn parse_http_response(buf: &[u8]) -> Result<(u16, Vec<u8>)> {
    let s = String::from_utf8_lossy(buf);
    let header_end = s
        .find("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("no HTTP header terminator in InfluxDB response"))?;
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

/// Minimal RFC-7230 chunked-transfer decoder (handles the common case:
/// size-line, chunk, repeat, zero-size terminator).
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
        let data_start = line_end + 2; // past the size line's "\r\n"
        if p.len() < data_start + size {
            break;
        }
        out.extend_from_slice(&p[data_start..data_start + size]);
        p = &p[data_start + size + 2..]; // skip data + trailing \r\n
    }
    out
}

/// Extract every series from an InfluxDB `/query` JSON response. Returns
/// `(series_name, column_names, row_values)` triples so callers can treat
/// the result set uniformly whether it came from `SHOW ...` or `SELECT`.
fn extract_series(resp: &Value) -> Vec<(Option<String>, Vec<String>, Vec<Vec<Value>>)> {
    let mut out = Vec::new();
    if let Some(results) = resp.get("results").and_then(|v| v.as_array()) {
        for r in results {
            if let Some(series) = r.get("series").and_then(|v| v.as_array()) {
                for s in series {
                    let name = s.get("name").and_then(|v| v.as_str()).map(String::from);
                    let columns: Vec<String> = s
                        .get("columns")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|c| c.as_str().map(String::from)).collect())
                        .unwrap_or_default();
                    let values: Vec<Vec<Value>> = s
                        .get("values")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|row| row.as_array().cloned()).collect())
                        .unwrap_or_default();
                    out.push((name, columns, values));
                }
            }
        }
    }
    out
}

/// Render one InfluxDB column value as the Keystone wire text (NULL =
/// `None`). Numbers are kept as their JSON text form, timestamps as epoch-ms
/// strings (the `epoch=ms` query parameter guarantees that shape).
fn json_to_cell(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.as_bytes().to_vec()),
        Value::Bool(b) => Some(if *b { b"true".to_vec() } else { b"false".to_vec() }),
        Value::Number(n) => Some(n.to_string().as_bytes().to_vec()),
        other => Some(serde_json::to_vec(other).unwrap_or_default()),
    }
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
        let res = self.conn.query("SHOW MEASUREMENTS").await.map_err(ConnectorError::Other)?;
        let mut tables = Vec::new();
        for (name, _, _) in extract_series(&res) {
            let Some(measurement) = name else { continue };
            let fields_res = self
                .conn
                .query(&format!("SHOW FIELD KEYS FROM \"{measurement}\""))
                .await
                .map_err(ConnectorError::Other)?;
            let tags_res = self
                .conn
                .query(&format!("SHOW TAG KEYS FROM \"{measurement}\""))
                .await
                .map_err(ConnectorError::Other)?;

            let mut columns = vec![ColumnSchema {
                name: "time".to_string(),
                source_type: "timestamp".to_string(),
                keystone_type: "BIGINT".to_string(),
                nullable: false,
                is_primary_key: true,
            }];

            for (_, _, vals) in extract_series(&fields_res) {
                for row in vals {
                    if row.len() >= 2 {
                        let fname = row[0].as_str().unwrap_or_default().to_string();
                        let ftype = row[1].as_str().unwrap_or("string").to_string();
                        columns.push(ColumnSchema {
                            name: fname,
                            source_type: ftype.clone(),
                            keystone_type: from_influx_type(&ftype),
                            nullable: true,
                            is_primary_key: false,
                        });
                    }
                }
            }
            for (_, _, vals) in extract_series(&tags_res) {
                for row in vals {
                    if let Some(tname) = row.first().and_then(|v| v.as_str()) {
                        columns.push(ColumnSchema {
                            name: tname.to_string(),
                            source_type: "tag".to_string(),
                            keystone_type: "TEXT".to_string(),
                            nullable: true,
                            is_primary_key: false,
                        });
                    }
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
        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let res = self
            .conn
            .query(&format!("SELECT * FROM \"{}\"", table.name))
            .await
            .map_err(ConnectorError::Other)?;
        let mut total: u64 = 0;
        let mut batch: Vec<SourceRow> = Vec::with_capacity(SNAPSHOT_BATCH_SIZE);
        for (_, _, values) in extract_series(&res) {
            for row in values {
                let cells: SourceRow = row.iter().map(json_to_cell).collect();
                batch.push(cells);
                total += 1;
                if batch.len() >= SNAPSHOT_BATCH_SIZE {
                    if tx.send(std::mem::take(&mut batch)).await.is_err() {
                        return Ok(total);
                    }
                }
            }
        }
        if !batch.is_empty() {
            let _ = tx.send(batch).await;
        }
        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<crate::connector::ChangeEvent>) -> Result<(), ConnectorError> {
        // InfluxDB exposes no universal row-level change feed (only
        // per-subscription Telegraf output), so there's no portable CDC to
        // implement here — documented scope cut, not an unstarted stub.
        Err(ConnectorError::Unimplemented {
            connector: "Harbor/TimeSeries",
            detail: "InfluxDB has no standard row-level change-feed API; replays from snapshots only",
        })
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let res = self
            .conn
            .query(&format!("SELECT * FROM \"{}\"", table.name))
            .await
            .map_err(ConnectorError::Other)?;
        let mut out = Vec::new();
        for (_, _, values) in extract_series(&res) {
            for row in values {
                let cells: SourceRow = row.iter().map(json_to_cell).collect();
                out.push(crate::verify::hash_row(&cells));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_series_from_query_response() {
        let resp = json!({
            "results": [{
                "statement_id": 0,
                "series": [{
                    "name": "cpu",
                    "columns": ["time", "host", "usage"],
                    "values": [["1609459200000", "a", 12.3], ["1609459260000", "b", 7.0]]
                }]
            }]
        });
        let series = extract_series(&resp);
        assert_eq!(series.len(), 1);
        let (name, cols, rows) = &series[0];
        assert_eq!(name.as_deref(), Some("cpu"));
        assert_eq!(cols, &["time", "host", "usage"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], json!("a"));
        assert_eq!(rows[1][2], json!(7.0));
    }

    #[test]
    fn extracts_series_from_show_measurements() {
        let resp = json!({
            "results": [{
                "series": [{ "columns": ["name"], "values": [["cpu"], ["mem"]] }]
            }]
        });
        let series = extract_series(&resp);
        assert_eq!(series.len(), 1);
        let names: Vec<Option<String>> = series[0].2.iter().map(|r| r.first().and_then(|v| v.as_str().map(String::from))).collect();
        assert_eq!(names, vec![Some("cpu".into()), Some("mem".into())]);
    }

    #[test]
    fn json_cell_rendering() {
        assert_eq!(json_to_cell(&json!(null)), None);
        assert_eq!(json_to_cell(&json!("hi")), Some(b"hi".to_vec()));
        assert_eq!(json_to_cell(&json!(12.5)), Some(b"12.5".to_vec()));
        assert_eq!(json_to_cell(&json!(true)), Some(b"true".to_vec()));
    }

    #[test]
    fn parses_content_length_response() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nContent-Type: application/json\r\n\r\n{\"a\":1}".to_vec();
        let (status, body) = parse_http_response(&raw).unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, b"{\"a\":1}");
    }

    #[test]
    fn parses_chunked_response() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\nMozilla\r\n9\r\nDeveloper\r\n0\r\n\r\n".to_vec();
        let (status, body) = parse_http_response(&raw).unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, b"MozillaDeveloper");
    }

    #[test]
    fn urlsafe_encoding() {
        assert_eq!(urlencode("a b/c"), "a%20b%2Fc");
        assert_eq!(urlencode("keep-this_~."), "keep-this_~.");
    }
}
