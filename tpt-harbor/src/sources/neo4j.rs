//! Harbor/Graph — Neo4j source connector. Hand-written Bolt protocol v4+
//! over TCP (port 7687). Discovery queries Neo4j's metadata functions
//! (`db.labels()`, `db.relationshipTypes()`). Snapshot runs Cypher `MATCH`
//! queries per label. CDC is scope-cut — Neo4j lacks a standard change-feed
//! API.

use crate::connector::{ConnectorError, SourceConnector, SourceRow, ChangeEvent};
use crate::schema::{from_neo4j_type, ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 5_000;

// ── Minimal Bolt v4.4 protocol client ────────────────────────────────

struct BoltConn {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    request_id: i64,
}

impl BoltConn {
    async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.with_context(|| format!("connecting to Neo4j at {addr}"))?;
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(65536),
            write_buf: BytesMut::with_capacity(65536),
            request_id: 1,
        };

        // Bolt handshake: 4 magic bytes + 4 proposed versions
        conn.write_buf.put_slice(b"GBEF");
        conn.write_buf.put_u32(1); // version 1
        conn.write_buf.put_u32(0);
        conn.write_buf.put_u32(0);
        conn.write_buf.put_u32(0);
        conn.stream.write_all(&conn.write_buf).await?;
        conn.stream.flush().await?;
        conn.write_buf.clear();

        // Read server response (4 bytes: one agreed version or 0x00000000)
        conn.fill(4).await?;
        let agreed = u32::from_be_bytes(conn.read_buf[0..4].try_into().unwrap());
        conn.read_buf.advance(4);
        if agreed == 0 {
            bail!("Neo4j rejected all proposed Bolt versions");
        }

        // INIT message
        conn.send_init().await?;

        Ok(conn)
    }

    async fn send_init(&mut self) -> Result<()> {
        let mut msg = BytesMut::new();
        // INIT signature (0x01)
        msg.put_u8(0x01);
        // User agent string
        put_bolt_string(&mut msg, "tpt-harbor/0.1.0");

        self.send_message(&msg).await?;

        // Read SUCCESS or FAILURE
        let response = self.read_message().await?;
        if response.is_empty() {
            bail!("empty INIT response");
        }
        if response[0] == 0x7F {
            // FAILURE
            bail!("Neo4j INIT failed");
        }
        Ok(())
    }

    async fn run_cypher(&mut self, cypher: &str) -> Result<Vec<Vec<BsonValue>>> {
        let mut msg = BytesMut::new();

        // RUN signature (0x10)
        msg.put_u8(0x10);
        put_bolt_string(&mut msg, cypher);
        // Parameters (empty map)
        put_bolt_map(&mut msg, &[]);

        self.send_message(&msg).await?;

        // Read response
        let response = self.read_message().await?;
        if response.is_empty() {
            bail!("empty RUN response");
        }
        if response[0] == 0x7F {
            // FAILURE
            return Err(anyhow::anyhow!("Neo4j RUN failed").into());
        }

        // PULL all records
        let mut pull_msg = BytesMut::new();
        pull_msg.put_u8(0x3F); // PULL_ALL
        self.send_message(&pull_msg).await?;

        // Read records + summary
        self.read_records().await
    }

    async fn read_records(&mut self) -> Result<Vec<Vec<BsonValue>>> {
        let mut all_rows = Vec::new();

        loop {
            let msg = self.read_message().await?;
            if msg.is_empty() {
                break;
            }
            match msg[0] {
                0x71 => {
                    // RECORD
                    let mut p = &msg[1..];
                    // List of field values
                    if let Some((values, _)) = bolt_decode_list(&p) {
                        all_rows.push(values);
                    }
                }
                0x70 => {
                    // SUCCESS — end of result set
                    break;
                }
                0x7F => {
                    // FAILURE
                    bail!("Neo4j PULL failed");
                }
                _ => break,
            }
        }

        Ok(all_rows)
    }

    async fn send_message(&mut self, body: &[u8]) -> Result<()> {
        // Bolt v4 uses: 1 byte marker (0x00 = INIT, 0x01 = RUN, etc.)
        // then the message body. The framing is: message_size(4) + body.
        self.write_buf.put_u32_le(body.len() as u32);
        self.write_buf.put_slice(body);
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();
        self.request_id += 1;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<Vec<u8>> {
        self.fill(4).await?;
        let size = u32::from_le_bytes(self.read_buf[0..4].try_into().unwrap()) as usize;
        self.read_buf.advance(4);
        self.fill(size).await?;
        let data = self.read_buf.split_to(size).to_vec();
        Ok(data)
    }

    async fn fill(&mut self, n: usize) -> Result<()> {
        while self.read_buf.len() < n {
            let read = self.stream.read_buf(&mut self.read_buf).await?;
            if read == 0 {
                bail!("connection closed by peer");
            }
        }
        Ok(())
    }
}

// ── Bolt Packer/Unpacker (minimal) ──────────────────────────────────

fn put_bolt_string(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    if bytes.len() <= 0x7F {
        buf.put_u8(bytes.len() as u8);
    } else if bytes.len() <= 0xFF {
        buf.put_u8(0xD0);
        buf.put_u8(bytes.len() as u8);
    } else if bytes.len() <= 0xFFFF {
        buf.put_u8(0xD1);
        buf.put_u16(bytes.len() as u16);
    } else {
        buf.put_u8(0xD2);
        buf.put_u32(bytes.len() as u32);
    }
    buf.put_slice(bytes);
}

fn put_bolt_map(buf: &mut BytesMut, entries: &[(&str, &str)]) {
    if entries.len() <= 0x0F {
        buf.put_u8(entries.len() as u8);
    } else {
        buf.put_u8(0xD9);
        buf.put_u8(entries.len() as u8);
    }
    for (k, v) in entries {
        put_bolt_string(buf, k);
        put_bolt_string(buf, v);
    }
}

#[derive(Debug, Clone)]
enum BsonValue {
    Double(f64),
    String(String),
    Int64(i64),
    Boolean(bool),
    Null,
    List(Vec<BsonValue>),
    Map(Vec<(String, BsonValue)>),
    Node { labels: Vec<String>, props: Vec<(String, BsonValue)> },
    Relationship { rel_type: String, props: Vec<(String, BsonValue)> },
    Path(Vec<BsonValue>),
    Unknown,
}

fn bolt_decode_list(buf: &[u8]) -> Option<(Vec<BsonValue>, &[u8])> {
    if buf.is_empty() {
        return None;
    }
    let (count, mut p) = match buf[0] {
        0x90..=0x9F => (buf[0] as usize - 0x90, &buf[1..]),
        0xD4 => {
            if buf.len() < 2 { return None; }
            (buf[1] as usize, &buf[2..])
        }
        0xD5 => {
            if buf.len() < 3 { return None; }
            let c = u16::from_be_bytes(buf[1..3].try_into().unwrap_or([0,0])) as usize;
            (c, &buf[3..])
        }
        _ => return None,
    };

    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let (val, rest) = bolt_decode_value(p)?;
        items.push(val);
        p = rest;
    }
    Some((items, p))
}

fn bolt_decode_map(buf: &[u8]) -> Option<(Vec<(String, BsonValue)>, &[u8])> {
    if buf.is_empty() {
        return None;
    }
    let (count, mut p) = match buf[0] {
        0xA0..=0xAF => (buf[0] as usize - 0xA0, &buf[1..]),
        0xD8 => {
            if buf.len() < 2 { return None; }
            (buf[1] as usize, &buf[2..])
        }
        0xD9 => {
            if buf.len() < 3 { return None; }
            let c = u16::from_be_bytes(buf[1..3].try_into().unwrap_or([0,0])) as usize;
            (c, &buf[3..])
        }
        _ => return None,
    };

    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let (key, rest) = bolt_decode_value(p)?;
        let key_str = match key {
            BsonValue::String(s) => s,
            _ => continue,
        };
        let (val, rest2) = bolt_decode_value(rest)?;
        entries.push((key_str, val));
        p = rest2;
    }
    Some((entries, p))
}

fn bolt_decode_value(buf: &[u8]) -> Option<(BsonValue, &[u8])> {
    if buf.is_empty() {
        return None;
    }
    match buf[0] {
        // Null
        0xC0 => Some((BsonValue::Null, &buf[1..])),
        // Boolean
        0xC3 => Some((BsonValue::Boolean(true), &buf[1..])),
        0xC2 => Some((BsonValue::Boolean(false), &buf[1..])),
        // Integer types
        0xC8 => {
            if buf.len() < 2 { return None; }
            Some((BsonValue::Int64(buf[1] as i8 as i64), &buf[2..]))
        }
        0xC9 => {
            if buf.len() < 3 { return None; }
            let v = i16::from_be_bytes(buf[1..3].try_into().unwrap_or([0,0])) as i64;
            Some((BsonValue::Int64(v), &buf[3..]))
        }
        0xCA => {
            if buf.len() < 5 { return None; }
            let v = i32::from_be_bytes(buf[1..5].try_into().unwrap_or([0;4])) as i64;
            Some((BsonValue::Int64(v), &buf[5..]))
        }
        0xCB => {
            if buf.len() < 9 { return None; }
            let v = i64::from_be_bytes(buf[1..9].try_into().unwrap_or([0;8]));
            Some((BsonValue::Int64(v), &buf[9..]))
        }
        // Tiny int (0x00..=0x7F are positive, 0xF0..=0xFF are negative)
        0x00..=0x7F => Some((BsonValue::Int64(buf[0] as i64), &buf[1..])),
        0xF0..=0xFF => Some((BsonValue::Int64(buf[0] as i8 as i64), &buf[1..])),
        // Float
        0xC1 => {
            if buf.len() < 9 { return None; }
            let v = f64::from_be_bytes(buf[1..9].try_into().unwrap_or([0;8]));
            Some((BsonValue::Double(v), &buf[9..]))
        }
        // String
        0x80..=0x8F => {
            let len = buf[0] as usize - 0x80;
            if buf.len() < 1 + len { return None; }
            let s = String::from_utf8_lossy(&buf[1..1 + len]).to_string();
            Some((BsonValue::String(s), &buf[1 + len..]))
        }
        0xD0 => {
            if buf.len() < 2 { return None; }
            let len = buf[1] as usize;
            if buf.len() < 2 + len { return None; }
            let s = String::from_utf8_lossy(&buf[2..2 + len]).to_string();
            Some((BsonValue::String(s), &buf[2 + len..]))
        }
        0xD1 => {
            if buf.len() < 3 { return None; }
            let len = u16::from_be_bytes(buf[1..3].try_into().unwrap_or([0,0])) as usize;
            if buf.len() < 3 + len { return None; }
            let s = String::from_utf8_lossy(&buf[3..3 + len]).to_string();
            Some((BsonValue::String(s), &buf[3 + len..]))
        }
        // List
        0x90..=0x9F | 0xD4 | 0xD5 => {
            bolt_decode_list(buf).map(|(v, r)| (BsonValue::List(v), r))
        }
        // Map
        0xA0..=0xAF | 0xD8 | 0xD9 => {
            bolt_decode_map(buf).map(|(v, r)| (BsonValue::Map(v), r))
        }
        // Node (0x4E)
        0x4E => {
            let mut p = &buf[1..];
            // Node signature: num_labels(1) + labels* + properties_map
            if p.is_empty() { return None; }
            let num_labels = p[0] as usize;
            p = &p[1..];
            let mut labels = Vec::new();
            for _ in 0..num_labels {
                let (val, rest) = bolt_decode_value(p)?;
                p = rest;
                if let BsonValue::String(s) = val {
                    labels.push(s);
                }
            }
            let (props, rest) = bolt_decode_map(p)?;
            Some((BsonValue::Node { labels, props }, rest))
        }
        // Relationship (0x52)
        0x52 => {
            let mut p = &buf[1..];
            // rel_id, start_node_id, end_node_id, rel_type, properties
            if p.len() < 21 { return None; }
            let _rel_id = i64::from_be_bytes(p[0..8].try_into().unwrap_or([0;8]));
            p = &p[8..];
            let _start = i64::from_be_bytes(p[0..8].try_into().unwrap_or([0;8]));
            p = &p[8..];
            let _end = i64::from_be_bytes(p[0..8].try_into().unwrap_or([0;8]));
            p = &p[8..];
            let (rel_type_val, rest) = bolt_decode_value(p)?;
            p = rest;
            let rel_type = match rel_type_val {
                BsonValue::String(s) => s,
                _ => "UNKNOWN".to_string(),
            };
            let (props, rest) = bolt_decode_map(p)?;
            Some((BsonValue::Relationship { rel_type, props }, rest))
        }
        // Path (0x50)
        0x50 => {
            let mut p = &buf[1..];
            let (nodes, rest) = bolt_decode_list(p)?;
            let (_, rest) = bolt_decode_list(rest)?;
            Some((BsonValue::Path(nodes), rest))
        }
        // Bytes
        0xCC => {
            if buf.len() < 2 { return None; }
            let len = buf[1] as usize;
            if buf.len() < 2 + len { return None; }
            Some((BsonValue::Unknown, &buf[2 + len..]))
        }
        _ => Some((BsonValue::Unknown, &buf[1..])),
    }
}

fn neo4j_value_to_bytes(v: &BsonValue) -> Vec<u8> {
    match v {
        BsonValue::String(s) => s.as_bytes().to_vec(),
        BsonValue::Int64(i) => i.to_string().into_bytes(),
        BsonValue::Double(d) => d.to_string().into_bytes(),
        BsonValue::Boolean(b) => b.to_string().into_bytes(),
        BsonValue::Null => vec![],
        BsonValue::List(items) => {
            let strs: Vec<String> = items.iter().map(|v| neo4j_value_to_string(v)).collect();
            format!("[{}]", strs.join(",")).into_bytes()
        }
        BsonValue::Map(entries) => {
            let items: Vec<String> = entries.iter().map(|(k, v)| format!("{}:{}", k, neo4j_value_to_string(v))).collect();
            format!("{{{}}}", items.join(",")).into_bytes()
        }
        _ => vec![],
    }
}

fn neo4j_value_to_string(v: &BsonValue) -> String {
    match v {
        BsonValue::String(s) => s.clone(),
        BsonValue::Int64(i) => i.to_string(),
        BsonValue::Double(d) => d.to_string(),
        BsonValue::Boolean(b) => b.to_string(),
        BsonValue::Null => "null".to_string(),
        _ => "".to_string(),
    }
}

pub struct Neo4jSource {
    conn: BoltConn,
}

impl Neo4jSource {
    pub async fn connect(addr: &str) -> Result<Self> {
        let conn = BoltConn::connect(addr).await?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl SourceConnector for Neo4jSource {
    fn name(&self) -> &'static str {
        "Harbor/Graph"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        // Get all node labels
        let label_rows = self.conn.run_cypher("CALL db.labels() YIELD label RETURN label")
            .await
            .map_err(ConnectorError::Other)?;

        let mut tables = Vec::new();

        for row in &label_rows {
            let label = match row.first() {
                Some(BsonValue::String(s)) => s.clone(),
                _ => continue,
            };

            // Get properties for this label
            let prop_query = format!("MATCH (n:{label}) RETURN DISTINCT keys(n) AS keys LIMIT 100");
            let prop_rows = self.conn.run_cypher(&prop_query)
                .await
                .map_err(ConnectorError::Other)?;

            let mut prop_names: Vec<String> = Vec::new();
            for prop_row in &prop_rows {
                if let Some(BsonValue::List(keys)) = prop_row.first() {
                    for k in keys {
                        if let BsonValue::String(s) = k {
                            if !prop_names.contains(s) {
                                prop_names.push(s.clone());
                            }
                        }
                    }
                }
            }

            // Sample a node to infer types
            let sample_query = format!("MATCH (n:{label}) RETURN n LIMIT 5");
            let sample_rows = self.conn.run_cypher(&sample_query)
                .await
                .map_err(ConnectorError::Other)?;

            let mut columns = Vec::new();
            columns.push(ColumnSchema {
                name: "_node_id".to_string(),
                source_type: "integer".to_string(),
                keystone_type: "BIGINT".to_string(),
                nullable: false,
                is_primary_key: true,
            });

            for prop_name in &prop_names {
                let mut keystone_type = "TEXT".to_string();
                for sample_row in &sample_rows {
                    if let Some(BsonValue::Node { props, .. }) = sample_row.first() {
                        if let Some((_, v)) = props.iter().find(|(k, _)| k == prop_name) {
                            keystone_type = match v {
                                BsonValue::Int64(_) => "BIGINT".to_string(),
                                BsonValue::Double(_) => "DOUBLE PRECISION".to_string(),
                                BsonValue::Boolean(_) => "BOOLEAN".to_string(),
                                BsonValue::String(_) => "TEXT".to_string(),
                                _ => "TEXT".to_string(),
                            };
                            break;
                        }
                    }
                }
                columns.push(ColumnSchema {
                    keystone_type,
                    nullable: true,
                    is_primary_key: false,
                    name: prop_name.clone(),
                    source_type: "property".to_string(),
                });
            }

            tables.push(TableSchema {
                schema: "neo4j".to_string(),
                name: label,
                columns,
            });
        }

        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let label = &table.name;

        // Build property list for SELECT
        let prop_names: Vec<&str> = table.columns.iter()
            .filter(|c| c.name != "_node_id")
            .map(|c| c.name.as_str())
            .collect();

        let return_clause = if prop_names.is_empty() {
            "id(n) AS _node_id".to_string()
        } else {
            let props = prop_names.iter()
                .map(|p| format!("n.{p} AS {p}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("id(n) AS _node_id, {props}")
        };

        let cypher = format!("MATCH (n:{label}) RETURN {return_clause}");
        let rows = self.conn.run_cypher(&cypher)
            .await
            .map_err(ConnectorError::Other)?;

        let mut total: u64 = 0;
        for chunk in rows.chunks(SNAPSHOT_BATCH_SIZE) {
            let batch: Vec<SourceRow> = chunk.iter().map(|row| {
                row.iter().map(|v| Some(neo4j_value_to_bytes(v))).collect()
            }).collect();
            total += batch.len() as u64;
            if tx.send(batch).await.is_err() {
                break;
            }
        }

        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        Err(ConnectorError::Unimplemented { connector: "Harbor/Graph", detail: "Neo4j change feed not yet written" })
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let label = &table.name;
        let prop_names: Vec<&str> = table.columns.iter()
            .filter(|c| c.name != "_node_id")
            .map(|c| c.name.as_str())
            .collect();

        let return_clause = if prop_names.is_empty() {
            "id(n) AS _node_id".to_string()
        } else {
            let props = prop_names.iter()
                .map(|p| format!("n.{p} AS {p}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("id(n) AS _node_id, {props}")
        };

        let cypher = format!("MATCH (n:{label}) RETURN {return_clause} ORDER BY _node_id");
        let rows = self.conn.run_cypher(&cypher)
            .await
            .map_err(ConnectorError::Other)?;

        Ok(rows.iter().map(|row| {
            let source_row: SourceRow = row.iter().map(|v| Some(neo4j_value_to_bytes(v))).collect();
            crate::verify::hash_row(&source_row)
        }).collect())
    }
}
