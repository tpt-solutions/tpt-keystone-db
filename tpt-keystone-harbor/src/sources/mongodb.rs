//! Harbor/Mongo — MongoDB source connector. Hand-written MongoDB OP_MSG
//! protocol over TCP (port 27017). Discovery uses `listCollections` to
//! enumerate collections; each becomes a fixed `(_id TEXT PK, doc JSON)`
//! table, GIN-indexed, and snapshot/checksums serialize each whole document
//! to the `doc` column rather than flattening fields into typed columns —
//! Mongo is schemaless by design, and Canopy's JSON column is built to store
//! documents directly, so this is both more faithful (no small-sample type
//! inference silently dropping unseen fields) and the engine-native shape.
//! Snapshot uses `find` with batch cursor iteration via `getMore`. CDC
//! (`replicate`) opens a whole-database `$changeStream` aggregate cursor —
//! just another command over the same `send_op_msg`/`getMore` cursor loop
//! `snapshot_table` uses, cheaper than a from-scratch oplog-tailing
//! implementation — and requires the source to be a replica set or sharded
//! cluster (change streams don't exist against a standalone `mongod`).

use crate::connector::{ConnectorError, SourceConnector, SourceRow, ChangeEvent};
use crate::schema::{ColumnSchema, IndexSpec, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashSet;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: i64 = 1_000;
const CHANGE_STREAM_BATCH_SIZE: i32 = 1_000;
/// `getMore`'s `maxTimeMS` — how long the server long-polls for new change
/// events before returning an empty batch, so the client isn't spinning.
const CHANGE_STREAM_MAX_TIME_MS: i32 = 10_000;

// ── Minimal BSON encoder ─────────────────────────────────────────────

fn bson_encode_string(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(5 + bytes.len());
    out.extend_from_slice(&(bytes.len() as i32 + 1).to_le_bytes());
    out.extend_from_slice(bytes);
    out.push(0);
    out
}

fn bson_encode_cstring(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() + 1);
    out.extend_from_slice(s.as_bytes());
    out.push(0);
    out
}

fn bson_encode_int32(name: &str, val: i32) -> Vec<u8> {
    let mut out = vec![0x10]; // type: int32
    out.extend_from_slice(&bson_encode_cstring(name));
    out.extend_from_slice(&val.to_le_bytes());
    out
}

fn bson_encode_int64(name: &str, val: i64) -> Vec<u8> {
    let mut out = vec![0x12]; // type: int64
    out.extend_from_slice(&bson_encode_cstring(name));
    out.extend_from_slice(&val.to_le_bytes());
    out
}

fn bson_encode_string_typed(name: &str, val: &str) -> Vec<u8> {
    let mut out = vec![0x02]; // type: string
    out.extend_from_slice(&bson_encode_cstring(name));
    out.extend_from_slice(&bson_encode_string(val));
    out
}

/// `elements` are already-encoded child elements (each carrying its own
/// field name, e.g. from `bson_encode_string_typed`/`bson_encode_document`)
/// — this just wraps them as one nested-document-typed element named `name`.
fn bson_encode_document(name: &str, elements: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out = vec![0x03]; // type: document
    out.extend_from_slice(&bson_encode_cstring(name));
    out.extend_from_slice(&bson_build_doc(elements));
    out
}

/// Same as [`bson_encode_document`] but tagged as a BSON array — callers
/// must key each element "0", "1", ... (e.g. via `bson_encode_document("0", ...)`)
/// since BSON arrays are documents with stringified-index keys.
fn bson_encode_array(name: &str, elements: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out = vec![0x04]; // type: array
    out.extend_from_slice(&bson_encode_cstring(name));
    out.extend_from_slice(&bson_build_doc(elements));
    out
}

fn bson_build_doc(elements: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::new();
    for el in elements {
        out.extend_from_slice(&el);
    }
    out.push(0); // terminator
    // BSON's declared document length includes its own 4-byte length
    // field (spec: "total number of bytes comprising the document");
    // omitting that +4 under-reported every document this crate has ever
    // built by 4 bytes, corrupting the tail of any nested document/array a
    // decoder tried to bound by it — caught by the first test that actually
    // round-trips `bson_build_doc` output back through `bson_decode_doc`
    // (previously nothing did; existing tests construct `BsonValue` by hand).
    let total = out.len() as i32 + 4;
    let mut doc = total.to_le_bytes().to_vec();
    doc.extend_from_slice(&out);
    doc
}

// ── Minimal BSON decoder ─────────────────────────────────────────────

#[derive(Debug, Clone)]
enum BsonValue {
    Double(f64),
    String(String),
    Document(Vec<(String, BsonValue)>),
    Array(Vec<BsonValue>),
    Boolean(bool),
    Null,
    Int32(i32),
    Int64(i64),
    ObjectId([u8; 12]),
    Binary(Vec<u8>),
    Unknown,
}

fn bson_decode_doc(buf: &[u8]) -> Result<Vec<(String, BsonValue)>> {
    if buf.len() < 5 {
        bail!("BSON document too short");
    }
    let doc_len = i32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let data = &buf[4..doc_len.min(buf.len())];
    let mut out = Vec::new();
    let mut p = data;
    while !p.is_empty() {
        if p[0] == 0 {
            break;
        }
        let elem_type = p[0];
        p = &p[1..];
        // Element name (cstring)
        let name_end = p.iter().position(|&b| b == 0).unwrap_or(p.len());
        let name = String::from_utf8_lossy(&p[..name_end]).to_string();
        p = &p[name_end + 1..];

        let (value, consumed) = bson_decode_value(elem_type, p)?;
        out.push((name, value));
        p = &p[consumed..];
    }
    Ok(out)
}

fn bson_decode_value(type_id: u8, buf: &[u8]) -> Result<(BsonValue, usize)> {
    match type_id {
        0x01 => {
            // Double
            if buf.len() < 8 { return Ok((BsonValue::Unknown, 0)); }
            let v = f64::from_le_bytes(buf[0..8].try_into().unwrap());
            Ok((BsonValue::Double(v), 8))
        }
        0x02 => {
            // String
            if buf.len() < 4 { return Ok((BsonValue::Unknown, 0)); }
            let slen = i32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            if buf.len() < 4 + slen { return Ok((BsonValue::Unknown, buf.len())); }
            let s = String::from_utf8_lossy(&buf[4..4 + slen - 1]).to_string();
            Ok((BsonValue::String(s), 4 + slen))
        }
        0x03 => {
            // Document
            if buf.len() < 4 { return Ok((BsonValue::Unknown, 0)); }
            let dlen = i32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            if buf.len() < dlen { return Ok((BsonValue::Unknown, buf.len())); }
            let doc = bson_decode_doc(&buf[..dlen]).unwrap_or_default();
            Ok((BsonValue::Document(doc), dlen))
        }
        0x04 => {
            // Array
            if buf.len() < 4 { return Ok((BsonValue::Unknown, 0)); }
            let dlen = i32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            if buf.len() < dlen { return Ok((BsonValue::Unknown, buf.len())); }
            let doc = bson_decode_doc(&buf[..dlen]).unwrap_or_default();
            let arr: Vec<BsonValue> = doc.into_iter().map(|(_, v)| v).collect();
            Ok((BsonValue::Array(arr), dlen))
        }
        0x08 => {
            // Boolean
            if buf.is_empty() { return Ok((BsonValue::Unknown, 0)); }
            Ok((BsonValue::Boolean(buf[0] != 0), 1))
        }
        0x0A => Ok((BsonValue::Null, 0)),
        0x10 => {
            // Int32
            if buf.len() < 4 { return Ok((BsonValue::Unknown, 0)); }
            let v = i32::from_le_bytes(buf[0..4].try_into().unwrap());
            Ok((BsonValue::Int32(v), 4))
        }
        0x12 => {
            // Int64
            if buf.len() < 8 { return Ok((BsonValue::Unknown, 0)); }
            let v = i64::from_le_bytes(buf[0..8].try_into().unwrap());
            Ok((BsonValue::Int64(v), 8))
        }
        0x07 => {
            // ObjectId
            if buf.len() < 12 { return Ok((BsonValue::Unknown, 0)); }
            let mut id = [0u8; 12];
            id.copy_from_slice(&buf[..12]);
            Ok((BsonValue::ObjectId(id), 12))
        }
        0x05 => {
            // Binary
            if buf.len() < 5 { return Ok((BsonValue::Unknown, 0)); }
            let blen = i32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            let _subtype = buf[4];
            if buf.len() < 5 + blen { return Ok((BsonValue::Unknown, buf.len())); }
            Ok((BsonValue::Binary(buf[5..5 + blen].to_vec()), 5 + blen))
        }
        _ => Ok((BsonValue::Unknown, buf.len())),
    }
}

/// The fixed table shape every Mongo collection migrates to: the whole
/// document preserved as JSON (see module doc), GIN-indexed for containment/
/// path queries.
fn document_table_schema(database: &str, collection: &str) -> TableSchema {
    TableSchema {
        schema: database.to_string(),
        name: collection.to_string(),
        columns: vec![
            ColumnSchema { name: "_id".to_string(), source_type: "objectId".to_string(), keystone_type: "TEXT".to_string(), nullable: false, is_primary_key: true },
            ColumnSchema { name: "doc".to_string(), source_type: "object".to_string(), keystone_type: "JSONB".to_string(), nullable: false, is_primary_key: false },
        ],
        indexes: vec![IndexSpec { columns: vec!["doc".to_string()], using: "GIN".to_string(), with: vec![] }],
        topic_partitions: None,
    }
}

/// One BSON document → a `(_id, doc)` row, `doc` the whole document
/// serialized as JSON text (including `_id`, harmless duplication with the PK
/// column and consistent with how nested documents/arrays already serialize
/// via [`doc_to_json_string`]).
fn doc_to_row(doc: &[(String, BsonValue)]) -> SourceRow {
    let id = doc.iter().find(|(k, _)| k == "_id").map(|(_, v)| bson_value_to_bytes(v));
    let body = Some(doc_to_json_string(doc).into_bytes());
    vec![id, body]
}

// ── MongoDB wire protocol client ─────────────────────────────────────

struct MongoConn {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    request_id: i32,
    /// `hello`'s `setName` field — present only for a replica-set/sharded
    /// member, absent on a standalone `mongod`. Change streams need this.
    set_name: Option<String>,
}

impl MongoConn {
    async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.with_context(|| format!("connecting to MongoDB at {addr}"))?;
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(65536),
            write_buf: BytesMut::with_capacity(65536),
            request_id: 1,
            set_name: None,
        };

        // OP_MSG hello/ismaster
        let cmd = bson_build_doc(vec![
            bson_encode_string_typed("hello", "1"),
            bson_encode_int32("helloOk", 1),
            bson_encode_string_typed("$db", "admin"),
        ]);
        let response = conn.send_op_msg(1, &cmd, 0).await?;

        // Check for ok
        if let Some((_, BsonValue::Double(v))) = response.iter().find(|(k, _)| k == "ok") {
            if (*v - 1.0).abs() > 0.01 {
                bail!("MongoDB hello failed: ok={v}");
            }
        }

        conn.set_name = response.iter().find(|(k, _)| k == "setName").and_then(|(_, v)| if let BsonValue::String(s) = v { Some(s.clone()) } else { None });

        Ok(conn)
    }

    async fn send_op_msg(&mut self, flags: u32, body: &[u8], _db_selector: u8) -> Result<Vec<(String, BsonValue)>> {
        // OP_MSG: header(16) + flags(4) + section_kind(1) + section_body
        let section_len = 4 + 1 + body.len() as u32; // size(4) + body
        let msg_len = 16 + 4 + 1 + section_len;
        let req_id = self.request_id;
        self.request_id += 1;

        self.write_buf.put_i32_le(msg_len as i32); // message length
        self.write_buf.put_i32_le(req_id);  // request id
        self.write_buf.put_i32_le(0);       // response to (0 = request)
        self.write_buf.put_i32_le(2013);    // opCode: OP_MSG = 2013
        self.write_buf.put_u32_le(flags);
        self.write_buf.put_u8(0);           // section kind: body
        self.write_buf.put_slice(body);

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();

        // Read response
        loop {
            self.fill(16).await?;
            let msg_len = i32::from_le_bytes(self.read_buf[0..4].try_into().unwrap()) as usize;
            let _resp_to = i32::from_le_bytes(self.read_buf[8..12].try_into().unwrap());
            let _op_code = i32::from_le_bytes(self.read_buf[12..16].try_into().unwrap());
            self.read_buf.advance(16);

            self.fill(msg_len - 16).await?;
            let payload = self.read_buf.split_to(msg_len - 16);

            // OP_MSG response: flags(4) + sections...
            if payload.len() < 4 {
                return Ok(vec![]);
            }
            let _flags = u32::from_le_bytes(payload[0..4].try_into().unwrap());
            let mut p = &payload[4..];

            while !p.is_empty() {
                let kind = p[0];
                p = &p[1..];
                if kind == 0 {
                    // Body section: size(4) + BSON doc
                    if p.len() < 4 { break; }
                    let _size = i32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
                    p = &p[4..];
                    let doc_size = if p.len() >= 4 {
                        i32::from_le_bytes(p[0..4].try_into().unwrap()) as usize
                    } else {
                        break;
                    };
                    if p.len() < doc_size { break; }
                    let doc = bson_decode_doc(&p[..doc_size]).unwrap_or_default();
                    return Ok(doc);
                } else {
                    // Document sequence: size(4) + identifier(cstring) + docs
                    if p.len() < 4 { break; }
                    let _seq_size = i32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
                    // Skip remaining documents in this section
                    break;
                }
            }
        }
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

pub struct MongoSource {
    conn: MongoConn,
    database: String,
}

impl MongoSource {
    pub async fn connect(addr: &str, database: &str) -> Result<Self> {
        let conn = MongoConn::connect(addr).await?;
        Ok(Self {
            conn,
            database: database.to_string(),
        })
    }
}

#[async_trait]
impl SourceConnector for MongoSource {
    fn name(&self) -> &'static str {
        "Harbor/Mongo"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let cmd = bson_build_doc(vec![
            bson_encode_string_typed("listCollections", "1"),
            bson_encode_int32("cursor", 0),
            bson_encode_string_typed("$db", &self.database),
        ]);

        let response = self
            .conn
            .send_op_msg(0, &cmd, 0)
            .await
            .map_err(ConnectorError::Other)?;

        // Extract cursor firstBatch
        let mut tables = Vec::new();
        if let Some((_, BsonValue::Document(cursor_doc))) = response.iter().find(|(k, _)| k == "cursor") {
            if let Some((_, BsonValue::Array(batch))) = cursor_doc.iter().find(|(k, _)| k == "firstBatch") {
                for item in batch {
                    if let BsonValue::Document(coll_info) = item {
                        let coll_name = coll_info
                            .iter()
                            .find(|(k, _)| k == "name")
                            .and_then(|(_, v)| if let BsonValue::String(s) = v { Some(s.clone()) } else { None })
                            .unwrap_or_default();

                        if coll_name.starts_with("system.") {
                            continue;
                        }

                        tables.push(document_table_schema(&self.database, &coll_name));
                    }
                }
            }
        }

        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let mut total: u64 = 0;
        let batch_size: i64 = SNAPSHOT_BATCH_SIZE;

        // First query
        let cmd = bson_build_doc(vec![
            bson_encode_string_typed("find", &table.name),
            bson_encode_int64("batchSize", batch_size),
            bson_encode_string_typed("$db", &self.database),
        ]);

        let response = self
            .conn
            .send_op_msg(0, &cmd, 0)
            .await
            .map_err(ConnectorError::Other)?;

        // Extract firstBatch
        let mut batches = extract_batch(&response);
        let mut next_cursor = extract_cursor_id(&response);

        loop {
            let batch_rows: Vec<SourceRow> = batches.iter().map(|doc| doc_to_row(doc)).collect();

            total += batch_rows.len() as u64;
            if !batch_rows.is_empty() {
                if tx.send(batch_rows).await.is_err() {
                    break;
                }
            }

            if next_cursor == 0 {
                break;
            }

            // getMore — cursor id is BSON int64, not a string (a strict
            // server rejects a mistyped `getMore` field).
            let getmore_cmd = bson_build_doc(vec![
                bson_encode_int64("getMore", next_cursor),
                bson_encode_string_typed("collection", &table.name),
                bson_encode_int64("batchSize", batch_size),
                bson_encode_string_typed("$db", &self.database),
            ]);

            let resp = self
                .conn
                .send_op_msg(0, &getmore_cmd, 0)
                .await
                .map_err(ConnectorError::Other)?;

            batches = extract_batch(&resp);
            next_cursor = extract_cursor_id(&resp);
        }

        Ok(total)
    }

    async fn replicate(&mut self, tables: &[TableSchema], resume_token: Option<String>, tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        if self.conn.set_name.is_none() {
            return Err(ConnectorError::Other(anyhow::anyhow!(
                "MongoDB change streams require a replica set or sharded cluster (no setName in the hello response); \
                 this server looks like a standalone mongod. Run rs.initiate() to enable Harbor's live CDC."
            )));
        }

        let known_tables: HashSet<&str> = tables.iter().map(|t| t.name.as_str()).collect();

        let mut stage_fields = vec![bson_encode_string_typed("fullDocument", "updateLookup")];
        if let Some(token) = &resume_token {
            stage_fields.push(bson_encode_document("resumeAfter", vec![bson_encode_string_typed("_data", token)]));
        }
        let pipeline = bson_encode_array("pipeline", vec![bson_encode_document("0", vec![bson_encode_document("$changeStream", stage_fields)])]);
        let cursor_opt = bson_encode_document("cursor", vec![bson_encode_int32("batchSize", CHANGE_STREAM_BATCH_SIZE)]);
        // Whole-database stream (aggregate: 1, not a specific collection) so
        // one cursor covers every migrating table — events are filtered
        // against `known_tables` below by their own `ns.coll` field.
        let cmd = bson_build_doc(vec![
            bson_encode_int32("aggregate", 1),
            pipeline,
            cursor_opt,
            bson_encode_string_typed("$db", &self.database),
        ]);

        let mut response = self.conn.send_op_msg(0, &cmd, 0).await.map_err(ConnectorError::Other)?;
        let collection = extract_cursor_ns(&response)
            .and_then(|ns| ns.splitn(2, '.').nth(1).map(|s| s.to_string()))
            .unwrap_or_else(|| "$cmd.aggregate".to_string());

        loop {
            for doc in extract_batch(&response) {
                match map_change_event(&doc, &known_tables).map_err(ConnectorError::Other)? {
                    Some(MappedChange::Event(event)) => {
                        if tx.send(event).await.is_err() {
                            return Ok(());
                        }
                    }
                    Some(MappedChange::Invalidate) => return Ok(()),
                    None => {}
                }
                if let Some(token) = extract_resume_token(&doc) {
                    if tx.send(ChangeEvent::CommitLsn(token)).await.is_err() {
                        return Ok(());
                    }
                }
            }

            let cursor_id = extract_cursor_id(&response);
            if cursor_id == 0 {
                return Ok(());
            }

            let getmore_cmd = bson_build_doc(vec![
                bson_encode_int64("getMore", cursor_id),
                bson_encode_string_typed("collection", &collection),
                bson_encode_int32("maxTimeMS", CHANGE_STREAM_MAX_TIME_MS),
                bson_encode_string_typed("$db", &self.database),
            ]);
            response = self.conn.send_op_msg(0, &getmore_cmd, 0).await.map_err(ConnectorError::Other)?;
        }
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let mut checksums = Vec::new();

        let cmd = bson_build_doc(vec![
            bson_encode_string_typed("find", &table.name),
            bson_encode_int64("batchSize", 1000),
            bson_encode_string_typed("$db", &self.database),
        ]);

        let response = self
            .conn
            .send_op_msg(0, &cmd, 0)
            .await
            .map_err(ConnectorError::Other)?;

        let mut batches = extract_batch(&response);
        let mut next_cursor = extract_cursor_id(&response);

        loop {
            for doc in &batches {
                checksums.push(crate::verify::hash_row(&doc_to_row(doc)));
            }

            if next_cursor == 0 {
                break;
            }

            let getmore_cmd = bson_build_doc(vec![
                bson_encode_int64("getMore", next_cursor),
                bson_encode_string_typed("collection", &table.name),
                bson_encode_int64("batchSize", 1000),
                bson_encode_string_typed("$db", &self.database),
            ]);

            let resp = self
                .conn
                .send_op_msg(0, &getmore_cmd, 0)
                .await
                .map_err(ConnectorError::Other)?;

            batches = extract_batch(&resp);
            next_cursor = extract_cursor_id(&resp);
        }

        Ok(checksums)
    }
}

fn bson_value_to_bytes(v: &BsonValue) -> Vec<u8> {
    match v {
        BsonValue::Double(d) => d.to_string().into_bytes(),
        BsonValue::String(s) => s.as_bytes().to_vec(),
        BsonValue::Int32(i) => i.to_string().into_bytes(),
        BsonValue::Int64(i) => i.to_string().into_bytes(),
        BsonValue::Boolean(b) => b.to_string().into_bytes(),
        BsonValue::Null => vec![],
        BsonValue::ObjectId(id) => {
            // Represent as hex string
            id.iter().map(|b| format!("{:02x}", b)).collect::<String>().into_bytes()
        }
        BsonValue::Document(d) => {
            // Serialize to JSON-like string
            let json = doc_to_json_string(d);
            json.into_bytes()
        }
        BsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(|v| bson_value_to_json(v)).collect();
            format!("[{}]", items.join(",")).into_bytes()
        }
        BsonValue::Binary(b) => base64_encode(b),
        BsonValue::Unknown => vec![],
    }
}

fn doc_to_json_string(doc: &[(String, BsonValue)]) -> String {
    let items: Vec<String> = doc
        .iter()
        .map(|(k, v)| format!("\"{}\": {}", k, bson_value_to_json(v)))
        .collect();
    format!("{{{}}}", items.join(","))
}

fn bson_value_to_json(v: &BsonValue) -> String {
    match v {
        BsonValue::Double(d) => d.to_string(),
        BsonValue::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
        BsonValue::Int32(i) => i.to_string(),
        BsonValue::Int64(i) => i.to_string(),
        BsonValue::Boolean(b) => b.to_string(),
        BsonValue::Null => "null".to_string(),
        BsonValue::ObjectId(id) => {
            let hex: String = id.iter().map(|b| format!("{:02x}", b)).collect();
            format!("\"ObjectId(\\\"{}\\\")\"", hex)
        }
        BsonValue::Document(d) => doc_to_json_string(d),
        BsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(|v| bson_value_to_json(v)).collect();
            format!("[{}]", items.join(","))
        }
        BsonValue::Binary(b) => format!("\"{}\"", base64_encode(b).iter().map(|c| *c as char).collect::<String>()),
        BsonValue::Unknown => "null".to_string(),
    }
}

fn base64_encode(data: &[u8]) -> Vec<u8> {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((triple >> 18) & 0x3F) as usize]);
        out.push(CHARS[((triple >> 12) & 0x3F) as usize]);
        if chunk.len() > 1 { out.push(CHARS[((triple >> 6) & 0x3F) as usize]); } else { out.push(b'='); }
        if chunk.len() > 2 { out.push(CHARS[(triple & 0x3F) as usize]); } else { out.push(b'='); }
    }
    out
}

/// A `find`/`aggregate` response's cursor batch is `firstBatch`; a
/// `getMore` response's is `nextBatch` — different field names for the
/// same purpose, so both are checked (using either name unconditionally
/// broke `getMore` pagination for any collection larger than one batch).
fn extract_batch(response: &[(String, BsonValue)]) -> Vec<Vec<(String, BsonValue)>> {
    let mut result = Vec::new();
    if let Some((_, BsonValue::Document(cursor_doc))) = response.iter().find(|(k, _)| k == "cursor") {
        let batch_field = cursor_doc.iter().find(|(k, _)| k == "firstBatch").or_else(|| cursor_doc.iter().find(|(k, _)| k == "nextBatch"));
        if let Some((_, BsonValue::Array(batch))) = batch_field {
            for item in batch {
                if let BsonValue::Document(d) = item {
                    result.push(d.clone());
                }
            }
        }
    }
    result
}

fn extract_cursor_id(response: &[(String, BsonValue)]) -> i64 {
    if let Some((_, BsonValue::Document(cursor_doc))) = response.iter().find(|(k, _)| k == "cursor") {
        if let Some((_, BsonValue::Int64(id))) = cursor_doc.iter().find(|(k, _)| k == "id") {
            return *id;
        }
    }
    0
}

/// The cursor's `ns` (`"<database>.<collection>"`), needed so `getMore`
/// against a whole-database `aggregate: 1` cursor targets the exact
/// collection name the server assigned (typically `$cmd.aggregate`) rather
/// than a guessed constant.
fn extract_cursor_ns(response: &[(String, BsonValue)]) -> Option<String> {
    let cursor_doc = response.iter().find(|(k, _)| k == "cursor").and_then(|(_, v)| if let BsonValue::Document(d) = v { Some(d) } else { None })?;
    cursor_doc.iter().find(|(k, _)| k == "ns").and_then(|(_, v)| if let BsonValue::String(s) = v { Some(s.clone()) } else { None })
}

enum MappedChange {
    Event(ChangeEvent),
    /// The stream ended (collection/database dropped or renamed, or the
    /// resume token aged out) — `replicate()` treats this as a clean stop,
    /// matching how Postgres's own `replicate()` treats logical-replication
    /// stream end.
    Invalidate,
}

fn bson_doc_field<'a>(doc: &'a [(String, BsonValue)], key: &str) -> Option<&'a Vec<(String, BsonValue)>> {
    doc.iter().find(|(k, _)| k == key).and_then(|(_, v)| if let BsonValue::Document(d) = v { Some(d) } else { None })
}

fn bson_str_field(doc: &[(String, BsonValue)], key: &str) -> Option<String> {
    doc.iter().find(|(k, _)| k == key).and_then(|(_, v)| if let BsonValue::String(s) = v { Some(s.clone()) } else { None })
}

/// Maps one MongoDB change-stream event document to a `ChangeEvent`,
/// filtering out events for collections not in `known_tables` (the
/// whole-database stream covers every collection, not just migrating
/// tables) and events this simplification doesn't handle: `"update"`
/// without a resolvable `fullDocument` (the document was deleted again
/// before `updateLookup` could resolve it — a real edge case even with
/// that option set) is dropped rather than fabricated from the partial
/// `updateDescription` diff; `"drop"`/`"rename"`/`"dropDatabase"` are
/// dropped as out of scope for a per-row migration feed.
fn map_change_event(doc: &[(String, BsonValue)], known_tables: &HashSet<&str>) -> Result<Option<MappedChange>> {
    let Some(op_type) = bson_str_field(doc, "operationType") else { return Ok(None) };

    if op_type == "invalidate" {
        return Ok(Some(MappedChange::Invalidate));
    }
    if !matches!(op_type.as_str(), "insert" | "update" | "replace" | "delete") {
        return Ok(None);
    }

    let Some(table) = bson_doc_field(doc, "ns").and_then(|ns| bson_str_field(ns, "coll")) else { return Ok(None) };
    if !known_tables.contains(table.as_str()) {
        return Ok(None);
    }

    let full_document = bson_doc_field(doc, "fullDocument").cloned();
    let document_key = bson_doc_field(doc, "documentKey").cloned();

    let event = match op_type.as_str() {
        "insert" | "replace" => match full_document {
            Some(full) => ChangeEvent::Insert { table, row: doc_to_row(&full) },
            None => return Ok(None),
        },
        "update" => match (full_document, document_key) {
            (Some(full), Some(key)) => ChangeEvent::Update { table, key: doc_to_row(&key), row: doc_to_row(&full) },
            _ => return Ok(None),
        },
        "delete" => match document_key {
            Some(key) => ChangeEvent::Delete { table, key: doc_to_row(&key) },
            None => return Ok(None),
        },
        _ => unreachable!("filtered above"),
    };
    Ok(Some(MappedChange::Event(event)))
}

/// A change event's `_id` is itself a BSON document whose `_data` field is,
/// per the MongoDB change-streams spec, always a printable string — used
/// directly as Harbor's resume token (no extra encoding needed) and passed
/// back verbatim as `resumeAfter._data` to resume.
fn extract_resume_token(doc: &[(String, BsonValue)]) -> Option<String> {
    bson_doc_field(doc, "_id").and_then(|id_doc| bson_str_field(id_doc, "_data"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_schema_is_id_plus_json_with_gin_index() {
        let t = document_table_schema("mydb", "orders");
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[0].name, "_id");
        assert!(t.columns[0].is_primary_key);
        assert_eq!(t.columns[1].name, "doc");
        assert_eq!(t.columns[1].keystone_type, "JSONB");
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(t.indexes[0].using, "GIN");
        assert_eq!(t.indexes[0].columns, vec!["doc".to_string()]);
    }

    #[test]
    fn doc_to_row_preserves_whole_document_as_json() {
        let doc = vec![
            ("_id".to_string(), BsonValue::String("abc123".to_string())),
            ("name".to_string(), BsonValue::String("Ada".to_string())),
            ("age".to_string(), BsonValue::Int32(30)),
        ];
        let row = doc_to_row(&doc);
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Some(b"abc123".to_vec()));
        let body = String::from_utf8(row[1].clone().unwrap()).unwrap();
        assert!(body.contains("\"name\": \"Ada\""));
        assert!(body.contains("\"age\": 30"));
    }

    #[test]
    fn doc_to_row_handles_missing_id() {
        let doc = vec![("name".to_string(), BsonValue::String("Ada".to_string()))];
        let row = doc_to_row(&doc);
        assert_eq!(row[0], None);
    }

    #[test]
    fn nested_document_and_array_round_trip_through_decoder() {
        let doc = bson_build_doc(vec![
            bson_encode_document("stage", vec![bson_encode_string_typed("fullDocument", "updateLookup"), bson_encode_int32("x", 5)]),
            bson_encode_array("pipeline", vec![bson_encode_document("0", vec![bson_encode_int32("y", 1)])]),
        ]);
        let decoded = bson_decode_doc(&doc).unwrap();

        let BsonValue::Document(stage) = &decoded.iter().find(|(k, _)| k == "stage").unwrap().1 else { panic!("expected document") };
        assert!(matches!(stage.iter().find(|(k, _)| k == "fullDocument").unwrap().1, BsonValue::String(ref s) if s == "updateLookup"));
        assert!(matches!(stage.iter().find(|(k, _)| k == "x").unwrap().1, BsonValue::Int32(5)));

        let BsonValue::Array(pipeline) = &decoded.iter().find(|(k, _)| k == "pipeline").unwrap().1 else { panic!("expected array") };
        assert_eq!(pipeline.len(), 1);
        let BsonValue::Document(first_stage) = &pipeline[0] else { panic!("expected document") };
        assert!(matches!(first_stage.iter().find(|(k, _)| k == "y").unwrap().1, BsonValue::Int32(1)));
    }

    fn ns_doc(db: &str, coll: &str) -> BsonValue {
        BsonValue::Document(vec![("db".to_string(), BsonValue::String(db.to_string())), ("coll".to_string(), BsonValue::String(coll.to_string()))])
    }

    fn id_key_doc(id: &str) -> BsonValue {
        BsonValue::Document(vec![("_id".to_string(), BsonValue::String(id.to_string()))])
    }

    #[test]
    fn map_change_event_insert() {
        let known: HashSet<&str> = ["orders"].into_iter().collect();
        let doc = vec![
            ("operationType".to_string(), BsonValue::String("insert".to_string())),
            ("ns".to_string(), ns_doc("test", "orders")),
            ("fullDocument".to_string(), id_key_doc("1")),
        ];
        match map_change_event(&doc, &known).unwrap().unwrap() {
            MappedChange::Event(ChangeEvent::Insert { table, row }) => {
                assert_eq!(table, "orders");
                assert_eq!(row[0], Some(b"1".to_vec()));
            }
            _ => panic!("expected Insert event"),
        }
    }

    #[test]
    fn map_change_event_update_with_full_document() {
        let known: HashSet<&str> = ["orders"].into_iter().collect();
        let doc = vec![
            ("operationType".to_string(), BsonValue::String("update".to_string())),
            ("ns".to_string(), ns_doc("test", "orders")),
            ("documentKey".to_string(), id_key_doc("1")),
            ("fullDocument".to_string(), id_key_doc("1")),
        ];
        match map_change_event(&doc, &known).unwrap().unwrap() {
            MappedChange::Event(ChangeEvent::Update { table, key, row }) => {
                assert_eq!(table, "orders");
                assert_eq!(key[0], Some(b"1".to_vec()));
                assert_eq!(row[0], Some(b"1".to_vec()));
            }
            _ => panic!("expected Update event"),
        }
    }

    #[test]
    fn map_change_event_update_without_full_document_is_skipped() {
        // fullDocument absent (e.g. deleted before updateLookup resolved) — a
        // documented gap, not fabricated from the partial updateDescription diff.
        let known: HashSet<&str> = ["orders"].into_iter().collect();
        let doc = vec![
            ("operationType".to_string(), BsonValue::String("update".to_string())),
            ("ns".to_string(), ns_doc("test", "orders")),
            ("documentKey".to_string(), id_key_doc("1")),
        ];
        assert!(map_change_event(&doc, &known).unwrap().is_none());
    }

    #[test]
    fn map_change_event_delete() {
        let known: HashSet<&str> = ["orders"].into_iter().collect();
        let doc = vec![
            ("operationType".to_string(), BsonValue::String("delete".to_string())),
            ("ns".to_string(), ns_doc("test", "orders")),
            ("documentKey".to_string(), id_key_doc("1")),
        ];
        match map_change_event(&doc, &known).unwrap().unwrap() {
            MappedChange::Event(ChangeEvent::Delete { table, key }) => {
                assert_eq!(table, "orders");
                assert_eq!(key[0], Some(b"1".to_vec()));
            }
            _ => panic!("expected Delete event"),
        }
    }

    #[test]
    fn map_change_event_invalidate() {
        let known: HashSet<&str> = ["orders"].into_iter().collect();
        let doc = vec![("operationType".to_string(), BsonValue::String("invalidate".to_string()))];
        assert!(matches!(map_change_event(&doc, &known).unwrap().unwrap(), MappedChange::Invalidate));
    }

    #[test]
    fn map_change_event_filters_unknown_collections() {
        let known: HashSet<&str> = ["orders"].into_iter().collect();
        let doc = vec![
            ("operationType".to_string(), BsonValue::String("insert".to_string())),
            ("ns".to_string(), ns_doc("test", "other_collection")),
            ("fullDocument".to_string(), id_key_doc("1")),
        ];
        assert!(map_change_event(&doc, &known).unwrap().is_none());
    }

    #[test]
    fn map_change_event_drops_ddl_style_operations() {
        let known: HashSet<&str> = ["orders"].into_iter().collect();
        let doc = vec![("operationType".to_string(), BsonValue::String("drop".to_string())), ("ns".to_string(), ns_doc("test", "orders"))];
        assert!(map_change_event(&doc, &known).unwrap().is_none());
    }

    #[test]
    fn extract_resume_token_reads_data_field() {
        let doc = vec![("_id".to_string(), BsonValue::Document(vec![("_data".to_string(), BsonValue::String("82650A...".to_string()))]))];
        assert_eq!(extract_resume_token(&doc), Some("82650A...".to_string()));
    }

    #[test]
    fn extract_resume_token_none_when_missing() {
        let doc = vec![("operationType".to_string(), BsonValue::String("insert".to_string()))];
        assert_eq!(extract_resume_token(&doc), None);
    }
}
