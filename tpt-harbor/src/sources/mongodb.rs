//! Harbor/Mongo — MongoDB source connector. Hand-written MongoDB OP_MSG
//! protocol over TCP (port 27017). Discovery uses `listCollections` to
//! enumerate collections and their field types. Snapshot uses `find` with
//! batch cursor iteration via `getMore`. CDC is scope-cut to
//! `Unimplemented` — MongoDB change streams/oplog tailing is a substantial
//! protocol effort on its own.

use crate::connector::{ConnectorError, SourceConnector, SourceRow, ChangeEvent};
use crate::schema::{from_mongodb_type, ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: i64 = 1_000;

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

fn bson_encode_doc(name: &str, elements: &[u8]) -> Vec<u8> {
    let mut out = vec![0x03]; // type: document
    out.extend_from_slice(&bson_encode_cstring(name));
    let doc_len = elements.len() as i32 + 5; // elements + length(4) + terminator(1)
    out.extend_from_slice(&doc_len.to_le_bytes());
    out.extend_from_slice(elements);
    out.push(0);
    out
}

fn bson_build_doc(elements: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::new();
    for el in elements {
        out.extend_from_slice(&el);
    }
    out.push(0); // terminator
    let total = out.len() as i32;
    let mut doc = total.to_le_bytes().to_vec();
    doc.extend_from_slice(&out);
    doc
}

fn bson_encode_array(name: &str, items: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out = vec![0x04]; // type: array
    out.extend_from_slice(&bson_encode_cstring(name));
    let mut arr = Vec::new();
    for (i, item) in items.into_iter().enumerate() {
        arr.extend_from_slice(&bson_encode_cstring(&i.to_string()));
        arr.extend_from_slice(&item);
    }
    arr.push(0);
    let arr_len = arr.len() as i32 + 4;
    out.extend_from_slice(&arr_len.to_le_bytes());
    out.extend_from_slice(&arr);
    out
}

fn bson_empty_doc() -> Vec<u8> {
    vec![5, 0, 0, 0, 0]
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

// Infer a flat schema from a list of BSON documents — maps each unique
// top-level field to the narrowest Keystone type that fits all observed
// values.
fn infer_schema(docs: &[Vec<(String, BsonValue)>]) -> Vec<(String, String)> {
    let mut field_types: Vec<(String, String)> = Vec::new();

    // Collect all unique field names across all docs
    let mut seen_names = Vec::new();
    for doc in docs {
        for (name, _) in doc {
            if !seen_names.contains(name) {
                seen_names.push(name.clone());
            }
        }
    }

    for name in seen_names {
        let mut keystone_type = "TEXT".to_string();
        for doc in docs {
            for (k, v) in doc {
                if k == &name {
                    let t = match v {
                        BsonValue::Double(_) => "DOUBLE PRECISION",
                        BsonValue::Int32(_) => "INTEGER",
                        BsonValue::Int64(_) => "BIGINT",
                        BsonValue::Boolean(_) => "BOOLEAN",
                        BsonValue::String(_) => "TEXT",
                        BsonValue::Document(_) | BsonValue::Array(_) => "JSONB",
                        BsonValue::Null => continue,
                        _ => "TEXT",
                    };
                    keystone_type = t.to_string();
                    break;
                }
            }
        }
        field_types.push((name, keystone_type));
    }

    field_types
}

// ── MongoDB wire protocol client ─────────────────────────────────────

struct MongoConn {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    request_id: i32,
}

impl MongoConn {
    async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.with_context(|| format!("connecting to MongoDB at {addr}"))?;
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(65536),
            write_buf: BytesMut::with_capacity(65536),
            request_id: 1,
        };

        // OP_MSG hello/ismaster
        let cmd = bson_build_doc(vec![
            bson_encode_string_typed("hello", "1"),
            bson_encode_int32("helloOk", 1),
        ]);
        let response = conn.send_op_msg(1, &cmd, 0).await?;

        // Check for ok
        if let Some((_, BsonValue::Double(v))) = response.iter().find(|(k, _)| k == "ok") {
            if (*v - 1.0).abs() > 0.01 {
                bail!("MongoDB hello failed: ok={v}");
            }
        }

        Ok(conn)
    }

    async fn send_op_msg(&mut self, flags: u32, body: &[u8], db_selector: u8) -> Result<Vec<(String, BsonValue)>> {
        // OP_MSG: header(16) + flags(4) + section_kind(1) + section_body
        let section_len = 4 + 1 + body.len() as u32; // size(4) + body
        let msg_len = 16 + 4 + 1 + section_len;
        let req_id = self.request_id;
        self.request_id += 1;

        self.write_buf.put_i32_le(msg_len); // message length
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
                    p = &p[doc_size..];
                    return Ok(doc);
                } else {
                    // Document sequence: size(4) + identifier(cstring) + docs
                    if p.len() < 4 { break; }
                    let _seq_size = i32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
                    p = &p[4..];
                    let _id_end = p.iter().position(|&b| b == 0).unwrap_or(p.len());
                    p = &p[_id_end + 1..];
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
    addr: String,
    database: String,
}

impl MongoSource {
    pub async fn connect(addr: &str, database: &str) -> Result<Self> {
        let conn = MongoConn::connect(addr).await?;
        Ok(Self {
            conn,
            addr: addr.to_string(),
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

                        // Extract field types from the options.info.fields or
                        // do a sample query to infer the schema
                        let fields = coll_info
                            .iter()
                            .find(|(k, _)| k == "options")
                            .and_then(|(_, v)| if let BsonValue::Document(d) = v { Some(d) } else { None })
                            .and_then(|opts| opts.iter().find(|(k, _)| k == "validator"))
                            .and_then(|(_, v)| if let BsonValue::Document(d) = v { Some(d) } else { None });

                        // Sample a few documents to infer schema
                        let sample_cmd = bson_build_doc(vec![
                            bson_encode_string_typed("find", &coll_name),
                            bson_encode_string_typed("filter", "{}"),
                            bson_encode_int64("limit", 10),
                        ]);

                        if let Ok(sample_resp) = self.conn.send_op_msg(0, &sample_cmd, 0).await {
                            let mut sample_docs = Vec::new();
                            if let Some((_, BsonValue::Document(cursor_doc))) = sample_resp.iter().find(|(k, _)| k == "cursor") {
                                if let Some((_, BsonValue::Array(batch))) = cursor_doc.iter().find(|(k, _)| k == "firstBatch") {
                                    for doc in batch {
                                        if let BsonValue::Document(d) = doc {
                                            sample_docs.push(d.clone());
                                        }
                                    }
                                }
                            }

                            let inferred = infer_schema(&sample_docs);

                            let mut columns: Vec<ColumnSchema> = Vec::new();
                            // Always add _id as primary key
                            columns.push(ColumnSchema {
                                name: "_id".to_string(),
                                source_type: "objectId".to_string(),
                                keystone_type: "TEXT".to_string(),
                                nullable: false,
                                is_primary_key: true,
                            });

                            for (fname, ftype) in inferred {
                                if fname == "_id" {
                                    continue;
                                }
                                columns.push(ColumnSchema {
                                    keystone_type: from_mongodb_type(&ftype),
                                    nullable: true,
                                    is_primary_key: false,
                                    name: fname.clone(),
                                    source_type: ftype,
                                });
                            }

                            tables.push(TableSchema {
                                schema: self.database.clone(),
                                name: coll_name,
                                columns,
                            });
                        }
                    }
                }
            }
        }

        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let mut total: u64 = 0;
        let mut batch_size: i64 = SNAPSHOT_BATCH_SIZE;
        let mut cursor_id: i64 = 0;

        // First query
        let mut cmd = bson_build_doc(vec![
            bson_encode_string_typed("find", &table.name),
            bson_encode_int64("batchSize", batch_size),
        ]);

        let response = self
            .conn
            .send_op_msg(0, &cmd, 0)
            .await
            .map_err(ConnectorError::Other)?;

        // Extract firstBatch
        let mut batches = extract_batch(&response);

        loop {
            let mut batch_rows: Vec<SourceRow> = Vec::new();
            for doc in &batches {
                let mut row = Vec::with_capacity(table.columns.len());
                for col in &table.columns {
                    if col.name == "_id" {
                        // Extract _id from document
                        let id_val = doc
                            .iter()
                            .find(|(k, _)| k == "_id")
                            .map(|(_, v)| bson_value_to_bytes(v));
                        row.push(id_val);
                    } else {
                        let val = doc
                            .iter()
                            .find(|(k, _)| k == &col.name)
                            .map(|(_, v)| bson_value_to_bytes(v));
                        row.push(val);
                    }
                }
                batch_rows.push(row);
            }

            total += batch_rows.len() as u64;
            if !batch_rows.is_empty() {
                if tx.send(batch_rows).await.is_err() {
                    break;
                }
            }

            // Check if there's a next batch
            let next_cursor = extract_cursor_id(&response);
            if next_cursor == 0 || batches.is_empty() {
                break;
            }

            // getMore
            cursor_id = next_cursor;
            let getmore_cmd = bson_build_doc(vec![
                bson_encode_string_typed("getMore", &cursor_id.to_string()),
                bson_encode_string_typed("collection", &table.name),
                bson_encode_int64("batchSize", batch_size),
            ]);

            let resp = self
                .conn
                .send_op_msg(0, &getmore_cmd, 0)
                .await
                .map_err(ConnectorError::Other)?;

            batches = extract_batch(&resp);
        }

        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        Err(ConnectorError::Unimplemented { connector: "Harbor/Mongo", detail: "MongoDB change stream CDC not yet written" })
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let mut checksums = Vec::new();
        let mut cursor_id: i64 = 0;

        let mut cmd = bson_build_doc(vec![
            bson_encode_string_typed("find", &table.name),
            bson_encode_int64("batchSize", 1000),
        ]);

        let response = self
            .conn
            .send_op_msg(0, &cmd, 0)
            .await
            .map_err(ConnectorError::Other)?;

        let mut batches = extract_batch(&response);

        loop {
            for doc in &batches {
                let row: SourceRow = table
                    .columns
                    .iter()
                    .map(|col| {
                        if col.name == "_id" {
                            doc.iter().find(|(k, _)| k == "_id").map(|(_, v)| bson_value_to_bytes(v))
                        } else {
                            doc.iter().find(|(k, _)| k == &col.name).map(|(_, v)| bson_value_to_bytes(v))
                        }
                    })
                    .collect();
                checksums.push(crate::verify::hash_row(&row));
            }

            let next_cursor = extract_cursor_id(&response);
            if next_cursor == 0 || batches.is_empty() {
                break;
            }

            cursor_id = next_cursor;
            let getmore_cmd = bson_build_doc(vec![
                bson_encode_string_typed("getMore", &cursor_id.to_string()),
                bson_encode_string_typed("collection", &table.name),
                bson_encode_int64("batchSize", 1000),
            ]);

            let resp = self
                .conn
                .send_op_msg(0, &getmore_cmd, 0)
                .await
                .map_err(ConnectorError::Other)?;

            batches = extract_batch(&resp);
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

fn extract_batch(response: &[(String, BsonValue)]) -> Vec<Vec<(String, BsonValue)>> {
    let mut result = Vec::new();
    if let Some((_, BsonValue::Document(cursor_doc))) = response.iter().find(|(k, _)| k == "cursor") {
        if let Some((_, BsonValue::Array(batch))) = cursor_doc.iter().find(|(k, _)| k == "firstBatch") {
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
