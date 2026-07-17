//! Harbor/Stream — Kafka source connector. Hand-written Kafka wire
//! protocol client over TCP (port 9092, no `rdkafka` dependency, per this
//! repo's from-scratch rule). Discovery lists topic partitions via the
//! `Metadata` API; snapshot reads every partition from offset 0 and decodes
//! v2 (`magic=2`) record batches into rows; the Verification Engine's
//! checksums read the same batches. `replicate` is a real long-poll tailing
//! consumer (at-least-once) — Kafka has no server-side change-feed concept
//! beyond consuming its own log, so this *is* the change feed.
//!
//! Scope cuts, all documented: consumer-group coordination / rebalancing /
//! partition-leader redirection across a multi-broker cluster is not
//! implemented — the connector talks to the single broker it connected to
//! (correct for single-node / dev Kafka, and the common Harbor target); and
//! compressed record batches (`compression != 0`) are not decoded here
//! (there's no `flate2`/`lz4` dependency wired in yet).

use crate::connector::{ChangeEvent, ConnectorError, SourceConnector, SourceRow};
use crate::schema::{ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 5_000;
const FETCH_MAX_BYTES: i32 = 1_048_576;

mod api_keys {
    pub const METADATA: i16 = 3;
    pub const FETCH: i16 = 1;
}

/// One decoded Kafka message.
#[derive(Debug, Clone)]
struct Record {
    offset: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    ts: i64,
}

/// Minimal Kafka wire-protocol client.
struct KafkaConn {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    correlation_id: i32,
}

impl KafkaConn {
    async fn connect(addr: &str) -> Result<Self> {
        let (host, port) = if let Some(colon_idx) = addr.find(':') {
            (addr[..colon_idx].to_string(), addr[colon_idx + 1..].parse().unwrap_or(9092))
        } else {
            (addr.to_string(), 9092)
        };
        let stream = TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("connecting to Kafka at {host}:{port}"))?;
        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(65536),
            write_buf: BytesMut::with_capacity(16384),
            correlation_id: 0,
        })
    }

    async fn send_request(&mut self, api_key: i16, api_version: i16, body: &[u8]) -> Result<Vec<u8>> {
        self.write_buf.clear();
        // Request size = header(api_key,api_version,correlation_id,client_id)
        // + body. Empty client_id is encoded as the int16 -1.
        let header_len = 2 + 2 + 4 + 2;
        let total = header_len + body.len();
        self.write_buf.put_i32(total as i32);
        self.write_buf.put_i16(api_key);
        self.write_buf.put_i16(api_version);
        self.write_buf.put_i32(self.correlation_id);
        self.write_buf.put_i16(-1); // empty client_id
        self.write_buf.extend_from_slice(body);
        self.correlation_id += 1;

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;

        self.fill(4).await?;
        let len = i32::from_be_bytes(self.read_buf[0..4].try_into().unwrap()) as usize;
        self.read_buf.advance(4);
        self.fill(len).await?;
        Ok(self.read_buf.split_to(len).to_vec())
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

    /// List the partition indices of `topic` via the Metadata API (v1).
    async fn metadata_partitions(&mut self, topic: &str) -> Result<Vec<i32>> {
        let mut body = BytesMut::new();
        body.put_i32(1); // one topic
        put_string(&mut body, topic);
        let resp = self.send_request(api_keys::METADATA, 1, &body).await?;
        parse_metadata_partitions(&resp, topic)
    }

    /// Fetch one partition's records starting at `fetch_offset`. Returns the
    /// decoded records plus the partition's high-watermark (used as the
    /// "are we caught up?" sentinel by the reader).
    async fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
        max_wait_ms: i32,
    ) -> Result<(Vec<Record>, i64)> {
        let mut body = BytesMut::new();
        body.put_i32(-1); // replica_id (consumer = -1)
        body.put_i32(max_wait_ms);
        body.put_i32(1); // min_bytes
        body.put_i32(FETCH_MAX_BYTES);
        body.put_i32(1); // one topic
        put_string(&mut body, topic);
        body.put_i32(1); // one partition
        body.put_i32(partition);
        body.put_i64(fetch_offset);
        body.put_i32(FETCH_MAX_BYTES);
        let resp = self.send_request(api_keys::FETCH, 2, &body).await?;
        parse_fetch_response(&resp, topic, partition)
    }
}

fn put_string(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    buf.put_i16(bytes.len() as i16);
    buf.put_slice(bytes);
}

fn read_string(p: &[u8]) -> Result<(String, &[u8])> {
    if p.len() < 2 {
        bail!("truncated kafka string");
    }
    let len = i16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
    if len == 0xFFFF {
        return Ok((String::new(), &p[2..]));
    }
    if p.len() < 2 + len {
        bail!("truncated kafka string body");
    }
    let s = String::from_utf8_lossy(&p[2..2 + len]).into_owned();
    Ok((s, &p[2 + len..]))
}

/// Zig-zag varint decoding (Kafka record fields).
fn read_varint(buf: &[u8]) -> Result<(i32, usize)> {
    let mut result: i32 = 0;
    let mut shift = 0;
    let mut i = 0;
    loop {
        if i >= buf.len() {
            bail!("varint truncated");
        }
        let byte = buf[i];
        i += 1;
        result |= ((byte & 0x7f) as i32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 32 {
            bail!("varint too long");
        }
    }
    Ok((((result >> 1) ^ -(result & 1)), i))
}

fn read_varlong(buf: &[u8]) -> Result<(i64, usize)> {
    let mut result: i64 = 0;
    let mut shift = 0;
    let mut i = 0;
    loop {
        if i >= buf.len() {
            bail!("varlong truncated");
        }
        let byte = buf[i];
        i += 1;
        result |= ((byte & 0x7f) as i64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            bail!("varlong too long");
        }
    }
    Ok((((result >> 1) ^ -(result & 1)), i))
}

fn parse_metadata_partitions(resp: &[u8], want: &str) -> Result<Vec<i32>> {
    let mut p = &resp[4..]; // skip correlation_id
    let nbrokers = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
    p = &p[4..];
    for _ in 0..nbrokers {
        p = &p[4..]; // node_id
        let (_host, r) = read_string(p)?;
        p = r;
        p = &p[4..]; // port
        let (_rack, r) = read_string(p)?; // rack (nullable string) in v1
        p = r;
    }
    p = &p[4..]; // controller_id
    let ntopics = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
    p = &p[4..];
    let mut parts = Vec::new();
    for _ in 0..ntopics {
        let _err = i16::from_be_bytes(p[0..2].try_into().unwrap());
        p = &p[2..];
        let (name, r) = read_string(p)?;
        p = r;
        p = &p[1..]; // is_internal
        let nparts = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
        p = &p[4..];
        for _ in 0..nparts {
            let perr = i16::from_be_bytes(p[0..2].try_into().unwrap());
            p = &p[2..];
            let pid = i32::from_be_bytes(p[0..4].try_into().unwrap());
            p = &p[4..];
            p = &p[4..]; // leader
            let nrepl = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
            p = &p[4..];
            p = &p[nrepl * 4..];
            let nisr = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
            p = &p[4..];
            p = &p[nisr * 4..];
            if name == want && perr == 0 {
                parts.push(pid);
            }
        }
    }
    Ok(parts)
}

fn parse_fetch_response(resp: &[u8], _topic: &str, _partition: i32) -> Result<(Vec<Record>, i64)> {
    let mut p = &resp[4..]; // skip correlation_id
    p = &p[4..]; // throttle_time_ms
    // responses array
    let nresp = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
    p = &p[4..];
    let mut records = Vec::new();
    let mut high_watermark = 0i64;
    for _ in 0..nresp {
        let (topic, r) = read_string(p)?;
        p = r;
        let nparts = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
        p = &p[4..];
        for _ in 0..nparts {
            let _pid = i32::from_be_bytes(p[0..4].try_into().unwrap());
            p = &p[4..];
            let _err = i16::from_be_bytes(p[0..2].try_into().unwrap());
            p = &p[2..];
            let hw = i64::from_be_bytes(p[0..8].try_into().unwrap());
            p = &p[8..];
            high_watermark = hw;
            let rec_len = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
            p = &p[4..];
            let batch = &p[..rec_len];
            records.extend(decode_record_batch(batch)?);
            p = &p[rec_len..];
        }
        let _ = topic;
    }
    Ok((records, high_watermark))
}

/// Decode a Kafka v2 (`magic=2`) record batch. Returns each record's
/// `(offset, key, value, timestamp)`.
fn decode_record_batch(buf: &[u8]) -> Result<Vec<Record>> {
    if buf.len() < 21 {
        return Ok(Vec::new());
    }
    let base_offset = i64::from_be_bytes(buf[0..8].try_into().unwrap());
    let batch_length = i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
    let end = (12 + batch_length).min(buf.len());
    let b = &buf[12..end];
    let mut p = 0;
    p += 4; // partitionLeaderEpoch
    let magic = b[p];
    p += 1;
    if magic != 2 {
        bail!("unsupported record batch magic {magic} (only magic=2 supported)");
    }
    p += 4; // crc
    let attributes = i16::from_be_bytes(b[p..p + 2].try_into().unwrap());
    p += 2;
    p += 4; // lastOffsetDelta
    let first_ts = i64::from_be_bytes(b[p..p + 8].try_into().unwrap());
    p += 8;
    p += 8; // maxTimestamp
    p += 8; // producerId
    p += 2; // producerEpoch
    p += 4; // baseSequence
    let count = i32::from_be_bytes(b[p..p + 4].try_into().unwrap()) as usize;
    p += 4;

    let compression = attributes & 0x07;
    if compression != 0 {
        bail!(
            "compressed record batches (compression={compression}) are not decoded by this snapshot path (no flate2/lz4 dependency wired in)"
        );
    }

    let mut records = Vec::with_capacity(count);
    let mut rp = &b[p..];
    for i in 0..count {
        let (_len, n) = read_varint(rp)?;
        rp = &rp[n..];
        rp = &rp[1..]; // attributes (int8)
        let (ts_delta, n) = read_varlong(rp)?;
        rp = &rp[n..];
        let _off_delta = read_varint(rp)?;
        rp = &rp[_off_delta.1..];
        let (key_len, n) = read_varint(rp)?;
        rp = &rp[n..];
        let key = if key_len < 0 {
            None
        } else {
            let kl = key_len as usize;
            if rp.len() < kl {
                bail!("truncated record key");
            }
            let k = rp[..kl].to_vec();
            rp = &rp[kl..];
            Some(k)
        };
        let (val_len, n) = read_varint(rp)?;
        rp = &rp[n..];
        let value = if val_len < 0 {
            None
        } else {
            let vl = val_len as usize;
            if rp.len() < vl {
                bail!("truncated record value");
            }
            let v = rp[..vl].to_vec();
            rp = &rp[vl..];
            Some(v)
        };
        let (hdr_count, n) = read_varint(rp)?;
        rp = &rp[n..];
        for _ in 0..hdr_count {
            let (hk, n) = read_varint(rp)?;
            rp = &rp[n..];
            let hkl = if hk < 0 { 0 } else { hk as usize };
            if rp.len() < hkl {
                bail!("truncated record header key");
            }
            rp = &rp[hkl..];
            let (hv, n) = read_varint(rp)?;
            rp = &rp[n..];
            let hvl = if hv < 0 { 0 } else { hv as usize };
            if rp.len() < hvl {
                bail!("truncated record header value");
            }
            rp = &rp[hvl..];
        }
        records.push(Record {
            offset: base_offset + i as i64,
            key,
            value,
            ts: first_ts + ts_delta,
        });
    }
    Ok(records)
}

pub struct KafkaSource {
    conn: KafkaConn,
    addr: String,
}

impl KafkaSource {
    pub async fn connect(addr: &str) -> Result<Self> {
        let conn = KafkaConn::connect(addr).await?;
        Ok(Self {
            conn,
            addr: addr.to_string(),
        })
    }

    async fn read_all(&mut self, topic: &str) -> Result<(Vec<Record>, i64)> {
        let partitions = self.conn.metadata_partitions(topic).await?;
        if partitions.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let mut all = Vec::new();
        let mut hw = 0i64;
        for pid in partitions {
            let mut offset: i64 = 0;
            loop {
                let (records, part_hw) = self.conn.fetch(topic, pid, offset, 0).await?;
                hw = part_hw;
                if records.is_empty() {
                    break;
                }
                let next = records.last().map(|r| r.offset + 1).unwrap_or(offset);
                all.extend(records);
                offset = next;
                if offset >= part_hw {
                    break;
                }
            }
        }
        Ok((all, hw))
    }
}

#[async_trait]
impl SourceConnector for KafkaSource {
    fn name(&self) -> &'static str {
        "Harbor/Stream"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        // Kafka has no catalog of schemas, only topics/partitions. Each topic
        // becomes a table with a fixed shape; real message bodies are left as
        // raw bytes (the target column is BYTEA/JSONB) since the wire format
        // (JSON / Avro / Protobuf) is consumer-defined.
        let body = {
            let mut b = BytesMut::new();
            b.put_i32(-1); // all topics
            b
        };
        let resp = self
            .conn
            .send_request(api_keys::METADATA, 1, &body)
            .await
            .map_err(ConnectorError::Other)?;
        let topics = parse_metadata_topic_names(&resp);

        Ok(topics
            .into_iter()
            .map(|name| TableSchema {
                schema: "kafka".to_string(),
                name,
                columns: vec![
                    ColumnSchema {
                        name: "offset".to_string(),
                        source_type: "int64".to_string(),
                        keystone_type: "BIGINT".to_string(),
                        nullable: false,
                        is_primary_key: true,
                    },
                    ColumnSchema {
                        name: "key".to_string(),
                        source_type: "bytes".to_string(),
                        keystone_type: "BYTEA".to_string(),
                        nullable: true,
                        is_primary_key: false,
                    },
                    ColumnSchema {
                        name: "value".to_string(),
                        source_type: "bytes".to_string(),
                        keystone_type: "JSONB".to_string(),
                        nullable: true,
                        is_primary_key: false,
                    },
                    ColumnSchema {
                        name: "timestamp".to_string(),
                        source_type: "int64".to_string(),
                        keystone_type: "BIGINT".to_string(),
                        nullable: true,
                        is_primary_key: false,
                    },
                ],
            })
            .collect())
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let (records, _) = self.read_all(&table.name).await.map_err(ConnectorError::Other)?;
        let mut total: u64 = 0;
        let mut batch: Vec<SourceRow> = Vec::with_capacity(SNAPSHOT_BATCH_SIZE);
        for r in &records {
            let row: SourceRow = vec![
                Some(r.offset.to_string().into_bytes()),
                r.key.clone(),
                r.value.clone(),
                Some(r.ts.to_string().into_bytes()),
            ];
            batch.push(row);
            total += 1;
            if batch.len() >= SNAPSHOT_BATCH_SIZE {
                if tx.send(std::mem::take(&mut batch)).await.is_err() {
                    return Ok(total);
                }
            }
        }
        if !batch.is_empty() {
            let _ = tx.send(batch).await;
        }
        Ok(total)
    }

    async fn replicate(&mut self, tables: &[TableSchema], resume_token: Option<String>, tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        let mut offsets: HashMap<i32, i64> = resume_token
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default();

        for table in tables {
            let partitions = self
                .conn
                .metadata_partitions(&table.name)
                .await
                .map_err(ConnectorError::Other)?;
            if partitions.is_empty() {
                continue;
            }
            // Seed each partition at the high-watermark (tail) when no resume
            // offset is known, so the live phase only streams new messages.
            for pid in &partitions {
                if !offsets.contains_key(pid) {
                    let (_, hw) = self.conn.fetch(&table.name, *pid, 0, 0).await.map_err(ConnectorError::Other)?;
                    offsets.insert(*pid, hw);
                }
            }

            let mut stop = false;
            while !stop {
                for pid in &partitions {
                    let off = *offsets.get(pid).unwrap_or(&0);
                    let (records, _hw) = self
                        .conn
                        .fetch(&table.name, *pid, off, 500)
                        .await
                        .map_err(ConnectorError::Other)?;
                    for r in &records {
                        let row: SourceRow = vec![
                            Some(r.offset.to_string().into_bytes()),
                            r.key.clone(),
                            r.value.clone(),
                            Some(r.ts.to_string().into_bytes()),
                        ];
                        let ev = ChangeEvent::Insert {
                            table: table.name.clone(),
                            row,
                        };
                        if tx.send(ev).await.is_err() {
                            stop = true;
                            break;
                        }
                    }
                    if let Some(last) = records.last() {
                        offsets.insert(*pid, last.offset + 1);
                    }
                }
                // Persist the resume position after each sweep.
                let token = json!(&offsets).to_string();
                if tx.send(ChangeEvent::CommitLsn(token)).await.is_err() {
                    stop = true;
                }
                if !stop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        Ok(())
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let (records, _) = self.read_all(&table.name).await.map_err(ConnectorError::Other)?;
        Ok(records
            .iter()
            .map(|r| {
                let row: SourceRow = vec![
                    Some(r.offset.to_string().into_bytes()),
                    r.key.clone(),
                    r.value.clone(),
                    Some(r.ts.to_string().into_bytes()),
                ];
                crate::verify::hash_row(&row)
            })
            .collect())
    }
}

fn parse_metadata_topic_names(resp: &[u8]) -> Vec<String> {
    let mut names = Vec::new();
    let mut p = &resp[4..]; // skip correlation_id
    let nbrokers = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
    p = &p[4..];
    for _ in 0..nbrokers {
        p = &p[4..];
        if let Ok((_h, r)) = read_string(p) {
            p = r;
        } else {
            break;
        }
        p = &p[4..];
        if let Ok((_r, r)) = read_string(p) {
            p = r;
        } else {
            break;
        }
    }
    p = &p[4..]; // controller_id
    let ntopics = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
    p = &p[4..];
    for _ in 0..ntopics {
        p = &p[2..]; // error_code
        if let Ok((name, r)) = read_string(p) {
            p = r;
            if !name.is_empty() && !name.starts_with('_') {
                names.push(name);
            }
        } else {
            break;
        }
        p = &p[1..]; // is_internal
        let nparts = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
        p = &p[4..];
        for _ in 0..nparts {
            p = &p[2 + 4 + 4..]; // err, partition, leader
            let nrepl = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
            p = &p[4..];
            p = &p[nrepl * 4..];
            let nisr = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
            p = &p[4..];
            p = &p[nisr * 4..];
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for v in [0i32, 1, -1, 63, -64, 8191, -8192] {
            let mut buf = Vec::new();
            // encode by hand (zigzag + LEB128) just to feed the decoder
            let zz = ((v << 1) ^ (v >> 31)) as u32;
            let mut x = zz;
            loop {
                let mut b = (x & 0x7f) as u8;
                x >>= 7;
                if x != 0 {
                    b |= 0x80;
                }
                buf.push(b);
                if x == 0 {
                    break;
                }
            }
            let (dec, n) = read_varint(&buf).unwrap();
            assert_eq!(dec, v, "varint decode for {v}");
            assert_eq!(n, buf.len());
        }
    }

    #[test]
    fn decodes_uncompressed_v2_record_batch() {
        // Build a minimal v2 batch with 2 records: magic=2, no compression.
        let mut b = BytesMut::new();
        b.put_i64(100); // baseOffset
        b.put_i32(0); // batchLength placeholder (filled later)
        let header_start = 12usize;
        b.put_i32(0); // partitionLeaderEpoch
        b.put_i8(2); // magic
        b.put_i32(0); // crc
        b.put_i16(0); // attributes (compression=0)
        b.put_i32(1); // lastOffsetDelta
        b.put_i64(1_600_000_000_000); // firstTimestamp
        b.put_i64(1_600_000_000_000); // maxTimestamp
        b.put_i64(0); // producerId
        b.put_i16(0); // producerEpoch
        b.put_i32(0); // baseSequence
        b.put_i32(2); // record count
        // Records are zigzag-varint-framed on the wire (length, ts delta,
        // offset delta, key/value lengths, header count), not fixed-width
        // ints — build each record body first so its (varint-encoded)
        // length can be computed, matching what `decode_record_batch`
        // actually parses.
        fn zigzag_varint(n: i64) -> Vec<u8> {
            let mut v = ((n << 1) ^ (n >> 63)) as u64;
            let mut out = Vec::new();
            loop {
                let mut byte = (v & 0x7f) as u8;
                v >>= 7;
                if v != 0 {
                    byte |= 0x80;
                }
                out.push(byte);
                if v == 0 {
                    break;
                }
            }
            out
        }
        fn encode_record(off_delta: i64, ts_delta: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> Vec<u8> {
            let mut body = Vec::new();
            body.push(0u8); // attributes (int8)
            body.extend(zigzag_varint(ts_delta));
            body.extend(zigzag_varint(off_delta));
            match key {
                Some(k) => { body.extend(zigzag_varint(k.len() as i64)); body.extend_from_slice(k); }
                None => body.extend(zigzag_varint(-1)),
            }
            match value {
                Some(v) => { body.extend(zigzag_varint(v.len() as i64)); body.extend_from_slice(v); }
                None => body.extend(zigzag_varint(-1)),
            }
            body.extend(zigzag_varint(0)); // header count
            let mut record = zigzag_varint(body.len() as i64);
            record.extend(body);
            record
        }
        // record 1: no key, value "abc"
        b.put_slice(&encode_record(0, 0, None, Some(b"abc")));
        // record 2: no key, value "xy"
        b.put_slice(&encode_record(1, 0, None, Some(b"xy")));
        let batch = b.freeze();
        let batch_length = (batch.len() - header_start) as i32;
        let mut final_buf = batch.to_vec();
        final_buf[8..12].copy_from_slice(&batch_length.to_be_bytes());

        let recs = decode_record_batch(&final_buf).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].offset, 100);
        assert_eq!(recs[0].value.as_deref(), Some(b"abc".as_ref()));
        assert_eq!(recs[0].key, None);
        assert_eq!(recs[1].offset, 101);
        assert_eq!(recs[1].value.as_deref(), Some(b"xy".as_ref()));
    }

    #[test]
    fn metadata_partitions_parses() {
        // Hand-built Metadata v1 response: one broker, one topic "t" with
        // partitions 0 and 1 (both leader present).
        let mut b = BytesMut::new();
        b.put_i32(4); // correlation_id (skip target)
        b.put_i32(1); // # brokers
        b.put_i32(1); // node_id
        put_string(&mut b, "localhost");
        b.put_i32(9092); // port
        put_string(&mut b, ""); // rack (empty string)
        b.put_i32(1); // controller_id
        b.put_i32(1); // # topics
        b.put_i16(0); // topic error_code
        put_string(&mut b, "t");
        b.put_i8(0); // is_internal
        b.put_i32(2); // # partitions
        for pid in [0i32, 1i32] {
            b.put_i16(0); // partition error_code
            b.put_i32(pid);
            b.put_i32(1); // leader
            b.put_i32(1); // # replicas
            b.put_i32(1);
            b.put_i32(1); // # isr
            b.put_i32(1);
        }
        let parts = parse_metadata_partitions(&b, "t").unwrap();
        assert_eq!(parts, vec![0, 1]);
    }
}
