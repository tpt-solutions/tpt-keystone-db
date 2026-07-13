//! Minimal, hand-written Protocol Buffers (proto3) wire codec for exactly the
//! Flux gRPC service's message set — no `prost`/`protobuf` crate, no build-time
//! codegen, consistent with this repo's from-scratch wire-protocol rule (see
//! `wire::codec` for the Postgres side and `wire::websocket` for Flux's
//! WebSocket bridge).
//!
//! Only the proto3 wire features these messages actually use are implemented:
//! varint (wire type 0) for `uint64`/`int64`/`uint32`/`bool`, and
//! length-delimited (wire type 2) for `string`/`bytes`/embedded messages/packed
//! or unpacked `repeated`. Unknown fields are skipped rather than rejected, so a
//! newer client sending extra fields still round-trips (proto3 forward
//! compatibility). Fixed32/fixed64/group wire types are skipped-on-read but
//! never emitted (none of these messages use them).
//!
//! The `.proto` this mirrors (kept in `docs/formats/flux_grpc.proto` for
//! independent reimplementation) is:
//!
//! ```proto
//! service Flux {
//!   rpc Subscribe(SubscribeRequest) returns (stream Record);
//!   rpc Publish(PublishRequest) returns (PublishResponse);
//!   rpc Poll(PollRequest) returns (PollResponse);
//! }
//! ```

/// A decoded proto3 message is walked field-by-field; the wire type tells us how
/// to read (or skip) each value.
#[derive(Debug, Clone, PartialEq)]
enum WireType {
    Varint,
    Len,
    Fixed64,
    Fixed32,
    /// Deprecated groups — skipped-on-read, never emitted.
    StartGroup,
    EndGroup,
}

impl WireType {
    fn from_u64(v: u64) -> anyhow::Result<Self> {
        Ok(match v {
            0 => WireType::Varint,
            1 => WireType::Fixed64,
            2 => WireType::Len,
            3 => WireType::StartGroup,
            4 => WireType::EndGroup,
            5 => WireType::Fixed32,
            other => anyhow::bail!("unsupported protobuf wire type {other}"),
        })
    }
}

// ---- low-level varint / length-delimited encoders --------------------------

pub fn write_varint(out: &mut Vec<u8>, mut v: u64) {
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
}

fn write_tag(out: &mut Vec<u8>, field: u32, wire: u64) {
    write_varint(out, ((field as u64) << 3) | wire);
}

fn write_varint_field(out: &mut Vec<u8>, field: u32, v: u64) {
    if v == 0 {
        return; // proto3: default scalar values are not emitted.
    }
    write_tag(out, field, 0);
    write_varint(out, v);
}

fn write_len_field(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    if bytes.is_empty() {
        return; // proto3: empty string/bytes are not emitted.
    }
    write_tag(out, field, 2);
    write_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// A cursor over an encoded protobuf message that yields `(field_number,
/// value)` pairs, transparently skipping unknown/irrelevant fields.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn eof(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn read_varint(&mut self) -> anyhow::Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        loop {
            anyhow::ensure!(self.pos < self.buf.len(), "protobuf: truncated varint");
            let byte = self.buf[self.pos];
            self.pos += 1;
            anyhow::ensure!(shift < 64, "protobuf: varint too long");
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        Ok(result)
    }

    fn read_len_slice(&mut self) -> anyhow::Result<&'a [u8]> {
        let len = self.read_varint()? as usize;
        anyhow::ensure!(
            self.pos + len <= self.buf.len(),
            "protobuf: length-delimited field exceeds buffer"
        );
        let s = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(s)
    }

    /// Returns `Some((field, wire_type))` or `None` at end-of-buffer.
    fn next_tag(&mut self) -> anyhow::Result<Option<(u32, WireType)>> {
        if self.eof() {
            return Ok(None);
        }
        let key = self.read_varint()?;
        let field = (key >> 3) as u32;
        let wire = WireType::from_u64(key & 0x7)?;
        Ok(Some((field, wire)))
    }

    /// Consumes and discards the value for a field of the given wire type.
    fn skip(&mut self, wire: &WireType) -> anyhow::Result<()> {
        match wire {
            WireType::Varint => {
                self.read_varint()?;
            }
            WireType::Len => {
                self.read_len_slice()?;
            }
            WireType::Fixed64 => {
                anyhow::ensure!(self.pos + 8 <= self.buf.len(), "protobuf: truncated fixed64");
                self.pos += 8;
            }
            WireType::Fixed32 => {
                anyhow::ensure!(self.pos + 4 <= self.buf.len(), "protobuf: truncated fixed32");
                self.pos += 4;
            }
            WireType::StartGroup | WireType::EndGroup => {
                anyhow::bail!("protobuf: groups are not supported");
            }
        }
        Ok(())
    }
}

// ---- message types ---------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SubscribeRequest {
    pub topic: String,
}

impl SubscribeRequest {
    pub fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let mut msg = SubscribeRequest::default();
        let mut r = Reader::new(buf);
        while let Some((field, wire)) = r.next_tag()? {
            match (field, &wire) {
                (1, WireType::Len) => {
                    msg.topic = String::from_utf8_lossy(r.read_len_slice()?).into_owned()
                }
                _ => r.skip(&wire)?,
            }
        }
        Ok(msg)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Record {
    pub offset: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp_ms: i64,
    pub topic: String,
}

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint_field(&mut out, 1, self.offset);
        write_len_field(&mut out, 2, &self.key);
        write_len_field(&mut out, 3, &self.value);
        write_varint_field(&mut out, 4, self.timestamp_ms as u64);
        write_len_field(&mut out, 5, self.topic.as_bytes());
        out
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PublishRequest {
    pub topic: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub has_partition: bool,
    pub partition: u32,
}

impl PublishRequest {
    pub fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let mut msg = PublishRequest::default();
        let mut r = Reader::new(buf);
        while let Some((field, wire)) = r.next_tag()? {
            match (field, &wire) {
                (1, WireType::Len) => {
                    msg.topic = String::from_utf8_lossy(r.read_len_slice()?).into_owned()
                }
                (2, WireType::Len) => msg.key = r.read_len_slice()?.to_vec(),
                (3, WireType::Len) => msg.value = r.read_len_slice()?.to_vec(),
                (4, WireType::Varint) => msg.has_partition = r.read_varint()? != 0,
                (5, WireType::Varint) => msg.partition = r.read_varint()? as u32,
                _ => r.skip(&wire)?,
            }
        }
        Ok(msg)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PublishResponse {
    pub partition: u32,
    pub offset: u64,
}

impl PublishResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint_field(&mut out, 1, self.partition as u64);
        write_varint_field(&mut out, 2, self.offset);
        out
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PollRequest {
    pub topic: String,
    pub partition: u32,
    pub group: String,
    pub max: u32,
}

impl PollRequest {
    pub fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let mut msg = PollRequest::default();
        let mut r = Reader::new(buf);
        while let Some((field, wire)) = r.next_tag()? {
            match (field, &wire) {
                (1, WireType::Len) => {
                    msg.topic = String::from_utf8_lossy(r.read_len_slice()?).into_owned()
                }
                (2, WireType::Varint) => msg.partition = r.read_varint()? as u32,
                (3, WireType::Len) => {
                    msg.group = String::from_utf8_lossy(r.read_len_slice()?).into_owned()
                }
                (4, WireType::Varint) => msg.max = r.read_varint()? as u32,
                _ => r.skip(&wire)?,
            }
        }
        Ok(msg)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PollResponse {
    pub records: Vec<Record>,
}

impl PollResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for rec in &self.records {
            // Each `repeated Record` element is a length-delimited embedded
            // message on field 1.
            write_len_field(&mut out, 1, &rec.encode());
        }
        out
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CreateTopicRequest {
    pub name: String,
}

impl CreateTopicRequest {
    pub fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let mut msg = CreateTopicRequest::default();
        let mut r = Reader::new(buf);
        while let Some((field, wire)) = r.next_tag()? {
            match (field, &wire) {
                (1, WireType::Len) => {
                    msg.name = String::from_utf8_lossy(r.read_len_slice()?).into_owned()
                }
                _ => r.skip(&wire)?,
            }
        }
        Ok(msg)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CreateTopicResponse {}

impl CreateTopicResponse {
    pub fn encode(&self) -> Vec<u8> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trips_boundaries() {
        for v in [0u64, 1, 127, 128, 300, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let mut r = Reader::new(&buf);
            assert_eq!(r.read_varint().unwrap(), v);
            assert!(r.eof());
        }
    }

    #[test]
    fn record_encode_is_decodable_as_embedded_message() {
        let rec = Record {
            offset: 42,
            key: b"k".to_vec(),
            value: b"hello".to_vec(),
            timestamp_ms: 1234567890,
            topic: "events".into(),
        };
        let poll = PollResponse {
            records: vec![rec.clone()],
        };
        let bytes = poll.encode();
        // Decode it back by hand to confirm the embedded-message framing.
        let mut r = Reader::new(&bytes);
        let (field, wire) = r.next_tag().unwrap().unwrap();
        assert_eq!(field, 1);
        assert_eq!(wire, WireType::Len);
        let inner = r.read_len_slice().unwrap();
        // Re-parse the inner Record.
        let mut ir = Reader::new(inner);
        let mut got = Record::default();
        while let Some((f, w)) = ir.next_tag().unwrap() {
            match (f, &w) {
                (1, WireType::Varint) => got.offset = ir.read_varint().unwrap(),
                (2, WireType::Len) => got.key = ir.read_len_slice().unwrap().to_vec(),
                (3, WireType::Len) => got.value = ir.read_len_slice().unwrap().to_vec(),
                (4, WireType::Varint) => got.timestamp_ms = ir.read_varint().unwrap() as i64,
                (5, WireType::Len) => {
                    got.topic = String::from_utf8_lossy(ir.read_len_slice().unwrap()).into_owned()
                }
                _ => ir.skip(&w).unwrap(),
            }
        }
        assert_eq!(got, rec);
    }

    #[test]
    fn subscribe_request_decodes_topic() {
        // field 1 (topic), wire type 2, len 3, "abc"
        let mut buf = Vec::new();
        write_len_field(&mut buf, 1, b"abc");
        let req = SubscribeRequest::decode(&buf).unwrap();
        assert_eq!(req.topic, "abc");
    }

    #[test]
    fn publish_request_round_trips_via_manual_encode() {
        let mut buf = Vec::new();
        write_len_field(&mut buf, 1, b"topic");
        write_len_field(&mut buf, 2, b"key");
        write_len_field(&mut buf, 3, b"val");
        write_varint_field(&mut buf, 4, 1);
        write_varint_field(&mut buf, 5, 2);
        let req = PublishRequest::decode(&buf).unwrap();
        assert_eq!(req.topic, "topic");
        assert_eq!(req.key, b"key");
        assert_eq!(req.value, b"val");
        assert!(req.has_partition);
        assert_eq!(req.partition, 2);
    }

    #[test]
    fn unknown_fields_are_skipped() {
        let mut buf = Vec::new();
        write_len_field(&mut buf, 1, b"abc");
        // Unknown field 9, varint.
        write_varint_field(&mut buf, 9, 12345);
        // Unknown field 10, len.
        write_len_field(&mut buf, 10, b"ignored");
        let req = SubscribeRequest::decode(&buf).unwrap();
        assert_eq!(req.topic, "abc");
    }

    #[test]
    fn publish_response_omits_default_zero_values() {
        // proto3: partition=0, offset=0 encode to empty (no bytes).
        let resp = PublishResponse {
            partition: 0,
            offset: 0,
        };
        assert!(resp.encode().is_empty());
        let resp2 = PublishResponse {
            partition: 3,
            offset: 0,
        };
        assert!(!resp2.encode().is_empty());
    }
}
