//! Canopy (Phase 10) native JSON binary storage format — a hand-written,
//! compact tag/length/value encoding for `serde_json::Value`, in the spirit
//! of Postgres's `jsonb`/MongoDB's BSON (though not bit-compatible with
//! either — same "honest, not a drop-in" discipline as Meridian's S2/H3
//! implementations).
//!
//! This is used internally by the path index (`canopy_index::JsonPathIndex`)
//! and the inverted full-text index (`canopy_index::FtsIndex`) to build
//! compact, canonically-ordered on-disk keys, and — as of the Phase 10
//! "native JSON/BSON binary storage format" wiring — optionally as the on-disk
//! representation of `Json` row columns themselves, via [`encode_cell`] /
//! [`decode_cell`] (opt-in through `Database::set_jsonb_binary_storage` /
//! `TPT_JSONB_BINARY=1`; off by default, so raw-text storage remains the
//! default and every stored cell is self-describing via [`CELL_MARKER`]).
//! Object keys are sorted before encoding so two structurally-equal documents
//! with differently-ordered keys encode to identical bytes — required for both
//! the path index and containment-style comparisons.

use anyhow::{bail, Result};
use serde_json::{Map, Number, Value};

const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_INT: u8 = 3;
const TAG_FLOAT: u8 = 4;
const TAG_STRING: u8 = 5;
const TAG_ARRAY: u8 = 6;
const TAG_OBJECT: u8 = 7;

/// Encode a `serde_json::Value` into Canopy's native binary format.
pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(value, &mut out);
    out
}

/// Row-storage marker prefix identifying a stored cell as native binary jsonb
/// rather than raw JSON text. Byte 0 (`0x00`) can never begin a valid
/// wire-text cell (JSON text, Postgres text-format integers/floats/bools, or
/// `\x`-prefixed bytea hex all start with printable ASCII), so a stored cell
/// is self-describing: the read path (`decode_cell`) checks this prefix
/// without needing the column's declared type. Byte 1 (`0x01`) is a format
/// version, leaving room to evolve the encoding later.
pub const CELL_MARKER: [u8; 2] = [0x00, 0x01];

/// Encode a stored row cell holding JSON *text* into native binary jsonb,
/// prefixed with [`CELL_MARKER`]. If the text is not valid JSON, the original
/// bytes are returned unchanged (invalid JSON is stored verbatim rather than
/// rejected here — schema validation, if any, already ran upstream).
pub fn encode_cell(json_text: &[u8]) -> Vec<u8> {
    match serde_json::from_slice::<Value>(json_text) {
        Ok(value) => {
            let mut out = Vec::with_capacity(json_text.len());
            out.extend_from_slice(&CELL_MARKER);
            encode_into(&value, &mut out);
            out
        }
        Err(_) => json_text.to_vec(),
    }
}

/// True if `bytes` is a native-binary-jsonb stored cell (has [`CELL_MARKER`]).
pub fn is_binary_cell(bytes: &[u8]) -> bool {
    bytes.len() >= 2 && bytes[0] == CELL_MARKER[0] && bytes[1] == CELL_MARKER[1]
}

/// Decode a stored row cell back into canonical (compact, sorted-key) JSON
/// text if it is a native-binary-jsonb cell; returns `None` for any cell that
/// isn't (raw-text cells, other column types) so the caller leaves it as-is.
///
/// Like Postgres `jsonb`, this does not preserve the original insertion
/// whitespace or object-key order — the returned text is `serde_json`'s
/// compact serialization of the decoded value.
pub fn decode_cell(bytes: &[u8]) -> Option<Vec<u8>> {
    if !is_binary_cell(bytes) {
        return None;
    }
    let value = decode(&bytes[CELL_MARKER.len()..]).ok()?;
    serde_json::to_vec(&value).ok()
}

fn write_varint(out: &mut Vec<u8>, mut n: u64) {
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = *buf
            .get(*pos)
            .ok_or_else(|| anyhow::anyhow!("jsonb: truncated varint"))?;
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

fn encode_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(TAG_NULL),
        Value::Bool(false) => out.push(TAG_FALSE),
        Value::Bool(true) => out.push(TAG_TRUE),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push(TAG_INT);
                out.extend_from_slice(&i.to_be_bytes());
            } else {
                out.push(TAG_FLOAT);
                out.extend_from_slice(&n.as_f64().unwrap_or(0.0).to_be_bytes());
            }
        }
        Value::String(s) => {
            out.push(TAG_STRING);
            write_varint(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        Value::Array(items) => {
            out.push(TAG_ARRAY);
            write_varint(out, items.len() as u64);
            for item in items {
                encode_into(item, out);
            }
        }
        Value::Object(map) => {
            out.push(TAG_OBJECT);
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            write_varint(out, entries.len() as u64);
            for (k, v) in entries {
                write_varint(out, k.len() as u64);
                out.extend_from_slice(k.as_bytes());
                encode_into(v, out);
            }
        }
    }
}

/// Decode Canopy's native binary format back into a `serde_json::Value`.
pub fn decode(buf: &[u8]) -> Result<Value> {
    let mut pos = 0;
    let value = decode_at(buf, &mut pos)?;
    Ok(value)
}

fn decode_at(buf: &[u8], pos: &mut usize) -> Result<Value> {
    let tag = *buf
        .get(*pos)
        .ok_or_else(|| anyhow::anyhow!("jsonb: truncated value"))?;
    *pos += 1;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_FALSE => Ok(Value::Bool(false)),
        TAG_TRUE => Ok(Value::Bool(true)),
        TAG_INT => {
            let bytes = buf
                .get(*pos..*pos + 8)
                .ok_or_else(|| anyhow::anyhow!("jsonb: truncated int"))?;
            *pos += 8;
            Ok(Value::Number(Number::from(i64::from_be_bytes(
                bytes.try_into().unwrap(),
            ))))
        }
        TAG_FLOAT => {
            let bytes = buf
                .get(*pos..*pos + 8)
                .ok_or_else(|| anyhow::anyhow!("jsonb: truncated float"))?;
            *pos += 8;
            let f = f64::from_be_bytes(bytes.try_into().unwrap());
            Ok(Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        TAG_STRING => {
            let len = read_varint(buf, pos)? as usize;
            let bytes = buf
                .get(*pos..*pos + len)
                .ok_or_else(|| anyhow::anyhow!("jsonb: truncated string"))?;
            *pos += len;
            Ok(Value::String(String::from_utf8_lossy(bytes).into_owned()))
        }
        TAG_ARRAY => {
            let len = read_varint(buf, pos)? as usize;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(decode_at(buf, pos)?);
            }
            Ok(Value::Array(items))
        }
        TAG_OBJECT => {
            let len = read_varint(buf, pos)? as usize;
            let mut map = Map::with_capacity(len);
            for _ in 0..len {
                let klen = read_varint(buf, pos)? as usize;
                let kbytes = buf
                    .get(*pos..*pos + klen)
                    .ok_or_else(|| anyhow::anyhow!("jsonb: truncated key"))?;
                *pos += klen;
                let key = String::from_utf8_lossy(kbytes).into_owned();
                let value = decode_at(buf, pos)?;
                map.insert(key, value);
            }
            Ok(Value::Object(map))
        }
        other => bail!("jsonb: unknown tag byte {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn roundtrip(v: Value) {
        let encoded = encode(&v);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(v, decoded);
    }

    #[test]
    fn roundtrips_scalars() {
        roundtrip(json!(null));
        roundtrip(json!(true));
        roundtrip(json!(false));
        roundtrip(json!(42));
        roundtrip(json!(-17));
        roundtrip(json!(3.5));
        roundtrip(json!("hello world"));
        roundtrip(json!(""));
    }

    #[test]
    fn roundtrips_nested_structures() {
        roundtrip(json!({
            "user": {"name": "Ada", "address": {"city": "Wellington", "zip": 6011}},
            "tags": ["admin", "beta", 3, null, true],
        }));
    }

    #[test]
    fn object_key_order_is_canonicalized() {
        let a = json!({"b": 1, "a": 2});
        let b = json!({"a": 2, "b": 1});
        assert_eq!(encode(&a), encode(&b));
    }

    #[test]
    fn cell_roundtrip_through_binary_storage() {
        let text = br#"{"user": {"name": "Ada"}, "tags": [1, 2, 3]}"#;
        let stored = encode_cell(text);
        assert!(is_binary_cell(&stored));
        // Stored form is smaller than / different from the text.
        assert_ne!(&stored[..], &text[..]);
        let decoded = decode_cell(&stored).unwrap();
        // Decodes to canonical (compact, sorted-key) JSON, structurally equal.
        let orig: Value = serde_json::from_slice(text).unwrap();
        let back: Value = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(orig, back);
    }

    #[test]
    fn non_binary_cell_passes_through() {
        let text = b"just some text";
        assert!(!is_binary_cell(text));
        assert!(decode_cell(text).is_none());
    }

    #[test]
    fn invalid_json_is_stored_verbatim() {
        let text = b"{not valid json";
        let stored = encode_cell(text);
        assert_eq!(&stored[..], &text[..]);
        assert!(!is_binary_cell(&stored));
    }

    #[test]
    fn text_starting_with_brace_is_not_mistaken_for_binary() {
        // Raw JSON text starts with '{' (0x7b), never the 0x00 marker.
        assert!(!is_binary_cell(br#"{"a":1}"#));
    }
}
