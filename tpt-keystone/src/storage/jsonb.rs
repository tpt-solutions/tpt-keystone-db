//! Canopy (Phase 10) native JSON binary storage format — a hand-written,
//! compact tag/length/value encoding for `serde_json::Value`, in the spirit
//! of Postgres's `jsonb`/MongoDB's BSON (though not bit-compatible with
//! either — same "honest, not a drop-in" discipline as Meridian's S2/H3
//! implementations).
//!
//! This is used internally by the path index (`canopy_index::JsonPathIndex`)
//! and the inverted full-text index (`canopy_index::FtsIndex`) to build
//! compact, canonically-ordered on-disk keys. Row storage itself keeps
//! storing `Json` columns as text (see `ColumnType::Json` docs) — consistent
//! with the pre-existing text-sniffing row model every other column type
//! uses (`executor::eval::Value::from_bytes`), the same limitation
//! `Geometry` already lives with. Object keys are sorted before encoding so
//! two structurally-equal documents with differently-ordered keys encode to
//! identical bytes — required for both the path index and future
//! containment-style comparisons.

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
        let byte = *buf.get(*pos).ok_or_else(|| anyhow::anyhow!("jsonb: truncated varint"))?;
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
    let tag = *buf.get(*pos).ok_or_else(|| anyhow::anyhow!("jsonb: truncated value"))?;
    *pos += 1;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_FALSE => Ok(Value::Bool(false)),
        TAG_TRUE => Ok(Value::Bool(true)),
        TAG_INT => {
            let bytes = buf.get(*pos..*pos + 8).ok_or_else(|| anyhow::anyhow!("jsonb: truncated int"))?;
            *pos += 8;
            Ok(Value::Number(Number::from(i64::from_be_bytes(bytes.try_into().unwrap()))))
        }
        TAG_FLOAT => {
            let bytes = buf.get(*pos..*pos + 8).ok_or_else(|| anyhow::anyhow!("jsonb: truncated float"))?;
            *pos += 8;
            let f = f64::from_be_bytes(bytes.try_into().unwrap());
            Ok(Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null))
        }
        TAG_STRING => {
            let len = read_varint(buf, pos)? as usize;
            let bytes = buf.get(*pos..*pos + len).ok_or_else(|| anyhow::anyhow!("jsonb: truncated string"))?;
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
                let kbytes = buf.get(*pos..*pos + klen).ok_or_else(|| anyhow::anyhow!("jsonb: truncated key"))?;
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
}
