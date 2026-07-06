//! Chronos compression codecs: Gorilla XOR-delta float encoding,
//! delta-of-delta integer encoding, and dictionary encoding for
//! low-cardinality tag columns. Pure, allocation-based (no `unsafe`,
//! no SIMD) implementations — correctness over raw throughput, matching
//! this codebase's "hand-written, no crates" constraint.

/// Minimal MSB-first bit writer used by the Gorilla encoder.
struct BitWriter {
    buf: Vec<u8>,
    cur: u8,
    nbits: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self { buf: Vec::new(), cur: 0, nbits: 0 }
    }

    fn write_bit(&mut self, bit: bool) {
        self.cur = (self.cur << 1) | (bit as u8);
        self.nbits += 1;
        if self.nbits == 8 {
            self.buf.push(self.cur);
            self.cur = 0;
            self.nbits = 0;
        }
    }

    fn write_bits(&mut self, value: u64, count: u8) {
        for i in (0..count).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    /// Flushes the partial trailing byte (padded with zero bits) and returns
    /// the buffer along with how many bits of the final byte are valid.
    fn finish(mut self) -> (Vec<u8>, u8) {
        let valid_bits_in_last_byte = self.nbits;
        if self.nbits > 0 {
            self.cur <<= 8 - self.nbits;
            self.buf.push(self.cur);
        }
        (self.buf, valid_bits_in_last_byte)
    }
}

struct BitReader<'a> {
    buf: &'a [u8],
    byte_pos: usize,
    bit_pos: u8,
}

impl<'a> BitReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, byte_pos: 0, bit_pos: 0 }
    }

    fn read_bit(&mut self) -> Option<bool> {
        let byte = *self.buf.get(self.byte_pos)?;
        let bit = (byte >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.bit_pos = 0;
            self.byte_pos += 1;
        }
        Some(bit)
    }

    fn read_bits(&mut self, count: u8) -> Option<u64> {
        let mut v = 0u64;
        for _ in 0..count {
            v = (v << 1) | (self.read_bit()? as u64);
        }
        Some(v)
    }
}

/// Encodes a sequence of `f64`s with Gorilla-style XOR delta compression:
/// the first value is stored verbatim (64 bits); every subsequent value is
/// XORed against the previous one, and the XOR result is stored either as a
/// single `0` control bit (identical to previous value) or a `1` control bit
/// followed by the leading/trailing zero-run lengths and the meaningful XOR
/// bits in between (classic Facebook Gorilla paper scheme).
pub fn gorilla_encode(values: &[f64]) -> Vec<u8> {
    let mut w = BitWriter::new();
    if values.is_empty() {
        return Vec::new();
    }
    let mut prev = values[0].to_bits();
    w.write_bits(prev, 64);

    let mut prev_leading: u8 = 65; // sentinel: "no previous window"
    let mut prev_trailing: u8 = 0;

    for &v in &values[1..] {
        let bits = v.to_bits();
        let xor = bits ^ prev;
        if xor == 0 {
            w.write_bit(false);
        } else {
            w.write_bit(true);
            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;
            if prev_leading != 65 && leading >= prev_leading && trailing >= prev_trailing {
                w.write_bit(false);
                let meaningful = 64 - prev_leading - prev_trailing;
                w.write_bits(xor >> prev_trailing, meaningful);
            } else {
                w.write_bit(true);
                w.write_bits(leading as u64, 6);
                let meaningful = 64 - leading - trailing;
                w.write_bits(meaningful as u64, 6); // 0 encodes as 64 on decode
                w.write_bits(xor >> trailing, meaningful);
                prev_leading = leading;
                prev_trailing = trailing;
            }
        }
        prev = bits;
    }

    let (bytes, valid_last_bits) = w.finish();
    let mut out = Vec::with_capacity(bytes.len() + 5);
    out.extend_from_slice(&(values.len() as u32).to_be_bytes());
    out.push(valid_last_bits);
    out.extend_from_slice(&bytes);
    out
}

/// Inverse of [`gorilla_encode`].
pub fn gorilla_decode(data: &[u8]) -> Vec<f64> {
    if data.len() < 5 {
        return Vec::new();
    }
    let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    if count == 0 {
        return Vec::new();
    }
    let mut r = BitReader::new(&data[5..]);
    let mut out = Vec::with_capacity(count);
    let mut prev = r.read_bits(64).unwrap_or(0);
    out.push(f64::from_bits(prev));

    let mut prev_leading: u8 = 65;
    let mut prev_trailing: u8 = 0;

    for _ in 1..count {
        let Some(control) = r.read_bit() else { break };
        if !control {
            out.push(f64::from_bits(prev));
            continue;
        }
        let Some(new_window) = r.read_bit() else { break };
        let (leading, trailing) = if new_window {
            let leading = r.read_bits(6).unwrap_or(0) as u8;
            let meaningful_raw = r.read_bits(6).unwrap_or(0) as u8;
            let meaningful = if meaningful_raw == 0 { 64 } else { meaningful_raw };
            let trailing = 64 - leading - meaningful;
            prev_leading = leading;
            prev_trailing = trailing;
            (leading, trailing)
        } else {
            (prev_leading, prev_trailing)
        };
        let meaningful = 64 - leading - trailing;
        let bits = r.read_bits(meaningful).unwrap_or(0);
        let xor = bits << trailing;
        prev ^= xor;
        out.push(f64::from_bits(prev));
    }
    out
}

fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
}

fn read_varint(data: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let byte = *data.get(*pos)?;
        *pos += 1;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Some(v)
}

/// Delta-of-delta encoding for integer sequences (timestamps or integer
/// metrics): stores the first value verbatim, the second value's delta from
/// the first, and every subsequent value as the second difference — the
/// typical case for evenly-spaced time-series timestamps, where the
/// second difference is usually zero and zigzag-varint-encodes to one byte.
/// Intended for timestamps/slowly-varying counters: values whose successive
/// deltas swing across most of the `i64` range can overflow the internal
/// `delta`/second-difference arithmetic (panics on overflow in debug
/// builds, wraps in release — same behavior as any other plain integer
/// arithmetic in Rust).
pub fn delta_delta_encode(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    write_varint(&mut out, values.len() as u64);
    if values.is_empty() {
        return out;
    }
    write_varint(&mut out, zigzag_encode(values[0]));
    if values.len() == 1 {
        return out;
    }
    let mut prev = values[0];
    let mut prev_delta = values[1] - values[0];
    write_varint(&mut out, zigzag_encode(prev_delta));
    prev = values[1];
    for &v in &values[2..] {
        let delta = v - prev;
        let dd = delta - prev_delta;
        write_varint(&mut out, zigzag_encode(dd));
        prev_delta = delta;
        prev = v;
    }
    out
}

/// Inverse of [`delta_delta_encode`].
pub fn delta_delta_decode(data: &[u8]) -> Vec<i64> {
    let mut pos = 0;
    let Some(count) = read_varint(data, &mut pos) else { return Vec::new() };
    let count = count as usize;
    let mut out = Vec::with_capacity(count);
    if count == 0 {
        return out;
    }
    let Some(first) = read_varint(data, &mut pos) else { return out };
    let first = zigzag_decode(first);
    out.push(first);
    if count == 1 {
        return out;
    }
    let Some(d0) = read_varint(data, &mut pos) else { return out };
    let mut prev_delta = zigzag_decode(d0);
    let mut prev = first + prev_delta;
    out.push(prev);
    for _ in 2..count {
        let Some(dd_raw) = read_varint(data, &mut pos) else { break };
        let dd = zigzag_decode(dd_raw);
        let delta = prev_delta + dd;
        let v = prev + delta;
        out.push(v);
        prev_delta = delta;
        prev = v;
    }
    out
}

/// Dictionary-encodes a sequence of strings for low-cardinality tag columns:
/// returns the distinct values (in first-seen order) and, for each input
/// value, its index into that dictionary.
pub fn dictionary_encode(values: &[String]) -> (Vec<String>, Vec<u32>) {
    let mut dict: Vec<String> = Vec::new();
    let mut index: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut codes = Vec::with_capacity(values.len());
    for v in values {
        let code = *index.entry(v.clone()).or_insert_with(|| {
            dict.push(v.clone());
            (dict.len() - 1) as u32
        });
        codes.push(code);
    }
    (dict, codes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gorilla_round_trip_constant() {
        let values = vec![42.5; 100];
        let encoded = gorilla_encode(&values);
        let decoded = gorilla_decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn gorilla_round_trip_slowly_varying() {
        let values: Vec<f64> = (0..500).map(|i| 20.0 + (i as f64 * 0.01).sin()).collect();
        let encoded = gorilla_encode(&values);
        let decoded = gorilla_decode(&encoded);
        assert_eq!(decoded.len(), values.len());
        for (a, b) in decoded.iter().zip(values.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
        // Sanity check on compression ratio for near-constant data (not a
        // hard milestone assertion — just documents the codec is doing
        // something useful).
        assert!(encoded.len() < values.len() * 8);
    }

    #[test]
    fn gorilla_empty() {
        assert!(gorilla_encode(&[]).is_empty());
        assert!(gorilla_decode(&[]).is_empty());
    }

    #[test]
    fn delta_delta_round_trip_evenly_spaced() {
        let values: Vec<i64> = (0..1000).map(|i| 1_700_000_000_000 + i * 1000).collect();
        let encoded = delta_delta_encode(&values);
        let decoded = delta_delta_decode(&encoded);
        assert_eq!(decoded, values);
        // Evenly spaced timestamps should compress far better than 8 bytes/value.
        assert!(encoded.len() < values.len() * 2);
    }

    #[test]
    fn delta_delta_round_trip_irregular() {
        // Second differences must themselves fit in `i64` — fine for the
        // intended use (timestamps, slowly-varying counters), but not for
        // arbitrary values spanning the full `i64` range (a documented
        // limitation of delta-of-delta encoding, not specific to this
        // implementation).
        let values = vec![5, 3, 100, -50, -50, 0, 1_000_000_000, -1_000_000_000];
        let encoded = delta_delta_encode(&values);
        let decoded = delta_delta_decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn delta_delta_empty_and_single() {
        assert_eq!(delta_delta_decode(&delta_delta_encode(&[])), Vec::<i64>::new());
        assert_eq!(delta_delta_decode(&delta_delta_encode(&[7])), vec![7]);
    }

    #[test]
    fn dictionary_encode_round_trip() {
        let values: Vec<String> = ["us-east", "us-west", "us-east", "eu", "us-east"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (dict, codes) = dictionary_encode(&values);
        assert_eq!(dict, vec!["us-east", "us-west", "eu"]);
        let decoded: Vec<String> = codes.iter().map(|&c| dict[c as usize].clone()).collect();
        assert_eq!(decoded, values);
    }
}
