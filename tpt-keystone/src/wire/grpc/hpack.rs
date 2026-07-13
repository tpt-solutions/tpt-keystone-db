//! Hand-written HPACK (RFC 7541) header compression for the Flux gRPC
//! endpoint's HTTP/2 layer — no `hpack`/`h2` crate, consistent with this repo's
//! from-scratch wire-protocol rule.
//!
//! Scope: a full decoder (static + dynamic table, integer/string primitives,
//! and the complete RFC 7541 Appendix B Huffman code for string literals) since
//! real gRPC clients (grpcio, grpc-go, tonic) Huffman-encode most header values
//! and use incremental indexing. The encoder is intentionally simpler: it never
//! Huffman-encodes and never adds to a dynamic table (it emits indexed static
//! entries where an exact match exists, otherwise literal-without-indexing with
//! a static name reference or a fresh literal name). That is fully valid HPACK —
//! Huffman and dynamic indexing are always optional on the *sender* side — and
//! keeps the encoder stateless, which is all a server pushing small gRPC
//! response/trailer header sets needs.
//!
//! The Huffman code table below was generated from, and round-trip verified
//! against, Python's `hpack` library (see `wire::grpc::grpc_tests`), which is
//! itself the canonical RFC 7541 Appendix B table.

use std::collections::VecDeque;

/// One decoded header field.
pub type Header = (String, String);

// ---- RFC 7541 Appendix A static table --------------------------------------

/// `(name, value)` — index i in this array is HPACK static index `i + 1`.
static STATIC_TABLE: &[(&str, &str)] = &[
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
];

// ---- RFC 7541 Appendix B Huffman table -------------------------------------

/// `(code, bit_length)` for symbols 0..=255 plus EOS at index 256. Generated
/// from Python `hpack` (canonical RFC 7541 Appendix B) and round-trip verified.
static HUFFMAN: &[(u32, u8)] = &[
    (0x00001ff8, 13),
    (0x007fffd8, 23),
    (0x0fffffe2, 28),
    (0x0fffffe3, 28),
    (0x0fffffe4, 28),
    (0x0fffffe5, 28),
    (0x0fffffe6, 28),
    (0x0fffffe7, 28),
    (0x0fffffe8, 28),
    (0x00ffffea, 24),
    (0x3ffffffc, 30),
    (0x0fffffe9, 28),
    (0x0fffffea, 28),
    (0x3ffffffd, 30),
    (0x0fffffeb, 28),
    (0x0fffffec, 28),
    (0x0fffffed, 28),
    (0x0fffffee, 28),
    (0x0fffffef, 28),
    (0x0ffffff0, 28),
    (0x0ffffff1, 28),
    (0x0ffffff2, 28),
    (0x3ffffffe, 30),
    (0x0ffffff3, 28),
    (0x0ffffff4, 28),
    (0x0ffffff5, 28),
    (0x0ffffff6, 28),
    (0x0ffffff7, 28),
    (0x0ffffff8, 28),
    (0x0ffffff9, 28),
    (0x0ffffffa, 28),
    (0x0ffffffb, 28),
    (0x00000014, 6),
    (0x000003f8, 10),
    (0x000003f9, 10),
    (0x00000ffa, 12),
    (0x00001ff9, 13),
    (0x00000015, 6),
    (0x000000f8, 8),
    (0x000007fa, 11),
    (0x000003fa, 10),
    (0x000003fb, 10),
    (0x000000f9, 8),
    (0x000007fb, 11),
    (0x000000fa, 8),
    (0x00000016, 6),
    (0x00000017, 6),
    (0x00000018, 6),
    (0x00000000, 5),
    (0x00000001, 5),
    (0x00000002, 5),
    (0x00000019, 6),
    (0x0000001a, 6),
    (0x0000001b, 6),
    (0x0000001c, 6),
    (0x0000001d, 6),
    (0x0000001e, 6),
    (0x0000001f, 6),
    (0x0000005c, 7),
    (0x000000fb, 8),
    (0x00007ffc, 15),
    (0x00000020, 6),
    (0x00000ffb, 12),
    (0x000003fc, 10),
    (0x00001ffa, 13),
    (0x00000021, 6),
    (0x0000005d, 7),
    (0x0000005e, 7),
    (0x0000005f, 7),
    (0x00000060, 7),
    (0x00000061, 7),
    (0x00000062, 7),
    (0x00000063, 7),
    (0x00000064, 7),
    (0x00000065, 7),
    (0x00000066, 7),
    (0x00000067, 7),
    (0x00000068, 7),
    (0x00000069, 7),
    (0x0000006a, 7),
    (0x0000006b, 7),
    (0x0000006c, 7),
    (0x0000006d, 7),
    (0x0000006e, 7),
    (0x0000006f, 7),
    (0x00000070, 7),
    (0x00000071, 7),
    (0x00000072, 7),
    (0x000000fc, 8),
    (0x00000073, 7),
    (0x000000fd, 8),
    (0x00001ffb, 13),
    (0x0007fff0, 19),
    (0x00001ffc, 13),
    (0x00003ffc, 14),
    (0x00000022, 6),
    (0x00007ffd, 15),
    (0x00000003, 5),
    (0x00000023, 6),
    (0x00000004, 5),
    (0x00000024, 6),
    (0x00000005, 5),
    (0x00000025, 6),
    (0x00000026, 6),
    (0x00000027, 6),
    (0x00000006, 5),
    (0x00000074, 7),
    (0x00000075, 7),
    (0x00000028, 6),
    (0x00000029, 6),
    (0x0000002a, 6),
    (0x00000007, 5),
    (0x0000002b, 6),
    (0x00000076, 7),
    (0x0000002c, 6),
    (0x00000008, 5),
    (0x00000009, 5),
    (0x0000002d, 6),
    (0x00000077, 7),
    (0x00000078, 7),
    (0x00000079, 7),
    (0x0000007a, 7),
    (0x0000007b, 7),
    (0x00007ffe, 15),
    (0x000007fc, 11),
    (0x00003ffd, 14),
    (0x00001ffd, 13),
    (0x0ffffffc, 28),
    (0x000fffe6, 20),
    (0x003fffd2, 22),
    (0x000fffe7, 20),
    (0x000fffe8, 20),
    (0x003fffd3, 22),
    (0x003fffd4, 22),
    (0x003fffd5, 22),
    (0x007fffd9, 23),
    (0x003fffd6, 22),
    (0x007fffda, 23),
    (0x007fffdb, 23),
    (0x007fffdc, 23),
    (0x007fffdd, 23),
    (0x007fffde, 23),
    (0x00ffffeb, 24),
    (0x007fffdf, 23),
    (0x00ffffec, 24),
    (0x00ffffed, 24),
    (0x003fffd7, 22),
    (0x007fffe0, 23),
    (0x00ffffee, 24),
    (0x007fffe1, 23),
    (0x007fffe2, 23),
    (0x007fffe3, 23),
    (0x007fffe4, 23),
    (0x001fffdc, 21),
    (0x003fffd8, 22),
    (0x007fffe5, 23),
    (0x003fffd9, 22),
    (0x007fffe6, 23),
    (0x007fffe7, 23),
    (0x00ffffef, 24),
    (0x003fffda, 22),
    (0x001fffdd, 21),
    (0x000fffe9, 20),
    (0x003fffdb, 22),
    (0x003fffdc, 22),
    (0x007fffe8, 23),
    (0x007fffe9, 23),
    (0x001fffde, 21),
    (0x007fffea, 23),
    (0x003fffdd, 22),
    (0x003fffde, 22),
    (0x00fffff0, 24),
    (0x001fffdf, 21),
    (0x003fffdf, 22),
    (0x007fffeb, 23),
    (0x007fffec, 23),
    (0x001fffe0, 21),
    (0x001fffe1, 21),
    (0x003fffe0, 22),
    (0x001fffe2, 21),
    (0x007fffed, 23),
    (0x003fffe1, 22),
    (0x007fffee, 23),
    (0x007fffef, 23),
    (0x000fffea, 20),
    (0x003fffe2, 22),
    (0x003fffe3, 22),
    (0x003fffe4, 22),
    (0x007ffff0, 23),
    (0x003fffe5, 22),
    (0x003fffe6, 22),
    (0x007ffff1, 23),
    (0x03ffffe0, 26),
    (0x03ffffe1, 26),
    (0x000fffeb, 20),
    (0x0007fff1, 19),
    (0x003fffe7, 22),
    (0x007ffff2, 23),
    (0x003fffe8, 22),
    (0x01ffffec, 25),
    (0x03ffffe2, 26),
    (0x03ffffe3, 26),
    (0x03ffffe4, 26),
    (0x07ffffde, 27),
    (0x07ffffdf, 27),
    (0x03ffffe5, 26),
    (0x00fffff1, 24),
    (0x01ffffed, 25),
    (0x0007fff2, 19),
    (0x001fffe3, 21),
    (0x03ffffe6, 26),
    (0x07ffffe0, 27),
    (0x07ffffe1, 27),
    (0x03ffffe7, 26),
    (0x07ffffe2, 27),
    (0x00fffff2, 24),
    (0x001fffe4, 21),
    (0x001fffe5, 21),
    (0x03ffffe8, 26),
    (0x03ffffe9, 26),
    (0x0ffffffd, 28),
    (0x07ffffe3, 27),
    (0x07ffffe4, 27),
    (0x07ffffe5, 27),
    (0x000fffec, 20),
    (0x00fffff3, 24),
    (0x000fffed, 20),
    (0x001fffe6, 21),
    (0x003fffe9, 22),
    (0x001fffe7, 21),
    (0x001fffe8, 21),
    (0x007ffff3, 23),
    (0x003fffea, 22),
    (0x003fffeb, 22),
    (0x01ffffee, 25),
    (0x01ffffef, 25),
    (0x00fffff4, 24),
    (0x00fffff5, 24),
    (0x03ffffea, 26),
    (0x007ffff4, 23),
    (0x03ffffeb, 26),
    (0x07ffffe6, 27),
    (0x03ffffec, 26),
    (0x03ffffed, 26),
    (0x07ffffe7, 27),
    (0x07ffffe8, 27),
    (0x07ffffe9, 27),
    (0x07ffffea, 27),
    (0x07ffffeb, 27),
    (0x0ffffffe, 28),
    (0x07ffffec, 27),
    (0x07ffffed, 27),
    (0x07ffffee, 27),
    (0x07ffffef, 27),
    (0x07fffff0, 27),
    (0x03ffffee, 26),
    (0x3fffffff, 30), // EOS (index 256)
];

/// Decode a Huffman-encoded byte string (RFC 7541 §5.2). Walks the input
/// bit-by-bit against the code table; padding must be all-ones and shorter than
/// a full code (any complete EOS symbol is an error, per the RFC).
fn huffman_decode(input: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len());
    // Current accumulated code and its bit length.
    let mut cur: u32 = 0;
    let mut cur_bits: u8 = 0;
    for &byte in input {
        for bit_i in (0..8).rev() {
            let bit = (byte >> bit_i) & 1;
            cur = (cur << 1) | bit as u32;
            cur_bits += 1;
            anyhow::ensure!(cur_bits <= 30, "hpack huffman: code longer than 30 bits");
            // Try to match a symbol of exactly `cur_bits` length.
            if let Some(sym) = match_symbol(cur, cur_bits) {
                anyhow::ensure!(sym != 256, "hpack huffman: EOS symbol encountered");
                out.push(sym as u8);
                cur = 0;
                cur_bits = 0;
            }
        }
    }
    // Remaining bits must be valid EOS padding: all ones, fewer than 8 bits.
    if cur_bits > 0 {
        anyhow::ensure!(cur_bits < 8, "hpack huffman: leftover exceeds one octet");
        let all_ones = (1u32 << cur_bits) - 1;
        anyhow::ensure!(
            cur == all_ones,
            "hpack huffman: invalid non-ones padding at end of string"
        );
    }
    Ok(out)
}

/// Returns the symbol whose code equals `cur` at exactly `bits` length, if any.
/// Linear over the table but only consulted once per completed code; header
/// strings are short, so this is not a hot path worth a prebuilt trie.
fn match_symbol(cur: u32, bits: u8) -> Option<usize> {
    HUFFMAN.iter().enumerate().find_map(|(sym, &(code, len))| {
        if len == bits && code == cur {
            Some(sym)
        } else {
            None
        }
    })
}

// ---- integer / string primitives -------------------------------------------

/// Decodes an HPACK integer with an `n`-bit prefix (RFC 7541 §5.1). `first` is
/// the byte containing the prefix; `rest` is advanced past any continuation
/// bytes.
fn decode_integer(first: u8, n: u8, rest: &mut &[u8]) -> anyhow::Result<u64> {
    let mask = (1u16 << n) as u16 - 1;
    let prefix = (first as u16 & mask) as u64;
    if prefix < mask as u64 {
        return Ok(prefix);
    }
    let mut value = mask as u64;
    let mut m = 0u32;
    loop {
        anyhow::ensure!(!rest.is_empty(), "hpack integer: truncated continuation");
        let b = rest[0];
        *rest = &rest[1..];
        anyhow::ensure!(m < 63, "hpack integer: overflow");
        value = value
            .checked_add(((b & 0x7f) as u64) << m)
            .ok_or_else(|| anyhow::anyhow!("hpack integer: overflow"))?;
        m += 7;
        if b & 0x80 == 0 {
            break;
        }
    }
    Ok(value)
}

/// Encodes an HPACK integer into `out`, OR-ing the prefix into `prefix_bits`
/// (the high bits of the first byte carrying a representation flag).
fn encode_integer(out: &mut Vec<u8>, mut value: u64, n: u8, prefix_bits: u8) {
    let max = (1u16 << n) as u16 - 1;
    if value < max as u64 {
        out.push(prefix_bits | value as u8);
        return;
    }
    out.push(prefix_bits | max as u8);
    value -= max as u64;
    while value >= 128 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Decodes a string literal (RFC 7541 §5.2): a 1-bit Huffman flag + 7-bit
/// length prefix, then the (possibly Huffman-encoded) octets.
fn decode_string(first: u8, rest: &mut &[u8]) -> anyhow::Result<Vec<u8>> {
    let huffman = first & 0x80 != 0;
    let len = decode_integer(first, 7, rest)? as usize;
    anyhow::ensure!(rest.len() >= len, "hpack string: truncated literal");
    let raw = &rest[..len];
    *rest = &rest[len..];
    if huffman {
        huffman_decode(raw)
    } else {
        Ok(raw.to_vec())
    }
}

/// Encodes a string literal without Huffman (always valid; see module docs).
fn encode_string(out: &mut Vec<u8>, s: &str) {
    // Huffman flag = 0, 7-bit length prefix.
    encode_integer(out, s.len() as u64, 7, 0x00);
    out.extend_from_slice(s.as_bytes());
}

// ---- decoder with dynamic table --------------------------------------------

/// HPACK decoder holding the per-connection dynamic table. HTTP/2 requires one
/// decoder instance per connection since the dynamic table is stateful across
/// header blocks.
pub struct Decoder {
    dynamic: VecDeque<(String, String)>,
    dynamic_size: usize,
    max_dynamic_size: usize,
}

impl Decoder {
    pub fn new() -> Self {
        Decoder {
            dynamic: VecDeque::new(),
            dynamic_size: 0,
            // SETTINGS_HEADER_TABLE_SIZE default (RFC 7540 §6.5.2).
            max_dynamic_size: 4096,
        }
    }

    fn entry_size(name: &str, value: &str) -> usize {
        // RFC 7541 §4.1: size = name.len + value.len + 32.
        name.len() + value.len() + 32
    }

    fn insert_dynamic(&mut self, name: String, value: String) {
        let size = Self::entry_size(&name, &value);
        // Evict from the end (oldest) until it fits.
        while self.dynamic_size + size > self.max_dynamic_size && !self.dynamic.is_empty() {
            if let Some((n, v)) = self.dynamic.pop_back() {
                self.dynamic_size -= Self::entry_size(&n, &v);
            }
        }
        if size <= self.max_dynamic_size {
            self.dynamic.push_front((name, value));
            self.dynamic_size += size;
        }
        // If a single entry is larger than the table, the table is emptied and
        // the entry is not added (RFC 7541 §4.4) — handled by the loop above.
    }

    fn set_max_size(&mut self, new_max: usize) {
        self.max_dynamic_size = new_max;
        while self.dynamic_size > self.max_dynamic_size {
            if let Some((n, v)) = self.dynamic.pop_back() {
                self.dynamic_size -= Self::entry_size(&n, &v);
            } else {
                break;
            }
        }
    }

    /// Resolves a combined static+dynamic table index (1-based) to a header.
    fn resolve_index(&self, index: u64) -> anyhow::Result<(String, String)> {
        anyhow::ensure!(index != 0, "hpack: index 0 is not valid");
        let idx = index as usize;
        if idx <= STATIC_TABLE.len() {
            let (n, v) = STATIC_TABLE[idx - 1];
            Ok((n.to_string(), v.to_string()))
        } else {
            let dyn_idx = idx - STATIC_TABLE.len() - 1;
            self.dynamic
                .get(dyn_idx)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("hpack: dynamic index {index} out of range"))
        }
    }

    fn resolve_name(&self, index: u64) -> anyhow::Result<String> {
        Ok(self.resolve_index(index)?.0)
    }

    /// Decodes a complete header block into an ordered list of headers.
    pub fn decode(&mut self, mut block: &[u8]) -> anyhow::Result<Vec<Header>> {
        let mut headers = Vec::new();
        while !block.is_empty() {
            let first = block[0];
            block = &block[1..];
            if first & 0x80 != 0 {
                // Indexed Header Field.
                let index = decode_integer(first, 7, &mut block)?;
                headers.push(self.resolve_index(index)?);
            } else if first & 0x40 != 0 {
                // Literal Header Field with Incremental Indexing.
                let (name, value) = self.decode_literal(first, 6, &mut block)?;
                self.insert_dynamic(name.clone(), value.clone());
                headers.push((name, value));
            } else if first & 0x20 != 0 {
                // Dynamic Table Size Update.
                let new_max = decode_integer(first, 5, &mut block)? as usize;
                self.set_max_size(new_max);
            } else {
                // Literal without Indexing (0x00) or Never Indexed (0x10);
                // both use a 4-bit name-index prefix and are not added to the
                // dynamic table.
                let (name, value) = self.decode_literal(first, 4, &mut block)?;
                headers.push((name, value));
            }
        }
        Ok(headers)
    }

    fn decode_literal(
        &self,
        first: u8,
        name_prefix_bits: u8,
        block: &mut &[u8],
    ) -> anyhow::Result<(String, String)> {
        let name_index = decode_integer(first, name_prefix_bits, block)?;
        let name = if name_index == 0 {
            anyhow::ensure!(!block.is_empty(), "hpack: truncated literal name");
            let first_str = block[0];
            *block = &block[1..];
            String::from_utf8(decode_string(first_str, block)?)?
        } else {
            self.resolve_name(name_index)?
        };
        anyhow::ensure!(!block.is_empty(), "hpack: truncated literal value");
        let first_val = block[0];
        *block = &block[1..];
        let value = String::from_utf8(decode_string(first_val, block)?)?;
        Ok((name, value))
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

// ---- stateless encoder -----------------------------------------------------

/// Encodes a header list for a response/trailer block. Uses an indexed static
/// entry when name+value match exactly, a literal-without-indexing with a
/// static name reference when only the name matches, and a fresh literal name
/// otherwise. Never Huffman-encodes and never mutates a dynamic table (all
/// optional on the sender). Header names must be lowercase per HTTP/2.
pub fn encode(headers: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    for &(name, value) in headers {
        if let Some(idx) = static_full_match(name, value) {
            // Indexed Header Field: 1-bit prefix set.
            encode_integer(&mut out, idx as u64, 7, 0x80);
        } else if let Some(idx) = static_name_match(name) {
            // Literal without Indexing, name index from static table.
            encode_integer(&mut out, idx as u64, 4, 0x00);
            encode_string(&mut out, value);
        } else {
            // Literal without Indexing, new (literal) name.
            out.push(0x00);
            encode_string(&mut out, name);
            encode_string(&mut out, value);
        }
    }
    out
}

fn static_full_match(name: &str, value: &str) -> Option<usize> {
    STATIC_TABLE
        .iter()
        .position(|&(n, v)| n == name && v == value)
        .map(|i| i + 1)
}

fn static_name_match(name: &str) -> Option<usize> {
    STATIC_TABLE
        .iter()
        .position(|&(n, _)| n == name)
        .map(|i| i + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_prefix_examples_from_rfc() {
        // RFC 7541 §C.1.1: value 10, 5-bit prefix -> single byte 0x0a.
        let mut out = Vec::new();
        encode_integer(&mut out, 10, 5, 0x00);
        assert_eq!(out, vec![0x0a]);
        let mut rest: &[u8] = &[];
        assert_eq!(decode_integer(0x0a, 5, &mut rest).unwrap(), 10);

        // RFC 7541 §C.1.2: value 1337, 5-bit prefix -> 0x1f 0x9a 0x0a.
        let mut out = Vec::new();
        encode_integer(&mut out, 1337, 5, 0x00);
        assert_eq!(out, vec![0x1f, 0x9a, 0x0a]);
        let mut rest: &[u8] = &out[1..];
        assert_eq!(decode_integer(out[0], 5, &mut rest).unwrap(), 1337);
    }

    #[test]
    fn indexed_static_header_decodes() {
        // 0x82 = indexed header field, index 2 -> :method GET.
        let mut dec = Decoder::new();
        let headers = dec.decode(&[0x82]).unwrap();
        assert_eq!(headers, vec![(":method".to_string(), "GET".to_string())]);
    }

    #[test]
    fn literal_incremental_indexing_populates_dynamic_table() {
        // RFC 7541 §C.2.1: custom-key: custom-header (literal w/ incremental).
        // 0x40, name literal "custom-key", value literal "custom-header".
        let mut block = vec![0x40];
        encode_string(&mut block, "custom-key");
        encode_string(&mut block, "custom-header");
        let mut dec = Decoder::new();
        let headers = dec.decode(&block).unwrap();
        assert_eq!(
            headers,
            vec![("custom-key".to_string(), "custom-header".to_string())]
        );
        // It should now be resolvable at dynamic index 62.
        assert_eq!(
            dec.resolve_index(62).unwrap(),
            ("custom-key".to_string(), "custom-header".to_string())
        );
    }

    #[test]
    fn encode_then_decode_round_trips_a_grpc_header_set() {
        let headers = vec![
            (":status", "200"),
            ("content-type", "application/grpc"),
            ("grpc-status", "0"),
            ("grpc-message", "OK"),
        ];
        let block = encode(&headers);
        let mut dec = Decoder::new();
        let decoded = dec.decode(&block).unwrap();
        let decoded_refs: Vec<(&str, &str)> = decoded
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect();
        assert_eq!(decoded_refs, headers);
    }

    #[test]
    fn huffman_decodes_known_string() {
        // "www.example.com" Huffman-encoded (RFC 7541 §C.4.1).
        let encoded = [
            0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
        ];
        let decoded = huffman_decode(&encoded).unwrap();
        assert_eq!(decoded, b"www.example.com");
    }

    #[test]
    fn huffman_rejects_bad_padding() {
        // Trailing zero padding is invalid (must be all-ones EOS bits).
        // Encode "0" (symbol '0' = code 0x0, 5 bits) then pad with zeros.
        // One octet: 5 bits of code (00000) + 3 bits padding 000 -> 0x00.
        let err = huffman_decode(&[0x00]);
        assert!(err.is_err());
    }
}
