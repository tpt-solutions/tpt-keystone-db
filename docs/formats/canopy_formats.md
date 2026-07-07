# Canopy (Phase 10) On-Disk Formats

Covers three distinct formats from Canopy: the native JSONB binary value
encoding (`jsonb.rs`), and two local secondary-index files (`canopy_index.rs`)
that use a different persistence strategy from every other index in this
codebase — see the note below.

## JSONB value encoding (`jsonb.rs`)

A hand-written, compact tag/length/value encoding for `serde_json::Value` —
"in the spirit of" Postgres's `jsonb`/MongoDB's BSON but **not** bit-compatible
with either. Used internally by the two indexes below to build compact,
canonically-ordered keys; row storage itself still stores `Json` columns as
raw text (this encoding isn't used for row values on disk today).

```
u8 tag, then:
  TAG_NULL   = 0   -- no payload
  TAG_FALSE  = 1   -- no payload
  TAG_TRUE   = 2   -- no payload
  TAG_INT    = 3   -- i64, 8 bytes big-endian (to_be_bytes)
  TAG_FLOAT  = 4   -- f64, 8 bytes big-endian (to_be_bytes)
  TAG_STRING = 5   -- varint byte-length, then raw UTF-8 bytes
  TAG_ARRAY  = 6   -- varint element count, then that many encoded values, in order
  TAG_OBJECT = 7   -- varint entry count, then that many
                      { varint key byte-length, raw UTF-8 key bytes, encoded value }
                      entries, **sorted by key** before encoding
```

Varints are the [standard LEB128-style encoding](primitives.md#varint-leb128-style)
used throughout this codebase. Object keys are sorted lexicographically by
byte value before encoding specifically so two structurally-equal documents
with differently-ordered keys always produce identical bytes — required for
both indexes below and any future containment-style comparison. Numbers that
fit in `i64` are always encoded as `TAG_INT` (never `TAG_FLOAT`), even if the
source `serde_json::Number` could also represent them as a float — decoding
back to `serde_json::Value` therefore always produces an integer `Number`
for values that round-tripped through `TAG_INT`, regardless of how the
original document spelled the literal.

## JSON path index and full-text index (`canopy_index.rs`)

**Persistence model, different from every other index in this repo:** these
two indexes are *not* append-only record logs. Each is a single
[bincode-v1](primitives.md#bincode-v1-blobs)-encoded blob of the index's
entire in-memory state, and the **whole file is rewritten from scratch on
every single mutation** (`save()` calls `bincode::serialize` on the full
struct and `std::fs::write`s it, replacing the file). There is no delta/log
format to replay — reading the file means deserializing the one blob it
contains. This is a documented, deliberate simplicity trade-off (see the
module doc comment) that does not scale to large indexes the way the
append-only `sst`/`wal`/other-index formats do; a reimplementation just
needs one `bincode::deserialize::<T>(&whole_file_bytes)` call per format,
not incremental replay logic.

### `JsonPathIndex` file contents

```rust
struct PathIndexData {
    json_path: String,                    // e.g. "user.address.city"
    map: HashMap<String, Vec<Vec<u8>>>,   // scalar value's text form -> matching row keys
}
```

`map`'s keys are the canonical text form of a scalar JSON leaf
(`scalar_key_text`: booleans/numbers via their `Display` impl, strings
verbatim; `null`/arrays/objects are never indexed — a document whose path
resolves to one of those, or doesn't resolve at all, is simply absent from
every bucket). Values are the list of row keys (raw bytes) whose document
has that exact text value at `json_path`; a path is dot-separated
object-key traversal only (no array-index segments).

Bincode-v1 encoding notes: `HashMap<K, V>` follows the same sequence
convention as `Vec<T>` — a little-endian `u64` entry-count prefix, then
each `(key, value)` pair encoded in whatever (unspecified, hash-order)
iteration order the `HashMap` produced at serialize time. **This means the
byte-for-byte encoding of this file is not deterministic across writes with
the same logical contents** — only the decoded structure is meaningful; do
not diff or hash these files expecting stability.

### `FtsIndex` file contents

```rust
struct FtsIndexData {
    postings: HashMap<String, Vec<Vec<u8>>>,   // lowercase token -> matching row keys
}
```

Tokens come from `tokenize()`: split on any non-alphanumeric character,
drop empty runs, lowercase each remaining run — no stemming, no stop-word
list, and both indexing and query-time tokenization go through this same
function so a token always means the same thing on both sides. `insert`
appends `row_key` to every token's bucket found in the given text (JSON
documents are expected to be pre-flattened via `collect_json_strings`,
which space-joins every string leaf in document order, before tokenizing).
`search_and` intersects the postings buckets for every query token
(AND-only semantics; no ranking, no OR).
