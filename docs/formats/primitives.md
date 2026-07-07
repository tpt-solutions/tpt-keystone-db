# Shared Primitives

## Fixed-width integers

Unless stated otherwise, every fixed-width integer in every format in this
directory is **big-endian** (`u32::to_be_bytes()` / `from_be_bytes()`, etc.
in Rust terms). This is a deliberate, consistent choice across the codebase
— there is no format here that mixes endianness.

## Varint (LEB128-style)

Used by: Canopy's JSONB encoding (`jsonb.rs`), Chronos's delta-of-delta
integer compression (`compress.rs`).

Unsigned 64-bit values are encoded 7 bits at a time, least-significant group
first. Each output byte holds 7 payload bits in its low bits; the high bit
(`0x80`) is a continuation flag — set on every byte except the last.

```
encode(n):
    loop:
        byte = n & 0x7f
        n >>= 7
        if n == 0: emit(byte); stop
        else: emit(byte | 0x80)
```

Decoding reverses this: read bytes, OR each byte's low 7 bits into the
result at an increasing bit shift (`shift += 7` per byte), stop after a byte
with the high bit clear.

### Zigzag (signed integers only)

Chronos's delta-of-delta encoding additionally zigzag-maps signed `i64`
deltas to unsigned before varint-encoding them, so small negative numbers
stay small instead of becoming huge unsigned values:

```
zigzag_encode(v: i64) -> u64 = (v << 1) ^ (v >> 63)
zigzag_decode(v: u64) -> i64 = (v >> 1) ^ -(v & 1)
```

## Bincode-v1 blobs

Used by: the shared manifest (`manifest.rs`), the write lease
(`lease.rs`), the local secondary-index record logs (`geo_index.rs`,
`ts_index.rs`, `graph_index.rs`, `canopy_index.rs`).

These structures are serialized with the Rust [`bincode`](https://docs.rs/bincode/1)
crate, major version 1, using `bincode::serialize`/`deserialize` with the
crate's **default configuration**. A reimplementation needs to match that
configuration exactly:

- Integers are encoded as fixed-width, **little-endian** (bincode's default
  — note this is the opposite endianness from every hand-written framing
  byte elsewhere in these docs; only the *bincode payload itself* is
  little-endian).
- `String`/`Vec<T>`/other dynamically-sized sequences are prefixed with
  their length as a `u64` (little-endian), followed by the elements in
  order.
- `Option<T>` is a `u8` tag (`0` = `None`, `1` = `Some`) followed by `T`'s
  encoding if present.
- Struct fields are encoded in declaration order with no padding, no field
  tags, and no length prefix for the struct as a whole — the schema is
  implicit; the reader must know the exact struct shape (field order and
  types) that produced the bytes.
- Enums are a `u32` (little-endian) variant index followed by that variant's
  fields, in declaration order.

Because there is no self-describing schema, every bincode blob referenced
from this directory documents the exact Rust struct shape it corresponds to.

## Append-only record log convention

Used by: `geo_index.rs`, `ts_index.rs`, `graph_index.rs` (local, per-node
secondary indexes — deliberately **not** replicated through the shared
object store; each node rebuilds them from the table's own data if the file
is missing).

All three files share one on-disk shape:

```
[ fixed-size header, format-specific per file ]
[ record ]*
```

Where each `record` is:

```
u32 length (big-endian)
<length> bytes of a bincode-v1-encoded value (the value type is
   format-specific — see each file's own doc)
```

Records are appended one at a time (`OpenOptions::append(true)`) and never
rewritten or removed in place — deletions/updates append a new record and
the reader's replay logic handles superseding old state in memory. Opening
the file means reading the header, then reading `record`s until EOF; hitting
`UnexpectedEof` exactly at a record-length boundary is normal end-of-file,
not corruption (a partial trailing record — a crash mid-append — is treated
as EOF too, since `read_exact` on a short remaining buffer also errors,
which the loop cannot distinguish from truncation and does not attempt to).
