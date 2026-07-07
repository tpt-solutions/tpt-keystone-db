# Chronos (Phase 8) Time-Bucketed Index Format

Source: `tpt-keystone/src/storage/ts_index.rs` (on-disk record log) and
`tpt-keystone/src/storage/compress.rs` (the Gorilla/delta-of-delta codecs
used only for in-memory sealed-bucket compression — see below). Local
per-node only, same scope cut as the B-Tree/Meridian/Plexus indexes.

Uses the [append-only record log convention](primitives.md#append-only-record-log-convention),
with a larger, variable-length header than Meridian's.

## File header

```
i64 granularity_ms       (bucket width, e.g. one hour/day/month in ms)
i64 retention_raw        (-1 = no retention/`None`; otherwise `retention_ms`)
u16 col_len
<col_len> bytes value_column name (UTF-8 — the numeric column this index
                                    compresses/rolls up alongside the
                                    indexed timestamp column)
```

## Record value: `TsEntry` (bincode-v1)

```rust
struct TsEntry {
    row_key: Vec<u8>,
    ts: i64,      // event timestamp, ms
    value: f64,   // the value_column's value for this row
}
```

Encoded per the [bincode-v1 conventions](primitives.md#bincode-v1-blobs).
**This is the entire on-disk payload** — every insert appends one `TsEntry`
record, full stop. Nothing else (bucket boundaries, rollups, compressed
series) is written to disk; all of that is derived, in memory, by replaying
every `TsEntry` through the same bucketing/rollup/sealing logic on every
open. A reimplementation that wants Chronos-equivalent behavior needs to
reconstruct the in-memory model below from the raw entry stream — there is
no shortcut format to read instead.

## In-memory model (reconstructed from the entry stream — not itself a file format)

Entries are bucketed by `bucket_start(ts, granularity_ms)` — a floor-divide
of `ts` down to a multiple of `granularity_ms` — into a `BTreeMap<i64,
Bucket>` keyed by that bucket-start timestamp. Each `Bucket` carries:

- A `Rollup { count: u64, sum: f64, min: f64, max: f64 }`, updated
  incrementally on every insert into that bucket (`min`/`max` seeded at
  `+inf`/`-inf`) — this is the "continuous aggregate" substrate; aggregate
  queries read this directly rather than a materialized view.
- A `BucketState`, one of:
  - `Open { entries: Vec<TsEntry> }` — the bucket is the newest (or ties the
    newest) bucket seen so far; raw entries accumulate uncompressed.
  - `Sealed(CompressedSeries)` — a later timestamp advanced past this
    bucket's window, so it was sealed: its accumulated entries are sorted by
    `ts` (this sort is what makes delta-of-delta encoding effective — see
    below) and compressed into:
    ```rust
    struct CompressedSeries {
        ts_deltas: Vec<u8>,   // delta_delta_encode(&sorted_timestamps)
        values: Vec<u8>,      // gorilla_encode(&values_in_same_order)
        row_keys: Vec<Vec<u8>>,
    }
    ```
    using [`delta_delta_encode`](#delta-of-delta-integer-encoding) for
    timestamps and [`gorilla_encode`](#gorilla-float-encoding) for values —
    both described below.
  - `Downsampled` — the bucket's window is older than `retention_ms` behind
    the newest inserted timestamp; its raw/compressed series has been
    dropped entirely, leaving only the `Rollup`. This is irreversible from
    the file alone — the underlying `TsEntry` records for a downsampled
    bucket are still physically present in the log (nothing is deleted from
    the append-only file), but a correct replay must re-derive
    `Downsampled` state for old buckets and discard their entries from
    memory again, rather than keeping them, to match the running node's
    behavior.

## Gorilla float encoding

Implemented in `compress.rs::{gorilla_encode, gorilla_decode}`, following
the [Facebook Gorilla paper](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf)'s
XOR-delta scheme for `f64` sequences, MSB-first bit-packed:

```
u32 count            (big-endian, byte-aligned prefix)
u8  valid_last_bits   (how many bits of the final byte are meaningful; the
                        rest of that byte is zero-padding)
<bitstream>
```

Bitstream contents (bit-packed, not byte-aligned after the 5-byte prefix):

1. First value: 64 raw bits (`f64::to_bits()`), verbatim.
2. Each subsequent value: XOR its bits against the previous value's bits.
   - XOR == 0 (identical to previous): write a single `0` control bit.
   - XOR != 0: write a `1` control bit, then:
     - If a "previous window" exists (`leading`/`trailing` zero-run lengths
       from the last non-zero XOR) and this XOR's meaningful bits fit
       within that same window (`leading >= prev_leading && trailing >=
       prev_trailing`): write `0` (reuse-window bit), then the
       `64 - prev_leading - prev_trailing` meaningful bits of this XOR
       (shifted right by `prev_trailing`).
     - Otherwise: write `1` (new-window bit), then a 6-bit `leading` count,
       a 6-bit `meaningful` bit count (0 is used to mean 64 on decode — a
       full-width window), then the `meaningful` bits of the XOR (shifted
       right by `trailing`), and update the remembered `prev_leading`/
       `prev_trailing` window to this XOR's.

Decoding mirrors this exactly bit-for-bit; there is no other framing.

## Delta-of-delta integer encoding

Implemented in `compress.rs::{delta_delta_encode, delta_delta_decode}`, for
`i64` sequences (used here for sorted timestamps within a sealed bucket):

```
varint count
if count == 0: stop
varint zigzag(values[0])                          -- first value, verbatim
if count == 1: stop
varint zigzag(values[1] - values[0])              -- first delta
for each subsequent value:
    delta = value - prev
    varint zigzag(delta - prev_delta)              -- second difference
    prev_delta = delta
```

See [`primitives.md`](primitives.md#varint-leb128-style) for the varint and
zigzag encodings. Note the module doc's caveat: deltas/second-differences
that swing across most of the `i64` range can overflow this arithmetic
(panics in debug builds, wraps in release) — this codec assumes evenly- or
near-evenly-spaced values (timestamps, slow counters), not arbitrary `i64`
sequences.
