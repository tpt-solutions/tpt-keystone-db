# Prism IVF-PQ Vector Index Format

Source: `tpt-keystone/src/storage/ivf_pq_index.rs` (on-disk wrapper) and
`tpt-keystone/src/vector/ivf_pq.rs` (the in-memory model it rebuilds).

Created with `CREATE INDEX ... USING IVFPQ (col) WITH (...)` (or `USING IVF`).
Local per-node only. Not replicated through the shared object store, same
scope cut as the other local secondary indexes. The file is named
`<table>_<column>.ivfpq` in the database's local index directory.

## Header (13 bytes, fixed, hand-written; not bincode)

All integers are big-endian.

```
u8  metric        0 = L2 (Euclidean), 1 = Cosine
u32 n_lists       IVF coarse-quantizer list count (WITH (lists = ...), default 100)
u32 pq_m          number of PQ sub-quantizers (WITH (pq_m = ...), default 8)
u32 n_probe       lists probed per query (WITH (n_probe = ...), default 8)
```

The header holds parameters only. It does **not** store trained state. There
are no coarse centroids and no PQ codebooks anywhere in the file.

## Records

Same shape as the append-only record log convention in
[`primitives.md`](primitives.md#append-only-record-log-convention): each
record is a `u32` big-endian length followed by that many bytes of a
bincode-v1 blob. The blob is a `VectorEntry`:

```rust
struct VectorEntry {
    row_key: Vec<u8>,   // the row's primary-key bytes
    vector: Vec<f32>,   // the VECTOR column's components, in order
}
```

Raw `f32` vectors are stored, not PQ codes. The compact representation exists
only in memory.

## Reconstruction on open

On open, the header is read first, then every `VectorEntry` is read in file
order. The index is then **retrained from scratch** via
`IvfPqIndex::train(vectors, metric, n_lists, pq_m, n_probe)`, which runs
k-means for the coarse quantizer and the PQ codebooks over all logged vectors.
Training is deterministic: k-means is seeded from the first `k` training
points, so the same log in the same order yields the same model.

Consequences for reimplementers:

- Reopen cost is proportional to the number of rows, because every open
  retrains. This is not a one-time cost paid at `CREATE INDEX`.
- Training is backfill-only. `CREATE INDEX` trains on the table's existing
  rows (`train_and_create`), so there is no empty-table path. An index file
  with a valid header but **zero** records is rejected on open
  (`"has a header but no logged vectors"`).
- Updates to an indexed `VECTOR` column append a new record. The stale vector
  remains until the index is dropped and rebuilt, same as the HNSW index.

Note: the module doc comment in `ivf_pq_index.rs` says training "happens once,
at index creation, not on every reopen". The code above is what actually runs.
The doc comment is out of date.
