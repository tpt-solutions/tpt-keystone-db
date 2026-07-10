# Prism (Phase 7) Vector Index Format

Source: `tpt-keystone/src/storage/vector_index.rs`. Local per-node only, same
scope cut as the B-Tree/Meridian/Chronos/Plexus/Canopy indexes — not
replicated through the shared object store; a node rebuilds it from the
file's own append-only log, not from the table's live data (unlike the other
local indexes, there's no fallback "recompute from a table scan" path if the
file goes missing, since a vector's source-of-truth column value is a plain
`VECTOR` cell rather than something the index author trusts to always be
re-derivable in the same form).

## File header (17 bytes, fixed-size, hand-written — not bincode)

```
u8  metric        -- 0 = L2 (Euclidean), 1 = Cosine
u32 m              (big-endian) -- HNSW per-layer neighbor count
u32 m0             (big-endian) -- HNSW layer-0 neighbor count
u32 ef_construction (big-endian) -- HNSW build-time candidate list size
u32 ef_search      (big-endian) -- HNSW default query-time candidate list size
```

`metric`/`m`/`m0`/`ef_construction`/`ef_search` are fixed at `CREATE INDEX
... USING VECTOR/HNSW WITH (...)` time and read back from this header on
every later open — a later `VectorIndex::open` call's `default_metric`/
`default_config` arguments are only used when the file doesn't exist yet.

## Records

Follows the same shape as the [append-only record log convention](primitives.md#append-only-record-log-convention)
(`u32` big-endian length + a bincode-v1 blob), but predates that convention
doc's canonical three files (`geo_index.rs`/`ts_index.rs`/`graph_index.rs`) —
included here as a fourth user of the same shape rather than a variant.

### Record value: `VectorEntry` (bincode-v1, per the [conventions](primitives.md#bincode-v1-blobs))

```rust
struct VectorEntry {
    row_key: Vec<u8>,   // the row's primary-key bytes
    vector: Vec<f32>,   // the VECTOR column's components, in order
}
```

## In-memory reconstruction

On open, every `VectorEntry` is replayed in file order via
`HnswIndex::insert(vector)` (`src/vector/hnsw.rs`), which assigns each vector
a dense, insertion-order internal `id` (`0, 1, 2, ...`) and builds it into
the multi-layer HNSW graph. `VectorIndex` keeps a parallel `row_keys: Vec<Vec<u8>>`
so `row_keys[id]` maps an internal HNSW id back to the row key that produced
it — a k-NN search returns internal ids, translated back to row keys via this
vector before being handed to a caller.

Same scope cut as `storage::btree`: insert-only. A row `UPDATE` on an indexed
`VECTOR` column appends a *new* `VectorEntry` and inserts a *new* HNSW node
rather than mutating or removing the old one, so a stale graph node for the
row's previous vector value can linger until the index is dropped and
rebuilt (`CREATE INDEX` again) — acceptable for a local secondary-index
accelerator, not acceptable if this were the source of truth for the vector
data itself (it isn't; the `VECTOR` column's own cell in row storage is).

There is no on-disk representation of the HNSW graph's layer/edge structure
itself — reading the file always means replaying every `VectorEntry` from
the beginning and rebuilding the graph via ordinary inserts; there is no
snapshot/checkpoint format to short-circuit that (same limitation
`plexus_graph_index.md` documents for its own adjacency-list rebuild).
