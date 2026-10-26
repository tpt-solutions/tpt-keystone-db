# Prism DiskANN Index Format

Source: `tpt-keystone/src/storage/diskann_index.rs`, built on the Vamana graph
in `tpt-keystone/src/vector/vamana.rs`.

**Not reachable from SQL.** Nothing in `executor/` or `storage/database/`
creates a `DiskAnnIndex`, so `CREATE INDEX ... USING DISKANN` does not exist.
This format is used only through the Rust API. Treat this document as the
specification of a library-level artifact, not of a user-facing index.

Unlike the HNSW, IVF-PQ and other local indexes, this format is **not an
append-only log**. It is one flat file with fixed-size records, built once and
then read in place.

## Layout

All integers and floats are big-endian.

```
header (21 bytes):
    b"DANN"          magic (4 bytes); anything else is rejected on open
    u8  metric       0 = L2, 1 = Cosine
    u32 dim          vector dimensionality
    u32 count        number of rows (graph nodes)
    u32 r            max out-degree per node (graph slot count)
    u32 medoid       node id of the graph's medoid (search entry point)

count * record:
    dim * f32        the vector components
    u32 degree       number of valid neighbor slots (<= r)
    r * u32          neighbor node ids; u32::MAX marks an empty slot

count * key:
    u32 key_len
    <key_len> bytes  row key for node i, in node-id order
```

Record size is fixed at `dim*4 + 4 + r*4` bytes. Node `id` therefore lives at
byte offset `21 + id * record_size`, with no index needed to find it. The key
list starts at `21 + count * record_size`.

## Reading

On open, only the header and the key list are read into memory. Vectors and
adjacency records are not. A query's greedy graph search calls `read_record(id)`
for each node it visits, which is one seek plus one read. This is what keeps
memory flat as the index grows. The cost is I/O per visited node.

Keys are read eagerly on open, in full. For a very large row count, that is
the dominant memory cost of opening the index.

## Scope cuts (documented in `diskann_index.rs`)

- Real DiskANN keeps product-quantized codes in RAM to re-rank candidates
  cheaply. This format keeps no such cache, so every candidate's full vector
  is read from disk.
- The file is written once. There is no insert, delete or update path on disk.
  Changing the data means rebuilding the file.
