# Plexus (Phase 9) Adjacency Index Format

Source: `tpt-keystone/src/storage/graph_index.rs`. Local per-node only, same
scope cut as the B-Tree/Meridian/Chronos indexes.

Uses the [append-only record log convention](primitives.md#append-only-record-log-convention)
with its own hand-written (not bincode) header.

## File header

```
u32 to_column_len
<to_column_len> bytes to_column name (UTF-8)   -- edge table's destination-vertex column
u8  has_type_column                            -- 0 or 1
if has_type_column == 1:
    u32 type_column_len
    <type_column_len> bytes type_column name (UTF-8)   -- edge table's relationship-type column
```

Both length-prefixed strings use the file's own `write_string`/`read_string`
helpers (`u32` big-endian length + raw UTF-8 bytes) — not the varint or
bincode-v1 conventions used elsewhere in this codebase.

## Record value: `EdgeRecord` (bincode-v1, per the [record-log convention](primitives.md#append-only-record-log-convention))

```rust
struct EdgeRecord {
    from: Vec<u8>,           // source vertex row key
    to: Vec<u8>,             // destination vertex row key
    rel_type: Option<String>, // relationship-type value, if the edge table declared one
}
```

Encoded per the [bincode-v1 conventions](primitives.md#bincode-v1-blobs).

## In-memory reconstruction

On open, every `EdgeRecord` is replayed via
`AdjacencyGraph::add_edge(from, to, rel_type)` (`src/graph.rs`), which builds
a dense in-memory adjacency-list structure keyed by vertex row key. There is
no on-disk representation of the adjacency structure itself — reading the
file always means replaying every edge record from the beginning; there is
no snapshot/checkpoint format to short-circuit that.
