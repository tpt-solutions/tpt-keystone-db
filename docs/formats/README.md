# TPT Keystone — On-Disk Format Specifications

Phase 12 checklist item: "publish versioned, language-independent on-disk
format specifications ... so readers can be reimplemented independently of
the original Rust codebase."

These documents describe every persistent binary layout Keystone produces,
as of **format version 1** (the only version that has ever existed — none of
these layouts carry an explicit version byte yet; see the "Versioning" note
in each doc). They're derived directly from the current serialization code,
not from design intent — if a doc and the code in `tpt-keystone/src/storage/`
disagree, the code is authoritative and the doc has drifted.

All multi-byte integers are **big-endian** unless a document says otherwise.
"Varint" always means the LEB128-style variable-length encoding described in
[`primitives.md`](primitives.md), and "bincode blob" always means the
[bincode](https://github.com/bincode-org/bincode) crate's default
(version 1) configuration, described in the same file.

## Index

| Document | Covers |
|---|---|
| [`primitives.md`](primitives.md) | Shared encodings (varint, bincode-v1 conventions, the "append-only record log" framing convention) referenced by every other doc |
| [`sstable.md`](sstable.md) | Keystone LSM engine: SSTable blob format |
| [`wal.md`](wal.md) | Keystone LSM engine: write-ahead log format |
| [`manifest_and_lease.md`](manifest_and_lease.md) | Cloud-native (Phase 3) shared manifest and write-lease objects |
| [`btree.md`](btree.md) | Local secondary B-Tree index file format |
| [`meridian_geo_index.md`](meridian_geo_index.md) | Meridian (Phase 6) spatial secondary index |
| [`chronos_ts_index.md`](chronos_ts_index.md) | Chronos (Phase 8) time-bucketed index + Gorilla/delta-of-delta compression |
| [`plexus_graph_index.md`](plexus_graph_index.md) | Plexus (Phase 9) adjacency index |
| [`canopy_formats.md`](canopy_formats.md) | Canopy (Phase 10): native JSONB binary encoding, JSON path index, full-text index |
| [`prism_vector_index.md`](prism_vector_index.md) | Prism (Phase 7) HNSW vector secondary index |

## Versioning

None of these formats currently embeds a format-version number anywhere in
its header/footer. That's a known gap, not a design choice: a future
breaking change to any layout here needs to either (a) add an explicit
version field before making the change, or (b) bump a documented "format
version N" convention across all of these docs simultaneously. Until then,
"the current format" and "format version 1" are the same thing, and any
externally-written reader/writer should assume it always is talking to
exactly the layout documented here.
