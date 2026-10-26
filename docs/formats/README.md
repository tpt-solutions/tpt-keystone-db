# TPT Keystone — On-Disk Format Specifications

Phase 12 checklist item: "publish versioned, language-independent on-disk
format specifications ... so readers can be reimplemented independently of
the original Rust codebase."

These documents describe every persistent binary layout Keystone produces, as
of the current code on `master` (last checked against the source in the
docs-refresh pass; see the Versioning section below for what changed and
when). They're derived directly from the current serialization code, not from
design intent. If a doc and the code in `tpt-keystone/src/storage/` disagree,
the code is authoritative and the doc has drifted.

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
| [`ivf_pq_index.md`](ivf_pq_index.md) | Prism IVF-PQ vector secondary index (`USING IVFPQ`) |
| [`diskann_index.md`](diskann_index.md) | Prism DiskANN flat-file graph index (library-only; not reachable from SQL) |

## Versioning

None of these formats embeds a format-version number anywhere in its
header/footer. That is a known gap, not a design choice. A breaking change to
a layout must either (a) add an explicit version field before making the
change, or (b) bump a documented "format version N" convention across all of
these docs at once.

**Breaking changes made without a version bump.** Two layouts changed during
the MVCC work (TODO Phase 1 Stage 2+3). Data written by a build before that
change is not readable by the current one:

- **WAL** (`wal.md`): records are now grouped into batches with a `0xFF` batch
  marker. A pre-change `wal.log` is not parsed. `replay` stops at the first
  byte that is not `0xFF`, so the old log is silently ignored, not rejected.
  Flush or drain a node's WAL on the old build before upgrading.
- **SSTable** (`sstable.md`): index entries now carry `seq` and `tag`. An
  SSTable written by the old build will be misparsed, not rejected.
- **Canopy FTS index** (`canopy_formats.md`): postings went from presence-only
  buckets to term frequencies, and `doc_lengths` was added for BM25. A file
  written by the old build fails to deserialize. How a node handles that file
  at startup has not been checked.

The manifest handles its own change, with a fallback decode for the pre-change
shape (`manifest_and_lease.md`).

Any externally-written reader or writer must target the current layouts
documented here. Pre-change data needs a migration, which does not exist yet.
