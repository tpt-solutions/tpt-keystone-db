# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

TPT is a ground-up, multi-engine data platform written in Rust. **Keystone** (`tpt-keystone/`) is the
only engine implemented so far — a cloud-native relational (Postgres-wire-compatible) database engine.
Everything else described in `TODO.md` (Meridian/geospatial, Prism/vector, Chronos/time-series,
Plexus/graph, Canopy/document, Flux/streaming, Canvas frontend, SDKs, Harbor migration, Synapse/Mirror
agent layers) is unbuilt roadmap, not code — don't assume any of it exists.

`TODO.md` is the authoritative phase-by-phase roadmap/checklist; check it before assuming a feature is
missing or planned. `PHASE2_PLAN.md` (root and duplicated in `tpt-keystone/`) has finer-grained notes on
the SQL engine phase specifically. The numbered `*spec.txt` files in the repo root (`1keystonespec.txt`,
`2meridianspec.txt`, etc.) are per-engine design specs for future phases.

## Hard constraints (do not violate)

- **No `pgwire`/`sqlparser-rs` or similar crates.** The Postgres wire protocol codec (`src/wire/`) and
  the SQL lexer/parser/AST (`src/sql/`) are hand-written from scratch by design — this is a deliberate
  project goal, not a gap to fill with a library.
- Row-Level Security is permanently out of scope. Access control (when built) follows a Zanzibar-style
  ReBAC tuple model + RBAC layer instead.

## Build / run / test

All commands run from `tpt-keystone/` (the only crate; no workspace `Cargo.toml` at repo root):

```
cargo build
cargo run                      # starts a single-node writer on 0.0.0.0:5432, local-fs storage under tpt-data/
cargo test                     # unit tests + storage::phase3_tests (multi-node cloud-storage integration tests)
cargo test phase3_tests::<name>  # run one Phase 3 test
psql -h localhost -p 5432      # connect once running (no auth; startup handshake auto-approves)
```

No lint/CI config exists in the repo (no `.github/workflows`, no `rustfmt.toml`/`clippy.toml`) — use
`cargo fmt` / `cargo clippy` at your own discretion but there is no enforced convention to match.

### Running two nodes against shared storage (Phase 3 cloud-native mode)

Compute nodes are stateless; everything durable lives behind the `ObjectStore` trait (local-fs emulation
or real S3). Configure via env vars (see `src/storage/config.rs`): `TPT_STORAGE_BACKEND` (`local`|`s3`),
`TPT_LOCAL_STORE_DIR`, `TPT_S3_BUCKET`/`TPT_S3_REGION`/`TPT_S3_ENDPOINT`, `TPT_NODE_ROLE`
(`writer`|`reader`), `TPT_NODE_ID`, `TPT_LEASE_TTL_SECS`, `TPT_MANIFEST_REFRESH_SECS`, `TPT_LOCAL_DIR`,
`TPT_CACHE_DIR`, `TPT_CACHE_MAX_BYTES`. Only one writer may hold the lease at a time (CAS-based fencing,
`src/storage/lease.rs`); readers poll-refresh the manifest instead of acquiring it.

## Architecture

`src/main.rs` wires everything together: build an `ObjectStore` → wrap it in `CachedObjectStore` (NVMe
cache-aside) → acquire/renew the write lease if this node is a writer → open `Database` → spawn a
manifest-refresh loop if this node is a reader → accept TCP connections and hand each one to
`wire::session::handle`.

Four top-level modules, each a thin layer over the one below:

- **`src/wire/`** — Postgres wire protocol v3, hand-written. `codec.rs` frames raw bytes into
  `FrontendMessage`/`BackendMessage`; `messages.rs` defines the message types and type OIDs;
  `session.rs` drives one client connection through startup handshake → simple query loop and the
  extended query protocol (Parse/Bind/Describe/Execute/Sync/Close, tracked per-connection via
  `ExtendedState`: prepared statements, bound portals, and `DECLARE CURSOR` state — cursors and
  LISTEN/NOTIFY only work over the simple query protocol, not extended).
- **`src/sql/`** — hand-written lexer → recursive-descent parser → `ast.rs` node types. `sql::parse()`
  is the single entry point, producing a `Stmt`.
- **`src/executor/`** — turns a parsed `Stmt` into a `QueryResult` (`mod.rs`), evaluating expressions
  and aggregates (`eval.rs`, incl. the `Value` type and `RowContext` used to evaluate expressions
  against a row/table-scope/outer-query/params), resolving system/virtual tables (`catalog.rs`), and
  choosing join/scan strategy (`planner.rs`). SELECT execution is one big pipeline in
  `execute_select_with_cte`: materialize CTEs → resolve FROM/JOINs → apply WHERE → aggregate (if
  GROUP BY/aggregate present) → window functions → ORDER BY → LIMIT/OFFSET → project. Equi-joins use a
  hash join (build side = smaller input); everything else falls back to nested-loop. The executor is
  otherwise stateless — no query plan caching, no persistent session state beyond what `wire::session`
  keeps.
- **`src/storage/`** — the LSM storage engine, and the biggest module. Key pieces:
  - `wal.rs` — write-ahead log with fsync; `lsm.rs` — MemTable (BTreeMap) + SSTable-based LSM engine
    with levelled compaction; `sstable.rs` — SSTable format + bloom filters; `mvcc.rs`/`tx.rs` —
    multi-version concurrency control and the transaction manager (BEGIN/COMMIT/ROLLBACK); `btree.rs` —
    local B-Tree secondary indexes (deliberately local-only, not replicated through the object store,
    per the Phase 3 scope cut).
  - `database.rs` — `Database`, the struct that ties LSM + MVCC + schema catalog + indexes together and
    implements the `StorageEngine` trait (`mod.rs`) the executor calls against. Schemas live in the
    shared object store under `schemas/` so every node sees the same catalog; indexes are rebuilt
    per-node from local disk.
  - Cloud-native layer (Phase 3): `objectstore.rs` defines the `ObjectStore` trait plus `S3ObjectStore`
    (real `aws-sdk-s3`, using conditional `If-Match`/`If-None-Match` PUTs) and `LocalFsObjectStore`
    (dev/test emulation of one shared bucket); `cache.rs` is the NVMe cache-aside layer
    (`CachedObjectStore`) — only immutable `sst/`/`wal/` objects are cached, manifest/lease reads always
    go to the backing store fresh; `manifest.rs` is the single-writer/multi-reader shared manifest;
    `lease.rs` is the CAS-based write lease with a monotonic fencing token, so a superseded writer's
    manifest CAS is rejected even if it never noticed its lease expired; `config.rs` reads all of the
    above from env vars.
  - `phase3_tests.rs` (`#[cfg(test)]`) — the integration test proving the disaggregated-storage model:
    two in-process `Database`s share one `LocalFsObjectStore` root; covers writer-writes-reader-reads,
    write rejection on a non-writer, and lease-takeover fencing.

Row values on the wire (and in storage) are encoded as a length-prefixed sequence of
`Option<Vec<u8>>` cells (`parse_rows` / the inverse in `execute_insert`/`execute_update`) — there's no
separate row struct once a row leaves the executor's `RowContext`.
