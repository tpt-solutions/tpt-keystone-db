# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

TPT is a ground-up, multi-engine data platform written in Rust. **All 17 roadmap phases now have an
implementation** — this is not just Keystone anymore. Do not assume a feature is unbuilt without
checking `TODO.md` first.

- **`tpt-keystone/`** is the core crate and the only thing everything else depends on: a cloud-native,
  Postgres-wire-compatible relational engine (Phases 0–4, 12), plus six other engines implemented as
  SQL-extension modules *inside the same binary* rather than separate storage engines — Meridian/geo
  (`src/geo/`), Prism/vector (`src/vector/`), Chronos/time-series (`src/storage/ts_index.rs`,
  `compress.rs`), Plexus/graph (`src/graph/`), Canopy/document (JSON operators + indexes, no separate
  module), Flux/streaming (`src/storage/flux.rs`, `src/wire/websocket.rs`, `src/wire/grpc/`) — plus two agent-facing
  layers, Synapse (`src/synapse/`) and Mirror (`src/mirror/`), and an MCP server (`src/mcp/`).
- **`tpt-canvas/`** (WASM frontend), **`tpt-harbor/`** (migration platform), **`tpt-cli/`**, **`tpt-sdk/`**
  (Rust SDK, incl. a `copy_in` bulk-ingest path over `COPY ... FROM STDIN`), **`tpt-operator/`**
  (Kubernetes operator), **`sdk-go/`**, **`sdk-python/`**, **`packages/`** (`sdk-web`/`sdk-server`/
  `sdk-edge`/`sdk-react-native` TypeScript SDKs), **`tpt_sdk/`** (Flutter/Dart), **`tpt-sdk-android/`**
  (Kotlin) are separate crates/packages, each building independently (no root Cargo workspace). The three
  mobile SDKs are an offline-first Canvas HTTP/JSON client + Flux WebSocket subscriber, not a native
  Postgres-wire bridge; React Native and Flutter are test-verified in this repo, Kotlin/Android is not
  (no `gradle`/`kotlinc` here), and Swift/iOS is unbuilt (no Xcode).

Every "implemented" item has real, scoped caveats — wire-level SCRAM-SHA-256 auth and TLS exist but are
opt-in (empty `_tpt_roles`/no `TPT_TLS_CERT_PATH`+`TPT_TLS_KEY_PATH` keeps the old no-auth/no-TLS
zero-config default), no distributed secondary indexes (all six extension engines' indexes are
local/single-node), no benchmark harness. **`TODO.md` is the authoritative phase-by-phase status** — it documents
scope cuts and what's actually been verified (often "unit/integration-tested in-process," not "run
against a real external server" or "run at scale") vs. just claimed; read the relevant phase there before
assuming a feature is missing, complete, or production-ready. `PHASE2_PLAN.md` (root and duplicated in
`tpt-keystone/`) has finer-grained notes on Keystone's SQL engine phase specifically. The numbered
`*spec.txt` files in the repo root (`1keystonespec.txt`, `2meridianspec.txt`, etc.) are the original
per-engine design specs.

## Hard constraints (do not violate)

- **No `pgwire`/`sqlparser-rs` or similar crates.** The Postgres wire protocol codec (`src/wire/`) and
  the SQL lexer/parser/AST (`src/sql/`) are hand-written from scratch by design — this is a deliberate
  project goal, not a gap to fill with a library.
- Row-Level Security is permanently out of scope. Access control (when built) follows a Zanzibar-style
  ReBAC tuple model + RBAC layer instead.

## Build / run / test

There is no workspace `Cargo.toml` at the repo root — every Rust crate (`tpt-keystone/`, `tpt-canvas/`,
`tpt-harbor/`, `tpt-cli/`, `tpt-sdk/`, `tpt-operator/`) builds independently; `cd` into it first. The
core engine, which every other crate/SDK talks to over the wire:

```
cd tpt-keystone
cargo build
cargo run                      # starts a single-node writer on 0.0.0.0:55432 (TPT_PG_ADDR overrides), local-fs storage under tpt-data/
cargo test                     # unit tests + storage::phase3_tests (multi-node cloud-storage integration tests)
cargo test phase3_tests::<name>  # run one Phase 3 test
psql -h localhost -p 55432     # connect once running (no auth by default; SCRAM only kicks in once _tpt_roles is non-empty)
```

Other listeners `cargo run` starts on the same node: MCP (`TPT_MCP_ADDR`, default `:5433`), the HTTP/JSON
bridge for browsers/edge SDKs (`TPT_HTTP_ADDR`, default `:5435`, `src/wire/http_query.rs`), the Flux
WebSocket push bridge (`TPT_FLUX_WS_ADDR`, default `:5434`, `src/wire/websocket.rs`), the Flux gRPC
streaming endpoint (`TPT_FLUX_GRPC_ADDR`, default `:5436`, `src/wire/grpc/` — hand-rolled HTTP/2 h2c +
HPACK + protobuf + gRPC framing, no `h2`/`tonic` crate, same from-scratch-wire-protocol rule as
everything else), and Prometheus metrics (`TPT_METRICS_ADDR`, default `:9187`, `src/metrics.rs`).

Other crates build/test the same way (`cd <dir> && cargo build`/`cargo test`); `tpt-canvas` additionally
targets `wasm32-unknown-unknown` (`cargo build --target wasm32-unknown-unknown`) and has no meaningful
native test path since it's browser-only. TypeScript packages (`packages/*`) use `npm run build`/
`node --test`; `sdk-python` uses `uv pip install -e ".[pandas,dev]"`; `sdk-go` uses `go build ./...`/
`go test ./...` (`go test -tags live ./...` needs a running `tpt-keystone` node).

`.github/workflows/ci.yml` runs the test suite; no `rustfmt.toml`/`clippy.toml` exist yet, so use
`cargo fmt` / `cargo clippy` at your own discretion but there is no enforced formatting/lint convention
to match.

### Running two nodes against shared storage (Phase 3 cloud-native mode)

Compute nodes are stateless; everything durable lives behind the `ObjectStore` trait (local-fs emulation
or real S3). Configure via env vars (see `src/storage/config.rs`): `TPT_STORAGE_BACKEND` (`local`|`s3`),
`TPT_LOCAL_STORE_DIR`, `TPT_S3_BUCKET`/`TPT_S3_REGION`/`TPT_S3_ENDPOINT`, `TPT_NODE_ROLE`
(`writer`|`reader`), `TPT_NODE_ID`, `TPT_LEASE_TTL_SECS`, `TPT_MANIFEST_REFRESH_SECS`, `TPT_LOCAL_DIR`,
`TPT_CACHE_DIR`, `TPT_CACHE_MAX_BYTES`. Only one writer may hold the lease at a time (CAS-based fencing,
`src/storage/lease.rs`); readers poll-refresh the manifest instead of acquiring it.

## Architecture

This section covers `tpt-keystone/`'s core (wire/sql/executor/storage) in depth, since it's the
foundation every other engine module and every external crate/SDK builds on. The six extension engines
(`src/geo/`, `src/vector/`, `src/graph/`, `src/storage/ts_index.rs`+`compress.rs`, the Canopy JSON
operators in `executor/eval.rs`, `src/storage/flux.rs`+`src/wire/websocket.rs`) and the agent layers
(`src/synapse/`, `src/mirror/`) each add DDL options (`CREATE INDEX ... USING <KIND>`) and table-valued
SQL functions on top of this same pipeline rather than a separate query path — see each module's own
doc comments and the corresponding `TODO.md` phase for detail; they're intentionally not re-documented
here.

`src/main.rs` wires everything together: build an `ObjectStore` → wrap it in `CachedObjectStore` (NVMe
cache-aside) → acquire/renew the write lease if this node is a writer → open `Database` → spawn a
manifest-refresh loop if this node is a reader → accept TCP connections and hand each one to
`wire::session::handle`.

Four top-level modules, each a thin layer over the one below:

- **`src/wire/`** — Postgres wire protocol v3, hand-written. `codec.rs` frames raw bytes into
  `FrontendMessage`/`BackendMessage`; `messages.rs` defines the message types, type OIDs, and the
  binary-format cell codec (`text_cell_to_binary`/`decode_param`, keyed on OID: int2/int4/int8,
  float4/float8, bool, text, bytea — everything else stays text); `session.rs` drives one client
  connection through startup handshake → simple query loop and the extended query protocol
  (Parse/Bind/Describe/Execute/Sync/Close, tracked per-connection via `ExtendedState`: prepared
  statements, bound portals, and `DECLARE CURSOR` state — cursors and LISTEN/NOTIFY only work over the
  simple query protocol, not extended). `Bind`'s `result_formats` selects binary vs. text per result
  column (opt-in, text remains the default); binary applies to result columns and `Bind` parameters only,
  not `Parse` args or the simple-query path.
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
