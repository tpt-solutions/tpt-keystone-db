# AGENTS.md — TPT Keystone DB

## What's real vs roadmap

This is a multi-engine data platform where the **Keystone relational engine** (`tpt-keystone/`) is the core, and most other engines are implemented as modules inside it (not separate crates). Phases 0–15 of `TODO.md` are implemented. Only Phase 16 (Synapse) and Phase 17 (Mirror) remain unbuilt.

**Rust crates** (each independent — no workspace `Cargo.toml`):
| Crate | Purpose | Status |
|---|---|---|
| `tpt-keystone/` | Core engine (relational + geo/graph/vector/time-series/document/streaming) | Substantial |
| `tpt-sdk/` | Rust client SDK (sync + async, FFI) | Real, unverified by `cargo test` |
| `tpt-cli/` | `tpt` CLI binary (REPL, query, export/import, migrate, stream) | Real |
| `tpt-harbor/` | Migration platform (Harbor/PG implemented, others are stubs) | Real |
| `tpt-canvas/` | WASM frontend framework (builds to `wasm32-unknown-unknown`) | Real |
| `tpt-operator/` | Kubernetes operator (CRD-based lifecycle) | Real |

**SDK packages**: `packages/sdk-web/`, `packages/sdk-server/`, `packages/sdk-edge/` (TypeScript), `sdk-python/` (Python), `sdk-go/` (Go) — all have real implementations, not stubs.

**There is no workspace `Cargo.toml`** — each Rust crate is independent. Build/test from individual crate directories.

## Hard constraints (never violate)

- **No `pgwire`/`sqlparser-rs` or similar crates.** Wire protocol codec (`src/wire/`) and SQL lexer/parser/AST (`src/sql/`) are hand-written by design.
- **Row-Level Security is permanently out of scope.** Access control (future) follows Zanzibar-style ReBAC + RBAC.

## Build / test / run (Keystone — the only implemented engine)

All commands from `tpt-keystone/`:

| Command | What |
|---|---|
| `cargo build` | Build the binary |
| `cargo run` | Starts single-node writer on `0.0.0.0:55432`, local-fs storage under `tpt-data/` |
| `cargo test` | Unit tests + Phase 3 multi-node integration tests |
| `cargo test phase3_tests::<name>` | Single Phase 3 test |
| `cargo test geo_tests` | Geospatial engine tests |
| `cargo test chronos_tests` | Time-series engine tests |
| `cargo test plexus_tests` | Graph engine tests |
| `cargo test canopy_tests` | Document/JSON engine tests |
| `cargo test flux_tests` | Event streaming tests |
| `cargo test prism_tests` | Vector/AI engine tests |
| `cargo test phase4_tests` | Phase 4 (WASM UDFs, pg_catalog, COPY, cursors, etc.) |
| `cargo test pg_dump_tests` | pg_dump compatibility tests |
| `psql -h localhost -p 55432` | Connect (no auth; auto-approved) |

No CI, no `rustfmt.toml`/`clippy.toml`, no lint/format enforcement — use `cargo fmt`/`cargo clippy` at discretion.

### Listeners (all started by `cargo run`)

The process binds multiple TCP listeners simultaneously:

| Port | Purpose | Env var override |
|---|---|---|
| 55432 | Postgres wire protocol v3 | hardcoded in `main.rs` |
| 5433 | MCP server (JSON-RPC 2.0 for AI agents) | `TPT_MCP_ADDR` |
| 5434 | Flux WebSocket streaming | `TPT_FLUX_WS_ADDR` |
| 5435 | Canvas HTTP/JSON query bridge | `TPT_HTTP_ADDR` |
| 9187 | Prometheus metrics | `TPT_METRICS_ADDR` |

### Two-node shared-storage mode (Phase 3)

Configure via env vars (`src/storage/config.rs`): `TPT_STORAGE_BACKEND` (`local`|`s3`), `TPT_LOCAL_STORE_DIR`, `TPT_S3_BUCKET`/`TPT_S3_REGION`/`TPT_S3_ENDPOINT`, `TPT_NODE_ROLE` (`writer`|`reader`), `TPT_NODE_ID`, `TPT_LEASE_TTL_SECS`, `TPT_MANIFEST_REFRESH_SECS`, `TPT_LOCAL_DIR`, `TPT_CACHE_DIR`, `TPT_CACHE_MAX_BYTES`. Readers skip lease acquisition; only one writer at a time (CAS-based fencing).

## SDK packages

| Package | Language | Dir | Build | Test |
|---|---|---|---|---|
| Keystone engine | Rust | `tpt-keystone/` | `cargo build` | `cargo test` |
| SDK/Web | TypeScript | `packages/sdk-web/` | `npm run build` | `node --test dist/**/*.test.js` |
| SDK/Server | TypeScript | `packages/sdk-server/` | `npm run build` | `node --test dist/**/*.test.js` |
| SDK/Edge | TypeScript | `packages/sdk-edge/` | `npm run build` | `node --test dist/**/*.test.js` |
| SDK/Python | Python | `sdk-python/` | `pip install .` | `pytest` |
| SDK/Go | Go | `sdk-go/` | `go build` | `go test` |

TS packages use Node's built-in `node --test` runner (not jest/vitest). `pretest` runs `npm run build` automatically.

## Entrypoint

`tpt-keystone/src/main.rs` wires: `ObjectStore` → `CachedObjectStore` (NVMe cache-aside) → lease acquire/renew (if writer) → `Database` → manifest-refresh loop (if reader) → TCP accept → `wire::session::handle`. Also spawns MCP, Flux WebSocket, Canvas HTTP, and Prometheus metrics listeners.

## Architecture (4 layers, each thin over the one below)

- **`src/wire/`** — Postgres wire protocol v3, hand-written. `codec.rs` frames bytes; `messages.rs` defines types/OIDs; `session.rs` drives startup handshake → simple/extended query loop. Also includes `http_query.rs` (Canvas HTTP/JSON bridge) and `websocket.rs` (Flux WebSocket streaming).
- **`src/sql/`** — Hand-written lexer → recursive-descent parser → `ast.rs` node types. Single entry point: `sql::parse()` returns a `Stmt`. Includes `cache.rs` (shared prepared-statement cache).
- **`src/executor/`** — Turns `Stmt` → `QueryResult` (`mod.rs`). SELECT executes as one pipeline in `execute_select_with_cte`. Hash join (build = smaller input); else nested-loop.
- **`src/storage/`** — LSM engine (MemTable + SSTable + levelled compaction, `lsm.rs`/`sstable.rs`), MVCC/transaction manager (`mvcc.rs`/`tx.rs`), local B-Tree indexes (`btree.rs`, local-only per Phase 3 scope cut). `database.rs` ties LSM + MVCC + schema catalog + indexes together. Also contains `compress.rs` (Gorilla/delta-of-delta codecs), `flux.rs` (event streaming log), `jsonb.rs` (JSON binary format), `json_schema.rs` (schema validation).

## Important src modules beyond the core 4

- **`src/mcp/`** — MCP server (JSON-RPC 2.0 over HTTP on port 5433) for AI agent tools.
- **`src/metrics.rs`** / **`src/telemetry.rs`** — OpenTelemetry tracing setup.
- **`src/geo/`** — Meridian geospatial engine: hand-written computational geometry, S2-inspired hierarchical grid, H3-inspired hex grid. Real implementations, not stubs.
- **`src/graph/`** — Plexus graph engine: adjacency-list property graph, BFS/PageRank/community detection/triangle counting. Real implementations, not stubs.
- **`src/vector/`** — Prism vector engine: HNSW index, L2/cosine/dot-product similarity. Real implementations, not stubs.
- **`src/storage/geo_index.rs`**, **`src/storage/graph_index.rs`**, **`src/storage/ts_index.rs`**, **`src/storage/canopy_index.rs`**, **`src/storage/vector_index.rs`** — Local secondary index accelerators for each engine (all local-only, not object-store-replicated).

## Key reference files

- `TODO.md` — Authoritative phase-by-phase roadmap/checklist (including what's verified vs not).
- `CLAUDE.md` — Deeper architecture notes and build/test commands for AI-assisted dev.
- `PHASE2_PLAN.md` — Fine-grained SQL engine implementation notes.
- `*spec.txt` (`1keystonespec.txt` … `10harbourspec.txt`) — Per-engine design specs.

## Testing quirks

- Phase 3 tests run two in-process `Database`s sharing one `LocalFsObjectStore` — writer-writes-reader-reads, write rejection on non-writer, lease-takeover fencing.
- WASM UDF tests crash on Windows in wasmtime's trap handler (`STATUS_STACK_BUFFER_OVERRUN`) — fuel/memory limit tests must be verified on Linux or a normal Windows host.
- End-to-end `pg_dump`/`psql -f` fidelity is not verified; only primitives are unit-tested.

## Deleted from CLAUDE.md (superseded)

CLAUDE.md is still the deeper source of truth. AGENTS.md extracts the subset an agent is most likely to miss or need fast. If in doubt, read CLAUDE.md.

.gitignore highlights: `**/target/`, `Cargo.lock` (root cruft, not workspace), `**/tpt-data/`, `node_modules/`, `dist/`.
