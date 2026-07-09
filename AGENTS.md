# AGENTS.md — TPT Keystone DB

## What's real vs roadmap

This is a multi-engine data platform where **only the first engine (Keystone) is implemented** — a cloud-native, Postgres-wire-compatible relational database in `tpt-keystone/`. Everything in `TODO.md` beyond Phase 4 (Meridian, Prism, Chronos, Plexus, Canopy, Flux, Canvas, SDKs, Harbor, Synapse/Mirror) is design intent, not code.

The repo root has multiple Rust crates (`tpt-keystone`, `tpt-cli`, `tpt-harbor`, `tpt-operator`, `tpt-canvas`, `tpt-sdk`), TypeScript packages (`packages/sdk-web`, `packages/sdk-server`, `packages/sdk-edge`), Go SDK (`sdk-go/`), and Python SDK (`sdk-python/`). Most are stubs/skeletons — `tpt-keystone` is the only one with substantial code.

**There is no workspace `Cargo.toml`** — each Rust crate is independent. Build/test from individual crate directories.

## Hard constraints (never violate)

- **No `pgwire`/`sqlparser-rs` or similar crates.** Wire protocol codec (`src/wire/`) and SQL lexer/parser/AST (`src/sql/`) are hand-written by design.
- **Row-Level Security is permanently out of scope.** Access control (future) follows Zanzibar-style ReBAC + RBAC.

## Build / test / run (Keystone — the only implemented engine)

All commands from `tpt-keystone/`:

| Command | What |
|---|---|
| `cargo build` | Build the binary |
| `cargo run` | Starts single-node writer on `0.0.0.0:5432`, local-fs storage under `tpt-data/` |
| `cargo test` | Unit tests + Phase 3 multi-node integration tests |
| `cargo test phase3_tests::<name>` | Single Phase 3 test |
| `psql -h localhost -p 5432` | Connect (no auth; auto-approved) |

No CI, no `rustfmt.toml`/`clippy.toml`, no lint/format enforcement — use `cargo fmt`/`cargo clippy` at discretion.

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

`tpt-keystone/src/main.rs` wires: `ObjectStore` → `CachedObjectStore` (NVMe cache-aside) → lease acquire/renew (if writer) → `Database` → manifest-refresh loop (if reader) → TCP accept → `wire::session::handle`.

## Architecture (4 layers, each thin over the one below)

- **`src/wire/`** — Postgres wire protocol v3, hand-written. `codec.rs` frames bytes; `messages.rs` defines types/OIDs; `session.rs` drives startup handshake → simple/extended query loop.
- **`src/sql/`** — Hand-written lexer → recursive-descent parser → `ast.rs` node types. Single entry point: `sql::parse()` returns a `Stmt`.
- **`src/executor/`** — Turns `Stmt` → `QueryResult` (`mod.rs`). SELECT executes as one pipeline in `execute_select_with_cte`. Hash join (build = smaller input); else nested-loop.
- **`src/storage/`** — LSM engine (MemTable + SSTable + levelled compaction, `lsm.rs`/`sstable.rs`), MVCC/transaction manager (`mvcc.rs`/`tx.rs`), local B-Tree indexes (`btree.rs`, local-only per Phase 3 scope cut). `database.rs` ties LSM + MVCC + schema catalog + indexes together.

## Important src modules beyond the core 4

- `src/mcp/` — MCP server (JSON-RPC 2.0 over HTTP on port 5433) for AI agent tools.
- `src/metrics.rs` / `src/telemetry.rs` — OpenTelemetry tracing setup.
- `src/geo/`, `src/graph/`, `src/vector/` — Unbuilt engine stubs for future phases.

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
