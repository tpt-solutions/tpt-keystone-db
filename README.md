# TPT Keystone DB

TPT is a ground-up, multi-engine data platform written in Rust, developed by TPT Solutions. The
long-term plan (see [`TODO.md`](TODO.md)) is seven purpose-built data engines — relational, geospatial,
vector, time-series, graph, document, and event-streaming — plus an AI/MCP layer, a WebGPU-adjacent
frontend framework, a multi-language SDK ecosystem, a universal migration platform, and an agent
orchestration/observability layer, all sharing one storage substrate.

**All 17 roadmap phases have an implementation now** (see [`TODO.md`](TODO.md) for the authoritative,
line-by-line status of every checklist item, including explicit scope cuts and what has/hasn't been
verified against real external services). This is still a from-scratch, single-team project, not a
production-hardened platform — most components are verified in-process or against each other, not at
scale or against third-party servers. Treat every "implemented" claim below as "implemented and
unit/integration-tested in this repo," not "battle-tested in production."

## Keystone — the relational engine (core)

Keystone (`tpt-keystone/`) is the only engine every other piece in this repo is built on top of — a
cloud-native, PostgreSQL-wire-compatible database, built from scratch with no `pgwire` or
`sqlparser-rs` dependency (the wire protocol codec and the SQL lexer/parser are hand-written). It now
also hosts every other engine's SQL-extension surface (see below) as additional modules in the same
binary, plus an MCP server, an HTTP/JSON bridge for browsers, and WebSocket + gRPC streaming bridges.

Implemented (Phases 0–4, 12): full `SELECT`/`JOIN`/subqueries/CTEs/window functions/DDL, a
write-ahead-logged LSM storage engine (MemTable → SSTables with bloom filters, levelled compaction),
MVCC with a transaction manager and local B-Tree indexes, disaggregated cloud-native storage
(`ObjectStore` trait over S3 or a local-fs emulation, NVMe cache-aside, shared manifest, CAS-based write
lease with fencing), WASM UDFs (sandboxed via `wasmtime`, scalar `int8`/`float8`/`bool` plus
`float8[]`/`bytea` args and returns via a `(ptr, len)` linear-memory ABI), `pg_catalog`/
`information_schema`, COPY/cursors/LISTEN-NOTIFY, binary-format result/parameter encoding on the
extended query protocol (per-column, opt-in via `result_formats`; text remains the default), a
connection admission limiter + statement cache, `pg_dump`-plain-compatible DDL (sequences, SERIAL,
UNIQUE/FOREIGN KEY), a Kubernetes operator, Prometheus metrics, and OpenTelemetry tracing, plus opt-in
wire-level SCRAM-SHA-256 auth and TLS (Phase 18 — no-op until `_tpt_roles` is populated or
`TPT_TLS_CERT_PATH`/`TPT_TLS_KEY_PATH` are set, so the zero-config quickstart below is unchanged). See
`TODO.md` Phase 4/12/18 for exact scope cuts (no `ALTER TABLE ADD/DROP COLUMN`, etc.).

## The other six engines (SQL extensions over Keystone, same binary)

Rather than separate storage engines, Phases 6–11 each add indexes, functions, and DDL options onto
Keystone's existing row storage — a `USING SPATIAL`/`USING VECTOR`/`USING GRAPH`/`USING TIME`/
`USING JSONPATH` index option, plus table-valued SQL functions to query it:

- **Meridian** (geospatial) — `geo/`: hand-written WKT geometry, S2/H3-inspired grid indexing, SRID/EWKB
  support, `ST_*` functions including `ST_Transform`, GPU-accelerated (`wgpu`) spatial joins above a
  row-count threshold.
- **Prism** (vector/AI) — `vector/`: a from-scratch HNSW index plus sharded HNSW, DiskANN/Vamana, and
  IVF-PQ index strategies, `vector_search()`, cosine/L2/dot-product functions, hybrid BM25 search, and
  GPU-accelerated (`wgpu`) batch similarity.
- **Chronos** (time-series) — `storage/ts_index.rs`, `storage/compress.rs`: Gorilla + delta-of-delta
  compression, time-bucketed indexes, retention/rollups, `time_bucket()`/`moving_average()`.
- **Plexus** (graph) — `graph/`: adjacency-list indexing, BFS/PageRank/label-propagation/connected-
  components/triangle-count, `graph_neighbors()`/`graph_bfs()`/etc. as table functions, plus a
  GQL-subset `MATCH` pattern-query statement.
- **Canopy** (document/JSON) — `->`/`->>`/`@>` JSON operators, JSONPath + full-text (GIN-style) indexes,
  JSON Schema validation on insert.
- POSIX regex match operators (`~`/`!~`/`~*`/`!~*`) are also available on any text column.
- **Flux** (event streaming) — `storage/flux.rs`: append-only partitioned logs, consumer groups,
  automatic per-table CDC, tumbling/sliding/session windowing, a hand-rolled WebSocket push bridge
  (`wire/websocket.rs`) and a hand-rolled gRPC streaming endpoint (`wire/grpc/` — own HTTP/2 h2c + HPACK
  + protobuf stack, no `h2`/`tonic`).

Plus two agent-facing layers built the same way — **Synapse** (`synapse/`: actor runtime, tiered agent
memory over Chronos/Prism, tool registry, task delegation over Flux) and **Mirror** (`mirror/`: agent
action tracing, session replay, hash-chained audit log, provenance tracking) — and an MCP server
(`mcp/`, port 5433) exposing `query`/`schema`/`explain`/`related` tools to AI agents.

## Frontend, SDKs, and migration tooling (separate crates/packages)

- **`tpt-canvas/`** — a Rust→WASM data-aware frontend framework (Canvas2D rendering, reactive
  primitives, `<Canvas.Map>`/`<Canvas.TimeSeries>`/`<Canvas.Graph>`/`<Canvas.VectorSearch>`/
  `<Canvas.Document>`/`<Canvas.AgentMonitor>` components, auto WebSocket sync with Flux).
- **`packages/sdk-web`**, **`packages/sdk-server`**, **`packages/sdk-edge`** — TypeScript SDKs for
  browsers, Node/Deno/Bun, and edge/Workers runtimes, each with a hand-written Postgres-wire or
  HTTP/JSON client (no `pg`/`postgres` deps).
- **`tpt-sdk/`** — the Rust native SDK (sync + async clients, FFI/C ABI, zero-copy row views, a
  `copy_in` bulk-ingest path driving `COPY table FROM STDIN` instead of one round trip per row).
- **`sdk-go/`**, **`sdk-python/`** — Go and Python clients, both hand-written wire-protocol codecs.
- **`packages/sdk-react-native`**, **`tpt_sdk` (Flutter)**, **`tpt-sdk-android` (Kotlin)** — mobile SDKs:
  an offline-first Canvas HTTP/JSON client plus a Flux WebSocket subscriber, each with a pluggable
  cache-with-TTL storage adapter. Not a native Postgres-wire bridge — Canvas is the bridge, by design.
  React Native and Flutter are test-verified in this repo (`node --test`, `flutter test`); the Kotlin
  SDK is unverified here (no `gradle`/`kotlinc` on this machine). Swift/iOS remains unbuilt (no Xcode).
- **`tpt-cli/`** — the `tpt` binary: REPL, `query`/`export`/`import`/`schema`/`stream`/`migrate`
  subcommands.
- **`tpt-harbor/`** — a universal migration platform (discover → validate → snapshot → replicate →
  verify → cutover) targeting Keystone. PostgreSQL and PostGIS sources are fully implemented and
  verified against a live server; MongoDB/Neo4j/MySQL/MSSQL/InfluxDB/Kafka/vector-DB (Pinecone/Weaviate/
  Qdrant)/Elasticsearch/Oracle all now have real protocol-level source connectors, though most (and
  Oracle especially, which reverse-engineers the TTC wire protocol) are unverified against a live server
  of that kind. See `TODO.md` Phase 15 for the exact per-connector state.
- **`tpt-operator/`** — a Kubernetes operator (kube-rs) for cluster lifecycle management.

## Getting started

Every crate above builds independently — there is no Cargo workspace at the repo root. To run the core
engine:

```bash
cd tpt-keystone
cargo build
cargo run                      # starts a single-node writer on 0.0.0.0:55432 (TPT_PG_ADDR overrides), local-fs storage under tpt-data/
cargo test                     # unit tests + the Phase 3 multi-node cloud-storage integration tests
```

Once running, connect with any Postgres client, e.g. `psql -h localhost -p 55432`, or use the `tpt` CLI
(`cd tpt-cli && cargo run -- query "SELECT 1"`). Other services on the same node: MCP on `:5433`
(`TPT_MCP_ADDR`), the HTTP/JSON bridge for browsers on `:5435` (`TPT_HTTP_ADDR`), the Flux WebSocket
bridge on `:5434` (`TPT_FLUX_WS_ADDR`), the Flux gRPC streaming endpoint on `:5436`
(`TPT_FLUX_GRPC_ADDR`), and Prometheus metrics on `:9187` (`TPT_METRICS_ADDR`).

Compute nodes are configured entirely through environment variables (storage backend, node role, cache
sizing, lease TTL, etc.) — see `tpt-keystone/src/storage/config.rs` for the full list, or
[`CLAUDE.md`](CLAUDE.md) for a summary and an example of running two nodes against shared storage.

## Quickstart with Docker Compose

A one-command local deployment is provided at the repo root (`docker-compose.yml`),
which builds `tpt-keystone` via its `Dockerfile` and exposes every listener:

```bash
docker compose up --build
```

This starts a single-node writer and maps the following host ports:

| Port | Service |
|------|---------|
| 5432 | Postgres wire protocol v3 (`psql -h localhost -p 5432`) |
| 5433 | MCP server (JSON-RPC 2.0 for AI agents) |
| 5434 | Flux WebSocket streaming bridge |
| 5435 | Canvas HTTP/JSON query bridge |
| 5436 | Flux gRPC streaming endpoint |
| 9187 | Prometheus metrics |

> Note: the engine defaults its Postgres-wire listener to `55432`; the
> compose file relocates it to `5432` via the `TPT_PG_ADDR` env var so
> standard Postgres tooling works unchanged. Engine state persists under
> `./tpt-data` on the host.

## Repository layout

- `tpt-keystone/` — the core engine crate: relational storage/SQL/wire protocol, plus every other
  engine's SQL-extension modules (`geo/`, `vector/`, `graph/`, `synapse/`, `mirror/`), the MCP server,
  and the HTTP/WebSocket/gRPC bridges
- `tpt-canvas/` — the WASM frontend framework
- `tpt-harbor/` — the migration platform crate
- `tpt-cli/`, `tpt-sdk/`, `tpt-operator/` — the CLI, Rust SDK, and Kubernetes operator crates
- `sdk-go/`, `sdk-python/`, `packages/` — the Go, Python, and TypeScript (web/server/edge/react-native) SDKs
- `tpt_sdk/` (Flutter), `tpt-sdk-android/` (Kotlin) — mobile SDKs
- `docs/formats/` — versioned, language-independent on-disk format specs (SSTable, WAL, manifest/lease,
  and each engine's index format) for reimplementing a reader outside this codebase
- `TODO.md` — the authoritative, phase-by-phase build roadmap and status for the whole platform
- `PHASE2_PLAN.md` — detailed implementation notes for Keystone's SQL engine phase
- `1keystonespec.txt` … `10harbourspec.txt` — the original per-engine design specs
- `CLAUDE.md` — architecture notes and build/test commands for AI-assisted development in this repo

## Roadmap

See [`TODO.md`](TODO.md) for the full 17-phase plan (plus Phase 18/19 hardening follow-ups) and,
critically, the per-item honesty notes on what's verified vs. stubbed vs. explicitly scope-cut. Known
open gaps across the whole platform: wire-level auth/TLS is opt-in, not mandatory, and off by default;
no distributed (multi-node) secondary indexes for any of the six extension engines (all
local/single-node); no benchmark harness (every throughput/latency figure in the specs is unverified);
and the SDK-mobile targets are scaffolded but partially unverified — React Native and Flutter are
test-verified in this repo, Kotlin/Android is unverified (no `gradle`/`kotlinc` here), and Swift/iOS is
still blocked on platform availability (Harbor's source connectors are no longer stubs — see above).

## License

Dual-licensed under MIT ([`LICENSE-MIT`](LICENSE-MIT)) or Apache License 2.0
([`LICENSE-APACHE`](LICENSE-APACHE)), Copyright 2026 TPT Solutions, at your option. See
[`LICENSE`](LICENSE) for details.
