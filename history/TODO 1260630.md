# TPT Platform — Build Roadmap

> Track progress across all 7 engines and the AI layer.
> Check off items as they are completed.

---

## Phase 0 — Foundation: Keystone Core

- [x] Cargo workspace + `tpt-keystone` crate
- [ ] Tokio TCP listener on :5432
- [ ] PostgreSQL wire protocol v3 (from scratch) — startup handshake
- [ ] PostgreSQL wire protocol v3 — Simple Query Protocol loop
- [ ] SQL Lexer (hand-written tokenizer)
- [ ] SQL AST node types
- [ ] SQL Parser (recursive-descent)
- [ ] Expression evaluator (literals + arithmetic)

**Milestone:** `psql` connects and `SELECT 1` returns a result

---

## Phase 1 — Keystone: Storage Engine

- [ ] Write-Ahead Log (WAL) with fsync guarantees
- [ ] MemTable (BTreeMap-based, in-memory write buffer)
- [ ] SSTable format + bloom filters
- [ ] LSM-tree compaction (levelled strategy)
- [ ] MVCC (Multi-Version Concurrency Control)
- [ ] Transaction manager (BEGIN / COMMIT / ROLLBACK)
- [ ] B-Tree indexes for primary keys + secondary indexes
- [ ] io_uring async I/O integration (Linux NVMe path)

**Milestone:** INSERT rows, restart process, SELECT them back

---

## Phase 2 — Keystone: SQL Engine

- [ ] Full SELECT (FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET)
- [ ] JOINs — hash join, merge join, nested loop
- [ ] INSERT, UPDATE, DELETE with MVCC isolation
- [ ] DDL: CREATE / DROP / ALTER TABLE, CREATE INDEX
- [ ] Subqueries + CTEs (WITH)
- [ ] Window functions
- [ ] Prepared statements (extended query protocol — Parse/Bind/Execute)
- [ ] Query planner + cost-based optimiser

**Milestone:** TPC-H benchmark queries run correctly

---

## Phase 3 — Keystone: Cloud-Native Storage

- [ ] Disaggregated storage: S3-compatible object store as source of truth
- [ ] Local NVMe cache layer (cache-aside, LRU eviction)
- [ ] Stateless compute nodes (no local durable state)
- [ ] Horizontal scale-out: multiple compute nodes share one S3 bucket
- [ ] Fencing / lease mechanism for concurrent writers

**Milestone:** Two compute nodes share one S3 bucket, queries return consistent results

---

## Phase 4 — Keystone: Extensions + Compatibility

- [ ] Wasmtime integration for sandboxed UDFs (WASM-based user-defined functions)
- [ ] Full Postgres wire protocol parity (COPY, server-side cursors, LISTEN/NOTIFY)
- [ ] `pg_catalog` system tables (`\d`, `\dt`, `\di` etc. in psql)
- [ ] Built-in connection pooler (session multiplexing)
- [ ] `pg_dump` / `pg_restore` compatibility

**Milestone:** Most psql meta-commands work; existing Postgres client libraries connect

---

## Phase 5 — AI Layer

- [ ] **MCP server** — TPT exposes a Model Context Protocol server for AI agents
  - Port: 5433 (alongside Postgres listener on 5432)
  - Tools: `query(sql)`, `schema()`, `tables()`, `columns(table)`, `explain(sql)`, `mutate(sql)`
  - Auth: TPT token header
- [ ] **Schema introspection API** — structured metadata for LLM context
  - Table names, column types, nullability, defaults, constraints, indexes, foreign keys
  - Row count statistics and value distribution histograms
  - Relationship graph (FK chains) as machine-readable JSON
- [ ] **AI-optimised SDK** — idiomatic clients for Rust, TypeScript, Python
  - Typed query builder (no raw SQL string construction)
  - Schema-aware types generated from live database introspection
  - Batch operations, streaming results, built-in connection pool

**Milestone:** Claude (or any MCP client) can discover schema and query TPT without a Postgres driver

---

## Phase 6 — Meridian: Geospatial Engine

- [ ] Custom Rust computational geometry library (replaces GEOS / C++ bindings)
- [ ] S2 Geometry hierarchical grid indexing
- [ ] Uber H3 hexagonal grid indexing
- [ ] 4D spatiotemporal storage model (lat, lon, alt, time as first-class)
- [ ] GPU-accelerated spatial joins via wgpu compute shaders
- [ ] Raster + vector unified storage model
- [ ] OGC Simple Features + SQL/MM Spatial compatibility

**Milestone:** "Find all drones within 500m of coordinate between T1 and T2" runs as a single index scan in <10ms on 10M rows

---

## Phase 7 — Prism: Vector / AI Engine

- [ ] HNSW index — native Rust, SIMD-accelerated (AVX-512 / NEON)
- [ ] IVF-PQ index (inverted file + product quantisation)
- [ ] DiskANN index for billion-scale on-disk graphs
- [ ] Automatic index selection by query planner (latency vs recall trade-off)
- [ ] Cosine / dot-product / L2 similarity, hardware-accelerated
- [ ] Hybrid search: vector + BM25 full-text + SQL filters in single pass
- [ ] Native product / scalar / binary quantisation at storage layer
- [ ] Consistent hashing for distributed vector shards
- [ ] Optional CUDA / ROCm GPU offload for batch similarity

**Milestone:** 1M-vector ANN search returns top-10 in <5ms with >95% recall

---

## Phase 8 — Chronos: Time-Series Engine

- [ ] Time-aware append-only storage pages
- [ ] Gorilla compression (XOR-encoded float deltas)
- [ ] Delta-of-delta integer compression
- [ ] Dictionary encoding for low-cardinality tag columns
- [ ] Automatic time-based partitioning (hourly / daily / monthly)
- [ ] Configurable retention + automatic downsampling policies
- [ ] Continuous aggregates — real-time incrementally-updated materialised views
- [ ] SQL time extensions: `time_bucket()`, `interpolate()`, `moving_average()`, `gap_fill()`

**Milestone:** 1M rows/sec sustained ingest with ≥15:1 compression ratio; query last 30 days in <100ms

---

## Phase 9 — Plexus: Graph Engine

- [ ] Native adjacency-list storage format (zero-copy neighbour traversal)
- [ ] Property graph model — vertices and edges each carry arbitrary properties
- [ ] Multi-relational edges (typed relationships)
- [ ] Bidirectional traversal with direction filters
- [ ] GQL (Graph Query Language) compatibility layer
- [ ] Native parallel graph algorithms: shortest path, PageRank, community detection, connected components, triangle counting
- [ ] Triangle indexing for fast neighbour lookups
- [ ] Hybrid SQL + GQL queries (filter vertices by SQL, traverse by GQL)

**Milestone:** 6-hop BFS traversal on a 100M-edge graph in <100ms

---

## Phase 10 — Canopy: Document / JSON Engine

- [ ] Native JSON/BSON binary storage format
- [ ] Path-based deep indexing (e.g. `user.address.city` → B-Tree index)
- [ ] Inverted full-text index over string fields within JSON documents
- [ ] JSON Schema validation engine (strict / relaxed / off per collection)
- [ ] ACID transactions spanning document collections + relational tables
- [ ] Aggregation pipeline (MongoDB-compatible stages: `$match`, `$group`, `$project`, etc.)
- [ ] `jsonb` column type in relational tables (unified with Canopy collections)

**Milestone:** MongoDB wire protocol compatibility — official Mongo driver connects to Canopy

---

## Phase 11 — Flux: Event Streaming Engine

- [ ] Append-only partitioned log optimised for sequential NVMe I/O
- [ ] Native consumer groups + per-consumer offset tracking
- [ ] Configurable retention: time-based and size-based
- [ ] Native Change Data Capture (CDC) — every Keystone mutation auto-generates an immutable event
- [ ] Event replay and time-travel queries (reconstruct DB state at any past timestamp)
- [ ] Windowing functions over event streams (tumbling, sliding, session windows)
- [ ] WebSocket streaming endpoint (real-time low-latency push to clients)
- [ ] gRPC streaming endpoint (high-throughput consumer protocol)

**Milestone:** 1M messages/sec sustained write; Kafka consumer client connects to Flux

---

## Phase 12 — Production Hardening

- [ ] Kubernetes operator (CRD-based cluster lifecycle management)
- [ ] Prometheus `/metrics` endpoint (all engines instrument standard metrics)
- [ ] Distributed tracing via OpenTelemetry (spans across network + storage layers)
- [ ] Formal benchmark suite vs Postgres, InfluxDB, Neo4j, MongoDB, Kafka
- [ ] Documentation site (architecture, SQL reference, SDK docs, tutorials)
- [ ] Security audit (wire protocol auth, WASM sandbox, S3 credential handling)
- [ ] Apache 2.0 open-source release

---

## Phase 13 — Canvas: Data-Aware Frontend Framework

- [ ] Core framework in Rust compiled to WebAssembly (WASM bundle targeting browsers)
- [ ] WebGPU rendering backend for hardware-accelerated maps, charts, and graphs
- [ ] Reactive primitives (SolidJS-inspired, optimised for multi-model data streams)
- [ ] Automatic WebSocket connection to TPT Flux for zero-config real-time updates
- [ ] `<Canvas.Map>` — geospatial component (Mapbox GL alternative, Meridian-native)
  - Clustering, heatmaps, spatial filter queries built-in
- [ ] `<Canvas.TimeSeries>` — time-series chart (Chronos-native)
  - Auto-downsampling, interpolation, real-time Flux stream updates
- [ ] `<Canvas.Graph>` — graph visualisation (Plexus-native)
  - Force-directed layout, native traversal controls
- [ ] `<Canvas.VectorSearch>` — ANN result renderer (Prism-native)
- [ ] `<Canvas.Document>` — JSON document viewer/editor (Canopy-native)
- [ ] Automatic TypeScript type generation from live Keystone schemas
- [ ] Built-in reactive state stores that auto-sync with Keystone queries (no external state lib)
- [ ] Plugin API for custom Canvas components with WebGPU shader hooks
- [ ] Integration with popular bundlers (Vite, Webpack, esbuild)

**Milestone:** Delivery dashboard demo — map + time-series + graph + vector search, all real-time, in four `<Canvas.*>` components with zero manual WebSocket code

---

## Phase 14 — SDK Ecosystem

### SDK/Web (TypeScript / JavaScript)
- [ ] `@tpt/sdk-web` npm package
- [ ] Full TypeScript type definitions auto-generated from Keystone schemas
- [ ] `useKeystoneQuery` / `useKeystoneMutation` reactive hooks
- [ ] Native WebSocket integration with TPT Flux
- [ ] Type-safe builders for all 7 data models
- [ ] Plugin API for custom Canvas components
- [ ] Bundler integration (Vite, Webpack, esbuild)

### SDK/Rust (Native Desktop & Server)
- [ ] `tpt-sdk` crate with `canvas` + `keystone` feature flags
- [ ] Sync + async APIs
- [ ] Direct Canvas rendering pipeline access (WebGPU / Vulkan)
- [ ] Embedded Keystone client for edge / desktop (Tauri, GTK)
- [ ] Zero-copy data transfer between Canvas and native code
- [ ] FFI bindings for C/C++ interop

### SDK/Mobile
- [ ] `@tpt/sdk-react-native` — native bridge to Canvas, offline-first, Flux push notifications
- [ ] Flutter SDK (`tpt_sdk`) — custom widgets, hot reload, Metal/Vulkan backends
- [ ] Swift SDK (iOS) — async/await, SwiftUI + UIKit, CoreLocation + Metal
- [ ] Kotlin SDK (Android) — coroutines, Jetpack Compose, Fused Location + Vulkan

### SDK/Server (Backend)
- [ ] `@tpt/sdk-server` — Node.js / Deno / Bun, streaming queries, SSR, Flux broadcast
- [ ] Python SDK (`tpt-sdk`) — type hints, Pandas/NumPy, Jupyter, async/await
- [ ] Go SDK (`github.com/tpt/sdk-go`) — idiomatic Go, context cancellation, connection pooling

### SDK/CLI
- [ ] Single binary CLI (`tpt`) — interactive REPL, export/import, schema introspection
- [ ] `tpt query` — execute SQL, output JSON/CSV/table
- [ ] `tpt stream` — tail a Flux event stream in real-time
- [ ] `tpt migrate` — schema migration tooling

### SDK/Plugin (Canvas Extensions)
- [ ] Plugin lifecycle management (register, mount, unmount)
- [ ] Custom rendering hooks (WebGPU compute + fragment shaders)
- [ ] Inter-plugin event system
- [ ] Marketplace publishing toolchain

### SDK/Edge (Wasm Workers)
- [ ] `@tpt/sdk-edge` — <50KB WASM bundle for Cloudflare Workers / Fastly / Lambda@Edge
- [ ] Streaming responses + edge caching integration
- [ ] Zero cold-start profile

**Milestone:** Single-line connection to Keystone works from Web, Rust, Python, and CLI; `tpt query "SELECT 1"` returns a result

---

*All engines + SDKs: Apache 2.0 licensed. Built in Rust. Cloud-native from day one.*
