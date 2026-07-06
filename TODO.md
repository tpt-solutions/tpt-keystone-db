# TPT Platform — Build Roadmap

> Track progress across all 7 engines and the AI layer.
> Check off items as they are completed.

---

## Phase 0 — Foundation: Keystone Core

- [x] Cargo workspace + `tpt-keystone` crate
- [x] Tokio TCP listener on :5432
- [x] PostgreSQL wire protocol v3 (from scratch) — startup handshake
- [x] PostgreSQL wire protocol v3 — Simple Query Protocol loop
- [x] SQL Lexer (hand-written tokenizer)
- [x] SQL AST node types
- [x] SQL Parser (recursive-descent)
- [x] Expression evaluator (literals + arithmetic)

**Milestone:** `psql` connects and `SELECT 1` returns a result

---

## Phase 1 — Keystone: Storage Engine

- [x] Write-Ahead Log (WAL) with fsync guarantees
- [x] MemTable (BTreeMap-based, in-memory write buffer)
- [x] SSTable format + bloom filters
- [x] LSM-tree compaction (levelled strategy)
- [x] MVCC (Multi-Version Concurrency Control)
- [x] Transaction manager (BEGIN / COMMIT / ROLLBACK)
- [x] B-Tree indexes for primary keys + secondary indexes
- [ ] io_uring async I/O integration (Linux NVMe path)

**Milestone:** INSERT rows, restart process, SELECT them back

---

## Phase 2 — Keystone: SQL Engine

- [x] Full SELECT (FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET)
- [x] JOINs — hash join, merge join, nested loop
- [x] INSERT, UPDATE, DELETE with MVCC isolation
- [x] DDL: CREATE / DROP / ALTER TABLE, CREATE INDEX
- [x] Subqueries + CTEs (WITH) — scalar/correlated/EXISTS/IN subqueries, derived tables, recursive CTEs (UNION/UNION ALL)
- [x] Window functions — ranking (ROW_NUMBER/RANK/DENSE_RANK/NTILE), LAG/LEAD, aggregate-as-window with ROWS/RANGE frames
- [x] Prepared statements (extended query protocol — Parse/Bind/Describe/Execute/Sync/Close)
- [x] Query planner + cost-based optimiser (heuristic: index-aware point lookups, size-aware hash-join build side; not a full Selinger-style cost model)

**Milestone:** TPC-H benchmark queries run correctly

---

## Phase 3 — Keystone: Cloud-Native Storage

- [x] Disaggregated storage: S3-compatible object store as source of truth (`storage/objectstore.rs` — `ObjectStore` trait, real `aws-sdk-s3`-backed `S3ObjectStore`, plus a `LocalFsObjectStore` emulation for dev/test)
- [x] Local NVMe cache layer (cache-aside, LRU eviction) (`storage/cache.rs` — `NvmeCache` + `CachedObjectStore`; only immutable `sst/`/`wal/` objects are cached, manifest/lease always read fresh)
- [x] Stateless compute nodes (no local durable state) (`storage/config.rs`, `main.rs` — local disk holds only the active WAL segment + local B-Tree indexes; SSTables, sealed WAL segments, schemas, manifest, and lease all live in the object store)
- [x] Horizontal scale-out: multiple compute nodes share one S3 bucket (`storage/manifest.rs` — single-writer/multi-reader; readers poll-refresh the shared manifest)
- [x] Fencing / lease mechanism for concurrent writers (`storage/lease.rs` — CAS-based lease with monotonic fencing token; a superseded writer's manifest CAS is rejected even if it never notices its own lease expired)

**Milestone verified:** `storage::phase3_tests` runs two in-process `Database`s against one shared `LocalFsObjectStore` root (emulating one bucket) — the writer creates a table, writes, and flushes; the reader sees the same schema and rows after `refresh()`, is rejected on any write attempt, and a lease-takeover test confirms a superseded writer's later flush is fenced off. The real `S3ObjectStore` path is implemented against the S3 API contract (conditional `If-Match`/`If-None-Match` PUTs) but has not been exercised against a live AWS S3/MinIO endpoint in this environment.
B-Tree secondary indexes remain local-only (deliberate scope cut — see plan `fizzy-growing-harp.md`).

**Milestone:** Two compute nodes share one S3 bucket, queries return consistent results

---

## Phase 4 — Keystone: Extensions + Compatibility

- [x] Wasmtime integration for sandboxed UDFs (WASM-based user-defined functions) — `CREATE FUNCTION name(args) RETURNS type LANGUAGE wasm AS '<base64>'` (`sql/parser.rs`) registers a WASM module (persisted like table schemas, `storage::UserFunction` under `functions/`), validated against its declared signature at creation time and invoked sandboxed (`executor/udf.rs`: empty `Linker` — zero host imports/I/O, fuel budget, linear-memory cap) from any SQL expression via `eval_function`'s UDF-registry fallback. Scope cut: only `int8`/`float8`/`bool` argument/return types (no `text`/`bytea` — would need a linear-memory + allocator ABI, deferred)
- [x] Full Postgres wire protocol parity (COPY, server-side cursors, LISTEN/NOTIFY) — `COPY table FROM STDIN`/`TO STDOUT` (default text format only, no `WITH (...)` options), `DECLARE`/`FETCH`/`MOVE`/`CLOSE` server-side cursors over the simple query protocol, and `LISTEN`/`NOTIFY`/`UNLISTEN` via an in-process (single-node, not cross-replica) broadcast bus with async `NotificationResponse` delivery
- [x] `pg_catalog` system tables (`\d`, `\dt`, `\di` etc. in psql) — `pg_tables`, `pg_class`, `pg_namespace`, `pg_attribute`, `pg_type`, `pg_indexes`, `pg_index`, `information_schema.tables`/`columns` materialized live from the schema/index catalog (`executor/catalog.rs`), queryable via plain SQL including schema-qualified names (`pg_catalog.pg_tables`)
- [x] Built-in connection pooler (session multiplexing) — redefined for this architecture (every connection already shares one `Arc<Database>`/one LSM engine, so a pgbouncer-style backend-process pool has nothing to pool): a `tokio::sync::Semaphore`-based admission limit (`TPT_MAX_CONNECTIONS`, `main.rs`) that queues connections past the limit instead of erroring, plus a shared, bounded statement cache (`sql/cache.rs::StatementCache`, keyed by raw SQL text, living on `Database` so it's shared across every connection) wired into both the simple and extended query protocol parse paths
- [x] `pg_dump` / `pg_restore` compatibility — scoped to plain-SQL format (`pg_dump --format=plain`; no custom/directory binary archive, no `pg_restore` binary parser). Real `FOREIGN KEY`/`UNIQUE`/`SERIAL` support (not just catalog cosmetics): column-list-aware `INSERT` with working `DEFAULT` evaluation (including `nextval(...)`), `CREATE SEQUENCE`/`nextval`/`currval`/`setval`, `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` shorthand, column- and table-level `UNIQUE`/`FOREIGN KEY` constraints enforced on INSERT/UPDATE (O(n) table scan per check, no index acceleration yet), `pg_catalog.pg_constraint`/`pg_sequence`, a real `ALTER TABLE ... ALTER COLUMN ... SET/DROP DEFAULT|NOT NULL` (previously unparseable — `ALTER TABLE` had no lexer/parser support at all despite Phase 2 claiming it done), `::regclass`/`::regproc`/`::regtype` cast pass-through, and `public.`-qualified DDL/DML object names. Explicit scope cuts: `ON DELETE`/`ON UPDATE` referential actions are parsed but not enforced (no cascade, no delete-time `RESTRICT`); `ALTER TABLE ADD/DROP COLUMN` remain unimplemented no-ops (would need a row-backfill pass); composite/multi-column primary keys still only use the first column as the physical row key (pre-existing engine limitation, unchanged)

**Milestone:** Most psql meta-commands work; existing Postgres client libraries connect
**Milestone verified:** `cargo test` (43 tests) covers pg_catalog/information_schema queries, COPY IN/OUT round-tripping, DECLARE/FETCH/MOVE/CLOSE cursor sequencing, LISTEN/NOTIFY delivery, WASM UDF creation/invocation, the shared statement cache's hit/miss counting, column-list-aware INSERT + defaults, sequences/SERIAL, UNIQUE/FOREIGN KEY enforcement (column- and table-level), `pg_constraint`/`pg_sequence`, `ALTER TABLE ... SET DEFAULT`, `::regclass` pass-through, and `public.`-qualified names — all against an in-process `Database`. Not yet verified: a real `psql` client's actual `\d`/`\dt`/`\di` meta-command queries (which join through `pg_type`/call `format_type()`/`pg_table_is_visible()` — not implemented) — only direct `SELECT`s against the catalog tables are covered. Also not verified: WASM trap behavior (fuel exhaustion / memory-limit exceeded) — in this sandboxed Windows dev environment, wasmtime's OS-level trap handling crashes the test process (`STATUS_STACK_BUFFER_OVERRUN`, confirmed via backtrace to originate inside wasmtime's own `traphandlers` code, not this codebase) instead of returning a catchable error; verify fuel/memory limits by hand on a normal Linux/Windows host before relying on them in production. Also not verified: an actual `pg_dump --format=plain` output file, generated by real Postgres, fed through `psql -f` against a running `cargo run` server — the primitives are implemented and unit-tested, but end-to-end fidelity against real `pg_dump`'s exact verbose output (e.g. `ALTER SEQUENCE ... OWNED BY`, full `ALTER TABLE ONLY` sequences) has not been exercised.

---

## Phase 5 — AI Layer

- [x] **MCP server** — TPT exposes a Model Context Protocol server for AI agents
  - Port: 5433 (alongside Postgres listener on 5432; overridable via `TPT_MCP_ADDR`)
  - Tools: `query(sql)`, `schema()`, `tables()`, `columns(table)`, `explain(sql)`, `mutate(sql)`, `related(table, id)`
  - Auth: TPT token header (`X-TPT-Token`, configured via `TPT_MCP_TOKEN`)
  - Transport: hand-rolled JSON-RPC 2.0 over HTTP (`src/mcp/`) — single request/response per
    connection, no SSE/streaming notifications (documented scope cut)
  - `explain(sql)` returns the parsed statement's structural shape, not a cost-based plan —
    the executor has no EXPLAIN/cost estimation yet
- [x] **Structured retrieval tools** — `related(table, id, max_depth?, limit?)` (`src/mcp/tools.rs`)
  walks the FK graph outward from one row — both the FKs it declares and the FKs other tables
  declare against it, up to 2 hops — and returns compact `{subject, relation, object}` triples
  with human-readable labels (first non-null `text` column, else `pk_col=value`), not raw rows
  or unfiltered joins. Facts are capped (200 total, configurable per-hop row limit) so the agent
  gets bounded, self-describing output regardless of graph size. Scope cut: FK-graph traversal
  only — no similarity/vector traversal exists yet (that's Prism, Phase 7, unbuilt)
- [x] **Schema introspection API** — `schema()` (`src/mcp/tools.rs`) returns, per table: columns
  (name/type/nullable/default/PK), foreign keys, indexed columns, exact row count
  (`SELECT COUNT(*)`), and per-column value-distribution histograms (top-10 buckets via
  `GROUP BY ... ORDER BY COUNT(*) DESC`, skipped for `bytea`/`json` columns and for tables over
  10k rows to keep introspection cheap) — plus a `relationship_graph` of `{nodes, edges}` built
  from every table's FK list, as machine-readable JSON
- [ ] **AI-optimised SDK** — idiomatic clients for Rust, TypeScript, Python
  - Typed query builder (no raw SQL string construction)
  - Schema-aware types generated from live database introspection
  - Batch operations, streaming results, built-in connection pool
  - Deliberately deferred: a multi-language SDK with codegen is a distinct, multi-session effort
    from the MCP-server work above and hasn't been scoped/started yet

**Milestone:** Claude (or any MCP client) can discover schema and query TPT without a Postgres driver

---

## Phase 6 — Meridian: Geospatial Engine

- [x] Custom Rust computational geometry library (replaces GEOS / C++ bindings) — `geo/geometry.rs`: hand-written WKT parse/serialize (`POINT`/`LINESTRING`/`POLYGON`, with `Z`/`M` ordinates for altitude/time), bounding boxes, haversine + planar distance, ray-casting point-in-polygon, bbox intersection. Scope cut: no buffering, no polygon boolean ops (union/difference/intersection-as-geometry), no CRS reprojection, holes not subtracted from polygon point-in-polygon tests
- [x] S2 Geometry hierarchical grid indexing — `geo/s2.rs`: an S2-*inspired* cube-face hierarchical quadtree (real hierarchy — `parent`/`ancestor_at` shrink by exact grid levels — and real locality). Honestly not bit-compatible with Google's S2: linear UV→grid mapping instead of S2's tangent reprojection curve, direct `(face, level, i, j)` id packing instead of Hilbert-curve cell numbering, and cross-cube-face neighbor lookups are a documented gap (a query whose radius crosses a face edge — roughly every 90° of longitude, or near the poles — can under-cover)
- [x] Uber H3 hexagonal grid indexing — `geo/h3.rs`: an H3-*inspired* axial-coordinate hex grid with aperture-2 (not H3's aperture-7) resolution levels and k-ring queries. Honestly not bit-compatible with real H3: flat equirectangular-ish projection (not icosahedral), so distortion grows near the poles. Implemented standalone with its own unit tests; the live spatial index (below) currently uses the S2-inspired grid, not this one — an application could pick either
- [x] 4D spatiotemporal storage model (lat, lon, alt, time as first-class) — `Coord { x, y, z: Option<f64>, t: Option<i64> }` end to end: WKT `POINT(lon lat alt time)` (`Z`=alt, `M`=time — WKT has no native time axis), `ST_MakePoint`/`ST_X`/`ST_Y`/`ST_Z`/`ST_T`, and `storage/geo_index.rs`'s spatial index stores `time` per entry so a radius query can also filter by time range from the same cell lookup (see milestone below)
- [ ] GPU-accelerated spatial joins via wgpu compute shaders — not implemented. Explicit scope cut, not a stub-and-claim-done: a real wgpu compute pipeline needs a GPU-present environment to develop and verify correctness against (this sandbox's wasmtime-trap crash issue, `project_wasmtime_windows_trap_crash.md`, is a preview of how badly "wrote GPU code, never ran it" would go), and is a substantial, separable unit of work from the CPU-side indexing above
- [ ] Raster + vector unified storage model — not implemented, same honesty policy as the GPU item. No raster tile type, no `ST_Value`/`ST_AsRaster`, no unified storage path exists yet
- [~] OGC Simple Features + SQL/MM Spatial compatibility — partial: WKT text in/out and a core `ST_*` function subset (`ST_MakePoint`/`ST_Point`, `ST_GeomFromText`, `ST_AsText`, `ST_X`/`ST_Y`/`ST_Z`/`ST_T`, `ST_Distance`, `ST_DWithin`, `ST_Within`/`ST_Contains`, `ST_Intersects` (bbox-only, not exact polygon/line intersection), `ST_Length`, `ST_Area` (planar shoelace, not geodesic)). Not attempted: WKB (binary) I/O, SRID/`ST_Transform`, the OGC conformance test suite, `GEOGRAPHY` vs `GEOMETRY` type distinction (both parse to the same internal type)

**Milestone (verified for the CPU-side, non-GPU query path):** `CREATE INDEX ON drones USING SPATIAL (pos)` builds a `storage/geo_index.rs` index (S2-inspired cell → row-key buckets, each entry also carrying its point's time). `executor/planner.rs`'s `extract_spatial_predicate` recognizes a top-level `ST_DWithin(pos, ST_MakePoint(...), radius) AND ST_T(pos) BETWEEN t1 AND t2` WHERE clause and answers it with one `Database::spatial_query` call (a handful of cell lookups, not a table scan) instead of falling through to `resolve_table_ref`'s full scan — verified end-to-end in `executor/geo_tests.rs::spatial_index_scan_combines_radius_and_time_range` against an in-process `Database`. Not verified: actual latency at 10M-row scale (no benchmark harness run in this environment — the milestone's "<10ms on 10M rows" figure is unverified, only the query-shape/correctness claim is)

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

- [x] Time-aware append-only storage pages — `storage/ts_index.rs`'s `TimeIndex` buckets rows by fixed time window at the granularity chosen at `CREATE INDEX` time (see partitioning item below); each bucket is an append-only in-memory/on-disk log until it's no longer the newest, at which point it's sealed and compressed (mirrors an LSM memtable → immutable SSTable transition). Scope cut, same as Meridian's spatial index: this is a local, node-only secondary-index accelerator layered on top of the existing row-oriented LSM/SSTable storage (`storage/sstable.rs`), not a rewrite of the base storage format into time-partitioned columnar pages — see `storage/ts_index.rs` module docs
- [x] Gorilla compression (XOR-encoded float deltas) — `storage/compress.rs::gorilla_encode`/`gorilla_decode`, hand-written bit-level XOR-delta codec (leading/trailing zero-run + meaningful-bits encoding, per the original Facebook Gorilla paper), unit-tested for round-trip correctness on constant and slowly-varying series; applied to a `TimeIndex` bucket's value column once the bucket seals
- [x] Delta-of-delta integer compression — `storage/compress.rs::delta_delta_encode`/`delta_delta_decode`, zigzag-varint-encoded second differences, applied to a sealed bucket's timestamp series. Documented limitation: intended for timestamps/slowly-varying counters — integer sequences whose successive deltas swing across most of the `i64` range can overflow the internal arithmetic
- [x] Dictionary encoding for low-cardinality tag columns — `storage/compress.rs::dictionary_encode` (distinct-value list + per-row code), unit-tested; not yet wired into `TimeIndex`/table storage for an actual tag column (no tag-column concept exists elsewhere in the schema yet) — the codec exists and is tested standalone, wiring it into a real column is future work
- [x] Automatic time-based partitioning (hourly / daily / monthly) — `CREATE INDEX ... USING TIME(ts_col) WITH (interval = '1 hour' | '1 day' | '30 days', ...)` (`sql/parser.rs`'s generic `WITH (...)` index-options grammar, `executor/mod.rs::execute_create_index`) sets `TimeIndex`'s fixed bucket granularity, mirroring how Meridian's `CREATE INDEX ... USING SPATIAL` picks an S2 grid level
- [x] Configurable retention + automatic downsampling policies — `WITH (retention = '30 days')` on the same DDL; `TimeIndex::apply_retention` runs synchronously on every insert (no background scheduler — a documented scope cut, see below) and evicts a bucket's raw/compressed series once it falls outside the retention window, keeping only its incrementally-maintained `Rollup` (count/sum/min/max)
- [x] Continuous aggregates — real-time incrementally-updated materialised views — scoped down to the `Rollup` a `TimeIndex` bucket maintains incrementally on every insert (`storage/ts_index.rs`), queryable via `Database::rollup_query` and backing `moving_average()`. Explicit scope cut: no `CREATE MATERIALIZED VIEW` DDL exists anywhere in this engine (a separate, larger effort) — this is a fixed-shape rollup keyed to one indexed timestamp+value column pair, not an arbitrary incrementally-maintained `SELECT ... GROUP BY` view
- [x] SQL time extensions — `time_bucket(interval, ts)` (rounds a unix-ms timestamp down to an interval boundary) and `moving_average(value, window_size)`/`interpolate(value)` (window functions, dispatched alongside `ROW_NUMBER`/`LAG`/`LEAD` in `executor/mod.rs::compute_window_value`) are implemented and tested (`executor/chronos_tests.rs`). `gap_fill()` is an explicit scope cut: it would need to materialize new rows for missing timestamp buckets, which no other function in the window-function pipeline does (every existing window function, and `interpolate()`, computes one value per *existing* row) — not implemented
- Both `time_bucket(...) = const` and plain `BETWEEN` predicates on an indexed timestamp column are planner-rewritten to a `TimeIndex` range scan instead of a full table scan (`executor/planner.rs::extract_time_bucket_predicate`, mirroring Meridian's `extract_spatial_predicate`), verified in `executor/chronos_tests.rs::time_index_scan_answers_range_query`/`time_index_scan_answers_time_bucket_equality`
- No `INTERVAL` type or arithmetic was added — interval literals are hand-parsed text (`'1 hour'`, `'30 days'`, `eval::parse_interval`) into millisecond `i64`s, consistent with timestamps already being plain `Value::Int` (unix ms) rather than a dedicated `Value::Timestamp` variant in this engine
- Retention/downsampling is synchronous (checked on every `TimeIndex::insert` against the newest-seen timestamp), not a background cron-style sweep — deliberately not faking a scheduler that can't be verified in this environment, the same discipline behind Meridian's GPU-compute scope cut

**Milestone:** 1M rows/sec sustained ingest with ≥15:1 compression ratio; query last 30 days in <100ms — **unverified**. No benchmark harness exists in this repo/environment (same honesty precedent as Phases 4/6's unverified milestone numbers); `storage::compress::tests` only assert that Gorilla/delta-of-delta compression is smaller than the naive 8-bytes/value encoding on representative synthetic series, not the specific ≥15:1 ratio or any ingest-rate/query-latency figure at scale.

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
- [ ] Publish versioned, language-independent on-disk format specifications
  (Keystone SSTable/WAL, Chronos, Canopy, Prism index formats) so readers can be
  reimplemented independently of the original Rust codebase
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

---

## Phase 15 — Harbor: Universal Data Migration Platform

- [ ] Core migration engine (Rust, zero-copy bulk transfer, parallel workers, checkpoint/resume on failure)
- [ ] Schema Translator — rule-based AST engine mapping source DDL to optimal TPT engine schemas
- [ ] Verification Engine — xxHash3 per-row/per-column checksums, distribution analysis, query regression testing
- [ ] Migration lifecycle: Discover → Validate (Dry Run) → Snapshot → Replicate (Live CDC) → Verify → Cutover
- [ ] **Harbor/PG** — PostgreSQL → Keystone (pgwire bulk reads + WAL logical replication for live sync; PL/pgSQL → WASM UDFs)
- [ ] **Harbor/Mongo** — MongoDB → Canopy (wire protocol bulk export + oplog for live sync; inferred JSON Schema)
- [ ] **Harbor/Graph** — Neo4j → Plexus (Bolt protocol export, adjacency-list bulk rebuild)
- [ ] **Harbor/TimeSeries** — InfluxDB / TimescaleDB → Chronos (TSM direct read, hypertable introspection, Gorilla compression on import)
- [ ] **Harbor/Stream** — Kafka / RabbitMQ → Flux (consumer group replay from earliest offset, preserves keys/headers/partition order)
- [ ] **Harbor/Vector** — Pinecone / Weaviate / Qdrant → Prism (REST/gRPC bulk export, native quantisation applied during import)
- [ ] **Harbor/GIS** — PostGIS → Meridian (geometry/geography columns, GiST index → S2/H3 spatial partitions, ST_* function mapping)
- [ ] **Harbor/Oracle** — Oracle → Keystone (PL/SQL → WASM transpiler, Oracle type mapping engine, LogMiner CDC, VPD/FGAC → Aegis policy translation, character-set audit)
- [ ] CLI: `tpt-harbor discover / validate / transfer / replicate / verify / cutover`
- [ ] Web dashboard — real-time progress monitoring, validation reports, cutover management (React/TypeScript)

**Milestone:** Zero-downtime migration of a full Postgres production database to Keystone with every row checksum-verified

---

## Phase 16 — Synapse: Agent Orchestration & Memory

- [ ] Agent memory abstraction — short-term (Keystone in-session), long-term (Keystone persistent), episodic (Chronos time-indexed), semantic (Prism vector search)
- [ ] Tool registry and discovery — OpenAPI / JSON Schema tool definitions with semantic search via Prism
- [ ] Multi-agent coordination — delegation, shared state, conflict resolution (Flux-backed task queues)
- [ ] Agent lifecycle management — spawn, pause, checkpoint, terminate; persistent session state across restarts
- [ ] Actor model runtime — Rust + Tokio, one actor per agent, message-passing coordination
- [ ] MCP server integration — agents discover and invoke tools via Phase 5 AI Layer endpoints
- [ ] Memory GC policies — configurable TTL per memory tier (short-term expires, long-term retained, semantic deduplicated)

**Milestone:** A 3-agent workflow completes with shared state; semantic memory persists and is recalled across sessions; tool discovery returns ranked results via Prism

---

## Phase 17 — Mirror: Agent Observability & Debugging

- [ ] Agent action tracing — every decision, tool call, and outcome written to Flux as an immutable, ordered event
- [ ] Session replay engine — replay any past agent session using Flux time-travel queries
- [ ] Debug REPL — step through a past session event-by-event, inspect agent state at each point
- [ ] Performance metrics store — per-agent latency, token usage, success/failure rates stored in Chronos
- [ ] Compliance auditing — policy enforcement log, tamper-evident audit trail in Keystone
- [ ] Provenance tracking on stored data (not just agent actions) — every fact
  carries who/what asserted it and when, so consumers (human or AI) can weight
  reliability without a human sanity-check in the loop
- [ ] OTel span integration — agent action spans annotate existing distributed traces from Phase 12
- [ ] Dashboard — live agent activity monitor, per-agent performance charts, replay controls

**Milestone:** Replay a failed agent session end-to-end, trace root cause to the exact tool call; auto-generate a compliance audit report for any session

---

*All engines + SDKs: Apache 2.0 licensed. Built in Rust. Cloud-native from day one.*