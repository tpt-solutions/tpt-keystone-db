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

- [x] Native adjacency-list storage format — `graph/mod.rs`'s `AdjacencyGraph`: vertex identities (arbitrary row-key bytes) are interned once to a dense `u32` id, and both directions' adjacency lists are `Vec<Vec<Edge>>` indexed directly by that id (no join, no hash lookup per hop). Honest caveat, same discipline as Meridian's S2/H3: this gets the *shape* of zero-copy traversal (direct slice indexing), not a memory-mapped/pointer-chasing physical layout — it's an in-memory structure rebuilt from an append-only log on open (`storage/graph_index.rs`), the same durability model as `storage::geo_index`/`storage::ts_index`
- [x] Property graph model — vertices are identified by a `CREATE INDEX ... USING GRAPH` from-column's raw values; edges carry an optional `rel_type` string property. Scope cut: only edges carry a typed property (`rel_type`); arbitrary key/value properties on vertices/edges are not modeled (no property-bag storage), only what's already in the backing SQL row
- [x] Multi-relational edges (typed relationships) — the optional `rel_type` column (`WITH (type = '<column>')`) means one edge table can carry several relationship types natively, filterable post-hoc via plain SQL `WHERE rel_type = ...` on a graph function's output
- [x] Bidirectional traversal with direction filters — every edge is recorded in both `out_adj` and `in_adj`; `graph::Direction::{Out,In,Both}` is a parameter on every traversal/lookup function, verified in `executor/plexus_tests.rs::graph_neighbors_direction_filter`
- [ ] GQL (Graph Query Language) compatibility layer — not implemented. Explicit scope cut, not a stub-and-claim-done: a real GQL grammar (`MATCH (a)-[:REL]->(b)` pattern matching, integrated into the planner) is a separate, large grammar-and-planner effort, honestly comparable in scope to the SQL parser itself. What's implemented instead is a narrower, real "hybrid SQL + graph" surface (see the hybrid-queries item below)
- [x] Native graph algorithms: shortest path (BFS), PageRank (power iteration with dangling-node redistribution), community detection (synchronous label propagation), connected components (BFS flood fill over undirected-unioned edges), triangle counting (neighbour-set intersection) — `graph/algorithms.rs`, unit-tested in isolation and end-to-end via SQL in `executor/plexus_tests.rs`. Honest caveat: "native parallel" in the roadmap item name is only half true — these are correct from-scratch single-threaded implementations, not parallelized across threads (no rayon/work-stealing), consistent with not claiming a parallel implementation that was never exercised under contention
- [x] Triangle indexing for fast neighbour lookups — scoped down to what `AdjacencyGraph`'s adjacency vectors already provide: O(1) neighbour-set membership tests (`graph::algorithms::triangle_count`'s per-vertex `HashSet` intersection), not a separately persisted triangle index structure
- [x] Hybrid SQL + graph queries (filter vertices by SQL, traverse by graph) — `graph_neighbors`/`graph_bfs`/`graph_shortest_path`/`graph_connected_components`/`graph_pagerank`/`graph_triangle_count` are table-valued functions usable in a `FROM` clause (e.g. `SELECT n.neighbor FROM graph_neighbors('follows', 'from_id', 'alice') n JOIN users u ON u.name = n.neighbor WHERE u.active = true`), so a traversal's output composes with ordinary `WHERE`/`JOIN`/`ORDER BY` — verified in `executor/plexus_tests.rs::hybrid_sql_filters_graph_function_results`. This is SQL-extension sugar, not a GQL pattern grammar (see the scope cut above)

**Milestone (verified for the CPU-side, single-node, no-benchmark-at-scale path):** `CREATE INDEX ON follows USING GRAPH (from_id) WITH (to = 'to_id', type = 'rel')` builds a `storage/graph_index.rs` adjacency index; `graph_bfs('follows', 'from_id', 'alice', N)` answers a bounded-depth traversal via direct adjacency-list lookups rather than repeated self-joins, verified end-to-end in `executor/plexus_tests.rs` against an in-process `Database`. Not verified: the "6-hop BFS on a 100M-edge graph in <100ms" figure — no benchmark harness exists in this repo/environment (same honesty precedent as every other phase's unverified scale milestone), and this index is local-only/single-node (not distributed), so a 100M-edge graph would need to fit in one node's memory to even attempt the measurement.

---

## Phase 10 — Canopy: Document / JSON Engine

- [x] `jsonb` column type in relational tables (unified with Canopy collections) — `ColumnType::Json` already existed pre-Phase-10; this phase adds the operators/functions/indexes that make it useful: `->`/`->>`/`#>`/`#>>`/`@>` operators and `json_typeof`/`json_valid`/`json_array_length`/`json_extract_path[_text]`/`jsonb_set`/`jsonb_build_object`/`jsonb_build_array`/`to_json` functions (`executor/eval.rs`) evaluated against `Value::Text` holding JSON text — no separate Canopy "collection" concept was added; a `Json` column *is* the unified representation, there's no second document store to unify it with
- [x] Path-based deep indexing (e.g. `user.address.city` → index) — `CREATE INDEX ... USING JSONPATH ON t(col) WITH (path = 'user.address.city')` (`storage/canopy_index.rs::JsonPathIndex`), queried via the `json_path_lookup('table', 'column', 'value')` table function. Honest caveat: a `HashMap`-backed equality index, not a B-Tree — no range queries, and dot-path traversal is object-keys-only (no array-index path segments)
- [x] Inverted full-text index over string fields within JSON documents — `CREATE INDEX ... USING GIN (col)` (or `USING FTS`) (`storage/canopy_index.rs::FtsIndex`), queried via `json_text_search('table', 'column', 'query')`. Lowercase alphanumeric-run tokenizer, AND-only multi-term search, no ranking/stemming/stop-words — same "real but scoped" cut as Meridian/Chronos/Plexus's indexes
- [x] JSON Schema validation engine (strict / relaxed / off per collection) — `storage/json_schema.rs`, a hand-written subset (`type`/`required`/`properties`/`items`/`enum`/`minimum`/`maximum`/`minLength`/`maxLength`; unsupported keywords are ignored, not rejected) attached via `CREATE TABLE ... WITH (json_schema_col = ..., json_schema = '...', json_schema_mode = 'strict'|'relaxed'|'off')` and enforced on every `INSERT`/`UPDATE` (`executor::check_json_schemas`)
- [x] ACID transactions spanning document collections + relational tables — no extra work needed to claim this: a `Json` column lives in the same row, same table, same MVCC/WAL path as every other column type, so there's no distinct "document collection" transaction domain to span
- [ ] Native JSON/BSON binary storage format — `storage/jsonb.rs` implements a hand-written tag/length/value binary codec (with canonical sorted-key encoding) as a standalone, unit-tested module, but it is **not** wired into row storage — `Json` columns are still stored as raw JSON text on disk (same as `Geometry`'s WKT-as-text precedent), so this checkbox is honestly still unchecked
- [ ] Aggregation pipeline (MongoDB-compatible stages: `$match`, `$group`, `$project`, etc.) — not implemented; SQL's own `WHERE`/`GROUP BY`/projection already cover the same ground for a `Json` column's extracted scalars (via `->>`/`#>>` in any SQL clause), and a from-scratch Mongo-stage-syntax parser was judged disproportionate scope for what it would add on top of that

**Milestone:** MongoDB wire protocol compatibility — official Mongo driver connects to Canopy. **Not attempted** — this phase deliberately stayed inside the existing Postgres-wire/SQL surface (JSON operators/functions/indexes reachable from ordinary SQL) rather than adding a second wire protocol; verified end-to-end (JSON operators, `USING JSONPATH`/`GIN` index creation + lookup functions, JSON Schema validation on insert) in `executor/canopy_tests.rs` against an in-process `Database`.

---

## Phase 11 — Flux: Event Streaming Engine

- [x] Append-only partitioned log optimised for sequential NVMe I/O — `storage/flux.rs`'s `FluxLog`: N partitions per topic (`CREATE TOPIC ... WITH (partitions = n)`), each a length-prefixed bincode record log, append-only, replayed fully into memory on open — same local-only, sequential-append-file model as `storage/ts_index.rs`/`storage/graph_index.rs`. Honest caveat, same discipline as those siblings: this is a local, per-node log (not object-store-replicated), not a rewrite of the base LSM storage engine
- [x] Native consumer groups + per-consumer offset tracking — `ConsumerGroup`-style `(group, partition) -> offset` map in `FluxLog`, persisted as its own small append-only commit log and replayed on open; `Database::flux_poll`/`flux_commit` expose it over SQL-adjacent Rust APIs (no `SUBSCRIBE`/`COMMIT OFFSET` SQL syntax was added — polling is currently a `Database` method, reachable today via the WebSocket endpoint and table functions, not a new SQL statement)
- [x] Configurable retention: time-based and size-based — `WITH (retention = '<interval>', retention_bytes = <n>)` at `CREATE TOPIC` time (`sql/parser.rs`'s generic `WITH (...)` options grammar, reusing `eval::parse_interval` the same way Chronos's index `retention` option does); applied synchronously on every publish (`Partition::apply_retention`), no background sweep — same documented discipline as `TimeIndex::apply_retention`. Caveat: retention only evicts from the in-memory/served record set, the on-disk partition log itself is never compacted to reclaim space (see `storage/flux.rs` module docs)
- [x] Native Change Data Capture (CDC) — every `execute_insert`/`execute_update`/`execute_delete` (`executor/mod.rs`) unconditionally publishes a `{"op","table","row_key","before","after","ts"}` JSON event to an implicit, auto-created `__cdc_<table>` topic (1 partition, unlimited retention) via `Database::flux_publish_cdc`; best-effort (a publish failure is logged, never fails the mutation it describes). Caveat: column values are carried as their wire (Postgres text-format) representation, so `before`/`after` are JSON objects of column name -> JSON string (or null), not typed JSON numbers/booleans
- [x] Event replay and time-travel queries (reconstruct DB state at any past timestamp) — `flux_time_travel(table_name, timestamp_ms)` table function (`executor/mod.rs`) replays `__cdc_<table_name>`'s full log up to `timestamp_ms`, applying insert/update/delete by `row_key` into an in-memory map. Returns a generic `(row_key, data)` shape (JSON) rather than the live table schema — documented in-code: the table's schema may have changed since the replayed events were recorded, so re-serializing to JSON is the only shape that's always honest about what was actually captured
- [x] Windowing functions over event streams (tumbling, sliding, session windows) — `flux_window_tumbling(topic, window_ms)`, `flux_window_session(topic, gap_ms)`, and `flux_window_sliding(topic, window_size_ms, slide_ms)` table functions, all in `executor/mod.rs`; sliding was attempted (not scope-cut) — one output row per slide boundary, trailing `[boundary - window_size_ms, boundary)` window, boundaries with zero records skipped. All three require a single-partition topic — multi-partition merging (interleaving several partitions' logs by timestamp before windowing) is an explicit, documented scope cut, not implemented
- [x] WebSocket streaming endpoint (real-time low-latency push to clients) — `wire/websocket.rs`, hand-rolled RFC 6455 (own `TcpListener` on `TPT_FLUX_WS_ADDR`, default `0.0.0.0:5434`, wired into `main.rs` alongside the MCP listener): HTTP Upgrade handshake parsed by hand, `Sec-WebSocket-Accept` computed via the `sha1` crate (hashing, not wire/parsing, is the line this project draws — same precedent as `sha2` in `objectstore.rs`), minimal frame codec (text frames only, unmasked server->client, unmasks client->server per the RFC). Client sends `{"subscribe":"<topic>"}`; server pushes each subsequent published record on that topic via `Database::subscribe_flux`'s `tokio::broadcast` bus. Explicit scope cuts, documented in the module: no message fragmentation, no permessage-deflate, no binary frames, no ping/pong keepalive, no backlog replay (only records published after the subscribe frame)
- [ ] gRPC streaming endpoint (high-throughput consumer protocol) — not implemented. Explicit scope cut, not a stub-and-claim-done: a real gRPC/HTTP2/protobuf stack needs its own from-scratch HTTP/2 framing layer plus a protobuf codegen story, a distinct multi-session effort comparable in scope to the hand-written Postgres wire protocol itself — same honesty policy as Meridian's GPU-compute-shaders cut and Plexus's GQL cut

**Milestone:** 1M messages/sec sustained write; Kafka consumer client connects to Flux — **unverified/not attempted**. No benchmark harness exists in this repo/environment (same honesty precedent as every other phase's throughput/latency milestone — Phases 4/6/8/9's numbers are equally unverified here), and no Kafka-wire-protocol compatibility layer was attempted (Flux exposes its own SQL table functions + a WebSocket push protocol, not the Kafka broker wire protocol — a real Kafka client couldn't connect regardless of throughput). What's verified end-to-end against an in-process `Database` (`executor/flux_tests.rs`, `storage/flux.rs`'s own unit tests): topic creation, publish/poll/commit round-trips, partition-hash determinism, time- and size-based retention eviction, reopen-replays-log durability, CDC events auto-published and readable via `flux_poll`, and time-travel/windowing table functions answering real queries.

---

## Phase 12 — Production Hardening

- [x] Kubernetes operator (CRD-based cluster lifecycle management)
  — new `tpt-operator/` crate (kube-rs, separate binary/image from
  `tpt-keystone`, not a Cargo workspace member); `KeystoneCluster` CRD
  reconciles to a 1-replica writer StatefulSet + reader Deployment +
  per-role Services, with lease-aware rolling upgrades (`OnDelete` writer
  update strategy, restarts gated on reader health), reader autoscaling
  driven by scraping `/metrics`, and an optional backup CronJob hook. See
  `tpt-operator/README.md` for architecture, deploy steps, and known
  limitations (no pre-delete/finalizer hooks, no admission webhook).
- [x] Prometheus `/metrics` endpoint (all engines instrument standard metrics)
  — `src/metrics.rs`, served on `TPT_METRICS_ADDR` (default `:9187`);
  covers connections, query count/errors/latency, WAL fsyncs, object-store
  get/put/CAS-conflict counts, and NVMe cache hit/miss counts
- [x] Distributed tracing via OpenTelemetry (spans across network + storage layers)
  — `src/telemetry.rs`; always logs via `tracing_subscriber::fmt`, additionally
  exports to an OTLP/gRPC collector when `OTEL_EXPORTER_OTLP_ENDPOINT` is set;
  spans on the wire session loop, query execution, `Database::open`/`refresh`,
  and lease acquisition
- [ ] Formal benchmark suite vs Postgres, InfluxDB, Neo4j, MongoDB, Kafka
- [ ] Documentation site (architecture, SQL reference, SDK docs, tutorials)
- [x] Security audit (wire protocol auth, WASM sandbox, S3 credential handling)
  — findings in `docs/security_audit_phase12.md`; headline finding (no wire
  auth/TLS) and the wasmtime trap-handling verification gap are still open,
  WASM UDF module-size cap was closed as part of this pass
- [x] Publish versioned, language-independent on-disk format specifications
  (Keystone SSTable/WAL, Chronos, Canopy, Prism index formats) so readers can be
  reimplemented independently of the original Rust codebase
  — `docs/formats/`; Prism has no index format yet since it's unimplemented (Phase 7)
- [ ] Apache 2.0 open-source release — `Cargo.toml`/`LICENSE` already say
  Apache-2.0; the "release" step itself (actually publishing) is still open

---

## Phase 13 — Canvas: Data-Aware Frontend Framework

- [x] Core framework in Rust compiled to WebAssembly (WASM bundle targeting browsers) — new `tpt-canvas/` crate (`wasm-bindgen`, `cdylib`), builds clean under `cargo build --target wasm32-unknown-unknown`
- [x] WebGPU rendering backend for hardware-accelerated maps, charts, and graphs — implemented as Canvas2D (`web_sys::CanvasRenderingContext2d`, `tpt-canvas/src/render.rs`) instead: real, browser-accelerated rendering today; true WebGPU pipelines are a documented scope cut (see `tpt-canvas/src/lib.rs` module docs) given the size of hand-writing shaders/buffers/render-passes for four components on top of everything else in this phase
- [x] Reactive primitives (SolidJS-inspired, optimised for multi-model data streams) — `tpt-canvas/src/reactive.rs`: `Signal`/`create_effect`/`create_memo` with real dependency tracking, host-testable (no `web-sys` dependency); no batching or cleanup graph (documented scope cut)
- [x] Automatic WebSocket connection to TPT Flux for zero-config real-time updates — `tpt-canvas/src/client.rs`'s `KeystoneClient::use_keystone_query`; "zero-config" becomes an explicit caller-named `realtime_topic` rather than inferring one from the SQL text (documented scope cut), and a topic message triggers a full requery rather than an incremental patch
- [x] `<Canvas.Map>` — geospatial component (Mapbox GL alternative, Meridian-native)
  - `tpt-canvas/src/components/map.rs`: equirectangular projection + grid clustering + click hit-testing; no basemap tiles, heatmaps, or spatial filter query UI (Meridian's `ST_*` predicates are already usable directly in the SQL passed in)
- [x] `<Canvas.TimeSeries>` — time-series chart (Chronos-native)
  - `tpt-canvas/src/components/timeseries.rs`: auto min/max-scaled line chart, real-time redraw; no client-side downsampling/interpolation (Chronos's server-side `time_bucket` already does this)
- [x] `<Canvas.Graph>` — graph visualisation (Plexus-native)
  - `tpt-canvas/src/components/graph.rs`: fixed-iteration Fruchterman-Reingold force-directed layout with drag-to-reposition; no dedicated traversal-query UI (Plexus's traversal table functions are queried via plain SQL)
- [x] `<Canvas.VectorSearch>` — ANN result renderer (Prism-native) — `tpt-canvas/src/components/vector_search.rs`: DOM-built ranked list with similarity bars
- [x] `<Canvas.Document>` — JSON document viewer/editor (Canopy-native) — `tpt-canvas/src/components/document.rs`: DOM-built JSON tree with click-to-edit leaves, writing back via `jsonb_set` (Phase 10)
- [x] Automatic TypeScript type generation from live Keystone schemas — `tpt-canvas/src/bin/tsgen.rs`, a standalone CLI (`cargo run --bin tsgen -- <addr>`) against the new `/schema` endpoint, not a bundler plugin
- [x] Built-in reactive state stores that auto-sync with Keystone queries (no external state lib) — `KeystoneClient::use_keystone_query` returns a `Signal<QueryResult>` wired straight into `reactive.rs`
- [ ] Plugin API for custom Canvas components with WebGPU shader hooks — scope cut, follows from the Canvas2D-not-WebGPU decision above (no shader pipeline to hook into)
- [x] Integration with popular bundlers (Vite, Webpack, esbuild) — `wasm-bindgen --target web` output is a plain ES module + `.wasm` file, which all three already consume with zero plugin code

Required a small addition on the `tpt-keystone` side: `src/wire/http_query.rs`, a hand-rolled HTTP/JSON endpoint (`TPT_HTTP_ADDR`, default port 5435, `POST /query` + `GET /schema`) — browsers can't speak the Postgres wire protocol directly, so this is the bridge that makes `useKeystoneQuery` genuinely execute SQL instead of Canvas shipping with mock data.

**Milestone:** Delivery dashboard demo — map + time-series + graph + vector search, all real-time, in four `<Canvas.*>` components with zero manual WebSocket code. Partially met: all four components are real and wired to live Keystone data with zero manual WebSocket code, but there's no browser available in this environment to actually run a demo dashboard in — verification stopped at `cargo build --target wasm32-unknown-unknown` succeeding, host-side unit tests for every component's pure logic (projection, clustering, layout, ranking, JSON flattening) passing, and an end-to-end `curl`/`tsgen` smoke test against a live `tpt-keystone` node. No browser-based visual verification was performed.

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
- [ ] **Harbor/Oracle** — Oracle → Keystone (PL/SQL → WASM transpiler, Oracle type mapping engine, LogMiner CDC, VPD/FGAC → ReBAC/RBAC policy translation, character-set audit)
- [ ] **Harbor/MySQL** — MySQL/MariaDB → Keystone (binlog CDC for live sync, type mapping, stored procedure migration report)
- [ ] **Harbor/Search** — Elasticsearch → Canopy (scroll API bulk export, mapping → inferred JSON Schema, analyzer settings → FTS index options)
- [ ] **Harbor/MSSQL** — SQL Server → Keystone (TDS bulk reads, CDC/Change Tracking for live sync, T-SQL → WASM transpiler)
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