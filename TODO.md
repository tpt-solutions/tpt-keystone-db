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
- [x] LSM-tree compaction — implemented as a full (size-tiered, not true
  multi-level) compaction rather than real per-level LSM leveling: once the
  live SSTable count reaches a threshold (`TPT_COMPACTION_SSTABLE_THRESHOLD`,
  default 4), `LsmEngine::compact_all` (`storage/lsm.rs`) merges every current
  SSTable into one, dropping any key shadowed by a newer table's write or
  tombstone, and commits the new single-SSTable set via the same manifest CAS
  flush already uses. Fixed a real latent bug found while building this:
  `SSTable::scan()` used to walk only the data section, which never contains
  tombstone bytes at all (`build_bytes` only records a tombstone in the
  index), so a tombstone in a newer SSTable was invisible to any multi-table
  scan and could never shadow an older table's live value — `scan()` now
  walks the index (`Option<Vec<u8>>`, `None` = tombstone) and both
  `LsmEngine::scan()` and `compact_all` merge correctly across tables.
  Reader nodes' `refresh()` also used to only ever *add* SSTables listed in
  a newer manifest, never drop ones no longer listed — meaning a reader's
  own SSTable list would keep growing across writer-side compactions,
  defeating the point for anyone but the writer; fixed to retain only what
  the current manifest lists. Compacted-away objects are deleted from the
  store best-effort (a delete failure just orphans the object rather than
  failing the compaction that already committed — no GC grace period/delay,
  a documented scope cut given this is a single-writer model). Verified in
  `storage/lsm.rs`'s own tests: multi-flush compaction bounds the SSTable
  list and preserves the latest value per key, tombstones survive a
  compaction and disappear from `scan()`, and a reader's `refresh()` drops
  SSTables the writer compacted away while still answering `read()`
  correctly for both surviving keys. Not attempted: true per-level LSM
  leveling (separate L0/L1/... tiers with per-level size ratios) — this is a
  single global merge, cheaper to reason about correctness-wise but doesn't
  bound compaction I/O cost the way real leveling does at very large data
  sizes.
- [x] MVCC (Multi-Version Concurrency Control)
- [x] Transaction manager (BEGIN / COMMIT / ROLLBACK)
- [x] B-Tree indexes for primary keys + secondary indexes
- [x] io_uring async I/O integration (Linux NVMe path) — `storage/io_backend.rs` puts the WAL's
  append+fsync behind a `WalIo` trait with two backends: `StdWalIo` (portable `std::fs`, the default
  everywhere and the only one this Windows dev host runs) and `linux_uring::UringWalIo`
  (`#[cfg(target_os = "linux")]`, a real `io_uring` crate Write+Fsync SQE pair per append), selected at
  runtime only on Linux with `TPT_IO_URING=1` (falls back to std if ring creation fails, e.g. an old
  kernel). Honest caveat: the `io_uring` path is written against the crate's documented
  submission/completion contract but **has not been compiled or run** in this environment — there's no
  Linux host available here, so `#[cfg(target_os = "linux")]` compiles it out entirely. `cargo test`
  here only exercises `StdWalIo` (`storage/io_backend.rs::tests`, behaviourally identical to the
  pre-existing `Wal` code it replaced). Same "written against the contract, not exercised against the
  real backend in this environment" caveat as `S3ObjectStore`.

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
- [x] **AI-optimised SDK** — idiomatic clients for Rust, TypeScript, Python. Scoped to what's
  actually additive on top of the Phase 14 SDKs already built (idiomatic clients, batch operations,
  streaming results, and connection pooling already exist per-language there — `streamQuery`, `Pool`,
  `blocking::Client`, etc.) rather than a fourth parallel SDK family:
  - Typed query builder (no raw SQL string construction) — `tpt-sdk/src/query_builder.rs`'s
    `Table` trait + `QueryBuilder<T>` (Rust); `packages/sdk-web/src/query-builder.ts` and
    `packages/sdk-server/src/query-builder.ts`'s `TableDef<Row>` + `TypedQueryBuilder<Row>`
    (TypeScript — each package gets its own copy, no workspace link between them, same "duplicate the
    hand-written protocol code per package" precedent every wire client here already follows);
    `sdk-python/tpt_sdk/query_builder.py`'s `TableDef`/`QueryBuilder` (Python). All three: chainable
    `.select()`/`.filter_eq()`(`.whereEq()` in TS)/`.order_by()`/`.limit()`/`.offset()` building a
    parameterized `(sql, params)` pair — pure sugar over each SDK's existing `query_params`/
    `queryParams`/`query_params` method, never a replacement for raw SQL.
  - Schema-aware types generated from live database introspection — `tpt-sdk/src/bin/typegen.rs`
    (`cargo run --bin tpt-sdk-typegen -- host:port`), `packages/sdk-web/src/bin/typegen.ts` (extended
    this pass to also emit a `TableDef` const per generated `interface`, not just the interface
    itself), `sdk-python/tpt_sdk/typegen.py` (`python -m tpt_sdk.typegen host:port`) — each reads
    `information_schema.columns` (or `GET /schema` for the browser-facing TS path) from a live node
    and emits one typed struct/interface/dataclass + one query-builder table definition per table.
  - Not ported to `@tpt/sdk-edge` (would be a near-identical copy of sdk-web's version, since both
    talk the same HTTP bridge) — a documented follow-up, not attempted this pass.
  - Verified via each language's own test suite: `cargo test` (3 new `tpt-sdk` unit tests, plus the
    existing `tpt-keystone` suite unaffected), `npm test` (4 new tests in `@tpt/sdk-web`, 3 in
    `@tpt/sdk-server`, both via a clean `tsc` build), `pytest` (4 new tests in `sdk-python`) — all
    against builder logic (SQL string/param construction) with a fake client, not a live server
    round trip (no live instance running in this environment for this pass).

**Milestone:** Claude (or any MCP client) can discover schema and query TPT without a Postgres driver

---

## Phase 6 — Meridian: Geospatial Engine

- [x] Custom Rust computational geometry library (replaces GEOS / C++ bindings) — `geo/geometry.rs`: hand-written WKT parse/serialize (`POINT`/`LINESTRING`/`POLYGON`, with `Z`/`M` ordinates for altitude/time), bounding boxes, haversine + planar distance, ray-casting point-in-polygon, bbox intersection. Scope cut: no buffering, no polygon boolean ops (union/difference/intersection-as-geometry), no CRS reprojection, holes not subtracted from polygon point-in-polygon tests
- [x] S2 Geometry hierarchical grid indexing — `geo/s2.rs`: an S2-*inspired* cube-face hierarchical quadtree (real hierarchy — `parent`/`ancestor_at` shrink by exact grid levels — and real locality). Honestly not bit-compatible with Google's S2: linear UV→grid mapping instead of S2's tangent reprojection curve, direct `(face, level, i, j)` id packing instead of Hilbert-curve cell numbering, and cross-cube-face neighbor lookups are a documented gap (a query whose radius crosses a face edge — roughly every 90° of longitude, or near the poles — can under-cover)
- [x] Uber H3 hexagonal grid indexing — `geo/h3.rs`: an H3-*inspired* axial-coordinate hex grid with aperture-2 (not H3's aperture-7) resolution levels and k-ring queries. Honestly not bit-compatible with real H3: flat equirectangular-ish projection (not icosahedral), so distortion grows near the poles. Implemented standalone with its own unit tests; the live spatial index (below) currently uses the S2-inspired grid, not this one — an application could pick either
- [x] 4D spatiotemporal storage model (lat, lon, alt, time as first-class) — `Coord { x, y, z: Option<f64>, t: Option<i64> }` end to end: WKT `POINT(lon lat alt time)` (`Z`=alt, `M`=time — WKT has no native time axis), `ST_MakePoint`/`ST_X`/`ST_Y`/`ST_Z`/`ST_T`, and `storage/geo_index.rs`'s spatial index stores `time` per entry so a radius query can also filter by time range from the same cell lookup (see milestone below)
- [x] GPU-accelerated spatial joins via wgpu compute shaders — implemented once a real GPU became available to develop/verify against (previously an explicit scope cut, not a stub-and-claim-done, per the prior note about untested native/GPU code paths). `geo/gpu.rs` + `geo/shaders/spatial_join.wgsl`: two WGSL compute kernels — `bbox_overlap` (bbox-vs-bbox closed-interval overlap, exactly matching the CPU path's existing `bbox_intersects` semantics) and `dwithin` (bbox-centroid-vs-point haversine radius test) — wired into `executor/mod.rs::apply_join`'s nested-loop fallback (`executor/planner.rs::extract_spatial_join_predicate`) for `ST_Intersects`/`ST_DWithin` join predicates once `left_rows.len() * right_rows.len()` exceeds `TPT_GPU_JOIN_THRESHOLD` (default 1,000,000, unbenchmarked at scale — see milestone note below). Scope actually covered: broad-phase only (this is not a new precision limitation — `ST_Intersects` was already bbox-only on CPU); `f32` GPU coordinate precision (narrower than the CPU path's `f64`, an accepted broad-phase tradeoff); a single top-level spatial predicate per join (no compound `a.id = b.id AND ST_Intersects(...)` in v1 — falls back to the nested-loop path unchanged); always fails safe to the existing CPU nested-loop join — GPU unavailability, no adapter, device-creation failure, `TPT_DISABLE_GPU_JOIN`, an oversized batch (`TPT_GPU_JOIN_MAX_PAIRS`), or a runtime device error all fall back rather than erroring the query. Verified via `cargo run --example gpu_smoke_test` (isolated OS-process smoke test, run manually before any executor wiring) and `executor/gpu_join_tests.rs` (GPU vs. CPU row-set agreement for both predicates) against an NVIDIA RTX 3050 (Vulkan backend) in this dev environment — not verified on other vendors/drivers (AMD/Intel/Linux) or in a headless CI environment
- [x] Raster + vector unified storage model — `geo/raster.rs`'s `Raster`: a single-band `f64` grid, georeferenced by an upper-left origin + per-axis pixel scale + SRID (PostGIS's `ST_UpperLeftX`/`Y`/`ST_ScaleX`/`Y`/`ST_SRID` fields), stored the same way `Geometry` is — a hand-rolled header+row-major-`f64` binary encoding, hex-encoded and surfaced as `Value::Text` (`ColumnType::Raster`, wired through `storage::ColumnType`/`executor::catalog`'s `type_name`/`pg_type` the same 3 places `Geometry`/`Geography` are, per `from_name("raster")`). "Unified" means `ST_AsRaster` rasterizes a `Geometry` (point/linestring/polygon) using the *existing* `point_in_polygon` test — not a separate raster subsystem with its own geometry model. New functions: `ST_MakeEmptyRaster`, `ST_Value`, `ST_SetValue`, `ST_AsRaster`, `ST_Width`, `ST_Height` (`executor/eval.rs`). Honest scope cuts: single band only (no multi-band/RGB, no pixel-type variety — every cell is `f64`), no image-format import (no GeoTIFF/PNG loading), no raster algebra (`ST_MapAlgebra`), and the binary encoding is hand-rolled — not PostGIS's actual WKB-raster wire format, same "approximates the idea, not bit-compatible" caveat as `geo::s2`/`geo::h3`. Verified: `geo::raster::tests` (7 tests — hex round-trip, value/set-value bounds and error, bad-magic rejection, rasterizing a fully-covering square, a triangle with genuinely-outside/-inside cells checked individually to avoid a hypotenuse-boundary false pass, and a single-point raster) plus `executor::geo_tests` SQL-level coverage (catalog type reporting, `ST_MakeEmptyRaster`+`ST_Value`/`ST_Width`/`ST_Height`, `ST_SetValue` round-tripped through a real `INSERT`, `ST_AsRaster` rasterizing a polygon).
- [x] OGC Simple Features + SQL/MM Spatial compatibility — WKT text in/out, a core `ST_*` function subset (`ST_MakePoint`/`ST_Point`, `ST_GeomFromText`, `ST_AsText`, `ST_X`/`ST_Y`/`ST_Z`/`ST_T`, `ST_Distance`, `ST_DWithin`, `ST_Within`/`ST_Contains`, `ST_Intersects` (bbox-only, not exact polygon/line intersection), `ST_Length`, `ST_Area` (planar shoelace, not geodesic)), EWKB/WKB binary I/O (`ST_AsBinary`/`ST_AsEWKB`/`ST_GeomFromWKB`/`ST_GeomFromEWKB`, `geo/geometry.rs`'s `to_wkb_hex`/`from_wkb_hex` — little-endian ISO WKB plus PostGIS's EWKB Z/M/SRID type-word flag bits; surfaced as lowercase hex text since there's no dedicated binary `Value` variant, same "reuse `Value::Text`" precedent as WKT), SRID support via EWKT (`SRID=<n>;<WKT>`, `strip_srid_prefix`/`Geometry::from_ewkt`/`to_ewkt`, `ST_SRID`/`ST_SetSRID`/`ST_AsEWKT`, and a 2-arg `ST_GeomFromText(wkt, srid)` form), `ST_Transform` (`geo::geometry::transform_geometry`), and a `GEOGRAPHY` vs `GEOMETRY` column-type distinction (`storage::ColumnType::Geography`, separate catalog/`\d`/`information_schema` reporting from `Geometry` — still the same WKT-as-text storage and haversine-based distance/area functions under the hood, so the split is catalog-only, not a different evaluation engine yet). `ST_Transform` honestly only reprojects the EPSG:4326<->3857 pair (closed-form Web Mercator formulas) — any other SRID pair errors rather than silently no-op'ing, since a general CRS/PROJ-equivalent library is out of scope. This pass added the OGC conformance test suite and, alongside it, the handful of standard OGC scalar functions that were conspicuously missing and cheap to add given the existing `Geometry`/`bbox()` model: `ST_GeometryType`, `ST_Dimension`, `ST_IsEmpty` (plus `POLYGON EMPTY`/`LINESTRING EMPTY` WKT parsing/rendering support, added because `ST_IsEmpty` needs a way to construct/round-trip an empty geometry to test against — `POINT EMPTY` is a documented remaining gap, since `Geometry::Point(Coord)` has no empty representation), `ST_Envelope`, and `ST_Equals` (exact coordinate-order equality, not full OGC spatial equivalence — a documented simplification, same discipline as `ST_Intersects`'s bbox-only precision). `executor/ogc_conformance_tests.rs`: modeled on the OGC 05-134 (SFA — SQL Option) Annex A conformance suite's function coverage and expected-result style — this engine has neither the official suite's `geometry_columns` catalog view nor its seven-table worked-example dataset, so this is a from-scratch equivalent, not a verbatim port; the file's own header comment lists every OGC-required function still out of scope (`ST_IsSimple`, `ST_Boundary`, `ST_StartPoint`/`ST_EndPoint`/`ST_IsClosed`, `ST_NumPoints`/`ST_PointN`, `ST_Centroid`, ring/multi-geometry accessors, `ST_Disjoint`/`ST_Touches`/`ST_Overlaps`/`ST_Crosses`/`ST_Relate`, and every boolean-set-operation function) rather than silently treating the suite as complete. Verified: `geo::geometry::tests` unit-tests WKB round-trips (point/polygon, with/without SRID and Z), EWKT round-trips, and the 4326<->3857 transform against London's known Web Mercator coordinates; `executor::geo_tests` adds SQL-level coverage of `ST_GeomFromText(..., srid)`/`ST_SRID`/`ST_SetSRID`/`ST_AsEWKT`/`ST_Transform`/`ST_AsBinary`+`ST_GeomFromWKB`/`ST_AsEWKB`+`ST_GeomFromEWKB`/the `GEOGRAPHY` column type/the new scalar functions; `executor::ogc_conformance_tests` adds 13 more. Full crate suite (`cargo test --lib`) run and passing at 401/401 as of this pass — a prior pass on this same item had claimed "run and passing" while the crate didn't even compile (fixed then; noted here so this item's own history stays honest), so this note exists to make clear the count above was actually observed, not assumed.

**Milestone (verified for the CPU-side, non-GPU query path):** `CREATE INDEX ON drones USING SPATIAL (pos)` builds a `storage/geo_index.rs` index (S2-inspired cell → row-key buckets, each entry also carrying its point's time). `executor/planner.rs`'s `extract_spatial_predicate` recognizes a top-level `ST_DWithin(pos, ST_MakePoint(...), radius) AND ST_T(pos) BETWEEN t1 AND t2` WHERE clause and answers it with one `Database::spatial_query` call (a handful of cell lookups, not a table scan) instead of falling through to `resolve_table_ref`'s full scan — verified end-to-end in `executor/geo_tests.rs::spatial_index_scan_combines_radius_and_time_range` against an in-process `Database`. Not verified: actual latency at 10M-row scale (no benchmark harness run in this environment — the milestone's "<10ms on 10M rows" figure is unverified, only the query-shape/correctness claim is). The GPU join path's `TPT_GPU_JOIN_THRESHOLD` crossover point is likewise single-machine/unbenchmarked-at-scale (see `executor/gpu_join_tests.rs::gpu_vs_cpu_wall_clock_at_moderate_scale`'s unasserted timing print) — not a general performance claim across hardware.

---

## Phase 7 — Prism: Vector / AI Engine

New module `src/vector/` inside `tpt-keystone` (per `7prismspec.txt`, implemented in-engine rather
than as a separate crate/process — same "one binary" precedent as every other engine so far).

- [x] HNSW index — `vector/hnsw.rs`: a real, from-scratch multi-layer Hierarchical Navigable Small
  World graph (Malkov & Yashunin), configurable `M`/`ef_construction`/`ef_search`, insert + k-NN
  search — not a brute-force scan pretending to be HNSW (`vector::hnsw::tests::graph_is_not_a_flat_full_scan`
  asserts it doesn't visit every node). Honest caveat on "SIMD-accelerated (AVX-512/NEON)": no explicit
  SIMD intrinsics (`std::arch`, `packed_simd`) are used anywhere — plain scalar loops over `&[f32]`
  that the compiler's auto-vectorizer is free to use, same "can't claim a speedup that was never
  benchmarked" discipline as every other phase's hardware-acceleration claims
- [x] IVF-PQ index (inverted file + product quantisation) — `vector/kmeans.rs` (plain Lloyd's-algorithm
  k-means, shared by both stages), `vector/pq.rs` (real Product Quantization: per-subvector k-means
  codebooks, `encode`/`decode`, and an asymmetric-distance-table (ADC) path that scores candidates without
  decoding codes back to floats), `vector/ivf_pq.rs` (`IvfPqIndex`: coarse k-means quantizer over full
  vectors → inverted lists, each storing PQ-encoded *residuals* — the standard IVF-PQ refinement over
  quantizing raw vectors directly — with `n_probe`-nearest-list search). `CREATE INDEX ... USING IVFPQ ON
  t(col) WITH (metric=.., lists=.., pq_m=.., n_probe=..)` (`executor/ddl.rs`) backed by
  `storage/ivf_pq_index.rs`'s `IvfPqStorageIndex`, which trains from a backfill of existing rows (IVF-PQ,
  unlike HNSW, needs training data before it can accept its first insert — there's no incremental/online
  variant here) and persists an on-disk log of raw `(row_key, vector)` records (same uncompressed-log
  format as `VectorIndex`) that a reopen replays through a from-scratch retrain against the header's
  stored `metric`/`n_lists`/`pq_m`/`n_probe`, so the coarse centroids and PQ codebooks are reconstructed
  deterministically rather than persisted separately. The real memory saving is in the *in-memory*
  `IvfPqIndex` structure (single-byte PQ codes per subvector, not raw `f32`s) — same "raw log replayed
  into a compact in-memory structure" precedent as every other local index in this codebase (`storage::btree`/
  `storage::geo_index`). WITH-option values must be quoted (`lists = '100'`, not `lists = 100`) — the
  hand-written `WITH (...)` grammar's `parse_kv_option` (`sql/parser.rs`) only accepts a string literal
  or bare identifier for an option value, not an integer literal, same requirement every other engine's
  numeric DDL options (Chronos's `retention`, Meridian's spatial grid level) already have. Test coverage:
  `vector::kmeans::tests`, `vector::pq::tests` (encode/decode round-trip error bound, asymmetric-distance
  ordering matches true distance ordering), `vector::ivf_pq::tests` (trains and searches the correct
  synthetic cluster, a freshly-inserted vector is findable post-training — approximate/lossy PQ doesn't
  guarantee it strictly outranks a genuinely-closer existing point, so this checks presence in a small
  top-k rather than rank-1, wide `n_probe` still respects `k`), `storage::ivf_pq_index::tests`
  (train+insert+query, reopen replays+retrains, empty training set errors), and now
  `executor::prism_tests::ivfpq_index_created_and_answers_vector_search` — SQL-level coverage (`CREATE
  INDEX ... USING IVFPQ` through `executor/ddl.rs` and `vector_search` routing to an IVF-PQ-only index
  with no HNSW present) that didn't exist when this item was first marked done; running the full suite
  this pass also caught and fixed: a missing `#[derive(Debug)]` on `executor::QueryResult` that broke
  the whole crate's test compilation, two IVF-PQ tests whose assertions were mathematically impossible
  for a lossy approximate index to satisfy (asserting a farther point would outrank closer real training
  points) rather than real index bugs, and (see the OGC item above) the Web-Mercator-radius fix.
- [x] DiskANN index for billion-scale on-disk graphs — `vector/vamana.rs` (a real, from-scratch
  Vamana graph builder: greedy-search-guided construction plus the paper's `RobustPrune` alpha-pruning
  rule, not a relabeled HNSW) plus `storage/diskann_index.rs` (`DiskAnnIndex`), which is what actually
  earns the "on-disk" claim `VectorIndex`/`IvfPqStorageIndex` (above) don't: `open()` reads only a small
  header plus the row-key list, never the vectors or adjacency lists — `query_knn`'s greedy search reads
  exactly the node records (`seek`+`read_exact`, one per visited node) it visits, directly off a flat
  fixed-record-size file, once per query. Honest scope cuts vs. the real DiskANN system: no in-memory
  PQ vector cache for fast approximate re-ranking (re-reads full-precision vectors from disk on every
  visit instead), no SSD-page-aligned batched reads, batch build only (`vector::vamana::build` needs
  the full point set in memory to construct the graph — the disk-residency property is about querying,
  not building — same "train from a batch" precedent as `vector::ivf_pq`). Not wired into `CREATE INDEX`
  DDL yet — usable today via the Rust API (`DiskAnnIndex::build`/`open`/`query_knn`), same as any other
  index kind before its DDL surface lands. Verified: `vector::vamana::tests` (bounded out-degree,
  >90% recall vs. brute force on a clustered synthetic dataset, single-point edge case) and
  `storage::diskann_index::tests` (build-then-reopen preserves header/count, exact-match k-NN, and —
  the key property test — a handle opened *after* the source vectors were dropped from memory still
  answers queries correctly, proving `open()` doesn't secretly need them).
- [x] Automatic index selection by query planner (latency vs recall trade-off) — `Database::vector_knn_query`
  (`storage/database.rs`): when a `(table, column)` has *both* an HNSW and an IVF-PQ index, queries route to
  HNSW below a fixed row-count threshold (100k) and IVF-PQ at/above it. This is an honest, documented
  size-based heuristic — HNSW favors recall per query at small/medium scale, IVF-PQ's compressed in-memory
  representation matters once the graph would otherwise be large — not a latency/recall cost-model a real
  query optimizer would use; there's no benchmark harness in this repo to tune or validate a fancier policy
  against (same honesty precedent as every other "automatic"/"optimal" claim in this codebase).
- [x] Cosine / dot-product / L2 similarity — `vector/vector.rs`'s `l2_distance`/`cosine_distance`/
  `cosine_similarity`/`dot_product`, exposed as SQL scalar functions (`executor/eval.rs`) and used
  internally by the HNSW graph's distance metric (`Metric::L2`/`Metric::Cosine`). Same "hardware-accelerated"
  caveat as the HNSW item above — plain scalar loops, no explicit SIMD
  test coverage: `executor/prism_tests.rs::l2_distance_known_case`/`cosine_distance_identical_vectors_is_zero`/`dot_product_known_case`
- [x] Hybrid search: vector + BM25 full-text + SQL filters in single pass — `vector_search('table',
  'column', '[..]', k)` (`executor/mod.rs`, a table-valued function following Plexus's `graph_neighbors`
  precedent since k-NN's "ORDER BY distance LIMIT k" shape doesn't fit the planner's WHERE-clause
  pushdown pattern the way Meridian/Chronos's index rewrites do) returns full matched rows plus a
  `distance` column, so it composes with ordinary `JOIN`/`WHERE` (verified in
  `executor/prism_tests.rs::vector_search_hybrid_sql_filter`). Real BM25 ranking now exists: Canopy's
  `FtsIndex` (`storage/canopy_index.rs`) was rebuilt from presence-only postings (`token -> Vec<row_key>`)
  to term-frequency postings (`token -> row_key -> tf`) plus per-row document length, backing a new
  `search_bm25` (Robertson/Zaragoza Okapi BM25, standard `k1=1.2`/`b=0.75`, OR semantics — a doc needs
  only one query term, unlike `search_and`'s AND boolean match) exposed as `Database::fts_search_bm25`.
  A new `hybrid_search('table', 'vec_col', '[..]', 'fts_col', 'query', k)` table function
  (`executor/mod.rs`) fuses the vector k-NN ranking and the BM25 ranking via Reciprocal Rank Fusion
  (`score = Σ 1/(60 + rank)` over whichever ranked list a row appears in, no tunable weights between
  the two signals) into one result with `vec_distance`/`bm25_score`/`fused_score` columns. Honest scope
  note, unchanged from before: this is still two internal lookups (HNSW search, FTS BM25 scoring) fused
  into one ranked SQL result, not literally a single index scan. Verified end-to-end in
  `executor/prism_tests.rs::hybrid_search_fuses_vector_and_bm25_rankings` (a row winning on both signals
  ranks first; rows winning on only one signal still surface via RRF; a row matching neither is
  excluded) and `::hybrid_search_requires_both_indexes`; BM25 scoring itself in
  `executor/canopy_tests.rs::fts_bm25_ranks_by_relevance`. All 212 `tpt-keystone` tests pass.
- [x] Native product quantisation at storage layer — `vector/pq.rs`'s `ProductQuantizer`, wired into the
  `IvfPqIndex` above (the in-memory representation stores PQ codes, not raw floats). Scalar and binary
  quantisation remain **not implemented** — only product quantisation exists; the base `VECTOR` column
  itself (used by the HNSW path) still stores plain `f32` components with no compression, unchanged.
- [x] Consistent hashing for distributed vector shards — `vector/shard.rs`'s `ConsistentHashRing`
  (real Karger-style hash ring + virtual nodes, FNV-1a plus a MurmurHash3-style `fmix64` finalizer for
  even key distribution — plain FNV-1a alone visibly skewed shard sizes on the ring's own virtual-node
  labels, a real bug the distribution test caught and this finalizer fixes) plus
  `storage/sharded_vector_index.rs`'s `ShardedVectorIndex`, which partitions a `VECTOR` column's HNSW
  index across N per-shard `VectorIndex` logs: `insert` routes by consistent hash of the row key,
  `query_knn` scatter-gathers every shard's local top-k and merges by distance. Honest scope cut: this
  is the routing/partitioning primitive, not a distributed *deployment* — every shard is still opened
  and queried in-process on one node (this engine has no cross-node RPC anywhere; the cloud-native model
  shares state through the object store, not node-to-node calls), so "distributed" here means "the
  index size/insert/query cost is split N ways," not "shards live on different machines." Verified:
  `vector::shard::tests` (near-uniform distribution across 10k keys, same key always routes the same way,
  adding/removing a shard remaps only the fraction of keys that must move — not a `hash(key) % n`-style
  full remap — and removing a shard only reassigns *its own* keys) and
  `storage::sharded_vector_index::tests` (routing is deterministic and exercises every shard, and
  scatter-gather k-NN finds the same true nearest neighbor a single non-sharded `VectorIndex` baseline
  does).
- [x] Optional CUDA / ROCm GPU offload for batch similarity — delivered via `wgpu` (Vulkan/Metal/DX12)
  rather than a vendor CUDA/ROCm backend, for the same portability/verification reasons Meridian's
  `geo::gpu` used `wgpu` (a real GPU-present environment is needed to develop/verify native
  CUDA/ROCm paths, and `wgpu` already ships in this repo as the GPU dependency). `vector/gpu.rs` +
  `vector/shaders/similarity.wgsl`: a WGSL compute kernel computes the full query×base distance matrix
  on the device in one dispatch (a genuine matmul-shaped batch workload, not per-pair scalar loops),
  with the host doing a cheap top-k after readback (`gpu_batch_similarity` for the matrix,
  `gpu_brute_force_knn` for single-query k-NN). Wired into `Database::vector_knn_query` as a
  fail-safe brute-force path for `vector_search`/`hybrid_search` when no HNSW/IVF-PQ index exists and
  an adapter is available — returns `None` (the historical "no vector index" contract, so callers keep
  erroring the same way) whenever the GPU is unavailable (`gpu_available()` false), disabled via
  `TPT_DISABLE_GPU_VECTOR`, the base is too large for a single dispatch, or any row fails to decode.
  Same fail-safe discipline as `geo::gpu`: `f32` throughout (an exact-precision match to the CPU path
  here, not the precision narrowing Meridian's geo path incurs since its CPU side is `f64`), poison-on
  uncaptured-error so a broken device falls back to CPU for the rest of the process, and an
  `Oncelock`-cached adapter probe. Env vars: `TPT_DISABLE_GPU_VECTOR`, `TPT_GPU_VECTOR_MAX` (default
  256M query×base pairs). Tests: `vector::gpu`'s own `TPT_TEST_GPU`-gated unit tests (L2 + cosine
  matrix against hand-computed values, single-query k-NN, disable-env forces error) plus
  `executor::prism_gpu_tests` (GPU `vector_search` with no index agrees with a CPU brute-force baseline
  on `(label, distance)`, and errors when `TPT_DISABLE_GPU_VECTOR` is set). **Verified on a real GPU
  adapter in this environment**: `cargo test --lib 'vector::gpu'` (4 tests) and `cargo test --lib
  'prism_gpu_tests'` (2 tests) all pass — the WGSL kernel's L2/cosine distance matrix and top-k match a
  CPU baseline, and `vector_search` with no index returns GPU brute-force k-NN results that agree with a
  CPU brute-force baseline, while still erroring the historical "no vector index" way when the GPU is
  disabled. (Note: the executor test modules only compile under the lib test target in this environment
  because `tempfile`, a dev-dependency, isn't linked into the bin test target here — run them via
  `cargo test --lib`, not `cargo test --bin`.) An honest remaining gap vs. the literal roadmap wording:
  this is a `wgpu` backend, not CUDA/ROCm specifically — Vulkan/Metal/DX12 cover the same hardware, but
  no NVIDIA-CUDA or AMD-ROCm-only path exists (a deliberate portability choice, not an unbuilt stub).

New `VECTOR` column type (`storage::ColumnType::Vector`, stored as `[1,2,3]` text via `Value::Text` —
same "no new row-encoding path" precedent as `Geometry`'s WKT-as-text) and `CREATE INDEX ... USING
VECTOR/HNSW ON t(col) WITH (metric = 'l2'|'cosine', m = ..., ef_construction = ..., ef_search = ...)`
(`executor/mod.rs::execute_create_index`), backed by `storage::vector_index::VectorIndex` (append-only
on-disk log of `(row_key, vector)` records replayed into the in-memory HNSW graph on open — insert-only,
no delete/update of a stale entry, same precedent as `storage::btree`). Vector literals are canonicalized
to `Vector::to_text()`'s form on `INSERT`/`UPDATE` (`executor::normalize_vector_cells`) so a value read
back always matches its parsed form regardless of how it was written. A second index kind, `CREATE INDEX
... USING IVFPQ ON t(col) WITH (metric = 'l2', lists = '100', pq_m = '8', n_probe = '8')`
(`executor/ddl.rs::execute_create_index`), backed by `storage::ivf_pq_index::IvfPqStorageIndex`, can coexist
with an HNSW index on the same column — `vector_search`/`hybrid_search` route between the two via
`Database::vector_knn_query`'s row-count heuristic described above.

**Milestone (verified for the CPU-side, non-benchmarked-at-scale path):** `CREATE INDEX ON docs USING
VECTOR (embedding) WITH (metric = 'l2')` builds a `VectorIndex`; `vector_search('docs', 'embedding',
'[1.0,0.0,0.0]', k)` answers a k-NN query via the HNSW graph and composes with an ordinary SQL `JOIN`/
`WHERE`, verified end-to-end in `executor/prism_tests.rs` (7 tests, all passing) against an in-process
`Database`. `vector::hnsw::tests::knn_recall_against_brute_force_is_high`/`cosine_metric_recall_is_high`
confirm high recall against brute-force on small synthetic sets. **Not verified:** the "1M-vector ANN
search returns top-10 in <5ms with >95% recall" figure — no benchmark harness exists in this repo/
environment (same honesty precedent as every other phase's unverified scale milestone).

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
- [~] GQL (Graph Query Language) compatibility layer — a real, working `MATCH` statement now exists
  (`sql::ast::MatchStmt`, `sql::parser::parse_match`, `executor::gql::execute_match`):
  `MATCH (a)-[:REL]->(b)-[:REL2]->(c) ON table(from_column) [WHERE var = 'lit'] RETURN a, b [LIMIT n]`,
  a new top-level statement (not a table function) that chains `Database::graph_neighbors` calls one
  hop per pattern edge against an existing `CREATE INDEX ... USING GRAPH` index — the same underlying
  traversal primitive `graph_neighbors`/`graph_bfs` already use, so `MATCH` is a second SQL-level surface
  over the identical graph index, not a new storage/traversal engine. Supports typed
  (`-[:REL]->`)/untyped (`-[]->`) edges, all three directions (`-[...]->`/`<-[...]-`/`-[...]-`), and
  multi-hop chains. Still an honest, explicitly scoped subset, not the full GQL grammar the original
  item named — a full grammar (arbitrary `WHERE` expressions over bound variables, `OPTIONAL MATCH`,
  multiple disjoint patterns, pattern comprehensions, `CREATE`/`MERGE`) remains a separate,
  large grammar-and-planner effort comparable to the SQL parser itself, hence `[~]` not `[x]`. What
  *is* real here: a single linear-chain pattern, one starting-vertex equality filter (`WHERE var =
  'literal'`, or every known vertex as a start candidate if omitted), and a `RETURN` list of the
  pattern's own bound node variables. Verified in `executor::gql::tests` (single/multi-hop, all three
  directions, typed-edge filtering, unfiltered full-vertex-scan start, `LIMIT`, and a clear error for a
  missing graph index) and `sql::parser_tests` (grammar shape, and that `RETURN`ing an unbound variable
  or `WHERE`-filtering a non-first variable are rejected at parse time, not silently accepted). What was
  already implemented instead of a MATCH grammar — the narrower "hybrid SQL + graph" table-function
  surface — remains and is unchanged (see the hybrid-queries item below); `MATCH` is additive to it, not
  a replacement.
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
- [x] Native JSON/BSON binary storage format — `storage/jsonb.rs`'s hand-written tag/length/value binary
  codec (canonical sorted-key encoding) is now wired into row storage: `encode_cell`/`decode_cell` plus a
  `CELL_MARKER` prefix (`0x00 0x01`, byte that can never begin a text/bytea cell, so stored cells are
  self-describing regardless of the toggle below). **Opt-in, off by default:**
  `Database::set_jsonb_binary_storage`/`TPT_JSONB_BINARY=1` (`main.rs`) — raw JSON text remains the
  default storage form. When on, `executor::dml::build_row_value` (shared by INSERT/UPDATE/COPY) encodes
  `Json` columns to binary on write; `executor::parse_rows` and `storage::database::decode_column` decode
  back to canonical JSON text on every read, so every operator/index (`->`/`->>`/`@>`, `JSONPATH`/`GIN`
  indexes) works transparently against binary-stored rows — verified in
  `executor/canopy_tests.rs::jsonb_binary_storage_is_transparent_to_queries` (also asserts the on-disk
  bytes are genuinely marker-prefixed binary, not text) and
  `jsonb_binary_rows_readable_after_disabling` (toggling the flag off mid-lifetime still reads back rows
  written while it was on, since decoding doesn't depend on the current toggle state, only the stored
  marker).
- [x] Aggregation pipeline (MongoDB-compatible stages: `$match`, `$group`, `$project`, etc.) —
  `executor/canopy_aggregate.rs`'s `run_pipeline`, driven by a real JSON pipeline (`serde_json`,
  already a dependency via `storage::jsonb`) rather than a from-scratch Mongo-stage-syntax parser:
  `$match` (equality plus `$eq`/`$ne`/`$gt`/`$gte`/`$lt`/`$lte`/`$in`/`$nin`), `$group` (`_id` as a
  `$field` reference or a compound sub-document of several field references; accumulators `$sum`
  (including the idiomatic `{"$sum": 1}` count-via-sum), `$avg`, `$min`, `$max`, `$count`, `$push`,
  `$first`), `$project` (inclusion/exclusion/rename via `$field` refs, `_id` passthrough matching
  Mongo's default), `$sort` (multi-key), `$limit`, `$skip`. Exposed to SQL as
  `aggregate('table', '<json pipeline array>')` (`executor/graph_fn.rs`), the same FROM-clause
  table-valued-function pattern `graph_neighbors`/`vector_search`/`hybrid_search` already established —
  a table's rows become JSON documents (a `Json`-typed column's text is parsed as nested JSON, every
  other column becomes a JSON scalar), run through the pipeline, and re-flattened into ordinary rows
  (the schema is the union of every key seen across the result documents), so the output composes with
  `WHERE`/`JOIN`/`ORDER BY` exactly like any other table function — proven by an end-to-end test that
  pipes `aggregate(...)`'s output through a real SQL `ORDER BY`. An unrecognized stage name (`$unwind`,
  `$lookup`, `$facet`, computed `$project` expression operators like `$add`/`$multiply` — none
  implemented) errors clearly rather than silently no-op'ing. Verified: `executor::canopy_aggregate::tests`
  (each stage individually, a full `$match`→`$group`→`$sort` pipeline, and the unknown-stage error) plus
  `executor::canopy_tests` end-to-end SQL coverage (`aggregate_table_function_runs_match_group_sort_pipeline`,
  `aggregate_table_function_errors_on_unknown_stage`).

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
- [~] Formal benchmark suite vs Postgres, InfluxDB, Neo4j, MongoDB, Kafka — scoped down: a real
  `criterion`-based harness (`tpt-keystone/benches/keystone_bench.rs`, `cargo bench`) measuring
  Keystone's own throughput/latency, not a head-to-head comparison against the other four systems
  (none are installed in this environment — same honesty policy as every other unverified-at-scale
  milestone in this file). Required widening `tpt-keystone/src/lib.rs` from a `geo`-only re-export
  (previously there just so `examples/gpu_smoke_test` could depend on it) to also re-export
  `executor`/`storage`/`sql`/`vector`/`wire`/`metrics`/`synapse`/`mirror` as a second, independent
  compilation unit — the same precedent `lib.rs`'s own module docs already established for `geo`,
  just widened to what a real end-to-end bench needs; `main.rs` still declares its own copies of every
  module for the actual `tpt-keystone` binary, unchanged. Five benchmarks against an in-process
  `Database` (`LocalFsObjectStore`-backed, so these measure engine logic cost, not real S3/NVMe I/O):
  `insert_throughput` (500-row batched INSERT), `point_select`/`full_scan` (5000-row table),
  `vector_knn` (Prism HNSW k-NN, 2000 vectors), `bm25_search` (Canopy BM25, 2000 docs, exercising this
  session's new ranking). **Measured in this environment** (informal run, `--measurement-time 1`, not
  the harness's real multi-second sample config — indicative, not the number `cargo bench` itself
  would report): insert ~410 elem/s (fsync-per-statement dominates, not a batched-write path), point
  select ~3.0ms, full-table-count ~3.7ms, HNSW k-NN ~36µs, BM25 top-10 ~550µs. These are this one
  dev machine's numbers, not a portable performance claim.
- [x] Documentation site (architecture, SQL reference, SDK docs, tutorials) — scoped to a static
  Markdown tree in-repo (`docs/`, see `docs/README.md`'s index), no site-generator build step:
  `docs/architecture.md` (the `wire`/`sql`/`executor`/`storage` layering, the cloud-native Phase 3
  model, why this is one binary rather than seven engines), `docs/sql-reference.md` (DDL/DML/query
  clauses plus every engine-specific function and table function), `docs/sdks.md` (what each of the
  Phase 14 SDKs — Rust/TS-web/TS-server/TS-edge/Python/Go/CLI — gives you and where its code lives),
  and two tutorials (`docs/tutorials/quickstart.md`, `docs/tutorials/hybrid-search.md` walking through
  this session's `vector_search`/`hybrid_search` work). Cross-links `docs/formats/` (already existing)
  rather than duplicating its on-disk-format content.
- [x] Security audit (wire protocol auth, WASM sandbox, S3 credential handling)
  — findings in `docs/security_audit_phase12.md`; headline finding (no wire
  auth/TLS) and the wasmtime trap-handling verification gap are still open,
  WASM UDF module-size cap was closed as part of this pass
- [x] Publish versioned, language-independent on-disk format specifications
  (Keystone SSTable/WAL, Chronos, Canopy, Prism index formats) so readers can be
  reimplemented independently of the original Rust codebase
  — `docs/formats/`, including `prism_vector_index.md` for Phase 7's HNSW index format
- [ ] Apache 2.0 open-source release — `Cargo.toml`/`LICENSE` already say
  Apache-2.0; the "release" step itself (actually publishing) is still open

---

## Phase 12a — Follow-ups from external review (2026-07-10)

A third-party architecture review raised 6 concerns about distributed coordination,
S3 throughput, and query planning. Verified against the actual code; two were already
addressed by the existing single-writer/CAS-lease design (see `storage/lease.rs`,
Phase 3) and are not tracked here. The remaining real gaps:

- [x] LSM-tree compaction — see Phase 1 item above (cross-referenced, not duplicated work)
- [x] Query planner statistics (`ANALYZE`, row-count/distinct-value tracking,
  cost-based join-order selection) — scoped down from a full histogram-based
  cost model: `ANALYZE [table]` (new `Stmt::Analyze`, `sql/lexer.rs`/`parser.rs`/`ast.rs`)
  full-table-scans (no sampling) and persists a row count plus per-column distinct-value
  count into a new `_tpt_stats` system table (`executor/stats.rs`), following the same
  "plain Keystone table, `CREATE TABLE IF NOT EXISTS` at open time" precedent Synapse/
  Mirror/`_tpt_roles` already established. `executor/planner.rs::reorder_inner_joins`
  uses the persisted row counts to reorder a 3+-way join: a maximal leading run of
  `INNER`/`CROSS` joins (stopping at the first `LEFT`/`RIGHT`/`FULL`, whose semantics and
  anything depending on its null-extended columns are order-sensitive) gets greedily
  scheduled smallest-estimated-row-count-first, respecting a dependency graph built from
  each join's `ON`-clause column references so a join can never be scheduled ahead of a
  table its predicate actually reads from. Safety-first design: if any table lacks a
  persisted stat, if any `ON`-clause column reference can't be unambiguously attributed to
  exactly one table, or if the schedule would require a forward reference, the whole
  reorder is abandoned and the original (today's) join order runs unchanged — a bug or
  blind spot in this heuristic can only leave a plan as it already was, never produce
  wrong rows, since `INNER`/`CROSS` joins are commutative/associative regardless of order.
  Verified in `executor/planner.rs`'s own tests: independent joins get reordered by
  ascending row count even when listed larger-table-first in SQL; a dependency chain
  (`c` depends on `b` depends on `a`) is respected even though naive size-only sorting
  would try to schedule the smallest table first; missing stats leave the order
  unchanged; and an end-to-end 3-table join with `ANALYZE` run first returns correct
  results. Not attempted: per-column histograms/selectivity estimation for WHERE-clause
  cardinality, or reordering across `LEFT`/`RIGHT`/`FULL` joins (semantically unsafe
  without a much larger rewrite) — this is real, working, safety-netted join-order
  selection for the common "several INNER joins" case, not a general cost-based optimizer.
- [x] S3 object-key prefix sharding for `sst/`/`wal/` — implemented in
   `storage/lsm.rs` (`sst_shard` / `sstable_key` / `wal_segment_key`, fanned out
   by a hash of the numeric id via `TPT_SST_SHARD_COUNT`, default 256). A reader
   reconstructs the exact object key from the manifest's id list with zero extra
   state, so it composes with the writer/reader model with no shard map to keep
   in sync. No app-level token bucket / jitter tuning on top of the AWS SDK's
   default retry/backoff (a documented, lower-priority follow-up if real S3
   SlowDown throttling is ever observed) — the prefix fan-out itself is done.
- [x] Memory-based backpressure / circuit breaker for S3 latency spikes —
   implemented in `storage/guard.rs` (`GuardedObjectStore`, wired into
   `main.rs` ahead of `CachedObjectStore`): a bounded in-flight request counter
   (memory backpressure, `TPT_OSS_MAX_INFLIGHT`, default 64) plus a circuit
   breaker (`TPT_OSS_CIRCUIT_FAILURES` / `TPT_OSS_CIRCUIT_OPEN_SECS`) that trips
   open after a run of backend failures and sheds load / fails fast until a
   half-open probe succeeds, with Prometheus metrics
   (`tpt_object_store_circuit_open`, `tpt_object_store_inflight`). A `CasError::Conflict`
   (a successful CAS round-trip) does not trip the breaker. Verified in
   `storage::guard::tests`.
- [x] Reader staleness signal — `storage/database.rs`'s `ReaderStaleness` (consecutive-failure
  count, last error, last-success timestamp, all shared atomics behind `Database::reader_staleness()`).
  `Database::refresh()` records success/failure on every attempt; `main.rs`'s reader refresh loop
  calls `ReaderStaleness::publish_metrics` every tick, which sets `tpt_reader_manifest_stale`
  (`metrics.rs`, a Prometheus gauge, already pre-existing in this codebase before this pass wired
  a real producer into it) and logs a `warn!` when refresh has consecutive failures. `is_stale`
  also flags a reader whose refresh keeps *succeeding* but hasn't run recently enough relative to
  a caller-supplied max age — a reader with no writer activity isn't distinguishable from one whose
  refresh loop silently stopped without this. Still fail-open in the sense that a stale reader keeps
  serving last-known-good reads rather than refusing them — the "staleness indicator exposed to
  clients" is a Prometheus metric for monitoring/alerting, not a per-query SQL-visible flag or a
  hard read-refusal switch (no wire-level "reject reads when stale" opt-in was built this pass).
  Verified in `storage::database::reader_staleness_tests` (fresh state isn't stale, a failure trips
  `is_stale` until a success clears it, a success older than the caller's max age is still flagged).

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
- [x] `<Canvas.AgentMonitor>` — live agent activity/replay monitor (Mirror-native, Phase 17), added
  after this phase's initial pass — `tpt-canvas/src/components/agent_monitor.rs`: session event
  timeline + replay cursor + per-agent latency bar chart, see Phase 17's Dashboard item for detail
- [x] Automatic TypeScript type generation from live Keystone schemas — `tpt-canvas/src/bin/tsgen.rs`, a standalone CLI (`cargo run --bin tsgen -- <addr>`) against the new `/schema` endpoint, not a bundler plugin
- [x] Built-in reactive state stores that auto-sync with Keystone queries (no external state lib) — `KeystoneClient::use_keystone_query` returns a `Signal<QueryResult>` wired straight into `reactive.rs`
- [x] Plugin API for custom Canvas components with WebGPU shader hooks — was mismarked `[ ]` in a
  prior pass despite the note below already describing it as done; corrected here after verifying it
  directly (`npm test` in `packages/sdk-web`: 33/33 passing, including the plugin-registry suite).
  `tpt-canvas` itself still renders via Canvas2D, not WebGPU (deliberate decision, see above — no Rust
  shader pipeline to hook into), so this lives one layer up in `@tpt/sdk-web`:
  `packages/sdk-web/src/plugin.ts`'s `PluginRegistry` (component registration, install/uninstall
  lifecycle with per-plugin instance teardown, a shared cross-plugin event bus) plus `plugin-gpu.ts`'s
  `CanvasGpuContext`, a real WebGPU compute-and-fragment-shader wrapper (`runCompute` dispatches a WGSL
  compute shader over storage buffers with optional readback; `renderFragment` runs a vertex+fragment
  shader pair against the plugin's own WebGPU canvas) — genuine hardware-accelerated shader hooks
  against the browser's native WebGPU API, just registered from the JS/TS plugin layer instead of from
  `tpt-canvas`'s Rust core.
- [x] Integration with popular bundlers (Vite, Webpack, esbuild) — `wasm-bindgen --target web` output is a plain ES module + `.wasm` file, which all three already consume with zero plugin code

Required a small addition on the `tpt-keystone` side: `src/wire/http_query.rs`, a hand-rolled HTTP/JSON endpoint (`TPT_HTTP_ADDR`, default port 5435, `POST /query` + `GET /schema`) — browsers can't speak the Postgres wire protocol directly, so this is the bridge that makes `useKeystoneQuery` genuinely execute SQL instead of Canvas shipping with mock data.

**Milestone:** Delivery dashboard demo — map + time-series + graph + vector search, all real-time, in four `<Canvas.*>` components with zero manual WebSocket code. Partially met: all four components are real and wired to live Keystone data with zero manual WebSocket code, but there's no browser available in this environment to actually run a demo dashboard in — verification stopped at `cargo build --target wasm32-unknown-unknown` succeeding, host-side unit tests for every component's pure logic (projection, clustering, layout, ranking, JSON flattening) passing, and an end-to-end `curl`/`tsgen` smoke test against a live `tpt-keystone` node. No browser-based visual verification was performed.

---

## Phase 14 — SDK Ecosystem

### SDK/Web (TypeScript / JavaScript)
- [x] `@tpt/sdk-web` npm package — `packages/sdk-web/`, plain ESM + `.d.ts` output (`tsc`, no bundler-specific build step)
- [x] Full TypeScript type definitions auto-generated from Keystone schemas — `packages/sdk-web/src/bin/typegen.ts` (`npx tpt-typegen <url>`), a TS sibling of `tpt-canvas/src/bin/tsgen.rs` against the same `GET /schema` endpoint
- [x] `useKeystoneQuery` / `useKeystoneMutation` reactive hooks — `src/hooks.ts` (framework-agnostic, built on a minimal `Store` in `src/reactive.ts`) plus a React adapter (`src/react.tsx`, `useSyncExternalStore`) exported from `@tpt/sdk-web/react`
- [x] Native WebSocket integration with TPT Flux — `src/flux.ts`'s `subscribeFlux` speaks `wire::websocket`'s subscribe/push protocol directly; `useKeystoneQuery`'s `realtimeTopic` option wires a topic to an automatic requery, same "full requery, not incremental patch" scope cut as `tpt-canvas`'s `use_keystone_query`
- [x] Type-safe builders for all 7 data models — `src/models.ts`: `relational`/`geospatial`/`timeseries`/`graph`/`document` build real SQL against function names verified in `executor/*_tests.rs` (`ST_DWithin`, `time_bucket`, `graph_neighbors`/`graph_bfs`/`graph_shortest_path`, `jsonb_set`); `vector` takes a caller-supplied distance expression since Prism (Phase 7) has no native ANN operator yet; `events` is a thin topic-name helper since the poll/commit cursor API is Postgres-wire-only, unreachable from a browser
- [x] Plugin API for custom Canvas components — `src/plugin.ts`'s `definePlugin`/`PluginRegistry`; components draw into a supplied `CanvasRenderingContext2D` (no WebGPU shader hooks, following `tpt-canvas`'s own Canvas2D-not-WebGPU scope cut)
- [x] Bundler integration (Vite, Webpack, esbuild) — plain ESM output via `"exports"` in `package.json`, zero plugin code required, same story as `tpt-canvas`'s `wasm-bindgen --target web` output

Verified end-to-end against a live `tpt-keystone` node (`cargo build --release` + `POST /query`/`GET /schema`/Flux WebSocket): schema introspection, typed query coercion, the relational builder, `useKeystoneMutation`/`useKeystoneQuery`, Flux subscribe/unsubscribe, and the `tpt-typegen` CLI all confirmed working against real running SQL, not mocked. Unit tests (`node --test`) cover the reactive `Store` and all five real SQL builders. No React-app / bundler integration test was run (no browser environment here — same limitation `tpt-canvas`'s Phase 13 milestone already notes), so the `@tpt/sdk-web/react` adapter is verified by type-checking and code inspection only, not by rendering in an actual React app.

### SDK/Rust (Native Desktop & Server)
- [x] `tpt-sdk` crate with `canvas` + `keystone` feature flags — `tpt-sdk/` (standalone crate, no workspace), `keystone` default-on, `canvas` pulls in `tpt-canvas`, `ffi` gates the C ABI
- [x] Sync + async APIs — `tpt_sdk::keystone::KeystoneClient` (async) and `tpt_sdk::keystone::blocking::Client` (current-thread Tokio runtime + `block_on`, for non-async Tauri/GTK callbacks)
- [x] Direct Canvas rendering pipeline access (WebGPU / Vulkan) — scope cut: no native GPU pipeline exists anywhere in the repo to bind to (`tpt-canvas` only renders via `web_sys::CanvasRenderingContext2d` behind `#[cfg(target_arch = "wasm32")]`, per its own Phase 13 scope cut); `src/canvas.rs` instead re-exports `tpt-canvas`'s target-agnostic reactive core (`Signal`/`create_effect`/`create_memo`) so a native host can stay in sync with the same signals Canvas components use
- [x] Embedded Keystone client for edge / desktop (Tauri, GTK) — same `KeystoneClient`/`blocking::Client`; `src/keystone/wire.rs` is a hand-written client-side codec for the same Postgres wire protocol v3 `tpt-keystone/src/wire` speaks server-side (no `pgwire`/`tokio-postgres`), supporting both the simple query protocol and the extended protocol (Parse/Bind/Describe/Execute/Sync) for parameterized queries
- [x] Zero-copy data transfer between Canvas and native code — `src/zerocopy.rs`'s `RowView` borrows cells straight from the wire read buffer instead of allocating a `Vec<Vec<u8>>` per row, and `src/ffi.rs`'s `tpt_sdk_result_cell` hands back a borrowed pointer into the owned `QueryResult` rather than copying; row-batch-level, not columnar/Arrow-level
- [x] FFI bindings for C/C++ interop — `src/ffi.rs`: `tpt_sdk_connect`/`tpt_sdk_query`/`tpt_sdk_result_row_count`/`tpt_sdk_result_column_count`/`tpt_sdk_result_cell`/`tpt_sdk_free_result`/`tpt_sdk_free_client`/`tpt_sdk_last_error`; covers request/response query execution only, no FFI surface for streaming/async callbacks

Not yet verified by an actual `cargo build`/`cargo test` run or a real connection against a live `tpt-keystone` node — written and self-reviewed only.

### SDK/Mobile
- [ ] `@tpt/sdk-react-native` — native bridge to Canvas, offline-first, Flux push notifications
- [ ] Flutter SDK (`tpt_sdk`) — custom widgets, hot reload, Metal/Vulkan backends
- [ ] Swift SDK (iOS) — async/await, SwiftUI + UIKit, CoreLocation + Metal
- [ ] Kotlin SDK (Android) — coroutines, Jetpack Compose, Fused Location + Vulkan

### SDK/Server (Backend)
- [x] `@tpt/sdk-server` — Node.js / Deno / Bun, streaming queries, SSR, Flux broadcast —
  `packages/sdk-server/`, a hand-written Postgres wire protocol v3 client over `node:net`
  (`wire.ts`, no `pg`/`postgres`/`pg-protocol` dependency, mirroring `tpt-sdk`'s Rust client
  codec). `KeystoneClient.query`/`queryParams` (simple + extended protocol) and `streamQuery`
  (an async generator yielding `DataRow`s as they're decoded off the socket — one server round
  trip, not `max_rows`/`PortalSuspended` chunking, since this Keystone build's `Execute` handler
  re-slices from the start on every call rather than advancing a cursor; documented in
  `client.ts`). SSR support: `schema()`/`queryTyped()`/`queryOne()` (`schema.ts`) introspect
  `information_schema.tables`/`columns` directly over the wire for typed cell coercion (there's
  no HTTP `/schema` endpoint reachable at this layer) — a plain awaitable API meant to be called
  from a loader/handler, no Next.js/Remix-specific adapter (scope cut). Flux broadcast: a
  hand-rolled RFC 6455 WebSocket client (`ws/client.ts`) consumes one Keystone Flux topic and a
  hand-rolled WebSocket server (`ws/server.ts`, no `ws` npm package) re-broadcasts it to
  downstream browser clients (`broadcast.ts`'s `FluxBroadcastServer`); no reconnect/backoff if
  the upstream Flux connection drops (scope cut). Only the Node (`node:net`) code path was
  written/tested — Deno/Bun compatibility is claimed only in the sense that `node:net` is
  available there via Node compat, not verified against either runtime directly.
  **Verified**: `npm run build` (clean `tsc`) and, against a live `tpt-keystone` node on
  `127.0.0.1:5432` (an older build without the Phase 13 HTTP/Flux-WS bridges listening, so the
  Flux broadcast path was verified as a WS client+server loopback, not against Keystone's own
  Flux bridge — real end-to-end Flux verification is still open): parameterized
  `CREATE TABLE`/`INSERT`/`SELECT`, `streamQuery` returning multiple rows including a `NULL`
  cell, and `schema()`+`queryTyped()` returning correctly typed `number`/`boolean` values. One
  real bug caught and fixed during this verification pass: `queryTyped`/`queryOne` originally ran
  `client.queryParams()` and `schema()` concurrently via `Promise.all` on the same TCP
  connection — the wire protocol has no pipelining, so this desynced the response stream; fixed
  to run sequentially.
- [x] Python SDK (`tpt-sdk`) — type hints, Pandas/NumPy, Jupyter, async/await — `sdk-python/`
  (distribution name `tpt-sdk`, importable module `tpt_sdk`; a new top-level directory since
  `tpt-sdk/` is already the Rust SDK crate's name). Hand-written async wire-protocol client
  (`wire.py`, ported from `tpt-sdk/src/keystone/wire.rs`) over stdlib `asyncio.open_connection`
  (no `psycopg2`/`asyncpg`). Full type hints throughout plus a `py.typed` marker; `Row` supports
  attribute access (`row.id`, per `9sdkspec.txt`'s example); `QueryResult._repr_html_` renders an
  HTML table for Jupyter cells (no `%%tpt_sql` magic — scope cut); `to_pandas()`/
  `to_pandas_async()` (optional `pandas`/`numpy` extra, `pip install tpt-sdk[pandas]`) coerce
  cells using `information_schema.columns` types rather than pandas' own text-sniffing;
  `blocking.Client` is a sync wrapper (fresh connection + `asyncio.run()` per call — no
  cross-call connection reuse or shared transactions, documented tradeoff).
  **Verified**: built/installed with `uv venv` + `uv pip install -e ".[pandas,dev]"` (Python
  3.13.7) and run against the same live instance: parameterized `CREATE TABLE`/`INSERT`/`SELECT`
  with attribute access, a multi-row `SELECT` through both `repr()` and `_repr_html_()`, and
  `to_pandas_async()` producing a DataFrame with real `Int64`/`string`/`Float64`/`boolean` dtypes
  (a `NULL` float round-tripped to pandas' `<NA>`). Incidental finding, not an SDK bug: this live
  instance's `DROP TABLE` reports success but leaves the table listed in `pg_tables` — worked
  around with timestamp-suffixed table names in the smoke test, flagged here in case it's
  relevant to future catalog/DDL work.
- [x] Go SDK (`github.com/tpt/sdk-go`) — idiomatic Go, context cancellation, connection pooling —
  `sdk-go/`, hand-written wire-protocol client over `net.Conn` (`wire.go`, no `lib/pq`/`pgx`).
  `Rows.Next()`/`Scan()` deliberately mirror `database/sql`'s shape without implementing the
  `database/sql/driver` interfaces (documented scope cut — no `sql.Open("keystone", ...)`
  registration). `Conn.Query` streams `DataRow`s directly off the socket as they arrive (no
  result-set buffering, real O(1)-memory backpressure — see `rows.go`'s doc comment for exactly
  which streaming strategy this is vs. `max_rows`/`PortalSuspended` chunking); `Conn.Exec` for
  statements. Every network-touching method takes a `context.Context`, honored via a `net.Conn`
  deadline plus a per-call cancellation-watcher goroutine that force-closes the socket on
  `ctx.Done()` (`Conn.Broken()` flags a conn as unusable so `Pool` discards rather than reuses
  it). `Pool` is min/max sized with checkout-time health checks via `Broken()`; no idle reaping,
  max-lifetime eviction, or dial retry/backoff (documented scope cuts in `pool.go`).
  **Verified**: `go build ./...`, `go vet ./...` clean, and `go test -tags live ./...` (a live
  server is required, so these are excluded from a plain `go test ./...` — unlike
  `storage::phase3_tests`, this SDK has no way to embed the Rust engine in-process) against the
  same live instance: a streaming `Query`+`Scan` round trip including a `NULL` column, a
  `context.WithTimeout` query that actually returned `context.DeadlineExceeded` and correctly
  marked its `Conn` broken, and 8 goroutines concurrently acquiring/querying/releasing a
  4-connection `Pool` without ever exceeding `MaxOpen`.

### SDK/CLI
- [x] Single binary CLI (`tpt`) — `tpt-cli/`, one binary (bin name `tpt`) built on
  `tpt_sdk::keystone::blocking::Client` (the sync wrapper `tpt-sdk` already documents as built for
  exactly this: "a plain non-async ... CLI tool that never touches Tokio directly") — no
  `pgwire`/`tokio-postgres`, same hand-written wire client every other SDK in this repo uses.
  Interactive REPL (`tpt` with no subcommand, or `tpt repl`): `;`-terminated statements, `\dt`/`\q`
  meta-commands. Export/import: `tpt export <table> --format csv|json [-o file]`, `tpt import
  <table> -f file --format csv|json` (hand-parsed CSV incl. quoted/embedded-comma fields — no `csv`
  crate; JSON via `serde_json`, already a dependency elsewhere in the repo) issuing one
  parameterized `INSERT` per row. Schema introspection: `tpt schema` (lists `public` tables) / `tpt
  schema <table>` (columns via `information_schema.columns`). Scope cut: REPL line editing is
  `stdin::read_line` — no history/readline-style arrow-key editing.
- [x] `tpt query` — `tpt query "<sql>"` (or `-f file.sql`) execute one statement, `--format
  table|json|csv` (table is the default, psql-style column alignment + `(N rows)` footer).
- [x] `tpt stream` — `tpt stream <topic>` tails Keystone's Flux WebSocket bridge (`wire/websocket.rs`,
  default port 5434) in real time. Hand-rolled RFC 6455 client (blocking `std::net::TcpStream`,
  masked client frames per §5.3) mirroring the server's own hand-rolled implementation rather than
  a `tungstenite` dependency — same from-scratch-wire-protocol rule as everywhere else in this repo.
  Verified against a live server's automatic per-table CDC stream (`__cdc_<table>`, `INSERT`
  correctly pushed a `{"key","offset","ts","value"}` frame instantly).
- [x] `tpt migrate` — `tpt migrate up|status --dir <dir>`, plain numbered `.sql` files applied in
  filename order inside `BEGIN`/`COMMIT`, tracked in an auto-created `_tpt_migrations(id TEXT
  PRIMARY KEY, applied_at TEXT)` table. Building this surfaced and fixed a real engine bug: `CREATE
  TABLE IF NOT EXISTS` parsed the clause (`sql/parser.rs`) but silently discarded it
  (`_if_not_exists`) instead of storing it on `CreateTableStmt` or checking it in
  `execute_create_table` — every `IF NOT EXISTS` `CREATE TABLE` unconditionally errored "already
  exists" on a second run (unlike `CREATE TOPIC`, which already honored the same clause correctly).
  Fixed to match the `CREATE TOPIC` pattern (`ast.rs`/`parser.rs`/`executor/mod.rs`); `CREATE
  SEQUENCE IF NOT EXISTS` has the identical bug and remains unfixed (out of scope for this pass, but
  same shape — parses and discards, never stored on `CreateSequenceStmt`).
  **Verified**: live end-to-end against a running `cargo run` instance — `tpt query`
  (table/json/csv), `tpt schema`/`tpt schema <table>`, `tpt export`/`tpt import` CSV round-trip,
  `tpt migrate up`/`status` (including confirming idempotent re-runs only after the engine fix),
  and `tpt stream` against a live CDC event.

### SDK/Plugin (Canvas Extensions)
- [x] Plugin lifecycle management (register, mount, unmount) — `packages/sdk-web/src/plugin.ts`'s
  `PluginRegistry.install`/`uninstall` (plugin-level: `setup` → `mount`, and `unmount` on teardown)
  plus `mountComponent`/`MountedComponent.update`/`unmount` (per-instance render lifecycle); uninstalling
  a plugin also tears down any live component instances it owns before calling its `unmount` hook
- [x] Custom rendering hooks (WebGPU compute + fragment shaders) — `packages/sdk-web/src/plugin-gpu.ts`'s
  `createGpuContext`/`CanvasGpuContext.runCompute`/`renderFragment`, a real `navigator.gpu` compute +
  vertex/fragment pipeline a `CanvasComponentDefinition.renderGpu` can opt into; lives in the JS/TS SDK
  layer rather than `tpt-canvas`'s Rust core since that crate's own Phase 13 scope cut already committed
  to Canvas2D over WebGPU (see `tpt-canvas/src/lib.rs`) — there's no Rust shader pipeline to hook into,
  so this hooks the browser's native WebGPU API directly instead. Only exercisable in a real browser
  (no `node --test` coverage, same DOM-availability limitation as Phase 13's milestone)
- [x] Inter-plugin event system — `packages/sdk-web/src/plugin-events.ts`'s `PluginEventBus`
  (`on`/`once`/`off`/`emit`/`clear`), exposed per-registry as `PluginRegistry.events` so any installed
  plugin's `setup`/component `render` can publish/subscribe without importing another plugin directly
- [x] Marketplace publishing toolchain — `packages/sdk-web/src/plugin-manifest.ts` (manifest schema +
  validation) and `src/bin/plugin-publish.ts` (`npx tpt-plugin-publish <manifest> [--out dir] [--registry
  url]`): validates the manifest, dynamically imports the built entry file to confirm it actually exports
  a `CanvasPlugin` shape, packages it into a self-contained `<name>-<version>.tptplugin.json` artifact
  (sha256 checksum + base64 code), and optionally POSTs it to a caller-supplied registry URL. There's no
  hosted TPT plugin registry to publish to yet, so `--registry` was verified against a local `node:http`
  server in `plugin-publish.test.ts`, not a real marketplace.

**Verified**: `npm run build` (clean `tsc`) and `node --test` — 29 passing tests across `plugin.test.ts`,
`plugin-events.test.ts`, `plugin-manifest.test.ts`, and `plugin-publish.test.ts` (existing suites
unaffected); `tpt-plugin-publish` run end-to-end against a real fixture plugin + manifest, producing a
valid `.tptplugin.json` artifact with a checksum verified against the source file by hand. `plugin-gpu.ts`
is written and self-reviewed only — no browser environment here to exercise `navigator.gpu` for real.

### SDK/Edge (Wasm Workers)
- [x] `@tpt/sdk-edge` — `packages/sdk-edge/`, zero-dependency TS client (no `node:*` imports) for
  Cloudflare Workers / Fastly Compute / Vercel Edge / Lambda@Edge. Deliberate scope adjustment from the
  literal "WASM bundle" phrasing: these runtimes already expose `fetch`/`WebSocket`/`Cache` as ambient
  globals, and most (Fastly, Vercel Edge, Lambda@Edge) can't open a raw outbound TCP socket at all — only
  Cloudflare Workers can via `connect()`. Talking the same Canvas HTTP/JSON bridge `@tpt/sdk-web` uses
  (`src/client.ts`, `wire::http_query.rs`) keeps one code path working on every target instead of a
  Workers-only fast path plus a fallback, and avoids shipping/instantiating an actual `.wasm` binary,
  which would cost cold-start time for no benefit here (no compute-heavy inner loop to justify it)
- [x] Streaming responses + edge caching integration — `src/stream.ts`'s `subscribeFlux` reuses the Flux
  WebSocket bridge (`wire::websocket.rs`) for real-time push (the HTTP/JSON bridge is explicitly
  non-streaming server-side, see that file's module doc); throws a clear error instead of hanging on
  runtimes without global `WebSocket` (e.g. Lambda@Edge). `src/cache.ts`'s `cachedQuery`/
  `invalidateCachedQuery` synthesize a GET `Request` keyed by `sql`+`params` against the standard Web
  `Cache` API (`caches.default` / `caches.open`), so repeat reads skip Keystone entirely within a
  caller-set `ttlSeconds`
- [x] Zero cold-start profile — no `.wasm` instantiation step and no runtime dependencies at all; built
  output is ~4.6KB across `client.js`/`stream.js`/`cache.js`/`index.js`, well under the 50KB budget

**Verified**: `npm run build` (clean `tsc`) and `node --test` — 5 passing tests (`client.test.ts`,
`cache.test.ts`) mocking global `fetch`/`Cache` to cover query zipping, typed coercion via `schema()`,
error propagation, Flux URL derivation, and cache hit/miss/invalidate behavior. No real edge-runtime
deployment (Cloudflare Workers/Fastly/Vercel) was exercised — verification stopped at Node-based unit
tests against mocked globals plus a clean build; `subscribeFlux`'s "no global `WebSocket`" guard is
unit-tested but not confirmed against a real Lambda@Edge environment.

**Milestone:** Single-line connection to Keystone works from Web, Rust, Python, and CLI; `tpt query "SELECT 1"` returns a result

---

---

## Phase 15 — Harbor: Universal Data Migration Platform

New crate `tpt-harbor/`. Scope for this pass (see PR/commit): core engine + Harbor/PG only, per an
explicit scope-down decision — 10 source-specific wire protocols plus a web dashboard is multiple
projects' worth of work; every other connector is a named stub so the CLI/trait surface is ready for
whichever gets built next, but none of their protocol code exists.

- [x] Core migration engine (Rust, checkpoint/resume on failure) — `engine/mod.rs` + `engine/checkpoint.rs`. Not "zero-copy": snapshot reads go through `DECLARE CURSOR`/`FETCH` batches (bounded memory, not zero-copy) rather than the `COPY` sub-protocol; "parallel workers" is also not implemented — tables are migrated one at a time, not in parallel.
- [x] Schema Translator — `schema.rs`: Postgres `information_schema` type names → Keystone SQL DDL. Not a "rule-based AST engine" — it's a direct type-name lookup table, not an AST-to-AST rewrite (Postgres DDL has no AST here to rewrite; column type + nullability + PK is all Harbor discovers).
- [x] Verification Engine — `verify.rs`: xxHash3 per-row checksums (via `xxhash-rust`) + row-count diffing. Not implemented: per-column checksums (only whole-row) and query regression testing (would need a captured query corpus + a judgment call on acceptable plan/latency drift — out of scope for this pass).
- [x] Migration lifecycle: Discover → Validate (Dry Run) → Snapshot → Replicate (Live CDC) → Verify → Cutover — all six phases implemented as `MigrationEngine` methods and CLI subcommands. Cutover doesn't pause application traffic or flip connection strings itself (environment-specific); it confirms verification passed and tells the operator to redirect writes.
- [x] **Harbor/PG** — PostgreSQL → Keystone. Bulk reads via cursor-batched `SELECT`/`FETCH` (see core-engine note above, not raw `COPY`); live sync via real `pgoutput` logical replication (`CREATE_REPLICATION_SLOT`/`START_REPLICATION`, hand-decoded `Begin`/`Relation`/`Insert`/`Update`/`Delete`/`Commit` messages — no `pgwire` crate, consistent with this repo's from-scratch rule). **Not implemented:** PL/pgSQL → WASM UDF transpilation (would need a PL/pgSQL parser this repo doesn't have — only table data/DDL migrates, not stored procedures). **Not verified in this environment:** an end-to-end run against a real standalone PostgreSQL server (no Postgres install available here) — verified instead by pointing Harbor/PG's source connector at a live `tpt-keystone` node (which is itself Postgres-wire/`information_schema`-compatible per Phase 4) for `discover`/`validate`/`transfer`; logical replication was exercised against `tpt-harbor`'s unit tests and code review, not a live `START_REPLICATION` session (`tpt-keystone` doesn't implement the replication sub-protocol server-side, so that path has no counterpart to test against in this repo).
- [x] **Harbor/Mongo** — MongoDB → Canopy. **Correction to this line's own former text:** not "stub only" —
  `sources/mongodb.rs` has a real hand-written `OP_MSG` wire-protocol client (discovery, snapshot,
  checksums). It was present on disk but **did not compile** (an `i32`/`u32` mismatch on
  `put_i32_le`) before this pass; fixed. Still unverified against a real MongoDB server in this
  environment — "compiles and passes its own unit tests" is as far as honesty can currently go.
- [x] **Harbor/Graph** — Neo4j → Plexus. Same correction: `sources/neo4j.rs` has a real hand-written
  Bolt protocol v4+ client, not a stub. Did not compile before this pass — `put_u32_be`/`put_u16_be`
  aren't real `bytes::BufMut` methods (fixed to the unsuffixed `put_u32`/`put_u16`, which are
  big-endian in this crate, not `_le` as the compiler's own suggestion would have produced), and
  `run_cypher`/`read_records` were declared to return `Vec<Vec<(String, BsonValue)>>` while every
  call site (and `bolt_decode_list`'s real return type) treated rows as `Vec<BsonValue>` — fixed by
  correcting the declared return type. Unverified against a real Neo4j server.
- [x] **Harbor/TimeSeries** — InfluxDB → Chronos. Genuinely stub only — `sources/influxdb.rs` didn't
  exist on disk at all (declared in `sources/mod.rs` but the file was missing, so this crate failed
  to compile) despite this line previously claiming "stub only" as if it existed; created using the
  `unimplemented_source!` macro that was already defined in `connector.rs` but never actually used
  anywhere until now.
- [x] **Harbor/Stream** — Kafka → Flux. Same as TimeSeries: `sources/kafka.rs` was missing from disk;
  created as a genuine `unimplemented_source!` stub.
- [x] **Harbor/Vector** — Pinecone/Weaviate/Qdrant → Prism. Same: `sources/vector.rs` was missing;
  created as a genuine stub.
- [x] **Harbor/GIS** — PostGIS → Meridian. `sources/postgis.rs` already existed with a real
  `information_schema`-based connector (reusing `schema::from_postgis_type`) and already compiled —
  no fix needed here.
- [x] **Harbor/Oracle** — Oracle → Keystone. `sources/oracle.rs` was missing from disk; created as a
  genuine stub.
- [x] **Harbor/MySQL** — MySQL/MariaDB → Keystone. `sources/mysql.rs` already existed with a real
  wire-protocol connector and already compiled — no fix needed.
- [x] **Harbor/Search** — Elasticsearch → Canopy. `sources/elasticsearch.rs` was missing from disk;
  created as a genuine stub.
- [x] **Harbor/MSSQL** — SQL Server → Keystone. `sources/mssql.rs` already existed with a real
  TDS 7.4 login/query client but **did not compile** (a closure capturing `data: &mut BytesMut` for
  its whole lifetime conflicted with direct `data.len()`/`put_slice` calls made while the closure was
  still alive, building the XOR-encoded password field) — fixed by turning the closure into a free
  function taking `data: &mut BytesMut` explicitly per call. Unverified against a real SQL Server.
- [x] **Keystone target connector** (`target/keystone.rs`) — also entirely missing from disk (the
  whole `target/` directory didn't exist, despite `lib.rs`'s own module docs listing it as
  implemented) — every source above targets this. Connects via `pgwire::Client` (Keystone speaks the
  same Postgres wire protocol Harbor/PG's source already does); applies translated DDL, bulk-inserts
  snapshot batches as multi-row `INSERT`s, and applies live CDC `Insert`/`Update`/`Delete` events —
  `UPDATE`/`DELETE` build their `WHERE` clause by zipping a change event's key positionally against
  the table's primary-key columns, skipping the change rather than guessing if the widths don't match.
  Every cell is inlined as a quoted SQL text literal (this client has no `Parse`/`Bind`, only
  `query`/`execute` over raw text) — a documented SQL-injection-shaped scope cut consistent with this
  whole engine having no auth/security boundary anywhere else either.
- [x] CLI: `tpt-harbor discover / validate / transfer / replicate / verify / cutover` — `tpt-harbor/src/main.rs`, all six subcommands wired to the engine.
- [x] Web dashboard — `tpt-harbor/src/dashboard.rs`: a hand-rolled, read-only HTTP status server
  (mirroring `tpt-keystone::wire::http_query`'s request/response idiom almost verbatim, since Harbor
  doesn't depend on `tpt-keystone` as a library) plus an embedded self-contained HTML/vanilla-JS page
  that polls `GET /status` every second and renders a phase badge, per-table progress bars, and
  verification results. Opt-in via `--dashboard-addr <addr>` on `transfer`/`replicate`/`verify`/
  `cutover`, spawned as a `tokio::spawn`ed task alongside whatever phase is actually running.
  Read-only scope cut: no way to start/stop/pause a migration from the dashboard, and it exists only
  for the duration of the CLI process (no persistence once the command exits). Verified end-to-end
  against a real loopback socket (`dashboard::tests::status_and_index_routes_respond_over_a_real_socket`
  — a raw `TcpStream` request against `GET /status`/`GET /`, confirming real HTTP responses, not just
  the `StatusHandle` struct in isolation).

**Milestone:** Zero-downtime migration of a full Postgres production database to Keystone with every row
checksum-verified — **not attempted at production scale.** Harbor/PG's `discover`/`validate`/`transfer`/
`verify` path is verified end-to-end against a live `tpt-keystone` node standing in for a Postgres source
(see the Harbor/PG note above for why); no live multi-GB dataset or real zero-downtime cutover (source
writes continuing during `replicate`, then a real `cutover`) was exercised in this environment.

**Pre-existing repo-state finding, corrected in this pass:** before this pass, `tpt-harbor` **did not
compile at all** — `sources/elasticsearch.rs`/`influxdb.rs`/`kafka.rs`/`oracle.rs`/`vector.rs` and the
entire `target/` directory were declared as modules but never existed on disk in any commit on any
branch (`git log --all`/`git branch -a` confirmed), and `mongodb.rs`/`mssql.rs`/`neo4j.rs` had real but
broken protocol code. None of this was caused by this pass's dashboard work; all of it predates this
session. `cargo build`/`cargo test` on `tpt-harbor` are green now (19/19 tests passing) — see the
per-connector notes above for exactly what was missing vs. broken vs. already fine.

**Follow-up (2026-07-12): replace the five `unimplemented_source!` stubs with real connectors** — the
prior pass's `unimplemented_source!` stubs got `tpt-harbor` compiling, but five sources had zero protocol
code. All five now have real client code; see the per-connector confidence notes below — they are not
uniformly trustworthy, and that's tracked explicitly rather than glossed over:
- [x] **Harbor/TimeSeries** — InfluxDB → Chronos, real client (`sources/influxdb.rs`): HTTP client against
  `/query`/`/write`, handling both `Content-Length` and chunked transfer-encoding (own hand-written
  decoder — fixed an off-by-one in this pass that dropped the first record's leading byte and the last
  chunk's trailing byte). Same confidence tier as this crate's other HTTP/text-protocol connectors
  (Elasticsearch, PostGIS): a fully public wire format, unverified against a live server.
- [x] **Harbor/Stream** — Kafka → Flux, real client (`sources/kafka.rs`): hand-written Kafka wire protocol
  (`Metadata`/`Fetch` v-appropriate request/response framing, record-batch v2 decoding including the
  zigzag-varint record-level fields). Same public-protocol confidence tier as the others; CDC-equivalent
  (`replicate`) treats `Fetch` from committed offsets as the "change feed." Uncompressed batches only
  (`gzip`/`snappy`/`lz4`/`zstd` compressed batches are rejected with a clear error, not silently
  misdecoded) — no compression codec dependency was wired in.
- [x] **Harbor/Vector** — Pinecone/Weaviate/Qdrant → Prism, real client (`sources/vector.rs`, one file
  covering all three per the existing stub's shape): REST/HTTP clients against each service's own public
  index/collection and vector-fetch APIs (`api_key` now an explicit `Option<&str>` parameter — the CLI
  doesn't have a `--source-api-key` flag yet, so it's passed as `None` from `main.rs` for now). Same
  public-protocol confidence tier as InfluxDB/Elasticsearch.
- [x] **Harbor/Search** — Elasticsearch → Canopy, real client (`sources/elasticsearch.rs`): HTTP client
  against `_cat/indices`, `_mapping`, and the scroll API for snapshot/checksums. Same confidence tier.
- [x] **Harbor/Oracle** — Oracle → Keystone, real client (`sources/oracle.rs`) — **materially different
  confidence tier than the other four.** The TNS packet-framing layer (8-byte header, CONNECT/ACCEPT/
  REFUSE packet types, the CONNECT payload's field layout) is genuinely public (the same shape every
  public TNS protocol dissector documents) and is written with the same confidence as this crate's other
  from-scratch wire-protocol code. The TTC (Two-Task Common) layer carried inside DATA packets — the
  login/auth handshake and the OALL8/OFETCH RPC opcodes used to run queries and fetch rows — has **no
  public specification**; Oracle's own OCI client is the only officially documented way to speak it. That
  layer is a best-effort reconstruction (documented in the module's own doc comment) and should be read as
  a structural skeleton to correct against a real packet capture, not as validated protocol code — a
  strictly higher-risk claim than "unverified against a real server," which is what the other nine Harbor
  source connectors in this file get to claim. This flags exactly the OCI/proprietary-protocol risk called
  out in the original follow-up note; the recommended next step if real Oracle connectivity is ever
  needed is linking against Oracle Instant Client (OCI) rather than continuing to reverse-engineer TTC.
  Unit-tested at the layer that can be (TNS framing helpers, TTC row-cell length-prefix decoding).

**Test-gap fixes made alongside this pass (not new work, bugs found while verifying the above 5
connectors actually build/pass their own tests — `tpt-harbor` was not green before this):**
`sources/vector.rs`'s `VectorDbType` was missing `PartialEq`/`Eq` (wouldn't compile), a `collect()` call
in `VectorSource::discover` needed an explicit `Vec<String>` type annotation, and its
`parses_content_length_response` test asserted against a `Content-Length` header that didn't match its
own body's byte length. `sources/influxdb.rs` had an `if`/`else` branch returning differently-sized byte
array literals (wouldn't compile) and the chunked-decoder off-by-one already described above.
`sources/kafka.rs` had two `Ok(x, y)` calls missing the tuple parens `Ok((x, y))` (wouldn't compile), and
its own record-batch decode test encoded records as fixed-width integers when the format (and this file's
own decoder) uses zigzag varints, so the test was silently checking the wrong thing before this pass.
`cargo test` on `tpt-harbor` is green (36/36) as of this pass.

---

## Phase 16 — Synapse: Agent Orchestration & Memory

New module `src/synapse/` inside `tpt-keystone`. Deliberately **not** a new storage engine: every
persistent piece is plain rows in ordinary Keystone tables, indexed by the *existing* Chronos/Prism
local secondary indexes, with task delegation on the *existing* Flux topic/consumer-group machinery —
the same "SQL extension over Keystone core" shape Chronos/Plexus/Canopy/Flux already use. The only
genuinely new engine code is the actor runtime, since nothing else in this codebase provides live
in-process message-passing concurrency.

- [x] Actor model runtime — `synapse/actor.rs`: a real Tokio actor per agent (one task, one
  `mpsc` mailbox), coordinated through `AgentRegistry`. What an agent actually *does* in response to a
  message is a caller-supplied `StepFn` closure — this is infrastructure (mailbox, lifecycle,
  checkpoint persistence), not an LLM agent framework, the same boundary Flux already draws around a
  consumer's business logic
- [x] Agent lifecycle management — spawn/pause/resume/terminate (`AgentRegistry::spawn`/`set_status`),
  checkpoint (`AgentRegistry::checkpoint`) persisted to `_synapse_agents` (Keystone-durable). "Persistent
  session state across restarts": the checkpoint text survives a restart, but there's no automatic
  respawn-on-boot — a caller explicitly calls `resume_from_checkpoint(id, ...)` to rebuild a live actor
  from its last checkpoint, same "no background scheduler" discipline as every other phase
- [x] Agent memory abstraction — `synapse/memory.rs`: all four tiers live in one `_synapse_memory` table
  (a `tier` column), since the Chronos/Prism indexes below are column-scoped, not row-filtered:
  short-term (`tier='short'`, TTL'd + GC'd — "Keystone in-session"), long-term (`tier='long'`, no expiry
  — "Keystone persistent"), episodic (`tier='episodic'`, `CREATE INDEX ... USING TIME(ts)` — "Chronos
  time-indexed"; the Chronos index's required numeric value-column pairing is filled by `seq`, a
  monotonic insert counter, since episodic memory has no intrinsic metric to roll up), semantic
  (`tier='semantic'`, `CREATE INDEX ... USING VECTOR(embedding)` — "Prism vector search", deduplicated
  at write time against the same agent's existing near-identical embeddings rather than swept later)
- [x] Tool registry and discovery — `synapse/tools.rs`: `_synapse_tools` (name, description, an
  OpenAPI/JSON-Schema-shaped definition, optional caller-supplied embedding), discoverable by exact name
  or via the same Prism `VectorIndex` k-NN search as semantic memory. Embedding generation itself is out
  of scope (same boundary Prism's own `VECTOR` columns draw: caller supplies the floats)
- [x] Multi-agent coordination — `synapse/coordination.rs`: task delegation on a per-workflow Flux topic
  (`__synapse_tasks_<workflow>`, single-partition so delegation order is preserved) via
  `delegate_task`/`claim_task`/`complete_task` (manual-ack, same at-least-once contract Flux's consumer
  groups already document), plus shared workflow state (`_synapse_shared_state`). Conflict resolution is
  last-write-wins (`Database::write`'s natural overwrite-by-key semantics) — not a CRDT/vector-clock
  merge, a documented scope cut consistent with this codebase's discipline elsewhere
- [x] MCP server integration — `synapse::invoke_mcp_tool` calls `mcp::tools::call` (now re-exported as
  `mcp::call_tool`) directly in-process, the same dispatcher the wire-level MCP server already uses, no
  network hop needed since Synapse and the MCP server share one `Arc<Database>` process
- [x] Memory GC policies — `MemoryStore::gc()` deletes expired `tier='short'` rows synchronously
  (caller-invoked, no background sweep — same discipline as `TimeIndex::apply_retention`/
  `Partition::apply_retention` elsewhere); long-term is never GC'd; semantic is deduplicated at write
  time instead of swept

Two read-only, ranked-recall SQL table functions — `synapse_recall_semantic(agent_id, query, k)` and
`synapse_discover_tools(query, k)` (`executor/mod.rs`) — expose the k-NN paths to plain SQL, the same
`vector_search`-precedent table-function treatment (a k-NN's "ORDER BY distance LIMIT k" shape doesn't
fit the planner's WHERE-pushdown pattern). Agent lifecycle, task delegation, and shared state are Rust
`Database`-adjacent APIs, not new SQL statements — mirroring Flux's own "polling is a `Database` method,
not new SQL syntax" precedent.

**Milestone verified:** `executor/synapse_tests.rs::milestone_three_agent_workflow_with_shared_state_and_cross_session_recall`
— three agents spawn, claim/complete delegated tasks from one Flux-backed workflow queue, and write
shared state (verified `len() == 3`, all `"done"`); a semantic memory and two tool registrations are
made, then the `Database` is fully closed and reopened at the same on-disk location (a real
close-then-reopen, not just a second in-memory handle) to prove durability across a simulated process
restart; the reopened instance recalls the semantic memory correctly and `synapse_discover_tools`
returns the nearest-ranked tool via SQL. 19 tests total across `synapse::actor`/`memory`/`tools`/
`coordination`'s own unit tests plus `executor/synapse_tests.rs`, all passing against an in-process
`Database`. Not verified: any multi-node/distributed coordination (this is single-node, same scope cut
as every other phase's local secondary indexes), and no LLM/agent "brain" was built or evaluated — this
phase is the orchestration/memory substrate, not an agent implementation.

---

## Phase 17 — Mirror: Agent Observability & Debugging

New module `src/mirror/`, built directly on Synapse (Phase 16) and the primitives it already composes —
Flux for ordered event logs, Chronos for time-indexed metrics, plain Keystone tables for everything
else — the same "SQL extension over Keystone core" shape every prior phase uses, not a sixth storage
engine. Cell encode/decode/id-generation helpers are reused directly from `synapse` (already `pub(crate)`)
rather than duplicated a third time.

- [x] Agent action tracing — `mirror/trace.rs`'s `Tracer`: every decision/tool-call/outcome/error is
  written as an immutable JSON event to a per-session Flux topic (`__mirror_trace_<session_id>`,
  single-partition so the Flux offset is the event's permanent position — no separate sequence field
  needed)
- [x] Session replay engine — `mirror/replay.rs`'s `ReplayEngine::replay_session`, via
  `Database::flux_all` (the same "replay the whole log" primitive `flux_time_travel`/the windowing
  table functions already use) — "using Flux time-travel queries" from the checklist, reusing the
  existing primitive rather than adding a second one
- [x] Debug REPL — scoped to the stepping *engine* (`ReplayEngine::find_first_error`,
  `replay::SessionCursor::step`/`back`/`seek`), not a literal interactive terminal front-end (that
  would be `tpt-cli`'s job, out of scope for this crate — same boundary Chronos/Plexus/Canopy never
  crossed into a UI either). "Inspect agent state at each point" means the accumulated trace events up
  to the cursor's position; an agent's own internal memory beyond what it traced is out of this
  module's reach, the same boundary `synapse::actor`'s caller-supplied `StepFn` already draws
- [x] Performance metrics store — `mirror/metrics.rs`'s `MetricsStore`: `_mirror_metrics` (agent_id,
  session_id, latency_ms, tokens, success, ts) indexed by a *real* Chronos time index on `latency_ms`
  (unlike Synapse's episodic-memory table, where the Chronos value-column pairing is a placeholder) —
  `latency_rollup` reuses `Database::rollup_query`'s count/sum/min/max for free, `success_rate` answers
  the checklist's "success/failure rates" directly
- [x] Compliance auditing — `mirror/audit.rs`'s `AuditLog`: a hash-chained (`sha256`, via the same
  `sha2` crate `objectstore.rs`'s S3 signing already uses — no new crypto primitive) tamper-evident
  audit trail in `_mirror_audit`, one chain per session. `verify_chain` recomputes every hash from
  genesis and detects any entry altered, reordered, or deleted after the fact (a deletion breaks the
  *next* surviving entry's `prev_hash` linkage even though the deleted entry itself is gone) —
  verified in `mirror::audit::tests::tampering_with_a_stored_entry_breaks_the_chain`, which rewrites a
  stored row directly (bypassing `AuditLog::record`) and confirms `verify_chain` catches it
- [x] Provenance tracking on stored data — `mirror/provenance.rs`'s `ProvenanceLog`: `_mirror_provenance`
  records who/what (`source`) asserted a caller-defined `fact_ref` and when, with optional confidence;
  `history`/`latest` let a consumer see how a fact's assertions changed over time, not just the newest
  one. `fact_ref` is a caller convention (a Synapse memory id, a `"table:row_key:column"` triple,
  anything) — this module indexes assertions by it, doesn't interpret it
- [x] OTel span integration — `Tracer::record`/`AuditLog::record` carry `#[tracing::instrument]`,
  reusing Phase 12's existing global `tracing` subscriber (`telemetry::init`) verbatim rather than
  standing up a second OTel pipeline — the same "spans exist and export to OTLP when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set" mechanism every other instrumented path in this codebase
  (`wire::session`, `Database::open`, query execution, lease acquisition) already relies on
- [x] Dashboard — `tpt-canvas/src/components/agent_monitor.rs`'s `Canvas.AgentMonitor` (sixth
  `Canvas.*` component, wired into `tpt-canvas/src/lib.rs`): a DOM-built session event timeline
  from `mirror_session_events(session_id)` with step/back replay controls (`SessionCursor`-style
  clamped cursor, no seek — a click-to-jump UI wasn't built this pass) plus a Canvas2D-drawn
  per-agent latency bar chart from `mirror_agent_metrics(agent_id, t0, t1)`, both as independent
  `use_keystone_query` subscriptions. Same "no browser available in this environment" limitation as
  every other Canvas component (Phase 13): verified via host-side unit tests (`cargo test`, 5
  passing — event parsing/sorting, metric decoding, bar-height scaling, event-kind color fallback,
  cursor clamping) and a clean `cargo build`, not a real rendered browser session.

**Milestone verified:** `executor/mirror_tests.rs::milestone_replay_failed_session_traces_root_cause_and_audit_report_is_tamper_evident`
— a session traces a decision, a successful tool call, then a failing one (`send_email`: "SMTP
connection refused"); `ReplayEngine::find_first_error` and `SessionCursor` stepping both land on the
exact failing tool call (same Flux offset), also reachable via `SELECT ... FROM mirror_session_events(...)`
in plain SQL; `AuditLog::generate_report` auto-generates a report confirming the session's compliance
chain is tamper-evident; `MetricsStore::success_rate` reflects the one failure out of two recorded
calls. 14 tests total across `mirror::trace`/`replay`/`metrics`/`audit`/`provenance`'s own unit tests
plus `executor/mirror_tests.rs`, all passing against an in-process `Database`. Not verified: any
multi-node/distributed replay (single-node, same scope cut as every other phase's local secondary
indexes), and no dashboard UI was built or visually verified.

---

## Phase 18 — Hardening & Follow-ups

Gaps identified while reviewing the backend for frontend (`tpt-appfront`) integration — either genuinely
unbuilt or existing-but-heuristic work worth deepening. Not yet started.

- [x] Deterministic simulation testing (DST) harness for crash recovery — `storage/chaos_tests.rs`'s
  `FaultStore` wraps `ObjectStore` and splits every successful `put`/`put_if_match` into "lost" (crash
  arrives before the write is durable) vs. "committed", with `commit_all`/`commit_by_key` to selectively
  replay only what a real crash would have made durable, plus a `panic_after(n)` counter to fault a
  specific `put` call. Building on the two-in-process-`Database`s-sharing-one-`LocalFsObjectStore` shape
  `phase3_tests.rs` already established. Covers: crash before/after flush (WAL-only vs. SSTable-only
  recovery), a mixed WAL+SSTable crash, crash after UPDATE/DELETE (newest-value and tombstone survival),
  compaction crash before/after the new manifest is committed, zombie-writer flush rejection after lease
  takeover, reader convergence after a writer crash, and a seeded multi-scenario sweep
  (`multi_seed_crash_recovery_with_flush_and_wal_mix`, 30 seeds × 25 rows with randomized flush points,
  not "thousands" as originally envisioned but a real seeded/reproducible sweep, not a single fixed case).
  All 11 scenario tests pass (`cargo test --lib chaos_tests`, both single- and multi-threaded). This pass
  fixed 3 bugs found while getting there, all in the *tests*, not production code (traced by hand through
  `storage::lsm`/`storage::lease` to confirm): (1)
  `compaction_completes_recovers_from_single_merged_sstable` asserted 1 SSTable survives 8 writes/flushes
  at threshold 4, but `compact_all` correctly triggers at `sstables.len() >= 4` and resets to 1, so 8
  flushes legitimately leaves 2 (the 8th flush is mid-cycle); fixed by using 7 flushes, which lands
  exactly on a post-compaction boundary. (2) `zombie_writer_flush_rejected_after_lease_takeover` called
  `db_a.flush()` a second time with an empty memtable — `trigger_flush` returns `Ok(())` for an empty
  memtable *before* ever reaching the manifest CAS that actually enforces fencing (`lease.rs`'s own doc
  comment: fencing is CAS-based, not the `is_valid()` flag), so the "rejection" was never exercised; fixed
  by writing new data for Writer A before the second flush, which now genuinely hits the stale-etag CAS
  rejection. (3) that same test's final assertion then tried to open a *third*, fresh-holder-id `Database`
  against the bucket to inspect state, which Writer B's still-unexpired real lease correctly rejects (the
  same fencing this test exists to prove) — fixed by reading the recovered state through `db_b` (the
  legitimate holder) directly instead. Also fixed a real, pre-existing test-isolation bug unrelated to
  this test's own logic: `TPT_COMPACTION_SSTABLE_THRESHOLD` is process-global env state that `cargo test`
  reads/writes across parallel threads with no synchronization (`storage::lsm`'s own
  `compaction_merges_sstables_and_bounds_the_list` test already had this latent bug); added
  `storage::lsm::COMPACTION_THRESHOLD_ENV_LOCK`, a `#[cfg(test)]` mutex both that test and the new chaos
  test now hold for the override's lifetime — confirmed fixed by re-running the full suite (`cargo test
  --lib`, 372 tests) twice back-to-back with no failures, where it previously flaked. Scope note: this is
  deliberately not a literal SIGKILL/subprocess-kill script — DST is faster to build/run and reproducible
  by seed, and a real OS-signal harness doesn't translate across platforms (`SIGKILL` isn't meaningful on
  this Windows dev host the way it is on Linux CI). Fault injection is at the `ObjectStore` layer only —
  WAL-file-level torn writes were already covered separately by the pre-existing
  `torn_write_recovers_exactly_the_last_durable_prefix` test in this same file.
- [x] Property-based testing (`proptest`) for the MVCC/transaction layer — implemented in
   `storage/mvcc_tests.rs`: `proptest!` generates randomized action sequences
   (200 cases) — interleaved reads / writes / commits / rollbacks across
   transactions — and asserts snapshot-isolation / durability invariants hold
   against an oracle model. (Marked `[ ]` in the original checklist; the harness
   already existed and now passes — corrected here.)
- [x] Wire-level authentication — real SCRAM-SHA-256 (RFC 5802), matching real Postgres exactly so
  unmodified drivers (`psql`, libpq-based clients, JDBC, node-postgres) authenticate with no
  special-casing. Credentials live in a new `_tpt_roles` system catalog table (`wire/roles.rs`,
  `StoredKey`/`ServerKey` only — never the plaintext password), following the same
  `CREATE TABLE IF NOT EXISTS`-at-open-time precedent Synapse/Mirror's own system tables use.
  **Opt-in, not mandatory:** `wire/session.rs::run` only requires the SCRAM exchange when `_tpt_roles`
  is non-empty; an empty catalog (the default) keeps today's unconditional `AuthenticationOk`, so the
  documented zero-config quickstart (`psql -h localhost -p 5432`, no flags) is unchanged. The catalog is
  seeded via `TPT_AUTH_BOOTSTRAP_USER`/`TPT_AUTH_BOOTSTRAP_PASSWORD` (mirrors the existing `TPT_MCP_TOKEN`
  bootstrap-secret precedent) — solves "how do you create the first role with no SQL access yet."
  **Scope cut:** no `CREATE ROLE`/`ALTER ROLE`/`DROP ROLE` DDL yet, and no authorization layer at all —
  every authenticated connection has unrestricted access to everything (authentication only, no
  privilege model). See **Phase 20** for the follow-up that closes this gap. Verified against a real
  `psql` client (not just unit tests) — see
  `wire::scram::tests` for the protocol-level tests (including a regression test for real `libpq`
  sending a `y,,` gs2-header over TLS, which the first implementation didn't handle and failed
  `psql sslmode=require` against a real bootstrap credential until fixed).
- [x] TLS on the Postgres wire listener — `wire/tls.rs`, `rustls`/`tokio-rustls`, the real Postgres
  `SSLRequest` pre-startup negotiation (peek 8 bytes, reply `S`/`N`). Required `wire/codec.rs::Conn`'s
  `stream` field to become a boxed `AsyncRead + AsyncWrite` trait object (`BoxedStream`) instead of a
  hardcoded `TcpStream`, since upgrading a live connection means swapping the underlying stream, not
  wrapping the existing one. **Opt-in:** only negotiated if both `TPT_TLS_CERT_PATH`/`TPT_TLS_KEY_PATH`
  are set (PEM files); otherwise `SSLRequest` is still declined with `N` exactly as before this existed.
  Verified against a real `psql sslmode=require` connection with an `openssl`-generated self-signed dev
  cert, both alone and combined with the SCRAM auth above over the same connection.
- [x] Extend `pg_catalog`/`information_schema` coverage — `pg_constraint`/`pg_attribute`/`pg_index`
   already existed in `executor/catalog.rs`. Direct `SELECT`s against every virtual catalog table work.
   `\dt`'s actual query text now runs and returns the right shape: the four POSIX regex infix operators
   `~`/`!~`/`~*`/`!~*` (`sql::lexer`, `sql::ast::BinOp`, `sql::parser::infix_bp`,
   `executor::eval::regex_is_match` with a per-thread compiled-pattern cache); the `COLLATE <name>` clause
   (parsed and discarded — a single collation exists here, `sql::parser`'s `Token::Collate` handling);
   the `OPERATOR(schema.opname)` schema-qualified infix syntax (`sql::parser`'s `Token::Operator`
   handling, rewritten to a normal `BinaryOp`); `pg_get_userbyid(oid)` (returns the bootstrap role name,
   consistent with `pg_tables.tableowner`) and `pg_table_is_visible(oid)` (resolves the oid back against
   `synthetic_oid` over every table/index, since the engine's search path is just `public`) as new
   built-in functions (`executor/eval.rs`); a `pg_am` virtual table plus `pg_class.relowner`/`relam`
   columns so `\dt`'s `pg_class JOIN pg_am ON relam = pg_am.oid` join resolves (`executor/catalog.rs`);
   and `format_type(oid, typmod)` (`executor::catalog::pg_type_name_by_oid`, the reverse of
   `ColumnType::oid()`) for `\d`'s column-type display. Fixed a real parser bug found while building
   this: `Token::Collate`/`Token::Operator` weren't registered in `infix_bp`, so the Pratt-parser loop
   broke out *before* ever reaching their dedicated handling blocks — every `OPERATOR(...)`/`COLLATE`
   clause was silently ignored rather than parsed, turning e.g. a `JOIN ... ON a.oid OPERATOR(pg_catalog.=)
   c.relam` into an unconditional cross join. Verified end-to-end in `executor::phase4_tests`: the exact
   SQL psql 15 sends for `\dt` now returns the right rows/columns/owner
   (`psql_dt_meta_command_lists_user_tables`), and a `\d`-style query exercising the `pg_am` join +
   `OPERATOR(pg_catalog.~)` + `COLLATE` together returns the correct single row
   (`d_style_query_joins_pg_am_with_operator_and_collate`). Honest remaining gap: only the specific
   functions/joins psql's own `\dt`/`\d` queries exercise are covered — this is not a general OID
   subsystem (no `pg_get_expr`, no `pg_depend`, no full `\d+`/`\di`/`\dv` query shapes tested against a
   live `psql` client yet, only the query text captured from a real psql 15 session).
- [x] Multi-table statistics for the query planner — see Phase 12a's "Query planner statistics" item
   above (cross-referenced, not duplicated work)
- [ ] Harbor production-scale validation — exercise `discover`/`validate`/`transfer`/`replicate`/
  `verify`/`cutover` against a real multi-GB source database with concurrent writes during `replicate`,
  and a real zero-downtime `cutover`, not just the current in-process/loopback verification

**Milestone:** Chaos harness runs unattended overnight against a loaded node with zero data loss — the
in-process DST sweep (`storage/chaos_tests.rs`, 30 seeded scenarios plus 10 other scenario tests, all
passing) is done, but "unattended overnight against a loaded node" implies a longer-running/higher-volume
soak than this pass built regardless, so treat the harness itself as done and the overnight-soak milestone
specifically as not yet attempted; a real Postgres client (psql, an ORM) authenticates over TLS with a
password — **done**, verified
against a real `psql sslmode=require` connection with a bootstrap-seeded SCRAM credential and a self-signed
dev cert.

---

## Phase 19 — Adoption, CI, and Test-Coverage Hardening

Added 2026-07-12 after a test-coverage/adoption assessment: `tpt-keystone` core is strongly tested
(455 tests), but the surrounding project — CLI, SDKs, operator, Harbor's non-relational connectors —
is thinner, and nothing runs automatically on push/PR anywhere in the repo. Tracked here so progress is
checkable the same way every other phase is.

- [x] CI (GitHub Actions) — `.github/workflows/ci.yml` now runs `cargo test` for `tpt-keystone`;
  `cargo build`/`cargo test` for `tpt-cli`, `tpt-sdk` (plus a separate `cargo test --features ffi` job),
  `tpt-harbor`, `tpt-operator`; `cargo build --target wasm32-unknown-unknown` for `tpt-canvas`; `npm test`
  for `packages/sdk-web`/`sdk-server`/`sdk-edge`; `pytest` for `sdk-python` (offline tests); `go test ./...`
  for `sdk-go`. Scope cut: running a live `tpt-keystone` service in-job for `-tags live` Go tests and
  `sdk-python`'s `live_smoke.py` was not attempted — CI runs the offline subset only.
- [x] `docker-compose.yml` at repo root — runs `tpt-keystone`, exposes 5432 (Postgres wire)/5433 (MCP)/
  5434 (Flux WS)/5435 (HTTP)/9187 (metrics), referenced from the root `README.md` quickstart
  (`README.md:101`).
- [x] Test-gap: `tpt-cli` — unit tests added across `data.rs` (4), `format.rs` (6), `main.rs` (5) covering
  output formatting and argument parsing; `migrate.rs`/`repl.rs`/`schema.rs`/`stream.rs` remain untested
  (live-connection-only logic).
- [x] Test-gap: `tpt-sdk` (Rust) — offline tests added: `zerocopy.rs` (5, `RowView` parsing against
  hand-built byte buffers), `keystone/mod.rs` (6, `Row`), `ffi.rs` (3, the C ABI), plus the existing 3 in
  `query_builder.rs`; `client.rs`/`keystone/wire.rs`/`canvas.rs` remain untested (need a live server).
- [x] Test-gap: `tpt-operator` — `resources.rs` grew from a few tests to 10 (Deployment/Service/CronJob
  resource-building), plus `autoscale.rs` (1) and `types.rs` (2); the live reconcile loop itself
  (`reconcile.rs`) is still untested — not separable from `kube::Client` without a fake/mock API server,
  which wasn't built.
- [x] Test-gap: `sdk-go` — offline unit tests added in `wire_test.go` (12, wire encode/decode, error
  mapping) and `connstring_test.go` (1, connection-string parsing); `smoke_test.go` (3) remains
  `-tags live`-gated against a running server.
- [x] Multi-engine example/cookbook — `docs/tutorials/cookbook.md`, a runnable walkthrough touching
  Keystone (core SQL), Meridian (geo), Prism (vector), Chronos (time-series), Plexus (`MATCH`/graph),
  Canopy (JSON/aggregate), and Flux (streaming) in one connected session.


**Milestone: reached.** A newcomer can `git clone` this repo, run one command (`docker compose up`),
connect with `psql`, and run the cookbook script — and every push to the repo gets automated pass/fail
feedback across every crate/SDK, not just whatever the last contributor happened to run locally.

---

## Phase 20 — RBAC Authorization Layer

Phase 18 built wire-level *authentication* (SCRAM-SHA-256) but explicitly scoped out authorization —
today an authenticated connection has unrestricted access to every table. This phase is that follow-up:
a Postgres-style DAC layer (role catalog, membership/inheritance, GRANT/REVOKE object privileges,
superuser bypass), not the Zanzibar-style ReBAC tuple model CLAUDE.md separately anticipates pairing
with it later (ReBAC itself stays out of scope here).

- [x] Role-attribute catalog — extended `_tpt_roles` (`wire/roles.rs`) with `rolsuper`/`rolcanlogin`
  columns, with `migrate_legacy_rows` normalizing any pre-Phase-20 row (at most one — the bootstrap role)
  to `SUPERUSER LOGIN` in place, matching what `bootstrap_if_empty` would have created; `bootstrap_if_empty`
  marks the env-var-seeded first role superuser via `upsert_with_attrs`.
- [x] Role membership — new `_tpt_role_members` system table + `wire/role_members.rs::RoleMemberStore`
  (`grant_membership`/`revoke_membership`/`revoke_all`/`direct_memberships`/`all_memberships` transitive
  closure via BFS, cycle rejection on grant checked in both directions). Scope cut: no `WITH ADMIN OPTION`,
  no `NOINHERIT` roles.
- [x] Object privileges — new `_tpt_privileges` system table + `wire/privileges.rs::PrivilegeStore`
  (`PrivilegeRepr`: Select/Insert/Update/Delete/Create/Drop/Usage/All; `GrantObjectRepr`: `Table`/`Database`;
  `has_privilege` walks the requesting role's transitive memberships too). Scope cuts: no column-level
  privileges, no `WITH GRANT OPTION` re-delegation, no `ALTER DEFAULT PRIVILEGES`, no object-ownership model
  (a non-superuser's own `CREATE TABLE` grants no implicit privileges on it — must be separately `GRANT`ed
  or be superuser), no `SET ROLE`/`SET SESSION AUTHORIZATION`.
- [x] Parser/AST — `CREATE ROLE`/`ALTER ROLE`/`DROP ROLE`/`GRANT`/`REVOKE` (`sql/ast.rs`, `sql/lexer.rs`,
  `sql/parser.rs`), including `GRANT role TO role` vs. `GRANT priv ON obj TO role` disambiguation. Scope
  cut: no `GRANT ... ON ALL TABLES IN SCHEMA ...` (no schema/namespace concept in this codebase —
  `ON DATABASE` is the only whole-instance granularity).
- [x] Session identity threading — `executor/rbac.rs::Actor` (rolname/superuser/transitive memberships),
  built once per connection right after the SCRAM handshake in `wire/session.rs::run()` (or
  `Actor::unrestricted()` when `_tpt_roles` is empty, preserving the zero-config default), threaded through
  `run_query_loop`/`execute_simple`/the new `executor::execute_parsed_as` (the old `execute_parsed` now
  delegates to it with an unrestricted actor, for trusted in-process/internal/test callers).
- [x] Enforcement — `executor/rbac.rs::Actor::check()` (superuser/unrestricted short-circuit, reserved
  `_tpt_*` system-catalog guard, then per-`Stmt`-arm privilege checks via `PrivilegeStore::has_privilege`,
  which also walks membership); denial returns `InsufficientPrivilege`, a downcastable marker error that
  `wire/session.rs::sqlstate_for` maps to SQLSTATE `42501` (vs. the generic `42601` for everything else).
  `CREATE ROLE`/`ALTER ROLE`/`DROP ROLE`/`GRANT`/`REVOKE` are superuser-only, no delegation model this
  pass. A role without `LOGIN` is also rejected at `authenticate()`, before any SCRAM exchange.
- [x] `pg_catalog` surface — `pg_roles`/`pg_auth_members` virtual tables added to
  `executor/catalog.rs::resolve_virtual_table`, following the existing synthesized-OID virtual-table
  pattern.
- [~] Tests — `sql/parser_tests.rs` has `Stmt`-shape coverage for all five statement kinds (154 new lines).
  **Still missing:** store-level round-trip tests for `RoleMemberStore`/`PrivilegeStore` (no
  `wire/role_members_tests.rs`/`wire/privileges_tests.rs` yet — zero `#[test]`s in either new file), an
  `executor/rbac_tests.rs` integration suite (per-statement allow/deny, superuser bypass, membership
  inheritance, the `_tpt_roles`-empty no-op regression, system-catalog write protection, admin-only DDL),
  and a wire-level end-to-end test asserting a denied query's `ErrorInfo` carries SQLSTATE `42501`.

**Milestone:** met functionally — a bootstrapped superuser can `CREATE ROLE`/`GRANT` a restricted role via
SQL alone (no further env-var dependency), that role is denied access to tables/statements it hasn't been
granted (SQLSTATE `42501`), and the zero-config (`_tpt_roles` empty) quickstart remains behaviorally
unchanged. Not yet verified by an automated integration/e2e test — see the Tests bullet above.

---

## Open / Remaining Tasks

Consolidated view of every unchecked (`[ ]`) / partial (`[~]`) item across the roadmap.

> **Note (2026-07-12, earlier pass):** three items previously listed as `[ ]` were already
> implemented in code but stale in this checklist — S3 key-prefix sharding
> (`storage/lsm.rs`), the object-store circuit breaker / memory backpressure
> (`storage/guard.rs`), and MVCC property-based testing (`storage/mvcc_tests.rs`).
> They are now marked `[x]` above and verified. That pass also closed out the
> `pg_catalog`/psql meta-command coverage item: the four POSIX regex infix
> operators, `COLLATE`, `OPERATOR(...)`, `pg_am`/`relam`, `pg_get_userbyid`,
> `pg_table_is_visible`, and `format_type` are now implemented, and a real
> parser bug (`Token::Collate`/`Token::Operator` missing from `infix_bp`,
> silently no-op'ing both clauses) was found and fixed along the way.

> **Note (2026-07-12, this pass):** every item this checklist marked as a genuinely-buildable-here
> scope cut (as opposed to needing external infrastructure/hardware this environment doesn't have)
> is now implemented: Prism's DiskANN index (`vector::vamana` + `storage::diskann_index`) and
> consistent hashing for vector shards (`vector::shard` + `storage::sharded_vector_index`); Canopy's
> MongoDB-compatible aggregation pipeline (`executor::canopy_aggregate`, exposed as
> `aggregate(table, pipeline_json)`); Plexus's GQL layer, now a real (explicitly scoped-down) `MATCH`
> statement (`sql::ast::MatchStmt`/`executor::gql`) rather than nothing. Canvas's WebGPU plugin API
> turned out to already exist (`packages/sdk-web/src/plugin.ts`/`plugin-gpu.ts`, 33/33 tests passing)
> and was simply mismarked `[ ]` — corrected, no new code needed. Each item above documents its own
> honest remaining scope cuts (none of these claim full parity with the systems they're modeled on);
> see each for detail rather than assuming "implemented" means "complete."

> **Note (2026-07-12, adoption/coverage pass):** a test-coverage and adoption assessment found
> `tpt-keystone` core strongly tested (455 tests) but the surrounding project thin — no CI anywhere in
> the repo, weak/zero test coverage in `tpt-cli`/`tpt-sdk`/`tpt-operator`/`sdk-go`, no one-command
> quickstart, and five Harbor source connectors still genuine `unimplemented_source!` stubs. Tracked as
> **Phase 19** above (CI, docker-compose, test gaps, cookbook) plus a new Harbor follow-up list under
> Phase 15 (replacing the InfluxDB/Kafka/Vector/Oracle/Elasticsearch stubs with real connectors, "no
> stubs" per explicit direction — Oracle flagged as a real environment-dependency risk given its OCI
> native-library requirement).

> **Note (2026-07-12, later same day — Phase 19 closeout):** every Phase 19 checklist item is now `[x]`:
> `.github/workflows/ci.yml` covers every crate/SDK, `docker-compose.yml` + README quickstart exist,
> `tpt-cli`/`tpt-sdk`/`tpt-operator`/`sdk-go` all gained offline unit tests (each honestly documents what
> remains untestable without a live server or a mocked `kube::Client`), and `docs/tutorials/cookbook.md`
> exercises all seven engines in one session. The Phase 15 Harbor stub-connector follow-up was unrelated
> and, as of the Harbor stub-connector closeout note further down this section, is now closed too.

### Engine gaps (documented scope cuts — not attempted; need external infra/hardware this
environment doesn't have, not just more engineering time)
- **Phase 11 — Flux:** gRPC streaming endpoint (`TODO.md:398`) — a real gRPC/HTTP2/protobuf stack is
  a separate, large from-scratch effort on the order of the hand-written Postgres wire protocol itself;
  unlike this pass's other items, partial progress here wouldn't be a complete, testable unit

### Follow-ups / hardenings (genuinely unbuilt or partial; need real external systems)
- **Phase 12 — Production Hardening:** Formal benchmark suite vs Postgres/InfluxDB/Neo4j/MongoDB/Kafka
  (`[~]`, scoped to a `criterion` harness measuring Keystone alone — `tpt-keystone/benches/keystone_bench.rs`,
  already implemented and measured on this dev machine; the head-to-head comparison against the other
  four systems needs them actually installed, which this environment doesn't have) (`TODO.md:424`);
  Apache 2.0 open-source *release* step (already Apache-2.0 licensed — the publishing act itself is a
  decision for the maintainer to make, not something to do autonomously) (`TODO.md:459`)
- **Phase 18 — Hardening & Follow-ups:** Harbor production-scale validation against a real multi-GB
  source DB with concurrent writes + true zero-downtime cutover (`TODO.md:1046`) — not attempted (needs
  a real external database)

### SDKs (Mobile) — entirely unbuilt
- **Phase 14 — SDK/Mobile:** `@tpt/sdk-react-native` (`TODO.md:577`); Flutter SDK (`TODO.md:578`);
  Swift SDK (iOS) (`TODO.md:579`); Kotlin SDK (Android) (`TODO.md:580`). Checked this dev machine's
  toolchains: Node/npm is present (React Native is scaffoldable), Flutter is installed at `D:\flutter`
  (though full Android/iOS build-target configuration wasn't verified), but there is no Swift/Xcode on
  Windows at all — the iOS SDK specifically cannot be built in this environment regardless of engineering
  time spent.

> **Note (2026-07-12, later same day — Harbor stub-connector closeout):** all five `unimplemented_source!`
> stubs (InfluxDB, Kafka, Pinecone/Weaviate/Qdrant, Elasticsearch, Oracle) now have real client code —
> see the Phase 15 follow-up note above for the per-connector detail and confidence tiers. Four are the
> same "fully public protocol, unverified against a live server" tier as this crate's other connectors.
> Oracle is explicitly flagged as a materially higher-risk tier: its TNS packet-framing is public and
> solid, but the TTC login/query layer inside it has no public specification, so that part is a
> best-effort reconstruction rather than validated protocol code — do not treat it as equivalent to the
> other nine Harbor source connectors without a correction pass against a real packet capture or a
> rewrite against Oracle Instant Client (OCI). `cargo test` on `tpt-harbor` is green (36/36); three
> unrelated compile errors and three test bugs found in the InfluxDB/Kafka/Vector connectors while
> verifying this were fixed along the way (see the follow-up note above for exactly what).

### Authorization — buildable here, no external infra needed
- **Phase 20 — RBAC Authorization Layer:** functionally complete — role catalog (`rolsuper`/`rolcanlogin`
  + legacy-row migration), role membership with transitive closure, object privileges, `CREATE`/`ALTER`/
  `DROP ROLE`/`GRANT`/`REVOKE` parsing and execution, per-connection `Actor` enforcement (SQLSTATE
  `42501` on denial), and a `pg_roles`/`pg_auth_members` `pg_catalog` surface are all wired up — see
  Phase 20 above. **Remaining:** only the test suite — no store-level round-trip tests for
  `RoleMemberStore`/`PrivilegeStore`, no `executor/rbac_tests.rs` integration suite, no wire-level e2e
  test for the `42501` SQLSTATE. Parser shape tests exist; enforcement itself is unverified by automated
  tests. Pure in-repo engineering with no external dependency.

**Remaining:** 1 engine gap needing infrastructure this environment lacks (gRPC), 3 follow-ups needing
real external systems (cross-engine benchmarks, Harbor at scale, and the release-publishing decision
itself), 4 unbuilt mobile SDKs (one of which, Swift/iOS, is blocked on platform availability, not
effort), and 1 fully in-repo authorization layer (Phase 20 — RBAC) not yet started. Phase 19
(adoption/CI/test-coverage hardening) and Harbor's 5 stub-connector replacements are now both fully
closed out — see their respective closeout notes above (Oracle's connector carries an explicit,
higher-than-usual confidence caveat rather than being unqualified). Every item that was a pure
"more code, no external dependency" scope cut *from every prior pass* has been closed out except the new
Phase 20 RBAC work; what else remains either needs real external infrastructure/hardware, or is blocked
on platform availability.

---

*All engines + SDKs: Apache 2.0 licensed. Built in Rust. Cloud-native from day one.*