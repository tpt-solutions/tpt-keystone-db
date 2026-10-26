# Architecture

Keystone is a single Rust binary (`tpt-keystone/src/main.rs`) with no external
runtime dependencies beyond an optional S3-compatible object store. Four
modules, each a thin layer over the one below:

```
 client (psql, any Postgres driver, or a TPT SDK)
     │  Postgres wire protocol v3
     ▼
 wire/            hand-written codec + session state machine
     │  parsed Stmt
     ▼
 sql/             hand-written lexer → recursive-descent parser → AST
     │  Stmt
     ▼
 executor/        turns a Stmt into a QueryResult
     │  StorageEngine trait calls
     ▼
 storage/         LSM engine + MVCC + cloud-native object-store layer
```

Neither `pgwire` nor `sqlparser-rs` (nor any similar crate) is used anywhere
— the wire protocol codec and the SQL lexer/parser/AST are hand-written from
scratch by deliberate project design, not a gap waiting to be filled with a
library. See [`CLAUDE.md`](../CLAUDE.md) for the full list of hard
constraints (e.g. Row-Level Security is permanently out of scope in favor of
a Zanzibar-style ReBAC model, once access control is built).

## `wire/` — Postgres wire protocol v3

- `codec.rs` frames raw TCP bytes into `FrontendMessage`/`BackendMessage`.
- `messages.rs` defines the message types and Postgres type OIDs.
- `session.rs` drives one client connection: startup handshake → simple
  query loop, plus the extended query protocol (Parse/Bind/Describe/
  Execute/Sync/Close) tracked per-connection via `ExtendedState` (prepared
  statements, bound portals, `DECLARE CURSOR` state).
- Cursors and `LISTEN`/`NOTIFY` only work over the **simple** query
  protocol, not extended — a documented scope cut, not an oversight.
- `http_query.rs` (Phase 13) is a second, much smaller entry point: a plain
  HTTP/JSON bridge (`POST /query`, `GET /schema`) so a browser — which can't
  speak the Postgres wire protocol directly — can still run SQL. Backs
  `tpt-keystone-canvas` and every browser-facing SDK package.
- `websocket.rs` (Phase 11) is a third entry point: a hand-rolled RFC 6455
  server that pushes Flux topic records to subscribed clients in real time.
- `grpc/` is a fourth: the Flux gRPC endpoint, over a hand-rolled HTTP/2 (h2c)
  transport with HPACK and protobuf. No `tonic`/`h2`/`prost`. The contract is
  in [`formats/flux_grpc.proto`](formats/flux_grpc.proto).
- `scram.rs`, `roles.rs`, `role_members.rs`, `privileges.rs` implement
  SCRAM-SHA-256 login and the role catalog, driven by the `_tpt_roles` table.
  While `_tpt_roles` is empty, every connection is accepted with no login. That
  is the zero-config default.
- `tls.rs` is optional TLS. The server answers an `SSLRequest` with `S` only
  when both `TPT_TLS_CERT_PATH` and `TPT_TLS_KEY_PATH` are set. Otherwise it
  answers `N` and stays plaintext.
- `bridge_auth.rs` is the one auth helper shared by the HTTP, WebSocket and gRPC
  listeners (`Authorization: Basic`) and by the MCP server (token gate, plus
  an RBAC actor). See [`security_audit_phase12.md`](security_audit_phase12.md).

## `sql/` — lexer, parser, AST

`sql::parse()` is the single entry point. Tokenizes, then recursive-descent
parses into `ast.rs`'s `Stmt` node types (`Select`, `Insert`, `CreateTable`,
...). No separate optimizer pass lives here — the executor's planner
(below) does cost-aware rewriting directly against the parsed `Stmt`.

## `executor/` — query execution

- `mod.rs` — `execute_query(sql, db)` is the entry point; dispatches DDL/DML
  directly and drives `execute_select_with_cte` for `SELECT`: materialize
  CTEs → resolve FROM/JOINs → apply WHERE → aggregate (if GROUP BY/aggregate
  present) → window functions → ORDER BY → LIMIT/OFFSET → project.
- `graph_fn.rs` — every engine-specific **table-valued function**:
  `vector_search`, `hybrid_search`, `graph_bfs`, `json_text_search`,
  `flux_window_tumbling`, `synapse_recall_semantic`, `mirror_session_events`,
  and the rest. They are dispatched from the FROM clause, not from `mod.rs`,
  because a "return ranked/traversed/windowed rows" shape doesn't fit the
  planner's WHERE-clause pushdown pattern the way an index rewrite does.
- `gql.rs` — the GQL-subset `MATCH` statement, which chains graph-index
  lookups hop by hop.
- `ddl.rs` / `dml.rs` / `select.rs` / `join.rs` / `copy.rs` — DDL, DML, the
  SELECT pipeline, joins, and `COPY ... FROM STDIN`/`TO STDOUT`.
- `udf.rs` — sandboxed WASM UDFs (`CREATE FUNCTION ... LANGUAGE wasm`). These
  sit behind the `wasm-udf` Cargo feature.
- `rbac.rs` — per-table privilege checks. The wire, HTTP and MCP entry points
  pass an `Actor` into them.
- `eval.rs` — expression/aggregate evaluation; the `Value` enum and
  `RowContext` (the scope an expression evaluates against: a row, the
  outer-query scope for correlated subqueries, bound parameters).
- `catalog.rs` — resolves `pg_catalog`/`information_schema` system tables
  live from the schema/index catalog, so `\d`/`\dt`-style introspection
  queries work as plain SQL.
- `planner.rs` — join/scan strategy. Equi-joins use a hash join (build side
  = the smaller input); everything else falls back to nested-loop. Also
  where index-aware predicate rewrites live: `extract_spatial_predicate`
  (Meridian), `extract_time_bucket_predicate` (Chronos),
  `extract_spatial_join_predicate` (Meridian GPU joins) — each recognizes a
  specific WHERE-clause shape and answers it via a secondary-index lookup
  instead of a full scan.

The executor is otherwise stateless: no query plan caching (a shared
`sql/cache.rs::StatementCache` caches *parsed* statements, not execution
plans) and no persistent session state beyond what `wire::session` keeps.

## `storage/` — the LSM engine

The largest module, and the one every other engine's "secondary index" is
layered on top of.

**Core LSM:**
- `wal.rs` — batch-framed write-ahead log, one fsync per commit. See
  [`formats/wal.md`](formats/wal.md). `io_backend.rs` supplies the append
  backend: portable `std::fs` by default, or io_uring on Linux with
  `TPT_IO_URING=1`.
- `lsm.rs` — MemTable (lock-free `SkipMap`) + SSTable-based engine with
  levelled compaction. Writers take one `write` mutex. Readers take a brief
  `view` lock to clone an `Arc`, and never block on I/O. See
  [`concurrency_model_audit.md`](concurrency_model_audit.md).
- `sstable.rs` — SSTable format + bloom filters. Every version is kept, keyed
  by `InternalKey`. See [`formats/sstable.md`](formats/sstable.md).
- `internal_key.rs` — `InternalKey`: user key, commit `seq`, and a put/delete
  `tag`. Its ordering is user key ascending, then `seq` descending.
- `mvcc.rs` — now only `new_tx_id()`. The earlier `MvccStore` and
  `TransactionManager` were removed. See
  [`mvcc_snapshot_audit.md`](mvcc_snapshot_audit.md) for how snapshot
  isolation got here.
- `database/txn.rs` — per-connection `TxnHandle`. It holds the staging buffer
  of uncommitted writes and the `snapshot_seq` taken at `BEGIN`, which gives
  snapshot isolation. The snapshot is released on drop, so an abandoned
  transaction can't pin compaction forever.
- `btree.rs` — local B-Tree secondary indexes. Deliberately **local-only**,
  not replicated through the object store (a Phase 3 scope cut) — rebuilt
  per-node from local disk on open, same as every other secondary index
  below.

**`database/`** ties all of the above together. `database/mod.rs` defines
`Database`, which implements the `StorageEngine` trait the executor calls
against. It owns the schema catalog (stored in the shared object store under
`schemas/` so every node sees the same tables) and every per-engine secondary
index (below). The submodules are split by concern: `catalog`, `storage_engine`,
`txn`, `json`, `spatial`, `time`, `graph`, `vector`, and `flux`.

**Cloud-native layer (Phase 3) — compute/storage disaggregation:**
- `objectstore.rs` — the `ObjectStore` trait, plus `S3ObjectStore` (real
  `aws-sdk-s3`, conditional `If-Match`/`If-None-Match` PUTs for CAS) and
  `LocalFsObjectStore` (single-shared-bucket emulation for dev/test — what
  every `*_tests.rs` module and this repo's benchmark harness use).
- `cache.rs` — `CachedObjectStore`, an NVMe cache-aside layer. Only
  immutable `sst/`/`wal/` objects are cached; manifest/lease reads always
  go to the backing store fresh (they must be, for correctness).
- `manifest.rs` — the single-writer/multi-reader shared manifest.
- `lease.rs` — CAS-based write lease with a monotonic fencing token, so a
  superseded writer's manifest CAS is rejected even if it never noticed its
  own lease expired.
- `guard.rs` — a circuit breaker and an in-flight concurrency bound wrapping
  any `ObjectStore`, so a slow or unreachable object store fails fast instead
  of stalling queries.
- `config.rs` — reads all of the above from `TPT_*` env vars (see
  [`CLAUDE.md`](../CLAUDE.md) for the full list).

**Per-engine secondary indexes** (each: local-only, rebuilt from its own
on-disk file on open. Most are append-only record logs. Canopy's are
whole-file blobs, and DiskANN is a fixed-record flat file. Each is a genuinely
separate concern from the base row-oriented storage above):
- `geo_index.rs` (Meridian, Phase 6) — S2-inspired cell → row-key buckets.
- `ts_index.rs` (Chronos, Phase 8) — time-bucketed pages with Gorilla /
  delta-of-delta compression (`compress.rs`).
- `graph_index.rs` (Plexus, Phase 9) — `AdjacencyGraph` adjacency lists.
- `canopy_index.rs` (Canopy, Phase 10) — `JsonPathIndex` (equality lookup)
  and `FtsIndex` (inverted index with per-row term frequency, backing both
  boolean AND search and BM25 ranked search).
- `vector_index.rs` (Prism, Phase 7) — HNSW graph (`vector/hnsw.rs`).
- `flux.rs` (Flux, Phase 11) — append-only partitioned log + consumer-group
  offsets; also the transport CDC events and `synapse`/`mirror`'s
  Flux-backed logs ride on.
- `ivf_pq_index.rs` (Prism) — IVF-PQ index, `USING IVFPQ`. It keeps a raw
  vector log, and retrains the model from that log on every open. See
  [`formats/ivf_pq_index.md`](formats/ivf_pq_index.md).
- `sharded_vector_index.rs` (Prism) — an HNSW index split across N shard files
  by consistent hash. Each shard is a standalone `VectorIndex`.
- `diskann_index.rs` (Prism) — a disk-resident Vamana graph in one flat file.
  It has a Rust API only and is not reachable from SQL. See
  [`formats/diskann_index.md`](formats/diskann_index.md).
- `json_schema.rs` / `jsonb.rs` (Canopy) — JSON Schema validation for
  `json_schema_col`, and the internal binary JSON encoding used for index keys.

## Why one binary, not seven engines/crates

Every "engine" in the roadmap (Meridian/geospatial, Prism/vector,
Chronos/time-series, Plexus/graph, Canopy/document, Flux/streaming,
Synapse/agent-orchestration, Mirror/observability) is a SQL extension over
this one `Database` + LSM storage, not a separate storage engine or wire
protocol. A `VECTOR` column, a `GEOMETRY`-as-WKT-text column, and a `JSON`
column all live in the same row, same table, same MVCC/WAL path as every
other column type — so there's no distinct transaction domain to span
between them, and no second wire protocol (e.g. MongoDB's) was ever
attempted. The one deliberate exception is Synapse's actor runtime
(`src/synapse/actor.rs`): live in-process message-passing concurrency isn't
something any SQL extension can express, so that one piece is genuinely new
engine code rather than rows-plus-an-index.
