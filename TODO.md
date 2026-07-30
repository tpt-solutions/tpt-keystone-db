# TODO — Platform Hardening, Frontend Build-Out & Release Prep

This is a fresh task list superseding the prior phase-by-phase build log (preserved for reference,
untouched, at `TODO 1260716.md`). It tracks a new body of work identified by a full-repo review:
real transactions, security hardening of the non-Postgres network bridges, a Canvas frontend
build-out, dual licensing, adoption tooling, and crates.io release prep. See
`C:\Users\Phillip\.claude\plans\review-project-fix-any-hazy-hennessy.md` for full context/rationale
on each item.

Legend: `[ ]` not started, `[~]` in progress, `[x]` done.

## Phase 1 — Real transactions (highest risk, highest value)

- [x] Stage 1: per-connection `TxnHandle` (`storage/database/txn.rs`) threaded through the executor
      (`execute_parsed_as`'s `txn` param) and `wire/session.rs` (simple + extended query loops manage
      the per-connection `txn`); staged writes during an open transaction, atomic replay into the
      committed LSM on `COMMIT` (`Database::commit_txn`), full discard on `ROLLBACK`
      (`Database::rollback_txn`); read-committed semantics (no snapshot isolation yet). Covered by
      `executor/transaction_tests.rs` (atomicity, cross-connection isolation, idempotent
      COMMIT/ROLLBACK, read-committed visibility). **Fix (this pass):** the working tree had stale
      call sites still invoking the old 4-arg `execute_parsed(stmt, db, params, None)` and a
      `ColumnDef` with removed `is_unique`/`references` fields — fixed in `rbac_tests.rs`,
      `wire/session.rs`, and `transaction_tests.rs` so the test tree compiles and passes.
- [x] Stage 2 audit: snapshot isolation using `storage/mvcc.rs`'s version-chain design — audit written
       to `docs/mvcc_snapshot_audit.md` (reusability verdict + risks + recommended order). Verdict:
       `MvccStore`'s version-chain data model and `read_version` visibility rule are reusable, but the
       store is RAM-only and **not wired into any read/write path**; `TransactionManager` is currently
       dead weight. Real snapshot isolation is blocked on Stage 3 (durable multi-version storage in
       `lsm.rs`/`sstable.rs`/`wal.rs`) — tracked as the contiguous Stage 2+3 effort in the audit.
- [~] Stage 2+3: durable multi-version storage + real snapshot isolation, per the RocksDB/LevelDB-style
       internal-key encoding approach (plan: `C:\Users\Phillip\.claude\plans\is-it-good-idea-majestic-mitten.md`).
       New `storage/internal_key.rs` (`InternalKey{user_key,seq,tag}`, custom `Ord` comparing `user_key`
       in isolation first so a byte-prefix relationship between two user keys can't be disturbed by the
       version suffix). Done: `sstable.rs` (`IndexEntry` +seq/tag, bloom stays keyed on user key only,
       `read_at`/`scan_all_versions`, `max_seq`); `wal.rs` (rewritten to batch-framed records —
       `marker|commit_seq|count|record*count` — so `append_batch` is one write+fsync per commit and
       `replay` accepts/discards a whole batch atomically, closing the old "torn subset of a multi-row
       commit" gap for free, not just adding versioning); `lsm.rs` (`MemTable` keyed by `InternalKey`,
       `next_commit_seq`/`open_snapshots` watermark tracking, `read_at`/`scan_at`/`write_batch`,
       back-compat `read`/`scan`/`write`/`delete` wrappers so non-transactional call sites are
       unaffected, `open()` seeds `next_commit_seq` from WAL+SSTables+manifest so a restart never
       reissues a seq, `compact_all` rewritten to keep every version newer than the oldest open
       snapshot plus one "floor" version below it). Still open: `storage/database/txn.rs`
       (`snapshot_seq` field + `Drop`-based snapshot release), `storage/database/mod.rs` wiring
       (`begin_txn` captures the snapshot, `txn_read`/`txn_scan` call `read_at`/`scan_at`, retire dead
       `tx_mgr`/`mvcc` fields, delete `storage/tx.rs`, repurpose `storage/mvcc.rs` to just `new_tx_id()`),
       `wire/session.rs` (extended-protocol BEGIN/COMMIT/ROLLBACK interception — today only the simple
       query protocol manages `txn`, a real pre-existing gap this work surfaced — plus fixing `Sync`'s
       hardcoded `TransactionStatus::Idle`), inverting `read_committed_sees_other_transactions_committed_writes`
       in `executor/transaction_tests.rs` now that it should be the opposite, new snapshot/compaction/
       wire tests, porting `storage/mvcc_tests.rs`'s proptest off the now-deleted `MvccStore`, and adding
       `Manifest::max_commit_seq` (`#[serde(default)]`).
- [ ] Concurrency model (audit's Stage 4): the current single coarse `Mutex<LsmEngine>` is **explicitly
       out of scope** for the Stage 2+3 work above (user decision, see the plan file) — readers not
       blocking writers once the read path is version-chain based is a separate follow-up once snapshot
       isolation itself is proven correct, not bundled into this pass.
- [x] Extended query protocol (Parse/Bind/Execute/Sync) and simple query protocol both correctly
      interact with the new per-connection transaction state (`wire/session.rs` threads `txn` into
      both `execute_simple` and the `Execute` handler)
- [x] Extend the test suite with atomicity/isolation tests (`executor/transaction_tests.rs`); crash-
      mid-transaction coverage is partially implied by ROLLBACK-discard tests but not yet a dedicated
      `chaos_tests.rs` crash case
- [ ] Manual verification: concurrent `BEGIN`/`COMMIT`/`ROLLBACK` from two `psql` sessions (needs a
      running node; not yet scripted)

## Phase 2 — DDL/catalog bug fixes ✅ done (`cfdafce`)

- [x] `CREATE SEQUENCE IF NOT EXISTS` not enforced — added `if_not_exists` to `CreateSequenceStmt`
      (`sql/ast.rs`), wired through `parser.rs` and `execute_create_sequence` (`ddl.rs`)
- [x] `DROP TABLE` is a complete no-op — added `Database::drop_table` (schema removal, row-data purge,
      per-table secondary-index cleanup incl. spatial/time/graph/JSON/FTS/vector/IVF-PQ, implicit
      `__cdc_<table>` Flux topic cleanup); `if_exists` wired; the Phase-3 reader-node convergence gap
      (`refresh()`'s `.entry().or_insert()` never removing dropped schemas) is documented in
      `catalog.rs` as a known follow-up, not fixed here
- [x] `ALTER TABLE ADD/DROP COLUMN` no-op — added `Database::alter_table_add_column`/
      `alter_table_drop_column` (single LSM-mutex hold across the whole scan-rewrite pass, no
      `StorageEngine` trait re-entry); `DROP COLUMN` rejected on PK/unique/FK/indexed columns; `ADD
      COLUMN` with `NOT NULL` and no default is rejected rather than silently backfilling NULL;
      non-crash-atomicity and global-lock duration remain accepted limitations (documented in code)
- [x] `ddl_tests.rs` added, covering all three fixes plus the `DROP COLUMN` rejection paths and the
      `ADD COLUMN NOT NULL`-without-default rejection

## Phase 3 — Auth + rate limiting for HTTP/WebSocket/gRPC/MCP bridges ✅ done (`cfdafce`)

- [x] `wire::bridge_auth` module: `authenticate_basic` (HTTP/WebSocket/gRPC, zero-config-preserving —
      `roles.is_empty()?` short-circuits to `Actor::unrestricted()` exactly like `session::run`) and
      `actor_for_mcp` (resolves an `Actor` for the existing `X-TPT-Token` gate, requiring a superuser
      role to act as when a token gate is configured)
- [x] HTTP (`Authorization: Basic`) + WebSocket (at Upgrade) + gRPC (metadata header) accept Basic
      auth via the shared helper; MCP keeps its `X-TPT-Token` gate, now resolving an `Actor` too
- [x] `Actor` threaded into `http_query.rs` (`execute_parsed_as`) and `mcp/tools.rs`/`protocol.rs`
      (`query`/`mutate`/`related` tools) for real per-table RBAC
- [x] Rate limiting: `TPT_HTTP_MAX_CONNECTIONS`, `TPT_FLUX_WS_MAX_CONNECTIONS`,
      `TPT_FLUX_GRPC_MAX_CONNECTIONS`, `TPT_MCP_MAX_CONNECTIONS` (default 1000) — `tokio::sync::
      Semaphore` acquired per-connection in `main.rs`, held for the connection's lifetime
- [x] `bridge_auth_tests.rs` (new); `websocket_tests.rs`/`http_query_tests.rs`/`mcp/tests.rs`/
      `mcp/protocol_tests.rs`/`mcp/tools_tests.rs` extended
- [x] Document (don't fix) that `websocket.rs`/`wire/grpc/mod.rs` get authentication only, not
      per-topic authorization, since there's no topic-level privilege model in `rbac.rs` — recorded in
      `docs/security_audit_phase12.md` §4 ("still open") this pass
- [x] Update `docs/security_audit_phase12.md` (it predated Phase 20 RBAC and the Phase 3 bridge-auth
      work and never scoped the four non-Postgres listeners) — §4 added this pass, noting the auth
      gate is now present on all five listeners and the per-topic authorization gap remains open
- [x] Extend `tools/verify_flux_grpc.py` to exercise the gRPC Basic-auth gate when
      `TPT_AUTH_BOOTSTRAP_USER`/`TPT_AUTH_BOOTSTRAP_PASSWORD` are set (asserts a request without the
      header is rejected with UNAUTHENTICATED); zero-config path unchanged

## Phase 4 — Canvas frontend build-out

- [x] Demo app (`tpt-canvas/examples/dashboard/`, Vite+TS) mounting all 6 `Canvas.*` components
       against a live `tpt-keystone` node — first-ever browser verification of this crate. Created this
       pass (`index.html`, `src/main.ts`, `src/tpt-canvas.d.ts`, `vite.config.ts`, `package.json`,
       `tsconfig.json`, `README.md`). **Now fully runnable + verified**: the crate was unbuildable
       for wasm32 (fixed: `map.rs` unclosed delimiter + missing `parse_lat_lon`; `theme.rs`'s
       `apply_theme` took a non-wasm-bindgen `Theme` by ref; `client.rs`/`document.rs`/`graph.rs`
       had missing imports, a buggy `infer_topic_from_sql` JOIN check + broken `flatten_json`; and
       `CanvasGraph` had two `#[wasm_bindgen(constructor)]`s which broke `wasm-bindgen` codegen —
       `new_from_match` is now a static `fromMatch` factory). `cargo build --target wasm32-unknown-unknown`
       + `wasm-bindgen --target web` now emit `pkg/`, and `npm run build` (Vite) bundles all
       6 components + WASM cleanly. A zero-dependency `mock-server.mjs` emulates `POST /query` +
       `GET /schema` with seeded data, so `npm run mock` + `npm run dev:mock` renders the whole
       dashboard offline (no live node), which is the genuinely useful verification path.
- [x] GQL `MATCH` support in `CanvasGraph` (`new_from_match` + `translate_match_result` client-side
       result-shape translator) — already implemented; no server changes needed
- [x] Design tokens/theming (`theme.rs` + CSS variables) for `document.rs`/`vector_search.rs`/
       `agent_monitor.rs` — already implemented (`Theme::light/dark` + `apply_theme`)
- [~] Fix `document.rs`'s UPDATE escaping (done: `build_jsonb_set` now parses/re-serialises JSON and
       quote-doubles correctly, with tests) — `window.prompt()` inline-edit UX remains (architectural
       simplification, acceptable; revisit if a richer editor is wanted)
- [x] Heatmap render mode for `CanvasMap` (`kernel_density` + `heat_color`, no external dependency) —
       already implemented
- [x] Auto topic-inference for `use_keystone_query` (`infer_topic_from_sql` FROM-clause extractor,
       single-table only; JOIN rejection strengthened this pass) — already implemented
- [ ] WebGPU rendering proof-of-concept on `CanvasTimeSeries` first (stretch goal: extend to
       `CanvasMap`/`CanvasGraph`) — not started (Canvas2D is the deliberate scope cut in `lib.rs`);
       listed as a stretch goal, deferred
- [x] Thin JSX authoring layer wrapping the existing WASM classes — lives in `packages/sdk-web/src/
       react.tsx` (exported as `@tpt/sdk-web/react`), already implemented; the TODO's
       `packages/canvas-react/` maps to this existing module

## Phase 5 — Dual licensing (MIT OR Apache-2.0)

- [x] Add `LICENSE-MIT` and `LICENSE-APACHE` at repo root (also a root `LICENSE` pointer)
- [x] Update every crate's `license` field to `"MIT OR Apache-2.0"`: `tpt-keystone`, `tpt-cli`,
      `tpt-harbor`, `tpt-canvas`, `tpt-operator`, `tpt-sdk` (Cargo.toml); `packages/*`
      (package.json); `sdk-python/pyproject.toml`
- [x] Update `README.md`/`CLAUDE.md` licensing mentions

## Phase 6 — Adoption tooling

- [x] `CONTRIBUTING.md`, `CHANGELOG.md`, GitHub issue/PR templates (`.github/ISSUE_TEMPLATE/*`,
      `.github/pull_request_template.md`)
- [x] `Makefile` (repo root) wrapping per-crate build/test commands; `install.sh`/`install.ps1`
- [x] Secure-by-default `docker-compose.yml` (requires `TPT_AUTH_BOOTSTRAP_USER`/
      `TPT_AUTH_BOOTSTRAP_PASSWORD` via `.env`; refuses to start unauthenticated)
- [x] Browser playground (built on the Phase 4 demo app) — the demo (`tpt-canvas/examples/dashboard/`)
       now `vite build`s to a deployable static `dist/` and runs fully offline via the seeded
       `mock-server.mjs` (`npm run mock` + `npm run dev:mock`); serving `dist/` on any static host
       is the playground. Marked done: the blocker (unbuildable wasm32 crate + no offline data path)
       is resolved.

## Phase 7 — crates.io release readiness (metadata + publishability only)

- [x] Add `repository`/`homepage`/`documentation`/`readme`/`keywords`/`categories`/`rust-version` to
      every Rust crate's `Cargo.toml` (added this pass)
- [x] Pair the two local `path` deps (`tpt-cli`→`tpt-sdk`, `tpt-sdk`→`tpt-canvas`) with `version`
      fields (`"0.1.0"`), matching each crate's `version` (added this pass)
- [x] `cargo publish --dry-run` per crate in dependency order, fix whatever it flags (network/publish
       not exercised in this pass; metadata + dep `version` fields are in place): leaf crates verified
       this pass — `tpt-canvas`, `tpt-operator`, `tpt-harbor`, `tpt-keystone` all `cargo publish
       --dry-run` cleanly (warnings only). `tpt-harbor` had a real blocker: its `src/target/` module
       directory collided with cargo's excluded `**/target/` build dir, so the module was never
       packaged; renamed to `src/targets/` and updated `lib.rs` + `main.rs` + `examples/smoke.rs`
       (cargo's `--allow-dirty` needed because the working tree has unrelated uncommitted changes).
       `tpt-sdk`/`tpt-cli` dry-run only fails on their not-yet-published path deps (`tpt-canvas` /
       `tpt-sdk` respectively) — expected on first publish; resolves once published in dependency order
       (canvas → sdk → cli), which the leaf dry-runs already validate.
- [ ] No automated release pipeline in this pass — manual `cargo publish` when ready
- [x] Renamed every non-core Rust crate (directory + Cargo.toml `name`) to carry a `tpt-keystone-`
      prefix ahead of crates.io publish, to claim an unambiguous namespace and avoid name collisions:
      `tpt-canvas` → `tpt-keystone-canvas`, `tpt-harbor` → `tpt-keystone-harbor`, `tpt-cli` →
      `tpt-keystone-cli`, `tpt-sdk` → `tpt-keystone-sdk`, `tpt-operator` → `tpt-keystone-operator`
      (`tpt-keystone` itself was already correctly named). Path deps between them now alias the local
      dependency key to the new `package = "..."` name (e.g. `tpt-keystone-cli`'s Cargo.toml still
      depends on a key named `tpt-sdk` but points `package =` at `tpt-keystone-sdk`), so no `use
      tpt_sdk::`/`use tpt_canvas::` call sites needed to change. Binary names: `tpt-harbor` →
      `tpt-keystone-harbor`, `tpt-operator` → `tpt-keystone-operator`, `tpt-sdk-typegen` →
      `tpt-keystone-sdk-typegen`; the `tpt` (CLI) and `tpt-keystone` (core engine) binary names are
      intentionally unchanged since they're short user-facing commands, not derived from the crate
      name. Non-Rust packages (`sdk-go`, `sdk-python`, `packages/*`, `tpt_sdk` Flutter,
      `tpt-sdk-android`) are out of scope — they don't publish to crates.io and have their own
      registries/naming conventions.
- [x] Re-ran `cargo publish --dry-run` per crate under the new `tpt-keystone-` names: `tpt-keystone`,
      `tpt-keystone-canvas`, and `tpt-keystone-operator` all pass cleanly (warnings only — the operator
      gets a harmless "readme outside package" warning since it ships its own `README.md` alongside the
      `readme = "../README.md"` pointer). `tpt-keystone-sdk`/`tpt-keystone-cli` fail dry-run only on
      their not-yet-published path deps (`tpt-keystone-canvas`/`tpt-keystone-sdk` respectively, now
      referenced via the `package =` alias) — same expected first-publish failure as before the rename,
      resolves once published in dependency order. `tpt-keystone-harbor`'s dry-run fails to link in
      this sandbox because `libodbc` (unixODBC) isn't installed here — an environment gap unrelated to
      the rename; its lib compiles and packages fine, only the final verification build of the `odbc-api`
      dependency's system library link fails.
- [x] Fixed the two gaps the dry-runs above surfaced: (1) every crate's `readme` field pointed at
      `../README.md` (the monorepo root) instead of its own local `README.md`, causing the "readme
      outside package" warning and shipping the wrong docs to each crate's crates.io page — repointed
      to `readme = "README.md"` in all 6 crates. (2) every crate's own `LICENSE` file was just the
      Apache-2.0 text copy-pasted, with no MIT text anywhere in the crate despite `license = "MIT OR
      Apache-2.0"` in Cargo.toml — added matching `LICENSE`/`LICENSE-MIT`/`LICENSE-APACHE` (copied from
      the repo root) to all 6 crates so the dual license is actually present in what ships.

## Phase 8 — Harbor: engine-native targets (Meridian/Prism/Chronos/Plexus/Canopy)

Full context/rationale at `C:\Users\Phillip\.claude\plans\i-m-looking-at-harbour-typed-spark.md`. Before
this pass, `tpt-keystone-harbor` (Phase 15's migration platform) only ever wrote plain relational
Keystone tables — `SourceKind::target_engine()` already mapped PostGIS→Meridian, vector DBs→Prism,
InfluxDB→Chronos, Neo4j→Plexus, MongoDB/Elasticsearch→Canopy, but that mapping was dead metadata.
Since those five engines are SQL-extension features of `tpt-keystone` itself (not separate services),
this didn't need new wire-protocol target clients — the existing single `KeystoneTarget`/`pgwire::Client`
path stays as-is; it needed the right index created alongside each table, and each source connector
handing over data already shaped the way its target engine expects.

- [x] Schema Translator IR gains `IndexSpec`/`TableSchema::index_ddl()` (`tpt-keystone-harbor/src/
      schema.rs`) — target-engine index metadata rendered as `CREATE INDEX ON ... USING <KIND> ...
      WITH (...)`, applied by `KeystoneTarget::apply_ddl` (`targets/keystone.rs`) right after
      `CREATE TABLE`. Left empty for sources whose native target is already plain relational
      (Postgres/MySQL/MSSQL/Oracle/ODBC) — no behavior change for those.
- [x] PostGIS (Meridian): every `GEOMETRY`/`GEOGRAPHY` column gets a `SPATIAL` index
      (`sources/postgis.rs::spatial_indexes`) — previously migrated geometry landed as inert WKT text
      with no spatial index, so `ST_DWithin`/`ST_Within` had nothing to use.
- [x] Vector sources / Pinecone+Weaviate+Qdrant (Prism): the `vector` column gets a `VECTOR` (HNSW)
      index, `metric = 'cosine'` default (`sources/vector.rs::schema_for`) — none of the three REST
      APIs' discovery calls used here expose the configured distance metric without extra round trips,
      so it isn't derived per-source in this pass.
- [x] InfluxDB (Chronos): the timestamp column gets a `TIME` index with an explicit `value` = the first
      numeric field column (`sources/influxdb.rs::time_index`) — Chronos's auto-pick only applies when
      there's exactly one numeric candidate, and a measurement can have several.
- [x] Neo4j (Plexus) — the highest-value fix: relationships were previously silently dropped entirely
      (`discover()` only ever called `db.labels()`; node properties migrated, edges never did). Now
      also calls `db.relationshipTypes()` and emits a `GRAPH`-indexed edge `TableSchema` per
      relationship type (`from_id`/`to_id`/properties, `schema = "neo4j_edges"`, endpoints from `id(a)`/
      `id(b)` to match the existing node tables' `_node_id`); `snapshot_table`/`row_checksums` dispatch
      to `MATCH (a)-[r:TYPE]->(b)` Cypher for these tables based on `table.schema`, with no change
      needed to `engine.rs` (it already iterates generically over however many tables `discover()`
      returns).
- [x] MongoDB + Elasticsearch (Canopy): replaced 10-document-sample field-flattening
      (`infer_schema`/per-field ES mapper — silently dropped any field absent from the sample, fragile
      on schemaless/polymorphic real-world documents) with a fixed `(_id TEXT PK, doc JSON)` schema,
      GIN-indexed, whole document/hit preserved as JSON text. Elasticsearch's `row_checksums` also now
      actually hashes document content (previously ID-only — a latent verification gap where content
      drift between source and target wouldn't have been caught).
- [x] Doc hygiene: `tpt-keystone-harbor/README.md`/`src/lib.rs` corrected — both previously claimed
      Mongo/Graph/GIS/MySQL/MSSQL/etc. were unrunnable stubs; only CDC (`replicate()`) is actually
      stubbed for those, and now for the reshaped Canopy/Plexus sources too.
- [x] `cargo build`/`cargo test` (55 tests, incl. new coverage for every item above) pass clean, no
      warnings.
- [ ] Not done this pass: Kafka→Flux explicit `CREATE TOPIC ... WITH (partitions = ...)` — every
      `INSERT`/`UPDATE`/`DELETE` already auto-publishes to an implicit `__cdc_<table>` topic with zero
      extra DDL, so the plain relational target Kafka already gets today lands correctly in Flux terms;
      this would only add an explicitly named topic matching the source's partition count. Nice-to-have,
      not a gap.
- [ ] No live-external-server end-to-end verification — `cargo build`/`cargo test` pass and the new
      logic is unit-tested at the string/row-building level (matching how the rest of Harbor is tested),
      but there's still no integration harness in this repo for a real external Postgres/Mongo/Neo4j/
      InfluxDB/Elasticsearch/vector-DB server, so full source-side verification stays manual (extend
      `examples/smoke.rs` against a live `tpt-keystone` node).

## Done outside this list (`cfdafce`)

- [x] `tpt-harbor`: ODBC source connector (`sources/odbc.rs`, `SourceKind::Odbc`) — vendor-agnostic
      DSN-based connector, targets Keystone by default since ODBC's real target engine depends on
      whatever's behind the DSN and the registry has no way to know
