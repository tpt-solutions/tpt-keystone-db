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
- [x] Stage 2+3: durable multi-version storage + real snapshot isolation, per the RocksDB/LevelDB-style
       internal-key encoding approach (plan: `C:\Users\Phillip\.claude\plans\is-it-good-idea-majestic-mitten.md`).
       New `storage/internal_key.rs` (`InternalKey{user_key,seq,tag}`, custom `Ord` comparing `user_key`
       in isolation first so a byte-prefix relationship between two user keys can't be disturbed by the
       version suffix). `sstable.rs` (`IndexEntry` +seq/tag, bloom stays keyed on user key only,
       `read_at`/`scan_all_versions`, `max_seq`); `wal.rs` (rewritten to batch-framed records —
       `marker|commit_seq|count|record*count` — so `append_batch` is one write+fsync per commit and
       `replay` accepts/discards a whole batch atomically, closing the old "torn subset of a multi-row
       commit" gap for free, not just adding versioning); `lsm.rs` (`MemTable` keyed by `InternalKey`,
       `next_commit_seq`/`open_snapshots` watermark tracking, `read_at`/`scan_at`/`write_batch`,
       back-compat `read`/`scan`/`write`/`delete` wrappers so non-transactional call sites are
       unaffected, `open()` seeds `next_commit_seq` from WAL+SSTables+manifest so a restart never
       reissues a seq, `compact_all` rewritten to keep every version newer than the oldest open
       snapshot plus one "floor" version below it). Closed out this pass: `storage/database/txn.rs` has
       the `snapshot_seq` field, released via `lsm.release_snapshot()` on `Drop`; `storage/database/mod.rs`
       wires `begin_txn` to capture the snapshot and routes `txn_read`/`txn_scan` through `read_at`/
       `scan_at`; the dead `tx_mgr`/`mvcc` fields are retired, `storage/tx.rs` is deleted, and
       `storage/mvcc.rs` is down to just `new_tx_id()`; `wire/session.rs` now intercepts
       BEGIN/COMMIT/ROLLBACK over the extended query protocol too (previously only the simple query
       protocol managed `txn` — a real pre-existing gap this work surfaced), including `Sync`'s
       transaction-status reporting; `executor/transaction_tests.rs` replaces the old
       `read_committed_sees_other_transactions_committed_writes` with
       `snapshot_isolation_hides_other_transactions_later_commits` plus new coverage
       (`snapshot_pinned_across_multiple_statements_despite_concurrent_commits`,
       `own_writes_visible_within_snapshot_alongside_others_hidden`,
       `abandoned_transaction_snapshot_is_released_on_drop`); `storage/mvcc_tests.rs`'s proptest was
       ported off the deleted `MvccStore`; and `Manifest::max_commit_seq` (`#[serde(default)]`) was added,
       with back-compat decode tests in `storage/manifest.rs`.
- [x] Concurrency model (audit's Stage 4): `LsmEngine` now splits the old coarse `Mutex<LsmEngine>` into
       a `write: Mutex<WriteState>` (WAL append/commit-seq/memtable-insert bookkeeping only) and a
       `view: Mutex<Arc<EngineView>>` (held only for a nanosecond `Arc` clone on the read path) — reads
       never take `write`, and flush/compaction upload their SSTable blob lock-free between a brief
       `begin_*`/`commit_*` pair. Audited in `docs/concurrency_model_audit.md`: readers don't block
       writers and vice versa (proven by `finish_flush_does_not_block_a_concurrent_read`/
       `finish_compact_does_not_block_a_concurrent_read` in `storage/lsm.rs`); writers still serialize on
       `write` (accepted — single-writer-node model per Phase 3 lease fencing) and per-engine index
       locks (BTree/Geo/Graph/etc.) remain per-structure, both explicitly out of scope as follow-ups.
- [x] Extended query protocol (Parse/Bind/Execute/Sync) and simple query protocol both correctly
      interact with the new per-connection transaction state (`wire/session.rs` threads `txn` into
      both `execute_simple` and the `Execute` handler)
- [x] Extend the test suite with atomicity/isolation tests (`executor/transaction_tests.rs`); crash-
      mid-transaction coverage is partially implied by ROLLBACK-discard tests but not yet a dedicated
      `chaos_tests.rs` crash case
- [x] Manual verification: concurrent `BEGIN`/`COMMIT`/`ROLLBACK` from two `psql` sessions — scripted
       in `tools/verify_transactions.sh` (bash) + `tools/verify_transactions.ps1` (PowerShell); covers
       isolation, snapshot pinning, COMMIT visibility, ROLLBACK discard, and concurrent two-session
       isolation. Actually run against a live `cargo build`-ed node this pass (not just written) — doing
       so surfaced and fixed three real, previously-unverified wire-protocol bugs no in-process Rust test
       had caught: (1) `wire/session.rs`'s zero-config auth path never sent `AuthenticationOk` (dropped
       by the Phase 20 RBAC refactor, `9db38d7`), so every real Postgres client failed the handshake
       against an unauthenticated node; (2) the simple-query protocol only ever executed the *first*
       statement of a `;`-separated batch and silently dropped the rest (`sql::parse`/`parse_stmt` was
       never looped) — fixed with `sql::parse_all`/`Parser::parse_all_stmts` plus a batch-aware
       `StatementCache::parse_all`, and `wire::session::handle_simple_query` now loops over every
       statement, sending one `ReadyForQuery` only after the whole batch; (3) `pg_sleep` was a no-op
       (`Ok(Value::Null)`, ignored its argument) and, once implemented for real, an initial
       `std::thread::sleep` blocked whichever Tokio worker happened to be driving the runtime's I/O at
       that moment — stalling *all* connections including the accept loop, not just the sleeping one —
       fixed by wrapping it in `tokio::task::block_in_place`.

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
- [x] **Found and fixed:** `wire::http_query_tests::query_requires_basic_auth_when_roles_configured`
      hung indefinitely — a true deadlock (confirmed reproducing alone, single-threaded, past 60s),
      found while running the full `cargo test --lib` suite during an unrelated pass, pre-existing since
      `cfdafce`. Root-caused to two stacked bugs, both fixed this pass:
      1. **Test bug (the actual deadlock trigger):** the test declared `Content-Length: 31` for a body
         that's actually 25 bytes (`{"sql": "select 1 as ok"}`). `http_query.rs`'s `read_request` does
         `stream.read_exact(content_length)`, so it blocked forever waiting for 6 bytes the test's
         `raw_request` helper never sent (having already `write_all`'d its whole request), while the
         client blocked in `read_to_end` waiting for a response the server never got to write — a real
         mutual deadlock, not slowness. Fixed by correcting the length to 25 (all 3 occurrences) and
         documenting why it must match exactly.
      2. **Real production bug this exposed once the deadlock stopped masking it:** `read_request`
         searched the same `head.lines()` iterator twice — once via `.find()` for `Content-Length`, then
         again via `.find()` for `Authorization` — but `Iterator::find` consumes everything it scans
         past. Since every real request here sends `Authorization` *before* `Content-Length`, the first
         `.find()` call always consumed past (and lost) the `Authorization` line before the second
         `.find()` ever ran, so **the HTTP query bridge silently ignored `Authorization` headers sent
         before `Content-Length`** — any such request fell through to "missing or malformed
         Authorization header" regardless of what was actually sent. Fixed by collecting `head.lines()`
         into a `Vec` once and searching it independently for each header (`wire::websocket`'s
         equivalent handshake parser already did this correctly — only `http_query.rs` had the bug).
         Also fixed while in the same function: `json_response`'s reason-phrase table fell through to
         `"Internal Server Error"` for `401` responses (cosmetic — the status code itself was always
         correct; no caller keyed behavior off the reason phrase).
      Full `cargo test --lib` now completes in ~30s (559 passed) instead of hanging forever.
      **Newly found while verifying this fix, fixed in a later pass:** `mcp::tests::tables_columns_schema_reflect_created_table`
      failed (reproduces alone) — `db.list_tables()` returned `_tpt_roles`/`_tpt_role_members`/
      `_tpt_privileges` alongside the test's own `widgets` table. Root cause: `RoleStore::new` (via
      `bridge_auth::actor_for_mcp`, called on every MCP request even in zero-config mode to check
      `roles.is_empty()`) unconditionally creates all three RBAC catalog tables as a side effect the
      first time any MCP request is handled, and `list_tables()` didn't filter internal `_tpt_*`
      catalogs from its output. Also pre-existing since `cfdafce` — likely never visible before because
      the deadlock above always prevented a full-suite run from reaching this far. The leak turned out
      to reach 9 user-facing call sites, not just MCP — also `wire/http_query.rs`'s Canvas `GET /schema`
      and 6 Postgres-wire-protocol catalog builders in `executor/catalog.rs` (`pg_tables`, `pg_class`,
      `pg_attribute`, `pg_constraint`, `information_schema.tables`, `information_schema.columns`, plus
      `pg_table_is_visible()` in `eval.rs`) — meaning `\dt`/`information_schema.*` over the real Postgres
      wire protocol leaked the same three tables. **Fixed:** `storage/mod.rs` gained a free
      `is_internal_table(name)` helper (the precise `_tpt_` prefix, matching `executor::rbac`'s existing
      convention rather than `executor::stats`'s broader bare-`_` one) and a default `StorageEngine`
      method `list_user_tables()` filtering through it; all 9 user-facing call sites now call
      `list_user_tables()` instead of `list_tables()`, while the `CREATE TABLE IF NOT EXISTS` collision
      check (`executor/ddl.rs`) and `ANALYZE`'s default target list (`executor/stats.rs`, already filtered
      more broadly) were deliberately left on the raw `list_tables()`. `executor/rbac.rs`'s
      `references_system_table` was also switched to call the new shared helper instead of inlining the
      same `_tpt_` prefix check. Covered by a new `storage::internal_table_tests` unit test suite and a
      `executor::catalog::internal_table_leak_tests` regression test proving `pg_tables`/
      `information_schema.tables` hide the RBAC catalogs after a `RoleStore` is constructed; the
      previously-failing `mcp::tests::tables_columns_schema_reflect_created_table` now passes.

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
- [x] Fix `document.rs`'s UPDATE escaping (`build_jsonb_set` parses/re-serialises JSON and quote-doubles
       correctly, with tests) — `window.prompt()` inline-edit UX implemented and accepted as the
       deliberate architectural simplification
- [x] Heatmap render mode for `CanvasMap` (`kernel_density` + `heat_color`, no external dependency) —
       already implemented
- [x] Auto topic-inference for `use_keystone_query` (`infer_topic_from_sql` FROM-clause extractor,
       single-table only; JOIN rejection strengthened this pass) — already implemented
- [x] WebGPU rendering proof-of-concept on `CanvasTimeSeries` — new `webgpu.rs` (`GpuRenderer`):
       feature-detects `navigator.gpu`, negotiates an adapter/device, builds a fixed
       `line-strip` render pipeline (one hardcoded-color WGSL shader, no uniforms/bind groups —
       `GpuAutoLayoutMode::Auto`), and redraws by re-uploading a vertex buffer + encoding one render
       pass per frame. Deliberately narrow scope: connecting line only, no point markers/axis text/
       per-series color (Canvas2D still draws those on every other component). Real constraint this
       surfaced: a `<canvas>` element commits permanently to its first successfully-requested context
       type (`"2d"` xor `"webgpu"`), so `CanvasTimeSeries::new` can no longer mount Canvas2D
       synchronously up front — it now defers context selection to an async task
       (`wasm_bindgen_futures::spawn_local`) that tries WebGPU first and only falls back to Canvas2D
       if that fails *before* any context was requested; a `redraw_trigger` signal forces one repaint
       once the async decision resolves, even for a non-realtime query. `web-sys`'s `Gpu*` bindings are
       gated behind `--cfg=web_sys_unstable_apis` (new `tpt-keystone-canvas/.cargo/config.toml`).
       Extending to `CanvasMap`/`CanvasGraph` remains unstarted (needs a real pan/zoom transform +
       multiple draw calls). **Not browser-tested** — no headless-WebGPU harness in this repo, same
       caveat as the rest of this WASM-only crate; verified only by `cargo build --target
       wasm32-unknown-unknown` compiling clean (zero warnings) and `cargo test` (34 host-target tests,
       unrelated to WebGPU itself) passing.
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
- [x] Automated release pipeline: `.github/workflows/publish.yml` — triggers on `v*` tags or manual
      `workflow_dispatch` (with a `dry_run` input), three jobs in dependency order (leaf crates in
      parallel → `tpt-keystone-sdk` → `tpt-keystone-cli`), `unixodbc-dev` installed for
      `tpt-keystone-harbor`'s `odbc-api` dep. Fixed this pass: the working tree had a
      `Setup cargo publish credentials` step invoking `rust-lang/audit-patched-rust-action@v1`, which
      does not exist on the Marketplace (verified by search) — replaced with the standard, no-action
      approach of passing `CARGO_REGISTRY_TOKEN` directly as an env var to each `cargo publish` step.
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
- [x] Kafka→Flux explicit `CREATE TOPIC IF NOT EXISTS ... WITH (partitions = 'n')` — Harbor-only change,
      since Keystone's own SQL engine already fully supported this DDL end to end
      (`sql/ast.rs`/`parser.rs`/`executor/ddl.rs`/`storage/database/flux.rs`) with no engine-side work
      needed. `TableSchema` (`tpt-keystone-harbor/src/schema.rs`) gained a `topic_partitions: Option<u32>`
      field and a `topic_ddl()` render method (mirroring `IndexSpec`/`index_ddl()`'s existing pattern),
      targeting the exact same `__cdc_<table>` name `Database::ensure_cdc_topic` auto-creates so this
      only fixes the partition count rather than creating a second, orphaned topic — and using
      `IF NOT EXISTS` since the implicit auto-topic may already have landed first. `sources/kafka.rs`'s
      `discover()` now calls the already-existing (previously private-use-only) `metadata_partitions`
      per topic to populate the field, falling back to `None` (implicit 1-partition behavior) on a
      lookup error or an empty partition list rather than failing discovery outright. `targets/
      keystone.rs::apply_ddl` executes `topic_ddl()` after the existing index-DDL loop; `engine/mod.rs`
      needed no changes, since `apply_ddl` was already the single per-table DDL choke point. Every other
      source's `TableSchema` literal got `topic_partitions: None` (Rust struct-literal fields are all
      mandatory, no `..Default::default()` used for this type). Tested at the string-rendering level
      (`schema.rs`'s `topic_ddl_renders_partitions_when_present`/`topic_ddl_none_when_absent`), matching
      this crate's existing test depth for DDL-building helpers — no live Kafka broker in this repo to
      verify `discover()`'s new wiring end to end.
- [ ] No live-external-server end-to-end verification — `cargo build`/`cargo test` pass and the new
      logic is unit-tested at the string/row-building level (matching how the rest of Harbor is tested),
      but there's still no integration harness in this repo for a real external Postgres/Mongo/Neo4j/
      InfluxDB/Elasticsearch/vector-DB server, so full source-side verification stays manual (extend
      `examples/smoke.rs` against a live `tpt-keystone` node).

## Phase 9 — Harbor: MSSQL + MongoDB live CDC

Full context/rationale at `C:\Users\phill\.claude\plans\oh-for-harbour-did-jolly-thacker.md`. Of the
nine Harbor source connectors whose `replicate()` (live CDC) is stubbed to `ConnectorError::Unimplemented`,
MSSQL and MongoDB have the best effort/risk ratio — both can reuse existing connection plumbing (MSSQL's
`TdsConn::query`, Mongo's `send_op_msg`/`getMore` cursor loop) rather than needing a new protocol layer
from scratch. MySQL (binlog, comparable in size to the Postgres `pgoutput` decoder) and Oracle (LogMiner,
additionally blocked on `oracle.rs`'s own TTC protocol layer being an unverified "best-effort
reconstruction") are deliberately deferred to a later phase. Elasticsearch/Neo4j/InfluxDB/vector stores
have no native change-feed API at all and stay snapshot-only by design — no polling-based pseudo-CDC.

- [x] MSSQL TDS row-parser fix (`sources/mssql.rs`, `parse_result_set`) — blocking prerequisite for CDC
      and a latent correctness fix for `snapshot_table`/`row_checksums`: the old ROW-token handling
      ignored COLMETADATA entirely and returned one opaque blob per row instead of per-column cells.
      Replaced with full TYPE_INFO parsing (fixed-length, `BYTELEN_TYPE`, `USHORTLEN_TYPE`,
      `LONGLEN_TYPE`, PLP `MAX` types) and typed-to-text cell decoding (`ColumnMeta`/`parse_type_info`/
      `read_cell` + per-type converters). Also fixed the pre-existing COLMETADATA walker itself, which
      never skipped each column's UserType(4)+Flags(2)+ColName per MS-TDS §2.2.7.4 and used a
      nonstandard column-count encoding — both would have misaligned on any multi-column result.
      `NBCROW` (`0xD2`, row compression) is an explicit `bail!` rather than silently mis-parsed, since
      Harbor doesn't negotiate the capability that triggers it.
- [x] MSSQL CDC (`sources/mssql.rs::replicate`) — `check_cdc_enabled` preflight (`sys.databases.
      is_cdc_enabled`, `cdc.change_tables`) with a clear DBA-facing error naming the exact
      `sp_cdc_enable_db`/`_table` commands to run, rather than Harbor enabling CDC itself; poll-based
      loop (`CDC_POLL_INTERVAL` = 2s) over `cdc.fn_cdc_get_all_changes_<capture_instance>`,
      `map_cdc_operation` mapping `__$operation` to `ChangeEvent::Insert/Update/Delete` (op 3,
      update-before-image, is skipped as informational-only); `lsn_to_hex`/`hex_to_lsn` resume-token
      codec (SQL Server's native `0x`-hex LSN convention).
- [x] MongoDB change-stream CDC (`sources/mongodb.rs::replicate`) — replica-set preflight via the
      `hello` response's `setName` field (captured at connect time); new nested BSON encoders
      (`bson_encode_document`/`bson_encode_array`) building a whole-database `{aggregate: 1, pipeline:
      [{$changeStream: {fullDocument: 'updateLookup'[, resumeAfter: {...}]}}]}` over the existing
      `getMore` cursor loop; `map_change_event` mapping insert/update/replace/delete/invalidate to
      `ChangeEvent` (filtered against `known_tables` by each event's `ns.coll`); `_data` resume-token
      passthrough via `extract_resume_token`. Found and fixed three pre-existing bugs in the process,
      all of which would have broken this same `getMore` mechanism (and, for the first two, already
      silently broke `snapshot_table`/`row_checksums` pagination past the first batch):
      **(1)** `bson_build_doc`'s declared document length omitted the 4 bytes of the length field
      itself, under-reporting every BSON document this crate has ever built by 4 bytes and corrupting
      the tail of any nested document/array a decoder bounds by it (caught by the first test that
      round-trips `bson_build_doc` output back through `bson_decode_doc` — no prior test did);
      **(2)** `extract_batch` only read `firstBatch`, but `getMore` responses carry the same cursor
      batch under `nextBatch` instead — now checks both; **(3)** `getMore`'s cursor-id argument was
      encoded as a BSON string instead of the int64 the wire protocol requires. Also added `$db` (a
      mandatory OP_MSG field this connector never sent) to every command this file builds
      (`hello`/`listCollections`/`find`/`getMore`/the new `aggregate`).
- [x] Unit tests: MSSQL TYPE_INFO decode fixtures per type category (int/nvarchar/decimal/bigint/
      datetime/date/null), `lsn_to_hex`/`hex_to_lsn` round trip, `civil_from_days`/`days_from_civil`
      round trip, `map_cdc_operation` (10 tests total, `sources/mssql.rs`); MongoDB nested BSON encoder
      round trip, `map_change_event` fixtures incl. the missing-`fullDocument` edge case and the
      unknown-collection filter, resume-token round trip (13 tests total, `sources/mongodb.rs`).
      `parse_result_set` was changed from a method requiring a live `TdsConn` to an associated function
      so it's callable directly from fixtures. No live external SQL Server/MongoDB server in this repo,
      so end-to-end verification stays manual (`examples/smoke.rs`), consistent with the rest of
      Harbor's connectors.
- [x] Doc touch-ups: `sources/mod.rs`, `src/lib.rs`, `README.md`, and `Cargo.toml`'s `description` all
      updated so MSSQL/Mongo are no longer listed among CDC-stubbed connectors.

## Phase 10 — Canvas: extend WebGPU PoC from CanvasTimeSeries to CanvasMap/CanvasGraph

Full context/rationale at `C:\Users\phill\.claude\plans\is-there-anything-left-rustling-peach.md`.
`webgpu.rs`'s existing PoC (single hardcoded-color `line-strip` pipeline, no bind groups/uniforms,
async context-selection race vs. Canvas2D) covers only `CanvasTimeSeries` today; its own module docs
call extending it to `CanvasMap`/`CanvasGraph` "unstarted," needing "a real pan/zoom transform +
multiple draw calls." Confirmed neither component has any pan/zoom or theming today (whole-crate grep
for `wheel|zoom|pan|transform|matrix` turns up nothing relevant). This phase builds a shared,
backend-agnostic pan/zoom transform once, then adds one new WebGPU pipeline per component mirroring
the existing single-purpose-struct, no-bind-groups, hardcoded-color style.

- [x] `view_transform.rs` (new, host-testable): `ViewTransform` (scale/translate, `to_screen`/
      `to_data`/`pan`/`zoom`/`screen_to_data_distance`), `normalized_to_clip`, `quad_vertices_clip`,
      `zoom_factor_from_wheel_delta`, with unit tests (round-trip, zoom-pivot-fixed, scale clamping) —
      8 tests, all pure `f64`/`f32` math, no `#[cfg(target_arch = "wasm32")]` gate needed
- [x] `render::canvas_dimensions` — reads canvas pixel size without committing to a context type,
      needed before the WebGPU-vs-Canvas2D async race decides which context to request
- [x] Split `webgpu.rs` into a directory module (`webgpu/{mod,timeseries,map,graph}.rs`), extracting
      shared `GpuContext::negotiate`/`upload_vertices`/`begin_pass`/`end_and_submit` out of the
      3x-duplicated adapter/device/context negotiation (`GpuContext` also now captures the canvas's
      pixel `width`/`height` at negotiation time, needed by the two new renderers below to size
      markers/nodes in clip space); `GpuRenderer` (timeseries) keeps its exact public API — zero
      behavior change to the working `CanvasTimeSeries` path
- [x] `GpuMapRenderer` (`webgpu/map.rs`): `TriangleList` marker-quad pipeline (CPU-expanded quads via
      `quad_vertices_clip`), hardcoded blue matching Canvas2D markers
- [x] `GpuGraphRenderer` (`webgpu/graph.rs`): `LineList` edge pipeline + `TriangleList` node-quad
      pipeline in one render pass, hardcoded gray/green matching Canvas2D edges/nodes
- [x] `components/map.rs`: `Renderer` enum + async race (mirroring `timeseries.rs`); both `heatmap`
      *and* `cluster` force synchronous Canvas2D (kernel-density fill and variable-radius/count-label
      cluster bubbles both stay out of WebGPU scope this pass — only plain point markers get a GPU
      path, so this widens the TODO's original "heatmap-only" cut slightly, documented in `new`'s doc
      comment and `webgpu/map.rs`'s module docs); `install_click_handler` → `install_pointer_handlers`
      (adds wheel-to-zoom, promotes click to a 3-event mousedown/move/up state machine for
      pan-vs-click disambiguation via a `PAN_THRESHOLD_PX` move-distance gate)
- [x] `components/graph.rs`: `Renderer` enum + async race for both `new`/`new_from_match` (factored
      into a shared `init_renderer` helper rather than duplicated); `install_drag_handler` →
      `install_pointer_handlers` (wheel-to-zoom, mousedown hit-tests via `view.to_data`/
      `screen_to_data_distance`, picks node-drag vs. pan); bundled fix: node-drag mousemove now
      writes through `view.to_data` instead of raw screen coords, and fires `redraw_trigger` on every
      drag/pan move (previously dragging a node didn't visibly redraw until an unrelated data refetch
      happened). Node layout positions (`fruchterman_reingold` output) are now stored in unzoomed data
      space and mapped through `view.to_screen` at draw time, so panning/zooming never distorts the
      cached layout itself.
- [x] `Cargo.toml`: added `"WheelEvent"`/`"Event"` to the `web-sys` feature list (for `WheelEvent::
      delta_y`/`Event::prevent_default`); no new `Gpu*` bindings needed (`TriangleList`/`LineList` are
      existing `GpuPrimitiveTopology` variants)
- [x] Updated stale module docs: `lib.rs` no longer claims "CanvasTimeSeries always mounts Canvas2D
      synchronously first" (was already wrong vs. the actual WebGPU-first race) and now describes all
      three canvas-drawing components' shared async-race behavior; `webgpu/mod.rs`/`webgpu/map.rs`
      module docs updated for the directory split and the `cluster`-also-forces-Canvas2D decision
      above.
- [x] Verify: `cargo test` (host) passes 42/42, including 8 new `view_transform` tests plus the
      existing `kernel_density`/`cluster_grid`/`fruchterman_reingold`/etc. suites unaffected;
      `cargo build --target wasm32-unknown-unknown` compiles the whole crate clean (zero warnings). No
      headless-WebGPU harness exists in this repo (no `wasm-bindgen` CLI available in this environment
      either), so actual GPU-path rendering correctness/interaction feel stays manual/unverified, same
      caveat as the existing timeseries PoC.

Explicitly out of scope this pass (documented, not silent cuts, matching this crate's established
PoC precedent): heatmap *and* cluster rendering in WebGPU (both stay Canvas2D-only, see above),
node-id/cluster-count text in the GPU path (no glyph atlas), per-instance/data-driven coloring (one
hardcoded color per pipeline, no uniforms/bind groups), and `GpuVertexStepMode` instancing
(CPU-expanded quads instead).

## Done outside this list (`cfdafce`)

- [x] `tpt-harbor`: ODBC source connector (`sources/odbc.rs`, `SourceKind::Odbc`) — vendor-agnostic
      DSN-based connector, targets Keystone by default since ODBC's real target engine depends on
      whatever's behind the DSN and the registry has no way to know
