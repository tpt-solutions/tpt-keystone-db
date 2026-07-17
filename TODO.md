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
- [ ] Stage 2: snapshot isolation using `storage/mvcc.rs`'s version-chain design — audit how much of
      `mvcc.rs`/`tx.rs` is actually reusable vs. needs rework before committing to this design. The
      committed `MvccStore` exists but is not yet the storage substrate (Stage 1 keeps the
      single-version LSM as the source of truth and replays staged buffers on COMMIT).
- [ ] Storage-layer support for multiple versions of a key in `lsm.rs`/`sstable.rs`/`wal.rs`, plus
      compaction-time GC of versions no longer visible to any open transaction
- [ ] Concurrency model: verify whether the current single coarse `Mutex<LsmEngine>` remains correct
      under MVCC or needs finer-grained locking
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

- [ ] Demo app (`tpt-canvas/examples/dashboard/`, Vite+TS) mounting all 6 `Canvas.*` components
      against a live `tpt-keystone` node — first-ever browser verification of this crate
- [ ] GQL `MATCH` support in `CanvasGraph` (client-side result-shape translator only, no server
      changes needed)
- [ ] Design tokens/theming (`theme.rs` + CSS variables) for `document.rs`/`vector_search.rs`/
      `agent_monitor.rs`
- [ ] Fix `document.rs`'s naive string-interpolated `UPDATE` + `window.prompt()` UX
- [ ] Heatmap render mode for `CanvasMap` (kernel density, no external dependency); basemap tiles via
      user-supplied tile URL template
- [ ] Auto topic-inference for `use_keystone_query` (client-side FROM-clause extractor,
      single-table only — joins have no auto-inference target, document as architectural ceiling)
- [ ] WebGPU rendering proof-of-concept on `CanvasTimeSeries` first (stretch goal: extend to
      `CanvasMap`/`CanvasGraph`)
- [ ] Thin JSX authoring layer: new `packages/canvas-react/` wrapping the existing WASM classes

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
- [ ] Browser playground (built on the Phase 4 demo app) — blocked on Phase 4

## Phase 7 — crates.io release readiness (metadata + publishability only)

- [x] Add `repository`/`homepage`/`documentation`/`readme`/`keywords`/`categories`/`rust-version` to
      every Rust crate's `Cargo.toml` (added this pass)
- [x] Pair the two local `path` deps (`tpt-cli`→`tpt-sdk`, `tpt-sdk`→`tpt-canvas`) with `version`
      fields (`"0.1.0"`), matching each crate's `version` (added this pass)
- [ ] `cargo publish --dry-run` per crate in dependency order, fix whatever it flags (network/publish
      not exercised in this pass; metadata + dep `version` fields are in place)
- [ ] No automated release pipeline in this pass — manual `cargo publish` when ready

## Done outside this list (`cfdafce`)

- [x] `tpt-harbor`: ODBC source connector (`sources/odbc.rs`, `SourceKind::Odbc`) — vendor-agnostic
      DSN-based connector, targets Keystone by default since ODBC's real target engine depends on
      whatever's behind the DSN and the registry has no way to know
