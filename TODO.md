# TODO — Platform Hardening, Frontend Build-Out & Release Prep

This is a fresh task list superseding the prior phase-by-phase build log (preserved for reference,
untouched, at `TODO 1260716.md`). It tracks a new body of work identified by a full-repo review:
real transactions, security hardening of the non-Postgres network bridges, a Canvas frontend
build-out, dual licensing, adoption tooling, and crates.io release prep. See
`C:\Users\Phillip\.claude\plans\review-project-fix-any-hazy-hennessy.md` for full context/rationale
on each item.

Legend: `[ ]` not started, `[~]` in progress, `[x]` done.

## Phase 1 — Real transactions (highest risk, highest value)

- [ ] Stage 1: per-connection `TxnHandle` in `wire/session.rs`, threaded into `executor/mod.rs`'s
      `execute_parsed`/`execute_query`; staged writes during an open transaction, atomic visibility
      on `COMMIT`, full discard on `ROLLBACK`; read-committed semantics (no snapshot isolation yet)
- [ ] Stage 2: snapshot isolation using `storage/mvcc.rs`'s version-chain design — audit how much of
      `mvcc.rs`/`tx.rs` is actually reusable vs. needs rework before committing to this design
- [ ] Storage-layer support for multiple versions of a key in `lsm.rs`/`sstable.rs`/`wal.rs`, plus
      compaction-time GC of versions no longer visible to any open transaction
- [ ] Concurrency model: verify whether the current single coarse `Mutex<LsmEngine>` remains correct
      under MVCC or needs finer-grained locking
- [ ] Extended query protocol (Parse/Bind/Execute/Sync) and simple query protocol both correctly
      interact with the new per-connection transaction state
- [ ] Extend `phase3_tests.rs`/`chaos_tests.rs` with atomicity/isolation/crash-mid-transaction tests
- [ ] Manual verification: concurrent `BEGIN`/`COMMIT`/`ROLLBACK` from two `psql` sessions

## Phase 2 — DDL/catalog bug fixes

- [ ] `CREATE SEQUENCE IF NOT EXISTS` not enforced — add `if_not_exists` to `CreateSequenceStmt`
      (`sql/ast.rs`), wire through `parser.rs` and `execute_create_sequence` (`ddl.rs`); update
      `docs/sql-reference.md:37`
- [ ] `DROP TABLE` is a complete no-op — add `Database::drop_table` (schema removal, row-data purge,
      per-table secondary-index cleanup); verify `if_exists` is actually wired; document the Phase-3
      reader-node convergence gap (`refresh()`'s `.entry().or_insert()`) as a known follow-up
- [ ] `ALTER TABLE ADD/DROP COLUMN` no-op — add `Database::alter_table_add_column`/
      `alter_table_drop_column` (hold the `lsm` mutex for the whole scan-rewrite pass, use the
      already-locked guard directly, never re-enter via `StorageEngine` trait methods); reject
      `DROP COLUMN` on indexed/PK/unique/FK columns; document non-crash-atomicity and global-lock
      duration as accepted limitations
- [ ] Extend `phase4_tests.rs` / add `ddl_tests.rs` covering all three fixes

## Phase 3 — Auth + rate limiting for HTTP/WebSocket/gRPC/MCP bridges

- [ ] `RoleStore::verify_password` + shared `wire::bridge_auth::authenticate_actor` helper
      (zero-config-preserving: `roles.is_empty()?` checked first, exactly like `session::run`)
- [ ] HTTP + WebSocket (at Upgrade) + gRPC (metadata header) accept `Authorization: Basic`, checked
      via the shared helper; MCP keeps its `X-TPT-Token` gate but gets an `Actor` threaded through too
- [ ] Thread `Actor` into `http_query.rs` and `mcp/tools.rs`/`protocol.rs` for real per-table RBAC
- [ ] Document (don't attempt to fix here) that `websocket.rs`/`wire/grpc/mod.rs` get authentication
      only, not per-topic authorization, since there's no topic-level privilege model in `rbac.rs`
- [ ] Rate limiting: `TPT_HTTP_MAX_CONNECTIONS`, `TPT_FLUX_WS_MAX_CONNECTIONS`,
      `TPT_FLUX_GRPC_MAX_CONNECTIONS`, `TPT_MCP_MAX_CONNECTIONS` (default 1000, `Semaphore` pattern)
- [ ] Update `docs/security_audit_phase12.md` (stale, predates Phase 20 RBAC, never scoped these
      four listeners)
- [ ] New WebSocket auth test file; extend `mcp/tests.rs`, `http_query_tests.rs`,
      `tools/verify_flux_grpc.py`

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

- [ ] Add `LICENSE-MIT` and `LICENSE-APACHE` at repo root
- [ ] Update every crate's `license` field to `"MIT OR Apache-2.0"`: `tpt-keystone`, `tpt-cli`,
      `tpt-harbor`, `tpt-canvas`, `tpt-operator`, `tpt-sdk` (Cargo.toml); `packages/*`
      (package.json); `sdk-python/pyproject.toml`
- [ ] Update `README.md`/`CLAUDE.md` licensing mentions

## Phase 6 — Adoption tooling

- [ ] `CONTRIBUTING.md`, `CHANGELOG.md`, GitHub issue/PR templates
- [ ] `Makefile`/`justfile` wrapping per-crate build/test commands; `install.sh`/`install.ps1`
- [ ] Secure-by-default `docker-compose.yml` (bootstrap credentials required/generated, no more
      silently-open six-port default)
- [ ] Browser playground (built on the Phase 4 demo app)

## Phase 7 — crates.io release readiness (metadata + publishability only)

- [ ] Add `repository`/`keywords`/`categories`/`readme`/`rust-version` to every Rust crate's
      `Cargo.toml`
- [ ] Pair the two local `path` deps (`tpt-cli`→`tpt-sdk`, `tpt-sdk`→`tpt-canvas`) with `version`
      fields
- [ ] `cargo publish --dry-run` per crate in dependency order, fix whatever it flags
- [ ] No automated release pipeline in this pass — manual `cargo publish` when ready
