# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project doesn't yet follow strict
semantic versioning across its (currently unversioned, 0.1.0-everywhere) crates — see `TODO.md`
Phase 7 for the crates.io release-readiness work tracking that.

## [Unreleased]

### Added
- Real multi-statement transactions: `BEGIN`/`COMMIT`/`ROLLBACK` now actually stage/commit/discard
  writes instead of being no-ops (see `TODO.md` Phase 1).
- Authentication and rate limiting for the HTTP, WebSocket, gRPC, and MCP bridges, matching the
  Postgres wire listener's SCRAM-SHA-256 + RBAC model, while preserving the zero-config default
  when no roles are configured (see `TODO.md` Phase 3).
- Canvas frontend build-out: a first browser-verified demo app, GQL `MATCH` support in
  `CanvasGraph`, a design-token theming system, and other items from `TODO.md` Phase 4.
- Dual licensing under MIT and Apache-2.0 (`LICENSE-MIT`, `LICENSE-APACHE`), replacing the prior
  Apache-2.0-only license across every crate and package.
- `CONTRIBUTING.md`, this `CHANGELOG.md`, GitHub issue/PR templates, and a repo-root `Makefile`
  wrapping the per-crate build/test commands.
- crates.io release-readiness metadata (`repository`/`keywords`/`categories`/`readme`/
  `rust-version`) across all six Rust crates.

### Fixed
- `CREATE SEQUENCE IF NOT EXISTS` is now actually enforced (was previously parsed and discarded).
- `DROP TABLE` now actually removes the table from the catalog and purges its row data and
  secondary indexes (was previously a complete no-op).
- `ALTER TABLE ADD/DROP COLUMN` now backfills/rewrites existing row data instead of being a no-op.

### Changed
- `docker-compose.yml` now requires `TPT_AUTH_BOOTSTRAP_USER`/`TPT_AUTH_BOOTSTRAP_PASSWORD` (via a
  `.env` file, see `.env.example`) instead of starting fully open on every exposed port by default.

### Security
- The HTTP, WebSocket, and gRPC bridges previously had no authentication mechanism at all, and the
  MCP server's token auth was off by default and bypassed RBAC even when set. All four now
  authenticate through the same role store as the Postgres wire listener when roles are configured.
