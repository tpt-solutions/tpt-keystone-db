# Changelog

All notable changes to `tpt-keystone` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is at `0.1.0` and has not been published to crates.io yet, so all changes are listed under
`[Unreleased]`. Changes that span the whole repository are also recorded in the root
[`CHANGELOG.md`](../CHANGELOG.md).

## [Unreleased]

### Added

- Snapshot isolation. Versioned internal keys, MVCC snapshot reads, and snapshot-pinned transactions,
  so a transaction keeps seeing the data it started with while other sessions commit.
- Transactions over the extended query protocol as well as the simple one. `BEGIN`, `COMMIT`, and
  `ROLLBACK` are handled in both, including the transaction-status byte reported by `Sync`.
- Batch-framed write-ahead log. Each commit is one write and one fsync, and replay accepts or discards
  a whole batch at once, so a multi-row commit can no longer be applied partially after a crash.
- Role-based access control (`CREATE`/`ALTER`/`DROP ROLE`, `GRANT`, `REVOKE`).
- SCRAM-SHA-256 authentication and TLS on the wire listener, both opt-in.
- Authentication and rate limiting for the HTTP, WebSocket, gRPC, and MCP bridges, sharing the wire
  listener's role store.
- Optional Cargo features `wasm-udf`, `gpu`, `s3`, and `otlp`, all enabled by default, so a lean build
  can leave out their dependencies.
- Prism vector indexes: IVF-PQ, sharded indexes, DiskANN, and GPU batch-similarity offload.
- Meridian geospatial support: SRIDs, EWKB, and `ST_Transform`.
- Plexus graph support, including GQL `MATCH`.
- Canopy JSON operators and indexes.
- POSIX regular-expression match operators (`~`, `!~`, `~*`, `!~*`).
- Chronos time-series indexes and compression.
- Flux streaming over WebSocket and gRPC, with native `__cdc_<table>` change topics.
- MCP server, HTTP/JSON bridge, Prometheus metrics, and OpenTelemetry export over OTLP/gRPC.
- A Criterion benchmark (`cargo bench`) that measures the engine in-process against the local
  filesystem object store.
- Per-crate `README.md`, `CHANGELOG.md`, and `LICENSE-MIT`/`LICENSE-APACHE` files, and crates.io
  metadata (`repository`, `readme`, `keywords`, `categories`, `rust-version`).

### Changed

- The LSM engine no longer uses one coarse lock. Writers serialise on a write-state lock, and readers
  take only a short lock to clone the current view, so reads do not wait for writes.
- Optional dependencies are behind Cargo features. A build with no features has roughly 71 crates in
  its dependency graph, compared with roughly 379 for the default build.
- Licensed under `MIT OR Apache-2.0`.

### Fixed

- Object writes are synced to disk before they become visible.
- The manifest's `max_commit_seq` field now decodes correctly from older manifests that predate it, and
  reader nodes refresh their watermark when the manifest changes.
- `CREATE SEQUENCE IF NOT EXISTS` is enforced. It was previously parsed and then ignored.
- `DROP TABLE` removes the table from the catalog and deletes its row data and secondary indexes. It was
  previously a no-op.
- `ALTER TABLE ADD COLUMN` and `DROP COLUMN` now backfill or rewrite existing rows. They were previously
  no-ops for existing data.
- The HTTP, WebSocket, and gRPC bridges had no authentication. The MCP token check was off by default and
  bypassed RBAC when it was on. All four now authenticate through the same role store as the wire
  listener.
