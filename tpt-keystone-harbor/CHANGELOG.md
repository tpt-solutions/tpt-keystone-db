# Changelog

All notable changes to `tpt-keystone-harbor` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is at `0.1.0` and has not been published to crates.io yet, so all changes are listed under
`[Unreleased]`. Changes that span the whole repository are also recorded in the root
[`CHANGELOG.md`](../CHANGELOG.md).

## [Unreleased]

### Added

- Migration pipeline with the phases `discover`, `validate`, `transfer`, `replicate`, `verify`, and
  `cutover`, with checkpoint and resume.
- PostgreSQL source (Harbor/PG): schema discovery, cursor-batched snapshot, `pgoutput` logical-replication
  CDC, and cutover.
- PostGIS source (Harbor/GIS), which shares the PostgreSQL CDC path and maps to a `SPATIAL` index.
- Source connectors for MySQL, Oracle, Elasticsearch, Neo4j, InfluxDB, Kafka, Pinecone, Weaviate, and
  Qdrant, and an ODBC source for any database with an ODBC driver. These support discovery, snapshot, and
  checksums.
- Live CDC for SQL Server (polls its CDC tables), MongoDB (`$changeStream`), and Kafka (tails the topic).
- Kafka topic DDL.
- Engine-native target shapes: `SPATIAL` for PostGIS, `VECTOR` (HNSW) for vector sources, `TIME` for
  InfluxDB, `GRAPH` edge tables for Neo4j, and GIN-indexed JSON for MongoDB and Elasticsearch.
- Verification engine: per-row xxHash3 checksums and row-count diffing.
- Read-only web dashboard, enabled with `--dashboard-addr`.
- Per-crate `README.md`, `CHANGELOG.md`, and `LICENSE-MIT`/`LICENSE-APACHE` files, and crates.io
  metadata (`repository`, `readme`, `keywords`, `categories`, `rust-version`).

### Changed

- Renamed from `tpt-harbor` to `tpt-keystone-harbor`. The binary is `tpt-keystone-harbor`.
- Licensed under `MIT OR Apache-2.0`.
- The target module is now `src/targets/`. It was `src/target/`, which Cargo's packaging excluded because
  it matches the build-output directory name.

### Fixed

- Linking failure in CI on the ODBC dependency, and compiler warnings.

### Known limits

- Live CDC is not implemented for MySQL, Oracle, Neo4j, InfluxDB, Elasticsearch, vector databases, or
  ODBC.
- The `--source` help text still says only `postgres` is implemented. The sources table in the README is
  the accurate list.
