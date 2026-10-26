# tpt-keystone-harbor

[![crates.io](https://img.shields.io/crates/v/tpt-keystone-harbor.svg)](https://crates.io/crates/tpt-keystone-harbor)
[![license](https://img.shields.io/crates/l/tpt-keystone-harbor.svg)](https://github.com/tpt-solutions/tpt-keystone-db/blob/master/tpt-keystone-harbor/LICENSE-MIT)

Harbor is the data-migration platform for [TPT Keystone](../tpt-keystone/README.md). It moves data from
an existing system into TPT engines through a fixed pipeline:

```
discover → validate → transfer (snapshot) → replicate (live CDC) → verify → cutover
```

Each phase is a subcommand. Progress is saved to a checkpoint file, so an interrupted run can resume.
Verification compares every row's xxHash3 checksum between source and target, and reports row-count
differences.

Harbor is a single binary, `tpt-keystone-harbor`, and a library, `tpt_harbor`.

## What's implemented

Coverage differs by source. The table below reflects what the code implements, not what is planned.

| Source (`--source`) | Discovery and snapshot | Live CDC (`replicate`) | Native target engine |
|---|---|---|---|
| `postgres`: PostgreSQL | Yes | **Yes**: `pgoutput` logical replication | Keystone |
| `gis`: PostGIS | Yes | **Yes**: same path as `postgres` | Meridian (`SPATIAL` index) |
| `ms-sql`: SQL Server | Yes | **Yes**: polls SQL Server's own CDC tables | Keystone |
| `mongo`: MongoDB | Yes | **Yes**: `$changeStream` | Canopy (GIN-indexed JSON) |
| `stream`: Kafka | Yes | **Yes**: tails the topic | Flux |
| `my-sql`: MySQL | Yes | No (binlog CDC not written) | Keystone |
| `oracle`: Oracle | Yes | No (LogMiner CDC not written) | Keystone |
| `graph`: Neo4j | Yes | No (no change-feed API) | Plexus (`GRAPH` edge tables) |
| `time-series`: InfluxDB | Yes | No (no change-feed API) | Chronos (`TIME` index) |
| `search`: Elasticsearch | Yes | No (no change-feed API) | Canopy (GIN-indexed JSON) |
| `vector`: Pinecone, Weaviate, Qdrant | Yes | No (no change-feed API) | Prism (`VECTOR`/HNSW index) |
| `odbc`: any ODBC database | Yes | No (no change-feed API) | Keystone |

Each `sources::*` module's doc comment gives the specific reason for a "No" entry.

The Keystone target connector (`targets::keystone`) is the only target. It speaks the same Postgres
wire protocol v3 that `tpt-keystone` serves. The engine-native shapes in the table come from the Schema
Translator IR, which maps each source's types and indexes to the matching Keystone SQL extension rather
than to generic relational columns.

The verification engine computes per-row xxHash3 checksums and diffs row counts.

Harbor also has an opt-in web dashboard (`--dashboard-addr`). It is a read-only status page for the
duration of one CLI-driven run. It does not drive migrations.

## Requirements

- Rust **1.85** or newer
- **ODBC only:** a unixODBC or Windows ODBC driver manager, plus the vendor's ODBC driver for your
  database. Harbor loads the driver at runtime through the OS driver manager.
- For `replicate`, the source must be configured for CDC:
  - PostgreSQL: `wal_level = logical` and a role with the replication privilege. Harbor creates its
    publication (`tpt_harbor_pub`, for all tables) and replication slot itself.
  - SQL Server: CDC enabled on the database and tables (`sys.sp_cdc_enable_db` /
    `sys.sp_cdc_enable_table`)
  - MongoDB: a replica set or sharded cluster

## Install

From a checkout:

```bash
cd tpt-keystone-harbor
cargo build --release
./target/release/tpt-keystone-harbor --help
```

## Usage

Every phase takes the source and target connection flags. `transfer` is the usual first step:

```bash
tpt-keystone-harbor transfer \
  --source postgres \
  --source-addr 127.0.0.1:5432 \
  --source-user migrator \
  --source-db app \
  --target-addr 127.0.0.1:55432 \
  --checkpoint ./migration-checkpoint.json \
  --dashboard-addr 127.0.0.1:8080
```

Run `tpt-keystone-harbor <command> --help` for the exact flags of each phase.

| Flag | Default | Meaning |
|---|---|---|
| `--source` | `postgres` | Source connector, one of the names in the table above |
| `--source-addr` | required | Source host and port |
| `--source-user`, `--source-db` | `postgres` | Source login and database |
| `--source-conn-str` | | Full ODBC connection string, used only with `--source odbc` |
| `--target-addr` | required | Keystone host and port (the wire port, usually 55432) |
| `--checkpoint` | `./tpt-keystone-harbor-checkpoint.json` | Where progress is saved for resume |
| `--dashboard-addr` | | Serves the read-only dashboard while the command runs |

Phases:

| Command | Purpose |
|---|---|
| `discover` | Lists source tables and their translated schemas |
| `validate` | Dry run: prints the Keystone DDL each table would become, without running it |
| `transfer` | Creates the target tables and bulk-copies the rows |
| `replicate` | Streams live changes until Ctrl-C, resuming from the checkpoint |
| `verify` | Compares per-row checksums and row counts between source and target |
| `cutover` | Checks that verification passed, then marks the migration complete |

## Limitations

- The `--source` flag's help text says only `postgres` is implemented. That text is out of date. The
  table above reflects the code.
- Live CDC is missing for MySQL, Oracle, Neo4j, InfluxDB, Elasticsearch, vector databases, and ODBC.
  For those sources, use `transfer` followed by `verify`, then stop writes to the source before the
  final pass.

## Changelog

Release notes are in [`CHANGELOG.md`](CHANGELOG.md).

## Contributing

See [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE))
- MIT license ([`LICENSE-MIT`](LICENSE-MIT))

at your option. Copyright 2026 TPT Solutions.
