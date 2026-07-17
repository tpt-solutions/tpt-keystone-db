# tpt-harbor

Universal data migration platform for TPT Keystone. Handles discover → validate → snapshot →
replicate → verify → cutover pipelines with checkpoint/resume, per-row xxHash3 checksums, and
row-count diffing.

## What's implemented

- **Harbor/PG** (PostgreSQL → Keystone) end-to-end: schema discovery, cursor-batched snapshot,
  `pgoutput` logical-replication CDC, live cutover
- Keystone target connector (speaks the same hand-written Postgres wire protocol v3 as `tpt-keystone`)
- Schema Translator IR + Postgres → Keystone type mapping
- Verification engine: per-row xxHash3 checksums + row-count diffing
- Web dashboard (opt-in via `--dashboard-addr`): read-only HTTP status page over a CLI-driven migration
- ODBC source stub (Oracle, MySQL, MSSQL via the OS driver manager) — trait-level, not yet runnable

## What's stubbed (not yet runnable)

Mongo, Graph, TimeSeries, Stream, Vector, GIS, MySQL, Search, MSSQL — named connectors that return
`ConnectorError::Unimplemented`. The trait plumbing and CLI connector matrix are in place.

## Usage

```bash
cd tpt-harbor
cargo build
./target/debug/tpt-harbor --help

# Snapshot a Postgres database into Keystone
tpt-harbor transfer \
  --source-dsn "postgres://user:pass@localhost/mydb" \
  --target-dsn "postgres://localhost:55432/mydb" \
  --dashboard-addr 0.0.0.0:8080
```

## License

Apache-2.0 — Copyright 2026 TPT Solutions
