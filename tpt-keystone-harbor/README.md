# tpt-keystone-harbor

Universal data migration platform for TPT Keystone. Handles discover → validate → snapshot →
replicate → verify → cutover pipelines with checkpoint/resume, per-row xxHash3 checksums, and
row-count diffing.

## What's implemented

- **Harbor/PG** (PostgreSQL → Keystone) end-to-end: schema discovery, cursor-batched snapshot,
  `pgoutput` logical-replication CDC, live cutover
- Discovery, snapshot, and verification checksums for every other named source too — PostGIS, MySQL,
  MSSQL, Oracle, MongoDB, Neo4j, InfluxDB, Kafka, Elasticsearch, Pinecone/Weaviate/Qdrant, and ODBC
  (Oracle/MySQL/MSSQL/DB2/etc. via the OS driver manager). Live CDC (`replicate`) is also real for
  **Harbor/MSSQL** (polls SQL Server's own CDC tables — requires CDC enabled on the source database/
  tables), **Harbor/Mongo** (`$changeStream` — requires a replica set or sharded cluster), and Kafka
  (gets CDC for free since tailing its own log already is one). It's a named, documented scope cut for
  MySQL/Oracle (binlog/LogMiner CDC not yet written) and for Elasticsearch/Neo4j/InfluxDB/vector
  sources/ODBC (no standard change-feed API to tap at all) — see each `sources::*` module's doc comment
  for its specific reason.
- Keystone target connector (speaks the same hand-written Postgres wire protocol v3 as `tpt-keystone`) —
  the single target every source migrates through, since Meridian/Prism/Chronos/Plexus/Canopy are
  SQL-extension features of `tpt-keystone` itself, not separate services
- Schema Translator IR: per-source type mapping, plus engine-native target shaping so migrated data
  isn't just generic relational columns — PostGIS gets a `SPATIAL` index, vector sources get a `VECTOR`
  (HNSW) index, InfluxDB gets a `TIME` index, Neo4j migrates relationships as `GRAPH`-indexed edge tables
  alongside node properties, and MongoDB/Elasticsearch preserve whole documents as GIN-indexed JSON
  instead of flattening fields into typed columns
- Verification engine: per-row xxHash3 checksums + row-count diffing
- Web dashboard (opt-in via `--dashboard-addr`): read-only HTTP status page over a CLI-driven migration

## Usage

```bash
cd tpt-keystone-harbor
cargo build
./target/debug/tpt-keystone-harbor --help

# Snapshot a Postgres database into Keystone
tpt-keystone-harbor transfer \
  --source-dsn "postgres://user:pass@localhost/mydb" \
  --target-dsn "postgres://localhost:55432/mydb" \
  --dashboard-addr 0.0.0.0:8080
```

## License

Apache-2.0 — Copyright 2026 TPT Solutions
