# tpt-keystone

Cloud-native, Postgres-wire-compatible relational database engine written from scratch in Rust. Seven
purpose-built engines share one LSM storage substrate: relational (Keystone), geospatial (Meridian),
vector (Prism), time-series (Chronos), graph (Plexus), document (Canopy), and event-streaming (Flux) —
all accessible through standard SQL and the Postgres wire protocol.

## Features

- **Postgres wire protocol v3** — hand-written codec; connect with `psql`, any Postgres driver, or
  `tpt-sdk`
- **LSM storage engine** — MemTable + SSTable + levelled compaction, WAL with fsync
- **MVCC** — `BEGIN`/`COMMIT`/`ROLLBACK` with multi-version concurrency control
- **Cloud-native disaggregated storage** — stateless compute nodes over a shared `ObjectStore`
  (local-fs dev emulation or real S3); single writer, multiple readers via CAS-based lease fencing
- **Six extension engines** — Meridian/geo, Prism/vector, Chronos/time-series, Plexus/graph,
  Canopy/document, Flux/streaming — as SQL extension modules inside the same binary
- **SCRAM-SHA-256 auth + TLS** — opt-in (zero-config default keeps no-auth/no-TLS for development)
- **MCP server**, **HTTP/JSON bridge**, **Flux WebSocket** and **gRPC** streaming endpoints
- **OpenTelemetry** tracing + Prometheus metrics

## Run

```bash
cd tpt-keystone
cargo run
# starts on 0.0.0.0:55432 (Postgres wire), :5433 (MCP), :5434 (Flux WS), :5435 (HTTP/JSON), :5436 (Flux gRPC), :9187 (Prometheus)

psql -h localhost -p 55432
```

Override the listen address with `TPT_PG_ADDR`. Storage defaults to `tpt-data/` on local disk;
set `TPT_STORAGE_BACKEND=s3` and `TPT_S3_BUCKET`/`TPT_S3_REGION` for S3.

## Two-node cloud-native mode

```bash
# Node 1 — writer
TPT_NODE_ROLE=writer TPT_NODE_ID=n1 cargo run

# Node 2 — reader (separate terminal / machine)
TPT_NODE_ROLE=reader TPT_NODE_ID=n2 TPT_PG_ADDR=0.0.0.0:55433 cargo run
```

Both nodes share the same `TPT_LOCAL_STORE_DIR` (or S3 bucket). Only one writer holds the CAS lease
at a time; readers poll-refresh the manifest.

## Status

All 17 roadmap phases have an implementation. This is a single-team, from-scratch project — not a
production-hardened platform. See [`TODO.md`](../TODO.md) for honest per-phase scope and what has
been verified vs. just implemented.

## License

Apache-2.0 — Copyright 2026 TPT Solutions
