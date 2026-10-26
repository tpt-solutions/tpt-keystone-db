# tpt-keystone

[![crates.io](https://img.shields.io/crates/v/tpt-keystone.svg)](https://crates.io/crates/tpt-keystone)
[![docs.rs](https://img.shields.io/docsrs/tpt-keystone)](https://docs.rs/tpt-keystone)
[![CI](https://github.com/tpt-solutions/tpt-keystone-db/actions/workflows/ci.yml/badge.svg)](https://github.com/tpt-solutions/tpt-keystone-db/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/tpt-keystone.svg)](https://github.com/tpt-solutions/tpt-keystone-db/blob/master/tpt-keystone/LICENSE-MIT)

`tpt-keystone` is a cloud-native, Postgres-wire-compatible relational database engine written from
scratch in Rust. Seven purpose-built engines share one LSM storage substrate and are all reachable
through standard SQL over the Postgres wire protocol:

| Engine | Name | What it adds |
|---|---|---|
| Relational | **Keystone** | Tables, SQL, MVCC transactions, secondary indexes |
| Geospatial | **Meridian** | PostGIS-style geometry types, SRIDs, spatial indexes |
| Vector | **Prism** | k-NN vector search (HNSW, IVF-PQ, DiskANN, sharded) with optional GPU offload |
| Time-series | **Chronos** | Time-bucketed indexes and compressed series storage |
| Graph | **Plexus** | Graph tables and GQL-style `MATCH` queries |
| Document | **Canopy** | JSON/JSONB storage, operators, and GIN-style indexes |
| Streaming | **Flux** | Change-data-capture topics, pushed over WebSocket and gRPC |

The six extension engines are SQL extension modules inside the same binary, not separate storage
engines. They add `CREATE INDEX ... USING <KIND>` options and table-valued functions on top of the
same query pipeline.

## Features

- **Postgres wire protocol v3**, hand-written codec. Connect with `psql`, any Postgres driver, or
  [`tpt-keystone-sdk`](../tpt-keystone-sdk/README.md). The simple and extended query protocols are
  both supported, including binary result formats for common types.
- **LSM storage engine**: MemTable, SSTables with bloom filters, levelled compaction, and a
  batch-framed write-ahead log with fsync. Reads do not take the writer's lock.
- **MVCC transactions**: `BEGIN` / `COMMIT` / `ROLLBACK` with snapshot isolation over internal
  versioned keys.
- **Cloud-native disaggregated storage**: compute nodes are stateless and share an `ObjectStore`.
  Use the local-filesystem emulation for development or real S3 in production. There is one writer
  at a time, enforced by a CAS-based lease with a monotonic fencing token. Readers poll-refresh the
  shared manifest.
- **Authentication and TLS, opt-in**: SCRAM-SHA-256 over the wire and TLS when configured. With no
  roles and no certificate configured, the server keeps its zero-config no-auth, no-TLS default for
  local development.
- **Bridges**: an MCP server, an HTTP/JSON bridge (used by browser and edge SDKs), a Flux WebSocket
  push endpoint, and a Flux gRPC streaming endpoint. All of them share the wire listener's role
  store when roles are configured.
- **Observability**: Prometheus metrics on `/metrics`, and OpenTelemetry span export over OTLP/gRPC
  when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
- **Optional accelerators**: sandboxed WASM user-defined functions and GPU offload for vector k-NN and
  spatial joins. Both can be left out of the build entirely (see [Lean builds](#lean-builds)).

## Requirements

- Rust **1.85** or newer (`rust-version` in `Cargo.toml`)
- No external database, message broker, or Postgres installation is required

## Quick start

```bash
cd tpt-keystone
cargo run
# Listening on 0.0.0.0:55432 (Postgres wire), :5433 (MCP), :5434 (Flux WS),
#              :5435 (HTTP/JSON), :5436 (Flux gRPC), :9187 (Prometheus)

psql -h localhost -p 55432
```

```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO users VALUES (1, 'ada');
BEGIN;
UPDATE users SET name = 'Ada' WHERE id = 1;
COMMIT;
SELECT * FROM users;
```

With no configuration, data lives under `tpt-data/` in the working directory and no authentication is
required. Do not expose that default to a network.

## Ports

| Port | Variable | Protocol |
|---|---|---|
| `55432` | `TPT_PG_ADDR` | Postgres wire protocol v3 |
| `5433` | `TPT_MCP_ADDR` | MCP server |
| `5434` | `TPT_FLUX_WS_ADDR` | Flux WebSocket push |
| `5435` | `TPT_HTTP_ADDR` | HTTP/JSON bridge (used by browser and edge SDKs; serves `/schema`) |
| `5436` | `TPT_FLUX_GRPC_ADDR` | Flux gRPC streaming (h2c) |
| `9187` | `TPT_METRICS_ADDR` | Prometheus `/metrics` |

## Configuration

Every setting is an environment variable. The defaults shown are the zero-config values.

### Storage

| Variable | Default | Purpose |
|---|---|---|
| `TPT_STORAGE_BACKEND` | `local` | `local` (filesystem emulation) or `s3` |
| `TPT_LOCAL_STORE_DIR` | `tpt-data/objects` | Shared object-store root when `local` |
| `TPT_LOCAL_DIR` | `tpt-data` | Per-node working directory (WAL in flight, local indexes) |
| `TPT_CACHE_DIR` | `tpt-data/cache` | NVMe cache directory for the cache-aside layer |
| `TPT_CACHE_MAX_BYTES` | | Cache size cap |
| `TPT_S3_BUCKET` | | Required when `TPT_STORAGE_BACKEND=s3` |
| `TPT_S3_REGION`, `TPT_S3_ENDPOINT`, `TPT_S3_PREFIX` | | Optional; the endpoint is for S3-compatible stores, the prefix for key namespacing |

### Cluster role

| Variable | Default | Purpose |
|---|---|---|
| `TPT_NODE_ROLE` | `writer` | `writer` (takes the lease) or `reader` |
| `TPT_NODE_ID` | random | Node identifier used in logs and the lease record |
| `TPT_LEASE_TTL_SECS` | | Write-lease time-to-live |
| `TPT_MANIFEST_REFRESH_SECS` | | How often readers poll the manifest |

### Security

| Variable | Purpose |
|---|---|
| `TPT_AUTH_BOOTSTRAP_USER` / `TPT_AUTH_BOOTSTRAP_PASSWORD` | Creates the first role so SCRAM auth switches on |
| `TPT_TLS_CERT_PATH` / `TPT_TLS_KEY_PATH` | Enables TLS on the wire listener when both are set |
| `TPT_MCP_TOKEN` | Bearer token for the MCP server |

### Limits and tuning

| Variable | Purpose |
|---|---|
| `TPT_MAX_CONNECTIONS` | Postgres wire connection cap |
| `TPT_HTTP_MAX_CONNECTIONS`, `TPT_MCP_MAX_CONNECTIONS`, `TPT_FLUX_WS_MAX_CONNECTIONS`, `TPT_FLUX_GRPC_MAX_CONNECTIONS` | Per-bridge connection caps |
| `TPT_OSS_MAX_INFLIGHT`, `TPT_OSS_CIRCUIT_FAILURES`, `TPT_OSS_CIRCUIT_OPEN_SECS` | Object-store concurrency and circuit breaker |
| `TPT_COMPACTION_SSTABLE_THRESHOLD`, `TPT_SST_SHARD_COUNT` | LSM compaction and SSTable sharding |
| `TPT_IO_URING` | Set to `1` to use the io_uring I/O backend (Linux only) |
| `TPT_JSONB_BINARY` | Set to `1` to store JSONB in binary form (off by default, raw text) |
| `TPT_UDF_MEMORY_LIMIT_BYTES`, `TPT_UDF_FUEL_LIMIT`, `TPT_UDF_MAX_MODULE_BYTES` | Limits for WASM UDFs |
| `TPT_DISABLE_GPU_VECTOR`, `TPT_DISABLE_GPU_JOIN`, `TPT_GPU_VECTOR_MAX`, `TPT_GPU_JOIN_THRESHOLD`, `TPT_GPU_JOIN_MAX_PAIRS` | GPU offload switches and thresholds |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Enables OTLP span export |

## Two-node cloud-native mode

```bash
# Node 1: writer
TPT_NODE_ROLE=writer TPT_NODE_ID=n1 cargo run

# Node 2: reader (separate terminal or machine)
TPT_NODE_ROLE=reader TPT_NODE_ID=n2 TPT_PG_ADDR=0.0.0.0:55433 cargo run
```

Both nodes must point at the same store, either the same `TPT_LOCAL_STORE_DIR` or the same S3 bucket.
Only one writer holds the lease at a time. A writer that has been superseded is rejected by the
manifest's compare-and-swap, even if it has not yet noticed that its lease expired. Readers serve
reads from the last manifest they refreshed.

Secondary indexes are local to each node and are not replicated through the object store. Each node
rebuilds them from its own disk.

## Lean builds

Four capabilities are behind Cargo features, all enabled by default:

| Feature | Enables | Extra dependencies |
|---|---|---|
| `wasm-udf` | `CREATE FUNCTION ... LANGUAGE wasm` sandboxed UDFs | `wasmtime`, `cranelift` |
| `gpu` | GPU offload for vector k-NN and spatial joins (falls back to CPU when no adapter exists) | `wgpu` |
| `s3` | `TPT_STORAGE_BACKEND=s3` | `aws-sdk-s3`, `aws-config` |
| `otlp` | OTLP/gRPC span export | `opentelemetry-otlp`, `tonic`, `hyper` |

`cargo build` and `cargo run` include all four. To build without them, pass `--no-default-features`
and then choose a subset:

```bash
# Local-only build: no AWS, GPU, or OTLP dependencies
cargo build --no-default-features --features wasm-udf
```

A build with no features has roughly 71 unique crates in its dependency graph, compared with roughly
379 for the default build. A disabled feature fails at runtime with a clear error (for example,
`CREATE FUNCTION ... LANGUAGE wasm` or `TPT_STORAGE_BACKEND=s3`), or falls back to a no-op where that
is safe (GPU paths use the CPU path, and OTLP stays stdout-only).

## Testing and benchmarks

```bash
cargo test                          # unit tests and integration tests
cargo test phase3_tests::           # multi-node cloud-storage integration tests
cargo bench                         # Criterion benchmark: engine throughput and latency, in-process
cargo run --example gpu_smoke_test  # GPU path smoke test (falls back to CPU without an adapter)
```

The benchmark measures the engine alone, in-process, against the local-filesystem object store. It
does not measure real S3 or NVMe I/O, and it is not a comparison against other databases.

## Library use

The crate also builds as a library, `tpt_keystone`, which exposes the engine modules (`wire`, `sql`,
`executor`, `storage`, and the extension engines). The library exists so that the benchmarks and
examples can use the engine without starting a server. Its API is not stable before 1.0.

## Documentation

- [`docs/architecture.md`](../docs/architecture.md): module layout and request flow
- [`docs/sql-reference.md`](../docs/sql-reference.md): supported SQL
- [`docs/sdks.md`](../docs/sdks.md): client libraries
- [`docs/security_audit_phase12.md`](../docs/security_audit_phase12.md): security review
- [`docs/mvcc_snapshot_audit.md`](../docs/mvcc_snapshot_audit.md) and
  [`docs/concurrency_model_audit.md`](../docs/concurrency_model_audit.md): transaction and
  concurrency design notes

## Status and scope

All 17 roadmap phases have an implementation. This is a single-team, from-scratch project and is not
a production-hardened platform. Each phase's scope cuts, and whether it was verified in-process or
against a real external server, are recorded in [`TODO.md`](../TODO.md). Read that file before
relying on a feature.

Known limits include:

- Secondary indexes, including the extension engines' indexes, are local to one node.
- Authentication and TLS are opt-in. The zero-config default has neither.
- The benchmark is an in-process engine benchmark, not an end-to-end database comparison.

## Changelog

Release notes are in [`CHANGELOG.md`](CHANGELOG.md).

## Contributing

See [`CONTRIBUTING.md`](../CONTRIBUTING.md) for build, test, and pull-request conventions.

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE))
- MIT license ([`LICENSE-MIT`](LICENSE-MIT))

at your option. Copyright 2026 TPT Solutions.
