# TPT Keystone DB

TPT Keystone DB is a ground-up, multi-engine data platform written in Rust, developed by TPT Solutions.
The long-term plan (see
[`TODO.md`](TODO.md)) is seven purpose-built data engines — relational, geospatial, vector, time-series,
graph, document, and event-streaming — plus an AI layer, a WebGPU frontend framework, a multi-language
SDK ecosystem, a universal migration platform, and an agent orchestration/observability layer, all
sharing one storage substrate.

**Only the first engine, Keystone, is implemented.** Everything else described below the "Roadmap"
section is design intent, not working code.

## Keystone

Keystone (`tpt-keystone/`) is TPT Keystone DB's relational engine — a cloud-native,
PostgreSQL-wire-compatible database, built from scratch with no `pgwire` or `sqlparser-rs` dependency —
the wire protocol codec and the SQL lexer/parser are hand-written. It is currently the only implemented
engine in the platform.

Implemented so far (Phases 0–3 of the roadmap):

- PostgreSQL wire protocol v3 — startup handshake, simple query protocol, and the extended query
  protocol (Parse/Bind/Describe/Execute/Sync/Close)
- Hand-written SQL lexer, recursive-descent parser, and AST
- Full `SELECT` (`WHERE`, `GROUP BY`/`HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`), `JOIN`s (hash/merge/nested
  loop), subqueries, CTEs (including `RECURSIVE`), window functions, `INSERT`/`UPDATE`/`DELETE`, and DDL
- A write-ahead log, MemTable, SSTables with bloom filters, and levelled LSM compaction
- MVCC with a transaction manager (`BEGIN`/`COMMIT`/`ROLLBACK`) and local B-Tree secondary indexes
- Disaggregated cloud-native storage: an `ObjectStore` trait backed by S3 (`aws-sdk-s3`) or a local-fs
  emulation, a cache-aside NVMe layer, a shared manifest, and a CAS-based write lease with fencing —
  stateless compute nodes can scale out and share one bucket, with a single enforced writer

Not yet implemented: WASM UDFs, full `pg_catalog`, a connection pooler, `pg_dump`/`pg_restore`
compatibility, and everything from Phase 5 onward.

## Getting started

```bash
cd tpt-keystone
cargo build
cargo run                      # starts a single-node writer on 0.0.0.0:5432, local-fs storage under tpt-data/
cargo test                     # unit tests + the Phase 3 multi-node cloud-storage integration tests
```

Once running, connect with any Postgres client, e.g. `psql -h localhost -p 5432`.

Compute nodes are configured entirely through environment variables (storage backend, node role, cache
sizing, lease TTL, etc.) — see `tpt-keystone/src/storage/config.rs` for the full list, or
[`CLAUDE.md`](CLAUDE.md) for a summary and an example of running two nodes against shared storage.

## Repository layout

- `tpt-keystone/` — the Keystone engine crate (the only implemented engine)
- `TODO.md` — the authoritative, phase-by-phase build roadmap for the whole platform
- `PHASE2_PLAN.md` — detailed implementation notes for Keystone's SQL engine phase
- `1keystonespec.txt` … `10harbourspec.txt` — per-engine design specs for current and future phases
- `CLAUDE.md` — architecture notes and build/test commands for AI-assisted development in this repo

## Roadmap

See [`TODO.md`](TODO.md) for the full 17-phase plan: Keystone extensions/Postgres compatibility, an AI/MCP
layer, six additional engines (Meridian/geospatial, Prism/vector, Chronos/time-series, Plexus/graph,
Canopy/document, Flux/streaming), production hardening, the Canvas frontend framework, a multi-language
SDK ecosystem, Harbor (universal migration), and Synapse/Mirror (agent orchestration and observability).

## License

Apache License 2.0, Copyright 2026 TPT Solutions. See [`LICENSE`](LICENSE).
