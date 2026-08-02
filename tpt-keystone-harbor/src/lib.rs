//! tpt-keystone-harbor — universal data migration platform (TODO.md Phase 15).
//!
//! Scope actually implemented in this crate (see each module's doc for
//! detail and honest scope cuts):
//! - Core migration engine + lifecycle state machine + checkpoint/resume
//!   ([`engine`]).
//! - Schema Translator IR + per-source type mapping, plus an `IndexSpec` IR
//!   for engine-native target indexes ([`schema`]).
//! - Verification Engine: per-row xxHash3 checksums + row-count diffing
//!   ([`verify`]).
//! - Discovery, snapshot, and checksums are real for every named source
//!   below. Live CDC (`replicate`) is real for **Harbor/PG**/**Harbor/GIS**
//!   (`pgoutput` logical replication, [`sources::postgres`]), **Harbor/
//!   Stream** (Kafka gets CDC for free since tailing its own log already is
//!   one), **Harbor/MSSQL** (polls SQL Server's own CDC tables, requires CDC
//!   enabled on the source database/tables), and **Harbor/Mongo**
//!   (`$changeStream`, requires a replica set or sharded cluster). It's a
//!   named, documented scope cut for MySQL/Oracle (binlog/LogMiner CDC not
//!   yet written) and for Elasticsearch/Neo4j/InfluxDB/vector sources/ODBC
//!   (no standard change-feed API to tap at all) — see each `sources::*`
//!   module's own doc for its specific reason.
//! - Keystone target connector ([`targets::keystone`]) — the single target
//!   every source migrates through, since Meridian/Prism/Chronos/Plexus/
//!   Canopy are SQL-extension features of `tpt-keystone` itself rather than
//!   separate services. Sources whose native target is one of those engines
//!   (`sources::SourceKind::target_engine()`) shape their `TableSchema`/rows
//!   accordingly instead of a generic relational layout: PostGIS/Harbor-Gis
//!   gets a `SPATIAL` index, vector sources get a `VECTOR` (HNSW) index,
//!   InfluxDB gets a `TIME` index, Neo4j also migrates relationships as
//!   `GRAPH`-indexed edge tables (previously dropped entirely), and
//!   Mongo/Elasticsearch preserve whole documents as GIN-indexed JSON rather
//!   than flattening fields into typed columns.
//! - Web dashboard: a hand-rolled, read-only HTTP status server + embedded
//!   polling page ([`dashboard`]), opt-in via `--dashboard-addr` on
//!   `transfer`/`replicate`/`verify`/`cutover` — a status view over a
//!   CLI-driven migration, not a second way to drive one.

pub mod connector;
pub mod dashboard;
pub mod engine;
pub mod http;
pub mod pgwire;
pub mod schema;
pub mod sources;
pub mod targets;
pub mod verify;
