//! tpt-keystone-harbor — universal data migration platform (TODO.md Phase 15).
//!
//! Scope actually implemented in this crate (see each module's doc for
//! detail and honest scope cuts):
//! - Core migration engine + lifecycle state machine + checkpoint/resume
//!   ([`engine`]).
//! - Schema Translator IR + Postgres -> Keystone type mapping ([`schema`]).
//! - Verification Engine: per-row xxHash3 checksums + row-count diffing
//!   ([`verify`]).
//! - **Harbor/PG** end to end: discovery, cursor-batched snapshot,
//!   `pgoutput` logical-replication CDC ([`sources::postgres`]).
//! - Keystone target connector ([`target::keystone`]).
//!
//! Every other Phase 15 connector (Mongo, Graph, TimeSeries, Stream,
//! Vector, GIS, Oracle, MySQL, Search, MSSQL) is a named stub in
//! [`sources`] that reports [`connector::ConnectorError::Unimplemented`] —
//! present so the CLI's connector matrix and the engine's trait plumbing
//! are already in place for whichever gets built next.
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
