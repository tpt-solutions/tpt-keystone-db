//! Source connector registry. Each module implements
//! [`crate::connector::SourceConnector`] for its source database/service.
//! Every connector implements discovery, snapshot, and checksums; live CDC
//! (`replicate`) is implemented for `postgres`/`postgis` (`pgoutput` logical
//! replication), `kafka` (its native log is itself a change feed), `mssql`
//! (polls `cdc.fn_cdc_get_all_changes_*`, requires CDC enabled on the
//! source), and `mongodb` (`$changeStream` aggregate, requires a replica
//! set/sharded cluster). It's scope-cut to `ConnectorError::Unimplemented`
//! for `mysql`/`oracle` (binlog/LogMiner CDC not yet written — a real
//! backlog item, unlike the next group) and for `elasticsearch`/`neo4j`/
//! `influxdb`/`vector`/`odbc`, which lack a standard change-feed API at all.

pub mod elasticsearch;
pub mod influxdb;
pub mod kafka;
pub mod mongodb;
pub mod mssql;
pub mod mysql;
pub mod neo4j;
pub mod odbc;
pub mod oracle;
pub mod postgres;
pub mod postgis;
pub mod vector;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SourceKind {
    Postgres,
    Mongo,
    Graph,
    TimeSeries,
    Stream,
    Vector,
    Gis,
    Oracle,
    MySql,
    Search,
    MsSql,
    Odbc,
}

impl SourceKind {
    pub fn target_engine(&self) -> &'static str {
        match self {
            SourceKind::Postgres | SourceKind::Oracle | SourceKind::MySql | SourceKind::MsSql => "Keystone",
            SourceKind::Mongo | SourceKind::Search => "Canopy",
            SourceKind::Graph => "Plexus",
            SourceKind::TimeSeries => "Chronos",
            SourceKind::Stream => "Flux",
            SourceKind::Vector => "Prism",
            SourceKind::Gis => "Meridian",
            // ODBC is vendor-agnostic — the "real" target depends on the
            // underlying database behind the DSN, which this registry has
            // no way to know. Keystone (the relational engine) is the sane
            // default since ODBC's primary use case here is other
            // relational databases without their own connector.
            SourceKind::Odbc => "Keystone",
        }
    }
}
