//! Source connector registry. Each module implements
//! [`crate::connector::SourceConnector`] for its source database/service.
//! The Postgres connector (`postgres`) is the most complete (including
//! `pgoutput` logical replication CDC); other connectors implement
//! discovery, snapshot, and checksums, with CDC scope-cut to
//! `ConnectorError::Unimplemented` where the source lacks a standard
//! change-feed API.

pub mod elasticsearch;
pub mod influxdb;
pub mod kafka;
pub mod mongodb;
pub mod mssql;
pub mod mysql;
pub mod neo4j;
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
        }
    }
}
