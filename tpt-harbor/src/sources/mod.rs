//! Source connector registry. `postgres` (Harbor/PG) is fully implemented;
//! every other TODO.md Phase 15 connector is a named stub so
//! `tpt-harbor discover --source <kind>` recognizes the target and reports
//! "not yet implemented" instead of failing to parse the CLI arg at all.

pub mod postgres;

use crate::connector::unimplemented_source;

unimplemented_source!(MongoSource, "Harbor/Mongo", "MongoDB wire protocol bulk export + oplog CDC not yet written");
unimplemented_source!(GraphSource, "Harbor/Graph", "Neo4j Bolt protocol export not yet written");
unimplemented_source!(TimeSeriesSource, "Harbor/TimeSeries", "InfluxDB/TimescaleDB TSM/hypertable readers not yet written");
unimplemented_source!(StreamSource, "Harbor/Stream", "Kafka/RabbitMQ consumer-group replay not yet written");
unimplemented_source!(VectorSource, "Harbor/Vector", "Pinecone/Weaviate/Qdrant REST/gRPC export not yet written");
unimplemented_source!(GisSource, "Harbor/GIS", "PostGIS geometry/GiST-index reader not yet written");
unimplemented_source!(OracleSource, "Harbor/Oracle", "Oracle Net/LogMiner client and PL/SQL transpiler not yet written");
unimplemented_source!(MySqlSource, "Harbor/MySQL", "MySQL protocol + binlog CDC client not yet written");
unimplemented_source!(SearchSource, "Harbor/Search", "Elasticsearch scroll-API export not yet written");
unimplemented_source!(MsSqlSource, "Harbor/MSSQL", "TDS protocol + CDC/Change Tracking client not yet written");

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
