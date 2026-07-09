//! Source/target connector traits every Harbor plugin implements. Only
//! `sources::postgres` (source) and `target::keystone` (target) have real
//! bodies; the rest of `sources` are stubs that return
//! [`ConnectorError::Unimplemented`] so the CLI's connector matrix is
//! complete even where the protocol work isn't.

use crate::schema::TableSchema;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("{connector}: not yet implemented ({detail})")]
    Unimplemented { connector: &'static str, detail: &'static str },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// One row of table data in the source's own text-ish representation —
/// `None` is SQL NULL, `Some` is the wire's text-format bytes. Kept
/// column-name-free here; callers zip against the owning [`TableSchema`].
pub type SourceRow = Vec<Option<Vec<u8>>>;

/// A single change captured during the `Replicate` (live CDC) phase.
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    Insert { table: String, row: SourceRow },
    Update { table: String, key: SourceRow, row: SourceRow },
    Delete { table: String, key: SourceRow },
    CommitLsn(String),
}

#[async_trait]
pub trait SourceConnector: Send {
    /// Human-readable name, e.g. `"Harbor/PG"`.
    fn name(&self) -> &'static str;

    /// Discover phase: enumerate tables and translate each to the IR.
    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError>;

    /// Snapshot phase: stream all rows of one table as batches over `tx`,
    /// so memory use stays bounded regardless of table size. The caller
    /// (the migration engine) drains `tx`'s receiver concurrently via
    /// `tokio::join!`, applying each batch to the target as it arrives —
    /// see `engine::MigrationEngine::snapshot_one_table`.
    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError>;

    /// Replicate phase: begin (or resume, from `resume_token`) live CDC,
    /// sending each captured change over `tx`. Returns once the source
    /// ends the stream or `tx`'s receiver is dropped (the engine dropping
    /// it is how a caller signals "stop").
    async fn replicate(&mut self, tables: &[TableSchema], resume_token: Option<String>, tx: Sender<ChangeEvent>) -> Result<(), ConnectorError>;

    /// Row checksum inputs for the Verification Engine: `(table, xxh3 of
    /// each row's canonical bytes)`, computed source-side.
    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError>;
}

#[async_trait]
pub trait TargetConnector: Send {
    fn name(&self) -> &'static str;

    async fn apply_ddl(&mut self, table: &TableSchema) -> Result<(), ConnectorError>;

    async fn write_rows(&mut self, table: &TableSchema, rows: Vec<SourceRow>) -> Result<(), ConnectorError>;

    async fn apply_change(&mut self, table: &TableSchema, event: &ChangeEvent) -> Result<(), ConnectorError>;

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError>;
}

/// Shared helper: every stub source connector answers the same way.
macro_rules! unimplemented_source {
    ($ty:ident, $name:literal, $detail:literal) => {
        pub struct $ty;

        #[async_trait::async_trait]
        impl crate::connector::SourceConnector for $ty {
            fn name(&self) -> &'static str {
                $name
            }
            async fn discover(&mut self) -> Result<Vec<crate::schema::TableSchema>, crate::connector::ConnectorError> {
                Err(crate::connector::ConnectorError::Unimplemented { connector: $name, detail: $detail })
            }
            async fn snapshot_table(
                &mut self,
                _table: &crate::schema::TableSchema,
                _tx: tokio::sync::mpsc::Sender<Vec<crate::connector::SourceRow>>,
            ) -> Result<u64, crate::connector::ConnectorError> {
                Err(crate::connector::ConnectorError::Unimplemented { connector: $name, detail: $detail })
            }
            async fn replicate(
                &mut self,
                _tables: &[crate::schema::TableSchema],
                _resume_token: Option<String>,
                _tx: tokio::sync::mpsc::Sender<crate::connector::ChangeEvent>,
            ) -> Result<(), crate::connector::ConnectorError> {
                Err(crate::connector::ConnectorError::Unimplemented { connector: $name, detail: $detail })
            }
            async fn row_checksums(&mut self, _table: &crate::schema::TableSchema) -> Result<Vec<u64>, crate::connector::ConnectorError> {
                Err(crate::connector::ConnectorError::Unimplemented { connector: $name, detail: $detail })
            }
        }
    };
}

pub(crate) use unimplemented_source;
