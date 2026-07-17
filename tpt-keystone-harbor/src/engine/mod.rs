//! Core migration engine: drives the Discover -> Validate -> Snapshot ->
//! Replicate -> Verify -> Cutover lifecycle over a [`SourceConnector`] /
//! [`TargetConnector`] pair, persisting a [`Checkpoint`] so `transfer`/
//! `replicate` can resume after a crash instead of restarting.

pub mod checkpoint;

use crate::connector::{ConnectorError, SourceConnector, TargetConnector};
use crate::schema::TableSchema;
use crate::verify::{verify_table, TableVerification};
use checkpoint::{Checkpoint, Phase};
use std::path::PathBuf;

pub struct MigrationEngine {
    pub source: Box<dyn SourceConnector>,
    pub target: Box<dyn TargetConnector>,
    pub checkpoint_path: PathBuf,
    pub checkpoint: Checkpoint,
}

impl MigrationEngine {
    pub fn new(source: Box<dyn SourceConnector>, target: Box<dyn TargetConnector>, checkpoint_path: PathBuf) -> anyhow::Result<Self> {
        let checkpoint = Checkpoint::load(&checkpoint_path)?;
        Ok(Self { source, target, checkpoint_path, checkpoint })
    }

    fn save(&self) -> anyhow::Result<()> {
        self.checkpoint.save(&self.checkpoint_path)
    }

    /// Discover phase: enumerate + translate source schema. Read-only —
    /// touches neither checkpoint nor target.
    pub async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let tables = self.source.discover().await?;
        self.checkpoint.phase = Some(Phase::Discover);
        let _ = self.save();
        Ok(tables)
    }

    /// Validate (dry run) phase: translate schema and render target DDL
    /// without executing it, so the caller can review before committing.
    pub fn validate(&self, tables: &[TableSchema]) -> Vec<String> {
        tables.iter().map(|t| t.to_keystone_ddl()).collect()
    }

    /// Snapshot phase: apply DDL then bulk-copy every table not already
    /// marked done in the checkpoint. `on_progress` is called with
    /// `(table_name, rows_copied_so_far)` after each batch.
    pub async fn snapshot(&mut self, tables: &[TableSchema], mut on_progress: impl FnMut(&str, u64) + Send) -> Result<(), ConnectorError> {
        self.checkpoint.phase = Some(Phase::Snapshot);
        for table in tables {
            if self.checkpoint.tables_snapshotted.iter().any(|t| t == &table.qualified_name()) {
                continue;
            }
            self.target.apply_ddl(table).await?;
            self.snapshot_one_table(table, &mut on_progress).await?;
            self.checkpoint.mark_table_snapshotted(&table.qualified_name());
            let _ = self.save();
        }
        let _ = self.save(); // persist the Snapshot phase transition even if every table was already done
        Ok(())
    }

    /// Runs the source's producer (writing batches into a channel) and a
    /// local consumer loop (draining the channel into the target) as two
    /// futures polled concurrently on this task via `tokio::join!` — no
    /// `spawn`, so no `Send + 'static` requirement on the trait objects,
    /// and no blocking-inside-async hazard from mixing sync callbacks with
    /// an async target write.
    async fn snapshot_one_table(&mut self, table: &TableSchema, on_progress: &mut (dyn FnMut(&str, u64) + Send)) -> Result<(), ConnectorError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<crate::connector::SourceRow>>(4);
        let table_name = table.qualified_name();
        let source = &mut self.source;
        let target = &mut self.target;

        let produce = source.snapshot_table(table, tx);
        let consume = async {
            let mut total: u64 = 0;
            while let Some(batch) = rx.recv().await {
                total += batch.len() as u64;
                target.write_rows(table, batch).await?;
                on_progress(&table_name, total);
            }
            Ok::<(), ConnectorError>(())
        };

        let (produced, consumed) = tokio::join!(produce, consume);
        produced?;
        consumed?;
        Ok(())
    }

    /// Replicate phase: live CDC from `resume_token` (or the checkpoint's
    /// saved token) onward, applying each change to the target and
    /// persisting the resume token as changes commit. Runs until the
    /// source ends the stream or `should_stop` returns true (checked
    /// between events; returning true drops the channel receiver, which
    /// the source's producer side treats as a stop request).
    pub async fn replicate(&mut self, tables: &[TableSchema], mut should_stop: impl FnMut() -> bool + Send) -> Result<(), ConnectorError> {
        self.checkpoint.phase = Some(Phase::Replicate);
        let resume = self.checkpoint.replication_resume_token.clone();
        let table_by_name: std::collections::HashMap<&str, &TableSchema> = tables.iter().map(|t| (t.name.as_str(), t)).collect();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::connector::ChangeEvent>(64);
        let source = &mut self.source;
        let target = &mut self.target;
        let checkpoint = &mut self.checkpoint;

        let produce = source.replicate(tables, resume, tx);
        let consume = async {
            while let Some(event) = rx.recv().await {
                if should_stop() {
                    break;
                }
                match &event {
                    crate::connector::ChangeEvent::CommitLsn(token) if !token.is_empty() => {
                        checkpoint.replication_resume_token = Some(token.clone());
                    }
                    crate::connector::ChangeEvent::Insert { table, .. }
                    | crate::connector::ChangeEvent::Update { table, .. }
                    | crate::connector::ChangeEvent::Delete { table, .. } => {
                        if let Some(schema) = table_by_name.get(table.as_str()) {
                            target.apply_change(schema, &event).await?;
                        }
                    }
                    _ => {}
                }
            }
            Ok::<(), ConnectorError>(())
        };

        let (produced, consumed) = tokio::join!(produce, consume);
        let _ = self.checkpoint.save(&self.checkpoint_path);
        produced?;
        consumed?;
        Ok(())
    }

    /// Verify phase: compare per-row checksums table by table.
    pub async fn verify(&mut self, tables: &[TableSchema]) -> Result<Vec<TableVerification>, ConnectorError> {
        self.checkpoint.phase = Some(Phase::Verify);
        let mut results = Vec::new();
        for table in tables {
            let source_hashes = self.source.row_checksums(table).await?;
            let target_hashes = self.target.row_checksums(table).await?;
            results.push(verify_table(&table.qualified_name(), &source_hashes, &target_hashes));
        }
        let _ = self.save();
        Ok(results)
    }

    /// Cutover phase: marks the migration done. Harbor doesn't attempt to
    /// pause application traffic or flip DNS/connection strings itself —
    /// that's environment-specific; this records that verification passed
    /// and the operator can now redirect writers to the target.
    pub fn cutover(&mut self, verifications: &[TableVerification]) -> anyhow::Result<bool> {
        let all_passed = verifications.iter().all(|v| v.passed);
        if all_passed {
            self.checkpoint.phase = Some(Phase::Done);
            self.save()?;
        }
        Ok(all_passed)
    }
}
