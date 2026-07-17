//! Checkpoint/resume state — a small JSON file written after each phase
//! and after each table's snapshot, so a killed `tpt-keystone-harbor transfer` (or
//! `replicate`) picks back up instead of restarting from Discover.

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    Discover,
    Validate,
    Snapshot,
    Replicate,
    Verify,
    Cutover,
    Done,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Checkpoint {
    pub phase: Option<Phase>,
    /// Tables whose snapshot fully completed — `transfer` skips these on resume.
    pub tables_snapshotted: Vec<String>,
    /// Source-side resume position for CDC (a Postgres LSN for Harbor/PG).
    pub replication_resume_token: Option<String>,
}

impl Checkpoint {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&data)?)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_string_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }

    pub fn mark_table_snapshotted(&mut self, table: &str) {
        if !self.tables_snapshotted.iter().any(|t| t == table) {
            self.tables_snapshotted.push(table.to_string());
        }
    }
}
