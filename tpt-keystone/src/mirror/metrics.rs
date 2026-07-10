//! Performance metrics store: per-agent latency, token usage, and
//! success/failure rate, stored in `_mirror_metrics` and indexed by a real
//! Chronos time index — unlike Synapse's episodic-memory table (where the
//! Chronos value-column pairing is a placeholder `seq` counter),
//! `latency_ms` here is an actual metric worth rolling up, so
//! `Database::rollup_query`'s count/sum/min/max answers real aggregate
//! latency questions for free.

use std::sync::Arc;

use anyhow::Result;

use crate::storage::database::Database;
use crate::storage::ts_index::{Rollup, TimeBucketPolicy};
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{cell_i64, cell_text, col, decode_cell, encode_cells, int_cell, new_id, now_ms, text_cell};

const TABLE: &str = "_mirror_metrics";
const COL_ID: usize = 0;
const COL_AGENT: usize = 1;
const COL_SESSION: usize = 2;
const COL_LATENCY: usize = 3;
const COL_TOKENS: usize = 4;
const COL_SUCCESS: usize = 5;
const COL_TS: usize = 6;

#[derive(Debug, Clone)]
pub struct MetricEntry {
    pub agent_id: String,
    pub session_id: String,
    pub latency_ms: f64,
    pub tokens: i64,
    pub success: bool,
    pub ts: i64,
}

fn decode_entry(value: &[u8]) -> Option<MetricEntry> {
    Some(MetricEntry {
        agent_id: cell_text(&decode_cell(value, COL_AGENT))?,
        session_id: cell_text(&decode_cell(value, COL_SESSION))?,
        latency_ms: cell_text(&decode_cell(value, COL_LATENCY))?.parse().ok()?,
        tokens: cell_i64(&decode_cell(value, COL_TOKENS))?,
        success: cell_i64(&decode_cell(value, COL_SUCCESS))? != 0,
        ts: cell_i64(&decode_cell(value, COL_TS))?,
    })
}

pub struct MetricsStore {
    db: Arc<Database>,
}

impl MetricsStore {
    pub fn new(db: Arc<Database>) -> Result<Self> {
        if db.get_table(TABLE)?.is_none() {
            db.create_table_with_constraints(
                TABLE,
                &[
                    col("id", ColumnType::Text, false, true),
                    col("agent_id", ColumnType::Text, false, false),
                    col("session_id", ColumnType::Text, false, false),
                    col("latency_ms", ColumnType::Float8, false, false),
                    col("tokens", ColumnType::Int8, false, false),
                    col("success", ColumnType::Int8, false, false),
                    col("ts", ColumnType::Int8, false, false),
                ],
                vec![],
                vec![],
            )?;
        }
        if !db.indexed_column_time(TABLE, "ts") {
            db.create_time_index(TABLE, "ts", "latency_ms", TimeBucketPolicy { granularity_ms: 3_600_000, retention_ms: None })?;
        }
        Ok(Self { db })
    }

    pub fn record(&self, agent_id: &str, session_id: &str, latency_ms: f64, tokens: i64, success: bool) -> Result<String> {
        let id = new_id("metric");
        let cells = vec![
            text_cell(&id),
            text_cell(agent_id),
            text_cell(session_id),
            text_cell(latency_ms.to_string()),
            int_cell(tokens),
            int_cell(if success { 1 } else { 0 }),
            int_cell(now_ms()),
        ];
        self.db.write(TABLE, id.as_bytes(), &encode_cells(&cells))?;
        Ok(id)
    }

    /// `agent_id`'s metric rows with `t0 <= ts <= t1`, answered via the
    /// Chronos time-index range scan and post-filtered by agent (same
    /// "shared table, column-scoped index" tradeoff `synapse::memory`
    /// documents for its own per-agent recall).
    pub fn range(&self, agent_id: &str, t0: i64, t1: i64) -> Result<Vec<MetricEntry>> {
        let Some(rows) = self.db.time_range_query(TABLE, "ts", t0, t1) else { return Ok(Vec::new()) };
        let mut out: Vec<MetricEntry> = rows.into_iter()
            .filter_map(|kv| decode_entry(&kv.value))
            .filter(|e| e.agent_id == agent_id)
            .collect();
        out.sort_by_key(|e| e.ts);
        Ok(out)
    }

    /// `agent_id`'s success rate (`0.0..=1.0`) over `[t0, t1]`, or `None` if
    /// no metrics were recorded in that range.
    pub fn success_rate(&self, agent_id: &str, t0: i64, t1: i64) -> Result<Option<f64>> {
        let entries = self.range(agent_id, t0, t1)?;
        if entries.is_empty() {
            return Ok(None);
        }
        let successes = entries.iter().filter(|e| e.success).count();
        Ok(Some(successes as f64 / entries.len() as f64))
    }

    /// Per-bucket latency rollups (count/sum/min/max) covering `[t0, t1]`
    /// across every agent — the continuous-aggregate path Chronos already
    /// built (`Database::rollup_query`), reused rather than reimplemented.
    pub fn latency_rollup(&self, t0: i64, t1: i64) -> Result<Vec<(i64, Rollup)>> {
        Ok(self.db.rollup_query(TABLE, "ts", t0, t1).unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::config::NodeRole;
    use crate::storage::lease::LeaseManager;
    use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
    use std::time::Duration;

    fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let lease = Arc::new(LeaseManager::new(store.clone(), "db", "node-1".into(), Duration::from_secs(30)));
        lease.try_acquire().unwrap();
        let db = Arc::new(Database::open(local.path(), store, lease.handle(), NodeRole::Writer, Default::default()).unwrap());
        (db, bucket, local)
    }

    #[test]
    fn range_query_answers_via_time_index_and_filters_by_agent() {
        let (db, _b, _l) = test_db();
        let m = MetricsStore::new(db).unwrap();
        m.record("agent1", "sess1", 120.0, 50, true).unwrap();
        m.record("agent2", "sess2", 80.0, 30, true).unwrap();
        let entries = m.range("agent1", 0, now_ms() + 60_000).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].agent_id, "agent1");
    }

    #[test]
    fn success_rate_reflects_recorded_outcomes() {
        let (db, _b, _l) = test_db();
        let m = MetricsStore::new(db).unwrap();
        m.record("agent1", "sess1", 100.0, 10, true).unwrap();
        m.record("agent1", "sess1", 100.0, 10, true).unwrap();
        m.record("agent1", "sess1", 100.0, 10, false).unwrap();
        let rate = m.success_rate("agent1", 0, now_ms() + 60_000).unwrap().unwrap();
        assert!((rate - (2.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn latency_rollup_aggregates_across_agents() {
        let (db, _b, _l) = test_db();
        let m = MetricsStore::new(db).unwrap();
        m.record("agent1", "sess1", 100.0, 10, true).unwrap();
        m.record("agent2", "sess2", 200.0, 10, true).unwrap();
        let rollups = m.latency_rollup(0, now_ms() + 60_000).unwrap();
        let total: u64 = rollups.iter().map(|(_, r)| r.count).sum();
        assert_eq!(total, 2);
        let sum: f64 = rollups.iter().map(|(_, r)| r.sum).sum();
        assert!((sum - 300.0).abs() < 1e-9);
    }
}
