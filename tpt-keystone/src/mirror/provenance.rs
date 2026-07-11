//! Provenance tracking on stored data (not just agent actions): every fact
//! carries who/what asserted it and when, so a consumer (human or AI) can
//! weight reliability without a human sanity-check in the loop.
//!
//! `fact_ref` is a caller-defined string identifying whatever was asserted
//! — a Synapse memory id, a `"table:row_key:column"` triple, a tool result,
//! anything. This module doesn't interpret it, only indexes assertions by
//! it (same "caller owns the reference format" convention `synapse::tools`
//! already uses for its own `schema_json`). A fact can be (re)asserted more
//! than once — `history` returns every assertion, oldest first, so a
//! consumer can see a fact's assertions change confidence/source over time,
//! not just the latest one.

use std::sync::Arc;

use anyhow::Result;

use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{
    cell_i64, cell_text, col, decode_cell, encode_cells, int_cell, new_id, now_ms, text_cell,
};

const TABLE: &str = "_mirror_provenance";
const COL_ID: usize = 0;
const COL_FACT_REF: usize = 1;
const COL_SOURCE: usize = 2;
const COL_CONFIDENCE: usize = 3;
const COL_SEQ: usize = 4;
const COL_TS: usize = 5;

#[derive(Debug, Clone)]
pub struct ProvenanceRecord {
    pub fact_ref: String,
    /// Who/what asserted this — an agent id, `"human"`, `"system"`, or any
    /// other caller-defined actor identifier.
    pub source: String,
    /// Caller-supplied confidence in `[0.0, 1.0]`, if the source provided
    /// one — `None` for sources that don't quantify confidence (e.g. a
    /// direct human assertion).
    pub confidence: Option<f64>,
    pub ts: i64,
}

fn decode_record(value: &[u8]) -> Option<ProvenanceRecord> {
    Some(ProvenanceRecord {
        fact_ref: cell_text(&decode_cell(value, COL_FACT_REF))?,
        source: cell_text(&decode_cell(value, COL_SOURCE))?,
        confidence: cell_text(&decode_cell(value, COL_CONFIDENCE)).and_then(|s| s.parse().ok()),
        ts: cell_i64(&decode_cell(value, COL_TS))?,
    })
}

pub struct ProvenanceLog {
    db: Arc<Database>,
}

impl ProvenanceLog {
    pub fn new(db: Arc<Database>) -> Result<Self> {
        if db.get_table(TABLE)?.is_none() {
            db.create_table_with_constraints(
                TABLE,
                &[
                    col("id", ColumnType::Text, false, true),
                    col("fact_ref", ColumnType::Text, false, false),
                    col("source", ColumnType::Text, false, false),
                    col("confidence", ColumnType::Float8, true, false),
                    col("seq", ColumnType::Int8, false, false),
                    col("ts", ColumnType::Int8, false, false),
                ],
                vec![],
                vec![],
            )?;
        }
        Ok(Self { db })
    }

    pub fn assert(&self, fact_ref: &str, source: &str, confidence: Option<f64>) -> Result<String> {
        let id = new_id("prov");
        let cells = vec![
            text_cell(&id),
            text_cell(fact_ref),
            text_cell(source),
            confidence.map(|c| text_cell(c.to_string())).unwrap_or(None),
            int_cell(crate::mirror::next_seq() as i64),
            int_cell(now_ms()),
        ];
        self.db.write(TABLE, id.as_bytes(), &encode_cells(&cells))?;
        Ok(id)
    }

    /// Every assertion ever recorded for `fact_ref`, oldest first.
    pub fn history(&self, fact_ref: &str) -> Result<Vec<ProvenanceRecord>> {
        let mut records: Vec<(i64, ProvenanceRecord)> = self
            .db
            .scan(TABLE)?
            .into_iter()
            .filter_map(|kv| {
                let seq = cell_i64(&decode_cell(&kv.value, COL_SEQ))?;
                decode_record(&kv.value)
                    .filter(|r| r.fact_ref == fact_ref)
                    .map(|r| (seq, r))
            })
            .collect();
        records.sort_by_key(|(seq, _)| *seq);
        Ok(records.into_iter().map(|(_, r)| r).collect())
    }

    pub fn latest(&self, fact_ref: &str) -> Result<Option<ProvenanceRecord>> {
        Ok(self.history(fact_ref)?.into_iter().last())
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
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let lease = Arc::new(LeaseManager::new(
            store.clone(),
            "db",
            "node-1".into(),
            Duration::from_secs(30),
        ));
        lease.try_acquire().unwrap();
        let db = Arc::new(
            Database::open(
                local.path(),
                store,
                lease.handle(),
                NodeRole::Writer,
                Default::default(),
            )
            .unwrap(),
        );
        (db, bucket, local)
    }

    #[test]
    fn latest_reflects_the_most_recent_assertion() {
        let (db, _b, _l) = test_db();
        let log = ProvenanceLog::new(db).unwrap();
        log.assert("mem-123", "agent1", Some(0.6)).unwrap();
        log.assert("mem-123", "human", Some(1.0)).unwrap();
        let latest = log.latest("mem-123").unwrap().unwrap();
        assert_eq!(latest.source, "human");
        assert_eq!(latest.confidence, Some(1.0));
    }

    #[test]
    fn history_returns_every_assertion_in_order() {
        let (db, _b, _l) = test_db();
        let log = ProvenanceLog::new(db).unwrap();
        log.assert("fact-1", "agent1", Some(0.5)).unwrap();
        log.assert("fact-1", "agent2", Some(0.9)).unwrap();
        let hist = log.history("fact-1").unwrap();
        assert_eq!(hist.len(), 2);
        assert_eq!(hist[0].source, "agent1");
        assert_eq!(hist[1].source, "agent2");
    }
}
