//! Compliance auditing: a tamper-evident audit trail in Keystone. Each
//! session's entries form a hash chain (`hash = sha256(prev_hash || seq ||
//! ts || actor || action || details)`) — the same "hashing only, no
//! from-scratch crypto primitives" line this codebase already draws
//! elsewhere (`sha2` in `objectstore.rs`'s S3 request signing, `sha1` in
//! the WebSocket handshake). Any entry altered or removed after the fact
//! breaks the chain from that point forward, detectable by `verify_chain`
//! recomputing every hash and comparing.
//!
//! Ordering uses `mirror::next_seq()`, not `ts` — two entries recorded
//! within the same millisecond (easy to hit in tests, possible in
//! production under load) would otherwise tie, and a hash chain needs a
//! strict total order to mean anything.

use std::sync::Arc;

use anyhow::Result;
use sha2::{Digest, Sha256};

use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{cell_i64, cell_text, col, decode_cell, encode_cells, int_cell, new_id, now_ms, text_cell};

const TABLE: &str = "_mirror_audit";
const COL_ID: usize = 0;
const COL_SESSION: usize = 1;
const COL_SEQ: usize = 2;
const COL_TS: usize = 3;
const COL_ACTOR: usize = 4;
const COL_ACTION: usize = 5;
const COL_DETAILS: usize = 6;
const COL_PREV_HASH: usize = 7;
const COL_HASH: usize = 8;

/// The hash "before the first entry" in every session's chain.
/// 64 hex zeros — the same length a real `sha256` hex digest would be, so
/// the first entry's `prev_hash` isn't visually distinguishable from a
/// "real" one, even though no entry ever hashes to it.
const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub id: String,
    pub session_id: String,
    pub seq: i64,
    pub ts: i64,
    pub actor: String,
    pub action: String,
    pub details: String,
    pub prev_hash: String,
    pub hash: String,
}

fn decode_entry(value: &[u8]) -> Option<AuditEntry> {
    Some(AuditEntry {
        id: cell_text(&decode_cell(value, COL_ID))?,
        session_id: cell_text(&decode_cell(value, COL_SESSION))?,
        seq: cell_i64(&decode_cell(value, COL_SEQ))?,
        ts: cell_i64(&decode_cell(value, COL_TS))?,
        actor: cell_text(&decode_cell(value, COL_ACTOR))?,
        action: cell_text(&decode_cell(value, COL_ACTION))?,
        details: cell_text(&decode_cell(value, COL_DETAILS)).unwrap_or_default(),
        prev_hash: cell_text(&decode_cell(value, COL_PREV_HASH))?,
        hash: cell_text(&decode_cell(value, COL_HASH))?,
    })
}

fn compute_hash(prev_hash: &str, seq: i64, ts: i64, actor: &str, action: &str, details: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prev_hash.as_bytes());
    hasher.update(b"|");
    hasher.update(seq.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(ts.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(actor.as_bytes());
    hasher.update(b"|");
    hasher.update(action.as_bytes());
    hasher.update(b"|");
    hasher.update(details.as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Debug)]
pub struct AuditReport {
    pub session_id: String,
    pub entries: Vec<AuditEntry>,
    /// `true` if `verify_chain` found the whole hash chain intact — the
    /// "auto-generate a compliance audit report for any session" milestone
    /// answers this directly rather than making a human recompute hashes.
    pub tamper_evident: bool,
}

pub struct AuditLog {
    db: Arc<Database>,
}

impl AuditLog {
    pub fn new(db: Arc<Database>) -> Result<Self> {
        if db.get_table(TABLE)?.is_none() {
            db.create_table_with_constraints(
                TABLE,
                &[
                    col("id", ColumnType::Text, false, true),
                    col("session_id", ColumnType::Text, false, false),
                    col("seq", ColumnType::Int8, false, false),
                    col("ts", ColumnType::Int8, false, false),
                    col("actor", ColumnType::Text, false, false),
                    col("action", ColumnType::Text, false, false),
                    col("details", ColumnType::Text, true, false),
                    col("prev_hash", ColumnType::Text, false, false),
                    col("hash", ColumnType::Text, false, false),
                ],
                vec![],
                vec![],
            )?;
        }
        Ok(Self { db })
    }

    fn entries_for(&self, session_id: &str) -> Result<Vec<AuditEntry>> {
        let mut entries: Vec<AuditEntry> = self.db.scan(TABLE)?.into_iter()
            .filter_map(|kv| decode_entry(&kv.value))
            .filter(|e| e.session_id == session_id)
            .collect();
        entries.sort_by_key(|e| e.seq);
        Ok(entries)
    }

    /// Appends one audit entry, chained onto `session_id`'s last entry (or
    /// `GENESIS` if this is the first).
    #[tracing::instrument(skip(self, details), fields(session_id = %session_id, actor = %actor, action = %action))]
    pub fn record(&self, session_id: &str, actor: &str, action: &str, details: &str) -> Result<String> {
        let prev_hash = self.entries_for(session_id)?.last().map(|e| e.hash.clone()).unwrap_or_else(|| GENESIS.to_string());
        let seq = crate::mirror::next_seq() as i64;
        let ts = now_ms();
        let hash = compute_hash(&prev_hash, seq, ts, actor, action, details);
        let id = new_id("audit");
        let cells = vec![
            text_cell(&id),
            text_cell(session_id),
            int_cell(seq),
            int_cell(ts),
            text_cell(actor),
            text_cell(action),
            text_cell(details),
            text_cell(&prev_hash),
            text_cell(&hash),
        ];
        self.db.write(TABLE, id.as_bytes(), &encode_cells(&cells))?;
        Ok(id)
    }

    /// Recomputes every entry's hash from `GENESIS` forward and compares
    /// against what's stored — `false` if any entry was altered, reordered,
    /// or deleted (a deletion breaks the *next* surviving entry's
    /// `prev_hash` linkage, so it's detected even though the deleted entry
    /// itself is gone).
    pub fn verify_chain(&self, session_id: &str) -> Result<bool> {
        let entries = self.entries_for(session_id)?;
        let mut expected_prev = GENESIS.to_string();
        for entry in &entries {
            if entry.prev_hash != expected_prev {
                return Ok(false);
            }
            let recomputed = compute_hash(&entry.prev_hash, entry.seq, entry.ts, &entry.actor, &entry.action, &entry.details);
            if recomputed != entry.hash {
                return Ok(false);
            }
            expected_prev = entry.hash.clone();
        }
        Ok(true)
    }

    pub fn generate_report(&self, session_id: &str) -> Result<AuditReport> {
        let entries = self.entries_for(session_id)?;
        let tamper_evident = self.verify_chain(session_id)?;
        Ok(AuditReport { session_id: session_id.to_string(), entries, tamper_evident })
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
    fn chain_is_verified_intact_for_untouched_entries() {
        let (db, _b, _l) = test_db();
        let log = AuditLog::new(db).unwrap();
        log.record("sess1", "agent1", "tool_call", "called get_weather").unwrap();
        log.record("sess1", "agent1", "policy_check", "passed PII filter").unwrap();
        log.record("sess1", "system", "cutover", "workflow completed").unwrap();
        assert!(log.verify_chain("sess1").unwrap());
        let report = log.generate_report("sess1").unwrap();
        assert!(report.tamper_evident);
        assert_eq!(report.entries.len(), 3);
    }

    #[test]
    fn tampering_with_a_stored_entry_breaks_the_chain() {
        let (db, _b, _l) = test_db();
        let log = AuditLog::new(db.clone()).unwrap();
        log.record("sess1", "agent1", "tool_call", "called get_weather").unwrap();
        let id = log.record("sess1", "agent1", "policy_check", "passed PII filter").unwrap();

        // Directly rewrite one entry's `details` in storage, bypassing
        // `AuditLog::record` entirely — simulates someone editing the
        // underlying table by hand.
        let mut row = db.read(TABLE, id.as_bytes()).unwrap().unwrap();
        let mut entry = decode_entry(&row).unwrap();
        entry.details = "TAMPERED".to_string();
        let cells = vec![
            text_cell(&entry.id), text_cell(&entry.session_id), int_cell(entry.seq), int_cell(entry.ts),
            text_cell(&entry.actor), text_cell(&entry.action), text_cell(&entry.details),
            text_cell(&entry.prev_hash), text_cell(&entry.hash), // hash NOT recomputed — that's the tamper
        ];
        row = encode_cells(&cells);
        db.write(TABLE, id.as_bytes(), &row).unwrap();

        assert!(!log.verify_chain("sess1").unwrap());
        assert!(!log.generate_report("sess1").unwrap().tamper_evident);
    }

    #[test]
    fn separate_sessions_have_independent_chains() {
        let (db, _b, _l) = test_db();
        let log = AuditLog::new(db).unwrap();
        log.record("sess1", "agent1", "a", "x").unwrap();
        log.record("sess2", "agent2", "b", "y").unwrap();
        assert_eq!(log.generate_report("sess1").unwrap().entries.len(), 1);
        assert_eq!(log.generate_report("sess2").unwrap().entries.len(), 1);
    }
}
