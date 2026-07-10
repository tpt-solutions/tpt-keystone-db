//! Agent memory abstraction: the four tiers from the Phase 16 checklist,
//! all backed by one `_synapse_memory` table (distinguished by a `tier`
//! column) rather than four separate schemas, since the Chronos/Prism
//! indexes below are already column-scoped, not row-filtered — a shared
//! table plus per-tier query helpers gets the same effect for free.
//!
//! | tier      | checklist wording           | how it's answered                          |
//! |-----------|------------------------------|---------------------------------------------|
//! | short     | "Keystone in-session"        | `tier='short'`, `expires_at` set, GC'd      |
//! | long      | "Keystone persistent"        | `tier='long'`, no expiry, no GC             |
//! | episodic  | "Chronos time-indexed"       | `CREATE INDEX ... USING TIME(ts)`           |
//! | semantic  | "Prism vector search"        | `CREATE INDEX ... USING VECTOR(embedding)`  |
//!
//! Chronos's time index requires a numeric value column paired with the
//! timestamp (it's built for metric rollups); episodic memory has no
//! intrinsic metric, so `seq` (a monotonic insert counter) fills that slot
//! — a documented placeholder, not a meaningful aggregate.
//!
//! Semantic dedup ("semantic deduplicated" GC policy) happens at write time,
//! not as a background sweep — before inserting, `remember_semantic` checks
//! whether the same agent already has a near-identical embedding (cosine
//! distance below `DEDUP_THRESHOLD`) and returns the existing entry's id
//! instead of inserting a duplicate. Same "synchronous, no background
//! scheduler" discipline `TimeIndex::apply_retention`/`Partition::apply_retention`
//! already use elsewhere in this codebase.

use std::sync::Arc;

use anyhow::Result;

use crate::storage::database::Database;
use crate::storage::ts_index::TimeBucketPolicy;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{cell_i64, cell_text, col, decode_cell, encode_cells, int_cell, new_id, now_ms, text_cell};
use crate::vector::hnsw::{HnswConfig, Metric};
use crate::vector::vector::Vector;

const TABLE: &str = "_synapse_memory";
/// Cosine *distance* (1 - similarity) below which a new semantic memory is
/// considered a duplicate of an existing one for the same agent.
const DEDUP_THRESHOLD: f32 = 0.02;

// Column indices, fixed by `ensure_schema`'s declaration order.
const COL_ID: usize = 0;
const COL_AGENT: usize = 1;
const COL_TIER: usize = 2;
const COL_KEY: usize = 3;
const COL_CONTENT: usize = 4;
const COL_EMBEDDING: usize = 5;
const COL_SEQ: usize = 6;
const COL_TS: usize = 7;
const COL_EXPIRES: usize = 8;

#[derive(Debug, Clone)]
pub struct MemoryEntry {
    pub id: String,
    pub agent_id: String,
    pub tier: String,
    pub key: Option<String>,
    pub content: String,
    pub ts: i64,
}

fn decode_entry(value: &[u8]) -> Option<MemoryEntry> {
    Some(MemoryEntry {
        id: cell_text(&decode_cell(value, COL_ID))?,
        agent_id: cell_text(&decode_cell(value, COL_AGENT))?,
        tier: cell_text(&decode_cell(value, COL_TIER))?,
        key: cell_text(&decode_cell(value, COL_KEY)),
        content: cell_text(&decode_cell(value, COL_CONTENT))?,
        ts: cell_i64(&decode_cell(value, COL_TS))?,
    })
}

pub struct MemoryStore {
    db: Arc<Database>,
}

impl MemoryStore {
    pub fn new(db: Arc<Database>) -> Result<Self> {
        Self::ensure_schema(&db)?;
        Ok(Self { db })
    }

    fn ensure_schema(db: &Arc<Database>) -> Result<()> {
        if db.get_table(TABLE)?.is_none() {
            db.create_table_with_constraints(
                TABLE,
                &[
                    col("id", ColumnType::Text, false, true),
                    col("agent_id", ColumnType::Text, false, false),
                    col("tier", ColumnType::Text, false, false),
                    col("key", ColumnType::Text, true, false),
                    col("content", ColumnType::Text, false, false),
                    col("embedding", ColumnType::Vector, true, false),
                    col("seq", ColumnType::Int8, false, false),
                    col("ts", ColumnType::Int8, false, false),
                    col("expires_at", ColumnType::Int8, true, false),
                ],
                vec![],
                vec![],
            )?;
        }
        if !db.indexed_column_time(TABLE, "ts") {
            db.create_time_index(TABLE, "ts", "seq", TimeBucketPolicy { granularity_ms: 3_600_000, retention_ms: None })?;
        }
        if !db.indexed_column_vector(TABLE, "embedding") {
            db.create_vector_index(TABLE, "embedding", Metric::Cosine, HnswConfig::default())?;
        }
        Ok(())
    }

    fn insert(&self, agent_id: &str, tier: &str, key: Option<&str>, content: &str, embedding: Option<&[f32]>, expires_at: Option<i64>) -> Result<String> {
        let id = new_id("mem");
        let seq = now_ms(); // monotonic enough for a single-node index; see module docs
        let cells = vec![
            text_cell(&id),
            text_cell(agent_id),
            text_cell(tier),
            key.map(text_cell).unwrap_or(None),
            text_cell(content),
            embedding.map(|v| Vector(v.to_vec()).to_text()).map(text_cell).unwrap_or(None),
            int_cell(seq),
            int_cell(now_ms()),
            expires_at.map(int_cell).unwrap_or(None),
        ];
        self.db.write(TABLE, id.as_bytes(), &encode_cells(&cells))?;
        Ok(id)
    }

    /// Short-term ("Keystone in-session") memory: durable in Keystone (not
    /// pure process memory, unlike `actor::AgentState::short_term`), but
    /// tagged with a TTL and swept by `gc`.
    pub fn remember_short(&self, agent_id: &str, key: Option<&str>, content: &str, ttl_ms: i64) -> Result<String> {
        self.insert(agent_id, "short", key, content, None, Some(now_ms() + ttl_ms))
    }

    /// Long-term ("Keystone persistent") memory: no expiry, never GC'd.
    pub fn remember_long(&self, agent_id: &str, key: Option<&str>, content: &str) -> Result<String> {
        self.insert(agent_id, "long", key, content, None, None)
    }

    /// Episodic ("Chronos time-indexed") memory: one row per event, `ts`
    /// answers a range recall via the Chronos index rather than a table scan.
    pub fn remember_episodic(&self, agent_id: &str, content: &str) -> Result<String> {
        self.insert(agent_id, "episodic", None, content, None, None)
    }

    /// Semantic ("Prism vector search") memory. Deduplicates against the
    /// same agent's existing semantic memories: if an existing entry's
    /// embedding is within `DEDUP_THRESHOLD` cosine distance, its id is
    /// returned instead of inserting a near-duplicate.
    pub fn remember_semantic(&self, agent_id: &str, content: &str, embedding: &[f32]) -> Result<String> {
        if let Some(existing) = self.find_near_duplicate(agent_id, embedding)? {
            return Ok(existing);
        }
        self.insert(agent_id, "semantic", None, content, Some(embedding), None)
    }

    fn find_near_duplicate(&self, agent_id: &str, embedding: &[f32]) -> Result<Option<String>> {
        let Some(hits) = self.db.vector_knn_query(TABLE, "embedding", embedding, 8, None) else { return Ok(None) };
        for (kv, dist) in hits {
            if dist > DEDUP_THRESHOLD {
                continue;
            }
            if let Some(entry) = decode_entry(&kv.value) {
                if entry.agent_id == agent_id && entry.tier == "semantic" {
                    return Ok(Some(entry.id));
                }
            }
        }
        Ok(None)
    }

    pub fn recall_long(&self, agent_id: &str, key: &str) -> Result<Option<String>> {
        let mut matches: Vec<MemoryEntry> = self.db.scan(TABLE)?.into_iter()
            .filter_map(|kv| decode_entry(&kv.value))
            .filter(|e| e.tier == "long" && e.agent_id == agent_id && e.key.as_deref() == Some(key))
            .collect();
        matches.sort_by_key(|e| e.ts);
        Ok(matches.pop().map(|e| e.content))
    }

    /// Rows with `tier='episodic'`, `agent_id`, and `t0 <= ts <= t1`,
    /// answered via the Chronos time-index range scan rather than a table
    /// scan, oldest first.
    pub fn recall_episodic(&self, agent_id: &str, t0: i64, t1: i64) -> Result<Vec<MemoryEntry>> {
        let Some(rows) = self.db.time_range_query(TABLE, "ts", t0, t1) else { return Ok(Vec::new()) };
        let mut out: Vec<MemoryEntry> = rows.into_iter()
            .filter_map(|kv| decode_entry(&kv.value))
            .filter(|e| e.tier == "episodic" && e.agent_id == agent_id)
            .collect();
        out.sort_by_key(|e| e.ts);
        Ok(out)
    }

    /// Approximate k-NN recall over `agent_id`'s semantic memories,
    /// nearest-first. Over-fetches from the shared (not per-agent) HNSW
    /// index and post-filters by `agent_id`/tier, then truncates to `k` —
    /// documented scope cut: at large multi-tenant scale this would need a
    /// per-agent-partitioned index to stay both correct and cheap; fine for
    /// the single-node scale everything else in this codebase targets.
    pub fn recall_semantic(&self, agent_id: &str, query: &[f32], k: usize) -> Result<Vec<(MemoryEntry, f32)>> {
        let Some(hits) = self.db.vector_knn_query(TABLE, "embedding", query, k.saturating_mul(4).max(k), None) else { return Ok(Vec::new()) };
        let mut out: Vec<(MemoryEntry, f32)> = hits.into_iter()
            .filter_map(|(kv, dist)| decode_entry(&kv.value).map(|e| (e, dist)))
            .filter(|(e, _)| e.tier == "semantic" && e.agent_id == agent_id)
            .collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        out.truncate(k);
        Ok(out)
    }

    /// Deletes expired `tier='short'` rows (`expires_at < now`). Synchronous,
    /// caller-invoked — no background sweep, same discipline as every other
    /// retention/GC path in this codebase. Long-term rows are never GC'd;
    /// semantic rows are deduplicated at write time instead of swept.
    pub fn gc(&self) -> Result<usize> {
        let now = now_ms();
        let mut deleted = 0usize;
        for kv in self.db.scan(TABLE)? {
            let tier = cell_text(&decode_cell(&kv.value, COL_TIER));
            let expires = cell_i64(&decode_cell(&kv.value, COL_EXPIRES));
            if tier.as_deref() == Some("short") {
                if let Some(exp) = expires {
                    if exp < now {
                        self.db.delete(TABLE, &kv.key)?;
                        deleted += 1;
                    }
                }
            }
        }
        Ok(deleted)
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
    fn long_term_recall_returns_latest_value_for_key() {
        let (db, _b, _l) = test_db();
        let mem = MemoryStore::new(db).unwrap();
        mem.remember_long("agent1", Some("name"), "Alice").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        mem.remember_long("agent1", Some("name"), "Alice Smith").unwrap();
        assert_eq!(mem.recall_long("agent1", "name").unwrap(), Some("Alice Smith".to_string()));
    }

    #[test]
    fn short_term_expires_and_is_gcd() {
        let (db, _b, _l) = test_db();
        let mem = MemoryStore::new(db).unwrap();
        mem.remember_short("agent1", None, "ephemeral", -1).unwrap(); // already expired
        mem.remember_short("agent1", None, "still fresh", 60_000).unwrap();
        let deleted = mem.gc().unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn episodic_recall_answers_time_range_in_order() {
        let (db, _b, _l) = test_db();
        let mem = MemoryStore::new(db).unwrap();
        mem.remember_episodic("agent1", "event-1").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        mem.remember_episodic("agent1", "event-2").unwrap();
        let events = mem.recall_episodic("agent1", 0, now_ms() + 60_000).unwrap();
        assert_eq!(events.iter().map(|e| e.content.clone()).collect::<Vec<_>>(), vec!["event-1", "event-2"]);
    }

    #[test]
    fn semantic_recall_ranks_nearest_first_and_dedups_near_identical() {
        let (db, _b, _l) = test_db();
        let mem = MemoryStore::new(db).unwrap();
        let id1 = mem.remember_semantic("agent1", "cats are great", &[1.0, 0.0, 0.0]).unwrap();
        let id2 = mem.remember_semantic("agent1", "cats are great (paraphrase)", &[1.0, 0.0001, 0.0]).unwrap();
        assert_eq!(id1, id2, "near-identical embedding should dedup to the same entry");
        mem.remember_semantic("agent1", "dogs are great", &[0.0, 1.0, 0.0]).unwrap();

        let hits = mem.recall_semantic("agent1", &[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].0.content, "cats are great");
    }

    #[test]
    fn semantic_recall_is_scoped_to_the_requesting_agent() {
        let (db, _b, _l) = test_db();
        let mem = MemoryStore::new(db).unwrap();
        mem.remember_semantic("agent1", "agent1 memory", &[1.0, 0.0]).unwrap();
        mem.remember_semantic("agent2", "agent2 memory", &[1.0, 0.0]).unwrap();
        let hits = mem.recall_semantic("agent1", &[1.0, 0.0], 5).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0.agent_id, "agent1");
    }
}
