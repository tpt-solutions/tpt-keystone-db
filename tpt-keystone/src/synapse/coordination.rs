//! Multi-agent coordination: task delegation on a per-workflow Flux
//! (Phase 11) topic — `__synapse_tasks_<workflow>`, single-partition so
//! delegation order is preserved — plus shared workflow state as rows in
//! `_synapse_shared_state`.
//!
//! "Conflict resolution" is last-write-wins: `Database::write` overwrites
//! whatever was previously stored under the same primary key (the same
//! mechanic `execute_insert`/`execute_update` already rely on for every
//! ordinary table), not a CRDT/vector-clock merge. That's a documented scope
//! cut, consistent with this codebase's "real but simply scoped" discipline
//! elsewhere (Flux's retention sweep, Chronos's synchronous eviction, ...).
//! Concurrent writers racing on the exact same `(workflow, key)` can still
//! interleave check-then-act logic built on top of `get_shared_state`/
//! `set_shared_state` — this module only guarantees the *stored* value is
//! whichever write landed last, not that a read-modify-write built on top of
//! it is atomic.

use std::sync::Arc;

use anyhow::Result;

use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{cell_text, col, decode_cell, encode_cells, now_ms, text_cell};

const STATE_TABLE: &str = "_synapse_shared_state";
const COL_ID: usize = 0;
const COL_WORKFLOW: usize = 1;
const COL_KEY: usize = 2;
const COL_VALUE: usize = 3;
const COL_UPDATED_BY: usize = 4;
const COL_UPDATED_AT: usize = 5;

fn topic_name(workflow: &str) -> String {
    format!("__synapse_tasks_{workflow}")
}

pub struct Coordinator {
    db: Arc<Database>,
}

impl Coordinator {
    pub fn new(db: Arc<Database>) -> Result<Self> {
        if db.get_table(STATE_TABLE)?.is_none() {
            db.create_table_with_constraints(
                STATE_TABLE,
                &[
                    col("id", ColumnType::Text, false, true),
                    col("workflow_id", ColumnType::Text, false, false),
                    col("key", ColumnType::Text, false, false),
                    col("value", ColumnType::Text, true, false),
                    col("updated_by", ColumnType::Text, true, false),
                    col("updated_at", ColumnType::Int8, false, false),
                ],
                vec![],
                vec![],
            )?;
        }
        Ok(Self { db })
    }

    /// Creates `workflow`'s task topic if it doesn't exist yet; a no-op
    /// otherwise (unlike `Database::create_topic`, which errors on
    /// "already exists" — callers here shouldn't have to track whether
    /// they're the first delegator for a workflow).
    fn ensure_workflow_topic(&self, workflow: &str) -> Result<String> {
        let topic = topic_name(workflow);
        if self.db.flux_num_partitions(&topic).is_none() {
            self.db.create_topic(&topic, 1, None, None)?;
        }
        Ok(topic)
    }

    /// Enqueues `task_json` (any caller-defined text, typically JSON) onto
    /// `workflow`'s task queue. Returns the Flux offset it was published at.
    pub fn delegate_task(&self, workflow: &str, task_json: &str) -> Result<u64> {
        let topic = self.ensure_workflow_topic(workflow)?;
        let (_partition, offset) =
            self.db
                .flux_publish(&topic, Some(0), None, task_json.as_bytes().to_vec())?;
        Ok(offset)
    }

    /// Claims the next unclaimed task for consumer `group` on `workflow`'s
    /// queue, without removing it from the queue — mirrors Flux's own
    /// manual-ack semantics (`flux_poll` doesn't advance the offset).
    /// Callers must call `complete_task` once they've actually finished the
    /// work, or another `claim_task` call from the same `group` will hand
    /// back the same task again (at-least-once, not exactly-once — same
    /// contract Flux's consumer groups already document).
    pub fn claim_task(&self, workflow: &str, group: &str) -> Result<Option<(u64, String)>> {
        let topic = self.ensure_workflow_topic(workflow)?;
        let records = self.db.flux_poll(&topic, 0, group, 1)?;
        Ok(records
            .into_iter()
            .next()
            .map(|r| (r.offset, String::from_utf8_lossy(&r.value).into_owned())))
    }

    /// Acknowledges a claimed task, advancing `group`'s offset past it.
    pub fn complete_task(&self, workflow: &str, group: &str, offset: u64) -> Result<()> {
        let topic = self.ensure_workflow_topic(workflow)?;
        self.db.flux_commit(&topic, 0, group, offset + 1)
    }

    /// Sets `workflow`'s shared `key` to `value`. Last-write-wins: this
    /// simply overwrites the row stored under `(workflow, key)`'s
    /// deterministic id.
    pub fn set_shared_state(
        &self,
        workflow: &str,
        key: &str,
        value: &str,
        updated_by: &str,
    ) -> Result<()> {
        let id = format!("{workflow}:{key}");
        let cells = vec![
            text_cell(&id),
            text_cell(workflow),
            text_cell(key),
            text_cell(value),
            text_cell(updated_by),
            Some(now_ms().to_string().into_bytes()),
        ];
        self.db
            .write(STATE_TABLE, id.as_bytes(), &encode_cells(&cells))
    }

    pub fn get_shared_state(&self, workflow: &str, key: &str) -> Result<Option<String>> {
        let id = format!("{workflow}:{key}");
        Ok(self
            .db
            .read(STATE_TABLE, id.as_bytes())?
            .and_then(|v| cell_text(&decode_cell(&v, COL_VALUE))))
    }

    /// `(key, value)` for every shared-state entry belonging to `workflow`.
    pub fn list_shared_state(&self, workflow: &str) -> Result<Vec<(String, String)>> {
        Ok(self
            .db
            .scan(STATE_TABLE)?
            .into_iter()
            .filter_map(|kv| {
                let wf = cell_text(&decode_cell(&kv.value, COL_WORKFLOW))?;
                if wf != workflow {
                    return None;
                }
                let key = cell_text(&decode_cell(&kv.value, COL_KEY))?;
                let value = cell_text(&decode_cell(&kv.value, COL_VALUE)).unwrap_or_default();
                Some((key, value))
            })
            .collect())
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
    fn delegate_claim_and_complete_a_task() {
        let (db, _b, _l) = test_db();
        let coord = Coordinator::new(db).unwrap();
        coord
            .delegate_task("wf1", "{\"job\":\"summarize\"}")
            .unwrap();

        let (offset, payload) = coord.claim_task("wf1", "workers").unwrap().unwrap();
        assert_eq!(payload, "{\"job\":\"summarize\"}");

        // Not yet completed: the same group re-claims the same task.
        let (offset2, payload2) = coord.claim_task("wf1", "workers").unwrap().unwrap();
        assert_eq!((offset2, payload2), (offset, payload.clone()));

        coord.complete_task("wf1", "workers", offset).unwrap();
        assert!(coord.claim_task("wf1", "workers").unwrap().is_none());
    }

    #[test]
    fn independent_consumer_groups_each_see_the_full_queue() {
        let (db, _b, _l) = test_db();
        let coord = Coordinator::new(db).unwrap();
        coord.delegate_task("wf1", "task-a").unwrap();
        let (offset, _) = coord.claim_task("wf1", "group-a").unwrap().unwrap();
        coord.complete_task("wf1", "group-a", offset).unwrap();

        // group-b never claimed anything yet, so it still sees task-a.
        assert!(coord.claim_task("wf1", "group-b").unwrap().is_some());
    }

    #[test]
    fn shared_state_last_write_wins() {
        let (db, _b, _l) = test_db();
        let coord = Coordinator::new(db).unwrap();
        coord
            .set_shared_state("wf1", "status", "planning", "agent-a")
            .unwrap();
        coord
            .set_shared_state("wf1", "status", "executing", "agent-b")
            .unwrap();
        assert_eq!(
            coord.get_shared_state("wf1", "status").unwrap(),
            Some("executing".to_string())
        );
        assert_eq!(
            coord.list_shared_state("wf1").unwrap().len(),
            1,
            "overwrite, not a second row"
        );
    }
}
