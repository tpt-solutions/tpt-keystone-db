//! Agent action tracing: every decision, tool call, and outcome is written
//! as an immutable, ordered JSON event to a per-session Flux topic
//! (`__mirror_trace_<session_id>`, single-partition so the Flux offset
//! itself is the event's position in the session — no separate `seq` field
//! needed, unlike `mirror::audit`'s cross-table hash chain). Mirrors
//! `Database::flux_publish_cdc`'s own "auto-create the topic on first use"
//! shape.

use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::storage::database::Database;

fn topic_name(session_id: &str) -> String {
    format!("__mirror_trace_{session_id}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    /// Filled from the Flux record itself on decode, not the JSON payload
    /// (see `decode_event`) — defaults are only relevant to `serde` when
    /// deserializing the payload before that overwrite happens.
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub ts: i64,
    pub agent_id: String,
    pub session_id: String,
    /// `"decision"` | `"tool_call"` | `"outcome"` | `"error"` — an open
    /// vocabulary (not a Rust enum) so a caller can record whatever kinds
    /// of step their own agent loop has, mirroring how `synapse::actor`
    /// leaves an agent's actual behavior caller-defined.
    pub kind: String,
    pub detail: String,
    pub tool_name: Option<String>,
    pub input: Option<String>,
    pub output: Option<String>,
    pub error: Option<String>,
}

pub struct Tracer {
    db: Arc<Database>,
}

impl Tracer {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn ensure_topic(&self, session_id: &str) -> Result<String> {
        let topic = topic_name(session_id);
        if self.db.flux_num_partitions(&topic).is_none() {
            self.db.create_topic(&topic, 1, None, None)?;
        }
        Ok(topic)
    }

    /// Appends one trace event and returns its Flux offset (the event's
    /// permanent position within the session, used by `replay`/`audit` to
    /// point at "the exact tool call").
    #[tracing::instrument(skip(self, detail, input, output, error), fields(agent_id = %agent_id, session_id = %session_id, kind = %kind))]
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        agent_id: &str,
        session_id: &str,
        kind: &str,
        detail: &str,
        tool_name: Option<&str>,
        input: Option<&str>,
        output: Option<&str>,
        error: Option<&str>,
    ) -> Result<u64> {
        let topic = self.ensure_topic(session_id)?;
        let ts = crate::storage::flux::now_ms();
        let event = json!({
            "ts": ts,
            "agent_id": agent_id,
            "session_id": session_id,
            "kind": kind,
            "detail": detail,
            "tool_name": tool_name,
            "input": input,
            "output": output,
            "error": error,
        });
        let (_partition, offset) = self.db.flux_publish(&topic, Some(0), None, serde_json::to_vec(&event)?)?;
        Ok(offset)
    }

    pub fn record_decision(&self, agent_id: &str, session_id: &str, detail: &str) -> Result<u64> {
        self.record(agent_id, session_id, "decision", detail, None, None, None, None)
    }

    pub fn record_tool_call(&self, agent_id: &str, session_id: &str, tool_name: &str, input: &str, output: &str) -> Result<u64> {
        self.record(agent_id, session_id, "tool_call", &format!("called {tool_name}"), Some(tool_name), Some(input), Some(output), None)
    }

    pub fn record_error(&self, agent_id: &str, session_id: &str, tool_name: Option<&str>, error: &str) -> Result<u64> {
        self.record(agent_id, session_id, "error", error, tool_name, None, None, Some(error))
    }

    pub fn record_outcome(&self, agent_id: &str, session_id: &str, detail: &str) -> Result<u64> {
        self.record(agent_id, session_id, "outcome", detail, None, None, None, None)
    }
}

/// Decodes one raw Flux record into a `TraceEvent`, filling `offset`/`ts`
/// from the record itself (not the JSON payload) so a corrupted/hand-edited
/// payload can't spoof its own position — used by `replay.rs`.
pub(crate) fn decode_event(record: &crate::storage::flux::FluxRecord) -> Option<TraceEvent> {
    let mut event: TraceEvent = serde_json::from_slice(&record.value).ok()?;
    event.offset = record.offset;
    event.ts = record.timestamp_ms;
    Some(event)
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
    fn records_are_ordered_by_offset() {
        let (db, _b, _l) = test_db();
        let tracer = Tracer::new(db.clone());
        let o1 = tracer.record_decision("agent1", "sess1", "decided to search").unwrap();
        let o2 = tracer.record_tool_call("agent1", "sess1", "web_search", "cats", "[results]").unwrap();
        let o3 = tracer.record_outcome("agent1", "sess1", "done").unwrap();
        assert_eq!((o1, o2, o3), (0, 1, 2));
    }
}
