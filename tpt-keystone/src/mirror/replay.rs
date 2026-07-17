//! Session replay: reconstructs a past agent session's full event sequence
//! from its `mirror::trace` Flux topic, bypassing consumer-group offset
//! tracking entirely (`Database::flux_all`, the same "replay the whole log"
//! primitive `flux_time_travel`/the windowing table functions already use)
//! since a replay isn't a live consumer, it's a read of history.
//!
//! `SessionCursor` is the "debug REPL" *engine* — step forward/back through
//! a session's events one at a time. A literal interactive terminal
//! front-end wasn't built here (that's `tpt-keystone-cli`'s job); this module is the
//! part a REPL, a dashboard's "replay controls", or a test assertion would
//! all sit on top of.

use std::sync::Arc;

use anyhow::Result;

use crate::mirror::trace::{decode_event, TraceEvent};
use crate::storage::database::Database;

fn topic_name(session_id: &str) -> String {
    format!("__mirror_trace_{session_id}")
}

pub struct ReplayEngine {
    db: Arc<Database>,
}

impl ReplayEngine {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Every event ever recorded for `session_id`, oldest first. Empty
    /// (not an error) if the session never recorded anything.
    pub fn replay_session(&self, session_id: &str) -> Result<Vec<TraceEvent>> {
        let topic = topic_name(session_id);
        let Some(records) = self.db.flux_all(&topic, 0) else {
            return Ok(Vec::new());
        };
        let mut events: Vec<TraceEvent> = records.iter().filter_map(decode_event).collect();
        events.sort_by_key(|e| e.offset);
        Ok(events)
    }

    /// Same as `replay_session`, truncated to events at or before `offset`
    /// — "replay up to the point things went wrong."
    pub fn replay_up_to(&self, session_id: &str, offset: u64) -> Result<Vec<TraceEvent>> {
        Ok(self
            .replay_session(session_id)?
            .into_iter()
            .filter(|e| e.offset <= offset)
            .collect())
    }

    /// The first event that represents a failure — either `kind == "error"`
    /// or any event carrying a non-null `error` field — the "trace root
    /// cause to the exact tool call" milestone helper. `None` if the
    /// session has no recorded failure.
    pub fn find_first_error(&self, session_id: &str) -> Result<Option<TraceEvent>> {
        Ok(self
            .replay_session(session_id)?
            .into_iter()
            .find(|e| e.kind == "error" || e.error.is_some()))
    }
}

/// Steps through one session's events one at a time, forward or backward.
/// Loads the whole session up front (`ReplayEngine::replay_session`) — fine
/// at the single-node, non-benchmarked scale every other local index in
/// this codebase already targets.
pub struct SessionCursor {
    events: Vec<TraceEvent>,
    pos: usize,
}

impl SessionCursor {
    pub fn open(engine: &ReplayEngine, session_id: &str) -> Result<Self> {
        Ok(Self {
            events: engine.replay_session(session_id)?,
            pos: 0,
        })
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn current(&self) -> Option<&TraceEvent> {
        self.events.get(self.pos)
    }

    pub fn step(&mut self) -> Option<&TraceEvent> {
        if self.pos + 1 < self.events.len() {
            self.pos += 1;
        }
        self.current()
    }

    pub fn back(&mut self) -> Option<&TraceEvent> {
        self.pos = self.pos.saturating_sub(1);
        self.current()
    }

    /// Jumps directly to the event with this Flux offset, if present.
    pub fn seek(&mut self, offset: u64) -> Option<&TraceEvent> {
        let idx = self.events.iter().position(|e| e.offset == offset)?;
        self.pos = idx;
        self.current()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mirror::trace::Tracer;
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

    fn seed_failed_session(db: &Arc<Database>) {
        let tracer = Tracer::new(db.clone());
        tracer
            .record_decision("agent1", "sess1", "decided to look up the forecast")
            .unwrap();
        tracer
            .record_tool_call(
                "agent1",
                "sess1",
                "get_weather",
                "\"Paris\"",
                "{\"temp\":18}",
            )
            .unwrap();
        tracer
            .record_error(
                "agent1",
                "sess1",
                Some("send_email"),
                "SMTP connection refused",
            )
            .unwrap();
    }

    #[test]
    fn replay_session_returns_events_in_order() {
        let (db, _b, _l) = test_db();
        seed_failed_session(&db);
        let engine = ReplayEngine::new(db);
        let events = engine.replay_session("sess1").unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind, "decision");
        assert_eq!(events[1].kind, "tool_call");
        assert_eq!(events[2].kind, "error");
    }

    #[test]
    fn find_first_error_identifies_the_exact_failing_tool_call() {
        let (db, _b, _l) = test_db();
        seed_failed_session(&db);
        let engine = ReplayEngine::new(db);
        let err = engine.find_first_error("sess1").unwrap().unwrap();
        assert_eq!(err.tool_name.as_deref(), Some("send_email"));
        assert_eq!(err.error.as_deref(), Some("SMTP connection refused"));
    }

    #[test]
    fn cursor_steps_forward_and_back() {
        let (db, _b, _l) = test_db();
        seed_failed_session(&db);
        let engine = ReplayEngine::new(db);
        let mut cursor = SessionCursor::open(&engine, "sess1").unwrap();
        assert_eq!(cursor.len(), 3);
        assert_eq!(cursor.current().unwrap().kind, "decision");
        assert_eq!(cursor.step().unwrap().kind, "tool_call");
        assert_eq!(cursor.step().unwrap().kind, "error");
        assert_eq!(cursor.step().unwrap().kind, "error", "stays put at the end");
        assert_eq!(cursor.back().unwrap().kind, "tool_call");
    }

    #[test]
    fn cursor_seek_jumps_to_a_specific_offset() {
        let (db, _b, _l) = test_db();
        seed_failed_session(&db);
        let engine = ReplayEngine::new(db);
        let mut cursor = SessionCursor::open(&engine, "sess1").unwrap();
        assert_eq!(cursor.seek(2).unwrap().kind, "error");
    }
}
