//! End-to-end test for Mirror (Phase 17)'s milestone: "replay a failed
//! agent session end-to-end, trace root cause to the exact tool call;
//! auto-generate a compliance audit report for any session" — exercised
//! against an in-process `Database`, mirroring `synapse_tests.rs`'s role
//! for Phase 16.

use std::sync::Arc;
use std::time::Duration;

use super::execute_query;
use crate::mirror::audit::AuditLog;
use crate::mirror::metrics::MetricsStore;
use crate::mirror::replay::ReplayEngine;
use crate::mirror::trace::Tracer;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};

fn open_db(bucket: &std::path::Path, local: &std::path::Path) -> Arc<Database> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket).unwrap());
    let lease = Arc::new(LeaseManager::new(
        store.clone(),
        "db",
        "node-1".into(),
        Duration::from_secs(30),
    ));
    lease.try_acquire().unwrap();
    Arc::new(
        Database::open(
            local,
            store,
            lease.handle(),
            NodeRole::Writer,
            Default::default(),
        )
        .unwrap(),
    )
}

fn cell_text(cell: &Option<Vec<u8>>) -> String {
    String::from_utf8(cell.clone().unwrap()).unwrap()
}

#[test]
fn milestone_replay_failed_session_traces_root_cause_and_audit_report_is_tamper_evident() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let db = open_db(bucket.path(), local.path());

    let tracer = Tracer::new(db.clone());
    let audit = AuditLog::new(db.clone()).unwrap();
    let metrics = MetricsStore::new(db.clone()).unwrap();

    let agent = "researcher";
    let session = "sess-report-42";

    // A session that starts fine, then fails on its second tool call.
    tracer
        .record_decision(
            agent,
            session,
            "decided to gather sources, then email a summary",
        )
        .unwrap();
    audit
        .record(
            session,
            agent,
            "policy_check",
            "search query passed content policy",
        )
        .unwrap();
    tracer
        .record_tool_call(
            agent,
            session,
            "web_search",
            "\"quarterly earnings\"",
            "[3 results]",
        )
        .unwrap();
    metrics.record(agent, session, 120.0, 400, true).unwrap();
    audit
        .record(
            session,
            agent,
            "policy_check",
            "recipient list passed PII policy",
        )
        .unwrap();
    let fail_offset = tracer
        .record_error(
            agent,
            session,
            Some("send_email"),
            "SMTP connection refused",
        )
        .unwrap();
    metrics.record(agent, session, 50.0, 0, false).unwrap();
    audit
        .record(session, "system", "session_end", "session ended in failure")
        .unwrap();

    // --- Replay + root-cause tracing ---
    let engine = ReplayEngine::new(db.clone());
    let events = engine.replay_session(session).unwrap();
    assert_eq!(events.len(), 3, "decision, tool_call, error");

    let root_cause = engine
        .find_first_error(session)
        .unwrap()
        .expect("session has a recorded failure");
    assert_eq!(root_cause.offset, fail_offset);
    assert_eq!(root_cause.tool_name.as_deref(), Some("send_email"));
    assert_eq!(root_cause.error.as_deref(), Some("SMTP connection refused"));

    // Stepping through the session with the cursor lands on the same event.
    let mut cursor = crate::mirror::replay::SessionCursor::open(&engine, session).unwrap();
    cursor.step(); // decision -> tool_call
    cursor.step(); // tool_call -> error
    assert_eq!(cursor.current().unwrap().offset, fail_offset);

    // The same history is reachable from plain SQL.
    let result = execute_query(
        &format!(
            "SELECT kind, tool_name FROM mirror_session_events('{session}') ORDER BY evt_offset"
        ),
        db.clone(),
    )
    .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(cell_text(&result.rows[2][0]), "error");
    assert_eq!(cell_text(&result.rows[2][1]), "send_email");

    // --- Compliance audit report ---
    let report = audit.generate_report(session).unwrap();
    assert_eq!(report.entries.len(), 3);
    assert!(
        report.tamper_evident,
        "an untouched chain auto-generates as tamper-evident"
    );
    assert_eq!(report.entries[0].action, "policy_check");
    assert_eq!(report.entries.last().unwrap().action, "session_end");

    // --- Metrics: a failed session shows up in the success rate ---
    let rate = metrics
        .success_rate(agent, 0, crate::synapse::now_ms() + 60_000)
        .unwrap()
        .unwrap();
    assert!((rate - 0.5).abs() < 1e-9);

    let sql_metrics = execute_query(
        &format!(
            "SELECT success FROM mirror_agent_metrics('{agent}', 0, {}) ORDER BY ts",
            crate::synapse::now_ms() + 60_000
        ),
        db.clone(),
    )
    .unwrap();
    assert_eq!(sql_metrics.rows.len(), 2);
    assert_eq!(
        cell_text(&sql_metrics.rows[1][0]),
        "0",
        "the failed call's metric row"
    );
}
