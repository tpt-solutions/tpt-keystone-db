//! End-to-end tests for Flux (Phase 11): `CREATE TOPIC`, native CDC
//! (`__cdc_<table>` auto-published on every insert/update/delete), and the
//! `flux_time_travel`/`flux_window_tumbling`/`flux_window_session` table
//! functions — mirroring `plexus_tests.rs`/`chronos_tests.rs`'s "index/topic
//! answers a real query end-to-end" pattern.

use std::sync::Arc;
use std::time::Duration;

use super::execute_query;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};

fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let lease = Arc::new(LeaseManager::new(store.clone(), "db", "node-1".into(), Duration::from_secs(30)));
    lease.try_acquire().unwrap();
    let db = Arc::new(Database::open(local.path(), store, lease.handle(), NodeRole::Writer, Default::default()).unwrap());
    (db, bucket, local)
}

fn cell_text(cell: &Option<Vec<u8>>) -> String {
    String::from_utf8(cell.clone().unwrap()).unwrap()
}

#[test]
fn create_topic_and_list() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TOPIC orders WITH (partitions = '3')", db.clone()).unwrap();
    let topics = db.list_topics();
    assert!(topics.contains(&("orders".to_string(), 3)));
}

#[test]
fn create_topic_if_not_exists_is_idempotent() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TOPIC orders", db.clone()).unwrap();
    // Without IF NOT EXISTS this should fail the second time.
    assert!(execute_query("CREATE TOPIC orders", db.clone()).is_err());
    // With it, it's a no-op.
    execute_query("CREATE TOPIC IF NOT EXISTS orders", db.clone()).unwrap();
}

#[test]
fn publish_poll_commit_via_database_api() {
    let (db, _b, _l) = test_db();
    db.create_topic("events", 1, None, None).unwrap();
    db.flux_publish("events", None, None, b"hello".to_vec()).unwrap();
    db.flux_publish("events", None, None, b"world".to_vec()).unwrap();

    let polled = db.flux_poll("events", 0, "consumer-a", 10).unwrap();
    assert_eq!(polled.len(), 2);
    assert_eq!(polled[0].value, b"hello");

    db.flux_commit("events", 0, "consumer-a", 1).unwrap();
    let after_commit = db.flux_poll("events", 0, "consumer-a", 10).unwrap();
    assert_eq!(after_commit.len(), 1);
    assert_eq!(after_commit[0].value, b"world");
}

#[test]
fn insert_produces_cdc_event_on_implicit_topic() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE items (id INT4, name TEXT)", db.clone()).unwrap();
    execute_query("INSERT INTO items VALUES (1, 'widget')", db.clone()).unwrap();

    let records = db.flux_all("__cdc_items", 0).expect("CDC topic should exist after insert");
    assert_eq!(records.len(), 1);
    let event: serde_json::Value = serde_json::from_slice(&records[0].value).unwrap();
    assert_eq!(event["op"], "insert");
    assert_eq!(event["table"], "items");
    assert_eq!(event["before"], serde_json::Value::Null);
    assert_eq!(event["after"]["name"], "widget");
}

#[test]
fn update_and_delete_produce_matching_cdc_events() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE items (id INT4, name TEXT)", db.clone()).unwrap();
    execute_query("INSERT INTO items VALUES (1, 'widget')", db.clone()).unwrap();
    execute_query("UPDATE items SET name = 'gadget' WHERE id = 1", db.clone()).unwrap();
    execute_query("DELETE FROM items WHERE id = 1", db.clone()).unwrap();

    let records = db.flux_all("__cdc_items", 0).unwrap();
    assert_eq!(records.len(), 3);

    let update_event: serde_json::Value = serde_json::from_slice(&records[1].value).unwrap();
    assert_eq!(update_event["op"], "update");
    assert_eq!(update_event["before"]["name"], "widget");
    assert_eq!(update_event["after"]["name"], "gadget");

    let delete_event: serde_json::Value = serde_json::from_slice(&records[2].value).unwrap();
    assert_eq!(delete_event["op"], "delete");
    assert_eq!(delete_event["before"]["name"], "gadget");
    assert_eq!(delete_event["after"], serde_json::Value::Null);
}

#[test]
fn cdc_events_are_readable_via_flux_poll() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE items (id INT4, name TEXT)", db.clone()).unwrap();
    execute_query("INSERT INTO items VALUES (1, 'widget')", db.clone()).unwrap();
    execute_query("INSERT INTO items VALUES (2, 'sprocket')", db.clone()).unwrap();

    let polled = db.flux_poll("__cdc_items", 0, "downstream", 10).unwrap();
    assert_eq!(polled.len(), 2);
    let e0: serde_json::Value = serde_json::from_slice(&polled[0].value).unwrap();
    assert_eq!(e0["after"]["name"], "widget");
}

#[test]
fn time_travel_reconstructs_state_at_a_past_timestamp() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE items (id INT4, name TEXT)", db.clone()).unwrap();
    execute_query("INSERT INTO items VALUES (1, 'widget')", db.clone()).unwrap();

    // Grab the actual event timestamp so the test isn't racing wall-clock ms.
    let records = db.flux_all("__cdc_items", 0).unwrap();
    let insert_ts: i64 = serde_json::from_slice::<serde_json::Value>(&records[0].value).unwrap()["ts"].as_i64().unwrap();

    execute_query("UPDATE items SET name = 'gadget' WHERE id = 1", db.clone()).unwrap();
    execute_query("DELETE FROM items WHERE id = 1", db.clone()).unwrap();

    // As of the insert's own timestamp, the row should still show "widget".
    let result = execute_query(&format!("SELECT row_key, data FROM flux_time_travel('items', {insert_ts})"), db.clone()).unwrap();
    assert_eq!(result.rows.len(), 1);
    let data: serde_json::Value = serde_json::from_str(&cell_text(&result.rows[0][1])).unwrap();
    assert_eq!(data["name"], "widget");

    // As of "now" (after the delete), the row should be gone.
    let now_result = execute_query(
        &format!("SELECT row_key FROM flux_time_travel('items', {})", insert_ts + 10_000),
        db.clone(),
    ).unwrap();
    assert_eq!(now_result.rows.len(), 0);
}

#[test]
fn tumbling_window_buckets_records_by_fixed_interval() {
    let (db, _b, _l) = test_db();
    db.create_topic("clicks", 1, None, None).unwrap();
    // Publish straight through the Database API isn't timestamp-controllable
    // (records stamp `now_ms()`), so this test only asserts the *shape*
    // (windows partition all published records, total count matches) rather
    // than exact bucket boundaries relative to wall-clock time.
    for _ in 0..5 {
        db.flux_publish("clicks", None, None, b"click".to_vec()).unwrap();
    }
    let result = execute_query(
        "SELECT window_start, window_end, count FROM flux_window_tumbling('clicks', 3600000)",
        db.clone(),
    ).unwrap();
    let total: i64 = result.rows.iter().map(|r| cell_text(&r[2]).parse::<i64>().unwrap()).sum();
    assert_eq!(total, 5);
}

#[test]
fn session_window_splits_on_large_gaps() {
    let (db, _b, _l) = test_db();
    db.create_topic("sessions", 1, None, None).unwrap();
    db.flux_publish("sessions", None, None, b"a".to_vec()).unwrap();
    db.flux_publish("sessions", None, None, b"b".to_vec()).unwrap();

    // With an effectively-infinite gap, both records (published moments
    // apart) fall into one session.
    let result = execute_query(
        "SELECT count FROM flux_window_session('sessions', 3600000)",
        db.clone(),
    ).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(cell_text(&result.rows[0][0]), "2");

    // With a 1ms gap tolerance, two records published moments apart split
    // into separate sessions or stay together depending on timing — but the
    // count across all sessions must still sum to 2 either way.
    let result2 = execute_query(
        "SELECT count FROM flux_window_session('sessions', 1)",
        db.clone(),
    ).unwrap();
    let total: i64 = result2.rows.iter().map(|r| cell_text(&r[0]).parse::<i64>().unwrap()).sum();
    assert_eq!(total, 2);
}
