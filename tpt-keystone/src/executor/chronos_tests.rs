//! End-to-end tests for Chronos (Phase 8): `time_bucket()`/`moving_average()`
//! /`interpolate()` SQL functions and `CREATE INDEX ... USING TIME` driving
//! a time-range index scan through the same `resolve_primary_table` path
//! Phase 2's B-Tree index lookup and Phase 6's spatial index lookup use.

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
fn time_bucket_rounds_down_to_interval_boundary() {
    let (db, _b, _l) = test_db();
    // 1 hour = 3_600_000 ms. 5_400_000 ms falls in the [3_600_000, 7_200_000) bucket.
    let result = execute_query("SELECT time_bucket('1 hour', 5400000)", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "3600000");
}

#[test]
fn moving_average_over_known_series() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE metrics (ts INT8, value FLOAT8)", db.clone()).unwrap();
    for (ts, value) in [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)] {
        execute_query(&format!("INSERT INTO metrics VALUES ({ts}, {value})"), db.clone()).unwrap();
    }
    let result = execute_query(
        "SELECT moving_average(value, 2) OVER (ORDER BY ts) FROM metrics ORDER BY ts",
        db.clone(),
    ).unwrap();
    let avgs: Vec<f64> = result.rows.iter().map(|r| cell_text(&r[0]).parse().unwrap()).collect();
    // row1: avg(10) = 10; row2: avg(10,20) = 15; row3: avg(20,30) = 25; row4: avg(30,40) = 35
    assert_eq!(avgs, vec![10.0, 15.0, 25.0, 35.0]);
}

#[test]
fn interpolate_fills_nulls_between_known_values() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE metrics (ts INT8, value FLOAT8)", db.clone()).unwrap();
    execute_query("INSERT INTO metrics VALUES (1, 10.0)", db.clone()).unwrap();
    execute_query("INSERT INTO metrics VALUES (2, NULL)", db.clone()).unwrap();
    execute_query("INSERT INTO metrics VALUES (3, NULL)", db.clone()).unwrap();
    execute_query("INSERT INTO metrics VALUES (4, 40.0)", db.clone()).unwrap();

    let result = execute_query(
        "SELECT interpolate(value) OVER (ORDER BY ts) FROM metrics ORDER BY ts",
        db.clone(),
    ).unwrap();
    let values: Vec<f64> = result.rows.iter().map(|r| cell_text(&r[0]).parse().unwrap()).collect();
    assert_eq!(values, vec![10.0, 20.0, 30.0, 40.0]);
}

/// The Phase 8 milestone shape: "query rows in a time range" answered by a
/// time index rather than a full scan. As with the Phase 6 spatial-index
/// test, we can't directly assert "single index scan" from SQL, so this
/// confirms the index answers the query correctly end-to-end by inserting a
/// decoy row outside the queried range that a naive scan would still visit.
#[test]
fn time_index_scan_answers_range_query() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE metrics (ts INT8, value FLOAT8)", db.clone()).unwrap();
    execute_query("INSERT INTO metrics VALUES (1000, 1.0)", db.clone()).unwrap(); // inside range
    execute_query("INSERT INTO metrics VALUES (1500, 2.0)", db.clone()).unwrap(); // inside range
    execute_query("INSERT INTO metrics VALUES (9000000, 3.0)", db.clone()).unwrap(); // far outside

    execute_query("CREATE INDEX ON metrics USING TIME (ts) WITH (interval = '1 hour', value = 'value')", db.clone()).unwrap();
    assert!(db.indexed_column_time("metrics", "ts"));

    let result = execute_query("SELECT value FROM metrics WHERE ts BETWEEN 0 AND 2000 ORDER BY value", db.clone()).unwrap();
    let values: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(values, vec!["1", "2"]);
}

#[test]
fn time_index_scan_answers_time_bucket_equality() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE metrics (ts INT8, value FLOAT8)", db.clone()).unwrap();
    execute_query("INSERT INTO metrics VALUES (1000, 1.0)", db.clone()).unwrap(); // bucket 0
    execute_query("INSERT INTO metrics VALUES (3700000, 2.0)", db.clone()).unwrap(); // bucket 3_600_000

    execute_query("CREATE INDEX ON metrics USING TIME (ts) WITH (interval = '1 hour', value = 'value')", db.clone()).unwrap();

    let result = execute_query(
        "SELECT value FROM metrics WHERE time_bucket('1 hour', ts) = 0",
        db.clone(),
    ).unwrap();
    let values: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(values, vec!["1"]);
}

/// Retention + automatic downsampling: raw rows past the retention window
/// are evicted from the time index (a range query over them returns
/// nothing), but the per-bucket rollup survives and still answers
/// aggregate-style queries — the "continuous aggregate" substrate.
#[test]
fn retention_evicts_raw_rows_but_rollup_survives() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE metrics (ts INT8, value FLOAT8)", db.clone()).unwrap();
    execute_query("INSERT INTO metrics VALUES (0, 5.0)", db.clone()).unwrap();

    // 1-hour buckets, 2-hour retention.
    execute_query(
        "CREATE INDEX ON metrics USING TIME (ts) WITH (interval = '1 hour', retention = '2 hours', value = 'value')",
        db.clone(),
    ).unwrap();

    // Push the "newest" timestamp far enough ahead that the first bucket
    // falls outside the retention window and gets downsampled.
    execute_query("INSERT INTO metrics VALUES (36000000, 50.0)", db.clone()).unwrap(); // 10 hours later

    let raw = db.time_range_query("metrics", "ts", 0, 3_600_000).unwrap();
    assert!(raw.is_empty(), "raw rows should be evicted past retention");

    let rollups = db.rollup_query("metrics", "ts", 0, 3_600_000).unwrap();
    assert_eq!(rollups.len(), 1);
    assert_eq!(rollups[0].1.count, 1);
    assert_eq!(rollups[0].1.sum, 5.0);
}
