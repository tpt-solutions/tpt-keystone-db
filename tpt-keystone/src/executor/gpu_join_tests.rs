//! End-to-end tests proving the GPU broad-phase spatial join path
//! (`geo::gpu`, wired into `apply_join`) produces exactly the same row set
//! as the existing CPU nested-loop join, for both `ST_Intersects` and
//! `ST_DWithin` join predicates.
//!
//! Real GPU hardware isn't available on every machine that runs `cargo
//! test` (CI, other contributors' laptops), so these are gated behind an
//! explicit opt-in env var rather than `#[ignore]` (this repo's existing
//! convention — see `storage/config.rs`'s `TPT_*` vars, and `geo::gpu`'s own
//! `TPT_TEST_GPU`-gated unit tests). Run with
//! `TPT_TEST_GPU=1 cargo test gpu_join_tests::`.
//!
//! Rather than inserting enough rows to exceed `TPT_GPU_JOIN_THRESHOLD`'s
//! default (1,000,000 row-pairs — impractical to generate row-by-row via SQL
//! `INSERT`s in a unit test), these tests override the threshold to `1` via
//! env var so a small, hand-checkable dataset still exercises the real GPU
//! dispatch path. This proves correctness (GPU and CPU paths agree exactly);
//! it does not benchmark the crossover point empirically — see the module
//! doc for how to do that manually against a running server via `psql`.
//!
//! Manual end-to-end verification: start the server (`cargo run`), `CREATE
//! TABLE`s with `GEOMETRY` columns, populate past
//! `TPT_GPU_JOIN_THRESHOLD` rows, run `SELECT ... FROM a JOIN b ON
//! ST_Intersects(a.geom, b.geom)` via `psql`, and cross-check the row count
//! against the same query with `TPT_DISABLE_GPU_JOIN=1` set before starting
//! the server.

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

fn cell_text(cell: &Option<Vec<u8>>) -> String {
    String::from_utf8(cell.clone().unwrap()).unwrap()
}

fn gpu_tests_enabled() -> bool {
    std::env::var("TPT_TEST_GPU").is_ok()
}

// Env var mutation makes these tests order-sensitive with respect to each
// other (and to `geo::gpu`'s own env-var-gated tests, compiled into the same
// test binary via `main.rs`) if run concurrently; both share
// `geo::gpu::GPU_ENV_TEST_LOCK` rather than each having an independent lock,
// since two independent locks wouldn't stop the two modules' tests from
// racing on the same process-wide env vars.
use crate::geo::gpu::GPU_ENV_TEST_LOCK as ENV_LOCK;

#[test]
fn gpu_intersects_join_matches_cpu_nested_loop() {
    if !gpu_tests_enabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("TPT_GPU_JOIN_THRESHOLD", "1");

    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE zones (id INT4, area GEOMETRY)", db.clone()).unwrap();
    execute_query("CREATE TABLE drones (id INT4, pos GEOMETRY)", db.clone()).unwrap();
    execute_query(
        "INSERT INTO zones VALUES (1, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')",
        db.clone(),
    )
    .unwrap();
    execute_query(
        "INSERT INTO zones VALUES (2, 'POLYGON((100 100, 100 110, 110 110, 110 100, 100 100))')",
        db.clone(),
    )
    .unwrap();
    execute_query("INSERT INTO drones VALUES (1, 'POINT(5 5)')", db.clone()).unwrap(); // inside zone 1's bbox
    execute_query(
        "INSERT INTO drones VALUES (2, 'POINT(500 500)')",
        db.clone(),
    )
    .unwrap(); // inside neither
    execute_query(
        "INSERT INTO drones VALUES (3, 'POINT(105 105)')",
        db.clone(),
    )
    .unwrap(); // inside zone 2's bbox

    let query = "SELECT zones.id, drones.id FROM zones JOIN drones ON ST_Intersects(zones.area, drones.pos) ORDER BY zones.id, drones.id";

    let gpu_result = execute_query(query, db.clone()).unwrap();
    let mut gpu_pairs: Vec<(String, String)> = gpu_result
        .rows
        .iter()
        .map(|r| (cell_text(&r[0]), cell_text(&r[1])))
        .collect();
    gpu_pairs.sort();

    std::env::set_var("TPT_DISABLE_GPU_JOIN", "1");
    let cpu_result = execute_query(query, db.clone()).unwrap();
    std::env::remove_var("TPT_DISABLE_GPU_JOIN");
    std::env::remove_var("TPT_GPU_JOIN_THRESHOLD");
    let mut cpu_pairs: Vec<(String, String)> = cpu_result
        .rows
        .iter()
        .map(|r| (cell_text(&r[0]), cell_text(&r[1])))
        .collect();
    cpu_pairs.sort();

    assert_eq!(
        gpu_pairs, cpu_pairs,
        "GPU and CPU spatial join paths must agree exactly"
    );
    assert_eq!(
        gpu_pairs,
        vec![
            ("1".to_string(), "1".to_string()),
            ("2".to_string(), "3".to_string())
        ]
    );
}

#[test]
fn gpu_dwithin_join_matches_cpu_nested_loop() {
    if !gpu_tests_enabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("TPT_GPU_JOIN_THRESHOLD", "1");

    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE cities (id INT4, pos GEOMETRY)", db.clone()).unwrap();
    execute_query("CREATE TABLE drones (id INT4, pos GEOMETRY)", db.clone()).unwrap();
    execute_query(
        "INSERT INTO cities VALUES (1, 'POINT(-0.1276 51.5074)')",
        db.clone(),
    )
    .unwrap(); // London
    execute_query(
        "INSERT INTO cities VALUES (2, 'POINT(-74.0060 40.7128)')",
        db.clone(),
    )
    .unwrap(); // New York
    execute_query(
        "INSERT INTO drones VALUES (1, 'POINT(2.3522 48.8566)')",
        db.clone(),
    )
    .unwrap(); // Paris (~344km from London)

    let query = "SELECT cities.id, drones.id FROM cities JOIN drones ON ST_DWithin(cities.pos, drones.pos, 400000) ORDER BY cities.id, drones.id";

    let gpu_result = execute_query(query, db.clone()).unwrap();
    let mut gpu_pairs: Vec<(String, String)> = gpu_result
        .rows
        .iter()
        .map(|r| (cell_text(&r[0]), cell_text(&r[1])))
        .collect();
    gpu_pairs.sort();

    std::env::set_var("TPT_DISABLE_GPU_JOIN", "1");
    let cpu_result = execute_query(query, db.clone()).unwrap();
    std::env::remove_var("TPT_DISABLE_GPU_JOIN");
    std::env::remove_var("TPT_GPU_JOIN_THRESHOLD");
    let mut cpu_pairs: Vec<(String, String)> = cpu_result
        .rows
        .iter()
        .map(|r| (cell_text(&r[0]), cell_text(&r[1])))
        .collect();
    cpu_pairs.sort();

    assert_eq!(
        gpu_pairs, cpu_pairs,
        "GPU and CPU spatial join paths must agree exactly"
    );
    assert_eq!(gpu_pairs, vec![("1".to_string(), "1".to_string())]);
}

/// Not asserted against a hard number — CI/dev-machine timing isn't a
/// reliable pass/fail signal — but printed for manual inspection to get a
/// real, if single-machine, sense of where the GPU/CPU crossover point sits.
#[test]
fn gpu_vs_cpu_wall_clock_at_moderate_scale() {
    if !gpu_tests_enabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("TPT_GPU_JOIN_THRESHOLD", "1");

    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE zones (id INT4, area GEOMETRY)", db.clone()).unwrap();
    execute_query("CREATE TABLE drones (id INT4, pos GEOMETRY)", db.clone()).unwrap();
    for i in 0..200 {
        let x = (i % 20) as f64 * 10.0;
        let y = (i / 20) as f64 * 10.0;
        execute_query(&format!("INSERT INTO zones VALUES ({i}, 'POLYGON(({x} {y}, {x} {y2}, {x2} {y2}, {x2} {y}, {x} {y}))')", x = x, y = y, x2 = x + 5.0, y2 = y + 5.0), db.clone()).unwrap();
    }
    for i in 0..200 {
        let x = (i % 20) as f64 * 10.0 + 2.0;
        let y = (i / 20) as f64 * 10.0 + 2.0;
        execute_query(
            &format!("INSERT INTO drones VALUES ({i}, 'POINT({x} {y})')"),
            db.clone(),
        )
        .unwrap();
    }
    let query = "SELECT zones.id, drones.id FROM zones JOIN drones ON ST_Intersects(zones.area, drones.pos)";

    let t0 = std::time::Instant::now();
    let gpu_result = execute_query(query, db.clone()).unwrap();
    let gpu_elapsed = t0.elapsed();

    std::env::set_var("TPT_DISABLE_GPU_JOIN", "1");
    let t1 = std::time::Instant::now();
    let cpu_result = execute_query(query, db.clone()).unwrap();
    let cpu_elapsed = t1.elapsed();
    std::env::remove_var("TPT_DISABLE_GPU_JOIN");
    std::env::remove_var("TPT_GPU_JOIN_THRESHOLD");

    eprintln!("200x200 spatial join: GPU {gpu_elapsed:?}, CPU {cpu_elapsed:?}");
    assert_eq!(gpu_result.rows.len(), cpu_result.rows.len());
}
