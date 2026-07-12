//! End-to-end tests proving the GPU batch-similarity path (`vector::gpu`,
//! wired into `Database::vector_knn_query`) answers `vector_search` correctly
//! when no HNSW/IVF-PQ index exists, agreeing with a CPU brute-force baseline.
//!
//! Real GPU hardware isn't available on every machine that runs `cargo test`
//! (CI, other contributors' laptops), so these are gated behind an explicit
//! opt-in env var rather than `#[ignore]` (this repo's existing convention —
//! see `storage/config.rs`'s `TPT_*` vars, and `geo::gpu`'s own
//! `TPT_TEST_GPU`-gated unit tests). Run with
//! `TPT_TEST_GPU=1 cargo test prism_gpu_tests::`.
//!
//! These tests share `vector::gpu::GPU_ENV_TEST_LOCK` with `vector::gpu`'s own
//! unit tests (compiled into the same test binary via `main.rs`) since both
//! mutate the same process-wide `TPT_DISABLE_GPU_VECTOR` env var.

use std::sync::Arc;
use std::time::Duration;

use super::execute_query;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::vector::gpu::GPU_ENV_TEST_LOCK as ENV_LOCK;
use crate::vector::vector::{l2_distance, Vector};

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

fn gpu_tests_enabled() -> bool {
    std::env::var("TPT_TEST_GPU").is_ok()
}

fn cell_text(cell: &Option<Vec<u8>>) -> String {
    String::from_utf8(cell.clone().unwrap()).unwrap()
}

/// Builds a `docs` table with a `VECTOR` column and rows, deliberately
/// WITHOUT a vector index, so `vector_search` must take the GPU brute-force
/// path (when a GPU is available).
fn make_doc_table_no_index(db: &Arc<Database>) {
    execute_query(
        "CREATE TABLE docs (id INT4, label TEXT, embedding VECTOR)",
        db.clone(),
    )
    .unwrap();
    let rows = [
        (1, "near-x", "[1.0,0.0,0.0]"),
        (2, "near-y", "[0.0,1.0,0.0]"),
        (3, "also-near-x", "[0.9,0.1,0.0]"),
        (4, "far", "[-1.0,-1.0,-1.0]"),
        (5, "mid", "[0.5,0.5,0.0]"),
    ];
    for (id, label, emb) in rows {
        execute_query(
            &format!("INSERT INTO docs VALUES ({id}, '{label}', '{emb}')"),
            db.clone(),
        )
        .unwrap();
    }
}

#[test]
fn gpu_vector_search_without_index_matches_cpu_brute_force() {
    if !gpu_tests_enabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let (db, _b, _l) = test_db();
    make_doc_table_no_index(&db);

    let query = "[1.0,0.0,0.0]";
    let k = 3;

    // GPU-backed `vector_search` (no index -> GPU brute-force path).
    let gpu_result = execute_query(
        &format!("SELECT label, distance FROM vector_search('docs', 'embedding', '{query}', {k})"),
        db.clone(),
    )
    .unwrap();
    let mut gpu: Vec<(String, f64)> = gpu_result
        .rows
        .iter()
        .map(|r| (cell_text(&r[0]), cell_text(&r[1]).parse::<f64>().unwrap()))
        .collect();
    gpu.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    // CPU brute-force baseline over the same rows.
    let all = execute_query("SELECT label, embedding FROM docs ORDER BY id", db.clone()).unwrap();
    let q = Vector::from_text(query).unwrap();
    let mut cpu: Vec<(String, f64)> = all
        .rows
        .iter()
        .map(|r| {
            let label = cell_text(&r[0]);
            let emb = Vector::from_text(&cell_text(&r[1])).unwrap();
            let d = l2_distance(q.as_slice(), emb.as_slice()).unwrap() as f64;
            (label, d)
        })
        .collect();
    cpu.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    cpu.truncate(k);

    assert_eq!(gpu.len(), cpu.len());
    for (g, c) in gpu.iter().zip(cpu.iter()) {
        assert_eq!(g.0, c.0, "nearest-label ranking must match CPU");
        assert!(
            (g.1 - c.1).abs() < 1e-3,
            "distance must match CPU within tolerance"
        );
    }
}

#[test]
fn vector_search_without_index_errors_when_gpu_disabled() {
    if !gpu_tests_enabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let (db, _b, _l) = test_db();
    make_doc_table_no_index(&db);

    std::env::set_var("TPT_DISABLE_GPU_VECTOR", "1");
    let result = execute_query(
        "SELECT label FROM vector_search('docs', 'embedding', '[1.0,0.0,0.0]', 3)",
        db.clone(),
    );
    std::env::remove_var("TPT_DISABLE_GPU_VECTOR");
    // With GPU disabled and no index, the historical "no vector index"
    // contract is preserved (caller errors) rather than silently returning
    // partial/CPU results.
    assert!(result.is_err());
}
