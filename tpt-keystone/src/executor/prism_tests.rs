//! End-to-end tests for Prism (Phase 7): `VECTOR` columns, vector literal
//! text round-tripping, distance/similarity scalar functions, and
//! `CREATE INDEX ... USING VECTOR` backing the `vector_search` table-valued
//! function — mirroring `geo_tests.rs`'s "index answers a real query
//! end-to-end" pattern.

use std::sync::Arc;
use std::time::Duration;

use super::execute_query;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::vector::gpu::GPU_ENV_TEST_LOCK as ENV_LOCK;

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

#[test]
fn vector_column_round_trips_as_text() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE docs (id INT4, embedding VECTOR)", db.clone()).unwrap();
    execute_query("INSERT INTO docs VALUES (1, '[1.0,2.0,3.0]')", db.clone()).unwrap();

    let result = execute_query("SELECT embedding FROM docs", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "[1,2,3]");
}

#[test]
fn l2_distance_known_case() {
    let (db, _b, _l) = test_db();
    let result = execute_query("SELECT l2_distance('[0,0]', '[3,4]')", db.clone()).unwrap();
    let dist: f64 = cell_text(&result.rows[0][0]).parse().unwrap();
    assert!((dist - 5.0).abs() < 1e-6);
}

#[test]
fn cosine_distance_identical_vectors_is_zero() {
    let (db, _b, _l) = test_db();
    let result = execute_query("SELECT cosine_distance('[1,2,3]', '[1,2,3]')", db.clone()).unwrap();
    let dist: f64 = cell_text(&result.rows[0][0]).parse().unwrap();
    assert!(dist.abs() < 1e-5);
}

#[test]
fn dot_product_known_case() {
    let (db, _b, _l) = test_db();
    let result = execute_query("SELECT dot_product('[1,2,3]', '[4,5,6]')", db.clone()).unwrap();
    let v: f64 = cell_text(&result.rows[0][0]).parse().unwrap();
    assert!((v - 32.0).abs() < 1e-6);
}

fn make_doc_table(db: &Arc<Database>) {
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
    ];
    for (id, label, emb) in rows {
        execute_query(
            &format!("INSERT INTO docs VALUES ({id}, '{label}', '{emb}')"),
            db.clone(),
        )
        .unwrap();
    }
    execute_query(
        "CREATE INDEX ON docs USING VECTOR (embedding) WITH (metric = 'l2')",
        db.clone(),
    )
    .unwrap();
}

#[test]
fn vector_index_created_and_visible() {
    let (db, _b, _l) = test_db();
    make_doc_table(&db);
    assert!(db.indexed_column_vector("docs", "embedding"));
    assert_eq!(
        db.list_vector_indexes(),
        vec![("docs".to_string(), "embedding".to_string())]
    );
}

/// The Phase 7 milestone shape: a k-NN query answered by the HNSW index
/// rather than a full scan. We can't directly assert "index scan, not full
/// scan" from SQL, so this instead confirms the vector index answers the
/// query correctly end-to-end via the `vector_search` table function,
/// composed with an ordinary SQL `JOIN`/`WHERE` the same way Plexus's
/// `graph_neighbors` composes (hybrid SQL + vector search).
#[test]
fn vector_search_knn_returns_nearest_neighbors() {
    let (db, _b, _l) = test_db();
    make_doc_table(&db);

    let result = execute_query(
        "SELECT label FROM vector_search('docs', 'embedding', '[1.0,0.0,0.0]', 2) ORDER BY label",
        db.clone(),
    )
    .unwrap();
    let labels: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(
        labels,
        vec!["also-near-x".to_string(), "near-x".to_string()]
    );
}

fn make_hybrid_table(db: &Arc<Database>) {
    execute_query(
        "CREATE TABLE hdocs (id INT4, label TEXT, body TEXT, embedding VECTOR)",
        db.clone(),
    )
    .unwrap();
    let rows = [
        (
            1,
            "a",
            "rust systems programming rust rust",
            "[1.0,0.0,0.0]",
        ),
        (2, "b", "python scripting language", "[0.95,0.05,0.0]"),
        (3, "c", "rust programming language", "[-1.0,-1.0,-1.0]"),
        (4, "d", "cooking recipes and food", "[-0.9,-0.9,-1.0]"),
    ];
    for (id, label, body, emb) in rows {
        execute_query(
            &format!("INSERT INTO hdocs VALUES ({id}, '{label}', '{body}', '{emb}')"),
            db.clone(),
        )
        .unwrap();
    }
    execute_query(
        "CREATE INDEX ON hdocs USING VECTOR (embedding) WITH (metric = 'l2')",
        db.clone(),
    )
    .unwrap();
    execute_query("CREATE INDEX ON hdocs USING GIN (body)", db.clone()).unwrap();
}

/// `hybrid_search` fuses a vector k-NN ranking and a BM25 ranking via RRF: a
/// row that ranks well on *both* signals (id 1: nearest vector neighbor AND
/// the strongest BM25 match for "rust") should come out on top, ahead of a
/// row that only wins on one signal (id 2: vector-near but zero BM25 score;
/// id 3: on-topic but vector-far).
#[test]
fn hybrid_search_fuses_vector_and_bm25_rankings() {
    let (db, _b, _l) = test_db();
    make_hybrid_table(&db);

    let result = execute_query(
        "SELECT label, vec_distance, bm25_score, fused_score FROM hybrid_search('hdocs', 'embedding', '[1.0,0.0,0.0]', 'body', 'rust', 3)",
        db.clone(),
    ).unwrap();
    assert_eq!(
        result.rows.len(),
        3,
        "'cooking' doc (no vector-near, no bm25 match) must be excluded"
    );
    let labels: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(
        labels[0], "a",
        "wins on both vector-nearness and BM25 relevance"
    );
    assert!(
        labels.contains(&"b".to_string()),
        "vector-near-only row still surfaces via RRF"
    );
    assert!(
        labels.contains(&"c".to_string()),
        "bm25-relevant-only row still surfaces via RRF"
    );
    assert!(!labels.contains(&"d".to_string()));
}

/// Confirms `hybrid_search` errors clearly (rather than silently returning
/// nothing) when one of the two required indexes is missing.
///
/// Forces `TPT_DISABLE_GPU_VECTOR=1` around the query so this is
/// deterministic regardless of whether the machine running `cargo test` has
/// a real GPU: `Database::vector_knn_query` has a GPU brute-force fallback
/// (`vector::gpu`) that would otherwise silently answer this query on a
/// GPU-equipped host instead of erroring, since it also runs when no
/// HNSW/IVF-PQ index exists. Same pattern
/// `prism_gpu_tests::vector_search_without_index_errors_when_gpu_disabled`
/// already established, sharing its `GPU_ENV_TEST_LOCK` guard since both
/// mutate the same process-wide env var.
#[test]
fn hybrid_search_requires_both_indexes() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE nodex (id INT4, label TEXT, body TEXT, embedding VECTOR)",
        db.clone(),
    )
    .unwrap();
    execute_query(
        "INSERT INTO nodex VALUES (1, 'a', 'rust', '[1.0,0.0,0.0]')",
        db.clone(),
    )
    .unwrap();
    std::env::set_var("TPT_DISABLE_GPU_VECTOR", "1");
    let result = execute_query(
        "SELECT * FROM hybrid_search('nodex', 'embedding', '[1.0,0.0,0.0]', 'body', 'rust', 3)",
        db.clone(),
    );
    std::env::remove_var("TPT_DISABLE_GPU_VECTOR");
    match result {
        Err(e) => assert!(e.to_string().contains("no vector index")),
        Ok(_) => panic!("expected an error for a missing vector index"),
    }
}

#[test]
fn vector_search_hybrid_sql_filter() {
    let (db, _b, _l) = test_db();
    make_doc_table(&db);

    let result = execute_query(
        "SELECT label FROM vector_search('docs', 'embedding', '[1.0,0.0,0.0]', 4) v JOIN docs d ON d.label = v.label WHERE d.id != 4 ORDER BY label",
        db.clone(),
    ).unwrap();
    let labels: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(
        labels,
        vec![
            "also-near-x".to_string(),
            "near-x".to_string(),
            "near-y".to_string()
        ]
    );
}

/// End-to-end SQL-level coverage for `CREATE INDEX ... USING IVFPQ`: unlike
/// `storage::ivf_pq_index::tests`/`vector::ivf_pq::tests` (which exercise
/// the Rust types directly), this drives the whole stack from parsed SQL
/// through `executor/ddl.rs`'s DDL handler, `Database::create_ivfpq_index`'s
/// backfill-from-existing-rows training, and `vector_search`'s
/// `Database::vector_knn_query` routing — this table has no HNSW index, so
/// routing falls through to the `(None, Some(ivf))` IVF-PQ-only arm.
#[test]
fn ivfpq_index_created_and_answers_vector_search() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE idocs (id INT4, label TEXT, embedding VECTOR)",
        db.clone(),
    )
    .unwrap();
    let rows = [
        (1, "near-x", "[1.0,0.0,0.0]"),
        (2, "near-y", "[0.0,1.0,0.0]"),
        (3, "also-near-x", "[0.9,0.1,0.0]"),
        (4, "far", "[-1.0,-1.0,-1.0]"),
    ];
    for (id, label, emb) in rows {
        execute_query(
            &format!("INSERT INTO idocs VALUES ({id}, '{label}', '{emb}')"),
            db.clone(),
        )
        .unwrap();
    }
    // dim=3 needs a pq_m that divides it evenly; the DDL default (8) would
    // error on this small test table, so override it.
    execute_query(
        "CREATE INDEX ON idocs USING IVFPQ (embedding) WITH (metric = 'l2', lists = '2', pq_m = '1', n_probe = '2')",
        db.clone(),
    )
    .unwrap();

    assert!(db.indexed_column_ivfpq("idocs", "embedding"));
    assert_eq!(
        db.list_ivfpq_indexes(),
        vec![("idocs".to_string(), "embedding".to_string())]
    );

    let result = execute_query(
        "SELECT label FROM vector_search('idocs', 'embedding', '[1.0,0.0,0.0]', 2) ORDER BY label",
        db.clone(),
    )
    .unwrap();
    let labels: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(
        labels,
        vec!["also-near-x".to_string(), "near-x".to_string()]
    );
}
