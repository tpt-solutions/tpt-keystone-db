//! End-to-end tests for Plexus (Phase 9): `CREATE INDEX ... USING GRAPH`
//! backing the graph-traversal/algorithm table-valued functions
//! (`graph_neighbors`, `graph_bfs`, `graph_shortest_path`,
//! `graph_connected_components`, `graph_pagerank`, `graph_triangle_count`)
//! exposed in the `FROM` clause — mirroring `geo_tests.rs`'s "spatial index
//! answers a real query end-to-end" and `chronos_tests.rs`'s time-index
//! pattern.

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

// `id` leads every row so it becomes the storage row key (this engine keys
// an unconstrained table by its first column's value) — without it, two
// edges sharing a `from_id` would collide and overwrite each other.
fn make_social_graph(db: &Arc<Database>) {
    execute_query("CREATE TABLE follows (id INT4, from_id TEXT, to_id TEXT, rel TEXT)", db.clone()).unwrap();
    for (i, (a, b)) in [("alice", "bob"), ("bob", "carol"), ("carol", "dave"), ("alice", "carol")].into_iter().enumerate() {
        execute_query(&format!("INSERT INTO follows VALUES ({i}, '{a}', '{b}', 'FOLLOWS')"), db.clone()).unwrap();
    }
    execute_query("CREATE INDEX ON follows USING GRAPH (from_id) WITH (to = 'to_id', type = 'rel')", db.clone()).unwrap();
}

#[test]
fn graph_index_created_and_visible() {
    let (db, _b, _l) = test_db();
    make_social_graph(&db);
    assert!(db.indexed_column_graph("follows", "from_id"));
    assert_eq!(db.list_graph_indexes(), vec![("follows".to_string(), "from_id".to_string())]);
}

#[test]
fn graph_neighbors_returns_outgoing_edges_with_rel_type() {
    let (db, _b, _l) = test_db();
    make_social_graph(&db);
    let result = execute_query(
        "SELECT neighbor, rel_type FROM graph_neighbors('follows', 'from_id', 'alice') ORDER BY neighbor",
        db.clone(),
    ).unwrap();
    let rows: Vec<(String, String)> = result.rows.iter().map(|r| (cell_text(&r[0]), cell_text(&r[1]))).collect();
    assert_eq!(rows, vec![("bob".to_string(), "FOLLOWS".to_string()), ("carol".to_string(), "FOLLOWS".to_string())]);
}

#[test]
fn graph_neighbors_direction_filter() {
    let (db, _b, _l) = test_db();
    make_social_graph(&db);
    // carol is followed by both bob and alice — 'in' direction should surface both.
    let result = execute_query(
        "SELECT neighbor FROM graph_neighbors('follows', 'from_id', 'carol', 'in') ORDER BY neighbor",
        db.clone(),
    ).unwrap();
    let rows: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(rows, vec!["alice".to_string(), "bob".to_string()]);
}

#[test]
fn graph_bfs_respects_max_depth() {
    let (db, _b, _l) = test_db();
    make_social_graph(&db);
    let result = execute_query(
        "SELECT vertex, depth FROM graph_bfs('follows', 'from_id', 'alice', 1) ORDER BY vertex",
        db.clone(),
    ).unwrap();
    let rows: Vec<(String, String)> = result.rows.iter().map(|r| (cell_text(&r[0]), cell_text(&r[1]))).collect();
    // depth 0: alice; depth 1: bob, carol (dave is 2 hops away, excluded)
    assert_eq!(rows, vec![
        ("alice".to_string(), "0".to_string()),
        ("bob".to_string(), "1".to_string()),
        ("carol".to_string(), "1".to_string()),
    ]);
}

#[test]
fn graph_shortest_path_finds_hops() {
    let (db, _b, _l) = test_db();
    make_social_graph(&db);
    let result = execute_query(
        "SELECT vertex FROM graph_shortest_path('follows', 'from_id', 'alice', 'dave') ORDER BY step",
        db.clone(),
    ).unwrap();
    let rows: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    // alice -> carol -> dave is the 2-hop path (alice -> bob -> carol -> dave is longer).
    assert_eq!(rows, vec!["alice".to_string(), "carol".to_string(), "dave".to_string()]);
}

#[test]
fn graph_connected_components_separates_disjoint_subgraphs() {
    let (db, _b, _l) = test_db();
    make_social_graph(&db);
    execute_query("INSERT INTO follows VALUES (99, 'xavier', 'yolanda', 'FOLLOWS')", db.clone()).unwrap();
    let result = execute_query(
        "SELECT vertex, component FROM graph_connected_components('follows', 'from_id')",
        db.clone(),
    ).unwrap();
    let mut by_vertex = std::collections::HashMap::new();
    for r in &result.rows {
        by_vertex.insert(cell_text(&r[0]), cell_text(&r[1]));
    }
    assert_eq!(by_vertex["alice"], by_vertex["dave"]);
    assert_ne!(by_vertex["alice"], by_vertex["xavier"]);
}

#[test]
fn graph_pagerank_ranks_sink_highest() {
    let (db, _b, _l) = test_db();
    make_social_graph(&db);
    let result = execute_query(
        "SELECT vertex, score FROM graph_pagerank('follows', 'from_id')",
        db.clone(),
    ).unwrap();
    let mut by_vertex = std::collections::HashMap::new();
    for r in &result.rows {
        by_vertex.insert(cell_text(&r[0]), cell_text(&r[1]).parse::<f64>().unwrap());
    }
    // dave has no outgoing edges (pure sink) and two hops of incoming rank
    // flowing toward it; alice has no incoming edges at all.
    assert!(by_vertex["dave"] > by_vertex["alice"]);
}

#[test]
fn graph_triangle_count_finds_triangle() {
    let (db, _b, _l) = test_db();
    // alice -> bob -> carol -> alice forms a triangle.
    execute_query("CREATE TABLE edges (from_id TEXT, to_id TEXT)", db.clone()).unwrap();
    for (a, b) in [("alice", "bob"), ("bob", "carol"), ("carol", "alice")] {
        execute_query(&format!("INSERT INTO edges VALUES ('{a}', '{b}')"), db.clone()).unwrap();
    }
    execute_query("CREATE INDEX ON edges USING GRAPH (from_id) WITH (to = 'to_id')", db.clone()).unwrap();

    let result = execute_query(
        "SELECT vertex, triangles FROM graph_triangle_count('edges', 'from_id')",
        db.clone(),
    ).unwrap();
    for r in &result.rows {
        assert_eq!(cell_text(&r[1]), "1");
    }
}

#[test]
fn hybrid_sql_filters_graph_function_results() {
    let (db, _b, _l) = test_db();
    make_social_graph(&db);
    // "Filter vertices by SQL, traverse by graph" — WHERE on a table-valued
    // graph function's output, joined against a plain relational table.
    execute_query("CREATE TABLE users (name TEXT, active BOOL)", db.clone()).unwrap();
    for (name, active) in [("bob", true), ("carol", false)] {
        execute_query(&format!("INSERT INTO users VALUES ('{name}', {active})"), db.clone()).unwrap();
    }
    let result = execute_query(
        "SELECT n.neighbor FROM graph_neighbors('follows', 'from_id', 'alice') n \
         JOIN users u ON u.name = n.neighbor WHERE u.active = true",
        db.clone(),
    ).unwrap();
    let rows: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(rows, vec!["bob".to_string()]);
}
