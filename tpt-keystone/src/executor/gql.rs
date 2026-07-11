//! Plexus's GQL-subset `MATCH` statement (Phase 9 roadmap item, "GQL
//! compatibility layer") — executes `ast::MatchStmt` against an existing
//! `CREATE INDEX ... USING GRAPH` index by chaining `Database::graph_neighbors`
//! calls one hop per pattern edge, the same underlying primitive
//! `graph_neighbors`/`graph_bfs` (the table-valued-function surface,
//! `executor::graph_fn`) already use — `MATCH` is a second, statement-level
//! surface over the identical graph index, not a new storage/traversal
//! engine.
//!
//! See `ast::MatchStmt`'s doc comment for the honest scope of this GQL
//! subset (single linear-chain pattern, one starting-vertex equality
//! filter, `RETURN` limited to the pattern's own node variables).

use std::sync::Arc;

use crate::graph::Direction;
use crate::sql::ast::{MatchDirection, MatchStmt};
use crate::storage::database::Database;
use crate::wire::messages::{oid, FieldDescription};

use super::QueryResult;

fn to_graph_direction(d: MatchDirection) -> Direction {
    match d {
        MatchDirection::Out => Direction::Out,
        MatchDirection::In => Direction::In,
        MatchDirection::Both => Direction::Both,
    }
}

pub fn execute_match(stmt: MatchStmt, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    anyhow::ensure!(
        db.indexed_column_graph(&stmt.table, &stmt.column),
        "no graph index on {}.{} (CREATE INDEX ... USING GRAPH first)",
        stmt.table,
        stmt.column
    );

    let starts: Vec<Vec<u8>> = match &stmt.start_filter {
        Some(lit) => vec![lit.clone().into_bytes()],
        None => db
            .graph_all_vertices(&stmt.table, &stmt.column)
            .unwrap_or_default(),
    };

    // Each path is one binding of the pattern's node variables, in order,
    // grown one hop at a time -- a standard bounded-width BFS over the
    // pattern chain (not the whole graph: only vertices actually reachable
    // via a matching-typed edge from a live path are ever expanded).
    let mut paths: Vec<Vec<Vec<u8>>> = starts.into_iter().map(|v| vec![v]).collect();

    for hop in &stmt.hops {
        let dir = to_graph_direction(hop.direction);
        let mut next_paths = Vec::new();
        for path in &paths {
            let last = path.last().expect("path always has >= 1 node");
            let Some(neighbors) = db.graph_neighbors(&stmt.table, &stmt.column, last, dir) else {
                continue;
            };
            for (neighbor_key, rel) in neighbors {
                if let Some(want) = &hop.rel_type {
                    if rel.as_deref() != Some(want.as_str()) {
                        continue;
                    }
                }
                let mut next = path.clone();
                next.push(neighbor_key);
                next_paths.push(next);
            }
        }
        paths = next_paths;
    }

    if let Some(limit) = stmt.limit {
        paths.truncate(limit as usize);
    }

    let return_indices: Vec<usize> = stmt
        .returns
        .iter()
        .map(|r| {
            stmt.nodes
                .iter()
                .position(|n| n == r)
                .expect("parser already validated every RETURN var is a pattern node")
        })
        .collect();

    let fields = stmt
        .returns
        .iter()
        .map(|r| FieldDescription::simple(r, oid::TEXT))
        .collect();
    let rows: Vec<Vec<Option<Vec<u8>>>> = paths
        .iter()
        .map(|path| {
            return_indices
                .iter()
                .map(|&i| Some(path[i].clone()))
                .collect()
        })
        .collect();
    let tag = format!("MATCH {}", rows.len());

    Ok(QueryResult { fields, rows, tag })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::executor::execute_query;
    use crate::storage::config::NodeRole;
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
            Database::open(local.path(), store, lease.handle(), NodeRole::Writer, Default::default())
                .unwrap(),
        );
        (db, bucket, local)
    }

    fn cell_text(cell: &Option<Vec<u8>>) -> String {
        String::from_utf8(cell.clone().unwrap()).unwrap()
    }

    // `id` leads every row so it becomes the storage row key (same fixture
    // shape as `plexus_tests.rs::make_social_graph` — without it, two edges
    // sharing a `from_id` would collide and overwrite each other).
    fn make_social_graph(db: &Arc<Database>) {
        execute_query(
            "CREATE TABLE follows (id INT4, from_id TEXT, to_id TEXT, rel TEXT)",
            db.clone(),
        )
        .unwrap();
        for (i, (a, b)) in [
            ("alice", "bob"),
            ("bob", "carol"),
            ("alice", "carol"),
            ("carol", "dave"),
        ]
        .into_iter()
        .enumerate()
        {
            execute_query(
                &format!("INSERT INTO follows VALUES ({i}, '{a}', '{b}', 'FOLLOWS')"),
                db.clone(),
            )
            .unwrap();
        }
        execute_query(
            "CREATE INDEX ON follows USING GRAPH (from_id) WITH (to = 'to_id', type = 'rel')",
            db.clone(),
        )
        .unwrap();
    }

    #[test]
    fn single_hop_match_with_start_filter_and_typed_edge() {
        let (db, _b, _l) = test_db();
        make_social_graph(&db);

        let result = execute_query(
            "MATCH (a)-[:FOLLOWS]->(b) ON follows(from_id) WHERE a = 'alice' RETURN a, b",
            db.clone(),
        )
        .unwrap();
        let mut pairs: Vec<(String, String)> = result
            .rows
            .iter()
            .map(|r| (cell_text(&r[0]), cell_text(&r[1])))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("alice".to_string(), "bob".to_string()),
                ("alice".to_string(), "carol".to_string()),
            ]
        );
    }

    #[test]
    fn single_hop_match_untyped_edge_and_return_both_vars() {
        let (db, _b, _l) = test_db();
        make_social_graph(&db);

        let result = execute_query(
            "MATCH (a)-[]->(b) ON follows(from_id) WHERE a = 'alice' RETURN a, b",
            db.clone(),
        )
        .unwrap();
        let mut pairs: Vec<(String, String)> = result
            .rows
            .iter()
            .map(|r| (cell_text(&r[0]), cell_text(&r[1])))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("alice".to_string(), "bob".to_string()),
                ("alice".to_string(), "carol".to_string()),
            ]
        );
    }

    #[test]
    fn two_hop_chain_returns_all_three_nodes() {
        let (db, _b, _l) = test_db();
        make_social_graph(&db);

        let result = execute_query(
            "MATCH (a)-[]->(b)-[]->(c) ON follows(from_id) WHERE a = 'alice' RETURN a, b, c",
            db.clone(),
        )
        .unwrap();
        let mut triples: Vec<(String, String, String)> = result
            .rows
            .iter()
            .map(|r| (cell_text(&r[0]), cell_text(&r[1]), cell_text(&r[2])))
            .collect();
        triples.sort();
        // alice -> bob -> carol, alice -> carol -> dave
        assert_eq!(
            triples,
            vec![
                ("alice".to_string(), "bob".to_string(), "carol".to_string()),
                ("alice".to_string(), "carol".to_string(), "dave".to_string()),
            ]
        );
    }

    #[test]
    fn reverse_direction_match_finds_incoming_edges() {
        let (db, _b, _l) = test_db();
        make_social_graph(&db);

        let result = execute_query(
            "MATCH (a)<-[]-(b) ON follows(from_id) WHERE a = 'carol' RETURN b",
            db.clone(),
        )
        .unwrap();
        let mut froms: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
        froms.sort();
        assert_eq!(froms, vec!["alice".to_string(), "bob".to_string()]);
    }

    #[test]
    fn match_without_where_scans_every_vertex_as_a_start() {
        let (db, _b, _l) = test_db();
        make_social_graph(&db);

        let result = execute_query(
            "MATCH (a)-[]->(b) ON follows(from_id) RETURN a, b",
            db.clone(),
        )
        .unwrap();
        // 4 edges were inserted -> 4 (a,b) bindings, regardless of which
        // vertex each search started from.
        assert_eq!(result.rows.len(), 4);
    }

    #[test]
    fn limit_bounds_result_rows() {
        let (db, _b, _l) = test_db();
        make_social_graph(&db);

        let result = execute_query(
            "MATCH (a)-[]->(b) ON follows(from_id) RETURN a, b LIMIT 2",
            db.clone(),
        )
        .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn typed_relationship_filters_edges() {
        let (db, _b, _l) = test_db();
        execute_query(
            "CREATE TABLE edges2 (id INT4, from_id TEXT, to_id TEXT, kind TEXT)",
            db.clone(),
        )
        .unwrap();
        execute_query(
            "INSERT INTO edges2 VALUES (0, 'x', 'y', 'friend')",
            db.clone(),
        )
        .unwrap();
        execute_query(
            "INSERT INTO edges2 VALUES (1, 'x', 'z', 'blocked')",
            db.clone(),
        )
        .unwrap();
        execute_query(
            "CREATE INDEX ON edges2 USING GRAPH (from_id) WITH (to = 'to_id', type = 'kind')",
            db.clone(),
        )
        .unwrap();

        let result = execute_query(
            "MATCH (a)-[:friend]->(b) ON edges2(from_id) WHERE a = 'x' RETURN b",
            db.clone(),
        )
        .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(cell_text(&result.rows[0][0]), "y");
    }

    #[test]
    fn missing_graph_index_errors_clearly() {
        let (db, _b, _l) = test_db();
        execute_query("CREATE TABLE t (a TEXT, b TEXT)", db.clone()).unwrap();
        let err = execute_query(
            "MATCH (a)-[]->(b) ON t(a) WHERE a = 'x' RETURN a, b",
            db.clone(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("no graph index"));
    }
}
