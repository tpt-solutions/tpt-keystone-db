//! End-to-end tests for Canopy (Phase 10): JSON operators/functions,
//! `CREATE INDEX ... USING JSONPATH`/`GIN` and their table-valued lookup
//! functions, and `CREATE TABLE ... WITH (json_schema_col = ...)` validation
//! — mirroring `plexus_tests.rs`'s "index created via SQL answers a real
//! query end-to-end" pattern.

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

fn make_docs_table(db: &Arc<Database>) {
    execute_query("CREATE TABLE docs (id INT4, body JSON)", db.clone()).unwrap();
    let rows = [
        (1, r#"{"user":{"name":"Ada","address":{"city":"Wellington"}},"tags":["admin","beta"]}"#),
        (2, r#"{"user":{"name":"Bob","address":{"city":"Auckland"}},"tags":["user"]}"#),
        (3, r#"{"user":{"name":"Cleo","address":{"city":"Wellington"}},"tags":["user","beta"]}"#),
    ];
    for (id, body) in rows {
        execute_query(&format!("INSERT INTO docs VALUES ({id}, '{}')", body.replace('\'', "''")), db.clone()).unwrap();
    }
}

#[test]
fn arrow_operator_extracts_object_key() {
    let (db, _b, _l) = test_db();
    make_docs_table(&db);
    let result = execute_query("SELECT body -> 'user' ->> 'name' FROM docs WHERE id = 1", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "Ada");
}

#[test]
fn hasharrow_operator_extracts_nested_path() {
    let (db, _b, _l) = test_db();
    make_docs_table(&db);
    let result = execute_query("SELECT body #>> '{user,address,city}' FROM docs WHERE id = 1", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "Wellington");
}

#[test]
fn contains_operator_matches_array_membership() {
    let (db, _b, _l) = test_db();
    make_docs_table(&db);
    let result = execute_query(
        "SELECT id FROM docs WHERE body @> '{\"tags\":[\"beta\"]}' ORDER BY id",
        db.clone(),
    ).unwrap();
    let ids: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(ids, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn jsonb_build_object_and_typeof_roundtrip() {
    let (db, _b, _l) = test_db();
    let result = execute_query("SELECT jsonb_typeof(jsonb_build_object('a', 1, 'b', 'x'))", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "object");
}

#[test]
fn jsonb_set_replaces_nested_value() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        r#"SELECT jsonb_set('{"a":{"b":1}}', '{a,b}', '2')"#,
        db.clone(),
    ).unwrap();
    let doc: serde_json::Value = serde_json::from_str(&cell_text(&result.rows[0][0])).unwrap();
    assert_eq!(doc, serde_json::json!({"a": {"b": 2}}));
}

#[test]
fn json_path_index_created_and_queried() {
    let (db, _b, _l) = test_db();
    make_docs_table(&db);
    execute_query("CREATE INDEX ON docs USING JSONPATH (body) WITH (path = 'user.address.city')", db.clone()).unwrap();
    assert!(db.indexed_column_json_path("docs", "body"));

    let result = execute_query(
        "SELECT row_key FROM json_path_lookup('docs', 'body', 'Wellington') ORDER BY row_key",
        db.clone(),
    ).unwrap();
    let mut keys: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    keys.sort();
    assert_eq!(keys, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn fts_index_created_and_searched() {
    let (db, _b, _l) = test_db();
    make_docs_table(&db);
    execute_query("CREATE INDEX ON docs USING GIN (body)", db.clone()).unwrap();
    assert!(db.indexed_column_fts("docs", "body"));

    let result = execute_query(
        "SELECT row_key FROM json_text_search('docs', 'body', 'beta') ORDER BY row_key",
        db.clone(),
    ).unwrap();
    let mut keys: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    keys.sort();
    assert_eq!(keys, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn json_schema_strict_mode_rejects_invalid_insert() {
    let (db, _b, _l) = test_db();
    execute_query(
        r#"CREATE TABLE people (id INT4, profile JSON) WITH (
            json_schema_col = 'profile',
            json_schema = '{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}',
            json_schema_mode = 'strict'
        )"#,
        db.clone(),
    ).unwrap();

    execute_query(r#"INSERT INTO people VALUES (1, '{"name":"Ada"}')"#, db.clone()).unwrap();

    let result = execute_query(r#"INSERT INTO people VALUES (2, '{"age":30}')"#, db.clone());
    let err = match result {
        Ok(_) => panic!("expected json_schema validation to reject the insert"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("json_schema validation failed"), "unexpected error: {err}");
}

#[test]
fn json_schema_off_mode_never_rejects() {
    let (db, _b, _l) = test_db();
    execute_query(
        r#"CREATE TABLE loose (id INT4, profile JSON) WITH (
            json_schema_col = 'profile',
            json_schema = '{"type":"object","required":["name"]}',
            json_schema_mode = 'off'
        )"#,
        db.clone(),
    ).unwrap();

    execute_query(r#"INSERT INTO loose VALUES (1, '{"anything":"goes"}')"#, db.clone()).unwrap();
    let result = execute_query("SELECT id FROM loose", db.clone()).unwrap();
    assert_eq!(result.rows.len(), 1);
}
