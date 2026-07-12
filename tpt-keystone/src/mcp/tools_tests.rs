use serde_json::json;

use super::tools::call;
use crate::storage::test_support::test_db;

fn create_users_posts(db: &std::sync::Arc<crate::storage::database::Database>) {
    crate::executor::execute_query(
        "CREATE TABLE users (id int PRIMARY KEY, name text)",
        db.clone(),
    )
    .unwrap();
    crate::executor::execute_query(
        "CREATE TABLE posts (id int PRIMARY KEY, user_id int REFERENCES users(id), title text)",
        db.clone(),
    )
    .unwrap();
    crate::executor::execute_query(
        "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')",
        db.clone(),
    )
    .unwrap();
    crate::executor::execute_query(
        "INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'hello'), (2, 1, 'world')",
        db.clone(),
    )
    .unwrap();
}

#[test]
fn tables_lists_created_tables() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    let result = call(&db, "tables", &json!({})).unwrap();
    let names: Vec<String> = result
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"users".to_string()));
    assert!(names.contains(&"posts".to_string()));
}

#[test]
fn columns_reports_schema() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    let result = call(&db, "columns", &json!({"table": "users"})).unwrap();
    let cols = result.as_array().unwrap();
    assert_eq!(cols.len(), 2);
}

#[test]
fn schema_combines_tables_and_columns() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    let result = call(&db, "schema", &json!({})).unwrap();
    let tables = result["tables"].as_array().unwrap();
    assert!(tables.iter().any(|t| t["name"] == "users"));
    let edges = result["relationship_graph"]["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
}

#[test]
fn query_rejects_non_select_show() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    let err = call(
        &db,
        "query",
        &json!({"sql": "INSERT INTO users (id, name) VALUES (3, 'x')"}),
    )
    .unwrap_err();
    assert!(err.to_string().contains("read-only"));
}

#[test]
fn query_returns_rows_columns_row_count() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    let result = call(&db, "query", &json!({"sql": "SELECT * FROM users"})).unwrap();
    assert_eq!(result["row_count"], 2);
    assert!(result["columns"].as_array().unwrap().len() >= 2);
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
}

#[test]
fn mutate_parses_insert_rows_affected_tag() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    let result = call(
        &db,
        "mutate",
        &json!({"sql": "INSERT INTO users (id, name) VALUES (3, 'carol')"}),
    )
    .unwrap();
    assert_eq!(result["rows_affected"], 1);
}

#[test]
fn mutate_parses_ddl_with_no_count_tag() {
    let (db, _bucket, _local) = test_db();
    let result = call(
        &db,
        "mutate",
        &json!({"sql": "CREATE TABLE t (id int PRIMARY KEY)"}),
    )
    .unwrap();
    // DDL tags have no trailing numeric count to parse.
    assert!(result["rows_affected"].is_null());
}

#[test]
fn explain_returns_plan_shape() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    let result = call(
        &db,
        "explain",
        &json!({"sql": "SELECT * FROM users WHERE id = 1"}),
    )
    .unwrap();
    assert_eq!(result["statement"], "select");
    assert_eq!(result["has_where"], true);
    assert_eq!(result["tables"][0], "users");
}

#[test]
fn related_nonexistent_table_returns_empty_not_error() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    let result = call(&db, "related", &json!({"table": "nope", "id": "1"})).unwrap();
    assert!(result["facts"].as_array().unwrap().is_empty());
}

#[test]
fn related_walks_fk_graph() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    let result = call(&db, "related", &json!({"table": "users", "id": "1"})).unwrap();
    let facts = result["facts"].as_array().unwrap();
    // user 1 has two posts pointing at it (incoming FK relation).
    assert!(facts.iter().any(|f| f["direction"] == "incoming"));
}

#[test]
fn related_respects_limit_clamp() {
    let (db, _bucket, _local) = test_db();
    create_users_posts(&db);
    // limit 0 clamps up to 1, limit 1000 clamps down to 100 — neither should error.
    assert!(call(
        &db,
        "related",
        &json!({"table": "users", "id": "1", "limit": 0})
    )
    .is_ok());
    assert!(call(
        &db,
        "related",
        &json!({"table": "users", "id": "1", "limit": 1000})
    )
    .is_ok());
}

#[test]
fn call_missing_required_args_returns_clear_error() {
    let (db, _bucket, _local) = test_db();
    let err = call(&db, "query", &json!({})).unwrap_err();
    assert!(err.to_string().contains("sql"));

    let err = call(&db, "columns", &json!({})).unwrap_err();
    assert!(err.to_string().contains("table"));
}

#[test]
fn call_unknown_tool_returns_error() {
    let (db, _bucket, _local) = test_db();
    let err = call(&db, "nonexistent", &json!({})).unwrap_err();
    assert!(err.to_string().contains("unknown tool"));
}
