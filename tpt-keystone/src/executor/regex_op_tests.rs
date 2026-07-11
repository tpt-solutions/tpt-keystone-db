//! Tests for the POSIX regex infix operators `~` / `!~` / `~*` / `!~*`
//! (Phase 18 pg_catalog / psql meta-command coverage follow-up). These are
//! real Postgres-compatible pattern-matching operators that psql's `\dt` /
//! `\d` introspection queries rely on (`c.relname !~ '^pg_'`, etc.).

use std::sync::Arc;
use std::time::Duration;

use super::execute_query;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::storage::{ColumnDef, ColumnType, StorageEngine};

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

/// Evaluate a single-scalar SELECT and return its text, or `None` if NULL.
fn scalar(db: &Arc<Database>, sql: &str) -> Option<String> {
    let res = execute_query(sql, db.clone()).unwrap();
    assert_eq!(res.rows.len(), 1, "expected exactly one row for: {sql}");
    res.rows[0][0].as_ref().map(|b| String::from_utf8(b.clone()).unwrap())
}

#[test]
fn tilde_matches_positive_and_negative() {
    let (db, _b, _l) = test_db();
    assert_eq!(scalar(&db, "SELECT 'foobar' ~ '^foo'"), Some("t".into()));
    assert_eq!(scalar(&db, "SELECT 'foobar' ~ '^baz'"), Some("f".into()));
}

#[test]
fn not_tilde_negates_match() {
    let (db, _b, _l) = test_db();
    // `!~` is true when the pattern does NOT match.
    assert_eq!(scalar(&db, "SELECT 'foobar' !~ '^baz'"), Some("t".into()));
    assert_eq!(scalar(&db, "SELECT 'foobar' !~ '^foo'"), Some("f".into()));
}

#[test]
fn tilde_star_is_case_insensitive() {
    let (db, _b, _l) = test_db();
    assert_eq!(scalar(&db, "SELECT 'FOOBAR' ~* '^foo'"), Some("t".into()));
    assert_eq!(scalar(&db, "SELECT 'FOOBAR' ~ '^foo'"), Some("f".into()));
}

#[test]
fn not_tilde_star_is_case_insensitive_negation() {
    let (db, _b, _l) = test_db();
    assert_eq!(scalar(&db, "SELECT 'FOOBAR' !~* '^baz'"), Some("t".into()));
    assert_eq!(scalar(&db, "SELECT 'FOOBAR' !~* '^foo'"), Some("f".into()));
}

#[test]
fn regex_null_operand_yields_null() {
    let (db, _b, _l) = test_db();
    assert_eq!(scalar(&db, "SELECT 'x' ~ NULL"), None);
    assert_eq!(scalar(&db, "SELECT NULL ~ '^x'"), None);
}

#[test]
fn invalid_regex_is_an_error() {
    let (db, _b, _l) = test_db();
    assert!(execute_query("SELECT 'x' ~ '('", db.clone()).is_err());
}

#[test]
fn regex_filters_a_real_table() {
    let (db, _b, _l) = test_db();
    db.create_table(
        "names",
        &[
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int4,
                nullable: false,
                default: None,
                is_pk: true,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: true,
                default: None,
                is_pk: false,
            },
        ],
    )
    .unwrap();
    for (id, name) in [("1", "alpha"), ("2", "beta"), ("3", "gamma")] {
        execute_query(
            &format!("INSERT INTO names (id, name) VALUES ({id}, '{name}')"),
            db.clone(),
        )
        .unwrap();
    }

    let res = execute_query(
        "SELECT name FROM names WHERE name ~ '^a' ORDER BY id",
        db.clone(),
    )
    .unwrap();
    let matched: Vec<String> = res
        .rows
        .iter()
        .map(|r| String::from_utf8(r[0].clone().unwrap()).unwrap())
        .collect();
    assert_eq!(matched, vec!["alpha".to_string()]);

    // `!~` finds the rows the pattern does NOT match.
    let res2 = execute_query(
        "SELECT name FROM names WHERE name !~ '^a' ORDER BY id",
        db.clone(),
    )
    .unwrap();
    let rest: Vec<String> = res2
        .rows
        .iter()
        .map(|r| String::from_utf8(r[0].clone().unwrap()).unwrap())
        .collect();
    assert_eq!(rest, vec!["beta".to_string(), "gamma".to_string()]);
}

