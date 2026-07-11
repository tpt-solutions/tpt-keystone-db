//! Tests for the pg_dump/pg_restore compatibility primitives: column-list-
//! aware INSERT with defaults, sequences/SERIAL, UNIQUE/FOREIGN KEY
//! enforcement, and the small `pg_dump`-output compatibility odds and ends
//! (`::regclass` pass-through, `public.`-qualified DDL names).

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

#[test]
fn insert_with_column_list_fills_correct_columns_and_defaults() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE people (id int8 PRIMARY KEY, name text, active bool DEFAULT true)",
        db.clone(),
    )
    .unwrap();

    execute_query(
        "INSERT INTO people (id, name) VALUES (1, 'Alice')",
        db.clone(),
    )
    .unwrap();
    let result = execute_query("SELECT id, name, active FROM people", db.clone()).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(cell_text(&result.rows[0][0]), "1");
    assert_eq!(cell_text(&result.rows[0][1]), "Alice");
    assert_eq!(cell_text(&result.rows[0][2]), "t");
}

#[test]
fn insert_missing_not_null_column_without_default_errors() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE t (id int8 PRIMARY KEY, name text NOT NULL)",
        db.clone(),
    )
    .unwrap();
    assert!(execute_query("INSERT INTO t (id) VALUES (1)", db.clone()).is_err());
}

#[test]
fn sequence_nextval_currval_setval() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE SEQUENCE s", db.clone()).unwrap();

    let first = execute_query("SELECT nextval('s')", db.clone()).unwrap();
    assert_eq!(cell_text(&first.rows[0][0]), "1");
    let second = execute_query("SELECT nextval('s')", db.clone()).unwrap();
    assert_eq!(cell_text(&second.rows[0][0]), "2");

    let curr = execute_query("SELECT currval('s')", db.clone()).unwrap();
    assert_eq!(cell_text(&curr.rows[0][0]), "2");

    execute_query("SELECT setval('s', 100)", db.clone()).unwrap();
    let after_setval = execute_query("SELECT nextval('s')", db.clone()).unwrap();
    assert_eq!(cell_text(&after_setval.rows[0][0]), "101");
}

#[test]
fn serial_primary_key_auto_increments() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE items (id SERIAL PRIMARY KEY, name text)",
        db.clone(),
    )
    .unwrap();
    execute_query("INSERT INTO items (name) VALUES ('a')", db.clone()).unwrap();
    execute_query("INSERT INTO items (name) VALUES ('b')", db.clone()).unwrap();

    let result = execute_query("SELECT id, name FROM items ORDER BY id", db.clone()).unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(cell_text(&result.rows[0][0]), "1");
    assert_eq!(cell_text(&result.rows[1][0]), "2");
}

#[test]
fn unique_constraint_rejects_duplicate() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE u (id int8 PRIMARY KEY, email text UNIQUE)",
        db.clone(),
    )
    .unwrap();
    execute_query(
        "INSERT INTO u (id, email) VALUES (1, 'a@x.com')",
        db.clone(),
    )
    .unwrap();
    assert!(execute_query(
        "INSERT INTO u (id, email) VALUES (2, 'a@x.com')",
        db.clone()
    )
    .is_err());
    // A second NULL is fine — NULLs don't participate in uniqueness.
    execute_query("INSERT INTO u (id) VALUES (3)", db.clone()).unwrap();
    execute_query("INSERT INTO u (id) VALUES (4)", db.clone()).unwrap();
}

#[test]
fn foreign_key_enforces_referential_integrity() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE parent (id int8 PRIMARY KEY)", db.clone()).unwrap();
    execute_query(
        "CREATE TABLE child (id int8 PRIMARY KEY, parent_id int8 REFERENCES parent(id))",
        db.clone(),
    )
    .unwrap();

    // No matching parent row: rejected.
    assert!(execute_query(
        "INSERT INTO child (id, parent_id) VALUES (1, 99)",
        db.clone()
    )
    .is_err());

    // Matching parent row: accepted.
    execute_query("INSERT INTO parent (id) VALUES (99)", db.clone()).unwrap();
    execute_query(
        "INSERT INTO child (id, parent_id) VALUES (1, 99)",
        db.clone(),
    )
    .unwrap();

    // NULL FK value is exempt.
    execute_query("INSERT INTO child (id) VALUES (2)", db.clone()).unwrap();
}

#[test]
fn table_level_unique_and_foreign_key_constraints() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE parent (id int8 PRIMARY KEY)", db.clone()).unwrap();
    execute_query("INSERT INTO parent (id) VALUES (1)", db.clone()).unwrap();
    execute_query(
        "CREATE TABLE child (id int8, parent_id int8, UNIQUE (id), FOREIGN KEY (parent_id) REFERENCES parent(id))",
        db.clone(),
    ).unwrap();

    execute_query(
        "INSERT INTO child (id, parent_id) VALUES (1, 1)",
        db.clone(),
    )
    .unwrap();
    assert!(execute_query(
        "INSERT INTO child (id, parent_id) VALUES (1, 1)",
        db.clone()
    )
    .is_err());
    assert!(execute_query(
        "INSERT INTO child (id, parent_id) VALUES (2, 5)",
        db.clone()
    )
    .is_err());
}

#[test]
fn pg_constraint_and_pg_sequence_reflect_created_objects() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE parent (id int8 PRIMARY KEY)", db.clone()).unwrap();
    execute_query(
        "CREATE TABLE child (id int8 PRIMARY KEY, parent_id int8 REFERENCES parent(id), email text UNIQUE)",
        db.clone(),
    ).unwrap();
    execute_query("CREATE SEQUENCE my_seq START WITH 5", db.clone()).unwrap();

    let constraints =
        execute_query("SELECT contype FROM pg_catalog.pg_constraint", db.clone()).unwrap();
    let types: Vec<String> = constraints.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert!(types.contains(&"p".to_string()));
    assert!(types.contains(&"u".to_string()));
    assert!(types.contains(&"f".to_string()));

    let sequences =
        execute_query("SELECT seqname FROM pg_catalog.pg_sequence", db.clone()).unwrap();
    let names: Vec<String> = sequences.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert!(names.contains(&"my_seq".to_string()));
}

#[test]
fn alter_table_set_default_applies_to_future_inserts() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE t (id int8 PRIMARY KEY, status text)",
        db.clone(),
    )
    .unwrap();
    execute_query(
        "ALTER TABLE t ALTER COLUMN status SET DEFAULT 'pending'",
        db.clone(),
    )
    .unwrap();
    execute_query("INSERT INTO t (id) VALUES (1)", db.clone()).unwrap();
    let result = execute_query("SELECT status FROM t", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "pending");
}

#[test]
fn regclass_cast_passes_through_as_text() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE SEQUENCE s2", db.clone()).unwrap();
    let result = execute_query("SELECT nextval('s2'::regclass)", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "1");
}

#[test]
fn schema_qualified_ddl_and_dml_names_strip_public_prefix() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE public.widgets (id int8 PRIMARY KEY)",
        db.clone(),
    )
    .unwrap();
    execute_query("INSERT INTO public.widgets (id) VALUES (1)", db.clone()).unwrap();
    let result = execute_query("SELECT id FROM widgets", db.clone()).unwrap();
    assert_eq!(result.rows.len(), 1);
    let result_qualified = execute_query("SELECT id FROM public.widgets", db.clone()).unwrap();
    assert_eq!(result_qualified.rows.len(), 1);
}
