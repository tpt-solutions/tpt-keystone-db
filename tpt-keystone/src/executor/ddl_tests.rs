//! Tests for DDL/catalog bug fixes (TODO.md Phase 2):
//! `CREATE SEQUENCE IF NOT EXISTS`, `DROP TABLE` (row + index purge), and
//! `ALTER TABLE ADD/DROP COLUMN` (row backfill).

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

#[test]
fn create_sequence_if_not_exists_is_idempotent() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE SEQUENCE myseq START WITH 5", db.clone()).unwrap();
    // Second creation without IF NOT EXISTS must fail.
    assert!(execute_query("CREATE SEQUENCE myseq", db.clone()).is_err());
    // With IF NOT EXISTS it is a no-op, not an error.
    execute_query("CREATE SEQUENCE IF NOT EXISTS myseq", db.clone()).unwrap();
    // Value was not reset to 1 by the no-op re-create.
    assert_eq!(db.nextval("myseq").unwrap(), 5);
}

#[test]
fn drop_table_purges_rows_and_indexes() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT)",
        db.clone(),
    )
    .unwrap();
    execute_query("CREATE INDEX ON t (name)", db.clone()).unwrap();
    execute_query("INSERT INTO t VALUES (1, 'a'), (2, 'b')", db.clone()).unwrap();

    // Rows and the index exist before DROP.
    assert_eq!(db.scan("t").unwrap().len(), 2);
    assert!(db.indexed_column("t", "name"));

    execute_query("DROP TABLE t", db.clone()).unwrap();

    // Schema gone, rows gone, B-Tree index entry gone.
    assert!(db.get_table("t").unwrap().is_none());
    assert_eq!(db.scan("t").unwrap().len(), 0);
    assert!(!db.indexed_column("t", "name"));
    assert!(!db.list_tables().unwrap().contains(&"t".to_string()));
}

#[test]
fn drop_table_if_exists_is_safe_noop() {
    let (db, _b, _l) = test_db();
    // No error on a missing table with IF EXISTS.
    execute_query("DROP TABLE IF EXISTS ghost", db.clone()).unwrap();
    // Error without IF EXISTS.
    assert!(execute_query("DROP TABLE ghost", db.clone()).is_err());
}

#[test]
fn alter_table_add_column_backfills_rows() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE t (id INT8 PRIMARY KEY)", db.clone()).unwrap();
    execute_query("INSERT INTO t VALUES (1), (2)", db.clone()).unwrap();

    execute_query(
        "ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'x'",
        db.clone(),
    )
    .unwrap();

    let schema = db.get_table("t").unwrap().unwrap();
    assert!(schema.columns.iter().any(|c| c.name == "tag"));

    // Every existing row now exposes the new column with its default.
    let rows = db.scan("t").unwrap();
    assert_eq!(rows.len(), 2);
    let res = execute_query("SELECT id, tag FROM t ORDER BY id", db.clone()).unwrap();
    let tags: Vec<String> = res
        .rows
        .iter()
        .map(|r| String::from_utf8(r[1].clone().unwrap()).unwrap())
        .collect();
    assert_eq!(tags, vec!["x".to_string(), "x".to_string()]);

    // New inserts see the column too.
    execute_query("INSERT INTO t (id, tag) VALUES (3, 'y')", db.clone()).unwrap();
    let res = execute_query("SELECT tag FROM t WHERE id = 3", db.clone()).unwrap();
    assert_eq!(String::from_utf8(res.rows[0][0].clone().unwrap()).unwrap(), "y");
}

/// Phase 1 Stage 4 (lock-free read path) accepted a narrow race window as a
/// side effect: `ALTER TABLE ADD/DROP COLUMN`/`DROP TABLE` used to
/// incidentally block every concurrent read of the table for their whole
/// rewrite duration, purely because that rewrite held the single engine-wide
/// lock that reads also needed — never a deliberate guarantee. Now that reads
/// no longer need any lock `ALTER TABLE` holds, a concurrent scan racing an
/// in-flight `ADD COLUMN` can observe a mix of old-shape and new-shape rows
/// within one scan. This test characterizes that race rather than
/// eliminating it (see `storage::database::catalog::alter_table_add_column`'s
/// doc comment): it asserts the query engine handles the interleaving
/// cleanly (no panic, no dropped/duplicated rows), not that every row is
/// seen in a specific shape.
#[test]
fn concurrent_read_during_add_column_does_not_panic_or_lose_rows() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE t (id INT8 PRIMARY KEY)", db.clone()).unwrap();

    const ROW_COUNT: i64 = 5000;
    for chunk in &(1..=ROW_COUNT).collect::<Vec<i64>>().chunks(500).collect::<Vec<_>>() {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i})")).collect();
        execute_query(
            &format!("INSERT INTO t VALUES {}", values.join(", ")),
            db.clone(),
        )
        .unwrap();
    }

    let reader_db = db.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader_stop = stop.clone();
    let reader = std::thread::spawn(move || {
        while !reader_stop.load(std::sync::atomic::Ordering::Relaxed) {
            let res = execute_query("SELECT id FROM t", reader_db.clone()).unwrap();
            assert_eq!(
                res.rows.len() as i64,
                ROW_COUNT,
                "a concurrent scan during ADD COLUMN must never lose or duplicate \
                 rows -- only their shape (old vs. new column count) may race"
            );
        }
    });

    execute_query(
        "ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'x'",
        db.clone(),
    )
    .unwrap();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    reader.join().unwrap();

    let schema = db.get_table("t").unwrap().unwrap();
    assert!(schema.columns.iter().any(|c| c.name == "tag"));
    let res = execute_query("SELECT id, tag FROM t", db.clone()).unwrap();
    assert_eq!(res.rows.len() as i64, ROW_COUNT);
}

#[test]
fn alter_table_drop_column_backfills_rows() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE t (id INT8 PRIMARY KEY, a TEXT, b TEXT)",
        db.clone(),
    )
    .unwrap();
    execute_query("INSERT INTO t VALUES (1, 'a1', 'b1'), (2, 'a2', 'b2')", db.clone()).unwrap();

    execute_query("ALTER TABLE t DROP COLUMN a", db.clone()).unwrap();

    let schema = db.get_table("t").unwrap().unwrap();
    assert!(!schema.columns.iter().any(|c| c.name == "a"));
    assert!(schema.columns.iter().any(|c| c.name == "b"));

    let res = execute_query("SELECT id, b FROM t ORDER BY id", db.clone()).unwrap();
    assert_eq!(res.rows.len(), 2);
    assert_eq!(String::from_utf8(res.rows[0][1].clone().unwrap()).unwrap(), "b1");
}

#[test]
fn alter_table_drop_column_rejects_pk_and_indexed() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT)",
        db.clone(),
    )
    .unwrap();
    execute_query("CREATE INDEX ON t (name)", db.clone()).unwrap();

    // Primary key column cannot be dropped.
    assert!(execute_query("ALTER TABLE t DROP COLUMN id", db.clone()).is_err());
    // Indexed column cannot be dropped.
    assert!(execute_query("ALTER TABLE t DROP COLUMN name", db.clone()).is_err());
}

#[test]
fn alter_table_add_not_null_without_default_is_rejected() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE t (id INT8 PRIMARY KEY)", db.clone()).unwrap();
    execute_query("INSERT INTO t VALUES (1)", db.clone()).unwrap();

    // Existing rows can't satisfy a NOT NULL column that has no default.
    assert!(
        execute_query("ALTER TABLE t ADD COLUMN tag TEXT NOT NULL", db.clone()).is_err()
    );
}
