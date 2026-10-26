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

#[test]
fn create_index_if_not_exists_is_idempotent() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT)", db.clone()).unwrap();
    execute_query("CREATE INDEX ON t (name)", db.clone()).unwrap();
    // Second create of the same index must be a no-op, not an error.
    let res = execute_query("CREATE INDEX IF NOT EXISTS ON t (name)", db.clone()).unwrap();
    assert_eq!(res.tag, "CREATE INDEX");
    assert!(db.index_exists("t", "name"));
}

#[test]
fn drop_index_removes_the_index_and_its_name_resolves() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT)", db.clone()).unwrap();
    execute_query("CREATE INDEX ON t (name)", db.clone()).unwrap();
    assert!(db.indexed_column("t", "name"));

    // Default PostgreSQL name `<table>_<column>_idx`.
    execute_query("DROP INDEX t_name_idx", db.clone()).unwrap();
    assert!(!db.index_exists("t", "name"));
    assert!(!db.indexed_column("t", "name"));

    // Dropping it again fails, unless IF EXISTS is given.
    assert!(execute_query("DROP INDEX t_name_idx", db.clone()).is_err());
    execute_query("DROP INDEX IF EXISTS t_name_idx", db.clone()).unwrap();
}

#[test]
fn unknown_column_in_projection_or_predicate_is_an_error() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT)", db.clone()).unwrap();
    execute_query("INSERT INTO t VALUES (1, 'a')", db.clone()).unwrap();

    // Previously these returned NULL cells, or silently matched no rows.
    assert!(execute_query("SELECT nosuchcol FROM t", db.clone()).is_err());
    assert!(execute_query("SELECT id FROM t WHERE nosuchcol = 1", db.clone()).is_err());
    assert!(execute_query("SELECT id FROM t ORDER BY nosuchcol", db.clone()).is_err());
    assert!(execute_query("UPDATE t SET name = 'x' WHERE nosuchcol = 1", db.clone()).is_err());
    assert!(execute_query("DELETE FROM t WHERE nosuchcol = 1", db.clone()).is_err());

    // The rows were not touched by the failed UPDATE/DELETE.
    assert_eq!(db.scan("t").unwrap().len(), 1);
}

#[test]
fn order_by_output_alias_sorts_by_the_aliased_expression() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE w (id INT4 PRIMARY KEY, name TEXT)", db.clone()).unwrap();
    execute_query("INSERT INTO w VALUES (1, 'b'), (2, 'a'), (3, 'a')", db.clone()).unwrap();
    let res = execute_query(
        "SELECT name, COUNT(*) AS cnt FROM w GROUP BY name ORDER BY cnt DESC",
        db.clone(),
    )
    .unwrap();
    // 'a' has two rows and 'b' has one, so 'a' must come first.
    let first_name = res.rows[0][0].as_ref().unwrap();
    assert_eq!(first_name.as_slice(), b"a");
}

/// Regression (data loss): a table with no PRIMARY KEY used its first column as
/// the row key, so a second row with the same first value replaced the first.
#[test]
fn no_pk_rows_with_equal_first_column_are_all_kept() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE nopk (id INT4, v TEXT)", db.clone()).unwrap();
    execute_query("INSERT INTO nopk VALUES (1, 'a')", db.clone()).unwrap();
    execute_query("INSERT INTO nopk VALUES (1, 'b')", db.clone()).unwrap();
    let res = execute_query("SELECT v FROM nopk ORDER BY v", db.clone()).unwrap();
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0].as_deref(), Some(b"a".as_slice()));
    assert_eq!(res.rows[1][0].as_deref(), Some(b"b".as_slice()));
}

/// Regression: a duplicate PRIMARY KEY was silently accepted and overwrote the
/// existing row. It must be rejected, and the original row kept.
#[test]
fn duplicate_primary_key_insert_is_rejected_and_keeps_the_original() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE pk (id INT4 PRIMARY KEY, v TEXT)", db.clone()).unwrap();
    execute_query("INSERT INTO pk VALUES (1, 'original')", db.clone()).unwrap();
    let err = execute_query("INSERT INTO pk VALUES (1, 'replacement')", db.clone()).unwrap_err();
    assert!(err.to_string().contains("duplicate key value"), "{err}");
    let res = execute_query("SELECT v FROM pk WHERE id = 1", db.clone()).unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].as_deref(), Some(b"original".as_slice()));
}

/// Composite keys must not collide across different tuples that share a prefix.
#[test]
fn composite_primary_key_distinct_tuples_do_not_collide() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE ck (a TEXT, b TEXT, v TEXT, PRIMARY KEY (a, b))",
        db.clone(),
    )
    .unwrap();
    execute_query("INSERT INTO ck VALUES ('x', 'yz', '1')", db.clone()).unwrap();
    execute_query("INSERT INTO ck VALUES ('xy', 'z', '2')", db.clone()).unwrap();
    assert_eq!(db.scan("ck").unwrap().len(), 2);
}

/// A quoted literal is coerced to a numeric column's type, as in PostgreSQL.
#[test]
fn quoted_literal_compares_with_an_integer_column() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE q (id INT4 PRIMARY KEY, v TEXT)", db.clone()).unwrap();
    execute_query("INSERT INTO q VALUES (1, 'a'), (2, 'b')", db.clone()).unwrap();
    let res = execute_query("SELECT v FROM q WHERE id = '1'", db.clone()).unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].as_deref(), Some(b"a".as_slice()));
    // A non-numeric literal is an error, not a silent empty result.
    assert!(execute_query("SELECT v FROM q WHERE id = 'abc'", db.clone()).is_err());
}

/// Regression: `ST_Distance` and `ST_DWithin` measured between representative
/// points, not the shapes. Now they use the minimum distance.
#[test]
fn st_distance_to_a_line_and_into_a_polygon_is_the_minimum_distance() {
    let (db, _b, _l) = test_db();
    // The point (0.5, 0) is 1 degree of latitude (about 111,195 m) from the line y = 1.
    let res = execute_query(
        "SELECT ST_Distance(ST_MakePoint(0.5, 0), ST_GeomFromText('LINESTRING(0 1, 1 1)')) AS d",
        db.clone(),
    )
    .unwrap();
    let d: f64 = String::from_utf8(res.rows[0][0].clone().unwrap())
        .unwrap()
        .parse()
        .unwrap();
    assert!((d - 111_195.0).abs() < 100.0, "distance was {d}");

    // A point inside a polygon is at distance 0, even though the polygon's
    // representative point may be far away.
    let res = execute_query(
        "SELECT ST_Distance(ST_MakePoint(0.5, 0.5), ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')) AS d",
        db.clone(),
    )
    .unwrap();
    assert_eq!(res.rows[0][0].as_deref(), Some(b"0".as_slice()));

    let res = execute_query(
        "SELECT ST_DWithin(ST_MakePoint(0.5, 0), ST_GeomFromText('LINESTRING(0 1, 1 1)'), 200000) AS near",
        db.clone(),
    )
    .unwrap();
    assert_eq!(res.rows[0][0].as_deref(), Some(b"t".as_slice()));
}

#[test]
fn basic_numeric_functions_round_floor_ceil_sqrt_power_mod() {
    let (db, _b, _l) = test_db();
    let cell = |sql: &str| -> String {
        let res = execute_query(sql, db.clone()).unwrap();
        String::from_utf8(res.rows[0][0].clone().unwrap()).unwrap()
    };
    assert_eq!(cell("SELECT round(2.5)"), "3");
    assert_eq!(cell("SELECT round(3.14159, 2)"), "3.14");
    assert_eq!(cell("SELECT floor(2.9)"), "2");
    assert_eq!(cell("SELECT ceil(2.1)"), "3");
    assert_eq!(cell("SELECT trunc(-2.9)"), "-2");
    assert_eq!(cell("SELECT sign(-7)"), "-1");
    assert_eq!(cell("SELECT sqrt(16)"), "4");
    assert_eq!(cell("SELECT power(2, 10)"), "1024");
    assert_eq!(cell("SELECT mod(10, 3)"), "1");
    assert_eq!(cell("SELECT ROUND(1.5)"), "2");
    assert!(execute_query("SELECT mod(1, 0)", db.clone()).is_err());
}

/// Regression: a user-supplied index name was discarded, so `DROP INDEX <name>`
/// only worked with the default `<table>_<column>_idx`.
#[test]
fn index_with_custom_name_can_be_dropped_by_that_name() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE cn (id INT4 PRIMARY KEY, name TEXT)", db.clone()).unwrap();
    execute_query("CREATE INDEX by_name ON cn (name)", db.clone()).unwrap();
    assert!(db.index_exists("cn", "name"));
    // The name is taken, so a second index can't reuse it.
    assert!(execute_query("CREATE INDEX by_name ON cn (id)", db.clone()).is_err());

    execute_query("DROP INDEX by_name", db.clone()).unwrap();
    assert!(!db.index_exists("cn", "name"));
    assert!(!db.index_name_taken("by_name"));
}

/// `FROM a, b` is an implicit cross join, as in PostgreSQL. psql's `\d` uses it
/// inside a scalar subquery.
#[test]
fn comma_separated_from_list_is_a_cross_join() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE ca (x INT4)", db.clone()).unwrap();
    execute_query("CREATE TABLE cb (y INT4)", db.clone()).unwrap();
    execute_query("INSERT INTO ca VALUES (1), (2)", db.clone()).unwrap();
    execute_query("INSERT INTO cb VALUES (10), (20), (30)", db.clone()).unwrap();
    let res = execute_query("SELECT x, y FROM ca, cb", db.clone()).unwrap();
    assert_eq!(res.rows.len(), 6);
    let res = execute_query(
        "SELECT (SELECT count(*) FROM ca c, cb t WHERE c.x = 1) AS n",
        db.clone(),
    )
    .unwrap();
    assert_eq!(res.rows[0][0].as_deref(), Some(b"3".as_slice()));
}
