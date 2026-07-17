//! Tests for Phase 1 read-committed transactions: per-connection `TxnHandle`
//! staged writes, atomic `COMMIT`, full `ROLLBACK`, and isolation (an open
//! transaction sees its own uncommitted writes; the committed store — and any
//! other "connection" — does not, until COMMIT replays the buffer).

use std::sync::Arc;

use crate::executor::{execute_parsed_as, QueryResult};
use crate::storage::config::NodeRole;
use crate::storage::database::txn::TxnHandle;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::storage::ColumnDef;
use crate::storage::ColumnType;

fn open_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let lease = Arc::new(LeaseManager::new(
        store.clone(),
        "db",
        "node-1".into(),
        std::time::Duration::from_secs(30),
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

fn create_t(db: &Arc<Database>) {
    db.create_table(
        "t",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Int8,
            nullable: false,
            default: None,
            is_pk: true,
            is_unique: false,
            references: None,
        }],
    )
    .unwrap();
}

fn count(db: &Arc<Database>, txn: Option<&TxnHandle>) -> usize {
    db.txn_scan(txn, "t").unwrap().len()
}

#[test]
fn autocommit_sees_immediate_writes() {
    let (db, _b, _l) = open_db();
    create_t(&db);
    execute_parsed_as(
        crate::sql::parse("INSERT INTO t VALUES (1)").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        None,
    )
    .unwrap();
    assert_eq!(count(&db, None), 1);
}

#[test]
fn commit_makes_staged_writes_visible() {
    let (db, _b, _l) = open_db();
    create_t(&db);
    let txn = db.begin_txn();
    assert_eq!(count(&db, Some(&txn)), 0);

    execute_parsed_as(
        crate::sql::parse("INSERT INTO t VALUES (1)").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        Some(&txn),
    )
    .unwrap();
    execute_parsed_as(
        crate::sql::parse("INSERT INTO t VALUES (2)").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        Some(&txn),
    )
    .unwrap();

    // Before commit, the committed store is unaffected; the txn sees its own.
    assert_eq!(count(&db, None), 0);
    assert_eq!(count(&db, Some(&txn)), 2);

    db.commit_txn(&txn).unwrap();
    // After commit, both see the rows atomically.
    assert_eq!(count(&db, None), 2);
    assert_eq!(count(&db, Some(&txn)), 2);
}

#[test]
fn rollback_discards_staged_writes() {
    let (db, _b, _l) = open_db();
    create_t(&db);
    let txn = db.begin_txn();
    execute_parsed_as(
        crate::sql::parse("INSERT INTO t VALUES (1)").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        Some(&txn),
    )
    .unwrap();
    assert_eq!(count(&db, Some(&txn)), 1);

    db.rollback_txn(&txn);
    assert_eq!(count(&db, None), 0);
    // A finished handle accepts no further writes.
    assert_eq!(count(&db, Some(&txn)), 0);
}

#[test]
fn transaction_isolation_between_connections() {
    let (db, _b, _l) = open_db();
    create_t(&db);
    let txn_a = db.begin_txn();
    let txn_b = db.begin_txn();

    execute_parsed_as(
        crate::sql::parse("INSERT INTO t VALUES (1)").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        Some(&txn_a),
    )
    .unwrap();

    // Connection B's open transaction must not see A's uncommitted write.
    assert_eq!(count(&db, Some(&txn_b)), 0);
    // Nor the committed store.
    assert_eq!(count(&db, None), 0);
    // A sees its own.
    assert_eq!(count(&db, Some(&txn_a)), 1);

    db.commit_txn(&txn_a).unwrap();
    db.rollback_txn(&txn_b);
    assert_eq!(count(&db, None), 1);
}

#[test]
fn update_and_delete_within_transaction() {
    let (db, _b, _l) = open_db();
    create_t(&db);
    execute_parsed_as(
        crate::sql::parse("INSERT INTO t VALUES (1)").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        None,
    )
    .unwrap();

    let txn = db.begin_txn();
    execute_parsed_as(
        crate::sql::parse("UPDATE t SET id = 10 WHERE id = 1").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        Some(&txn),
    )
    .unwrap();
    execute_parsed_as(
        crate::sql::parse("INSERT INTO t VALUES (2)").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        Some(&txn),
    )
    .unwrap();
    execute_parsed_as(
        crate::sql::parse("DELETE FROM t WHERE id = 10").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        Some(&txn),
    )
    .unwrap();

    // Within the txn the UPDATE'd row is gone (id changed 1->10 then deleted),
    // but the freshly inserted id=2 remains.
    assert_eq!(count(&db, Some(&txn)), 1);
    // Committed store still has the original id=1.
    assert_eq!(count(&db, None), 1);

    db.commit_txn(&txn).unwrap();
    // Net effect: original deleted, new id=2 present.
    assert_eq!(count(&db, None), 1);
    let rows = db.txn_scan(None, "t").unwrap();
    let id = String::from_utf8_lossy(&rows[0].value);
    assert!(id.contains("2"), "expected surviving row to be id=2, got {id}");
}

#[test]
fn select_sees_own_uncommitted_writes() {
    let (db, _b, _l) = open_db();
    create_t(&db);
    let txn = db.begin_txn();
    execute_parsed_as(
        crate::sql::parse("INSERT INTO t VALUES (42)").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        Some(&txn),
    )
    .unwrap();
    let result: QueryResult = execute_parsed_as(
        crate::sql::parse("SELECT * FROM t").unwrap(),
        db.clone(),
        &[],
        &crate::executor::rbac::Actor::unrestricted(),
        Some(&txn),
    )
    .unwrap();
    assert_eq!(result.rows.len(), 1);
    db.rollback_txn(&txn);
}
