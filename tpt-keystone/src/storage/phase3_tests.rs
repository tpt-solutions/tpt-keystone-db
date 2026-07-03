//! End-to-end tests for Phase 3 (disaggregated storage, stateless compute,
//! horizontal scale-out, fencing) exercised as two in-process `Database`
//! instances sharing one `LocalFsObjectStore` root — i.e. "two compute nodes
//! share one bucket" with the bucket emulated on local disk.

use super::cache::CachedObjectStore;
use super::config::NodeRole;
use super::database::Database;
use super::lease::LeaseManager;
use super::objectstore::{LocalFsObjectStore, ObjectStore};
use super::{ColumnDef, ColumnType, StorageEngine};
use std::sync::Arc;
use std::time::Duration;

fn node_store(bucket_dir: &std::path::Path, cache_dir: &std::path::Path) -> Arc<dyn ObjectStore> {
    let backend: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket_dir).unwrap());
    Arc::new(CachedObjectStore::new(backend, cache_dir, 64 * 1024 * 1024).unwrap())
}

#[test]
fn two_nodes_share_one_bucket_consistent_reads() {
    let bucket = tempfile::tempdir().unwrap();
    let writer_local = tempfile::tempdir().unwrap();
    let writer_cache = tempfile::tempdir().unwrap();
    let reader_local = tempfile::tempdir().unwrap();
    let reader_cache = tempfile::tempdir().unwrap();

    let writer_store = node_store(bucket.path(), writer_cache.path());
    let writer_lease = Arc::new(LeaseManager::new(writer_store.clone(), "db", "writer-1".into(), Duration::from_secs(30)));
    writer_lease.try_acquire().unwrap();
    let writer_db = Database::open(writer_local.path(), writer_store, writer_lease.handle(), NodeRole::Writer).unwrap();

    writer_db
        .create_table(
            "orders",
            &[ColumnDef { name: "id".into(), col_type: ColumnType::Int4, nullable: false, default: None, is_pk: true }],
        )
        .unwrap();
    writer_db.write("orders", b"1", b"row1").unwrap();
    writer_db.write("orders", b"2", b"row2").unwrap();
    writer_db.flush().unwrap();

    // A second compute node, pointed at the same bucket, starts cold.
    let reader_store = node_store(bucket.path(), reader_cache.path());
    let reader_lease = Arc::new(LeaseManager::new(reader_store.clone(), "db", "reader-1".into(), Duration::from_secs(30)));
    let reader_db = Database::open(reader_local.path(), reader_store, reader_lease.handle(), NodeRole::Reader).unwrap();

    // It sees the table (schema is shared) and, after refreshing, the rows
    // the writer already flushed.
    assert!(reader_db.get_table("orders").unwrap().is_some());
    reader_db.refresh().unwrap();
    let mut rows = reader_db.scan("orders").unwrap();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].value, b"row1");
    assert_eq!(rows[1].value, b"row2");

    // A write flushed *after* the reader last refreshed becomes visible on
    // the next refresh — this is the "consistent reads" half of the milestone.
    writer_db.write("orders", b"3", b"row3").unwrap();
    writer_db.flush().unwrap();
    reader_db.refresh().unwrap();
    let rows_after = reader_db.scan("orders").unwrap();
    assert_eq!(rows_after.len(), 3);

    // The reader is truly read-only.
    let err = reader_db.write("orders", b"4", b"row4").unwrap_err();
    assert!(err.to_string().contains("read-only"));
    let err = reader_db.flush().unwrap_err();
    assert!(err.to_string().contains("read-only"));
}

#[test]
fn stale_writer_is_fenced_off_after_lease_takeover() {
    let bucket = tempfile::tempdir().unwrap();
    let a_local = tempfile::tempdir().unwrap();
    let a_cache = tempfile::tempdir().unwrap();
    let b_local = tempfile::tempdir().unwrap();
    let b_cache = tempfile::tempdir().unwrap();

    // Node A acquires the lease with a very short TTL and flushes once.
    let a_store = node_store(bucket.path(), a_cache.path());
    let a_lease = Arc::new(LeaseManager::new(a_store.clone(), "db", "node-a".into(), Duration::from_millis(1)));
    a_lease.try_acquire().unwrap();
    let a_db = Database::open(a_local.path(), a_store, a_lease.handle(), NodeRole::Writer).unwrap();
    a_db.create_table("t", &[ColumnDef { name: "id".into(), col_type: ColumnType::Int4, nullable: false, default: None, is_pk: true }]).unwrap();
    a_db.write("t", b"1", b"v1").unwrap();
    a_db.flush().unwrap();

    // A's lease lapses (TTL already elapsed); node B takes over with a fresh
    // lease and fencing token, and flushes its own write.
    std::thread::sleep(Duration::from_millis(10));
    let b_store = node_store(bucket.path(), b_cache.path());
    let b_lease = Arc::new(LeaseManager::new(b_store.clone(), "db", "node-b".into(), Duration::from_secs(30)));
    b_lease.try_acquire().unwrap();
    assert_eq!(b_lease.handle().token(), 2, "fencing token must have advanced on takeover");
    let b_db = Database::open(b_local.path(), b_store, b_lease.handle(), NodeRole::Writer).unwrap();
    b_db.write("t", b"2", b"v2").unwrap();
    b_db.flush().unwrap();

    // Node A — unaware it's been superseded — tries to flush again. Its
    // manifest CAS no longer matches (B already advanced it), so the write
    // is rejected: a zombie writer cannot corrupt shared state even if it
    // never notices its own lease expired.
    a_db.write("t", b"3", b"v3").unwrap();
    let err = a_db.flush().unwrap_err();
    assert!(err.to_string().contains("another writer may be active") || err.to_string().contains("conditional put failed"));
}
