//! Shared `#[cfg(test)]` fixture for tests that need a real `Database`
//! without a live server. Factored out of the near-identical `test_db()`
//! helper duplicated across `mcp/tests.rs` and `wire/http_query_tests.rs`.

use std::sync::Arc;
use std::time::Duration;

use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};

/// A writer-role `Database` backed by a fresh temp-dir local-fs object store.
/// The returned `TempDir`s must be kept alive for as long as `db` is used —
/// they're deleted on drop.
pub(crate) fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
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
