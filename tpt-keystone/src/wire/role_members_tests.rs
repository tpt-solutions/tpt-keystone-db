//! Store-level round-trip tests for the `_tpt_role_members` catalog
//! (`wire::role_members::RoleMemberStore`).

use std::sync::Arc;

use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::storage::config::NodeRole;

use crate::wire::role_members::RoleMemberStore;

fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
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

#[test]
fn grant_and_revoke_membership_round_trips() {
    let (db, _b, _l) = test_db();
    let store = RoleMemberStore::new(db.clone()).unwrap();

    store.grant_membership("alice", "managers").unwrap();
    assert!(store.direct_memberships("alice").unwrap().contains(&"managers".to_string()));
    assert!(store.all_memberships("alice").unwrap().contains(&"managers".to_string()));

    store.revoke_membership("alice", "managers").unwrap();
    assert!(store.direct_memberships("alice").unwrap().is_empty());
}

#[test]
fn transitive_membership_closure_is_reachable() {
    let (db, _b, _l) = test_db();
    let store = RoleMemberStore::new(db.clone()).unwrap();

    // alice -> managers -> admins
    store.grant_membership("managers", "admins").unwrap();
    store.grant_membership("alice", "managers").unwrap();

    let all = store.all_memberships("alice").unwrap();
    assert!(all.contains(&"managers".to_string()));
    assert!(all.contains(&"admins".to_string()));
    // direct only, not transitive:
    assert!(!store.direct_memberships("alice").unwrap().contains(&"admins".to_string()));
}

#[test]
fn self_membership_is_rejected() {
    let (db, _b, _l) = test_db();
    let store = RoleMemberStore::new(db.clone()).unwrap();
    assert!(store.grant_membership("alice", "alice").is_err());
}

#[test]
fn cyclic_membership_is_rejected() {
    let (db, _b, _l) = test_db();
    let store = RoleMemberStore::new(db.clone()).unwrap();

    store.grant_membership("a", "b").unwrap();
    // a -> b already; granting b -> a would close the cycle.
    assert!(store.grant_membership("b", "a").is_err());
    // granting c -> a when a -> b -> c would also cycle.
    store.grant_membership("b", "c").unwrap();
    assert!(store.grant_membership("c", "a").is_err());
}

#[test]
fn revoke_all_removes_every_edge_touching_a_role() {
    let (db, _b, _l) = test_db();
    let store = RoleMemberStore::new(db.clone()).unwrap();

    store.grant_membership("alice", "managers").unwrap();
    store.grant_membership("bob", "managers").unwrap();
    store.revoke_all("managers").unwrap();

    assert!(store.direct_memberships("alice").unwrap().is_empty());
    assert!(store.direct_memberships("bob").unwrap().is_empty());
}
