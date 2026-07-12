//! Store-level round-trip tests for the `_tpt_privileges` catalog
//! (`wire::privileges::PrivilegeStore`), including membership-inherited
//! privilege resolution.

use std::sync::Arc;

use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};

use super::privileges::{GrantObjectRepr, PrivilegeRepr, PrivilegeStore};
use super::role_members::RoleMemberStore;

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
fn grant_and_revoke_privilege_round_trips() {
    let (db, _b, _l) = test_db();
    let store = PrivilegeStore::new(db.clone()).unwrap();

    let obj = GrantObjectRepr::table("users");
    store.grant("alice", PrivilegeRepr::Select, &obj).unwrap();
    assert!(store
        .has_privilege(&db, "alice", PrivilegeRepr::Select, &obj)
        .unwrap());
    assert!(!store
        .has_privilege(&db, "alice", PrivilegeRepr::Insert, &obj)
        .unwrap());

    store.revoke("alice", PrivilegeRepr::Select, &obj).unwrap();
    assert!(!store
        .has_privilege(&db, "alice", PrivilegeRepr::Select, &obj)
        .unwrap());
}

#[test]
fn all_privilege_satisfies_specific_checks() {
    let (db, _b, _l) = test_db();
    let store = PrivilegeStore::new(db.clone()).unwrap();

    let obj = GrantObjectRepr::database();
    store.grant("alice", PrivilegeRepr::All, &obj).unwrap();
    for p in [
        PrivilegeRepr::Select,
        PrivilegeRepr::Insert,
        PrivilegeRepr::Update,
        PrivilegeRepr::Delete,
        PrivilegeRepr::Create,
        PrivilegeRepr::Drop,
        PrivilegeRepr::Usage,
    ] {
        assert!(store.has_privilege(&db, "alice", p, &obj).unwrap());
    }
}

#[test]
fn revoke_all_drops_every_privilege_on_object() {
    let (db, _b, _l) = test_db();
    let store = PrivilegeStore::new(db.clone()).unwrap();

    let obj = GrantObjectRepr::table("users");
    store.grant("alice", PrivilegeRepr::Select, &obj).unwrap();
    store.grant("alice", PrivilegeRepr::Insert, &obj).unwrap();
    store.revoke("alice", PrivilegeRepr::All, &obj).unwrap();

    assert!(!store
        .has_privilege(&db, "alice", PrivilegeRepr::Select, &obj)
        .unwrap());
    assert!(!store
        .has_privilege(&db, "alice", PrivilegeRepr::Insert, &obj)
        .unwrap());
}

#[test]
fn membership_inherits_privileges() {
    let (db, _b, _l) = test_db();
    let store = PrivilegeStore::new(db.clone()).unwrap();
    let members = RoleMemberStore::new(db.clone()).unwrap();

    let obj = GrantObjectRepr::table("users");
    // Grant the privilege to the group, make alice a member of it.
    store.grant("managers", PrivilegeRepr::Select, &obj).unwrap();
    members.grant_membership("alice", "managers").unwrap();

    assert!(store
        .has_privilege(&db, "alice", PrivilegeRepr::Select, &obj)
        .unwrap());
    // A non-member does not inherit it.
    assert!(!store
        .has_privilege(&db, "bob", PrivilegeRepr::Select, &obj)
        .unwrap());
}
