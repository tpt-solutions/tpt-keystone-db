//! Unit tests for the shared bridge-auth helper (`wire::bridge_auth`):
//! `parse_basic_auth` decoding and `authenticate_basic`'s zero-config
//! short-circuit + Basic-auth verification path.

use std::sync::Arc;
use std::time::Duration;

use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::wire::bridge_auth::{authenticate_basic, parse_basic_auth};
use base64::Engine;
use crate::wire::roles::RoleStore;

fn test_db() -> (Arc<Database>, Arc<RoleStore>, tempfile::TempDir, tempfile::TempDir) {
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
    let roles = Arc::new(RoleStore::new(db.clone()).unwrap());
    (db, roles, bucket, local)
}

#[test]
fn parse_basic_auth_decodes_user_and_password() {
    let good = base64::engine::general_purpose::STANDARD.encode("alice:hunter2");
    let (user, pass) = parse_basic_auth(Some(&format!("Basic {good}"))).unwrap();
    assert_eq!(user, "alice");
    assert_eq!(pass, "hunter2");

    assert!(parse_basic_auth(None).is_none());
    assert!(parse_basic_auth(Some("Bearer xyz")).is_none());
    assert!(parse_basic_auth(Some("Basic not-valid-base64-!")).is_none());
}

#[test]
fn zero_config_skips_auth() {
    let (db, roles, _b, _l) = test_db();
    // No roles configured -> any request (even none) is authorized.
    let actor = authenticate_basic(&roles, &db, None).unwrap();
    assert!(actor.unrestricted);
}

#[test]
fn basic_auth_verifies_password() {
    let (db, roles, _b, _l) = test_db();
    roles.bootstrap_if_empty("alice", "hunter2").unwrap();

    // Missing header -> denied.
    assert!(authenticate_basic(&roles, &db, None).is_err());

    // Wrong password -> denied.
    let bad = base64::engine::general_purpose::STANDARD.encode("alice:wrong");
    assert!(authenticate_basic(&roles, &db, Some(&format!("Basic {bad}"))).is_err());

    // Correct password -> actor resolves to alice.
    let good = base64::engine::general_purpose::STANDARD.encode("alice:hunter2");
    let actor = authenticate_basic(&roles, &db, Some(&format!("Basic {good}"))).unwrap();
    assert_eq!(actor.rolname, "alice");
    assert!(!actor.unrestricted);
}
