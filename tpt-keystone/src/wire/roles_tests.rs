use base64::{engine::general_purpose::STANDARD, Engine};

use super::roles::RoleStore;
use crate::storage::test_support::test_db;
use crate::storage::StorageEngine;
use crate::synapse::{encode_cells, int_cell, now_ms, text_cell};

#[test]
fn new_creates_schema_and_starts_empty() {
    let (db, _bucket, _local) = test_db();
    let store = RoleStore::new(db).unwrap();
    assert!(store.is_empty().unwrap());
}

#[test]
fn upsert_then_find_roundtrip() {
    let (db, _bucket, _local) = test_db();
    let store = RoleStore::new(db).unwrap();
    store.upsert("alice", "hunter2").unwrap();
    assert!(!store.is_empty().unwrap());

    let cred = store.find("alice").unwrap().expect("credential");
    assert_eq!(cred.iterations, 4096);
    assert_eq!(cred.salt.len(), 16);
    assert_eq!(cred.stored_key.len(), 32);
    assert_eq!(cred.server_key.len(), 32);
}

#[test]
fn find_missing_role_returns_none() {
    let (db, _bucket, _local) = test_db();
    let store = RoleStore::new(db).unwrap();
    assert!(store.find("nobody").unwrap().is_none());
}

#[test]
fn find_corrupt_stored_key_returns_err() {
    let (db, _bucket, _local) = test_db();
    let store = RoleStore::new(db.clone()).unwrap();

    // Write a row directly with a stored_key that's valid base64 but decodes
    // to the wrong length (not 32 bytes), bypassing `upsert`.
    let cells = vec![
        text_cell("bob"),
        text_cell(STANDARD.encode([0u8; 16])),
        int_cell(4096),
        text_cell(STANDARD.encode([0u8; 2])), // wrong length
        text_cell(STANDARD.encode([0u8; 32])),
        int_cell(now_ms()),
    ];
    db.write("_tpt_roles", b"bob", &encode_cells(&cells))
        .unwrap();

    let err = store.find("bob").unwrap_err();
    assert!(err.to_string().contains("corrupt stored_key"));
}

#[test]
fn find_corrupt_server_key_returns_err() {
    let (db, _bucket, _local) = test_db();
    let store = RoleStore::new(db.clone()).unwrap();

    let cells = vec![
        text_cell("carol"),
        text_cell(STANDARD.encode([0u8; 16])),
        int_cell(4096),
        text_cell(STANDARD.encode([0u8; 32])),
        text_cell(STANDARD.encode([0u8; 5])), // wrong length
        int_cell(now_ms()),
    ];
    db.write("_tpt_roles", b"carol", &encode_cells(&cells))
        .unwrap();

    let err = store.find("carol").unwrap_err();
    assert!(err.to_string().contains("corrupt server_key"));
}

#[test]
fn upsert_overwrites_existing_role() {
    let (db, _bucket, _local) = test_db();
    let store = RoleStore::new(db).unwrap();
    store.upsert("dave", "pw1").unwrap();
    let first = store.find("dave").unwrap().unwrap();

    store.upsert("dave", "pw2").unwrap();
    let second = store.find("dave").unwrap().unwrap();

    assert_ne!(first.salt, second.salt);
    assert_ne!(first.stored_key, second.stored_key);
}

#[test]
fn bootstrap_if_empty_seeds_first_role() {
    let (db, _bucket, _local) = test_db();
    let store = RoleStore::new(db).unwrap();
    assert!(store.is_empty().unwrap());

    store.bootstrap_if_empty("admin", "adminpw").unwrap();
    assert!(!store.is_empty().unwrap());
    assert!(store.find("admin").unwrap().is_some());
}

#[test]
fn bootstrap_if_empty_is_noop_when_nonempty() {
    let (db, _bucket, _local) = test_db();
    let store = RoleStore::new(db).unwrap();
    store.upsert("admin", "original").unwrap();
    let before = store.find("admin").unwrap().unwrap();

    store.bootstrap_if_empty("someone_else", "ignored").unwrap();

    assert!(store.find("someone_else").unwrap().is_none());
    let after = store.find("admin").unwrap().unwrap();
    assert_eq!(before.stored_key, after.stored_key);
}
