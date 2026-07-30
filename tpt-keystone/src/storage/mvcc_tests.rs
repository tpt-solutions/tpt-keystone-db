//! Property-based (randomized differential) tests for the transaction layer
//! — the Phase 18 follow-up "Property-based testing (`proptest`) for the
//! MVCC/transaction layer — generate randomized transaction interleavings and
//! assert isolation/durability invariants hold."
//!
//! This used to drive `storage::mvcc::MvccStore` directly, but that store
//! was a RAM-only prototype never wired into any read/write path (see
//! `docs/mvcc_snapshot_audit.md`) and has since been replaced by durable
//! multi-version storage in `LsmEngine` (`storage::internal_key`). This test
//! is ported to drive the real, end-to-end transaction path instead:
//! `Database::begin_txn`/`txn_write`/`txn_delete`/`commit_txn`/
//! `rollback_txn`/`txn_read`/`txn_scan` — exercising the raw KV layer
//! directly (no SQL table needed: these methods work against composite
//! `table\0key` bytes regardless of whether a schema exists for `table`).
//!
//! Strategy: generate a randomized sequence of `Action`s (write / delete /
//! commit / rollback / verify) against a single open transaction at a time,
//! and after every `Verify` action assert the real engine's observed state —
//! both point reads and a full scan, both read with no open transaction
//! (i.e. "the latest committed state") — exactly matches a simple oracle
//! that only ever records committed mutations. This differentially proves:
//!   - commit visibility: a committed write is visible afterward,
//!   - rollback isolation: a rolled-back transaction's writes are never
//!     visible,
//!   - last-committer-wins ordering: a later commit's value for a key
//!     supersedes an earlier one.
//!
//! Case/action counts are deliberately much smaller than the old in-memory
//! version's (200 cases x up to 300 actions): every `Commit` here does a
//! real WAL fsync and real object-store writes through a fresh `Database`,
//! not an in-memory map, so the same coverage would be prohibitively slow.

use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::storage::config::NodeRole;
use crate::storage::database::txn::TxnHandle;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};

#[derive(Clone, Debug)]
enum Action {
    Set(u8, u8),
    Del(u8),
    Commit,
    Rollback,
    Verify,
}

fn action_strategy() -> impl Strategy<Value = Action> {
    prop_oneof![
        (0u8..4, 0u8..16).prop_map(|(k, v)| Action::Set(k, v)),
        (0u8..4).prop_map(Action::Del),
        Just(Action::Commit),
        Just(Action::Rollback),
        Just(Action::Verify),
    ]
}

/// Oracle model: the latest committed value per key. `None` = deleted.
/// Absence from the map = never written.
struct Oracle {
    committed: BTreeMap<u8, Option<u8>>,
}

impl Oracle {
    fn apply_commit(&mut self, writes: &[(u8, Option<u8>)]) {
        for (k, v) in writes {
            self.committed.insert(*k, *v);
        }
    }

    fn scan(&self) -> BTreeMap<u8, u8> {
        self.committed
            .iter()
            .filter_map(|(k, v)| v.map(|val| (*k, val)))
            .collect()
    }
}

fn open_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let lease = Arc::new(LeaseManager::new(
        store.clone(),
        "db",
        "mvcc-proptest".into(),
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    #[test]
    fn transaction_commit_rollback_matches_oracle(actions in proptest::collection::vec(action_strategy(), 1..40)) {
        let (db, _bucket, _local) = open_db();
        let mut oracle = Oracle { committed: BTreeMap::new() };
        let mut open_txn: Option<TxnHandle> = None;
        let mut pending: Vec<(u8, Option<u8>)> = Vec::new();

        for action in &actions {
            match action {
                Action::Set(k, v) => {
                    let txn = open_txn.get_or_insert_with(|| db.begin_txn());
                    db.txn_write(Some(txn), "t", &[*k], &[*v]).unwrap();
                    pending.push((*k, Some(*v)));
                }
                Action::Del(k) => {
                    let txn = open_txn.get_or_insert_with(|| db.begin_txn());
                    db.txn_delete(Some(txn), "t", &[*k]).unwrap();
                    pending.push((*k, None));
                }
                Action::Commit => {
                    if let Some(txn) = open_txn.take() {
                        db.commit_txn(&txn).unwrap();
                        oracle.apply_commit(&pending);
                        pending.clear();
                    }
                }
                Action::Rollback => {
                    if let Some(txn) = open_txn.take() {
                        db.rollback_txn(&txn);
                        // Rolled-back writes are intentionally NOT recorded
                        // in the oracle — that's exactly what we're
                        // checking.
                        pending.clear();
                    }
                }
                Action::Verify => {
                    for key in 0u8..4 {
                        let expected = oracle.committed.get(&key).copied().flatten();
                        let got = db.txn_read(None, "t", &[key]).unwrap().map(|v| v[0]);
                        prop_assert_eq!(
                            got, expected,
                            "key {}: db {:?} != oracle {:?}", key, got, expected
                        );
                    }
                    let expected_scan = oracle.scan();
                    let got_scan: BTreeMap<u8, u8> = db
                        .txn_scan(None, "t")
                        .unwrap()
                        .into_iter()
                        .map(|kv| (kv.key[0], kv.value[0]))
                        .collect();
                    prop_assert_eq!(got_scan, expected_scan, "scan mismatch");
                }
            }
        }
    }
}
