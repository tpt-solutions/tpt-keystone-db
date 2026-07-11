//! Property-based (randomized differential) tests for the MVCC / transaction
//! layer — the Phase 18 follow-up "Property-based testing (`proptest`) for the
//! MVCC/transaction layer — generate randomized transaction interleavings and
//! assert isolation/durability invariants hold."
//!
//! Strategy: generate a randomized sequence of `Action`s (write / delete /
//! commit / rollback / verify) and run them against the real `MvccStore`
//! while maintaining a simple in-memory oracle that models snapshot-isolation
//! MVCC rules. After every `Verify` action we assert the store's observed
//! state (per-key reads *and* full scan) exactly matches the oracle. Because
//! the oracle only ever records *committed* transactions, this differential
//! check simultaneously proves:
//!   - commit visibility: a committed write is visible to snapshots taken
//!     after the commit,
//!   - rollback isolation: a rolled-back transaction's writes are never
//!     visible to any snapshot,
//!   - snapshot stability: an in-flight (uncommitted or later) transaction's
//!     writes don't leak into an already-taken snapshot.

use proptest::prelude::*;
use std::collections::BTreeMap;

use super::mvcc::{new_tx_id, MvccStore};

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

/// Oracle model: a log of committed mutations in tx-id order. `key -> [(tx_id,
/// Some(value) | None for delete)]`. Reads take the latest entry whose tx_id is
/// below the snapshot (i.e. the same predicate `MvccStore::read_version` uses).
struct Oracle {
    /// committed mutations in increasing tx-id order
    log: Vec<(u64, u8, Option<u8>)>,
}

impl Oracle {
    fn apply_commit(&mut self, tx_id: u64, writes: &[(u8, Option<u8>)]) {
        for (k, v) in writes {
            self.log.push((tx_id, *k, *v));
        }
    }

    fn visible(&self, snapshot: u64, key: u8) -> Option<Option<u8>> {
        let mut result: Option<Option<u8>> = None;
        for (tx_id, k, v) in &self.log {
            if *k == key && *tx_id < snapshot {
                result = Some(*v);
            }
        }
        result
    }

    fn scan(&self, snapshot: u64) -> BTreeMap<u8, u8> {
        let mut map = BTreeMap::new();
        for (tx_id, k, v) in &self.log {
            if *tx_id < snapshot {
                match v {
                    Some(val) => {
                        map.insert(*k, *val);
                    }
                    None => {
                        map.remove(k);
                    }
                }
            }
        }
        map
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn mvcc_snapshot_isolation_matches_oracle(actions in proptest::collection::vec(action_strategy(), 1..300)) {
        let store = MvccStore::new();
        let mut oracle = Oracle { log: Vec::new() };
        let mut open_tx: Option<u64> = None;
        let mut pending: Vec<(u8, Option<u8>)> = Vec::new();

        for action in &actions {
            match action {
                Action::Set(k, v) => {
                    let tx = match open_tx {
                        Some(t) => t,
                        None => {
                            let t = new_tx_id();
                            open_tx = Some(t);
                            t
                        }
                    };
                    store.write_version(vec![*k], vec![*v], tx).unwrap();
                    pending.push((*k, Some(*v)));
                }
                Action::Del(k) => {
                    let tx = match open_tx {
                        Some(t) => t,
                        None => {
                            let t = new_tx_id();
                            open_tx = Some(t);
                            t
                        }
                    };
                    store.delete_version(vec![*k], tx).unwrap();
                    pending.push((*k, None));
                }
                Action::Commit => {
                    if let Some(tx) = open_tx.take() {
                        store.commit_tx(tx);
                        oracle.apply_commit(tx, &pending);
                        pending.clear();
                    }
                }
                Action::Rollback => {
                    if let Some(tx) = open_tx.take() {
                        store.rollback_tx(tx);
                        // Rolled-back writes are intentionally NOT recorded in
                        // the oracle — that's exactly what we're checking.
                        pending.clear();
                    }
                }
                Action::Verify => {
                    let snapshot = new_tx_id();
                    for key in 0u8..4 {
                        // `oracle.visible` distinguishes "never touched" (None)
                        // from "deleted" (Some(None)); `read_version` collapses
                        // both to `None`, so flatten before comparing.
                        let expected = oracle.visible(snapshot, key).flatten();
                        let got = store
                            .read_version(&[key], snapshot)
                            .unwrap()
                            .map(|v| v[0]);
                        prop_assert_eq!(
                            got,
                            expected,
                            "snapshot {} key {}: store {:?} != oracle {:?}",
                            snapshot,
                            key,
                            got,
                            expected
                        );
                    }
                    let expected_scan = oracle.scan(snapshot);
                    let got_scan = store.scan(snapshot).unwrap();
                    let expected_vec: Vec<(u8, u8)> =
                        expected_scan.into_iter().collect();
                    let got_vec: Vec<(u8, u8)> = got_scan
                        .into_iter()
                        .map(|(k, v)| (k[0], v[0]))
                        .collect();
                    prop_assert_eq!(got_vec, expected_vec, "scan mismatch at snapshot {}", snapshot);
                }
            }
        }
    }
}
