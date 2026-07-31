//! Per-connection transaction state (Phase 1, Stage 2+3: snapshot isolation).
//!
//! A `TxnHandle` holds a staging buffer of pending writes made since
//! `BEGIN`, plus the `snapshot_seq` captured at `BEGIN` time — the MVCC
//! commit-sequence watermark (`storage::lsm::LsmEngine::current_seq`) this
//! transaction's reads are pinned to. Every `Database` read consults the
//! staging buffer first (a `None` entry is a delete tombstone, so the
//! transaction always sees its own writes), then falls through to the
//! committed store *as of `snapshot_seq`* — so an open transaction sees
//! exactly what was committed before it began, plus its own uncommitted
//! writes, and nothing committed by anyone else afterward. `COMMIT` replays
//! the staged buffer atomically (one shared `commit_seq`, via
//! `LsmEngine::write_batch`) under the LSM lock; `ROLLBACK` discards it.
//!
//! `snapshot_seq` also registers this transaction with the LSM engine's
//! open-snapshot watermark (`LsmEngine::register_snapshot`), so compaction
//! never drops a version this transaction might still need to read. That
//! registration is released on `COMMIT`/`ROLLBACK` — and, critically, on
//! `Drop` too, since a connection can disappear (crash, network error) with
//! neither ever being issued; without the `Drop` release, an abandoned
//! transaction's snapshot would pin the GC watermark forever.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::super::lsm::LsmEngine;


/// A staged write: `Some(value)` is an insert/update, `None` is a delete
/// tombstone. Keyed by the composite `table\0key` bytes `Database` uses
/// internally, so staging is table-agnostic and commit just replays entries.
pub struct StagedWrite {
    pub value: Option<Vec<u8>>,
}

/// The mutable state of one open transaction. Cheap to clone as an `Arc`
/// handle (the state itself is behind a `Mutex`), so it can be carried
/// through the executor and the wire session without lifetime pain.
pub struct TransactionState {
    pub id: u64,
    /// The MVCC commit-sequence watermark this transaction's reads are
    /// pinned to, captured once at `BEGIN`.
    pub snapshot_seq: u64,
    /// `true` once `COMMIT`/`ROLLBACK` has been issued — a handle in this
    /// state must not accept further writes (the session resets it first),
    /// and its snapshot registration has already been released.
    pub finished: bool,
    /// Pending writes keyed by composite key. `None` value = delete.
    pub staged: BTreeMap<Vec<u8>, StagedWrite>,
    /// Held so `Drop` can release this transaction's snapshot registration
    /// even if neither `COMMIT` nor `ROLLBACK` ever ran.
    lsm: Arc<LsmEngine>,
}

impl Drop for TransactionState {
    fn drop(&mut self) {
        if !self.finished {
            self.lsm.release_snapshot(self.snapshot_seq);
        }
    }
}

/// A reference-counted, shareable handle to an open transaction. Cloning is
/// shallow (shares the same underlying `Mutex<TransactionState>`); the
/// session holds the "owning" clone and passes reads of it into the
/// executor, which only stages writes through it.
#[derive(Clone)]
pub struct TxnHandle {
    pub(crate) inner: Arc<Mutex<TransactionState>>,
    id: u64,
}

impl TxnHandle {
    /// Begin a new transaction. `snapshot_seq` must already have been
    /// registered with `lsm` (`LsmEngine::register_snapshot`) by the caller
    /// (`Database::begin_txn`) before constructing this handle — the handle
    /// only owns *releasing* that registration, not creating it, so the two
    /// stay clearly paired at the one call site that opens a transaction.
    pub fn new(id: u64, snapshot_seq: u64, lsm: Arc<LsmEngine>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TransactionState {
                id,
                snapshot_seq,
                finished: false,
                staged: BTreeMap::new(),
                lsm,
            })),
            id,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// The commit-sequence watermark this transaction's reads are pinned to.
    pub fn snapshot_seq(&self) -> u64 {
        self.inner.lock().unwrap().snapshot_seq
    }

    pub fn is_finished(&self) -> bool {
        self.inner.lock().unwrap().finished
    }

    /// Stage an insert/update for `composite_key`. No-op once finished.
    pub fn stage_write(&self, composite_key: Vec<u8>, value: Vec<u8>) {
        let mut state = self.inner.lock().unwrap();
        if state.finished {
            return;
        }
        state.staged.insert(
            composite_key,
            StagedWrite {
                value: Some(value),
            },
        );
    }

    /// Stage a delete for `composite_key`. No-op once finished.
    pub fn stage_delete(&self, composite_key: Vec<u8>) {
        let mut state = self.inner.lock().unwrap();
        if state.finished {
            return;
        }
        state.staged.insert(
            composite_key,
            StagedWrite { value: None },
        );
    }

    /// Look up a staged value for `composite_key`. Returns the staged
    /// `Some(value)`, `None` meaning a tombstone (deleted in this txn), or
    /// `Some(None)`? — use the explicit enum-like tuple: `Ok(Some(...))` is a
    /// live staged value, `Ok(None)` is a tombstone, `Err(())` means "not
    /// staged here, look at the committed store".
    pub fn staged_read(&self, composite_key: &[u8]) -> Result<Option<Vec<u8>>, ()> {
        let state = self.inner.lock().unwrap();
        match state.staged.get(composite_key) {
            Some(StagedWrite { value: Some(v) }) => Ok(Some(v.clone())),
            Some(StagedWrite { value: None }) => Ok(None),
            None => Err(()),
        }
    }

    /// Take the staged writes out, marking the transaction finished and
    /// releasing its snapshot registration. Used by `COMMIT` to replay
    /// buffered writes into the committed store.
    pub fn take_staged(&self) -> Vec<(Vec<u8>, StagedWrite)> {
        let mut state = self.inner.lock().unwrap();
        self.finish_locked(&mut state);
        std::mem::take(&mut state.staged).into_iter().collect()
    }

    /// Discard all staged writes, marking the transaction finished and
    /// releasing its snapshot registration.
    pub fn discard(&self) {
        let mut state = self.inner.lock().unwrap();
        self.finish_locked(&mut state);
        state.staged.clear();
    }

    /// Shared "mark finished + release the snapshot exactly once" logic
    /// between `take_staged` and `discard`, so `Drop` sees `finished` and
    /// doesn't release a second time.
    fn finish_locked(&self, state: &mut TransactionState) {
        if state.finished {
            return;
        }
        state.finished = true;
        state.lsm.release_snapshot(state.snapshot_seq);
    }

    /// Number of pending writes (for tests/observability).
    pub fn pending_count(&self) -> usize {
        self.inner.lock().unwrap().staged.len()
    }
}
