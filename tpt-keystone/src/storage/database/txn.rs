//! Per-connection transaction state (Phase 1, Stage 1: read-committed).
//!
//! A `TxnHandle` holds a staging buffer of pending writes made since
//! `BEGIN`. Every `Database` read consults the staging buffer first (a
//! `None` entry is a delete tombstone), then falls through to the committed
//! LSM state — so an open transaction sees its own uncommitted writes but
//! not those of any other transaction, and other connections never see a
//! transaction's writes until `COMMIT` flushes the buffer atomically under
//! the LSM lock. `ROLLBACK` simply discards the buffer.
//!
//! This is read-committed isolation: there is no snapshot taken at `BEGIN`
//! (a transaction sees committed writes made by others after it began), and
//! no true MVCC version chain. Snapshot isolation (Stage 2) and versioned
//! storage with background GC (Stage 3) are tracked as follow-ups; the
//! `mvcc.rs`/`tx.rs` versioned design is reused by the commit path's
//! bookkeeping but the committed store remains the single-version LSM.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

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
    /// `true` once `COMMIT`/`ROLLBACK` has been issued — a handle in this
    /// state must not accept further writes (the session resets it first).
    pub finished: bool,
    /// Pending writes keyed by composite key. `None` value = delete.
    pub staged: BTreeMap<Vec<u8>, StagedWrite>,
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
    pub fn new(id: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TransactionState {
                id,
                finished: false,
                staged: BTreeMap::new(),
            })),
            id,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
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

    /// Take the staged writes out, marking the transaction finished. Used by
    /// `COMMIT` to replay buffered writes into the committed store.
    pub fn take_staged(&self) -> Vec<(Vec<u8>, StagedWrite)> {
        let mut state = self.inner.lock().unwrap();
        state.finished = true;
        std::mem::take(&mut state.staged)
            .into_iter()
            .collect()
    }

    /// Discard all staged writes, marking the transaction finished.
    pub fn discard(&self) {
        let mut state = self.inner.lock().unwrap();
        state.finished = true;
        state.staged.clear();
    }

    /// Number of pending writes (for tests/observability).
    pub fn pending_count(&self) -> usize {
        self.inner.lock().unwrap().staged.len()
    }
}
