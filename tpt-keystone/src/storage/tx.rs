use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::debug;

use super::mvcc::{self, MvccStore};

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IsolationLevel {
    ReadCommitted,
    Serializable,
}

/// A transaction context.
#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: u64,
    pub snapshot_id: u64,
    pub isolation: IsolationLevel,
    pub writable: bool,
}

/// Transaction manager — handles BEGIN, COMMIT, ROLLBACK.
pub struct TransactionManager {
    mvcc: Arc<MvccStore>,
    active: Arc<Mutex<HashMap<u64, Transaction>>>,
}

impl TransactionManager {
    pub fn new(mvcc: Arc<MvccStore>) -> Self {
        Self {
            mvcc,
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Begin a new transaction.
    pub fn begin(&self, isolation: IsolationLevel) -> Transaction {
        let tx_id = mvcc::new_tx_id();
        let tx = Transaction {
            id: tx_id,
            snapshot_id: tx_id,
            isolation,
            writable: true,
        };

        let mut active = self.active.lock().unwrap();
        active.insert(tx_id, tx.clone());
        debug!(tx_id, "transaction started");
        tx
    }

    /// Begin a read-only transaction (no snapshot isolation needed).
    pub fn begin_readonly(&self) -> Transaction {
        let tx_id = mvcc::new_tx_id();
        let tx = Transaction {
            id: tx_id,
            snapshot_id: tx_id,
            isolation: IsolationLevel::ReadCommitted,
            writable: false,
        };

        let mut active = self.active.lock().unwrap();
        active.insert(tx_id, tx.clone());
        tx
    }

    /// Commit a transaction.
    pub fn commit(&self, tx: &Transaction) -> Result<()> {
        if tx.writable {
            self.mvcc.commit_tx(tx.id);
        }

        let mut active = self.active.lock().unwrap();
        active.remove(&tx.id);
        debug!(tx_id = tx.id, "transaction committed");
        Ok(())
    }

    /// Rollback a transaction.
    pub fn rollback(&self, tx: &Transaction) -> Result<()> {
        if tx.writable {
            self.mvcc.rollback_tx(tx.id);
        }

        let mut active = self.active.lock().unwrap();
        active.remove(&tx.id);
        debug!(tx_id = tx.id, "transaction rolled back");
        Ok(())
    }

    /// Get the MVCC store reference.
    pub fn mvcc(&self) -> &Arc<MvccStore> {
        &self.mvcc
    }

    /// Get the number of active transactions.
    pub fn active_count(&self) -> usize {
        self.active.lock().unwrap().len()
    }
}
