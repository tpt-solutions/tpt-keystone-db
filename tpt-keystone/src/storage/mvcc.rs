use anyhow::Result;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// A transaction ID generator.
static NEXT_TX_ID: AtomicU64 = AtomicU64::new(1);

/// Generate a new unique transaction ID.
pub fn new_tx_id() -> u64 {
    NEXT_TX_ID.fetch_add(1, Ordering::SeqCst)
}

/// A versioned entry in the MVCC store.
#[derive(Debug, Clone)]
pub struct Version {
    pub value: Vec<u8>,
    pub tx_id: u64,       // transaction that created this version
    pub is_deleted: bool, // tombstone
}

/// MVCC (Multi-Version Concurrency Control) layer.
///
/// Each key has a list of versions. A transaction sees the latest version
/// that was committed before it began (snapshot isolation).
pub struct MvccStore {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<Version>>>>,
    committed: Arc<Mutex<HashSet<u64>>>,
}

impl MvccStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
            committed: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Write a new version of a key under the given transaction.
    pub fn write_version(&self, key: Vec<u8>, value: Vec<u8>, tx_id: u64) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let versions = data.entry(key).or_insert_with(Vec::new);
        versions.push(Version {
            value,
            tx_id,
            is_deleted: false,
        });
        Ok(())
    }

    /// Mark a key as deleted under the given transaction.
    pub fn delete_version(&self, key: Vec<u8>, tx_id: u64) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let versions = data.entry(key).or_insert_with(Vec::new);
        versions.push(Version {
            value: Vec::new(),
            tx_id,
            is_deleted: true,
        });
        Ok(())
    }

    /// Read the latest committed version of a key visible to the given transaction.
    pub fn read_version(&self, key: &[u8], snapshot_tx_id: u64) -> Result<Option<Vec<u8>>> {
        let data = self.data.lock().unwrap();
        let committed = self.committed.lock().unwrap();

        if let Some(versions) = data.get(key) {
            // Find the latest version that was committed before snapshot_tx_id
            for version in versions.iter().rev() {
                if version.tx_id < snapshot_tx_id && committed.contains(&version.tx_id) {
                    if version.is_deleted {
                        return Ok(None);
                    }
                    return Ok(Some(version.value.clone()));
                }
            }
        }

        Ok(None)
    }

    /// Commit a transaction — mark its ID as committed.
    pub fn commit_tx(&self, tx_id: u64) {
        let mut committed = self.committed.lock().unwrap();
        committed.insert(tx_id);
    }

    /// Rollback a transaction — remove all versions created by it.
    pub fn rollback_tx(&self, tx_id: u64) {
        let mut data = self.data.lock().unwrap();
        data.retain(|_, versions| {
            versions.retain(|v| v.tx_id != tx_id);
            !versions.is_empty()
        });
    }

    /// Scan all visible keys for a snapshot.
    pub fn scan(&self, snapshot_tx_id: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let data = self.data.lock().unwrap();
        let committed = self.committed.lock().unwrap();
        let mut results = Vec::new();

        for (key, versions) in data.iter() {
            for version in versions.iter().rev() {
                if version.tx_id < snapshot_tx_id && committed.contains(&version.tx_id) {
                    if !version.is_deleted {
                        results.push((key.clone(), version.value.clone()));
                    }
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Get the number of unique keys.
    pub fn key_count(&self) -> usize {
        self.data.lock().unwrap().len()
    }
}
