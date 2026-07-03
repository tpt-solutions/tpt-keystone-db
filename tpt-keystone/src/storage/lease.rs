//! Single-writer fencing via an object-store lease.
//!
//! Exactly one compute node may hold the write lease at a time. It is
//! acquired and renewed with compare-and-swap against a `_lease/{keyspace}`
//! object, carrying a monotonically increasing fencing token. A node whose
//! lease has lapsed (e.g. after a GC pause or network partition) is expected
//! to notice its renewal failing and demote itself to read-only *before* a
//! new writer takes over — but even if it doesn't, any manifest write it
//! attempts will be rejected because the manifest CAS will no longer match
//! (see `manifest.rs` / `lsm.rs`), so a "zombie" writer cannot corrupt state.

use super::objectstore::ObjectStore;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Lease {
    holder_id: String,
    fencing_token: u64,
    expires_at_ms: u64,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

/// Cheap, shareable view of the lease's current validity/token, read by the
/// LSM engine on every flush without needing to talk to the object store.
#[derive(Default)]
pub struct LeaseHandle {
    token: AtomicU64,
    valid: AtomicBool,
}

impl LeaseHandle {
    pub fn token(&self) -> u64 {
        self.token.load(Ordering::Acquire)
    }

    pub fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }
}

pub struct LeaseManager {
    store: Arc<dyn ObjectStore>,
    key: String,
    holder_id: String,
    ttl: Duration,
    handle: Arc<LeaseHandle>,
    current_etag: Mutex<Option<String>>,
}

impl LeaseManager {
    pub fn new(store: Arc<dyn ObjectStore>, keyspace: &str, holder_id: String, ttl: Duration) -> Self {
        Self {
            store,
            key: format!("_lease/{keyspace}"),
            holder_id,
            ttl,
            handle: Arc::new(LeaseHandle::default()),
            current_etag: Mutex::new(None),
        }
    }

    pub fn handle(&self) -> Arc<LeaseHandle> {
        self.handle.clone()
    }

    /// Attempt to become (or remain) the writer. Fails if another holder's
    /// lease is still current.
    pub fn try_acquire(&self) -> Result<()> {
        let current = self.store.get(&self.key)?;
        let (expected_etag, next_token) = match &current {
            None => (None, 1u64),
            Some((bytes, meta)) => {
                let existing: Lease = bincode::deserialize(bytes)?;
                let expired = existing.expires_at_ms < now_ms();
                let ours_already = existing.holder_id == self.holder_id;
                if !expired && !ours_already {
                    self.handle.valid.store(false, Ordering::Release);
                    bail!(
                        "lease held by \"{}\" until {} (fencing token {})",
                        existing.holder_id,
                        existing.expires_at_ms,
                        existing.fencing_token
                    );
                }
                (Some(meta.etag.clone()), existing.fencing_token + 1)
            }
        };

        let new_lease = Lease {
            holder_id: self.holder_id.clone(),
            fencing_token: next_token,
            expires_at_ms: now_ms() + self.ttl.as_millis() as u64,
        };
        let bytes = bincode::serialize(&new_lease)?;
        match self.store.put_if_match(&self.key, &bytes, expected_etag.as_deref()) {
            Ok(meta) => {
                *self.current_etag.lock().unwrap() = Some(meta.etag);
                self.handle.token.store(next_token, Ordering::Release);
                self.handle.valid.store(true, Ordering::Release);
                Ok(())
            }
            Err(e) => {
                self.handle.valid.store(false, Ordering::Release);
                Err(anyhow::anyhow!(e).context("acquiring write lease"))
            }
        }
    }

    /// Extend the currently-held lease's expiry. If we don't believe we
    /// currently hold it, this is equivalent to `try_acquire`.
    pub fn renew(&self) -> Result<()> {
        let expected_etag = self.current_etag.lock().unwrap().clone();
        let Some(expected_etag) = expected_etag else {
            return self.try_acquire();
        };
        let token = self.handle.token();
        let new_lease = Lease {
            holder_id: self.holder_id.clone(),
            fencing_token: token,
            expires_at_ms: now_ms() + self.ttl.as_millis() as u64,
        };
        let bytes = bincode::serialize(&new_lease)?;
        match self.store.put_if_match(&self.key, &bytes, Some(&expected_etag)) {
            Ok(meta) => {
                *self.current_etag.lock().unwrap() = Some(meta.etag);
                Ok(())
            }
            Err(e) => {
                self.handle.valid.store(false, Ordering::Release);
                Err(anyhow::anyhow!(e).context("renewing write lease"))
            }
        }
    }

    /// Spawn a background task that renews the lease at `ttl / 3` intervals,
    /// demoting the node (via `LeaseHandle::is_valid`) if renewal fails.
    pub fn spawn_renewal_task(self: Arc<Self>) {
        let interval = self.ttl / 3;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = self.renew() {
                    tracing::warn!(error = %e, "lease renewal failed; node demoted to read-only");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::objectstore::LocalFsObjectStore;
    use std::time::Duration;

    #[test]
    fn first_acquire_gets_token_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsObjectStore::open(dir.path()).unwrap());
        let mgr = LeaseManager::new(store, "db", "node-a".into(), Duration::from_secs(30));
        mgr.try_acquire().unwrap();
        assert_eq!(mgr.handle().token(), 1);
        assert!(mgr.handle().is_valid());
    }

    #[test]
    fn second_node_cannot_acquire_active_lease() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(dir.path()).unwrap());
        let a = LeaseManager::new(store.clone(), "db", "node-a".into(), Duration::from_secs(30));
        a.try_acquire().unwrap();

        let b = LeaseManager::new(store, "db", "node-b".into(), Duration::from_secs(30));
        assert!(b.try_acquire().is_err());
        assert!(!b.handle().is_valid());
    }

    #[test]
    fn takeover_after_expiry_bumps_fencing_token() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(dir.path()).unwrap());
        let a = LeaseManager::new(store.clone(), "db", "node-a".into(), Duration::from_millis(1));
        a.try_acquire().unwrap();
        assert_eq!(a.handle().token(), 1);

        std::thread::sleep(Duration::from_millis(10));

        let b = LeaseManager::new(store, "db", "node-b".into(), Duration::from_secs(30));
        b.try_acquire().unwrap();
        assert_eq!(b.handle().token(), 2, "fencing token must strictly increase on takeover");
    }

    #[test]
    fn renew_extends_without_changing_token() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(dir.path()).unwrap());
        let a = LeaseManager::new(store, "db", "node-a".into(), Duration::from_secs(30));
        a.try_acquire().unwrap();
        a.renew().unwrap();
        assert_eq!(a.handle().token(), 1);
        assert!(a.handle().is_valid());
    }
}
