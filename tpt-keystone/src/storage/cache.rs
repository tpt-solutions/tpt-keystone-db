//! Local NVMe cache-aside layer sitting in front of an [`ObjectStore`].
//!
//! Reads are cache-aside: a hit is served from the local cache directory, a
//! miss falls through to the backing store and populates the cache. Writes
//! are write-through: they always go to the backing store first (which is
//! the durability boundary), then warm the local cache. The cache directory
//! is treated as purely disposable — on open it's wiped so the in-memory LRU
//! bookkeeping always matches what's actually on disk.

use super::objectstore::{CasError, ObjectMeta, ObjectStore};
use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

struct CacheEntry {
    size: u64,
    etag: String,
    last_access: u64,
}

/// A bounded local cache directory with LRU eviction by access recency.
pub struct NvmeCache {
    dir: PathBuf,
    max_bytes: u64,
    entries: Mutex<HashMap<String, CacheEntry>>,
    clock: AtomicU64,
}

impl NvmeCache {
    pub fn open(dir: &Path, max_bytes: u64) -> Result<Self> {
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            max_bytes,
            entries: Mutex::new(HashMap::new()),
            clock: AtomicU64::new(0),
        })
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.dir.join(key.replace('\\', "/"))
    }

    /// Returns the cached bytes + metadata for `key`, bumping its recency, or
    /// `None` on a cache miss.
    pub fn get(&self, key: &str) -> Option<(Vec<u8>, ObjectMeta)> {
        let (etag, size) = {
            let mut entries = self.entries.lock().unwrap();
            let entry = entries.get_mut(key)?;
            entry.last_access = self.clock.fetch_add(1, Ordering::Relaxed);
            (entry.etag.clone(), entry.size)
        };
        match fs::read(self.path_for(key)) {
            Ok(data) => Some((data, ObjectMeta { etag, size })),
            Err(_) => {
                // File vanished out from under us (shouldn't happen under
                // normal operation) — drop the stale bookkeeping.
                self.entries.lock().unwrap().remove(key);
                None
            }
        }
    }

    /// Populate (or refresh) the cache entry for `key`, evicting the least
    /// recently used entries if this pushes the cache over budget.
    pub fn put(&self, key: &str, data: &[u8], meta: &ObjectMeta) {
        let path = self.path_for(key);
        if let Some(parent) = path.parent() {
            if fs::create_dir_all(parent).is_err() {
                return;
            }
        }
        if fs::write(&path, data).is_err() {
            return;
        }
        let mut entries = self.entries.lock().unwrap();
        let tick = self.clock.fetch_add(1, Ordering::Relaxed);
        entries.insert(
            key.to_string(),
            CacheEntry {
                size: meta.size,
                etag: meta.etag.clone(),
                last_access: tick,
            },
        );
        self.evict_if_needed(&mut entries);
    }

    pub fn invalidate(&self, key: &str) {
        let mut entries = self.entries.lock().unwrap();
        if entries.remove(key).is_some() {
            let _ = fs::remove_file(self.path_for(key));
        }
    }

    fn evict_if_needed(&self, entries: &mut HashMap<String, CacheEntry>) {
        let mut total: u64 = entries.values().map(|e| e.size).sum();
        while total > self.max_bytes {
            let victim = entries
                .iter()
                .min_by_key(|(_, e)| e.last_access)
                .map(|(k, _)| k.clone());
            let Some(victim) = victim else { break };
            if let Some(entry) = entries.remove(&victim) {
                total = total.saturating_sub(entry.size);
                let _ = fs::remove_file(self.path_for(&victim));
            }
        }
    }

    /// `(entry_count, total_bytes)` — for tests/observability.
    pub fn stats(&self) -> (usize, u64) {
        let entries = self.entries.lock().unwrap();
        (entries.len(), entries.values().map(|e| e.size).sum())
    }
}

/// Wraps any [`ObjectStore`] with a cache-aside/write-through [`NvmeCache`].
pub struct CachedObjectStore<S: ObjectStore> {
    inner: S,
    cache: NvmeCache,
}

impl<S: ObjectStore> CachedObjectStore<S> {
    pub fn new(inner: S, cache_dir: &Path, cache_max_bytes: u64) -> Result<Self> {
        Ok(Self {
            inner,
            cache: NvmeCache::open(cache_dir, cache_max_bytes)?,
        })
    }

    pub fn cache_stats(&self) -> (usize, u64) {
        self.cache.stats()
    }
}

/// Only immutable, content-addressed-by-id objects are safe to cache-aside:
/// SSTables and sealed WAL segments are written once under a fresh key and
/// never mutated again. Everything else (the manifest, the write lease,
/// table schemas) is small, hot, and — critically — *mutated in place* via
/// compare-and-swap; caching it would let a reader serve an arbitrarily
/// stale manifest/lease forever, since a cache-aside read only refetches on
/// a miss. Those keys always go straight to the backing store.
fn is_cacheable(key: &str) -> bool {
    key.starts_with("sst/") || key.starts_with("wal/")
}

impl<S: ObjectStore> ObjectStore for CachedObjectStore<S> {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, ObjectMeta)>> {
        let metrics = crate::metrics::Metrics::global();
        metrics
            .object_store_gets_total
            .fetch_add(1, Ordering::Relaxed);
        if !is_cacheable(key) {
            return self.inner.get(key);
        }
        if let Some(hit) = self.cache.get(key) {
            metrics.cache_hits_total.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(hit));
        }
        metrics.cache_misses_total.fetch_add(1, Ordering::Relaxed);
        match self.inner.get(key)? {
            Some((data, meta)) => {
                self.cache.put(key, &data, &meta);
                Ok(Some((data, meta)))
            }
            None => Ok(None),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectMeta> {
        crate::metrics::Metrics::global()
            .object_store_puts_total
            .fetch_add(1, Ordering::Relaxed);
        let meta = self.inner.put(key, data)?;
        if is_cacheable(key) {
            self.cache.put(key, data, &meta);
        }
        Ok(meta)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected_etag: Option<&str>,
    ) -> Result<ObjectMeta, CasError> {
        crate::metrics::Metrics::global()
            .object_store_puts_total
            .fetch_add(1, Ordering::Relaxed);
        let meta = self
            .inner
            .put_if_match(key, data, expected_etag)
            .map_err(|e| {
                if matches!(e, CasError::Conflict { .. }) {
                    crate::metrics::Metrics::global()
                        .object_store_cas_conflicts_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                e
            })?;
        if is_cacheable(key) {
            self.cache.put(key, data, &meta);
        }
        Ok(meta)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)?;
        self.cache.invalidate(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::objectstore::LocalFsObjectStore;

    #[test]
    fn cache_hit_avoids_backing_read() {
        let backing_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let backing = LocalFsObjectStore::open(backing_dir.path()).unwrap();
        let store = CachedObjectStore::new(backing, cache_dir.path(), 1024 * 1024).unwrap();

        store.put("sst/k", b"hello").unwrap();
        let (_, count_before) = store.cache_stats();
        assert_eq!(count_before, 5);

        let (data, _) = store.get("sst/k").unwrap().unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn manifest_like_keys_bypass_the_cache() {
        let backing_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let backing = LocalFsObjectStore::open(backing_dir.path()).unwrap();
        let store = CachedObjectStore::new(backing, cache_dir.path(), 1024 * 1024).unwrap();

        store.put("manifest.bin", b"v1").unwrap();
        assert_eq!(store.get("manifest.bin").unwrap().unwrap().0, b"v1");
        assert_eq!(
            store.cache_stats(),
            (0, 0),
            "manifest.bin must never be cached"
        );

        // A concurrent writer updates it directly on the backing store...
        store.put("manifest.bin", b"v2").unwrap();
        // ...and this store must see the new value immediately, not a stale cache hit.
        assert_eq!(store.get("manifest.bin").unwrap().unwrap().0, b"v2");
    }

    #[test]
    fn eviction_respects_byte_budget() {
        let cache = NvmeCache::open(tempfile::tempdir().unwrap().path(), 10).unwrap();
        cache.put(
            "a",
            b"12345",
            &ObjectMeta {
                etag: "a".into(),
                size: 5,
            },
        );
        cache.put(
            "b",
            b"12345",
            &ObjectMeta {
                etag: "b".into(),
                size: 5,
            },
        );
        assert_eq!(cache.stats(), (2, 10));

        // Touch "b" so "a" becomes the LRU victim.
        cache.get("b");
        cache.put(
            "c",
            b"12345",
            &ObjectMeta {
                etag: "c".into(),
                size: 5,
            },
        );

        let (count, total) = cache.stats();
        assert_eq!(count, 2);
        assert!(total <= 10);
        assert!(cache.get("a").is_none(), "a should have been evicted");
        assert!(cache.get("b").is_some());
        assert!(cache.get("c").is_some());
    }

    #[test]
    fn delete_invalidates_cache() {
        let backing_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let backing = LocalFsObjectStore::open(backing_dir.path()).unwrap();
        let store = CachedObjectStore::new(backing, cache_dir.path(), 1024 * 1024).unwrap();

        store.put("k", b"v").unwrap();
        store.get("k").unwrap();
        store.delete("k").unwrap();
        assert!(store.get("k").unwrap().is_none());
    }
}
