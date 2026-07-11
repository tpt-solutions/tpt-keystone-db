//! Object-store resilience decorator: a circuit breaker plus an in-flight
//! concurrency bound (memory backpressure) wrapping any `ObjectStore`.
//!
//! This is the Phase 12a follow-up "Memory-based backpressure / circuit
//! breaker for S3 latency spikes". A slow or unreachable object store would
//! otherwise surface only as a delayed query error; this layer (a) bounds how
//! many object-store operations can be in flight at once so a latency spike
//! can't pile up unbounded buffered futures / request memory, and (b) trips
//! open after a run of failures so a hard outage fails fast — and sheds load
//! on the backend — instead of every query queueing behind a dead connection
//! and timing out one by one.
//!
//! It's deliberately a thin pass-through decorator over the existing
//! `ObjectStore` trait (same "wrap the seam, don't fork the storage engine"
//! shape as `CachedObjectStore`), so it composes with both the local-fs and
//! real S3 backends and is exercised here against `LocalFsObjectStore` in the
//! unit tests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::storage::objectstore::{CasError, ObjectMeta};
use crate::metrics::Metrics;

const DEFAULT_MAX_INFLIGHT: usize = 64;
const DEFAULT_FAILURE_THRESHOLD: u64 = 5;
const DEFAULT_OPEN_FOR: Duration = Duration::from_secs(10);

fn max_inflight() -> usize {
    std::env::var("TPT_OSS_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_INFLIGHT)
        .max(1)
}

fn failure_threshold() -> u64 {
    std::env::var("TPT_OSS_CIRCUIT_FAILURES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_FAILURE_THRESHOLD)
        .max(1)
}

fn open_for() -> Duration {
    std::env::var("TPT_OSS_CIRCUIT_OPEN_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_OPEN_FOR)
}

/// A blocking, bounded in-flight counter (memory backpressure). Acquiring
/// blocks the calling thread until a slot frees, so a latency spike can't
/// accumulate unbounded concurrent object-store requests (and their buffered
/// payloads).
struct InFlightBound {
    state: Mutex<usize>,
    cond: Condvar,
    max: usize,
}

impl InFlightBound {
    fn new(max: usize) -> Self {
        Self {
            state: Mutex::new(0),
            cond: Condvar::new(),
            max,
        }
    }

    fn acquire(&self) -> InFlightGuard {
        let mut count = self.state.lock().unwrap();
        while *count >= self.max {
            count = self.cond.wait(count).unwrap();
        }
        *count += 1;
        let inflight = *count;
        drop(count);
        Metrics::global().set_object_store_inflight(inflight as u64);
        InFlightGuard { bound: self }
    }

    fn release(&self) {
        let mut count = self.state.lock().unwrap();
        *count = count.saturating_sub(1);
        let inflight = *count;
        drop(count);
        Metrics::global().set_object_store_inflight(inflight as u64);
        self.cond.notify_one();
    }
}

struct InFlightGuard<'a> {
    bound: &'a InFlightBound,
}

impl<'a> Drop for InFlightGuard<'a> {
    fn drop(&mut self) {
        self.bound.release();
    }
}

struct BreakerState {
    failures: u64,
    opened_at: Option<Instant>,
    /// When open, exactly one caller is allowed through as a probe.
    probing: bool,
}

/// Wraps an `Arc<dyn ObjectStore>` with a circuit breaker and an in-flight
/// concurrency bound. Use `GuardedObjectStore::new(store)` then pass the
/// `Arc<dyn ObjectStore>` it derefs to wherever you'd use the raw store.
pub struct GuardedObjectStore {
    inner: Arc<dyn crate::storage::objectstore::ObjectStore>,
    inflight: InFlightBound,
    breaker: Mutex<BreakerState>,
}

impl GuardedObjectStore {
    pub fn new(inner: Arc<dyn crate::storage::objectstore::ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            inflight: InFlightBound::new(max_inflight()),
            breaker: Mutex::new(BreakerState {
                failures: 0,
                opened_at: None,
                probing: false,
            }),
        })
    }

    /// Decide whether a call should be attempted now, given the breaker state.
    /// Returns `Ok(())` to proceed, or `Err` to fail fast (open and not yet in
    /// the half-open probe window).
    fn admit(&self) -> std::result::Result<(), CasError> {
        let mut b = self.breaker.lock().unwrap();
        match b.opened_at {
            None => Ok(()),
            Some(opened) => {
                if opened.elapsed() >= open_for() {
                    // Half-open: allow a single probe caller through.
                    b.probing = true;
                    b.opened_at = None;
                    Ok(())
                } else {
                    Err(CasError::Other(anyhow::anyhow!(
                        "object store circuit breaker is open (tripped); request shed"
                    )))
                }
            }
        }
    }

    fn on_success(&self) {
        let mut b = self.breaker.lock().unwrap();
        b.failures = 0;
        b.opened_at = None;
        b.probing = false;
    }

    fn on_failure(&self) {
        let mut b = self.breaker.lock().unwrap();
        // A probe that failed just re-opens; don't double-count.
        if !b.probing {
            b.failures += 1;
        }
        b.probing = false;
        if b.failures >= failure_threshold() {
            if b.opened_at.is_none() {
                Metrics::global().record_object_store_circuit_trip();
            }
            b.opened_at = Some(Instant::now());
            Metrics::global().set_object_store_circuit_open(true);
        }
    }

    /// Run an object-store op under the in-flight bound + circuit breaker. The
    /// breaker is only tripped by genuine backend failures, not by a
    /// `CasError::Conflict` (which is an expected, successful CAS round-trip —
    /// it means the object store answered just fine). The inner store already
    /// reports failures as `CasError`, so the closure just forwards them.
    fn run<T, F>(&self, op: F) -> std::result::Result<T, CasError>
    where
        F: FnOnce(&Arc<dyn crate::storage::objectstore::ObjectStore>) -> std::result::Result<T, CasError>,
    {
        let _guard = self.inflight.acquire();
        let result = match self.admit() {
            Ok(()) => op(&self.inner),
            Err(e) => Err(e),
        };
        match &result {
            Ok(_) => self.on_success(),
            Err(CasError::Conflict { .. }) => self.on_success(),
            Err(CasError::Other(_)) => self.on_failure(),
        }
        if self.breaker.lock().unwrap().opened_at.is_none() {
            Metrics::global().set_object_store_circuit_open(false);
        }
        result
    }
}

impl crate::storage::objectstore::ObjectStore for GuardedObjectStore {
    fn get(&self, key: &str) -> anyhow::Result<Option<(Vec<u8>, ObjectMeta)>> {
        self.run(|s| s.get(key).map_err(CasError::Other))
            .map_err(|e| anyhow::anyhow!(e))
    }

    fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<ObjectMeta> {
        self.run(|s| s.put(key, data).map_err(CasError::Other))
            .map_err(|e| anyhow::anyhow!(e))
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected_etag: Option<&str>,
    ) -> std::result::Result<ObjectMeta, CasError> {
        self.run(|s| s.put_if_match(key, data, expected_etag))
    }

    fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.run(|s| s.delete(key).map_err(CasError::Other))
            .map_err(|e| anyhow::anyhow!(e))
    }

    fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        self.run(|s| s.list(prefix).map_err(CasError::Other))
            .map_err(|e| anyhow::anyhow!(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::objectstore::ObjectStore;
    use anyhow::Result;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Serialize env-var mutations across the guard tests. Each test overrides
    /// `TPT_OSS_*` process-global config; without a lock, the default
    /// multi-threaded test runner races these set/remove calls between tests
    /// and produces intermittent failures (same root cause as
    /// `lsm::COMPACTION_THRESHOLD_ENV_LOCK`).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A store that fails its first `fail_times` `get`s, then succeeds.
    struct FlakyStore {
        fail_times: Mutex<usize>,
        calls: Arc<AtomicUsize>,
    }

    impl FlakyStore {
        fn new(fail_times: usize, calls: Arc<AtomicUsize>) -> Self {
            Self {
                fail_times: Mutex::new(fail_times),
                calls,
            }
        }
    }

    impl crate::storage::objectstore::ObjectStore for FlakyStore {
        fn get(&self, _key: &str) -> anyhow::Result<Option<(Vec<u8>, ObjectMeta)>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut ft = self.fail_times.lock().unwrap();
            if *ft > 0 {
                *ft -= 1;
                return Err(anyhow::anyhow!("flaky failure"));
            }
            Ok(Some((b"ok".to_vec(), ObjectMeta { etag: "e".into(), size: 2 })))
        }
        fn put(&self, _key: &str, _data: &[u8]) -> anyhow::Result<ObjectMeta> {
            Ok(ObjectMeta { etag: "e".into(), size: 0 })
        }
        fn put_if_match(
            &self,
            _key: &str,
            _data: &[u8],
            _expected_etag: Option<&str>,
        ) -> std::result::Result<ObjectMeta, CasError> {
            Ok(ObjectMeta { etag: "e".into(), size: 0 })
        }
        fn delete(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
        fn list(&self, _prefix: &str) -> anyhow::Result<Vec<String>> {
            Ok(vec![])
        }
    }

    #[test]
    fn circuit_breaker_trips_and_sheds_without_hitting_backend() {
        // Trip after a single failure and stay open for a long window so the
        // "shed" assertions don't race a half-open probe.
        let _env = ENV_LOCK.lock().unwrap();
        std::env::set_var("TPT_OSS_CIRCUIT_FAILURES", "1");
        std::env::set_var("TPT_OSS_CIRCUIT_OPEN_SECS", "60");

        let calls = Arc::new(AtomicUsize::new(0));
        let inner: Arc<dyn crate::storage::objectstore::ObjectStore> = Arc::new(FlakyStore::new(2, calls.clone()));
        let guarded = GuardedObjectStore::new(inner);

        // First call fails -> breaker opens.
        assert!(guarded.get("k").is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(Metrics::global().object_store_circuit_open.load(Ordering::Relaxed), 1);

        // While open, subsequent calls are shed without touching the backend.
        assert!(guarded.get("k").is_err());
        assert!(guarded.get("k").is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1, "backend not hit while open");

        std::env::remove_var("TPT_OSS_CIRCUIT_FAILURES");
        std::env::remove_var("TPT_OSS_CIRCUIT_OPEN_SECS");
    }

    #[test]
    fn circuit_breaker_recovers_on_half_open_probe() {
        // Zero open window -> the very next call after a trip is the probe.
        let _env = ENV_LOCK.lock().unwrap();
        std::env::set_var("TPT_OSS_CIRCUIT_FAILURES", "1");
        std::env::set_var("TPT_OSS_CIRCUIT_OPEN_SECS", "0");

        let calls = Arc::new(AtomicUsize::new(0));
        let inner: Arc<dyn crate::storage::objectstore::ObjectStore> = Arc::new(FlakyStore::new(1, calls.clone()));
        let guarded = GuardedObjectStore::new(inner);

        assert!(guarded.get("k").is_err()); // fail -> open
        assert!(guarded.get("k").is_ok()); // probe -> backend now succeeds -> closed
        assert_eq!(Metrics::global().object_store_circuit_open.load(Ordering::Relaxed), 0);
        assert!(guarded.get("k").is_ok()); // stays closed

        std::env::remove_var("TPT_OSS_CIRCUIT_FAILURES");
        std::env::remove_var("TPT_OSS_CIRCUIT_OPEN_SECS");
    }

    #[test]
    fn cas_conflict_does_not_trip_breaker() {
        let _env = ENV_LOCK.lock().unwrap();
        std::env::set_var("TPT_OSS_CIRCUIT_FAILURES", "1");
        let inner: Arc<dyn crate::storage::objectstore::ObjectStore> = Arc::new(
            crate::storage::objectstore::LocalFsObjectStore::open(tempfile::tempdir().unwrap().path())
                .unwrap(),
        );
        let guarded = GuardedObjectStore::new(inner);

        guarded.put_if_match("k", b"v1", None).unwrap();
        // A conflicting create-if-absent is a *successful* CAS round-trip.
        assert!(matches!(
            guarded.put_if_match("k", b"v2", None),
            Err(CasError::Conflict { .. })
        ));
        // And a real get still works (breaker stayed closed).
        assert!(guarded.get("k").unwrap().is_some());
        assert_eq!(Metrics::global().object_store_circuit_open.load(Ordering::Relaxed), 0);

        std::env::remove_var("TPT_OSS_CIRCUIT_FAILURES");
    }
}
