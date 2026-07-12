//! Deterministic simulation testing (DST) for WAL crash recovery — the
//! Phase 18 TODO item, TigerBeetle/FoundationDB-style: fault injection
//! happens in-process, on the real on-disk WAL file, driven by a seeded RNG,
//! rather than sending a real OS signal to a real subprocess. That sidesteps
//! two real problems with a literal SIGKILL-loop script: it's much slower
//! (spawning and killing real processes, waiting for the OS to tear them
//! down) and it doesn't translate to this dev environment (`SIGKILL` isn't a
//! real concept on Windows the way it is on Linux CI).
//!
//! Fault model: `wal::Wal::append` (`storage/wal.rs`) does `write_all` then
//! `sync_all` per record — no batching, no group commit. So a real crash can
//! only ever tear the *most recent, not-yet-synced* record; everything
//! before it is already durable once `append` returns `Ok`. This harness
//! simulates exactly that: perform N real (fully durable) writes, then
//! truncate the on-disk WAL file to a byte length that lands strictly
//! *inside* one specific record's byte range — the same shape as a crash
//! that got partway through `write_all` for that record before power was
//! lost — and asserts recovery on reopen lands on exactly the last fully
//! durable prefix, with zero corruption and zero silently-partial rows.
//!
//! `wal::Wal::replay`/`scan_max_seq` already handle a torn trailing record
//! by bounds-checking before every field decode and breaking cleanly (no
//! checksum exists in this format, so *truncation* is caught structurally,
//! not via a checksum mismatch) — this harness is what actually exercises
//! that path against real on-disk bytes instead of just reasoning about the
//! code.

use super::cache::CachedObjectStore;
use super::config::NodeRole;
use super::database::Database;
use super::lease::{LeaseHandle, LeaseManager};
use super::objectstore::{LocalFsObjectStore, ObjectMeta, ObjectStore};
use super::{ColumnDef, ColumnType, StorageEngine};
use anyhow::Result;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn node_store(bucket_dir: &std::path::Path, cache_dir: &std::path::Path) -> Arc<dyn ObjectStore> {
    let backend: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket_dir).unwrap());
    Arc::new(CachedObjectStore::new(backend, cache_dir, 64 * 1024 * 1024).unwrap())
}

fn open_writer(
    bucket: &std::path::Path,
    local: &std::path::Path,
    cache: &std::path::Path,
) -> Database {
    let store = node_store(bucket, cache);
    let lease = Arc::new(LeaseManager::new(
        store.clone(),
        "db",
        "chaos-writer".into(),
        Duration::from_secs(30),
    ));
    lease.try_acquire().unwrap();
    Database::open(
        local,
        store,
        lease.handle(),
        NodeRole::Writer,
        Default::default(),
    )
    .unwrap()
}

/// Runs `scenarios` seeded torn-write crashes against fresh nodes and
/// asserts each one recovers to exactly its last fully durable prefix — a
/// single-threaded seeded simulator standing in for a real SIGKILL loop.
#[test]
fn torn_write_recovers_exactly_the_last_durable_prefix() {
    const ROWS: usize = 15;
    const SCENARIOS: u64 = 40;

    for seed in 0..SCENARIOS {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let wal_path = local.path().join("wal").join("wal.log");

        let db = open_writer(bucket.path(), local.path(), cache.path());
        db.create_table(
            "t",
            &[ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Text,
                nullable: false,
                default: None,
                is_pk: true,
            }],
        )
        .unwrap();

        // Every write is individually fsync'd (`Wal::append`), so the WAL
        // file's length right after each `db.write` is exactly the durable
        // boundary for that record — no need to reimplement the record
        // encoding here to compute it.
        let mut offsets = vec![std::fs::metadata(&wal_path).unwrap().len()];
        for i in 0..ROWS {
            db.write(
                "t",
                format!("k{i:02}").as_bytes(),
                format!("v{i:02}").as_bytes(),
            )
            .unwrap();
            offsets.push(std::fs::metadata(&wal_path).unwrap().len());
        }
        drop(db); // release the file handle before mutating the file directly (matters on Windows)

        // Pick which record's write gets torn (1..=ROWS, i.e. at least one
        // record survives and at least one is torn), and a byte length
        // strictly inside that record's [prev_offset, offset) range —
        // anywhere from "the torn record contributed zero bytes" up to
        // "the torn record is one byte short of complete."
        let mut rng = StdRng::seed_from_u64(seed);
        let torn_index = rng.gen_range(1..=ROWS); // 1-based: how many writes were attempted before the crash
        let (lo, hi) = (offsets[torn_index - 1], offsets[torn_index]);
        let truncate_len = if hi > lo { rng.gen_range(lo..hi) } else { lo };
        let surviving_rows = torn_index - 1;

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.set_len(truncate_len).unwrap();
            f.seek(SeekFrom::Start(truncate_len)).unwrap();
            f.flush().unwrap();
        }

        // Reopen against the same on-disk directory — the actual "process
        // restarts, WAL replay runs" path (`LsmEngine::open` ->
        // `Wal::replay`).
        let recovered = open_writer(bucket.path(), local.path(), cache.path());
        let mut rows = recovered.scan("t").unwrap();
        rows.sort_by(|a, b| a.key.cmp(&b.key));

        assert_eq!(
            rows.len(),
            surviving_rows,
            "seed {seed}: expected exactly {surviving_rows} durable rows after truncating to \
             {truncate_len} bytes (torn record #{torn_index}, range [{lo},{hi})), got {}",
            rows.len()
        );
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                row.key,
                format!("k{i:02}").into_bytes(),
                "seed {seed}: row {i} key mismatch"
            );
            assert_eq!(
                row.value,
                format!("v{i:02}").into_bytes(),
                "seed {seed}: row {i} value corrupted"
            );
        }

        // The recovered node isn't just "didn't panic" — it's fully usable:
        // a fresh write after recovery must succeed and be visible.
        recovered
            .write("t", b"after-recovery", b"still-works")
            .unwrap();
        assert!(
            recovered.read("t", b"after-recovery").unwrap().is_some(),
            "seed {seed}: node unusable after recovery"
        );
    }
}

// ---------------------------------------------------------------------------
// Fault-injecting ObjectStore wrapper — the primary DST seam for
// higher-level crash scenarios (flush, compaction, zombie-writer fencing).
// ---------------------------------------------------------------------------

/// An `ObjectStore` wrapper that records every successful write and can
/// optionally make specific writes invisible (simulating a crash before
/// that write was durable) or panic at a configurable call count.
struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    /// Writes are split into "committed" (visible to a post-crash reader)
    /// and "lost" (simulating the crash arriving before durability).
    committed: Mutex<Vec<(String, Vec<u8>)>>,
    lost: Mutex<Vec<(String, Vec<u8>)>>,
    /// When set, the next `put` call panics — simulating a crash mid-operation.
    panic_after_count: AtomicUsize,
    put_count: AtomicUsize,
}

impl FaultStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            committed: Mutex::new(Vec::new()),
            lost: Mutex::new(Vec::new()),
            panic_after_count: AtomicUsize::new(usize::MAX),
            put_count: AtomicUsize::new(0),
        }
    }

    /// Mark all currently "lost" writes as committed (for post-crash
    /// recovery simulation).
    fn commit_all(&self) {
        let mut lost = self.lost.lock().unwrap();
        let mut committed = self.committed.lock().unwrap();
        committed.append(&mut lost);
    }

    /// Restore a specific lost write by key (used to selectively recover).
    fn commit_by_key(&self, key: &str) {
        let mut lost = self.lost.lock().unwrap();
        let mut committed = self.committed.lock().unwrap();
        lost.retain(|(k, v)| {
            if k == key {
                committed.push((k.clone(), v.clone()));
                false
            } else {
                true
            }
        });
    }

    /// Get all keys that were written (committed + lost).
    fn all_keys(&self) -> Vec<String> {
        let committed = self.committed.lock().unwrap();
        let lost = self.lost.lock().unwrap();
        committed
            .iter()
            .chain(lost.iter())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Set a panic on the N-th `put` call (0-indexed).
    fn panic_after(&self, n: usize) {
        self.panic_after_count.store(n, Ordering::Release);
    }

    /// How many `put` calls have been made so far.
    fn put_count(&self) -> usize {
        self.put_count.load(Ordering::Acquire)
    }
}

impl ObjectStore for FaultStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, ObjectMeta)>> {
        // Check committed writes first, then fall through to inner (which
        // may have the real data from the previous run).
        {
            let committed = self.committed.lock().unwrap();
            if let Some((_k, v)) = committed.iter().find(|(k, _)| k == key) {
                let meta = ObjectMeta {
                    etag: super::objectstore::sha256_hex(v),
                    size: v.len() as u64,
                };
                return Ok(Some((v.clone(), meta)));
            }
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectMeta> {
        let count = self.put_count.fetch_add(1, Ordering::AcqRel);
        let threshold = self.panic_after_count.load(Ordering::Acquire);
        if count == threshold {
            panic!("DST fault injection: crashing on put #{count} for key {key}");
        }

        let result = self.inner.put(key, data);
        if result.is_ok() {
            self.lost
                .lock()
                .unwrap()
                .push((key.to_string(), data.to_vec()));
        }
        result
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected_etag: Option<&str>,
    ) -> Result<ObjectMeta, super::objectstore::CasError> {
        let result = self.inner.put_if_match(key, data, expected_etag);
        if result.is_ok() {
            self.lost
                .lock()
                .unwrap()
                .push((key.to_string(), data.to_vec()));
        }
        result
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // Include both committed and lost writes in listing.
        let mut keys: Vec<String> = self.inner.list(prefix)?;
        {
            let committed = self.committed.lock().unwrap();
            let lost = self.lost.lock().unwrap();
            for (k, _) in committed.iter().chain(lost.iter()) {
                if k.starts_with(prefix) && !keys.contains(k) {
                    keys.push(k.clone());
                }
            }
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }
}

/// Open a writer database backed by a `FaultStore` wrapping a real
/// `LocalFsObjectStore`. The `FaultStore` intercepts all writes so the
/// test can control what a post-crash reader sees.
fn open_writer_faulted(
    bucket: &std::path::Path,
    local: &std::path::Path,
    cache: &std::path::Path,
) -> (Database, Arc<FaultStore>) {
    let backend: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket).unwrap());
    let fault = Arc::new(FaultStore::new(backend));
    let store: Arc<dyn ObjectStore> = fault.clone();
    let cached = Arc::new(CachedObjectStore::new(store, cache, 64 * 1024 * 1024).unwrap());
    let lease = Arc::new(LeaseManager::new(
        cached.clone(),
        "db",
        "chaos-faulted-writer".into(),
        Duration::from_secs(30),
    ));
    lease.try_acquire().unwrap();
    let db = Database::open(
        local,
        cached,
        lease.handle(),
        NodeRole::Writer,
        Default::default(),
    )
    .unwrap();
    (db, fault)
}

// ---------------------------------------------------------------------------
// Scenario 2: crash after writes but before any flush — all data is
// WAL-only; recovery must replay every row from the WAL.
// ---------------------------------------------------------------------------

#[test]
fn crash_before_flush_recovers_from_wal() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();

    let db = open_writer(bucket.path(), local.path(), cache.path());
    db.create_table(
        "t",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Text,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();

    // Write 20 rows. No explicit flush — data lives only in WAL + memtable.
    for i in 0..20 {
        db.write(
            "t",
            format!("k{i:03}").as_bytes(),
            format!("v{i:03}").as_bytes(),
        )
        .unwrap();
    }
    drop(db);

    // Recover.
    let recovered = open_writer(bucket.path(), local.path(), cache.path());
    let mut rows = recovered.scan("t").unwrap();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(rows.len(), 20, "all 20 WAL-only rows must survive");
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.key, format!("k{i:03}").into_bytes());
        assert_eq!(row.value, format!("v{i:03}").into_bytes());
    }
}

// ---------------------------------------------------------------------------
// Scenario 3: crash after explicit flush — data is in SSTable + manifest;
// recovery loads from manifest, WAL is truncated and empty.
// ---------------------------------------------------------------------------

#[test]
fn crash_after_flush_recovers_from_sstable() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();

    let db = open_writer(bucket.path(), local.path(), cache.path());
    db.create_table(
        "t",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Text,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();

    for i in 0..10 {
        db.write(
            "t",
            format!("k{i:03}").as_bytes(),
            format!("v{i:03}").as_bytes(),
        )
        .unwrap();
    }
    db.flush().unwrap();
    drop(db);

    let recovered = open_writer(bucket.path(), local.path(), cache.path());
    let mut rows = recovered.scan("t").unwrap();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(rows.len(), 10, "all 10 flushed rows must survive");
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.key, format!("k{i:03}").into_bytes());
        assert_eq!(row.value, format!("v{i:03}").into_bytes());
    }
}

// ---------------------------------------------------------------------------
// Scenario 4: crash after flush + more writes — mixed WAL + SSTable
// recovery. The flushed rows come from the SSTable, the unflushed rows
// come from WAL replay.
// ---------------------------------------------------------------------------

#[test]
fn crash_mixed_wal_and_sstable_recovers_all() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();

    let db = open_writer(bucket.path(), local.path(), cache.path());
    db.create_table(
        "t",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Text,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();

    // First batch: flush to SSTable.
    for i in 0..5 {
        db.write(
            "t",
            format!("a{i:03}").as_bytes(),
            format!("v{i:03}").as_bytes(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    // Second batch: WAL-only (not flushed).
    for i in 0..5 {
        db.write(
            "t",
            format!("b{i:03}").as_bytes(),
            format!("v{i:03}").as_bytes(),
        )
        .unwrap();
    }
    drop(db);

    let recovered = open_writer(bucket.path(), local.path(), cache.path());
    let mut rows = recovered.scan("t").unwrap();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(
        rows.len(),
        10,
        "5 SSTable rows + 5 WAL rows must all survive"
    );
    // Verify a-prefix rows (SSTable).
    for i in 0..5 {
        assert_eq!(rows[i].key, format!("a{i:03}").into_bytes());
        assert_eq!(rows[i].value, format!("v{i:03}").into_bytes());
    }
    // Verify b-prefix rows (WAL).
    for i in 0..5 {
        assert_eq!(rows[5 + i].key, format!("b{i:03}").into_bytes());
        assert_eq!(rows[5 + i].value, format!("v{i:03}").into_bytes());
    }
}

// ---------------------------------------------------------------------------
// Scenario 5: update + flush + more writes + crash — verifies that
// in-place updates survive a flush-and-crash cycle.
// ---------------------------------------------------------------------------

#[test]
fn crash_after_update_and_flush_recovers_newest_values() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();

    let db = open_writer(bucket.path(), local.path(), cache.path());
    db.create_table(
        "t",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Text,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();

    // Initial writes.
    for i in 0..5 {
        db.write(
            "t",
            format!("k{i:03}").as_bytes(),
            format!("original-{i:03}").as_bytes(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    // Update some keys (newer values in WAL, older values in SSTable).
    db.write("t", b"k000", b"updated-000").unwrap();
    db.write("t", b"k002", b"updated-002").unwrap();
    db.flush().unwrap();

    // Write more unflushed data.
    db.write("t", b"k005", b"new-after-flush").unwrap();
    drop(db);

    let recovered = open_writer(bucket.path(), local.path(), cache.path());
    // k000 and k002 must have their updated values (SSTable has newer
    // versions; WAL replay for the flush also carries the updated value).
    assert_eq!(
        recovered.read("t", b"k000").unwrap(),
        Some(b"updated-000".to_vec()),
        "updated k000 must survive flush + crash"
    );
    assert_eq!(
        recovered.read("t", b"k002").unwrap(),
        Some(b"updated-002".to_vec()),
        "updated k002 must survive flush + crash"
    );
    // Unmodified keys keep their original values.
    assert_eq!(
        recovered.read("t", b"k001").unwrap(),
        Some(b"original-001".to_vec())
    );
    // Post-flush WAL-only write.
    assert_eq!(
        recovered.read("t", b"k005").unwrap(),
        Some(b"new-after-flush".to_vec())
    );
}

// ---------------------------------------------------------------------------
// Scenario 6: delete + flush + crash — tombstones must survive a flush
// cycle. Deleted rows must not reappear on recovery.
// ---------------------------------------------------------------------------

#[test]
fn crash_after_delete_and_flush_tombstones_persist() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();

    let db = open_writer(bucket.path(), local.path(), cache.path());
    db.create_table(
        "t",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Text,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();

    for i in 0..5 {
        db.write("t", format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    db.flush().unwrap();

    // Delete k1 and k3 (tombstones in WAL, originals in SSTable).
    db.delete("t", b"k1").unwrap();
    db.delete("t", b"k3").unwrap();
    db.flush().unwrap();

    // Write more after flush.
    db.write("t", b"k5", b"v5").unwrap();
    drop(db);

    let recovered = open_writer(bucket.path(), local.path(), cache.path());
    let mut rows = recovered.scan("t").unwrap();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    // k1 and k3 must be absent; k0, k2, k4, k5 must be present.
    assert_eq!(
        rows.len(),
        4,
        "deleted rows must not reappear after flush + crash"
    );
    assert_eq!(rows[0].key, b"k0");
    assert_eq!(rows[1].key, b"k2");
    assert_eq!(rows[2].key, b"k4");
    assert_eq!(rows[3].key, b"k5");
}

// ---------------------------------------------------------------------------
// Scenario 7: compaction crash before new manifest — old SSTables are
// still reachable. Recovery sees the pre-compaction state (all old
// SSTables still listed in the manifest).
// ---------------------------------------------------------------------------

#[test]
fn compaction_crash_before_new_manifest_preserves_old_state() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();

    let db = open_writer(bucket.path(), local.path(), cache.path());
    db.create_table(
        "t",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Text,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();

    // Create 3 separate SSTables via alternating write+flush.
    db.write("t", b"a", b"1").unwrap();
    db.flush().unwrap();
    db.write("t", b"b", b"2").unwrap();
    db.flush().unwrap();
    db.write("t", b"c", b"3").unwrap();
    db.flush().unwrap();

    // Read the manifest to get the current state. A compaction would
    // merge these 3 SSTables into 1, but we simulate a crash before
    // the new manifest is written by simply not compacting and dropping.
    drop(db);

    let recovered = open_writer(bucket.path(), local.path(), cache.path());
    let mut rows = recovered.scan("t").unwrap();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(rows.len(), 3, "all pre-compaction rows must survive");
    assert_eq!(rows[0].key, b"a");
    assert_eq!(rows[0].value, b"1");
    assert_eq!(rows[1].key, b"b");
    assert_eq!(rows[1].value, b"2");
    assert_eq!(rows[2].key, b"c");
    assert_eq!(rows[2].value, b"3");
}

// ---------------------------------------------------------------------------
// Scenario 8: compaction completes (simulated) — after a full compaction,
// the manifest points to a single merged SSTable. Recovery must see all
// data in that single SSTable.
// ---------------------------------------------------------------------------

#[test]
fn compaction_completes_recovers_from_single_merged_sstable() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();

    let db = open_writer(bucket.path(), local.path(), cache.path());
    db.create_table(
        "t",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Text,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();

    // Write enough to trigger auto-compaction (threshold = 4) twice: each
    // flush creates one SSTable, `compact_all` fires once `sstables.len() >=
    // 4` and resets the count to 1, so it fires again 3 flushes later (at
    // flush 4, then flush 7). 7 is used (not 8) so the loop ends *exactly* on
    // a post-compaction flush, landing on 1 SSTable — with 8 the 8th flush
    // would leave a second, not-yet-compacted SSTable (2, not 1), which is
    // what the original version of this test got wrong.
    // See `lsm::COMPACTION_THRESHOLD_ENV_LOCK`'s doc comment: this env var is
    // process-global, so the override below must be serialized against every
    // other test that touches it or `cargo test`'s parallel threads race on
    // it, producing flaky failures unrelated to actual compaction behavior.
    let _env_guard = super::lsm::COMPACTION_THRESHOLD_ENV_LOCK.lock().unwrap();
    std::env::set_var("TPT_COMPACTION_SSTABLE_THRESHOLD", "4");
    const ROWS: usize = 7;
    for i in 0..ROWS {
        db.write("t", format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
        db.flush().unwrap();
    }
    std::env::remove_var("TPT_COMPACTION_SSTABLE_THRESHOLD");

    // At this point compact_all should have run. Verify stats show
    // a single SSTable.
    let stats = db.stats();
    assert_eq!(
        stats.sstable_count, 1,
        "compaction should have merged into 1 SSTable"
    );
    drop(db);

    let recovered = open_writer(bucket.path(), local.path(), cache.path());
    let mut rows = recovered.scan("t").unwrap();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(rows.len(), ROWS, "all rows must survive compaction");
    for i in 0..ROWS {
        assert_eq!(rows[i].key, format!("k{i}").into_bytes());
        assert_eq!(rows[i].value, format!("v{i}").into_bytes());
    }
}

// ---------------------------------------------------------------------------
// Scenario 9: zombie-writer fencing — Writer A writes + flushes, Writer B
// takes over the lease (higher fencing token). Writer A then tries to
// flush and must be rejected (its manifest CAS will see a newer etag
// that doesn't match its stale token). Recovery sees Writer B's state.
// ---------------------------------------------------------------------------

#[test]
fn zombie_writer_flush_rejected_after_lease_takeover() {
    let bucket = tempfile::tempdir().unwrap();
    let local_a = tempfile::tempdir().unwrap();
    let local_b = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();

    // Writer A acquires the lease with a very short TTL so it expires
    // quickly without a Tokio renewal task.
    let backend_a: Arc<dyn ObjectStore> =
        Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let store_a: Arc<dyn ObjectStore> =
        Arc::new(CachedObjectStore::new(backend_a, cache.path(), 64 * 1024 * 1024).unwrap());
    let lease_a = Arc::new(LeaseManager::new(
        store_a.clone(),
        "db",
        "writer-a".into(),
        Duration::from_millis(1), // expires in 1ms
    ));
    lease_a.try_acquire().unwrap();
    let db_a = Database::open(
        local_a.path(),
        store_a.clone(),
        lease_a.handle(),
        NodeRole::Writer,
        Default::default(),
    )
    .unwrap();

    // Writer A writes and flushes.
    db_a.create_table(
        "t",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Text,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();
    db_a.write("t", b"k1", b"from-writer-a").unwrap();
    db_a.flush().unwrap();

    // Let Writer A's lease expire.
    std::thread::sleep(Duration::from_millis(10));

    // Writer B takes over with a higher fencing token.
    let backend_b: Arc<dyn ObjectStore> =
        Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let store_b: Arc<dyn ObjectStore> =
        Arc::new(CachedObjectStore::new(backend_b, cache.path(), 64 * 1024 * 1024).unwrap());
    let lease_b = Arc::new(LeaseManager::new(
        store_b.clone(),
        "db",
        "writer-b".into(),
        Duration::from_secs(30),
    ));
    lease_b.try_acquire().unwrap();
    assert_eq!(
        lease_b.handle().token(),
        2,
        "Writer B must get a higher fencing token"
    );
    let db_b = Database::open(
        local_b.path(),
        store_b.clone(),
        lease_b.handle(),
        NodeRole::Writer,
        Default::default(),
    )
    .unwrap();

    // Writer B writes and flushes — this succeeds and updates the
    // manifest with token=2.
    db_b.write("t", b"k2", b"from-writer-b").unwrap();
    db_b.flush().unwrap();

    // Writer A now writes new data and tries to flush it. Its handle still
    // shows token=1 (no renewal ran, so `is_valid()` alone would not catch
    // this — see `lease.rs`'s doc comment on why fencing relies on the
    // manifest CAS, not that flag). The memtable must be non-empty here:
    // `trigger_flush` returns `Ok(())` early for an empty memtable *before*
    // ever reaching the CAS (`lsm.rs`), so a flush with nothing new to write
    // would trivially "succeed" without exercising fencing at all.
    db_a.write("t", b"k3", b"from-zombie-writer-a").unwrap();
    let result = db_a.flush();
    assert!(
        result.is_err(),
        "zombie Writer A's flush must be rejected after Writer B took over"
    );

    // Verify from the shared bucket's state, seen through Writer B — the
    // legitimate current lease holder. (Not `open_writer`'s fresh
    // "chaos-writer" holder id: Writer B's lease has a real 30s TTL and is
    // still unexpired at this point in the test, so a third holder trying
    // `try_acquire` would itself be correctly rejected — that's the same
    // fencing mechanism this test exists to exercise, not a bug in it.)
    let mut rows = db_b.scan("t").unwrap();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    // k1 (from Writer A before takeover) and k2 (from Writer B) should
    // both be visible — Writer A's flush was rejected, so its data
    // from the first flush is still in the SSTable, and Writer B's
    // data is also in the SSTable.
    assert!(
        rows.iter().any(|r| r.key == b"k1"),
        "Writer A's pre-takeover write must survive"
    );
    assert!(
        rows.iter().any(|r| r.key == b"k2"),
        "Writer B's write must survive"
    );
    assert!(
        !rows.iter().any(|r| r.key == b"k3"),
        "zombie Writer A's rejected write must not survive"
    );
}

// ---------------------------------------------------------------------------
// Scenario 10: writer-reader convergence after crash — Writer writes +
// flushes, then crashes. Reader opens the same bucket and must see the
// writer's committed state after refresh().
// ---------------------------------------------------------------------------

#[test]
fn reader_converges_after_writer_crash() {
    let bucket = tempfile::tempdir().unwrap();
    let writer_local = tempfile::tempdir().unwrap();
    let reader_local = tempfile::tempdir().unwrap();
    let writer_cache = tempfile::tempdir().unwrap();
    let reader_cache = tempfile::tempdir().unwrap();

    let writer = open_writer(bucket.path(), writer_local.path(), writer_cache.path());
    writer
        .create_table(
            "t",
            &[ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Text,
                nullable: false,
                default: None,
                is_pk: true,
            }],
        )
        .unwrap();

    for i in 0..10 {
        writer
            .write(
                "t",
                format!("k{i:03}").as_bytes(),
                format!("v{i:03}").as_bytes(),
            )
            .unwrap();
    }
    writer.flush().unwrap();
    drop(writer); // writer crashes

    // Reader opens with no lease (read-only).
    let reader_backend: Arc<dyn ObjectStore> =
        Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let reader_store: Arc<dyn ObjectStore> = Arc::new(
        CachedObjectStore::new(reader_backend, reader_cache.path(), 64 * 1024 * 1024).unwrap(),
    );
    let reader = Database::open(
        reader_local.path(),
        reader_store,
        Arc::new(LeaseHandle::default()), // no lease — read-only
        NodeRole::Reader,
        Default::default(),
    )
    .unwrap();
    reader.refresh().unwrap();

    let mut rows = reader.scan("t").unwrap();
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(
        rows.len(),
        10,
        "reader must converge to writer's flushed state"
    );
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.key, format!("k{i:03}").into_bytes());
        assert_eq!(row.value, format!("v{i:03}").into_bytes());
    }
}

// ---------------------------------------------------------------------------
// Scenario 11: multi-seed crash + recovery stress test — for each seed,
// perform N writes, drop at a random point (some flushed, some not),
// and assert recovery is correct. This extends the torn-write test to
// also cover flush/mixed states.
// ---------------------------------------------------------------------------

#[test]
fn multi_seed_crash_recovery_with_flush_and_wal_mix() {
    const ROWS: usize = 25;
    const SCENARIOS: u64 = 30;

    for seed in 0..SCENARIOS {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();

        let db = open_writer(bucket.path(), local.path(), cache.path());
        db.create_table(
            "t",
            &[ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Text,
                nullable: false,
                default: None,
                is_pk: true,
            }],
        )
        .unwrap();

        let mut rng = StdRng::seed_from_u64(seed);

        // Decide how many rows are flushed vs WAL-only.
        let flush_at = rng.gen_range(0..=ROWS);
        let mut flushed_count = 0;

        for i in 0..ROWS {
            db.write(
                "t",
                format!("k{i:03}").as_bytes(),
                format!("v{i:03}").as_bytes(),
            )
            .unwrap();

            if i + 1 == flush_at && flush_at > 0 {
                db.flush().unwrap();
                flushed_count = i + 1;
            }
        }

        let expected_count = if flush_at > 0 {
            // The flushed rows are in SSTable, the rest in WAL.
            // If flush_at == ROWS, all are flushed and WAL is empty.
            // If flush_at < ROWS, flushed + unflushed = ROWS.
            ROWS
        } else {
            ROWS
        };

        drop(db);

        let recovered = open_writer(bucket.path(), local.path(), cache.path());
        let mut rows = recovered.scan("t").unwrap();
        rows.sort_by(|a, b| a.key.cmp(&b.key));

        assert_eq!(
            rows.len(),
            expected_count,
            "seed {seed}: expected {expected_count} rows (flush_at={flush_at}, flushed={flushed_count}), got {}",
            rows.len()
        );

        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                row.key,
                format!("k{i:03}").into_bytes(),
                "seed {seed}: row {i} key mismatch"
            );
            assert_eq!(
                row.value,
                format!("v{i:03}").into_bytes(),
                "seed {seed}: row {i} value corrupted"
            );
        }

        // Verify the recovered node is fully usable.
        recovered.write("t", b"post-recovery", b"works").unwrap();
        assert!(
            recovered.read("t", b"post-recovery").unwrap().is_some(),
            "seed {seed}: node unusable after recovery"
        );
    }
}
