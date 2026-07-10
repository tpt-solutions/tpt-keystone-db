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
use super::lease::LeaseManager;
use super::objectstore::{LocalFsObjectStore, ObjectStore};
use super::{ColumnDef, ColumnType, StorageEngine};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Duration;

fn node_store(bucket_dir: &std::path::Path, cache_dir: &std::path::Path) -> Arc<dyn ObjectStore> {
    let backend: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket_dir).unwrap());
    Arc::new(CachedObjectStore::new(backend, cache_dir, 64 * 1024 * 1024).unwrap())
}

fn open_writer(bucket: &std::path::Path, local: &std::path::Path, cache: &std::path::Path) -> Database {
    let store = node_store(bucket, cache);
    let lease = Arc::new(LeaseManager::new(store.clone(), "db", "chaos-writer".into(), Duration::from_secs(30)));
    lease.try_acquire().unwrap();
    Database::open(local, store, lease.handle(), NodeRole::Writer, Default::default()).unwrap()
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
            &[ColumnDef { name: "id".into(), col_type: ColumnType::Text, nullable: false, default: None, is_pk: true }],
        )
        .unwrap();

        // Every write is individually fsync'd (`Wal::append`), so the WAL
        // file's length right after each `db.write` is exactly the durable
        // boundary for that record — no need to reimplement the record
        // encoding here to compute it.
        let mut offsets = vec![std::fs::metadata(&wal_path).unwrap().len()];
        for i in 0..ROWS {
            db.write("t", format!("k{i:02}").as_bytes(), format!("v{i:02}").as_bytes()).unwrap();
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
            let mut f = std::fs::OpenOptions::new().write(true).open(&wal_path).unwrap();
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
            assert_eq!(row.key, format!("k{i:02}").into_bytes(), "seed {seed}: row {i} key mismatch");
            assert_eq!(row.value, format!("v{i:02}").into_bytes(), "seed {seed}: row {i} value corrupted");
        }

        // The recovered node isn't just "didn't panic" — it's fully usable:
        // a fresh write after recovery must succeed and be visible.
        recovered.write("t", b"after-recovery", b"still-works").unwrap();
        assert!(recovered.read("t", b"after-recovery").unwrap().is_some(), "seed {seed}: node unusable after recovery");
    }
}
