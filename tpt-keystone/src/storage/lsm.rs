use anyhow::{bail, Result};
use crossbeam_skiplist::SkipMap;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

use super::internal_key::{InternalKey, DELETE_TAG, PUT_TAG};
use super::lease::LeaseHandle;
use super::manifest::Manifest;
use super::objectstore::ObjectStore;
use super::sstable::SSTable;
use super::wal::{Wal, WalRecord};

const MEMTABLE_MAX_SIZE: usize = 4 * 1024 * 1024;

/// A MemTable is an in-memory write buffer backed by a lock-free
/// `SkipMap`, keyed by [`InternalKey`] (user key + MVCC commit-sequence +
/// put/delete tag) rather than the raw user key — so multiple versions of
/// the same user key can be held here at once, which is what makes an open
/// snapshot's reads (of a key written again after the snapshot was taken)
/// resolvable without waiting for a flush. When it reaches a threshold size,
/// it is frozen and flushed to an SSTable.
///
/// Unlike the pre-lock-free-read-path `BTreeMap` version, every method here
/// takes `&self`, not `&mut self`: a reader may hold an `Arc<MemTable>` clone
/// of the *same* generation a writer is concurrently inserting into (see
/// `LsmEngine::write_batch`), and `SkipMap` is designed exactly for that
/// concurrent-read-during-insert access pattern (epoch-based reclamation
/// guarantees a reader never observes a torn individual entry).
pub struct MemTable {
    data: SkipMap<InternalKey, Vec<u8>>,
    size_bytes: AtomicUsize,
    max_size: usize,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: SkipMap::new(),
            size_bytes: AtomicUsize::new(0),
            max_size,
        }
    }

    /// Record a new version of `user_key` at `seq`. Unlike the pre-MVCC
    /// design, this never overwrites an existing entry in place — every
    /// write before the next flush is a distinct `InternalKey` (a different
    /// `seq`), because a concurrently open snapshot may need to resolve to
    /// exactly one of the intermediate versions rather than only the latest.
    pub fn insert(&self, user_key: Vec<u8>, seq: u64, value: Vec<u8>, tag: u8) {
        let entry_size = user_key.len() + value.len() + 8 + 1;
        self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
        let ikey = InternalKey::new(user_key, seq, tag);
        self.data.insert(ikey, value);
    }

    /// The version of `key` visible to `snapshot_seq`, if this memtable
    /// holds any entry for it at all. `None` (outer) means "no entry here —
    /// fall through to the next source"; `Some(None)` means "visible entry
    /// here, and it's a tombstone" (stop: the key is deleted as of this
    /// snapshot); `Some(Some(v))` is the visible value. Returns owned data
    /// (rather than the `&Vec<u8>` the old `BTreeMap` version returned) since
    /// a `SkipMap` entry's borrow is tied to its own guard, not to `&self`.
    pub fn get_at(&self, key: &[u8], snapshot_seq: u64) -> Option<Option<Vec<u8>>> {
        let seek = InternalKey::seek(key, snapshot_seq);
        let entry = self.data.range(seek..).next()?;
        if entry.key().user_key != key {
            return None;
        }
        if entry.key().is_delete() {
            Some(None)
        } else {
            Some(Some(entry.value().clone()))
        }
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes.load(Ordering::Relaxed) >= self.max_size
    }

    /// Every entry, cloned out in `InternalKey` order (user key ascending,
    /// then seq descending within a key) — the order the SSTable format
    /// requires, and the order `SkipMap` iteration already gives. Unlike the
    /// old `BTreeMap`-backed `drain`, this can't move entries out in place:
    /// a reader may concurrently hold this same frozen `Arc<MemTable>` (see
    /// `EngineView::immutable`) and still be iterating it, so entries are
    /// cloned rather than removed. Bounded by `MEMTABLE_MAX_SIZE`, so this is
    /// at most a few milliseconds of extra copying per flush.
    pub fn to_entries(&self) -> Vec<(InternalKey, Vec<u8>)> {
        self.data
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Exact entry count. `SkipMap` doesn't track a length counter (a
    /// lock-free structure can't cheaply maintain one), so this is O(n) —
    /// fine for its only caller, `stats()`, which isn't a hot path.
    pub fn len(&self) -> usize {
        self.data.iter().count()
    }

    /// Every entry, in `InternalKey` order, as `(user_key, seq, tag,
    /// value_or_none)` — the shape [`first_visible_per_key`] expects.
    pub fn iter_versions(&self) -> impl Iterator<Item = (Vec<u8>, u64, u8, Option<Vec<u8>>)> + '_ {
        self.data.iter().map(|e| {
            let ikey = e.key();
            let v = if ikey.is_delete() {
                None
            } else {
                Some(e.value().clone())
            };
            (ikey.user_key.clone(), ikey.seq, ikey.tag, v)
        })
    }
}

/// Reduce a stream of `(user_key, seq, tag, value)` entries from a single
/// source — which may hold multiple versions of the same key, arriving in
/// `InternalKey` order (user key ascending, then seq descending within a
/// key) — down to at most one entry per key: the newest version with
/// `seq <= snapshot_seq`, or nothing at all if every version of that key in
/// this source is newer than the snapshot. `value` is `None` for a
/// tombstone. Shared by `LsmEngine::scan_at` and `finish_compact`, which both
/// need this same "resolve to the one version a snapshot/watermark can see"
/// reduction, just over different sources.
fn first_visible_per_key(
    entries: impl Iterator<Item = (Vec<u8>, u64, u8, Option<Vec<u8>>)>,
    snapshot_seq: u64,
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut results = Vec::new();
    let mut resolved_key: Option<Vec<u8>> = None;
    for (key, seq, _tag, value) in entries {
        if resolved_key.as_deref() == Some(key.as_slice()) {
            continue; // already resolved this key from a newer version
        }
        if seq <= snapshot_seq {
            results.push((key.clone(), value));
            resolved_key = Some(key);
        }
    }
    results
}

/// Spread `sst/` and `wal/` objects across hash-derived sub-prefixes so they
/// don't all land under a single S3 key prefix. S3-compatible stores throttle
/// (HTTP 503 SlowDown) requests that exceed a per-prefix rate, so a hot writer
/// flushing many SSTables under one flat `sst/` prefix risks exactly that;
/// distributing by id keeps each prefix's request rate bounded.
///
/// The shard is a pure function of the numeric id, so a reader reconstructs the
/// exact object key from the manifest's id list with zero extra state — there's
/// no shard map to keep in sync between writer and reader. Override (or disable
/// with `0`) via `TPT_SST_SHARD_COUNT`.
const DEFAULT_SST_SHARD_COUNT: u64 = 256;

fn sst_shard(id: u64) -> String {
    let n = std::env::var("TPT_SST_SHARD_COUNT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SST_SHARD_COUNT);
    if n <= 1 {
        String::new()
    } else {
        // Pad to a fixed width so directory listing order matches numeric order
        // and the prefix fan-out is stable regardless of how large `n` grows.
        let width = ((n - 1).max(1) as f64).log10().floor() as usize + 1;
        let shard = (id % n) as u32;
        let mut s = format!("{shard:x}");
        while s.len() < width {
            s.insert(0, '0');
        }
        format!("{s}/")
    }
}

fn sstable_key(id: u64) -> String {
    format!("sst/{}{id:020}", sst_shard(id))
}

fn wal_segment_key(id: u64) -> String {
    format!("wal/{}seg_{id:020}", sst_shard(id))
}

/// Number of live SSTables that triggers a full compaction (a single merge of
/// every current SSTable into one, dropping shadowed/tombstoned keys).
/// Overridable for tests; same "env var, not a config-struct field" precedent
/// as `TPT_GPU_JOIN_THRESHOLD` (`executor/mod.rs`).
const DEFAULT_COMPACTION_SSTABLE_THRESHOLD: usize = 4;

fn compaction_sstable_threshold() -> usize {
    std::env::var("TPT_COMPACTION_SSTABLE_THRESHOLD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_COMPACTION_SSTABLE_THRESHOLD)
}

/// `TPT_COMPACTION_SSTABLE_THRESHOLD` is process-global env state, but
/// `cargo test` runs tests in parallel threads within the same process — any
/// test that temporarily overrides it (here and in `storage::chaos_tests`)
/// must hold this lock for the override's entire lifetime, or a concurrently
/// running test reading the default threshold can observe the override (or
/// vice versa), producing a flaky, seemingly-unrelated failure instead of a
/// deterministic one.
#[cfg(test)]
pub(crate) static COMPACTION_THRESHOLD_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Everything a flush needs to build + upload its SSTable, captured while
/// under the engine's `write` lock, so the actual upload (the expensive,
/// size-proportional part — up to a full memtable's worth of data) can run
/// via [`LsmEngine::finish_flush`] with *no* lock held at all. The caller
/// must eventually pass this to [`LsmEngine::commit_flush`] — dropping it
/// instead silently discards the drained memtable data, so every call site
/// that gets one back from `write`/`delete`/`write_batch` must drive it to
/// completion.
pub struct PendingFlush {
    entries: Vec<(InternalKey, Vec<u8>)>,
    sstable_id: u64,
    sst_key: String,
    store: Arc<dyn ObjectStore>,
}

/// Everything a compaction needs to rescan + merge + upload its SSTable,
/// captured cheaply (an `Arc` clone of the current SSTable list, no I/O) so
/// [`LsmEngine::finish_compact`] — the expensive part — can run with *no*
/// lock held at all, exactly mirroring [`PendingFlush`]/[`LsmEngine::finish_flush`].
pub struct PendingCompaction {
    sstables: Arc<Vec<Arc<SSTable>>>,
    /// The oldest open snapshot's `commit_seq` at the moment compaction
    /// began — versions at or above this are kept in full; the first version
    /// below it is kept as the "floor" a still-open snapshot resolves to,
    /// everything older than that is unreachable and dropped.
    watermark: u64,
    new_id: u64,
    new_key: String,
    store: Arc<dyn ObjectStore>,
}

/// Bookkeeping mutated by every write: the WAL (whose appends must be
/// strictly ordered — only one writer may append at a time), the
/// commit-sequence allocator, the open-snapshot watermark, and manifest/
/// SSTable-id state touched by flush/compaction commits. This is the one
/// remaining serialization point in `LsmEngine` — everything else (the
/// queryable memtable/immutable-memtable/SSTable data) lives in
/// [`EngineView`], published separately so reads never need this lock at
/// all.
struct WriteState {
    wal: Wal,
    sstable_id: u64,
    wal_seg_id: u64,
    manifest_etag: Option<String>,
    /// The `wal_segment_seq` most recently committed to the manifest.
    /// Compaction rewrites `sstable_ids` but never touches the WAL, so it
    /// reuses this value rather than `wal_seg_id` (which is always one ahead,
    /// pointing at the *next* segment to allocate).
    committed_wal_segment_seq: u64,
    /// The next MVCC commit-sequence number to allocate (`write_batch`
    /// allocates exactly one per call, shared by every row in that batch).
    /// Seeded on `open` from the durable high-water mark across the WAL,
    /// loaded SSTables, and the manifest, so a restart never reissues a
    /// `commit_seq` that was ever durably attempted.
    next_commit_seq: u64,
    /// Currently open snapshots, refcounted by `commit_seq` watermark — a
    /// transaction can `register_snapshot`/`release_snapshot` the same seq
    /// more than once only if it's cloned, so this tracks how many live
    /// holders remain rather than a plain set. Compaction consults the
    /// smallest key to know how far back it must keep old versions.
    open_snapshots: BTreeMap<u64, u32>,
}

/// The queryable state of the engine at a point in time: the active
/// memtable, the frozen/immutable memtable pending flush (if any), and the
/// current SSTable list. Published as a whole, new `Arc<EngineView>` at a
/// time, via `LsmEngine::view` — a reader clones that `Arc` under a
/// near-instantaneous lock, then does all of its actual work (memtable
/// lookups/scans, SSTable reads) against the clone with no further locking,
/// concurrently with writers, flushes, and compactions.
///
/// The active `memtable` itself is *not* republished on every write — writes
/// insert directly into the current generation's `SkipMap` in place (safe:
/// see `MemTable`'s doc comment), so a reader's `Arc<MemTable>` clone can
/// legitimately observe entries inserted after the clone was taken. This is
/// still correct because every read filters by `seq <= snapshot_seq`
/// (`MemTable::get_at`/`iter_versions`); a reader pinned to an older
/// snapshot simply ignores any newer-seq entries it happens to see. The
/// `memtable` field only changes identity when a flush freezes it into
/// `immutable` and installs a fresh one.
struct EngineView {
    memtable: Arc<MemTable>,
    immutable: Option<Arc<MemTable>>,
    sstables: Arc<Vec<Arc<SSTable>>>,
}

/// The LSM engine. Local disk is used only for the active WAL segment (the
/// low-latency write path) and is otherwise a disposable cache; SSTables and
/// sealed WAL segments live in `store`, and the `manifest.bin` object there
/// is the durable source of truth for which SSTables currently make up the
/// database — this is what lets a stateless compute node restart (or a
/// second node) pick up exactly where the durable state left off.
///
/// Every method takes `&self`; there is no outer `Mutex<LsmEngine>` (see
/// `Database.lsm`) — internally, `write` and `view` are two separate locks
/// with disjoint responsibilities (see their doc comments), and no method
/// ever holds `view` across I/O. Reads (`read_at`/`scan_at`) never touch
/// `write` at all.
pub struct LsmEngine {
    write: Mutex<WriteState>,
    view: Mutex<Arc<EngineView>>,
    store: Arc<dyn ObjectStore>,
    lease: Arc<LeaseHandle>,
}

impl LsmEngine {
    /// Open the engine. `local_dir` holds only the active WAL segment (a
    /// cache/staging area); `store` is the shared durable backend. `lease`
    /// is consulted before every flush/compaction — a node without a valid
    /// lease cannot advance the manifest (see `storage::lease`).
    pub fn open(
        local_dir: &Path,
        store: Arc<dyn ObjectStore>,
        lease: Arc<LeaseHandle>,
    ) -> Result<Self> {
        std::fs::create_dir_all(local_dir)?;

        let wal = Wal::open(local_dir)?;
        let memtable = MemTable::new(MEMTABLE_MAX_SIZE);

        let mut recovered = 0u64;
        let wal_max_seq = wal.replay(|record: WalRecord| {
            memtable.insert(record.key, record.seq, record.value, record.record_type);
            recovered += 1;
        })?;
        if recovered > 0 {
            info!(recovered, "WAL replay complete");
        }

        let (manifest, manifest_etag) = match Manifest::load(&*store)? {
            Some((m, etag)) => (m, Some(etag)),
            None => (Manifest::default(), None),
        };

        let mut sstables: Vec<Arc<SSTable>> = Vec::new();
        let mut sstable_max_seq = 0u64;
        for id in &manifest.sstable_ids {
            let key = sstable_key(*id);
            match SSTable::open_from_store(&*store, &key, *id) {
                Ok(sst) => {
                    sstable_max_seq = sstable_max_seq.max(sst.max_seq());
                    sstables.push(Arc::new(sst));
                }
                Err(e) => warn!(sstable = %key, error = %e, "failed to load sstable from manifest"),
            }
        }
        sstables.sort_by_key(|s| s.id());

        let sstable_id = manifest
            .sstable_ids
            .iter()
            .max()
            .map(|m| m + 1)
            .unwrap_or(1);
        let wal_seg_id = manifest.wal_segment_seq + 1;
        let committed_wal_segment_seq = manifest.wal_segment_seq;

        // Never reissue a commit_seq that was ever durably attempted,
        // whether it's visible in the WAL (including a torn trailing
        // batch's header — `Wal::replay` already folds that in), in a
        // loaded SSTable, or only recorded in the manifest (e.g. a batch
        // that made it into an SSTable which then failed to load above).
        let next_commit_seq = wal_max_seq
            .max(sstable_max_seq)
            .max(manifest.max_commit_seq)
            + 1;

        info!(
            sstable_count = sstables.len(),
            "SSTables loaded from manifest"
        );

        let write = WriteState {
            wal,
            sstable_id,
            wal_seg_id,
            manifest_etag,
            committed_wal_segment_seq,
            next_commit_seq,
            open_snapshots: BTreeMap::new(),
        };
        let view = EngineView {
            memtable: Arc::new(memtable),
            immutable: None,
            sstables: Arc::new(sstables),
        };

        Ok(Self {
            write: Mutex::new(write),
            view: Mutex::new(Arc::new(view)),
            store,
            lease,
        })
    }

    /// The most recent `commit_seq` actually assigned — what a new
    /// autocommit read/write should use as "as of now". `0` for a brand new
    /// database (before anything has ever committed), which correctly makes
    /// every real row invisible (all real `commit_seq`s start at 1).
    pub fn current_seq(&self) -> u64 {
        self.write.lock().unwrap().next_commit_seq - 1
    }

    /// Atomically read the current `commit_seq` watermark *and* register it
    /// as an open snapshot, under one `write` lock acquisition — used by
    /// `Database::begin_txn`. Doing this as two separate calls (`current_seq`
    /// then `register_snapshot`) would leave a window where a concurrent
    /// commit and compaction could land in between and drop a version this
    /// about-to-open transaction will need.
    pub fn snapshot_now(&self) -> u64 {
        let mut ws = self.write.lock().unwrap();
        let seq = ws.next_commit_seq - 1;
        *ws.open_snapshots.entry(seq).or_insert(0) += 1;
        seq
    }

    /// Register an open snapshot at `seq` (refcounted — the same seq can be
    /// registered more than once). Compaction will not drop a version some
    /// registered snapshot might still need to read.
    pub fn register_snapshot(&self, seq: u64) {
        let mut ws = self.write.lock().unwrap();
        *ws.open_snapshots.entry(seq).or_insert(0) += 1;
    }

    /// Release a previously registered snapshot. No-op if it was never
    /// registered (defensive — callers should not hit this in practice).
    pub fn release_snapshot(&self, seq: u64) {
        let mut ws = self.write.lock().unwrap();
        if let Some(count) = ws.open_snapshots.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                ws.open_snapshots.remove(&seq);
            }
        }
    }

    /// Write every entry in `entries` as one atomic batch sharing a single,
    /// freshly allocated `commit_seq` — returned so the caller (e.g.
    /// `Database::commit_txn`) can use it if needed. Allocating exactly one
    /// `commit_seq` per call (not one per entry) is what makes a
    /// multi-row transaction's commit atomic under snapshot isolation: a
    /// concurrently open snapshot either sees all of this batch's rows or
    /// none of them, never a torn subset. A plain autocommit write is simply
    /// a batch of size 1.
    ///
    /// The WAL append, `commit_seq` allocation, and memtable inserts all
    /// happen while `write` is held, for exactly the same atomicity reason:
    /// a reader must never be able to observe this batch's `commit_seq` as
    /// "current" (via `current_seq`) before every one of its rows has
    /// actually landed in the memtable.
    ///
    /// If this batch fills the memtable, the returned `PendingFlush` (see
    /// its doc comment) must be driven to completion via [`Self::finish_flush`]
    /// then [`Self::commit_flush`] — the caller decides when, so it can do
    /// so without holding any lock across the expensive part (see
    /// `Database`'s write paths).
    pub fn write_batch(
        &self,
        entries: &[(String, Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<(u64, Option<PendingFlush>)> {
        let mut ws = self.write.lock().unwrap();
        let commit_seq = ws.next_commit_seq;
        ws.next_commit_seq += 1;

        let wal_entries: Vec<(String, Vec<u8>, Vec<u8>, u8)> = entries
            .iter()
            .map(|(table, key, value)| match value {
                Some(v) => (table.clone(), key.clone(), v.clone(), PUT_TAG),
                None => (table.clone(), key.clone(), Vec::new(), DELETE_TAG),
            })
            .collect();
        ws.wal.append_batch(commit_seq, &wal_entries)?;

        // Insert into the *current* generation's memtable directly (no view
        // republish needed for a plain write — see `EngineView`'s doc
        // comment). Grabbing it requires only a near-instant `view` lock.
        let current_memtable = self.view.lock().unwrap().memtable.clone();
        for (_table, key, value) in entries {
            match value {
                Some(v) => current_memtable.insert(key.clone(), commit_seq, v.clone(), PUT_TAG),
                None => {
                    current_memtable.insert(key.clone(), commit_seq, Vec::new(), DELETE_TAG)
                }
            }
        }

        let pending = if current_memtable.is_full() {
            self.begin_flush(&mut ws)?
        } else {
            None
        };
        Ok((commit_seq, pending))
    }

    /// Write the latest value for `key`, visible to any read from now on.
    /// Thin wrapper over [`Self::write_batch`] — kept so every
    /// non-transactional call site (`StorageEngine` impl, catalog
    /// bookkeeping) is unaffected by the versioned storage underneath.
    pub fn write(&self, table: &str, key: &[u8], value: &[u8]) -> Result<Option<PendingFlush>> {
        let (_, pending) =
            self.write_batch(&[(table.to_string(), key.to_vec(), Some(value.to_vec()))])?;
        Ok(pending)
    }

    pub fn delete(&self, table: &str, key: &[u8]) -> Result<Option<PendingFlush>> {
        let (_, pending) = self.write_batch(&[(table.to_string(), key.to_vec(), None)])?;
        Ok(pending)
    }

    /// Read the latest committed value — equivalent to `read_at(key,
    /// current_seq())`.
    pub fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_at(key, self.current_seq())
    }

    /// Read the version of `key` visible to `snapshot_seq`: memtable →
    /// immutable memtable → SSTables newest-to-oldest, stopping at the first
    /// source that has *any* version of `key` visible to this snapshot
    /// (whether live or a tombstone) — a source with only newer, invisible
    /// versions correctly falls through to the next, older source instead.
    ///
    /// Lock-free beyond a single `Arc` clone: the `view` lock is held only
    /// long enough to clone the current `Arc<EngineView>` pointer, never
    /// across the memtable/SSTable lookups themselves, so this never queues
    /// up behind an in-flight write's fsync or a flush/compaction's I/O.
    pub fn read_at(&self, key: &[u8], snapshot_seq: u64) -> Result<Option<Vec<u8>>> {
        let snap = self.view.lock().unwrap().clone();
        if let Some(v) = snap.memtable.get_at(key, snapshot_seq) {
            return Ok(v);
        }
        if let Some(ref imm) = snap.immutable {
            if let Some(v) = imm.get_at(key, snapshot_seq) {
                return Ok(v);
            }
        }
        for sst in snap.sstables.iter().rev() {
            if let Some(v) = sst.read_at(key, snapshot_seq)? {
                return Ok(v);
            }
        }
        Ok(None)
    }

    /// Scan every live key at the latest committed state — equivalent to
    /// `scan_at(current_seq())`.
    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_at(self.current_seq())
    }

    /// Scan every key visible to `snapshot_seq`. Each source (SSTables
    /// oldest-to-newest, then immutable memtable, then memtable) is reduced
    /// to at most one entry per key via [`first_visible_per_key`], then
    /// overlaid in that same oldest-to-newest order so a newer source's
    /// resolved entry for a key (including a tombstone) correctly shadows an
    /// older source's. Lock-free beyond the initial `Arc` clone, same as
    /// [`Self::read_at`].
    pub fn scan_at(&self, snapshot_seq: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let snap = self.view.lock().unwrap().clone();
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for sst in snap.sstables.iter() {
            let versions = sst.scan_all_versions()?;
            for (key, value) in first_visible_per_key(versions.into_iter(), snapshot_seq) {
                merged.insert(key, value);
            }
        }
        if let Some(ref imm) = snap.immutable {
            for (key, value) in first_visible_per_key(imm.iter_versions(), snapshot_seq) {
                merged.insert(key, value);
            }
        }
        for (key, value) in first_visible_per_key(snap.memtable.iter_versions(), snapshot_seq) {
            merged.insert(key, value);
        }
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect())
    }

    /// Freeze the active memtable into `immutable` and install a fresh empty
    /// one, publishing the new `EngineView` immediately — so a concurrent
    /// reader can keep seeing the frozen data (via `immutable`) the whole
    /// time its SSTable upload is in flight, not just once the flush
    /// commits. Captures what [`Self::finish_flush`] needs to build the
    /// SSTable. Returns `None` if there's nothing to flush. Must be called
    /// with `ws` (the already-held `write` guard) so `sstable_id`
    /// allocation can't race a concurrent flush.
    fn begin_flush(&self, ws: &mut WriteState) -> Result<Option<PendingFlush>> {
        if !self.lease.is_valid() {
            bail!("fenced off: this node no longer holds the write lease");
        }

        let mut view_guard = self.view.lock().unwrap();
        let old_view = view_guard.clone();
        *view_guard = Arc::new(EngineView {
            memtable: Arc::new(MemTable::new(MEMTABLE_MAX_SIZE)),
            immutable: Some(old_view.memtable.clone()),
            sstables: old_view.sstables.clone(),
        });
        drop(view_guard);

        let entries = old_view.memtable.to_entries();
        if entries.is_empty() {
            return Ok(None);
        }

        let sstable_id = ws.sstable_id;
        ws.sstable_id += 1;
        let sst_key = sstable_key(sstable_id);

        Ok(Some(PendingFlush {
            entries,
            sstable_id,
            sst_key,
            store: self.store.clone(),
        }))
    }

    /// Build and upload the SSTable blob for a pending flush. Takes no
    /// `&self` — everything it needs is already captured in `pending` — so
    /// callers run this with no lock held, letting concurrent reads/writes
    /// proceed during the upload.
    pub fn finish_flush(pending: &PendingFlush) -> Result<SSTable> {
        SSTable::create_in_store(
            &*pending.store,
            &pending.sst_key,
            pending.sstable_id,
            &pending.entries,
        )
    }

    /// Commit a finished flush: ship the sealed WAL segment, CAS the
    /// manifest, splice the new SSTable into the live view (clearing
    /// `immutable`, since its data is now durable in the SSTable), and only
    /// then truncate the local WAL — preserving the crash-safety property
    /// that the WAL is never truncated until both the SSTable and manifest
    /// are durably committed.
    ///
    /// If the SSTable count now crosses the compaction threshold, returns a
    /// [`PendingCompaction`] for the caller to drive to completion (mirrors
    /// how a full memtable returns a `PendingFlush`) rather than running the
    /// (expensive) compaction synchronously here.
    pub fn commit_flush(
        &self,
        pending: PendingFlush,
        sst: SSTable,
    ) -> Result<Option<PendingCompaction>> {
        let mut ws = self.write.lock().unwrap();

        // Ship the sealed WAL segment to the shared store *before*
        // truncating the local copy, so a stateless compute node never
        // depends solely on this node's local disk for durability.
        let wal_bytes = ws.wal.read_all_bytes()?;
        if !wal_bytes.is_empty() {
            self.store
                .put(&wal_segment_key(ws.wal_seg_id), &wal_bytes)?;
        }

        let current_view = self.view.lock().unwrap().clone();
        let new_manifest = Manifest {
            sstable_ids: {
                let mut ids: Vec<u64> = current_view.sstables.iter().map(|s| s.id()).collect();
                ids.push(pending.sstable_id);
                ids
            },
            wal_segment_seq: ws.wal_seg_id,
            writer_fencing_token: self.lease.token(),
            max_commit_seq: ws.next_commit_seq - 1,
        };
        let new_etag =
            Manifest::save_cas(&*self.store, &new_manifest, ws.manifest_etag.as_deref())
                .map_err(|e| {
                    anyhow::anyhow!(e)
                        .context("updating manifest after flush — another writer may be active")
                })?;

        ws.manifest_etag = Some(new_etag);
        ws.committed_wal_segment_seq = ws.wal_seg_id;
        ws.wal_seg_id += 1;
        let sst_key = pending.sst_key.clone();
        let entries_len = pending.entries.len();

        let new_sstable_count;
        {
            let mut view_guard = self.view.lock().unwrap();
            let current = view_guard.clone();
            let mut new_sstables = (*current.sstables).clone();
            new_sstables.push(Arc::new(sst));
            new_sstable_count = new_sstables.len();
            *view_guard = Arc::new(EngineView {
                memtable: current.memtable.clone(),
                immutable: None,
                sstables: Arc::new(new_sstables),
            });
        }

        ws.wal.truncate()?;
        info!(sstable = %sst_key, entries = entries_len, "SSTable flushed to object store");

        if new_sstable_count >= compaction_sstable_threshold() {
            self.begin_compact(&mut ws)
        } else {
            Ok(None)
        }
    }

    /// Cheaply capture what a compaction needs (an `Arc` clone of the
    /// current SSTable list, plus the oldest-open-snapshot watermark) so
    /// [`Self::finish_compact`] — the expensive rescan+merge+upload — can
    /// run with no lock held at all. Must be called with `ws` already held,
    /// same reason as `begin_flush`. Returns `None` if there's nothing
    /// worth compacting (fewer than 2 SSTables).
    fn begin_compact(&self, ws: &mut WriteState) -> Result<Option<PendingCompaction>> {
        let view = self.view.lock().unwrap().clone();
        if view.sstables.len() < 2 {
            return Ok(None);
        }
        if !self.lease.is_valid() {
            bail!("fenced off: this node no longer holds the write lease");
        }

        let watermark = ws.open_snapshots.keys().next().copied().unwrap_or(u64::MAX);
        let new_id = ws.sstable_id;
        ws.sstable_id += 1;
        let new_key = sstable_key(new_id);

        Ok(Some(PendingCompaction {
            sstables: view.sstables.clone(),
            watermark,
            new_id,
            new_key,
            store: self.store.clone(),
        }))
    }

    /// Merge every SSTable captured in `pending` into a single new one. For
    /// each key, keeps every version still newer than the watermark
    /// (`pending.watermark`, the oldest snapshot open when compaction
    /// began), plus exactly one older "floor" version (what that oldest
    /// snapshot itself resolves to); everything beyond that floor is
    /// unreachable by any open snapshot and is dropped. With no snapshots
    /// open (the common case), the floor version is the single latest
    /// version — the same full collapse-to-one-version behavior this method
    /// had before MVCC, including dropping a dead tombstone entirely.
    ///
    /// This is a full (size-tiered, not true multi-level) compaction — every
    /// SSTable captured in `pending` is scanned. Takes no `&self`/lock —
    /// mirrors [`Self::finish_flush`] — so this runs fully concurrently with
    /// reads, writes, and even another flush landing in the meantime (see
    /// [`Self::commit_compact`] for how that race is reconciled).
    pub fn finish_compact(pending: &PendingCompaction) -> Result<SSTable> {
        let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, u8, Option<Vec<u8>>)>> = BTreeMap::new();
        for sst in pending.sstables.iter() {
            for (key, seq, tag, value) in sst.scan_all_versions()? {
                by_key.entry(key).or_default().push((seq, tag, value));
            }
        }

        let mut entries: Vec<(InternalKey, Vec<u8>)> = Vec::new();
        for (key, mut versions) in by_key {
            versions.sort_by(|a, b| b.0.cmp(&a.0)); // newest (highest seq) first
            for (seq, tag, value) in versions {
                if seq >= pending.watermark {
                    entries.push((InternalKey::new(key.clone(), seq, tag), value.unwrap_or_default()));
                    continue;
                }
                // First version older than the watermark: this is exactly
                // what the oldest open snapshot resolves to. Keep it and
                // stop — everything older is unreachable by any open
                // snapshot — unless there are no snapshots open at all
                // (`watermark == u64::MAX`) and this floor version is
                // itself a dead tombstone, in which case nothing needs it
                // either.
                if !(pending.watermark == u64::MAX && tag == DELETE_TAG) {
                    entries.push((InternalKey::new(key.clone(), seq, tag), value.unwrap_or_default()));
                }
                break;
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0)); // InternalKey order for the SSTable format

        SSTable::create_in_store(&*pending.store, &pending.new_key, pending.new_id, &entries)
    }

    /// Commit a finished compaction: CAS the manifest, then publish the new
    /// SSTable list. Reconciles against whatever the *current* view actually
    /// is rather than blindly replacing it with `[new_sst]`, because a
    /// concurrent flush can land while `finish_compact` was running
    /// lock-free — any SSTable in the current view that isn't one this
    /// compaction consumed (i.e. wasn't in `pending.sstables`) must survive
    /// the publish, or that flush's data would be silently dropped. Held
    /// under `write` for its entire duration (including the manifest CAS
    /// I/O) so this reconciliation and the publish are atomic together —
    /// matches the existing precedent `commit_flush` already sets for
    /// running its own manifest CAS under a lock.
    pub fn commit_compact(&self, pending: PendingCompaction, new_sst: SSTable) -> Result<()> {
        let mut ws = self.write.lock().unwrap();

        let consumed_ids: HashSet<u64> = pending.sstables.iter().map(|s| s.id()).collect();
        let current_view = self.view.lock().unwrap().clone();
        let mut new_sstables: Vec<Arc<SSTable>> = current_view
            .sstables
            .iter()
            .filter(|s| !consumed_ids.contains(&s.id()))
            .cloned()
            .collect();
        new_sstables.push(Arc::new(new_sst));

        let new_manifest = Manifest {
            sstable_ids: new_sstables.iter().map(|s| s.id()).collect(),
            wal_segment_seq: ws.committed_wal_segment_seq,
            writer_fencing_token: self.lease.token(),
            max_commit_seq: ws.next_commit_seq - 1,
        };
        let new_etag =
            Manifest::save_cas(&*self.store, &new_manifest, ws.manifest_etag.as_deref())
                .map_err(|e| {
                    anyhow::anyhow!(e).context(
                        "updating manifest after compaction — another writer may be active",
                    )
                })?;
        ws.manifest_etag = Some(new_etag);

        let merged_count = pending.sstables.len();
        let sstable_count_after = new_sstables.len();
        {
            let mut view_guard = self.view.lock().unwrap();
            let current = view_guard.clone();
            *view_guard = Arc::new(EngineView {
                memtable: current.memtable.clone(),
                immutable: current.immutable.clone(),
                sstables: Arc::new(new_sstables),
            });
        }

        // Old objects are no longer reachable from the manifest a fresh
        // reader would load; readers that already have them in memory keep
        // their own `Arc<SSTable>` clone regardless. Best-effort: a delete
        // failure just leaves an orphaned object in the store (same "space
        // not reclaimed on this error path" tradeoff `Partition::apply_retention`
        // already documents elsewhere), it doesn't fail the compaction that
        // already committed.
        for sst in pending.sstables.iter() {
            if let Err(e) = self.store.delete(sst.object_key()) {
                warn!(object = %sst.object_key(), error = %e, "failed to delete compacted-away sstable");
            }
        }

        info!(merged_count, sstable_count_after, sstable = %pending.new_key, "compacted SSTables");
        Ok(())
    }

    /// Merge every current SSTable into a single new one. Synchronous
    /// convenience wrapper around `begin_compact`/`finish_compact`/
    /// `commit_compact` — used by tests and by `commit_flush`'s internal
    /// trigger no longer uses this directly (it drives the split form so
    /// its caller can choose when to pay for the expensive part), but
    /// manual/test call sites where there's no concurrent reader to avoid
    /// blocking still want the one-call form.
    pub fn compact_all(&self) -> Result<()> {
        let pending = {
            let mut ws = self.write.lock().unwrap();
            self.begin_compact(&mut ws)?
        };
        if let Some(pending) = pending {
            let sst = Self::finish_compact(&pending)?;
            self.commit_compact(pending, sst)?;
        }
        Ok(())
    }

    /// Reload the manifest and fetch any SSTables it lists that we don't
    /// already have locally. Reader (replica) nodes call this on an interval
    /// to converge with whatever the writer has flushed. Fetches run with no
    /// lock held (`view` is only taken briefly, before and after, never
    /// across the network I/O); `write` is held for the whole call, but
    /// `refresh` only ever runs on reader nodes, which never contend for
    /// `write` with anything else (readers reject writes outright).
    pub fn refresh(&self) -> Result<bool> {
        let Some((manifest, etag)) = Manifest::load(&*self.store)? else {
            return Ok(false);
        };
        let mut ws = self.write.lock().unwrap();
        if Some(&etag) == ws.manifest_etag.as_ref() {
            return Ok(false);
        }

        let before = self.view.lock().unwrap().clone();
        let wanted: HashSet<u64> = manifest.sstable_ids.iter().copied().collect();
        let known: HashSet<u64> = before.sstables.iter().map(|s| s.id()).collect();

        let mut fetched_sstables: Vec<Arc<SSTable>> = Vec::new();
        let mut fetched = 0;
        for id in &manifest.sstable_ids {
            if known.contains(id) {
                continue;
            }
            let key = sstable_key(*id);
            let sst = SSTable::open_from_store(&*self.store, &key, *id)?;
            fetched_sstables.push(Arc::new(sst));
            fetched += 1;
        }

        let dropped;
        {
            let mut view_guard = self.view.lock().unwrap();
            let latest = view_guard.clone();
            // The manifest's set can also *shrink* — e.g. the writer
            // compacted several SSTables into one — so drop anything we're
            // holding that's no longer listed, or a reader's SSTable list
            // would grow forever right alongside the writer's, defeating
            // the point of compaction.
            let mut new_sstables: Vec<Arc<SSTable>> = latest
                .sstables
                .iter()
                .filter(|s| wanted.contains(&s.id()))
                .cloned()
                .collect();
            dropped = latest.sstables.len() - new_sstables.len();
            new_sstables.extend(fetched_sstables);
            new_sstables.sort_by_key(|s| s.id());

            *view_guard = Arc::new(EngineView {
                memtable: latest.memtable.clone(),
                immutable: latest.immutable.clone(),
                sstables: Arc::new(new_sstables),
            });
        }

        ws.manifest_etag = Some(etag);
        // Advance the commit-seq watermark to at least what the manifest
        // now reflects, so a plain (non-transactional) read on this reader
        // node sees everything it just fetched. Without this, a reader's
        // `current_seq()` stays frozen at whatever it was when the engine
        // was opened, and every row the writer commits afterward — even
        // though its SSTable gets fetched right here — stays invisible
        // (its `seq` is above the reader's stale watermark). Refresh only
        // ever pulls in newer state, so this never needs to move backward.
        ws.next_commit_seq = ws.next_commit_seq.max(manifest.max_commit_seq + 1);

        if fetched > 0 || dropped > 0 {
            info!(fetched, dropped, "refreshed manifest — SSTable set changed");
        }
        Ok(fetched > 0 || dropped > 0)
    }

    /// Synchronous convenience wrapper — runs a whole flush (begin, build,
    /// commit) in one call. Used by tests and explicit manual-flush call
    /// sites where there's no concurrent reader to unblock, so the
    /// lock-splitting `PendingFlush` dance would just be overhead. Any
    /// compaction the flush triggers is driven to completion inline too, for
    /// the same reason.
    pub fn flush(&self) -> Result<()> {
        let pending = {
            let mut ws = self.write.lock().unwrap();
            self.begin_flush(&mut ws)?
        };
        if let Some(pending) = pending {
            let sst = Self::finish_flush(&pending)?;
            if let Some(pc) = self.commit_flush(pending, sst)? {
                let merged = Self::finish_compact(&pc)?;
                self.commit_compact(pc, merged)?;
            }
        }
        Ok(())
    }

    pub fn stats(&self) -> super::StorageStats {
        let ws = self.write.lock().unwrap();
        let snap = self.view.lock().unwrap().clone();
        super::StorageStats {
            wal_bytes_written: ws.wal.bytes_written(),
            memtable_entries: snap.memtable.len(),
            sstable_count: snap.sstables.len(),
            total_disk_bytes: snap.sstables.iter().map(|s| s.blob_size()).sum(),
            open_snapshot_count: ws.open_snapshots.len(),
        }
    }

    /// Test-only accessors so unit tests can assert on internal state
    /// without needing direct field access (which no longer works now that
    /// `sstables`/`memtable` live behind `view`, not directly on `self`).
    #[cfg(test)]
    fn sstable_count(&self) -> usize {
        self.view.lock().unwrap().sstables.len()
    }

    #[cfg(test)]
    fn begin_flush_for_test(&self) -> Result<Option<PendingFlush>> {
        let mut ws = self.write.lock().unwrap();
        self.begin_flush(&mut ws)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::lease::LeaseManager;
    use crate::storage::objectstore::LocalFsObjectStore;
    use std::collections::HashMap;
    use std::time::Duration;

    fn writer_lease(store: Arc<dyn ObjectStore>) -> Arc<LeaseHandle> {
        let mgr = LeaseManager::new(store, "db", "node-1".into(), Duration::from_secs(30));
        mgr.try_acquire().unwrap();
        // Leak the manager so its background renewal (if any) isn't relevant —
        // tests only need a validated, never-expiring handle.
        Box::leak(Box::new(mgr)).handle()
    }

    fn open_engine(bucket: &Path, local: &Path) -> (LsmEngine, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket).unwrap());
        let lease = writer_lease(store.clone());
        let engine = LsmEngine::open(local, store.clone(), lease).unwrap();
        (engine, store)
    }

    fn force_flush(engine: &LsmEngine, key: &[u8], value: &[u8]) {
        engine.write("t", key, value).unwrap();
        // A flush only fires automatically once the memtable hits its byte
        // threshold; tests write one small row per SSTable, so flush
        // explicitly instead of writing megabytes of filler.
        engine.flush().unwrap();
    }

    /// Wraps another `ObjectStore` and sleeps before every `sst/`-prefixed
    /// `put`, standing in for a slow real network PUT — lets a test observe
    /// whether something is holding a lock across that delay without
    /// needing an actual multi-second SSTable upload.
    struct DelayedPutStore {
        inner: Arc<dyn ObjectStore>,
        delay: Duration,
    }

    impl ObjectStore for DelayedPutStore {
        fn get(
            &self,
            key: &str,
        ) -> Result<Option<(Vec<u8>, crate::storage::objectstore::ObjectMeta)>> {
            self.inner.get(key)
        }
        fn put(&self, key: &str, data: &[u8]) -> Result<crate::storage::objectstore::ObjectMeta> {
            if key.starts_with("sst/") {
                std::thread::sleep(self.delay);
            }
            self.inner.put(key, data)
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected_etag: Option<&str>,
        ) -> Result<crate::storage::objectstore::ObjectMeta, crate::storage::objectstore::CasError>
        {
            self.inner.put_if_match(key, data, expected_etag)
        }
        fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix)
        }
    }

    /// The whole point of splitting `flush` into
    /// `begin_flush`/`finish_flush`/`commit_flush` was so that a flush's
    /// SSTable upload — the expensive, size-proportional part — doesn't
    /// block concurrent readers for its duration. This test proves it
    /// empirically: with a `put` artificially slowed down to simulate a
    /// real network round trip, a concurrent read must still complete
    /// promptly instead of queuing up behind the upload.
    #[test]
    fn finish_flush_does_not_block_a_concurrent_read() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let store: Arc<dyn ObjectStore> = Arc::new(DelayedPutStore {
            inner,
            delay: Duration::from_millis(300),
        });
        let lease = writer_lease(store.clone());
        let engine = LsmEngine::open(local.path(), store, lease).unwrap();

        engine.write("t", b"a", b"1").unwrap();
        let pending = engine
            .begin_flush_for_test()
            .unwrap()
            .expect("a non-empty memtable must yield a pending flush");

        let engine = Arc::new(engine);
        let engine_for_read = engine.clone();

        // Mirrors `Database`'s write path: the slow upload runs with no
        // engine lock held at all.
        let io_thread = std::thread::spawn(move || LsmEngine::finish_flush(&pending).unwrap());

        // Give the upload a moment to actually be in-flight before racing it.
        std::thread::sleep(Duration::from_millis(50));
        let before_read = std::time::Instant::now();
        // The frozen memtable's data must still be visible via `immutable`
        // while the upload is in flight.
        let value = engine_for_read.read(b"a").unwrap();
        let read_latency = before_read.elapsed();

        io_thread.join().unwrap();
        assert_eq!(
            value,
            Some(b"1".to_vec()),
            "data must remain readable via the frozen immutable memtable while its flush uploads"
        );
        assert!(
            read_latency < Duration::from_millis(200),
            "a concurrent read should not queue up behind the in-flight SSTable \
             upload (300ms delay), took {read_latency:?}"
        );
    }

    /// Mirrors `finish_flush_does_not_block_a_concurrent_read`, but for
    /// compaction: `finish_compact`'s rescan+merge+upload must not block a
    /// concurrent read either.
    #[test]
    fn finish_compact_does_not_block_a_concurrent_read() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let store: Arc<dyn ObjectStore> = Arc::new(DelayedPutStore {
            inner,
            delay: Duration::from_millis(300),
        });
        let lease = writer_lease(store.clone());
        let engine = LsmEngine::open(local.path(), store, lease).unwrap();

        force_flush(&engine, b"a", b"1");
        force_flush(&engine, b"b", b"2");
        assert_eq!(engine.sstable_count(), 2);

        let pending = {
            let mut ws = engine.write.lock().unwrap();
            engine
                .begin_compact(&mut ws)
                .unwrap()
                .expect("2 sstables should yield a pending compaction")
        };

        let engine = Arc::new(engine);
        let engine_for_read = engine.clone();

        let io_thread = std::thread::spawn(move || LsmEngine::finish_compact(&pending).unwrap());

        std::thread::sleep(Duration::from_millis(50));
        let before_read = std::time::Instant::now();
        let value = engine_for_read.read(b"a").unwrap();
        let read_latency = before_read.elapsed();

        io_thread.join().unwrap();
        assert_eq!(value, Some(b"1".to_vec()));
        assert!(
            read_latency < Duration::from_millis(200),
            "a concurrent read should not queue up behind an in-flight compaction \
             upload (300ms delay), took {read_latency:?}"
        );
    }

    #[test]
    fn compaction_merges_sstables_and_bounds_the_list() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (engine, _store) = open_engine(bucket.path(), local.path());

        let _env_guard = COMPACTION_THRESHOLD_ENV_LOCK.lock().unwrap();
        std::env::set_var("TPT_COMPACTION_SSTABLE_THRESHOLD", "3");

        force_flush(&engine, b"a", b"1");
        force_flush(&engine, b"b", b"2");
        force_flush(&engine, b"a", b"1-updated");
        // The third flush pushes the SSTable count to the threshold, so this
        // flush's tail should have compacted all three down to one.
        assert_eq!(engine.sstable_count(), 1);

        assert_eq!(engine.read(b"a").unwrap(), Some(b"1-updated".to_vec()));
        assert_eq!(engine.read(b"b").unwrap(), Some(b"2".to_vec()));

        std::env::remove_var("TPT_COMPACTION_SSTABLE_THRESHOLD");
    }

    #[test]
    fn compaction_drops_tombstoned_keys() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (engine, _store) = open_engine(bucket.path(), local.path());

        force_flush(&engine, b"a", b"1");
        engine.delete("t", b"a").unwrap();
        engine.flush().unwrap();

        engine.compact_all().unwrap();
        assert_eq!(engine.sstable_count(), 1);
        assert_eq!(engine.read(b"a").unwrap(), None);
        assert_eq!(engine.scan().unwrap(), Vec::<(Vec<u8>, Vec<u8>)>::new());
    }

    #[test]
    fn reader_refresh_drops_sstables_removed_by_writer_compaction() {
        let bucket = tempfile::tempdir().unwrap();
        let writer_local = tempfile::tempdir().unwrap();
        let reader_local = tempfile::tempdir().unwrap();

        let (writer, store) = open_engine(bucket.path(), writer_local.path());
        force_flush(&writer, b"a", b"1");
        force_flush(&writer, b"b", b"2");

        let reader = LsmEngine::open(
            reader_local.path(),
            store.clone(),
            Arc::new(LeaseHandle::default()),
        )
        .unwrap();
        reader.refresh().unwrap();
        assert_eq!(reader.sstable_count(), 2);

        writer.compact_all().unwrap();
        assert_eq!(writer.sstable_count(), 1);

        reader.refresh().unwrap();
        assert_eq!(reader.sstable_count(), 1);
        assert_eq!(reader.read(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(reader.read(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn read_at_resolves_an_older_snapshot_across_a_later_write() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (engine, _store) = open_engine(bucket.path(), local.path());

        engine.write("t", b"k", b"v1").unwrap();
        let snap = engine.current_seq();
        engine.write("t", b"k", b"v2").unwrap();

        assert_eq!(engine.read_at(b"k", snap).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.read(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn read_at_resolves_an_older_snapshot_across_a_flush() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (engine, _store) = open_engine(bucket.path(), local.path());

        engine.write("t", b"k", b"v1").unwrap();
        let snap = engine.current_seq();
        engine.write("t", b"k", b"v2").unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.read_at(b"k", snap).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.read(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn long_running_snapshot_survives_a_compaction() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (engine, _store) = open_engine(bucket.path(), local.path());

        let _env_guard = COMPACTION_THRESHOLD_ENV_LOCK.lock().unwrap();
        std::env::set_var("TPT_COMPACTION_SSTABLE_THRESHOLD", "3");

        force_flush(&engine, b"k", b"v1");
        let snap = engine.current_seq();
        engine.register_snapshot(snap);

        force_flush(&engine, b"k", b"v2");
        // Third flush pushes the SSTable count to the threshold and triggers
        // compaction.
        force_flush(&engine, b"k", b"v3");
        assert_eq!(engine.sstable_count(), 1, "compaction should still have run");

        assert_eq!(
            engine.read_at(b"k", snap).unwrap(),
            Some(b"v1".to_vec()),
            "the old snapshot must still see its original value after compaction"
        );
        assert_eq!(
            engine.read(b"k").unwrap(),
            Some(b"v3".to_vec()),
            "a fresh read must see the latest value"
        );

        engine.release_snapshot(snap);
        std::env::remove_var("TPT_COMPACTION_SSTABLE_THRESHOLD");
    }

    #[test]
    fn compaction_still_collapses_to_one_version_once_the_snapshot_is_released() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (engine, _store) = open_engine(bucket.path(), local.path());

        let _env_guard = COMPACTION_THRESHOLD_ENV_LOCK.lock().unwrap();
        std::env::set_var("TPT_COMPACTION_SSTABLE_THRESHOLD", "2");

        force_flush(&engine, b"k", b"v1");
        let snap = engine.current_seq();
        engine.register_snapshot(snap);
        engine.release_snapshot(snap); // released immediately -- no longer pinned

        force_flush(&engine, b"k", b"v2"); // triggers compaction at threshold 2
        assert_eq!(engine.sstable_count(), 1);
        assert_eq!(engine.read(b"k").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(
            engine.read_at(b"k", snap).unwrap(),
            None,
            "the old version is gone once nothing holds its snapshot anymore"
        );

        std::env::remove_var("TPT_COMPACTION_SSTABLE_THRESHOLD");
    }

    /// A multi-row `write_batch` must never be observable as a torn subset:
    /// a concurrent scan either sees every row of a batch or none of them.
    /// Many reader threads hammer `scan_at(current_seq())` while a writer
    /// thread issues batches of several rows sharing one key prefix; every
    /// observed scan must show all-or-nothing per batch.
    ///
    /// This also doubles as the "a write burst doesn't block a reader"
    /// timing check: `read_at`/`scan_at` never take the `write` lock at all
    /// (see their doc comments), so a reader racing a continuous stream of
    /// writer-thread `write_batch` calls should never see a scan take
    /// anywhere near as long as the whole writer burst — each reader thread
    /// tracks its own worst-case single-scan latency and the assertion below
    /// checks it stayed low throughout, not just once at the end.
    #[test]
    fn concurrent_scans_never_observe_a_torn_batch_or_stall_behind_the_writer() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (engine, _store) = open_engine(bucket.path(), local.path());
        let engine = Arc::new(engine);

        const BATCHES: usize = 200;
        const ROWS_PER_BATCH: usize = 5;

        let writer_engine = engine.clone();
        let writer = std::thread::spawn(move || {
            for batch_id in 0..BATCHES {
                let entries: Vec<(String, Vec<u8>, Option<Vec<u8>>)> = (0..ROWS_PER_BATCH)
                    .map(|row| {
                        let key = format!("batch{batch_id:04}-row{row}").into_bytes();
                        (
                            "t".to_string(),
                            key,
                            Some(batch_id.to_le_bytes().to_vec()),
                        )
                    })
                    .collect();
                writer_engine.write_batch(&entries).unwrap();
            }
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader_engine = engine.clone();
            readers.push(std::thread::spawn(move || {
                let mut max_scan_latency = Duration::ZERO;
                for _ in 0..500 {
                    let before = std::time::Instant::now();
                    let rows = reader_engine.scan().unwrap();
                    max_scan_latency = max_scan_latency.max(before.elapsed());

                    let mut counts: HashMap<usize, usize> = HashMap::new();
                    for (key, _) in &rows {
                        if let Ok(key_str) = std::str::from_utf8(key) {
                            if let Some(id_str) = key_str
                                .strip_prefix("batch")
                                .and_then(|s| s.split('-').next())
                            {
                                if let Ok(id) = id_str.parse::<usize>() {
                                    *counts.entry(id).or_insert(0) += 1;
                                }
                            }
                        }
                    }
                    for (batch_id, count) in counts {
                        assert_eq!(
                            count, ROWS_PER_BATCH,
                            "batch {batch_id} observed with {count}/{ROWS_PER_BATCH} rows — torn batch"
                        );
                    }
                }
                max_scan_latency
            }));
        }

        writer.join().unwrap();
        for r in readers {
            let max_scan_latency = r.join().unwrap();
            assert!(
                max_scan_latency < Duration::from_secs(1),
                "a single scan took {max_scan_latency:?} — a reader should never \
                 queue up behind the writer's `write` lock, which read_at/scan_at \
                 never acquire"
            );
        }
    }
}
