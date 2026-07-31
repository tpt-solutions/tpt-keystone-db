use anyhow::{bail, Result};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

use super::internal_key::{InternalKey, DELETE_TAG, PUT_TAG};
use super::lease::LeaseHandle;
use super::manifest::Manifest;
use super::objectstore::ObjectStore;
use super::sstable::SSTable;
use super::wal::{Wal, WalRecord};

/// A MemTable is an in-memory write buffer backed by a BTreeMap, keyed by
/// [`InternalKey`] (user key + MVCC commit-sequence + put/delete tag) rather
/// than the raw user key — so multiple versions of the same user key can be
/// held here at once, which is what makes an open snapshot's reads (of a key
/// written again after the snapshot was taken) resolvable without waiting
/// for a flush. When it reaches a threshold size, it is frozen and flushed
/// to an SSTable.
pub struct MemTable {
    data: BTreeMap<InternalKey, Vec<u8>>,
    size_bytes: usize,
    max_size: usize,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0,
            max_size,
        }
    }

    /// Record a new version of `user_key` at `seq`. Unlike the pre-MVCC
    /// design, this never overwrites an existing entry in place — every
    /// write before the next flush is a distinct `InternalKey` (a different
    /// `seq`), because a concurrently open snapshot may need to resolve to
    /// exactly one of the intermediate versions rather than only the latest.
    pub fn insert(&mut self, user_key: Vec<u8>, seq: u64, value: Vec<u8>, tag: u8) {
        let entry_size = user_key.len() + value.len() + 8 + 1;
        self.size_bytes += entry_size;
        let ikey = InternalKey::new(user_key, seq, tag);
        self.data.insert(ikey, value);
    }

    /// The version of `key` visible to `snapshot_seq`, if this memtable
    /// holds any entry for it at all. `None` (outer) means "no entry here —
    /// fall through to the next source"; `Some(None)` means "visible entry
    /// here, and it's a tombstone" (stop: the key is deleted as of this
    /// snapshot); `Some(Some(v))` is the visible value.
    pub fn get_at(&self, key: &[u8], snapshot_seq: u64) -> Option<Option<&Vec<u8>>> {
        let seek = InternalKey::seek(key, snapshot_seq);
        let (ikey, value) = self.data.range(seek..).next()?;
        if ikey.user_key != key {
            return None;
        }
        if ikey.is_delete() {
            Some(None)
        } else {
            Some(Some(value))
        }
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.max_size
    }

    /// Drain all entries in `InternalKey` order (user key ascending, then
    /// seq descending within a key) — the order the SSTable format requires,
    /// and exactly the iteration order a `BTreeMap` already gives for free.
    pub fn drain(&mut self) -> Vec<(InternalKey, Vec<u8>)> {
        let result: Vec<_> = std::mem::take(&mut self.data).into_iter().collect();
        self.size_bytes = 0;
        result
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Every entry, in `InternalKey` order, as `(user_key, seq, tag,
    /// value_or_none)` — the shape [`first_visible_per_key`] expects.
    pub fn iter_versions(&self) -> impl Iterator<Item = (Vec<u8>, u64, u8, Option<Vec<u8>>)> + '_ {
        self.data.iter().map(|(ikey, value)| {
            let v = if ikey.is_delete() {
                None
            } else {
                Some(value.clone())
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
/// tombstone. Shared by `LsmEngine::scan_at` and `compact_all`, which both
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

/// The LSM engine. Local disk is used only for the active WAL segment (the
/// low-latency write path) and is otherwise a disposable cache; SSTables and
/// sealed WAL segments live in `store`, and the `manifest.bin` object there
/// is the durable source of truth for which SSTables currently make up the
/// database — this is what lets a stateless compute node restart (or a
/// second node) pick up exactly where the durable state left off.
pub struct LsmEngine {
    memtable: MemTable,
    immutable: Option<MemTable>,
    sstables: Vec<SSTable>,
    wal: Wal,
    store: Arc<dyn ObjectStore>,
    lease: Arc<LeaseHandle>,
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
    /// holders remain rather than a plain set. `compact_all` consults the
    /// smallest key to know how far back it must keep old versions.
    open_snapshots: BTreeMap<u64, u32>,
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

impl LsmEngine {
    /// Open the engine. `local_dir` holds only the active WAL segment (a
    /// cache/staging area); `store` is the shared durable backend. `lease`
    /// is consulted before every flush — a node without a valid lease cannot
    /// advance the manifest (see `storage::lease`).
    pub fn open(
        local_dir: &Path,
        store: Arc<dyn ObjectStore>,
        lease: Arc<LeaseHandle>,
    ) -> Result<Self> {
        std::fs::create_dir_all(local_dir)?;

        let wal = Wal::open(local_dir)?;
        let mut memtable = MemTable::new(4 * 1024 * 1024);

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

        let mut sstables = Vec::new();
        let mut sstable_max_seq = 0u64;
        for id in &manifest.sstable_ids {
            let key = sstable_key(*id);
            match SSTable::open_from_store(&*store, &key, *id) {
                Ok(sst) => {
                    sstable_max_seq = sstable_max_seq.max(sst.max_seq());
                    sstables.push(sst);
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

        Ok(Self {
            memtable,
            immutable: None,
            sstables,
            wal,
            store,
            lease,
            sstable_id,
            wal_seg_id,
            manifest_etag,
            committed_wal_segment_seq,
            next_commit_seq,
            open_snapshots: BTreeMap::new(),
        })
    }

    /// The most recent `commit_seq` actually assigned — what a new
    /// autocommit read/write or freshly begun transaction should use as "as
    /// of now". `0` for a brand new database (before anything has ever
    /// committed), which correctly makes every real row invisible (all real
    /// `commit_seq`s start at 1).
    pub fn current_seq(&self) -> u64 {
        self.next_commit_seq - 1
    }

    /// Register an open snapshot at `seq` (refcounted — the same seq can be
    /// registered more than once). `compact_all` will not drop a version
    /// some registered snapshot might still need to read.
    pub fn register_snapshot(&mut self, seq: u64) {
        *self.open_snapshots.entry(seq).or_insert(0) += 1;
    }

    /// Release a previously registered snapshot. No-op if it was never
    /// registered (defensive — callers should not hit this in practice).
    pub fn release_snapshot(&mut self, seq: u64) {
        if let Some(count) = self.open_snapshots.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                self.open_snapshots.remove(&seq);
            }
        }
    }

    fn oldest_open_snapshot(&self) -> Option<u64> {
        self.open_snapshots.keys().next().copied()
    }

    /// Write every entry in `entries` as one atomic batch sharing a single,
    /// freshly allocated `commit_seq` — returned so the caller (e.g.
    /// `Database::commit_txn`) can use it if needed. Allocating exactly one
    /// `commit_seq` per call (not one per entry) is what makes a
    /// multi-row transaction's commit atomic under snapshot isolation: a
    /// concurrently open snapshot either sees all of this batch's rows or
    /// none of them, never a torn subset. A plain autocommit write is simply
    /// a batch of size 1.
    pub fn write_batch(&mut self, entries: &[(String, Vec<u8>, Option<Vec<u8>>)]) -> Result<u64> {
        let commit_seq = self.next_commit_seq;
        self.next_commit_seq += 1;

        let wal_entries: Vec<(String, Vec<u8>, Vec<u8>, u8)> = entries
            .iter()
            .map(|(table, key, value)| match value {
                Some(v) => (table.clone(), key.clone(), v.clone(), PUT_TAG),
                None => (table.clone(), key.clone(), Vec::new(), DELETE_TAG),
            })
            .collect();
        self.wal.append_batch(commit_seq, &wal_entries)?;

        for (_table, key, value) in entries {
            match value {
                Some(v) => self
                    .memtable
                    .insert(key.clone(), commit_seq, v.clone(), PUT_TAG),
                None => self
                    .memtable
                    .insert(key.clone(), commit_seq, Vec::new(), DELETE_TAG),
            }
        }

        if self.memtable.is_full() {
            self.trigger_flush()?;
        }
        Ok(commit_seq)
    }

    /// Write the latest value for `key`, visible to any read from now on.
    /// Thin wrapper over [`Self::write_batch`] — kept so every
    /// non-transactional call site (`StorageEngine` impl, catalog
    /// bookkeeping) is unaffected by the versioned storage underneath.
    pub fn write(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[(table.to_string(), key.to_vec(), Some(value.to_vec()))])?;
        Ok(())
    }

    pub fn delete(&mut self, table: &str, key: &[u8]) -> Result<()> {
        self.write_batch(&[(table.to_string(), key.to_vec(), None)])?;
        Ok(())
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
    pub fn read_at(&self, key: &[u8], snapshot_seq: u64) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.memtable.get_at(key, snapshot_seq) {
            return Ok(v.cloned());
        }
        if let Some(ref imm) = self.immutable {
            if let Some(v) = imm.get_at(key, snapshot_seq) {
                return Ok(v.cloned());
            }
        }
        for sst in self.sstables.iter().rev() {
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
    /// older source's.
    pub fn scan_at(&self, snapshot_seq: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for sst in &self.sstables {
            let versions = sst.scan_all_versions()?;
            for (key, value) in first_visible_per_key(versions.into_iter(), snapshot_seq) {
                merged.insert(key, value);
            }
        }
        if let Some(ref imm) = self.immutable {
            for (key, value) in first_visible_per_key(imm.iter_versions(), snapshot_seq) {
                merged.insert(key, value);
            }
        }
        for (key, value) in first_visible_per_key(self.memtable.iter_versions(), snapshot_seq) {
            merged.insert(key, value);
        }
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect())
    }

    fn trigger_flush(&mut self) -> Result<()> {
        if !self.lease.is_valid() {
            bail!("fenced off: this node no longer holds the write lease");
        }

        let old_mem = std::mem::replace(&mut self.memtable, MemTable::new(4 * 1024 * 1024));
        self.immutable = Some(old_mem);

        if let Some(mut imm) = self.immutable.take() {
            let entries = imm.drain();
            if entries.is_empty() {
                return Ok(());
            }

            let sst_key = sstable_key(self.sstable_id);
            let sst = SSTable::create_in_store(&*self.store, &sst_key, self.sstable_id, &entries)?;

            // Ship the sealed WAL segment to the shared store *before*
            // truncating the local copy, so a stateless compute node never
            // depends solely on this node's local disk for durability.
            let wal_bytes = self.wal.read_all_bytes()?;
            if !wal_bytes.is_empty() {
                self.store
                    .put(&wal_segment_key(self.wal_seg_id), &wal_bytes)?;
            }

            let new_manifest = Manifest {
                sstable_ids: {
                    let mut ids: Vec<u64> = self.sstables.iter().map(|s| s.id()).collect();
                    ids.push(self.sstable_id);
                    ids
                },
                wal_segment_seq: self.wal_seg_id,
                writer_fencing_token: self.lease.token(),
                max_commit_seq: self.current_seq(),
            };
            let new_etag =
                Manifest::save_cas(&*self.store, &new_manifest, self.manifest_etag.as_deref())
                    .map_err(|e| {
                        anyhow::anyhow!(e)
                            .context("updating manifest after flush — another writer may be active")
                    })?;

            self.manifest_etag = Some(new_etag);
            self.committed_wal_segment_seq = self.wal_seg_id;
            self.wal_seg_id += 1;
            self.sstables.push(sst);
            self.sstable_id += 1;

            self.wal.truncate()?;
            info!(sstable = %sst_key, entries = entries.len(), "SSTable flushed to object store");
        }

        if self.sstables.len() >= compaction_sstable_threshold() {
            self.compact_all()?;
        }
        Ok(())
    }

    /// Merge every current SSTable into a single new one. For each key,
    /// keeps every version still newer than the oldest currently open
    /// snapshot, plus exactly one older "floor" version (what that oldest
    /// snapshot itself resolves to); everything beyond that floor is
    /// unreachable by any open snapshot and is dropped. With no snapshots
    /// open (the common case), the floor version is the single latest
    /// version — the same full collapse-to-one-version behavior this method
    /// had before MVCC, including dropping a dead tombstone entirely.
    ///
    /// This is a full (size-tiered, not true multi-level) compaction — every
    /// live SSTable is scanned every time, so there's never an "older
    /// SSTable outside this compaction pass" to worry about shadowing.
    pub fn compact_all(&mut self) -> Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }
        if !self.lease.is_valid() {
            bail!("fenced off: this node no longer holds the write lease");
        }

        let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, u8, Option<Vec<u8>>)>> = BTreeMap::new();
        for sst in &self.sstables {
            for (key, seq, tag, value) in sst.scan_all_versions()? {
                by_key.entry(key).or_default().push((seq, tag, value));
            }
        }

        let watermark = self.oldest_open_snapshot().unwrap_or(u64::MAX);
        let mut entries: Vec<(InternalKey, Vec<u8>)> = Vec::new();
        for (key, mut versions) in by_key {
            versions.sort_by(|a, b| b.0.cmp(&a.0)); // newest (highest seq) first
            for (seq, tag, value) in versions {
                if seq >= watermark {
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
                if !(watermark == u64::MAX && tag == DELETE_TAG) {
                    entries.push((InternalKey::new(key.clone(), seq, tag), value.unwrap_or_default()));
                }
                break;
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0)); // InternalKey order for the SSTable format

        let old_keys: Vec<String> = self
            .sstables
            .iter()
            .map(|s| s.object_key().to_string())
            .collect();

        let new_id = self.sstable_id;
        let new_key = sstable_key(new_id);
        let entries_len = entries.len();
        let new_sst = SSTable::create_in_store(&*self.store, &new_key, new_id, &entries)?;

        let new_manifest = Manifest {
            sstable_ids: vec![new_id],
            wal_segment_seq: self.committed_wal_segment_seq,
            writer_fencing_token: self.lease.token(),
            max_commit_seq: self.current_seq(),
        };
        let new_etag =
            Manifest::save_cas(&*self.store, &new_manifest, self.manifest_etag.as_deref())
                .map_err(|e| {
                    anyhow::anyhow!(e).context(
                        "updating manifest after compaction — another writer may be active",
                    )
                })?;

        self.manifest_etag = Some(new_etag);
        self.sstable_id += 1;
        let merged_count = self.sstables.len();
        self.sstables = vec![new_sst];

        // Old objects are no longer reachable from the manifest a fresh
        // reader would load; readers that already have them in memory keep
        // their own copy regardless. Best-effort: a delete failure just
        // leaves an orphaned object in the store (same "space not reclaimed
        // on this error path" tradeoff `Partition::apply_retention` already
        // documents elsewhere), it doesn't fail the compaction that already
        // committed.
        for key in &old_keys {
            if let Err(e) = self.store.delete(key) {
                warn!(object = %key, error = %e, "failed to delete compacted-away sstable");
            }
        }

        info!(merged_count, entries = entries_len, sstable = %new_key, "compacted SSTables");
        Ok(())
    }

    /// Reload the manifest and fetch any SSTables it lists that we don't
    /// already have locally. Reader (replica) nodes call this on an interval
    /// to converge with whatever the writer has flushed.
    pub fn refresh(&mut self) -> Result<bool> {
        let Some((manifest, etag)) = Manifest::load(&*self.store)? else {
            return Ok(false);
        };
        if Some(&etag) == self.manifest_etag.as_ref() {
            return Ok(false);
        }

        let wanted: std::collections::HashSet<u64> = manifest.sstable_ids.iter().copied().collect();
        let known: std::collections::HashSet<u64> = self.sstables.iter().map(|s| s.id()).collect();
        let mut fetched = 0;
        for id in &manifest.sstable_ids {
            if known.contains(id) {
                continue;
            }
            let key = sstable_key(*id);
            let sst = SSTable::open_from_store(&*self.store, &key, *id)?;
            self.sstables.push(sst);
            fetched += 1;
        }

        // The manifest's set can also *shrink* — e.g. the writer compacted
        // several SSTables into one — so drop anything we're holding that's
        // no longer listed, or a reader's SSTable list would grow forever
        // right alongside the writer's, defeating the point of compaction.
        let before = self.sstables.len();
        self.sstables.retain(|s| wanted.contains(&s.id()));
        let dropped = before - self.sstables.len();

        self.sstables.sort_by_key(|s| s.id());
        self.manifest_etag = Some(etag);

        // Advance the commit-seq watermark to at least what the manifest
        // now reflects, so a plain (non-transactional) read on this reader
        // node sees everything it just fetched. Without this, a reader's
        // `current_seq()` stays frozen at whatever it was when the engine
        // was opened, and every row the writer commits afterward — even
        // though its SSTable gets fetched right here — stays invisible
        // (its `seq` is above the reader's stale watermark). Refresh only
        // ever pulls in newer state, so this never needs to move backward.
        self.next_commit_seq = self.next_commit_seq.max(manifest.max_commit_seq + 1);

        if fetched > 0 || dropped > 0 {
            info!(fetched, dropped, "refreshed manifest — SSTable set changed");
        }
        Ok(fetched > 0 || dropped > 0)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.trigger_flush()
    }
    pub fn wal(&self) -> &Wal {
        &self.wal
    }

    pub fn stats(&self) -> super::StorageStats {
        super::StorageStats {
            wal_bytes_written: self.wal.bytes_written(),
            memtable_entries: self.memtable.len(),
            sstable_count: self.sstables.len(),
            total_disk_bytes: self.sstables.iter().map(|s| s.blob_size()).sum(),
            open_snapshot_count: self.open_snapshots.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::lease::LeaseManager;
    use crate::storage::objectstore::LocalFsObjectStore;
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

    fn force_flush(engine: &mut LsmEngine, key: &[u8], value: &[u8]) {
        engine.write("t", key, value).unwrap();
        // trigger_flush() only fires automatically once the memtable hits its
        // byte threshold; tests write one small row per SSTable, so flush
        // explicitly instead of writing megabytes of filler.
        engine.flush().unwrap();
    }

    #[test]
    fn compaction_merges_sstables_and_bounds_the_list() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (mut engine, _store) = open_engine(bucket.path(), local.path());

        let _env_guard = COMPACTION_THRESHOLD_ENV_LOCK.lock().unwrap();
        std::env::set_var("TPT_COMPACTION_SSTABLE_THRESHOLD", "3");

        force_flush(&mut engine, b"a", b"1");
        force_flush(&mut engine, b"b", b"2");
        force_flush(&mut engine, b"a", b"1-updated");
        // The third flush pushes the SSTable count to the threshold, so this
        // flush's tail should have compacted all three down to one.
        assert_eq!(engine.sstables.len(), 1);

        assert_eq!(engine.read(b"a").unwrap(), Some(b"1-updated".to_vec()));
        assert_eq!(engine.read(b"b").unwrap(), Some(b"2".to_vec()));

        std::env::remove_var("TPT_COMPACTION_SSTABLE_THRESHOLD");
    }

    #[test]
    fn compaction_drops_tombstoned_keys() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (mut engine, _store) = open_engine(bucket.path(), local.path());

        force_flush(&mut engine, b"a", b"1");
        engine.delete("t", b"a").unwrap();
        engine.flush().unwrap();

        engine.compact_all().unwrap();
        assert_eq!(engine.sstables.len(), 1);
        assert_eq!(engine.read(b"a").unwrap(), None);
        assert_eq!(engine.scan().unwrap(), Vec::<(Vec<u8>, Vec<u8>)>::new());
    }

    #[test]
    fn reader_refresh_drops_sstables_removed_by_writer_compaction() {
        let bucket = tempfile::tempdir().unwrap();
        let writer_local = tempfile::tempdir().unwrap();
        let reader_local = tempfile::tempdir().unwrap();

        let (mut writer, store) = open_engine(bucket.path(), writer_local.path());
        force_flush(&mut writer, b"a", b"1");
        force_flush(&mut writer, b"b", b"2");

        let mut reader = LsmEngine::open(
            reader_local.path(),
            store.clone(),
            Arc::new(LeaseHandle::default()),
        )
        .unwrap();
        reader.refresh().unwrap();
        assert_eq!(reader.sstables.len(), 2);

        writer.compact_all().unwrap();
        assert_eq!(writer.sstables.len(), 1);

        reader.refresh().unwrap();
        assert_eq!(reader.sstables.len(), 1);
        assert_eq!(reader.read(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(reader.read(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn read_at_resolves_an_older_snapshot_across_a_later_write() {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let (mut engine, _store) = open_engine(bucket.path(), local.path());

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
        let (mut engine, _store) = open_engine(bucket.path(), local.path());

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
        let (mut engine, _store) = open_engine(bucket.path(), local.path());

        let _env_guard = COMPACTION_THRESHOLD_ENV_LOCK.lock().unwrap();
        std::env::set_var("TPT_COMPACTION_SSTABLE_THRESHOLD", "3");

        force_flush(&mut engine, b"k", b"v1");
        let snap = engine.current_seq();
        engine.register_snapshot(snap);

        force_flush(&mut engine, b"k", b"v2");
        // Third flush pushes the SSTable count to the threshold and triggers
        // compaction.
        force_flush(&mut engine, b"k", b"v3");
        assert_eq!(engine.sstables.len(), 1, "compaction should still have run");

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
        let (mut engine, _store) = open_engine(bucket.path(), local.path());

        let _env_guard = COMPACTION_THRESHOLD_ENV_LOCK.lock().unwrap();
        std::env::set_var("TPT_COMPACTION_SSTABLE_THRESHOLD", "2");

        force_flush(&mut engine, b"k", b"v1");
        let snap = engine.current_seq();
        engine.register_snapshot(snap);
        engine.release_snapshot(snap); // released immediately -- no longer pinned

        force_flush(&mut engine, b"k", b"v2"); // triggers compaction at threshold 2
        assert_eq!(engine.sstables.len(), 1);
        assert_eq!(engine.read(b"k").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(
            engine.read_at(b"k", snap).unwrap(),
            None,
            "the old version is gone once nothing holds its snapshot anymore"
        );

        std::env::remove_var("TPT_COMPACTION_SSTABLE_THRESHOLD");
    }
}
