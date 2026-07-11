use anyhow::{bail, Result};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

use super::lease::LeaseHandle;
use super::manifest::Manifest;
use super::objectstore::ObjectStore;
use super::sstable::SSTable;
use super::wal::{Wal, WalRecord};

/// A MemTable is an in-memory write buffer backed by a BTreeMap.
/// When it reaches a threshold size, it is frozen and flushed to an SSTable.
pub struct MemTable {
    data: BTreeMap<Vec<u8>, (Vec<u8>, u8)>, // key -> (value, record_type)
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

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>, record_type: u8) {
        let entry_size = key.len() + value.len() + 8;
        self.size_bytes += entry_size;
        if let Some((old_val, _)) = self.data.insert(key, (value, record_type)) {
            self.size_bytes = self.size_bytes.saturating_sub(old_val.len() + 8);
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&(Vec<u8>, u8)> {
        self.data.get(key)
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.max_size
    }

    pub fn drain(&mut self) -> Vec<(Vec<u8>, Vec<u8>, u8)> {
        let result: Vec<_> = std::mem::take(&mut self.data)
            .into_iter()
            .map(|(k, (v, t))| (k, v, t))
            .collect();
        self.size_bytes = 0;
        result
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8], u8)> {
        self.data
            .iter()
            .map(|(k, (v, t))| (k.as_slice(), v.as_slice(), *t))
    }
}

fn sstable_key(id: u64) -> String {
    format!("sst/{id:020}")
}

fn wal_segment_key(id: u64) -> String {
    format!("wal/seg_{id:020}")
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
        wal.replay(|record: WalRecord| {
            memtable.insert(record.key, record.value, record.record_type);
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
        for id in &manifest.sstable_ids {
            let key = sstable_key(*id);
            match SSTable::open_from_store(&*store, &key, *id) {
                Ok(sst) => sstables.push(sst),
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
        })
    }

    pub fn write(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.wal.append(table, key, value, 0)?;
        self.memtable.insert(key.to_vec(), value.to_vec(), 0);
        if self.memtable.is_full() {
            self.trigger_flush()?;
        }
        Ok(())
    }

    pub fn delete(&mut self, table: &str, key: &[u8]) -> Result<()> {
        self.wal.append(table, key, &[], 2)?;
        self.memtable.insert(key.to_vec(), Vec::new(), 2);
        if self.memtable.is_full() {
            self.trigger_flush()?;
        }
        Ok(())
    }

    pub fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((value, record_type)) = self.memtable.get(key) {
            return match record_type {
                2 => Ok(None),
                _ => Ok(Some(value.clone())),
            };
        }
        if let Some(ref imm) = self.immutable {
            if let Some((value, record_type)) = imm.get(key) {
                return match record_type {
                    2 => Ok(None),
                    _ => Ok(Some(value.clone())),
                };
            }
        }
        for sst in self.sstables.iter().rev() {
            if let Some(value) = sst.read(key)? {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for sst in &self.sstables {
            for (k, v) in sst.scan()? {
                match v {
                    Some(val) => {
                        merged.insert(k, val);
                    }
                    None => {
                        merged.remove(&k);
                    }
                }
            }
        }
        if let Some(ref imm) = self.immutable {
            for (k, v, t) in imm.iter() {
                if t != 2 {
                    merged.insert(k.to_vec(), v.to_vec());
                } else {
                    merged.remove(k);
                }
            }
        }
        for (k, v, t) in self.memtable.iter() {
            if t != 2 {
                merged.insert(k.to_vec(), v.to_vec());
            } else {
                merged.remove(k);
            }
        }
        Ok(merged.into_iter().collect())
    }

    fn trigger_flush(&mut self) -> Result<()> {
        if !self.lease.is_valid() {
            bail!("fenced off: this node no longer holds the write lease");
        }

        let old_mem = std::mem::replace(&mut self.memtable, MemTable::new(4 * 1024 * 1024));
        self.immutable = Some(old_mem);

        if let Some(mut imm) = self.immutable.take() {
            let entries: Vec<_> = imm.drain();
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

    /// Merge every current SSTable into a single new one, dropping any key
    /// shadowed by a newer table's write or tombstone. This is a full
    /// (size-tiered, not true multi-level) compaction — simpler than real
    /// LSM levelling, but it does the two things that actually matter:
    /// bounds the SSTable list so `read`/`scan` stop growing unboundedly, and
    /// reclaims space by dropping overwritten/tombstoned keys instead of
    /// carrying them forward forever.
    pub fn compact_all(&mut self) -> Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }
        if !self.lease.is_valid() {
            bail!("fenced off: this node no longer holds the write lease");
        }

        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for sst in &self.sstables {
            for (k, v) in sst.scan()? {
                match v {
                    Some(val) => {
                        merged.insert(k, val);
                    }
                    None => {
                        merged.remove(&k);
                    }
                }
            }
        }

        let entries: Vec<(Vec<u8>, Vec<u8>, u8)> =
            merged.into_iter().map(|(k, v)| (k, v, 0u8)).collect();

        let old_keys: Vec<String> = self
            .sstables
            .iter()
            .map(|s| s.object_key().to_string())
            .collect();

        let new_id = self.sstable_id;
        let new_key = sstable_key(new_id);
        let new_sst = SSTable::create_in_store(&*self.store, &new_key, new_id, &entries)?;

        let new_manifest = Manifest {
            sstable_ids: vec![new_id],
            wal_segment_seq: self.committed_wal_segment_seq,
            writer_fencing_token: self.lease.token(),
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

        info!(merged_count, entries = entries.len(), sstable = %new_key, "compacted SSTables");
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
}
