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
        self.data.iter().map(|(k, (v, t))| (k.as_slice(), v.as_slice(), *t))
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
}

impl LsmEngine {
    /// Open the engine. `local_dir` holds only the active WAL segment (a
    /// cache/staging area); `store` is the shared durable backend. `lease`
    /// is consulted before every flush — a node without a valid lease cannot
    /// advance the manifest (see `storage::lease`).
    pub fn open(local_dir: &Path, store: Arc<dyn ObjectStore>, lease: Arc<LeaseHandle>) -> Result<Self> {
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

        let sstable_id = manifest.sstable_ids.iter().max().map(|m| m + 1).unwrap_or(1);
        let wal_seg_id = manifest.wal_segment_seq + 1;

        info!(sstable_count = sstables.len(), "SSTables loaded from manifest");

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
                if value.is_empty() { return Ok(None); }
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for sst in &self.sstables {
            for (k, v) in sst.scan()? {
                if !v.is_empty() { merged.insert(k, v); }
                else { merged.remove(&k); }
            }
        }
        if let Some(ref imm) = self.immutable {
            for (k, v, t) in imm.iter() {
                if t != 2 { merged.insert(k.to_vec(), v.to_vec()); }
                else { merged.remove(k); }
            }
        }
        for (k, v, t) in self.memtable.iter() {
            if t != 2 { merged.insert(k.to_vec(), v.to_vec()); }
            else { merged.remove(k); }
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
            if entries.is_empty() { return Ok(()); }

            let sst_key = sstable_key(self.sstable_id);
            let sst = SSTable::create_in_store(&*self.store, &sst_key, self.sstable_id, &entries)?;

            // Ship the sealed WAL segment to the shared store *before*
            // truncating the local copy, so a stateless compute node never
            // depends solely on this node's local disk for durability.
            let wal_bytes = self.wal.read_all_bytes()?;
            if !wal_bytes.is_empty() {
                self.store.put(&wal_segment_key(self.wal_seg_id), &wal_bytes)?;
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
            let new_etag = Manifest::save_cas(&*self.store, &new_manifest, self.manifest_etag.as_deref())
                .map_err(|e| anyhow::anyhow!(e).context("updating manifest after flush — another writer may be active"))?;

            self.manifest_etag = Some(new_etag);
            self.wal_seg_id += 1;
            self.sstables.push(sst);
            self.sstable_id += 1;

            self.wal.truncate()?;
            info!(sstable = %sst_key, entries = entries.len(), "SSTable flushed to object store");
        }
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
        self.sstables.sort_by_key(|s| s.id());
        self.manifest_etag = Some(etag);
        if fetched > 0 {
            info!(fetched, "refreshed manifest — picked up new SSTables");
        }
        Ok(fetched > 0)
    }

    pub fn flush(&mut self) -> Result<()> { self.trigger_flush() }
    pub fn wal(&self) -> &Wal { &self.wal }

    pub fn stats(&self) -> super::StorageStats {
        super::StorageStats {
            wal_bytes_written: self.wal.bytes_written(),
            memtable_entries: self.memtable.len(),
            sstable_count: self.sstables.len(),
            total_disk_bytes: self.sstables.iter().map(|s| s.blob_size()).sum(),
        }
    }
}
