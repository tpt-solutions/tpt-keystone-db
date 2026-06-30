use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;
use tracing::info;

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

pub struct LsmEngine {
    memtable: MemTable,
    immutable: Option<MemTable>,
    sstables: Vec<SSTable>,
    wal: Wal,
    data_dir: std::path::PathBuf,
    sstable_id: u64,
}

impl LsmEngine {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let data_dir = dir.to_path_buf();

        let wal = Wal::open(dir)?;
        let mut memtable = MemTable::new(4 * 1024 * 1024);

        let mut recovered = 0u64;
        wal.replay(|record: WalRecord| {
            memtable.insert(record.key, record.value, record.record_type);
            recovered += 1;
        })?;

        if recovered > 0 {
            info!(recovered, "WAL replay complete");
        }

        let mut sstables = Vec::new();
        let mut max_id = 0u64;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_stem() {
                    if let Some(name_str) = name.to_str() {
                        if name_str.starts_with("sst_") {
                            if let Ok(id) = name_str.trim_start_matches("sst_").parse::<u64>() {
                                max_id = max_id.max(id);
                                if let Ok(sst) = SSTable::open(&path) {
                                    sstables.push(sst);
                                }
                            }
                        }
                    }
                }
            }
        }

        sstables.sort_by_key(|s| s.id());
        info!(sstable_count = sstables.len(), "SSTables loaded");

        Ok(Self {
            memtable,
            immutable: None,
            sstables,
            wal,
            data_dir,
            sstable_id: max_id + 1,
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
        let old_mem = std::mem::replace(&mut self.memtable, MemTable::new(4 * 1024 * 1024));
        self.immutable = Some(old_mem);

        if let Some(mut imm) = self.immutable.take() {
            let entries: Vec<_> = imm.drain();
            if entries.is_empty() { return Ok(()); }

            let sst_path = self.data_dir.join(format!("sst_{:020}", self.sstable_id));
            self.sstable_id += 1;

            let sst = SSTable::create(&sst_path, &entries)?;
            self.sstables.push(sst);
            self.wal.truncate()?;
            info!(path = %sst_path.display(), entries = entries.len(), "SSTable flushed");
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> { self.trigger_flush() }
    pub fn wal(&self) -> &Wal { &self.wal }

    pub fn stats(&self) -> super::StorageStats {
        super::StorageStats {
            wal_bytes_written: self.wal.bytes_written(),
            memtable_entries: self.memtable.len(),
            sstable_count: self.sstables.len(),
            total_disk_bytes: self.sstables.iter().map(|s| s.file_size()).sum(),
        }
    }
}