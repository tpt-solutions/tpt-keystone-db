use super::objectstore::ObjectStore;
use anyhow::{bail, Result};
use bloomfilter::Bloom;
use std::sync::Arc;

/// A Sorted String Table (SSTable) — an immutable data blob, addressed by an
/// object-store key, with a bloom filter for fast negative lookups and an
/// index for binary search. The on-disk/on-wire binary layout is unchanged
/// from the original local-file version; only *where the bytes live* has
/// moved (see `storage::objectstore`). Because SSTables are flush-sized
/// (a few MB at most), the whole decoded blob is kept in memory once fetched
/// — there is no more per-read file reopen/seek.
pub struct SSTable {
    key: String,
    id: u64,
    index: Vec<IndexEntry>,
    bloom: Bloom<Vec<u8>>,
    data: Arc<Vec<u8>>,
}

#[derive(Debug)]
struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
    value_len: u32,
}

fn read_u32(buf: &[u8], pos: usize) -> Result<u32> {
    let end = pos + 4;
    if end > buf.len() {
        bail!("truncated sstable buffer (want u32 at {pos})");
    }
    Ok(u32::from_be_bytes(buf[pos..end].try_into().unwrap()))
}

fn read_u64(buf: &[u8], pos: usize) -> Result<u64> {
    let end = pos + 8;
    if end > buf.len() {
        bail!("truncated sstable buffer (want u64 at {pos})");
    }
    Ok(u64::from_be_bytes(buf[pos..end].try_into().unwrap()))
}

impl SSTable {
    /// Serialize sorted entries into the SSTable binary format:
    /// `data section | index section | bloom section | 24-byte footer`.
    fn build_bytes(entries: &[(Vec<u8>, Vec<u8>, u8)]) -> Vec<u8> {
        let mut index = Vec::with_capacity(entries.len());
        let mut bloom = Bloom::new_for_fp_rate(entries.len().max(1), 0.01);
        let mut data_buf = Vec::new();

        for (key, value, record_type) in entries {
            bloom.set(key);
            let offset = data_buf.len() as u64;

            if *record_type == 2 {
                index.push(IndexEntry { key: key.clone(), offset, value_len: 0 });
                continue;
            }

            data_buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
            data_buf.extend_from_slice(key);
            data_buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
            data_buf.extend_from_slice(value);

            index.push(IndexEntry { key: key.clone(), offset, value_len: value.len() as u32 });
        }

        let mut buf = data_buf;
        let index_offset = buf.len() as u64;
        buf.extend_from_slice(&(index.len() as u32).to_be_bytes());
        for entry in &index {
            buf.extend_from_slice(&(entry.key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&entry.key);
            buf.extend_from_slice(&entry.offset.to_be_bytes());
            buf.extend_from_slice(&entry.value_len.to_be_bytes());
        }

        let bloom_offset = buf.len() as u64;
        let bloom_bits = bloom.bitmap();
        let num_bits = bloom.number_of_bits();
        let num_hashes = bloom.number_of_hash_functions();
        let sip_keys = bloom.sip_keys();

        buf.extend_from_slice(&(bloom_bits.len() as u32).to_be_bytes());
        buf.extend_from_slice(&bloom_bits);
        buf.extend_from_slice(&(num_bits as u64).to_be_bytes());
        buf.extend_from_slice(&(num_hashes as u32).to_be_bytes());
        for (k0, k1) in &sip_keys {
            buf.extend_from_slice(&k0.to_be_bytes());
            buf.extend_from_slice(&k1.to_be_bytes());
        }

        buf.extend_from_slice(&0u64.to_be_bytes()); // data_offset (always 0)
        buf.extend_from_slice(&index_offset.to_be_bytes());
        buf.extend_from_slice(&bloom_offset.to_be_bytes());

        buf
    }

    /// Parse a decoded blob (as produced by `build_bytes`) into an `SSTable`.
    fn decode(key: String, id: u64, data: Arc<Vec<u8>>) -> Result<Self> {
        let buf = data.as_slice();
        if buf.len() < 24 {
            bail!("sstable blob too small to contain a footer");
        }
        let footer_start = buf.len() - 24;
        let index_offset = read_u64(buf, footer_start + 8)? as usize;
        let bloom_offset = read_u64(buf, footer_start + 16)? as usize;

        // Index
        let mut pos = index_offset;
        let count = read_u32(buf, pos)? as usize;
        pos += 4;
        let mut index = Vec::with_capacity(count);
        for _ in 0..count {
            let key_len = read_u32(buf, pos)? as usize;
            pos += 4;
            let end = pos + key_len;
            if end > buf.len() {
                bail!("truncated sstable index entry");
            }
            let entry_key = buf[pos..end].to_vec();
            pos = end;

            let offset = read_u64(buf, pos)?;
            pos += 8;
            let value_len = read_u32(buf, pos)?;
            pos += 4;

            index.push(IndexEntry { key: entry_key, offset, value_len });
        }

        // Bloom filter: bitmap, bit count, hash-function count, then exactly
        // two SipHash key pairs (`Bloom::sip_keys()` always returns 2
        // regardless of hash-function count — see `build_bytes`).
        let mut pos = bloom_offset;
        let bits_len = read_u32(buf, pos)? as usize;
        pos += 4;
        let bits_end = pos + bits_len;
        if bits_end > buf.len() {
            bail!("truncated sstable bloom bitmap");
        }
        let bits = buf[pos..bits_end].to_vec();
        pos = bits_end;
        let num_bits = read_u64(buf, pos)?;
        pos += 8;
        let num_hashes = read_u32(buf, pos)?;
        pos += 4;

        let mut sip_keys = [(0u64, 0u64); 2];
        for slot in &mut sip_keys {
            let k0 = read_u64(buf, pos)?;
            pos += 8;
            let k1 = read_u64(buf, pos)?;
            pos += 8;
            *slot = (k0, k1);
        }

        let bloom = Bloom::from_existing(&bits, num_bits, num_hashes, sip_keys);

        Ok(Self { key, id, index, bloom, data })
    }

    /// Build a new SSTable from sorted entries and persist it to `store`
    /// under `key`.
    pub fn create_in_store(store: &dyn ObjectStore, key: &str, id: u64, entries: &[(Vec<u8>, Vec<u8>, u8)]) -> Result<Self> {
        let bytes = Self::build_bytes(entries);
        store.put(key, &bytes)?;
        Self::decode(key.to_string(), id, Arc::new(bytes))
    }

    /// Fetch and parse an existing SSTable from `store` (served through the
    /// caller's cache layer, if any).
    pub fn open_from_store(store: &dyn ObjectStore, key: &str, id: u64) -> Result<Self> {
        let (bytes, _meta) = store
            .get(key)?
            .ok_or_else(|| anyhow::anyhow!("sstable object {key} not found"))?;
        Self::decode(key.to_string(), id, Arc::new(bytes))
    }

    /// Read a value by key using binary search on the index + bloom filter.
    pub fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.bloom.check(&key.to_vec()) {
            return Ok(None);
        }

        let entry = match self.index.binary_search_by(|e| e.key.as_slice().cmp(key)) {
            Ok(idx) => &self.index[idx],
            Err(_) => return Ok(None),
        };

        if entry.value_len == 0 {
            return Ok(Some(Vec::new()));
        }

        let buf = self.data.as_slice();
        let mut pos = entry.offset as usize;
        let key_len = read_u32(buf, pos)? as usize;
        pos += 4 + key_len;
        let value_len = read_u32(buf, pos)? as usize;
        pos += 4;
        let end = pos + value_len;
        if end > buf.len() {
            bail!("truncated sstable data record");
        }
        Ok(Some(buf[pos..end].to_vec()))
    }

    /// Scan all entries in the SSTable.
    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let buf = self.data.as_slice();
        let footer_start = buf.len() - 24;
        let index_offset = read_u64(buf, footer_start + 8)? as usize;

        let mut results = Vec::new();
        let mut pos = 0;
        while pos + 8 <= index_offset {
            let key_len = read_u32(buf, pos)? as usize;
            pos += 4;
            if pos + key_len + 4 > index_offset {
                break;
            }
            let key = buf[pos..pos + key_len].to_vec();
            pos += key_len;

            let value_len = read_u32(buf, pos)? as usize;
            pos += 4;
            if pos + value_len > index_offset {
                break;
            }
            let value = buf[pos..pos + value_len].to_vec();
            pos += value_len;

            results.push((key, value));
        }
        Ok(results)
    }

    pub fn id(&self) -> u64 { self.id }
    pub fn object_key(&self) -> &str { &self.key }
    pub fn blob_size(&self) -> u64 { self.data.len() as u64 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::objectstore::LocalFsObjectStore;

    #[test]
    fn create_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec(), 0u8),
            (b"b".to_vec(), b"2".to_vec(), 0u8),
            (b"c".to_vec(), Vec::new(), 2u8), // tombstone
        ];
        let sst = SSTable::create_in_store(&store, "sst/1", 1, &entries).unwrap();
        assert_eq!(sst.read(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(sst.read(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(sst.read(b"c").unwrap(), Some(Vec::new()));
        assert_eq!(sst.read(b"z").unwrap(), None);

        let reopened = SSTable::open_from_store(&store, "sst/1", 1).unwrap();
        assert_eq!(reopened.read(b"a").unwrap(), Some(b"1".to_vec()));
        let scanned = reopened.scan().unwrap();
        assert_eq!(scanned, vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec())]);
    }
}
