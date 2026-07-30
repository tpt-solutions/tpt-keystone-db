use super::internal_key::{InternalKey, DELETE_TAG};
use super::objectstore::ObjectStore;
use anyhow::{bail, Result};
use bloomfilter::Bloom;
use std::sync::Arc;

/// A Sorted String Table (SSTable) — an immutable data blob, addressed by an
/// object-store key, with a bloom filter for fast negative lookups and an
/// index for binary search. Because SSTables are flush-sized (a few MB at
/// most), the whole decoded blob is kept in memory once fetched — there is
/// no more per-read file reopen/seek.
///
/// Every entry in an SSTable is tagged with the MVCC commit-sequence number
/// (and put/delete tag) of the write that produced it (`storage::internal_key`),
/// so a single user key can have several live versions here at once — the
/// index, not the data section, carries that metadata (see `IndexEntry`).
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
    seq: u64,
    tag: u8,
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

/// Read the value bytes for a live (non-tombstone) index entry out of the
/// data section. Works unmodified for an entry whose value happens to be
/// zero-length (the data record's `value_len` field is simply `0`, and
/// slicing `buf[pos..pos]` yields an empty `Vec` directly) — no special
/// casing needed, unlike tombstone detection, which is driven entirely by
/// `IndexEntry::tag` now rather than by an ambiguous `value_len == 0` check.
fn read_value_at(buf: &[u8], entry: &IndexEntry) -> Result<Vec<u8>> {
    let mut pos = entry.offset as usize;
    let key_len = read_u32(buf, pos)? as usize;
    pos += 4 + key_len;
    let value_len = read_u32(buf, pos)? as usize;
    pos += 4;
    let end = pos + value_len;
    if end > buf.len() {
        bail!("truncated sstable data record");
    }
    Ok(buf[pos..end].to_vec())
}

impl SSTable {
    /// Serialize sorted entries into the SSTable binary format:
    /// `data section | index section | bloom section | 24-byte footer`.
    /// `entries` must already be in `InternalKey` order (user key ascending,
    /// then seq descending within a key) — callers hand this off straight
    /// from a `BTreeMap<InternalKey, _>` traversal, so that's automatic.
    fn build_bytes(entries: &[(InternalKey, Vec<u8>)]) -> Vec<u8> {
        let mut index = Vec::with_capacity(entries.len());
        let mut bloom = Bloom::new_for_fp_rate(entries.len().max(1), 0.01);
        let mut data_buf = Vec::new();

        for (ikey, value) in entries {
            bloom.set(&ikey.user_key);
            let offset = data_buf.len() as u64;

            if ikey.tag == DELETE_TAG {
                index.push(IndexEntry {
                    key: ikey.user_key.clone(),
                    seq: ikey.seq,
                    tag: ikey.tag,
                    offset,
                    value_len: 0,
                });
                continue;
            }

            data_buf.extend_from_slice(&(ikey.user_key.len() as u32).to_be_bytes());
            data_buf.extend_from_slice(&ikey.user_key);
            data_buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
            data_buf.extend_from_slice(value);

            index.push(IndexEntry {
                key: ikey.user_key.clone(),
                seq: ikey.seq,
                tag: ikey.tag,
                offset,
                value_len: value.len() as u32,
            });
        }

        let mut buf = data_buf;
        let index_offset = buf.len() as u64;
        buf.extend_from_slice(&(index.len() as u32).to_be_bytes());
        for entry in &index {
            buf.extend_from_slice(&(entry.key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&entry.key);
            buf.extend_from_slice(&entry.seq.to_be_bytes());
            buf.push(entry.tag);
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

            let seq = read_u64(buf, pos)?;
            pos += 8;
            if pos >= buf.len() {
                bail!("truncated sstable index entry (tag)");
            }
            let tag = buf[pos];
            pos += 1;

            let offset = read_u64(buf, pos)?;
            pos += 8;
            let value_len = read_u32(buf, pos)?;
            pos += 4;

            index.push(IndexEntry {
                key: entry_key,
                seq,
                tag,
                offset,
                value_len,
            });
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

        Ok(Self {
            key,
            id,
            index,
            bloom,
            data,
        })
    }

    /// Build a new SSTable from sorted entries and persist it to `store`
    /// under `key`.
    pub fn create_in_store(
        store: &dyn ObjectStore,
        key: &str,
        id: u64,
        entries: &[(InternalKey, Vec<u8>)],
    ) -> Result<Self> {
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

    /// Read the version of `key` visible to `snapshot_seq`. The outer
    /// `Option` distinguishes "this table has no version of `key` visible to
    /// this snapshot at all" (`None` — the caller must fall through to the
    /// next, older source) from "this table *does* have a visible version"
    /// (`Some`), which is either a live value (`Some(Some(v))`) or a
    /// tombstone (`Some(None)`) — critically, a tombstone must stop the
    /// caller's fallthrough too, since a newer table's delete must shadow an
    /// older table's live value for the same key, not be skipped past.
    pub fn read_at(&self, key: &[u8], snapshot_seq: u64) -> Result<Option<Option<Vec<u8>>>> {
        if !self.bloom.check(&key.to_vec()) {
            return Ok(None);
        }

        // Index is sorted by user key ascending, then seq descending within
        // a key (`InternalKey`'s `Ord`). `partition_point` finds the first
        // entry that is *not* "strictly before the seek point" — i.e. the
        // first entry with this key whose seq is <= snapshot_seq, or the
        // first entry of the next key if every version here is too new.
        let idx = self.index.partition_point(|e| match e.key.as_slice().cmp(key) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => e.seq > snapshot_seq,
            std::cmp::Ordering::Greater => false,
        });
        let Some(entry) = self.index.get(idx) else {
            return Ok(None);
        };
        if entry.key != key {
            return Ok(None);
        }
        if entry.tag == DELETE_TAG {
            return Ok(Some(None));
        }
        Ok(Some(Some(read_value_at(self.data.as_slice(), entry)?)))
    }

    /// Highest `seq` stored in this table, or `0` if it holds no entries.
    /// Used to seed `LsmEngine`'s `next_commit_seq` watermark on restart so a
    /// fresh writer never reissues a `commit_seq` already burned into an
    /// SSTable.
    pub fn max_seq(&self) -> u64 {
        self.index.iter().map(|e| e.seq).max().unwrap_or(0)
    }

    /// Every retained version of every key in this table, in on-disk
    /// (`InternalKey`) order: `(user_key, seq, tag, value_or_none)`, `None`
    /// for a tombstone. Used by compaction and multi-version scans, which
    /// need to see every version to decide what's still visible to some open
    /// snapshot — unlike `read_at`, which only needs the one version a
    /// single snapshot resolves to.
    pub fn scan_all_versions(&self) -> Result<Vec<(Vec<u8>, u64, u8, Option<Vec<u8>>)>> {
        let buf = self.data.as_slice();
        let mut results = Vec::with_capacity(self.index.len());
        for entry in &self.index {
            if entry.tag == DELETE_TAG {
                results.push((entry.key.clone(), entry.seq, entry.tag, None));
                continue;
            }
            let value = read_value_at(buf, entry)?;
            results.push((entry.key.clone(), entry.seq, entry.tag, Some(value)));
        }
        Ok(results)
    }

    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn object_key(&self) -> &str {
        &self.key
    }
    pub fn blob_size(&self) -> u64 {
        self.data.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::internal_key::PUT_TAG;
    use crate::storage::objectstore::LocalFsObjectStore;

    fn ik(key: &[u8], seq: u64, tag: u8) -> InternalKey {
        InternalKey::new(key.to_vec(), seq, tag)
    }

    #[test]
    fn create_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        let entries = vec![
            (ik(b"a", 1, PUT_TAG), b"1".to_vec()),
            (ik(b"b", 1, PUT_TAG), b"2".to_vec()),
            (ik(b"c", 1, DELETE_TAG), Vec::new()), // tombstone
        ];
        let sst = SSTable::create_in_store(&store, "sst/1", 1, &entries).unwrap();
        assert_eq!(sst.read_at(b"a", 10).unwrap(), Some(Some(b"1".to_vec())));
        assert_eq!(sst.read_at(b"b", 10).unwrap(), Some(Some(b"2".to_vec())));
        assert_eq!(sst.read_at(b"c", 10).unwrap(), Some(None), "tombstone");
        assert_eq!(sst.read_at(b"z", 10).unwrap(), None, "no entry at all");

        let reopened = SSTable::open_from_store(&store, "sst/1", 1).unwrap();
        assert_eq!(reopened.read_at(b"a", 10).unwrap(), Some(Some(b"1".to_vec())));
        let scanned = reopened.scan_all_versions().unwrap();
        assert_eq!(
            scanned,
            vec![
                (b"a".to_vec(), 1, PUT_TAG, Some(b"1".to_vec())),
                (b"b".to_vec(), 1, PUT_TAG, Some(b"2".to_vec())),
                (b"c".to_vec(), 1, DELETE_TAG, None),
            ]
        );
    }

    #[test]
    fn read_at_resolves_the_right_version_for_the_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        // Three versions of "k": put@50, put@90, delete@100 — sorted newest
        // (highest seq) first per `InternalKey::Ord`.
        let entries = vec![
            (ik(b"k", 100, DELETE_TAG), Vec::new()),
            (ik(b"k", 90, PUT_TAG), b"v90".to_vec()),
            (ik(b"k", 50, PUT_TAG), b"v50".to_vec()),
        ];
        let sst = SSTable::create_in_store(&store, "sst/1", 1, &entries).unwrap();

        assert_eq!(sst.read_at(b"k", 40).unwrap(), None, "before any version exists");
        assert_eq!(sst.read_at(b"k", 50).unwrap(), Some(Some(b"v50".to_vec())));
        assert_eq!(sst.read_at(b"k", 89).unwrap(), Some(Some(b"v50".to_vec())));
        assert_eq!(sst.read_at(b"k", 90).unwrap(), Some(Some(b"v90".to_vec())));
        assert_eq!(sst.read_at(b"k", 99).unwrap(), Some(Some(b"v90".to_vec())));
        assert_eq!(
            sst.read_at(b"k", 100).unwrap(),
            Some(None),
            "deleted as of seq 100"
        );
        assert_eq!(
            sst.read_at(b"k", 1000).unwrap(),
            Some(None),
            "still deleted later"
        );
    }

    #[test]
    fn empty_value_put_is_distinct_from_a_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        let entries = vec![(ik(b"k", 1, PUT_TAG), Vec::new())];
        let sst = SSTable::create_in_store(&store, "sst/1", 1, &entries).unwrap();
        assert_eq!(
            sst.read_at(b"k", 10).unwrap(),
            Some(Some(Vec::new())),
            "an empty-value put must read back Some(Some(empty)), not a tombstone"
        );
    }

    #[test]
    fn max_seq_reports_the_highest_stored_seq() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        let entries = vec![
            (ik(b"a", 5, PUT_TAG), b"1".to_vec()),
            (ik(b"b", 12, PUT_TAG), b"2".to_vec()),
        ];
        let sst = SSTable::create_in_store(&store, "sst/1", 1, &entries).unwrap();
        assert_eq!(sst.max_seq(), 12);
    }
}
