use anyhow::Result;
use bloomfilter::Bloom;
use std::fs::{OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// A Sorted String Table (SSTable) — an immutable, on-disk data file
/// with a bloom filter for fast negative lookups and an index for binary search.
pub struct SSTable {
    path: PathBuf,
    id: u64,
    index: Vec<IndexEntry>,
    bloom: Bloom<Vec<u8>>,
    file_size: u64,
}

#[derive(Debug)]
struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
    value_len: u32,
}

impl SSTable {
    /// Create a new SSTable from sorted entries.
    pub fn create(path: &Path, entries: &[(Vec<u8>, Vec<u8>, u8)]) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

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

            index.push(IndexEntry {
                key: key.clone(),
                offset,
                value_len: value.len() as u32,
            });
        }

        let data_offset: u64 = 0;
        file.write_all(&data_buf)?;

        let index_offset = file.stream_position()?;
        file.write_all(&(index.len() as u32).to_be_bytes())?;
        for entry in &index {
            file.write_all(&(entry.key.len() as u32).to_be_bytes())?;
            file.write_all(&entry.key)?;
            file.write_all(&entry.offset.to_be_bytes())?;
            file.write_all(&entry.value_len.to_be_bytes())?;
        }

        let bloom_offset = file.stream_position()?;
        let bloom_bits = bloom.bitmap();
        let num_bits = bloom.number_of_bits();
        let num_hashes = bloom.number_of_hash_functions();
        let sip_keys = bloom.sip_keys();

        file.write_all(&(bloom_bits.len() as u32).to_be_bytes())?;
        file.write_all(&bloom_bits)?;
        file.write_all(&(num_bits as u64).to_be_bytes())?;
        file.write_all(&(num_hashes as u32).to_be_bytes())?;
        for (k0, k1) in &sip_keys {
            file.write_all(&k0.to_be_bytes())?;
            file.write_all(&k1.to_be_bytes())?;
        }

        file.write_all(&data_offset.to_be_bytes())?;
        file.write_all(&index_offset.to_be_bytes())?;
        file.write_all(&bloom_offset.to_be_bytes())?;

        file.sync_all()?;
        let file_size = file.metadata()?.len();
        drop(file);

        Self::open(path)
    }

    /// Open an existing SSTable.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let file_size = file.metadata()?.len();

        file.seek(SeekFrom::End(-24))?;
        let mut footer = [0u8; 24];
        file.read_exact(&mut footer)?;

        let data_offset = u64::from_be_bytes(footer[0..8].try_into()?);
        let index_offset = u64::from_be_bytes(footer[8..16].try_into()?);
        let bloom_offset = u64::from_be_bytes(footer[16..24].try_into()?);

        // Read index
        file.seek(SeekFrom::Start(index_offset))?;
        let mut count_buf = [0u8; 4];
        file.read_exact(&mut count_buf)?;
        let count = u32::from_be_bytes(count_buf) as usize;

        let mut index = Vec::with_capacity(count);
        for _ in 0..count {
            let mut key_len_buf = [0u8; 4];
            file.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_be_bytes(key_len_buf) as usize;
            let mut key = vec![0u8; key_len];
            file.read_exact(&mut key)?;

            let mut offset_buf = [0u8; 8];
            file.read_exact(&mut offset_buf)?;
            let offset = u64::from_be_bytes(offset_buf);

            let mut value_len_buf = [0u8; 4];
            file.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_be_bytes(value_len_buf);

            index.push(IndexEntry { key, offset, value_len });
        }

        // Read bloom filter
        file.seek(SeekFrom::Start(bloom_offset))?;
        let mut bits_len_buf = [0u8; 4];
        file.read_exact(&mut bits_len_buf)?;
        let bits_len = u32::from_be_bytes(bits_len_buf) as usize;
        let mut bits = vec![0u8; bits_len];
        file.read_exact(&mut bits)?;

        let mut num_bits_buf = [0u8; 8];
        file.read_exact(&mut num_bits_buf)?;
        let num_bits = u64::from_be_bytes(num_bits_buf);

        let mut num_hashes_buf = [0u8; 4];
        file.read_exact(&mut num_hashes_buf)?;
        let num_hashes = u32::from_be_bytes(num_hashes_buf);

        let mut sip_keys = Vec::new();
        for _ in 0..num_hashes as usize {
            let mut k0_buf = [0u8; 8];
            let mut k1_buf = [0u8; 8];
            file.read_exact(&mut k0_buf)?;
            file.read_exact(&mut k1_buf)?;
            sip_keys.push((u64::from_be_bytes(k0_buf), u64::from_be_bytes(k1_buf)));
        }

        // Rebuild bloom filter from stored parameters
        let bloom = Bloom::new_for_fp_rate_with_seed(
            (num_bits as f64 / 0.01) as usize,
            0.01,
            &[0u8; 32],
        );

        let id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.trim_start_matches("sst_").parse::<u64>().ok())
            .unwrap_or(0);

        Ok(Self { path: path.to_path_buf(), id, index, bloom, file_size })
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

        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        file.seek(SeekFrom::Start(entry.offset))?;

        let mut key_len_buf = [0u8; 4];
        file.read_exact(&mut key_len_buf)?;
        let key_len = u32::from_be_bytes(key_len_buf) as u64;
        file.seek(SeekFrom::Current(key_len as i64))?;

        let mut value_len_buf = [0u8; 4];
        file.read_exact(&mut value_len_buf)?;
        let value_len = u32::from_be_bytes(value_len_buf) as usize;
        let mut value = vec![0u8; value_len];
        file.read_exact(&mut value)?;

        Ok(Some(value))
    }

    /// Scan all entries in the SSTable.
    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        file.seek(SeekFrom::End(-24))?;
        let mut footer = [0u8; 24];
        file.read_exact(&mut footer)?;
        let index_offset = u64::from_be_bytes(footer[8..16].try_into()?);

        file.seek(SeekFrom::Start(0))?;
        let mut buf = vec![0u8; index_offset as usize];
        file.read_exact(&mut buf)?;

        let mut results = Vec::new();
        let mut pos = 0;
        while pos + 8 <= buf.len() {
            let key_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + key_len + 4 > buf.len() { break; }
            let key = buf[pos..pos + key_len].to_vec();
            pos += key_len;

            let value_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + value_len > buf.len() { break; }
            let value = buf[pos..pos + value_len].to_vec();
            pos += value_len;

            results.push((key, value));
        }
        Ok(results)
    }

    pub fn id(&self) -> u64 { self.id }
    pub fn file_size(&self) -> u64 { self.file_size }
    pub fn path(&self) -> &Path { &self.path }
}