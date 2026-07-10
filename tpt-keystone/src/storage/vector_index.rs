//! Local (per-node, not object-store-replicated — the same documented
//! scope cut `storage::btree`/`storage::geo_index`'s indexes already carry)
//! vector index for Prism `VECTOR` columns.
//!
//! Wraps `vector::hnsw::HnswIndex` (a real, from-scratch HNSW graph) the same
//! way `storage::geo_index::GeoIndex` wraps `geo::s2` cell bucketing: an
//! append-only on-disk log of `(row_key, vector)` records is replayed fully
//! into an in-memory HNSW graph on open. `HnswIndex` assigns dense,
//! insertion-order internal ids; `row_keys[id]` maps an internal id back to
//! the row it came from, mirroring how the HNSW module's own doc comment
//! says "callers map this to a row key externally."
//!
//! Same scope cut as the HNSW index itself: insert-and-search only, no
//! delete/update (a row update re-inserts a new HNSW node rather than
//! mutating the old one, so a stale entry can linger until the index is
//! rebuilt — acceptable for a local secondary-index accelerator, consistent
//! with `storage::btree`'s own no-delete precedent).

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::vector::hnsw::{HnswConfig, HnswIndex, Metric};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VectorEntry {
    row_key: Vec<u8>,
    vector: Vec<f32>,
}

fn metric_to_u8(m: Metric) -> u8 {
    match m {
        Metric::L2 => 0,
        Metric::Cosine => 1,
    }
}

fn metric_from_u8(b: u8) -> Metric {
    match b {
        1 => Metric::Cosine,
        _ => Metric::L2,
    }
}

pub struct VectorIndex {
    path: PathBuf,
    metric: Metric,
    config: HnswConfig,
    hnsw: HnswIndex,
    /// Internal HNSW id (dense, insertion order) → row key.
    row_keys: Vec<Vec<u8>>,
}

impl VectorIndex {
    /// Opens (replaying any existing records) or creates a fresh vector
    /// index file. `default_metric`/`default_config` are only used when
    /// creating a brand-new file — a later open reads the metric/config back
    /// from the file's own header, mirroring `GeoIndex::open`'s `level`.
    pub fn open(path: &Path, default_metric: Metric, default_config: HnswConfig) -> Result<Self> {
        if !path.exists() {
            let idx = Self {
                path: path.to_path_buf(),
                metric: default_metric,
                config: default_config,
                hnsw: HnswIndex::new(default_metric, default_config),
                row_keys: Vec::new(),
            };
            idx.write_header()?;
            return Ok(idx);
        }

        let mut file = BufReader::new(File::open(path)?);
        let mut header = [0u8; 17];
        file.read_exact(&mut header)?;
        let metric = metric_from_u8(header[0]);
        let m = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        let m0 = u32::from_be_bytes(header[5..9].try_into().unwrap()) as usize;
        let ef_construction = u32::from_be_bytes(header[9..13].try_into().unwrap()) as usize;
        let ef_search = u32::from_be_bytes(header[13..17].try_into().unwrap()) as usize;
        let config = HnswConfig { m, m0, ef_construction, ef_search };

        let mut hnsw = HnswIndex::new(metric, config);
        let mut row_keys = Vec::new();
        let mut len_buf = [0u8; 4];
        loop {
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            let entry: VectorEntry = bincode::deserialize(&buf)?;
            hnsw.insert(entry.vector);
            row_keys.push(entry.row_key);
        }

        Ok(Self { path: path.to_path_buf(), metric, config, hnsw, row_keys })
    }

    fn write_header(&self) -> Result<()> {
        let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(&self.path)?;
        let mut header = Vec::with_capacity(17);
        header.push(metric_to_u8(self.metric));
        header.extend_from_slice(&(self.config.m as u32).to_be_bytes());
        header.extend_from_slice(&(self.config.m0 as u32).to_be_bytes());
        header.extend_from_slice(&(self.config.ef_construction as u32).to_be_bytes());
        header.extend_from_slice(&(self.config.ef_search as u32).to_be_bytes());
        file.write_all(&header)?;
        Ok(())
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn config(&self) -> HnswConfig {
        self.config
    }

    pub fn len(&self) -> usize {
        self.row_keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_keys.is_empty()
    }

    /// Indexes one row's vector value. Appends to the on-disk log and
    /// inserts into the in-memory HNSW graph.
    pub fn insert(&mut self, row_key: &[u8], vector: Vec<f32>) -> Result<()> {
        let entry = VectorEntry { row_key: row_key.to_vec(), vector: vector.clone() };
        let encoded = bincode::serialize(&entry)?;
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&(encoded.len() as u32).to_be_bytes())?;
        file.write_all(&encoded)?;
        let id = self.hnsw.insert(vector);
        debug_assert_eq!(id, self.row_keys.len());
        self.row_keys.push(row_key.to_vec());
        Ok(())
    }

    /// Approximate k-nearest-neighbor search. Returns `(row_key, distance)`
    /// pairs sorted nearest-first, length `<= k`.
    pub fn query_knn(&self, query: &[f32], k: usize, ef_search: Option<usize>) -> Vec<(Vec<u8>, f32)> {
        self.hnsw
            .search(query, k, ef_search)
            .into_iter()
            .map(|(id, dist)| (self.row_keys[id].clone(), dist))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_query_knn() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.vec");
        let mut idx = VectorIndex::open(&path, Metric::L2, HnswConfig::default()).unwrap();
        idx.insert(b"row1", vec![1.0, 0.0, 0.0]).unwrap();
        idx.insert(b"row2", vec![0.0, 1.0, 0.0]).unwrap();
        idx.insert(b"row3", vec![0.9, 0.1, 0.0]).unwrap();

        let hits = idx.query_knn(&[1.0, 0.0, 0.0], 2, None);
        assert_eq!(hits.len(), 2);
        let keys: Vec<Vec<u8>> = hits.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys.contains(&b"row1".to_vec()));
        assert!(keys.contains(&b"row3".to_vec()));
    }

    #[test]
    fn reopen_replays_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.vec");
        {
            let mut idx = VectorIndex::open(&path, Metric::Cosine, HnswConfig::default()).unwrap();
            idx.insert(b"row1", vec![1.0, 0.0]).unwrap();
            idx.insert(b"row2", vec![0.0, 1.0]).unwrap();
        }
        let reopened = VectorIndex::open(&path, Metric::L2, HnswConfig::default()).unwrap();
        assert_eq!(reopened.metric(), Metric::Cosine); // read back from header, not the fallback
        assert_eq!(reopened.len(), 2);
        let hits = reopened.query_knn(&[1.0, 0.0], 1, None);
        assert_eq!(hits[0].0, b"row1".to_vec());
    }
}
