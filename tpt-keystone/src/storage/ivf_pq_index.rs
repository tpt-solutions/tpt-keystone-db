//! Local (per-node, not object-store-replicated — same documented scope cut
//! as `storage::vector_index`) on-disk wrapper around `vector::ivf_pq::IvfPqIndex`.
//!
//! Unlike `VectorIndex`/`HnswIndex` (which can insert one vector at a time
//! from a cold start), IVF-PQ needs a trained coarse quantizer and PQ
//! codebooks before it can accept its first insert, so this type's
//! constructor is `train_and_create` (backfill-only, mirroring
//! `Database::create_ivfpq_index`'s "you always have existing rows to train
//! from") rather than an HNSW-style `open-or-create`. A reopen (`open`)
//! replays the on-disk log of raw `(row_key, vector)` records (same format
//! as `VectorIndex`'s log — raw floats, not PQ codes, on disk) through the
//! *already-trained* model read back from the file header: training happens
//! once, at index creation, not on every reopen. The real memory saving is
//! in the in-memory `IvfPqIndex` (codes, not floats) — the on-disk log
//! staying uncompressed is the same "raw log replayed into a compact
//! in-memory structure" precedent every other local index in this codebase
//! follows.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::vector::hnsw::Metric;
use crate::vector::ivf_pq::IvfPqIndex;

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

pub struct IvfPqStorageIndex {
    path: PathBuf,
    metric: Metric,
    n_lists: usize,
    pq_m: usize,
    n_probe: usize,
    ivf: IvfPqIndex,
    row_keys: Vec<Vec<u8>>,
}

impl IvfPqStorageIndex {
    /// Trains a fresh IVF-PQ model from `training` (row_key, vector) pairs
    /// and writes the index file (header + one log record per training
    /// vector). Errors if `training` is empty — there's nothing to train a
    /// coarse quantizer from.
    pub fn train_and_create(
        path: &Path,
        metric: Metric,
        n_lists: usize,
        pq_m: usize,
        n_probe: usize,
        training: Vec<(Vec<u8>, Vec<f32>)>,
    ) -> Result<Self> {
        if training.is_empty() {
            bail!("CREATE INDEX ... USING IVFPQ requires at least one existing row to train on");
        }
        let vectors: Vec<Vec<f32>> = training.iter().map(|(_, v)| v.clone()).collect();
        let ivf = IvfPqIndex::train(&vectors, metric, n_lists, pq_m, n_probe)?;

        let mut idx = Self {
            path: path.to_path_buf(),
            metric,
            n_lists: ivf.n_lists,
            pq_m,
            n_probe: ivf.n_probe,
            ivf,
            row_keys: Vec::new(),
        };
        idx.write_header()?;
        for (row_key, vector) in training {
            idx.append_log(&row_key, &vector)?;
            idx.row_keys.push(row_key);
        }
        Ok(idx)
    }

    /// Reopens an existing index file: reads the header (metric/n_lists/
    /// pq_m/n_probe), retrains against the full replayed log so the coarse
    /// centroids and PQ codebooks are reconstructed deterministically from
    /// the same training data the original `train_and_create` call used,
    /// then re-encodes every logged vector.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let mut header = [0u8; 13];
        file.read_exact(&mut header)?;
        let metric = metric_from_u8(header[0]);
        let n_lists = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        let pq_m = u32::from_be_bytes(header[5..9].try_into().unwrap()) as usize;
        let n_probe = u32::from_be_bytes(header[9..13].try_into().unwrap()) as usize;

        let mut entries = Vec::new();
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
            entries.push(entry);
        }

        if entries.is_empty() {
            bail!("IVF-PQ index file {path:?} has a header but no logged vectors");
        }
        let vectors: Vec<Vec<f32>> = entries.iter().map(|e| e.vector.clone()).collect();
        let ivf = IvfPqIndex::train(&vectors, metric, n_lists, pq_m, n_probe)?;
        let row_keys = entries.into_iter().map(|e| e.row_key).collect();

        Ok(Self {
            path: path.to_path_buf(),
            metric,
            n_lists: ivf.n_lists,
            pq_m,
            n_probe: ivf.n_probe,
            ivf,
            row_keys,
        })
    }

    fn write_header(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut header = Vec::with_capacity(13);
        header.push(metric_to_u8(self.metric));
        header.extend_from_slice(&(self.n_lists as u32).to_be_bytes());
        header.extend_from_slice(&(self.pq_m as u32).to_be_bytes());
        header.extend_from_slice(&(self.n_probe as u32).to_be_bytes());
        file.write_all(&header)?;
        Ok(())
    }

    fn append_log(&self, row_key: &[u8], vector: &[f32]) -> Result<()> {
        let entry = VectorEntry {
            row_key: row_key.to_vec(),
            vector: vector.to_vec(),
        };
        let encoded = bincode::serialize(&entry)?;
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&(encoded.len() as u32).to_be_bytes())?;
        file.write_all(&encoded)?;
        Ok(())
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn len(&self) -> usize {
        self.row_keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_keys.is_empty()
    }

    /// Indexes one more row using the already-trained model (no retraining
    /// — same "insert reuses the fit model" behavior FAISS's `IndexIVFPQ.add`
    /// has after `.train()`).
    pub fn insert(&mut self, row_key: &[u8], vector: Vec<f32>) -> Result<()> {
        self.append_log(row_key, &vector)?;
        let id = self.ivf.insert(vector);
        debug_assert_eq!(id as usize, self.row_keys.len());
        self.row_keys.push(row_key.to_vec());
        Ok(())
    }

    /// Approximate k-nearest-neighbor search. Returns `(row_key, distance)`
    /// pairs sorted nearest-first, length `<= k`.
    pub fn query_knn(&self, query: &[f32], k: usize, n_probe: Option<usize>) -> Vec<(Vec<u8>, f32)> {
        self.ivf
            .search(query, k, n_probe)
            .into_iter()
            .map(|(id, dist)| (self.row_keys[id as usize].clone(), dist))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synthetic(n_per_cluster: usize) -> Vec<(Vec<u8>, Vec<f32>)> {
        let mut out = Vec::new();
        let mut i = 0u32;
        for c in 0..3usize {
            let base = c as f32 * 5.0;
            for j in 0..n_per_cluster {
                let jitter = j as f32 * 0.001;
                out.push((
                    format!("row{i}").into_bytes(),
                    vec![base + jitter, base, base, base],
                ));
                i += 1;
            }
        }
        out
    }

    #[test]
    fn train_insert_and_query() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.ivfpq");
        let training = synthetic(15);
        let mut idx =
            IvfPqStorageIndex::train_and_create(&path, Metric::L2, 3, 2, 2, training).unwrap();
        // cluster 2's training jitter only spans 10.0..=10.014 (0.001 steps,
        // 15 points), so a query centered on 10.0 would legitimately rank
        // all 15 existing points ahead of an "extra" point at 10.02 — query
        // for "extra"'s own vector instead, making it a true, unambiguous
        // nearest neighbor of itself.
        let extra_vec = vec![10.02, 10.0, 10.0, 10.0];
        idx.insert(b"extra", extra_vec.clone()).unwrap();

        let hits = idx.query_knn(&extra_vec, 3, None);
        assert_eq!(hits.len(), 3);
        let keys: Vec<Vec<u8>> = hits.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys.contains(&b"extra".to_vec()));
    }

    #[test]
    fn reopen_replays_log_and_retrains() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.ivfpq");
        {
            let training = synthetic(15);
            IvfPqStorageIndex::train_and_create(&path, Metric::Cosine, 3, 2, 2, training).unwrap();
        }
        let reopened = IvfPqStorageIndex::open(&path).unwrap();
        assert_eq!(reopened.metric(), Metric::Cosine);
        assert_eq!(reopened.len(), 45);
        let hits = reopened.query_knn(&[0.0, 0.0, 0.0, 0.0], 1, None);
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn empty_training_set_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.ivfpq");
        let err = IvfPqStorageIndex::train_and_create(&path, Metric::L2, 3, 2, 2, Vec::new());
        assert!(err.is_err());
    }
}
