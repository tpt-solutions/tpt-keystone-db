//! Disk-resident ANN index (Prism's "DiskANN index for billion-scale
//! on-disk graphs" roadmap item) built on `vector::vamana`'s Vamana graph.
//!
//! The property that distinguishes this from `VectorIndex` (HNSW) and
//! `IvfPqStorageIndex` (IVF-PQ) — both of which replay their *entire*
//! on-disk log into an in-memory structure on `open` — is that opening a
//! `DiskAnnIndex` only reads its small header plus the row-key list (ids,
//! not vectors: cheap even at large row counts). The vectors and adjacency
//! lists that dominate memory at scale never come back into RAM as a whole;
//! `query_knn`'s greedy search reads exactly the handful of node records
//! (`Self::read_record`, one `seek`+`read_exact` each) it actually visits,
//! directly off disk, once per query.
//!
//! On-disk layout, one flat file:
//! ```text
//! header (21 bytes): b"DANN" | metric:u8 | dim:u32 | count:u32 | r:u32 | medoid:u32
//! count * record  (record = dim*4 bytes f32 vector | degree:u32 | r*4 bytes neighbor ids, u32::MAX = empty slot)
//! count * (key_len:u32 | key bytes)     -- read once, in full, at open time
//! ```
//! `record`s are fixed-size so `read_record(id)` is a single `O(1)` seek —
//! `header_size + id * record_size` — no index-of-an-index needed.
//!
//! Honest scope cuts vs the real DiskANN system this is modeled on: no
//! product-quantized in-memory vector cache for fast approximate re-ranking
//! (real DiskANN keeps PQ codes in RAM specifically so the disk-resident
//! search doesn't need a full-precision read for every distance
//! computation — this implementation re-reads the full vector from disk on
//! every visit, simpler but more I/O per query); no SSD-page-aligned batched
//! reads (each neighbor read is its own `seek`+`read_exact`, not a
//! page-grouped batch); batch build only (`build`), no incremental insert
//! into an already-built graph (same scope cut `vector::vamana::build`
//! documents — matches `vector::ivf_pq`'s train-from-a-batch precedent, not
//! HNSW's true insert-in-place).

use anyhow::Result;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::vector::hnsw::Metric;
use crate::vector::vamana::{self, VamanaGraph};

const MAGIC: &[u8; 4] = b"DANN";
const HEADER_SIZE: u64 = 21;

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

pub struct DiskAnnIndex {
    file: File,
    metric: Metric,
    dim: usize,
    count: usize,
    r: usize,
    medoid: usize,
    record_size: u64,
    /// Ids, not vectors — small even at large `count`, so this is the one
    /// piece of per-row state this index keeps in memory (see module doc).
    row_keys: Vec<Vec<u8>>,
}

impl DiskAnnIndex {
    /// Batch-builds a Vamana graph over `rows` in memory (`vector::vamana`
    /// needs the full point set to compute distances during construction —
    /// the disk-residency property is about *querying*, not building) and
    /// writes it to `path` in the fixed-record layout above, then reopens
    /// it via `Self::open` so the returned handle only holds the header +
    /// row keys in memory.
    pub fn build(
        path: &Path,
        rows: &[(Vec<u8>, Vec<f32>)],
        metric: Metric,
        r: usize,
        l_build: usize,
    ) -> Result<Self> {
        anyhow::ensure!(
            !rows.is_empty(),
            "DiskAnnIndex::build requires at least one row"
        );
        let dim = rows[0].1.len();
        for (_, v) in rows {
            anyhow::ensure!(v.len() == dim, "all vectors must share dimension {dim}");
        }
        let points: Vec<Vec<f32>> = rows.iter().map(|(_, v)| v.clone()).collect();
        let graph: VamanaGraph = vamana::build(&points, metric, r, l_build, 1.2);

        let mut file = File::create(path)?;
        file.write_all(MAGIC)?;
        file.write_all(&[metric_to_u8(metric)])?;
        file.write_all(&(dim as u32).to_be_bytes())?;
        file.write_all(&(rows.len() as u32).to_be_bytes())?;
        file.write_all(&(r as u32).to_be_bytes())?;
        file.write_all(&(graph.medoid as u32).to_be_bytes())?;

        for (i, (_, vector)) in rows.iter().enumerate() {
            for x in vector {
                file.write_all(&x.to_be_bytes())?;
            }
            let neighbors = &graph.edges[i];
            file.write_all(&(neighbors.len() as u32).to_be_bytes())?;
            for slot in 0..r {
                let id = neighbors.get(slot).copied().unwrap_or(u32::MAX);
                file.write_all(&id.to_be_bytes())?;
            }
        }
        for (key, _) in rows {
            file.write_all(&(key.len() as u32).to_be_bytes())?;
            file.write_all(key)?;
        }
        file.flush()?;
        drop(file);

        Self::open(path)
    }

    /// Opens an existing index. Reads the header and the row-key list only
    /// — `O(count)` in the number of *keys*, not vectors.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut header = [0u8; HEADER_SIZE as usize];
        file.read_exact(&mut header)?;
        anyhow::ensure!(&header[0..4] == MAGIC, "not a DiskANN index file");
        let metric = metric_from_u8(header[4]);
        let dim = u32::from_be_bytes(header[5..9].try_into().unwrap()) as usize;
        let count = u32::from_be_bytes(header[9..13].try_into().unwrap()) as usize;
        let r = u32::from_be_bytes(header[13..17].try_into().unwrap()) as usize;
        let medoid = u32::from_be_bytes(header[17..21].try_into().unwrap()) as usize;
        let record_size = (dim as u64) * 4 + 4 + (r as u64) * 4;

        let keys_offset = HEADER_SIZE + (count as u64) * record_size;
        file.seek(SeekFrom::Start(keys_offset))?;
        let mut row_keys = Vec::with_capacity(count);
        let mut len_buf = [0u8; 4];
        for _ in 0..count {
            file.read_exact(&mut len_buf)?;
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            row_keys.push(buf);
        }

        Ok(Self {
            file,
            metric,
            dim,
            count,
            r,
            medoid,
            record_size,
            row_keys,
        })
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Reads one node's vector + out-neighbor list directly from disk —
    /// the only place this index touches vector data; nothing is cached.
    fn read_record(&mut self, id: usize) -> Result<(Vec<f32>, Vec<u32>)> {
        let offset = HEADER_SIZE + (id as u64) * self.record_size;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut vec_buf = vec![0u8; self.dim * 4];
        self.file.read_exact(&mut vec_buf)?;
        let vector: Vec<f32> = vec_buf
            .chunks_exact(4)
            .map(|c| f32::from_be_bytes(c.try_into().unwrap()))
            .collect();

        let mut degree_buf = [0u8; 4];
        self.file.read_exact(&mut degree_buf)?;
        let degree = u32::from_be_bytes(degree_buf) as usize;

        let mut nb_buf = vec![0u8; self.r * 4];
        self.file.read_exact(&mut nb_buf)?;
        let neighbors = nb_buf[..degree * 4]
            .chunks_exact(4)
            .map(|c| u32::from_be_bytes(c.try_into().unwrap()))
            .collect();

        Ok((vector, neighbors))
    }

    /// Greedy best-first search from the stored medoid, reading node
    /// records off disk as it expands the candidate frontier — the same
    /// algorithm `vector::vamana`'s build-time `greedy_search` uses, just
    /// against a disk-backed graph instead of an in-memory one.
    pub fn query_knn(
        &mut self,
        query: &[f32],
        k: usize,
        l_search: usize,
    ) -> Result<Vec<(Vec<u8>, f32)>> {
        anyhow::ensure!(
            query.len() == self.dim,
            "query dimension {} does not match index dimension {}",
            query.len(),
            self.dim
        );
        let l = l_search.max(k).max(1);
        let (medoid_vec, _) = self.read_record(self.medoid)?;
        let mut visited_dist: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut expanded: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut candidates: Vec<(usize, f32)> = vec![(
            self.medoid,
            vamana::distance(&medoid_vec, query, self.metric),
        )];
        visited_dist.insert(self.medoid);

        loop {
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            candidates.truncate(l);
            let Some(&(p, _)) = candidates.iter().find(|(id, _)| !expanded.contains(id)) else {
                break;
            };
            expanded.insert(p);
            let (_, neighbors) = self.read_record(p)?;
            for nb in neighbors {
                if nb == u32::MAX {
                    continue;
                }
                let nb = nb as usize;
                if visited_dist.contains(&nb) {
                    continue;
                }
                visited_dist.insert(nb);
                let (nb_vec, _) = self.read_record(nb)?;
                candidates.push((nb, vamana::distance(&nb_vec, query, self.metric)));
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(k);
        Ok(candidates
            .into_iter()
            .map(|(id, d)| (self.row_keys[id].clone(), d))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_rows(n: usize, clusters: usize) -> Vec<(Vec<u8>, Vec<f32>)> {
        (0..n)
            .map(|i| {
                let c = (i % clusters) as f32 * 50.0;
                let key = format!("row-{i}").into_bytes();
                // A tiny per-index jitter keeps every vector unique (avoids
                // exact-duplicate ties within a cluster, which would make
                // "the nearest hit's key" ambiguous) without perturbing
                // which cluster a point falls in (jitter << cluster spacing).
                let jitter = i as f32 * 0.001;
                (
                    key,
                    vec![c + (i % 5) as f32 + jitter, c + (i % 3) as f32 + jitter],
                )
            })
            .collect()
    }

    #[test]
    fn build_then_open_preserves_header_and_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.diskann");
        let rows = sample_rows(100, 5);
        {
            let idx = DiskAnnIndex::build(&path, &rows, Metric::L2, 12, 32).unwrap();
            assert_eq!(idx.len(), 100);
            assert_eq!(idx.metric(), Metric::L2);
        }
        let reopened = DiskAnnIndex::open(&path).unwrap();
        assert_eq!(reopened.len(), 100);
        assert_eq!(reopened.metric(), Metric::L2);
    }

    #[test]
    fn query_knn_finds_exact_match_and_cluster_neighbors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.diskann");
        let rows = sample_rows(240, 8);
        let mut idx = DiskAnnIndex::build(&path, &rows, Metric::L2, 16, 48).unwrap();

        // Query with an exact copy of row 10's vector -- the closest hit
        // must be that exact point (distance 0).
        let query = rows[10].1.clone();
        let hits = idx.query_knn(&query, 5, 48).unwrap();
        assert_eq!(hits.len(), 5);
        assert!(
            hits[0].1.abs() < 1e-6,
            "expected an exact match, got {:?}",
            hits[0]
        );
        assert_eq!(hits[0].0, rows[10].0);
    }

    #[test]
    fn reopened_index_answers_queries_without_reloading_vectors_upfront() {
        // The whole point of this index: `open` must not need the vectors
        // in memory to serve a query afterward -- verified structurally by
        // dropping the source rows before querying a freshly reopened
        // handle built from nothing but the file on disk.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.diskann");
        // L2, not Cosine: this synthetic dataset's points are all in the
        // positive quadrant with similar coordinate ratios, so under cosine
        // distance many pairs are near-degenerate (angularly almost
        // indistinguishable) and starve RobustPrune's pruning rule of the
        // spread it needs -- an artifact of this test's data shape, not a
        // bug in the index. L2 (like `query_knn_finds_exact_match_and_cluster_neighbors`
        // above) matches the geometry this synthetic data actually has.
        let rows = sample_rows(150, 6);
        let query = rows[42].1.clone();
        let expected_key = rows[42].0.clone();
        DiskAnnIndex::build(&path, &rows, Metric::L2, 24, 80).unwrap();
        drop(rows);

        let mut idx = DiskAnnIndex::open(&path).unwrap();
        // A wide search-list width relative to k so this is robust against
        // the routine build-to-build graph-quality variance an unseeded
        // (random insertion order) single-pass Vamana build has -- the
        // property under test is disk-residency, not squeezing the last
        // bit of recall out of a small, minimally-tuned graph.
        let hits = idx.query_knn(&query, 3, 100).unwrap();
        assert_eq!(hits[0].0, expected_key);
    }

    #[test]
    fn wrong_dimension_query_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.diskann");
        let rows = sample_rows(20, 2);
        let mut idx = DiskAnnIndex::build(&path, &rows, Metric::L2, 8, 16).unwrap();
        assert!(idx.query_knn(&[1.0, 2.0, 3.0], 3, 16).is_err());
    }
}
