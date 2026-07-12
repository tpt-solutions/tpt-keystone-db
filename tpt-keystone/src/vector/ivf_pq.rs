//! IVF-PQ: inverted-file index (coarse k-means quantizer over full vectors)
//! with product-quantized residuals per inverted list — the memory-
//! constrained/larger-scale counterpart to `vector::hnsw`'s graph index.
//! Standard two-stage design (Jegou et al.):
//!
//! 1. **Coarse quantizer**: `n_lists` k-means centroids over full vectors.
//!    Every inserted vector is assigned to its nearest centroid's inverted
//!    list.
//! 2. **Residual PQ**: within a list, vectors are stored not as raw floats
//!    but as `ProductQuantizer`-encoded *residuals* (`vector - centroid`),
//!    which are smaller in magnitude than the raw vectors and so quantize
//!    more accurately for a given code budget — the standard IVF-PQ
//!    refinement over quantizing raw vectors directly.
//!
//! Search probes the `n_probe` nearest centroids (not just the single
//! nearest — points near a Voronoi cell boundary can have their true
//! nearest neighbor in an adjacent cell) and, within each probed list,
//! ranks candidates by asymmetric distance (`ProductQuantizer::distance_table`
//! computed against that list's residual query, i.e. `query - centroid`)
//! without ever decoding a stored code back to a float vector.
//!
//! Honest scope cut: like `vector::hnsw`, insert-only (no delete/update) and
//! no SIMD. Training (`IvfPqIndex::train`) needs a non-trivial upfront
//! sample to fit both the coarse centroids and the PQ codebooks — the same
//! "index needs data before it's useful" property every real IVF-PQ
//! implementation (FAISS included) has; there's no incremental/online
//! variant here.

use anyhow::{bail, Result};

use super::hnsw::Metric;
use super::kmeans::{kmeans, nearest_centroid};
use super::pq::ProductQuantizer;
use super::vector::l2_distance_squared;

struct ListEntry {
    id: u32,
    codes: Vec<u8>,
}

pub struct IvfPqIndex {
    pub metric: Metric,
    pub dim: usize,
    pub n_lists: usize,
    pub n_probe: usize,
    centroids: Vec<Vec<f32>>,
    pq: ProductQuantizer,
    lists: Vec<Vec<ListEntry>>,
    next_id: u32,
}

impl IvfPqIndex {
    /// Trains coarse centroids + PQ codebooks from `training_vectors` and
    /// returns an empty index ready for `insert`. `n_lists` is clamped to
    /// the training set size (an index over 10 vectors can't usefully have
    /// 100 inverted lists).
    pub fn train(
        training_vectors: &[Vec<f32>],
        metric: Metric,
        n_lists: usize,
        pq_m: usize,
        n_probe: usize,
    ) -> Result<Self> {
        if training_vectors.is_empty() {
            bail!("IVF-PQ training set must be non-empty");
        }
        let dim = training_vectors[0].len();
        let n_lists = n_lists.max(1).min(training_vectors.len());
        let n_probe = n_probe.max(1).min(n_lists);

        let centroids = kmeans(training_vectors, n_lists, 25);

        // Train PQ on residuals (vector - its assigned centroid), the
        // standard IVF-PQ refinement described in the module doc-comment.
        let residuals: Vec<Vec<f32>> = training_vectors
            .iter()
            .map(|v| {
                let c = nearest_centroid(v, &centroids);
                subtract(v, &centroids[c])
            })
            .collect();
        let pq = ProductQuantizer::train(&residuals, pq_m)?;

        let mut idx = Self {
            metric,
            dim,
            n_lists: centroids.len(),
            n_probe,
            centroids,
            pq,
            lists: (0..n_lists.max(1)).map(|_| Vec::new()).collect(),
            next_id: 0,
        };
        // lists was sized before centroids.len() could differ from the
        // clamped n_lists local; keep it in sync explicitly.
        idx.lists.resize_with(idx.centroids.len(), Vec::new);

        for v in training_vectors {
            idx.insert(v.clone());
        }
        Ok(idx)
    }

    /// Assigns `vector` to its nearest list and stores its PQ-encoded
    /// residual. Returns the internal id (dense, insertion order — same
    /// convention as `hnsw::HnswIndex::insert`).
    pub fn insert(&mut self, vector: Vec<f32>) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        let c = nearest_centroid(&vector, &self.centroids);
        let residual = subtract(&vector, &self.centroids[c]);
        let codes = self.pq.encode(&residual);
        self.lists[c].push(ListEntry { id, codes });
        id
    }

    pub fn len(&self) -> usize {
        self.next_id as usize
    }

    pub fn is_empty(&self) -> bool {
        self.next_id == 0
    }

    /// Approximate k-NN search: probes the `n_probe` (or caller-overridden)
    /// nearest centroids and ranks every candidate in those lists by
    /// asymmetric PQ distance. Returns `(internal_id, approx_distance)`
    /// pairs sorted nearest-first, length `<= k`.
    pub fn search(&self, query: &[f32], k: usize, n_probe: Option<usize>) -> Vec<(u32, f32)> {
        if self.is_empty() {
            return Vec::new();
        }
        let n_probe = n_probe.unwrap_or(self.n_probe).max(1).min(self.n_lists);

        let mut centroid_dists: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, l2_distance_squared(query, c).unwrap_or(f32::INFINITY)))
            .collect();
        centroid_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut candidates: Vec<(u32, f32)> = Vec::new();
        for &(list_idx, _) in centroid_dists.iter().take(n_probe) {
            let residual_query = subtract(query, &self.centroids[list_idx]);
            let table = self.pq.distance_table(&residual_query);
            for entry in &self.lists[list_idx] {
                let dist = ProductQuantizer::asymmetric_distance(&table, &entry.codes);
                candidates.push((entry.id, dist));
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(k);
        candidates
    }
}

fn subtract(a: &[f32], b: &[f32]) -> Vec<f32> {
    a.iter().zip(b.iter()).map(|(x, y)| x - y).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synthetic_dataset() -> Vec<Vec<f32>> {
        // Three well-separated clusters in 8D so IVF-PQ (n_lists=3) has
        // an obvious structure to learn, with enough points per cluster
        // for both the coarse quantizer and the PQ codebooks to train.
        let mut points = Vec::new();
        for i in 0..30 {
            let j = (i as f32) * 0.01;
            points.push(vec![0.0 + j, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);
            points.push(vec![5.0 + j, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0]);
            points.push(vec![-5.0 + j, -5.0, -5.0, -5.0, -5.0, -5.0, -5.0, -5.0]);
        }
        points
    }

    #[test]
    fn trains_and_searches_correct_cluster() {
        let data = synthetic_dataset();
        let idx = IvfPqIndex::train(&data, Metric::L2, 3, 4, 3).unwrap();
        assert_eq!(idx.len(), data.len());

        let hits = idx.search(&[5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0], 5, None);
        assert_eq!(hits.len(), 5);
        // Every hit should have come from the "cluster around 5.0" group,
        // i.e. every original id % 3 == 1 (insertion order interleaves
        // clusters 0/1/2 per the loop above).
        for (id, _) in &hits {
            assert_eq!(id % 3, 1, "hit {id} wasn't from the expected cluster");
        }
    }

    #[test]
    fn insert_after_training_is_searchable() {
        let data = synthetic_dataset();
        let mut idx = IvfPqIndex::train(&data, Metric::L2, 3, 4, 3).unwrap();
        // 5.055 doesn't fall on the training set's 0.01-step jitter grid
        // (5.0, 5.01, .., 5.29), so this is a true nearest neighbor of
        // itself in exact-distance terms. PQ is lossy, though: this near
        // point (5.05, id 16) can quantize to the same code as the new
        // insert, in which case the tie is broken by insertion order (the
        // training point, inserted first, wins) — that's expected/honest
        // approximate-index behavior, not a bug, so this only asserts the
        // new point is *findable* (within a small top-k), not that it's
        // strictly ranked first.
        let new_vec = vec![5.055, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0];
        let new_id = idx.insert(new_vec.clone());
        let hits = idx.search(&new_vec, 3, None);
        assert!(
            hits.iter().any(|(id, _)| *id == new_id),
            "newly inserted vector not found among top hits: {hits:?}"
        );
    }

    #[test]
    fn n_probe_wider_than_one_finds_boundary_points() {
        let data = synthetic_dataset();
        let idx = IvfPqIndex::train(&data, Metric::L2, 3, 4, 1).unwrap();
        // Query near the boundary between clusters, with a wide n_probe
        // override — should still return results without panicking and
        // respect the requested k.
        let hits = idx.search(&[2.5; 8], 10, Some(3));
        assert_eq!(hits.len(), 10);
    }
}
