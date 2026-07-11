//! Product Quantization (Jegou, Douze, Schmid 2011): splits each vector into
//! `m` equal-width subvectors and independently k-means-clusters each
//! subvector space into `2^bits` centroids (a "codebook"), so a full vector
//! is approximated by `m` single-byte (for `bits == 8`) codebook indexes
//! instead of `dim` `f32` components. This is the compression piece behind
//! IVF-PQ (`vector::ivf_pq`): the in-memory index stores codes, not raw
//! floats, and distances are computed via a precomputed asymmetric distance
//! table (ADC) rather than decoding codes back to floats first.
//!
//! Honest scope cut: only `bits == 8` (one `u8` per subvector, 256-entry
//! codebooks) is implemented — the spec's "PQ" doesn't commit to a specific
//! code width, and 8 bits is the standard default in every real PQ
//! implementation (FAISS included). No SIMD (see `vector::mod` doc-comment).

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use super::kmeans::{kmeans, nearest_centroid};

/// One codebook per subvector: `codebooks[i]` has up to 256 entries, each of
/// length `sub_dim`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantizer {
    pub dim: usize,
    pub m: usize,
    pub sub_dim: usize,
    codebooks: Vec<Vec<Vec<f32>>>,
}

impl ProductQuantizer {
    /// Trains `m` subvector codebooks (256 centroids each, via k-means) from
    /// `training_vectors`. `dim` must be evenly divisible by `m`.
    pub fn train(training_vectors: &[Vec<f32>], m: usize) -> Result<Self> {
        if training_vectors.is_empty() {
            bail!("product quantizer training set must be non-empty");
        }
        let dim = training_vectors[0].len();
        if dim == 0 || m == 0 || dim % m != 0 {
            bail!("vector dimension {dim} must be a non-zero multiple of pq_m {m}");
        }
        let sub_dim = dim / m;

        let mut codebooks = Vec::with_capacity(m);
        for i in 0..m {
            let start = i * sub_dim;
            let end = start + sub_dim;
            let subvectors: Vec<Vec<f32>> = training_vectors
                .iter()
                .map(|v| v[start..end].to_vec())
                .collect();
            let k = 256.min(subvectors.len());
            codebooks.push(kmeans(&subvectors, k, 25));
        }

        Ok(Self {
            dim,
            m,
            sub_dim,
            codebooks,
        })
    }

    /// Encodes a full vector into `m` codebook indexes (one byte each).
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut codes = Vec::with_capacity(self.m);
        for i in 0..self.m {
            let start = i * self.sub_dim;
            let end = start + self.sub_dim;
            let sub = &vector[start..end];
            let idx = nearest_centroid(sub, &self.codebooks[i]);
            codes.push(idx as u8);
        }
        codes
    }

    /// Reconstructs an approximate vector from codes (each subvector
    /// replaced by its codebook centroid) — used for the honest "how lossy
    /// is this" recall tests, not on the index's hot path.
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        let mut out = Vec::with_capacity(self.dim);
        for (i, &code) in codes.iter().enumerate() {
            out.extend_from_slice(&self.codebooks[i][code as usize]);
        }
        out
    }

    /// Builds an asymmetric distance table for `query`: `table[i][c]` is the
    /// squared L2 distance from `query`'s `i`th subvector to codebook `i`'s
    /// `c`th centroid. Summing `table[i][codes[i]]` over `i` gives the (still
    /// approximate, since the database side is quantized) squared L2
    /// distance between `query` and an encoded vector, without ever
    /// decoding the codes back to floats.
    pub fn distance_table(&self, query: &[f32]) -> Vec<Vec<f32>> {
        let mut table = Vec::with_capacity(self.m);
        for i in 0..self.m {
            let start = i * self.sub_dim;
            let end = start + self.sub_dim;
            let sub = &query[start..end];
            let row: Vec<f32> = self.codebooks[i]
                .iter()
                .map(|centroid| {
                    super::vector::l2_distance_squared(sub, centroid).unwrap_or(f32::INFINITY)
                })
                .collect();
            table.push(row);
        }
        table
    }

    /// Sums a precomputed distance table over a code sequence — the actual
    /// per-candidate cost in `IvfPqIndex::search`'s inner loop.
    pub fn asymmetric_distance(table: &[Vec<f32>], codes: &[u8]) -> f32 {
        codes
            .iter()
            .enumerate()
            .map(|(i, &c)| table[i][c as usize])
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn training_set() -> Vec<Vec<f32>> {
        // Two well-separated clusters in 4D, split into 2 subvectors of dim 2.
        let mut points = Vec::new();
        for i in 0..20 {
            let jitter = (i as f32) * 0.001;
            points.push(vec![0.0 + jitter, 0.0, 0.0 + jitter, 0.0]);
            points.push(vec![10.0 + jitter, 10.0, 10.0 + jitter, 10.0]);
        }
        points
    }

    #[test]
    fn encode_decode_round_trip_is_close() {
        let pq = ProductQuantizer::train(&training_set(), 2).unwrap();
        let v = vec![0.0f32, 0.0, 0.0, 0.0];
        let codes = pq.encode(&v);
        assert_eq!(codes.len(), 2);
        let decoded = pq.decode(&codes);
        let err = super::super::vector::l2_distance(&v, &decoded).unwrap();
        assert!(err < 1.0, "reconstruction error too high: {err}");
    }

    #[test]
    fn asymmetric_distance_matches_full_distance_ordering() {
        let pq = ProductQuantizer::train(&training_set(), 2).unwrap();
        let near = vec![0.05f32, 0.02, 0.01, 0.03];
        let far = vec![10.0f32, 10.0, 10.0, 10.0];
        let near_codes = pq.encode(&near);
        let far_codes = pq.encode(&far);

        let query = vec![0.0f32, 0.0, 0.0, 0.0];
        let table = pq.distance_table(&query);
        let d_near = ProductQuantizer::asymmetric_distance(&table, &near_codes);
        let d_far = ProductQuantizer::asymmetric_distance(&table, &far_codes);
        assert!(d_near < d_far);
    }

    #[test]
    fn rejects_dim_not_divisible_by_m() {
        let points = vec![vec![1.0, 2.0, 3.0]];
        assert!(ProductQuantizer::train(&points, 2).is_err());
    }
}
