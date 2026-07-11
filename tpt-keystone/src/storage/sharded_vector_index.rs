//! A `VECTOR` column index partitioned across N shards via
//! `vector::shard::ConsistentHashRing`, each shard a standalone
//! `VectorIndex` (HNSW) log file. This is the concrete structure the Prism
//! "consistent hashing for distributed vector shards" roadmap item
//! describes: `insert` routes a row to exactly one shard by consistent hash
//! of its row key, and `query_knn` scatters the query to every shard and
//! merges each shard's local top-k into one global top-k — the standard
//! scatter-gather shape a partitioned ANN index uses, same idea as Prism's
//! own IVF-PQ inverted-list probe just applied at the shard level instead
//! of the cluster level.
//!
//! Honest scope: every shard is opened and queried in-process on the node
//! that owns this `ShardedVectorIndex` — see `vector::shard`'s module doc
//! for why that's the real, current boundary (no cross-node RPC exists
//! anywhere in this engine yet). What this *does* give a caller today: a
//! vector column's total index size and insert/query cost split N ways
//! instead of one HNSW graph holding every row, which is the part of
//! "distributed shards" that's actually node-local infrastructure
//! (per-shard memory footprint, per-shard file I/O) rather than networking.

use anyhow::Result;
use std::path::{Path, PathBuf};

use crate::vector::hnsw::{HnswConfig, Metric};
use crate::vector::shard::ConsistentHashRing;
use crate::storage::vector_index::VectorIndex;

pub struct ShardedVectorIndex {
    ring: ConsistentHashRing,
    shards: Vec<VectorIndex>,
}

impl ShardedVectorIndex {
    /// Opens (or creates) `shard_count` per-shard log files under `dir`,
    /// named `shard-<n>.vec`. Mirrors `VectorIndex::open`'s
    /// create-if-missing / read-header-if-present behavior per shard.
    pub fn open(
        dir: &Path,
        shard_count: u32,
        default_metric: Metric,
        default_config: HnswConfig,
    ) -> Result<Self> {
        assert!(shard_count > 0, "ShardedVectorIndex needs at least 1 shard");
        std::fs::create_dir_all(dir)?;
        let mut shards = Vec::with_capacity(shard_count as usize);
        for i in 0..shard_count {
            let path = Self::shard_path(dir, i);
            shards.push(VectorIndex::open(&path, default_metric, default_config)?);
        }
        Ok(Self {
            ring: ConsistentHashRing::new(shard_count),
            shards,
        })
    }

    fn shard_path(dir: &Path, shard: u32) -> PathBuf {
        dir.join(format!("shard-{shard}.vec"))
    }

    pub fn shard_count(&self) -> u32 {
        self.ring.shard_count()
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Which shard a row key is routed to. Exposed so callers/tests can
    /// verify placement without duplicating the ring's hashing logic.
    pub fn shard_for(&self, row_key: &[u8]) -> u32 {
        self.ring.shard_for(row_key)
    }

    /// Indexes one row's vector value on whichever shard its key hashes to.
    pub fn insert(&mut self, row_key: &[u8], vector: Vec<f32>) -> Result<()> {
        let shard = self.ring.shard_for(row_key) as usize;
        self.shards[shard].insert(row_key, vector)
    }

    /// Scatter-gather k-NN: query every shard for its local top-k, then
    /// merge by distance and keep the global top-k. Each shard already
    /// returns its results nearest-first (`VectorIndex::query_knn`), so the
    /// merge is a standard k-way merge by distance.
    pub fn query_knn(
        &self,
        query: &[f32],
        k: usize,
        ef_search: Option<usize>,
    ) -> Vec<(Vec<u8>, f32)> {
        let mut merged: Vec<(Vec<u8>, f32)> = self
            .shards
            .iter()
            .flat_map(|s| s.query_knn(query, k, ef_search))
            .collect();
        merged.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        merged.truncate(k);
        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_routes_deterministically_and_is_queryable() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx =
            ShardedVectorIndex::open(dir.path(), 4, Metric::L2, HnswConfig::default()).unwrap();
        for i in 0..50 {
            let v = vec![i as f32, 0.0, 0.0];
            idx.insert(format!("row-{i}").as_bytes(), v).unwrap();
        }
        assert_eq!(idx.len(), 50);

        // Every shard should have received at least one row across 50
        // inserts and 4 shards -- if routing were broken (e.g. everything
        // landing on shard 0) this would fail.
        let dir_reopened = dir.path();
        for shard in 0..4 {
            let path = ShardedVectorIndex::shard_path(dir_reopened, shard);
            let reopened = VectorIndex::open(&path, Metric::L2, HnswConfig::default()).unwrap();
            assert!(reopened.len() > 0, "shard {shard} got zero rows");
        }
    }

    #[test]
    fn scatter_gather_knn_matches_single_index_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let mut sharded =
            ShardedVectorIndex::open(dir.path(), 4, Metric::L2, HnswConfig::default()).unwrap();
        let mut baseline = VectorIndex::open(
            &dir.path().join("baseline.vec"),
            Metric::L2,
            HnswConfig::default(),
        )
        .unwrap();

        for i in 0..200 {
            let v = vec![(i % 17) as f32, (i % 5) as f32, (i % 3) as f32];
            let key = format!("row-{i}");
            sharded.insert(key.as_bytes(), v.clone()).unwrap();
            baseline.insert(key.as_bytes(), v).unwrap();
        }

        let query = vec![3.0, 2.0, 1.0];
        let sharded_hits = sharded.query_knn(&query, 5, None);
        let baseline_hits = baseline.query_knn(&query, 5, None);

        assert_eq!(sharded_hits.len(), 5);
        // The true nearest neighbor (distance 0, an exact match exists in
        // the data) must be found by both -- scatter-gather over exact
        // per-shard HNSW search must not lose the globally-closest point.
        assert_eq!(sharded_hits[0].1, baseline_hits[0].1);
    }

    #[test]
    fn reopen_after_writes_preserves_all_shards() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut idx =
                ShardedVectorIndex::open(dir.path(), 3, Metric::Cosine, HnswConfig::default())
                    .unwrap();
            for i in 0..30 {
                idx.insert(format!("row-{i}").as_bytes(), vec![i as f32, 1.0])
                    .unwrap();
            }
        }
        let reopened =
            ShardedVectorIndex::open(dir.path(), 3, Metric::L2, HnswConfig::default()).unwrap();
        assert_eq!(reopened.len(), 30);
    }
}
