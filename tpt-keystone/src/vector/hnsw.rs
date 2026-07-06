//! A real, from-scratch Hierarchical Navigable Small World (HNSW)
//! approximate-nearest-neighbor graph index (Malkov & Yashunin, "Efficient
//! and robust approximate nearest neighbor search using Hierarchical
//! Navigable Small World graphs", 2016/2018).
//!
//! This is the actual multi-layer graph-construction and greedy-search
//! algorithm, not a brute-force linear scan relabeled as HNSW: each
//! inserted point is assigned a random top layer (geometrically decaying
//! layer-assignment probability), greedy nearest-descent through the upper
//! layers finds an entry point into the base layer, and layer 0 (plus every
//! layer up to the point's top layer) gets a bounded-degree neighbor list
//! built from the classic simple heuristic (nearest-of-candidates, capped at
//! `M`/`m0`). Search performs the same layered greedy descent down to layer
//! 1, then a beam search of width `ef` on layer 0.
//!
//! Honest scope cut: no explicit SIMD (see `vector::mod`'s doc-comment) and
//! no delete/update support (insert-and-search only, matching the "local
//! secondary-index accelerator" scope of `storage::geo_index`/`storage::btree`).

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use rand::Rng;

use super::vector::l2_distance_squared;

/// Which scalar function ranks neighbors. Squared L2 avoids the internal
/// index needing floating-point sqrt in its inner loop; cosine distance is
/// offered because it's the more common metric for text/image embeddings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    L2,
    Cosine,
}

impl Metric {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Metric::L2 => l2_distance_squared(a, b).unwrap_or(f32::INFINITY),
            Metric::Cosine => super::vector::cosine_distance(a, b).unwrap_or(f32::INFINITY),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ScoredId {
    dist: f32,
    id: usize,
}
impl Eq for ScoredId {}
impl Ord for ScoredId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist.partial_cmp(&other.dist).unwrap_or(Ordering::Equal)
    }
}
impl PartialOrd for ScoredId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Reverses ordering so a `BinaryHeap<Reverse<ScoredId>>`-free min-heap can
/// be built directly from a max-heap by negating comparisons.
#[derive(Debug, Clone, Copy, PartialEq)]
struct MinScoredId(ScoredId);
impl Eq for MinScoredId {}
impl Ord for MinScoredId {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.cmp(&self.0)
    }
}
impl PartialOrd for MinScoredId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// One inserted point: its vector plus an opaque caller-supplied payload
/// (Prism's storage layer stashes the row key here).
struct Node {
    vector: Vec<f32>,
    /// Per-layer neighbor lists, `layers[l]` = neighbor node ids at layer `l`.
    layers: Vec<Vec<usize>>,
}

/// Configuration knobs from the original paper.
#[derive(Debug, Clone, Copy)]
pub struct HnswConfig {
    /// Max neighbors per node at layers >= 1.
    pub m: usize,
    /// Max neighbors per node at layer 0 (paper recommends `2*m`).
    pub m0: usize,
    /// Candidate list size used while *building* the graph.
    pub ef_construction: usize,
    /// Candidate list size used while *searching* the graph (can be tuned
    /// per-query independently of construction; stored here as the default).
    pub ef_search: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self { m: 16, m0: 32, ef_construction: 100, ef_search: 50 }
    }
}

/// A from-scratch HNSW graph index over `Vec<f32>` vectors, keyed by a
/// dense internal `usize` id assigned in insertion order.
pub struct HnswIndex {
    metric: Metric,
    config: HnswConfig,
    nodes: Vec<Node>,
    entry_point: Option<usize>,
    top_layer: usize,
    /// `1 / ln(m)` — the exponential layer-assignment distribution's scale,
    /// precomputed once (paper's `mL`).
    level_mult: f64,
}

impl HnswIndex {
    pub fn new(metric: Metric, config: HnswConfig) -> Self {
        let level_mult = 1.0 / (config.m.max(2) as f64).ln();
        Self { metric, config, nodes: Vec::new(), entry_point: None, top_layer: 0, level_mult }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        self.metric.distance(a, b)
    }

    fn random_layer(&self) -> usize {
        let mut rng = rand::thread_rng();
        let r: f64 = rng.gen_range(1e-12..1.0);
        (-r.ln() * self.level_mult).floor() as usize
    }

    /// Greedy descent from `entry` at `layer`, returning the single closest
    /// node found (used to find the entry point into the next layer down).
    fn greedy_search_layer(&self, query: &[f32], entry: usize, layer: usize) -> usize {
        let mut current = entry;
        let mut current_dist = self.dist(query, &self.nodes[current].vector);
        loop {
            let mut improved = false;
            if let Some(neighbors) = self.nodes[current].layers.get(layer) {
                for &n in neighbors {
                    let d = self.dist(query, &self.nodes[n].vector);
                    if d < current_dist {
                        current_dist = d;
                        current = n;
                        improved = true;
                    }
                }
            }
            if !improved {
                return current;
            }
        }
    }

    /// Beam search at `layer` starting from `entry_points`, returning up to
    /// `ef` nearest candidates found (the paper's `SEARCH-LAYER`).
    fn search_layer(&self, query: &[f32], entry_points: &[usize], ef: usize, layer: usize) -> Vec<ScoredId> {
        let mut visited: HashSet<usize> = entry_points.iter().copied().collect();
        let mut candidates: BinaryHeap<MinScoredId> = BinaryHeap::new();
        let mut found: BinaryHeap<ScoredId> = BinaryHeap::new(); // max-heap: worst-of-found on top

        for &ep in entry_points {
            let d = self.dist(query, &self.nodes[ep].vector);
            candidates.push(MinScoredId(ScoredId { dist: d, id: ep }));
            found.push(ScoredId { dist: d, id: ep });
        }

        while let Some(MinScoredId(c)) = candidates.pop() {
            let worst_found = found.peek().map(|s| s.dist).unwrap_or(f32::INFINITY);
            if c.dist > worst_found && found.len() >= ef {
                break;
            }
            if let Some(neighbors) = self.nodes[c.id].layers.get(layer) {
                for &n in neighbors {
                    if !visited.insert(n) {
                        continue;
                    }
                    let d = self.dist(query, &self.nodes[n].vector);
                    let worst = found.peek().map(|s| s.dist).unwrap_or(f32::INFINITY);
                    if found.len() < ef || d < worst {
                        candidates.push(MinScoredId(ScoredId { dist: d, id: n }));
                        found.push(ScoredId { dist: d, id: n });
                        if found.len() > ef {
                            found.pop();
                        }
                    }
                }
            }
        }

        let mut out: Vec<ScoredId> = found.into_vec();
        out.sort();
        out
    }

    /// Inserts `vector` into the graph, returning its assigned internal id
    /// (dense, insertion-order — callers map this to a row key externally).
    pub fn insert(&mut self, vector: Vec<f32>) -> usize {
        let id = self.nodes.len();
        let layer = self.random_layer();
        self.nodes.push(Node { vector, layers: vec![Vec::new(); layer + 1] });

        let Some(entry) = self.entry_point else {
            self.entry_point = Some(id);
            self.top_layer = layer;
            return id;
        };

        let query = self.nodes[id].vector.clone();
        let mut current = entry;

        // Descend from the current top layer down to `layer + 1` with pure
        // greedy single-best-neighbor search (paper's phase 1).
        for l in (layer + 1..=self.top_layer).rev() {
            current = self.greedy_search_layer(&query, current, l);
        }

        // From `min(layer, top_layer)` down to 0, do a beam search with
        // `ef_construction` and connect to the best `m`/`m0` of them
        // (paper's phase 2).
        let start_layer = layer.min(self.top_layer);
        let mut entry_points = vec![current];
        for l in (0..=start_layer).rev() {
            let candidates = self.search_layer(&query, &entry_points, self.config.ef_construction, l);
            let max_conn = if l == 0 { self.config.m0 } else { self.config.m };
            let selected: Vec<usize> = candidates.iter().take(max_conn).map(|c| c.id).collect();

            self.nodes[id].layers[l] = selected.clone();
            for &nb in &selected {
                self.connect(nb, id, l, max_conn);
            }

            entry_points = candidates.into_iter().map(|c| c.id).collect();
            if entry_points.is_empty() {
                entry_points = vec![current];
            }
        }

        if layer > self.top_layer {
            self.top_layer = layer;
            self.entry_point = Some(id);
        }

        id
    }

    /// Adds `id` to `node`'s neighbor list at `layer`, pruning back down to
    /// `max_conn` by keeping the `max_conn` nearest if it overflows (the
    /// paper's simple neighbor-selection heuristic, not the more elaborate
    /// diversity heuristic — a documented simplification).
    fn connect(&mut self, node: usize, id: usize, layer: usize, max_conn: usize) {
        // `node` might not have a slot at `layer` if it was inserted with a
        // lower top layer than `layer` — skip if so (shouldn't happen given
        // how `search_layer` restricts candidates to that layer's members,
        // but defensive since layer vectors are lazily sized at insert time).
        if layer >= self.nodes[node].layers.len() {
            return;
        }
        self.nodes[node].layers[layer].push(id);
        if self.nodes[node].layers[layer].len() > max_conn {
            let node_vec = self.nodes[node].vector.clone();
            let mut scored: Vec<ScoredId> = self.nodes[node].layers[layer]
                .iter()
                .map(|&n| ScoredId { dist: self.dist(&node_vec, &self.nodes[n].vector), id: n })
                .collect();
            scored.sort();
            scored.truncate(max_conn);
            self.nodes[node].layers[layer] = scored.into_iter().map(|s| s.id).collect();
        }
    }

    /// Approximate k-nearest-neighbor search. Returns `(internal_id,
    /// distance)` pairs sorted nearest-first, length `<= k`.
    pub fn search(&self, query: &[f32], k: usize, ef_search: Option<usize>) -> Vec<(usize, f32)> {
        let Some(entry) = self.entry_point else { return Vec::new() };
        let ef = ef_search.unwrap_or(self.config.ef_search).max(k);

        let mut current = entry;
        for l in (1..=self.top_layer).rev() {
            current = self.greedy_search_layer(query, current, l);
        }

        let candidates = self.search_layer(query, &[current], ef, 0);
        candidates.into_iter().take(k).map(|c| (c.id, c.dist)).collect()
    }

    /// The vector stored at internal id `id`, for callers that need to map
    /// back from a search hit to the original row (e.g. re-checking exact
    /// distance, or debugging).
    pub fn vector(&self, id: usize) -> &[f32] {
        &self.nodes[id].vector
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    fn random_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..n).map(|_| (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect()).collect()
    }

    fn brute_force_knn(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<usize> {
        let mut scored: Vec<(usize, f32)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i, l2_distance_squared(v, query).unwrap()))
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        scored.into_iter().take(k).map(|(i, _)| i).collect()
    }

    #[test]
    fn insert_and_search_finds_self() {
        let mut idx = HnswIndex::new(Metric::L2, HnswConfig::default());
        let vectors = random_vectors(50, 8, 42);
        for v in &vectors {
            idx.insert(v.clone());
        }
        for (i, v) in vectors.iter().enumerate() {
            let results = idx.search(v, 1, None);
            assert_eq!(results[0].0, i, "nearest neighbor of an inserted point should be itself");
        }
    }

    #[test]
    fn knn_recall_against_brute_force_is_high() {
        let n = 500;
        let dim = 16;
        let k = 10;
        let vectors = random_vectors(n, dim, 7);

        let mut idx = HnswIndex::new(Metric::L2, HnswConfig { m: 16, m0: 32, ef_construction: 100, ef_search: 80 });
        for v in &vectors {
            idx.insert(v.clone());
        }

        let queries = random_vectors(30, dim, 99);
        let mut total_recall = 0.0;
        for q in &queries {
            let ground_truth: HashSet<usize> = brute_force_knn(&vectors, q, k).into_iter().collect();
            let approx: HashSet<usize> = idx.search(q, k, None).into_iter().map(|(id, _)| id).collect();
            let hits = ground_truth.intersection(&approx).count();
            total_recall += hits as f64 / k as f64;
        }
        let avg_recall = total_recall / queries.len() as f64;
        assert!(avg_recall > 0.90, "expected >90% recall on this small synthetic set, got {avg_recall}");
    }

    #[test]
    fn graph_is_not_a_flat_full_scan() {
        // A brute-force-pretending-to-be-HNSW implementation would give
        // every node a neighbor list containing (almost) every other node.
        // Assert node degree stays bounded by the configured M even as the
        // graph grows well past M — proof the neighbor-pruning/selection
        // logic (not a full scan) is actually what's answering queries.
        let mut idx = HnswIndex::new(Metric::L2, HnswConfig { m: 8, m0: 16, ef_construction: 50, ef_search: 30 });
        let vectors = random_vectors(300, 12, 123);
        for v in &vectors {
            idx.insert(v.clone());
        }
        for node in &idx.nodes {
            if let Some(base_layer) = node.layers.first() {
                assert!(base_layer.len() <= idx.config.m0, "base-layer degree {} exceeds m0 {}", base_layer.len(), idx.config.m0);
            }
        }
    }

    #[test]
    fn cosine_metric_recall_is_high() {
        let n = 300;
        let dim = 12;
        let k = 5;
        let vectors = random_vectors(n, dim, 11);

        let mut idx = HnswIndex::new(Metric::Cosine, HnswConfig::default());
        for v in &vectors {
            idx.insert(v.clone());
        }

        let queries = random_vectors(20, dim, 55);
        let mut total_recall = 0.0;
        for q in &queries {
            let mut scored: Vec<(usize, f32)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| (i, super::super::vector::cosine_distance(v, q).unwrap()))
                .collect();
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let ground_truth: HashSet<usize> = scored.into_iter().take(k).map(|(i, _)| i).collect();
            let approx: HashSet<usize> = idx.search(q, k, None).into_iter().map(|(id, _)| id).collect();
            total_recall += ground_truth.intersection(&approx).count() as f64 / k as f64;
        }
        let avg_recall = total_recall / queries.len() as f64;
        assert!(avg_recall > 0.90, "expected >90% cosine recall, got {avg_recall}");
    }
}
