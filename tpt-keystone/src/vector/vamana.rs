//! Vamana graph construction — the algorithm behind Microsoft's DiskANN
//! (Subramanya et al., "DiskANN: Fast Accurate Billion-point Nearest
//! Neighbor Search on a Single Node", NeurIPS 2019). This module is the
//! pure, in-memory graph-*building* half; `storage::diskann_index` is the
//! disk-resident structure that stores the result and answers queries
//! without ever loading the full vector set back into memory (the actual
//! "billion-scale on-disk graph" property that distinguishes this from
//! `vector::hnsw`, whose graph and vectors always live fully in RAM).
//!
//! Real, from-scratch Vamana, not a relabeled HNSW: greedy search from a
//! fixed medoid entry point plus `RobustPrune`'s alpha-pruning rule (which
//! is what gives Vamana its long-range edges and bounded out-degree, unlike
//! HNSW's layered small-world structure). Honest scope cut vs the paper:
//! single pruning pass at the configured `alpha` (the paper's two-pass
//! `alpha=1` then `alpha=1.2` refinement is skipped — this is graph
//! *quality* tuning, not a correctness gap) and no incremental insert
//! (`build` is a batch operation over the full point set, same "hold the
//! current corpus, train/retrain don't-incrementally-update" precedent as
//! `vector::ivf_pq`'s IVF-PQ training, not HNSW's true insert-in-place).

use std::collections::HashSet;

use super::hnsw::Metric;
use super::vector::{cosine_distance, l2_distance_squared};

/// Distance under `metric`, matching `hnsw::Metric`'s own dispatch (kept
/// `pub` here since `storage::diskann_index` needs the same dispatch when
/// scoring a query against a vector it just read off disk).
pub fn distance(a: &[f32], b: &[f32], metric: Metric) -> f32 {
    match metric {
        Metric::L2 => l2_distance_squared(a, b).unwrap_or(f32::INFINITY),
        Metric::Cosine => cosine_distance(a, b).unwrap_or(f32::INFINITY),
    }
}

pub struct VamanaGraph {
    pub medoid: usize,
    /// `edges[i]` is point `i`'s out-neighbor list, length `<= r`.
    pub edges: Vec<Vec<u32>>,
}

/// The point closest to the centroid of `points` — Vamana's fixed search
/// entry point (DiskANN's paper calls this the "medoid").
fn compute_medoid(points: &[Vec<f32>], metric: Metric) -> usize {
    let dim = points[0].len();
    let mut centroid = vec![0f32; dim];
    for p in points {
        for i in 0..dim {
            centroid[i] += p[i];
        }
    }
    let n = points.len() as f32;
    for c in centroid.iter_mut() {
        *c /= n;
    }
    let mut best = 0usize;
    let mut best_d = f32::INFINITY;
    for (i, p) in points.iter().enumerate() {
        let d = distance(p, &centroid, metric);
        if d < best_d {
            best_d = d;
            best = i;
        }
    }
    best
}

/// Greedy best-first search over the (partially built) graph from `start`
/// toward `query`, keeping a candidate list bounded to `l` entries. Returns
/// every node id visited (expanded) during the search — the candidate pool
/// `RobustPrune` prunes down to a point's final neighbor list.
fn greedy_search(
    points: &[Vec<f32>],
    edges: &[Vec<u32>],
    start: usize,
    query: &[f32],
    l: usize,
    metric: Metric,
) -> Vec<usize> {
    let mut visited: HashSet<usize> = HashSet::new();
    let mut expanded: HashSet<usize> = HashSet::new();
    let mut candidates: Vec<(usize, f32)> = vec![(start, distance(&points[start], query, metric))];

    loop {
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(l.max(1));
        let Some(&(p, _)) = candidates.iter().find(|(id, _)| !expanded.contains(id)) else {
            break;
        };
        expanded.insert(p);
        visited.insert(p);
        for &nb in &edges[p] {
            let nb = nb as usize;
            if visited.contains(&nb) || candidates.iter().any(|(id, _)| *id == nb) {
                continue;
            }
            candidates.push((nb, distance(&points[nb], query, metric)));
        }
    }
    visited.into_iter().collect()
}

/// `RobustPrune` (DiskANN paper, Algorithm 2): given a candidate pool for
/// point `p`, greedily keeps the closest remaining candidate and discards
/// any other candidate that `p*` already "covers" (is more than `1/alpha`
/// times closer to than `p` is) — this is what gives Vamana graphs their
/// long-range edges instead of only ever connecting to the nearest cluster,
/// while still bounding out-degree to `r`.
fn robust_prune(
    p: usize,
    mut candidates: Vec<usize>,
    points: &[Vec<f32>],
    metric: Metric,
    alpha: f32,
    r: usize,
) -> Vec<usize> {
    candidates.retain(|&c| c != p);
    candidates.sort_by(|&a, &b| {
        distance(&points[p], &points[a], metric)
            .partial_cmp(&distance(&points[p], &points[b], metric))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut result = Vec::new();
    let mut remaining = candidates;
    while let Some(&p_star) = remaining.first() {
        if result.len() >= r {
            break;
        }
        result.push(p_star);
        remaining.retain(|&pp| {
            if pp == p_star {
                return false;
            }
            let d_pstar_pp = distance(&points[p_star], &points[pp], metric);
            let d_p_pp = distance(&points[p], &points[pp], metric);
            alpha * d_pstar_pp > d_p_pp
        });
    }
    result
}

/// Builds a Vamana graph over `points` (batch, not incremental — see module
/// doc). `r` bounds each node's out-degree (DiskANN's on-disk record size
/// depends on this), `l_build` is the construction-time search-list width
/// (larger = better graph quality, slower build), `alpha` controls
/// `RobustPrune`'s long-range-edge aggressiveness (DiskANN's default is
/// `1.2`; `1.0` degrades toward a purely-greedy, more clustered graph).
pub fn build(
    points: &[Vec<f32>],
    metric: Metric,
    r: usize,
    l_build: usize,
    alpha: f32,
) -> VamanaGraph {
    assert!(
        !points.is_empty(),
        "vamana::build requires at least one point"
    );
    let n = points.len();
    let mut edges: Vec<Vec<u32>> = vec![Vec::new(); n];
    let medoid = compute_medoid(points, metric);

    let mut order: Vec<usize> = (0..n).collect();
    {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        order.shuffle(&mut rng);
    }

    for p in order {
        let visited = greedy_search(points, &edges, medoid, &points[p], l_build, metric);
        let pruned = robust_prune(p, visited, points, metric, alpha, r);
        edges[p] = pruned.iter().map(|&x| x as u32).collect();

        for &nb in &pruned {
            if edges[nb].iter().any(|&x| x as usize == p) {
                continue;
            }
            edges[nb].push(p as u32);
            if edges[nb].len() > r {
                let cands: Vec<usize> = edges[nb].iter().map(|&x| x as usize).collect();
                let repruned = robust_prune(nb, cands, points, metric, alpha, r);
                edges[nb] = repruned.iter().map(|&x| x as u32).collect();
            }
        }
    }

    VamanaGraph { medoid, edges }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brute_force_knn(points: &[Vec<f32>], query: &[f32], k: usize, metric: Metric) -> Vec<usize> {
        let mut scored: Vec<(usize, f32)> = points
            .iter()
            .enumerate()
            .map(|(i, p)| (i, distance(p, query, metric)))
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        scored.into_iter().take(k).map(|(i, _)| i).collect()
    }

    #[test]
    fn graph_has_bounded_out_degree() {
        let points: Vec<Vec<f32>> = (0..200)
            .map(|i| vec![(i % 20) as f32, (i % 7) as f32, (i % 3) as f32])
            .collect();
        let r = 16;
        let graph = build(&points, Metric::L2, r, 32, 1.2);
        for edges in &graph.edges {
            assert!(edges.len() <= r, "node exceeded max degree {r}: {edges:?}");
        }
    }

    #[test]
    fn greedy_search_finds_high_recall_neighbors() {
        // Well-separated clusters so an approximate graph search should
        // reliably land in the right cluster, same recall-check shape as
        // `hnsw::tests::knn_recall_against_brute_force_is_high`.
        let mut points = Vec::new();
        for cluster in 0..8 {
            let base = (cluster as f32) * 100.0;
            for i in 0..30 {
                points.push(vec![base + (i % 5) as f32, base + (i % 3) as f32]);
            }
        }
        let graph = build(&points, Metric::L2, 12, 48, 1.2);

        let mut hits = 0;
        let mut total = 0;
        for (qi, q) in points.iter().enumerate() {
            let expected: HashSet<usize> = brute_force_knn(&points, q, 5, Metric::L2)
                .into_iter()
                .collect();
            let found: HashSet<usize> =
                greedy_search(&points, &graph.edges, graph.medoid, q, 48, Metric::L2)
                    .into_iter()
                    .filter(|id| expected.contains(id))
                    .collect();
            hits += found.len();
            total += expected.len();
            let _ = qi;
        }
        let recall = hits as f64 / total as f64;
        assert!(recall > 0.9, "recall too low: {recall}");
    }

    #[test]
    fn single_point_graph_is_valid() {
        let points = vec![vec![1.0, 2.0, 3.0]];
        let graph = build(&points, Metric::L2, 8, 16, 1.2);
        assert_eq!(graph.medoid, 0);
        assert!(graph.edges[0].is_empty());
    }
}
