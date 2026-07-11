//! Plain Lloyd's-algorithm k-means, shared by the IVF coarse quantizer
//! (clusters over full vectors) and product quantization (clusters over
//! subvectors). Deterministic-enough for small test fixtures: centroids are
//! seeded by taking the first `k` distinct-ish training points rather than
//! `kmeans++`, since this is a from-scratch educational implementation, not
//! a tuned ANN library — same "correctness over cleverness" precedent as
//! `vector::hnsw`.

use super::vector::l2_distance_squared;

/// Runs Lloyd's algorithm for up to `max_iters` iterations (or until
/// assignments stop changing) and returns the `k` centroids. Panics if
/// `points` is empty or `k` is zero — callers (`ProductQuantizer::train`/
/// `IvfPqIndex::train`) are expected to validate that upfront since an empty
/// training set means "don't build this index yet."
pub fn kmeans(points: &[Vec<f32>], k: usize, max_iters: usize) -> Vec<Vec<f32>> {
    assert!(!points.is_empty(), "kmeans requires at least one point");
    assert!(k > 0, "kmeans requires k > 0");
    let dim = points[0].len();
    let k = k.min(points.len());

    // Seed centroids by striding evenly through the training set rather than
    // just taking the first k, so a caller that passes sorted/clustered
    // input still gets a reasonable initial spread.
    let mut centroids: Vec<Vec<f32>> = (0..k)
        .map(|i| points[i * points.len() / k].clone())
        .collect();

    let mut assignments = vec![0usize; points.len()];
    for _ in 0..max_iters {
        let mut changed = false;
        for (i, p) in points.iter().enumerate() {
            let mut best = 0usize;
            let mut best_dist = f32::INFINITY;
            for (c, centroid) in centroids.iter().enumerate() {
                let d = l2_distance_squared(p, centroid).unwrap_or(f32::INFINITY);
                if d < best_dist {
                    best_dist = d;
                    best = c;
                }
            }
            if assignments[i] != best {
                assignments[i] = best;
                changed = true;
            }
        }

        let mut sums = vec![vec![0.0f32; dim]; k];
        let mut counts = vec![0usize; k];
        for (i, p) in points.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for d in 0..dim {
                sums[c][d] += p[d];
            }
        }
        for c in 0..k {
            if counts[c] == 0 {
                // Dead centroid: re-seed it from the point currently
                // furthest from its own assigned centroid, so no cluster
                // silently vanishes.
                let (far_idx, _) = points
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        let d = l2_distance_squared(p, &centroids[assignments[i]])
                            .unwrap_or(0.0);
                        (i, d)
                    })
                    .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap();
                centroids[c] = points[far_idx].clone();
                continue;
            }
            for d in 0..dim {
                centroids[c][d] = sums[c][d] / counts[c] as f32;
            }
        }

        if !changed {
            break;
        }
    }

    centroids
}

/// Index of the nearest centroid to `point` (squared L2).
pub fn nearest_centroid(point: &[f32], centroids: &[Vec<f32>]) -> usize {
    centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, l2_distance_squared(point, c).unwrap_or(f32::INFINITY)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separates_two_obvious_clusters() {
        let points = vec![
            vec![0.0, 0.0],
            vec![0.1, 0.1],
            vec![-0.1, 0.0],
            vec![10.0, 10.0],
            vec![10.1, 9.9],
            vec![9.9, 10.1],
        ];
        let centroids = kmeans(&points, 2, 20);
        assert_eq!(centroids.len(), 2);
        let c0 = nearest_centroid(&[0.0, 0.0], &centroids);
        let c1 = nearest_centroid(&[10.0, 10.0], &centroids);
        assert_ne!(c0, c1);
        // Every "near origin" point should land in the same cluster.
        assert_eq!(nearest_centroid(&[0.1, 0.1], &centroids), c0);
        assert_eq!(nearest_centroid(&[9.9, 10.1], &centroids), c1);
    }

    #[test]
    fn k_larger_than_points_clamps() {
        let points = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
        let centroids = kmeans(&points, 10, 5);
        assert_eq!(centroids.len(), 2);
    }
}
