//! Native graph algorithms over `AdjacencyGraph`. Single-threaded — "native
//! parallel graph algorithms" per `TODO.md`/`5plexusspec.txt` is a documented
//! scope cut: these are correct, from-scratch implementations, but they do
//! not fork work across threads/rayon. Parallelizing PageRank's per-iteration
//! sweep or connected-components' union step would be the natural next step,
//! not attempted here so as not to claim a "parallel" implementation that
//! was never actually exercised under contention.

use std::collections::{HashSet, VecDeque};

use super::{AdjacencyGraph, Direction, VertexId};

/// Breadth-first traversal from `start`, bounded to `max_depth` hops.
/// Returns `(vertex, depth)` pairs, `start` included at depth 0.
pub fn bfs_traverse(
    g: &AdjacencyGraph,
    start: VertexId,
    max_depth: usize,
    dir: Direction,
) -> Vec<(VertexId, usize)> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut out = Vec::new();
    visited.insert(start);
    queue.push_back((start, 0usize));
    while let Some((v, depth)) = queue.pop_front() {
        out.push((v, depth));
        if depth >= max_depth {
            continue;
        }
        for (n, _) in g.neighbors(v, dir) {
            if visited.insert(n) {
                queue.push_back((n, depth + 1));
            }
        }
    }
    out
}

/// Unweighted shortest path (fewest hops) from `start` to `end`, as a
/// sequence of vertices including both endpoints. `None` if unreachable.
pub fn shortest_path(
    g: &AdjacencyGraph,
    start: VertexId,
    end: VertexId,
    dir: Direction,
) -> Option<Vec<VertexId>> {
    if start == end {
        return Some(vec![start]);
    }
    let mut visited = HashSet::new();
    let mut prev: std::collections::HashMap<VertexId, VertexId> = std::collections::HashMap::new();
    let mut queue = VecDeque::new();
    visited.insert(start);
    queue.push_back(start);
    while let Some(v) = queue.pop_front() {
        for (n, _) in g.neighbors(v, dir) {
            if visited.insert(n) {
                prev.insert(n, v);
                if n == end {
                    let mut path = vec![end];
                    let mut cur = end;
                    while let Some(&p) = prev.get(&cur) {
                        path.push(p);
                        cur = p;
                    }
                    path.reverse();
                    return Some(path);
                }
                queue.push_back(n);
            }
        }
    }
    None
}

/// Weakly-connected components (edges treated as undirected — a directed
/// graph's out+in adjacency is unioned) via BFS flood fill. Returns a
/// component id per vertex, ids are arbitrary but stable within one call.
pub fn connected_components(g: &AdjacencyGraph) -> Vec<u32> {
    let n = g.vertex_count();
    let mut component = vec![u32::MAX; n];
    let mut next_component = 0u32;
    for start in g.vertex_ids() {
        if component[start as usize] != u32::MAX {
            continue;
        }
        let mut queue = VecDeque::new();
        queue.push_back(start);
        component[start as usize] = next_component;
        while let Some(v) = queue.pop_front() {
            for (n_id, _) in g.neighbors(v, Direction::Both) {
                if component[n_id as usize] == u32::MAX {
                    component[n_id as usize] = next_component;
                    queue.push_back(n_id);
                }
            }
        }
        next_component += 1;
    }
    component
}

/// Classic power-iteration PageRank over directed out-edges. Dangling nodes
/// (no out-edges) redistribute their rank uniformly, as in the standard
/// formulation, so total rank mass is conserved.
pub fn pagerank(g: &AdjacencyGraph, damping: f64, iterations: usize) -> Vec<f64> {
    let n = g.vertex_count();
    if n == 0 {
        return Vec::new();
    }
    let base = (1.0 - damping) / n as f64;
    let mut rank = vec![1.0 / n as f64; n];
    let out_degree: Vec<usize> = g.vertex_ids().map(|v| g.out_edges(v).len()).collect();

    for _ in 0..iterations {
        let dangling_mass: f64 = g
            .vertex_ids()
            .filter(|&v| out_degree[v as usize] == 0)
            .map(|v| rank[v as usize])
            .sum();
        let mut next = vec![base + damping * dangling_mass / n as f64; n];
        for v in g.vertex_ids() {
            let deg = out_degree[v as usize];
            if deg == 0 {
                continue;
            }
            let share = damping * rank[v as usize] / deg as f64;
            for e in g.out_edges(v) {
                next[e.to as usize] += share;
            }
        }
        rank = next;
    }
    rank
}

/// Per-vertex triangle count treating edges as undirected (deduplicated
/// out+in neighbour set). Also returns the graph-wide total (each triangle
/// counted once). O(sum of deg^2) via neighbour-set intersection — the
/// "triangle indexing" the adjacency lists already provide is the O(1)
/// membership test into each vertex's neighbour `HashSet`, not a separate
/// persisted index structure.
pub fn triangle_count(g: &AdjacencyGraph) -> (Vec<u64>, u64) {
    let n = g.vertex_count();
    let neighbor_sets: Vec<HashSet<VertexId>> = g
        .vertex_ids()
        .map(|v| {
            g.neighbors(v, Direction::Both)
                .into_iter()
                .map(|(id, _)| id)
                .filter(|&id| id != v)
                .collect()
        })
        .collect();

    let mut counts = vec![0u64; n];
    let mut total = 0u64;
    for v in 0..n as VertexId {
        let nv = &neighbor_sets[v as usize];
        for &u in nv {
            if u <= v {
                continue;
            }
            let nu = &neighbor_sets[u as usize];
            let (smaller, larger) = if nv.len() < nu.len() {
                (nv, nu)
            } else {
                (nu, nv)
            };
            for &w in smaller {
                if w > u && larger.contains(&w) {
                    counts[v as usize] += 1;
                    counts[u as usize] += 1;
                    counts[w as usize] += 1;
                    total += 1;
                }
            }
        }
    }
    (counts, total)
}

/// Community detection via synchronous label propagation (Raghavan et al.):
/// each vertex adopts the most common label among its (undirected)
/// neighbours, repeated for `iterations` rounds or until stable. Chosen over
/// modularity-maximization (e.g. Louvain) as the simplest correct from-scratch
/// implementation; ties break toward the numerically smallest label id for
/// determinism.
pub fn label_propagation(g: &AdjacencyGraph, iterations: usize) -> Vec<u32> {
    let n = g.vertex_count();
    let mut labels: Vec<u32> = (0..n as u32).collect();
    let neighbor_lists: Vec<Vec<VertexId>> = g
        .vertex_ids()
        .map(|v| {
            g.neighbors(v, Direction::Both)
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        })
        .collect();

    for _ in 0..iterations {
        let mut changed = false;
        for v in 0..n {
            let neighbors = &neighbor_lists[v];
            if neighbors.is_empty() {
                continue;
            }
            let mut counts: std::collections::HashMap<u32, usize> =
                std::collections::HashMap::new();
            for &n_id in neighbors {
                *counts.entry(labels[n_id as usize]).or_insert(0) += 1;
            }
            let max_count = *counts.values().max().unwrap();
            let best = counts
                .iter()
                .filter(|&(_, &c)| c == max_count)
                .map(|(&l, _)| l)
                .min()
                .unwrap();
            if best != labels[v] {
                labels[v] = best;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    labels
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line_graph() -> AdjacencyGraph {
        // a -> b -> c -> d
        let mut g = AdjacencyGraph::new();
        g.add_edge(b"a", b"b", None);
        g.add_edge(b"b", b"c", None);
        g.add_edge(b"c", b"d", None);
        g
    }

    #[test]
    fn bfs_respects_max_depth() {
        let g = line_graph();
        let a = g.id_of(b"a").unwrap();
        let reached: Vec<usize> = bfs_traverse(&g, a, 2, Direction::Out)
            .into_iter()
            .map(|(_, d)| d)
            .collect();
        assert_eq!(reached, vec![0, 1, 2]);
    }

    #[test]
    fn shortest_path_finds_hops() {
        let g = line_graph();
        let a = g.id_of(b"a").unwrap();
        let d = g.id_of(b"d").unwrap();
        let path = shortest_path(&g, a, d, Direction::Out).unwrap();
        assert_eq!(path.len(), 4);
        assert!(shortest_path(&g, d, a, Direction::Out).is_none());
        assert!(shortest_path(&g, d, a, Direction::Both).is_some());
    }

    #[test]
    fn connected_components_separates_disjoint_subgraphs() {
        let mut g = line_graph();
        g.add_edge(b"x", b"y", None); // separate component
        let comps = connected_components(&g);
        let a = g.id_of(b"a").unwrap();
        let d = g.id_of(b"d").unwrap();
        let x = g.id_of(b"x").unwrap();
        assert_eq!(comps[a as usize], comps[d as usize]);
        assert_ne!(comps[a as usize], comps[x as usize]);
    }

    #[test]
    fn pagerank_ranks_sink_highest_in_line_graph() {
        let g = line_graph();
        let ranks = pagerank(&g, 0.85, 50);
        let a = g.id_of(b"a").unwrap() as usize;
        let d = g.id_of(b"d").unwrap() as usize;
        // d only receives rank (no out-edges to redistribute it via a normal
        // edge), so it should end up with the highest rank in a pure line.
        assert!(ranks[d] > ranks[a]);
    }

    #[test]
    fn triangle_count_finds_single_triangle() {
        let mut g = AdjacencyGraph::new();
        g.add_edge(b"a", b"b", None);
        g.add_edge(b"b", b"c", None);
        g.add_edge(b"c", b"a", None);
        let (counts, total) = triangle_count(&g);
        assert_eq!(total, 1);
        assert!(counts.iter().all(|&c| c == 1));
    }

    #[test]
    fn label_propagation_merges_connected_vertices() {
        let g = line_graph();
        let labels = label_propagation(&g, 10);
        let a = g.id_of(b"a").unwrap() as usize;
        let b = g.id_of(b"b").unwrap() as usize;
        assert_eq!(labels[a], labels[b]);
    }
}
