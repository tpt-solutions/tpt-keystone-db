//! TPT Plexus — graph engine, implemented as a native module inside Keystone
//! (per `5plexusspec.txt`) rather than a separate crate/process, following
//! the same "engine as a module, not a bolt-on" precedent as `geo` (Meridian)
//! and `storage::ts_index` (Chronos).
//!
//! Scope actually implemented in this pass:
//! - `AdjacencyGraph`: a property-graph adjacency-list in dense-integer form
//!   — vertex identities (arbitrary row-key bytes, e.g. a table's primary
//!   key column value) are interned to a `VertexId` (`u32`) once, and both
//!   the outgoing and incoming adjacency lists are `Vec<Vec<Edge>>` indexed
//!   directly by that dense id. This gets the *shape* of "zero-copy
//!   neighbour traversal" — no join, no hash lookup per hop, direct slice
//!   indexing — but is honestly not the memory-mapped/pointer-chasing
//!   zero-copy layout a production graph engine would use; it's an in-memory
//!   `Vec`-of-`Vec` structure, rebuilt from a WAL-style log on open (see
//!   `storage::graph_index`), same durability model as Meridian's spatial
//!   index and Chronos's time index.
//! - Property graph model: vertices are identified by their raw key bytes
//!   (whatever a `CREATE INDEX ... USING GRAPH` from-column's values are);
//!   edges carry an optional `rel_type` string, so multi-relational
//!   (typed-edge) graphs are supported natively rather than needing a
//!   separate table per relationship type.
//! - Bidirectional traversal: every edge is recorded in both `out_adj` (at
//!   the source vertex) and `in_adj` (at the destination vertex), so
//!   `Direction::{Out,In,Both}` traversals are all direct lookups, not
//!   derived by re-scanning.
//! - `algorithms`: BFS-based shortest path and bounded-depth traversal,
//!   connected components, PageRank, triangle counting, and label
//!   propagation (community detection).
//!
//! Explicitly NOT implemented (documented scope cuts, tracked in
//! `TODO.md`): a GQL grammar/parser (a full graph query language is a
//! separate, large grammar-and-planner effort — see `TODO.md`), triangle
//! indexing beyond the O(1) neighbour-set membership test `algorithms`
//! already gets from `AdjacencyGraph`'s adjacency vectors, and true
//! zero-copy (mmap/pointer) storage. Traversal/algorithm entry points are
//! exposed to SQL as table-valued functions in the `FROM` clause (e.g.
//! `SELECT * FROM graph_bfs('edges', 'from_id', '1', 3)`), which is the
//! "hybrid SQL + graph" surface actually implemented — not a `MATCH (a)-[]->(b)`
//! GQL pattern grammar.

pub mod algorithms;

use std::collections::HashMap;

pub type VertexId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

impl Direction {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "out" | "outgoing" => Some(Self::Out),
            "in" | "incoming" => Some(Self::In),
            "both" | "any" => Some(Self::Both),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub to: VertexId,
    pub rel_type: Option<String>,
}

/// A property-graph adjacency-list index over dense integer vertex ids.
/// Vertex identity is external (row-key bytes); this structure only knows
/// about the ids it has interned via `intern`.
#[derive(Debug, Default)]
pub struct AdjacencyGraph {
    key_to_id: HashMap<Vec<u8>, VertexId>,
    /// Dense id -> original key bytes, for translating algorithm results
    /// back to vertex identities.
    keys: Vec<Vec<u8>>,
    out_adj: Vec<Vec<Edge>>,
    in_adj: Vec<Vec<Edge>>,
}

impl AdjacencyGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-create the dense id for a vertex identified by `key`.
    pub fn intern(&mut self, key: &[u8]) -> VertexId {
        if let Some(&id) = self.key_to_id.get(key) {
            return id;
        }
        let id = self.keys.len() as VertexId;
        self.keys.push(key.to_vec());
        self.out_adj.push(Vec::new());
        self.in_adj.push(Vec::new());
        self.key_to_id.insert(key.to_vec(), id);
        id
    }

    pub fn id_of(&self, key: &[u8]) -> Option<VertexId> {
        self.key_to_id.get(key).copied()
    }

    pub fn key_of(&self, id: VertexId) -> Option<&[u8]> {
        self.keys.get(id as usize).map(|v| v.as_slice())
    }

    pub fn vertex_count(&self) -> usize {
        self.keys.len()
    }

    pub fn vertex_ids(&self) -> impl Iterator<Item = VertexId> {
        0..self.keys.len() as VertexId
    }

    pub fn add_edge(&mut self, from: &[u8], to: &[u8], rel_type: Option<String>) {
        let from_id = self.intern(from);
        let to_id = self.intern(to);
        self.out_adj[from_id as usize].push(Edge { to: to_id, rel_type: rel_type.clone() });
        self.in_adj[to_id as usize].push(Edge { to: from_id, rel_type });
    }

    pub fn out_edges(&self, id: VertexId) -> &[Edge] {
        self.out_adj.get(id as usize).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn in_edges(&self, id: VertexId) -> &[Edge] {
        self.in_adj.get(id as usize).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Neighbours of `id` in the given direction, each as `(neighbour_id,
    /// rel_type)`. `Direction::Both` unions out- and in-neighbours
    /// (duplicates kept — a vertex connected by two typed edges to the same
    /// neighbour is two distinct relationships, not one).
    pub fn neighbors(&self, id: VertexId, dir: Direction) -> Vec<(VertexId, Option<String>)> {
        match dir {
            Direction::Out => self.out_edges(id).iter().map(|e| (e.to, e.rel_type.clone())).collect(),
            Direction::In => self.in_edges(id).iter().map(|e| (e.to, e.rel_type.clone())).collect(),
            Direction::Both => {
                let mut v: Vec<_> = self.out_edges(id).iter().map(|e| (e.to, e.rel_type.clone())).collect();
                v.extend(self.in_edges(id).iter().map(|e| (e.to, e.rel_type.clone())));
                v
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_is_stable_and_bidirectional() {
        let mut g = AdjacencyGraph::new();
        g.add_edge(b"a", b"b", Some("FOLLOWS".into()));
        let a = g.id_of(b"a").unwrap();
        let b = g.id_of(b"b").unwrap();
        assert_eq!(g.neighbors(a, Direction::Out), vec![(b, Some("FOLLOWS".into()))]);
        assert_eq!(g.neighbors(b, Direction::In), vec![(a, Some("FOLLOWS".into()))]);
        assert!(g.neighbors(a, Direction::In).is_empty());
    }

    #[test]
    fn direction_parse() {
        assert_eq!(Direction::parse("OUT"), Some(Direction::Out));
        assert_eq!(Direction::parse("in"), Some(Direction::In));
        assert_eq!(Direction::parse("both"), Some(Direction::Both));
        assert_eq!(Direction::parse("sideways"), None);
    }
}
