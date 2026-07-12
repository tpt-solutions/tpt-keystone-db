use anyhow::Result;
use std::collections::HashMap;

use super::decode_column;
use super::Database;
use crate::graph::{algorithms, Direction};
use crate::storage::graph_index::GraphIndex;
use crate::storage::StorageEngine;
use tracing::info;

impl Database {
    /// Create a Plexus adjacency index (`CREATE INDEX ... USING GRAPH`) on an
    /// edge table, backfilling from existing rows. `from_column` is the
    /// indexed column (matches `CreateIndexStmt.column`); `to_column` names
    /// the destination-vertex column and `type_column` (optional) names a
    /// relationship-type column for multi-relational (typed-edge) graphs.
    pub fn create_graph_index(
        &self,
        table: &str,
        from_column: &str,
        to_column: &str,
        type_column: Option<&str>,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.graph", table, from_column));

        let mut graph = GraphIndex::open(&index_path, to_column, type_column)?;

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let from_idx = schema
            .columns
            .iter()
            .position(|c| c.name == from_column)
            .ok_or_else(|| anyhow::anyhow!("column \"{from_column}\" does not exist"))?;
        let to_idx = schema
            .columns
            .iter()
            .position(|c| c.name == to_column)
            .ok_or_else(|| anyhow::anyhow!("column \"{to_column}\" does not exist"))?;
        let type_idx = type_column
            .map(|t| {
                schema
                    .columns
                    .iter()
                    .position(|c| c.name == t)
                    .ok_or_else(|| anyhow::anyhow!("column \"{t}\" does not exist"))
            })
            .transpose()?;

        for kv in self.scan(table)? {
            let from_bytes = decode_column(&kv.value, from_idx);
            let to_bytes = decode_column(&kv.value, to_idx);
            if let (Some(from_bytes), Some(to_bytes)) = (from_bytes, to_bytes) {
                let rel_type = type_idx
                    .and_then(|i| decode_column(&kv.value, i))
                    .and_then(|b| String::from_utf8(b).ok());
                graph.insert(&from_bytes, &to_bytes, rel_type)?;
            }
        }

        let mut idx_map = self.graph_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(from_column.to_string(), graph);

        info!(table, from_column, to_column, "graph index created");
        Ok(())
    }

    /// Whether a graph (adjacency) index exists for `table.from_column`.
    pub fn indexed_column_graph(&self, table: &str, from_column: &str) -> bool {
        self.graph_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(from_column))
    }

    /// List all `(table, from_column)` pairs that have a graph index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_graph_indexes(&self) -> Vec<(String, String)> {
        self.graph_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Every vertex key `table.from_column`'s graph index knows about.
    /// `None` if no such index exists. Used by `MATCH` (`executor::gql`)
    /// when a pattern's starting node has no `WHERE`-clause filter, so
    /// every vertex is a candidate start — real but potentially expensive
    /// on a large graph, the documented tradeoff of not having a real
    /// WHERE-clause planner for `MATCH` yet (see `ast::MatchStmt`'s doc).
    pub fn graph_all_vertices(&self, table: &str, from_column: &str) -> Option<Vec<Vec<u8>>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        Some(
            graph
                .vertex_ids()
                .filter_map(|id| graph.key_of(id).map(|k| k.to_vec()))
                .collect(),
        )
    }

    /// Neighbours of `vertex_key` in the given direction on `table.from_column`'s
    /// graph index, each as `(neighbour_key, rel_type)`. `None` if no such
    /// index exists or the vertex was never indexed (no edges touch it).
    pub fn graph_neighbors(
        &self,
        table: &str,
        from_column: &str,
        vertex_key: &[u8],
        dir: Direction,
    ) -> Option<Vec<(Vec<u8>, Option<String>)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let id = graph.id_of(vertex_key)?;
        Some(
            graph
                .neighbors(id, dir)
                .into_iter()
                .filter_map(|(n, rel)| graph.key_of(n).map(|k| (k.to_vec(), rel)))
                .collect(),
        )
    }

    /// Bounded-depth breadth-first traversal from `start_key`, as
    /// `(vertex_key, depth)` pairs.
    pub fn graph_bfs(
        &self,
        table: &str,
        from_column: &str,
        start_key: &[u8],
        max_depth: usize,
        dir: Direction,
    ) -> Option<Vec<(Vec<u8>, usize)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let start = graph.id_of(start_key)?;
        Some(
            algorithms::bfs_traverse(graph, start, max_depth, dir)
                .into_iter()
                .filter_map(|(id, depth)| graph.key_of(id).map(|k| (k.to_vec(), depth)))
                .collect(),
        )
    }

    /// Unweighted shortest path between two vertex keys, as an ordered list
    /// of vertex keys including both endpoints. `Some(None)` means the index
    /// exists but no path was found; `None` means the index or an endpoint
    /// vertex doesn't exist.
    pub fn graph_shortest_path(
        &self,
        table: &str,
        from_column: &str,
        start_key: &[u8],
        end_key: &[u8],
        dir: Direction,
    ) -> Option<Option<Vec<Vec<u8>>>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let start = graph.id_of(start_key)?;
        let end = graph.id_of(end_key)?;
        Some(
            algorithms::shortest_path(graph, start, end, dir).map(|path| {
                path.into_iter()
                    .filter_map(|id| graph.key_of(id).map(|k| k.to_vec()))
                    .collect()
            }),
        )
    }

    /// Weakly-connected component id per vertex, as `(vertex_key, component_id)`.
    pub fn graph_connected_components(
        &self,
        table: &str,
        from_column: &str,
    ) -> Option<Vec<(Vec<u8>, u32)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let components = algorithms::connected_components(graph);
        Some(
            graph
                .vertex_ids()
                .filter_map(|id| {
                    graph
                        .key_of(id)
                        .map(|k| (k.to_vec(), components[id as usize]))
                })
                .collect(),
        )
    }

    /// PageRank score per vertex, as `(vertex_key, score)`.
    pub fn graph_pagerank(
        &self,
        table: &str,
        from_column: &str,
        damping: f64,
        iterations: usize,
    ) -> Option<Vec<(Vec<u8>, f64)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let ranks = algorithms::pagerank(graph, damping, iterations);
        Some(
            graph
                .vertex_ids()
                .filter_map(|id| graph.key_of(id).map(|k| (k.to_vec(), ranks[id as usize])))
                .collect(),
        )
    }

    /// Per-vertex triangle count, as `(vertex_key, triangle_count)`.
    pub fn graph_triangle_count(
        &self,
        table: &str,
        from_column: &str,
    ) -> Option<Vec<(Vec<u8>, u64)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let (counts, _total) = algorithms::triangle_count(graph);
        Some(
            graph
                .vertex_ids()
                .filter_map(|id| graph.key_of(id).map(|k| (k.to_vec(), counts[id as usize])))
                .collect(),
        )
    }
}
