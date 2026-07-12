use anyhow::Result;
use std::collections::HashMap;

use super::decode_column;
use super::Database;
use crate::storage::ivf_pq_index::IvfPqStorageIndex;
use crate::storage::vector_index::VectorIndex;
use crate::storage::ColumnType;
use crate::storage::KeyValue;
use crate::storage::StorageEngine;
use crate::vector::gpu;
use crate::vector::hnsw::{HnswConfig, Metric};
use crate::vector::vector::Vector;
use tracing::info;

impl Database {
    /// Create a Prism vector index (`CREATE INDEX ... USING VECTOR/HNSW`) on
    /// a `VECTOR` column, backfilling from existing rows. `metric`/`config`
    /// are stored in the index file so later opens keep the same HNSW graph
    /// shape (mirrors how Meridian's `radius_hint_m` fixes a spatial index's
    /// grid level for its lifetime).
    pub fn create_vector_index(
        &self,
        table: &str,
        column: &str,
        metric: Metric,
        config: HnswConfig,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.vec", table, column));

        let mut vec_idx = VectorIndex::open(&index_path, metric, config)?;

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;

        for kv in self.scan(table)? {
            if let Some(text_bytes) = decode_column(&kv.value, col_idx) {
                if let Ok(text) = String::from_utf8(text_bytes) {
                    if let Ok(vector) = Vector::from_text(&text) {
                        vec_idx.insert(&kv.key, vector.0)?;
                    }
                }
            }
        }

        let mut idx_map = self.vector_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), vec_idx);

        info!(table, column, "vector index created");
        Ok(())
    }

    /// Whether a vector (HNSW) index exists for `table.column`.
    pub fn indexed_column_vector(&self, table: &str, column: &str) -> bool {
        self.vector_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a vector (HNSW) index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_vector_indexes(&self) -> Vec<(String, String)> {
        self.vector_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Create a Prism IVF-PQ index (`CREATE INDEX ... USING IVFPQ`) on a
    /// `VECTOR` column, training the coarse quantizer + PQ codebooks from
    /// existing rows (backfill-only — see `IvfPqStorageIndex`'s doc-comment
    /// for why this can't start empty the way `create_vector_index`'s HNSW
    /// index can).
    pub fn create_ivfpq_index(
        &self,
        table: &str,
        column: &str,
        metric: Metric,
        n_lists: usize,
        pq_m: usize,
        n_probe: usize,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.ivfpq", table, column));

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;

        let mut training = Vec::new();
        for kv in self.scan(table)? {
            if let Some(text_bytes) = decode_column(&kv.value, col_idx) {
                if let Ok(text) = String::from_utf8(text_bytes) {
                    if let Ok(vector) = Vector::from_text(&text) {
                        training.push((kv.key, vector.0));
                    }
                }
            }
        }

        let ivf_idx = IvfPqStorageIndex::train_and_create(
            &index_path,
            metric,
            n_lists,
            pq_m,
            n_probe,
            training,
        )?;

        let mut idx_map = self.ivf_pq_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), ivf_idx);

        info!(table, column, "IVF-PQ index created");
        Ok(())
    }

    /// Whether an IVF-PQ index exists for `table.column`.
    pub fn indexed_column_ivfpq(&self, table: &str, column: &str) -> bool {
        self.ivf_pq_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have an IVF-PQ index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_ivfpq_indexes(&self) -> Vec<(String, String)> {
        self.ivf_pq_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Approximate k-nearest-neighbor search against `table.column`'s vector
    /// index, returning `(row, distance)` pairs sorted nearest-first. `None`
    /// (rather than an empty vec) if no vector index of either kind exists,
    /// so callers can distinguish "no index" from "index, zero matches" —
    /// same convention as `spatial_query`/`time_range_query`.
    ///
    /// **Automatic index selection** (`TODO.md` Phase 7): when a column has
    /// *both* an HNSW and an IVF-PQ index, this picks HNSW below
    /// `IVFPQ_PREFERRED_ROW_COUNT` rows and IVF-PQ at or above it — a
    /// documented, honest size-based heuristic (HNSW gives better recall per
    /// query at small/medium scale; IVF-PQ's compressed in-memory
    /// representation matters once the graph would otherwise be large), not
    /// a latency/recall cost-model the way a real query optimizer would do
    /// it. There's no benchmark harness in this repo to tune or validate a
    /// fancier policy against (same honesty precedent as every other
    /// "automatic"/"optimal" claim in this codebase).
    pub fn vector_knn_query(
        &self,
        table: &str,
        column: &str,
        query: &[f32],
        k: usize,
        ef_search: Option<usize>,
    ) -> Option<Vec<(KeyValue, f32)>> {
        const IVFPQ_PREFERRED_ROW_COUNT: usize = 100_000;

        let hits = {
            let vec_map = self.vector_indexes.lock().unwrap();
            let hnsw_idx = vec_map.get(table).and_then(|m| m.get(column));
            let ivf_map = self.ivf_pq_indexes.lock().unwrap();
            let ivf_idx = ivf_map.get(table).and_then(|m| m.get(column));

            match (hnsw_idx, ivf_idx) {
                (Some(_hnsw), Some(ivf)) if ivf.len() >= IVFPQ_PREFERRED_ROW_COUNT => {
                    ivf.query_knn(query, k, ef_search)
                }
                (Some(hnsw), _) => hnsw.query_knn(query, k, ef_search),
                (None, Some(ivf)) => ivf.query_knn(query, k, ef_search),
                // No HNSW/IVF-PQ index: fall back to a GPU brute-force batch
                // k-NN when an adapter is available (fails safe to `None`, the
                // historical "no vector index" contract, when it isn't).
                (None, None) => return self.gpu_vector_knn(table, column, query, k, ef_search),
            }
        };
        Some(
            hits.into_iter()
                .filter_map(|(k, dist)| {
                    self.read(table, &k)
                        .ok()
                        .flatten()
                        .map(|v| (KeyValue { key: k, value: v }, dist))
                })
                .collect(),
        )
    }

    /// GPU-accelerated brute-force k-NN fallback for `vector_knn_query`, used
    /// when no HNSW/IVF-PQ index exists on `table.column`. Scans every row,
    /// extracts its `VECTOR` cell into a flat `f32` base matrix, runs the
    /// whole query×base distance matrix on the device, and returns the `k`
    /// nearest by distance.
    ///
    /// Fails safe to `None` — exactly the historical "no vector index" contract
    /// — whenever the GPU path is unavailable (`gpu_available()` is `false`),
    /// any row fails to decode, dimensions mismatch, or the dispatch is refused
    /// for being too large. So `vector_search`/`hybrid_search` keep erroring
    /// exactly as they always did when no GPU is present. Metric defaults to
    /// L2 (the HNSW default) since a bare `VECTOR` column carries no metric
    /// metadata.
    fn gpu_vector_knn(
        &self,
        table: &str,
        column: &str,
        query: &[f32],
        k: usize,
        _ef_search: Option<usize>,
    ) -> Option<Vec<(KeyValue, f32)>> {
        if !gpu::gpu_available() {
            return None;
        }
        let dim = query.len();
        if dim == 0 {
            return None;
        }
        let schema = self.get_table(table).ok().flatten()?;
        let col_pos = schema.columns.iter().position(|c| c.name == column)?;
        if schema.columns[col_pos].col_type != ColumnType::Vector {
            return None;
        }

        let rows = self.scan(table).ok()?;
        if rows.is_empty() {
            return None;
        }

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut base: Vec<f32> = Vec::with_capacity(rows.len() * dim);
        for kv in &rows {
            let text_bytes = decode_column(&kv.value, col_pos)?;
            let text = String::from_utf8(text_bytes).ok()?;
            let vector = Vector::from_text(&text).ok()?;
            if vector.dim() != dim {
                return None;
            }
            keys.push(kv.key.clone());
            base.extend_from_slice(vector.as_slice());
        }

        let hits = gpu::gpu_brute_force_knn(query, &base, dim, Metric::L2, k).ok()?;
        Some(
            hits.into_iter()
                .filter_map(|(idx, dist)| {
                    let key = &keys[idx as usize];
                    self.read(table, key).ok().flatten().map(|v| {
                        (
                            KeyValue {
                                key: key.clone(),
                                value: v,
                            },
                            dist,
                        )
                    })
                })
                .collect(),
        )
    }
}
