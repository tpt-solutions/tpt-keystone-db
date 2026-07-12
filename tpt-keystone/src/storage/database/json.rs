use anyhow::Result;
use std::collections::HashMap;

use serde_json::Value as JsonValue;

use super::decode_column;
use super::Database;
use crate::storage::canopy_index::{collect_json_strings, FtsIndex, JsonPathIndex};
use crate::storage::StorageEngine;
use tracing::info;

impl Database {
    /// Create a Canopy path index (`CREATE INDEX ... USING JSONPATH ON
    /// t(col) WITH (path = 'user.address.city')`) on a `Json` column,
    /// backfilling from existing rows.
    pub fn create_json_path_index(&self, table: &str, column: &str, json_path: &str) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.jsonpath", table, column));

        let mut jp = JsonPathIndex::open(&index_path, json_path)?;

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
                    jp.insert(&kv.key, &text)?;
                }
            }
        }

        let mut idx_map = self.json_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), jp);

        info!(table, column, json_path, "json path index created");
        Ok(())
    }

    /// Whether a Canopy path index exists for `table.column`.
    pub fn indexed_column_json_path(&self, table: &str, column: &str) -> bool {
        self.json_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a JSON path index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_json_indexes(&self) -> Vec<(String, String)> {
        self.json_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Row keys whose `table.column` document has `value_text` at the
    /// indexed path. `None` if no path index exists.
    pub fn json_path_lookup(
        &self,
        table: &str,
        column: &str,
        value_text: &str,
    ) -> Option<Vec<Vec<u8>>> {
        let idx_map = self.json_indexes.lock().unwrap();
        Some(idx_map.get(table)?.get(column)?.lookup(value_text))
    }

    /// Create a Canopy inverted full-text index (`CREATE INDEX ... USING
    /// GIN`/`USING FTS`) over a `Json` or `Text` column, backfilling from
    /// existing rows. For a `Json` column, only the string leaf values in
    /// each document are tokenized (see `canopy_index::collect_json_strings`).
    pub fn create_fts_index(&self, table: &str, column: &str) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.fts", table, column));

        let mut fts = FtsIndex::open(&index_path)?;

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
                    let searchable = match serde_json::from_str::<JsonValue>(&text) {
                        Ok(doc) => {
                            let mut s = String::new();
                            collect_json_strings(&doc, &mut s);
                            if s.is_empty() {
                                text.clone()
                            } else {
                                s
                            }
                        }
                        Err(_) => text.clone(),
                    };
                    fts.insert(&kv.key, &searchable)?;
                }
            }
        }

        let mut idx_map = self.fts_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), fts);

        info!(table, column, "full-text index created");
        Ok(())
    }

    /// Whether a full-text index exists for `table.column`.
    pub fn indexed_column_fts(&self, table: &str, column: &str) -> bool {
        self.fts_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a full-text index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_fts_indexes(&self) -> Vec<(String, String)> {
        self.fts_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Row keys whose `table.column` text contains every token in `query`
    /// (AND semantics). `None` if no full-text index exists.
    pub fn fts_search(&self, table: &str, column: &str, query: &str) -> Option<Vec<Vec<u8>>> {
        let idx_map = self.fts_indexes.lock().unwrap();
        Some(idx_map.get(table)?.get(column)?.search_and(query))
    }

    /// Row keys ranked by BM25 relevance against `query` (OR semantics,
    /// descending score, truncated to `limit`). `None` if no full-text index
    /// exists on `table.column`.
    pub fn fts_search_bm25(
        &self,
        table: &str,
        column: &str,
        query: &str,
        limit: usize,
    ) -> Option<Vec<(Vec<u8>, f64)>> {
        let idx_map = self.fts_indexes.lock().unwrap();
        Some(idx_map.get(table)?.get(column)?.search_bm25(query, limit))
    }
}
