use anyhow::Result;
use std::collections::HashMap;

use super::decode_column;
use super::decode_f64;
use super::decode_i64;
use super::Database;
use tracing::info;

use crate::geo::geometry::Geometry;
use crate::storage::btree::BTree;
use crate::storage::canopy_index::collect_json_strings;
use crate::storage::ColumnDef;
use crate::storage::KeyValue;
use crate::storage::StorageEngine;
use crate::storage::TableSchema;
use crate::vector::vector::Vector;

impl StorageEngine for Database {
    fn write(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_writable()?;
        let composite_key = Self::make_key(table, key);
        self.lsm
            .lock()
            .unwrap()
            .write(table, &composite_key, value)?;

        // Maintain any B-Tree indexes defined on this table.
        let indexed_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !indexed_cols.is_empty() {
            let mut idx_map = self.indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in indexed_cols {
                    if let Some(col_bytes) = decode_column(value, pos) {
                        if let Some(btree) = cols.get_mut(&col) {
                            btree.insert(&col_bytes, key)?;
                        }
                    }
                }
            }
        }

        // Maintain any spatial (Meridian) indexes defined on this table.
        let geo_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.geo_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !geo_cols.is_empty() {
            let mut idx_map = self.geo_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in geo_cols {
                    if let Some(wkt_bytes) = decode_column(value, pos) {
                        if let (Ok(wkt), Some(geo)) =
                            (String::from_utf8(wkt_bytes), cols.get_mut(&col))
                        {
                            if let Ok(geom) = Geometry::from_wkt(&wkt) {
                                let c = geom.representative_point();
                                geo.insert(key, c.x, c.y, c.t)?;
                            }
                        }
                    }
                }
            }
        }

        // Maintain any Chronos time indexes defined on this table.
        let ts_cols: Vec<(String, usize, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.ts_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .iter()
                    .filter_map(|(col, entry)| {
                        let ts_pos = schema.columns.iter().position(|c| c.name == *col)?;
                        let val_pos = schema
                            .columns
                            .iter()
                            .position(|c| c.name == entry.value_column)?;
                        Some((col.clone(), ts_pos, val_pos))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !ts_cols.is_empty() {
            let mut idx_map = self.ts_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, ts_pos, val_pos) in ts_cols {
                    let ts_bytes = decode_column(value, ts_pos);
                    let val_bytes = decode_column(value, val_pos);
                    if let (Some(ts_bytes), Some(val_bytes)) = (ts_bytes, val_bytes) {
                        if let (Some(ts), Some(val)) =
                            (decode_i64(&ts_bytes), decode_f64(&val_bytes))
                        {
                            if let Some(entry) = cols.get_mut(&col) {
                                entry.index.insert(key, ts, val)?;
                            }
                        }
                    }
                }
            }
        }

        // Maintain any Plexus graph (adjacency) indexes defined on this
        // table: `col` is the from-column; `to`/`type` columns are recorded
        // in the `GraphIndex` itself (set at `CREATE INDEX` time).
        let graph_cols: Vec<(String, usize, usize, Option<usize>)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.graph_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .iter()
                    .filter_map(|(col, graph)| {
                        let from_pos = schema.columns.iter().position(|c| c.name == *col)?;
                        let to_pos = schema
                            .columns
                            .iter()
                            .position(|c| c.name == graph.to_column())?;
                        let type_pos = graph
                            .type_column()
                            .and_then(|t| schema.columns.iter().position(|c| c.name == t));
                        Some((col.clone(), from_pos, to_pos, type_pos))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !graph_cols.is_empty() {
            let mut idx_map = self.graph_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, from_pos, to_pos, type_pos) in graph_cols {
                    let from_bytes = decode_column(value, from_pos);
                    let to_bytes = decode_column(value, to_pos);
                    if let (Some(from_bytes), Some(to_bytes)) = (from_bytes, to_bytes) {
                        let rel_type = type_pos
                            .and_then(|p| decode_column(value, p))
                            .and_then(|b| String::from_utf8(b).ok());
                        if let Some(graph) = cols.get_mut(&col) {
                            graph.insert(&from_bytes, &to_bytes, rel_type)?;
                        }
                    }
                }
            }
        }

        // Maintain any Canopy JSON path indexes defined on this table.
        let json_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.json_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !json_cols.is_empty() {
            let mut idx_map = self.json_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in json_cols {
                    if let Some(text_bytes) = decode_column(value, pos) {
                        if let (Ok(text), Some(jp)) =
                            (String::from_utf8(text_bytes), cols.get_mut(&col))
                        {
                            jp.insert(key, &text)?;
                        }
                    }
                }
            }
        }

        // Maintain any Canopy full-text indexes defined on this table.
        let fts_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.fts_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !fts_cols.is_empty() {
            let mut idx_map = self.fts_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in fts_cols {
                    if let Some(text_bytes) = decode_column(value, pos) {
                        if let (Ok(text), Some(fts)) =
                            (String::from_utf8(text_bytes), cols.get_mut(&col))
                        {
                            let searchable = match serde_json::from_str::<serde_json::Value>(&text)
                            {
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
                            fts.insert(key, &searchable)?;
                        }
                    }
                }
            }
        }

        // Maintain any Prism vector (HNSW) indexes defined on this table.
        let vector_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.vector_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !vector_cols.is_empty() {
            let mut idx_map = self.vector_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in vector_cols {
                    if let Some(text_bytes) = decode_column(value, pos) {
                        if let (Ok(text), Some(vec_idx)) =
                            (String::from_utf8(text_bytes), cols.get_mut(&col))
                        {
                            if let Ok(vector) = Vector::from_text(&text) {
                                vec_idx.insert(key, vector.0)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn read(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let composite_key = Self::make_key(table, key);
        self.lsm.lock().unwrap().read(&composite_key)
    }

    fn delete(&self, table: &str, key: &[u8]) -> Result<()> {
        self.check_writable()?;
        let composite_key = Self::make_key(table, key);
        self.lsm.lock().unwrap().delete(table, &composite_key)?;
        Ok(())
    }

    fn scan(&self, table: &str) -> Result<Vec<KeyValue>> {
        let all = self.lsm.lock().unwrap().scan()?;
        let prefix = Self::make_prefix(table);
        let results = all
            .into_iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, v)| {
                // Strip the table prefix from the key
                let key = k[prefix.len()..].to_vec();
                KeyValue { key, value: v }
            })
            .collect();
        Ok(results)
    }

    fn create_table(&self, name: &str, columns: &[ColumnDef]) -> Result<()> {
        self.create_table_with_constraints(name, columns, vec![], vec![])
    }

    fn get_table(&self, name: &str) -> Result<Option<TableSchema>> {
        Ok(self.schemas.lock().unwrap().get(name).cloned())
    }

    fn list_tables(&self) -> Result<Vec<String>> {
        Ok(self.schemas.lock().unwrap().keys().cloned().collect())
    }

    /// Create a B-Tree index on a column, backfilling it from existing rows.
    fn create_index(&self, table: &str, column: &str) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}", table, column));

        let mut btree = BTree::open(&index_path)?;

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
            if let Some(col_bytes) = decode_column(&kv.value, col_idx) {
                btree.insert(&col_bytes, &kv.key)?;
            }
        }

        let mut idx_map = self.indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), btree);

        info!(table, column, "index created");
        Ok(())
    }
}
