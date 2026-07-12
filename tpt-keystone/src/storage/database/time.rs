use anyhow::Result;
use std::collections::HashMap;

use super::decode_column;
use super::decode_f64;
use super::decode_i64;
use super::Database;
use crate::storage::ts_index::{Rollup, TimeBucketPolicy, TimeIndex};
use crate::storage::KeyValue;
use crate::storage::StorageEngine;
use tracing::info;

impl Database {
    /// Create a Chronos time index (`CREATE INDEX ... USING TIME`) on a
    /// `TIMESTAMP` column, backfilling from existing rows. `value_column`
    /// names the numeric column whose values are bucketed/compressed
    /// alongside each row's timestamp (see `storage::ts_index`); `policy`
    /// fixes the bucket granularity and retention for the lifetime of the
    /// index, stored in the index file so later opens keep the same
    /// bucketing.
    pub fn create_time_index(
        &self,
        table: &str,
        column: &str,
        value_column: &str,
        policy: TimeBucketPolicy,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.ts", table, column));

        let mut ts = TimeIndex::open(&index_path, policy, value_column)?;

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let ts_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;
        let val_idx = schema
            .columns
            .iter()
            .position(|c| c.name == value_column)
            .ok_or_else(|| anyhow::anyhow!("column \"{value_column}\" does not exist"))?;

        for kv in self.scan(table)? {
            let ts_bytes = decode_column(&kv.value, ts_idx);
            let val_bytes = decode_column(&kv.value, val_idx);
            if let (Some(ts_bytes), Some(val_bytes)) = (ts_bytes, val_bytes) {
                if let (Some(t), Some(v)) = (decode_i64(&ts_bytes), decode_f64(&val_bytes)) {
                    ts.insert(&kv.key, t, v)?;
                }
            }
        }

        let mut idx_map = self.ts_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(
                column.to_string(),
                super::TsIndexEntry {
                    index: ts,
                    value_column: value_column.to_string(),
                },
            );

        info!(table, column, value_column, "time index created");
        Ok(())
    }

    /// Whether a Chronos time index exists for `table.column`.
    pub fn indexed_column_time(&self, table: &str, column: &str) -> bool {
        self.ts_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a time index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_time_indexes(&self) -> Vec<(String, String)> {
        self.ts_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Row keys with `t0 <= timestamp <= t1` on `table.column`'s time index.
    /// Returns `None` (rather than an empty vec) if no time index exists.
    pub fn time_range_query(
        &self,
        table: &str,
        column: &str,
        t0: i64,
        t1: i64,
    ) -> Option<Vec<KeyValue>> {
        let idx_map = self.ts_indexes.lock().unwrap();
        let ts = &idx_map.get(table)?.get(column)?.index;
        let keys = ts.query_range(t0, t1);
        drop(idx_map);
        Some(
            keys.into_iter()
                .filter_map(|k| {
                    self.read(table, &k)
                        .ok()
                        .flatten()
                        .map(|v| KeyValue { key: k, value: v })
                })
                .collect(),
        )
    }

    /// Per-bucket rollups covering `[t0, t1]` on `table.column`'s time
    /// index — the continuous-aggregate query path (e.g. `moving_average`),
    /// which can answer from rollups even for buckets whose raw rows have
    /// already been evicted by retention. Returns `None` if no time index
    /// exists.
    pub fn rollup_query(
        &self,
        table: &str,
        column: &str,
        t0: i64,
        t1: i64,
    ) -> Option<Vec<(i64, Rollup)>> {
        let idx_map = self.ts_indexes.lock().unwrap();
        let ts = &idx_map.get(table)?.get(column)?.index;
        Some(ts.query_rollups(t0, t1))
    }
}
