use anyhow::Result;
use std::collections::HashMap;

use super::decode_column;
use super::Database;
use crate::geo::geometry::Geometry;
use crate::geo::s2;
use crate::storage::geo_index::GeoIndex;
use crate::storage::KeyValue;
use crate::storage::StorageEngine;
use tracing::info;

impl Database {
    /// Create a Meridian spatial index (`CREATE INDEX ... USING SPATIAL`) on
    /// a `GEOMETRY` column, backfilling from existing rows. `radius_hint_m`
    /// sizes the underlying S2-inspired grid level (see
    /// `geo::s2::level_for_radius`) — pick it around the typical
    /// `ST_DWithin` radius this index will serve; it's stored in the index
    /// file so later opens keep the same bucketing.
    pub fn create_spatial_index(
        &self,
        table: &str,
        column: &str,
        radius_hint_m: f64,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.geo", table, column));

        let level = s2::level_for_radius(radius_hint_m);
        let mut geo = GeoIndex::open(&index_path, level)?;

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
            if let Some(wkt_bytes) = decode_column(&kv.value, col_idx) {
                if let Ok(wkt) = String::from_utf8(wkt_bytes) {
                    if let Ok(geom) = Geometry::from_wkt(&wkt) {
                        let c = geom.representative_point();
                        geo.insert(&kv.key, c.x, c.y, c.t)?;
                    }
                }
            }
        }

        let mut idx_map = self.geo_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), geo);

        info!(table, column, "spatial index created");
        Ok(())
    }

    /// Whether a spatial index exists for `table.column`.
    pub fn indexed_column_spatial(&self, table: &str, column: &str) -> bool {
        self.geo_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a spatial index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_spatial_indexes(&self) -> Vec<(String, String)> {
        self.geo_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Row keys within `radius_m` meters of `(lon, lat)` on `table.column`'s
    /// spatial index, optionally also filtered to a `[t0, t1]` time range —
    /// a single index lookup answers both predicates at once. Returns
    /// `None` (rather than an empty vec) if no spatial index exists, so
    /// callers can distinguish "no index" from "index, zero matches".
    pub fn spatial_query(
        &self,
        table: &str,
        column: &str,
        lon: f64,
        lat: f64,
        radius_m: f64,
        time_range: Option<(i64, i64)>,
    ) -> Option<Vec<KeyValue>> {
        let idx_map = self.geo_indexes.lock().unwrap();
        let geo = idx_map.get(table)?.get(column)?;
        let keys = geo.query_radius(lon, lat, radius_m, time_range);
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
}
