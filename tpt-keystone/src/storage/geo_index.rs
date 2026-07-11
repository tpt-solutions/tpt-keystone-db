//! Local (per-node, not object-store-replicated — the same documented
//! scope cut `storage::btree`'s B-Tree indexes already carry) spatial index
//! for Meridian `GEOMETRY` columns.
//!
//! Rows are bucketed by S2-inspired cell id (`geo::s2`) at a fixed level
//! chosen at index-creation time from an expected query radius. A radius
//! query (`ST_DWithin`) covers a handful of cells via `geo::s2::covering`
//! and does an O(1) hash lookup per cell instead of scanning the table —
//! this is what makes "points within 500m, between T1 and T2" a bounded,
//! index-driven lookup rather than a full scan: the time range is filtered
//! from the same per-cell entry list, no separate index needed, since a
//! spatial cell's cardinality is already small.
//!
//! Persistence: append-only record log (like `storage::wal`, but replayed
//! fully into an in-memory map on open rather than checkpointed/compacted —
//! acceptable for a local secondary-index accelerator that's rebuilt from
//! the table on first open if the file is missing).

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::geo::s2::{self, CellId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoEntry {
    pub row_key: Vec<u8>,
    pub lon: f64,
    pub lat: f64,
    /// Time in unix milliseconds (Meridian's 4D spatiotemporal `t`
    /// ordinate), if the indexed geometry carried one.
    pub time: Option<i64>,
}

pub struct GeoIndex {
    path: PathBuf,
    level: u8,
    cells: HashMap<CellId, Vec<GeoEntry>>,
}

impl GeoIndex {
    /// Opens (replaying any existing records) or creates a fresh spatial
    /// index file. `level` is the S2-inspired grid level rows are bucketed
    /// at — pick it once at `CREATE INDEX` time via `geo::s2::level_for_radius`
    /// on the expected query radius; it's stored in the file header so a
    /// later open uses the same bucketing.
    pub fn open(path: &Path, default_level: u8) -> Result<Self> {
        if !path.exists() {
            let idx = Self {
                path: path.to_path_buf(),
                level: default_level,
                cells: HashMap::new(),
            };
            idx.write_header()?;
            return Ok(idx);
        }
        let mut file = BufReader::new(File::open(path)?);
        let mut level_buf = [0u8; 1];
        file.read_exact(&mut level_buf)?;
        let level = level_buf[0];
        let mut cells: HashMap<CellId, Vec<GeoEntry>> = HashMap::new();
        let mut len_buf = [0u8; 4];
        loop {
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            let entry: GeoEntry = bincode::deserialize(&buf)?;
            let cell = s2::cell_id_for_point(entry.lon, entry.lat, level);
            cells.entry(cell).or_default().push(entry);
        }
        Ok(Self {
            path: path.to_path_buf(),
            level,
            cells,
        })
    }

    fn write_header(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.write_all(&[self.level])?;
        Ok(())
    }

    pub fn level(&self) -> u8 {
        self.level
    }

    /// Indexes one row's geometry value. Appends to the on-disk log and
    /// updates the in-memory bucket map.
    pub fn insert(&mut self, row_key: &[u8], lon: f64, lat: f64, time: Option<i64>) -> Result<()> {
        let entry = GeoEntry {
            row_key: row_key.to_vec(),
            lon,
            lat,
            time,
        };
        let encoded = bincode::serialize(&entry)?;
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&(encoded.len() as u32).to_be_bytes())?;
        file.write_all(&encoded)?;
        let cell = s2::cell_id_for_point(lon, lat, self.level);
        self.cells.entry(cell).or_default().push(entry);
        Ok(())
    }

    /// Row keys of every indexed point within `radius_m` meters of
    /// `(center_lon, center_lat)`, optionally also restricted to
    /// `[time_start, time_end]` (inclusive) — a single covering-cell lookup
    /// evaluates both the spatial and temporal predicate together.
    pub fn query_radius(
        &self,
        center_lon: f64,
        center_lat: f64,
        radius_m: f64,
        time_range: Option<(i64, i64)>,
    ) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for cell in s2::neighborhood(center_lon, center_lat, self.level) {
            let Some(entries) = self.cells.get(&cell) else {
                continue;
            };
            for e in entries {
                let dist = crate::geo::geometry::haversine_distance_m(
                    center_lon, center_lat, e.lon, e.lat,
                );
                if dist > radius_m {
                    continue;
                }
                if let Some((t0, t1)) = time_range {
                    match e.time {
                        Some(t) if t >= t0 && t <= t1 => {}
                        _ => continue,
                    }
                }
                out.push(e.row_key.clone());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_query_radius() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.geo");
        let level = s2::level_for_radius(1000.0);
        let mut idx = GeoIndex::open(&path, level).unwrap();
        idx.insert(b"row1", -122.4194, 37.7749, Some(1000)).unwrap();
        idx.insert(b"row2", 2.3522, 48.8566, Some(2000)).unwrap(); // Paris, far away
        idx.insert(b"row3", -122.4193, 37.7750, Some(3000)).unwrap(); // near row1

        let hits = idx.query_radius(-122.4194, 37.7749, 500.0, None);
        assert!(hits.contains(&b"row1".to_vec()));
        assert!(hits.contains(&b"row3".to_vec()));
        assert!(!hits.contains(&b"row2".to_vec()));

        let hits_time = idx.query_radius(-122.4194, 37.7749, 500.0, Some((0, 1500)));
        assert!(hits_time.contains(&b"row1".to_vec()));
        assert!(!hits_time.contains(&b"row3".to_vec()));
    }

    #[test]
    fn reopen_replays_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.geo");
        let level = s2::level_for_radius(1000.0);
        {
            let mut idx = GeoIndex::open(&path, level).unwrap();
            idx.insert(b"row1", -122.4194, 37.7749, None).unwrap();
        }
        let reopened = GeoIndex::open(&path, level).unwrap();
        let hits = reopened.query_radius(-122.4194, 37.7749, 500.0, None);
        assert_eq!(hits, vec![b"row1".to_vec()]);
    }
}
