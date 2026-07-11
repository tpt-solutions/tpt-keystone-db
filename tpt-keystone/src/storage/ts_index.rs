//! Local (per-node, not object-store-replicated — the same documented scope
//! cut `geo_index.rs`'s spatial index and `btree.rs`'s B-Tree indexes
//! already carry) time-partitioned secondary index for Chronos.
//!
//! Rows are bucketed by fixed time window (`policy.granularity_ms` — hourly/
//! daily/monthly, chosen at `CREATE INDEX` time), giving Chronos's
//! "automatic time-based partitioning". Each bucket incrementally maintains
//! a [`Rollup`] (count/sum/min/max) on every insert — the substrate for
//! Chronos's "continuous aggregates" (no full `CREATE MATERIALIZED VIEW`
//! machinery exists in this engine, so aggregate queries read the rollup
//! directly instead of a materialized-view relation). Once a bucket is no
//! longer the newest (a later timestamp has been inserted), it's sealed and
//! its `(timestamp, value)` series is Gorilla/delta-of-delta compressed
//! (`storage::compress`) — mirroring how an LSM memtable becomes an
//! immutable SSTable. `policy.retention_ms`/`downsample_to_ms` implement
//! retention + automatic downsampling: on every insert, buckets older than
//! the retention cutoff are evicted down to just their `Rollup` (raw/
//! compressed series dropped), which is what "continuous aggregates that
//! outlive their raw data" means in this codebase.
//!
//! Persistence: append-only record log (same format convention as
//! `geo_index.rs`), replayed fully into memory on open — acceptable for a
//! local secondary-index accelerator that's rebuilt from the table if the
//! file is missing.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use super::compress::{delta_delta_encode, gorilla_encode};

/// Bucketing granularity, retention, and downsampling configuration for a
/// `CREATE INDEX ... USING TIME` index, parsed from the `WITH (...)` clause
/// at creation time (`interval`, `retention`, `downsample`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeBucketPolicy {
    pub granularity_ms: i64,
    /// Buckets whose window ends more than this many ms before the newest
    /// inserted timestamp are downsampled/evicted. `None` disables retention.
    pub retention_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TsEntry {
    row_key: Vec<u8>,
    ts: i64,
    value: f64,
}

/// Running aggregate for one time bucket — the continuous-aggregate
/// substrate: updated incrementally on every insert into the bucket, so
/// aggregate-only queries (e.g. `moving_average`) never need to touch raw
/// rows once a bucket has one.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Rollup {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl Rollup {
    fn add(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

/// A sealed bucket's series, Gorilla/delta-of-delta compressed. Kept only
/// for buckets that are no longer the newest — see module docs.
struct CompressedSeries {
    ts_deltas: Vec<u8>,
    values: Vec<u8>,
    row_keys: Vec<Vec<u8>>,
}

enum BucketState {
    /// Still receiving inserts (it's the newest bucket, or ties the newest).
    Open { entries: Vec<TsEntry> },
    /// No longer the newest — compressed, raw entries still available via
    /// decompression for row-key range queries.
    Sealed(CompressedSeries),
    /// Past the retention cutoff — raw/compressed series evicted, only the
    /// rollup survives.
    Downsampled,
}

struct Bucket {
    rollup: Rollup,
    state: BucketState,
}

pub struct TimeIndex {
    path: PathBuf,
    policy: TimeBucketPolicy,
    /// Name of the numeric column whose values this index compresses/rolls
    /// up alongside the indexed timestamp column.
    value_column: String,
    buckets: BTreeMap<i64, Bucket>,
    newest_ts: i64,
}

fn bucket_start(ts: i64, granularity_ms: i64) -> i64 {
    ts.div_euclid(granularity_ms) * granularity_ms
}

impl TimeIndex {
    /// Opens (replaying any existing records) or creates a fresh time index
    /// file. `default_policy`/`default_value_column` are only used when
    /// creating a new file — on reopen, both are read back from the header.
    pub fn open(
        path: &Path,
        default_policy: TimeBucketPolicy,
        default_value_column: &str,
    ) -> Result<Self> {
        if !path.exists() {
            let idx = Self {
                path: path.to_path_buf(),
                policy: default_policy,
                value_column: default_value_column.to_string(),
                buckets: BTreeMap::new(),
                newest_ts: i64::MIN,
            };
            idx.write_header()?;
            return Ok(idx);
        }
        let mut file = BufReader::new(File::open(path)?);
        let mut header = [0u8; 16];
        file.read_exact(&mut header)?;
        let granularity_ms = i64::from_be_bytes(header[0..8].try_into().unwrap());
        let retention_raw = i64::from_be_bytes(header[8..16].try_into().unwrap());
        let policy = TimeBucketPolicy {
            granularity_ms,
            retention_ms: if retention_raw < 0 {
                None
            } else {
                Some(retention_raw)
            },
        };
        let mut col_len_buf = [0u8; 2];
        file.read_exact(&mut col_len_buf)?;
        let col_len = u16::from_be_bytes(col_len_buf) as usize;
        let mut col_buf = vec![0u8; col_len];
        file.read_exact(&mut col_buf)?;
        let value_column = String::from_utf8(col_buf)?;
        let mut idx = Self {
            path: path.to_path_buf(),
            policy,
            value_column,
            buckets: BTreeMap::new(),
            newest_ts: i64::MIN,
        };
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
            let entry: TsEntry = bincode::deserialize(&buf)?;
            idx.apply_insert(entry, false)?;
        }
        Ok(idx)
    }

    fn write_header(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.write_all(&self.policy.granularity_ms.to_be_bytes())?;
        file.write_all(&self.policy.retention_ms.unwrap_or(-1).to_be_bytes())?;
        let col_bytes = self.value_column.as_bytes();
        file.write_all(&(col_bytes.len() as u16).to_be_bytes())?;
        file.write_all(col_bytes)?;
        Ok(())
    }

    pub fn policy(&self) -> TimeBucketPolicy {
        self.policy
    }

    pub fn value_column(&self) -> &str {
        &self.value_column
    }

    /// Indexes one row's `(timestamp, value)` pair. Appends to the on-disk
    /// log, updates the in-memory bucket, seals any bucket that's no longer
    /// the newest, and applies retention.
    pub fn insert(&mut self, row_key: &[u8], ts: i64, value: f64) -> Result<()> {
        let entry = TsEntry {
            row_key: row_key.to_vec(),
            ts,
            value,
        };
        let encoded = bincode::serialize(&entry)?;
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&(encoded.len() as u32).to_be_bytes())?;
        file.write_all(&encoded)?;
        self.apply_insert(entry, true)?;
        Ok(())
    }

    fn apply_insert(&mut self, entry: TsEntry, apply_retention: bool) -> Result<()> {
        let start = bucket_start(entry.ts, self.policy.granularity_ms);
        if entry.ts > self.newest_ts {
            self.newest_ts = entry.ts;
        }
        let bucket = self.buckets.entry(start).or_insert_with(|| Bucket {
            rollup: Rollup {
                count: 0,
                sum: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
            },
            state: BucketState::Open {
                entries: Vec::new(),
            },
        });
        bucket.rollup.add(entry.value);
        match &mut bucket.state {
            BucketState::Open { entries } => entries.push(entry),
            // A row landed in an already-sealed/downsampled bucket (can
            // happen with out-of-order inserts): re-open it so it isn't lost.
            BucketState::Sealed(_) | BucketState::Downsampled => {
                bucket.state = BucketState::Open {
                    entries: vec![entry],
                };
            }
        }
        self.seal_non_newest_buckets();
        if apply_retention {
            self.apply_retention();
        }
        Ok(())
    }

    fn seal_non_newest_buckets(&mut self) {
        let newest_start = bucket_start(self.newest_ts, self.policy.granularity_ms);
        for (&start, bucket) in self.buckets.iter_mut() {
            if start == newest_start {
                continue;
            }
            if let BucketState::Open { entries } = &bucket.state {
                let mut sorted = entries.clone();
                sorted.sort_by_key(|e| e.ts);
                let ts: Vec<i64> = sorted.iter().map(|e| e.ts).collect();
                let values: Vec<f64> = sorted.iter().map(|e| e.value).collect();
                let row_keys: Vec<Vec<u8>> = sorted.iter().map(|e| e.row_key.clone()).collect();
                bucket.state = BucketState::Sealed(CompressedSeries {
                    ts_deltas: delta_delta_encode(&ts),
                    values: gorilla_encode(&values),
                    row_keys,
                });
            }
        }
    }

    /// Evicts raw/compressed series for buckets whose window has fully
    /// elapsed past `retention_ms` relative to the newest inserted
    /// timestamp, keeping only their `Rollup` — the "configurable retention
    /// + automatic downsampling" behavior. Runs synchronously on every
    /// insert rather than on a background schedule (see module docs / plan).
    fn apply_retention(&mut self) {
        let Some(retention_ms) = self.policy.retention_ms else {
            return;
        };
        let cutoff = self.newest_ts - retention_ms;
        for (&start, bucket) in self.buckets.iter_mut() {
            let bucket_end = start + self.policy.granularity_ms;
            if bucket_end <= cutoff && !matches!(bucket.state, BucketState::Downsampled) {
                bucket.state = BucketState::Downsampled;
            }
        }
    }

    fn decompress(series: &CompressedSeries) -> Vec<(i64, f64, Vec<u8>)> {
        let ts = super::compress::delta_delta_decode(&series.ts_deltas);
        let values = super::compress::gorilla_decode(&series.values);
        ts.into_iter()
            .zip(values)
            .zip(series.row_keys.iter().cloned())
            .map(|((t, v), k)| (t, v, k))
            .collect()
    }

    /// Row keys of every indexed entry with `t0 <= ts <= t1`. Buckets that
    /// have been downsampled past retention contribute nothing (their raw
    /// rows are gone by design) — callers wanting aggregate data over that
    /// range should use [`Self::query_rollups`] instead.
    pub fn query_range(&self, t0: i64, t1: i64) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let lo = bucket_start(t0, self.policy.granularity_ms);
        for (&start, bucket) in self.buckets.range(lo..) {
            if start > t1 {
                break;
            }
            match &bucket.state {
                BucketState::Open { entries } => {
                    for e in entries {
                        if e.ts >= t0 && e.ts <= t1 {
                            out.push(e.row_key.clone());
                        }
                    }
                }
                BucketState::Sealed(series) => {
                    for (ts, _, key) in Self::decompress(series) {
                        if ts >= t0 && ts <= t1 {
                            out.push(key);
                        }
                    }
                }
                BucketState::Downsampled => {}
            }
        }
        out
    }

    /// Per-bucket rollups (start-of-bucket timestamp, aggregate) covering
    /// `[t0, t1]` — answers continuous-aggregate-style queries
    /// (`moving_average`, downsampled ranges) without needing raw rows,
    /// including for buckets that have already been downsampled past
    /// retention.
    pub fn query_rollups(&self, t0: i64, t1: i64) -> Vec<(i64, Rollup)> {
        let lo = bucket_start(t0, self.policy.granularity_ms);
        self.buckets
            .range(lo..)
            .take_while(|(&start, _)| start <= t1)
            .map(|(&start, bucket)| (start, bucket.rollup))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(granularity_ms: i64, retention_ms: Option<i64>) -> TimeBucketPolicy {
        TimeBucketPolicy {
            granularity_ms,
            retention_ms,
        }
    }

    #[test]
    fn insert_and_query_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.ts");
        let mut idx = TimeIndex::open(&path, policy(3_600_000, None), "value").unwrap();
        idx.insert(b"row1", 1_000, 10.0).unwrap();
        idx.insert(b"row2", 2_000_000, 20.0).unwrap(); // different hour bucket
        idx.insert(b"row3", 1_500, 15.0).unwrap();

        let hits = idx.query_range(0, 500_000);
        assert!(hits.contains(&b"row1".to_vec()));
        assert!(hits.contains(&b"row3".to_vec()));
        assert!(!hits.contains(&b"row2".to_vec()));
    }

    #[test]
    fn reopen_replays_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.ts");
        {
            let mut idx = TimeIndex::open(&path, policy(3_600_000, None), "value").unwrap();
            idx.insert(b"row1", 1_000, 10.0).unwrap();
            idx.insert(b"row2", 5_000_000, 20.0).unwrap();
        }
        let reopened = TimeIndex::open(&path, policy(3_600_000, None), "value").unwrap();
        let hits = reopened.query_range(0, 10_000_000);
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn rollup_tracks_aggregate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.ts");
        let mut idx = TimeIndex::open(&path, policy(3_600_000, None), "value").unwrap();
        idx.insert(b"row1", 1_000, 10.0).unwrap();
        idx.insert(b"row2", 2_000, 20.0).unwrap();
        idx.insert(b"row3", 3_000, 30.0).unwrap();

        let rollups = idx.query_rollups(0, 3_600_000);
        assert_eq!(rollups.len(), 1);
        let (_, rollup) = rollups[0];
        assert_eq!(rollup.count, 3);
        assert_eq!(rollup.avg(), 20.0);
        assert_eq!(rollup.min, 10.0);
        assert_eq!(rollup.max, 30.0);
    }

    #[test]
    fn retention_evicts_raw_rows_but_keeps_rollup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.ts");
        // 1-hour buckets, 2-hour retention.
        let mut idx =
            TimeIndex::open(&path, policy(3_600_000, Some(2 * 3_600_000)), "value").unwrap();
        idx.insert(b"old_row", 0, 5.0).unwrap();
        // Push the "newest" timestamp far enough ahead that the old bucket
        // falls outside the retention window.
        idx.insert(b"new_row", 10 * 3_600_000, 50.0).unwrap();

        let hits = idx.query_range(0, 3_600_000);
        assert!(
            !hits.contains(&b"old_row".to_vec()),
            "raw row should be evicted past retention"
        );

        let rollups = idx.query_rollups(0, 3_600_000);
        assert_eq!(rollups.len(), 1);
        assert_eq!(rollups[0].1.count, 1);
        assert_eq!(rollups[0].1.sum, 5.0);
    }

    #[test]
    fn sealed_bucket_compression_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.ts");
        let mut idx = TimeIndex::open(&path, policy(3_600_000, None), "value").unwrap();
        for i in 0..50 {
            idx.insert(format!("row{i}").as_bytes(), i * 1000, i as f64)
                .unwrap();
        }
        // Advance into a new bucket so the first bucket seals & compresses.
        idx.insert(b"trigger", 10_000_000, 999.0).unwrap();

        let hits = idx.query_range(0, 49_000);
        assert_eq!(hits.len(), 50);
    }
}
