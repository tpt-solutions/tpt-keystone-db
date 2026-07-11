//! Local (per-node, not object-store-replicated — same documented scope cut
//! as `ts_index.rs`/`graph_index.rs`/`canopy_index.rs`) append-only
//! partitioned log for Flux, TPT's event-streaming engine.
//!
//! A [`FluxLog`] (one per `CREATE TOPIC`) owns N independent partitions,
//! each an append-only sequence of [`FluxRecord`]s persisted to its own
//! length-prefixed bincode record log (same convention as `ts_index.rs`),
//! replayed fully into memory on open. Consumer-group offsets are tracked
//! the same way — a small append log of `(group, partition, offset)`
//! commits, replayed into a `HashMap` keyed by the latest commit per
//! `(group, partition)`. Retention (time- and size-based) is applied
//! synchronously on every publish, same discipline as
//! `TimeIndex::apply_retention` — no background compaction thread.
//!
//! Retention only evicts from the in-memory (and future-reopen) record set;
//! the physical per-partition log file is append-only and is never
//! compacted/truncated to reclaim the disk space of evicted records. A real
//! implementation would periodically rewrite/segment-roll the log file the
//! way an LSM engine compacts SSTables — out of scope here, same as this
//! module's other local-accelerator siblings.
//!
//! Partition assignment for a publish without an explicit partition hashes
//! the key (mod partition count) so same-key records always land on the
//! same partition, or round-robins if there's no key. This is ordering
//! determinism, not a rebalancing protocol — there is nothing to rebalance,
//! since a single node owns every partition of a topic it created.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

/// One published record. `offset` is a per-partition monotonic sequence
/// number starting at 0.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FluxRecord {
    pub offset: u64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp_ms: i64,
}

/// `WITH (retention = '<interval>', retention_bytes = <n>)` at `CREATE
/// TOPIC` time. `None` in either field disables that axis of retention.
#[derive(Debug, Clone, Copy, Default)]
pub struct RetentionPolicy {
    pub retention_ms: Option<i64>,
    pub retention_bytes: Option<u64>,
}

/// Current wall-clock time in unix milliseconds — shared by CDC event
/// timestamps and record publish timestamps so both use the same clock.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

struct Partition {
    path: PathBuf,
    records: VecDeque<FluxRecord>,
    next_offset: u64,
    total_bytes: u64,
    newest_ts: i64,
}

impl Partition {
    /// Opens (replaying any existing records, retention not yet applied —
    /// callers apply it once after loading using the topic's policy) or
    /// creates a fresh partition log file.
    fn open(path: &Path) -> Result<Self> {
        let mut records = VecDeque::new();
        let mut next_offset = 0u64;
        let mut total_bytes = 0u64;
        let mut newest_ts = i64::MIN;
        if path.exists() {
            let mut file = BufReader::new(File::open(path)?);
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
                let rec: FluxRecord = bincode::deserialize(&buf)?;
                next_offset = rec.offset + 1;
                total_bytes += rec.value.len() as u64;
                newest_ts = newest_ts.max(rec.timestamp_ms);
                records.push_back(rec);
            }
        }
        Ok(Self {
            path: path.to_path_buf(),
            records,
            next_offset,
            total_bytes,
            newest_ts,
        })
    }

    fn append(
        &mut self,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        timestamp_ms: i64,
        retention: RetentionPolicy,
    ) -> Result<u64> {
        let offset = self.next_offset;
        let rec = FluxRecord {
            offset,
            key,
            value,
            timestamp_ms,
        };
        let encoded = bincode::serialize(&rec)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&(encoded.len() as u32).to_be_bytes())?;
        file.write_all(&encoded)?;
        self.next_offset += 1;
        self.total_bytes += rec.value.len() as u64;
        self.newest_ts = self.newest_ts.max(rec.timestamp_ms);
        self.records.push_back(rec);
        self.apply_retention(retention);
        Ok(offset)
    }

    /// Evicts from the front (oldest first) of the in-memory record set —
    /// see module docs for why the on-disk log itself isn't compacted.
    fn apply_retention(&mut self, retention: RetentionPolicy) {
        if let Some(retention_ms) = retention.retention_ms {
            let cutoff = self.newest_ts - retention_ms;
            while let Some(front) = self.records.front() {
                if front.timestamp_ms < cutoff {
                    self.total_bytes -= front.value.len() as u64;
                    self.records.pop_front();
                } else {
                    break;
                }
            }
        }
        if let Some(max_bytes) = retention.retention_bytes {
            while self.total_bytes > max_bytes {
                match self.records.pop_front() {
                    Some(front) => self.total_bytes -= front.value.len() as u64,
                    None => break,
                }
            }
        }
    }

    fn poll(&self, from_offset: u64, max: usize) -> Vec<FluxRecord> {
        self.records
            .iter()
            .filter(|r| r.offset >= from_offset)
            .take(max)
            .cloned()
            .collect()
    }

    fn all(&self) -> Vec<FluxRecord> {
        self.records.iter().cloned().collect()
    }
}

/// Deterministic key → partition assignment (`DefaultHasher`, stable within
/// one process/build — not guaranteed stable across Rust versions, which is
/// fine here since a topic's partitions are always read back by the same
/// binary that wrote them).
fn partition_for_key(key: &[u8], num_partitions: u32) -> u32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % num_partitions as u64) as u32
}

/// A named topic: N partitions plus every consumer group's committed
/// offsets. One `FluxLog` per topic, held in `Database::topics`.
pub struct FluxLog {
    dir: PathBuf,
    partitions: Vec<Partition>,
    retention: RetentionPolicy,
    offsets_path: PathBuf,
    /// (group, partition) -> next offset to read. Updated by `commit`,
    /// persisted to `offsets_path`.
    offsets: HashMap<(String, u32), u64>,
    /// Round-robin cursor for publishes with no key — process-local, not
    /// persisted (an even split across restarts isn't a correctness
    /// requirement, just a load-balancing nicety).
    rr_counter: u32,
}

#[derive(Serialize, Deserialize)]
struct OffsetCommit {
    group: String,
    partition: u32,
    offset: u64,
}

impl FluxLog {
    /// Creates a brand-new topic directory (`meta` header + one file per
    /// partition), failing if `dir` already exists — callers implementing
    /// `CREATE TOPIC IF NOT EXISTS` check existence themselves first.
    pub fn create(dir: &Path, num_partitions: u32, retention: RetentionPolicy) -> Result<Self> {
        anyhow::ensure!(num_partitions > 0, "a topic must have at least 1 partition");
        std::fs::create_dir_all(dir)?;
        Self::write_meta(dir, num_partitions, retention)?;
        let mut partitions = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions {
            partitions.push(Partition::open(&dir.join(format!("partition-{i}.log")))?);
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            partitions,
            retention,
            offsets_path: dir.join("offsets.log"),
            offsets: HashMap::new(),
            rr_counter: 0,
        })
    }

    /// Reopens an existing topic directory, replaying every partition's log
    /// and the offsets log, then re-applying retention (in case the policy
    /// evicts records that were within bounds as of the last run but
    /// wouldn't be replayed fresh — a no-op in practice since retention is
    /// applied on every publish, but cheap and keeps the invariant explicit).
    pub fn open(dir: &Path) -> Result<Self> {
        let (num_partitions, retention) = Self::read_meta(dir)?;
        let mut partitions = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions {
            let mut p = Partition::open(&dir.join(format!("partition-{i}.log")))?;
            p.apply_retention(retention);
            partitions.push(p);
        }
        let offsets_path = dir.join("offsets.log");
        let offsets = Self::read_offsets(&offsets_path)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            partitions,
            retention,
            offsets_path,
            offsets,
            rr_counter: 0,
        })
    }

    fn write_meta(dir: &Path, num_partitions: u32, retention: RetentionPolicy) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dir.join("meta"))?;
        file.write_all(&num_partitions.to_be_bytes())?;
        file.write_all(&retention.retention_ms.unwrap_or(-1).to_be_bytes())?;
        file.write_all(
            &retention
                .retention_bytes
                .map(|b| b as i64)
                .unwrap_or(-1)
                .to_be_bytes(),
        )?;
        Ok(())
    }

    fn read_meta(dir: &Path) -> Result<(u32, RetentionPolicy)> {
        let mut file = File::open(dir.join("meta"))?;
        let mut buf = [0u8; 20];
        file.read_exact(&mut buf)?;
        let num_partitions = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let retention_ms = i64::from_be_bytes(buf[4..12].try_into().unwrap());
        let retention_bytes = i64::from_be_bytes(buf[12..20].try_into().unwrap());
        Ok((
            num_partitions,
            RetentionPolicy {
                retention_ms: if retention_ms < 0 {
                    None
                } else {
                    Some(retention_ms)
                },
                retention_bytes: if retention_bytes < 0 {
                    None
                } else {
                    Some(retention_bytes as u64)
                },
            },
        ))
    }

    fn read_offsets(path: &Path) -> Result<HashMap<(String, u32), u64>> {
        let mut offsets = HashMap::new();
        if !path.exists() {
            return Ok(offsets);
        }
        let mut file = BufReader::new(File::open(path)?);
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
            let commit: OffsetCommit = bincode::deserialize(&buf)?;
            offsets.insert((commit.group, commit.partition), commit.offset);
        }
        Ok(offsets)
    }

    pub fn num_partitions(&self) -> u32 {
        self.partitions.len() as u32
    }

    /// Publishes one record, returning `(partition, offset)`. See module
    /// docs for partition assignment when `partition` is `None`.
    pub fn publish(
        &mut self,
        partition: Option<u32>,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<(u32, u64)> {
        let n = self.num_partitions();
        let target = match partition {
            Some(p) => {
                anyhow::ensure!(
                    p < n,
                    "partition {p} out of range (topic has {n} partition(s))"
                );
                p
            }
            None => match &key {
                Some(k) => partition_for_key(k, n),
                None => {
                    let p = self.rr_counter % n;
                    self.rr_counter = self.rr_counter.wrapping_add(1);
                    p
                }
            },
        };
        let offset =
            self.partitions[target as usize].append(key, value, now_ms(), self.retention)?;
        Ok((target, offset))
    }

    /// Records at/after `group`'s tracked offset for `partition`, without
    /// advancing it. Defaults to offset 0 for a group never committed.
    pub fn poll(&self, group: &str, partition: u32, max: usize) -> Result<Vec<FluxRecord>> {
        let p = self
            .partitions
            .get(partition as usize)
            .ok_or_else(|| anyhow::anyhow!("partition {partition} does not exist"))?;
        let from = self
            .offsets
            .get(&(group.to_string(), partition))
            .copied()
            .unwrap_or(0);
        Ok(p.poll(from, max))
    }

    /// Advances `group`'s tracked offset for `partition` to `offset`,
    /// persisting the commit.
    pub fn commit(&mut self, group: &str, partition: u32, offset: u64) -> Result<()> {
        anyhow::ensure!(
            (partition as usize) < self.partitions.len(),
            "partition {partition} does not exist"
        );
        let commit = OffsetCommit {
            group: group.to_string(),
            partition,
            offset,
        };
        let encoded = bincode::serialize(&commit)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.offsets_path)?;
        file.write_all(&(encoded.len() as u32).to_be_bytes())?;
        file.write_all(&encoded)?;
        self.offsets.insert((group.to_string(), partition), offset);
        Ok(())
    }

    /// Every record currently retained in `partition`, offset order —
    /// bypasses consumer-group tracking entirely. Used by the time-travel/
    /// windowing table functions, which need the whole log rather than one
    /// consumer's unread tail.
    pub fn all_records(&self, partition: u32) -> Option<Vec<FluxRecord>> {
        self.partitions.get(partition as usize).map(|p| p.all())
    }

    #[cfg(test)]
    fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_retention() -> RetentionPolicy {
        RetentionPolicy {
            retention_ms: None,
            retention_bytes: None,
        }
    }

    #[test]
    fn publish_poll_commit_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = FluxLog::create(&dir.path().join("t"), 1, no_retention()).unwrap();
        log.publish(None, Some(b"k1".to_vec()), b"v1".to_vec())
            .unwrap();
        log.publish(None, Some(b"k1".to_vec()), b"v2".to_vec())
            .unwrap();

        let polled = log.poll("g1", 0, 10).unwrap();
        assert_eq!(polled.len(), 2);
        assert_eq!(polled[0].value, b"v1");
        assert_eq!(polled[1].value, b"v2");

        log.commit("g1", 0, 1).unwrap();
        let polled_after_commit = log.poll("g1", 0, 10).unwrap();
        assert_eq!(polled_after_commit.len(), 1);
        assert_eq!(polled_after_commit[0].value, b"v2");
    }

    #[test]
    fn partition_hashing_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = FluxLog::create(&dir.path().join("t"), 4, no_retention()).unwrap();
        let (p1, _) = log
            .publish(None, Some(b"same-key".to_vec()), b"a".to_vec())
            .unwrap();
        let (p2, _) = log
            .publish(None, Some(b"same-key".to_vec()), b"b".to_vec())
            .unwrap();
        assert_eq!(p1, p2);
    }

    #[test]
    fn round_robin_without_key_spreads_across_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = FluxLog::create(&dir.path().join("t"), 3, no_retention()).unwrap();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let (p, _) = log.publish(None, None, b"x".to_vec()).unwrap();
            seen.insert(p);
        }
        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn explicit_partition_out_of_range_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = FluxLog::create(&dir.path().join("t"), 2, no_retention()).unwrap();
        assert!(log.publish(Some(5), None, b"x".to_vec()).is_err());
    }

    #[test]
    fn time_based_retention_evicts_old_records() {
        let dir = tempfile::tempdir().unwrap();
        let retention = RetentionPolicy {
            retention_ms: Some(1000),
            retention_bytes: None,
        };
        let mut log = FluxLog::create(&dir.path().join("t"), 1, retention).unwrap();
        // Manually drive the partition with explicit timestamps by calling
        // through `Partition::append` isn't exposed publicly, so exercise
        // the same effect via successive publishes and `now_ms()` skew is
        // avoided by asserting on record count relative to a synthetic old
        // record injected directly into the log file before reopening.
        let p_path = log.dir().join("partition-0.log");
        let old = FluxRecord {
            offset: 0,
            key: None,
            value: b"old".to_vec(),
            timestamp_ms: 0,
        };
        let encoded = bincode::serialize(&old).unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&p_path)
            .unwrap();
        file.write_all(&(encoded.len() as u32).to_be_bytes())
            .unwrap();
        file.write_all(&encoded).unwrap();
        drop(file);
        drop(log);

        let mut reopened = FluxLog::open(&dir.path().join("t")).unwrap();
        // Newest ts is still 0 after replay (only the synthetic record
        // exists), so publishing something far in the future pushes the
        // retention cutoff past the old record.
        reopened.publish(None, None, b"new".to_vec()).unwrap();
        // The record just published carries `now_ms()`, which is far more
        // than 1000ms after timestamp 0, so "old" should be evicted.
        let all = reopened.all_records(0).unwrap();
        assert!(all.iter().all(|r| r.value != b"old"));
    }

    #[test]
    fn size_based_retention_evicts_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        let retention = RetentionPolicy {
            retention_ms: None,
            retention_bytes: Some(10),
        };
        let mut log = FluxLog::create(&dir.path().join("t"), 1, retention).unwrap();
        for i in 0..5 {
            log.publish(None, None, vec![i as u8; 4]).unwrap();
        }
        let all = log.all_records(0).unwrap();
        let total: usize = all.iter().map(|r| r.value.len()).sum();
        assert!(total <= 10);
        // Oldest records evicted first: the highest offsets should survive.
        let offsets: Vec<u64> = all.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![3, 4]);
    }

    #[test]
    fn reopen_replays_partitions_and_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let topic_dir = dir.path().join("t");
        {
            let mut log = FluxLog::create(&topic_dir, 2, no_retention()).unwrap();
            log.publish(Some(0), None, b"a".to_vec()).unwrap();
            log.publish(Some(1), None, b"b".to_vec()).unwrap();
            log.commit("g1", 0, 1).unwrap();
        }
        let reopened = FluxLog::open(&topic_dir).unwrap();
        assert_eq!(reopened.num_partitions(), 2);
        assert_eq!(reopened.all_records(0).unwrap().len(), 1);
        assert_eq!(reopened.all_records(1).unwrap().len(), 1);
        // Committed offset (1) survived, so polling partition 0 (which only
        // has offset 0) now returns nothing for that group.
        assert_eq!(reopened.poll("g1", 0, 10).unwrap().len(), 0);
    }
}
