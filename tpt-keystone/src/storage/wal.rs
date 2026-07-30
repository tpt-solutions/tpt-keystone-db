use super::io_backend::{self, WalIo};
use anyhow::Result;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use tracing::info;

/// Marks the start of a batch/group in the WAL (see the module doc for the
/// on-disk format). Chosen distinct from every `record_type` value in use
/// (0=insert, 1=update, 2=delete) so it can never be confused with one.
const BATCH_MARKER: u8 = 0xFF;

/// A single logical write within a WAL batch, as replayed back to the
/// caller. `seq` is the *batch's* commit-sequence number — shared by every
/// record in the same batch (see [`Wal::append_batch`]), not a per-record
/// counter.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub seq: u64,
    pub table: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub record_type: u8, // 0=insert, 1=update, 2=delete
}

/// Write-Ahead Log for crash-safe durability.
///
/// Every write goes through the WAL before being applied to the MemTable.
/// On recovery, the WAL is replayed to restore the MemTable state.
///
/// On-disk format: a sequence of *batches*. Each batch is
/// `marker(1)=0xFF | commit_seq(8) | count(4) | record*count`, where each
/// `record` is `table_len(4)|table|key_len(4)|key|value_len(4)|value|type(1)`
/// — every record in a batch shares the batch's `commit_seq`, and a single
/// autocommit write is simply a batch of size 1. Framing every commit as one
/// group, appended with a single write+fsync (`append_batch`), is what makes
/// a multi-row transaction's commit atomic at the WAL level: `replay` only
/// ever accepts a batch in full or discards it in full, never a partial
/// subset of its records (see `replay`'s doc comment).
///
/// The actual append+fsync is delegated to a pluggable [`WalIo`] backend
/// (`storage/io_backend.rs`): the portable `std::fs` path by default, or a
/// Linux `io_uring` backend when `TPT_IO_URING=1` is set on Linux.
pub struct Wal {
    io: Box<dyn WalIo>,
    path: PathBuf,
    bytes_written: u64,
}

impl Wal {
    /// Open or create a WAL file at the given path. Commit-sequence
    /// bookkeeping is *not* tracked here — the caller (`LsmEngine`) owns
    /// `commit_seq` allocation and derives its starting point from
    /// `replay`'s return value (plus whatever SSTables/manifest it loads);
    /// the WAL itself is just a dumb, ordered batch appender/replayer.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join("wal.log");
        // Deliberately not opened with `.append(true)`: on Windows that only
        // grants FILE_APPEND_DATA, which is not sufficient for `set_len`
        // (truncate) — we need full write access and manage the write
        // position ourselves instead (see the backend's `append`).
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        let bytes_written = file.metadata()?.len();
        let io = io_backend::open_backend(file, bytes_written, &path)?;

        info!(path = %path.display(), bytes_written, "WAL opened");

        Ok(Self {
            io,
            path,
            bytes_written,
        })
    }

    /// Append every entry in `entries` as one atomic batch tagged with
    /// `commit_seq`: one buffered write plus a single fsync for the whole
    /// group. Previously, each row of a transaction was written (and
    /// fsynced) with a separate call, so a crash between two of those calls
    /// could durably persist a torn subset of a commit. Framing the whole
    /// commit as one length-prefixed group, appended with a single
    /// write+fsync, closes that window: on replay, a group that isn't fully
    /// present (or fully well-formed) is dropped in its entirety rather than
    /// partially applied — see `replay`.
    pub fn append_batch(
        &mut self,
        commit_seq: u64,
        entries: &[(String, Vec<u8>, Vec<u8>, u8)],
    ) -> Result<Vec<WalRecord>> {
        let mut buf = Vec::new();
        buf.push(BATCH_MARKER);
        buf.extend_from_slice(&commit_seq.to_be_bytes());
        buf.extend_from_slice(&(entries.len() as u32).to_be_bytes());

        let mut records = Vec::with_capacity(entries.len());
        for (table, key, value, record_type) in entries {
            let table_bytes = table.as_bytes();
            buf.extend_from_slice(&(table_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(table_bytes);
            buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
            buf.extend_from_slice(value);
            buf.push(*record_type);

            records.push(WalRecord {
                seq: commit_seq,
                table: table.clone(),
                key: key.clone(),
                value: value.clone(),
                record_type: *record_type,
            });
        }

        self.io.append(&buf)?; // single write + fsync for the whole batch
        crate::metrics::Metrics::global()
            .wal_fsyncs_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_written += buf.len() as u64;

        Ok(records)
    }

    /// Replay all WAL records, calling `f` for each, and return the highest
    /// `commit_seq` observed — including from a batch header whose records
    /// were truncated (see below), so a restart never reuses a `commit_seq`
    /// that was ever attempted, whether or not it ended up fully durable.
    ///
    /// A batch is only ever replayed in full or not at all: if the file ends
    /// partway through a batch's records, that entire batch is discarded
    /// (not just the specific torn record it ends inside) — this is what
    /// makes multi-row transaction replay atomic. There's no checksum in
    /// this format, so truncation is caught structurally (bounds-checking
    /// before every field decode) rather than via a checksum mismatch.
    pub fn replay<F>(&self, mut f: F) -> Result<u64>
    where
        F: FnMut(WalRecord),
    {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut pos = 0;
        let mut max_seq = 0u64;

        'batches: while pos < buf.len() {
            if pos + 1 + 8 + 4 > buf.len() || buf[pos] != BATCH_MARKER {
                // Either trailing garbage from a torn write to the batch
                // header itself, or (if not at the very start) unrecognized
                // data — nothing safe to parse past this point either way.
                break;
            }
            let mut cursor = pos + 1;
            let commit_seq = u64::from_be_bytes(buf[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let count = u32::from_be_bytes(buf[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;

            // Bump the watermark even if this batch turns out to be torn:
            // whether or not any of its records get applied, this
            // `commit_seq` was attempted and must never be reissued.
            max_seq = max_seq.max(commit_seq);

            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                match Self::parse_record(&buf, cursor, commit_seq) {
                    Some((record, next)) => {
                        records.push(record);
                        cursor = next;
                    }
                    None => break 'batches, // torn trailing batch — discard it whole
                }
            }

            for record in records {
                f(record);
            }
            pos = cursor;
        }

        Ok(max_seq)
    }

    /// Parse one `table_len|table|key_len|key|value_len|value|type` record
    /// starting at `pos`, tagging it with the enclosing batch's `seq`.
    /// Returns the decoded record plus the position just past it, or `None`
    /// if the buffer runs out partway through — the signal to the caller
    /// that this record (and its whole containing batch) is torn.
    fn parse_record(buf: &[u8], pos: usize, seq: u64) -> Option<(WalRecord, usize)> {
        let mut pos = pos;
        if pos + 4 > buf.len() {
            return None;
        }
        let table_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + table_len > buf.len() {
            return None;
        }
        let table = String::from_utf8_lossy(&buf[pos..pos + table_len]).to_string();
        pos += table_len;

        if pos + 4 > buf.len() {
            return None;
        }
        let key_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + key_len > buf.len() {
            return None;
        }
        let key = buf[pos..pos + key_len].to_vec();
        pos += key_len;

        if pos + 4 > buf.len() {
            return None;
        }
        let value_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + value_len + 1 > buf.len() {
            return None;
        }
        let value = buf[pos..pos + value_len].to_vec();
        pos += value_len;

        let record_type = buf[pos];
        pos += 1;

        Some((
            WalRecord {
                seq,
                table,
                key,
                value,
                record_type,
            },
            pos,
        ))
    }

    /// Read the WAL's raw bytes as currently on disk (used to ship the
    /// sealed segment to the object store before truncating).
    pub fn read_all_bytes(&self) -> Result<Vec<u8>> {
        use anyhow::Context;
        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .with_context(|| format!("reopening wal for read at {}", self.path.display()))?;
        file.seek(SeekFrom::Start(0))
            .context("seeking wal to start")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).context("reading wal bytes")?;
        Ok(buf)
    }

    /// Truncate the WAL (after a successful flush to SSTable).
    pub fn truncate(&mut self) -> Result<()> {
        self.io.truncate()?;
        self.bytes_written = 0;
        info!("WAL truncated");
        Ok(())
    }

    /// Get total bytes written to the WAL.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_wal(dir: &Path) -> Wal {
        Wal::open(dir).unwrap()
    }

    #[test]
    fn append_batch_replays_every_record_with_the_shared_commit_seq() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = open_wal(dir.path());
        wal.append_batch(
            7,
            &[
                ("t".into(), b"k1".to_vec(), b"v1".to_vec(), 0),
                ("t".into(), b"k2".to_vec(), b"v2".to_vec(), 0),
            ],
        )
        .unwrap();

        let mut replayed = Vec::new();
        let max_seq = wal.replay(|r| replayed.push(r)).unwrap();
        assert_eq!(max_seq, 7);
        assert_eq!(replayed.len(), 2);
        assert!(replayed.iter().all(|r| r.seq == 7));
        assert_eq!(replayed[0].key, b"k1");
        assert_eq!(replayed[1].key, b"k2");
    }

    #[test]
    fn multiple_batches_replay_in_order_with_correct_seqs() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = open_wal(dir.path());
        wal.append_batch(1, &[("t".into(), b"a".to_vec(), b"1".to_vec(), 0)])
            .unwrap();
        wal.append_batch(2, &[("t".into(), b"b".to_vec(), b"2".to_vec(), 0)])
            .unwrap();

        let mut replayed = Vec::new();
        let max_seq = wal.replay(|r| replayed.push(r)).unwrap();
        assert_eq!(max_seq, 2);
        assert_eq!(replayed.len(), 2);
        assert_eq!((replayed[0].seq, replayed[0].key.clone()), (1, b"a".to_vec()));
        assert_eq!((replayed[1].seq, replayed[1].key.clone()), (2, b"b".to_vec()));
    }

    #[test]
    fn torn_batch_is_discarded_in_full_not_partially_applied() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");

        let (after_first, after_second) = {
            let mut wal = open_wal(dir.path());
            wal.append_batch(1, &[("t".into(), b"a".to_vec(), b"1".to_vec(), 0)])
                .unwrap();
            let after_first = std::fs::metadata(&wal_path).unwrap().len();
            wal.append_batch(
                2,
                &[
                    ("t".into(), b"b".to_vec(), b"2".to_vec(), 0),
                    ("t".into(), b"c".to_vec(), b"3".to_vec(), 0),
                    ("t".into(), b"d".to_vec(), b"4".to_vec(), 0),
                ],
            )
            .unwrap();
            let after_second = std::fs::metadata(&wal_path).unwrap().len();
            (after_first, after_second)
        };

        // Truncate to a length strictly inside the second batch's byte
        // range — landing mid-way through its records — simulating a crash
        // partway through writing it. A naive per-record replay would
        // recover "b" (and maybe "c") from this partially-written batch;
        // atomic-group replay must recover none of it, not even the
        // records that happen to precede the torn one.
        let truncate_at = after_first + (after_second - after_first) / 2;
        assert!(truncate_at > after_first && truncate_at < after_second);

        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        f.set_len(truncate_at).unwrap();
        f.seek(SeekFrom::Start(truncate_at)).unwrap();
        f.flush().unwrap();

        let wal = open_wal(dir.path());
        let mut replayed = Vec::new();
        let max_seq = wal.replay(|r| replayed.push(r)).unwrap();
        assert_eq!(
            replayed.len(),
            1,
            "only the fully-durable first batch's record must replay"
        );
        assert_eq!(replayed[0].key, b"a");
        assert_eq!(
            max_seq, 2,
            "the torn batch's header was fully present, so its seq must still bump the watermark"
        );
    }
}
