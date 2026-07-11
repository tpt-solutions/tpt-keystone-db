use super::io_backend::{self, WalIo};
use anyhow::Result;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use tracing::info;

/// A single WAL record.
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
/// The actual append+fsync is delegated to a pluggable [`WalIo`] backend
/// (`storage/io_backend.rs`): the portable `std::fs` path by default, or a
/// Linux `io_uring` backend when `TPT_IO_URING=1` is set on Linux.
pub struct Wal {
    io: Box<dyn WalIo>,
    path: PathBuf,
    seq: u64,
    bytes_written: u64,
}

impl Wal {
    /// Open or create a WAL file at the given path.
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

        // Determine the next sequence number by scanning existing records.
        let seq = Self::scan_max_seq(&file)? + 1;
        let bytes_written = file.metadata()?.len();
        let io = io_backend::open_backend(file, bytes_written, &path)?;

        info!(path = %path.display(), seq, bytes_written, "WAL opened");

        Ok(Self {
            io,
            path,
            seq,
            bytes_written,
        })
    }

    /// Append a record to the WAL and fsync.
    pub fn append(
        &mut self,
        table: &str,
        key: &[u8],
        value: &[u8],
        record_type: u8,
    ) -> Result<WalRecord> {
        let seq = self.seq;
        self.seq += 1;

        let record = WalRecord {
            seq,
            table: table.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
            record_type,
        };

        // Encode: seq(8) | table_len(4) | table | key_len(4) | key | value_len(4) | value | type(1)
        let table_bytes = table.as_bytes();
        let mut buf =
            Vec::with_capacity(8 + 4 + table_bytes.len() + 4 + key.len() + 4 + value.len() + 1);
        buf.extend_from_slice(&seq.to_be_bytes());
        buf.extend_from_slice(&(table_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(table_bytes);
        buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        buf.extend_from_slice(value);
        buf.push(record_type);

        self.io.append(&buf)?; // write + fsync via the active backend
        crate::metrics::Metrics::global()
            .wal_fsyncs_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_written += buf.len() as u64;

        Ok(record)
    }

    /// Replay all WAL records, calling `f` for each.
    pub fn replay<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(WalRecord),
    {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut pos = 0;

        while pos + 8 + 4 <= buf.len() {
            let seq = u64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap());
            pos += 8;

            if pos + 4 > buf.len() {
                break;
            }
            let table_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + table_len + 4 > buf.len() {
                break;
            }
            let table = String::from_utf8_lossy(&buf[pos..pos + table_len]).to_string();
            pos += table_len;

            if pos + 4 > buf.len() {
                break;
            }
            let key_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + key_len + 4 > buf.len() {
                break;
            }
            let key = buf[pos..pos + key_len].to_vec();
            pos += key_len;

            if pos + 4 > buf.len() {
                break;
            }
            let value_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + value_len + 1 > buf.len() {
                break;
            }
            let value = buf[pos..pos + value_len].to_vec();
            pos += value_len;

            let record_type = buf[pos];
            pos += 1;

            f(WalRecord {
                seq,
                table,
                key,
                value,
                record_type,
            });
        }

        Ok(())
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

    /// Get the current sequence number.
    pub fn current_seq(&self) -> u64 {
        self.seq
    }

    fn scan_max_seq(file: &File) -> Result<u64> {
        let mut max_seq = 0u64;
        let mut buf = Vec::new();
        let mut file = file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut buf)?;

        let mut pos = 0;
        while pos + 8 <= buf.len() {
            let seq = u64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap());
            max_seq = max_seq.max(seq);
            pos += 8;

            if pos + 4 > buf.len() {
                break;
            }
            let table_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + table_len + 4 > buf.len() {
                break;
            }
            pos += table_len;

            if pos + 4 > buf.len() {
                break;
            }
            let key_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + key_len + 4 > buf.len() {
                break;
            }
            pos += key_len;

            if pos + 4 > buf.len() {
                break;
            }
            let value_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + value_len + 1 > buf.len() {
                break;
            }
            pos += value_len + 1;
        }

        Ok(max_seq)
    }
}
