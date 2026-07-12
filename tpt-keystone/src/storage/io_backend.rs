//! WAL I/O backends — the Phase 1 "io_uring async I/O integration (Linux NVMe
//! path)" TODO item.
//!
//! The WAL (`storage/wal.rs`) does exactly one kind of durable operation:
//! append a byte buffer at the current end of the log file and make it durable
//! (fsync) before returning. This module abstracts *how* that append+fsync is
//! issued behind the [`WalIo`] trait so the rest of the engine is unchanged,
//! and provides two concrete backends:
//!
//! * [`StdWalIo`] — the portable default: `std::fs::File` positional writes +
//!   `sync_all`. This is what every platform (including this Windows dev host)
//!   actually runs, and what the existing WAL tests + `chaos_tests.rs`
//!   exercise. Behaviourally identical to the pre-existing `Wal` code.
//!
//! * `UringWalIo` — a real Linux `io_uring`-backed backend
//!   (`#[cfg(target_os = "linux")]`), submitting a `Write` SQE followed by an
//!   `Fsync` SQE per append through a per-WAL `io_uring` instance. It is
//!   selected at runtime only on Linux *and* only when `TPT_IO_URING=1` is
//!   set; otherwise the std backend is used everywhere.
//!
//! Honesty note (same discipline this codebase applies to the S3 and GPU
//! paths): the `io_uring` backend is written against the `io-uring` crate's
//! submission/completion contract, but **it has not been compiled or run in
//! this dev environment** — this is a Windows-only host, so the entire
//! `#[cfg(target_os = "linux")]` path is compiled out here. `cargo build`/
//! `cargo test` on this host verify the `StdWalIo` fallback (the one that
//! actually runs) end to end; the `io_uring` path must be validated on a real
//! Linux NVMe host before it is relied on. This mirrors how `S3ObjectStore`
//! was marked done "against the S3 API contract but not exercised against a
//! live AWS endpoint in this environment."

use anyhow::Result;
use std::fs::File;
use std::path::Path;

/// Abstraction over the WAL's single durable primitive: append bytes at the
/// end of the log and make them durable before returning.
///
/// Implementations own the underlying write file handle and track the current
/// write offset (the append position) themselves, so callers never seek.
pub trait WalIo: Send {
    /// Append `buf` at the current end of the log and fsync. On return, the
    /// bytes are durable (or an error is returned and nothing is assumed
    /// durable beyond the previous successful append).
    fn append(&mut self, buf: &[u8]) -> Result<()>;

    /// Truncate the log to zero length and fsync the truncation.
    fn truncate(&mut self) -> Result<()>;

    /// Current length of the log (== total durable bytes written).
    fn len(&self) -> u64;
}

/// Portable, always-available WAL backend: positional `std::fs` writes +
/// `sync_all`. This is the default on every platform and the only backend that
/// runs in this dev environment.
pub struct StdWalIo {
    file: File,
    offset: u64,
}

impl StdWalIo {
    /// Wrap an already-opened, read+write WAL file. `initial_len` is the
    /// current on-disk length (the append position).
    pub fn new(file: File, initial_len: u64) -> Self {
        Self {
            file,
            offset: initial_len,
        }
    }
}

impl WalIo for StdWalIo {
    fn append(&mut self, buf: &[u8]) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        // Not `.append(true)` mode (see wal.rs's note on Windows
        // FILE_APPEND_DATA vs set_len); pin the cursor to the tracked end.
        self.file.seek(SeekFrom::Start(self.offset))?;
        self.file.write_all(buf)?;
        self.file.sync_all()?; // fsync for durability
        self.offset += buf.len() as u64;
        Ok(())
    }

    fn truncate(&mut self) -> Result<()> {
        use anyhow::Context;
        use std::io::{Seek, SeekFrom};
        self.file.set_len(0).context("truncating wal file")?;
        self.file
            .seek(SeekFrom::Start(0))
            .context("seeking wal after truncate")?;
        self.file
            .sync_all()
            .context("fsyncing wal after truncate")?;
        self.offset = 0;
        Ok(())
    }

    fn len(&self) -> u64 {
        self.offset
    }
}

/// Whether the io_uring backend should be used for a WAL opened now.
///
/// True only on Linux with `TPT_IO_URING=1`. On any other platform, or without
/// the env var, the std backend is used. Kept as a free function so the choice
/// is testable and documented in one place.
pub fn io_uring_enabled() -> bool {
    cfg!(target_os = "linux") && std::env::var("TPT_IO_URING").as_deref() == Ok("1")
}

/// Build the WAL backend for a freshly-opened WAL file.
///
/// On Linux with `TPT_IO_URING=1` this attempts the io_uring backend and falls
/// back to std if the ring can't be created (e.g. an old kernel); everywhere
/// else it returns [`StdWalIo`].
pub fn open_backend(file: File, initial_len: u64, _path: &Path) -> Result<Box<dyn WalIo>> {
    #[cfg(target_os = "linux")]
    {
        if io_uring_enabled() {
            match linux_uring::UringWalIo::new(file.try_clone()?, initial_len) {
                Ok(io) => {
                    tracing::info!("WAL using io_uring backend");
                    return Ok(Box::new(io));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "io_uring WAL backend unavailable, falling back to std");
                }
            }
        }
    }
    Ok(Box::new(StdWalIo::new(file, initial_len)))
}

#[cfg(target_os = "linux")]
mod linux_uring {
    //! Real io_uring-backed WAL append. NOTE: unverified in this dev
    //! environment (Windows-only host) — see the module-level honesty note.
    use super::WalIo;
    use anyhow::{anyhow, Context, Result};
    use io_uring::{opcode, types, IoUring};
    use std::fs::File;
    use std::os::unix::io::AsRawFd;

    pub struct UringWalIo {
        file: File,
        ring: IoUring,
        offset: u64,
    }

    impl UringWalIo {
        pub fn new(file: File, initial_len: u64) -> Result<Self> {
            // A small ring is plenty: WAL appends are strictly serial
            // (one in flight at a time), so depth 8 comfortably covers the
            // write+fsync pair per append.
            let ring = IoUring::new(8).context("creating io_uring instance")?;
            Ok(Self {
                file,
                ring,
                offset: initial_len,
            })
        }

        /// Submit one SQE built by `build`, wait for its single completion, and
        /// return the completion result code.
        fn submit_one(&mut self, entry: io_uring::squeue::Entry) -> Result<i32> {
            // SAFETY: `entry` references buffers/fds that outlive the
            // submit_and_wait call below (we block until completion before
            // returning, so the referenced memory is valid for the whole
            // kernel-side operation).
            unsafe {
                self.ring
                    .submission()
                    .push(&entry)
                    .map_err(|e| anyhow!("io_uring submission queue full: {e}"))?;
            }
            self.ring
                .submit_and_wait(1)
                .context("io_uring submit_and_wait")?;
            let cqe = self
                .ring
                .completion()
                .next()
                .ok_or_else(|| anyhow!("io_uring completion missing"))?;
            Ok(cqe.result())
        }
    }

    impl WalIo for UringWalIo {
        fn append(&mut self, buf: &[u8]) -> Result<()> {
            let fd = types::Fd(self.file.as_raw_fd());
            // Positional writes (like pwrite): loop to handle short writes.
            let mut written = 0usize;
            while written < buf.len() {
                let chunk = &buf[written..];
                let write = opcode::Write::new(fd, chunk.as_ptr(), chunk.len() as u32)
                    .offset(self.offset + written as u64)
                    .build();
                let n = self.submit_one(write)?;
                if n < 0 {
                    return Err(anyhow!("io_uring write failed: errno {}", -n));
                }
                if n == 0 {
                    return Err(anyhow!("io_uring write returned 0 (short write stall)"));
                }
                written += n as usize;
            }
            // fsync (full sync, not fdatasync, matching std's sync_all).
            let fsync = opcode::Fsync::new(fd).build();
            let r = self.submit_one(fsync)?;
            if r < 0 {
                return Err(anyhow!("io_uring fsync failed: errno {}", -r));
            }
            self.offset += buf.len() as u64;
            Ok(())
        }

        fn truncate(&mut self) -> Result<()> {
            // ftruncate has no io_uring opcode across all supported kernels;
            // do it synchronously then fsync via the ring for durability.
            self.file.set_len(0).context("truncating wal file")?;
            let fd = types::Fd(self.file.as_raw_fd());
            let r = self.submit_one(opcode::Fsync::new(fd).build())?;
            if r < 0 {
                return Err(anyhow!(
                    "io_uring fsync after truncate failed: errno {}",
                    -r
                ));
            }
            self.offset = 0;
            Ok(())
        }

        fn len(&self) -> u64 {
            self.offset
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Read;

    fn open_rw(path: &Path) -> File {
        OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(path)
            .unwrap()
    }

    #[test]
    fn std_backend_appends_and_tracks_len() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut io = StdWalIo::new(open_rw(&path), 0);
        assert_eq!(io.len(), 0);
        io.append(b"hello").unwrap();
        io.append(b" world").unwrap();
        assert_eq!(io.len(), 11);

        let mut contents = String::new();
        open_rw(&path).read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "hello world");
    }

    #[test]
    fn std_backend_truncate_resets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut io = StdWalIo::new(open_rw(&path), 0);
        io.append(b"data").unwrap();
        io.truncate().unwrap();
        assert_eq!(io.len(), 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        // Still usable after truncate.
        io.append(b"x").unwrap();
        assert_eq!(io.len(), 1);
    }

    #[test]
    fn open_backend_respects_initial_len() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        std::fs::write(&path, b"existing").unwrap();
        let io = open_backend(open_rw(&path), 8, &path).unwrap();
        assert_eq!(io.len(), 8);
    }

    #[test]
    fn io_uring_disabled_without_env_on_non_linux() {
        // On this Windows dev host, io_uring is never enabled.
        if !cfg!(target_os = "linux") {
            assert!(!io_uring_enabled());
        }
    }
}
