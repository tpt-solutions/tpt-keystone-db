# Concurrency model audit (Phase 1, Stage 4)

Follow-up to the Stage 2 snapshot-isolation audit in `docs/mvcc_snapshot_audit.md`.
That document recommended ("step 4") narrowing the global mutex so readers don't
block once the read path is version-chain based. This document records what the
code actually looks like *now* (after Stages 2+3 landed in this pass) and
whether that recommendation is satisfied, deferred, or needs further work.

## What changed (Stages 2+3)

The pre-Stages-2+3 `LsmEngine` was described in the Stage 2 audit as holding a
single coarse `Mutex<LsmEngine>` that serialised all reads and writes. The
Stage 2+3 work restructured `LsmEngine` (`storage/lsm.rs`) into two disjoint
locks with non-overlapping responsibilities:

### `write: Mutex<WriteState>` — the write-side serialization point

Held only for in-memory bookkeeping:
- `commit_seq` allocation (`next_commit_seq`)
- WAL append (`Wal::append_batch` — the one fsync per commit)
- MemTable insert (lock-free `SkipMap`, so this is fast)
- `begin_flush` / `begin_compact` (SSTable-id + manifest-state allocation)
- `commit_flush` / `commit_compact` (manifest CAS + view swap)
- Snapshot registration / release (`snapshot_now`, `register_snapshot`,
  `release_snapshot`) — consults/ mutates `open_snapshots` watermark

### `view: Mutex<Arc<EngineView>>` — the read-side publication point

Held only long enough to clone a single `Arc<EngineView>` pointer (nanoseconds,
no I/O). `EngineView` contains:
- `memtable: Arc<MemTable>` — active, lock-free `SkipMap`, `&self` methods only
- `immutable: Option<Arc<MemTable>>` — frozen, lock-free, `&self` methods only
- `sstables: Arc<Vec<Arc<SSTable>>>` — immutable, each `Arc<SSTable>` is `&self`

### Read path (`read_at` / `scan_at`): never touches `write`

```text
read_at(key, snapshot_seq):
  snap = view.lock().clone()          // nanoseconds — just an Arc bump
  snap.memtable.get_at(key, snapshot_seq)        // lock-free SkipMap lookup
  snap.immutable.get_at(key, snapshot_seq)        // lock-free
  sst.read_at(key, snapshot_seq)                    // mmap'd in-memory, lock-free
```

The `view` lock is released immediately after the `Arc` clone. A concurrent
write's WAL fsync or a flush's SSTable upload cannot block a read.

### Write path (`write_batch`): never touches `view` (except briefly for memtable)

```text
write_batch(entries):
  ws = write.lock()                   // serialize writers
  commit_seq = ws.next_commit_seq++
  ws.wal.append_batch(commit_seq, ...)  // the one fsync
  memtable = view.lock().memtable.clone()  // nanoseconds
  memtable.insert(...) for each entry     // lock-free SkipMap insert
  if memtable.is_full(): begin_flush(&mut ws)
  ws unlocked, memtable insert already done
```

The only `view` lock taken during a write is to grab a clone of the current
`Arc<MemTable>` so new writes go into the active memtable — it is held for
nanoseconds and never across I/O.

### Flush/compaction: split into lock-free expensive parts

| Phase       | Lock held? | What it does                              |
|-------------|------------|-------------------------------------------|
| `begin_flush` / `begin_compact` | `write` (briefly) | Allocates IDs, captures frozen memtable / SSTable list as `Arc`s |
| `finish_flush` / `finish_compact` | **none** | Builds + uploads the SSTable blob (the expensive, I/O-bound part) |
| `commit_flush` / `commit_compact` | `write` (briefly) | Manifest CAS, view swap, WAL truncation |

This three-phase split (capture → upload lock-free → commit) is what lets the
existing tests in `lsm.rs` prove non-blocking:

- `finish_flush_does_not_block_a_concurrent_read` — wraps the object store in a
  `DelayedPutStore` that sleeps 300 ms on every SSTable `put`, then asserts a
  concurrent `read()` completes in < 200 ms (data stays visible via the frozen
  `immutable` memtable while the upload is in flight).
- `finish_compact_does_not_block_a_concurrent_read` — same pattern for
  compaction.

## Current state verdict

### ✓ Readers don't block writers
Satisfied. Reads never take the `write` lock. The only lock they touch
(`view`) is held for an `Arc` clone — no I/O, no fsync, no upload.

### ✓ Writers don't block readers (for the read path)
Satisfied. Writes take `write` (for WAL + seq allocation + memtable insert) and
`view` (for a nanosecond `Arc::clone`), but neither is held across I/O. A
concurrent read never waits on a write's fsync/flush/compaction.

### ⚠ Writers still serialise on `write`
**Not yet addressed** (explicitly deferred to a future pass, per the
"user decision" recorded in the Stage 2+3 plan). Two concurrent
`write_batch` calls will block each other on the `write` Mutex — the WAL
append + commit_seq allocation must be strictly ordered so a multi-row
commit's rows share one `commit_seq`. This is the same single-writer model
RocksDB uses (one `LogFile`/`MemTable` writer at a time), and it is
*sufficient* for the current single-writer-node design (Phase 3 lease
fencing ensures only one writer node at a time anyway). Lock-free
multi-writer (or a writer thread that batches concurrent writes) is a
separate scaling effort, not a correctness gap.

### ⚠ Database-level catalog/index locks are per-structure
`Database` (`storage/database/mod.rs`) holds separate `Arc<Mutex<HashMap>>`
locks for `schemas`, `indexes`, `geo_indexes`, `ts_indexes`, etc. — these are
not on the hot read path through the LSM (reads go through `lsm.read_at`/
`scan_at` / `txn_read` / `txn_scan`), but index *lookups* during indexed
queries do take the relevant per-engine index lock briefly. This is a follow-up
to concurrent index structure design (e.g. lock-free B-trees), not the coarse
global mutex the Stage 2 audit flagged.

## Test coverage

| Concern                         | Test                                             | Location            |
|---------------------------------|--------------------------------------------------|---------------------|
| Flush doesn't block reads       | `finish_flush_does_not_block_a_concurrent_read`  | `storage/lsm.rs`    |
| Compaction doesn't block reads  | `finish_compact_does_not_block_a_concurrent_read`| `storage/lsm.rs`    |
| Snapshot GC watermark pinning   | `abandoned_transaction_snapshot_is_released_on_drop` | `executor/transaction_tests.rs` |
| Concurrent transactions see isolation | `transaction_isolation_between_connections`, `snapshot_isolation_hides_other_transactions_later_commits` | `executor/transaction_tests.rs` |

## Remaining follow-ups (not done this pass)

1. **Lock-free multi-writer** — shard the WAL by partition or use a
   single-writer-thread pattern (RocksDB's `WritableFileWriter` model) to allow
   concurrent `INSERT`s to overlap their fsyncs. Out of scope: single writer
   node by lease.
2. **Per-index concurrent access** — the `Database`-level `Mutex<HashMap>`
   guards for BTree / GeoIndex / GraphIndex etc. could be narrowed, but these
   are local-only accelerators (not shared across nodes) and not on the
   critical read path for non-indexed scans.
3. **Write-read pipelining on the memtable** — `MemTable` uses a lock-free
   `SkipMap` for reads *during* writes, but the `SkipMap` still allocates
   epoch pages on each insert. Crossbeam-epoch's amortised cost is acceptable
   today; a pre-allocated arena memtable is a future optimisation.
