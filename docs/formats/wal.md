# Write-Ahead Log (WAL) Format

Source: `tpt-keystone/src/storage/wal.rs` (framing, replay, truncation) and
`tpt-keystone/src/storage/io_backend.rs` (the append+fsync backend).

One WAL is a single local file, `wal.log`, in the database's local directory.
It is *not* one of the shared object-store artifacts. It is local,
durable-on-fsync storage that gets sealed and shipped to the object store
under `wal/...` once full; the shipped copy is a byte-identical copy of this
file's contents, not a different encoding.

There is no file header. The file is purely a concatenation of **batches**
(see below), appended in commit-sequence order.

## Batch layout

All integers are big-endian.

```
u8  marker = 0xFF        (BATCH_MARKER; distinct from every record_type value)
u64 commit_seq           (the MVCC commit sequence shared by every record in this batch)
u32 count                (number of records in this batch)
count * record
```

Each record:

```
u32 table_len
<table_len> bytes table name (UTF-8)
u32 key_len
<key_len> bytes key
u32 value_len
<value_len> bytes value
u8  record_type          (0 = insert/put, 1 = update, 2 = delete)
```

A single autocommit write is a batch with `count = 1`. A multi-row
transaction's `COMMIT` is one batch containing all of its rows, so every row
in it shares one `commit_seq`.

There is **no checksum** in this format. Truncation is detected structurally:
every field decode is bounds-checked, and a short read anywhere inside a batch
marks that batch as torn.

## Append and durability

Each batch is written with one `write` followed by one `fsync`
(`Wal::append_batch` → `WalIo::append`). There is no group commit across
batches. The write position is managed explicitly rather than with
`O_APPEND`/`.append(true)`. On Windows, `.append(true)` grants only
`FILE_APPEND_DATA`, which is not enough for the truncate the WAL also needs.

The default backend is portable `std::fs`. On Linux with `TPT_IO_URING=1`, the
backend submits the same write through `io_uring`. The on-disk bytes are
identical either way.

## Recovery (replay)

`Wal::replay` reads the whole file and walks it batch by batch:

1. If fewer than 13 bytes remain, or the byte at the cursor is not `0xFF`, replay
   **stops**. Everything after that point is ignored. The format has no
   resynchronisation marker, so a non-`0xFF` byte cannot be skipped past.
2. Read `commit_seq` and `count`. The returned high-water mark is raised to
   `commit_seq` *before* the records are checked. A torn batch therefore still
   counts, so its `commit_seq` is never reissued.
3. Decode `count` records. If the buffer ends partway through any record, the
   **entire batch is discarded**, not just the torn record. This is what makes
   multi-row replay atomic.
4. Otherwise, call the replay callback with each record in order.

`replay` returns the highest `commit_seq` seen, including from torn batches.
The LSM engine uses this to seed its next commit sequence on open. The
MemTable is rebuilt by applying the replayed records in order. That is LSM
logic in `lsm.rs`, not part of this file's format.

## Truncation

After a successful MemTable flush to an SSTable, `Wal::truncate` empties the
file (`set_len(0)`, a seek to the start, and an `fsync`). The flushed SSTable
now holds those records durably, so the WAL is no longer needed for crash
recovery.

## Versioning

No version byte. This layout replaced an earlier per-record layout (one
record per append, no batch marker) during the MVCC work in TODO Phase 1
Stage 2+3. A `wal.log` written by the earlier layout is **not readable** by
this one. `replay` sees a first byte that is not `0xFF` and silently stops. Any
node upgraded with a non-empty, un-flushed WAL must flush or drain it on the
old build before upgrading. See the Versioning section of [`README.md`](README.md#versioning).
