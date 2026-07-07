# Write-Ahead Log (WAL) Format

Source: `tpt-keystone/src/storage/wal.rs`.

One WAL is a single local file, `wal.log`, in the database's local directory
(the WAL is *not* one of the shared object-store artifacts — it's local,
durable-on-fsync storage that gets sealed and shipped to the object store
under `wal/...` once full; the shipped copy is a byte-identical copy of this
file's contents, not a different encoding).

There is no file header — the file is purely a concatenation of records,
appended in sequence-number order starting at 1.

## Record layout

All integers are big-endian.

```
u64 seq                (monotonically increasing, starts at 1)
u32 table_len
<table_len> bytes table name (UTF-8)
u32 key_len
<key_len> bytes key
u32 value_len
<value_len> bytes value
u8  record_type         (0 = insert, 1 = update, 2 = delete)
```

Records are appended with the file cursor explicitly seeked to end-of-file
before each write (not via `O_APPEND`/`.append(true)` — see the comment in
`Wal::open`: on Windows, `.append(true)` only grants `FILE_APPEND_DATA`,
which isn't sufficient for the truncate operation the WAL also needs, so the
write position is managed manually instead), followed by an `fsync`
(`File::sync_all`) before the write is considered durable. Every single
`append` call fsyncs — there is no group-commit/batching.

## Recovery

On `Wal::open`, if the file already exists, its records are scanned front to
back to find the highest `seq` present (`Self::scan_max_seq`) — this is a
full linear scan of every WAL record on every open, decoding each record's
length-prefixed fields far enough to skip to the next one — and the next
write continues from `max_seq + 1`. The MemTable is separately rebuilt by
replaying every record in order and applying it (insert/update/delete) —
this is standard LSM WAL replay, not shown in this file since it's driven by
the LSM engine (`lsm.rs`), not `Wal` itself.

## Truncation

After a successful MemTable flush to an SSTable, the WAL is truncated
(`set_len(0)` plus a seek-to-start and an `fsync`) since its records are now
durable in the flushed SSTable and no longer needed for crash recovery.
