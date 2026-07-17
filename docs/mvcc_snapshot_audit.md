# MVCC snapshot-isolation audit (Phase 1, Stage 2)

Scope: before committing to turning `storage/mvcc.rs` + `storage/tx.rs` into the
real storage substrate for snapshot isolation, this document records what is
reusable as-is, what needs rework, and the order of operations. It is the
pre-implementation audit called for by `TODO.md` Phase 1 Stage 2. No storage
code was changed while writing it — Stage 1 (read-committed, single-version LSM
+ `TxnHandle` staging buffer) remains the active path.

## Current state (Stage 1)

- `storage/database/txn.rs` — `TxnHandle` holds a `BTreeMap` staging buffer of
  pending writes. Every read consults the buffer first (tombstone = `None`),
  then falls through to the committed LSM. `COMMIT` replays the buffer into the
  LSM under the single `Mutex<LsmEngine>`; `ROLLBACK` discards it. This is
  **read-committed**: a transaction sees its own uncommitted writes and any
  committed writes made by others *after* it began. There is **no snapshot
  taken at `BEGIN`**.
- `storage/mvcc.rs` — `MvccStore` is a self-contained `BTreeMap<Vec<u8>,
  Vec<Version>>` versioned store with `write_version`/`delete_version`/
  `read_version(snapshot_tx_id)`/`commit_tx`/`rollback_tx`/`scan`/`key_count`.
  It is a correct, minimal MVCC *design*, but it is **not wired into the read
  or write paths**.
- `storage/tx.rs` — `TransactionManager` owns an `Arc<MvccStore>`, hands out
  `Transaction { id, snapshot_id, isolation, writable }`, and calls
  `mvcc.commit_tx`/`rollback_tx` on commit/rollback. `Database` constructs a
  `TransactionManager` (`database/mod.rs:203`) but `commit_txn`/`rollback_txn`
  ignore it and use `TxnHandle` replay instead, so `TransactionManager`/
  `MvccStore` are currently **dead weight** (constructed, never driven).

## Reusability verdict

| Piece | Reuse as-is? | Notes |
|---|---|---|
| `MvccStore` data model (`Version`, version chains) | **Partial** | The in-memory `BTreeMap` version list is the right shape, but it must be backed by the durable LSM/SSTable/WAL, not a separate `Arc<Mutex<BTreeMap>>`. Today `LsmEngine` stores a single value per key (`MemTable.data: BTreeMap<Vec<u8>, (Vec<u8>, u8)>`), so multi-version storage requires `lsm.rs`/`sstable.rs`/`wal.rs` changes (Stage 3). |
| `read_version(snapshot_tx_id)` visibility rule | **Yes** | The "latest version with `tx_id < snapshot && committed`" rule is exactly snapshot isolation and can be reused once versions live in the durable store. |
| `TransactionManager` | **Rework** | It duplicates `TxnHandle`'s id generation and adds nothing the staging buffer doesn't already do. Plan: fold snapshot-tx-id assignment into `Database::begin_txn` and have `TxnHandle` carry a `snapshot_tx_id` instead of standing up a parallel manager. |
| `TxnHandle` staging buffer | **Keep, extend** | Keep the staging buffer for *uncommitted* writes (so a txn sees its own writes instantly). Snapshot isolation only needs to also *freeze the committed view* at `BEGIN` — i.e. reads consult versions committed before `snapshot_tx_id`, not "whatever is committed now". |
| `new_tx_id` global counter | **Yes** | Fine as the monotonic tx-id source. |

## Risks / open questions

1. **Durability of versions.** `MvccStore` lives only in RAM. Making it the
   substrate means SSTables and the WAL must record `(key, version, tx_id,
   is_deleted)` tuples, and the manifest must survive a versioned key set. The
   S3 shard layout in `lsm.rs` (`sst_shard`) is version-agnostic today and
   would need the version stamped into the key or a sidecar.
2. **Compaction GC (Stage 3).** With multiple versions per key, levelled
   compaction must keep a version alive only if some still-open transaction's
   `snapshot_tx_id` could still read it, else drop it. This couples compaction
   to the active-transaction set — `TransactionManager`'s `active` map (or a
   new `Database`-level registry) must be consulted during compaction.
3. **Concurrency (Stage 4 of Phase 1).** Today a single coarse
   `Mutex<LsmEngine>` serialises all reads/writes. Under MVCC, readers should
   not block writers: reads consult immutable version chains, only writes take
   the lock. The lock can likely be narrowed to *write* operations once the
   read path is version-chain based, but the `StorageEngine` trait's callers
   (`database/mod.rs` read/write helpers at ~601–659) assume a single mutable
   `&mut self`/`&self` through the mutex — that API boundary is where finer
   granularity is proven out.
4. **Reader-node convergence (known follow-up).** `catalog.rs`'s `refresh()`
   uses `.entry().or_insert()` and never removes dropped schemas; with
   versioned storage the same gap exists for dropped key versions replicated to
   a reader. Out of scope for this audit but must be revisited alongside Stage 3.

## Recommended order of operations

1. Make `TxnHandle` carry a `snapshot_tx_id` assigned at `begin_txn` (cheap; no
   storage change yet). Read paths that currently fall through to the LSM
   instead fall through to a `read_version(snapshot_tx_id)` view once the LSM
   can serve it. *(small, contained)*
2. Stage versions into the durable LSM (`lsm.rs`/`sstable.rs`/`wal.rs`) keyed
   by `(key, tx_id)`, keeping the single-value fast path for the common
   single-version case. *(Stage 3 — large)*
3. Add compaction-time GC gated on the active `snapshot_tx_id` set. *(Stage 3)*
4. Narrow the global mutex to writes; prove readers don't block. *(Stage 4)*
5. Retire the now-redundant `TransactionManager`; `Database` owns tx-id
   allocation and the active-snapshot registry directly.

Until step 2 lands, snapshot isolation is **not** achievable — the durable
store cannot represent more than one version of a key. This audit therefore
recommends treating Stage 2 + Stage 3 as one contiguous effort rather than two
independent passes.
