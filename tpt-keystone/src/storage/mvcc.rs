use std::sync::atomic::{AtomicU64, Ordering};

/// A transaction ID generator. This is an opaque per-connection identifier
/// (`TxnHandle::id`) used for logging/observability — it is independent of
/// the MVCC commit-sequence numbers (`LsmEngine::next_commit_seq`) that
/// actually drive snapshot-isolation visibility; see `storage::lsm` and
/// `storage::internal_key` for that mechanism.
///
/// This module used to also hold `MvccStore`, an in-memory, RAM-only
/// version-chain prototype (`docs/mvcc_snapshot_audit.md`'s Stage 2 audit).
/// Its visibility rule ("latest version with `tx_id` committed before the
/// snapshot") was the reused design for real snapshot isolation, but the
/// store itself was never wired into any read/write path and has been
/// replaced by the durable, `LsmEngine`-integrated version chain
/// (`storage::internal_key::InternalKey`, `LsmEngine::read_at`/`scan_at`/
/// `write_batch`/`compact_all`).
static NEXT_TX_ID: AtomicU64 = AtomicU64::new(1);

/// Generate a new unique transaction ID.
pub fn new_tx_id() -> u64 {
    NEXT_TX_ID.fetch_add(1, Ordering::SeqCst)
}
