//! TPT Synapse — agent orchestration & memory (Phase 16, `TODO.md`).
//!
//! Deliberately **not** a new storage engine: every persistent piece here is
//! plain rows in ordinary Keystone tables, indexed by the *existing*
//! Chronos (`storage::ts_index`) and Prism (`storage::vector_index`) local
//! secondary indexes, with task delegation built on the *existing* Flux
//! (`storage::flux`) topic/consumer-group machinery — the same "SQL
//! extension over Keystone core" shape Chronos/Plexus/Canopy/Flux already
//! use, not a fifth storage format. The only genuinely new engine code is
//! `actor`: a real Tokio actor runtime (one task per agent, mpsc mailbox),
//! since nothing else in this codebase provides live in-process,
//! message-passing concurrency.
//!
//! Submodules:
//! - `actor` — agent lifecycle (spawn/pause/resume/checkpoint/terminate),
//!   message-passing via `tokio::sync::mpsc`, checkpoints persisted to
//!   `_synapse_agents`
//! - `memory` — the four memory tiers from the TODO.md checklist, all rows
//!   in one `_synapse_memory` table distinguished by a `tier` column:
//!   short-term (TTL'd, GC'd), long-term (untouched), episodic (Chronos
//!   `USING TIME` index on `ts`), semantic (Prism `USING VECTOR` index on
//!   `embedding`, deduplicated at write time)
//! - `tools` — a tool registry (`_synapse_tools`) discoverable by exact name
//!   or by Prism semantic search over a caller-supplied embedding
//! - `coordination` — multi-agent task delegation on a per-workflow Flux
//!   topic, plus last-write-wins shared workflow state
//!
//! Explicit scope cuts (tracked unchecked in `TODO.md`, not stubbed and
//! claimed done):
//! - No LLM/agent "brain" of any kind — `actor::StepFn` is a caller-supplied
//!   closure; deciding what an agent actually does is application code, not
//!   this database engine's job (the same boundary Flux draws around a
//!   consumer's business logic)
//! - No automatic respawn of agents on process restart — checkpoints are
//!   durable (Keystone WAL/LSM), but resuming after a real restart means the
//!   caller explicitly calls `AgentRegistry::resume_from_checkpoint`, mirroring
//!   Flux's own "no background scheduler" discipline used throughout this
//!   codebase
//! - Coordination conflict resolution is last-write-wins (`Database::write`'s
//!   natural overwrite-by-key semantics), not a CRDT/vector-clock merge
//! - Memory/tool tables are local secondary indexes' *base tables* (ordinary
//!   Keystone rows), so they ARE object-store-replicated like any other
//!   table; only the Chronos/Prism *index* structures over them are
//!   local-only, the same split every other phase's indexes already have

pub mod actor;
pub mod coordination;
pub mod memory;
pub mod tools;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::storage::database::Database;
use crate::storage::{ColumnDef, ColumnType};

/// Invokes an MCP tool (`query`, `mutate`, `schema`, `tables`, `columns`,
/// `explain`, `related`) directly in-process — "MCP server integration:
/// agents discover and invoke tools via Phase 5 AI Layer endpoints"
/// (TODO.md) means calling the same dispatcher the wire-level MCP server
/// uses, not a second implementation, and no network hop since Synapse and
/// the MCP server share the same `Arc<Database>` process.
pub fn invoke_mcp_tool(
    db: &Arc<Database>,
    name: &str,
    args: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    crate::mcp::call_tool(db, &crate::executor::rbac::Actor::unrestricted(), name, args)
}

/// Encodes a row's cells into the same length-prefixed wire format
/// `executor::execute_insert`/`parse_rows` use — duplicated here (rather
/// than exported from `executor`) because every other index/table-owning
/// module in this codebase (`copy.rs`, `execute_insert`, `execute_update`)
/// already carries its own copy of this same ~6-line loop.
pub(crate) fn encode_cells(cells: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for cell in cells {
        match cell {
            Some(data) => {
                buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                buf.extend_from_slice(data);
            }
            None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    buf
}

/// Decodes the `idx`-th cell from a row's encoded value blob. Mirrors
/// `storage::database::decode_column`, which is private to that module.
pub(crate) fn decode_cell(value: &[u8], idx: usize) -> Option<Vec<u8>> {
    let mut pos = 0usize;
    let mut i = 0usize;
    while pos + 4 <= value.len() {
        let len = i32::from_be_bytes(value[pos..pos + 4].try_into().unwrap());
        pos += 4;
        if len < 0 {
            if i == idx {
                return None;
            }
            i += 1;
            continue;
        }
        let end = pos + len as usize;
        if end > value.len() {
            return None;
        }
        if i == idx {
            return Some(value[pos..end].to_vec());
        }
        pos = end;
        i += 1;
    }
    None
}

pub(crate) fn text_cell(s: impl Into<String>) -> Option<Vec<u8>> {
    Some(s.into().into_bytes())
}

pub(crate) fn int_cell(n: i64) -> Option<Vec<u8>> {
    Some(n.to_string().into_bytes())
}

pub(crate) fn cell_text(cell: &Option<Vec<u8>>) -> Option<String> {
    cell.as_ref()
        .and_then(|b| String::from_utf8(b.clone()).ok())
}

pub(crate) fn cell_i64(cell: &Option<Vec<u8>>) -> Option<i64> {
    cell_text(cell).and_then(|s| s.parse().ok())
}

pub(crate) fn col(name: &str, ty: ColumnType, nullable: bool, is_pk: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        col_type: ty,
        nullable,
        default: None,
        is_pk,
    }
}

/// Process-wide monotonic counter used to build unique row ids
/// (`<prefix>-<millis>-<seq>`) without pulling in a UUID dependency — a
/// from-scratch id scheme, consistent with this codebase's "no crate for
/// something a dozen lines of hand-written code covers" discipline
/// elsewhere (hex/sha2 are the only exceptions, both for actual
/// cryptographic hashing).
static ID_SEQ: AtomicU64 = AtomicU64::new(0);

pub(crate) fn new_id(prefix: &str) -> String {
    let seq = ID_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{seq}", crate::storage::flux::now_ms())
}

pub(crate) fn now_ms() -> i64 {
    crate::storage::flux::now_ms()
}
