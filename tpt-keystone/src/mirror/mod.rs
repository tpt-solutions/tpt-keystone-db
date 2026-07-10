//! TPT Mirror — agent observability & debugging (Phase 17, `TODO.md`).
//!
//! Built directly on Synapse (Phase 16) and the primitives it already
//! composes — Flux for ordered event logs, Chronos for time-indexed
//! metrics, plain Keystone tables for everything else — the same "SQL
//! extension over Keystone core" shape every prior phase uses, not a sixth
//! storage engine. Cell encode/decode/id-generation helpers are reused
//! directly from `synapse` (`pub(crate)`, so visible crate-wide) rather than
//! duplicated a third time.
//!
//! Submodules:
//! - `trace` — agent action tracing: every decision/tool-call/outcome
//!   written as an immutable, ordered event to a per-session Flux topic
//! - `replay` — session replay (`ReplayEngine`) and an event-by-event
//!   stepper (`SessionCursor`) — the "debug REPL" *engine*; a literal
//!   interactive terminal front-end wasn't built (that's `tpt-cli`'s job,
//!   out of scope for this crate, same boundary Chronos/Plexus/Canopy never
//!   crossed into a UI either)
//! - `metrics` — per-agent latency/token/success-rate metrics, stored via a
//!   real Chronos time index (unlike Synapse's episodic-memory placeholder
//!   value column, `latency_ms` here is an actual metric worth rolling up)
//! - `audit` — a hash-chained, tamper-evident compliance audit trail
//! - `provenance` — who/what asserted a stored fact, and when
//!
//! Explicit scope cuts (tracked unchecked in `TODO.md`, not stubbed and
//! claimed done):
//! - No dashboard UI. `executor/mirror_tests.rs`-adjacent SQL table
//!   functions (`mirror_session_events`, `mirror_agent_metrics`) expose the
//!   same data a real dashboard would query, but no chart/frontend
//!   rendering was built in this pass — same "no browser available in this
//!   environment" limitation Phase 13's Canvas milestone already documents,
//!   here applied honestly rather than faked
//! - OTel span integration reuses Phase 12's existing global `tracing`
//!   subscriber (`telemetry::init`) via `#[tracing::instrument]` on the
//!   trace/audit write paths — verified by the same "spans exist and
//!   export when `OTEL_EXPORTER_OTLP_ENDPOINT` is set" mechanism Phase 12
//!   already relies on, not a second OTel pipeline
//! - No multi-node/distributed replay — single-node, same scope cut as
//!   every other phase's local/session-scoped state

pub mod audit;
pub mod metrics;
pub mod provenance;
pub mod replay;
pub mod trace;

use std::sync::atomic::{AtomicU64, Ordering};

/// A monotonic counter independent of wall-clock resolution, used where
/// `synapse::new_id`'s embedded millisecond timestamp isn't precise enough
/// to order events reliably (e.g. several audit entries recorded within the
/// same millisecond) — `audit.rs`'s hash chain needs a strict total order.
static SEQ: AtomicU64 = AtomicU64::new(0);

pub(crate) fn next_seq() -> u64 {
    SEQ.fetch_add(1, Ordering::Relaxed)
}
