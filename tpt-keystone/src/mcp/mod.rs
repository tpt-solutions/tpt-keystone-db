//! Minimal MCP (Model Context Protocol) server, listening on a second port
//! alongside the Postgres wire listener so AI agents can discover schema and
//! run queries without a Postgres driver. Hand-rolled HTTP + JSON-RPC 2.0
//! (see `http.rs`/`protocol.rs`) rather than pulling in hyper/axum, in
//! keeping with this project's hand-written-protocol approach — see
//! `protocol.rs` for the documented transport scope cuts.

mod http;
mod protocol;
mod server;
mod tools;
#[cfg(test)]
mod tests;

pub use server::handle;
/// Re-exported so other in-process modules (Synapse's actor runtime) can
/// invoke an MCP tool directly by name without going over HTTP — "MCP
/// server integration" (Phase 16) means calling the same dispatcher the
/// wire-level `handle` loop already uses, not a second implementation.
pub use tools::call as call_tool;
