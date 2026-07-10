//! Library surface so standalone binaries under `examples/` (e.g.
//! `gpu_smoke_test`) and `benches/` (Phase 12's Keystone-only benchmark
//! harness) can depend on the engine without a running server. `main.rs`
//! still declares its own `mod ...;` for every module below, for the actual
//! `tpt-keystone` binary — this re-exports the same source files as a
//! second, independent compilation unit rather than restructuring `main.rs`'s
//! module wiring around a library crate it doesn't otherwise need. Started
//! as `geo`-only; widened to the modules a real end-to-end benchmark needs
//! (`storage`, `executor`, `sql`, `vector`).

pub mod executor;
pub mod geo;
pub mod graph;
pub mod mcp;
pub mod metrics;
pub mod mirror;
pub mod sql;
pub mod storage;
pub mod synapse;
pub mod vector;
pub mod wire;
