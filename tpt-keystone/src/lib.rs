//! Minimal library surface, existing only so standalone binaries under
//! `examples/` (e.g. `gpu_smoke_test`) can depend on `geo` without a running
//! server. `main.rs` still declares its own `mod geo;` for the actual
//! `tpt-keystone` binary — this re-exports the same source files as a
//! second, independent compilation unit rather than restructuring `main.rs`'s
//! module wiring around a library crate it doesn't otherwise need.

pub mod geo;
