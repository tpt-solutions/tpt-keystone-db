//! TPT SDK/Rust — native desktop & server access to TPT Keystone and TPT
//! Canvas, per `9sdkspec.txt` section 2 / `TODO.md`'s "SDK/Rust (Native
//! Desktop & Server)" checklist.
//!
//! ## Scope cuts from the spec, made explicit rather than silently dropped
//!
//! - **"Direct Canvas rendering pipeline access (WebGPU / Vulkan)" does not
//!   exist.** `tpt-canvas` itself only implements a Canvas2D renderer behind
//!   `wasm32-unknown-unknown` (see that crate's `lib.rs` doc comment) — there
//!   is no native GPU pipeline anywhere in the repo to bind to. The `canvas`
//!   feature here re-exports the one part of `tpt-canvas` that *is`
//!   target-agnostic (`reactive.rs`'s `Signal`/`create_effect`/`create_memo`),
//!   so a native host can drive the same reactive primitives Canvas
//!   components use, but it cannot render a `Canvas.Map` outside a browser.
//! - **FFI (`ffi` feature) covers query execution, not the whole client.**
//!   `tpt_sdk_connect`/`tpt_sdk_query`/`tpt_sdk_free_result` are enough to
//!   drive request/response query flows from C/C++; there's no FFI surface
//!   for streaming/async callbacks.
//! - **Zero-copy is row-batch-level, not column-level.** [`zerocopy::RowView`]
//!   borrows directly from the wire read buffer instead of allocating a
//!   `Vec<Vec<u8>>` per row, matching the `Option<Vec<u8>>`-cell row encoding
//!   `CLAUDE.md` documents for `tpt-keystone` itself — it does not implement
//!   columnar/Arrow-style zero-copy.
//!
//! ## Modules
//!
//! - [`keystone`] (feature `keystone`, default-on) — async
//!   [`keystone::KeystoneClient`] plus a [`keystone::blocking::Client`]
//!   synchronous wrapper, both speaking the same hand-written Postgres
//!   wire protocol v3 as `tpt-keystone/src/wire`.
//! - [`zerocopy`] — the borrowed row view shared by `keystone` and `ffi`.
//! - [`canvas`] (feature `canvas`) — re-export of `tpt-canvas`'s reactive
//!   core; see the scope-cut note above.
//! - [`ffi`] (feature `ffi`) — `extern "C"` bindings.

#[cfg(feature = "keystone")]
pub mod keystone;

pub mod zerocopy;

#[cfg(feature = "canvas")]
pub mod canvas;

#[cfg(feature = "ffi")]
pub mod ffi;

pub mod prelude {
    #[cfg(feature = "keystone")]
    pub use crate::keystone::{blocking, KeystoneClient, Row, Value};
    pub use crate::zerocopy::RowView;
}
