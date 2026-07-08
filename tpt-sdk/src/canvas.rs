//! `canvas` feature — re-exports `tpt-canvas`'s target-agnostic reactive
//! core so a native host (Tauri, GTK, a headless server) can drive the same
//! `Signal`/`create_effect`/`create_memo` primitives Canvas's
//! `Canvas.*` components use.
//!
//! **This is not "direct Canvas rendering pipeline access (WebGPU/Vulkan)"**
//! as `9sdkspec.txt` describes it — see `lib.rs`'s module doc for why: no
//! such native pipeline exists in `tpt-canvas` today, which only renders via
//! `web_sys::CanvasRenderingContext2d` behind `#[cfg(target_arch =
//! "wasm32")]`. What this module *does* give a native app is a way to
//! subscribe to the same reactive signals a Canvas component would recompute
//! from, e.g. to drive a native side panel that stays in sync with a
//! WASM-rendered Canvas view embedded elsewhere in the app.

pub use tpt_canvas::reactive::{create_effect, create_memo, Signal};
