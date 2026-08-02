//! TPT Canvas — the data-aware frontend framework for TPT Keystone
//! (Phase 13). Rust compiled to `wasm32-unknown-unknown` via `wasm-bindgen`,
//! consumed from JS/TS as a plain ES module (`wasm-bindgen --target web`
//! output) — Vite/Webpack/esbuild all load an ES module + `.wasm` file
//! natively, so "integration with popular bundlers" needs no custom plugin
//! code in this crate, unlike everything else here.
//!
//! ## Scope cuts from the Phase 13 / `8canvasspec.txt` spec, made explicit
//! rather than silently dropped:
//!
//! - **Rendering backend is Canvas2D (`web_sys::CanvasRenderingContext2d`,
//!   see `render.rs`) by default, with an opt-in WebGPU proof-of-concept on
//!   all three "drawn to a canvas" components — `CanvasTimeSeries`, `CanvasMap`,
//!   `CanvasGraph` (`webgpu/{timeseries,map,graph}.rs`).** Real WebGPU
//!   pipelines (shaders, buffer layouts, render passes) for a general-purpose
//!   renderer would still be an order of magnitude more code than the rest of
//!   this crate combined, so each component's PoC pipeline is narrowly scoped
//!   (one hardcoded color, no text, no per-instance uniforms — see
//!   `webgpu/mod.rs`'s module docs for the exact cut list, and `view_transform.rs`
//!   for the shared pan/zoom math both backends use). It's feature-detected
//!   and additive: on construction, each component's `<canvas>` element hasn't
//!   requested a rendering context yet, so an async task
//!   (`wasm_bindgen_futures::spawn_local`) races WebGPU negotiation first and
//!   only falls back to Canvas2D — which is *not* attempted until after WebGPU
//!   negotiation fails — if that fails, since a `<canvas>` commits permanently
//!   to whichever context type ("2d" xor "webgpu") is requested on it first.
//!   Canvas2D remains the guaranteed-working path on every browser; nothing is
//!   drawn until the async race resolves, at which point a `redraw_trigger`
//!   signal forces one repaint. Not browser-tested (no headless-WebGPU harness
//!   here) — verified only by `cargo build --target wasm32-unknown-unknown`
//!   compiling clean and `cargo test`'s host-side coverage of the pure
//!   math (`view_transform.rs`, `components/*.rs`'s projection/layout helpers).
//! - **No plugin API / custom WebGPU shader hooks** — falls out of the
//!   Canvas2D decision above; there's no shader pipeline to hook into.
//! - **`useKeystoneQuery`-equivalent (`client::KeystoneClient::use_keystone_query`)
//!   does not auto-infer which Flux topic to subscribe to from the SQL
//!   text.** The caller names an explicit `realtime_topic` (e.g. Phase 11's
//!   native `__cdc_<table>` CDC topic). A message on that topic triggers a
//!   full re-fetch of the query, not an incremental patch of the existing
//!   rows — see `client.rs` module docs.
//! - **No JSX/TSX component syntax.** `<Canvas.Map data={...} />` becomes
//!   `new CanvasMap(...)` from TS/JS — every component is a `#[wasm_bindgen]`
//!   class constructed and mounted imperatively, per `components/*.rs`.
//! - **Automatic TypeScript type generation** (`src/bin/tsgen.rs`) is a
//!   standalone CLI tool run against a live node's `/schema` endpoint, not a
//!   bundler plugin that runs on every build.
//!
//! See `reactive.rs` for the fine-grained reactive core, `client.rs` for the
//! Keystone HTTP/WebSocket bridge, and `components/` for the six
//! `Canvas.*` components (the sixth, `AgentMonitor`, is Mirror-native,
//! Phase 17).

    pub mod client;
    pub mod components;
    pub mod reactive;
    pub mod render;
    pub mod theme;
    pub mod view_transform;
    pub mod webgpu;


#[cfg(target_arch = "wasm32")]
pub use components::agent_monitor::CanvasAgentMonitor;
#[cfg(target_arch = "wasm32")]
pub use components::document::CanvasDocument;
#[cfg(target_arch = "wasm32")]
pub use components::graph::CanvasGraph;
#[cfg(target_arch = "wasm32")]
pub use components::map::CanvasMap;
#[cfg(target_arch = "wasm32")]
pub use components::timeseries::CanvasTimeSeries;
#[cfg(target_arch = "wasm32")]
pub use components::vector_search::CanvasVectorSearch;
