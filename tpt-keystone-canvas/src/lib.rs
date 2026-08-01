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
//!   see `render.rs`) for every component except an opt-in WebGPU
//!   proof-of-concept on `CanvasTimeSeries` (`webgpu.rs`).** Real WebGPU
//!   pipelines (shaders, buffer layouts, render passes) for four different
//!   visualisations would still be an order of magnitude more code than the
//!   rest of this crate combined, so only one component got the PoC, scoped
//!   to a single line-strip draw call with no text/markers/theming — see
//!   `webgpu.rs`'s module docs for the exact cut list. It's feature-detected
//!   and additive: `CanvasTimeSeries` always mounts Canvas2D synchronously
//!   first (unchanged behavior/API), then asynchronously tries WebGPU and
//!   switches future redraws to it only if negotiation succeeds — Canvas2D
//!   remains the guaranteed-working path on every browser. Not
//!   browser-tested (no headless-WebGPU harness here), and extending the PoC
//!   to `CanvasMap`/`CanvasGraph` is unstarted.
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
