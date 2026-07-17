# tpt-keystone-canvas

Data-aware frontend framework for TPT Keystone — Rust compiled to `wasm32-unknown-unknown` via
`wasm-bindgen`. Provides six reactive `Canvas.*` components (Map, Chart, Table, TimeSeries, Graph,
AgentMonitor), a fine-grained reactive core, and a Keystone HTTP/WebSocket client — all consumed from
JavaScript or TypeScript as a plain ES module.

## Build

```bash
cd tpt-keystone-canvas
cargo build --target wasm32-unknown-unknown
wasm-bindgen --target web --out-dir pkg target/wasm32-unknown-unknown/debug/tpt_canvas.wasm
```

Vite, Webpack, and esbuild load the resulting ES module + `.wasm` file natively with no custom plugin.

## TypeScript type generation

```bash
# Requires a running tpt-keystone node on :5435
cargo run --bin tsgen -- --host localhost --port 5435 --out types/
```

`tsgen` speaks a hand-rolled HTTP GET against the node's `/schema` endpoint (no reqwest dependency)
and emits TypeScript interfaces for your tables.

## Scope cuts (explicit)

- **Rendering is Canvas2D** (`CanvasRenderingContext2d`), not WebGPU. Canvas2D is hardware-accelerated
  by the browser and ships today; a full WebGPU pipeline per component would be an order of magnitude
  more code.
- **No JSX/TSX.** Components are `#[wasm_bindgen]` classes constructed imperatively from JS/TS.
- **`use_keystone_query` does not auto-infer Flux topics** — the caller supplies an explicit
  `realtime_topic`. A message on that topic triggers a full re-fetch, not an incremental row patch.
- **No native GPU rendering** — the `canvas` feature of `tpt-keystone-sdk` re-exports the target-agnostic
  reactive core only; `Canvas.*` components cannot render outside a browser.

## License

Apache-2.0 — Copyright 2026 TPT Solutions
