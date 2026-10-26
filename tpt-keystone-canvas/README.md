# tpt-keystone-canvas

[![crates.io](https://img.shields.io/crates/v/tpt-keystone-canvas.svg)](https://crates.io/crates/tpt-keystone-canvas)
[![docs.rs](https://img.shields.io/docsrs/tpt-keystone-canvas)](https://docs.rs/tpt-keystone-canvas)
[![license](https://img.shields.io/crates/l/tpt-keystone-canvas.svg)](https://github.com/tpt-solutions/tpt-keystone-db/blob/master/tpt-keystone-canvas/LICENSE-MIT)

`tpt-keystone-canvas` (TPT Canvas) is a data-aware frontend framework for
[TPT Keystone](../tpt-keystone/README.md). It is written in Rust, compiled to
`wasm32-unknown-unknown` with `wasm-bindgen`, and used from JavaScript or TypeScript as an ES module.

It provides:

- Six `Canvas*` components that render live query results: `CanvasMap`, `CanvasGraph`,
  `CanvasTimeSeries`, `CanvasDocument`, `CanvasVectorSearch`, and `CanvasAgentMonitor`
- A fine-grained reactive core (`Signal`, effects, and memos), shared with
  [`tpt-keystone-sdk`](../tpt-keystone-sdk/README.md) through its `canvas` feature
- A Keystone client that runs queries over HTTP and re-runs them when a Flux topic publishes
- `tsgen`, a host-side tool that generates TypeScript interfaces from a running node's schema

## Requirements

- Rust **1.85** or newer, with the wasm target installed:
  `rustup target add wasm32-unknown-unknown`
- `wasm-bindgen-cli` matching the crate's `wasm-bindgen` version, to generate the JavaScript glue
- A running `tpt-keystone` node for live data. The HTTP bridge serves queries on `:5435` and the Flux
  WebSocket on `:5434`.

## Build

```bash
cd tpt-keystone-canvas
cargo build --target wasm32-unknown-unknown
wasm-bindgen --target web --out-dir pkg target/wasm32-unknown-unknown/debug/tpt_canvas.wasm
```

Use `--release` for production builds. The output is an ES module plus a `.wasm` file. Vite, Webpack,
and esbuild all load it without a custom plugin.

The crate also builds for the host with plain `cargo build` and `cargo test`. Those tests cover the pure
logic (projection, layout, and pan and zoom math). Rendering runs only in a browser.

## Using a component

Each component is a `#[wasm_bindgen]` class. You construct it from JavaScript or TypeScript with the
ID of a container element, a base URL for the HTTP bridge, a base URL for the WebSocket, a SQL query, and
the field names to read:

```javascript
import init, { CanvasTimeSeries } from "./pkg/tpt_canvas.js";

await init();

const chart = new CanvasTimeSeries(
  "metrics-chart",            // <canvas> element id
  "http://localhost:5435",    // HTTP bridge
  "ws://localhost:5434",      // Flux WebSocket
  "SELECT ts, value FROM metrics ORDER BY ts DESC LIMIT 500",
  "ts",                       // x field
  "value",                    // y field
  "__cdc_metrics",            // Flux topic that triggers a re-fetch ("" to infer from the SQL)
);
```

The constructor returns a `Result`, so a bad container ID or query surfaces as a JavaScript exception.

## Components

| Component | Constructor arguments (after `canvas_id` or `container_id`, `http_base`, `ws_base`) | Drawn with |
|---|---|---|
| `CanvasMap` | `sql`, `location_field`, `realtime_topic`, `cluster`, `heatmap`, optional `on_click` | Canvas2D, or WebGPU when available |
| `CanvasTimeSeries` | `sql`, `x_field`, `y_field`, `realtime_topic` | Canvas2D, or WebGPU when available |
| `CanvasGraph` | `nodes_sql`, `edges_sql`, `realtime_topic` | Canvas2D, or WebGPU when available |
| `CanvasDocument` | `sql`, `table`, `column`, `pk_column`, `realtime_topic` | DOM (editable JSON tree) |
| `CanvasVectorSearch` | `sql`, `score_field`, `realtime_topic` | DOM (ranked list) |
| `CanvasAgentMonitor` | `events_sql`, `metrics_sql`, `realtime_topic` | DOM timeline, plus a Canvas2D latency chart |

`CanvasMap` draws points on a blank equirectangular canvas. It has no basemap tiles. It supports
grid-bucket clustering and a heatmap mode.

`CanvasDocument` writes edits back with `jsonb_set` on the server.

## Live queries

Each component re-runs its query when a message arrives on its `realtime_topic`. A message triggers a full
re-fetch, not an incremental update of the rows.

If you pass an empty topic, the client infers one from a single-table `SELECT`. Joins and non-`SELECT`
statements get no live updates.

Use the native CDC topic `__cdc_<table>` for a table, or name any Flux topic your writers publish to.

## Rendering backends

`CanvasMap`, `CanvasGraph`, and `CanvasTimeSeries` try WebGPU first and fall back to Canvas2D. The
fallback is automatic and works in every browser. The WebGPU renderers are a proof of concept. Each one
draws a single hard-coded colour, with no text and no per-instance uniforms.

The WebGPU path has not been exercised in a real browser in this repository. Its code compiles, and the
shared view math is unit-tested. Treat browser WebGPU as unverified until you have run it yourself.

## Reactive core

The reactive primitives (`Signal`, `create_effect`, `create_memo`) are in `src/reactive.rs`. They do not
depend on the DOM, so [`tpt-keystone-sdk`](../tpt-keystone-sdk/README.md) re-exports them for native code
with its `canvas` feature.

## TypeScript types with `tsgen`

`tsgen` reads a running node's `/schema` endpoint on the HTTP bridge and writes TypeScript interfaces to
stdout. It takes the bridge address as its only argument, and defaults to `127.0.0.1:5435`:

```bash
# Requires a running tpt-keystone node
cargo run --bin tsgen -- 127.0.0.1:5435 > types/schema.d.ts
```

Column types map as follows: integer and float types to `number`, `bool` to `boolean`, `json` to
`unknown`, and everything else to `string`. `tsgen` makes a plain HTTP GET using only the standard
library.

## Limitations

- **Rendering is mostly Canvas2D.** WebGPU covers only the three chart-style components above, and only
  as a narrow proof of concept.
- **No JSX or TSX.** Components are constructed imperatively. Wrap them in a framework component yourself.
- **No plugin API and no custom shaders.** The WebGPU pipelines are fixed.
- **No native rendering.** The `canvas` feature of `tpt-keystone-sdk` exposes only the reactive core.
  The `Canvas*` components render only in a browser.
- **Live queries re-fetch.** A topic message triggers a full query re-fetch, and joins get no automatic topic.

## Changelog

Release notes are in [`CHANGELOG.md`](CHANGELOG.md).

## Contributing

See [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE))
- MIT license ([`LICENSE-MIT`](LICENSE-MIT))

at your option. Copyright 2026 TPT Solutions.
