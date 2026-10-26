# Changelog

All notable changes to `tpt-keystone-canvas` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is at `0.1.0` and has not been published to crates.io yet, so all changes are listed under
`[Unreleased]`. Changes that span the whole repository are also recorded in the root
[`CHANGELOG.md`](../CHANGELOG.md).

## [Unreleased]

### Added

- `CanvasMap`, `CanvasGraph`, `CanvasTimeSeries`, `CanvasDocument`, `CanvasVectorSearch`, and
  `CanvasAgentMonitor` components, each constructed from JavaScript or TypeScript with a container ID.
- Reactive core (`Signal`, effects, and memos) that does not depend on the DOM, re-exported by
  `tpt-keystone-sdk` behind its `canvas` feature.
- Keystone HTTP and WebSocket client, with live queries that re-run when a Flux topic publishes.
- `tsgen`, a host-side binary that generates TypeScript interfaces from a node's `/schema` endpoint.
- Opt-in WebGPU proof-of-concept renderers for `CanvasTimeSeries`, `CanvasMap`, and `CanvasGraph`.
  Each falls back to Canvas2D when WebGPU is unavailable.
- `CanvasGraph` renders results of GQL `MATCH` queries.
- A design-token theming system for the components.
- Per-crate `README.md`, `CHANGELOG.md`, and `LICENSE-MIT`/`LICENSE-APACHE` files, and crates.io
  metadata (`repository`, `readme`, `keywords`, `categories`, `rust-version`).

### Changed

- Renamed from `tpt-canvas` to `tpt-keystone-canvas` ahead of crates.io publication. The library name is
  now `tpt_canvas`.
- Licensed under `MIT OR Apache-2.0`.
- Reformatted to the workspace's `rustfmt` style.

### Known limits

- Canvas2D is the default renderer. WebGPU is a proof of concept with one hard-coded colour and no text.
- The WebGPU path has not been exercised in a real browser in this repository.
- Live queries re-fetch the full result on each topic message; they do not patch rows.
