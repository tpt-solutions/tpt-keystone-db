//! TPT Meridian — geospatial engine, implemented as a native module inside
//! Keystone (per `2meridianspec.txt`) rather than a separate crate/process.
//!
//! Scope actually implemented in this pass:
//! - `geometry`: hand-written computational geometry (WKT in/out, distance,
//!   bounding boxes, point-in-polygon) — no GEOS/C++ bindings.
//! - `s2`: an S2-*inspired* hierarchical cell index (face + Hilbert-curve
//!   quadtree subdivision to a 64-bit cell id). This is a from-scratch
//!   approximation of the *idea* of Google's S2 (hierarchical, roughly
//!   equal-area cells, prefix-comparable ids) — it is not bit-compatible
//!   with the real S2 library's face/UV projection or cell id encoding.
//! - `h3`: an H3-*inspired* hexagonal grid (hex-binned lat/lon at a
//!   resolution level, axial coordinates packed into a u64 cell id, with a
//!   6-neighbor ring). Same caveat: approximates Uber H3's hierarchical-hex
//!   *model*, not its exact icosahedral projection or id bit layout.
//! - `index`: a local (per-node, not object-store-replicated — same
//!   documented scope cut as `storage::btree`'s B-Tree indexes) spatial
//!   index keyed by S2-inspired cell id, with a composite time dimension so
//!   a combined "near this point AND within this time range" query is a
//!   single index scan (`storage/geo_index.rs`).
//!
//! Explicitly NOT implemented (documented scope cuts, tracked in
//! `TODO.md`): GPU-accelerated spatial joins (wgpu compute shaders — would
//! need a GPU-present environment to develop/verify, not available in this
//! sandbox) and a unified raster+vector storage model. Both are left
//! unchecked in `TODO.md` rather than stubbed out and claimed done.

pub mod geometry;
pub mod h3;
pub mod s2;
