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
//! - `gpu`: GPU-accelerated broad-phase spatial join primitives (`wgpu`
//!   compute shaders) — bbox-vs-bbox overlap (`ST_Intersects` joins) and
//!   bbox-centroid-vs-point radius (`ST_DWithin` joins), wired into
//!   `executor::apply_join`'s nested-loop fallback above a row-count
//!   threshold. Broad-phase only (matches the CPU path's existing
//!   bbox-only `ST_Intersects` precision, not a new limitation) with a
//!   documented `f32` precision narrowing vs. the CPU path's `f64`. Always
//!   fails safe to the existing CPU nested-loop join — GPU unavailability,
//!   `TPT_DISABLE_GPU_JOIN`, an oversized batch, or a runtime GPU error all
//!   fall back rather than erroring the query. Verified against an NVIDIA
//!   adapter in this dev environment; not verified on other vendors/drivers
//!   or in a headless CI environment.
//!
//! - `raster`: a single-band `f64` raster type sharing `Geometry`'s "hex text
//!   via `Value::Text`" storage precedent, plus `ST_AsRaster` rasterization
//!   built on the same `point_in_polygon` test vector queries already use —
//!   see `raster`'s module docs for exact scope (single band, no image
//!   import, no raster algebra).

pub mod geometry;
pub mod gpu;
pub mod h3;
pub mod raster;
pub mod s2;
