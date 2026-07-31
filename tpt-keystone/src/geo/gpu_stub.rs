//! Stand-in for `geo::gpu` when the `gpu` Cargo feature is disabled at
//! compile time (`wgpu`/`bytemuck` aren't in this build's dependency graph
//! at all). Mirrors the real module's public API and its fail-safe contract:
//! every function behaves as "GPU unavailable", the same path
//! `try_gpu_spatial_join` already takes at runtime when no adapter is found
//! or the row-pair count is below the GPU threshold — no behavior change,
//! just a build that never has GPU broad-phase joins available.

use anyhow::{bail, Result};

#[derive(Debug, Clone, Copy)]
pub struct GpuBBox {
    pub min_x: f32,
    pub min_y: f32,
    pub max_x: f32,
    pub max_y: f32,
}

#[derive(Debug, Clone, Copy)]
pub struct GpuPoint {
    pub x: f32,
    pub y: f32,
}

pub fn gpu_bbox_overlap_pairs(_left: &[GpuBBox], _right: &[GpuBBox]) -> Result<Vec<(u32, u32)>> {
    bail!("GPU spatial join was not compiled into this build (rebuild with `--features gpu`)")
}

pub fn gpu_dwithin_pairs(
    _left: &[GpuPoint],
    _right: &[GpuPoint],
    _radius_m: f32,
) -> Result<Vec<(u32, u32)>> {
    bail!("GPU spatial join was not compiled into this build (rebuild with `--features gpu`)")
}
