//! Stand-in for `vector::gpu` when the `gpu` Cargo feature is disabled at
//! compile time (`wgpu`/`pollster`/`bytemuck` aren't in this build's
//! dependency graph at all). Mirrors the real module's public API and its
//! fail-safe-to-CPU contract exactly: `gpu_available()` always reports
//! `false` and every other function always returns `Err`, so callers take
//! the same "no GPU adapter" fallback path they already handle at runtime
//! on a machine with no usable adapter — no behavior change, just a build
//! that never has GPU offload available.

use anyhow::{bail, Result};

use crate::vector::hnsw::Metric;

pub fn gpu_available() -> bool {
    false
}

pub fn gpu_batch_similarity(
    _queries: &[f32],
    _base: &[f32],
    _dim: usize,
    _metric: Metric,
) -> Result<Vec<f32>> {
    bail!("GPU vector similarity was not compiled into this build (rebuild with `--features gpu`)")
}

pub fn gpu_brute_force_knn(
    _query: &[f32],
    _base: &[f32],
    _dim: usize,
    _metric: Metric,
    _k: usize,
) -> Result<Vec<(u32, f32)>> {
    bail!("GPU vector similarity was not compiled into this build (rebuild with `--features gpu`)")
}

/// Mirrors `gpu::GPU_ENV_TEST_LOCK` so `executor::prism_tests` (compiled
/// regardless of the `gpu` feature) can import it unconditionally.
#[cfg(test)]
pub(crate) static GPU_ENV_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
