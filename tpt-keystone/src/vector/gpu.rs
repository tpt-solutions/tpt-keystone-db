//! GPU-accelerated batch vector similarity (Prism). Computes the full
//! query×base distance matrix on the GPU as a WGSL compute shader
//! (`shaders/similarity.wgsl`), one invocation per `(query, base)` pair —
//! a genuine batch workload (matmul-shaped) rather than the per-pair scalar
//! loops the CPU path uses.
//!
//! This is the GPU offload for "batch similarity" from Phase 7: instead of
//! the CPU computing `O(queries * base * dim)` distance scalar products on
//! the host, the whole matrix is uploaded once and reduced on the device in
//! a single dispatch. The CPU then does a cheap top-k selection over the
//! returned matrix (top-k on GPU needs shared-memory bookkeeping that isn't
//! worth it for the modest `k` vector search uses) — see
//! `gpu_brute_force_knn`.
//!
//! Every public function is fail-safe-to-CPU: GPU unavailability,
//! disablement, an oversized batch, or any runtime error returns `Err`
//! rather than panicking; callers are responsible for falling back to the
//! existing CPU path. GPU is strictly a performance path here, never a
//! correctness dependency.
//!
//! Same honest discipline as Meridian's `geo::gpu` (which this module is
//! patterned on): the CPU path this mirrors (`vector::vector`) is already
//! `f32`, so this is an exact-precision match, not a precision narrowing.
//! The device is only probed once per process and cached in a `OnceLock`;
//! any uncaptured wgpu error poisons it so the rest of the process falls
//! back to CPU rather than repeatedly re-attempting a broken device.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use anyhow::{anyhow, bail, Result};
use bytemuck::{Pod, Zeroable};
use wgpu::util::DeviceExt;

use crate::vector::hnsw::Metric;

const SHADER_SRC: &str = include_str!("shaders/similarity.wgsl");

/// Env var: any value other than unset/"0"/"false" (case-insensitive)
/// disables GPU vector similarity entirely, without ever probing for an
/// adapter. Mirrors the `TPT_*` env var convention in `storage/config.rs`.
const DISABLE_ENV: &str = "TPT_DISABLE_GPU_VECTOR";

/// Env var overriding the default max (query × base) pair count a single GPU
/// dispatch will attempt before refusing (returning `Err`) rather than
/// risking an oversized allocation or an out-of-range dispatch dimension.
const MAX_PAIRS_ENV: &str = "TPT_GPU_VECTOR_MAX";
const DEFAULT_MAX_PAIRS: u64 = 256_000_000;

/// Max workgroups in any one dimension wgpu allows (`max_compute_workgroups_per_dimension`).
/// Used to refuse batches whose row count would overflow a single dispatch and
/// fall back to CPU rather than submit an invalid (panic-on-validation) pass.
const MAX_DISPATCH_DIM: u32 = 65535;

fn gpu_disabled() -> bool {
    match std::env::var(DISABLE_ENV) {
        Ok(v) => !(v.is_empty() || v == "0" || v.eq_ignore_ascii_case("false")),
        Err(_) => false,
    }
}

fn max_pairs() -> u64 {
    std::env::var(MAX_PAIRS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_PAIRS)
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
struct Params {
    num_queries: u32,
    num_base: u32,
    dim: u32,
    metric: u32, // 0 = L2, 1 = cosine distance
    _pad0: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

struct GpuContext {
    device: wgpu::Device,
    queue: wgpu::Queue,
    pipeline: wgpu::ComputePipeline,
    /// Set by the device's uncaptured-error callback on a runtime GPU error
    /// (e.g. device lost). Once poisoned, every subsequent call in this
    /// process falls back to CPU for the rest of the process's lifetime —
    /// wgpu doesn't support transparent device recovery, so no attempt is
    /// made to reinitialize.
    poisoned: AtomicBool,
}

impl GpuContext {
    fn is_usable(&self) -> bool {
        !self.poisoned.load(Ordering::Relaxed)
    }
}

fn init_gpu_context() -> Option<GpuContext> {
    let instance = wgpu::Instance::new(wgpu::InstanceDescriptor::default());

    let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
        power_preference: wgpu::PowerPreference::HighPerformance,
        compatible_surface: None,
        force_fallback_adapter: false,
    }));
    let adapter = match adapter {
        Some(a) => a,
        None => {
            tracing::warn!("GPU vector similarity unavailable: no wgpu adapter found; falling back to CPU batch similarity for this process");
            return None;
        }
    };

    let info = adapter.get_info();
    if info.device_type == wgpu::DeviceType::Cpu {
        tracing::warn!(
            "GPU vector similarity unavailable: only a software/CPU adapter was found ({}); falling back to CPU batch similarity for this process",
            info.name
        );
        return None;
    }
    tracing::info!(
        "GPU vector similarity: using adapter {} ({:?}, backend {:?})",
        info.name,
        info.device_type,
        info.backend
    );

    let device_result = pollster::block_on(adapter.request_device(
        &wgpu::DeviceDescriptor {
            label: Some("tpt-prism-gpu-vector"),
            required_features: wgpu::Features::empty(),
            required_limits: wgpu::Limits::downlevel_defaults(),
            memory_hints: wgpu::MemoryHints::default(),
        },
        None,
    ));
    let (device, queue) = match device_result {
        Ok(pair) => pair,
        Err(e) => {
            tracing::warn!("GPU vector similarity unavailable: device request failed: {e}; falling back to CPU batch similarity for this process");
            return None;
        }
    };

    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("similarity.wgsl"),
        source: wgpu::ShaderSource::Wgsl(SHADER_SRC.into()),
    });

    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("similarity"),
        layout: None,
        module: &shader,
        entry_point: "main",
        compilation_options: Default::default(),
        cache: None,
    });

    device.on_uncaptured_error(Box::new(|e| {
        tracing::warn!("GPU vector similarity: uncaptured wgpu error, GPU path disabled for remainder of process: {e}");
    }));

    Some(GpuContext {
        device,
        queue,
        pipeline,
        poisoned: AtomicBool::new(false),
    })
}

fn gpu_context() -> Option<&'static GpuContext> {
    static CTX: OnceLock<Option<GpuContext>> = OnceLock::new();
    CTX.get_or_init(init_gpu_context)
        .as_ref()
        .filter(|ctx| ctx.is_usable())
}

/// Whether the GPU batch-similarity path is actually usable right now
/// (adapter present, not a software adapter, not disabled). Cheap to call —
/// the adapter is probed at most once per process and cached. Callers use
/// this to decide whether to route a query through the GPU path at all,
/// preserving a CPU-only fallback contract when it returns `false`.
pub fn gpu_available() -> bool {
    if gpu_disabled() {
        return false;
    }
    gpu_context().is_some()
}

/// Computes the full `num_queries × num_base` distance matrix on the GPU.
/// `queries`/`base` are flat `[f32]` row-major buffers (`num_queries * dim`
/// and `num_base * dim` respectively); the returned `Vec<f32>` is the
/// row-major distance matrix `out[q * num_base + b]`. `Err` means the GPU
/// path could not run (unavailable, disabled, dimension mismatch, batch too
/// large, or a dispatch failure) — the caller must fall back to the CPU
/// path in that case.
pub fn gpu_batch_similarity(
    queries: &[f32],
    base: &[f32],
    dim: usize,
    metric: Metric,
) -> Result<Vec<f32>> {
    if gpu_disabled() {
        bail!("GPU vector similarity disabled via {DISABLE_ENV}");
    }
    let ctx = gpu_context().ok_or_else(|| anyhow!("GPU unavailable for vector similarity"))?;

    if dim == 0 {
        bail!("vectors must be non-empty");
    }
    if queries.is_empty() || base.is_empty() {
        return Ok(Vec::new());
    }
    if queries.len() % dim != 0 || base.len() % dim != 0 {
        bail!(
            "vector count is not a multiple of dim {dim} (queries len {}, base len {})",
            queries.len(),
            base.len()
        );
    }
    let num_queries = queries.len() / dim;
    let num_base = base.len() / dim;

    let pair_count = (num_queries as u64)
        .checked_mul(num_base as u64)
        .ok_or_else(|| anyhow!("GPU vector similarity pair count overflowed"))?;
    if pair_count > max_pairs() {
        bail!(
            "GPU vector batch too large ({pair_count} pairs > {} cap)",
            max_pairs()
        );
    }

    // wgpu rejects (panic-on-validation) dispatches whose any dimension
    // exceeds MAX_DISPATCH_DIM; refuse those and let the caller fall back
    // to CPU rather than submitting an invalid pass.
    let wg_x = num_queries.div_ceil(8) as u32;
    let wg_y = num_base.div_ceil(8) as u32;
    if wg_x == 0 || wg_y == 0 || wg_x > MAX_DISPATCH_DIM || wg_y > MAX_DISPATCH_DIM {
        bail!(
            "GPU vector batch dispatch dimension overflow ({wg_x} x {wg_y} workgroups); fall back to CPU"
        );
    }

    let device = &ctx.device;
    let queue = &ctx.queue;
    let metric_u = match metric {
        Metric::L2 => 0u32,
        Metric::Cosine => 1u32,
    };

    let query_buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("queries"),
        contents: bytemuck::cast_slice(queries),
        usage: wgpu::BufferUsages::STORAGE,
    });
    let base_buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("base"),
        contents: bytemuck::cast_slice(base),
        usage: wgpu::BufferUsages::STORAGE,
    });
    let params = Params {
        num_queries: num_queries as u32,
        num_base: num_base as u32,
        dim: dim as u32,
        metric: metric_u,
        _pad0: 0,
        _pad1: 0,
        _pad2: 0,
        _pad3: 0,
    };
    let params_buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("params"),
        contents: bytemuck::bytes_of(&params),
        usage: wgpu::BufferUsages::UNIFORM,
    });

    let result_len = pair_count as usize;
    let result_byte_len = (result_len * std::mem::size_of::<f32>()) as u64;
    let result_buf = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("result"),
        size: result_byte_len,
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    });
    let staging_buf = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("staging"),
        size: result_byte_len,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });

    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("similarity_bind_group"),
        layout: &ctx.pipeline.get_bind_group_layout(0),
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: params_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: query_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: base_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 3,
                resource: result_buf.as_entire_binding(),
            },
        ],
    });

    let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
        label: Some("similarity_encoder"),
    });
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("similarity_pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&ctx.pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        pass.dispatch_workgroups(wg_x, wg_y, 1);
    }
    encoder.copy_buffer_to_buffer(&result_buf, 0, &staging_buf, 0, result_byte_len);
    queue.submit(Some(encoder.finish()));

    let slice = staging_buf.slice(..);
    let (tx, rx) = std::sync::mpsc::channel();
    slice.map_async(wgpu::MapMode::Read, move |res| {
        let _ = tx.send(res);
    });
    device.poll(wgpu::Maintain::Wait);
    rx.recv()
        .map_err(|e| anyhow!("GPU result readback channel closed: {e}"))?
        .map_err(|e| anyhow!("GPU result buffer map failed: {e}"))?;

    let data = slice.get_mapped_range();
    let out: Vec<f32> = bytemuck::cast_slice(&data).to_vec();
    drop(data);
    staging_buf.unmap();

    Ok(out)
}

/// Convenience single-query brute-force k-NN: uploads `query` (one row) and
/// `base` (row-major) to the GPU, returns the `(base_index, distance)` pairs
/// of the `k` nearest base vectors sorted nearest-first. The GPU computes
/// the distance array; this function does the top-k selection on the host
/// (cheap for the modest `k` vector search uses).
pub fn gpu_brute_force_knn(
    query: &[f32],
    base: &[f32],
    dim: usize,
    metric: Metric,
    k: usize,
) -> Result<Vec<(u32, f32)>> {
    let dist = gpu_batch_similarity(query, base, dim, metric)?;
    let mut scored: Vec<(u32, f32)> = (0..dist.len() as u32).map(|i| (i, dist[i as usize])).collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(k);
    Ok(scored)
}

/// Serializes every `TPT_TEST_GPU`-gated test that touches `TPT_DISABLE_GPU_VECTOR`
/// (this module's unit tests and `executor::prism_gpu_tests`, compiled into
/// the same test binary via `main.rs`, since `cargo test` runs tests in
/// parallel threads by default and these env vars are process-wide).
/// `pub(crate)` so `prism_gpu_tests.rs` shares the same lock rather than
/// racing on a second, independent one.
#[cfg(test)]
pub(crate) static GPU_ENV_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    // Real GPU hardware isn't available on every machine that runs `cargo
    // test` (CI, other contributors' laptops), so these are gated behind an
    // explicit opt-in env var rather than `#[ignore]` (this repo's existing
    // convention: zero `#[ignore]` usage anywhere, `TPT_*` env vars used for
    // this kind of environment-dependent gating instead — see
    // `storage/config.rs`). Run with `TPT_TEST_GPU=1 cargo test gpu::`.
    fn gpu_tests_enabled() -> bool {
        std::env::var("TPT_TEST_GPU").is_ok()
    }

    #[test]
    fn batch_l2_matches_cpu() {
        if !gpu_tests_enabled() {
            return;
        }
        let _guard = GPU_ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let queries = [1.0f32, 0.0, 0.0, 0.0, 1.0, 0.0];
        let base = [
            1.0f32, 0.0, 0.0, // identical to q0 -> dist 0
            0.0f32, 0.0, 0.0, // dist 1 from q0
            0.0f32, 1.0, 0.0, // identical to q1 -> dist 0
            2.0f32, 0.0, 0.0, // dist 1 from q0
        ];
        let got = gpu_batch_similarity(&queries, &base, 3, Metric::L2).unwrap();
        // q0 vs base: [0, 1, sqrt(2), 1]
        assert!((got[0] - 0.0).abs() < 1e-4);
        assert!((got[1] - 1.0).abs() < 1e-4);
        assert!((got[2] - 2.0f32.sqrt()).abs() < 1e-4);
        assert!((got[3] - 1.0).abs() < 1e-4);
        // q1 vs base: [sqrt(2), 1, 0, sqrt(5)]
        assert!((got[4] - 2.0f32.sqrt()).abs() < 1e-4);
        assert!((got[5] - 1.0).abs() < 1e-4);
        assert!((got[6] - 0.0).abs() < 1e-4);
        assert!((got[7] - 5.0f32.sqrt()).abs() < 1e-4);
    }

    #[test]
    fn batch_cosine_matches_cpu() {
        if !gpu_tests_enabled() {
            return;
        }
        let _guard = GPU_ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let queries = [1.0f32, 0.0, 0.0, 1.0];
        let base = [
            1.0f32, 0.0, // identical to q0 -> cos dist 0
            0.0f32, 1.0, // orthogonal to q0 -> cos dist 1
            0.0f32, -1.0, // opposite to q1 -> cos dist 2
        ];
        let got = gpu_batch_similarity(&queries, &base, 2, Metric::Cosine).unwrap();
        // row-major: out[q * num_base + b]
        assert!((got[0] - 0.0).abs() < 1e-4); // q0 vs [1,0]  -> 0
        assert!((got[1] - 1.0).abs() < 1e-4); // q0 vs [0,1]  -> 1
        assert!((got[2] - 1.0).abs() < 1e-4); // q0 vs [0,-1] -> 1 (orthogonal)
        assert!((got[3] - 1.0).abs() < 1e-4); // q1 vs [1,0]  -> 1
        assert!((got[4] - 0.0).abs() < 1e-4); // q1 vs [0,1]  -> 0
        assert!((got[5] - 2.0).abs() < 1e-4); // q1 vs [0,-1] -> 2 (opposite)
    }

    #[test]
    fn knn_returns_nearest_k() {
        if !gpu_tests_enabled() {
            return;
        }
        let _guard = GPU_ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let query = [0.0f32, 0.0];
        let base = [3.0f32, 4.0, 1.0, 0.0, 0.0, 1.0, 2.0, 2.0];
        let mut got = gpu_brute_force_knn(&query, &base, 2, Metric::L2, 2).unwrap();
        got.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        // nearest two base vectors to origin: (1,0)->1.0, (0,1)->1.0
        assert_eq!(got.len(), 2);
        assert!((got[0].1 - 1.0).abs() < 1e-4);
        assert!((got[1].1 - 1.0).abs() < 1e-4);
    }

    #[test]
    fn disabled_env_var_forces_error() {
        if !gpu_tests_enabled() {
            return;
        }
        let _guard = GPU_ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var(DISABLE_ENV, "1");
        let result = gpu_batch_similarity(&[1.0f32, 0.0], &[0.0f32, 1.0], 2, Metric::L2);
        std::env::remove_var(DISABLE_ENV);
        assert!(result.is_err());
    }
}
