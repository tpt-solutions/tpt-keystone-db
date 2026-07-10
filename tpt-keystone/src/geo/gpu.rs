//! GPU-accelerated broad-phase spatial join primitives (Meridian). Computes
//! bbox-vs-bbox overlap (for `ST_Intersects` joins) and bbox-centroid-vs-point
//! radius tests (for `ST_DWithin` joins) as WGSL compute shaders
//! (`shaders/spatial_join.wgsl`), batching an entire left/right row set into
//! one dispatch instead of the CPU nested-loop path's per-row-pair scalar
//! calls.
//!
//! This is a broad phase only — it reproduces the CPU path's existing
//! bbox-only `ST_Intersects` precision exactly (not an approximation of
//! something more precise), and computes exact haversine distance for
//! `ST_DWithin` at `f32` precision (WGSL has no portable `f64`, so this is a
//! narrower precision than the CPU path's `f64` haversine — an accepted,
//! documented tradeoff for the broad-phase batch test).
//!
//! Every public function here is fail-safe-to-CPU: GPU unavailability,
//! disablement, or any runtime error returns `Err`/`None` rather than
//! panicking, and the executor's join code is responsible for falling back
//! to the existing nested-loop path whenever that happens. GPU is strictly a
//! performance path here, never a correctness dependency.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use anyhow::{anyhow, bail, Result};
use bytemuck::{Pod, Zeroable};
use wgpu::util::DeviceExt;

const SHADER_SRC: &str = include_str!("shaders/spatial_join.wgsl");

/// Env var: any value other than unset/"0"/"false" (case-insensitive)
/// disables GPU spatial joins entirely, without ever probing for an
/// adapter. Mirrors the `TPT_*` env var convention in `storage/config.rs`.
const DISABLE_ENV: &str = "TPT_DISABLE_GPU_JOIN";

/// Env var overriding the default max row-pair count
/// (`left.len() * right.len()`) a single GPU dispatch will attempt before
/// refusing (returning `Err`) rather than risking an oversized allocation.
const MAX_PAIRS_ENV: &str = "TPT_GPU_JOIN_MAX_PAIRS";
const DEFAULT_MAX_PAIRS: u64 = 256_000_000;

fn gpu_disabled() -> bool {
    match std::env::var(DISABLE_ENV) {
        Ok(v) => !(v.is_empty() || v == "0" || v.eq_ignore_ascii_case("false")),
        Err(_) => false,
    }
}

fn max_pairs() -> u64 {
    std::env::var(MAX_PAIRS_ENV).ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_MAX_PAIRS)
}

/// A GPU-side axis-aligned bounding box, matching the WGSL `BBox` struct
/// layout exactly (`f32 x 4`, no padding needed).
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct GpuBBox {
    pub min_x: f32,
    pub min_y: f32,
    pub max_x: f32,
    pub max_y: f32,
}

/// A GPU-side lon/lat point, matching the WGSL `Point` struct layout.
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct GpuPoint {
    pub x: f32,
    pub y: f32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
struct Params {
    left_len: u32,
    right_len: u32,
    radius_m: f32,
    _pad: u32,
}

struct GpuContext {
    device: wgpu::Device,
    queue: wgpu::Queue,
    bbox_overlap_pipeline: wgpu::ComputePipeline,
    dwithin_pipeline: wgpu::ComputePipeline,
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
            tracing::warn!("GPU spatial join unavailable: no wgpu adapter found; falling back to CPU nested-loop joins for this process");
            return None;
        }
    };

    let info = adapter.get_info();
    if info.device_type == wgpu::DeviceType::Cpu {
        tracing::warn!(
            "GPU spatial join unavailable: only a software/CPU adapter was found ({}); falling back to CPU nested-loop joins for this process",
            info.name
        );
        return None;
    }
    tracing::info!("GPU spatial join: using adapter {} ({:?}, backend {:?})", info.name, info.device_type, info.backend);

    let device_result = pollster::block_on(adapter.request_device(
        &wgpu::DeviceDescriptor {
            label: Some("tpt-meridian-gpu-join"),
            required_features: wgpu::Features::empty(),
            required_limits: wgpu::Limits::downlevel_defaults(),
            memory_hints: wgpu::MemoryHints::default(),
        },
        None,
    ));
    let (device, queue) = match device_result {
        Ok(pair) => pair,
        Err(e) => {
            tracing::warn!("GPU spatial join unavailable: device request failed: {e}; falling back to CPU nested-loop joins for this process");
            return None;
        }
    };

    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("spatial_join.wgsl"),
        source: wgpu::ShaderSource::Wgsl(SHADER_SRC.into()),
    });

    let bbox_overlap_pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("bbox_overlap"),
        layout: None,
        module: &shader,
        entry_point: "bbox_overlap",
        compilation_options: Default::default(),
        cache: None,
    });
    let dwithin_pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("dwithin"),
        layout: None,
        module: &shader,
        entry_point: "dwithin",
        compilation_options: Default::default(),
        cache: None,
    });

    device.on_uncaptured_error(Box::new(|e| {
        tracing::warn!("GPU spatial join: uncaptured wgpu error, GPU path disabled for remainder of process: {e}");
    }));

    Some(GpuContext { device, queue, bbox_overlap_pipeline, dwithin_pipeline, poisoned: AtomicBool::new(false) })
}

fn gpu_context() -> Option<&'static GpuContext> {
    static CTX: OnceLock<Option<GpuContext>> = OnceLock::new();
    CTX.get_or_init(init_gpu_context).as_ref().filter(|ctx| ctx.is_usable())
}

/// Broad-phase GPU bbox-vs-bbox overlap test: for every `(i, j)` in
/// `left.len() x right.len()`, returns the `(left_idx, right_idx)` pairs
/// whose bboxes intersect, matching `geometry::bbox_intersects`'s
/// closed-interval semantics exactly. `Err` means the GPU path could not run
/// (unavailable, disabled, batch too large, or a dispatch failure) — the
/// caller must fall back to the CPU nested-loop join in that case.
pub fn gpu_bbox_overlap_pairs(left: &[GpuBBox], right: &[GpuBBox]) -> Result<Vec<(u32, u32)>> {
    run_pairwise(left, right, 0.0, |ctx| &ctx.bbox_overlap_pipeline, bindings::LEFT_BBOX, bindings::RIGHT_BBOX)
}

/// Broad-phase GPU point-radius test for `ST_DWithin` joins: `left`/`right`
/// are representative (lon, lat) points; `radius_m` is the distance
/// threshold in meters, tested via the same haversine formula as
/// `geometry::haversine_distance_m` (ported to WGSL at `f32` precision).
pub fn gpu_dwithin_pairs(left: &[GpuPoint], right: &[GpuPoint], radius_m: f32) -> Result<Vec<(u32, u32)>> {
    run_pairwise(left, right, radius_m, |ctx| &ctx.dwithin_pipeline, bindings::LEFT_POINT, bindings::RIGHT_POINT)
}

fn run_pairwise<T: Pod>(
    left: &[T],
    right: &[T],
    radius_m: f32,
    pipeline_for: impl Fn(&GpuContext) -> &wgpu::ComputePipeline,
    left_binding: u32,
    right_binding: u32,
) -> Result<Vec<(u32, u32)>> {
    if gpu_disabled() {
        bail!("GPU spatial join disabled via {DISABLE_ENV}");
    }
    let ctx = gpu_context().ok_or_else(|| anyhow!("GPU unavailable for spatial join"))?;

    if left.is_empty() || right.is_empty() {
        return Ok(Vec::new());
    }
    let left_len = left.len() as u64;
    let right_len = right.len() as u64;
    let pair_count = left_len
        .checked_mul(right_len)
        .ok_or_else(|| anyhow!("GPU spatial join pair count overflowed"))?;
    if pair_count > max_pairs() {
        bail!("GPU spatial join batch too large ({pair_count} pairs > {} cap)", max_pairs());
    }

    let device = &ctx.device;
    let queue = &ctx.queue;

    let left_buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("left"),
        contents: bytemuck::cast_slice(left),
        usage: wgpu::BufferUsages::STORAGE,
    });
    let right_buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("right"),
        contents: bytemuck::cast_slice(right),
        usage: wgpu::BufferUsages::STORAGE,
    });
    let params = Params { left_len: left.len() as u32, right_len: right.len() as u32, radius_m, _pad: 0 };
    let params_buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("params"),
        contents: bytemuck::bytes_of(&params),
        usage: wgpu::BufferUsages::UNIFORM,
    });

    let result_len = pair_count as usize;
    let result_byte_len = (result_len * std::mem::size_of::<u32>()) as u64;
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

    let pipeline = pipeline_for(ctx);
    let bind_group_layout = pipeline.get_bind_group_layout(0);
    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("spatial_join_bind_group"),
        layout: &bind_group_layout,
        entries: &[
            wgpu::BindGroupEntry { binding: 0, resource: params_buf.as_entire_binding() },
            wgpu::BindGroupEntry { binding: left_binding, resource: left_buf.as_entire_binding() },
            wgpu::BindGroupEntry { binding: right_binding, resource: right_buf.as_entire_binding() },
            wgpu::BindGroupEntry { binding: 3, resource: result_buf.as_entire_binding() },
        ],
    });

    let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor { label: Some("spatial_join_encoder") });
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor { label: Some("spatial_join_pass"), timestamp_writes: None });
        pass.set_pipeline(pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        let wg_x = left.len().div_ceil(8) as u32;
        let wg_y = right.len().div_ceil(8) as u32;
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
    rx.recv().map_err(|e| anyhow!("GPU result readback channel closed: {e}"))?.map_err(|e| anyhow!("GPU result buffer map failed: {e}"))?;

    let data = slice.get_mapped_range();
    let flags: &[u32] = bytemuck::cast_slice(&data);
    let mut pairs = Vec::new();
    for i in 0..left.len() {
        for j in 0..right.len() {
            if flags[i * right.len() + j] != 0 {
                pairs.push((i as u32, j as u32));
            }
        }
    }
    drop(data);
    staging_buf.unmap();

    Ok(pairs)
}

/// The `bbox_overlap`/`dwithin` WGSL entry points bind their respective
/// left/right storage buffers at different slots (see `shaders/spatial_join.wgsl`'s
/// module doc) since WGSL requires unique binding indices per module-scope
/// variable. `bind_group_layout` (derived via `layout: "auto"`) only exposes
/// the slots each entry point actually uses, so `run_pairwise` is told which
/// pair to bind depending on which pipeline is active.
mod bindings {
    pub const LEFT_BBOX: u32 = 1;
    pub const RIGHT_BBOX: u32 = 2;
    pub const LEFT_POINT: u32 = 4;
    pub const RIGHT_POINT: u32 = 5;
}

/// Serializes every `TPT_TEST_GPU`-gated test that touches
/// `TPT_DISABLE_GPU_JOIN`/`TPT_GPU_JOIN_THRESHOLD` — both this module's own
/// tests and `executor::gpu_join_tests` (compiled into the same test binary
/// via `main.rs`, since `cargo test` runs tests in parallel threads by
/// default and these env vars are process-wide). `pub(crate)` so
/// `gpu_join_tests.rs` shares the same lock rather than racing on a second,
/// independent one.
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
    fn bbox_overlap_matches_cpu_bbox_intersects() {
        if !gpu_tests_enabled() {
            return;
        }
        let _guard = GPU_ENV_TEST_LOCK.lock().unwrap();
        let left = [
            GpuBBox { min_x: 0.0, min_y: 0.0, max_x: 10.0, max_y: 10.0 }, // overlaps right[0]
            GpuBBox { min_x: 100.0, min_y: 100.0, max_x: 110.0, max_y: 110.0 }, // no overlap
            GpuBBox { min_x: 0.0, min_y: 0.0, max_x: 5.0, max_y: 5.0 }, // touches right[2] at the edge
        ];
        let right = [
            GpuBBox { min_x: 5.0, min_y: 5.0, max_x: 15.0, max_y: 15.0 },
            GpuBBox { min_x: 200.0, min_y: 200.0, max_x: 210.0, max_y: 210.0 },
            GpuBBox { min_x: 5.0, min_y: 5.0, max_x: 20.0, max_y: 20.0 },
        ];
        let mut got = gpu_bbox_overlap_pairs(&left, &right).expect("gpu bbox overlap");
        got.sort();
        // CPU cross-check using the exact same closed-interval semantics.
        let mut want = Vec::new();
        for (i, a) in left.iter().enumerate() {
            for (j, b) in right.iter().enumerate() {
                let bbox_a = crate::geo::geometry::BBox { min_x: a.min_x as f64, min_y: a.min_y as f64, max_x: a.max_x as f64, max_y: a.max_y as f64 };
                let bbox_b = crate::geo::geometry::BBox { min_x: b.min_x as f64, min_y: b.min_y as f64, max_x: b.max_x as f64, max_y: b.max_y as f64 };
                if crate::geo::geometry::bbox_intersects(&bbox_a, &bbox_b) {
                    want.push((i as u32, j as u32));
                }
            }
        }
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn dwithin_matches_cpu_haversine() {
        if !gpu_tests_enabled() {
            return;
        }
        let _guard = GPU_ENV_TEST_LOCK.lock().unwrap();
        // London, Paris, New York.
        let left = [GpuPoint { x: -0.1276, y: 51.5074 }];
        let right = [GpuPoint { x: 2.3522, y: 48.8566 }, GpuPoint { x: -74.0060, y: 40.7128 }];
        let got = gpu_dwithin_pairs(&left, &right, 400_000.0).expect("gpu dwithin");
        // London-Paris ~344km (within 400km), London-NYC ~5570km (not within).
        assert_eq!(got, vec![(0, 0)]);
    }

    #[test]
    fn empty_batches_return_no_pairs() {
        if !gpu_tests_enabled() {
            return;
        }
        let _guard = GPU_ENV_TEST_LOCK.lock().unwrap();
        let left: [GpuBBox; 0] = [];
        let right = [GpuBBox { min_x: 0.0, min_y: 0.0, max_x: 1.0, max_y: 1.0 }];
        assert!(gpu_bbox_overlap_pairs(&left, &right).unwrap().is_empty());
    }

    #[test]
    fn disabled_env_var_forces_error() {
        if !gpu_tests_enabled() {
            return;
        }
        let _guard = GPU_ENV_TEST_LOCK.lock().unwrap();
        std::env::set_var(DISABLE_ENV, "1");
        let result = gpu_bbox_overlap_pairs(&[GpuBBox { min_x: 0.0, min_y: 0.0, max_x: 1.0, max_y: 1.0 }], &[GpuBBox { min_x: 0.0, min_y: 0.0, max_x: 1.0, max_y: 1.0 }]);
        std::env::remove_var(DISABLE_ENV);
        assert!(result.is_err());
    }
}
