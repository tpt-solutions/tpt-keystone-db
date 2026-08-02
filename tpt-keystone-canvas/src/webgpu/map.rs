//! `CanvasMap`'s WebGPU renderer (Phase 10) — a single fixed-color quad
//! marker pipeline. `CanvasMap`'s clustering and heatmap modes stay
//! Canvas2D-only (`components/map.rs` forces Canvas2D whenever `heatmap` is
//! set, and cluster markers still use variable radii/counts this PoC doesn't
//! attempt); only plain per-point markers get a WebGPU path. See
//! `webgpu/mod.rs` module docs for the shared scope cuts.

use web_sys::{
    GpuAutoLayoutMode, GpuCanvasContext, GpuColorTargetState, GpuDevice, GpuFragmentState,
    GpuPrimitiveState, GpuPrimitiveTopology, GpuQueue, GpuRenderPipeline,
    GpuRenderPipelineDescriptor, GpuShaderModuleDescriptor, GpuTextureFormat, GpuVertexAttribute,
    GpuVertexBufferLayout, GpuVertexFormat, GpuVertexState,
};

use js_sys::JsOption;

use super::{begin_pass, end_and_submit, upload_vertices, GpuContext};
use crate::view_transform::{normalized_to_clip, quad_vertices_clip};

/// Solid `#2563eb` marker fill, matching `components/map.rs`'s Canvas2D
/// point-marker color (`"rgba(37,99,235,0.9)"`) — alpha isn't meaningfully
/// applied here since the pipeline sets up no blend state (see
/// `webgpu/mod.rs`'s "no per-instance uniforms" scope cut), same
/// simplification `timeseries.rs`'s hardcoded stroke color already makes.
const WGSL_SHADER: &str = r#"
@vertex
fn vs_main(@location(0) pos: vec2<f32>) -> @builtin(position) vec4<f32> {
    return vec4<f32>(pos, 0.0, 1.0);
}

@fragment
fn fs_main() -> @location(0) vec4<f32> {
    return vec4<f32>(0.145, 0.388, 0.922, 0.9); // #2563eb
}
"#;

/// An initialized WebGPU renderer for one `<canvas>`. Cheap to redraw
/// (`draw_markers_normalized`) — the pipeline/shader/device are set up once
/// in `try_init`; each redraw only re-uploads one vertex buffer (all markers
/// CPU-expanded into triangles) and re-encodes one render pass.
pub struct GpuMapRenderer {
    device: GpuDevice,
    queue: GpuQueue,
    context: GpuCanvasContext,
    pipeline: GpuRenderPipeline,
    width: f64,
    height: f64,
}

impl GpuMapRenderer {
    /// Negotiates WebGPU against `<canvas id="{element_id}">` and builds the
    /// marker render pipeline. Returns `None` on any failure — see
    /// `GpuContext::negotiate`.
    pub async fn try_init(element_id: &str) -> Option<Self> {
        let ctx = GpuContext::negotiate(element_id).await?;
        let pipeline = build_pipeline(&ctx.device, ctx.format);
        Some(Self {
            device: ctx.device,
            queue: ctx.queue,
            context: ctx.context,
            pipeline,
            width: ctx.width,
            height: ctx.height,
        })
    }

    /// Draws one fixed-size quad marker per point in `points` (`[0,1]x[0,1]`
    /// normalized space, same convention as
    /// `timeseries::GpuRenderer::draw_line_strip_normalized`), `radius_px`
    /// CSS pixels wide, converted to clip-space half-extents using this
    /// canvas's pixel dimensions (captured at `try_init` time) so markers
    /// stay a consistent on-screen size regardless of canvas aspect ratio.
    /// No-op if `points` is empty or this canvas has zero size.
    pub fn draw_markers_normalized(&self, points: &[(f64, f64)], radius_px: f64) {
        if points.is_empty() || self.width <= 0.0 || self.height <= 0.0 {
            return;
        }
        let half_w = (radius_px * 2.0 / self.width) as f32;
        let half_h = (radius_px * 2.0 / self.height) as f32;

        let mut vertex_bytes: Vec<u8> = Vec::with_capacity(points.len() * 6 * 8);
        for &(x, y) in points {
            let (cx, cy) = normalized_to_clip(x, y);
            for (vx, vy) in quad_vertices_clip(cx, cy, half_w, half_h) {
                vertex_bytes.extend_from_slice(&vx.to_le_bytes());
                vertex_bytes.extend_from_slice(&vy.to_le_bytes());
            }
        }

        let Some(vertex_buffer) = upload_vertices(&self.device, &self.queue, &vertex_bytes) else {
            return;
        };
        let Some((encoder, pass)) = begin_pass(&self.device, &self.context) else {
            return;
        };
        pass.set_pipeline(&self.pipeline);
        pass.set_vertex_buffer(0, Some(&vertex_buffer));
        pass.draw((points.len() * 6) as u32);
        end_and_submit(&self.queue, encoder, pass);
    }
}

/// Builds the (fixed, reused-every-frame) marker render pipeline: one vertex
/// buffer of `vec2<f32>` positions already in clip space (CPU-expanded
/// quads), one hardcoded fragment color, no depth/stencil, no
/// multisampling, no bind groups/uniforms.
fn build_pipeline(device: &GpuDevice, format: GpuTextureFormat) -> GpuRenderPipeline {
    let shader = device.create_shader_module(&GpuShaderModuleDescriptor::new(WGSL_SHADER));

    let position_attr = GpuVertexAttribute::new(GpuVertexFormat::Float32x2, 0, 0);
    let vertex_layout = GpuVertexBufferLayout::new(8, &[position_attr]);

    let vertex_state = GpuVertexState::new(&shader);
    vertex_state.set_entry_point("vs_main");
    vertex_state.set_buffers(&[JsOption::wrap(vertex_layout)]);

    let color_target = GpuColorTargetState::new(format);
    let fragment_state = GpuFragmentState::new(&shader, &[JsOption::wrap(color_target)]);
    fragment_state.set_entry_point("fs_main");

    let primitive_state = GpuPrimitiveState::new();
    primitive_state.set_topology(GpuPrimitiveTopology::TriangleList);

    let pipeline_desc =
        GpuRenderPipelineDescriptor::new_with_gpu_auto_layout_mode(GpuAutoLayoutMode::Auto, &vertex_state);
    pipeline_desc.set_fragment(&fragment_state);
    pipeline_desc.set_primitive(&primitive_state);

    device
        .create_render_pipeline(&pipeline_desc)
        .expect("webgpu/map.rs: fixed marker pipeline descriptor should always be valid")
}
