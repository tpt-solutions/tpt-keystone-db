//! `CanvasTimeSeries`'s WebGPU renderer — the original PoC (see
//! `webgpu/mod.rs` module docs for full scope). Unchanged public API/behavior
//! from before the Phase 10 directory split: one `line-strip` draw call, no
//! point markers, no axis-label text, one hardcoded stroke color.

use web_sys::{
    GpuAutoLayoutMode, GpuCanvasContext, GpuColorTargetState, GpuDevice, GpuFragmentState,
    GpuPrimitiveState, GpuPrimitiveTopology, GpuQueue, GpuRenderPipeline,
    GpuRenderPipelineDescriptor, GpuShaderModuleDescriptor, GpuTextureFormat, GpuVertexAttribute,
    GpuVertexBufferLayout, GpuVertexFormat, GpuVertexState,
};

use js_sys::JsOption;

use super::{begin_pass, end_and_submit, upload_vertices, GpuContext};
use crate::view_transform::normalized_to_clip;

/// Vertex shader (clip-space passthrough) + fragment shader (hardcoded solid
/// color) — see `webgpu/mod.rs` module docs for why there's no uniform
/// buffer or per-series color.
const WGSL_SHADER: &str = r#"
@vertex
fn vs_main(@location(0) pos: vec2<f32>) -> @builtin(position) vec4<f32> {
    return vec4<f32>(pos, 0.0, 1.0);
}

@fragment
fn fs_main() -> @location(0) vec4<f32> {
    return vec4<f32>(0.145, 0.388, 0.922, 1.0); // #2563eb
}
"#;

/// An initialized WebGPU renderer for one `<canvas>`. Cheap to redraw
/// (`draw_line_strip_normalized`) — the pipeline/shader/device are set up
/// once in `try_init`; each redraw only re-uploads the vertex buffer and
/// re-encodes one render pass.
pub struct GpuRenderer {
    device: GpuDevice,
    queue: GpuQueue,
    context: GpuCanvasContext,
    pipeline: GpuRenderPipeline,
}

impl GpuRenderer {
    /// Negotiates WebGPU against `<canvas id="{element_id}">` and builds the
    /// line-strip render pipeline. Returns `None` on any failure — see
    /// `GpuContext::negotiate`.
    pub async fn try_init(element_id: &str) -> Option<Self> {
        let ctx = GpuContext::negotiate(element_id).await?;
        let pipeline = build_pipeline(&ctx.device, ctx.format);
        Some(Self {
            device: ctx.device,
            queue: ctx.queue,
            context: ctx.context,
            pipeline,
        })
    }

    /// Redraws the whole canvas as a single `line-strip` through `points`,
    /// each in `[0,1] x [0,1]` normalized space (`(0,0)` top-left, `(1,1)`
    /// bottom-right — same convention as `render.rs`'s pixel space, just
    /// resolution-independent since callers here don't have easy access to
    /// this canvas's real pixel dimensions). A no-op if `points` has fewer
    /// than 2 entries (a line-strip needs at least 2 vertices).
    pub fn draw_line_strip_normalized(&self, points: &[(f64, f64)]) {
        if points.len() < 2 {
            return;
        }

        let mut vertex_bytes: Vec<u8> = Vec::with_capacity(points.len() * 8);
        for &(x, y) in points {
            let (cx, cy) = normalized_to_clip(x, y);
            vertex_bytes.extend_from_slice(&cx.to_le_bytes());
            vertex_bytes.extend_from_slice(&cy.to_le_bytes());
        }

        let Some(vertex_buffer) = upload_vertices(&self.device, &self.queue, &vertex_bytes) else {
            return;
        };
        let Some((encoder, pass)) = begin_pass(&self.device, &self.context) else {
            return;
        };
        pass.set_pipeline(&self.pipeline);
        pass.set_vertex_buffer(0, Some(&vertex_buffer));
        pass.draw(points.len() as u32);
        end_and_submit(&self.queue, encoder, pass);
    }
}

/// Builds the (fixed, reused-every-frame) line-strip render pipeline: one
/// vertex buffer of `vec2<f32>` positions already in clip space, one
/// hardcoded fragment color, no depth/stencil, no multisampling, no bind
/// groups/uniforms (`GpuAutoLayoutMode::Auto` — nothing to bind here).
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
    primitive_state.set_topology(GpuPrimitiveTopology::LineStrip);

    let pipeline_desc =
        GpuRenderPipelineDescriptor::new_with_gpu_auto_layout_mode(GpuAutoLayoutMode::Auto, &vertex_state);
    pipeline_desc.set_fragment(&fragment_state);
    pipeline_desc.set_primitive(&primitive_state);

    // `create_render_pipeline` (synchronous variant) can only fail on a
    // malformed descriptor, which would be a bug in this fixed pipeline
    // setup above, not a runtime/environment condition — safe to expect.
    device
        .create_render_pipeline(&pipeline_desc)
        .expect("webgpu/timeseries.rs: fixed line-strip pipeline descriptor should always be valid")
}
