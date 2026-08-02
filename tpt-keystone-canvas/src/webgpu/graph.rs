//! `CanvasGraph`'s WebGPU renderer (Phase 10) — a `LineList` edge pipeline
//! plus a `TriangleList` node-quad pipeline, drawn in one render pass (two
//! `set_pipeline`/`draw` calls between one `begin_pass`/`end_and_submit`
//! pair). No node-id text — see `webgpu/mod.rs` module docs for the shared
//! scope cuts.

use web_sys::{
    GpuAutoLayoutMode, GpuCanvasContext, GpuColorTargetState, GpuDevice, GpuFragmentState,
    GpuPrimitiveState, GpuPrimitiveTopology, GpuQueue, GpuRenderPipeline,
    GpuRenderPipelineDescriptor, GpuShaderModuleDescriptor, GpuTextureFormat, GpuVertexAttribute,
    GpuVertexBufferLayout, GpuVertexFormat, GpuVertexState,
};

use js_sys::JsOption;

use super::{begin_pass, end_and_submit, upload_vertices, GpuContext};
use crate::view_transform::{normalized_to_clip, quad_vertices_clip};

/// Solid `#94a3b8` stroke, matching `components/graph.rs`'s Canvas2D edge
/// color.
const EDGE_SHADER: &str = r#"
@vertex
fn vs_main(@location(0) pos: vec2<f32>) -> @builtin(position) vec4<f32> {
    return vec4<f32>(pos, 0.0, 1.0);
}

@fragment
fn fs_main() -> @location(0) vec4<f32> {
    return vec4<f32>(0.580, 0.639, 0.722, 1.0); // #94a3b8
}
"#;

/// Solid `#16a34a` fill, matching `components/graph.rs`'s Canvas2D node
/// color.
const NODE_SHADER: &str = r#"
@vertex
fn vs_main(@location(0) pos: vec2<f32>) -> @builtin(position) vec4<f32> {
    return vec4<f32>(pos, 0.0, 1.0);
}

@fragment
fn fs_main() -> @location(0) vec4<f32> {
    return vec4<f32>(0.086, 0.639, 0.290, 1.0); // #16a34a
}
"#;

/// An initialized WebGPU renderer for one `<canvas>`. Cheap to redraw
/// (`draw_normalized`) — pipelines/shaders/device are set up once in
/// `try_init`; each redraw re-uploads two vertex buffers (edges, node
/// quads) and re-encodes one render pass covering both.
pub struct GpuGraphRenderer {
    device: GpuDevice,
    queue: GpuQueue,
    context: GpuCanvasContext,
    edge_pipeline: GpuRenderPipeline,
    node_pipeline: GpuRenderPipeline,
    width: f64,
    height: f64,
}

impl GpuGraphRenderer {
    /// Negotiates WebGPU against `<canvas id="{element_id}">` and builds
    /// both render pipelines. Returns `None` on any failure — see
    /// `GpuContext::negotiate`.
    pub async fn try_init(element_id: &str) -> Option<Self> {
        let ctx = GpuContext::negotiate(element_id).await?;
        let edge_pipeline = build_pipeline(&ctx.device, ctx.format, EDGE_SHADER, GpuPrimitiveTopology::LineList);
        let node_pipeline = build_pipeline(&ctx.device, ctx.format, NODE_SHADER, GpuPrimitiveTopology::TriangleList);
        Some(Self {
            device: ctx.device,
            queue: ctx.queue,
            context: ctx.context,
            edge_pipeline,
            node_pipeline,
            width: ctx.width,
            height: ctx.height,
        })
    }

    /// Draws every edge as a line segment and every node as a fixed-size
    /// quad. `nodes` are node centers in `[0,1]x[0,1]` normalized space
    /// (same convention as `timeseries::GpuRenderer::draw_line_strip_normalized`);
    /// `edges` are `(from, to)` index pairs into `nodes` (an out-of-range
    /// index is silently skipped rather than panicking, matching
    /// `components/graph.rs::resolve_edges`'s existing drop-invalid-refs
    /// behavior). `node_radius_px` is the fixed on-screen node quad
    /// half-width in CSS pixels. No-op if `nodes` is empty or this canvas
    /// has zero size.
    pub fn draw_normalized(&self, nodes: &[(f64, f64)], edges: &[(usize, usize)], node_radius_px: f64) {
        if nodes.is_empty() || self.width <= 0.0 || self.height <= 0.0 {
            return;
        }

        let mut edge_bytes: Vec<u8> = Vec::with_capacity(edges.len() * 2 * 8);
        for &(a, b) in edges {
            let (Some(&pa), Some(&pb)) = (nodes.get(a), nodes.get(b)) else {
                continue;
            };
            let (cxa, cya) = normalized_to_clip(pa.0, pa.1);
            let (cxb, cyb) = normalized_to_clip(pb.0, pb.1);
            edge_bytes.extend_from_slice(&cxa.to_le_bytes());
            edge_bytes.extend_from_slice(&cya.to_le_bytes());
            edge_bytes.extend_from_slice(&cxb.to_le_bytes());
            edge_bytes.extend_from_slice(&cyb.to_le_bytes());
        }

        let half_w = (node_radius_px * 2.0 / self.width) as f32;
        let half_h = (node_radius_px * 2.0 / self.height) as f32;
        let mut node_bytes: Vec<u8> = Vec::with_capacity(nodes.len() * 6 * 8);
        for &(x, y) in nodes {
            let (cx, cy) = normalized_to_clip(x, y);
            for (vx, vy) in quad_vertices_clip(cx, cy, half_w, half_h) {
                node_bytes.extend_from_slice(&vx.to_le_bytes());
                node_bytes.extend_from_slice(&vy.to_le_bytes());
            }
        }

        // `edge_bytes` may legitimately be empty (a node-only graph) —
        // `upload_vertices` returns `None` for that, so the edge draw below
        // is skipped without treating it as a failure.
        let edge_buffer = upload_vertices(&self.device, &self.queue, &edge_bytes);
        let Some(node_buffer) = upload_vertices(&self.device, &self.queue, &node_bytes) else {
            return;
        };
        let Some((encoder, pass)) = begin_pass(&self.device, &self.context) else {
            return;
        };

        if let Some(eb) = &edge_buffer {
            pass.set_pipeline(&self.edge_pipeline);
            pass.set_vertex_buffer(0, Some(eb));
            pass.draw((edges.len() * 2) as u32);
        }
        pass.set_pipeline(&self.node_pipeline);
        pass.set_vertex_buffer(0, Some(&node_buffer));
        pass.draw((nodes.len() * 6) as u32);

        end_and_submit(&self.queue, encoder, pass);
    }
}

/// Builds a fixed, reused-every-frame render pipeline for one `vec2<f32>`
/// vertex buffer already in clip space, one hardcoded fragment color, no
/// depth/stencil, no multisampling, no bind groups/uniforms — shared shape
/// between the edge (`LineList`) and node (`TriangleList`) pipelines, only
/// the shader source and topology differ.
fn build_pipeline(
    device: &GpuDevice,
    format: GpuTextureFormat,
    shader_src: &str,
    topology: GpuPrimitiveTopology,
) -> GpuRenderPipeline {
    let shader = device.create_shader_module(&GpuShaderModuleDescriptor::new(shader_src));

    let position_attr = GpuVertexAttribute::new(GpuVertexFormat::Float32x2, 0, 0);
    let vertex_layout = GpuVertexBufferLayout::new(8, &[position_attr]);

    let vertex_state = GpuVertexState::new(&shader);
    vertex_state.set_entry_point("vs_main");
    vertex_state.set_buffers(&[JsOption::wrap(vertex_layout)]);

    let color_target = GpuColorTargetState::new(format);
    let fragment_state = GpuFragmentState::new(&shader, &[JsOption::wrap(color_target)]);
    fragment_state.set_entry_point("fs_main");

    let primitive_state = GpuPrimitiveState::new();
    primitive_state.set_topology(topology);

    let pipeline_desc =
        GpuRenderPipelineDescriptor::new_with_gpu_auto_layout_mode(GpuAutoLayoutMode::Auto, &vertex_state);
    pipeline_desc.set_fragment(&fragment_state);
    pipeline_desc.set_primitive(&primitive_state);

    device
        .create_render_pipeline(&pipeline_desc)
        .expect("webgpu/graph.rs: fixed pipeline descriptor should always be valid")
}
