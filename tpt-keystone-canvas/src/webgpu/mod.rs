//! WebGPU rendering proof-of-concept, shared plumbing (Phase 10 split this
//! from a single `webgpu.rs` covering only `CanvasTimeSeries` into this
//! directory module once `CanvasMap`/`CanvasGraph` grew their own WebGPU
//! renderers too — see `timeseries.rs`/`map.rs`/`graph.rs`). Feature-detected
//! and strictly additive: if `navigator.gpu` isn't present, or adapter/device
//! negotiation fails for any reason, `GpuContext::negotiate` returns `None`
//! and the caller falls back to the existing Canvas2D path (`render.rs`) —
//! every renderer in this directory is an optional upgrade, never a
//! requirement.
//!
//! Scope, deliberately narrow (a proof-of-concept, not a general-purpose
//! renderer — see `lib.rs`'s own estimate that a real WebGPU backend for all
//! visual components is "an order of magnitude more code than the rest of
//! this crate combined"):
//! - One draw call (`timeseries.rs`) or a small fixed number of draw calls
//!   (`map.rs`: one marker pipeline; `graph.rs`: one edge pipeline + one node
//!   pipeline) per frame — no per-frame draw-call count that scales with a
//!   surrounding UI, no scene graph.
//! - Marker/node quads are expanded into two triangles on the CPU
//!   (`view_transform::quad_vertices_clip`) and uploaded as plain vertices —
//!   no `GpuVertexStepMode::Instance` instancing, no per-instance uniform
//!   buffer.
//! - One hardcoded solid color per pipeline baked into its fragment shader
//!   (matching each component's existing Canvas2D palette) — no per-series/
//!   per-cluster-size/per-node theming, no bind groups/uniforms at all.
//! - No point/marker outline or hover state, no axis-label or node-id text
//!   — WebGPU text rendering needs a glyph atlas, out of scope for a PoC.
//! - Pan/zoom (`view_transform::ViewTransform`) is shared with the Canvas2D
//!   path and applied entirely on the CPU before upload — no camera/
//!   projection uniform buffer in any pipeline here.
//!
//! **Not browser-tested.** There's no headless-WebGPU harness in this repo
//! (same caveat `CLAUDE.md` already documents for the rest of this
//! WASM-only crate) — verified only by `cargo build --target
//! wasm32-unknown-unknown` compiling clean against web-sys's real WebGPU
//! bindings (gated behind `--cfg=web_sys_unstable_apis`, set for this crate
//! in `.cargo/config.toml`).

#![cfg(target_arch = "wasm32")]

mod graph;
mod map;
mod timeseries;

pub use graph::GpuGraphRenderer;
pub use map::GpuMapRenderer;
pub use timeseries::GpuRenderer;

use js_sys::JsOption;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    GpuBuffer, GpuBufferDescriptor, GpuCanvasConfiguration, GpuCanvasContext, GpuCommandBuffer,
    GpuCommandEncoder, GpuDevice, GpuLoadOp, GpuQueue, GpuRenderPassColorAttachment,
    GpuRenderPassDescriptor, GpuRenderPassEncoder, GpuStoreOp, GpuTextureFormat,
    HtmlCanvasElement,
};

// WebGPU `GPUBufferUsage` flag values (stable per the spec; web-sys doesn't
// bind the JS-side namespace object, so these are hardcoded rather than
// pulling in a `GpuBufferUsage` feature for two constants).
const BUFFER_USAGE_VERTEX: u32 = 0x0020;
const BUFFER_USAGE_COPY_DST: u32 = 0x0008;

/// Negotiated WebGPU device/queue plus a configured canvas context — the
/// adapter/device/context negotiation dance every `Gpu*Renderer` in this
/// directory needs before building its own pipeline(s), factored out here so
/// it's written once instead of duplicated per renderer.
pub(crate) struct GpuContext {
    pub device: GpuDevice,
    pub queue: GpuQueue,
    pub context: GpuCanvasContext,
    pub format: GpuTextureFormat,
    /// `<canvas>`'s pixel `width`/`height` DOM attributes, captured at
    /// negotiation time. `GpuMapRenderer`/`GpuGraphRenderer` use these to
    /// convert a fixed on-screen marker/node radius (CSS pixels) into
    /// clip-space half-extents (`quad_vertices_clip`) — `timeseries.rs`
    /// doesn't need them since its line-strip has no marker geometry.
    pub width: f64,
    pub height: f64,
}

impl GpuContext {
    /// Feature-detects WebGPU and, if available, negotiates an adapter +
    /// device and configures `<canvas id="{element_id}">`'s WebGPU context.
    /// Returns `None` on *any* failure — unsupported browser, refused
    /// adapter/device request, missing canvas, wrong element type — so the
    /// caller can fall back to Canvas2D without needing to distinguish *why*
    /// WebGPU wasn't available.
    pub async fn negotiate(element_id: &str) -> Option<Self> {
        let window = web_sys::window()?;
        let navigator = window.navigator();
        let gpu = navigator.gpu();
        // `navigator.gpu` is `undefined` in browsers without WebGPU support;
        // the typed getter still returns a `Gpu` wrapping that `undefined`
        // rather than failing, so this has to be checked explicitly.
        if JsValue::from(gpu.clone()).is_undefined() {
            return None;
        }

        let adapter_value = JsFuture::from(js_sys::Promise::from(gpu.request_adapter()))
            .await
            .ok()?;
        if adapter_value.is_null() || adapter_value.is_undefined() {
            return None;
        }
        let adapter: web_sys::GpuAdapter = adapter_value.dyn_into().ok()?;
        let device_value = JsFuture::from(js_sys::Promise::from(adapter.request_device()))
            .await
            .ok()?;
        let device: GpuDevice = device_value.dyn_into().ok()?;
        let queue = device.queue();

        let document = window.document()?;
        let element = document.get_element_by_id(element_id)?;
        let canvas: HtmlCanvasElement = element.dyn_into().ok()?;
        let (width, height) = (canvas.width() as f64, canvas.height() as f64);
        let context_obj = canvas.get_context("webgpu").ok()??;
        let context: GpuCanvasContext = context_obj.dyn_into().ok()?;

        let format = gpu.get_preferred_canvas_format();
        context
            .configure(&GpuCanvasConfiguration::new(&device, format))
            .ok()?;

        Some(Self { device, queue, context, format, width, height })
    }
}

/// Uploads `bytes` into a fresh vertex buffer. Returns `None` (rather than
/// erroring) on an empty slice or any WebGPU call failure, matching this
/// module's "fail silently, caller just skips the draw" convention.
pub(crate) fn upload_vertices(device: &GpuDevice, queue: &GpuQueue, bytes: &[u8]) -> Option<GpuBuffer> {
    if bytes.is_empty() {
        return None;
    }
    let desc = GpuBufferDescriptor::new(bytes.len() as u32, BUFFER_USAGE_VERTEX | BUFFER_USAGE_COPY_DST);
    let buffer = device.create_buffer(&desc).ok()?;
    queue
        .write_buffer_with_f64_and_u8_slice(&buffer, 0.0, bytes)
        .ok()?;
    Some(buffer)
}

/// Begins one render pass against `context`'s current texture, clearing to
/// transparent black. Returns the command encoder (still needed to `finish`
/// once the caller's pipeline-specific draw calls are done) alongside the
/// pass encoder.
pub(crate) fn begin_pass(
    device: &GpuDevice,
    context: &GpuCanvasContext,
) -> Option<(GpuCommandEncoder, GpuRenderPassEncoder)> {
    let texture = context.get_current_texture().ok()?;
    let view = texture.create_view().ok()?;

    let color_attachment =
        GpuRenderPassColorAttachment::new_with_gpu_texture_view(GpuLoadOp::Clear, GpuStoreOp::Store, &view);
    color_attachment.set_clear_value(&[0.0.into(), 0.0.into(), 0.0.into(), 0.0.into()]);
    let pass_desc = GpuRenderPassDescriptor::new(&[JsOption::wrap(color_attachment)]);

    let encoder = device.create_command_encoder();
    let pass = encoder.begin_render_pass(&pass_desc).ok()?;
    Some((encoder, pass))
}

/// Ends `pass`, finishes `encoder` into a command buffer, and submits it to
/// `queue` — the tail three calls every renderer's redraw needs after its own
/// `set_pipeline`/`set_vertex_buffer`/`draw` call(s).
pub(crate) fn end_and_submit(queue: &GpuQueue, encoder: GpuCommandEncoder, pass: GpuRenderPassEncoder) {
    pass.end();
    let command_buffer: GpuCommandBuffer = encoder.finish();
    queue.submit(&[command_buffer]);
}
