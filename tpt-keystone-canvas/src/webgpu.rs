//! Minimal WebGPU rendering proof-of-concept for `CanvasTimeSeries` — the
//! stretch goal `lib.rs`'s module docs call out as deferred. Feature-detected
//! and strictly additive: if `navigator.gpu` isn't present, or adapter/device
//! negotiation fails for any reason, `try_init` returns `None` and the
//! existing Canvas2D path (`render.rs`) keeps drawing exactly as it does
//! today — this module is an optional upgrade, never a requirement.
//!
//! Scope, deliberately narrow (a proof-of-concept, not a general-purpose
//! renderer — see `lib.rs`'s own estimate that a real WebGPU backend for all
//! four visual components is "an order of magnitude more code than the rest
//! of this crate combined"):
//! - One draw call per frame: a single `line-strip` of 2D points.
//! - Points are converted to clip-space (`[-1,1]` NDC) on the CPU before
//!   upload — no uniform buffer, no camera/projection matrix.
//! - One hardcoded solid stroke color (`#2563eb`, matching `render.rs`'s
//!   Canvas2D line color) baked into the fragment shader — no per-series
//!   theming yet.
//! - No point markers (Canvas2D's per-point circles) and no axis-label text
//!   — WebGPU text rendering needs a glyph atlas, out of scope for a PoC.
//! - Extending this to `CanvasMap`/`CanvasGraph` (`lib.rs`'s stated
//!   follow-up) would need a real 2D-to-clip-space transform (pan/zoom) and
//!   several draw calls (fills + strokes + text) — not attempted here.
//!
//! **Not browser-tested.** There's no headless-WebGPU harness in this repo
//! (same caveat `CLAUDE.md` already documents for the rest of this
//! WASM-only crate) — verified only by `cargo build --target
//! wasm32-unknown-unknown` compiling clean against web-sys's real WebGPU
//! bindings (gated behind `--cfg=web_sys_unstable_apis`, set for this crate
//! in `.cargo/config.toml`).

#![cfg(target_arch = "wasm32")]

use js_sys::JsOption;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    GpuAutoLayoutMode, GpuBufferDescriptor, GpuCanvasConfiguration, GpuCanvasContext,
    GpuColorTargetState, GpuCommandBuffer, GpuDevice, GpuFragmentState, GpuLoadOp,
    GpuPrimitiveState, GpuPrimitiveTopology, GpuQueue, GpuRenderPassColorAttachment,
    GpuRenderPassDescriptor, GpuRenderPipeline, GpuRenderPipelineDescriptor,
    GpuShaderModuleDescriptor, GpuStoreOp, GpuTextureFormat, GpuVertexAttribute,
    GpuVertexBufferLayout, GpuVertexFormat, GpuVertexState, HtmlCanvasElement,
};

/// A single vertex shader (clip-space passthrough) + fragment shader
/// (hardcoded solid color) — see module docs for why there's no uniform
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

// WebGPU `GPUBufferUsage` flag values (stable per the spec; web-sys doesn't
// bind the JS-side namespace object, so these are hardcoded rather than
// pulling in a `GpuBufferUsage` feature for two constants).
const BUFFER_USAGE_VERTEX: u32 = 0x0020;
const BUFFER_USAGE_COPY_DST: u32 = 0x0008;

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
    /// Feature-detects WebGPU and, if available, negotiates an adapter +
    /// device and builds the line-strip render pipeline against
    /// `<canvas id="{element_id}">`. Returns `None` on *any* failure —
    /// unsupported browser, refused adapter/device request, missing canvas,
    /// wrong element type — so the caller can fall back to Canvas2D without
    /// needing to distinguish *why* WebGPU wasn't available.
    pub async fn try_init(element_id: &str) -> Option<Self> {
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
        let context_obj = canvas.get_context("webgpu").ok()??;
        let context: GpuCanvasContext = context_obj.dyn_into().ok()?;

        let format = gpu.get_preferred_canvas_format();
        context
            .configure(&GpuCanvasConfiguration::new(&device, format))
            .ok()?;

        let pipeline = build_pipeline(&device, format);

        Some(Self {
            device,
            queue,
            context,
            pipeline,
        })
    }

    /// Redraws the whole canvas as a single `line-strip` through `points`,
    /// each in `[0,1] x [0,1]` normalized space (`(0,0)` top-left, `(1,1)`
    /// bottom-right — same convention as `render.rs`'s pixel space, just
    /// resolution-independent since callers here don't have easy access to
    /// this canvas's real pixel dimensions). Converted to `[-1,1]` clip space
    /// here. A no-op if `points` has fewer than 2 entries (a line-strip
    /// needs at least 2 vertices).
    pub fn draw_line_strip_normalized(&self, points: &[(f64, f64)]) {
        if points.len() < 2 {
            return;
        }

        // [0,1]x[0,1], y-down -> [-1,1] clip space, y-up: x' = 2x - 1,
        // y' = 1 - 2y.
        let mut vertex_bytes: Vec<u8> = Vec::with_capacity(points.len() * 8);
        for &(x, y) in points {
            let cx = (2.0 * x - 1.0) as f32;
            let cy = (1.0 - 2.0 * y) as f32;
            vertex_bytes.extend_from_slice(&cx.to_le_bytes());
            vertex_bytes.extend_from_slice(&cy.to_le_bytes());
        }

        let buffer_desc = GpuBufferDescriptor::new(
            vertex_bytes.len() as u32,
            BUFFER_USAGE_VERTEX | BUFFER_USAGE_COPY_DST,
        );
        let Ok(vertex_buffer) = self.device.create_buffer(&buffer_desc) else {
            return;
        };
        if self
            .queue
            .write_buffer_with_f64_and_u8_slice(&vertex_buffer, 0.0, &vertex_bytes)
            .is_err()
        {
            return;
        }

        let Ok(texture) = self.context.get_current_texture() else {
            return;
        };
        let Ok(view) = texture.create_view() else {
            return;
        };

        let color_attachment =
            GpuRenderPassColorAttachment::new_with_gpu_texture_view(GpuLoadOp::Clear, GpuStoreOp::Store, &view);
        color_attachment.set_clear_value(&[0.0.into(), 0.0.into(), 0.0.into(), 0.0.into()]);
        let pass_desc = GpuRenderPassDescriptor::new(&[JsOption::wrap(color_attachment)]);

        let encoder = self.device.create_command_encoder();
        let Ok(pass) = encoder.begin_render_pass(&pass_desc) else {
            return;
        };
        pass.set_pipeline(&self.pipeline);
        pass.set_vertex_buffer(0, Some(&vertex_buffer));
        pass.draw(points.len() as u32);
        pass.end();

        let command_buffer: GpuCommandBuffer = encoder.finish();
        self.queue.submit(&[command_buffer]);
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
        .expect("webgpu.rs: fixed line-strip pipeline descriptor should always be valid")
}
