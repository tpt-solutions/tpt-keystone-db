//! Canvas2D rendering primitives shared by every `Canvas.*` component.
//!
//! Scope cut (see `src/lib.rs` module docs for the full rationale): this is
//! `web_sys::CanvasRenderingContext2d`, not WebGPU. Every component in this
//! crate draws through this one thin wrapper so that swapping the backend
//! later (a real WebGPU renderer) only means rewriting this file, not every
//! component.

#![cfg(target_arch = "wasm32")]

use wasm_bindgen::JsCast;
use web_sys::{CanvasRenderingContext2d, HtmlCanvasElement};

pub struct Canvas2d {
    pub ctx: CanvasRenderingContext2d,
    pub width: f64,
    pub height: f64,
}

impl Canvas2d {
    /// Looks up `<canvas id="{element_id}">` in the document and grabs its
    /// 2D rendering context.
    pub fn mount(element_id: &str) -> Result<Self, String> {
        let window = web_sys::window().ok_or("no window")?;
        let document = window.document().ok_or("no document")?;
        let element = document.get_element_by_id(element_id).ok_or_else(|| format!("no element #{element_id}"))?;
        let canvas: HtmlCanvasElement = element.dyn_into().map_err(|_| format!("#{element_id} is not a <canvas>"))?;
        let ctx = canvas
            .get_context("2d")
            .map_err(|e| format!("{e:?}"))?
            .ok_or("2d context unavailable")?
            .dyn_into::<CanvasRenderingContext2d>()
            .map_err(|_| "get_context(\"2d\") did not return CanvasRenderingContext2d")?;
        Ok(Self { ctx, width: canvas.width() as f64, height: canvas.height() as f64 })
    }

    pub fn clear(&self) {
        self.ctx.clear_rect(0.0, 0.0, self.width, self.height);
    }

    pub fn circle(&self, x: f64, y: f64, radius: f64, fill: &str) {
        self.ctx.set_fill_style_str(fill);
        self.ctx.begin_path();
        let _ = self.ctx.arc(x, y, radius, 0.0, std::f64::consts::PI * 2.0);
        self.ctx.fill();
    }

    pub fn line(&self, x0: f64, y0: f64, x1: f64, y1: f64, stroke: &str, width: f64) {
        self.ctx.set_stroke_style_str(stroke);
        self.ctx.set_line_width(width);
        self.ctx.begin_path();
        self.ctx.move_to(x0, y0);
        self.ctx.line_to(x1, y1);
        self.ctx.stroke();
    }

    pub fn text(&self, x: f64, y: f64, text: &str, fill: &str) {
        self.ctx.set_fill_style_str(fill);
        let _ = self.ctx.fill_text(text, x, y);
    }

    pub fn fill_rect(&self, x: f64, y: f64, w: f64, h: f64) {
        self.ctx.fill_rect(x, y, w, h);
    }
}
