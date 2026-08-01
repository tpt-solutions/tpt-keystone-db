//! `Canvas.TimeSeries` — line chart with auto-scaling axes and real-time
//! redraw when built with a `realtime_topic` (Chronos rollups, Phase 8,
//! publish onto ordinary tables so this needs no Chronos-specific code path,
//! just a `KeystoneClient::use_keystone_query` like every other component).
//!
//! Scope cut: no interpolation/downsampling of the plotted series — it
//! draws exactly the rows the query returns. Chronos's own `time_bucket`
//! already does downsampling server-side (Phase 8), which is what the
//! spec's example query uses; re-implementing it client-side would be
//! duplicate work.

use crate::client::QueryResult;

/// Linearly maps `value` from `[min,max]` to `[0,height]`, y-flipped so
/// larger values plot higher on the canvas. Returns `height / 2` if
/// `min == max` (a flat series shouldn't divide by zero).
pub fn scale_y(value: f64, min: f64, max: f64, height: f64) -> f64 {
    if (max - min).abs() < f64::EPSILON {
        return height / 2.0;
    }
    height - (value - min) / (max - min) * height
}

#[allow(dead_code)]
fn numeric_column(result: &QueryResult, field: &str) -> Option<Vec<f64>> {
    let idx = result.columns.iter().position(|c| c == field)?;
    Some(
        result
            .rows
            .iter()
            .map(|row| {
                row.get(idx)
                    .and_then(|c| c.as_deref())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0)
            })
            .collect(),
    )
}

#[cfg(target_arch = "wasm32")]
mod wasm_impl {
    use std::cell::RefCell;
    use std::rc::Rc;

    use wasm_bindgen::prelude::*;

    use super::{numeric_column, scale_y};
    use crate::client::{KeystoneClient, QueryResult};
    use crate::reactive::Signal;
    use crate::render::Canvas2d;
    use crate::webgpu::GpuRenderer;

    /// Which backend ended up drawing this canvas — decided once,
    /// asynchronously, right after construction (see `CanvasTimeSeries::new`'s
    /// doc comment for why this can't be a synchronous decision: a `<canvas>`
    /// element commits to its first successfully-requested context type
    /// (`"2d"` xor `"webgpu"`) for its whole lifetime, so trying WebGPU can
    /// only happen *before* any `2d` context is ever requested on the same
    /// element).
    enum Renderer {
        Canvas2d(Canvas2d),
        Gpu(GpuRenderer),
    }

    #[wasm_bindgen]
    pub struct CanvasTimeSeries {
        _ws: Option<web_sys::WebSocket>,
        _effect: Rc<dyn std::any::Any>,
    }

    #[wasm_bindgen]
    impl CanvasTimeSeries {
        /// Mounting is asynchronous under the hood (not in this method's
        /// signature — it still returns synchronously): which backend draws
        /// this canvas is decided by a `wasm_bindgen_futures::spawn_local`
        /// task kicked off here, since WebGPU's adapter/device negotiation
        /// is inherently a `Promise`-based API with no synchronous
        /// equivalent. Until that task resolves, nothing is drawn — the
        /// `redraw_trigger` signal below forces one redraw once it does,
        /// even for a non-realtime query whose data never changes again.
        #[wasm_bindgen(constructor)]
        pub fn new(
            canvas_id: &str,
            http_base: &str,
            ws_base: &str,
            sql: &str,
            x_field: &str,
            y_field: &str,
            realtime_topic: &str,
        ) -> Result<CanvasTimeSeries, JsValue> {
            let client = Rc::new(KeystoneClient::new(http_base, ws_base));
            let topic = if realtime_topic.is_empty() {
                None
            } else {
                Some(realtime_topic)
            };
            let (data, ws) = client.use_keystone_query(sql, topic);

            let renderer: Rc<RefCell<Option<Renderer>>> = Rc::new(RefCell::new(None));
            let redraw_trigger: Signal<u32> = Signal::new(0);
            {
                let renderer = renderer.clone();
                let canvas_id = canvas_id.to_string();
                let redraw_trigger = redraw_trigger.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    let chosen = match GpuRenderer::try_init(&canvas_id).await {
                        Some(gpu) => Some(Renderer::Gpu(gpu)),
                        // WebGPU wasn't available/negotiation failed *before*
                        // any context was requested on this canvas (see
                        // `GpuRenderer::try_init`'s ordering) — `"2d"` is
                        // still free to request.
                        None => Canvas2d::mount(&canvas_id).ok().map(Renderer::Canvas2d),
                    };
                    *renderer.borrow_mut() = chosen;
                    redraw_trigger.update(|n| n + 1);
                });
            }

            let x_field = x_field.to_string();
            let y_field = y_field.to_string();
            let effect = crate::reactive::create_effect(move || {
                let result = data.get();
                let _ = redraw_trigger.get(); // subscribe: re-run once the renderer resolves
                if let Some(r) = renderer.borrow().as_ref() {
                    draw(r, &result, &x_field, &y_field);
                }
            });

            Ok(CanvasTimeSeries { _ws: ws, _effect: effect })
        }
    }

    fn draw(renderer: &Renderer, result: &QueryResult, x_field: &str, y_field: &str) {
        match renderer {
            Renderer::Canvas2d(canvas) => draw_canvas2d(canvas, result, x_field, y_field),
            Renderer::Gpu(gpu) => draw_gpu(gpu, result, y_field),
        }
    }

    /// WebGPU path (proof-of-concept, see `webgpu.rs` module docs for scope):
    /// just the connecting line-strip, no point markers, no axis labels, no
    /// per-series color — the deliberately minimal subset of
    /// `draw_canvas2d`'s output.
    fn draw_gpu(gpu: &GpuRenderer, result: &QueryResult, y_field: &str) {
        let Some(ys) = numeric_column(result, y_field) else {
            return;
        };
        if ys.is_empty() {
            return;
        }
        let min = ys.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        // `webgpu::GpuRenderer` doesn't expose its canvas size (only
        // `draw_line_strip` needs it, internally) — points are scaled to a
        // fixed logical space and the pixel conversion happens inside
        // `draw_line_strip` itself using the canvas's real dimensions
        // captured at `try_init` time, so this only needs to pick a
        // consistent `step`/`scale_y` domain, same as the Canvas2D path.
        let step = 1.0 / (ys.len().max(2) - 1) as f64;
        let points: Vec<(f64, f64)> = ys
            .iter()
            .enumerate()
            .map(|(i, &y)| (i as f64 * step, scale_y(y, min, max, 1.0)))
            .collect();
        gpu.draw_line_strip_normalized(&points);
    }

    fn draw_canvas2d(canvas: &Canvas2d, result: &QueryResult, x_field: &str, y_field: &str) {
        canvas.clear();
        let Some(ys) = numeric_column(result, y_field) else {
            return;
        };
        if ys.is_empty() {
            return;
        }
        let x_idx = result.columns.iter().position(|c| c == x_field);
        let min = ys.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let step = canvas.width / (ys.len().max(2) - 1) as f64;

        let points: Vec<(f64, f64)> = ys
            .iter()
            .enumerate()
            .map(|(i, &y)| (i as f64 * step, scale_y(y, min, max, canvas.height)))
            .collect();
        for pair in points.windows(2) {
            canvas.line(pair[0].0, pair[0].1, pair[1].0, pair[1].1, "#2563eb", 2.0);
        }
        for &(x, y) in &points {
            canvas.circle(x, y, 3.0, "#2563eb");
        }
        if let Some(idx) = x_idx {
            if let (Some(first), Some(last)) = (result.rows.first(), result.rows.last()) {
                let label = |row: &Vec<Option<String>>| {
                    row.get(idx)
                        .and_then(|c| c.as_deref())
                        .unwrap_or("")
                        .to_string()
                };
                canvas.text(2.0, canvas.height - 4.0, &label(first), "#333");
                canvas.text(
                    canvas.width - 60.0,
                    canvas.height - 4.0,
                    &label(last),
                    "#333",
                );
            }
        }
    }
} // mod wasm_impl

#[cfg(target_arch = "wasm32")]
pub use wasm_impl::CanvasTimeSeries;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_y_maps_range_and_flips() {
        assert_eq!(scale_y(0.0, 0.0, 10.0, 100.0), 100.0);
        assert_eq!(scale_y(10.0, 0.0, 10.0, 100.0), 0.0);
        assert_eq!(scale_y(5.0, 0.0, 10.0, 100.0), 50.0);
    }

    #[test]
    fn scale_y_flat_series_is_midline() {
        assert_eq!(scale_y(5.0, 5.0, 5.0, 100.0), 50.0);
    }

    #[test]
    fn numeric_column_extracts_and_defaults_unparseable() {
        let result = QueryResult {
            columns: vec!["bucket".into(), "avg_time".into()],
            rows: vec![
                vec![Some("t1".into()), Some("3.5".into())],
                vec![Some("t2".into()), None],
            ],
        };
        assert_eq!(numeric_column(&result, "avg_time"), Some(vec![3.5, 0.0]));
        assert_eq!(numeric_column(&result, "missing"), None);
    }
}
