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
    Some(result.rows.iter().map(|row| row.get(idx).and_then(|c| c.as_deref()).and_then(|s| s.parse().ok()).unwrap_or(0.0)).collect())
}

#[cfg(target_arch = "wasm32")]
mod wasm_impl {
    use std::rc::Rc;

    use wasm_bindgen::prelude::*;

    use super::{numeric_column, scale_y};
    use crate::client::{KeystoneClient, QueryResult};
    use crate::render::Canvas2d;

#[wasm_bindgen]
pub struct CanvasTimeSeries {
    _canvas: Canvas2d,
    _ws: Option<web_sys::WebSocket>,
    _effect: Rc<dyn std::any::Any>,
}

#[wasm_bindgen]
impl CanvasTimeSeries {
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
        let canvas = Canvas2d::mount(canvas_id).map_err(|e| JsValue::from_str(&e))?;
        let client = Rc::new(KeystoneClient::new(http_base, ws_base));
        let topic = if realtime_topic.is_empty() { None } else { Some(realtime_topic) };
        let (data, ws) = client.use_keystone_query(sql, topic);

        let redraw_canvas = Canvas2d { ctx: canvas.ctx.clone(), width: canvas.width, height: canvas.height };
        let x_field = x_field.to_string();
        let y_field = y_field.to_string();
        let effect = crate::reactive::create_effect(move || {
            let result = data.get();
            draw(&redraw_canvas, &result, &x_field, &y_field);
        });

        Ok(CanvasTimeSeries { _canvas: canvas, _ws: ws, _effect: effect })
    }
}

fn draw(canvas: &Canvas2d, result: &QueryResult, x_field: &str, y_field: &str) {
    canvas.clear();
    let Some(ys) = numeric_column(result, y_field) else { return };
    if ys.is_empty() {
        return;
    }
    let x_idx = result.columns.iter().position(|c| c == x_field);
    let min = ys.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let step = canvas.width / (ys.len().max(2) - 1) as f64;

    let points: Vec<(f64, f64)> = ys.iter().enumerate().map(|(i, &y)| (i as f64 * step, scale_y(y, min, max, canvas.height))).collect();
    for pair in points.windows(2) {
        canvas.line(pair[0].0, pair[0].1, pair[1].0, pair[1].1, "#2563eb", 2.0);
    }
    for &(x, y) in &points {
        canvas.circle(x, y, 3.0, "#2563eb");
    }
    if let Some(idx) = x_idx {
        if let (Some(first), Some(last)) = (result.rows.first(), result.rows.last()) {
            let label = |row: &Vec<Option<String>>| row.get(idx).and_then(|c| c.as_deref()).unwrap_or("").to_string();
            canvas.text(2.0, canvas.height - 4.0, &label(first), "#333");
            canvas.text(canvas.width - 60.0, canvas.height - 4.0, &label(last), "#333");
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
            rows: vec![vec![Some("t1".into()), Some("3.5".into())], vec![Some("t2".into()), None]],
        };
        assert_eq!(numeric_column(&result, "avg_time"), Some(vec![3.5, 0.0]));
        assert_eq!(numeric_column(&result, "missing"), None);
    }
}
