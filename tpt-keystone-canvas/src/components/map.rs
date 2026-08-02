//! `Canvas.Map` — geospatial point renderer. Equirectangular projection
//! (not a real Mercator/tile-based map — there's no basemap tile layer at
//! all, just points on a blank canvas) with grid-bucket clustering and
//! click hit-testing, per the Phase 13 checklist's "Clustering, heatmaps,
//! spatial filter queries built-in" — heatmaps and spatial filter queries
//! (`ST_*` predicates already exist server-side per Phase 6/Meridian) are a
//! documented scope cut; only point rendering + clustering are implemented.
//! Phase 10 added a pan/zoom `ViewTransform` (mouse-drag pan, wheel zoom)
//! and an opt-in WebGPU marker renderer (`webgpu::GpuMapRenderer`) for the
//! plain-point-marker mode — see `new`'s doc comment for when each backend
//! is chosen.

/// Plain equirectangular projection: `lon` in [-180,180] -> x in
/// [0,width], `lat` in [-90,90] -> y in [0,height] (y flipped so north is
/// up). No true Mercator distortion correction — a documented simplification
/// consistent with "not a Mapbox GL replacement".
pub fn project(lat: f64, lon: f64, width: f64, height: f64) -> (f64, f64) {
    let x = (lon + 180.0) / 360.0 * width;
    let y = (90.0 - lat) / 180.0 * height;
    (x, y)
}

/// Buckets projected points into `cell_size`-px grid cells and returns one
/// `(centroid_x, centroid_y, count, member_indices)` per non-empty cell —
/// the "clustering" half of `Canvas.Map`.
pub fn cluster_grid(points: &[(f64, f64)], cell_size: f64) -> Vec<(f64, f64, Vec<usize>)> {
    use std::collections::HashMap;
    let mut cells: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
    for (i, (x, y)) in points.iter().enumerate() {
        let key = ((x / cell_size).floor() as i64, (y / cell_size).floor() as i64);
        cells.entry(key).or_default().push(i);
    }
    cells
        .into_values()
        .map(|members| {
            let (sx, sy) = members.iter().fold((0.0, 0.0), |(ax, ay), &i| (ax + points[i].0, ay + points[i].1));
            let n = members.len() as f64;
            (sx / n, sy / n, members)
        })
        .collect()
}

/// Kernel-density estimate over a grid. Each projected point casts a small
/// Gaussian "heat" contribution onto the cells within `bandwidth` px, summed
/// and normalized to `[0.0, 1.0]` (the hottest cell maps to 1.0, so a
/// renderer can pick a color ramp stop). Pure and host-testable — no DOM,
/// no external dependency, exactly as the Phase 4 checklist's "kernel density,
/// no external dependency" item asks. Returns a flat `grid_w * grid_h` row-major
/// vector (index `y * grid_w + x`).
pub fn kernel_density(
    points: &[(f64, f64)],
    width: f64,
    height: f64,
    grid_w: usize,
    grid_h: usize,
    bandwidth: f64,
) -> Vec<f64> {
    if grid_w == 0 || grid_h == 0 || points.is_empty() {
        return vec![0.0; grid_w * grid_h];
    }
    let cell_w = width / grid_w as f64;
    let cell_h = height / grid_h as f64;
    let mut grid = vec![0.0f64; grid_w * grid_h];

    for &(px, py) in points {
        // Only cells whose center falls within `bandwidth` of the point
        // contribute — a cheap bounding-box prune before the Gaussian.
        let cx0 = ((px - bandwidth) / cell_w).floor().max(0.0) as usize;
        let cx1 = ((px + bandwidth) / cell_w).floor().min(grid_w as f64 - 1.0) as usize;
        let cy0 = ((py - bandwidth) / cell_h).floor().max(0.0) as usize;
        let cy1 = ((py + bandwidth) / cell_h).floor().min(grid_h as f64 - 1.0) as usize;
        for gy in cy0..=cy1 {
            for gx in cx0..=cx1 {
                let center_x = (gx as f64 + 0.5) * cell_w;
                let center_y = (gy as f64 + 0.5) * cell_h;
                let dx = center_x - px;
                let dy = center_y - py;
                let d2 = dx * dx + dy * dy;
                // Gaussian; `bandwidth` is the std-dev. Guard d2 against a
                // zero bandwidth (a single-point map) so the point's own cell
                // still gets full weight.
                let w = if bandwidth <= 0.0 {
                    if d2 < 1e-6 { 1.0 } else { 0.0 }
                } else {
                    (-d2 / (2.0 * bandwidth * bandwidth)).exp()
                };
                grid[gy * grid_w + gx] += w;
            }
        }
    }

    if let Some(max) = grid.iter().fold(None, |acc: Option<f64>, &v| match acc {
        None => Some(v),
        Some(m) => Some(m.max(v)),
    }) {
        if max > 0.0 {
            for v in grid.iter_mut() {
                *v /= max;
            }
        }
    }
    grid
}

/// Maps a normalized density `[0,1]` to an "heat" RGB hex stop along a
/// blue→cyan→yellow→red ramp (the classic density plot palette). Clamps the
/// input and returns a `#rrggbb` string ready for canvas fill/stroke.
pub fn heat_color(t: f64) -> String {
    let t = t.clamp(0.0, 1.0);
    // Piecewise-linear ramp stops.
    let stops: [(f64, (u8, u8, u8)); 4] = [
        (0.0, (37, 99, 235)),   // blue
        (0.4, (34, 211, 238)),  // cyan
        (0.7, (250, 204, 21)),  // yellow
        (1.0, (239, 68, 68)),   // red
    ];
    let (mut lo, mut hi) = (stops[0], stops[3]);
    for w in stops.windows(2) {
        if t >= w[0].0 && t <= w[1].0 {
            lo = w[0];
            hi = w[1];
            break;
        }
    }
    let span = (hi.0 - lo.0).max(1e-6);
    let f = (t - lo.0) / span;
    let r = (lo.1 .0 as f64 + (hi.1 .0 as f64 - lo.1 .0 as f64) * f) as u8;
    let g = (lo.1 .1 as f64 + (hi.1 .1 as f64 - lo.1 .1 as f64) * f) as u8;
    let b = (lo.1 .2 as f64 + (hi.1 .2 as f64 - lo.1 .2 as f64) * f) as u8;
    format!("#{r:02x}{g:02x}{b:02x}")
}

/// Parses a `"lat,lon"` (or `"lat, lon"`) text column value into a `(lat,
/// lon)` float pair. Returns `None` for anything that isn't two comma-
/// separated finite numbers. Used by the `CanvasMap` renderer to turn a
/// query's `location_field` text into projected canvas coordinates.
pub fn parse_lat_lon(s: &str) -> Option<(f64, f64)> {
    let mut parts = s.split(',');
    let lat = parts.next()?.trim().parse::<f64>().ok()?;
    let lon = parts.next()?.trim().parse::<f64>().ok()?;
    if lat.is_finite() && lon.is_finite() {
        Some((lat, lon))
    } else {
        None
    }
}


// Everything below drives an actual `<canvas>`/DOM element, so — like
// `client.rs`'s `mod browser` — it only compiles for the wasm32 target;
// only the pure functions above are exercised by `cargo test` on the host.
#[cfg(target_arch = "wasm32")]
mod wasm_impl {
    use std::cell::RefCell;
    use std::rc::Rc;

    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;

    use super::{cluster_grid, heat_color, kernel_density, parse_lat_lon, project};
    use crate::client::{KeystoneClient, QueryResult};
    use crate::reactive::Signal;
    use crate::render::{canvas_dimensions, Canvas2d};
    use crate::view_transform::{zoom_factor_from_wheel_delta, ViewTransform};
    use crate::webgpu::GpuMapRenderer;

    /// A mouse-move must exceed this many CSS pixels (in either axis) before
    /// a mousedown-drag is treated as a pan rather than the start of a
    /// stationary click — otherwise every click would nudge the view by a
    /// pixel or two of unintended pan.
    const PAN_THRESHOLD_PX: f64 = 3.0;
    /// Marker hit-test / WebGPU marker radius, CSS pixels — matches the
    /// existing Canvas2D point-marker radius (`canvas.circle(x, y, 5.0, ...)`).
    const MARKER_RADIUS_PX: f64 = 5.0;

    /// Which backend ended up drawing this canvas — decided once,
    /// asynchronously (see `CanvasMap::new`'s doc comment for why: a
    /// `<canvas>` element commits to its first successfully-requested context
    /// type, `"2d"` xor `"webgpu"`, for its whole lifetime).
    enum Renderer {
        Canvas2d(Canvas2d),
        Gpu(GpuMapRenderer),
    }

    #[wasm_bindgen]
    pub struct CanvasMap {
        _location_field: String,
        _cluster: bool,
        data: Signal<QueryResult>,
        on_click: Option<js_sys::Function>,
        positions: Rc<RefCell<Vec<(f64, f64)>>>,
        view: Rc<RefCell<ViewTransform>>,
        redraw_trigger: Signal<u32>,
        _ws: Option<web_sys::WebSocket>,
        _effect: Rc<dyn std::any::Any>,
    }

    #[wasm_bindgen]
    impl CanvasMap {
        /// Mounts onto `<canvas id="{canvas_id}">`, running `sql` (expected to
        /// select `location_field` as a `"lat,lon"` text column) once and
        /// re-running it on every message from `realtime_topic` (pass `""` for
        /// no live updates). `on_click(row_json: string)` fires on a plain
        /// click (drag distance under `PAN_THRESHOLD_PX`) on a rendered
        /// marker. When `heatmap` is true, points are rendered as a
        /// kernel-density heat field instead of (or, if `cluster` is also set,
        /// in addition to) point/cluster markers — see `kernel_density`.
        ///
        /// Rendering backend: when `heatmap` is set, this always mounts
        /// Canvas2D synchronously (kernel-density fill has no WebGPU pipeline
        /// — see `webgpu::GpuMapRenderer`'s module docs), same as `cluster`
        /// (variable-radius/count-label bubbles aren't attempted in WebGPU
        /// either). Otherwise it races `GpuMapRenderer::try_init` against a
        /// Canvas2D fallback exactly like `CanvasTimeSeries` — nothing is
        /// drawn until the race resolves, mirrored the same way.
        #[wasm_bindgen(constructor)]
        pub fn new(
            canvas_id: &str,
            http_base: &str,
            ws_base: &str,
            sql: &str,
            location_field: &str,
            realtime_topic: &str,
            cluster: bool,
            heatmap: bool,
            on_click: Option<js_sys::Function>,
        ) -> Result<CanvasMap, JsValue> {
            let client = Rc::new(KeystoneClient::new(http_base, ws_base));
            let topic = if realtime_topic.is_empty() { None } else { Some(realtime_topic) };
            let (data, ws) = client.use_keystone_query(sql, topic);

            let (width, height) = canvas_dimensions(canvas_id).unwrap_or((0.0, 0.0));

            let renderer: Rc<RefCell<Option<Renderer>>> = Rc::new(RefCell::new(None));
            let redraw_trigger: Signal<u32> = Signal::new(0);
            let view: Rc<RefCell<ViewTransform>> = Rc::new(RefCell::new(ViewTransform::identity()));

            if heatmap || cluster {
                let canvas = Canvas2d::mount(canvas_id).map_err(|e| JsValue::from_str(&e))?;
                *renderer.borrow_mut() = Some(Renderer::Canvas2d(canvas));
            } else {
                let init_renderer = renderer.clone();
                let init_canvas_id = canvas_id.to_string();
                let init_redraw_trigger = redraw_trigger.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    let chosen = match GpuMapRenderer::try_init(&init_canvas_id).await {
                        Some(gpu) => Some(Renderer::Gpu(gpu)),
                        // WebGPU wasn't available/negotiation failed *before*
                        // any context was requested on this canvas — `"2d"`
                        // is still free to request.
                        None => Canvas2d::mount(&init_canvas_id).ok().map(Renderer::Canvas2d),
                    };
                    *init_renderer.borrow_mut() = chosen;
                    init_redraw_trigger.update(|n| n + 1);
                });
            }

            let positions = Rc::new(RefCell::new(Vec::new()));
            let draw_data = data.clone();
            let draw_field = location_field.to_string();
            let draw_positions = positions.clone();
            let draw_renderer = renderer.clone();
            let draw_view = view.clone();
            let draw_redraw_trigger = redraw_trigger.clone();
            let effect = crate::reactive::create_effect(move || {
                let result = draw_data.get();
                let _ = draw_redraw_trigger.get(); // subscribe: re-run once the renderer resolves or the view pans/zooms
                if let Some(r) = draw_renderer.borrow().as_ref() {
                    let view_snapshot = *draw_view.borrow();
                    draw(r, &result, &draw_field, cluster, heatmap, width, height, view_snapshot, &draw_positions);
                }
            });

            let map = CanvasMap {
                _location_field: location_field.to_string(),
                _cluster: cluster,
                data,
                on_click,
                positions,
                view,
                redraw_trigger,
                _ws: ws,
                _effect: effect,
            };
            map.install_pointer_handlers(canvas_id)?;
            Ok(map)
        }

        /// Wires wheel-to-zoom and a 3-event (mousedown/mousemove/mouseup)
        /// pan-vs-click state machine onto `<canvas id="{canvas_id}">`: a
        /// drag past `PAN_THRESHOLD_PX` pans `view` (redrawing on every
        /// move); a mouseup that never crossed that threshold is treated as
        /// a stationary click and hit-tested against the last-drawn marker
        /// `positions` (screen space, so no `view` conversion needed there).
        fn install_pointer_handlers(&self, canvas_id: &str) -> Result<(), JsValue> {
            let document = web_sys::window().unwrap().document().unwrap();
            let element = document.get_element_by_id(canvas_id).ok_or("canvas element missing")?;
            let canvas_el: web_sys::HtmlCanvasElement = element.dyn_into()?;

            let wheel_view = self.view.clone();
            let wheel_redraw = self.redraw_trigger.clone();
            let onwheel = Closure::<dyn FnMut(_)>::new(move |evt: web_sys::WheelEvent| {
                evt.prevent_default();
                let factor = zoom_factor_from_wheel_delta(evt.delta_y());
                let (px, py) = (evt.offset_x() as f64, evt.offset_y() as f64);
                wheel_view.borrow_mut().zoom(factor, px, py);
                wheel_redraw.update(|n| n + 1);
            });
            canvas_el.set_onwheel(Some(onwheel.as_ref().unchecked_ref()));
            onwheel.forget();

            // `(last_x, last_y, moved_past_threshold)`, `None` when no button is down.
            let drag_state: Rc<RefCell<Option<(f64, f64, bool)>>> = Rc::new(RefCell::new(None));

            let down_state = drag_state.clone();
            let onmousedown = Closure::<dyn FnMut(_)>::new(move |evt: web_sys::MouseEvent| {
                let (x, y) = (evt.offset_x() as f64, evt.offset_y() as f64);
                *down_state.borrow_mut() = Some((x, y, false));
            });
            canvas_el.set_onmousedown(Some(onmousedown.as_ref().unchecked_ref()));
            onmousedown.forget();

            let move_state = drag_state.clone();
            let move_view = self.view.clone();
            let move_redraw = self.redraw_trigger.clone();
            let onmousemove = Closure::<dyn FnMut(_)>::new(move |evt: web_sys::MouseEvent| {
                let mut state = move_state.borrow_mut();
                let Some((last_x, last_y, moved)) = *state else { return };
                let (x, y) = (evt.offset_x() as f64, evt.offset_y() as f64);
                let (dx, dy) = (x - last_x, y - last_y);
                if moved || dx.abs() > PAN_THRESHOLD_PX || dy.abs() > PAN_THRESHOLD_PX {
                    move_view.borrow_mut().pan(dx, dy);
                    move_redraw.update(|n| n + 1);
                    *state = Some((x, y, true));
                }
            });
            canvas_el.set_onmousemove(Some(onmousemove.as_ref().unchecked_ref()));
            onmousemove.forget();

            let up_state = drag_state.clone();
            let up_positions = self.positions.clone();
            let up_data = self.data.clone();
            let up_callback = self.on_click.clone();
            let onmouseup = Closure::<dyn FnMut(_)>::new(move |evt: web_sys::MouseEvent| {
                let moved = up_state.borrow().map(|(_, _, m)| m).unwrap_or(false);
                *up_state.borrow_mut() = None;
                if moved {
                    return;
                }
                let Some(callback) = up_callback.clone() else { return };
                let (cx, cy) = (evt.offset_x() as f64, evt.offset_y() as f64);
                let result = up_data.get();
                let hit = up_positions
                    .borrow()
                    .iter()
                    .position(|(x, y)| ((x - cx).powi(2) + (y - cy).powi(2)).sqrt() < 8.0);
                if let Some(idx) = hit {
                    if let Some(row) = result.rows.get(idx) {
                        let obj: serde_json::Map<String, serde_json::Value> = result
                            .columns
                            .iter()
                            .zip(row.iter())
                            .map(|(c, v)| (c.clone(), v.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null)))
                            .collect();
                        let json = serde_json::Value::Object(obj).to_string();
                        let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(&json));
                    }
                }
            });
            canvas_el.set_onmouseup(Some(onmouseup.as_ref().unchecked_ref()));
            onmouseup.forget();

            Ok(())
        }
    } // impl CanvasMap

    /// Projects `result` into data-space points (`project`, using the
    /// canvas's pixel `width`/`height` captured once at construction time —
    /// unaffected by `view`, matching `webgpu/mod.rs`'s "pan/zoom applied
    /// entirely on the CPU before upload" convention) and draws them through
    /// `view` for the current backend. `positions_out` always ends up holding
    /// *screen-space* coordinates (post-`view.to_screen`) since that's what
    /// `install_pointer_handlers`'s click hit-test compares mouse events
    /// against directly.
    fn draw(
        renderer: &Renderer,
        result: &QueryResult,
        location_field: &str,
        cluster: bool,
        heatmap: bool,
        width: f64,
        height: f64,
        view: ViewTransform,
        positions_out: &Rc<RefCell<Vec<(f64, f64)>>>,
    ) {
        let Some(col_idx) = result.columns.iter().position(|c| c == location_field) else { return };

        let points: Vec<(f64, f64)> = result
            .rows
            .iter()
            .filter_map(|row| row.get(col_idx).and_then(|c| c.as_deref()).and_then(parse_lat_lon))
            .map(|(lat, lon)| project(lat, lon, width, height))
            .collect();

        let mut positions = positions_out.borrow_mut();
        positions.clear();

        match renderer {
            Renderer::Canvas2d(canvas) => {
                canvas.clear();

                if heatmap {
                    draw_heatmap(canvas, &points, 40.0, width, height, view);
                }

                if cluster {
                    for (cx, cy, members) in cluster_grid(&points, 40.0) {
                        let (sx, sy) = view.to_screen(cx, cy);
                        let radius = (6.0 + (members.len() as f64).sqrt() * 3.0) * view.scale;
                        canvas.circle(sx, sy, radius, "rgba(37,99,235,0.75)");
                        if members.len() > 1 {
                            canvas.text(sx - 4.0, sy + 4.0, &members.len().to_string(), "#fff");
                        }
                        positions.push((sx, sy));
                    }
                } else if !heatmap {
                    for &(x, y) in &points {
                        let (sx, sy) = view.to_screen(x, y);
                        canvas.circle(sx, sy, MARKER_RADIUS_PX * view.scale, "rgba(37,99,235,0.9)");
                        positions.push((sx, sy));
                    }
                } else {
                    for &(x, y) in &points {
                        positions.push(view.to_screen(x, y));
                    }
                }
            }
            // `CanvasMap::new` only ever hands out a `Renderer::Gpu` when
            // both `cluster` and `heatmap` are false (see its doc comment),
            // so this is always the plain-point-marker case.
            Renderer::Gpu(gpu) => {
                if width <= 0.0 || height <= 0.0 {
                    return;
                }
                let normalized: Vec<(f64, f64)> = points
                    .iter()
                    .map(|&(x, y)| {
                        let (sx, sy) = view.to_screen(x, y);
                        positions.push((sx, sy));
                        (sx / width, sy / height)
                    })
                    .collect();
                gpu.draw_markers_normalized(&normalized, MARKER_RADIUS_PX * view.scale);
            }
        }
    }

    fn draw_heatmap(canvas: &Canvas2d, points: &[(f64, f64)], bandwidth: f64, width: f64, height: f64, view: ViewTransform) {
        let grid_w = (width / 4.0).max(1.0) as usize;
        let grid_h = (height / 4.0).max(1.0) as usize;
        let density = kernel_density(points, width, height, grid_w, grid_h, bandwidth);
        let cell_w = width / grid_w as f64;
        let cell_h = height / grid_h as f64;
        for gy in 0..grid_h {
            for gx in 0..grid_w {
                let t = density[gy * grid_w + gx];
                if t <= 0.0 {
                    continue;
                }
                let color = heat_color(t);
                let style = format!(
                    "rgba({},{},{},{})",
                    u8::from_str_radix(&color[1..3], 16).unwrap_or(0),
                    u8::from_str_radix(&color[3..5], 16).unwrap_or(0),
                    u8::from_str_radix(&color[5..7], 16).unwrap_or(0),
                    (t * 0.7).clamp(0.0, 0.7),
                );
                canvas.ctx.set_fill_style_str(&style);
                let (sx, sy) = view.to_screen(gx as f64 * cell_w, gy as f64 * cell_h);
                canvas.ctx.fill_rect(sx, sy, cell_w * view.scale, cell_h * view.scale);
            }
        }
    }
} // mod wasm_impl

#[cfg(target_arch = "wasm32")]
pub use wasm_impl::CanvasMap;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_maps_corners() {
        assert_eq!(project(90.0, -180.0, 360.0, 180.0), (0.0, 0.0));
        assert_eq!(project(-90.0, 180.0, 360.0, 180.0), (360.0, 180.0));
        assert_eq!(project(0.0, 0.0, 360.0, 180.0), (180.0, 90.0));
    }

    #[test]
    fn cluster_grid_groups_nearby_points() {
        let points = vec![(1.0, 1.0), (2.0, 2.0), (100.0, 100.0)];
        let clusters = cluster_grid(&points, 40.0);
        assert_eq!(clusters.len(), 2);
        let sizes: Vec<usize> = clusters.iter().map(|(_, _, m)| m.len()).collect();
        assert!(sizes.contains(&2));
        assert!(sizes.contains(&1));
    }

    #[test]
    fn parse_lat_lon_parses_and_rejects() {
        assert_eq!(parse_lat_lon("12.5, -8.25"), Some((12.5, -8.25)));
        assert_eq!(parse_lat_lon("garbage"), None);
    }

    #[test]
    fn kernel_density_normalizes_to_peak_one() {
        let pts = vec![(100.0, 100.0), (100.0, 100.0), (300.0, 300.0)];
        let grid = kernel_density(&pts, 400.0, 400.0, 10, 10, 40.0);
        assert_eq!(grid.len(), 100);
        let peak = grid.iter().cloned().fold(0.0f64, f64::max);
        assert!((peak - 1.0).abs() < 1e-9, "densest cell must normalize to 1.0, got {peak}");
        // Empty grid is all zeros.
        assert_eq!(kernel_density(&[], 400.0, 400.0, 10, 10, 40.0), vec![0.0; 100]);
    }

    #[test]
    fn kernel_density_empty_grid_is_safe() {
        let empty: Vec<f64> = vec![];
        assert_eq!(kernel_density(&[(1.0, 1.0)], 400.0, 400.0, 0, 10, 40.0), empty);
        assert_eq!(kernel_density(&[(1.0, 1.0)], 400.0, 400.0, 10, 0, 40.0), empty);
    }

    #[test]
    fn heat_color_ramps_blue_to_red() {
        assert_eq!(heat_color(0.0), "#2563eb");
        assert_eq!(heat_color(1.0), "#ef4444");
        // Mid-range stays within the hex palette bounds.
        let mid = heat_color(0.5);
        assert!(mid.starts_with('#') && mid.len() == 7);
        // Out-of-range clamps.
        assert_eq!(heat_color(-1.0), "#2563eb");
        assert_eq!(heat_color(2.0), "#ef4444");
    }
}
