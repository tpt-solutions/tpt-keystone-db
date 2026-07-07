//! `Canvas.Map` — geospatial point renderer. Equirectangular projection
//! (not a real Mercator/tile-based map — there's no basemap tile layer at
//! all, just points on a blank canvas) with grid-bucket clustering and
//! click hit-testing, per the Phase 13 checklist's "Clustering, heatmaps,
//! spatial filter queries built-in" — heatmaps and spatial filter queries
//! (`ST_*` predicates already exist server-side per Phase 6/Meridian) are a
//! documented scope cut; only point rendering + clustering are implemented.

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

/// Parses a `"lat,lon"` cell value out of a query row (the shape the spec's
/// `locationField` implies for a point column rendered as text).
fn parse_lat_lon(cell: &str) -> Option<(f64, f64)> {
    let (lat, lon) = cell.split_once(',')?;
    Some((lat.trim().parse().ok()?, lon.trim().parse().ok()?))
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

    use super::{cluster_grid, parse_lat_lon, project};
    use crate::client::{KeystoneClient, QueryResult};
    use crate::reactive::Signal;
    use crate::render::Canvas2d;

#[wasm_bindgen]
pub struct CanvasMap {
    // Unused after construction — the redraw effect closes over its own
    // clones (`redraw_canvas`, `redraw_field`, `cluster` by value) instead
    // of borrowing these. Kept on the struct anyway since a future feature
    // (e.g. reading the current `location_field` back out to JS) would want
    // them, and `_`-prefixing documents "intentionally unused" the same way
    // `_ws`/`_effect` already do on every other component.
    _canvas: Canvas2d,
    _location_field: String,
    _cluster: bool,
    data: Signal<QueryResult>,
    on_click: Option<js_sys::Function>,
    positions: Rc<RefCell<Vec<(f64, f64)>>>,
    _ws: Option<web_sys::WebSocket>,
    _effect: Rc<dyn std::any::Any>,
}

#[wasm_bindgen]
impl CanvasMap {
    /// Mounts onto `<canvas id="{canvas_id}">`, running `sql` (expected to
    /// select `location_field` as a `"lat,lon"` text column) once and
    /// re-running it on every message from `realtime_topic` (pass `""` for
    /// no live updates). `on_click(row_json: string)` fires when a rendered
    /// marker is clicked.
    #[wasm_bindgen(constructor)]
    pub fn new(
        canvas_id: &str,
        http_base: &str,
        ws_base: &str,
        sql: &str,
        location_field: &str,
        realtime_topic: &str,
        cluster: bool,
        on_click: Option<js_sys::Function>,
    ) -> Result<CanvasMap, JsValue> {
        let canvas = Canvas2d::mount(canvas_id).map_err(|e| JsValue::from_str(&e))?;
        let client = Rc::new(KeystoneClient::new(http_base, ws_base));
        let topic = if realtime_topic.is_empty() { None } else { Some(realtime_topic) };
        let (data, ws) = client.use_keystone_query(sql, topic);

        let positions = Rc::new(RefCell::new(Vec::new()));
        let redraw_canvas = Canvas2d { ctx: canvas.ctx.clone(), width: canvas.width, height: canvas.height };
        let redraw_data = data.clone();
        let redraw_field = location_field.to_string();
        let redraw_positions = positions.clone();
        let effect = crate::reactive::create_effect(move || {
            let result = redraw_data.get();
            draw(&redraw_canvas, &result, &redraw_field, cluster, &redraw_positions);
        });

        let map = CanvasMap {
            _canvas: canvas,
            data,
            _location_field: location_field.to_string(),
            _cluster: cluster,
            on_click,
            positions,
            _ws: ws,
            _effect: effect,
        };
        map.install_click_handler(canvas_id)?;
        Ok(map)
    }

    fn install_click_handler(&self, canvas_id: &str) -> Result<(), JsValue> {
        let Some(callback) = self.on_click.clone() else { return Ok(()) };
        let document = web_sys::window().unwrap().document().unwrap();
        let element = document.get_element_by_id(canvas_id).ok_or("canvas element missing")?;
        let canvas_el: web_sys::HtmlCanvasElement = element.dyn_into()?;
        let positions = self.positions.clone();
        let data = self.data.clone();
        let onclick = Closure::<dyn FnMut(_)>::new(move |evt: web_sys::MouseEvent| {
            let (cx, cy) = (evt.offset_x() as f64, evt.offset_y() as f64);
            let result = data.get();
            let hit = positions.borrow().iter().position(|(x, y)| ((x - cx).powi(2) + (y - cy).powi(2)).sqrt() < 8.0);
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
        canvas_el.set_onclick(Some(onclick.as_ref().unchecked_ref()));
        onclick.forget();
        Ok(())
    }
}

fn draw(canvas: &Canvas2d, result: &QueryResult, location_field: &str, cluster: bool, positions_out: &Rc<RefCell<Vec<(f64, f64)>>>) {
    canvas.clear();
    let Some(col_idx) = result.columns.iter().position(|c| c == location_field) else { return };

    let points: Vec<(f64, f64)> = result
        .rows
        .iter()
        .filter_map(|row| row.get(col_idx).and_then(|c| c.as_deref()).and_then(parse_lat_lon))
        .map(|(lat, lon)| project(lat, lon, canvas.width, canvas.height))
        .collect();

    let mut positions = positions_out.borrow_mut();
    positions.clear();

    if cluster {
        for (cx, cy, members) in cluster_grid(&points, 40.0) {
            let radius = 6.0 + (members.len() as f64).sqrt() * 3.0;
            canvas.circle(cx, cy, radius, "rgba(37,99,235,0.75)");
            if members.len() > 1 {
                canvas.text(cx - 4.0, cy + 4.0, &members.len().to_string(), "#fff");
            }
            positions.push((cx, cy));
        }
    } else {
        for &(x, y) in &points {
            canvas.circle(x, y, 5.0, "rgba(37,99,235,0.9)");
            positions.push((x, y));
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
}
