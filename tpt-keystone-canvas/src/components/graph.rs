//! `Canvas.Graph` — force-directed graph visualisation (Plexus-native,
//! Phase 9). Runs a fixed-iteration Fruchterman-Reingold simulation
//! client-side; "native traversal controls" from the spec is a documented
//! scope cut (no interactive path-query UI — Plexus's traversal table
//! functions are already queryable via plain SQL, which is what `edges_sql`
//! below runs). Phase 10 added a pan/zoom `ViewTransform` (mouse-drag pan
//! on empty space, wheel zoom, still-supported node dragging) and an opt-in
//! WebGPU renderer (`webgpu::GpuGraphRenderer`) — see `new`'s doc comment.
//!
//! Two separate queries rather than one, since Plexus returns vertices and
//! edges as distinct row shapes: `nodes_sql` must select an `id` column,
//! `edges_sql` must select `(from_id, to_id)`.

use crate::client::QueryResult;

/// One fixed-iteration Fruchterman-Reingold layout pass. Deterministic
/// initial placement (points on a circle) rather than random, so this is a
/// pure function of `(node_count, edges)` and unit-testable without a PRNG.
pub fn fruchterman_reingold(node_count: usize, edges: &[(usize, usize)], width: f64, height: f64, iterations: usize) -> Vec<(f64, f64)> {
    if node_count == 0 {
        return vec![];
    }
    let area = width * height;
    let k = (area / node_count as f64).sqrt();
    let mut pos: Vec<(f64, f64)> = (0..node_count)
        .map(|i| {
            let angle = 2.0 * std::f64::consts::PI * i as f64 / node_count as f64;
            (width / 2.0 + (width / 3.0) * angle.cos(), height / 2.0 + (height / 3.0) * angle.sin())
        })
        .collect();

    for iter in 0..iterations {
        let mut disp = vec![(0.0, 0.0); node_count];
        for i in 0..node_count {
            for j in 0..node_count {
                if i == j {
                    continue;
                }
                let dx = pos[i].0 - pos[j].0;
                let dy = pos[i].1 - pos[j].1;
                let dist = (dx * dx + dy * dy).sqrt().max(0.01);
                let force = k * k / dist;
                disp[i].0 += dx / dist * force;
                disp[i].1 += dy / dist * force;
            }
        }
        for &(a, b) in edges {
            if a >= node_count || b >= node_count {
                continue;
            }
            let dx = pos[a].0 - pos[b].0;
            let dy = pos[a].1 - pos[b].1;
            let dist = (dx * dx + dy * dy).sqrt().max(0.01);
            let force = dist * dist / k;
            disp[a].0 -= dx / dist * force;
            disp[a].1 -= dy / dist * force;
            disp[b].0 += dx / dist * force;
            disp[b].1 += dy / dist * force;
        }
        let temp = width.max(height) * (1.0 - iter as f64 / iterations as f64) * 0.1;
        for i in 0..node_count {
            let (dx, dy) = disp[i];
            let dist = (dx * dx + dy * dy).sqrt().max(0.01);
            pos[i].0 = (pos[i].0 + dx / dist * dist.min(temp)).clamp(0.0, width);
            pos[i].1 = (pos[i].1 + dy / dist * dist.min(temp)).clamp(0.0, height);
        }
    }
    pos
}

/// Maps each edge's `(from_id, to_id)` text values to indices into `node_ids`
/// (an edge referencing an id absent from `node_ids` is dropped).
fn resolve_edges(node_ids: &[String], edge_rows: &[(String, String)]) -> Vec<(usize, usize)> {
    edge_rows
        .iter()
        .filter_map(|(from, to)| {
            let a = node_ids.iter().position(|id| id == from)?;
            let b = node_ids.iter().position(|id| id == to)?;
            Some((a, b))
        })
        .collect()
}

/// Client-side translator for a GQL `MATCH` query result into the
/// `(node_ids, edges)` shape `CanvasGraph::draw` consumes. No server change
/// needed — `gql::execute_match` already returns ordinary rows; this just
/// reshapes them for the graph renderer.
///
/// Accepts two result shapes:
/// - a pre-joined edge table with `from`/`to` (or `from_id`/`to_id`)
///   columns (each row is one edge), or
/// - a node list with an `id` column (no edges — produces nodes only).
/// Either way it returns the sorted unique node ids plus resolved
/// `(usize, usize)` edges.
pub fn translate_match_result(result: &QueryResult) -> (Vec<String>, Vec<(usize, usize)>) {
    let find = |name: &str| result.columns.iter().position(|c| c.eq_ignore_ascii_case(name));

    // Shape A: edge rows via (from, to) / (from_id, to_id).
    if let (Some(fi), Some(ti)) = (find("from").or_else(|| find("from_id")), find("to").or_else(|| find("to_id"))) {
        let mut node_ids: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let edge_pairs: Vec<(String, String)> = result
            .rows
            .iter()
            .filter_map(|row| {
                let f = row.get(fi).and_then(|c| c.clone())?;
                let t = row.get(ti).and_then(|c| c.clone())?;
                if seen.insert(f.clone()) {
                    node_ids.push(f.clone());
                }
                if seen.insert(t.clone()) {
                    node_ids.push(t.clone());
                }
                Some((f, t))
            })
            .collect();
        let edges = resolve_edges(&node_ids, &edge_pairs);
        return (node_ids, edges);
    }

    // Shape B: a node list with `id`.
    if let Some(id_i) = find("id") {
        let node_ids: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| row.get(id_i).and_then(|c| c.clone()))
            .collect();
        return (node_ids, vec![]);
    }

    (vec![], vec![])
}

#[cfg(target_arch = "wasm32")]
mod wasm_impl {
    use std::cell::RefCell;
    use std::rc::Rc;

    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;

    use super::{fruchterman_reingold, resolve_edges, translate_match_result};
    use crate::client::{KeystoneClient, QueryResult};
    use crate::reactive::Signal;
    use crate::render::{canvas_dimensions, Canvas2d};
    use crate::view_transform::{zoom_factor_from_wheel_delta, ViewTransform};
    use crate::webgpu::GpuGraphRenderer;

    /// Fixed on-screen node quad half-width, CSS pixels — matches the
    /// existing Canvas2D node radius (`canvas.circle(x, y, 8.0, ...)`).
    const NODE_RADIUS_PX: f64 = 8.0;
    /// Mousedown-vs-node hit-test radius, CSS pixels (converted to data
    /// space via `ViewTransform::screen_to_data_distance` before comparing
    /// against `positions`, which are stored in data space).
    const NODE_HIT_RADIUS_PX: f64 = 10.0;

    /// Which backend ended up drawing this canvas — decided once,
    /// asynchronously (see `CanvasGraph::new`'s doc comment).
    enum Renderer {
        Canvas2d(Canvas2d),
        Gpu(GpuGraphRenderer),
    }

    #[wasm_bindgen]
    pub struct CanvasGraph {
        positions: Rc<RefCell<Vec<(f64, f64)>>>,
        view: Rc<RefCell<ViewTransform>>,
        redraw_trigger: Signal<u32>,
        _node_ws: Option<web_sys::WebSocket>,
        _edge_ws: Option<web_sys::WebSocket>,
        _effect: Rc<dyn std::any::Any>,
    }

    /// Races `GpuGraphRenderer::try_init` against a Canvas2D fallback
    /// exactly like `CanvasTimeSeries`/`CanvasMap` — nothing is drawn on
    /// `canvas_id` until the race resolves. Shared by `new` and
    /// `new_from_match` since both need identical setup here. Returns the
    /// (still-empty) renderer slot alongside a fresh `view`/`redraw_trigger`
    /// and this canvas's pixel dimensions (captured once via
    /// `canvas_dimensions`, unaffected by which backend wins — see
    /// `render::canvas_dimensions`'s doc comment for why this can't be read
    /// off either context type directly).
    fn init_renderer(canvas_id: &str) -> (Rc<RefCell<Option<Renderer>>>, Rc<RefCell<ViewTransform>>, Signal<u32>, f64, f64) {
        let (width, height) = canvas_dimensions(canvas_id).unwrap_or((0.0, 0.0));
        let renderer: Rc<RefCell<Option<Renderer>>> = Rc::new(RefCell::new(None));
        let redraw_trigger: Signal<u32> = Signal::new(0);
        let view: Rc<RefCell<ViewTransform>> = Rc::new(RefCell::new(ViewTransform::identity()));

        let init_renderer = renderer.clone();
        let init_canvas_id = canvas_id.to_string();
        let init_redraw_trigger = redraw_trigger.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let chosen = match GpuGraphRenderer::try_init(&init_canvas_id).await {
                Some(gpu) => Some(Renderer::Gpu(gpu)),
                // WebGPU wasn't available/negotiation failed *before* any
                // context was requested on this canvas — `"2d"` is still
                // free to request.
                None => Canvas2d::mount(&init_canvas_id).ok().map(Renderer::Canvas2d),
            };
            *init_renderer.borrow_mut() = chosen;
            init_redraw_trigger.update(|n| n + 1);
        });

        (renderer, view, redraw_trigger, width, height)
    }

    #[wasm_bindgen]
    impl CanvasGraph {
        /// Mounts onto `<canvas id="{canvas_id}">`, running `nodes_sql`/
        /// `edges_sql` once and re-running both on every message from
        /// `realtime_topic` (pass `""` for no live updates).
        ///
        /// Rendering backend: races `GpuGraphRenderer::try_init` against a
        /// Canvas2D fallback (see `init_renderer`) — node-id text labels
        /// only ever render on the Canvas2D path (no glyph atlas in the
        /// WebGPU PoC, see `webgpu::GpuGraphRenderer`'s module docs).
        #[wasm_bindgen(constructor)]
        pub fn new(
            canvas_id: &str,
            http_base: &str,
            ws_base: &str,
            nodes_sql: &str,
            edges_sql: &str,
            realtime_topic: &str,
        ) -> Result<CanvasGraph, JsValue> {
            let client = Rc::new(KeystoneClient::new(http_base, ws_base));
            let topic = if realtime_topic.is_empty() { None } else { Some(realtime_topic) };
            let (nodes, node_ws) = client.use_keystone_query(nodes_sql, topic);
            let (edges, edge_ws) = client.use_keystone_query(edges_sql, topic);

            let (renderer, view, redraw_trigger, width, height) = init_renderer(canvas_id);

            let positions = Rc::new(RefCell::new(Vec::new()));
            let draw_nodes = nodes.clone();
            let draw_edges = edges.clone();
            let draw_positions = positions.clone();
            let draw_renderer = renderer.clone();
            let draw_view = view.clone();
            let draw_redraw_trigger = redraw_trigger.clone();
            let effect = crate::reactive::create_effect(move || {
                let node_result = draw_nodes.get();
                let edge_result = draw_edges.get();
                let _ = draw_redraw_trigger.get(); // subscribe: re-run once the renderer resolves or the view pans/zooms/drags
                if let Some(r) = draw_renderer.borrow().as_ref() {
                    let view_snapshot = *draw_view.borrow();
                    draw(r, &node_result, &edge_result, &draw_positions, view_snapshot, width, height);
                }
            });

            let graph = CanvasGraph { positions, view, redraw_trigger, _node_ws: node_ws, _edge_ws: edge_ws, _effect: effect };
            graph.install_pointer_handlers(canvas_id)?;
            Ok(graph)
        }

        /// Like `new`, but consumes the result of a single GQL `MATCH` query
        /// (see `gql::execute_match` server-side) which returns either edge
        /// rows (`from`/`to`, or `from_id`/`to_id`) or a node list (`id`).
        /// `translate_match_result` reshapes it client-side into the node/edge
        /// shapes `draw` expects — no server change required. Exposed to JS as
        /// `CanvasGraph.fromMatch(...)` (wasm-bindgen allows only one
        /// `#[constructor]`, so this is a static factory rather than a second ctor).
        #[wasm_bindgen(js_name = "fromMatch")]
        #[allow(clippy::too_many_arguments)]
        pub fn new_from_match(
            canvas_id: &str,
            http_base: &str,
            ws_base: &str,
            match_sql: &str,
            realtime_topic: &str,
        ) -> Result<CanvasGraph, JsValue> {
            let client = Rc::new(KeystoneClient::new(http_base, ws_base));
            let topic = if realtime_topic.is_empty() { None } else { Some(realtime_topic) };
            let (data, node_ws) = client.use_keystone_query(match_sql, topic);

            let (renderer, view, redraw_trigger, width, height) = init_renderer(canvas_id);

            let positions = Rc::new(RefCell::new(Vec::new()));
            let draw_data = data.clone();
            let draw_positions = positions.clone();
            let draw_renderer = renderer.clone();
            let draw_view = view.clone();
            let draw_redraw_trigger = redraw_trigger.clone();
            let effect = crate::reactive::create_effect(move || {
                let result = draw_data.get();
                let _ = draw_redraw_trigger.get();
                let (node_ids, edges) = translate_match_result(&result);
                let nodes = QueryResult {
                    columns: vec!["id".into()],
                    rows: node_ids.iter().map(|id| vec![Some(id.clone())]).collect(),
                };
                let edges = QueryResult {
                    columns: vec!["from".into(), "to".into()],
                    rows: edges
                        .iter()
                        .map(|(a, b)| vec![Some(node_ids[*a].clone()), Some(node_ids[*b].clone())])
                        .collect(),
                };
                if let Some(r) = draw_renderer.borrow().as_ref() {
                    let view_snapshot = *draw_view.borrow();
                    draw(r, &nodes, &edges, &draw_positions, view_snapshot, width, height);
                }
            });

            let graph = CanvasGraph { positions, view, redraw_trigger, _node_ws: node_ws, _edge_ws: None, _effect: effect };
            graph.install_pointer_handlers(canvas_id)?;
            Ok(graph)
        }

        /// Wires wheel-to-zoom and a mousedown/mousemove/mouseup pointer
        /// state machine: mousedown hit-tests against `positions` (data
        /// space) via `view.to_data`/`screen_to_data_distance` to decide
        /// node-drag vs. pan; a node-drag mousemove writes the new node
        /// position through `view.to_data` (so it lands in the same data
        /// space `positions` is stored in and `draw`'s `view.to_screen`
        /// call places it back correctly), while a pan mousemove just
        /// shifts `view`'s translation by the raw screen-space delta. Both
        /// cases fire `redraw_trigger` on every move so dragging a node (or
        /// panning) redraws immediately instead of waiting for an unrelated
        /// data refetch.
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

            // `(last_x, last_y, dragging_node)` screen coords; `dragging_node
            // = None` means the gesture pans the view instead of moving a
            // node. `None` overall (not `Some((.., .., None))`) means no
            // button is currently down.
            let drag_state: Rc<RefCell<Option<(f64, f64, Option<usize>)>>> = Rc::new(RefCell::new(None));

            let down_positions = self.positions.clone();
            let down_view = self.view.clone();
            let down_state = drag_state.clone();
            let onmousedown = Closure::<dyn FnMut(_)>::new(move |evt: web_sys::MouseEvent| {
                let (x, y) = (evt.offset_x() as f64, evt.offset_y() as f64);
                let view = *down_view.borrow();
                let (data_x, data_y) = view.to_data(x, y);
                let threshold = view.screen_to_data_distance(NODE_HIT_RADIUS_PX);
                let hit = down_positions
                    .borrow()
                    .iter()
                    .position(|(px, py)| ((px - data_x).powi(2) + (py - data_y).powi(2)).sqrt() < threshold);
                *down_state.borrow_mut() = Some((x, y, hit));
            });
            canvas_el.set_onmousedown(Some(onmousedown.as_ref().unchecked_ref()));
            onmousedown.forget();

            let move_state = drag_state.clone();
            let move_positions = self.positions.clone();
            let move_view = self.view.clone();
            let move_redraw = self.redraw_trigger.clone();
            let onmousemove = Closure::<dyn FnMut(_)>::new(move |evt: web_sys::MouseEvent| {
                let mut state = move_state.borrow_mut();
                let Some((last_x, last_y, node)) = *state else { return };
                let (x, y) = (evt.offset_x() as f64, evt.offset_y() as f64);
                match node {
                    Some(idx) => {
                        let (data_x, data_y) = move_view.borrow().to_data(x, y);
                        if let Some(p) = move_positions.borrow_mut().get_mut(idx) {
                            *p = (data_x, data_y);
                        }
                    }
                    None => {
                        move_view.borrow_mut().pan(x - last_x, y - last_y);
                    }
                }
                move_redraw.update(|n| n + 1);
                *state = Some((x, y, node));
            });
            canvas_el.set_onmousemove(Some(onmousemove.as_ref().unchecked_ref()));
            onmousemove.forget();

            let onmouseup = Closure::<dyn FnMut()>::new(move || {
                *drag_state.borrow_mut() = None;
            });
            canvas_el.set_onmouseup(Some(onmouseup.as_ref().unchecked_ref()));
            onmouseup.forget();
            Ok(())
        }
    }

    /// Resolves `nodes`/`edges` into ids + index pairs, lays out `positions`
    /// (data space, persistent across redraws — only recomputed when the
    /// node count changes, so an in-progress drag or pan/zoom survives an
    /// unrelated data refetch) the first time a given node count is seen,
    /// and draws through `view` for the current backend.
    fn draw(
        renderer: &Renderer,
        nodes: &QueryResult,
        edges: &QueryResult,
        positions_out: &Rc<RefCell<Vec<(f64, f64)>>>,
        view: ViewTransform,
        width: f64,
        height: f64,
    ) {
        let Some(id_idx) = nodes.columns.first().map(|_| 0usize) else { return };
        let node_ids: Vec<String> = nodes.rows.iter().map(|row| row.get(id_idx).and_then(|c| c.clone()).unwrap_or_default()).collect();
        if node_ids.is_empty() {
            return;
        }

        let edge_pairs: Vec<(String, String)> = if edges.columns.len() >= 2 {
            edges
                .rows
                .iter()
                .filter_map(|row| Some((row.first()?.clone()?, row.get(1)?.clone()?)))
                .collect()
        } else {
            vec![]
        };
        let resolved_edges = resolve_edges(&node_ids, &edge_pairs);

        let mut positions = positions_out.borrow_mut();
        if positions.len() != node_ids.len() {
            *positions = fruchterman_reingold(node_ids.len(), &resolved_edges, width, height, 50);
        }

        match renderer {
            Renderer::Canvas2d(canvas) => {
                canvas.clear();
                for &(a, b) in &resolved_edges {
                    let (ax, ay) = view.to_screen(positions[a].0, positions[a].1);
                    let (bx, by) = view.to_screen(positions[b].0, positions[b].1);
                    canvas.line(ax, ay, bx, by, "#94a3b8", 1.0);
                }
                for (i, &(x, y)) in positions.iter().enumerate() {
                    let (sx, sy) = view.to_screen(x, y);
                    canvas.circle(sx, sy, NODE_RADIUS_PX * view.scale, "#16a34a");
                    if let Some(label) = node_ids.get(i) {
                        canvas.text(sx + 10.0, sy + 4.0, label, "#111");
                    }
                }
            }
            // Node-id text has no WebGPU path — see `webgpu::GpuGraphRenderer`'s
            // module docs.
            Renderer::Gpu(gpu) => {
                if width <= 0.0 || height <= 0.0 {
                    return;
                }
                let normalized_nodes: Vec<(f64, f64)> = positions
                    .iter()
                    .map(|&(x, y)| {
                        let (sx, sy) = view.to_screen(x, y);
                        (sx / width, sy / height)
                    })
                    .collect();
                gpu.draw_normalized(&normalized_nodes, &resolved_edges, NODE_RADIUS_PX * view.scale);
            }
        }
    }
} // mod wasm_impl

#[cfg(target_arch = "wasm32")]
pub use wasm_impl::CanvasGraph;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_match_result_reads_edge_rows() {
        let result = QueryResult {
            columns: vec!["from".into(), "to".into()],
            rows: vec![
                vec![Some("a".into()), Some("b".into())],
                vec![Some("b".into()), Some("c".into())],
            ],
        };
        let (ids, edges) = translate_match_result(&result);
        assert_eq!(ids, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(edges, vec![(0, 1), (1, 2)]);
    }

    #[test]
    fn translate_match_result_reads_node_list() {
        let result = QueryResult {
            columns: vec!["id".into()],
            rows: vec![vec![Some("x".into())], vec![Some("y".into())]],
        };
        let (ids, edges) = translate_match_result(&result);
        assert_eq!(ids, vec!["x".to_string(), "y".to_string()]);
        assert!(edges.is_empty());
    }

    #[test]
    fn translate_match_result_handles_empty() {
        let result = QueryResult { columns: vec!["other".into()], rows: vec![] };
        assert_eq!(translate_match_result(&result), (vec![], vec![]));
        assert_eq!(translate_match_result(&QueryResult::default()), (vec![], vec![]));
    }

    #[test]
    fn layout_keeps_all_nodes_in_bounds() {
        let positions = fruchterman_reingold(5, &[(0, 1), (1, 2), (2, 3), (3, 4)], 400.0, 300.0, 20);
        assert_eq!(positions.len(), 5);
        for (x, y) in positions {
            assert!((0.0..=400.0).contains(&x));
            assert!((0.0..=300.0).contains(&y));
        }
    }

    #[test]
    fn layout_of_zero_nodes_is_empty() {
        assert_eq!(fruchterman_reingold(0, &[], 100.0, 100.0, 10), vec![]);
    }
}
