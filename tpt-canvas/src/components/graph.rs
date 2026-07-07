//! `Canvas.Graph` — force-directed graph visualisation (Plexus-native,
//! Phase 9). Runs a fixed-iteration Fruchterman-Reingold simulation
//! client-side; "native traversal controls" from the spec is a documented
//! scope cut (no interactive path-query UI — Plexus's traversal table
//! functions are already queryable via plain SQL, which is what `edges_sql`
//! below runs).
//!
//! Two separate queries rather than one, since Plexus returns vertices and
//! edges as distinct row shapes: `nodes_sql` must select an `id` column,
//! `edges_sql` must select `(from_id, to_id)`.

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

#[cfg(target_arch = "wasm32")]
mod wasm_impl {
    use std::cell::RefCell;
    use std::rc::Rc;

    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;

    use super::{fruchterman_reingold, resolve_edges};
    use crate::client::{KeystoneClient, QueryResult};
    use crate::render::Canvas2d;

#[wasm_bindgen]
pub struct CanvasGraph {
    _canvas: Canvas2d,
    positions: Rc<RefCell<Vec<(f64, f64)>>>,
    _node_ws: Option<web_sys::WebSocket>,
    _edge_ws: Option<web_sys::WebSocket>,
    _effect: Rc<dyn std::any::Any>,
}

#[wasm_bindgen]
impl CanvasGraph {
    #[wasm_bindgen(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        canvas_id: &str,
        http_base: &str,
        ws_base: &str,
        nodes_sql: &str,
        edges_sql: &str,
        realtime_topic: &str,
    ) -> Result<CanvasGraph, JsValue> {
        let canvas = Canvas2d::mount(canvas_id).map_err(|e| JsValue::from_str(&e))?;
        let client = Rc::new(KeystoneClient::new(http_base, ws_base));
        let topic = if realtime_topic.is_empty() { None } else { Some(realtime_topic) };
        let (nodes, node_ws) = client.use_keystone_query(nodes_sql, topic);
        let (edges, edge_ws) = client.use_keystone_query(edges_sql, topic);

        let positions = Rc::new(RefCell::new(Vec::new()));
        let redraw_canvas = Canvas2d { ctx: canvas.ctx.clone(), width: canvas.width, height: canvas.height };
        let redraw_positions = positions.clone();
        let effect = crate::reactive::create_effect(move || {
            let node_result = nodes.get();
            let edge_result = edges.get();
            draw(&redraw_canvas, &node_result, &edge_result, &redraw_positions);
        });

        let graph = CanvasGraph { _canvas: canvas, positions, _node_ws: node_ws, _edge_ws: edge_ws, _effect: effect };
        graph.install_drag_handler(canvas_id)?;
        Ok(graph)
    }

    fn install_drag_handler(&self, canvas_id: &str) -> Result<(), JsValue> {
        let document = web_sys::window().unwrap().document().unwrap();
        let element = document.get_element_by_id(canvas_id).ok_or("canvas element missing")?;
        let canvas_el: web_sys::HtmlCanvasElement = element.dyn_into()?;
        let positions = self.positions.clone();
        let dragging = Rc::new(RefCell::new(None::<usize>));

        let down_positions = positions.clone();
        let down_dragging = dragging.clone();
        let onmousedown = Closure::<dyn FnMut(_)>::new(move |evt: web_sys::MouseEvent| {
            let (cx, cy) = (evt.offset_x() as f64, evt.offset_y() as f64);
            *down_dragging.borrow_mut() = down_positions.borrow().iter().position(|(x, y)| ((x - cx).powi(2) + (y - cy).powi(2)).sqrt() < 10.0);
        });
        canvas_el.set_onmousedown(Some(onmousedown.as_ref().unchecked_ref()));
        onmousedown.forget();

        let move_positions = positions.clone();
        let move_dragging = dragging.clone();
        let onmousemove = Closure::<dyn FnMut(_)>::new(move |evt: web_sys::MouseEvent| {
            if let Some(idx) = *move_dragging.borrow() {
                if let Some(p) = move_positions.borrow_mut().get_mut(idx) {
                    *p = (evt.offset_x() as f64, evt.offset_y() as f64);
                }
            }
        });
        canvas_el.set_onmousemove(Some(onmousemove.as_ref().unchecked_ref()));
        onmousemove.forget();

        let onmouseup = Closure::<dyn FnMut()>::new(move || {
            *dragging.borrow_mut() = None;
        });
        canvas_el.set_onmouseup(Some(onmouseup.as_ref().unchecked_ref()));
        onmouseup.forget();
        Ok(())
    }
}

fn draw(canvas: &Canvas2d, nodes: &QueryResult, edges: &QueryResult, positions_out: &Rc<RefCell<Vec<(f64, f64)>>>) {
    canvas.clear();
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
        *positions = fruchterman_reingold(node_ids.len(), &resolved_edges, canvas.width, canvas.height, 50);
    }

    for &(a, b) in &resolved_edges {
        canvas.line(positions[a].0, positions[a].1, positions[b].0, positions[b].1, "#94a3b8", 1.0);
    }
    for (i, &(x, y)) in positions.iter().enumerate() {
        canvas.circle(x, y, 8.0, "#16a34a");
        if let Some(label) = node_ids.get(i) {
            canvas.text(x + 10.0, y + 4.0, label, "#111");
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
    fn resolve_edges_maps_ids_to_indices() {
        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let edges = vec![("a".to_string(), "c".to_string()), ("b".to_string(), "missing".to_string())];
        assert_eq!(resolve_edges(&ids, &edges), vec![(0, 2)]);
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
