//! `Canvas.AgentMonitor` — Mirror-native (Phase 17) live agent activity
//! monitor: a DOM-built session event timeline (from
//! `mirror_session_events(session_id)`) with step/back replay controls, plus
//! a Canvas2D-drawn per-agent latency bar chart (from
//! `mirror_agent_metrics(agent_id, t0, t1)`). Two independent
//! `KeystoneClient::use_keystone_query` subscriptions, same as every other
//! component — this one just happens to combine the DOM-built idiom
//! (`vector_search.rs`) and the Canvas2D-drawn idiom (`timeseries.rs`) in
//! one component, since "live activity + performance chart + replay
//! controls" doesn't fit either shape alone.
//!
//! Scope cut: replay controls are step-forward/step-back only (no seek —
//! see `mirror::replay::SessionCursor` server-side, which already has
//! `seek`; a UI seek control would need a click-to-jump interaction this
//! pass didn't build). "Inspect agent state at each point" means the
//! highlighted event's own `detail`/`input`/`output`/`error` text, the same
//! boundary `mirror::replay` draws server-side — no reconstruction of an
//! agent's internal memory beyond what it traced.

use crate::client::QueryResult;

#[derive(Debug, Clone, PartialEq)]
pub struct EventRow {
    pub offset: i64,
    pub ts: i64,
    pub agent_id: String,
    pub kind: String,
    pub detail: String,
    pub tool_name: Option<String>,
    pub error: Option<String>,
}

fn column(result: &QueryResult, name: &str) -> Option<usize> {
    result.columns.iter().position(|c| c == name)
}

fn cell<'a>(row: &'a [Option<String>], idx: Option<usize>) -> Option<&'a str> {
    idx.and_then(|i| row.get(i)).and_then(|c| c.as_deref())
}

/// Parses `mirror_session_events`'s columns (`evt_offset, ts, agent_id,
/// kind, detail, tool_name, input, output, error`) into rows, oldest first
/// (the table function itself doesn't guarantee row order — callers should
/// `ORDER BY evt_offset` in `events_sql`, but this sorts defensively too).
pub fn parse_events(result: &QueryResult) -> Vec<EventRow> {
    let offset_i = column(result, "evt_offset");
    let ts_i = column(result, "ts");
    let agent_i = column(result, "agent_id");
    let kind_i = column(result, "kind");
    let detail_i = column(result, "detail");
    let tool_i = column(result, "tool_name");
    let error_i = column(result, "error");

    let mut rows: Vec<EventRow> = result
        .rows
        .iter()
        .filter_map(|row| {
            Some(EventRow {
                offset: cell(row, offset_i)?.parse().ok()?,
                ts: cell(row, ts_i).and_then(|s| s.parse().ok()).unwrap_or(0),
                agent_id: cell(row, agent_i).unwrap_or("").to_string(),
                kind: cell(row, kind_i).unwrap_or("").to_string(),
                detail: cell(row, detail_i).unwrap_or("").to_string(),
                tool_name: cell(row, tool_i).map(str::to_string),
                error: cell(row, error_i).map(str::to_string),
            })
        })
        .collect();
    rows.sort_by_key(|e| e.offset);
    rows
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricPoint {
    pub latency_ms: f64,
    pub success: bool,
}

/// Parses `mirror_agent_metrics`'s columns (`agent_id, session_id,
/// latency_ms, tokens, success, ts`) into `(latency, success)` points, in
/// the order the query returned them.
pub fn parse_metrics(result: &QueryResult) -> Vec<MetricPoint> {
    let latency_i = column(result, "latency_ms");
    let success_i = column(result, "success");
    result
        .rows
        .iter()
        .filter_map(|row| {
            Some(MetricPoint {
                latency_ms: cell(row, latency_i)?.parse().ok()?,
                success: cell(row, success_i).map(|s| s != "0").unwrap_or(true),
            })
        })
        .collect()
}

/// Scales `value` into a bar height in `[0, chart_height]` against `max`
/// (the tallest bar in the series) — `0` if `max <= 0` rather than dividing
/// by zero (an all-zero-latency series draws as flat bars, not a crash).
pub fn scale_bar_height(value: f64, max: f64, chart_height: f64) -> f64 {
    if max <= 0.0 {
        return 0.0;
    }
    (value / max * chart_height).clamp(0.0, chart_height)
}

/// Color for a trace event's `kind`, matching `mirror::trace::TraceEvent`'s
/// open vocabulary — an unrecognized kind (a caller can record anything)
/// falls back to neutral gray rather than erroring.
pub fn event_kind_color(kind: &str) -> &'static str {
    match kind {
        "error" => "#e5484d",
        "tool_call" => "#3b82f6",
        "decision" => "#a78bfa",
        "outcome" => "#22c55e",
        _ => "#94a3b8",
    }
}

/// Clamps a step/back cursor move into `[-1, len-1]` — `-1` means "nothing
/// selected yet" (the initial state before the first `step()`), matching
/// `mirror::replay::SessionCursor`'s own "stays put at the ends" behavior
/// rather than wrapping around.
pub fn clamp_cursor(pos: i64, len: usize) -> i64 {
    if len == 0 {
        return -1;
    }
    pos.clamp(-1, len as i64 - 1)
}

#[cfg(target_arch = "wasm32")]
mod wasm_impl {
    use std::rc::Rc;

    use wasm_bindgen::prelude::*;

    use super::{clamp_cursor, event_kind_color, parse_events, parse_metrics, scale_bar_height};
    use crate::client::{KeystoneClient, QueryResult};
    use crate::reactive::Signal;
    use crate::render::Canvas2d;

#[wasm_bindgen]
pub struct CanvasAgentMonitor {
    _canvas: Canvas2d,
    _events_ws: Option<web_sys::WebSocket>,
    _metrics_ws: Option<web_sys::WebSocket>,
    _events_effect: Rc<dyn std::any::Any>,
    _metrics_effect: Rc<dyn std::any::Any>,
    events: Signal<QueryResult>,
    cursor: Signal<i64>,
}

#[wasm_bindgen]
impl CanvasAgentMonitor {
    #[wasm_bindgen(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        canvas_id: &str,
        container_id: &str,
        http_base: &str,
        ws_base: &str,
        events_sql: &str,
        metrics_sql: &str,
        realtime_topic: &str,
    ) -> Result<CanvasAgentMonitor, JsValue> {
        let canvas = Canvas2d::mount(canvas_id).map_err(|e| JsValue::from_str(&e))?;
        let client = Rc::new(KeystoneClient::new(http_base, ws_base));
        let topic = if realtime_topic.is_empty() { None } else { Some(realtime_topic) };

        let (events, events_ws) = client.use_keystone_query(events_sql, topic);
        let (metrics, metrics_ws) = client.use_keystone_query(metrics_sql, topic);

        let cursor = Signal::new(-1i64);

        let events_for_effect = events.clone();
        let cursor_for_effect = cursor.clone();
        let container_id_owned = container_id.to_string();
        let events_effect = crate::reactive::create_effect(move || {
            let result = events_for_effect.get();
            let cur = cursor_for_effect.get();
            render_events(&container_id_owned, &result, cur);
        });

        let redraw_canvas = Canvas2d { ctx: canvas.ctx.clone(), width: canvas.width, height: canvas.height };
        let metrics_effect = crate::reactive::create_effect(move || {
            let result = metrics.get();
            draw_metrics(&redraw_canvas, &result);
        });

        Ok(CanvasAgentMonitor {
            _canvas: canvas,
            _events_ws: events_ws,
            _metrics_ws: metrics_ws,
            _events_effect: events_effect,
            _metrics_effect: metrics_effect,
            events,
            cursor,
        })
    }

    /// Advances the replay cursor to the next event (stays at the last
    /// event if already there).
    pub fn step(&self) {
        let len = parse_events(&self.events.get()).len();
        let next = clamp_cursor(self.cursor.get() + 1, len);
        self.cursor.set(next);
    }

    /// Moves the replay cursor to the previous event (stays at `-1`,
    /// "nothing selected", if already at or before the start).
    pub fn back(&self) {
        let len = parse_events(&self.events.get()).len();
        let next = clamp_cursor(self.cursor.get() - 1, len);
        self.cursor.set(next);
    }
}

fn render_events(container_id: &str, result: &QueryResult, cursor: i64) {
    let Some(window) = web_sys::window() else { return };
    let Some(document) = window.document() else { return };
    let Some(container) = document.get_element_by_id(container_id) else { return };
    container.set_inner_html("");

    let events = parse_events(result);
    for (i, event) in events.iter().enumerate() {
        let Ok(row) = document.create_element("div") else { continue };
        let is_current = i as i64 == cursor;
        let border = if is_current { "2px solid #111" } else { "1px solid transparent" };
        row.set_attribute(
            "style",
            &format!("display:flex;gap:8px;align-items:baseline;padding:3px 4px;border:{border};font:12px sans-serif;"),
        ).ok();
        let tool = event.tool_name.as_deref().map(|t| format!(" ({t})")).unwrap_or_default();
        let err = event.error.as_deref().map(|e| format!(" — {e}")).unwrap_or_default();
        row.set_inner_html(&format!(
            "<div style=\"width:14px;height:14px;border-radius:50%;background:{color}\"></div>\
             <div style=\"width:70px;color:#64748b\">{agent}</div>\
             <div style=\"width:70px;font-weight:600\">{kind}{tool}</div>\
             <div style=\"flex:1\">{detail}{err}</div>",
            color = event_kind_color(&event.kind),
            agent = event.agent_id,
            kind = event.kind,
            tool = tool,
            detail = event.detail,
            err = err,
        ));
        let _ = container.append_child(&row);
    }
}

fn draw_metrics(canvas: &Canvas2d, result: &QueryResult) {
    canvas.clear();
    let points = parse_metrics(result);
    if points.is_empty() {
        return;
    }
    let max = points.iter().map(|p| p.latency_ms).fold(0.0, f64::max);
    let bar_width = (canvas.width / points.len() as f64).max(1.0);
    for (i, point) in points.iter().enumerate() {
        let height = scale_bar_height(point.latency_ms, max, canvas.height);
        let x = i as f64 * bar_width + bar_width / 2.0;
        let color = if point.success { "#2563eb" } else { "#e5484d" };
        canvas.line(x, canvas.height, x, canvas.height - height, color, (bar_width * 0.7).max(1.0));
    }
}
} // mod wasm_impl

#[cfg(target_arch = "wasm32")]
pub use wasm_impl::CanvasAgentMonitor;

#[cfg(test)]
mod tests {
    use super::*;

    fn events_result() -> QueryResult {
        QueryResult {
            columns: vec!["evt_offset", "ts", "agent_id", "kind", "detail", "tool_name", "input", "output", "error"]
                .into_iter().map(String::from).collect(),
            rows: vec![
                vec![Some("0".into()), Some("100".into()), Some("agent1".into()), Some("decision".into()), Some("plan".into()), None, None, None, None],
                vec![Some("1".into()), Some("200".into()), Some("agent1".into()), Some("tool_call".into()), Some("called x".into()), Some("x".into()), Some("in".into()), Some("out".into()), None],
                vec![Some("2".into()), Some("300".into()), Some("agent1".into()), Some("error".into()), Some("failed".into()), Some("y".into()), None, None, Some("boom".into())],
            ],
        }
    }

    #[test]
    fn parse_events_decodes_all_columns_and_sorts_by_offset() {
        let events = parse_events(&events_result());
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].kind, "error");
        assert_eq!(events[2].tool_name.as_deref(), Some("y"));
        assert_eq!(events[2].error.as_deref(), Some("boom"));
    }

    #[test]
    fn parse_metrics_decodes_success_flag() {
        let result = QueryResult {
            columns: vec!["agent_id", "session_id", "latency_ms", "tokens", "success", "ts"].into_iter().map(String::from).collect(),
            rows: vec![
                vec![Some("a".into()), Some("s".into()), Some("100.0".into()), Some("5".into()), Some("1".into()), Some("1".into())],
                vec![Some("a".into()), Some("s".into()), Some("50.0".into()), Some("5".into()), Some("0".into()), Some("2".into())],
            ],
        };
        let points = parse_metrics(&result);
        assert_eq!(points.len(), 2);
        assert!(points[0].success);
        assert!(!points[1].success);
    }

    #[test]
    fn scale_bar_height_handles_zero_max() {
        assert_eq!(scale_bar_height(10.0, 0.0, 100.0), 0.0);
        assert_eq!(scale_bar_height(50.0, 100.0, 100.0), 50.0);
    }

    #[test]
    fn event_kind_color_falls_back_for_unknown_kind() {
        assert_eq!(event_kind_color("error"), "#e5484d");
        assert_eq!(event_kind_color("something_custom"), "#94a3b8");
    }

    #[test]
    fn clamp_cursor_stays_within_bounds() {
        assert_eq!(clamp_cursor(-1, 3), -1);
        assert_eq!(clamp_cursor(5, 3), 2);
        assert_eq!(clamp_cursor(-5, 3), -1);
        assert_eq!(clamp_cursor(0, 0), -1, "empty session has no valid position");
    }
}
