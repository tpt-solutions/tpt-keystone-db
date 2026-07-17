//! `Canvas.VectorSearch` — ANN result renderer (Prism-native, Phase 3
//! vector search). A ranked result set is fundamentally a list, not a
//! chart, so this is DOM-built (`web_sys::Document`), not canvas-drawn,
//! unlike the other four components.

use crate::client::QueryResult;

/// Sorts rows by `score_field` ascending (lower `<=>` distance = more
/// similar, matching the spec's example `ORDER BY similarity`) and returns
/// `(row_label, score)` pairs, where `row_label` is every non-score column
/// joined with " · " for display.
pub fn rank_rows(result: &QueryResult, score_field: &str) -> Vec<(String, f64)> {
    let Some(score_idx) = result.columns.iter().position(|c| c == score_field) else { return vec![] };
    let mut ranked: Vec<(String, f64)> = result
        .rows
        .iter()
        .map(|row| {
            let score: f64 = row.get(score_idx).and_then(|c| c.as_deref()).and_then(|s| s.parse().ok()).unwrap_or(f64::MAX);
            let label = result
                .columns
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != score_idx)
                .filter_map(|(i, _)| row.get(i).and_then(|c| c.as_deref()))
                .collect::<Vec<_>>()
                .join(" \u{b7} ");
            (label, score)
        })
        .collect();
    ranked.sort_by(|a, b| a.1.total_cmp(&b.1));
    ranked
}

#[cfg(target_arch = "wasm32")]
mod wasm_impl {
    use std::rc::Rc;

    use wasm_bindgen::prelude::*;

    use super::rank_rows;
    use crate::client::{KeystoneClient, QueryResult};

#[wasm_bindgen]
pub struct CanvasVectorSearch {
    _ws: Option<web_sys::WebSocket>,
    _effect: Rc<dyn std::any::Any>,
}

#[wasm_bindgen]
impl CanvasVectorSearch {
    #[wasm_bindgen(constructor)]
    pub fn new(container_id: &str, http_base: &str, ws_base: &str, sql: &str, score_field: &str, realtime_topic: &str) -> Result<CanvasVectorSearch, JsValue> {
        crate::theme::apply_theme(&crate::theme::Theme::default());
        let client = Rc::new(KeystoneClient::new(http_base, ws_base));
        let topic = if realtime_topic.is_empty() { None } else { Some(realtime_topic) };
        let (data, ws) = client.use_keystone_query(sql, topic);

        let container_id = container_id.to_string();
        let score_field = score_field.to_string();
        let effect = crate::reactive::create_effect(move || {
            let result = data.get();
            render(&container_id, &result, &score_field);
        });

        Ok(CanvasVectorSearch { _ws: ws, _effect: effect })
    }
}

fn render(container_id: &str, result: &QueryResult, score_field: &str) {
    let Some(window) = web_sys::window() else { return };
    let Some(document) = window.document() else { return };
    let Some(container) = document.get_element_by_id(container_id) else { return };
    container.set_inner_html("");

    let ranked = rank_rows(result, score_field);
    let max_score = ranked.iter().map(|(_, s)| *s).fold(f64::MIN, f64::max).max(f64::EPSILON);

    for (label, score) in ranked {
        let Ok(row) = document.create_element("div") else { continue };
        let pct = ((1.0 - score / max_score) * 100.0).clamp(0.0, 100.0);
        row.set_attribute(
            "style",
            "display:flex;align-items:center;gap:8px;padding:4px 0;font:13px sans-serif;color:var(--tpt-text);",
        )
        .ok();
        row.set_inner_html(&format!(
            "<div style=\"flex:1\">{label}</div>\
             <div style=\"width:80px;height:8px;background:var(--tpt-surface);border-radius:4px;overflow:hidden\">\
                <div style=\"width:{pct:.0}%;height:100%;background:var(--tpt-accent)\"></div>\
             </div>\
             <div style=\"width:60px;text-align:right;color:var(--tpt-muted)\">{score:.4}</div>"
        ));
        let _ = container.append_child(&row);
    }
}
} // mod wasm_impl

#[cfg(target_arch = "wasm32")]
pub use wasm_impl::CanvasVectorSearch;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_rows_sorts_ascending_by_score() {
        let result = QueryResult {
            columns: vec!["id".into(), "similarity".into()],
            rows: vec![
                vec![Some("a".into()), Some("0.9".into())],
                vec![Some("b".into()), Some("0.1".into())],
                vec![Some("c".into()), Some("0.5".into())],
            ],
        };
        let ranked = rank_rows(&result, "similarity");
        assert_eq!(ranked.iter().map(|(l, _)| l.as_str()).collect::<Vec<_>>(), vec!["b", "c", "a"]);
    }

    #[test]
    fn rank_rows_missing_field_is_empty() {
        let result = QueryResult { columns: vec!["id".into()], rows: vec![] };
        assert_eq!(rank_rows(&result, "similarity"), vec![]);
    }
}
