//! `Canvas.Document` — JSON tree viewer/editor (Canopy-native, Phase 10's
//! unified `jsonb` column type). DOM-built, like `CanvasVectorSearch`: a
//! nested tree is DOM structure, not something worth drawing on a canvas.
//!
//! Editing writes back via `UPDATE {table} SET {column} = jsonb_set({column},
//! '{path}', '{value}') WHERE {pk_column} = {pk_value}`, reusing the
//! `jsonb_set` function already implemented server-side in
//! `executor::eval` (Phase 10) rather than the client reconstructing and
//! sending a whole new document.

/// Flattens a JSON value into `(depth, key_path, display_value, is_leaf)`
/// rows in depth-first order, for building the tree DOM. `key_path` is a
/// Postgres jsonb path array literal (`{a,b,0}`) suitable for
/// `jsonb_set`'s second argument.
pub fn flatten_json(value: &serde_json::Value, path: &str, depth: usize) -> Vec<(usize, String, String, bool)> {
    let mut out = Vec::new();
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child_path = if path.is_empty() { k.clone() } else { format!("{path},{k}") };
                push_node(&mut out, v, k, &child_path, depth);
            }
        }
        serde_json::Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                let child_path = if path.is_empty() { i.to_string() } else { format!("{path},{i}") };
                push_node(&mut out, v, &format!("[{i}]"), &child_path, depth);
            }
        }
        leaf => out.push((depth, path.to_string(), leaf.to_string(), true)),
    }
    out
}

fn push_node(out: &mut Vec<(usize, String, String, bool)>, v: &serde_json::Value, label: &str, path: &str, depth: usize) {
    match v {
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            out.push((depth, path.to_string(), format!("{label}:"), false));
            out.extend(flatten_json(v, path, depth + 1));
        }
        leaf => out.push((depth, path.to_string(), format!("{label}: {leaf}"), true)),
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm_impl {
    use std::rc::Rc;

    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;

    use super::flatten_json;
    use crate::client::{KeystoneClient, QueryResult};

#[wasm_bindgen]
pub struct CanvasDocument {
    _ws: Option<web_sys::WebSocket>,
    _effect: Rc<dyn std::any::Any>,
}

#[wasm_bindgen]
impl CanvasDocument {
    /// `sql` must select exactly the jsonb `column` plus `pk_column`
    /// (in that order is not required, both are looked up by name).
    #[wasm_bindgen(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        container_id: &str,
        http_base: &str,
        ws_base: &str,
        sql: &str,
        table: &str,
        column: &str,
        pk_column: &str,
        realtime_topic: &str,
    ) -> Result<CanvasDocument, JsValue> {
        let client = Rc::new(KeystoneClient::new(http_base, ws_base));
        let topic = if realtime_topic.is_empty() { None } else { Some(realtime_topic) };
        let (data, ws) = client.use_keystone_query(sql, topic);

        let container_id = container_id.to_string();
        let column = column.to_string();
        let pk_column = pk_column.to_string();
        let table = table.to_string();
        let write_client = client.clone();
        let effect = crate::reactive::create_effect(move || {
            let result = data.get();
            render(&container_id, &result, &column, &pk_column, &table, write_client.clone());
        });

        Ok(CanvasDocument { _ws: ws, _effect: effect })
    }
}

fn render(container_id: &str, result: &QueryResult, column: &str, pk_column: &str, table: &str, client: Rc<KeystoneClient>) {
    let Some(window) = web_sys::window() else { return };
    let Some(document) = window.document() else { return };
    let Some(container) = document.get_element_by_id(container_id) else { return };
    container.set_inner_html("");

    let Some(col_idx) = result.columns.iter().position(|c| c == column) else { return };
    let Some(pk_idx) = result.columns.iter().position(|c| c == pk_column) else { return };

    for row in &result.rows {
        let Some(Some(raw)) = row.get(col_idx) else { continue };
        let Some(Some(pk)) = row.get(pk_idx) else { continue };
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw) else { continue };

        let Ok(tree) = document.create_element("div") else { continue };
        tree.set_attribute("style", "font:13px monospace;margin-bottom:8px;").ok();
        for (depth, path, label, is_leaf) in flatten_json(&parsed, "", 0) {
            let Ok(line) = document.create_element("div") else { continue };
            let indent = depth * 16;
            line.set_attribute("style", &format!("padding-left:{indent}px;")).ok();
            line.set_text_content(Some(&label));

            if is_leaf {
                line.set_attribute("style", &format!("padding-left:{indent}px;cursor:pointer;color:#1e293b;")).ok();
                let client = client.clone();
                let table = table.to_string();
                let column = column.to_string();
                let pk_column = pk_column.to_string();
                let pk = pk.clone();
                let path = path.clone();
                let onclick = Closure::<dyn FnMut()>::new(move || {
                    let Some(window) = web_sys::window() else { return };
                    let Ok(Some(new_value)) = window.prompt_with_message(&format!("New value for {path} (JSON literal):")) else { return };
                    let sql = format!(
                        "UPDATE {table} SET {column} = jsonb_set({column}, '{{{path}}}', '{value}') WHERE {pk_column} = '{pk}'",
                        value = new_value.replace('\'', "''"),
                    );
                    let client = client.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        if let Err(e) = client.query(&sql).await {
                            web_sys::console::error_1(&format!("tpt-canvas: document edit failed: {e}").into());
                        }
                    });
                });
                line.dyn_ref::<web_sys::HtmlElement>().map(|el| el.set_onclick(Some(onclick.as_ref().unchecked_ref())));
                onclick.forget();
            }
            let _ = tree.append_child(&line);
        }
        let _ = container.append_child(&tree);
    }
}
} // mod wasm_impl

#[cfg(target_arch = "wasm32")]
pub use wasm_impl::CanvasDocument;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flatten_json_walks_nested_object() {
        let value = json!({"a": 1, "b": {"c": 2}});
        let rows = flatten_json(&value, "", 0);
        assert!(rows.iter().any(|(_, path, label, leaf)| path == "a" && label == "a: 1" && *leaf));
        assert!(rows.iter().any(|(_, path, label, leaf)| path == "b,c" && label == "c: 2" && *leaf));
        assert!(rows.iter().any(|(_, _, label, leaf)| label == "b:" && !*leaf));
    }

    #[test]
    fn flatten_json_walks_array_with_index_paths() {
        let value = json!([10, 20]);
        let rows = flatten_json(&value, "", 0);
        assert_eq!(rows, vec![(0, "0".to_string(), "[0]: 10".to_string(), true), (0, "1".to_string(), "[1]: 20".to_string(), true)]);
    }
}
