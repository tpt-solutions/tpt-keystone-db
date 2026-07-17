//! `Canvas.Document` — JSON tree viewer/editor (Canopy-native, Phase 10's
//! unified `jsonb` column type). DOM-built, like `CanvasVectorSearch`: a
//! nested tree is DOM structure, not something worth drawing on a canvas.
//!
//! Editing writes back via `UPDATE {table} SET {column} = jsonb_set({column},
//! '{path}', '{value}') WHERE {pk_column} = {pk_value}`, reusing the
//! `jsonb_set` function already implemented server-side in
//! `executor::eval` (Phase 10) rather than the client reconstructing and
//! sending a whole new document.

/// Builds a `UPDATE {table} SET {column} = jsonb_set({column}, '{path}', {value}) WHERE {pk_column} = '{pk}'`
/// statement for an in-place edit of one JSON leaf. The jsonb `path`, `value`,
/// and `pk` are all interpolated as *SQL string literals*, so each is escaped
/// by doubling single quotes (the Postgres/SQL standard rule). The `value`
/// undergoes an extra JSON-normalisation step: it is first parsed as a JSON
/// value (so a user typing a raw object/array literal like `{"k":2}` is kept
/// as a JSON object, not wrapped into a JSON *string*), then re-serialised to
/// a compact JSON text — this guarantees the embedded value is valid JSON and
/// that any embedded quotes/`}`/`\` cannot break the surrounding SQL. If the
/// input is not valid JSON it is treated as a plain string. This replaces the
/// prior naive interpolation that let a `'` in the user's input terminate the
/// string literal early.
pub fn build_jsonb_set(
    table: &str,
    column: &str,
    path: &str,
    pk_column: &str,
    pk: &str,
    value: &str,
) -> String {
    // `value` -> valid JSON text. Parse as JSON first so a raw object/array
    // literal stays a JSON value; fall back to a JSON string for plain text.
    let json_value = match serde_json::from_str::<serde_json::Value>(value) {
        Ok(v) => serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string()),
        Err(_) => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    };
    let json_escaped = json_value.replace('\'', "''");
    let path_escaped = path.replace('\'', "''");
    let pk_escaped = pk.replace('\'', "''");
    format!(
        "UPDATE {table} SET {column} = jsonb_set({column}, '{{{path}}}', '{value}') WHERE {pk_column} = '{pk}'",
        table = table,
        column = column,
        path = path_escaped,
        value = json_escaped,
        pk_column = pk_column,
        pk = pk_escaped,
    )
}

/// Flattens a JSON value into `(depth, key_path, display_value, is_leaf)`
/// rows in depth-first order, for building the tree DOM. `key_path` is a
/// Postgres jsonb path array literal (`{a,b,0}`) suitable for
/// `jsonb_set`'s second argument.
pub fn flatten_json(value: &serde_json::Value, path: &str, depth: usize) -> Vec<(usize, String, String, bool)> {
    let mut out = Vec::new();
    match value {
        serde_json::Value::Object(map) => {
            out.push((depth, path.to_string(), "{ ... }".to_string(), false));
            for (key, v) in map {
                let child_path = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path},{key}")
                };
                out.extend(flatten_json(v, &child_path, depth + 1));
            }
        }
        serde_json::Value::Array(arr) => {
            out.push((depth, path.to_string(), "[ ... ]".to_string(), false));
            for (i, v) in arr.iter().enumerate() {
                let child_path = if path.is_empty() {
                    i.to_string()
                } else {
                    format!("{path},{i}")
                };
                out.extend(flatten_json(v, &child_path, depth + 1));
            }
        }
        leaf => out.push((depth, path.to_string(), format!("{leaf}"), true)),
    }
    out
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
        // Inject the default design tokens once (idempotent) so the inline
        // styles below can reference `var(--tpt-*)`.
        crate::theme::apply_theme(&crate::theme::Theme::default());
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
        tree.set_attribute("style", "font:13px monospace;margin-bottom:8px;color:var(--tpt-text);").ok();
        for (depth, path, label, is_leaf) in flatten_json(&parsed, "", 0) {
            let Ok(line) = document.create_element("div") else { continue };
            let indent = depth * 16;
            line.set_attribute("style", &format!("padding-left:{indent}px;")).ok();
            line.set_text_content(Some(&label));

            if is_leaf {
                line.set_attribute("style", &format!("padding-left:{indent}px;cursor:pointer;color:var(--tpt-accent);")).ok();
                let client = client.clone();
                let table = table.to_string();
                let column = column.to_string();
                let pk_column = pk_column.to_string();
                let pk = pk.clone();
                let path = path.clone();
                let onclick = Closure::<dyn FnMut()>::new(move || {
                    let Some(window) = web_sys::window() else { return };
                    let Ok(Some(new_value)) = window.prompt_with_message(&format!("New value for {path} (JSON literal):")) else { return };
                    let sql = super::build_jsonb_set(&table, &column, &path, &pk_column, &pk, &new_value);
                    let client = client.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        if let Err(e) = client.query(&sql).await {
                            web_sys::console::error_1(&format!("tpt-keystone-canvas: document edit failed: {e}").into());
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
    fn build_jsonb_set_escapes_quotes_and_wraps_json() {
        // A value with a single quote must be quote-doubled, and the
        // value must be a valid JSON literal (quoted).
        let sql = build_jsonb_set("t", "doc", "a,b", "id", "x'1", "it's");
        assert!(sql.contains("'\"it''s\"'"), "value single quote must be doubled: {sql}");
        assert!(sql.contains("jsonb_set(doc, '{a,b}', '\"it''s\"')"), "value must be JSON-quoted: {sql}");
        assert!(sql.contains("WHERE id = 'x''1'"), "pk single quote must be doubled: {sql}");
    }

    #[test]
    fn build_jsonb_set_handles_nested_json_value() {
        // A raw JSON object typed by the user stays a JSON value (not a
        // JSON string) after parse-and-re-serialise normalisation.
        let sql = build_jsonb_set("t", "doc", "0", "id", "1", "{\"k\":2}");
        assert!(sql.contains("'{\"k\":2}'"), "object value must be JSON-quoted: {sql}");
    }

    #[test]
    fn flatten_json_walks_array_with_index_paths() {
        let value = json!([10, 20]);
        let rows = flatten_json(&value, "", 0);
        // Array container node, then each element with its index path.
        assert_eq!(
            rows,
            vec![
                (0, "".to_string(), "[ ... ]".to_string(), false),
                (1, "0".to_string(), "10".to_string(), true),
                (1, "1".to_string(), "20".to_string(), true),
            ]
        );
    }
}
