//! `KeystoneClient` — the browser-side half of the bridge to
//! `tpt-keystone`'s Canvas HTTP query endpoint (`wire::http_query` on the
//! server) and its Flux WebSocket endpoint (`wire::websocket`). This is what
//! makes `use_keystone_query` genuinely run SQL and get live updates, rather
//! than components taking static in-memory data.
//!
//! Scope cut vs. the spec's `useKeystoneQuery`: there is no automatic
//! "subscribe to whatever the SQL touches" inference — a caller passes an
//! explicit `realtime_topic` naming the Flux topic to watch (e.g. the
//! `__cdc_<table>` topic Phase 11's native CDC already publishes to for a
//! given table). On a message from that topic, the query is *re-run from
//! scratch* and the signal replaced wholesale — invalidate-and-refetch, not
//! a fine-grained incremental patch of the existing rows.

use serde::{Deserialize, Serialize};

/// Plain data type, no `web-sys` dependency — kept compilable on the host
/// target (unlike the rest of this file) since every component's pure
/// layout/formatting functions take a `QueryResult` and are host-testable.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

/// Client-side auto-inference of the Flux topic to subscribe to from a SQL
/// `SELECT` (single-table only — see the module docs' scope cut). Extracts
/// the first identifier after `FROM`, ignoring `JOIN`/subqueries, and
/// returns the table's native CDC topic name `Phase 11` already publishes
/// to (`__cdc_<table>`). Returns `None` for anything that isn't a
/// single-table `SELECT ... FROM <table>` (multi-table `JOIN`s have no
/// single auto-target, and `INSERT`/`UPDATE`/`DELETE` are not CDC
/// sources here). Pure and host-testable.
pub fn infer_topic_from_sql(sql: &str) -> Option<String> {
    let sql = sql.trim();
    if !sql.to_ascii_lowercase().starts_with("select") {
        return None;
    }
    // Best-effort, ASCII-case-insensitive scan: find the first `FROM`
    // keyword, then the next whitespace-delimited token is the table.
    let from_idx = sql.to_ascii_lowercase().find(" from ")?;
    let after = &sql[from_idx + " from ".len()..];
    // The FROM clause runs until the next query clause boundary (or the end
    // of the statement). Capture it so we can reject multi-source FROMs
    // (anything containing a JOIN keyword has no single auto-target).
    let clause_boundary = after
        .to_ascii_lowercase()
        .find(|c| matches!(c, 'w' | 'g' | 'o' | 'h' | 'l'))
        .and_then(|i| {
            let rest = &after.to_ascii_lowercase()[i..];
            if rest.starts_with("where")
                || rest.starts_with("group")
                || rest.starts_with("order")
                || rest.starts_with("having")
                || rest.starts_with("limit")
            {
                Some(i)
            } else {
                None
            }
        });
    let from_clause = match clause_boundary {
        Some(i) => &after[..i],
        None => after,
    };
    if from_clause.to_ascii_lowercase().contains("join") {
        return None;
    }
    let table = after
        .split(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')')
        .find(|tok| !tok.is_empty() && tok.to_ascii_lowercase() != "from")?;
    Some(format!("__cdc_{}", table))
}

// Everything below actually talks to a browser (`fetch`, `WebSocket`), so it
// only compiles for the wasm32 target — see `lib.rs` module docs for why
// this crate's tests are split between "host-testable pure logic" and
// "wasm32-only, browser-dependent" halves.
#[cfg(target_arch = "wasm32")]
mod browser {
    use std::rc::Rc;

    use serde::{Deserialize, Serialize};
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{RequestInit, RequestMode};

    use super::QueryResult;
    use super::infer_topic_from_sql;
    use crate::reactive::Signal;

#[derive(Debug, Clone, Serialize)]
struct QueryRequest<'a> {
    sql: &'a str,
}

#[derive(Debug, Deserialize)]
struct QueryResponse {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<Vec<Option<String>>>,
    error: Option<String>,
}

/// Points at one `tpt-keystone` node's Canvas HTTP endpoint (`TPT_HTTP_ADDR`,
/// default port 5435) and Flux WebSocket endpoint (`TPT_FLUX_WS_ADDR`,
/// default port 5434).
#[derive(Clone)]
pub struct KeystoneClient {
    http_base: String,
    ws_base: String,
}

impl KeystoneClient {
    pub fn new(http_base: impl Into<String>, ws_base: impl Into<String>) -> Self {
        Self { http_base: http_base.into(), ws_base: ws_base.into() }
    }

    /// Runs `sql` against `POST {http_base}/query` and returns the parsed
    /// result (or an `Err` carrying the server's `{"error": ...}` message or
    /// a transport failure description).
    pub async fn query(&self, sql: &str) -> Result<QueryResult, String> {
        let body = serde_json::to_string(&QueryRequest { sql }).map_err(|e| e.to_string())?;

        let opts = RequestInit::new();
        opts.set_method("POST");
        opts.set_mode(RequestMode::Cors);
        opts.set_body(&JsValue::from_str(&body));

        let request = web_sys::Request::new_with_str_and_init(&format!("{}/query", self.http_base), &opts)
            .map_err(|e| format!("{e:?}"))?;
        request.headers().set("Content-Type", "application/json").map_err(|e| format!("{e:?}"))?;

        let window = web_sys::window().ok_or("no window (not running in a browser)")?;
        let resp_value = JsFuture::from(window.fetch_with_request(&request)).await.map_err(|e| format!("{e:?}"))?;
        let resp: web_sys::Response = resp_value.dyn_into().map_err(|_| "fetch() did not return a Response")?;
        let text_value = JsFuture::from(resp.text().map_err(|e| format!("{e:?}"))?).await.map_err(|e| format!("{e:?}"))?;
        let text = text_value.as_string().ok_or("response body was not text")?;

        let parsed: QueryResponse = serde_json::from_str(&text).map_err(|e| format!("invalid JSON from server: {e}"))?;
        if let Some(err) = parsed.error {
            return Err(err);
        }
        Ok(QueryResult { columns: parsed.columns, rows: parsed.rows })
    }

    /// Opens a WebSocket to `{ws_base}` and sends `{"subscribe": topic}`
    /// (the exact handshake `wire::websocket` expects), invoking `on_message`
    /// once per subsequent record pushed to that topic. The returned
    /// `web_sys::WebSocket` must be kept alive by the caller for the
    /// subscription to keep running.
    pub fn subscribe_topic(&self, topic: &str, on_message: impl Fn() + 'static) -> Result<web_sys::WebSocket, String> {
        let ws = web_sys::WebSocket::new(&self.ws_base).map_err(|e| format!("{e:?}"))?;
        let topic = topic.to_string();
        let ws_for_open = ws.clone();
        let onopen = Closure::<dyn FnMut()>::new(move || {
            let msg = serde_json::json!({ "subscribe": topic }).to_string();
            let _ = ws_for_open.send_with_str(&msg);
        });
        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
        onopen.forget();

        let onmessage = Closure::<dyn FnMut(_)>::new(move |_evt: web_sys::MessageEvent| {
            on_message();
        });
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        Ok(ws)
    }

    /// `use_keystone_query` from the spec: fetches `sql` once immediately,
    /// populating the returned `Signal`; if `realtime_topic` is set, each
    /// message on that Flux topic triggers a fresh fetch that replaces the
    /// signal's value. Returns the signal plus the live WebSocket handle (if
    /// any) that the caller must keep alive. When `realtime_topic` is
    /// `None`, the topic is auto-inferred from `sql` via
    /// `infer_topic_from_sql` (single-table `SELECT` only; multi-table
    /// joins and non-SELECT statements fall back to `None` — no live
    /// updates — documenting the architectural ceiling in the module docs.
    pub fn use_keystone_query(self: &Rc<Self>, sql: &str, realtime_topic: Option<&str>) -> (Signal<QueryResult>, Option<web_sys::WebSocket>) {
        let signal = Signal::new(QueryResult::default());
        let inferred = infer_topic_from_sql(sql);
        let topic = realtime_topic.or_else(|| inferred.as_deref());
        let refetch = {
            let client = self.clone();
            let sql = sql.to_string();
            let signal = signal.clone();
            move || {
                let client = client.clone();
                let sql = sql.clone();
                let signal = signal.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    match client.query(&sql).await {
                        Ok(result) => signal.set(result),
                        Err(e) => web_sys::console::error_1(&format!("tpt-canvas: query failed: {e}").into()),
                    }
                });
            }
        };
        refetch();

        let ws = match topic {
            Some(topic) => self.subscribe_topic(topic, refetch).ok(),
            None => None,
        };
        (signal, ws)
    }
}
} // mod browser

#[cfg(target_arch = "wasm32")]
pub use browser::KeystoneClient;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_topic_from_single_table_select() {
        assert_eq!(infer_topic_from_sql("SELECT * FROM users"), Some("__cdc_users".to_string()));
        assert_eq!(infer_topic_from_sql("select id from orders where id = 1"), Some("__cdc_orders".to_string()));
    }

    #[test]
    fn infer_topic_rejects_non_select_and_joins() {
        assert_eq!(infer_topic_from_sql("INSERT INTO users VALUES (1)"), None);
        assert_eq!(infer_topic_from_sql("UPDATE users SET x = 1"), None);
        // Multi-table FROM with a JOIN has no single auto-target.
        assert_eq!(infer_topic_from_sql("SELECT * FROM a JOIN b ON a.id = b.id"), None);
        assert_eq!(infer_topic_from_sql("not sql at all"), None);
    }
}
