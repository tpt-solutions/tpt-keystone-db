//! JSON-RPC 2.0 envelope + the minimal MCP method set (`initialize`,
//! `tools/list`, `tools/call`). Scope cut: no resources/prompts, no
//! server-initiated notifications — just enough for a client to discover and
//! call the 6 tools in `tools.rs`.

use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value as Json};

use crate::storage::database::Database;
use crate::executor::rbac::Actor;

use super::tools;

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[serde(default)]
    id: Json,
    #[serde(default)]
    method: String,
    #[serde(default)]
    params: Json,
}

/// Parses `body` as a JSON-RPC request and returns the JSON-RPC response
/// (never errors itself — malformed input becomes a JSON-RPC error object).
/// `actor` is the authenticated identity (resolved by `mcp::server::handle`
/// from the `X-TPT-Token` gate) threaded into tool execution for per-table
/// RBAC on the SQL-executing tools.
pub fn dispatch(db: &Arc<Database>, actor: &Actor, body: &[u8]) -> Json {
    let request: JsonRpcRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return error_response(Json::Null, -32700, &format!("Parse error: {e}")),
    };

    let id = request.id.clone();
    match request.method.as_str() {
        "initialize" => success_response(
            id,
            json!({
                "protocolVersion": "2024-11-05",
                "serverInfo": {"name": "tpt-keystone", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {"tools": {}},
            }),
        ),
        "tools/list" => success_response(id, json!({"tools": tool_descriptors()})),
        "tools/call" => handle_tools_call(db, actor, id, &request.params),
        other => error_response(id, -32601, &format!("Method not found: {other}")),
    }
}

fn handle_tools_call(db: &Arc<Database>, actor: &Actor, id: Json, params: &Json) -> Json {
    let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    match tools::call(db, actor, name, &args) {
        Ok(value) => success_response(
            id,
            json!({"content": [{"type": "text", "text": value.to_string()}]}),
        ),
        Err(e) => error_response(id, -32000, &e.to_string()),
    }
}

fn success_response(id: Json, result: Json) -> Json {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_response(id: Json, code: i32, message: &str) -> Json {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn tool_descriptors() -> Json {
    json!([
        {
            "name": "tables",
            "description": "List all table names in the database.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "columns",
            "description": "List columns (name, type, nullable, default, primary key) for a table.",
            "inputSchema": {
                "type": "object",
                "properties": {"table": {"type": "string"}},
                "required": ["table"],
            },
        },
        {
            "name": "schema",
            "description": "Full schema: every table's columns, foreign keys, indexed columns, row counts, per-column value-distribution histograms, and the FK relationship graph (nodes/edges).",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "query",
            "description": "Run a read-only SELECT/SHOW statement and return rows as JSON.",
            "inputSchema": {
                "type": "object",
                "properties": {"sql": {"type": "string"}},
                "required": ["sql"],
            },
        },
        {
            "name": "mutate",
            "description": "Run an INSERT/UPDATE/DELETE/DDL statement.",
            "inputSchema": {
                "type": "object",
                "properties": {"sql": {"type": "string"}},
                "required": ["sql"],
            },
        },
        {
            "name": "explain",
            "description": "Return a structural summary of a statement's parsed shape (not a cost-based plan).",
            "inputSchema": {
                "type": "object",
                "properties": {"sql": {"type": "string"}},
                "required": ["sql"],
            },
        },
        {
            "name": "related",
            "description": "Structured retrieval: walk the foreign-key graph outward from one row (both its own FKs and other tables' FKs pointing at it) and return compact {subject, relation, object} facts with human-readable labels, not raw rows.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "id": {"type": "string", "description": "Primary key value of the starting row."},
                    "max_depth": {"type": "integer", "description": "Traversal hops outward, 0-2 (default 1)."},
                    "limit": {"type": "integer", "description": "Max rows fetched per hop per FK, 1-100 (default 20)."},
                },
                "required": ["table", "id"],
            },
        },
    ])
}
