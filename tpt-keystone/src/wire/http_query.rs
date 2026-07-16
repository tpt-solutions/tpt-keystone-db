//! Canvas (Phase 13) HTTP/JSON query endpoint — hand-rolled HTTP/1.1, same
//! "own port, own accept loop, no external protocol crate" shape as
//! `wire::websocket`'s Flux endpoint. Exists because a browser can't open a
//! raw TCP socket to speak the Postgres wire protocol; this is the bridge
//! that makes `tpt-canvas`'s `useKeystoneQuery` genuinely execute SQL against
//! `Database` instead of shipping with mock data.
//!
//! Explicit scope cuts (same discipline as `wire::websocket`): no auth (this
//! engine has none anywhere else either — see the wire startup handshake),
//! no keep-alive (`Connection: close` on every response, one request per
//! TCP connection), no chunked transfer-encoding on read (`Content-Length`
//! only), no HTTPS/TLS. CORS is wide open (`Access-Control-Allow-Origin: *`)
//! since this is a dev-facing data API mirroring the rest of the engine's
//! no-auth stance, not a public-internet-facing service.
//!
//! Two routes:
//! - `POST /query` — body `{"sql": "...", "params": [...]}"`, runs it
//!   through the same `executor::execute_parsed_as` entry point the Postgres
//!   wire protocol uses, returns `{"columns": [...], "rows": [[...]]}`. Row
//!   cells are decoded as UTF-8 text and emitted as JSON strings (or `null`)
//!   regardless of the underlying column type — a documented scope cut;
//!   `GET /schema` is how a client learns the real type to parse a cell into.
//! - `GET /schema` — introspects `Database::list_tables`/`get_table` and
//!   returns `{"tables": [{"name":..., "columns":[{"name":..., "type":...}]}]}`,
//!   consumed by `tpt-canvas`'s `tsgen` binary for TypeScript codegen.
//!
//! Auth: when `_tpt_roles` is non-empty, `POST /query` requires
//! `Authorization: Basic <base64(user:pass)>` (verified via
//! `wire::bridge_auth`); the resolved `Actor` is threaded into
//! `execute_parsed_as` for per-table RBAC. `GET /schema` and `OPTIONS` stay
//! unauthenticated so a browser canvas can still discover the schema.
//! Zero-config (`_tpt_roles` empty) preserves today's no-auth behavior.

use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tracing::debug;

use crate::executor::eval::Value;
use crate::executor::rbac::Actor;
use crate::executor::execute_parsed_as;
use crate::storage::database::Database;
use crate::storage::StorageEngine;
use crate::wire::bridge_auth::authenticate_basic;
use crate::wire::roles::RoleStore;

pub async fn handle(
    stream: TcpStream,
    peer: std::net::SocketAddr,
    db: Arc<Database>,
    roles: Arc<RoleStore>,
    guard: Arc<Semaphore>,
) {
    // Admission-control backpressure: hold a permit for the connection's life.
    let _permit = match guard.acquire_owned().await {
        Ok(p) => p,
        Err(_) => return,
    };
    if let Err(e) = run(stream, db, roles).await {
        debug!(%peer, "http query session ended: {e}");
    }
}

async fn run(mut stream: TcpStream, db: Arc<Database>, roles: Arc<RoleStore>) -> anyhow::Result<()> {
    let Some((method, path, body, authorization)) = read_request(&mut stream).await? else {
        return Ok(());
    };

    let response = match (method.as_str(), path.as_str()) {
        ("OPTIONS", _) => json_response(204, &json!(null)),
        ("GET", "/schema") => handle_schema(&db),
        ("POST", "/query") => match authenticate_basic(&roles, &db, authorization.as_deref()) {
            Ok(actor) => handle_query(&db, &body, &actor),
            Err(e) => json_response(401, &json!({"error": format!("unauthorized: {e}")})),
        },
        _ => json_response(404, &json!({"error": "not found"})),
    };

    stream.write_all(&response).await?;
    Ok(())
}

fn handle_query(db: &Arc<Database>, body: &[u8], actor: &Actor) -> Vec<u8> {
    let req: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_response(400, &json!({"error": format!("invalid JSON body: {e}")})),
    };
    let Some(sql) = req.get("sql").and_then(|v| v.as_str()) else {
        return json_response(400, &json!({"error": "missing \"sql\" field"}));
    };

    let params: Vec<Value> = req
        .get("params")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().map(json_to_value).collect())
        .unwrap_or_default();

    let result = if params.is_empty() {
        // Reuse the trusted path but still authorize via the actor.
        db.parse_cached(sql)
            .and_then(|stmt| execute_parsed_as(stmt, db.clone(), &[], actor))
    } else {
        db.parse_cached(sql)
            .and_then(|stmt| execute_parsed_as(stmt, db.clone(), &params, actor))
    };

    match result {
        Ok(qr) => {
            let columns: Vec<&str> = qr.fields.iter().map(|f| f.name.as_str()).collect();
            let rows: Vec<Vec<Option<String>>> = qr
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| {
                            cell.as_ref()
                                .map(|b| String::from_utf8_lossy(b).into_owned())
                        })
                        .collect()
                })
                .collect();
            json_response(200, &json!({"columns": columns, "rows": rows}))
        }
        Err(e) => json_response(400, &json!({"error": e.to_string()})),
    }
}

fn handle_schema(db: &Arc<Database>) -> Vec<u8> {
    let tables = match db.list_tables() {
        Ok(t) => t,
        Err(e) => return json_response(500, &json!({"error": e.to_string()})),
    };
    let mut out = Vec::with_capacity(tables.len());
    for name in tables {
        let Ok(Some(schema)) = db.get_table(&name) else {
            continue;
        };
        let columns: Vec<_> = schema
            .columns
            .iter()
            .map(|c| json!({"name": c.name, "type": column_type_name(&c.col_type)}))
            .collect();
        out.push(json!({"name": name, "columns": columns}));
    }
    json_response(200, &json!({"tables": out}))
}

fn column_type_name(ty: &crate::storage::ColumnType) -> &'static str {
    use crate::storage::ColumnType::*;
    match ty {
        Int8 => "int8",
        Int4 => "int4",
        Int2 => "int2",
        Float8 => "float8",
        Float4 => "float4",
        Text => "text",
        Bool => "bool",
        Timestamp => "timestamp",
        Date => "date",
        Json => "json",
        Bytea => "bytea",
        Geometry => "geometry",
        Geography => "geography",
        Vector => "vector",
        Raster => "raster",
        Float8Array => "double precision[]",
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap_or_default())
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        other => Value::Text(other.to_string()),
    }
}

/// Reads the request line + headers (byte-by-byte, mirroring
/// `wire::websocket::read_handshake` — these requests are small enough that
/// framing-by-length isn't needed for the header portion), then reads
/// exactly `Content-Length` body bytes if present. Returns `None` on a clean
/// close before any bytes arrive.
async fn read_request(
    stream: &mut TcpStream,
) -> anyhow::Result<Option<(String, String, Vec<u8>, Option<String>)>> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return if buf.is_empty() {
                Ok(None)
            } else {
                anyhow::bail!("connection closed mid-request")
            };
        }
        buf.push(byte[0]);
        if buf.len() >= 4 && &buf[buf.len() - 4..] == b"\r\n\r\n" {
            break;
        }
        anyhow::ensure!(buf.len() <= 16_384, "HTTP request headers too large");
    }
    let head = String::from_utf8_lossy(&buf);
    let mut lines = head.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty request"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing HTTP method"))?
        .to_string();
    let path = parts.next().unwrap_or("/").to_string();

    let content_length: usize = lines
        .find(|line| line.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|line| line.splitn(2, ':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);

    let authorization: Option<String> = lines
        .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
        .and_then(|line| line.splitn(2, ':').nth(1))
        .map(|v| v.trim().to_string());

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        stream.read_exact(&mut body).await?;
    }
    Ok(Some((method, path, body, authorization)))
}

fn json_response(status: u16, body: &serde_json::Value) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Internal Server Error",
    };
    let payload = if status == 204 {
        String::new()
    } else {
        body.to_string()
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {len}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         Connection: close\r\n\r\n\
         {payload}",
        len = payload.len(),
    )
    .into_bytes()
}
