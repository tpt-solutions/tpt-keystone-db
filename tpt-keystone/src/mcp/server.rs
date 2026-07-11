use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::json;
use tokio::net::TcpStream;
use tracing::{debug, warn};

use crate::storage::database::Database;

use super::http::{read_request, write_json_response};
use super::protocol;

/// Handles one MCP connection: read a single HTTP request, check the auth
/// token (if configured), dispatch the JSON-RPC body, write the response,
/// and return — the caller drops the socket, closing the connection.
pub async fn handle(
    mut stream: TcpStream,
    peer: SocketAddr,
    db: Arc<Database>,
    token: Option<String>,
) {
    if let Err(e) = handle_inner(&mut stream, &db, &token).await {
        warn!(%peer, error = %e, "MCP connection error");
    }
}

async fn handle_inner(
    stream: &mut TcpStream,
    db: &Arc<Database>,
    token: &Option<String>,
) -> anyhow::Result<()> {
    let request = read_request(stream).await?;
    debug!(method = %request.method, path = %request.path, "MCP request");

    if let Some(expected) = token {
        let presented = request.header("x-tpt-token");
        if presented != Some(expected.as_str()) {
            let body = json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32001, "message": "missing or invalid X-TPT-Token header"},
            });
            return write_json_response(stream, 401, body.to_string().as_bytes()).await;
        }
    }

    let response = protocol::dispatch(db, &request.body);
    write_json_response(stream, 200, response.to_string().as_bytes()).await
}
