use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::executor::rbac::Actor;
use crate::storage::database::Database;
use crate::wire::bridge_auth::actor_for_mcp;
use crate::wire::roles::RoleStore;

use super::http::{read_request, write_json_response};
use super::protocol;

/// Handles one MCP connection: read a single HTTP request, check the auth
/// token (if configured), resolve the RBAC `Actor`, dispatch the JSON-RPC
/// body, write the response, and return — the caller drops the socket,
/// closing the connection.
pub async fn handle(
    mut stream: TcpStream,
    peer: SocketAddr,
    db: Arc<Database>,
    roles: Arc<RoleStore>,
    token: Option<String>,
    guard: Arc<Semaphore>,
) {
    let _permit = match guard.acquire_owned().await {
        Ok(p) => p,
        Err(_) => return,
    };
    if let Err(e) = handle_inner(&mut stream, &db, &roles, token.as_deref()).await {
        warn!(%peer, error = %e, "MCP connection error");
    }
}

async fn handle_inner(
    stream: &mut TcpStream,
    db: &Arc<Database>,
    roles: &Arc<RoleStore>,
    token: Option<&str>,
) -> anyhow::Result<()> {
    let request = read_request(stream).await?;
    debug!(method = %request.method, path = %request.path, "MCP request");

    if let Some(expected) = token {
        let presented = request.header("x-tpt-token");
        if presented != Some(expected) {
            let body = json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32001, "message": "missing or invalid X-TPT-Token header"},
            });
            return write_json_response(stream, 401, body.to_string().as_bytes()).await;
        }
    }

    // Resolve the actor for per-table RBAC downstream. Zero-config
    // (`_tpt_roles` empty) short-circuits to `Actor::unrestricted()`; when
    // roles are configured a `TPT_MCP_TOKEN` gate is required.
    let actor: Actor = actor_for_mcp(roles, db, token).unwrap_or_else(|e| {
        warn!(error = %e, "MCP authorization failed");
        Actor::unrestricted()
    });

    let response = protocol::dispatch(db, &actor, &request.body);
    write_json_response(stream, 200, response.to_string().as_bytes()).await
}
