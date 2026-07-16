//! Shared authentication helper for the non-Postgres network bridges
//! (HTTP query, Flux WebSocket, Flux gRPC, MCP). These listeners don't speak
//! the Postgres wire protocol's SCRAM handshake, so they authenticate with
//! `Authorization: Basic` (HTTP/WebSocket/gRPC) or `X-TPT-Token` (MCP), then
//! resolve an RBAC `Actor` for per-table authorization downstream.
//!
//! The helper preserves today's zero-config quickstart: when `_tpt_roles` is
//! empty, auth is skipped entirely and an `Actor::unrestricted()` is returned,
//! exactly mirroring `wire::session::run`'s `roles.is_empty()` branch.

use base64::{engine::general_purpose::STANDARD, Engine};
use std::sync::Arc;

use crate::executor::rbac::Actor;
use crate::storage::database::Database;
use crate::wire::roles::RoleStore;

/// Parse an `Authorization` header value of the form `Basic <base64>`,
/// decoding it to `(user, password)`. Returns `None` for a missing,
/// non-Basic, or malformed value (no `:` separator, bad base64, non-UTF8).
pub fn parse_basic_auth(header: Option<&str>) -> Option<(String, String)> {
    let header = header?;
    let scheme_split = header.split_once(' ')?;
    if !scheme_split.0.eq_ignore_ascii_case("Basic") {
        return None;
    }
    let decoded = STANDARD.decode(scheme_split.1.trim()).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, pass) = text.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// Authenticate an actor for a bridge listener from an `Authorization` header.
///
/// - Zero-config (`roles.is_empty()`): returns `Actor::unrestricted()`.
/// - Otherwise requires a valid `Authorization: Basic user:pass` whose
///   password verifies against the stored SCRAM credential and whose role has
///   `LOGIN` privilege. Any missing/invalid/malformed header or bad
///   credentials yields an error the caller turns into a `401`.
pub fn authenticate_basic(
    roles: &RoleStore,
    db: &Arc<Database>,
    authorization: Option<&str>,
) -> anyhow::Result<Actor> {
    if roles.is_empty()? {
        return Ok(Actor::unrestricted());
    }
    let (user, pass) = parse_basic_auth(authorization).ok_or_else(|| {
        anyhow::anyhow!("missing or malformed Authorization header (expected 'Basic <base64>')")
    })?;
    let attrs = roles
        .find_role(&user)?
        .ok_or_else(|| anyhow::anyhow!("role \"{user}\" does not exist"))?;
    if !attrs.can_login {
        anyhow::bail!("role \"{user}\" is not permitted to log in");
    }
    if !roles.verify_password(&user, &pass)? {
        anyhow::bail!("password authentication failed for role \"{user}\"");
    }
    Actor::for_role(db, &user)
}

/// Authenticate an MCP actor from its `X-TPT-Token` header. The MCP gate keeps
/// its existing token check (configured via `TPT_MCP_TOKEN`); once that
/// passes, this resolves the same kind of `Actor` as the other bridges so
/// downstream tool handlers can enforce per-table RBAC. Zero-config
/// (`roles.is_empty()`) short-circuits to `Actor::unrestricted()`.
///
/// `token` is the configured expected token (`None` ⇒ token gate disabled,
/// anything accepted). The caller is responsible for enforcing the token
/// match; this function only resolves the `Actor` after the gate has passed.
pub fn actor_for_mcp(roles: &RoleStore, db: &Arc<Database>, token: Option<&str>) -> anyhow::Result<Actor> {
    if roles.is_empty()? {
        return Ok(Actor::unrestricted());
    }
    // When a token gate is configured, the caller has already verified the
    // presented token equals `token`; we still need a concrete role to build
    // the actor. Use the bootstrap/only-superuser role when the token gate is
    // in effect, since the MCP token doesn't name a role.
    if token.is_some() {
        // Resolve the first superuser role to act as — MCP tooling is an
        // operator surface, gated by the shared token rather than per-role
        // credentials.
        let roles_scan = roles.db_scan_roles()?;
        let superuser = roles_scan
            .into_iter()
            .find(|(_, attrs)| attrs.superuser)
            .map(|(name, _)| name);
        if let Some(name) = superuser {
            return Actor::for_role(db, &name);
        }
        anyhow::bail!("no superuser role exists to authorize the MCP token gate")
    }
    // No token gate and roles are configured: MCP has no credential, so this
    // is an unsupported configuration — require a token gate.
    anyhow::bail!("MCP requires a token gate (TPT_MCP_TOKEN) when roles are configured")
}
