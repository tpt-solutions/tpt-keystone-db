use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{debug, error, info, warn};

use super::codec::{BoxedStream, Conn, FrontendMessage};
use super::messages::{oid, BackendMessage, ErrorInfo, FieldDescription, TransactionStatus};
use super::roles::RoleStore;
use super::scram;
use crate::executor::eval::Value;
use crate::executor::rbac::{Actor, InsufficientPrivilege};
use crate::executor::{describe_select, execute_parsed, execute_parsed_as, max_param_index, QueryResult};
use crate::sql::ast::{CopyStmt, FetchCount, Stmt};
use crate::storage::database::Database;
use crate::storage::StorageEngine;

/// A materialized `DECLARE CURSOR` result, with a `FETCH` read position.
/// Cursors are session-local state (simple query protocol only — see
/// `Stmt::DeclareCursor` in the executor, which rejects them outside this
/// loop), so they live here rather than in the stateless executor.
struct CursorState {
    fields: Vec<FieldDescription>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    pos: usize,
}

/// A statement registered via `Parse`, awaiting `Bind`.
struct PreparedStmt {
    stmt: Stmt,
    /// Client-declared parameter type OIDs (0 = unspecified/infer).
    param_types: Vec<i32>,
}

/// A statement bound to concrete parameter values via `Bind`, awaiting `Execute`.
struct Portal {
    stmt: Stmt,
    params: Vec<Value>,
}

/// Per-connection extended query protocol state.
#[derive(Default)]
struct ExtendedState {
    prepared: HashMap<String, PreparedStmt>,
    portals: HashMap<String, Portal>,
    cursors: HashMap<String, CursorState>,
    /// Channel names this session is currently `LISTEN`ing on.
    listening: HashSet<String>,
}

/// Drive a single client connection from startup through the query loop.
/// `stream` has already been through `wire::tls::negotiate` by the time it
/// gets here — plain `TcpStream` and TLS-upgraded connections are
/// indistinguishable from this point on.
pub async fn handle(
    stream: BoxedStream,
    peer: std::net::SocketAddr,
    db: Arc<Database>,
    roles: Arc<RoleStore>,
) {
    info!(%peer, "client connected");
    let mut conn = Conn::from_boxed(stream);

    crate::metrics::Metrics::global().connection_opened();
    if let Err(e) = run(&mut conn, peer, db, roles).await {
        debug!(%peer, "session ended: {e}");
    }
    crate::metrics::Metrics::global().connection_closed();
    info!(%peer, "client disconnected");
}

#[tracing::instrument(skip_all, fields(%peer))]
async fn run(
    conn: &mut Conn,
    peer: std::net::SocketAddr,
    db: Arc<Database>,
    roles: Arc<RoleStore>,
) -> anyhow::Result<()> {
    // --- Startup handshake ---
    let startup = conn.read_startup().await?;
    debug!(%peer, version = startup.protocol_version, "startup received");

    // The role this connection authenticates as (empty string on the
    // zero-config path). Captured up front so the `Actor` can be built once
    // regardless of which auth branch runs.
    let user = startup
        .params
        .iter()
        .find(|(k, _)| k == "user")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    // Wire-level auth is opt-in: an empty `_tpt_roles` catalog (the default
    // on a fresh node with no `TPT_AUTH_BOOTSTRAP_USER`/`_PASSWORD` set)
    // preserves today's unconditional `AuthenticationOk`, so the documented
    // zero-config quickstart (`psql -h localhost -p 5432`, no flags) keeps
    // working unchanged. Once at least one role exists, every connecting
    // user must complete a real SCRAM-SHA-256 exchange.
    if !roles.is_empty()? {
        if let Err(e) = authenticate(conn, &roles, &user).await {
            debug!(%peer, %user, error = %e, "authentication failed");
            conn.send_error(ErrorInfo::fatal(
                "28P01",
                format!("password authentication failed for user \"{user}\""),
            ));
            conn.flush().await?;
            return Ok(());
        }
    }

    // Build the per-connection `Actor` (RBAC identity) once, after auth. On
    // the zero-config path `_tpt_roles` is empty and the actor is fully
    // unrestricted — the quickstart is behaviorally unchanged.
    let actor = if roles.is_empty()? {
        Actor::unrestricted()
    } else {
        Actor::for_role(&db, &user)?
    };
    for (name, value) in [
        ("server_version", "16.0 (TPT Keystone 0.1.0)"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("TimeZone", "UTC"),
    ] {
        conn.send(&BackendMessage::ParameterStatus {
            name: name.into(),
            value: value.into(),
        });
    }
    conn.send(&BackendMessage::BackendKeyData { pid: 1, secret: 0 });
    conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    conn.flush().await?;

    run_query_loop(conn, peer, db, actor).await
}

/// Map an executor error to the SQLSTATE reported on the wire. A denied
/// RBAC check downcasts to `42501` (insufficient_privilege); everything else
/// keeps the generic `42601`.
fn sqlstate_for(e: &anyhow::Error) -> &'static str {
    if e.downcast_ref::<InsufficientPrivilege>().is_some() {
        "42501"
    } else {
        "42601"
    }
}

/// Runs the SCRAM-SHA-256 exchange (`AuthenticationSASL` →
/// `SASLInitialResponse` → `AuthenticationSASLContinue` → `SASLResponse` →
/// `AuthenticationSASLFinal`) for `user`, ending with `AuthenticationOk` on
/// success. Any failure (unknown user, wrong password, malformed message) is
/// surfaced identically to the caller, which reports one generic
/// "password authentication failed" error either way — same as real
/// Postgres, a client can't tell "no such user" from "wrong password".
async fn authenticate(conn: &mut Conn, roles: &RoleStore, user: &str) -> anyhow::Result<()> {
    // A role without LOGIN privilege may not authenticate at all.
    if let Some(attrs) = roles.find_role(user)? {
        if !attrs.can_login {
            anyhow::bail!("role \"{user}\" is not permitted to log in");
        }
    }
    let cred = roles
        .find(user)?
        .ok_or_else(|| anyhow::anyhow!("no such role \"{user}\""))?;

    conn.send(&BackendMessage::AuthenticationSASL(vec![
        scram::MECHANISM.to_string()
    ]));
    conn.flush().await?;

    let FrontendMessage::PasswordData(initial) = conn.read_message().await? else {
        anyhow::bail!("expected SASLInitialResponse");
    };
    let (mechanism, client_first) = parse_sasl_initial_response(&initial)?;
    if mechanism != scram::MECHANISM {
        anyhow::bail!("unsupported SASL mechanism \"{mechanism}\"");
    }

    let first = scram::server_first(&client_first, &cred)?;
    conn.send(&BackendMessage::AuthenticationSASLContinue(
        first.message_bytes(),
    ));
    conn.flush().await?;

    let FrontendMessage::PasswordData(client_final) = conn.read_message().await? else {
        anyhow::bail!("expected SASLResponse");
    };
    let server_final = scram::verify_client_final(&client_final, &first, &cred)?;

    conn.send(&BackendMessage::AuthenticationSASLFinal(server_final));
    conn.send(&BackendMessage::AuthenticationOk);
    Ok(())
}

/// `SASLInitialResponse`'s body: a C-string mechanism name, then an `i32`
/// length-prefixed blob of SASL data (the `client-first-message`). Unlike
/// the later `SASLResponse`, which is just raw SASL data with no framing.
fn parse_sasl_initial_response(data: &[u8]) -> anyhow::Result<(String, Vec<u8>)> {
    let nul = data
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| anyhow::anyhow!("malformed SASLInitialResponse"))?;
    let mechanism = String::from_utf8(data[..nul].to_vec())?;
    let rest = &data[nul + 1..];
    if rest.len() < 4 {
        anyhow::bail!("malformed SASLInitialResponse: missing length");
    }
    let len = i32::from_be_bytes(rest[0..4].try_into().unwrap());
    let sasl_data = if len < 0 {
        Vec::new()
    } else {
        rest.get(4..4 + len as usize)
            .ok_or_else(|| anyhow::anyhow!("SASLInitialResponse length out of bounds"))?
            .to_vec()
    };
    Ok((mechanism, sasl_data))
}

async fn run_query_loop(
    conn: &mut Conn,
    peer: std::net::SocketAddr,
    db: Arc<Database>,
    actor: Actor,
) -> anyhow::Result<()> {
    let mut ext = ExtendedState::default();
    let mut notify_rx = db.subscribe_notifications();

    // --- Query loop ---
    // `select!` between the client socket and the LISTEN/NOTIFY bus so an
    // idle, listening session can receive an asynchronous NotificationResponse
    // at any time, as real Postgres does, without blocking on the next
    // client message. `conn.read_message()` only ever awaits on cancel-safe
    // `AsyncRead` calls, so re-polling it on the next select iteration after
    // a notification branch fires never loses buffered bytes.
    loop {
        let msg = tokio::select! {
            msg = conn.read_message() => msg?,
            notification = notify_rx.recv() => {
                if let Ok((channel, payload)) = notification {
                    if ext.listening.contains(&channel) {
                        conn.send(&BackendMessage::NotificationResponse { pid: 1, channel, payload });
                        conn.flush().await?;
                    }
                }
                continue;
            }
        };
        match msg {
            FrontendMessage::Query(sql) => {
                let sql = sql.trim().to_string();
                debug!(%peer, %sql, "query received");
                handle_simple_query(conn, &sql, db.clone(), &mut ext.cursors, &mut ext.listening, &actor)
                    .await?;
            }

            FrontendMessage::Parse {
                name,
                query,
                param_types,
            } => {
                debug!(%peer, "parse: {query}");
                match db.parse_cached(&query) {
                    Ok(stmt) => {
                        ext.prepared
                            .insert(name, PreparedStmt { stmt, param_types });
                        conn.send(&BackendMessage::ParseComplete);
                    }
                    Err(e) => {
                        conn.send_error(ErrorInfo::new("42601", e.to_string()));
                    }
                }
                conn.flush().await?;
            }

            FrontendMessage::Bind {
                portal,
                stmt,
                params,
                param_formats,
                ..
            } => {
                match ext.prepared.get(&stmt) {
                    Some(prepared) => {
                        let values: Vec<Value> = params
                            .iter()
                            .enumerate()
                            .map(|(i, bytes)| {
                                let format = param_formats.get(i).copied().unwrap_or(0);
                                decode_param(bytes.as_deref(), format)
                            })
                            .collect();
                        ext.portals.insert(
                            portal,
                            Portal {
                                stmt: prepared.stmt.clone(),
                                params: values,
                            },
                        );
                        conn.send(&BackendMessage::BindComplete);
                    }
                    None => {
                        conn.send_error(ErrorInfo::new(
                            "26000",
                            format!("prepared statement \"{stmt}\" does not exist"),
                        ));
                    }
                }
                conn.flush().await?;
            }

            FrontendMessage::Describe { kind, name } => {
                if kind == b'S' {
                    match ext.prepared.get(&name) {
                        Some(prepared) => {
                            let n_params = max_param_index(&prepared.stmt)
                                .max(prepared.param_types.len() as u32);
                            let types: Vec<i32> = (0..n_params)
                                .map(|i| {
                                    prepared
                                        .param_types
                                        .get(i as usize)
                                        .copied()
                                        .filter(|t| *t != 0)
                                        .unwrap_or(oid::TEXT)
                                })
                                .collect();
                            conn.send(&BackendMessage::ParameterDescription(types));
                            send_row_description_for(conn, &prepared.stmt, &db)?;
                        }
                        None => {
                            conn.send_error(ErrorInfo::new(
                                "26000",
                                format!("prepared statement \"{name}\" does not exist"),
                            ));
                        }
                    }
                } else {
                    match ext.portals.get(&name) {
                        Some(portal) => send_row_description_for(conn, &portal.stmt, &db)?,
                        None => {
                            conn.send_error(ErrorInfo::new(
                                "34000",
                                format!("portal \"{name}\" does not exist"),
                            ));
                        }
                    }
                }
                conn.flush().await?;
            }

            FrontendMessage::Execute { portal, max_rows } => {
                match ext.portals.get(&portal) {
                    Some(p) => match execute_parsed_as(p.stmt.clone(), db.clone(), &p.params, &actor) {
                        Ok(result) => {
                            let total = result.rows.len();
                            let limited = max_rows > 0 && (max_rows as usize) < total;
                            let rows = if limited {
                                &result.rows[..max_rows as usize]
                            } else {
                                &result.rows[..]
                            };
                            for row in rows {
                                conn.send(&BackendMessage::DataRow(row.clone()));
                            }
                            if limited {
                                conn.send(&BackendMessage::PortalSuspended);
                            } else {
                                conn.send(&BackendMessage::CommandComplete(result.tag));
                            }
                        }
                        Err(e) => {
                            error!("execute error: {e}");
                            conn.send_error(ErrorInfo::new(sqlstate_for(&e), e.to_string()));
                        }
                    },
                    None => {
                        conn.send_error(ErrorInfo::new(
                            "34000",
                            format!("portal \"{portal}\" does not exist"),
                        ));
                    }
                }
                conn.flush().await?;
            }

            FrontendMessage::Close { kind, name } => {
                if kind == b'S' {
                    ext.prepared.remove(&name);
                } else {
                    ext.portals.remove(&name);
                }
                conn.send(&BackendMessage::CloseComplete);
                conn.flush().await?;
            }

            FrontendMessage::Sync => {
                conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
                conn.flush().await?;
            }
            FrontendMessage::Flush => {
                conn.flush().await?;
            }
            FrontendMessage::Terminate => {
                info!(%peer, "client sent Terminate");
                return Ok(());
            }
            FrontendMessage::CancelRequest => {
                warn!(%peer, "cancel request ignored");
            }
            FrontendMessage::CopyData(_)
            | FrontendMessage::CopyDone
            | FrontendMessage::CopyFail(_) => {
                // Only expected while `handle_copy_in` is reading directly
                // off `conn`, which bypasses this dispatch loop entirely.
                warn!(%peer, "unexpected COPY message outside an active COPY");
            }
            FrontendMessage::PasswordData(_) => {
                // Only expected mid-`authenticate()`, which reads directly
                // off `conn` before this loop starts.
                warn!(%peer, "unexpected password/SASL message outside authentication");
            }
        }
    }
}

async fn handle_simple_query(
    conn: &mut Conn,
    sql: &str,
    db: Arc<Database>,
    cursors: &mut HashMap<String, CursorState>,
    listening: &mut HashSet<String>,
    actor: &Actor,
) -> anyhow::Result<()> {
    if sql.is_empty() {
        conn.send(&BackendMessage::EmptyQueryResponse);
        conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
        conn.flush().await?;
        return Ok(());
    }

    let stmt = match db.parse_cached(sql) {
        Ok(stmt) => stmt,
        Err(e) => {
            conn.send_error(ErrorInfo::new("42601", e.to_string()));
            conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
            conn.flush().await?;
            return Ok(());
        }
    };

    // COPY takes over the connection for a sub-protocol of CopyData/CopyDone
    // messages, so it's handled directly here rather than through
    // `execute_simple`'s uniform QueryResult shape.
    match stmt {
        Stmt::CopyIn(copy) => return handle_copy_in(conn, copy, db).await,
        Stmt::CopyOut(copy) => return handle_copy_out(conn, copy, db).await,
        _ => {}
    }

    match execute_simple(stmt, db, cursors, listening, actor) {
        Ok(result) => {
            conn.send(&BackendMessage::RowDescription(result.fields));
            for row in result.rows {
                conn.send(&BackendMessage::DataRow(row));
            }
            conn.send(&BackendMessage::CommandComplete(result.tag));
        }
        Err(e) => {
            error!("query error: {e}");
            conn.send_error(ErrorInfo::new(sqlstate_for(&e), e.to_string()));
        }
    }
    conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    conn.flush().await?;
    Ok(())
}

/// Execute one already-parsed simple-query-protocol statement, intercepting
/// cursor statements (`DECLARE`/`FETCH`/`MOVE`/`CLOSE`) to manage
/// session-local `cursors` state; everything else is delegated to the
/// stateless executor.
fn execute_simple(
    stmt: Stmt,
    db: Arc<Database>,
    cursors: &mut HashMap<String, CursorState>,
    listening: &mut HashSet<String>,
    actor: &Actor,
) -> anyhow::Result<QueryResult> {
    match stmt {
        Stmt::Listen(channel) => {
            listening.insert(channel);
            Ok(QueryResult {
                fields: vec![],
                rows: vec![],
                tag: "LISTEN".into(),
            })
        }
        Stmt::Unlisten(channel) => {
            if channel == "*" {
                listening.clear();
            } else {
                listening.remove(&channel);
            }
            Ok(QueryResult {
                fields: vec![],
                rows: vec![],
                tag: "UNLISTEN".into(),
            })
        }
        Stmt::DeclareCursor(d) => {
            let result = execute_parsed_as(Stmt::Select(d.query), db, &[], actor)?;
            cursors.insert(
                d.name,
                CursorState {
                    fields: result.fields,
                    rows: result.rows,
                    pos: 0,
                },
            );
            Ok(QueryResult {
                fields: vec![],
                rows: vec![],
                tag: "DECLARE CURSOR".into(),
            })
        }
        Stmt::Fetch(f) => {
            let cursor = cursors
                .get_mut(&f.cursor)
                .ok_or_else(|| anyhow::anyhow!("cursor \"{}\" does not exist", f.cursor))?;
            let n = fetch_count(&f.count, cursor);
            let end = (cursor.pos + n).min(cursor.rows.len());
            let rows = cursor.rows[cursor.pos..end].to_vec();
            cursor.pos = end;
            let fields = cursor.fields.clone();
            let count = rows.len();
            Ok(QueryResult {
                fields,
                rows,
                tag: format!("FETCH {count}"),
            })
        }
        Stmt::MoveCursor(f) => {
            let cursor = cursors
                .get_mut(&f.cursor)
                .ok_or_else(|| anyhow::anyhow!("cursor \"{}\" does not exist", f.cursor))?;
            let n = fetch_count(&f.count, cursor);
            let end = (cursor.pos + n).min(cursor.rows.len());
            let moved = end - cursor.pos;
            cursor.pos = end;
            Ok(QueryResult {
                fields: vec![],
                rows: vec![],
                tag: format!("MOVE {moved}"),
            })
        }
        Stmt::CloseCursor(name) => {
            cursors
                .remove(&name)
                .ok_or_else(|| anyhow::anyhow!("cursor \"{name}\" does not exist"))?;
            Ok(QueryResult {
                fields: vec![],
                rows: vec![],
                tag: "CLOSE CURSOR".into(),
            })
        }
        other => execute_parsed_as(other, db, &[], actor),
    }
}

/// Number of rows a `FETCH`/`MOVE` direction represents, clamped to what's
/// left in the cursor (forward-only — negative counts are treated as 0).
fn fetch_count(count: &FetchCount, cursor: &CursorState) -> usize {
    match count {
        FetchCount::Next => 1,
        FetchCount::All => cursor.rows.len() - cursor.pos,
        FetchCount::Count(n) => (*n).max(0) as usize,
    }
}

/// Drive the `COPY table FROM STDIN` sub-protocol: send `CopyInResponse`,
/// then read `CopyData`/`CopyDone`/`CopyFail` messages directly off `conn`
/// (bypassing the normal message dispatch) until the client ends the copy.
async fn handle_copy_in(conn: &mut Conn, copy: CopyStmt, db: Arc<Database>) -> anyhow::Result<()> {
    let schema = match db.get_table(&copy.table) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return copy_abort(conn, format!("table \"{}\" does not exist", copy.table)).await
        }
        Err(e) => return copy_abort(conn, e.to_string()).await,
    };
    let target = match crate::executor::copy::target_columns(&schema, &copy.columns) {
        Ok(t) => t,
        Err(e) => return copy_abort(conn, e.to_string()).await,
    };

    conn.send(&BackendMessage::CopyInResponse {
        columns: target.len(),
    });
    conn.flush().await?;

    let mut buf: Vec<u8> = Vec::new();
    let mut row_count = 0u64;
    let mut failed: Option<String> = None;
    loop {
        match conn.read_message().await? {
            FrontendMessage::CopyData(data) => {
                buf.extend_from_slice(&data);
                while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=pos).collect();
                    let line = &line[..line.len().saturating_sub(1)]; // strip trailing '\n'
                    if failed.is_none() {
                        let text = String::from_utf8_lossy(line);
                        match crate::executor::copy::insert_copy_line(
                            &db,
                            &copy.table,
                            &schema,
                            &target,
                            &text,
                        ) {
                            Ok(()) => row_count += 1,
                            Err(e) => failed = Some(e.to_string()),
                        }
                    }
                }
            }
            FrontendMessage::CopyDone => break,
            FrontendMessage::CopyFail(msg) => {
                failed = Some(format!("COPY aborted by client: {msg}"));
                break;
            }
            _ => { /* any other message here is a client protocol violation; ignore */ }
        }
    }

    match failed {
        Some(msg) => conn.send_error(ErrorInfo::new("22P04", msg)),
        None => conn.send(&BackendMessage::CommandComplete(format!(
            "COPY {row_count}"
        ))),
    }
    conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    conn.flush().await?;
    Ok(())
}

/// Send an ErrorResponse and bail out of a COPY before the `CopyInResponse`/
/// `CopyOutResponse` handshake — used when the target table/columns are
/// invalid.
async fn copy_abort(conn: &mut Conn, message: String) -> anyhow::Result<()> {
    conn.send_error(ErrorInfo::new("42P01", message));
    conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    conn.flush().await?;
    Ok(())
}

/// Drive the `COPY table TO STDOUT` sub-protocol: send `CopyOutResponse`,
/// stream every row as a `CopyData` message, then `CopyDone`.
async fn handle_copy_out(conn: &mut Conn, copy: CopyStmt, db: Arc<Database>) -> anyhow::Result<()> {
    let schema = match db.get_table(&copy.table) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return copy_abort(conn, format!("table \"{}\" does not exist", copy.table)).await
        }
        Err(e) => return copy_abort(conn, e.to_string()).await,
    };
    let target = match crate::executor::copy::target_columns(&schema, &copy.columns) {
        Ok(t) => t,
        Err(e) => return copy_abort(conn, e.to_string()).await,
    };
    let rows = match crate::executor::copy::scan_for_copy(&db, &copy.table, &target) {
        Ok(r) => r,
        Err(e) => return copy_abort(conn, e.to_string()).await,
    };

    conn.send(&BackendMessage::CopyOutResponse {
        columns: target.len(),
    });
    let row_count = rows.len();
    for row in &rows {
        conn.send(&BackendMessage::CopyData(
            crate::executor::copy::encode_copy_line(row),
        ));
    }
    conn.send(&BackendMessage::CopyDone);
    conn.send(&BackendMessage::CommandComplete(format!(
        "COPY {row_count}"
    )));
    conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    conn.flush().await?;
    Ok(())
}

/// Send RowDescription (or NoData for non-SELECT / no-FROM statements) for
/// a prepared statement or portal, ahead of Execute.
fn send_row_description_for(
    conn: &mut Conn,
    stmt: &Stmt,
    db: &Arc<Database>,
) -> anyhow::Result<()> {
    match stmt {
        Stmt::Select(select) => match describe_select(select, db.clone()) {
            Ok(fields) if !fields.is_empty() => conn.send(&BackendMessage::RowDescription(fields)),
            Ok(_) => conn.send(&BackendMessage::NoData),
            Err(_) => conn.send(&BackendMessage::NoData),
        },
        _ => conn.send(&BackendMessage::NoData),
    }
    Ok(())
}

/// Decode one bound parameter value using its declared wire format
/// (0 = text, 1 = binary). Binary decoding is a best-effort fallback for
/// the common fixed-width numeric encodings.
fn decode_param(bytes: Option<&[u8]>, format: i16) -> Value {
    let Some(b) = bytes else { return Value::Null };
    if format == 0 {
        return Value::from_bytes(Some(b));
    }
    match b.len() {
        1 => Value::Bool(b[0] != 0),
        4 => Value::Int(i32::from_be_bytes(b.try_into().unwrap()) as i64),
        8 => Value::Int(i64::from_be_bytes(b.try_into().unwrap())),
        _ => Value::from_bytes(Some(b)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::rbac::Actor;
    use crate::storage::config::NodeRole;
    use crate::storage::lease::LeaseManager;
    use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
    use crate::wire::roles::RoleStore;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let lease = Arc::new(LeaseManager::new(
            store.clone(),
            "db",
            "node-1".into(),
            Duration::from_secs(30),
        ));
        lease.try_acquire().unwrap();
        let db = Arc::new(
            Database::open(
                local.path(),
                store,
                lease.handle(),
                NodeRole::Writer,
                Default::default(),
            )
            .unwrap(),
        );
        (db, bucket, local)
    }

    fn exec(
        sql: &str,
        db: Arc<Database>,
        cursors: &mut HashMap<String, CursorState>,
        listening: &mut HashSet<String>,
    ) -> anyhow::Result<QueryResult> {
        execute_simple(
            crate::sql::parse(sql).unwrap(),
            db,
            cursors,
            listening,
            &crate::executor::rbac::Actor::unrestricted(),
        )
    }

    #[test]
    fn cursor_declare_fetch_move_close() {
        let (db, _b, _l) = test_db();
        db.create_table(
            "nums",
            &[crate::storage::ColumnDef {
                name: "n".into(),
                col_type: crate::storage::ColumnType::Int4,
                nullable: false,
                default: None,
                is_pk: true,
            }],
        )
        .unwrap();
        for i in 1..=5 {
            execute_parsed(
                crate::sql::parse(&format!("INSERT INTO nums VALUES ({i})")).unwrap(),
                db.clone(),
                &[],
            )
            .unwrap();
        }

        let mut cursors = HashMap::new();
        let mut listening = HashSet::new();

        let declared = exec(
            "DECLARE c CURSOR FOR SELECT n FROM nums ORDER BY n",
            db.clone(),
            &mut cursors,
            &mut listening,
        )
        .unwrap();
        assert_eq!(declared.tag, "DECLARE CURSOR");
        assert!(cursors.contains_key("c"));

        let first_two = exec("FETCH 2 FROM c", db.clone(), &mut cursors, &mut listening).unwrap();
        assert_eq!(first_two.tag, "FETCH 2");
        assert_eq!(first_two.rows.len(), 2);
        assert_eq!(first_two.rows[0][0].as_deref(), Some(b"1".as_slice()));
        assert_eq!(first_two.rows[1][0].as_deref(), Some(b"2".as_slice()));

        let moved = exec("MOVE 1 FROM c", db.clone(), &mut cursors, &mut listening).unwrap();
        assert_eq!(moved.tag, "MOVE 1");

        let rest = exec("FETCH ALL FROM c", db.clone(), &mut cursors, &mut listening).unwrap();
        assert_eq!(rest.rows.len(), 2); // rows 4 and 5 — row 3 was skipped by MOVE
        assert_eq!(rest.rows[0][0].as_deref(), Some(b"4".as_slice()));

        let closed = exec("CLOSE c", db.clone(), &mut cursors, &mut listening).unwrap();
        assert_eq!(closed.tag, "CLOSE CURSOR");
        assert!(!cursors.contains_key("c"));

        assert!(exec("FETCH 1 FROM c", db.clone(), &mut cursors, &mut listening).is_err());
    }

    #[test]
    fn listen_unlisten_update_session_state() {
        let (db, _b, _l) = test_db();
        let mut cursors = HashMap::new();
        let mut listening = HashSet::new();

        let listened = exec("LISTEN foo", db.clone(), &mut cursors, &mut listening).unwrap();
        assert_eq!(listened.tag, "LISTEN");
        assert!(listening.contains("foo"));

        exec("UNLISTEN foo", db.clone(), &mut cursors, &mut listening).unwrap();
        assert!(!listening.contains("foo"));
    }

    #[test]
    fn notify_delivers_to_subscribed_channel() {
        let (db, _b, _l) = test_db();
        let mut rx = db.subscribe_notifications();

        let mut cursors = HashMap::new();
        let mut listening = HashSet::new();
        let result = exec(
            "NOTIFY foo, 'hello'",
            db.clone(),
            &mut cursors,
            &mut listening,
        )
        .unwrap();
        assert_eq!(result.tag, "NOTIFY");

        let (channel, payload) = rx.try_recv().unwrap();
        assert_eq!(channel, "foo");
        assert_eq!(payload, "hello");
    }

    /// Spawn the real post-auth query loop (`run_query_loop`, the exact
    /// function `run()` delegates to after a SCRAM handshake) over an
    /// in-memory pipe, with a pre-built `Actor`. Returns the client half so
    /// the test can speak the wire protocol to it. Skipping the startup +
    /// SCRAM steps is intentional: those are covered separately by the
    /// `scram` tests and the `roles.is_empty` zero-config branch; this test
    /// targets the wire encoding of the authorization result.
    async fn spawn_denied_session(db: Arc<Database>, actor: Actor) -> tokio::io::DuplexStream {
        let (client, server) = duplex(8192);
        let mut conn = Conn::from_boxed(Box::new(server));
        tokio::spawn(async move {
            let _ = run_query_loop(
                &mut conn,
                "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
                db,
                actor,
            )
            .await;
        });
        client
    }

    /// Send a simple-query (`Q`) protocol message over the wire.
    async fn wire_send_query(stream: &mut tokio::io::DuplexStream, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0); // cstring terminator
        let len = (body.len() as i32) + 4;
        let mut frame = Vec::with_capacity(body.len() + 5);
        frame.push(b'Q');
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&body);
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();
    }

    /// Read backend messages until the first `ErrorResponse`, returning its
    /// SQLSTATE (`C` field); or `None` once a `ReadyForQuery` (success) is
    /// seen. This is the field real clients key on to detect `42501`
    /// (insufficient_privilege) vs. every other error class.
    async fn wire_read_sqlstate(stream: &mut tokio::io::DuplexStream) -> Option<String> {
        let mut header = [0u8; 5];
        loop {
            stream.read_exact(&mut header).await.unwrap();
            let tag = header[0];
            let len = i32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
            let mut body = vec![0u8; len - 4];
            stream.read_exact(&mut body).await.unwrap();
            match tag {
                b'E' => return Some(parse_error_code(&body)),
                b'Z' => return None,
                _ => {}
            }
        }
    }

    /// Extract the `C` (SQLSTATE code) field from a Postgres `ErrorResponse`
    /// body (sequence of `type-byte` + null-terminated value, NUL-terminated).
    fn parse_error_code(body: &[u8]) -> String {
        let mut i = 0;
        while i < body.len() {
            let ftype = body[i];
            i += 1;
            let end = match body[i..].iter().position(|&b| b == 0) {
                Some(p) => i + p,
                None => break,
            };
            let val = String::from_utf8_lossy(&body[i..end]).into_owned();
            if ftype == b'C' {
                return val;
            }
            i = end + 1;
        }
        panic!("ErrorResponse without a SQLSTATE 'C' field");
    }

    #[tokio::test]
    async fn denied_query_returns_sqlstate_42501_on_the_wire() {
        let (db, _b, _l) = test_db();
        db.parse_cached("CREATE TABLE secret (id INT8 PRIMARY KEY)")
            .unwrap();

        // Seed a non-superuser role and build its actor exactly as `run()`
        // does right after a successful SCRAM handshake.
        let roles = RoleStore::new(db.clone()).unwrap();
        roles
            .create_role("reader", false, true, Some("reader-pw"), &[])
            .unwrap();
        let reader = Actor::for_role(&db, "reader").unwrap();
        assert!(!reader.superuser);

        let mut client = spawn_denied_session(db.clone(), reader).await;
        wire_send_query(&mut client, "SELECT * FROM secret").await;
        let code = wire_read_sqlstate(&mut client).await;
        assert_eq!(code.as_deref(), Some("42501"));
    }

    #[tokio::test]
    async fn allowed_query_without_privileges_is_not_denied() {
        let (db, _b, _l) = test_db();
        let roles = RoleStore::new(db.clone()).unwrap();
        roles
            .create_role("reader", false, true, Some("reader-pw"), &[])
            .unwrap();
        let reader = Actor::for_role(&db, "reader").unwrap();

        let mut client = spawn_denied_session(db.clone(), reader).await;
        // No FROM clause ⇒ no table privilege required ⇒ the non-superuser
        // actor is allowed, so no ErrorResponse is sent.
        wire_send_query(&mut client, "SELECT 1").await;
        let code = wire_read_sqlstate(&mut client).await;
        assert_eq!(code, None);
    }

    #[tokio::test]
    async fn syntax_error_returns_generic_sqlstate_42601() {
        let (db, _b, _l) = test_db();
        let roles = RoleStore::new(db.clone()).unwrap();
        roles
            .create_role("reader", false, true, Some("reader-pw"), &[])
            .unwrap();
        let reader = Actor::for_role(&db, "reader").unwrap();

        let mut client = spawn_denied_session(db.clone(), reader).await;
        wire_send_query(&mut client, "THIS IS NOT SQL").await;
        let code = wire_read_sqlstate(&mut client).await;
        assert_eq!(code.as_deref(), Some("42601"));
    }
}
