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
    /// The client's requested result-column wire formats from `Bind`
    /// (0 = text, 1 = binary). Postgres semantics: an empty list means all
    /// text, a single entry applies to every column, otherwise it's one code
    /// per column. Resolved against each column's actual type at `Execute`/
    /// `Describe` time via `resolve_result_formats`.
    result_formats: Vec<i16>,
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
    // Per-connection open transaction (Phase 1). `None` means autocommit.
    let mut txn: Option<crate::storage::database::txn::TxnHandle> = None;

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
                handle_simple_query(conn, &sql, db.clone(), &mut ext.cursors, &mut ext.listening, &mut txn, &actor)
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
                result_formats,
            } => {
                match ext.prepared.get(&stmt) {
                    Some(prepared) => {
                        let values: Vec<Value> = params
                            .iter()
                            .enumerate()
                            .map(|(i, bytes)| {
                                let format = param_formats.get(i).copied().unwrap_or(0);
                                let type_oid =
                                    prepared.param_types.get(i).copied().unwrap_or(0);
                                decode_param(bytes.as_deref(), format, type_oid)
                            })
                            .collect();
                        ext.portals.insert(
                            portal,
                            Portal {
                                stmt: prepared.stmt.clone(),
                                params: values,
                                result_formats,
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
                            send_row_description_for(conn, &prepared.stmt, &db, &[])?;
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
                        Some(portal) => {
                            send_row_description_for(conn, &portal.stmt, &db, &portal.result_formats)?
                        }
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
                    Some(p) => match execute_parsed_as(p.stmt.clone(), db.clone(), &p.params, &actor, txn.as_ref()) {
                        Ok(result) => {
                            let total = result.rows.len();
                            let limited = max_rows > 0 && (max_rows as usize) < total;
                            let rows = if limited {
                                &result.rows[..max_rows as usize]
                            } else {
                                &result.rows[..]
                            };
                            // Resolve the client's requested result formats
                            // (from `Bind`) against the actual column types,
                            // then re-encode each cell accordingly. Binary is
                            // only used for columns whose type we can encode
                            // that way; everything else stays text, matching
                            // the format codes a `Describe` on this portal
                            // would have reported.
                            let oids: Vec<i32> =
                                result.fields.iter().map(|f| f.type_oid).collect();
                            let formats = resolve_result_formats(&p.result_formats, &oids);
                            for row in rows {
                                conn.send(&BackendMessage::DataRow(encode_result_row(
                                    row, &oids, &formats,
                                )));
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
    txn: &mut Option<crate::storage::database::txn::TxnHandle>,
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

    // Transaction-control statements are intercepted here so the per-connection
    // `txn` handle is managed on the wire side; the executor only sees them as
    // no-ops (the actual staged-write flush/discard happens here via COMMIT/
    // ROLLBACK). `BEGIN` is idempotent (a second BEGIN just keeps the open
    // transaction), matching Postgres's implicit-transaction semantics.
    match &stmt {
        Stmt::Begin => {
            if txn.is_none() {
                *txn = Some(db.begin_txn());
            }
            conn.send(&BackendMessage::CommandComplete("BEGIN".into()));
            conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::InTransaction));
            conn.flush().await?;
            return Ok(());
        }
        Stmt::Commit => {
            if let Some(t) = txn.take() {
                if let Err(e) = db.commit_txn(&t) {
                    conn.send_error(ErrorInfo::new(sqlstate_for(&e), e.to_string()));
                    conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
                    conn.flush().await?;
                    return Ok(());
                }
            }
            conn.send(&BackendMessage::CommandComplete("COMMIT".into()));
            conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
            conn.flush().await?;
            return Ok(());
        }
        Stmt::Rollback => {
            if let Some(t) = txn.take() {
                db.rollback_txn(&t);
            }
            conn.send(&BackendMessage::CommandComplete("ROLLBACK".into()));
            conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
            conn.flush().await?;
            return Ok(());
        }
        _ => {}
    }

    match execute_simple(stmt, db, cursors, listening, txn.as_ref(), actor) {
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
    // Report InTransaction when a transaction is open so a client can tell it
    // is still inside one (mirrors Postgres's ReadyForQuery status byte).
    let status = if txn.is_some() {
        TransactionStatus::InTransaction
    } else {
        TransactionStatus::Idle
    };
    conn.send(&BackendMessage::ReadyForQuery(status));
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
    txn: Option<&crate::storage::database::txn::TxnHandle>,
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
            let result = execute_parsed_as(Stmt::Select(d.query), db, &[], actor, None)?;
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
        other => execute_parsed_as(other, db, &[], actor, txn),
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
/// a prepared statement or portal, ahead of Execute. `result_formats` is the
/// portal's requested per-column wire formats from `Bind` (empty for a
/// statement-level Describe, where the formats aren't known yet — Postgres
/// reports text in that case); each field's advertised `format` is set to
/// what will actually be sent, honoring a binary request only for types we
/// can encode in binary.
fn send_row_description_for(
    conn: &mut Conn,
    stmt: &Stmt,
    db: &Arc<Database>,
    result_formats: &[i16],
) -> anyhow::Result<()> {
    match stmt {
        Stmt::Select(select) => match describe_select(select, db.clone()) {
            Ok(mut fields) if !fields.is_empty() => {
                let oids: Vec<i32> = fields.iter().map(|f| f.type_oid).collect();
                let formats = resolve_result_formats(result_formats, &oids);
                for (f, fmt) in fields.iter_mut().zip(formats) {
                    f.format = fmt;
                }
                conn.send(&BackendMessage::RowDescription(fields))
            }
            Ok(_) => conn.send(&BackendMessage::NoData),
            Err(_) => conn.send(&BackendMessage::NoData),
        },
        _ => conn.send(&BackendMessage::NoData),
    }
    Ok(())
}

/// Resolve a `Bind` result-format list against the result columns' type OIDs,
/// yielding one effective wire-format code per column (0 = text, 1 = binary).
///
/// Postgres list semantics: no entries ⇒ all text; one entry ⇒ applies to
/// every column; otherwise one per column. A binary request is downgraded to
/// text for any type this engine can't binary-encode, so the returned code
/// always matches the bytes that will actually be written.
fn resolve_result_formats(requested: &[i16], oids: &[i32]) -> Vec<i16> {
    oids.iter()
        .enumerate()
        .map(|(i, &type_oid)| {
            let want = match requested.len() {
                0 => 0,
                1 => requested[0],
                _ => requested.get(i).copied().unwrap_or(0),
            };
            if want == 1 && crate::wire::messages::supports_binary(type_oid) {
                1
            } else {
                0
            }
        })
        .collect()
}

/// Re-encode one result row's text cells into the per-column wire formats
/// computed by `resolve_result_formats`. Text columns pass through unchanged;
/// binary columns are converted from the stored text form to the Postgres
/// binary representation (with a defensive text fallback that can't normally
/// trigger, since `resolve_result_formats` only picks binary for encodable
/// types).
fn encode_result_row(
    row: &[Option<Vec<u8>>],
    oids: &[i32],
    formats: &[i16],
) -> Vec<Option<Vec<u8>>> {
    row.iter()
        .enumerate()
        .map(|(i, cell)| {
            let cell = cell.as_ref()?;
            let fmt = formats.get(i).copied().unwrap_or(0);
            if fmt == 1 {
                let type_oid = oids.get(i).copied().unwrap_or(oid::TEXT);
                Some(
                    crate::wire::messages::text_cell_to_binary(cell, type_oid)
                        .unwrap_or_else(|| cell.clone()),
                )
            } else {
                Some(cell.clone())
            }
        })
        .collect()
}

/// Decode one bound parameter value using its declared wire format
/// (0 = text, 1 = binary) and, for binary, its declared type OID. Binary
/// decoding covers the fixed-width numeric types and bool; the type OID is
/// needed to tell an 8-byte `float8` apart from an 8-byte `int8` (both are
/// 8 bytes on the wire), which a length-only heuristic gets wrong. Falls back
/// to length-based sniffing when the OID is unspecified (0).
fn decode_param(bytes: Option<&[u8]>, format: i16, type_oid: i32) -> Value {
    let Some(b) = bytes else { return Value::Null };
    if format == 0 {
        return Value::from_bytes(Some(b));
    }
    // Binary format.
    match type_oid {
        oid::INT8 if b.len() == 8 => return Value::Int(i64::from_be_bytes(b.try_into().unwrap())),
        oid::INT4 if b.len() == 4 => {
            return Value::Int(i32::from_be_bytes(b.try_into().unwrap()) as i64)
        }
        oid::INT2 if b.len() == 2 => {
            return Value::Int(i16::from_be_bytes(b.try_into().unwrap()) as i64)
        }
        oid::FLOAT8 if b.len() == 8 => {
            return Value::Float(f64::from_be_bytes(b.try_into().unwrap()))
        }
        oid::FLOAT4 if b.len() == 4 => {
            return Value::Float(f32::from_be_bytes(b.try_into().unwrap()) as f64)
        }
        oid::BOOL if b.len() == 1 => return Value::Bool(b[0] != 0),
        oid::TEXT => {
            if let Ok(s) = std::str::from_utf8(b) {
                return Value::Text(s.to_string());
            }
        }
        _ => {}
    }
    // Unknown/unspecified OID: fall back to width-based sniffing (legacy
    // behavior), with a float8-vs-int8 caveat only relevant when the client
    // sends 8-byte binary params without declaring their type.
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
            None,
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

    // --- Binary result-format encoding (extended query protocol) ---

    /// Spawn the post-auth query loop with an unrestricted actor over a pipe,
    /// returning the client half — same shape as `spawn_denied_session` but
    /// without RBAC restrictions, for exercising the extended query protocol.
    async fn spawn_session(db: Arc<Database>) -> tokio::io::DuplexStream {
        spawn_denied_session(db, Actor::unrestricted()).await
    }

    /// Frame and send one extended-protocol message (`tag` + body).
    async fn wire_send(stream: &mut tokio::io::DuplexStream, tag: u8, body: &[u8]) {
        let len = (body.len() as i32) + 4;
        let mut frame = Vec::with_capacity(body.len() + 5);
        frame.push(tag);
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(body);
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();
    }

    fn cstr(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    /// Read one backend message, returning `(tag, body)`.
    async fn wire_read_msg(stream: &mut tokio::io::DuplexStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; 5];
        stream.read_exact(&mut header).await.unwrap();
        let len = i32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        let mut body = vec![0u8; len - 4];
        stream.read_exact(&mut body).await.unwrap();
        (header[0], body)
    }

    /// Read backend messages until the first `DataRow` (`D`), returning its
    /// decoded cells; panics if an `ErrorResponse` (`E`) arrives first.
    async fn wire_read_data_row(stream: &mut tokio::io::DuplexStream) -> Vec<Option<Vec<u8>>> {
        loop {
            let (tag, body) = wire_read_msg(stream).await;
            match tag {
                b'D' => return parse_data_row(&body),
                b'E' => panic!("unexpected ErrorResponse: {}", parse_error_code(&body)),
                _ => {}
            }
        }
    }

    /// Decode a `DataRow` body into its cells.
    fn parse_data_row(body: &[u8]) -> Vec<Option<Vec<u8>>> {
        let n = i16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
        let mut cells = Vec::with_capacity(n);
        let mut i = 2;
        for _ in 0..n {
            let len = i32::from_be_bytes(body[i..i + 4].try_into().unwrap());
            i += 4;
            if len < 0 {
                cells.push(None);
            } else {
                let len = len as usize;
                cells.push(Some(body[i..i + len].to_vec()));
                i += len;
            }
        }
        cells
    }

    /// Run a full Parse/Bind(result_formats)/Execute/Sync sequence for
    /// `SELECT * FROM <table>` and return the first data row's cells.
    async fn select_star_row(
        client: &mut tokio::io::DuplexStream,
        table: &str,
        result_formats: &[i16],
    ) -> Vec<Option<Vec<u8>>> {
        // Parse: unnamed statement, the query, zero param types.
        let mut parse = cstr(""); // statement name
        parse.extend_from_slice(&cstr(&format!("SELECT * FROM {table}")));
        parse.extend_from_slice(&0i16.to_be_bytes()); // param count
        wire_send(client, b'P', &parse).await;

        // Bind: unnamed portal, unnamed statement, no param formats, no
        // params, then the requested result formats.
        let mut bind = cstr(""); // portal
        bind.extend_from_slice(&cstr("")); // statement
        bind.extend_from_slice(&0i16.to_be_bytes()); // param format count
        bind.extend_from_slice(&0i16.to_be_bytes()); // param count
        bind.extend_from_slice(&(result_formats.len() as i16).to_be_bytes());
        for f in result_formats {
            bind.extend_from_slice(&f.to_be_bytes());
        }
        wire_send(client, b'B', &bind).await;

        // Execute: unnamed portal, no row limit.
        let mut exec = cstr("");
        exec.extend_from_slice(&0i32.to_be_bytes());
        wire_send(client, b'E', &exec).await;

        // Sync.
        wire_send(client, b'S', &[]).await;

        // Skip ParseComplete/BindComplete, read the first DataRow.
        wire_read_data_row(client).await
    }

    fn setup_typed_table(db: &Arc<Database>) {
        execute_parsed(
            crate::sql::parse(
                "CREATE TABLE t (id INT8 PRIMARY KEY, score FLOAT8, active BOOL, name TEXT)",
            )
            .unwrap(),
            db.clone(),
            &[],
        )
        .unwrap();
        execute_parsed(
            crate::sql::parse("INSERT INTO t VALUES (7, 2.5, true, 'hi')").unwrap(),
            db.clone(),
            &[],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn binary_result_format_encodes_numeric_columns() {
        let (db, _b, _l) = test_db();
        setup_typed_table(&db);
        let mut client = spawn_session(db.clone()).await;

        // result_formats = [1] applies binary to every column.
        let row = select_star_row(&mut client, "t", &[1]).await;
        assert_eq!(row.len(), 4);
        // id INT8 → 8-byte big-endian.
        assert_eq!(
            row[0].as_deref().unwrap(),
            &7i64.to_be_bytes(),
            "int8 should be binary"
        );
        // score FLOAT8 → 8-byte big-endian IEEE-754.
        assert_eq!(
            f64::from_be_bytes(row[1].as_deref().unwrap().try_into().unwrap()),
            2.5
        );
        // active BOOL → single 0/1 byte.
        assert_eq!(row[2].as_deref().unwrap(), &[1u8]);
        // name TEXT → raw UTF-8 (binary text == text bytes).
        assert_eq!(row[3].as_deref().unwrap(), b"hi");
    }

    #[tokio::test]
    async fn text_result_format_is_unchanged_default() {
        let (db, _b, _l) = test_db();
        setup_typed_table(&db);
        let mut client = spawn_session(db.clone()).await;

        // No result formats ⇒ all text (the default, backward-compatible path).
        let row = select_star_row(&mut client, "t", &[]).await;
        assert_eq!(row[0].as_deref().unwrap(), b"7");
        assert_eq!(row[1].as_deref().unwrap(), b"2.5");
        assert_eq!(row[2].as_deref().unwrap(), b"t");
        assert_eq!(row[3].as_deref().unwrap(), b"hi");
    }

    #[tokio::test]
    async fn per_column_result_formats_mix_binary_and_text() {
        let (db, _b, _l) = test_db();
        setup_typed_table(&db);
        let mut client = spawn_session(db.clone()).await;

        // Per-column: binary id, text score, binary active, text name.
        let row = select_star_row(&mut client, "t", &[1, 0, 1, 0]).await;
        assert_eq!(row[0].as_deref().unwrap(), &7i64.to_be_bytes());
        assert_eq!(row[1].as_deref().unwrap(), b"2.5"); // text
        assert_eq!(row[2].as_deref().unwrap(), &[1u8]);
        assert_eq!(row[3].as_deref().unwrap(), b"hi");
    }

    #[tokio::test]
    async fn describe_portal_reports_resolved_formats() {
        let (db, _b, _l) = test_db();
        setup_typed_table(&db);
        let mut client = spawn_session(db.clone()).await;

        // Parse + Bind with all-binary, then Describe the portal ('D' kind 'P').
        let mut parse = cstr("");
        parse.extend_from_slice(&cstr("SELECT * FROM t"));
        parse.extend_from_slice(&0i16.to_be_bytes());
        wire_send(&mut client, b'P', &parse).await;

        let mut bind = cstr("");
        bind.extend_from_slice(&cstr(""));
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&1i16.to_be_bytes()); // one result format...
        bind.extend_from_slice(&1i16.to_be_bytes()); // ...= binary for all
        wire_send(&mut client, b'B', &bind).await;

        let mut describe = vec![b'P'];
        describe.extend_from_slice(&cstr("")); // portal name
        wire_send(&mut client, b'D', &describe).await;
        wire_send(&mut client, b'S', &[]).await;

        // Skip ParseComplete/BindComplete, find the RowDescription ('T').
        let body = loop {
            let (tag, body) = wire_read_msg(&mut client).await;
            if tag == b'T' {
                break body;
            }
            assert_ne!(tag, b'E', "unexpected error");
        };
        // Every field's advertised format code (last i16 of each field entry)
        // must be 1 (binary), since all four columns are binary-encodable.
        let n = i16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
        assert_eq!(n, 4);
        let mut i = 2;
        for _ in 0..n {
            // name cstr
            let end = body[i..].iter().position(|&b| b == 0).unwrap() + i;
            i = end + 1;
            i += 4 + 2 + 4 + 2 + 4; // table_oid, col_attr, type_oid, type_size, type_modifier
            let format = i16::from_be_bytes(body[i..i + 2].try_into().unwrap());
            i += 2;
            assert_eq!(format, 1, "field format should be binary");
        }
    }
}

