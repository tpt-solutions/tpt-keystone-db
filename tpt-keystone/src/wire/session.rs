use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use super::codec::{Conn, FrontendMessage};
use super::messages::{oid, BackendMessage, ErrorInfo, FieldDescription, TransactionStatus};
use crate::executor::eval::Value;
use crate::executor::{describe_select, execute_parsed, max_param_index, QueryResult};
use crate::sql::ast::{FetchCount, Stmt};
use crate::storage::database::Database;

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
pub async fn handle(stream: TcpStream, peer: std::net::SocketAddr, db: Arc<Database>) {
    info!(%peer, "client connected");
    let mut conn = Conn::new(stream);

    if let Err(e) = run(&mut conn, peer, db).await {
        debug!(%peer, "session ended: {e}");
    }
    info!(%peer, "client disconnected");
}

async fn run(conn: &mut Conn, peer: std::net::SocketAddr, db: Arc<Database>) -> anyhow::Result<()> {
    // --- Startup handshake ---
    let startup = conn.read_startup().await?;
    debug!(%peer, version = startup.protocol_version, "startup received");

    conn.send(&BackendMessage::AuthenticationOk);
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
                handle_simple_query(conn, &sql, db.clone(), &mut ext.cursors, &mut ext.listening).await?;
            }

            FrontendMessage::Parse { name, query, param_types } => {
                debug!(%peer, "parse: {query}");
                match crate::sql::parse(&query) {
                    Ok(stmt) => {
                        ext.prepared.insert(name, PreparedStmt { stmt, param_types });
                        conn.send(&BackendMessage::ParseComplete);
                    }
                    Err(e) => {
                        conn.send_error(ErrorInfo::new("42601", e.to_string()));
                    }
                }
                conn.flush().await?;
            }

            FrontendMessage::Bind { portal, stmt, params, param_formats, .. } => {
                match ext.prepared.get(&stmt) {
                    Some(prepared) => {
                        let values: Vec<Value> = params.iter().enumerate().map(|(i, bytes)| {
                            let format = param_formats.get(i).copied().unwrap_or(0);
                            decode_param(bytes.as_deref(), format)
                        }).collect();
                        ext.portals.insert(portal, Portal { stmt: prepared.stmt.clone(), params: values });
                        conn.send(&BackendMessage::BindComplete);
                    }
                    None => {
                        conn.send_error(ErrorInfo::new("26000", format!("prepared statement \"{stmt}\" does not exist")));
                    }
                }
                conn.flush().await?;
            }

            FrontendMessage::Describe { kind, name } => {
                if kind == b'S' {
                    match ext.prepared.get(&name) {
                        Some(prepared) => {
                            let n_params = max_param_index(&prepared.stmt).max(prepared.param_types.len() as u32);
                            let types: Vec<i32> = (0..n_params).map(|i| {
                                prepared.param_types.get(i as usize).copied().filter(|t| *t != 0).unwrap_or(oid::TEXT)
                            }).collect();
                            conn.send(&BackendMessage::ParameterDescription(types));
                            send_row_description_for(conn, &prepared.stmt, &db)?;
                        }
                        None => {
                            conn.send_error(ErrorInfo::new("26000", format!("prepared statement \"{name}\" does not exist")));
                        }
                    }
                } else {
                    match ext.portals.get(&name) {
                        Some(portal) => send_row_description_for(conn, &portal.stmt, &db)?,
                        None => {
                            conn.send_error(ErrorInfo::new("34000", format!("portal \"{name}\" does not exist")));
                        }
                    }
                }
                conn.flush().await?;
            }

            FrontendMessage::Execute { portal, max_rows } => {
                match ext.portals.get(&portal) {
                    Some(p) => {
                        match execute_parsed(p.stmt.clone(), db.clone(), &p.params) {
                            Ok(result) => {
                                let total = result.rows.len();
                                let limited = max_rows > 0 && (max_rows as usize) < total;
                                let rows = if limited { &result.rows[..max_rows as usize] } else { &result.rows[..] };
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
                                conn.send_error(ErrorInfo::new("42601", e.to_string()));
                            }
                        }
                    }
                    None => {
                        conn.send_error(ErrorInfo::new("34000", format!("portal \"{portal}\" does not exist")));
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
        }
    }
}

async fn handle_simple_query(
    conn: &mut Conn,
    sql: &str,
    db: Arc<Database>,
    cursors: &mut HashMap<String, CursorState>,
    listening: &mut HashSet<String>,
) -> anyhow::Result<()> {
    if sql.is_empty() {
        conn.send(&BackendMessage::EmptyQueryResponse);
        conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
        conn.flush().await?;
        return Ok(());
    }

    match execute_simple(sql, db, cursors, listening) {
        Ok(result) => {
            conn.send(&BackendMessage::RowDescription(result.fields));
            for row in result.rows {
                conn.send(&BackendMessage::DataRow(row));
            }
            conn.send(&BackendMessage::CommandComplete(result.tag));
        }
        Err(e) => {
            error!("query error: {e}");
            conn.send_error(ErrorInfo::new("42601", e.to_string()));
        }
    }
    conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    conn.flush().await?;
    Ok(())
}

/// Parse and execute one simple-query-protocol statement, intercepting
/// cursor statements (`DECLARE`/`FETCH`/`MOVE`/`CLOSE`) to manage
/// session-local `cursors` state; everything else is delegated to the
/// stateless executor.
fn execute_simple(
    sql: &str,
    db: Arc<Database>,
    cursors: &mut HashMap<String, CursorState>,
    listening: &mut HashSet<String>,
) -> anyhow::Result<QueryResult> {
    let stmt = crate::sql::parse(sql)?;
    match stmt {
        Stmt::Listen(channel) => {
            listening.insert(channel);
            Ok(QueryResult { fields: vec![], rows: vec![], tag: "LISTEN".into() })
        }
        Stmt::Unlisten(channel) => {
            if channel == "*" { listening.clear(); } else { listening.remove(&channel); }
            Ok(QueryResult { fields: vec![], rows: vec![], tag: "UNLISTEN".into() })
        }
        Stmt::DeclareCursor(d) => {
            let result = execute_parsed(Stmt::Select(d.query), db, &[])?;
            cursors.insert(d.name, CursorState { fields: result.fields, rows: result.rows, pos: 0 });
            Ok(QueryResult { fields: vec![], rows: vec![], tag: "DECLARE CURSOR".into() })
        }
        Stmt::Fetch(f) => {
            let cursor = cursors.get_mut(&f.cursor)
                .ok_or_else(|| anyhow::anyhow!("cursor \"{}\" does not exist", f.cursor))?;
            let n = fetch_count(&f.count, cursor);
            let end = (cursor.pos + n).min(cursor.rows.len());
            let rows = cursor.rows[cursor.pos..end].to_vec();
            cursor.pos = end;
            let fields = cursor.fields.clone();
            let count = rows.len();
            Ok(QueryResult { fields, rows, tag: format!("FETCH {count}") })
        }
        Stmt::MoveCursor(f) => {
            let cursor = cursors.get_mut(&f.cursor)
                .ok_or_else(|| anyhow::anyhow!("cursor \"{}\" does not exist", f.cursor))?;
            let n = fetch_count(&f.count, cursor);
            let end = (cursor.pos + n).min(cursor.rows.len());
            let moved = end - cursor.pos;
            cursor.pos = end;
            Ok(QueryResult { fields: vec![], rows: vec![], tag: format!("MOVE {moved}") })
        }
        Stmt::CloseCursor(name) => {
            cursors.remove(&name).ok_or_else(|| anyhow::anyhow!("cursor \"{name}\" does not exist"))?;
            Ok(QueryResult { fields: vec![], rows: vec![], tag: "CLOSE CURSOR".into() })
        }
        other => execute_parsed(other, db, &[]),
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

/// Send RowDescription (or NoData for non-SELECT / no-FROM statements) for
/// a prepared statement or portal, ahead of Execute.
fn send_row_description_for(conn: &mut Conn, stmt: &Stmt, db: &Arc<Database>) -> anyhow::Result<()> {
    match stmt {
        Stmt::Select(select) => {
            match describe_select(select, db.clone()) {
                Ok(fields) if !fields.is_empty() => conn.send(&BackendMessage::RowDescription(fields)),
                Ok(_) => conn.send(&BackendMessage::NoData),
                Err(_) => conn.send(&BackendMessage::NoData),
            }
        }
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

