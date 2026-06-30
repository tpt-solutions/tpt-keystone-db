use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use super::codec::{Conn, FrontendMessage};
use super::messages::{BackendMessage, ErrorInfo, TransactionStatus};
use crate::executor::execute_query;

/// Drive a single client connection from startup through the query loop.
pub async fn handle(stream: TcpStream, peer: std::net::SocketAddr) {
    info!(%peer, "client connected");
    let mut conn = Conn::new(stream);

    if let Err(e) = run(&mut conn, peer).await {
        debug!(%peer, "session ended: {e}");
    }
    info!(%peer, "client disconnected");
}

async fn run(conn: &mut Conn, peer: std::net::SocketAddr) -> anyhow::Result<()> {
    // --- Startup handshake ---
    let startup = conn.read_startup().await?;
    debug!(%peer, version = startup.protocol_version, "startup received");

    // Send auth OK + required parameter statuses + backend key + ready.
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

    // --- Query loop ---
    loop {
        match conn.read_message().await? {
            FrontendMessage::Query(sql) => {
                let sql = sql.trim().to_string();
                debug!(%peer, %sql, "query received");
                handle_simple_query(conn, &sql).await?;
            }

            // Extended query protocol — minimal stubs so psql doesn't hang.
            FrontendMessage::Parse { query, .. } => {
                debug!(%peer, "parse: {query}");
                conn.send(&BackendMessage::NoticeResponse("extended query protocol is not fully supported".into()));
                conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
                conn.flush().await?;
            }
            FrontendMessage::Bind { .. } => {
                conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
                conn.flush().await?;
            }
            FrontendMessage::Describe { .. } => {
                conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
                conn.flush().await?;
            }
            FrontendMessage::Execute { .. } => {
                conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
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

async fn handle_simple_query(conn: &mut Conn, sql: &str) -> anyhow::Result<()> {
    if sql.is_empty() {
        conn.send(&BackendMessage::EmptyQueryResponse);
        conn.send(&BackendMessage::ReadyForQuery(TransactionStatus::Idle));
        conn.flush().await?;
        return Ok(());
    }

    match execute_query(sql) {
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
