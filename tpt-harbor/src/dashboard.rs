//! Web dashboard (TODO.md Phase 15): a hand-rolled HTTP server exposing
//! live migration progress, plus an embedded static page that polls it —
//! same "own port, own accept loop, no external protocol crate" idiom as
//! `tpt-keystone`'s `wire::http_query` (its `read_request`/`json_response`
//! shape is mirrored here almost verbatim; Harbor doesn't depend on
//! `tpt-keystone` as a library, so this is a second small hand-written
//! implementation rather than a shared one — consistent with every other
//! HTTP surface in this repo owning its own request/response code).
//!
//! Scope cut: read-only. There's no way to start/stop/pause a migration
//! from the dashboard — it's a status view over a CLI-driven migration, not
//! a second way to drive one. Opt-in via `--dashboard-addr` on
//! `transfer`/`replicate`/`verify`/`cutover`; no auth, no TLS, wide-open
//! CORS — the same no-auth stance every engine in this repo already takes.

use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::verify::TableVerification;

#[derive(Debug, Clone, Serialize, Default)]
pub struct TableProgress {
    pub table: String,
    pub rows_copied: u64,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct VerificationSummary {
    pub table: String,
    pub source_row_count: u64,
    pub target_row_count: u64,
    pub mismatched_rows: u64,
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct MigrationStatus {
    /// `"discover" | "snapshot" | "replicate" | "verify" | "cutover" | "done" | "failed"`
    pub phase: String,
    pub tables: Vec<TableProgress>,
    pub verifications: Vec<VerificationSummary>,
    pub error: Option<String>,
}

/// A shared, cheaply-cloneable handle a CLI command mutates as a migration
/// runs; the dashboard's `GET /status` route reads a snapshot of it on
/// every poll. `std::sync::Mutex` (not `tokio::sync`) is deliberate — every
/// critical section here is a short, synchronous struct mutation, never
/// held across an `.await`.
#[derive(Clone, Default)]
pub struct StatusHandle(Arc<Mutex<MigrationStatus>>);

impl StatusHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_phase(&self, phase: &str) {
        self.0.lock().unwrap().phase = phase.to_string();
    }

    pub fn set_tables(&self, tables: &[String]) {
        self.0.lock().unwrap().tables = tables.iter().map(|t| TableProgress { table: t.clone(), rows_copied: 0, done: false }).collect();
    }

    pub fn record_progress(&self, table: &str, rows_copied: u64) {
        let mut status = self.0.lock().unwrap();
        if let Some(t) = status.tables.iter_mut().find(|t| t.table == table) {
            t.rows_copied = rows_copied;
        }
    }

    pub fn mark_table_done(&self, table: &str) {
        let mut status = self.0.lock().unwrap();
        if let Some(t) = status.tables.iter_mut().find(|t| t.table == table) {
            t.done = true;
        }
    }

    pub fn set_verifications(&self, results: &[TableVerification]) {
        let mut status = self.0.lock().unwrap();
        status.verifications = results
            .iter()
            .map(|r| VerificationSummary {
                table: r.table.clone(),
                source_row_count: r.source_row_count,
                target_row_count: r.target_row_count,
                mismatched_rows: r.mismatched_rows,
                passed: r.passed,
            })
            .collect();
    }

    pub fn set_error(&self, err: impl std::fmt::Display) {
        self.0.lock().unwrap().error = Some(err.to_string());
    }

    fn snapshot(&self) -> MigrationStatus {
        self.0.lock().unwrap().clone()
    }
}

/// Binds `addr` and serves the dashboard until the process exits — meant to
/// be `tokio::spawn`ed alongside whatever migration phase is actually
/// running, same "own accept loop" shape as `tpt-keystone::wire::http_query::handle`.
pub async fn serve(addr: &str, status: StatusHandle) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    accept_loop(listener, status).await
}

/// Split out from `serve` so tests can `bind("127.0.0.1:0")`, read back the
/// OS-assigned port via `TcpListener::local_addr`, and only then start
/// accepting — `serve` itself never returns the bound address since normal
/// callers always pass a fixed `addr`.
async fn accept_loop(listener: TcpListener, status: StatusHandle) -> anyhow::Result<()> {
    loop {
        let (stream, _peer) = listener.accept().await?;
        let status = status.clone();
        tokio::spawn(async move {
            let _ = handle_conn(stream, status).await;
        });
    }
}

async fn handle_conn(mut stream: TcpStream, status: StatusHandle) -> anyhow::Result<()> {
    let Some((method, path)) = read_request(&mut stream).await? else { return Ok(()) };

    let response = match (method.as_str(), path.as_str()) {
        ("OPTIONS", _) => http_response(204, "application/json", ""),
        ("GET", "/status") => {
            let body = serde_json::to_string(&status.snapshot()).unwrap_or_else(|_| "{}".to_string());
            http_response(200, "application/json", &body)
        }
        ("GET", "/") => http_response(200, "text/html; charset=utf-8", DASHBOARD_HTML),
        _ => http_response(404, "application/json", "{\"error\":\"not found\"}"),
    };

    stream.write_all(&response).await?;
    Ok(())
}

/// Reads just the request line + headers (mirrors
/// `tpt-keystone::wire::http_query::read_request`) — the dashboard has no
/// routes that need a request body, so unlike that sibling implementation
/// this doesn't bother reading `Content-Length` bytes at all.
async fn read_request(stream: &mut TcpStream) -> anyhow::Result<Option<(String, String)>> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return if buf.is_empty() { Ok(None) } else { anyhow::bail!("connection closed mid-request") };
        }
        buf.push(byte[0]);
        if buf.len() >= 4 && &buf[buf.len() - 4..] == b"\r\n\r\n" {
            break;
        }
        anyhow::ensure!(buf.len() <= 16_384, "HTTP request headers too large");
    }
    let head = String::from_utf8_lossy(&buf);
    let mut lines = head.lines();
    let request_line = lines.next().ok_or_else(|| anyhow::anyhow!("empty request"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or_else(|| anyhow::anyhow!("missing HTTP method"))?.to_string();
    let path = parts.next().unwrap_or("/").to_string();
    Ok(Some((method, path)))
}

fn http_response(status: u16, content_type: &str, body: &str) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        404 => "Not Found",
        _ => "Internal Server Error",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {len}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Connection: close\r\n\r\n\
         {body}",
        len = body.len(),
    )
    .into_bytes()
}

/// Self-contained (no CDN/bundler dependency, per this repo's own
/// artifact/dashboard conventions) vanilla-JS page. Polls `GET /status`
/// every second and re-renders — no WebSocket push (this is a much lower
/// event rate than Flux/Canvas, table-by-table row counts, so polling is
/// simple and sufficient rather than adding a second live-push mechanism).
const DASHBOARD_HTML: &str = r##"<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>tpt-harbor migration dashboard</title>
<style>
  body { font: 14px/1.4 -apple-system, sans-serif; margin: 2rem; color: #111; background: #fafafa; }
  h1 { font-size: 1.1rem; }
  .phase { display: inline-block; padding: 2px 10px; border-radius: 999px; background: #2563eb; color: #fff; font-weight: 600; text-transform: uppercase; font-size: 12px; }
  .error { color: #e5484d; font-weight: 600; margin-top: 1rem; }
  table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
  th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #e2e8f0; }
  .bar { height: 8px; background: #e2e8f0; border-radius: 4px; overflow: hidden; width: 160px; }
  .bar > div { height: 100%; background: #2563eb; }
  .done { color: #22c55e; font-weight: 600; }
  .fail { color: #e5484d; font-weight: 600; }
  .pass { color: #22c55e; font-weight: 600; }
</style>
</head>
<body>
<h1>tpt-harbor migration dashboard <span id="phase" class="phase">-</span></h1>
<div id="error" class="error"></div>
<table>
  <thead><tr><th>Table</th><th>Rows copied</th><th></th><th>Status</th></tr></thead>
  <tbody id="tables"></tbody>
</table>
<table>
  <thead><tr><th>Verification</th><th>Source rows</th><th>Target rows</th><th>Mismatched</th><th>Result</th></tr></thead>
  <tbody id="verifications"></tbody>
</table>
<script>
async function poll() {
  try {
    const res = await fetch('/status');
    const s = await res.json();
    document.getElementById('phase').textContent = s.phase || 'idle';
    document.getElementById('error').textContent = s.error ? ('Error: ' + s.error) : '';

    const maxRows = Math.max(1, ...s.tables.map(t => t.rows_copied));
    document.getElementById('tables').innerHTML = (s.tables || []).map(t => `
      <tr>
        <td>${t.table}</td>
        <td>${t.rows_copied}</td>
        <td><div class="bar"><div style="width:${Math.round(100 * t.rows_copied / maxRows)}%"></div></div></td>
        <td class="${t.done ? 'done' : ''}">${t.done ? 'done' : 'in progress'}</td>
      </tr>`).join('');

    document.getElementById('verifications').innerHTML = (s.verifications || []).map(v => `
      <tr>
        <td>${v.table}</td>
        <td>${v.source_row_count}</td>
        <td>${v.target_row_count}</td>
        <td>${v.mismatched_rows}</td>
        <td class="${v.passed ? 'pass' : 'fail'}">${v.passed ? 'PASS' : 'FAIL'}</td>
      </tr>`).join('');
  } catch (e) {
    document.getElementById('error').textContent = 'dashboard: ' + e;
  }
}
poll();
setInterval(poll, 1000);
</script>
</body>
</html>
"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_handle_tracks_table_progress_and_completion() {
        let status = StatusHandle::new();
        status.set_phase("snapshot");
        status.set_tables(&["orders".to_string(), "customers".to_string()]);
        status.record_progress("orders", 100);
        status.mark_table_done("orders");

        let snap = status.snapshot();
        assert_eq!(snap.phase, "snapshot");
        assert_eq!(snap.tables.len(), 2);
        let orders = snap.tables.iter().find(|t| t.table == "orders").unwrap();
        assert_eq!(orders.rows_copied, 100);
        assert!(orders.done);
        let customers = snap.tables.iter().find(|t| t.table == "customers").unwrap();
        assert_eq!(customers.rows_copied, 0);
        assert!(!customers.done);
    }

    #[test]
    fn status_handle_records_verification_results_and_errors() {
        let status = StatusHandle::new();
        status.set_verifications(&[TableVerification {
            table: "orders".to_string(),
            source_row_count: 10,
            target_row_count: 10,
            mismatched_rows: 0,
            passed: true,
        }]);
        status.set_error("connection reset");

        let snap = status.snapshot();
        assert_eq!(snap.verifications.len(), 1);
        assert!(snap.verifications[0].passed);
        assert_eq!(snap.error.as_deref(), Some("connection reset"));
    }

    /// Real-socket end-to-end test: binds an OS-assigned loopback port,
    /// drives it with a plain `TcpStream` writing a raw HTTP/1.1 request
    /// (no HTTP client crate, matching this endpoint's own hand-rolled
    /// parsing) — same style as `tpt-keystone`'s
    /// `wire::http_query_tests`.
    #[tokio::test]
    async fn status_and_index_routes_respond_over_a_real_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let status = StatusHandle::new();
        status.set_phase("snapshot");
        status.set_tables(&["orders".to_string()]);
        status.record_progress("orders", 7);

        let status_for_server = status.clone();
        tokio::spawn(accept_loop(listener, status_for_server));

        // GET /status
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /status HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "unexpected response: {response}");
        assert!(response.contains("Content-Type: application/json"));
        assert!(response.contains("\"phase\":\"snapshot\""));
        assert!(response.contains("\"rows_copied\":7"));

        // GET /
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "unexpected response: {response}");
        assert!(response.contains("Content-Type: text/html"));
        assert!(response.contains("tpt-harbor migration dashboard"));
        assert!(response.contains("fetch('/status')"));
    }
}
