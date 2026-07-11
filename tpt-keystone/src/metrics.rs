//! Prometheus-compatible metrics: a hand-rolled counter/gauge registry plus
//! a minimal HTTP `/metrics` responder. No metrics/HTTP crate dependency —
//! consistent with this project's stance of hand-writing its own protocol
//! codecs (see `wire`, `sql`) rather than reaching for an off-the-shelf
//! implementation for something this small.
//!
//! Every counter is a plain `AtomicU64` published through a process-wide
//! [`Metrics::global`] singleton, so any module can record an observation
//! without threading a handle through every call site.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

pub struct Metrics {
    start_time: Instant,

    pub connections_active: AtomicU64,
    pub connections_total: AtomicU64,

    pub queries_total: AtomicU64,
    pub query_errors_total: AtomicU64,
    query_duration_nanos_sum: AtomicU64,
    query_duration_count: AtomicU64,

    pub wal_fsyncs_total: AtomicU64,

    pub object_store_gets_total: AtomicU64,
    pub object_store_puts_total: AtomicU64,
    pub object_store_cas_conflicts_total: AtomicU64,

    pub cache_hits_total: AtomicU64,
    pub cache_misses_total: AtomicU64,

    /// Object-store circuit breaker: 1 while open (tripped), 0 otherwise.
    pub object_store_circuit_open: AtomicU64,
    /// Object-store in-flight operations (memory backpressure bound).
    pub object_store_inflight: AtomicU64,
    /// Object-store circuit-breaker trip events since start.
    pub object_store_circuit_trips_total: AtomicU64,

    /// Reader node manifest-staleness: 1 while refresh is failing/aged-out.
    pub reader_manifest_stale: AtomicU64,
    /// Seconds since the reader last successfully refreshed the manifest.
    pub reader_last_refresh_age_seconds: AtomicU64,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

impl Metrics {
    fn new() -> Self {
        Metrics {
            start_time: Instant::now(),
            connections_active: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            queries_total: AtomicU64::new(0),
            query_errors_total: AtomicU64::new(0),
            query_duration_nanos_sum: AtomicU64::new(0),
            query_duration_count: AtomicU64::new(0),
            wal_fsyncs_total: AtomicU64::new(0),
            object_store_gets_total: AtomicU64::new(0),
            object_store_puts_total: AtomicU64::new(0),
            object_store_cas_conflicts_total: AtomicU64::new(0),
            cache_hits_total: AtomicU64::new(0),
            cache_misses_total: AtomicU64::new(0),
            object_store_circuit_open: AtomicU64::new(0),
            object_store_inflight: AtomicU64::new(0),
            object_store_circuit_trips_total: AtomicU64::new(0),
            reader_manifest_stale: AtomicU64::new(0),
            reader_last_refresh_age_seconds: AtomicU64::new(0),
        }
    }

    /// The process-wide metrics registry. Lazily initialized on first use so
    /// unit tests that never touch metrics don't pay for it.
    pub fn global() -> &'static Metrics {
        METRICS.get_or_init(Metrics::new)
    }

    pub fn connection_opened(&self) {
        self.connections_active.fetch_add(1, Ordering::Relaxed);
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_query(&self, duration: Duration, is_err: bool) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        if is_err {
            self.query_errors_total.fetch_add(1, Ordering::Relaxed);
        }
        self.query_duration_nanos_sum
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.query_duration_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_object_store_circuit_open(&self, open: bool) {
        self.object_store_circuit_open
            .store(if open { 1 } else { 0 }, Ordering::Relaxed);
    }

    pub fn set_object_store_inflight(&self, n: u64) {
        self.object_store_inflight.store(n, Ordering::Relaxed);
    }

    pub fn record_object_store_circuit_trip(&self) {
        self.object_store_circuit_trips_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_reader_manifest_stale(&self, stale: bool) {
        self.reader_manifest_stale
            .store(if stale { 1 } else { 0 }, Ordering::Relaxed);
    }

    pub fn set_reader_last_refresh_age_seconds(&self, age: f64) {
        self.reader_last_refresh_age_seconds
            .store(age as u64, Ordering::Relaxed);
    }

    /// Render the registry in Prometheus text exposition format
    /// (https://prometheus.io/docs/instrumenting/exposition_formats/).
    pub fn render(&self) -> String {
        let mut out = String::new();
        let uptime = self.start_time.elapsed().as_secs_f64();
        let dur_sum_seconds = self.query_duration_nanos_sum.load(Ordering::Relaxed) as f64 / 1e9;

        out.push_str("# HELP tpt_uptime_seconds Time since this node process started.\n");
        out.push_str("# TYPE tpt_uptime_seconds gauge\n");
        out.push_str(&format!("tpt_uptime_seconds {uptime}\n"));

        out.push_str("# HELP tpt_connections_active Currently open client connections.\n");
        out.push_str("# TYPE tpt_connections_active gauge\n");
        out.push_str(&format!(
            "tpt_connections_active {}\n",
            self.connections_active.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP tpt_connections_total Client connections accepted since start.\n");
        out.push_str("# TYPE tpt_connections_total counter\n");
        out.push_str(&format!(
            "tpt_connections_total {}\n",
            self.connections_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP tpt_queries_total Queries executed since start.\n");
        out.push_str("# TYPE tpt_queries_total counter\n");
        out.push_str(&format!(
            "tpt_queries_total {}\n",
            self.queries_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP tpt_query_errors_total Queries that returned an error.\n");
        out.push_str("# TYPE tpt_query_errors_total counter\n");
        out.push_str(&format!(
            "tpt_query_errors_total {}\n",
            self.query_errors_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP tpt_query_duration_seconds_sum Cumulative query execution time.\n");
        out.push_str("# TYPE tpt_query_duration_seconds_sum counter\n");
        out.push_str(&format!(
            "tpt_query_duration_seconds_sum {dur_sum_seconds}\n"
        ));

        out.push_str(
            "# HELP tpt_query_duration_seconds_count Number of query duration observations.\n",
        );
        out.push_str("# TYPE tpt_query_duration_seconds_count counter\n");
        out.push_str(&format!(
            "tpt_query_duration_seconds_count {}\n",
            self.query_duration_count.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP tpt_wal_fsyncs_total WAL fsync calls since start.\n");
        out.push_str("# TYPE tpt_wal_fsyncs_total counter\n");
        out.push_str(&format!(
            "tpt_wal_fsyncs_total {}\n",
            self.wal_fsyncs_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP tpt_object_store_gets_total Object store GET calls (post-cache).\n");
        out.push_str("# TYPE tpt_object_store_gets_total counter\n");
        out.push_str(&format!(
            "tpt_object_store_gets_total {}\n",
            self.object_store_gets_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP tpt_object_store_puts_total Object store PUT/PUT-if-match calls.\n");
        out.push_str("# TYPE tpt_object_store_puts_total counter\n");
        out.push_str(&format!(
            "tpt_object_store_puts_total {}\n",
            self.object_store_puts_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP tpt_object_store_cas_conflicts_total Failed compare-and-swap PUTs (lease/manifest contention).\n");
        out.push_str("# TYPE tpt_object_store_cas_conflicts_total counter\n");
        out.push_str(&format!(
            "tpt_object_store_cas_conflicts_total {}\n",
            self.object_store_cas_conflicts_total
                .load(Ordering::Relaxed)
        ));

        out.push_str("# HELP tpt_cache_hits_total NVMe cache-aside hits for sst/wal objects.\n");
        out.push_str("# TYPE tpt_cache_hits_total counter\n");
        out.push_str(&format!(
            "tpt_cache_hits_total {}\n",
            self.cache_hits_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP tpt_cache_misses_total NVMe cache-aside misses for sst/wal objects.\n",
        );
        out.push_str("# TYPE tpt_cache_misses_total counter\n");
        out.push_str(&format!(
            "tpt_cache_misses_total {}\n",
            self.cache_misses_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP tpt_object_store_circuit_open 1 while the object-store circuit breaker is open (tripped).\n",
        );
        out.push_str("# TYPE tpt_object_store_circuit_open gauge\n");
        out.push_str(&format!(
            "tpt_object_store_circuit_open {}\n",
            self.object_store_circuit_open.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP tpt_object_store_inflight Currently in-flight object-store operations (memory backpressure bound).\n",
        );
        out.push_str("# TYPE tpt_object_store_inflight gauge\n");
        out.push_str(&format!(
            "tpt_object_store_inflight {}\n",
            self.object_store_inflight.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP tpt_object_store_circuit_trips_total Object-store circuit-breaker trip events since start.\n",
        );
        out.push_str("# TYPE tpt_object_store_circuit_trips_total counter\n");
        out.push_str(&format!(
            "tpt_object_store_circuit_trips_total {}\n",
            self.object_store_circuit_trips_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP tpt_reader_manifest_stale 1 while a reader node's manifest refresh is failing or aged out.\n",
        );
        out.push_str("# TYPE tpt_reader_manifest_stale gauge\n");
        out.push_str(&format!(
            "tpt_reader_manifest_stale {}\n",
            self.reader_manifest_stale.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP tpt_reader_last_refresh_age_seconds Seconds since the reader last successfully refreshed the manifest.\n",
        );
        out.push_str("# TYPE tpt_reader_last_refresh_age_seconds gauge\n");
        out.push_str(&format!(
            "tpt_reader_last_refresh_age_seconds {}\n",
            self.reader_last_refresh_age_seconds.load(Ordering::Relaxed)
        ));

        out
    }
}

/// Serve `GET /metrics` (Prometheus text exposition) on `addr` until the
/// process exits. Any other path gets a bare 404. This is deliberately not a
/// general-purpose HTTP server — just enough request-line parsing to satisfy
/// a Prometheus scrape or `curl`.
pub async fn serve(addr: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("TPT Keystone metrics endpoint listening on {addr}");
    loop {
        let (mut stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                error!(error = %e, "metrics listener accept failed");
                continue;
            }
        };
        tokio::spawn(async move {
            if let Err(e) = serve_one(&mut stream).await {
                warn!(%peer, error = %e, "metrics request failed");
            }
        });
    }
}

async fn serve_one(stream: &mut tokio::net::TcpStream) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..n]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, body) = if path == "/metrics" {
        ("200 OK", Metrics::global().render())
    } else {
        ("404 Not Found", String::new())
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    Ok(())
}
