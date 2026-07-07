mod executor;
mod geo;
mod graph;
mod mcp;
mod metrics;
mod sql;
mod storage;
mod telemetry;
mod wire;

use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

use storage::cache::CachedObjectStore;
use storage::config::{NodeRole, StorageConfig};
use storage::database::Database;
use storage::lease::LeaseManager;
use storage::objectstore::ObjectStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();

    let config = StorageConfig::from_env();
    info!(node_id = %config.node_id, role = ?config.role, backend = ?config.backend, "starting TPT Keystone compute node");

    // The shared durable backend (local-fs emulation or real S3), wrapped in
    // a local NVMe cache-aside layer — this node's disk is otherwise
    // disposable, which is what makes it a stateless compute node.
    let backend = config.build_object_store().await?;
    let store: Arc<dyn ObjectStore> = Arc::new(CachedObjectStore::new(backend, &config.cache_dir, config.cache_max_bytes)?);

    // Only one node may hold the write lease at a time; readers never
    // attempt to acquire it and just poll the manifest instead.
    let lease_mgr = Arc::new(LeaseManager::new(store.clone(), "db", config.node_id.clone(), config.lease_ttl));
    if config.role == NodeRole::Writer {
        lease_mgr.try_acquire()?;
        lease_mgr.clone().spawn_renewal_task();
    }

    let db = Arc::new(Database::open(&config.local_dir, store, lease_mgr.handle(), config.role, config.udf)?);
    info!(dir = %config.local_dir.display(), "Database opened");

    if config.role == NodeRole::Reader {
        let db = db.clone();
        let interval = config.manifest_refresh_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(e) = db.refresh() {
                    error!(error = %e, "manifest refresh failed");
                }
            }
        });
    }

    let addr = "0.0.0.0:5432";
    let listener = TcpListener::bind(addr).await?;
    info!("TPT Keystone listening on {addr}");

    // Admission control: every connection already shares this one `db`, so
    // there's no per-connection backend resource to pool the way pgbouncer
    // pools real Postgres backend processes — this just bounds concurrency,
    // queuing new connections past the limit rather than erroring.
    let connection_slots = Arc::new(tokio::sync::Semaphore::new(config.max_connections));

    // MCP (Model Context Protocol) server, alongside the Postgres listener —
    // one request/response per connection, no admission-control semaphore
    // needed since there's no long-lived per-connection state to bound.
    // Overridable because 5433 is also a common alternate-Postgres port
    // (e.g. Postgres.app), so a fixed default can collide with an unrelated
    // service already on the host.
    let mcp_addr = std::env::var("TPT_MCP_ADDR").unwrap_or_else(|_| "0.0.0.0:5433".to_string());
    let mcp_listener = TcpListener::bind(&mcp_addr).await?;
    info!("TPT Keystone MCP server listening on {mcp_addr}");
    let mcp_db = db.clone();
    let mcp_token = config.mcp_token.clone();
    tokio::spawn(async move {
        loop {
            match mcp_listener.accept().await {
                Ok((stream, peer)) => {
                    let db = mcp_db.clone();
                    let token = mcp_token.clone();
                    tokio::spawn(async move {
                        mcp::handle(stream, peer, db, token).await;
                    });
                }
                Err(e) => error!(error = %e, "MCP listener accept failed"),
            }
        }
    });

    // Flux (Phase 11) WebSocket streaming endpoint, alongside the Postgres
    // and MCP listeners — same "own port, own accept loop" shape as MCP
    // above. Overridable for the same reason `TPT_MCP_ADDR` is: a fixed
    // default can collide with an unrelated service already on the host.
    let flux_ws_addr = std::env::var("TPT_FLUX_WS_ADDR").unwrap_or_else(|_| "0.0.0.0:5434".to_string());
    let flux_ws_listener = TcpListener::bind(&flux_ws_addr).await?;
    info!("TPT Keystone Flux WebSocket endpoint listening on {flux_ws_addr}");
    let flux_ws_db = db.clone();
    tokio::spawn(async move {
        loop {
            match flux_ws_listener.accept().await {
                Ok((stream, peer)) => {
                    let db = flux_ws_db.clone();
                    tokio::spawn(async move {
                        wire::websocket::handle(stream, peer, db).await;
                    });
                }
                Err(e) => error!(error = %e, "Flux WebSocket listener accept failed"),
            }
        }
    });

    // Prometheus metrics endpoint (Phase 12 — production hardening). Its own
    // port for the same reason MCP/Flux get their own: independent of the
    // Postgres wire listener's connection-admission semaphore, since scrapes
    // shouldn't queue behind client traffic.
    let metrics_addr = std::env::var("TPT_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9187".to_string());
    tokio::spawn(async move {
        if let Err(e) = metrics::serve(&metrics_addr).await {
            error!(error = %e, "metrics endpoint failed");
        }
    });

    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let db = db.clone();
        let permit = connection_slots.clone().acquire_owned().await?;
        tokio::spawn(async move {
            wire::session::handle(stream, peer, db).await;
            drop(permit);
        });
    }
}
