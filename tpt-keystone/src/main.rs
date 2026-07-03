mod executor;
mod sql;
mod storage;
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

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

    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let db = db.clone();
        tokio::spawn(async move {
            wire::session::handle(stream, peer, db).await;
        });
    }
}
