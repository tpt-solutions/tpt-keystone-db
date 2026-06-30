mod executor;
mod sql;
mod storage;
mod wire;

use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

use storage::database::Database;
use storage::StorageEngine;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Open or create the database
    let db_dir = Path::new("tpt-data");
    let db = Arc::new(Database::open(db_dir)?);
    info!(dir = %db_dir.display(), "Database opened");

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
