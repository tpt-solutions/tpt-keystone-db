mod executor;
mod sql;
mod wire;

use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let addr = "0.0.0.0:5432";
    let listener = TcpListener::bind(addr).await?;
    info!("TPT Keystone listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true)?;
        tokio::spawn(async move {
            wire::session::handle(stream, peer).await;
        });
    }
}
