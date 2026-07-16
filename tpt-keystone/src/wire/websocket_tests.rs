//! Tests for the Flux WebSocket bridge's Upgrade-time authentication
//! (`wire::websocket::handle`): when `_tpt_roles` is configured, the HTTP
//! Upgrade must carry a valid `Authorization: Basic` header or the handshake
//! is rejected with 401; the zero-config path still upgrades freely.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use base64::Engine;
use super::websocket;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::wire::roles::RoleStore;

async fn spawn_ws(
    db: Arc<Database>,
    roles: Arc<RoleStore>,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let guard = Arc::new(tokio::sync::Semaphore::new(1000));
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let db = db.clone();
            let roles = roles.clone();
            let guard = guard.clone();
            tokio::spawn(websocket::handle(stream, peer, db, roles, guard));
        }
    });
    addr
}

/// Send a raw WebSocket Upgrade request (optionally with an Authorization
/// header) and return the response status line.
async fn try_upgrade(addr: std::net::SocketAddr, authorization: Option<&str>) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut req = String::from(
        "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n",
    );
    if let Some(a) = authorization {
        req.push_str(&format!("Authorization: {a}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();

    let mut buf = [0u8; 512];
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or_default().to_string()
}

#[tokio::test]
async fn zero_config_upgrades_without_auth() {
    let (db, _b, _l) = test_db();
    let roles = Arc::new(RoleStore::new(db.clone()).unwrap());
    let addr = spawn_ws(db, roles).await;
    let status = try_upgrade(addr, None).await;
    assert!(status.starts_with("HTTP/1.1 101"), "got: {status}");
}

#[tokio::test]
async fn auth_required_when_roles_configured() {
    let (db, _b, _l) = test_db();
    let roles = Arc::new(RoleStore::new(db.clone()).unwrap());
    roles.bootstrap_if_empty("alice", "hunter2").unwrap();
    let addr = spawn_ws(db, roles).await;

    // No Authorization -> 401.
    let status = try_upgrade(addr, None).await;
    assert!(status.starts_with("HTTP/1.1 401"), "got: {status}");

    // Wrong password -> 401.
    let bad = base64::engine::general_purpose::STANDARD.encode("alice:wrong");
    let status = try_upgrade(addr, Some(&format!("Basic {bad}"))).await;
    assert!(status.starts_with("HTTP/1.1 401"), "got: {status}");

    // Correct password -> 101 Switching Protocols.
    let good = base64::engine::general_purpose::STANDARD.encode("alice:hunter2");
    let status = try_upgrade(addr, Some(&format!("Basic {good}"))).await;
    assert!(status.starts_with("HTTP/1.1 101"), "got: {status}");
}

fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
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
