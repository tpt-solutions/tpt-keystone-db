//! End-to-end test for the Canvas (Phase 13) HTTP query endpoint: spins up
//! `Database` + `http_query::handle` on a real loopback TCP listener (same
//! "in-process client speaks the real wire protocol" style as
//! `storage::phase3_tests`) and drives it with a plain `TcpStream` writing a
//! raw HTTP/1.1 request — no HTTP client crate, matching this endpoint's own
//! hand-rolled-parsing discipline.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::http_query;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::storage::{ColumnDef, ColumnType, StorageEngine};

async fn test_server() -> (std::net::SocketAddr, tempfile::TempDir, tempfile::TempDir) {
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

    db.create_table(
        "widgets",
        &[
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int4,
                nullable: false,
                default: None,
                is_pk: true,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: true,
                default: None,
                is_pk: false,
            },
        ],
    )
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let db = db.clone();
            tokio::spawn(http_query::handle(stream, peer, db));
        }
    });
    (addr, bucket, local)
}

async fn raw_request(addr: std::net::SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}

fn body_of(response: &str) -> &str {
    response.split("\r\n\r\n").nth(1).unwrap()
}

#[tokio::test]
async fn query_round_trip_returns_json_rows() {
    let (addr, _b, _l) = test_server().await;
    raw_request(
        addr,
        "POST /query HTTP/1.1\r\nContent-Length: 45\r\n\r\n{\"sql\": \"insert into widgets values (1,'a')\"}",
    )
    .await;

    let response = raw_request(
        addr,
        "POST /query HTTP/1.1\r\nContent-Length: 39\r\n\r\n{\"sql\": \"select id, name from widgets\"}",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 200"));
    let body: serde_json::Value = serde_json::from_str(body_of(&response)).unwrap();
    assert_eq!(body["columns"], serde_json::json!(["id", "name"]));
    assert_eq!(body["rows"], serde_json::json!([["1", "a"]]));
}

#[tokio::test]
async fn query_error_returns_400_with_message() {
    let (addr, _b, _l) = test_server().await;
    let response = raw_request(
        addr,
        "POST /query HTTP/1.1\r\nContent-Length: 29\r\n\r\n{\"sql\": \"select * from nope\"}",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 400"));
    let body: serde_json::Value = serde_json::from_str(body_of(&response)).unwrap();
    assert!(body["error"].as_str().unwrap().len() > 0);
}

#[tokio::test]
async fn schema_endpoint_reports_columns() {
    let (addr, _b, _l) = test_server().await;
    let response = raw_request(addr, "GET /schema HTTP/1.1\r\nContent-Length: 0\r\n\r\n").await;
    assert!(response.starts_with("HTTP/1.1 200"));
    let body: serde_json::Value = serde_json::from_str(body_of(&response)).unwrap();
    let tables = body["tables"].as_array().unwrap();
    let widgets = tables.iter().find(|t| t["name"] == "widgets").unwrap();
    let columns = widgets["columns"].as_array().unwrap();
    assert!(columns
        .iter()
        .any(|c| c["name"] == "id" && c["type"] == "int4"));
    assert!(columns
        .iter()
        .any(|c| c["name"] == "name" && c["type"] == "text"));
}
