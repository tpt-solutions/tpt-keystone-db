use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value as Json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::storage::{ColumnDef, ColumnType, StorageEngine};

fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let lease = Arc::new(LeaseManager::new(store.clone(), "db", "node-1".into(), Duration::from_secs(30)));
    lease.try_acquire().unwrap();
    let db = Arc::new(Database::open(local.path(), store, lease.handle(), NodeRole::Writer, Default::default()).unwrap());
    (db, bucket, local)
}

/// Spins up a real MCP listener on an ephemeral port and returns its addr.
async fn spawn_mcp(db: Arc<Database>, token: Option<String>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let db = db.clone();
            let token = token.clone();
            tokio::spawn(async move {
                super::handle(stream, peer, db, token).await;
            });
        }
    });
    addr
}

async fn rpc(addr: std::net::SocketAddr, token: Option<&str>, body: Json) -> (u16, Json) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let payload = body.to_string();
    let mut request = format!(
        "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        payload.len()
    );
    if let Some(t) = token {
        request.push_str(&format!("X-TPT-Token: {t}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(&payload);

    stream.write_all(request.as_bytes()).await.unwrap();
    stream.shutdown().await.ok();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8(raw).unwrap();
    let mut parts = text.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap();
    let body_text = parts.next().unwrap_or("");
    let status: u16 = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body_text).unwrap())
}

fn call_tool(name: &str, arguments: Json) -> Json {
    json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}})
}

#[tokio::test]
async fn initialize_and_tools_list() {
    let (db, _b, _l) = test_db();
    let addr = spawn_mcp(db, None).await;

    let (status, resp) = rpc(addr, None, json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})).await;
    assert_eq!(status, 200);
    assert_eq!(resp["result"]["serverInfo"]["name"], "tpt-keystone");

    let (status, resp) = rpc(addr, None, json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})).await;
    assert_eq!(status, 200);
    let tools = resp["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 7);
}

#[tokio::test]
async fn tables_columns_schema_reflect_created_table() {
    let (db, _b, _l) = test_db();
    db.create_table(
        "widgets",
        &[
            ColumnDef { name: "id".into(), col_type: ColumnType::Int4, nullable: false, default: None, is_pk: true },
            ColumnDef { name: "name".into(), col_type: ColumnType::Text, nullable: true, default: None, is_pk: false },
        ],
    )
    .unwrap();
    crate::executor::execute_query("INSERT INTO widgets VALUES (1, 'a')", db.clone()).unwrap();
    crate::executor::execute_query("INSERT INTO widgets VALUES (2, 'a')", db.clone()).unwrap();
    crate::executor::execute_query("INSERT INTO widgets VALUES (3, 'b')", db.clone()).unwrap();
    let addr = spawn_mcp(db, None).await;

    let (_status, resp) = rpc(addr, None, call_tool("tables", json!({}))).await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let tables: Vec<String> = serde_json::from_str(text).unwrap();
    assert_eq!(tables, vec!["widgets".to_string()]);

    let (_status, resp) = rpc(addr, None, call_tool("columns", json!({"table": "widgets"}))).await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let columns: Json = serde_json::from_str(text).unwrap();
    assert_eq!(columns.as_array().unwrap().len(), 2);
    assert_eq!(columns[0]["name"], "id");

    let (_status, resp) = rpc(addr, None, call_tool("schema", json!({}))).await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let schema: Json = serde_json::from_str(text).unwrap();
    assert_eq!(schema["tables"][0]["name"], "widgets");
    assert_eq!(schema["tables"][0]["row_count"], 3);
    let name_histogram = schema["tables"][0]["histograms"]["name"].as_array().unwrap();
    assert_eq!(name_histogram[0]["value"], "a");
    assert_eq!(name_histogram[0]["count"], 2);
    assert!(schema["relationship_graph"]["nodes"].as_array().unwrap().contains(&json!("widgets")));
}

#[tokio::test]
async fn related_walks_foreign_keys_in_both_directions() {
    let (db, _b, _l) = test_db();
    crate::executor::execute_query(
        "CREATE TABLE authors (id INT4 PRIMARY KEY, name TEXT)",
        db.clone(),
    )
    .unwrap();
    crate::executor::execute_query(
        "CREATE TABLE books (id INT4 PRIMARY KEY, title TEXT, author_id INT4 REFERENCES authors(id))",
        db.clone(),
    )
    .unwrap();
    crate::executor::execute_query("INSERT INTO authors VALUES (1, 'Ada')", db.clone()).unwrap();
    crate::executor::execute_query("INSERT INTO books VALUES (10, 'Notes', 1)", db.clone()).unwrap();
    let addr = spawn_mcp(db, None).await;

    // From the book: outgoing FK to its author.
    let (_status, resp) = rpc(addr, None, call_tool("related", json!({"table": "books", "id": "10"}))).await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let result: Json = serde_json::from_str(text).unwrap();
    let facts = result["facts"].as_array().unwrap();
    assert!(facts.iter().any(|f| f["direction"] == "outgoing" && f["object"] == "authors:1"));

    // From the author: incoming FK from the book.
    let (_status, resp) = rpc(addr, None, call_tool("related", json!({"table": "authors", "id": "1"}))).await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let result: Json = serde_json::from_str(text).unwrap();
    let facts = result["facts"].as_array().unwrap();
    assert!(facts.iter().any(|f| f["direction"] == "incoming" && f["subject"] == "books:10" && f["subject_label"] == "title=Notes"));
}

#[tokio::test]
async fn query_executes_select_and_rejects_mutating_sql() {
    let (db, _b, _l) = test_db();
    db.create_table(
        "nums",
        &[ColumnDef { name: "n".into(), col_type: ColumnType::Int4, nullable: false, default: None, is_pk: true }],
    )
    .unwrap();
    crate::executor::execute_query("INSERT INTO nums VALUES (1)", db.clone()).unwrap();
    crate::executor::execute_query("INSERT INTO nums VALUES (2)", db.clone()).unwrap();
    let addr = spawn_mcp(db, None).await;

    let (_status, resp) = rpc(addr, None, call_tool("query", json!({"sql": "SELECT n FROM nums ORDER BY n"}))).await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let result: Json = serde_json::from_str(text).unwrap();
    assert_eq!(result["row_count"], 2);
    assert_eq!(result["rows"][0]["n"], "1");

    let (_status, resp) = rpc(addr, None, call_tool("query", json!({"sql": "INSERT INTO nums VALUES (3)"}))).await;
    assert!(resp.get("error").is_some(), "query() must reject non-read-only statements");
}

#[tokio::test]
async fn mutate_runs_insert_and_explain_describes_shape() {
    let (db, _b, _l) = test_db();
    db.create_table(
        "nums",
        &[ColumnDef { name: "n".into(), col_type: ColumnType::Int4, nullable: false, default: None, is_pk: true }],
    )
    .unwrap();
    let addr = spawn_mcp(db, None).await;

    let (_status, resp) = rpc(addr, None, call_tool("mutate", json!({"sql": "INSERT INTO nums VALUES (1)"}))).await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let result: Json = serde_json::from_str(text).unwrap();
    assert_eq!(result["rows_affected"], 1);

    let (_status, resp) = rpc(addr, None, call_tool("explain", json!({"sql": "SELECT n FROM nums WHERE n > 0"}))).await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let plan: Json = serde_json::from_str(text).unwrap();
    assert_eq!(plan["statement"], "select");
    assert_eq!(plan["tables"][0], "nums");
    assert_eq!(plan["has_where"], true);
}

#[tokio::test]
async fn rejects_requests_with_missing_or_wrong_token() {
    let (db, _b, _l) = test_db();
    let addr = spawn_mcp(db, Some("secret".into())).await;

    let (status, _resp) = rpc(addr, None, call_tool("tables", json!({}))).await;
    assert_eq!(status, 401);

    let (status, _resp) = rpc(addr, Some("wrong"), call_tool("tables", json!({}))).await;
    assert_eq!(status, 401);

    let (status, resp) = rpc(addr, Some("secret"), call_tool("tables", json!({}))).await;
    assert_eq!(status, 200);
    assert!(resp.get("result").is_some());
}
