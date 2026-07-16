use serde_json::json;

use super::protocol::dispatch;
use crate::storage::test_support::test_db;

#[test]
fn dispatch_malformed_json_returns_parse_error() {
    let (db, _bucket, _local) = test_db();
    let resp = dispatch(&db, &crate::executor::rbac::Actor::unrestricted(), b"not json");
    assert_eq!(resp["id"], serde_json::Value::Null);
    assert_eq!(resp["error"]["code"], -32700);
}

#[test]
fn dispatch_initialize_returns_capabilities() {
    let (db, _bucket, _local) = test_db();
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}});
    let resp = dispatch(&db, &crate::executor::rbac::Actor::unrestricted(), body.to_string().as_bytes());
    assert_eq!(resp["result"]["serverInfo"]["name"], "tpt-keystone");
    assert!(resp["result"]["capabilities"]["tools"].is_object());
}

#[test]
fn dispatch_tools_list_returns_seven_tools() {
    let (db, _bucket, _local) = test_db();
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
    let resp = dispatch(&db, &crate::executor::rbac::Actor::unrestricted(), body.to_string().as_bytes());
    let tools = resp["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 7);
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for expected in [
        "tables", "columns", "schema", "query", "mutate", "explain", "related",
    ] {
        assert!(names.contains(&expected), "missing tool {expected}");
    }
}

#[test]
fn dispatch_unknown_method_returns_method_not_found() {
    let (db, _bucket, _local) = test_db();
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "nope"});
    let resp = dispatch(&db, &crate::executor::rbac::Actor::unrestricted(), body.to_string().as_bytes());
    assert_eq!(resp["error"]["code"], -32601);
}

#[test]
fn dispatch_tools_call_missing_name_returns_tool_error() {
    let (db, _bucket, _local) = test_db();
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {}});
    let resp = dispatch(&db, &crate::executor::rbac::Actor::unrestricted(), body.to_string().as_bytes());
    assert_eq!(resp["error"]["code"], -32000);
    assert!(resp["error"]["message"]
        .as_str()
        .unwrap()
        .contains("unknown tool"));
}

#[test]
fn dispatch_tools_call_success_wraps_content_text() {
    let (db, _bucket, _local) = test_db();
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "tables", "arguments": {}},
    });
    let resp = dispatch(&db, &crate::executor::rbac::Actor::unrestricted(), body.to_string().as_bytes());
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    // "tables" returns a JSON array of table names, serialized as text.
    assert!(text.starts_with('['));
}

#[test]
fn dispatch_tools_call_tool_error_maps_to_dash32000() {
    let (db, _bucket, _local) = test_db();
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "query", "arguments": {"sql": "CREATE TABLE t (a int)"}},
    });
    let resp = dispatch(&db, &crate::executor::rbac::Actor::unrestricted(), body.to_string().as_bytes());
    assert_eq!(resp["error"]["code"], -32000);
    assert!(resp["error"]["message"]
        .as_str()
        .unwrap()
        .contains("read-only"));
}
