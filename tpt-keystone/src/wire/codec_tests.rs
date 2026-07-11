use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::codec::{Conn, FrontendMessage};

/// Build a `Conn` fed by an in-memory duplex stream, plus the client half to
/// write raw bytes into it — no real socket needed.
fn conn_pair() -> (Conn, tokio::io::DuplexStream) {
    let (client, server) = tokio::io::duplex(8192);
    (Conn::from_boxed(Box::new(server)), client)
}

fn startup_packet(protocol_version: i32, params: &[(&str, &str)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&protocol_version.to_be_bytes());
    for (k, v) in params {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0); // empty key terminates the param list
    let total_len = (body.len() + 4) as i32;
    let mut packet = total_len.to_be_bytes().to_vec();
    packet.extend_from_slice(&body);
    packet
}

fn message(tag: u8, body: &[u8]) -> Vec<u8> {
    let len = (body.len() + 4) as i32;
    let mut msg = vec![tag];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(body);
    msg
}

#[tokio::test]
async fn read_startup_parses_params() {
    let (mut conn, mut client) = conn_pair();
    let packet = startup_packet(196608, &[("user", "alice"), ("database", "db")]);
    client.write_all(&packet).await.unwrap();

    let startup = conn.read_startup().await.unwrap();
    assert_eq!(startup.protocol_version, 196608);
    assert_eq!(
        startup.params,
        vec![
            ("user".to_string(), "alice".to_string()),
            ("database".to_string(), "db".to_string())
        ]
    );
}

#[tokio::test]
async fn read_startup_rejects_length_out_of_range() {
    let (mut conn, mut client) = conn_pair();
    // Length of 4 is below the minimum of 8.
    client.write_all(&4i32.to_be_bytes()).await.unwrap();
    assert!(conn.read_startup().await.is_err());
}

#[tokio::test]
async fn read_startup_rejects_length_too_large() {
    let (mut conn, mut client) = conn_pair();
    client.write_all(&100_000i32.to_be_bytes()).await.unwrap();
    assert!(conn.read_startup().await.is_err());
}

#[tokio::test]
async fn read_startup_handles_sslrequest_then_real_startup() {
    let (mut conn, mut client) = conn_pair();
    // SSLRequest: length 8, code 80877103, no body.
    let mut ssl_request = 8i32.to_be_bytes().to_vec();
    ssl_request.extend_from_slice(&80877103i32.to_be_bytes());
    client.write_all(&ssl_request).await.unwrap();
    // Followed by a real startup packet.
    let packet = startup_packet(196608, &[("user", "bob")]);
    client.write_all(&packet).await.unwrap();

    let startup = conn.read_startup().await.unwrap();
    assert_eq!(startup.protocol_version, 196608);

    // The server should have written back a single 'N' (SSL declined) byte.
    let mut resp = [0u8; 1];
    client.read_exact(&mut resp).await.unwrap();
    assert_eq!(resp[0], b'N');
}

#[tokio::test]
async fn read_startup_rejects_cancel_request() {
    let (mut conn, mut client) = conn_pair();
    let mut cancel = 8i32.to_be_bytes().to_vec();
    cancel.extend_from_slice(&80877102i32.to_be_bytes());
    client.write_all(&cancel).await.unwrap();
    assert!(conn.read_startup().await.is_err());
}

#[tokio::test]
async fn read_message_query() {
    let (mut conn, mut client) = conn_pair();
    let mut body = b"SELECT 1".to_vec();
    body.push(0);
    client.write_all(&message(b'Q', &body)).await.unwrap();

    match conn.read_message().await.unwrap() {
        FrontendMessage::Query(q) => assert_eq!(q, "SELECT 1"),
        other => panic!("expected Query, got {other:?}"),
    }
}

#[tokio::test]
async fn read_message_parse_with_param_types() {
    let (mut conn, mut client) = conn_pair();
    let mut body = Vec::new();
    body.push(0); // empty name
    body.extend_from_slice(b"SELECT $1");
    body.push(0);
    body.extend_from_slice(&1i16.to_be_bytes());
    body.extend_from_slice(&23i32.to_be_bytes()); // int4 oid
    client.write_all(&message(b'P', &body)).await.unwrap();

    match conn.read_message().await.unwrap() {
        FrontendMessage::Parse {
            name,
            query,
            param_types,
        } => {
            assert_eq!(name, "");
            assert_eq!(query, "SELECT $1");
            assert_eq!(param_types, vec![23]);
        }
        other => panic!("expected Parse, got {other:?}"),
    }
}

#[tokio::test]
async fn read_message_parse_truncated_param_count_defaults_zero() {
    let (mut conn, mut client) = conn_pair();
    let mut body = Vec::new();
    body.push(0);
    body.push(0);
    // No param-count bytes follow — should default to 0 params.
    client.write_all(&message(b'P', &body)).await.unwrap();

    match conn.read_message().await.unwrap() {
        FrontendMessage::Parse { param_types, .. } => assert!(param_types.is_empty()),
        other => panic!("expected Parse, got {other:?}"),
    }
}

#[tokio::test]
async fn read_message_bind_full() {
    let (mut conn, mut client) = conn_pair();
    let mut body = Vec::new();
    body.push(0); // portal
    body.push(0); // stmt
    body.extend_from_slice(&1i16.to_be_bytes()); // 1 param format
    body.extend_from_slice(&0i16.to_be_bytes()); // text
    body.extend_from_slice(&1i16.to_be_bytes()); // 1 param
    body.extend_from_slice(&3i32.to_be_bytes()); // length 3
    body.extend_from_slice(b"abc");
    body.extend_from_slice(&1i16.to_be_bytes()); // 1 result format
    body.extend_from_slice(&0i16.to_be_bytes());
    client.write_all(&message(b'B', &body)).await.unwrap();

    match conn.read_message().await.unwrap() {
        FrontendMessage::Bind {
            params,
            param_formats,
            result_formats,
            ..
        } => {
            assert_eq!(params, vec![Some(b"abc".to_vec())]);
            assert_eq!(param_formats, vec![0]);
            assert_eq!(result_formats, vec![0]);
        }
        other => panic!("expected Bind, got {other:?}"),
    }
}

#[tokio::test]
async fn read_message_bind_null_param() {
    let (mut conn, mut client) = conn_pair();
    let mut body = Vec::new();
    body.push(0); // portal
    body.push(0); // stmt
    body.extend_from_slice(&0i16.to_be_bytes()); // 0 param formats
    body.extend_from_slice(&1i16.to_be_bytes()); // 1 param
    body.extend_from_slice(&(-1i32).to_be_bytes()); // NULL param
    body.extend_from_slice(&0i16.to_be_bytes()); // 0 result formats
    client.write_all(&message(b'B', &body)).await.unwrap();

    match conn.read_message().await.unwrap() {
        FrontendMessage::Bind {
            params,
            param_formats,
            result_formats,
            ..
        } => {
            assert!(param_formats.is_empty());
            assert_eq!(params, vec![None]);
            assert!(result_formats.is_empty());
        }
        other => panic!("expected Bind, got {other:?}"),
    }
}

#[tokio::test]
async fn read_message_bind_truncated_after_names_defaults_all_arrays_empty() {
    let (mut conn, mut client) = conn_pair();
    // Body ends immediately after portal/stmt names — every following count
    // field is absent, so all three arrays should default to empty.
    let body = vec![0u8, 0u8]; // empty portal, empty stmt
    client.write_all(&message(b'B', &body)).await.unwrap();

    match conn.read_message().await.unwrap() {
        FrontendMessage::Bind {
            params,
            param_formats,
            result_formats,
            ..
        } => {
            assert!(param_formats.is_empty());
            assert!(params.is_empty());
            assert!(result_formats.is_empty());
        }
        other => panic!("expected Bind, got {other:?}"),
    }
}

#[tokio::test]
async fn read_message_describe_and_close_default_kind() {
    let (mut conn, mut client) = conn_pair();
    // No kind byte, no name -> kind defaults to 'S', name empty.
    client.write_all(&message(b'D', &[])).await.unwrap();
    match conn.read_message().await.unwrap() {
        FrontendMessage::Describe { kind, name } => {
            assert_eq!(kind, b'S');
            assert_eq!(name, "");
        }
        other => panic!("expected Describe, got {other:?}"),
    }

    client.write_all(&message(b'C', &[])).await.unwrap();
    match conn.read_message().await.unwrap() {
        FrontendMessage::Close { kind, name } => {
            assert_eq!(kind, b'S');
            assert_eq!(name, "");
        }
        other => panic!("expected Close, got {other:?}"),
    }
}

#[tokio::test]
async fn read_message_execute_missing_max_rows_defaults_zero() {
    let (mut conn, mut client) = conn_pair();
    let mut body = vec![0]; // empty portal name
    client.write_all(&message(b'E', &body)).await.unwrap();
    match conn.read_message().await.unwrap() {
        FrontendMessage::Execute { portal, max_rows } => {
            assert_eq!(portal, "");
            assert_eq!(max_rows, 0);
        }
        other => panic!("expected Execute, got {other:?}"),
    }

    // With an explicit max_rows.
    body.extend_from_slice(&10i32.to_be_bytes());
    client.write_all(&message(b'E', &body)).await.unwrap();
    match conn.read_message().await.unwrap() {
        FrontendMessage::Execute { max_rows, .. } => assert_eq!(max_rows, 10),
        other => panic!("expected Execute, got {other:?}"),
    }
}

#[tokio::test]
async fn read_message_sync_flush_terminate_copy_variants() {
    let (mut conn, mut client) = conn_pair();
    let variants: [(u8, &[u8]); 5] = [
        (b'S', &[]),
        (b'H', &[]),
        (b'X', &[]),
        (b'd', b"payload"),
        (b'c', &[]),
    ];
    for (tag, body) in variants {
        client.write_all(&message(tag, body)).await.unwrap();
    }
    let mut fail_body = b"oops".to_vec();
    fail_body.push(0);
    client.write_all(&message(b'f', &fail_body)).await.unwrap();

    assert!(matches!(conn.read_message().await.unwrap(), FrontendMessage::Sync));
    assert!(matches!(conn.read_message().await.unwrap(), FrontendMessage::Flush));
    assert!(matches!(conn.read_message().await.unwrap(), FrontendMessage::Terminate));
    match conn.read_message().await.unwrap() {
        FrontendMessage::CopyData(d) => assert_eq!(d, b"payload"),
        other => panic!("expected CopyData, got {other:?}"),
    }
    assert!(matches!(conn.read_message().await.unwrap(), FrontendMessage::CopyDone));
    match conn.read_message().await.unwrap() {
        FrontendMessage::CopyFail(msg) => assert_eq!(msg, "oops"),
        other => panic!("expected CopyFail, got {other:?}"),
    }
}

#[tokio::test]
async fn read_message_unknown_tag_falls_back_to_sync() {
    let (mut conn, mut client) = conn_pair();
    client.write_all(&message(b'?', &[])).await.unwrap();
    assert!(matches!(conn.read_message().await.unwrap(), FrontendMessage::Sync));
}

#[tokio::test]
async fn read_message_rejects_length_below_4() {
    let (mut conn, mut client) = conn_pair();
    let mut bytes = vec![b'Q'];
    bytes.extend_from_slice(&3i32.to_be_bytes());
    client.write_all(&bytes).await.unwrap();
    assert!(conn.read_message().await.is_err());
}

#[tokio::test]
async fn send_and_flush_writes_encoded_bytes() {
    use super::messages::BackendMessage;

    let (mut conn, mut client) = conn_pair();
    conn.send(&BackendMessage::AuthenticationOk);
    conn.flush().await.unwrap();

    let mut buf = [0u8; 9];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf[0], b'R');
    assert_eq!(i32::from_be_bytes(buf[1..5].try_into().unwrap()), 8);
}
