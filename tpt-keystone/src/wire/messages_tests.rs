use bytes::BytesMut;

use super::messages::*;

fn encode_to_vec(msg: &BackendMessage) -> Vec<u8> {
    let mut buf = BytesMut::new();
    encode(msg, &mut buf);
    buf.to_vec()
}

/// Split an encoded message into (tag, declared_length, body).
fn split(bytes: &[u8]) -> (u8, i32, &[u8]) {
    let tag = bytes[0];
    let len = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
    let body = &bytes[5..];
    // Declared length includes itself (4 bytes) but not the tag byte.
    assert_eq!(len as usize, body.len() + 4, "declared length must match body");
    (tag, len, body)
}

#[test]
fn encodes_authentication_ok() {
    let bytes = encode_to_vec(&BackendMessage::AuthenticationOk);
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'R');
    assert_eq!(body, &0i32.to_be_bytes());
}

#[test]
fn encodes_authentication_sasl() {
    let bytes = encode_to_vec(&BackendMessage::AuthenticationSASL(vec!["SCRAM-SHA-256".into()]));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'R');
    let mut expected = 10i32.to_be_bytes().to_vec();
    expected.extend_from_slice(b"SCRAM-SHA-256\0");
    expected.push(0); // terminator
    assert_eq!(body, expected.as_slice());
}

#[test]
fn encodes_authentication_sasl_continue_and_final() {
    let bytes = encode_to_vec(&BackendMessage::AuthenticationSASLContinue(b"srv-first".to_vec()));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'R');
    let mut expected = 11i32.to_be_bytes().to_vec();
    expected.extend_from_slice(b"srv-first");
    assert_eq!(body, expected.as_slice());

    let bytes = encode_to_vec(&BackendMessage::AuthenticationSASLFinal(b"v=sig".to_vec()));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'R');
    let mut expected = 12i32.to_be_bytes().to_vec();
    expected.extend_from_slice(b"v=sig");
    assert_eq!(body, expected.as_slice());
}

#[test]
fn encodes_parameter_status_and_backend_key_data() {
    let bytes = encode_to_vec(&BackendMessage::ParameterStatus {
        name: "server_version".into(),
        value: "16.0".into(),
    });
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'S');
    let mut expected = b"server_version\0".to_vec();
    expected.extend_from_slice(b"16.0\0");
    assert_eq!(body, expected.as_slice());

    let bytes = encode_to_vec(&BackendMessage::BackendKeyData { pid: 42, secret: 99 });
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'K');
    let mut expected = 42i32.to_be_bytes().to_vec();
    expected.extend_from_slice(&99i32.to_be_bytes());
    assert_eq!(body, expected.as_slice());
}

#[test]
fn encodes_ready_for_query_status_byte() {
    for (status, byte) in [
        (TransactionStatus::Idle, b'I'),
        (TransactionStatus::InTransaction, b'T'),
        (TransactionStatus::Failed, b'E'),
    ] {
        let bytes = encode_to_vec(&BackendMessage::ReadyForQuery(status));
        let (tag, _, body) = split(&bytes);
        assert_eq!(tag, b'Z');
        assert_eq!(body, &[byte]);
    }
}

#[test]
fn encodes_row_description_fields() {
    let field = FieldDescription::simple("id", oid::INT8);
    let bytes = encode_to_vec(&BackendMessage::RowDescription(vec![field]));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'T');
    assert_eq!(&body[0..2], &1i16.to_be_bytes()); // field count
    let mut expected = b"id\0".to_vec();
    expected.extend_from_slice(&0i32.to_be_bytes()); // table_oid
    expected.extend_from_slice(&0i16.to_be_bytes()); // col_attr
    expected.extend_from_slice(&oid::INT8.to_be_bytes()); // type_oid
    expected.extend_from_slice(&(-1i16).to_be_bytes()); // type_size
    expected.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier
    expected.extend_from_slice(&0i16.to_be_bytes()); // format
    assert_eq!(&body[2..], expected.as_slice());
}

#[test]
fn field_description_simple_defaults() {
    let f = FieldDescription::simple("col", 25);
    assert_eq!(f.name, "col");
    assert_eq!(f.table_oid, 0);
    assert_eq!(f.col_attr, 0);
    assert_eq!(f.type_oid, 25);
    assert_eq!(f.type_size, -1);
    assert_eq!(f.type_modifier, -1);
    assert_eq!(f.format, 0);
}

#[test]
fn encodes_data_row_with_and_without_nulls() {
    let bytes = encode_to_vec(&BackendMessage::DataRow(vec![
        Some(b"hello".to_vec()),
        None,
    ]));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'D');
    assert_eq!(&body[0..2], &2i16.to_be_bytes());
    let mut expected = 5i32.to_be_bytes().to_vec();
    expected.extend_from_slice(b"hello");
    expected.extend_from_slice(&(-1i32).to_be_bytes());
    assert_eq!(&body[2..], expected.as_slice());
}

#[test]
fn encodes_command_complete_tag() {
    let bytes = encode_to_vec(&BackendMessage::CommandComplete("SELECT 3".into()));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'C');
    assert_eq!(body, b"SELECT 3\0");
}

#[test]
fn encodes_error_response_field_layout() {
    let err = ErrorInfo::new("42601", "syntax error");
    let bytes = encode_to_vec(&BackendMessage::ErrorResponse(err));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'E');
    let mut expected = Vec::new();
    expected.push(b'S');
    expected.extend_from_slice(b"ERROR\0");
    expected.push(b'V');
    expected.extend_from_slice(b"ERROR\0");
    expected.push(b'C');
    expected.extend_from_slice(b"42601\0");
    expected.push(b'M');
    expected.extend_from_slice(b"syntax error\0");
    expected.push(0); // terminator
    assert_eq!(body, expected.as_slice());

    let fatal = ErrorInfo::fatal("XX000", "boom");
    assert_eq!(fatal.severity, "FATAL");
}

#[test]
fn encodes_notice_response() {
    let bytes = encode_to_vec(&BackendMessage::NoticeResponse("heads up".into()));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'N');
    let mut expected = vec![b'M'];
    expected.extend_from_slice(b"heads up\0");
    expected.push(0);
    assert_eq!(body, expected.as_slice());
}

#[test]
fn encodes_empty_query_response() {
    let bytes = encode_to_vec(&BackendMessage::EmptyQueryResponse);
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'I');
    assert!(body.is_empty());
}

#[test]
fn encodes_parse_bind_close_complete() {
    let bytes = encode_to_vec(&BackendMessage::ParseComplete);
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'1');
    assert!(body.is_empty());

    let bytes = encode_to_vec(&BackendMessage::BindComplete);
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'2');
    assert!(body.is_empty());

    let bytes = encode_to_vec(&BackendMessage::CloseComplete);
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'3');
    assert!(body.is_empty());
}

#[test]
fn encodes_no_data_and_portal_suspended() {
    let bytes = encode_to_vec(&BackendMessage::NoData);
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'n');
    assert!(body.is_empty());

    let bytes = encode_to_vec(&BackendMessage::PortalSuspended);
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b's');
    assert!(body.is_empty());
}

#[test]
fn encodes_parameter_description_types() {
    let bytes = encode_to_vec(&BackendMessage::ParameterDescription(vec![oid::INT8, oid::TEXT]));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b't');
    assert_eq!(&body[0..2], &2i16.to_be_bytes());
    let mut expected = oid::INT8.to_be_bytes().to_vec();
    expected.extend_from_slice(&oid::TEXT.to_be_bytes());
    assert_eq!(&body[2..], expected.as_slice());
}

#[test]
fn encodes_notification_response() {
    let bytes = encode_to_vec(&BackendMessage::NotificationResponse {
        pid: 7,
        channel: "chan".into(),
        payload: "hi".into(),
    });
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'A');
    let mut expected = 7i32.to_be_bytes().to_vec();
    expected.extend_from_slice(b"chan\0");
    expected.extend_from_slice(b"hi\0");
    assert_eq!(body, expected.as_slice());
}

#[test]
fn encodes_copy_in_out_responses_with_column_formats() {
    let bytes = encode_to_vec(&BackendMessage::CopyInResponse { columns: 3 });
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'G');
    assert_eq!(body[0], 0); // overall text format
    assert_eq!(&body[1..3], &3i16.to_be_bytes());
    for i in 0..3 {
        let off = 3 + i * 2;
        assert_eq!(&body[off..off + 2], &0i16.to_be_bytes());
    }

    let bytes = encode_to_vec(&BackendMessage::CopyOutResponse { columns: 0 });
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'H');
    assert_eq!(body[0], 0);
    assert_eq!(&body[1..3], &0i16.to_be_bytes());
    assert_eq!(body.len(), 3);
}

#[test]
fn encodes_copy_data_and_copy_done() {
    let bytes = encode_to_vec(&BackendMessage::CopyData(b"row1\n".to_vec()));
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'd');
    assert_eq!(body, b"row1\n");

    let bytes = encode_to_vec(&BackendMessage::CopyDone);
    let (tag, _, body) = split(&bytes);
    assert_eq!(tag, b'c');
    assert!(body.is_empty());
}
