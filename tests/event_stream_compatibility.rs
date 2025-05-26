use greengrass_ipc_rust::event_stream::{EventStreamMessage, HeaderValue};

#[test]
fn test_connect_message_format() {
    // Test that our CONNECT message format matches AWS expectations
    let mut connect_message = EventStreamMessage::new();
    connect_message = connect_message
        .with_header(":version".to_string(), HeaderValue::String("0.1.0".to_string()))
        .with_header(":message-type".to_string(), HeaderValue::I32(4)) // CONNECT = 4
        .with_header(":message-flags".to_string(), HeaderValue::I32(0)) // No flags
        .with_header(":stream-id".to_string(), HeaderValue::I32(0)) // Protocol messages use stream-id 0
        .with_payload(r#"{"authToken": "test-token"}"#.as_bytes());

    // Verify the message can be encoded without errors
    let encoded = connect_message.encode().expect("Should encode successfully");
    assert!(encoded.len() > 0, "Encoded message should not be empty");

    // Verify the message can be decoded back
    let decoded = EventStreamMessage::decode(&encoded).expect("Should decode successfully");
    
    // Verify all required headers are present
    assert!(decoded.get_header(":message-type").is_some());
    assert!(decoded.get_header(":message-flags").is_some());
    assert!(decoded.get_header(":stream-id").is_some());
    assert!(decoded.get_header(":version").is_some());

    // Verify message type is correct integer value
    if let Some(HeaderValue::I32(msg_type)) = decoded.get_header(":message-type") {
        assert_eq!(*msg_type, 4, "Message type should be 4 for CONNECT");
    } else {
        panic!("Message type should be I32");
    }

    // Verify payload is preserved
    let payload_str = String::from_utf8_lossy(&decoded.payload);
    assert!(payload_str.contains("authToken"), "Payload should contain authToken");
}

#[test]
fn test_header_type_encoding() {
    // Test that header types match AWS Event Stream specification
    let mut message = EventStreamMessage::new();
    
    // Add headers of different types
    message = message
        .with_header("bool_true".to_string(), HeaderValue::Bool(true))
        .with_header("bool_false".to_string(), HeaderValue::Bool(false))
        .with_header("byte".to_string(), HeaderValue::I8(42))
        .with_header("short".to_string(), HeaderValue::I16(1234))
        .with_header("integer".to_string(), HeaderValue::I32(123456))
        .with_header("long".to_string(), HeaderValue::I64(123456789))
        .with_header("string".to_string(), HeaderValue::String("test".to_string()))
        .with_header("bytes".to_string(), HeaderValue::Bytes(vec![1, 2, 3, 4]))
        .with_header("timestamp".to_string(), HeaderValue::Timestamp(1234567890))
        .with_header("uuid".to_string(), HeaderValue::Uuid([0; 16]));

    // Encode and decode
    let encoded = message.encode().expect("Should encode successfully");
    let decoded = EventStreamMessage::decode(&encoded).expect("Should decode successfully");

    // Verify all headers are preserved
    assert_eq!(decoded.headers.len(), 10, "All headers should be preserved");

    // Verify specific header values
    if let Some(HeaderValue::Bool(val)) = decoded.get_header("bool_true") {
        assert_eq!(*val, true);
    } else {
        panic!("bool_true header should be Bool(true)");
    }

    if let Some(HeaderValue::String(val)) = decoded.get_header("string") {
        assert_eq!(val, "test");
    } else {
        panic!("string header should be String");
    }

    if let Some(HeaderValue::I32(val)) = decoded.get_header("integer") {
        assert_eq!(*val, 123456);
    } else {
        panic!("integer header should be I32");
    }
}

#[test]
fn test_message_crc_validation() {
    // Test that CRC validation works correctly
    let message = EventStreamMessage::new()
        .with_header("test".to_string(), HeaderValue::String("value".to_string()))
        .with_payload(b"test payload".to_vec());

    let encoded = message.encode().expect("Should encode successfully");
    
    // Valid message should decode without errors
    EventStreamMessage::decode(&encoded).expect("Valid message should decode");

    // Corrupted message should fail CRC validation
    let mut corrupted = encoded.to_vec();
    if corrupted.len() > 10 {
        corrupted[10] ^= 0xFF; // Flip bits in the middle
        let result = EventStreamMessage::decode(&corrupted);
        assert!(result.is_err(), "Corrupted message should fail to decode");
    }
}