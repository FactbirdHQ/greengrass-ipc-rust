use greengrass_ipc_rust::event_stream::{EventStreamMessage, Header};

#[test]
fn test_connect_message_format() {
    // Test that our CONNECT message format matches AWS expectations
    let mut connect_message = EventStreamMessage::new();
    connect_message = connect_message
        .with_header(Header::Version("0.1.0".to_string()))
        .with_header(Header::MessageType(4)) // CONNECT = 4
        .with_header(Header::MessageFlags(0)) // No flags
        .with_header(Header::StreamId(0)) // Protocol messages use stream-id 0
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
    if let Some(Header::MessageType(msg_type)) = decoded.get_header(":message-type") {
        assert_eq!(*msg_type, 4, "Message type should be 4 for CONNECT");
    } else {
        panic!("Message type should be MessageType");
    }

    // Verify payload is preserved
    let payload_str = String::from_utf8_lossy(&decoded.payload);
    assert!(payload_str.contains("authToken"), "Payload should contain authToken");
}

#[test]
fn test_typed_headers() {
    // Test that typed headers work correctly
    let mut message = EventStreamMessage::new();
    
    // Add typed headers
    message = message
        .with_header(Header::Version("1.0.0".to_string()))
        .with_header(Header::StreamId(42))
        .with_header(Header::MessageType(1))
        .with_header(Header::MessageFlags(0))
        .with_header(Header::ContentType("application/json".to_string()))
        .with_header(Header::Operation("TestOperation".to_string()))
        .with_header(Header::ServiceModelType("TestModel".to_string()))
        .with_header(Header::OperationId("op-123".to_string()));

    // Encode and decode
    let encoded = message.encode().expect("Should encode successfully");
    let decoded = EventStreamMessage::decode(&encoded).expect("Should decode successfully");

    // Verify all headers are preserved
    assert_eq!(decoded.headers.len(), 8, "All headers should be preserved");

    // Verify specific header values
    if let Some(Header::Version(val)) = decoded.get_header(":version") {
        assert_eq!(val, "1.0.0");
    } else {
        panic!("version header should be Version");
    }

    if let Some(Header::StreamId(val)) = decoded.get_header(":stream-id") {
        assert_eq!(*val, 42);
    } else {
        panic!("stream-id header should be StreamId");
    }

    if let Some(Header::Operation(val)) = decoded.get_header("operation") {
        assert_eq!(val, "TestOperation");
    } else {
        panic!("operation header should be Operation");
    }
}

#[test]
fn test_message_crc_validation() {
    // Test that CRC validation works correctly
    let message = EventStreamMessage::new()
        .with_header(Header::Version("test-value".to_string()))
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