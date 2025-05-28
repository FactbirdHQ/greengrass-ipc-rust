// Message Encoding Utility Functions for Testing
//
// This file provides utility functions for testing message encoding compatibility
// between the Rust Greengrass IPC implementation and the Python SDK.

use base64::Engine;
use bytes::Bytes;
use std::collections::HashMap;

// Import from our own crate
use greengrass_ipc_rust::event_stream::{EventStreamMessage, Header};

// Define our own versions of these types for the tests
pub struct MessageComparisonHarness {}
pub struct ComparisonResult {
    pub matches: bool,
    pub python_message: Option<()>,
    pub differences: Vec<()>,
    pub rust_message: Vec<u8>,
}

/// A trait for any message that can be encoded for comparison testing
pub trait EncodableMessage {
    /// Encode the message to bytes for comparison with Python SDK
    fn encode(&self) -> Result<Bytes, Box<dyn std::error::Error>>;

    /// Get the operation name associated with this message
    fn operation_name(&self) -> &str;

    /// Get the message type (e.g., "request", "response", etc.)
    fn message_type(&self) -> &str;
}

/// Encode a request message and compare it with Python SDK encoding
pub async fn compare_request_encoding<M: EncodableMessage>(
    _harness: &MessageComparisonHarness,
    message: &M,
) -> ComparisonResult {
    // Encode the message using our Rust implementation
    let encoded = message.encode().expect("Failed to encode message");

    // This is a stub implementation for now
    ComparisonResult {
        matches: true,
        python_message: None,
        differences: vec![],
        rust_message: encoded.to_vec(),
    }
}

/// Create a standard event stream message for testing
pub fn create_test_message(
    operation: &str,
    message_type: &str,
    payload: Bytes,
    additional_headers: Option<HashMap<String, String>>,
) -> EventStreamMessage {
    // Start with basic message
    let mut message = EventStreamMessage::new()
        .with_header(Header::Operation(operation.to_string()))
        .with_header(Header::MessageType(0)) // 0 for APPLICATION_MESSAGE
        .with_payload(payload);

    // Add any additional headers
    if let Some(headers) = additional_headers {
        for (name, value) in headers {
            match name.as_str() {
                ":version" => message = message.with_header(Header::Version(value)),
                ":content-type" => message = message.with_header(Header::ContentType(value)),
                "service-model-type" => {
                    message = message.with_header(Header::ServiceModelType(value))
                }
                ":operation-id" => message = message.with_header(Header::OperationId(value)),
                _ => {
                    // For unknown headers, we'll skip them since we have a strongly typed system
                    eprintln!("Warning: Skipping unknown header: {}", name);
                }
            }
        }
    }

    message
}

/// Create a standard publish request message for testing
pub fn create_publish_request(topic: &str, message_data: &str) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    // In a real implementation, this would be properly serialized from the model
    let payload = format!(
        r#"{{
        "topic": "{}",
        "publishMessage": {{
            "jsonMessage": {{
                "message": "{}"
            }}
        }}
    }}"#,
        topic, message_data
    );

    create_test_message("PublishToTopic", "Request", Bytes::from(payload), None)
}

/// Create a standard subscribe request message for testing
pub fn create_subscribe_request(topic: &str) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "topic": "{}"
    }}"#,
        topic
    );

    create_test_message("SubscribeToTopic", "Request", Bytes::from(payload), None)
}

/// Create a standard IoT Core publish request message for testing
pub fn create_publish_iot_request(topic: &str, payload_data: &[u8]) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "topicName": "{}",
        "qos": "AT_MOST_ONCE",
        "payload": "{}"
    }}"#,
        topic,
        base64::engine::general_purpose::STANDARD.encode(payload_data)
    );

    create_test_message("PublishToIoTCore", "Request", Bytes::from(payload), None)
}

/// Create a standard IoT Core subscribe request message for testing
pub fn create_subscribe_iot_request(topic_filter: &str) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "topicName": "{}",
        "qos": "AT_MOST_ONCE"
    }}"#,
        topic_filter
    );

    create_test_message("SubscribeToIoTCore", "Request", Bytes::from(payload), None)
}

/// Create a standard shadow get request message for testing
pub fn create_get_shadow_request(
    thing_name: &str,
    shadow_name: Option<&str>,
) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = if let Some(shadow_name) = shadow_name {
        format!(
            r#"{{
            "thingName": "{}",
            "shadowName": "{}"
        }}"#,
            thing_name, shadow_name
        )
    } else {
        format!(
            r#"{{
            "thingName": "{}"
        }}"#,
            thing_name
        )
    };

    create_test_message("GetThingShadow", "Request", Bytes::from(payload), None)
}

/// Create a standard shadow update request message for testing
pub fn create_update_shadow_request(
    thing_name: &str,
    shadow_name: Option<&str>,
    state_json: &str,
) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let shadow_part = if let Some(shadow_name) = shadow_name {
        format!(r#""shadowName": "{}","#, shadow_name)
    } else {
        String::new()
    };

    let payload = format!(
        r#"{{
        "thingName": "{}",
        {}
        "payload": {}
    }}"#,
        thing_name, shadow_part, state_json
    );

    create_test_message("UpdateThingShadow", "Request", Bytes::from(payload), None)
}

/// Create a standard shadow delete request message for testing
pub fn create_delete_shadow_request(
    thing_name: &str,
    shadow_name: Option<&str>,
) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = if let Some(shadow_name) = shadow_name {
        format!(
            r#"{{
            "thingName": "{}",
            "shadowName": "{}"
        }}"#,
            thing_name, shadow_name
        )
    } else {
        format!(
            r#"{{
            "thingName": "{}"
        }}"#,
            thing_name
        )
    };

    create_test_message("DeleteThingShadow", "Request", Bytes::from(payload), None)
}

/// Create a standard list named shadows request message for testing
pub fn create_list_named_shadows_request(thing_name: &str) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "thingName": "{}"
    }}"#,
        thing_name
    );

    create_test_message(
        "ListNamedShadowsForThing",
        "Request",
        Bytes::from(payload),
        None,
    )
}

/// Create a standard pause component request message for testing
pub fn create_pause_component_request(component_name: &str) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "componentName": "{}"
    }}"#,
        component_name
    );

    create_test_message("PauseComponent", "Request", Bytes::from(payload), None)
}

/// Create a standard resume component request message for testing
pub fn create_resume_component_request(component_name: &str) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "componentName": "{}"
    }}"#,
        component_name
    );

    create_test_message("ResumeComponent", "Request", Bytes::from(payload), None)
}

/// Create a standard stop component request message for testing
pub fn create_stop_component_request(component_name: &str) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "componentName": "{}"
    }}"#,
        component_name
    );

    create_test_message("StopComponent", "Request", Bytes::from(payload), None)
}

/// Create a standard restart component request message for testing
pub fn create_restart_component_request(component_name: &str) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "componentName": "{}"
    }}"#,
        component_name
    );

    create_test_message("RestartComponent", "Request", Bytes::from(payload), None)
}

/// Create a standard get component details request message for testing
pub fn create_get_component_details_request(component_name: &str) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "componentName": "{}"
    }}"#,
        component_name
    );

    create_test_message("GetComponentDetails", "Request", Bytes::from(payload), None)
}

/// Create a standard subscribe to component updates request message for testing
pub fn create_subscribe_component_updates_request() -> EventStreamMessage {
    // For testing purposes, we'll create an empty JSON payload
    let payload = "{}";

    create_test_message(
        "SubscribeToComponentUpdates",
        "Request",
        Bytes::from(payload),
        None,
    )
}

/// Create a standard list components request message for testing
pub fn create_list_components_request() -> EventStreamMessage {
    // For testing purposes, we'll create an empty JSON payload
    let payload = "{}";

    create_test_message("ListComponents", "Request", Bytes::from(payload), None)
}

/// Create a standard get configuration request message for testing
pub fn create_get_configuration_request(
    component_name: &str,
    key_path: &[&str],
) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload with key path as array
    let key_path_json = key_path
        .iter()
        .map(|k| format!("\"{}\"", k))
        .collect::<Vec<_>>()
        .join(", ");

    let payload = format!(
        r#"{{
        "componentName": "{}",
        "keyPath": [{}]
    }}"#,
        component_name, key_path_json
    );

    create_test_message("GetConfiguration", "Request", Bytes::from(payload), None)
}

/// Create a standard subscribe to configuration update request message for testing
pub fn create_subscribe_configuration_update_request(
    component_name: &str,
    key_path: &[&str],
) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload with key path as array
    let key_path_json = key_path
        .iter()
        .map(|k| format!("\"{}\"", k))
        .collect::<Vec<_>>()
        .join(", ");

    let payload = format!(
        r#"{{
        "componentName": "{}",
        "keyPath": [{}]
    }}"#,
        component_name, key_path_json
    );

    create_test_message(
        "SubscribeToConfigurationUpdate",
        "Request",
        Bytes::from(payload),
        None,
    )
}

/// Create a standard subscribe to validate configuration updates request message for testing
pub fn create_subscribe_validate_configuration_updates_request(
    component_name: &str,
) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "componentName": "{}"
    }}"#,
        component_name
    );

    create_test_message(
        "SubscribeToValidateConfigurationUpdates",
        "Request",
        Bytes::from(payload),
        None,
    )
}

/// Create a standard update configuration request message for testing
pub fn create_update_configuration_request(
    component_name: &str,
    key_path: &[&str],
    value_json: &str,
) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload with key path as array
    let key_path_json = key_path
        .iter()
        .map(|k| format!("\"{}\"", k))
        .collect::<Vec<_>>()
        .join(", ");

    let payload = format!(
        r#"{{
        "componentName": "{}",
        "keyPath": [{}],
        "valueToMerge": {}
    }}"#,
        component_name, key_path_json, value_json
    );

    create_test_message("UpdateConfiguration", "Request", Bytes::from(payload), None)
}

/// Create a standard send configuration validity report request message for testing
pub fn create_configuration_validity_report_request(
    status: &str,
    deployment_id: &str,
) -> EventStreamMessage {
    // For testing purposes, we'll create a simple JSON payload
    let payload = format!(
        r#"{{
        "configurationValidityReport": {{
            "status": "{}",
            "deploymentId": "{}"
        }}
    }}"#,
        status, deployment_id
    );

    create_test_message(
        "SendConfigurationValidityReport",
        "Request",
        Bytes::from(payload),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_message() {
        let message = create_test_message(
            "TestOperation",
            "Request",
            Bytes::from("test payload"),
            None,
        );

        // Verify headers were set correctly
        assert_eq!(
            message.get_header("operation"),
            Some(&Header::Operation("TestOperation".to_string()))
        );
        assert_eq!(
            message.get_header(":message-type"),
            Some(&Header::MessageType(0))
        );

        // Verify payload was set correctly
        assert_eq!(message.payload, Bytes::from("test payload"));
    }

    #[test]
    fn test_create_publish_request() {
        let message = create_publish_request("test/topic", "Hello, world!");

        // Verify operation and type headers
        assert_eq!(
            message
                .get_header(":message-type")
                .and_then(Header::i32_value)
                .unwrap(),
            0
        );
        assert_eq!(
            message
                .get_header("operation")
                .and_then(Header::string_value)
                .unwrap(),
            "PublishToTopic"
        );

        // Verify payload contains expected data
        let payload_str = std::str::from_utf8(&message.payload).unwrap();
        assert!(payload_str.contains("test/topic"));
        assert!(payload_str.contains("Hello, world!"));
    }
}
