//! Data models for Greengrass IPC
//!
//! This module defines the data models used for IPC communication with the Greengrass Core.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as, DeserializeAs, SerializeAs};

/// Helper for serializing/deserializing Bytes as Vec<u8>
struct BytesAsVec;

impl SerializeAs<Bytes> for BytesAsVec {
    fn serialize_as<S>(source: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        source.to_vec().serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, Bytes> for BytesAsVec {
    fn deserialize_as<D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Vec::<u8>::deserialize(deserializer).map(Bytes::from)
    }
}

/// A JSON message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonMessage {
    /// The JSON message payload
    pub message: serde_json::Value,

    /// The message context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<MessageContext>,
}

/// A binary message
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryMessage {
    /// The binary message payload
    #[serde_as(as = "Base64")]
    pub message: Bytes,

    /// The message context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<MessageContext>,
}

/// Context information for a message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageContext {
    /// The topic where the message was published
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

/// A message to publish
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// A JSON message to publish
    #[serde(rename = "jsonMessage")]
    Json(JsonMessage),

    /// A binary message to publish
    #[serde(rename = "binaryMessage")]
    Binary(BinaryMessage),
}

impl Message {
    /// Create a new JSON message
    pub fn json(message: serde_json::Value) -> Self {
        Message::Json(JsonMessage {
            message,
            context: None,
        })
    }

    /// Create a new binary message
    pub fn binary(message: Bytes) -> Self {
        Message::Binary(BinaryMessage {
            message,
            context: None,
        })
    }

    /// Set the topic context for this message
    pub fn with_topic<S: Into<String>>(mut self, topic: S) -> Self {
        let topic_str = topic.into();
        let context = Some(MessageContext {
            topic: Some(topic_str),
        });

        match &mut self {
            Message::Json(msg) => msg.context = context,
            Message::Binary(msg) => msg.context = context,
        }

        self
    }
}

/// A response from a subscription operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionResponseMessage {
    /// The message that was received
    #[serde(flatten)]
    pub message: Message,

    /// The name of the topic (deprecated - use context instead)
    #[serde(rename = "topicName", skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,
}

/// Behavior that specifies whether a component receives messages from itself
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiveMode {
    /// Receive all messages that match the topic, including from the subscriber
    #[serde(rename = "RECEIVE_ALL_MESSAGES")]
    ReceiveAllMessages,

    /// Receive all messages that match the topic, except from the subscriber
    #[serde(rename = "RECEIVE_MESSAGES_FROM_OTHERS")]
    ReceiveMessagesFromOthers,
}

impl Default for ReceiveMode {
    fn default() -> Self {
        Self::ReceiveMessagesFromOthers
    }
}

/// Request to publish to a topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishToTopicRequest {
    /// The topic to publish to
    #[serde(rename = "topic")]
    pub topic: String,

    /// The message to publish
    #[serde(rename = "publishMessage")]
    pub publish_message: Message,
}

/// Response to a publish to topic request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishToTopicResponse {}

/// Request to subscribe to a topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToTopicRequest {
    /// The topic to subscribe to
    pub topic: String,

    /// The receive mode
    #[serde(rename = "receiveMode", skip_serializing_if = "Option::is_none")]
    pub receive_mode: Option<ReceiveMode>,
}

/// Response to a subscribe to topic request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToTopicResponse {}

/// Request to publish to IoT Core MQTT
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishToIoTCoreRequest {
    /// The topic to publish to
    #[serde(rename = "topicName")]
    pub topic_name: String,

    /// The QoS to use
    pub qos: QoS,

    /// The payload to publish
    #[serde_as(as = "Base64")]
    pub payload: Bytes,

    /// The user properties to include in the publish
    #[serde(rename = "userProperties", skip_serializing_if = "Option::is_none")]
    pub user_properties: Option<Vec<UserProperty>>,

    /// The message expiry interval in seconds
    #[serde(
        rename = "messageExpiryIntervalSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_expiry_interval_seconds: Option<u32>,

    /// The correlation data
    #[serde_as(as = "Option<BytesAsVec>")]
    #[serde(rename = "correlationData", skip_serializing_if = "Option::is_none")]
    pub correlation_data: Option<Bytes>,

    /// The response topic
    #[serde(rename = "responseTopic", skip_serializing_if = "Option::is_none")]
    pub response_topic: Option<String>,

    /// The content type
    #[serde(rename = "contentType", skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// Response to a publish to IoT Core MQTT request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishToIoTCoreResponse {}

/// Request to subscribe to IoT Core MQTT
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToIoTCoreRequest {
    /// The topic filter to subscribe to
    #[serde(rename = "topicName")]
    pub topic_name: String,

    /// The QoS to use
    pub qos: QoS,
}

/// Response to a subscribe to IoT Core MQTT request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToIoTCoreResponse {}

/// A message received from IoT Core MQTT
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoTCoreMessage {
    /// The message
    pub message: MqttMessage,
}

/// An MQTT message
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttMessage {
    /// The topic name
    #[serde(rename = "topicName")]
    pub topic_name: String,

    /// The payload
    #[serde_as(as = "Base64")]
    pub payload: Bytes,

    /// The user properties
    #[serde(rename = "userProperties", skip_serializing_if = "Option::is_none")]
    pub user_properties: Option<Vec<UserProperty>>,

    /// The message expiry interval in seconds
    #[serde(
        rename = "messageExpiryIntervalSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_expiry_interval_seconds: Option<u32>,

    /// The correlation data
    #[serde_as(as = "Option<BytesAsVec>")]
    #[serde(rename = "correlationData", skip_serializing_if = "Option::is_none")]
    pub correlation_data: Option<Bytes>,

    /// The response topic
    #[serde(rename = "responseTopic", skip_serializing_if = "Option::is_none")]
    pub response_topic: Option<String>,

    /// The content type
    #[serde(rename = "contentType", skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// MQTT QoS level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QoS {
    /// QoS 0 - At most once delivery
    AtMostOnce = 0,

    /// QoS 1 - At least once delivery
    AtLeastOnce = 1,
}

impl Serialize for QoS {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for QoS {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        match value {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            _ => Err(serde::de::Error::custom(format!("Invalid QoS value: {}", value))),
        }
    }
}

impl Default for QoS {
    fn default() -> Self {
        Self::AtMostOnce
    }
}

/// An MQTT user property
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProperty {
    /// The property key
    pub key: String,

    /// The property value
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_message_json_serialization() {
        let message = Message::json(serde_json::json!({"test": "value"}));
        let serialized = serde_json::to_string(&message).unwrap();
        let deserialized: Message = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            Message::Json(json_msg) => {
                assert_eq!(json_msg.message, serde_json::json!({"test": "value"}));
            }
            _ => panic!("Expected JSON message"),
        }
    }

    #[test]
    fn test_publish_message_binary_serialization() {
        let data = Bytes::from(vec![1, 2, 3, 4]);
        let message = Message::binary(data.clone());
        let serialized = serde_json::to_string(&message).unwrap();
        let deserialized: Message = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            Message::Binary(binary_msg) => {
                assert_eq!(binary_msg.message, data);
            }
            _ => panic!("Expected binary message"),
        }
    }

    #[test]
    fn test_subscription_response_message_serialization() {
        let json_message = Message::json(serde_json::json!({"key": "value"}));
        let response = SubscriptionResponseMessage {
            message: json_message,
            topic_name: Some("test/topic".to_string()),
        };

        let serialized = serde_json::to_string(&response).unwrap();
        let deserialized: SubscriptionResponseMessage = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.topic_name, Some("test/topic".to_string()));
        match deserialized.message {
            Message::Json(json_msg) => {
                assert_eq!(json_msg.message, serde_json::json!({"key": "value"}));
            }
            _ => panic!("Expected JSON message"),
        }
    }

    #[test]
    fn test_publish_message_helper_methods() {
        let json_msg = Message::json(serde_json::json!({"test": true}));
        assert!(matches!(json_msg, Message::Json(_)));

        let binary_data = Bytes::from(vec![0xFF, 0x00, 0xAA]);
        let binary_msg = Message::binary(binary_data);
        assert!(matches!(binary_msg, Message::Binary(_)));
    }

    #[test]
    fn test_publish_message_with_topic() {
        let message = Message::json(serde_json::json!({"data": "test"})).with_topic("my/topic");

        match message {
            Message::Json(json_msg) => {
                assert_eq!(
                    json_msg.context.unwrap().topic,
                    Some("my/topic".to_string())
                );
            }
            _ => panic!("Expected JSON message"),
        }
    }
}
