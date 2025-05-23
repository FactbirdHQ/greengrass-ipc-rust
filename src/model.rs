//! Data models for Greengrass IPC
//!
//! This module defines the data models used for IPC communication with the Greengrass Core.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

/// A JSON message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonMessage {
    /// The JSON message payload
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<serde_json::Value>,

    /// The message context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<MessageContext>,
}

/// A binary message
#[derive(Debug, Clone)]
pub struct BinaryMessage {
    /// The binary message payload
    pub message: Option<Bytes>,

    /// The message context
    pub context: Option<MessageContext>,
}

// Implement custom serialization for BinaryMessage
impl Serialize for BinaryMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("BinaryMessage", 2)?;

        if let Some(message) = &self.message {
            let vec = message.to_vec();
            state.serialize_field("message", &vec)?;
        }

        if let Some(context) = &self.context {
            state.serialize_field("context", context)?;
        }

        state.end()
    }
}

// Implement custom deserialization for BinaryMessage
impl<'de> Deserialize<'de> for BinaryMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            message: Option<Vec<u8>>,
            context: Option<MessageContext>,
        }

        let helper = Helper::deserialize(deserializer)?;

        Ok(BinaryMessage {
            message: helper.message.map(Bytes::from),
            context: helper.context,
        })
    }
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
pub struct PublishMessage {
    /// The JSON message to publish
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_message: Option<JsonMessage>,

    /// The binary message to publish
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binary_message: Option<BinaryMessage>,
}

/// A response from a subscription operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionResponseMessage {
    /// The JSON message that was received
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_message: Option<JsonMessage>,

    /// The binary message that was received
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binary_message: Option<BinaryMessage>,

    /// The name of the topic (deprecated - use context instead)
    #[serde(skip_serializing_if = "Option::is_none")]
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
    pub topic: String,

    /// The message to publish
    pub publish_message: PublishMessage,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_mode: Option<ReceiveMode>,
}

/// Response to a subscribe to topic request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToTopicResponse {}

/// Request to publish to IoT Core MQTT
#[derive(Debug, Clone)]
pub struct PublishToIoTCoreRequest {
    /// The topic to publish to
    pub topic_name: String,

    /// The QoS to use
    pub qos: QoS,

    /// The payload to publish
    pub payload: Bytes,

    /// The user properties to include in the publish
    pub user_properties: Option<Vec<UserProperty>>,

    /// The message expiry interval in seconds
    pub message_expiry_interval_seconds: Option<u32>,

    /// The correlation data
    pub correlation_data: Option<Bytes>,

    /// The response topic
    pub response_topic: Option<String>,

    /// The content type
    pub content_type: Option<String>,
}

// Implement custom serialization for PublishToIoTCoreRequest
impl Serialize for PublishToIoTCoreRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PublishToIoTCoreRequest", 8)?;

        state.serialize_field("topic_name", &self.topic_name)?;
        state.serialize_field("qos", &self.qos)?;

        let payload_vec = self.payload.to_vec();
        state.serialize_field("payload", &payload_vec)?;

        if let Some(props) = &self.user_properties {
            state.serialize_field("user_properties", props)?;
        }

        if let Some(expiry) = &self.message_expiry_interval_seconds {
            state.serialize_field("message_expiry_interval_seconds", expiry)?;
        }

        if let Some(data) = &self.correlation_data {
            let data_vec = data.to_vec();
            state.serialize_field("correlation_data", &data_vec)?;
        }

        if let Some(topic) = &self.response_topic {
            state.serialize_field("response_topic", topic)?;
        }

        if let Some(content_type) = &self.content_type {
            state.serialize_field("content_type", content_type)?;
        }

        state.end()
    }
}

// Implement custom deserialization for PublishToIoTCoreRequest
impl<'de> Deserialize<'de> for PublishToIoTCoreRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            topic_name: String,
            qos: QoS,
            payload: Vec<u8>,
            user_properties: Option<Vec<UserProperty>>,
            message_expiry_interval_seconds: Option<u32>,
            correlation_data: Option<Vec<u8>>,
            response_topic: Option<String>,
            content_type: Option<String>,
        }

        let helper = Helper::deserialize(deserializer)?;

        Ok(PublishToIoTCoreRequest {
            topic_name: helper.topic_name,
            qos: helper.qos,
            payload: Bytes::from(helper.payload),
            user_properties: helper.user_properties,
            message_expiry_interval_seconds: helper.message_expiry_interval_seconds,
            correlation_data: helper.correlation_data.map(Bytes::from),
            response_topic: helper.response_topic,
            content_type: helper.content_type,
        })
    }
}

/// Response to a publish to IoT Core MQTT request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishToIoTCoreResponse {}

/// Request to subscribe to IoT Core MQTT
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToIoTCoreRequest {
    /// The topic filter to subscribe to
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
#[derive(Debug, Clone)]
pub struct MqttMessage {
    /// The topic name
    pub topic_name: String,

    /// The payload
    pub payload: Bytes,

    /// The user properties
    pub user_properties: Option<Vec<UserProperty>>,

    /// The message expiry interval in seconds
    pub message_expiry_interval_seconds: Option<u32>,

    /// The correlation data
    pub correlation_data: Option<Bytes>,

    /// The response topic
    pub response_topic: Option<String>,

    /// The content type
    pub content_type: Option<String>,
}

// Implement custom serialization for MqttMessage
impl Serialize for MqttMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MqttMessage", 7)?;

        state.serialize_field("topic_name", &self.topic_name)?;

        let payload_vec = self.payload.to_vec();
        state.serialize_field("payload", &payload_vec)?;

        if let Some(props) = &self.user_properties {
            state.serialize_field("user_properties", props)?;
        }

        if let Some(expiry) = &self.message_expiry_interval_seconds {
            state.serialize_field("message_expiry_interval_seconds", expiry)?;
        }

        if let Some(data) = &self.correlation_data {
            let data_vec = data.to_vec();
            state.serialize_field("correlation_data", &data_vec)?;
        }

        if let Some(topic) = &self.response_topic {
            state.serialize_field("response_topic", topic)?;
        }

        if let Some(content_type) = &self.content_type {
            state.serialize_field("content_type", content_type)?;
        }

        state.end()
    }
}

// Implement custom deserialization for MqttMessage
impl<'de> Deserialize<'de> for MqttMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            topic_name: String,
            payload: Vec<u8>,
            user_properties: Option<Vec<UserProperty>>,
            message_expiry_interval_seconds: Option<u32>,
            correlation_data: Option<Vec<u8>>,
            response_topic: Option<String>,
            content_type: Option<String>,
        }

        let helper = Helper::deserialize(deserializer)?;

        Ok(MqttMessage {
            topic_name: helper.topic_name,
            payload: Bytes::from(helper.payload),
            user_properties: helper.user_properties,
            message_expiry_interval_seconds: helper.message_expiry_interval_seconds,
            correlation_data: helper.correlation_data.map(Bytes::from),
            response_topic: helper.response_topic,
            content_type: helper.content_type,
        })
    }
}

/// MQTT QoS level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QoS {
    /// QoS 0 - At most once delivery
    #[serde(rename = "AT_MOST_ONCE")]
    AtMostOnce = 0,

    /// QoS 1 - At least once delivery
    #[serde(rename = "AT_LEAST_ONCE")]
    AtLeastOnce = 1,
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

/// Message type for a Greengrass IPC message
pub enum Message {
    /// A JSON message
    Json(JsonMessage),

    /// A binary message
    Binary(BinaryMessage),
}

impl Message {
    /// Create a new JSON message
    pub fn json<V: Into<serde_json::Value>>(value: V) -> Self {
        Self::Json(JsonMessage {
            message: Some(value.into()),
            context: None,
        })
    }

    /// Create a new binary message
    pub fn binary<B: Into<Bytes>>(data: B) -> Self {
        Self::Binary(BinaryMessage {
            message: Some(data.into()),
            context: None,
        })
    }

    /// Set the topic context for this message
    pub fn with_topic<S: Into<String>>(self, topic: S) -> Self {
        match self {
            Self::Json(mut json) => {
                json.context = Some(MessageContext {
                    topic: Some(topic.into()),
                });
                Self::Json(json)
            }
            Self::Binary(mut binary) => {
                binary.context = Some(MessageContext {
                    topic: Some(topic.into()),
                });
                Self::Binary(binary)
            }
        }
    }
}
