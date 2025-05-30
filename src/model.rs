//! Data models for Greengrass IPC
//!
//! This module defines the data models used for IPC communication with the Greengrass Core.
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiveMode {
    /// Receive all messages that match the topic, including from the subscriber
    #[serde(rename = "RECEIVE_ALL_MESSAGES")]
    ReceiveAllMessages,

    /// Receive all messages that match the topic, except from the subscriber
    #[default]
    #[serde(rename = "RECEIVE_MESSAGES_FROM_OTHERS")]
    ReceiveMessagesFromOthers,
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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum QoS {
    /// QoS 0 - At most once delivery
    #[default]
    AtMostOnce = 0,

    /// QoS 1 - At least once delivery
    AtLeastOnce = 1,
}

/// An MQTT user property
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProperty {
    /// The property key
    pub key: String,

    /// The property value
    pub value: String,
}

/// Request to list local deployments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListLocalDeploymentsRequest {}

/// Response to list local deployments request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListLocalDeploymentsResponse {
    /// List of local deployments
    #[serde(rename = "localDeployments", skip_serializing_if = "Option::is_none")]
    pub local_deployments: Option<Vec<LocalDeployment>>,
}

/// Information about a local deployment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalDeployment {
    /// The ID of the deployment
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,

    /// The status of the deployment
    pub status: DeploymentStatus,

    /// The timestamp when the deployment was created
    #[serde(rename = "createdOn", skip_serializing_if = "Option::is_none")]
    pub created_on: Option<String>,

    /// The components in the deployment and their details
    #[serde(rename = "deploymentStatusDetails", skip_serializing_if = "Option::is_none")]
    pub deployment_status_details: Option<DeploymentStatusDetails>,
}

/// The status of a deployment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentStatus {
    /// The deployment is in progress
    #[serde(rename = "IN_PROGRESS")]
    InProgress,

    /// The deployment is queued
    #[serde(rename = "QUEUED")]
    Queued,

    /// The deployment failed
    #[serde(rename = "FAILED")]
    Failed,

    /// The deployment succeeded
    #[serde(rename = "SUCCEEDED")]
    Succeeded,
}

/// Detailed information about the deployment status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentStatusDetails {
    /// Detailed deployment status as a string
    #[serde(rename = "detailedDeploymentStatus", skip_serializing_if = "Option::is_none")]
    pub detailed_deployment_status: Option<String>,

    /// List of deployment error stacks
    #[serde(rename = "deploymentErrorStack", skip_serializing_if = "Option::is_none")]
    pub deployment_error_stack: Option<Vec<String>>,

    /// List of deployment error types
    #[serde(rename = "deploymentErrorTypes", skip_serializing_if = "Option::is_none")]
    pub deployment_error_types: Option<Vec<String>>,

    /// The reason for the current deployment status
    #[serde(rename = "deploymentFailureCause", skip_serializing_if = "Option::is_none")]
    pub deployment_failure_cause: Option<String>,
}

/// Request to get the status of a local deployment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLocalDeploymentStatusRequest {
    /// The ID of the deployment to get the status for
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,
}

/// Response to get local deployment status request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLocalDeploymentStatusResponse {
    /// The deployment information
    pub deployment: LocalDeployment,
}

/// Request to list components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListComponentsRequest {}

/// Response to list components request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListComponentsResponse {
    /// List of components
    #[serde(skip_serializing_if = "Option::is_none")]
    pub components: Option<Vec<ComponentDetails>>,
}

/// Information about a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentDetails {
    /// The name of the component
    #[serde(rename = "componentName")]
    pub component_name: String,

    /// The version of the component
    #[serde(rename = "version")]
    pub version: String,

    /// The state of the component
    #[serde(rename = "state")]
    pub state: ComponentState,

    /// Configuration for the component
    #[serde(rename = "configuration", skip_serializing_if = "Option::is_none")]
    pub configuration: Option<serde_json::Value>,
}

/// The state of a component
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComponentState {
    /// The component is new
    #[serde(rename = "NEW")]
    New,

    /// The component is installed
    #[serde(rename = "INSTALLED")]
    Installed,

    /// The component is starting
    #[serde(rename = "STARTING")]
    Starting,

    /// The component is running
    #[serde(rename = "RUNNING")]
    Running,

    /// The component is stopping
    #[serde(rename = "STOPPING")]
    Stopping,

    /// The component is errored
    #[serde(rename = "ERRORED")]
    Errored,

    /// The component is broken
    #[serde(rename = "BROKEN")]
    Broken,

    /// The component is finished
    #[serde(rename = "FINISHED")]
    Finished,
}

/// Request to get component details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetComponentDetailsRequest {
    /// The name of the component to get details for
    #[serde(rename = "componentName")]
    pub component_name: String,
}

/// Response to get component details request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetComponentDetailsResponse {
    /// The component details
    #[serde(rename = "componentDetails")]
    pub component_details: ComponentDetails,
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
