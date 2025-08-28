//! Data models for Greengrass IPC
//!
//! This module defines the data models used for IPC communication with the Greengrass Core.
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::{base64::Base64, serde_as, DeserializeAs, SerializeAs};

/// Helper for serializing/deserializing Bytes as Vec<u8>
struct BytesAsVec;

/// Helper module for Unix timestamp with fractional seconds
mod unix_timestamp_f64 {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let timestamp =
            dt.timestamp() as f64 + (dt.timestamp_subsec_nanos() as f64 / 1_000_000_000.0);
        timestamp.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let timestamp = f64::deserialize(deserializer)?;
        DateTime::from_timestamp(
            timestamp as i64,
            (timestamp.fract() * 1_000_000_000.0) as u32,
        )
        .ok_or_else(|| serde::de::Error::custom("Invalid timestamp"))
    }
}

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
    #[serde(
        rename = "deploymentStatusDetails",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        rename = "detailedDeploymentStatus",
        skip_serializing_if = "Option::is_none"
    )]
    pub detailed_deployment_status: Option<String>,

    /// List of deployment error stacks
    #[serde(
        rename = "deploymentErrorStack",
        skip_serializing_if = "Option::is_none"
    )]
    pub deployment_error_stack: Option<Vec<String>>,

    /// List of deployment error types
    #[serde(
        rename = "deploymentErrorTypes",
        skip_serializing_if = "Option::is_none"
    )]
    pub deployment_error_types: Option<Vec<String>>,

    /// The reason for the current deployment status
    #[serde(
        rename = "deploymentFailureCause",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

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

/// Request to restart a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartComponentRequest {
    /// The name of the component to restart
    #[serde(rename = "componentName")]
    pub component_name: String,
}

/// Response to restart component request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartComponentResponse {
    /// The restart status
    #[serde(rename = "restartStatus")]
    pub restart_status: RequestStatus,

    /// Optional message describing the restart result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Request to stop a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopComponentRequest {
    /// The name of the component to stop
    #[serde(rename = "componentName")]
    pub component_name: String,
}

/// Response to stop component request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopComponentResponse {
    /// The stop status
    #[serde(rename = "stopStatus")]
    pub stop_status: RequestStatus,

    /// Optional message describing the stop result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Request to pause a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseComponentRequest {
    /// The name of the component to pause
    #[serde(rename = "componentName")]
    pub component_name: String,
}

/// Response to pause component request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseComponentResponse {
    /// The pause status
    #[serde(rename = "pauseStatus")]
    pub pause_status: RequestStatus,

    /// Optional message describing the pause result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Request to resume a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeComponentRequest {
    /// The name of the component to resume
    #[serde(rename = "componentName")]
    pub component_name: String,
}

/// Response to resume component request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeComponentResponse {
    /// The resume status
    #[serde(rename = "resumeStatus")]
    pub resume_status: RequestStatus,

    /// Optional message describing the resume result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Status of a lifecycle operation request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestStatus {
    /// The request succeeded
    #[serde(rename = "SUCCEEDED")]
    Succeeded,

    /// The request failed
    #[serde(rename = "FAILED")]
    Failed,
}

// =============================================
// Configuration Operations
// =============================================

/// Request to get a configuration value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetConfigurationRequest {
    /// The name of the component to get configuration for (optional, defaults to calling component)
    #[serde(rename = "componentName", skip_serializing_if = "Option::is_none")]
    pub component_name: Option<String>,

    /// The key path to get the value for
    #[serde(rename = "keyPath")]
    pub key_path: Vec<String>,
}

/// Response to get configuration request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetConfigurationResponse {
    /// The configuration value
    #[serde(rename = "value")]
    pub value: serde_json::Value,

    /// The component name
    #[serde(rename = "componentName")]
    pub component_name: String,
}

/// Request to update a configuration value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfigurationRequest {
    /// The key path to update
    #[serde(rename = "keyPath")]
    pub key_path: Vec<String>,

    /// The value to merge at the specified key path
    #[serde(rename = "valueToMerge")]
    pub value_to_merge: serde_json::Value,

    /// The current Unix epoch time in milliseconds for concurrency control
    #[serde(rename = "timestamp")]
    pub timestamp: u64,
}

/// Response to update configuration request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfigurationResponse {}

/// Configuration validity status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigurationValidityStatus {
    /// The configuration is accepted
    #[serde(rename = "ACCEPTED")]
    Accepted,

    /// The configuration is rejected
    #[serde(rename = "REJECTED")]
    Rejected,
}

/// Configuration validity report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationValidityReport {
    /// The status of the configuration validation
    #[serde(rename = "status")]
    pub status: ConfigurationValidityStatus,

    /// The deployment ID associated with this validation
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,

    /// Optional message describing the validation result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Request to send configuration validity report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendConfigurationValidityReportRequest {
    /// The configuration validity report
    #[serde(rename = "configurationValidityReport")]
    pub configuration_validity_report: ConfigurationValidityReport,
}

/// Response to send configuration validity report request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendConfigurationValidityReportResponse {}

/// Request to subscribe to configuration updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToConfigurationUpdateRequest {
    /// The name of the component to subscribe to configuration updates for (optional, defaults to calling component)
    #[serde(rename = "componentName", skip_serializing_if = "Option::is_none")]
    pub component_name: Option<String>,

    /// The key path to subscribe to updates for
    #[serde(rename = "keyPath")]
    pub key_path: Vec<String>,
}

/// Response to subscribe to configuration update request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToConfigurationUpdateResponse {}

/// Request to subscribe to validate configuration updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToValidateConfigurationUpdatesRequest {}

/// Response to subscribe to validate configuration updates request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToValidateConfigurationUpdatesResponse {}

/// A configuration update event message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationUpdateEvent {
    /// The name of the component whose configuration was updated
    #[serde(rename = "componentName")]
    pub component_name: String,

    /// The key path that was updated
    #[serde(rename = "keyPath")]
    pub key_path: Vec<String>,
}

/// A configuration validation event message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateConfigurationUpdateEvent {
    /// The configuration to validate
    #[serde(rename = "configuration")]
    pub configuration: serde_json::Value,

    /// The deployment ID associated with this validation request
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,
}

// =============================================
// Authorization and Client Device Operations
// =============================================

/// Request to authorize a client device action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizeClientDeviceActionRequest {
    /// The session token for the client device from GetClientDeviceAuthToken
    #[serde(rename = "clientDeviceAuthToken")]
    pub client_device_auth_token: String,

    /// The operation to authorize
    pub operation: String,

    /// The resource the client device performs the operation on
    pub resource: String,
}

/// Response for authorize client device action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizeClientDeviceActionResponse {
    /// Whether the client device is authorized to perform the operation on the resource
    #[serde(rename = "isAuthorized")]
    pub is_authorized: bool,
}

/// Client device credential information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientDeviceCredential {
    /// The client device's X.509 device certificate
    #[serde(
        rename = "clientDeviceCertificate",
        skip_serializing_if = "Option::is_none"
    )]
    pub client_device_certificate: Option<String>,
}

/// Credential document for client device authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialDocument {
    /// The client device's MQTT credentials
    #[serde(rename = "mqttCredential", skip_serializing_if = "Option::is_none")]
    pub mqtt_credential: Option<MqttCredential>,
}

/// MQTT credential information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttCredential {
    /// The client ID to used to connect
    #[serde(rename = "clientId", skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// The client certificate in pem format
    #[serde(rename = "certificatePem", skip_serializing_if = "Option::is_none")]
    pub certificate_pem: Option<String>,

    /// The username (unused)
    pub username: Option<String>,

    /// The password (unused)
    pub password: Option<String>,
}

/// Request to get client device auth token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClientDeviceAuthTokenRequest {
    /// The client device's credentials
    pub credential: CredentialDocument,
}

/// Response for get client device auth token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClientDeviceAuthTokenResponse {
    /// The session token for the client device
    #[serde(rename = "clientDeviceAuthToken")]
    pub client_device_auth_token: String,
}

/// Request to verify client device identity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyClientDeviceIdentityRequest {
    /// The client device's credentials
    pub credential: ClientDeviceCredential,
}

/// Response for verify client device identity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyClientDeviceIdentityResponse {
    /// Whether the client device's identity is valid
    #[serde(rename = "isValidClientDevice")]
    pub is_valid_client_device: bool,
}

/// Request to validate authorization token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateAuthorizationTokenRequest {
    /// The token to validate
    pub token: String,
}

/// Response for validate authorization token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateAuthorizationTokenResponse {
    /// Whether the token is valid
    #[serde(rename = "isValid")]
    pub is_valid: bool,
}

// =============================================
// Local Deployment Operations
// =============================================

/// Failure handling policy for deployments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailureHandlingPolicy {
    #[serde(rename = "ROLLBACK")]
    Rollback,
    #[serde(rename = "DO_NOTHING")]
    DoNothing,
}

/// System resource limits for components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemResourceLimits {
    /// The maximum amount of RAM (in kilobytes) that this component's processes can use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<u64>,

    /// The maximum amount of CPU time that this component's processes can use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpus: Option<f64>,
}

/// Run with information for components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunWithInfo {
    /// The POSIX system user and, optionally, group to use to run this component on Linux
    #[serde(rename = "posixUser", skip_serializing_if = "Option::is_none")]
    pub posix_user: Option<String>,

    /// The Windows user to use to run this component on Windows
    #[serde(rename = "windowsUser", skip_serializing_if = "Option::is_none")]
    pub windows_user: Option<String>,

    /// The system resource limits to apply to this component's processes
    #[serde(
        rename = "systemResourceLimits",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_resource_limits: Option<SystemResourceLimits>,
}

/// Request to create a local deployment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateLocalDeploymentRequest {
    /// The thing group name the deployment is targeting
    #[serde(rename = "groupName", skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,

    /// Map of component name to version to add to the group's existing root components
    #[serde(
        rename = "rootComponentVersionsToAdd",
        skip_serializing_if = "Option::is_none"
    )]
    pub root_component_versions_to_add: Option<std::collections::HashMap<String, String>>,

    /// List of components that need to be removed from the group
    #[serde(
        rename = "rootComponentsToRemove",
        skip_serializing_if = "Option::is_none"
    )]
    pub root_components_to_remove: Option<Vec<String>>,

    /// Map of component names to configuration
    #[serde(
        rename = "componentToConfiguration",
        skip_serializing_if = "Option::is_none"
    )]
    pub component_to_configuration: Option<std::collections::HashMap<String, serde_json::Value>>,

    /// Map of component names to component run as info
    #[serde(
        rename = "componentToRunWithInfo",
        skip_serializing_if = "Option::is_none"
    )]
    pub component_to_run_with_info: Option<std::collections::HashMap<String, RunWithInfo>>,

    /// All recipes files in this directory will be copied over to the Greengrass package store
    #[serde(
        rename = "recipeDirectoryPath",
        skip_serializing_if = "Option::is_none"
    )]
    pub recipe_directory_path: Option<String>,

    /// All artifact files in this directory will be copied over to the Greengrass package store
    #[serde(
        rename = "artifactsDirectoryPath",
        skip_serializing_if = "Option::is_none"
    )]
    pub artifacts_directory_path: Option<String>,

    /// Deployment failure handling policy
    #[serde(
        rename = "failureHandlingPolicy",
        skip_serializing_if = "Option::is_none"
    )]
    pub failure_handling_policy: Option<FailureHandlingPolicy>,
}

/// Response for create local deployment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateLocalDeploymentResponse {
    /// The ID of the local deployment that the request created
    #[serde(rename = "deploymentId", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
}

/// Request to cancel a local deployment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelLocalDeploymentRequest {
    /// The ID of the local deployment to cancel
    #[serde(rename = "deploymentId", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
}

/// Response for cancel local deployment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelLocalDeploymentResponse {
    /// A message about the cancellation result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// =============================================
// Secret Management Operations
// =============================================

/// Secret value from AWS Secrets Manager
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretValue {
    /// The decrypted part of the protected secret information as a string
    #[serde(rename = "secretString", skip_serializing_if = "Option::is_none")]
    pub secret_string: Option<String>,

    /// The decrypted part of the protected secret information as binary data
    #[serde_as(as = "Option<Base64>")]
    #[serde(rename = "secretBinary", skip_serializing_if = "Option::is_none")]
    pub secret_binary: Option<Bytes>,
}

/// Request to get secret value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSecretValueRequest {
    /// The name of the secret to get
    #[serde(rename = "secretId")]
    pub secret_id: String,

    /// The ID of the version to get
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,

    /// The staging label of the version to get
    #[serde(rename = "versionStage", skip_serializing_if = "Option::is_none")]
    pub version_stage: Option<String>,

    /// Whether to fetch the latest secret from cloud when the request is handled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh: Option<bool>,
}

/// Response for get secret value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSecretValueResponse {
    /// The ID of the secret
    #[serde(rename = "secretId")]
    pub secret_id: String,

    /// The ID of this version of the secret
    #[serde(rename = "versionId")]
    pub version_id: String,

    /// The list of staging labels attached to this version of the secret
    #[serde(rename = "versionStage")]
    pub version_stage: Vec<String>,

    /// The value of this version of the secret
    #[serde(rename = "secretValue")]
    pub secret_value: SecretValue,
}

// =============================================
// Shadow Operations
// =============================================

/// Request to list named shadows for a thing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListNamedShadowsForThingRequest {
    /// The name of the thing
    #[serde(rename = "thingName")]
    pub thing_name: String,

    /// The token to retrieve the next set of results
    #[serde(rename = "nextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,

    /// The number of shadow names to return in each call
    #[serde(rename = "pageSize", skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
}

/// Response for list named shadows for thing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListNamedShadowsForThingResponse {
    /// The list of shadow names
    pub results: Vec<String>,

    /// The date and time that the response was generated
    #[serde(with = "unix_timestamp_f64")]
    pub timestamp: chrono::DateTime<chrono::Utc>,

    /// The token value to use in paged requests to retrieve the next page
    #[serde(rename = "nextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

/// Reported lifecycle state for components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReportedLifecycleState {
    #[serde(rename = "RUNNING")]
    Running,
    #[serde(rename = "ERRORED")]
    Errored,
}

/// Request to update component state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStateRequest {
    /// The state to set this component to
    pub state: ReportedLifecycleState,
}

/// Response for update state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStateResponse {}

/// Request to get thing shadow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetThingShadowRequest {
    /// The name of the thing
    #[serde(rename = "thingName")]
    pub thing_name: String,

    /// The name of the shadow
    #[serde(rename = "shadowName", skip_serializing_if = "Option::is_none")]
    pub shadow_name: Option<String>,
}

/// Response for get thing shadow
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetThingShadowResponse {
    /// The response state document as a JSON encoded blob
    #[serde_as(as = "Base64")]
    pub payload: Bytes,
}

/// Request to update thing shadow
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateThingShadowRequest {
    /// The name of the thing
    #[serde(rename = "thingName")]
    pub thing_name: String,

    /// The name of the shadow
    #[serde(rename = "shadowName", skip_serializing_if = "Option::is_none")]
    pub shadow_name: Option<String>,

    /// The request state document as a JSON encoded blob
    #[serde_as(as = "Base64")]
    pub payload: Bytes,
}

/// Response for update thing shadow
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateThingShadowResponse {
    /// The response state document as a JSON encoded blob
    #[serde_as(as = "Base64")]
    pub payload: Bytes,
}

/// Request to delete thing shadow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteThingShadowRequest {
    /// The name of the thing
    #[serde(rename = "thingName")]
    pub thing_name: String,

    /// The name of the shadow
    #[serde(rename = "shadowName", skip_serializing_if = "Option::is_none")]
    pub shadow_name: Option<String>,
}

/// Response for delete thing shadow
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteThingShadowResponse {
    /// An empty response state document
    #[serde_as(as = "Base64")]
    pub payload: Bytes,
}

// =============================================
// Component Update Operations
// =============================================

/// Request to defer component update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeferComponentUpdateRequest {
    /// The ID of the AWS IoT Greengrass deployment to defer
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,

    /// The name of the component for which to defer updates
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// The amount of time in milliseconds for which to defer the update
    #[serde(rename = "recheckAfterMs", skip_serializing_if = "Option::is_none")]
    pub recheck_after_ms: Option<u64>,
}

/// Response for defer component update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeferComponentUpdateResponse {}

/// Pre-component update event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreComponentUpdateEvent {
    /// The ID of the AWS IoT Greengrass deployment that updates the component
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,

    /// Whether or not Greengrass needs to restart to apply the update
    #[serde(rename = "isGgcRestarting")]
    pub is_ggc_restarting: bool,
}

/// Post-component update event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostComponentUpdateEvent {
    /// The ID of the AWS IoT Greengrass deployment that updated the component
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,
}

/// Component update policy events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentUpdatePolicyEvents {
    /// An event that indicates that the Greengrass wants to update a component
    #[serde(rename = "preUpdateEvent", skip_serializing_if = "Option::is_none")]
    pub pre_update_event: Option<PreComponentUpdateEvent>,

    /// An event that indicates that the nucleus updated a component
    #[serde(rename = "postUpdateEvent", skip_serializing_if = "Option::is_none")]
    pub post_update_event: Option<PostComponentUpdateEvent>,
}

/// Request to subscribe to component updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToComponentUpdatesRequest {}

/// Response for subscribe to component updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToComponentUpdatesResponse {}

// =============================================
// Certificate Operations
// =============================================

/// Certificate type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CertificateType {
    #[serde(rename = "SERVER")]
    Server,
}

/// Certificate options for subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateOptions {
    /// The types of certificate updates to subscribe to
    #[serde(rename = "certificateType")]
    pub certificate_type: CertificateType,
}

/// Certificate update information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateUpdate {
    /// The private key in pem format
    #[serde(rename = "privateKey", skip_serializing_if = "Option::is_none")]
    pub private_key: Option<String>,

    /// The public key in pem format
    #[serde(rename = "publicKey", skip_serializing_if = "Option::is_none")]
    pub public_key: Option<String>,

    /// The certificate in pem format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate: Option<String>,

    /// List of CA certificates in pem format
    #[serde(rename = "caCertificates", skip_serializing_if = "Option::is_none")]
    pub ca_certificates: Option<Vec<String>>,
}

/// Certificate update event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateUpdateEvent {
    /// The information about the new certificate
    #[serde(rename = "certificateUpdate", skip_serializing_if = "Option::is_none")]
    pub certificate_update: Option<CertificateUpdate>,
}

/// Request to subscribe to certificate updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToCertificateUpdatesRequest {
    /// Certificate options for the subscription
    #[serde(rename = "certificateOptions")]
    pub certificate_options: CertificateOptions,
}

/// Response for subscribe to certificate updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToCertificateUpdatesResponse {}

// =============================================
// Metrics Operations
// =============================================

/// Metric unit type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricUnitType {
    #[serde(rename = "BYTES")]
    Bytes,
    #[serde(rename = "BYTES_PER_SECOND")]
    BytesPerSecond,
    #[serde(rename = "COUNT")]
    Count,
    #[serde(rename = "COUNT_PER_SECOND")]
    CountPerSecond,
    #[serde(rename = "MEGABYTES")]
    Megabytes,
    #[serde(rename = "SECONDS")]
    Seconds,
}

/// Metric information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    /// The metric name
    pub name: String,

    /// The metric unit
    pub unit: MetricUnitType,

    /// The metric value
    pub value: f64,
}

/// Request to put component metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutComponentMetricRequest {
    /// The list of metrics to publish
    pub metrics: Vec<Metric>,
}

/// Response for put component metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutComponentMetricResponse {}

// =============================================
// Debug Operations
// =============================================

/// Request to create debug password
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDebugPasswordRequest {}

/// Response for create debug password
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDebugPasswordResponse {
    /// The generated password
    pub password: String,

    /// The username
    pub username: String,

    /// When the password expires
    #[serde(rename = "passwordExpiration")]
    pub password_expiration: chrono::DateTime<chrono::Utc>,

    /// SHA256 hash of the certificate
    #[serde(
        rename = "certificateSHA256Hash",
        skip_serializing_if = "Option::is_none"
    )]
    pub certificate_sha256_hash: Option<String>,

    /// SHA1 hash of the certificate
    #[serde(
        rename = "certificateSHA1Hash",
        skip_serializing_if = "Option::is_none"
    )]
    pub certificate_sha1_hash: Option<String>,
}

// =============================================
// Additional Enums for Completeness
// =============================================

/// Detailed deployment status enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DetailedDeploymentStatus {
    #[serde(rename = "SUCCESSFUL")]
    Successful,
    #[serde(rename = "FAILED_NO_STATE_CHANGE")]
    FailedNoStateChange,
    #[serde(rename = "FAILED_ROLLBACK_NOT_REQUESTED")]
    FailedRollbackNotRequested,
    #[serde(rename = "FAILED_ROLLBACK_COMPLETE")]
    FailedRollbackComplete,
    #[serde(rename = "REJECTED")]
    Rejected,
}

/// Payload format enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PayloadFormat {
    #[serde(rename = "0")]
    Bytes,
    #[serde(rename = "1")]
    Utf8,
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
