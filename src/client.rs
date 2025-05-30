//! Greengrass Core IPC client
//!
//! This module provides the main client interface for the Greengrass Core IPC service.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use tokio::sync::mpsc;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::event_stream::Header;
use crate::lifecycle::LifecycleHandler;
use crate::model::{
    BinaryMessage, GetComponentDetailsRequest, GetComponentDetailsResponse, 
    GetLocalDeploymentStatusRequest, GetLocalDeploymentStatusResponse,
    IoTCoreMessage, ListComponentsRequest, ListComponentsResponse, ListLocalDeploymentsRequest, 
    ListLocalDeploymentsResponse, Message, PauseComponentRequest, PauseComponentResponse,
    PublishToIoTCoreRequest, PublishToIoTCoreResponse, PublishToTopicRequest,
    PublishToTopicResponse, RestartComponentRequest, RestartComponentResponse,
    ResumeComponentRequest, ResumeComponentResponse, StopComponentRequest, StopComponentResponse,
    SubscribeToIoTCoreRequest, SubscribeToIoTCoreResponse, SubscribeToTopicRequest, 
    SubscribeToTopicResponse, SubscriptionResponseMessage,
};

/// Default timeout for operations in seconds
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Client for the Greengrass Core IPC service
pub struct GreengrassCoreIPCClient {
    /// The connection to the Greengrass Core IPC service
    connection: Arc<Connection>,
    /// Timeout for operations
    operation_timeout: Duration,
}

impl GreengrassCoreIPCClient {
    /// Create a new client with the given connection
    pub fn new(connection: Arc<Connection>, operation_timeout: Duration) -> Self {
        Self {
            connection,
            operation_timeout,
        }
    }

    /// Connect to the Greengrass Core IPC service with default parameters
    pub async fn connect() -> Result<Self> {
        Self::connect_with_options(None, None, None, None).await
    }

    /// Connect to the Greengrass Core IPC service with custom parameters
    pub async fn connect_with_options(
        ipc_socket: Option<PathBuf>,
        auth_token: Option<String>,
        lifecycle_handler: Option<Box<dyn LifecycleHandler>>,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        // Connect to the IPC service
        let connection =
            Connection::connect(ipc_socket, auth_token, lifecycle_handler, timeout).await?;

        Ok(Self {
            connection: Arc::new(connection),
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        })
    }

    /// Set the timeout for operations
    pub fn set_operation_timeout(&mut self, timeout: Duration) {
        self.operation_timeout = timeout;
    }

    /// Get the timeout for operations
    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Close the connection to the Greengrass Core IPC service
    pub async fn close(&self) -> Result<()> {
        self.connection.close(None).await
    }

    /// Create a new publish to topic operation
    pub async fn publish_to_topic(
        &self,
        topic: &str,
        payload: Vec<u8>,
    ) -> Result<PublishToTopicResponse> {
        // Create the request
        let request = PublishToTopicRequest {
            topic: topic.to_string(),
            publish_message: Message::Binary(BinaryMessage {
                message: payload.into(),
                context: None,
            }),
        };

        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message following AWS Event Stream RPC protocol
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#PublishToTopic".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#PublishToTopicRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!("Sending PublishToTopic operation on stream {}", stream_id);
        log::trace!("Message headers: {:?}", event_message.headers);
        log::trace!(
            "Message payload: {}",
            String::from_utf8_lossy(&event_message.payload)
        );

        match self.connection.send_message(&event_message).await {
            Ok(()) => log::debug!("PublishToTopic message sent successfully"),
            Err(e) => {
                log::error!("Failed to send PublishToTopic message: {}", e);
                return Err(e);
            }
        }

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("PublishToTopic failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: PublishToTopicResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!("Failed to deserialize response: {}", e))
            })?;

        Ok(response)
    }

    /// Subscribe to a topic and return a Stream of messages
    pub async fn subscribe_to_topic(&self, topic: &str) -> Result<Subscription> {
        // Create the request
        let request = SubscribeToTopicRequest {
            topic: topic.to_string(),
            receive_mode: None,
        };

        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a unique operation ID
        let operation_id = uuid::Uuid::new_v4().to_string();

        // Create channels for the response and message delivery
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
        let (message_sender, message_receiver) = mpsc::unbounded_channel();

        // Create the subscription handler
        let handler = SubscriptionMessageHandler::new(message_sender);

        // Register the response handler for the initial subscribe response
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Register the stream handler for ongoing subscription messages
        let stream_handler = Box::new(handler);
        self.connection
            .register_stream_handler(operation_id.clone(), stream_handler)
            .await?;

        // Register the mapping from stream ID to operation ID for subscription messages
        self.connection
            .register_stream_operation_mapping(stream_id, operation_id.clone())
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message following AWS Event Stream RPC protocol
        // Create subscription message
        let mut subscribe_message = crate::event_stream::EventStreamMessage::new();
        subscribe_message = subscribe_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#SubscribeToTopic".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#SubscribeToTopicRequest".to_string(),
            ))
            .with_header(Header::OperationId(operation_id.clone()))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending SubscribeToTopic operation on stream {} with operation ID {}",
            stream_id,
            operation_id
        );
        log::trace!("Message headers: {:?}", subscribe_message.headers);
        log::trace!(
            "Message payload: {}",
            String::from_utf8_lossy(&subscribe_message.payload)
        );

        match self.connection.send_message(&subscribe_message).await {
            Ok(()) => log::debug!("SubscribeToTopic message sent successfully"),
            Err(e) => {
                log::error!("Failed to send SubscribeToTopic message: {}", e);
                let _ = self
                    .connection
                    .unregister_stream_handler(&operation_id)
                    .await;
                return Err(e);
            }
        }

        // Wait for the initial response
        log::debug!(
            "Waiting for SubscribeToTopic response on stream {}...",
            stream_id
        );

        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!(
                        "Received SubscribeToTopic response for stream {}: {}",
                        stream_id,
                        json_str
                    );
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    let _ = self
                        .connection
                        .unregister_stream_handler(&operation_id)
                        .await;
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    let _ = self
                        .connection
                        .unregister_stream_handler(&operation_id)
                        .await;
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    let _ = self
                        .connection
                        .unregister_stream_handler(&operation_id)
                        .await;
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("SubscribeToTopic failed: {}", error_msg);
                let _ = self
                    .connection
                    .unregister_stream_handler(&operation_id)
                    .await;
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let _response: SubscribeToTopicResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!(
                    "Failed to deserialize SubscribeToTopic response: {}",
                    e
                ))
            })?;

        log::info!("Successfully subscribed to topic: {}", request.topic);

        // Create the subscription
        Ok(Subscription::new(
            self.connection.clone(),
            operation_id,
            message_receiver,
        ))
    }

    /// Publish an MQTT message to AWS IoT message broker
    pub async fn publish_to_iot_core(
        &self,
        request: PublishToIoTCoreRequest,
    ) -> Result<PublishToIoTCoreResponse> {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message following AWS Event Stream RPC protocol
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#PublishToIoTCore".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#PublishToIoTCoreRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!("Sending PublishToIoTCore operation on stream {}", stream_id);
        log::trace!("Message headers: {:?}", event_message.headers);
        log::trace!(
            "Message payload: {}",
            String::from_utf8_lossy(&event_message.payload)
        );

        match self.connection.send_message(&event_message).await {
            Ok(()) => log::debug!("PublishToIoTCore message sent successfully"),
            Err(e) => {
                log::error!("Failed to send PublishToIoTCore message: {}", e);
                return Err(e);
            }
        }

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("PublishToIoTCore failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: PublishToIoTCoreResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!(
                    "Failed to deserialize PublishToIoTCore response: {}",
                    e
                ))
            })?;

        log::info!(
            "Successfully published to IoT Core topic: {}",
            request.topic_name
        );
        Ok(response)
    }

    // =============================================
    // Authorization and Client Device Operations
    // =============================================

    /// Authorize action on some resource
    pub async fn authorize_client_device_action(
        &self,
        _request: (), // TODO: Replace with AuthorizeClientDeviceActionRequest
    ) -> Result<()> {
        // TODO: Replace with AuthorizeClientDeviceActionResponse
        todo!("Implement authorize_client_device_action")
    }

    /// Get session token for a client device
    pub async fn get_client_device_auth_token(
        &self,
        _request: (), // TODO: Replace with GetClientDeviceAuthTokenRequest
    ) -> Result<()> {
        // TODO: Replace with GetClientDeviceAuthTokenResponse
        todo!("Implement get_client_device_auth_token")
    }

    /// Verify client device credentials
    pub async fn verify_client_device_identity(
        &self,
        _request: (), // TODO: Replace with VerifyClientDeviceIdentityRequest
    ) -> Result<()> {
        // TODO: Replace with VerifyClientDeviceIdentityResponse
        todo!("Implement verify_client_device_identity")
    }

    /// Validate authorization token (NOTE: Only usable by stream manager)
    pub async fn validate_authorization_token(
        &self,
        _request: (), // TODO: Replace with ValidateAuthorizationTokenRequest
    ) -> Result<()> {
        // TODO: Replace with ValidateAuthorizationTokenResponse
        todo!("Implement validate_authorization_token")
    }

    // =============================================
    // Local Deployment Operations
    // =============================================

    /// Create a local deployment on the device
    pub async fn create_local_deployment(
        &self,
        _request: (), // TODO: Replace with CreateLocalDeploymentRequest
    ) -> Result<()> {
        // TODO: Replace with CreateLocalDeploymentResponse
        todo!("Implement create_local_deployment")
    }

    /// Cancel a local deployment on the device
    pub async fn cancel_local_deployment(
        &self,
        _request: (), // TODO: Replace with CancelLocalDeploymentRequest
    ) -> Result<()> {
        // TODO: Replace with CancelLocalDeploymentResponse
        todo!("Implement cancel_local_deployment")
    }

    /// Get the status of a local deployment with the given deployment ID
    ///
    /// This operation retrieves detailed information about a specific deployment,
    /// including its current status, creation timestamp, and any error details
    /// if the deployment failed.
    pub async fn get_local_deployment_status(
        &self,
        request: GetLocalDeploymentStatusRequest,
    ) -> Result<GetLocalDeploymentStatusResponse> {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#GetLocalDeploymentStatus".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#GetLocalDeploymentStatusRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending GetLocalDeploymentStatus operation on stream {}",
            stream_id
        );
        self.connection.send_message(&event_message).await?;

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("GetLocalDeploymentStatus failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: GetLocalDeploymentStatusResponse = serde_json::from_str(&response_json)
            .map_err(|e| {
                Error::SerializationError(format!("Failed to deserialize response: {}", e))
            })?;

        Ok(response)
    }

    /// List the last 5 local deployments along with their statuses
    pub async fn list_local_deployments(&self) -> Result<ListLocalDeploymentsResponse> {
        // Create the request
        let request = ListLocalDeploymentsRequest {};

        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#ListLocalDeployments".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#ListLocalDeploymentsRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending ListLocalDeployments operation on stream {}",
            stream_id
        );
        self.connection.send_message(&event_message).await?;

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("ListLocalDeployments failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: ListLocalDeploymentsResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!("Failed to deserialize response: {}", e))
            })?;

        Ok(response)
    }

    // =============================================
    // Component Management Operations
    // =============================================

    /// Get the status and version of the component with the given component name
    pub async fn get_component_details(
        &self,
        request: GetComponentDetailsRequest,
    ) -> Result<GetComponentDetailsResponse> {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#GetComponentDetails".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#GetComponentDetailsRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending GetComponentDetails operation on stream {} for component '{}'",
            stream_id, request.component_name
        );
        self.connection.send_message(&event_message).await?;

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("GetComponentDetails failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: GetComponentDetailsResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!("Failed to deserialize response: {}", e))
            })?;

        Ok(response)
    }

    /// Request for a list of components
    pub async fn list_components(&self) -> Result<ListComponentsResponse> {
        // Create the request
        let request = ListComponentsRequest {};

        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#ListComponents".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#ListComponentsRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending ListComponents operation on stream {}",
            stream_id
        );
        self.connection.send_message(&event_message).await?;

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("ListComponents failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: ListComponentsResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!("Failed to deserialize response: {}", e))
            })?;

        Ok(response)
    }

    /// Restart a component with the given name
    pub async fn restart_component(
        &self,
        request: RestartComponentRequest,
    ) -> Result<RestartComponentResponse> {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#RestartComponent".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#RestartComponentRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending RestartComponent operation on stream {} for component '{}'",
            stream_id, request.component_name
        );
        self.connection.send_message(&event_message).await?;

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("RestartComponent failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: RestartComponentResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!("Failed to deserialize response: {}", e))
            })?;

        Ok(response)
    }

    /// Stop a component with the given name
    pub async fn stop_component(
        &self,
        request: StopComponentRequest,
    ) -> Result<StopComponentResponse> {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#StopComponent".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#StopComponentRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending StopComponent operation on stream {} for component '{}'",
            stream_id, request.component_name
        );
        self.connection.send_message(&event_message).await?;

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("StopComponent failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: StopComponentResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!("Failed to deserialize response: {}", e))
            })?;

        Ok(response)
    }

    /// Pause a running component
    pub async fn pause_component(
        &self,
        request: PauseComponentRequest,
    ) -> Result<PauseComponentResponse> {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#PauseComponent".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#PauseComponentRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending PauseComponent operation on stream {} for component '{}'",
            stream_id, request.component_name
        );
        self.connection.send_message(&event_message).await?;

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("PauseComponent failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: PauseComponentResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!("Failed to deserialize response: {}", e))
            })?;

        Ok(response)
    }

    /// Resume a paused component
    pub async fn resume_component(
        &self,
        request: ResumeComponentRequest,
    ) -> Result<ResumeComponentResponse> {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#ResumeComponent".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#ResumeComponentRequest".to_string(),
            ))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending ResumeComponent operation on stream {} for component '{}'",
            stream_id, request.component_name
        );
        self.connection.send_message(&event_message).await?;

        log::debug!("Waiting for response on stream {}...", stream_id);
        // Wait for the response with timeout
        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!("Received response for stream {}: {}", stream_id, json_str);
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("ResumeComponent failed: {}", error_msg);
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let response: ResumeComponentResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!("Failed to deserialize response: {}", e))
            })?;

        Ok(response)
    }

    /// Defer the update of components by a given amount of time
    pub async fn defer_component_update(
        &self,
        _request: (), // TODO: Replace with DeferComponentUpdateRequest
    ) -> Result<()> {
        // TODO: Replace with DeferComponentUpdateResponse
        todo!("Implement defer_component_update")
    }

    /// Send component metrics (NOTE: Only usable by AWS components)
    pub async fn put_component_metric(
        &self,
        _request: (), // TODO: Replace with PutComponentMetricRequest
    ) -> Result<()> {
        // TODO: Replace with PutComponentMetricResponse
        todo!("Implement put_component_metric")
    }

    // =============================================
    // Configuration Operations
    // =============================================

    /// Get value of a given key from the configuration
    pub async fn get_configuration(
        &self,
        _request: (), // TODO: Replace with GetConfigurationRequest
    ) -> Result<()> {
        // TODO: Replace with GetConfigurationResponse
        todo!("Implement get_configuration")
    }

    /// Update this component's configuration by replacing the value of given keyName
    pub async fn update_configuration(
        &self,
        _request: (), // TODO: Replace with UpdateConfigurationRequest
    ) -> Result<()> {
        // TODO: Replace with UpdateConfigurationResponse
        todo!("Implement update_configuration")
    }

    /// Send configuration validity report
    pub async fn send_configuration_validity_report(
        &self,
        _request: (), // TODO: Replace with SendConfigurationValidityReportRequest
    ) -> Result<()> {
        // TODO: Replace with SendConfigurationValidityReportResponse
        todo!("Implement send_configuration_validity_report")
    }

    // =============================================
    // State and Shadow Operations
    // =============================================

    /// Update status of this component
    pub async fn update_state(
        &self,
        _request: (), // TODO: Replace with UpdateStateRequest
    ) -> Result<()> {
        // TODO: Replace with UpdateStateResponse
        todo!("Implement update_state")
    }

    /// Retrieve a device shadow document stored by the local shadow service
    pub async fn get_thing_shadow(
        &self,
        _request: (), // TODO: Replace with GetThingShadowRequest
    ) -> Result<()> {
        // TODO: Replace with GetThingShadowResponse
        todo!("Implement get_thing_shadow")
    }

    /// Update a device shadow document stored in the local shadow service
    pub async fn update_thing_shadow(
        &self,
        _request: (), // TODO: Replace with UpdateThingShadowRequest
    ) -> Result<()> {
        // TODO: Replace with UpdateThingShadowResponse
        todo!("Implement update_thing_shadow")
    }

    /// Delete a device shadow document stored in the local shadow service
    pub async fn delete_thing_shadow(
        &self,
        _request: (), // TODO: Replace with DeleteThingShadowRequest
    ) -> Result<()> {
        // TODO: Replace with DeleteThingShadowResponse
        todo!("Implement delete_thing_shadow")
    }

    /// List the named shadows for the specified thing
    pub async fn list_named_shadows_for_thing(&self) -> Result<Vec<String>> {
        // TODO: Replace with ListNamedShadowsForThingResponse
        todo!("Implement list_named_shadows_for_thing")
    }

    // =============================================
    // Secret Management Operations
    // =============================================

    /// Retrieve a secret stored in AWS Secrets Manager
    pub async fn get_secret_value(
        &self,
        _request: (), // TODO: Replace with GetSecretValueRequest
    ) -> Result<()> {
        // TODO: Replace with GetSecretValueResponse
        todo!("Implement get_secret_value")
    }

    // =============================================
    // Debug Operations
    // =============================================

    /// Generate a password for the LocalDebugConsole component
    pub async fn create_debug_password(
        &self,
        _request: (), // TODO: Replace with CreateDebugPasswordRequest
    ) -> Result<()> {
        // TODO: Replace with CreateDebugPasswordResponse
        todo!("Implement create_debug_password")
    }

    // =============================================
    // Streaming Subscription Operations
    // =============================================

    /// Subscribe to receive notification if GGC is about to update any components
    pub async fn subscribe_to_component_updates(
        &self,
        _request: (), // TODO: Replace with SubscribeToComponentUpdatesRequest
    ) -> Result<()> {
        // TODO: Return appropriate streaming type like Subscription
        todo!("Implement subscribe_to_component_updates")
    }

    /// Subscribe to be notified when GGC updates the configuration
    pub async fn subscribe_to_configuration_update(
        &self,
        _request: (), // TODO: Replace with SubscribeToConfigurationUpdateRequest
    ) -> Result<()> {
        // TODO: Return appropriate streaming type like Subscription
        todo!("Implement subscribe_to_configuration_update")
    }

    /// Subscribe to be notified when GGC is about to update configuration for this component
    pub async fn subscribe_to_validate_configuration_updates(
        &self,
        _request: (), // TODO: Replace with SubscribeToValidateConfigurationUpdatesRequest
    ) -> Result<()> {
        // TODO: Return appropriate streaming type like Subscription
        todo!("Implement subscribe_to_validate_configuration_updates")
    }

    /// Create a subscription for new certificates
    pub async fn subscribe_to_certificate_updates(
        &self,
        _request: (), // TODO: Replace with SubscribeToCertificateUpdatesRequest
    ) -> Result<()> {
        // TODO: Return appropriate streaming type like Subscription
        todo!("Implement subscribe_to_certificate_updates")
    }

    /// Subscribe to a topic in AWS IoT message broker
    pub async fn subscribe_to_iot_core(
        &self,
        request: SubscribeToIoTCoreRequest,
    ) -> Result<IoTCoreSubscription> {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a unique operation ID
        let operation_id = uuid::Uuid::new_v4().to_string();

        // Create channels for the response and message delivery
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
        let (message_sender, message_receiver) = mpsc::unbounded_channel();

        // Create the subscription handler
        let handler = IoTCoreMessageHandler::new(message_sender);

        // Register the response handler for the initial subscribe response
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Register the stream handler for ongoing subscription messages
        let stream_handler = Box::new(handler);
        self.connection
            .register_stream_handler(operation_id.clone(), stream_handler)
            .await?;

        // Register the mapping from stream ID to operation ID for subscription messages
        self.connection
            .register_stream_operation_mapping(stream_id, operation_id.clone())
            .await?;

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message following AWS Event Stream RPC protocol
        // Create subscription message
        let mut subscribe_message = crate::event_stream::EventStreamMessage::new();
        subscribe_message = subscribe_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(
                "aws.greengrass#SubscribeToIoTCore".to_string(),
            ))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#SubscribeToIoTCoreRequest".to_string(),
            ))
            .with_header(Header::OperationId(operation_id.clone()))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending SubscribeToIoTCore operation on stream {} with operation ID {}",
            stream_id,
            operation_id
        );
        log::trace!("Message headers: {:?}", subscribe_message.headers);
        log::trace!(
            "Message payload: {}",
            String::from_utf8_lossy(&subscribe_message.payload)
        );

        match self.connection.send_message(&subscribe_message).await {
            Ok(()) => log::debug!("SubscribeToIoTCore message sent successfully"),
            Err(e) => {
                log::error!("Failed to send SubscribeToIoTCore message: {}", e);
                let _ = self
                    .connection
                    .unregister_stream_handler(&operation_id)
                    .await;
                return Err(e);
            }
        }

        // Wait for the initial response
        log::debug!(
            "Waiting for SubscribeToIoTCore response on stream {}...",
            stream_id
        );

        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!(
                        "Received SubscribeToIoTCore response for stream {}: {}",
                        stream_id,
                        json_str
                    );
                    json_str
                }
                Ok(Ok(Err(e))) => {
                    log::error!("Error response for stream {}: {}", stream_id, e);
                    let _ = self
                        .connection
                        .unregister_stream_handler(&operation_id)
                        .await;
                    return Err(e);
                }
                Ok(Err(_)) => {
                    log::error!("Response channel closed for stream {}", stream_id);
                    let _ = self
                        .connection
                        .unregister_stream_handler(&operation_id)
                        .await;
                    return Err(Error::ConnectionClosed(
                        "Response channel closed".to_string(),
                    ));
                }
                Err(_) => {
                    log::error!("Timeout waiting for response on stream {}", stream_id);
                    let _ = self
                        .connection
                        .unregister_stream_handler(&operation_id)
                        .await;
                    return Err(Error::OperationTimeout);
                }
            };

        // Check if this is an error response
        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&response_json) {
            if let Some(error_code) = error_response.get("_errorCode") {
                let error_msg = format!(
                    "Greengrass service error: {} - {}",
                    error_code,
                    error_response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("No error message")
                );
                log::error!("SubscribeToIoTCore failed: {}", error_msg);
                let _ = self
                    .connection
                    .unregister_stream_handler(&operation_id)
                    .await;
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let _response: SubscribeToIoTCoreResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!(
                    "Failed to deserialize SubscribeToIoTCore response: {}",
                    e
                ))
            })?;

        log::info!(
            "Successfully subscribed to IoT Core topic: {}",
            request.topic_name
        );

        // Create the IoT Core subscription
        Ok(IoTCoreSubscription::new(
            self.connection.clone(),
            operation_id,
            message_receiver,
        ))
    }
}

/// A Stream-based subscription that yields messages from a subscribed topic
pub struct Subscription {
    connection: std::sync::Arc<Connection>,
    operation_id: String,
    message_receiver: mpsc::UnboundedReceiver<SubscriptionResponseMessage>,
}

/// A Stream-based subscription that yields IoT Core MQTT messages
pub struct IoTCoreSubscription {
    connection: std::sync::Arc<Connection>,
    operation_id: String,
    message_receiver: mpsc::UnboundedReceiver<IoTCoreMessage>,
}

impl Subscription {
    fn new(
        connection: std::sync::Arc<Connection>,
        operation_id: String,
        message_receiver: mpsc::UnboundedReceiver<SubscriptionResponseMessage>,
    ) -> Self {
        Self {
            connection,
            operation_id,
            message_receiver,
        }
    }
}

impl IoTCoreSubscription {
    fn new(
        connection: std::sync::Arc<Connection>,
        operation_id: String,
        message_receiver: mpsc::UnboundedReceiver<IoTCoreMessage>,
    ) -> Self {
        Self {
            connection,
            operation_id,
            message_receiver,
        }
    }
}

impl Stream for Subscription {
    type Item = SubscriptionResponseMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.message_receiver.poll_recv(cx)
    }
}

impl Stream for IoTCoreSubscription {
    type Item = IoTCoreMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.message_receiver.poll_recv(cx)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        log::debug!("Dropping subscription for operation {}", self.operation_id);
        let connection = self.connection.clone();
        let operation_id = self.operation_id.clone();

        // Spawn a cleanup task since Drop cannot be async
        tokio::spawn(async move {
            if let Err(e) = connection.unregister_stream_handler(&operation_id).await {
                log::warn!("Failed to unregister stream handler during drop: {}", e);
            }
        });
    }
}

impl Drop for IoTCoreSubscription {
    fn drop(&mut self) {
        log::debug!(
            "Dropping IoT Core subscription for operation {}",
            self.operation_id
        );
        let connection = self.connection.clone();
        let operation_id = self.operation_id.clone();

        // Spawn a cleanup task since Drop cannot be async
        tokio::spawn(async move {
            if let Err(e) = connection.unregister_stream_handler(&operation_id).await {
                log::warn!(
                    "Failed to unregister IoT Core stream handler during drop: {}",
                    e
                );
            }
        });
    }
}

/// A handler that forwards subscription messages to a channel
struct SubscriptionMessageHandler {
    message_sender: mpsc::UnboundedSender<SubscriptionResponseMessage>,
}

impl SubscriptionMessageHandler {
    fn new(message_sender: mpsc::UnboundedSender<SubscriptionResponseMessage>) -> Self {
        Self { message_sender }
    }
}

/// A handler that forwards IoT Core messages to a channel
struct IoTCoreMessageHandler {
    message_sender: mpsc::UnboundedSender<IoTCoreMessage>,
}

impl IoTCoreMessageHandler {
    fn new(message_sender: mpsc::UnboundedSender<IoTCoreMessage>) -> Self {
        Self { message_sender }
    }
}

impl crate::connection::StreamResponseHandler for SubscriptionMessageHandler {
    fn handle_message(&self, message: crate::event_stream::EventStreamMessage) -> Result<()> {
        // Check if this is a subscription response message by service model type
        if let Some(service_model) = message
            .get_header("service-model-type")
            .and_then(Header::string_value)
        {
            if service_model == "aws.greengrass#SubscriptionResponseMessage" {
                // Deserialize the message payload to a SubscriptionResponseMessage
                let payload_str = String::from_utf8_lossy(&message.payload);
                log::debug!("Received subscription message: {}", payload_str);

                match serde_json::from_str::<SubscriptionResponseMessage>(&payload_str) {
                    Ok(subscription_message) => {
                        // Send the message through the channel
                        if let Err(_) = self.message_sender.send(subscription_message) {
                            log::warn!("Failed to send subscription message - receiver dropped");
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to deserialize subscription message: {}", e);
                        log::debug!("Raw message payload: {}", payload_str);
                    }
                }
                return Ok(());
            }
        }

        // For other message types, just log
        log::debug!(
            "Received non-subscription message: {}",
            String::from_utf8_lossy(&message.payload)
        );
        Ok(())
    }

    fn handle_error(&self, error: &Error) -> Result<bool> {
        log::error!("Subscription stream error: {}", error);
        // Close the stream on error
        Ok(true)
    }

    fn handle_closed(&self) -> Result<()> {
        log::debug!("Subscription stream closed");
        Ok(())
    }
}

impl crate::connection::StreamResponseHandler for IoTCoreMessageHandler {
    fn handle_message(&self, message: crate::event_stream::EventStreamMessage) -> Result<()> {
        // Check if this is an IoT Core message by service model type
        if let Some(service_model) = message
            .get_header("service-model-type")
            .and_then(Header::string_value)
        {
            if service_model == "aws.greengrass#IoTCoreMessage" {
                // Deserialize the message payload to an IoTCoreMessage
                let payload_str = String::from_utf8_lossy(&message.payload);
                log::debug!("Received IoT Core message: {}", payload_str);

                match serde_json::from_str::<IoTCoreMessage>(&payload_str) {
                    Ok(iot_core_message) => {
                        // Send the message through the channel
                        if let Err(_) = self.message_sender.send(iot_core_message) {
                            log::warn!("Failed to send IoT Core message - receiver dropped");
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to deserialize IoT Core message: {}", e);
                        log::debug!("Raw message payload: {}", payload_str);
                    }
                }
                return Ok(());
            }
        }

        // For other message types, just log
        log::debug!(
            "Received non-IoT Core message: {}",
            String::from_utf8_lossy(&message.payload)
        );
        Ok(())
    }

    fn handle_error(&self, error: &Error) -> Result<bool> {
        log::error!("IoT Core subscription stream error: {}", error);
        // Close the stream on error
        Ok(true)
    }

    fn handle_closed(&self) -> Result<()> {
        log::debug!("IoT Core subscription stream closed");
        Ok(())
    }
}
