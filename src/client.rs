//! Greengrass Core IPC client
//!
//! This module provides the main client interface for the Greengrass Core IPC service.

use std::marker::PhantomData;
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
    GetConfigurationRequest, GetConfigurationResponse, GetLocalDeploymentStatusRequest,
    GetLocalDeploymentStatusResponse, IoTCoreMessage, ListComponentsRequest,
    ListComponentsResponse, ListLocalDeploymentsRequest, ListLocalDeploymentsResponse, Message,
    PauseComponentRequest, PauseComponentResponse, PublishToIoTCoreRequest,
    PublishToIoTCoreResponse, PublishToTopicRequest, PublishToTopicResponse,
    RestartComponentRequest, RestartComponentResponse, ResumeComponentRequest,
    ResumeComponentResponse, StopComponentRequest, StopComponentResponse,
    SubscribeToIoTCoreRequest, SubscribeToIoTCoreResponse, SubscribeToTopicRequest,
    SubscribeToTopicResponse, SubscriptionResponseMessage, UpdateConfigurationRequest,
    UpdateConfigurationResponse,
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

    /// Generic helper method for simple request-response operations
    async fn send_request<Req, Resp>(
        &self,
        operation_name: &str,
        request_type_name: &str,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a oneshot channel for the response
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Register the response handler for this stream
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Serialize the request to JSON
        let request_json =
            serde_json::to_string(request).map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message following AWS Event Stream RPC protocol
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(format!(
                "aws.greengrass#{}",
                operation_name
            )))
            .with_header(Header::ServiceModelType(format!(
                "aws.greengrass#{}",
                request_type_name
            )))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending {} operation on stream {}",
            operation_name,
            stream_id
        );
        log::trace!("Message headers: {:?}", event_message.headers);
        log::trace!(
            "Message payload: {}",
            String::from_utf8_lossy(&event_message.payload)
        );

        match self.connection.send_message(&event_message).await {
            Ok(()) => log::debug!("{} message sent successfully", operation_name),
            Err(e) => {
                log::error!("Failed to send {} message: {}", operation_name, e);
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
        if let Err(error) = crate::error::check_and_parse_error_response(&response_json) {
            log::error!("{} failed: {}", operation_name, error);
            return Err(error);
        }

        // Deserialize the response as success
        let response: Resp = serde_json::from_str(&response_json).map_err(|e| {
            Error::SerializationError(format!(
                "Failed to deserialize {} response: {}",
                operation_name, e
            ))
        })?;

        Ok(response)
    }

    /// Generic helper method for subscription operations
    async fn send_subscription_request<Req, Resp>(
        &self,
        operation_name: &str,
        request_type_name: &str,
        message_type_name: &str,
        request: &Req,
    ) -> Result<StreamOperation<Resp>>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;

        // Create a unique operation ID
        let operation_id = uuid::Uuid::new_v4().to_string();

        // Create channels for the response and message delivery
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
        let (message_sender, message_receiver) = mpsc::unbounded_channel();

        // Create the subscription handler
        let handler = StreamOperationHandler::new(message_sender, message_type_name);

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
        let request_json =
            serde_json::to_string(request).map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message following AWS Event Stream RPC protocol
        let mut subscribe_message = crate::event_stream::EventStreamMessage::new();
        subscribe_message = subscribe_message
            .with_header(Header::MessageType(0)) // APPLICATION_MESSAGE = 0
            .with_header(Header::StreamId(stream_id))
            .with_header(Header::MessageFlags(0)) // No flags
            .with_header(Header::ContentType("application/json".to_string()))
            .with_header(Header::Operation(format!(
                "aws.greengrass#{}",
                operation_name
            )))
            .with_header(Header::ServiceModelType(format!(
                "aws.greengrass#{}",
                request_type_name
            )))
            .with_header(Header::OperationId(operation_id.clone()))
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!(
            "Sending {} operation on stream {} with operation ID {}",
            operation_name,
            stream_id,
            operation_id
        );
        log::trace!("Message headers: {:?}", subscribe_message.headers);
        log::trace!(
            "Message payload: {}",
            String::from_utf8_lossy(&subscribe_message.payload)
        );

        match self.connection.send_message(&subscribe_message).await {
            Ok(()) => log::debug!("{} message sent successfully", operation_name),
            Err(e) => {
                log::error!("Failed to send {} message: {}", operation_name, e);
                let _ = self
                    .connection
                    .unregister_stream_handler(&operation_id)
                    .await;
                return Err(e);
            }
        }

        // Wait for the initial response
        log::debug!(
            "Waiting for {} response on stream {}...",
            operation_name,
            stream_id
        );

        let response_json =
            match tokio::time::timeout(self.operation_timeout, response_receiver).await {
                Ok(Ok(Ok(json_str))) => {
                    log::debug!(
                        "Received {} response for stream {}: {}",
                        operation_name,
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
        if let Err(error) = crate::error::check_and_parse_error_response(&response_json) {
            log::error!("{} failed: {}", operation_name, error);
            let _ = self
                .connection
                .unregister_stream_handler(&operation_id)
                .await;
            return Err(error);
        }

        // Deserialize the response to validate it (we don't need to keep it for subscriptions)
        let _response: SubscribeToTopicResponse =
            serde_json::from_str(&response_json).map_err(|e| {
                Error::SerializationError(format!(
                    "Failed to deserialize {} response: {}",
                    operation_name, e
                ))
            })?;

        // Create the subscription
        Ok(StreamOperation::new(
            self.connection.clone(),
            operation_id,
            message_receiver,
        ))
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
        let request = PublishToTopicRequest {
            topic: topic.to_string(),
            publish_message: Message::Binary(BinaryMessage {
                message: payload.into(),
                context: None,
            }),
        };

        self.send_request("PublishToTopic", "PublishToTopicRequest", &request)
            .await
    }

    /// Subscribe to a topic and return a Stream of messages
    pub async fn subscribe_to_topic(
        &self,
        topic: &str,
    ) -> Result<StreamOperation<SubscriptionResponseMessage>> {
        let request = SubscribeToTopicRequest {
            topic: topic.to_string(),
            receive_mode: None,
        };

        self.send_subscription_request(
            "SubscribeToTopic",
            "SubscribeToTopicRequest",
            "SubscriptionResponseMessage",
            &request,
        )
        .await
    }

    /// Publish an MQTT message to AWS IoT message broker
    pub async fn publish_to_iot_core(
        &self,
        request: PublishToIoTCoreRequest,
    ) -> Result<PublishToIoTCoreResponse> {
        let response = self
            .send_request("PublishToIoTCore", "PublishToIoTCoreRequest", &request)
            .await?;

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
        self.send_request(
            "GetLocalDeploymentStatus",
            "GetLocalDeploymentStatusRequest",
            &request,
        )
        .await
    }

    /// List the last 5 local deployments along with their statuses
    pub async fn list_local_deployments(&self) -> Result<ListLocalDeploymentsResponse> {
        let request = ListLocalDeploymentsRequest {};
        self.send_request(
            "ListLocalDeployments",
            "ListLocalDeploymentsRequest",
            &request,
        )
        .await
    }

    // =============================================
    // Component Management Operations
    // =============================================

    /// Get the status and version of the component with the given component name
    pub async fn get_component_details(
        &self,
        request: GetComponentDetailsRequest,
    ) -> Result<GetComponentDetailsResponse> {
        self.send_request(
            "GetComponentDetails",
            "GetComponentDetailsRequest",
            &request,
        )
        .await
    }

    /// Request for a list of components
    pub async fn list_components(&self) -> Result<ListComponentsResponse> {
        let request = ListComponentsRequest {};
        self.send_request("ListComponents", "ListComponentsRequest", &request)
            .await
    }

    /// Restart a component with the given name
    pub async fn restart_component(
        &self,
        request: RestartComponentRequest,
    ) -> Result<RestartComponentResponse> {
        self.send_request("RestartComponent", "RestartComponentRequest", &request)
            .await
    }

    /// Stop a component with the given name
    pub async fn stop_component(
        &self,
        request: StopComponentRequest,
    ) -> Result<StopComponentResponse> {
        self.send_request("StopComponent", "StopComponentRequest", &request)
            .await
    }

    /// Pause a running component
    pub async fn pause_component(
        &self,
        request: PauseComponentRequest,
    ) -> Result<PauseComponentResponse> {
        self.send_request("PauseComponent", "PauseComponentRequest", &request)
            .await
    }

    /// Resume a paused component
    pub async fn resume_component(
        &self,
        request: ResumeComponentRequest,
    ) -> Result<ResumeComponentResponse> {
        self.send_request("ResumeComponent", "ResumeComponentRequest", &request)
            .await
    }

    /// Defer the update of components by a given amount of time
    pub async fn defer_component_update(
        &self,
        _request: (), // TODO: Replace with DeferComponentUpdateRequest
    ) -> Result<()> {
        // TODO: Replace with DeferComponentUpdateResponse
        todo!("Implement defer_component_update")
    }

    // =============================================
    // Configuration Operations
    // =============================================

    /// Get value of a given key from the configuration
    pub async fn get_configuration(
        &self,
        request: GetConfigurationRequest,
    ) -> Result<GetConfigurationResponse> {
        self.send_request("GetConfiguration", "GetConfigurationRequest", &request)
            .await
    }

    /// Update this component's configuration by replacing the value of given keyName
    pub async fn update_configuration(
        &self,
        request: UpdateConfigurationRequest,
    ) -> Result<UpdateConfigurationResponse> {
        self.send_request(
            "UpdateConfiguration",
            "UpdateConfigurationRequest",
            &request,
        )
        .await
    }

    /// Send configuration validity report
    pub async fn send_configuration_validity_report(
        &self,
        request: crate::model::SendConfigurationValidityReportRequest,
    ) -> Result<crate::model::SendConfigurationValidityReportResponse> {
        self.send_request(
            "SendConfigurationValidityReport",
            "SendConfigurationValidityReportRequest",
            &request,
        )
        .await
    }

    // =============================================
    // State and Shadow Operations
    // =============================================

    // /// Update state of a component
    // pub async fn update_state(
    //     &self,
    //     request: crate::model::UpdateStateRequest,
    // ) -> Result<crate::model::UpdateStateResponse> {
    //     self.send_request("UpdateState", "UpdateStateRequest", &request)
    //         .await
    // }

    // /// Retrieve a device shadow document stored by the local shadow service
    // pub async fn get_thing_shadow(
    //     &self,
    //     request: crate::model::GetThingShadowRequest,
    // ) -> Result<crate::model::GetThingShadowResponse> {
    //     self.send_request("GetThingShadow", "GetThingShadowRequest", &request)
    //         .await
    // }

    // /// Update a device shadow document stored by the local shadow service
    // pub async fn update_thing_shadow(
    //     &self,
    //     request: crate::model::UpdateThingShadowRequest,
    // ) -> Result<crate::model::UpdateThingShadowResponse> {
    //     self.send_request("UpdateThingShadow", "UpdateThingShadowRequest", &request)
    //         .await
    // }

    // /// Retrieve a device shadow document stored by the local shadow service
    // pub async fn delete_thing_shadow(
    //     &self,
    //     request: crate::model::UpdateThingShadowRequest,
    // ) -> Result<crate::model::UpdateThingShadowResponse> {
    //     self.send_request("UpdateThingShadow", "UpdateThingShadowRequest", &request)
    //         .await
    // }

    /// Retrieve a device shadow document stored by the local shadow service
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
    ) -> Result<StreamOperation<SubscriptionResponseMessage>> {
        self.send_subscription_request(
            "SubscribeToIoTCore",
            "SubscribeToIoTCoreRequest",
            "IoTCoreMessage",
            &request,
        )
        .await
    }
}

/// A Stream-based subscription that yields messages from a subscribed topic
pub struct StreamOperation<Resp> {
    connection: std::sync::Arc<Connection>,
    operation_id: String,
    message_receiver: mpsc::UnboundedReceiver<Resp>,
}

impl<Resp> StreamOperation<Resp> {
    fn new(
        connection: std::sync::Arc<Connection>,
        operation_id: String,
        message_receiver: mpsc::UnboundedReceiver<Resp>,
    ) -> Self {
        Self {
            connection,
            operation_id,
            message_receiver,
        }
    }
}

impl<Resp> Stream for StreamOperation<Resp> {
    type Item = Resp;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.message_receiver.poll_recv(cx)
    }
}

impl<Resp> Drop for StreamOperation<Resp> {
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
/// A handler that forwards subscription messages to a channel
struct StreamOperationHandler<Resp> {
    message_type_name: String,
    message_sender: mpsc::UnboundedSender<Resp>,
}

impl<Resp> StreamOperationHandler<Resp> {
    fn new(message_sender: mpsc::UnboundedSender<Resp>, message_type_name: &str) -> Self {
        Self {
            message_sender,
            message_type_name: message_type_name.to_string(),
        }
    }
}

impl<Resp> crate::connection::StreamResponseHandler for StreamOperationHandler<Resp>
where
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    fn handle_message(&self, message: crate::event_stream::EventStreamMessage) -> Result<()> {
        // Check if this is a subscription response message by service model type
        if let Some(service_model) = message
            .get_header("service-model-type")
            .and_then(Header::string_value)
        {
            if service_model == format!("aws.greengrass#{}", self.message_type_name) {
                // Deserialize the message payload to a Resp
                let payload_str = String::from_utf8_lossy(&message.payload);
                log::debug!("Received streaming message: {}", payload_str);

                match serde_json::from_str::<Resp>(&payload_str) {
                    Ok(message) => {
                        // Send the message through the channel
                        if let Err(_) = self.message_sender.send(message) {
                            log::warn!("Failed to send streaming message - receiver dropped");
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to deserialize streaming message: {}", e);
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
