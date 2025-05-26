//! Greengrass Core IPC client
//!
//! This module provides the main client interface for the Greengrass Core IPC service.

use crate::operation::Operation;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::event_stream::StreamResponseHandler;
use crate::lifecycle::LifecycleHandler;
use crate::model::{
    BinaryMessage, PublishMessage, PublishToTopicRequest, PublishToTopicResponse,
    SubscribeToTopicRequest, SubscribeToTopicResponse, SubscriptionResponseMessage,
};
use crate::operation::StreamOperation;

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

    /// Get a reference to the connection
    pub fn connection(&self) -> &Arc<Connection> {
        &self.connection
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

    /// Check if the client is connected
    pub fn is_connected(&self) -> bool {
        // This is a placeholder - in a real implementation we would check the connection status
        true
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
            publish_message: PublishMessage {
                binary_message: Some(BinaryMessage {
                    message: Some(payload.into()),
                    context: None,
                }),
                json_message: None,
            },
        };

        // Send the request
        self.publish_to_topic_with_request(request).await
    }

    /// Create a new publish to topic operation with a custom request
    pub async fn publish_to_topic_with_request(
        &self,
        request: PublishToTopicRequest,
    ) -> Result<PublishToTopicResponse> {
        // Create a unique operation ID for this request
        let operation_id = format!("publish-to-topic-{}", uuid::Uuid::new_v4());

        // Create a oneshot channel for the response
        let (_response_sender, response_receiver) = tokio::sync::oneshot::channel();

        // Serialize the request to JSON
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Create the event stream message
        // In a real implementation, this would include proper headers and framing
        // based on the AWS event stream protocol
        let message = format!(
            "{{\"operation\":\"aws.greengrass#PublishToTopic\",\"operationId\":\"{}\",\"payload\":{}}}",
            operation_id,
            request_json
        );

        // Register the response handler
        // This would be handled by the connection's message routing system
        // which we'll assume exists as part of the Connection implementation
        // TODO: Implement a proper message routing system in the Connection

        // Send the message over the connection
        // Create an event stream message to send
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(
                ":operation".to_string(),
                crate::event_stream::HeaderValue::String(
                    "aws.greengrass#PublishToTopic".to_string(),
                ),
            )
            .with_header(
                ":content-type".to_string(),
                crate::event_stream::HeaderValue::String("application/json".to_string()),
            )
            .with_payload(message.as_bytes().to_vec());

        self.connection.send_message(&event_message).await?;

        // Create an operation that will resolve when the response is received
        let operation = Operation::new(
            self.connection.clone(),
            operation_id,
            response_receiver,
            self.operation_timeout,
        );

        // Return the result of the operation
        operation.get_result().await
    }

    /// Subscribe to a topic
    pub async fn subscribe_to_topic<F, Fut>(
        &self,
        topic: &str,
        callback: F,
    ) -> Result<StreamOperation<SubscribeToTopicResponse, SubscriptionHandler<F, Fut>>>
    where
        F: Fn(SubscriptionResponseMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        // Create a subscription handler with the callback
        let handler = SubscriptionHandler::new(callback);

        // Create the request
        let request = SubscribeToTopicRequest {
            topic: topic.to_string(),
            receive_mode: None,
        };

        // Subscribe with the handler
        self.subscribe_to_topic_with_handler(request, handler).await
    }

    /// Subscribe to a topic with a custom handler
    pub async fn subscribe_to_topic_with_handler<H>(
        &self,
        _request: SubscribeToTopicRequest,
        _handler: H,
    ) -> Result<StreamOperation<SubscribeToTopicResponse, H>>
    where
        H: StreamResponseHandler + 'static,
    {
        // This is a placeholder - in a real implementation we would:
        // 1. Create an operation ID
        // 2. Create response channels
        // 3. Serialize the request
        // 4. Send it over the connection
        // 5. Set up a stream task to handle incoming messages
        // 6. Return a StreamOperation that will resolve when the initial response is received

        // Placeholder implementation
        Err(Error::NotImplemented("subscribe_to_topic".to_string()))
    }
}

/// A handler for subscription responses
pub struct SubscriptionHandler<F, Fut>
where
    F: Fn(SubscriptionResponseMessage) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    callback: F,
}

impl<F, Fut> SubscriptionHandler<F, Fut>
where
    F: Fn(SubscriptionResponseMessage) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    /// Create a new subscription handler
    pub fn new(callback: F) -> Self {
        Self { callback }
    }
}

impl<F, Fut> StreamResponseHandler for SubscriptionHandler<F, Fut>
where
    F: Fn(SubscriptionResponseMessage) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    fn on_stream_event(&self, _message: &crate::event_stream::EventStreamMessage) {
        // This is a placeholder - in a real implementation we would:
        // 1. Deserialize the message to a SubscriptionResponseMessage
        // 2. Call the callback with the message
        // 3. Handle any errors

        // Placeholder implementation - we can't actually call the callback without deserializing a message
    }

    fn on_stream_error(&self, _error: &Error) -> bool {
        // Default to closing the stream on error
        true
    }

    fn on_stream_closed(&self) {
        // No action needed by default
    }
}
