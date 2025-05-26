//! Greengrass Core IPC client
//!
//! This module provides the main client interface for the Greengrass Core IPC service.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::event_stream::StreamResponseHandler;
use crate::lifecycle::LifecycleHandler;
use crate::model::{
    BinaryMessage, Message, PublishToTopicRequest, PublishToTopicResponse, SubscribeToTopicRequest,
    SubscribeToTopicResponse, SubscriptionResponseMessage,
};
use crate::operation::StreamOperation;

/// Default timeout for operations in seconds
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);

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
            publish_message: Message::Binary(BinaryMessage {
                message: payload.into(),
                context: None,
            }),
        };

        // Send the request
        self.publish_to_topic_with_request(request).await
    }

    /// Create a new publish to topic operation with a custom request
    pub async fn publish_to_topic_with_request(
        &self,
        request: PublishToTopicRequest,
    ) -> Result<PublishToTopicResponse> {
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
            .with_header(
                ":message-type".to_string(),
                crate::event_stream::HeaderValue::I32(0), // APPLICATION_MESSAGE = 0
            )
            .with_header(
                ":stream-id".to_string(),
                crate::event_stream::HeaderValue::I32(stream_id),
            )
            .with_header(
                ":message-flags".to_string(),
                crate::event_stream::HeaderValue::I32(0), // No flags
            )
            .with_header(
                ":content-type".to_string(),
                crate::event_stream::HeaderValue::String("application/json".to_string()),
            )
            .with_header(
                "operation".to_string(),
                crate::event_stream::HeaderValue::String(
                    "aws.greengrass#PublishToTopic".to_string(),
                ),
            )
            .with_header(
                "service-model-type".to_string(),
                crate::event_stream::HeaderValue::String(
                    "aws.greengrass#PublishToTopicRequest".to_string(),
                ),
            )
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

    /// Subscribe to a topic
    pub async fn subscribe_to_topic<F, Fut>(
        &self,
        topic: &str,
        callback: F,
    ) -> Result<StreamOperation<SubscribeToTopicResponse, std::sync::Arc<SubscriptionHandler<F, Fut>>>>
    where
        F: Fn(SubscriptionResponseMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        // Create a subscription handler with the callback
        let handler = std::sync::Arc::new(SubscriptionHandler::new(callback));

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
        request: SubscribeToTopicRequest,
        handler: std::sync::Arc<H>,
    ) -> Result<StreamOperation<SubscribeToTopicResponse, std::sync::Arc<H>>>
    where
        H: StreamResponseHandler + 'static,
    {
        // Create a unique stream ID for this operation
        let stream_id = self.connection.allocate_stream_id().await?;
        
        // Create a unique operation ID 
        let operation_id = uuid::Uuid::new_v4().to_string();

        // Create channels for the response and close control
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
        let (close_sender, close_receiver) = tokio::sync::mpsc::channel(1);

        // Register the response handler for the initial subscribe response
        self.connection
            .register_response_handler(stream_id, response_sender)
            .await?;

        // Register the stream handler for ongoing subscription messages
        let stream_handler = Box::new(SubscriptionStreamWrapper::new(
            operation_id.clone(), 
            handler.clone()
        ));
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
        let mut event_message = crate::event_stream::EventStreamMessage::new();
        event_message = event_message
            .with_header(
                ":message-type".to_string(),
                crate::event_stream::HeaderValue::I32(0), // APPLICATION_MESSAGE = 0
            )
            .with_header(
                ":stream-id".to_string(),
                crate::event_stream::HeaderValue::I32(stream_id),
            )
            .with_header(
                ":message-flags".to_string(),
                crate::event_stream::HeaderValue::I32(0), // No flags
            )
            .with_header(
                ":content-type".to_string(),
                crate::event_stream::HeaderValue::String("application/json".to_string()),
            )
            .with_header(
                "operation".to_string(),
                crate::event_stream::HeaderValue::String(
                    "aws.greengrass#SubscribeToTopic".to_string(),
                ),
            )
            .with_header(
                "service-model-type".to_string(),
                crate::event_stream::HeaderValue::String(
                    "aws.greengrass#SubscribeToTopicRequest".to_string(),
                ),
            )
            .with_header(
                ":operation-id".to_string(),
                crate::event_stream::HeaderValue::String(operation_id.clone()),
            )
            .with_payload(request_json.as_bytes().to_vec());

        // Send the message over the connection
        log::debug!("Sending SubscribeToTopic operation on stream {} with operation ID {}", stream_id, operation_id);
        log::trace!("Message headers: {:?}", event_message.headers);
        log::trace!(
            "Message payload: {}",
            String::from_utf8_lossy(&event_message.payload)
        );

        match self.connection.send_message(&event_message).await {
            Ok(()) => log::debug!("SubscribeToTopic message sent successfully"),
            Err(e) => {
                log::error!("Failed to send SubscribeToTopic message: {}", e);
                let _ = self.connection.unregister_stream_handler(&operation_id).await;
                return Err(e);
            }
        }

        // Wait for the initial response
        log::debug!("Waiting for SubscribeToTopic response on stream {}...", stream_id);
        
        let response_json = match tokio::time::timeout(self.operation_timeout, response_receiver).await {
            Ok(Ok(Ok(json_str))) => {
                log::debug!("Received SubscribeToTopic response for stream {}: {}", stream_id, json_str);
                json_str
            }
            Ok(Ok(Err(e))) => {
                log::error!("Error response for stream {}: {}", stream_id, e);
                let _ = self.connection.unregister_stream_handler(&operation_id).await;
                return Err(e);
            }
            Ok(Err(_)) => {
                log::error!("Response channel closed for stream {}", stream_id);
                let _ = self.connection.unregister_stream_handler(&operation_id).await;
                return Err(Error::ConnectionClosed("Response channel closed".to_string()));
            }
            Err(_) => {
                log::error!("Timeout waiting for response on stream {}", stream_id);
                let _ = self.connection.unregister_stream_handler(&operation_id).await;
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
                let _ = self.connection.unregister_stream_handler(&operation_id).await;
                return Err(Error::ServiceError(
                    error_code.as_str().unwrap_or("Unknown").to_string(),
                    error_msg,
                ));
            }
        }

        // Deserialize the response as success
        let _response: SubscribeToTopicResponse = serde_json::from_str(&response_json).map_err(|e| {
            Error::SerializationError(format!("Failed to deserialize SubscribeToTopic response: {}", e))
        })?;

        log::info!("Successfully subscribed to topic: {}", request.topic);

        // Create a dummy response receiver since we already handled the response
        let (_dummy_sender, dummy_receiver) = tokio::sync::oneshot::channel();
        
        // Create the stream operation that represents the subscription
        let stream_operation = StreamOperation::new(
            self.connection.clone(),
            operation_id.clone(),
            dummy_receiver,
            handler.clone(),
            close_sender,
            self.operation_timeout,
        );

        // Start the stream task to handle close requests
        let connection_clone = self.connection.clone();
        let operation_id_clone = operation_id.clone();
        tokio::spawn(async move {
            let mut close_receiver = close_receiver;
            if let Some(_) = close_receiver.recv().await {
                log::debug!("Closing subscription stream for operation {}", operation_id_clone);
                let _ = connection_clone.unregister_stream_handler(&operation_id_clone).await;
            }
        });

        Ok(stream_operation)
    }
}

// Implement StreamResponseHandler for Arc<H> where H: StreamResponseHandler
impl<H: StreamResponseHandler> crate::event_stream::StreamResponseHandler for std::sync::Arc<H> {
    fn on_stream_event(&self, message: &crate::event_stream::EventStreamMessage) {
        (**self).on_stream_event(message)
    }

    fn on_stream_error(&self, error: &Error) -> bool {
        (**self).on_stream_error(error)
    }

    fn on_stream_closed(&self) {
        (**self).on_stream_closed()
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
    fn on_stream_event(&self, message: &crate::event_stream::EventStreamMessage) {
        // Deserialize the message payload to a SubscriptionResponseMessage
        let payload_str = String::from_utf8_lossy(&message.payload);
        log::debug!("Received subscription message: {}", payload_str);
        
        match serde_json::from_str::<SubscriptionResponseMessage>(&payload_str) {
            Ok(subscription_message) => {
                // Spawn a task to call the callback
                let callback = &self.callback;
                let future = callback(subscription_message);
                tokio::spawn(future);
            }
            Err(e) => {
                log::error!("Failed to deserialize subscription message: {}", e);
                log::debug!("Raw message payload: {}", payload_str);
            }
        }
    }

    fn on_stream_error(&self, error: &Error) -> bool {
        log::error!("Subscription stream error: {}", error);
        // Default to closing the stream on error
        true
    }

    fn on_stream_closed(&self) {
        log::debug!("Subscription stream closed");
    }
}

/// A wrapper handler that bridges the connection's StreamResponseHandler trait to our StreamResponseHandler
struct SubscriptionStreamWrapper<H: StreamResponseHandler> {
    operation_id: String,
    handler: std::sync::Arc<H>,
}

impl<H: StreamResponseHandler> SubscriptionStreamWrapper<H> {
    fn new(operation_id: String, handler: std::sync::Arc<H>) -> Self {
        Self {
            operation_id,
            handler,
        }
    }
}

impl<H: StreamResponseHandler> crate::connection::StreamResponseHandler for SubscriptionStreamWrapper<H> {
    fn handle_message(&self, message: crate::event_stream::EventStreamMessage) -> Result<()> {
        // Check if this is a subscription response message by service model type
        if let Some(service_model) = message.get_string_header("service-model-type") {
            if service_model == "aws.greengrass#SubscriptionResponseMessage" {
                log::debug!("Routing subscription message to handler for operation {}", self.operation_id);
                self.handler.on_stream_event(&message);
                return Ok(());
            }
        }
        
        // For other message types, just log
        log::debug!("Received non-subscription message on operation {}: {}", 
                   self.operation_id, String::from_utf8_lossy(&message.payload));
        Ok(())
    }

    fn handle_error(&self, error: &Error) -> Result<bool> {
        Ok(self.handler.on_stream_error(error))
    }

    fn handle_closed(&self) -> Result<()> {
        self.handler.on_stream_closed();
        Ok(())
    }
}
