//! Connection handling for Greengrass IPC
//!
//! This module handles the low-level connection to the Greengrass IPC service,
//! including Unix domain socket communication and message framing.

use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::error::{Error, Result};
use crate::event_stream::{
    EventStreamMessage, HeaderValue, MESSAGE_TYPE_CONNECT, MESSAGE_TYPE_CONNECT_ACK,
};
use crate::lifecycle::{ConnectionError, LifecycleHandler};

/// The environment variable containing the path to the Greengrass IPC socket
pub const ENV_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH: &str =
    "AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT";

/// The environment variable containing the authentication token
pub const ENV_AUTH_TOKEN: &str = "SVCUID";

/// Default timeout for connection operations in seconds
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Version header name
pub const VERSION_HEADER: &str = ":version";

/// Content type header name
pub const CONTENT_TYPE_HEADER: &str = ":content-type";

/// Content type for application JSON
pub const CONTENT_TYPE_APPLICATION_JSON: &str = "application/json";

/// Service model type header
pub const SERVICE_MODEL_TYPE_HEADER: &str = "service-model-type";

/// Current SDK version
pub const VERSION_STRING: &str = "0.1.0";

/// Connection state for the state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    /// Connection is not active
    Disconnected,
    /// Connection is in the process of connecting to the socket
    ConnectingToSocket,
    /// Connection is waiting for a CONNECT_ACK from the server
    WaitingForConnectAck,
    /// Connection is fully established and ready for use
    Connected,
    /// Connection is in the process of disconnecting
    Disconnecting,
}

/// A connection to the Greengrass IPC service
pub struct Connection {
    // Socket path used for this connection
    socket_path: PathBuf,
    // Authentication token
    auth_token: String,
    // The lifecycle handler for this connection
    lifecycle_handler: Option<Arc<dyn LifecycleHandler>>,
    // Current connection state
    state: Arc<RwLock<ConnectionState>>,
    // The underlying Unix socket stream
    stream: Arc<RwLock<Option<UnixStream>>>,
    // Connect future for tracking connection progress
    connect_future: Arc<RwLock<Option<oneshot::Sender<Result<()>>>>>,
    // The reason for closure if the connection is closing
    close_reason: Arc<RwLock<Option<Error>>>,
    // Closed future for tracking when the connection is fully closed
    closed_future: Arc<RwLock<Option<oneshot::Sender<Result<()>>>>>,
    // Receiver for control messages to the read loop
    read_control: Arc<mpsc::Sender<ControlMessage>>,
    // Stream response handlers mapped by operation ID
    response_handlers: Arc<RwLock<HashMap<String, Box<dyn StreamResponseHandler>>>>,
}

/// Control messages for the read loop
enum ControlMessage {
    /// Stop the read loop
    Stop,
}

/// Handler for stream responses
pub trait StreamResponseHandler: Send + Sync + 'static {
    /// Handle a message received on the stream
    fn handle_message(&self, message: EventStreamMessage) -> Result<()>;

    /// Handle an error on the stream
    fn handle_error(&self, error: &Error) -> bool;

    /// Handle stream closure
    fn handle_closed(&self);
}

impl Connection {
    /// Connect to the Greengrass IPC service
    ///
    /// # Arguments
    ///
    /// * `socket_path` - Optional path to the Unix domain socket of the Greengrass nucleus.
    /// * `auth_token` - Optional authentication token.
    /// * `lifecycle_handler` - Optional handler for connection lifecycle events.
    /// * `timeout` - Optional timeout for the connection attempt in seconds.
    ///
    pub async fn connect(
        socket_path: Option<PathBuf>,
        auth_token: Option<String>,
        lifecycle_handler: Option<Box<dyn LifecycleHandler>>,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        // Resolve socket path from argument or environment
        let socket_path = socket_path.unwrap_or_else(|| {
            std::env::var(ENV_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH)
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    // Default path if environment variable is not set
                    // In a real implementation, this should probably return an error
                    PathBuf::from("/tmp/greengrass-ipc.sock")
                })
        });

        // Resolve auth token from argument or environment
        let auth_token = auth_token.unwrap_or_else(|| {
            std::env::var(ENV_AUTH_TOKEN).unwrap_or_else(|_| {
                // Default token if environment variable is not set
                // In a real implementation, this should probably return an error
                String::from("default-auth-token")
            })
        });

        // Create channel for read loop control
        let (read_control_tx, read_control_rx) = mpsc::channel::<ControlMessage>(10);

        // Create a connection with initial state
        let connection = Self {
            socket_path,
            auth_token,
            lifecycle_handler: lifecycle_handler.map(Arc::from),
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            stream: Arc::new(RwLock::new(None)),
            connect_future: Arc::new(RwLock::new(None)),
            close_reason: Arc::new(RwLock::new(None)),
            closed_future: Arc::new(RwLock::new(None)),
            read_control: Arc::new(read_control_tx),
            response_handlers: Arc::new(RwLock::new(HashMap::new())),
        };

        // Begin the connection process
        let result = connection
            .connect_internal(timeout.unwrap_or(DEFAULT_TIMEOUT), read_control_rx)
            .await;

        if result.is_err() {
            // Update state to disconnected if connection failed
            *connection.state.write().await = ConnectionState::Disconnected;
        }

        // Return the connection object or the error
        match result {
            Ok(_) => Ok(connection),
            Err(e) => Err(e),
        }
    }

    /// Internal connection implementation that handles the connection state machine
    async fn connect_internal(
        &self,
        timeout: Duration,
        read_control_rx: mpsc::Receiver<ControlMessage>,
    ) -> Result<()> {
        // Create the oneshot channel for the connect future
        let (connect_tx, connect_rx) = oneshot::channel::<Result<()>>();
        let (closed_tx, _closed_rx) = oneshot::channel::<Result<()>>();

        // Update state to connecting
        {
            let mut state = self.state.write().await;
            let mut connect_future = self.connect_future.write().await;
            let mut closed_future = self.closed_future.write().await;

            // Cannot connect if already connecting or connected
            if *state != ConnectionState::Disconnected {
                return Err(Error::InvalidOperation(
                    "Connection already in progress".to_string(),
                ));
            }

            *state = ConnectionState::ConnectingToSocket;
            *connect_future = Some(connect_tx);
            *closed_future = Some(closed_tx);
        }

        // Connect to the Unix socket with timeout
        info!(
            "Connecting to Unix socket at {}",
            self.socket_path.display()
        );

        let stream_result =
            tokio::time::timeout(timeout, UnixStream::connect(&self.socket_path)).await;

        // Handle connection errors
        let stream = match stream_result {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                let error = Error::ConnectionFailed(e.to_string());
                self.handle_connection_failure(error).await;
                return Err(Error::ConnectionFailed(e.to_string()));
            }
            Err(_) => {
                let error = Error::ConnectionTimeout;
                self.handle_connection_failure(error).await;
                return Err(Error::ConnectionTimeout);
            }
        };

        // Socket connected successfully, move to waiting for CONNECT_ACK
        {
            let mut state_guard = self.state.write().await;
            let mut stream_guard = self.stream.write().await;

            // Check if close() was called during connection
            if *state_guard == ConnectionState::Disconnecting {
                debug!("Connection was closed during socket connection, shutting down");
                return Err(Error::ConnectionClosed(
                    "Connection closed during setup".to_string(),
                ));
            }

            *state_guard = ConnectionState::WaitingForConnectAck;
            *stream_guard = Some(stream);
        }

        // Send the CONNECT message with auth token
        debug!("Socket connection established, sending CONNECT message");

        // Create connect message with auth token
        let auth_payload = format!("{{\"authToken\": \"{}\"}}", self.auth_token);

        let mut connect_message = EventStreamMessage::new();
        connect_message = connect_message
            .with_header(
                VERSION_HEADER.to_string(),
                HeaderValue::String(VERSION_STRING.to_string()),
            )
            .with_header(
                ":message-type".to_string(),
                HeaderValue::String(MESSAGE_TYPE_CONNECT.to_string()),
            )
            .with_payload(auth_payload.into_bytes());

        // Set up read loop that will handle messages including the CONNECT_ACK
        let read_state = self.clone();
        let read_stream = self.stream.clone();

        tokio::spawn(async move {
            read_state
                .message_read_loop(read_stream, read_control_rx)
                .await;
        });

        // Send the CONNECT message
        self.send_message(&connect_message).await?;

        // Wait for the connection to complete with timeout
        match tokio::time::timeout(timeout, connect_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::ConnectionClosed(
                "Connect future was dropped".to_string(),
            )),
            Err(_) => {
                // Connection timed out waiting for CONNECT_ACK
                self.close(Some(Error::ConnectionTimeout)).await?;
                Err(Error::ConnectionTimeout)
            }
        }
    }

    /// Handle connection failure before the socket is connected
    async fn handle_connection_failure(&self, error: Error) {
        let mut state = self.state.write().await;
        let mut connect_future = self.connect_future.write().await;

        if let Some(sender) = connect_future.take() {
            // Ignore if receiver was dropped
            let _ = sender.send(Err(error.clone()));
        }

        *state = ConnectionState::Disconnected;
    }

    /// Get the socket path used for this connection
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Get the authentication token used for this connection
    pub fn auth_token(&self) -> &str {
        &self.auth_token
    }

    /// Clone this connection for use in other parts of the code
    pub fn clone(&self) -> Self {
        Self {
            socket_path: self.socket_path.clone(),
            auth_token: self.auth_token.clone(),
            lifecycle_handler: self.lifecycle_handler.clone(),
            state: self.state.clone(),
            stream: self.stream.clone(),
            connect_future: self.connect_future.clone(),
            close_reason: self.close_reason.clone(),
            closed_future: self.closed_future.clone(),
            read_control: self.read_control.clone(),
            response_handlers: self.response_handlers.clone(),
        }
    }

    /// Close the connection
    pub async fn close(&self, reason: Option<Error>) -> Result<oneshot::Receiver<Result<()>>> {
        let mut result = None;

        {
            let mut state = self.state.write().await;
            let mut close_reason = self.close_reason.write().await;

            // Only try to close if not already disconnected or disconnecting
            if *state != ConnectionState::Disconnected && *state != ConnectionState::Disconnecting {
                // Set the close reason if one was provided
                if let Some(err) = reason {
                    *close_reason = Some(err);
                }

                *state = ConnectionState::Disconnecting;

                // If we have a stream, close it
                let mut stream_guard = self.stream.write().await;
                if let Some(stream) = stream_guard.take() {
                    debug!("Closing Unix socket connection");

                    // Create a new closed future if needed
                    let mut closed_future = self.closed_future.write().await;
                    let (tx, rx) = oneshot::channel();

                    *closed_future = Some(tx);
                    result = Some(rx);

                    // Signal the read loop to stop
                    let _ = self.read_control.send(ControlMessage::Stop).await;

                    // Close the socket
                    drop(stream);
                }
            }
        }

        // If we already had a closed future, return it
        if result.is_none() {
            let (tx, rx) = oneshot::channel();
            tx.send(Ok(())).unwrap();
            result = Some(rx);
        }

        Ok(result.unwrap())
    }

    /// Send a message over the connection
    pub async fn send_message(&self, message: &EventStreamMessage) -> Result<()> {
        let state = *self.state.read().await;

        if state != ConnectionState::Connected && state != ConnectionState::WaitingForConnectAck {
            return Err(Error::ConnectionClosed(
                "Connection is not established".to_string(),
            ));
        }

        let mut stream_guard = self.stream.write().await;
        if let Some(stream) = &mut *stream_guard {
            let encoded = message.encode()?;

            stream
                .write_all(&encoded)
                .await
                .map_err(|e| Error::SendFailed(e.to_string()))?;

            Ok(())
        } else {
            Err(Error::ConnectionClosed("No active stream".to_string()))
        }
    }

    /// Register a handler for stream responses
    pub async fn register_stream_handler(
        &self,
        operation_id: String,
        handler: Box<dyn StreamResponseHandler>,
    ) -> Result<()> {
        let mut handlers = self.response_handlers.write().await;
        handlers.insert(operation_id, handler);
        Ok(())
    }

    /// Unregister a stream handler
    pub async fn unregister_stream_handler(&self, operation_id: &str) -> Result<()> {
        let mut handlers = self.response_handlers.write().await;
        handlers.remove(operation_id);
        Ok(())
    }

    /// Main message read loop that processes incoming messages
    async fn message_read_loop(
        &self,
        stream: Arc<RwLock<Option<UnixStream>>>,
        mut control_rx: mpsc::Receiver<ControlMessage>,
    ) {
        let mut buffer = vec![0u8; 1024 * 64]; // 64KB buffer for reading
        let mut reader = crate::event_stream::EventStreamReader::new();

        'outer: loop {
            // Check for control messages
            if let Ok(msg) = control_rx.try_recv() {
                match msg {
                    ControlMessage::Stop => {
                        debug!("Message read loop received stop signal");
                        break;
                    }
                }
            }

            // Read from the stream
            let bytes_read = {
                let mut stream_guard = stream.write().await;
                if let Some(stream) = &mut *stream_guard {
                    match stream.read(&mut buffer).await {
                        Ok(0) => {
                            // EOF
                            debug!("Socket connection closed by remote");
                            self.handle_disconnection(Error::ConnectionClosed(
                                "Connection closed by remote".to_string(),
                            ))
                            .await;
                            break;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            // Read error
                            error!("Error reading from socket: {}", e);
                            self.handle_disconnection(Error::ReceiveFailed(e.to_string()))
                                .await;
                            break;
                        }
                    }
                } else {
                    // No stream, exit the loop
                    break;
                }
            };

            // Process the received data
            reader.add_data(&buffer[..bytes_read]);

            // Try to extract messages
            loop {
                match reader.try_read_message() {
                    Ok(Some(message)) => {
                        // Handle the message
                        if let Err(e) = self.handle_message(message).await {
                            error!("Error handling message: {}", e);

                            // If handling the CONNECT_ACK fails, we need to disconnect
                            if *self.state.read().await == ConnectionState::WaitingForConnectAck {
                                self.handle_disconnection(e).await;
                                break 'outer;
                            }
                        }
                    }
                    Ok(None) => {
                        // Need more data
                        break;
                    }
                    Err(e) => {
                        // Protocol error
                        error!("Protocol error decoding message: {}", e);
                        self.handle_disconnection(Error::ProtocolError(e.to_string()))
                            .await;
                        break 'outer;
                    }
                }
            }
        }

        debug!("Message read loop exiting");
    }

    /// Handle a received message
    async fn handle_message(&self, message: EventStreamMessage) -> Result<()> {
        trace!("Received message with {} headers", message.headers.len());

        // Check message type
        let message_type = match message.get_string_header(":message-type") {
            Some(msg_type) => msg_type,
            None => {
                warn!("Received message with no message-type header");
                return Ok(());
            }
        };

        if message_type == MESSAGE_TYPE_CONNECT_ACK {
            self.handle_connect_ack(message).await?;
        } else if message_type == "StreamEvent" {
            self.handle_stream_event(message).await?;
        } else if message_type == "Response" {
            self.handle_response(message).await?;
        } else if message_type == "Error" {
            self.handle_error(message).await?;
        } else if message_type == "Ping" {
            self.handle_ping(message).await?;
        } else if message_type == "Pong" {
            // Nothing to do for pong
        } else {
            warn!("Received unknown message type: {}", message_type);
        }

        Ok(())
    }

    /// Handle a CONNECT_ACK message
    async fn handle_connect_ack(&self, message: EventStreamMessage) -> Result<()> {
        let current_state = *self.state.read().await;

        if current_state != ConnectionState::WaitingForConnectAck {
            warn!("Received CONNECT_ACK in invalid state: {:?}", current_state);
            return Ok(());
        }

        // Check if connection was accepted
        if let Some(HeaderValue::String(flags)) = message.get_header(":connection-accept") {
            if flags != "true" {
                return Err(Error::AuthenticationError(
                    "Connection access denied".to_string(),
                ));
            }
        }

        debug!("CONNECT_ACK received, connection established");

        // Update state to connected
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Connected;
        }

        // Complete the connect future
        let mut connect_future = self.connect_future.write().await;
        if let Some(sender) = connect_future.take() {
            let _ = sender.send(Ok(()));
        }

        // Notify lifecycle handler
        if let Some(handler) = &self.lifecycle_handler {
            handler.on_connect();
        }

        Ok(())
    }

    /// Handle a stream event message
    async fn handle_stream_event(&self, message: EventStreamMessage) -> Result<()> {
        // Extract operation ID
        let operation_id = match message.get_string_header(":operation-id") {
            Some(id) => id,
            None => {
                warn!("Received stream event with no operation ID");
                return Ok(());
            }
        };

        // Find the appropriate handler
        let handlers = self.response_handlers.read().await;
        if let Some(handler) = handlers.get(operation_id) {
            handler.handle_message(message)?;
        } else {
            warn!("No handler found for operation ID: {}", operation_id);
        }

        Ok(())
    }

    /// Handle a response message
    async fn handle_response(&self, message: EventStreamMessage) -> Result<()> {
        // Extract operation ID
        let operation_id = match message.get_string_header(":operation-id") {
            Some(id) => id,
            None => {
                warn!("Received response with no operation ID");
                return Ok(());
            }
        };

        // Find the appropriate handler
        let handlers = self.response_handlers.read().await;
        if let Some(handler) = handlers.get(operation_id) {
            handler.handle_message(message)?;
        } else {
            warn!("No handler found for operation ID: {}", operation_id);
        }

        Ok(())
    }

    /// Handle an error message
    async fn handle_error(&self, message: EventStreamMessage) -> Result<()> {
        // Extract operation ID
        let operation_id = match message.get_string_header(":operation-id") {
            Some(id) => id,
            None => {
                warn!("Received error with no operation ID");
                return Ok(());
            }
        };

        // Find the appropriate handler
        let error = Error::ServiceError(
            message
                .get_string_header(":error-type")
                .unwrap_or("Unknown")
                .to_string(),
            message
                .get_string_header(":error-message")
                .unwrap_or("No error message")
                .to_string(),
        );

        let handlers = self.response_handlers.read().await;
        if let Some(handler) = handlers.get(operation_id) {
            // If handler returns true, it wants to close the operation
            if handler.handle_error(&error) {
                // Operation will be closed by the client
            }
        } else {
            warn!("No handler found for operation ID: {}", operation_id);
        }

        Ok(())
    }

    /// Handle a ping message
    async fn handle_ping(&self, message: EventStreamMessage) -> Result<()> {
        // Respond with a pong
        let mut pong = EventStreamMessage::new();
        pong = pong.with_header(
            ":message-type".to_string(),
            HeaderValue::String("Pong".to_string()),
        );

        self.send_message(&pong).await?;

        // Notify lifecycle handler
        if let Some(_handler) = &self.lifecycle_handler {
            // Convert to ConnectionError type expected by the lifecycle handler
            let headers_vec: Vec<(String, String)> = message
                .headers
                .iter()
                .filter_map(|(k, v)| {
                    if let HeaderValue::String(s) = v {
                        Some((k.clone(), s.clone()))
                    } else {
                        None
                    }
                })
                .collect();

            // Call ping handler with headers and payload
            // The LifecycleHandler doesn't have an on_ping method in this implementation
            // So we just log it
            debug!("Received ping message with {} headers", headers_vec.len());
        }

        Ok(())
    }

    /// Handle disconnection
    async fn handle_disconnection(&self, reason: Error) {
        debug!("Handling disconnection: {}", reason);

        // Update state
        {
            let mut state = self.state.write().await;

            // Only process if we're not already disconnected
            if *state == ConnectionState::Disconnected {
                return;
            }

            *state = ConnectionState::Disconnected;
        }

        // Complete the connect future if it exists
        {
            let mut connect_future = self.connect_future.write().await;
            if let Some(sender) = connect_future.take() {
                let _ = sender.send(Err(reason.clone()));
            }
        }

        // Complete the closed future
        {
            let mut closed_future = self.closed_future.write().await;
            if let Some(sender) = closed_future.take() {
                let _ = sender.send(Err(reason.clone()));
            }
        }

        // Notify all stream handlers
        {
            let handlers = self.response_handlers.read().await;
            for (_, handler) in handlers.iter() {
                handler.handle_closed();
            }
        }

        // Notify lifecycle handler
        if let Some(handler) = &self.lifecycle_handler {
            handler.on_disconnect(Some(&ConnectionError::Other(reason.to_string())));
        }
    }
}
