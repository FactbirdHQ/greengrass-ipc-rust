//! Connection handling for Greengrass IPC
//!
//! This module handles the low-level connection to the Greengrass IPC service,
//! including Unix domain socket communication and message framing.

use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    unix::{OwnedReadHalf, OwnedWriteHalf},
    UnixStream,
};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

use crate::error::{Error, Result};
use crate::event_stream::{EventStreamMessage, Header};
use crate::lifecycle::{ConnectionError, LifecycleHandler};

/// The environment variable containing the path to the Greengrass IPC socket
pub const ENV_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH: &str =
    "AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT";

/// The environment variable containing the authentication token
pub const ENV_AUTH_TOKEN: &str = "SVCUID";

/// Default timeout for connection operations in seconds
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

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
    // The write half of the Unix socket stream
    write_stream: Arc<Mutex<Option<OwnedWriteHalf>>>,
    // Receiver for control messages to the read loop
    read_control: mpsc::Sender<ControlMessage>,
    // Stream response handlers mapped by operation ID
    response_handlers: Arc<RwLock<HashMap<String, Box<dyn StreamResponseHandler>>>>,
    // Response handlers for operations mapped by stream ID
    operation_response_handlers: Arc<RwLock<HashMap<i32, oneshot::Sender<Result<String>>>>>,
    // Mapping from stream ID to operation ID for subscription handling
    stream_to_operation_map: Arc<RwLock<HashMap<i32, String>>>,
    // Next stream ID to allocate
    next_stream_id: Arc<AtomicI32>,
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
    fn handle_error(&self, error: &Error) -> Result<bool>;

    /// Handle stream closure
    fn handle_closed(&self) -> Result<()>;
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
            write_stream: Arc::new(Mutex::new(None)),
            read_control: read_control_tx,
            response_handlers: Arc::new(RwLock::new(HashMap::new())),
            operation_response_handlers: Arc::new(RwLock::new(HashMap::new())),
            stream_to_operation_map: Arc::new(RwLock::new(HashMap::new())),
            next_stream_id: Arc::new(AtomicI32::new(1)),
        };

        // Begin the connection process
        let timeout_duration = timeout.unwrap_or(DEFAULT_TIMEOUT);

        // Update state to connecting
        {
            let mut state = connection.state.write().await;
            if *state != ConnectionState::Disconnected {
                return Err(Error::InvalidOperation(
                    "Connection already in progress".to_string(),
                ));
            }
            *state = ConnectionState::ConnectingToSocket;
        }

        // Start the connection process
        match connection
            .connect_internal(timeout_duration, read_control_rx)
            .await
        {
            Ok(_) => Ok(connection),
            Err(e) => {
                *connection.state.write().await = ConnectionState::Disconnected;
                Err(e)
            }
        }
    }

    /// Internal connection implementation that handles the connection state machine
    async fn connect_internal(
        &self,
        timeout: Duration,
        read_control_rx: mpsc::Receiver<ControlMessage>,
    ) -> Result<()> {
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
                return Err(Error::ConnectionFailed(e.to_string()));
            }
            Err(_) => {
                return Err(Error::ConnectionTimeout);
            }
        };

        // Socket connected successfully, move to waiting for CONNECT_ACK
        let (read_half, write_half) = {
            let mut state_guard = self.state.write().await;

            // Check if close() was called during connection
            if *state_guard == ConnectionState::Disconnecting {
                debug!("Connection was closed during socket connection, shutting down");
                return Err(Error::ConnectionClosed(
                    "Connection closed during setup".to_string(),
                ));
            }
            *state_guard = ConnectionState::WaitingForConnectAck;

            // Split the stream into read and write halves
            stream.into_split()
        };

        // Store the write half
        {
            let mut write_guard = self.write_stream.lock().await;
            *write_guard = Some(write_half);
        }

        // Create a oneshot channel for receiving the CONNECT_ACK signal
        let (connect_ack_tx, connect_ack_rx) = oneshot::channel::<Result<()>>();

        // Clone the connection so it can be moved into the read loop
        let connection_for_read = self.clone();

        // Start the message read loop in the background with the read half
        let read_task = tokio::task::spawn(async move {
            connection_for_read
                .message_read_loop_with_reader(Some(connect_ack_tx), read_control_rx, read_half)
                .await;
        });

        // Send the CONNECT message with auth token
        debug!("Socket connection established, sending CONNECT message");

        // Create connect message with auth token
        let auth_payload = format!("{{\"authToken\": \"{}\"}}", self.auth_token);

        let mut connect_message = EventStreamMessage::new();
        connect_message = connect_message
            .with_header(crate::event_stream::Header::Version(
                VERSION_STRING.to_string(),
            ))
            .with_header(crate::event_stream::Header::MessageType(4)) // CONNECT = 4
            .with_header(crate::event_stream::Header::MessageFlags(0)) // No flags
            .with_header(crate::event_stream::Header::StreamId(0)) // Protocol messages use stream-id 0
            .with_payload(auth_payload.into_bytes());

        // Send the CONNECT message
        self.send_message(&connect_message).await?;

        // Wait for the connection to complete with timeout
        match tokio::time::timeout(timeout, connect_ack_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::ConnectionClosed(
                "Connect ACK channel was dropped".to_string(),
            )),
            Err(_) => {
                // Connection timed out waiting for CONNECT_ACK
                // Abort the read task
                read_task.abort();

                // Close the connection
                self.close_internal().await?;
                Err(Error::ConnectionTimeout)
            }
        }
    }

    /// Get the socket path used for this connection
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Get the auth token used for this connection
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
            write_stream: self.write_stream.clone(),
            read_control: self.read_control.clone(),
            response_handlers: self.response_handlers.clone(),
            operation_response_handlers: self.operation_response_handlers.clone(),
            stream_to_operation_map: self.stream_to_operation_map.clone(),
            next_stream_id: self.next_stream_id.clone(),
        }
    }

    /// Close the connection
    pub async fn close(&self, reason: Option<Error>) -> Result<()> {
        let state = *self.state.read().await;

        // Only try to close if not already disconnected or disconnecting
        if state != ConnectionState::Disconnected && state != ConnectionState::Disconnecting {
            // Set the state to disconnecting
            *self.state.write().await = ConnectionState::Disconnecting;

            // Close the connection
            self.close_internal().await?;

            // Notify lifecycle handler of disconnection
            if let Some(handler) = &self.lifecycle_handler {
                let conn_error = reason.map(|e| ConnectionError::Other(e.to_string()));
                handler.on_disconnect(conn_error.as_ref());
            }
        }

        Ok(())
    }

    /// Internal connection closing logic
    async fn close_internal(&self) -> Result<()> {
        // Signal the read loop to stop
        let _ = self.read_control.send(ControlMessage::Stop).await;

        // Take the write stream to close it
        let mut write_guard = self.write_stream.lock().await;
        if write_guard.take().is_some() {
            debug!("Closed Unix socket connection");
        }

        // Set state to disconnected
        *self.state.write().await = ConnectionState::Disconnected;

        Ok(())
    }

    /// Send a message over the connection
    pub async fn send_message(&self, message: &EventStreamMessage) -> Result<()> {
        let state = *self.state.read().await;
        debug!("Sending message in state: {:?}", state);

        if state != ConnectionState::Connected && state != ConnectionState::WaitingForConnectAck {
            error!(
                "Cannot send message - connection not established, state: {:?}",
                state
            );
            return Err(Error::ConnectionClosed(
                "Connection is not established".to_string(),
            ));
        }

        let mut write_guard = self.write_stream.lock().await;
        if let Some(write_stream) = &mut *write_guard {
            let encoded = message.encode()?;
            debug!("Encoded message size: {} bytes", encoded.len());

            write_stream.write_all(&encoded).await.map_err(|e| {
                error!("Failed to write message: {}", e);
                Error::SendFailed(e.to_string())
            })?;

            write_stream.flush().await.map_err(|e| {
                error!("Failed to flush stream: {}", e);
                Error::SendFailed(e.to_string())
            })?;

            debug!("Message sent successfully");
            Ok(())
        } else {
            error!("No active write stream available");
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

        // Also remove from stream mapping
        let mut mapping = self.stream_to_operation_map.write().await;
        mapping.retain(|_, op_id| op_id != operation_id);

        Ok(())
    }

    /// Send a TERMINATE_STREAM message for an operation
    pub async fn send_terminate_stream_message(&self, operation_id: &str) -> Result<()> {
        // Find the stream ID for this operation
        let stream_id = {
            let mapping = self.stream_to_operation_map.read().await;
            mapping
                .iter()
                .find(|(_, op_id)| *op_id == operation_id)
                .map(|(stream_id, _)| *stream_id)
        };

        if let Some(stream_id) = stream_id {
            // Create a TERMINATE_STREAM message
            let mut terminate_message = crate::event_stream::EventStreamMessage::new();
            terminate_message = terminate_message
                .with_header(crate::event_stream::Header::MessageType(0)) // APPLICATION_MESSAGE = 0
                .with_header(crate::event_stream::Header::StreamId(stream_id))
                .with_header(crate::event_stream::Header::MessageFlags(2)) // TERMINATE_STREAM = 2
                .with_header(crate::event_stream::Header::ContentType(
                    "application/json".to_string(),
                ));

            // Try to send the terminate message, but don't fail if it doesn't work
            // (stream might already be closed by the server)
            if let Err(e) = self.send_message(&terminate_message).await {
                log::debug!(
                    "Failed to send TERMINATE_STREAM message for operation {}: {}",
                    operation_id,
                    e
                );
            } else {
                log::debug!(
                    "Sent TERMINATE_STREAM message for operation {}",
                    operation_id
                );
            }
        } else {
            log::debug!(
                "No stream ID found for operation {}, cannot send TERMINATE_STREAM",
                operation_id
            );
        }

        Ok(())
    }

    /// Main message read loop that processes incoming messages with dedicated read half
    async fn message_read_loop_with_reader(
        &self,
        mut connect_ack_tx: Option<oneshot::Sender<Result<()>>>,
        mut control_rx: mpsc::Receiver<ControlMessage>,
        mut read_stream: OwnedReadHalf,
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
            let bytes_read = match read_stream.read(&mut buffer).await {
                Ok(0) => {
                    // EOF
                    debug!("Socket connection closed by remote");

                    if let Some(tx) = connect_ack_tx.take() {
                        let _ = tx.send(Err(Error::ConnectionClosed(
                            "Connection closed by remote".to_string(),
                        )));
                    }

                    // Call handle_disconnection to notify handlers
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

                    if let Some(tx) = connect_ack_tx.take() {
                        let _ = tx.send(Err(Error::ReceiveFailed(e.to_string())));
                    }

                    // Call handle_disconnection to notify handlers
                    self.handle_disconnection(Error::ReceiveFailed(e.to_string()))
                        .await;

                    break;
                }
            };

            // Process the received data
            reader.add_data(&buffer[..bytes_read]);

            // Try to extract messages
            loop {
                match reader.try_read_message() {
                    Ok(Some(message)) => {
                        let is_connect_ack = matches!(
                            message.get_header(":message-type"),
                            Some(crate::event_stream::Header::MessageType(5))
                        );

                        if is_connect_ack {
                            if let Some(tx) = connect_ack_tx.take() {
                                if let Err(e) = self.handle_connect_ack(message, tx).await {
                                    error!("Error handling CONNECT_ACK: {}", e);
                                    break 'outer;
                                }
                            }
                        } else {
                            if let Err(e) = self.handle_non_connect_message(message).await {
                                error!("Error handling message: {}", e);
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

                        if let Some(tx) = connect_ack_tx.take() {
                            let _ = tx.send(Err(Error::ProtocolError(e.to_string())));
                        }

                        // Call handle_disconnection to notify handlers
                        self.handle_disconnection(Error::ProtocolError(e.to_string()))
                            .await;

                        break 'outer;
                    }
                }
            }
        }

        debug!("Message read loop exiting");
    }

    /// Handle a non-connection message
    async fn handle_non_connect_message(&self, message: EventStreamMessage) -> Result<()> {
        trace!("Received message with {} headers", message.headers.len());

        // Debug: print all headers
        for header in &message.headers {
            trace!("Header: {} = {:?}", header.name(), header);
        }

        // Check message type
        match message.get_header(":message-type") {
            Some(crate::event_stream::Header::MessageType(msg_type)) => {
                trace!("Received message type: {}", msg_type);
                match *msg_type {
                    0 => {
                        // APPLICATION_MESSAGE - could be a response to an operation
                        self.handle_application_message(message).await?;
                    }
                    1 => {
                        // APPLICATION_ERROR - could be a response to an operation
                        self.handle_application_message(message).await?;
                    }
                    2 => {
                        // PING
                        self.handle_ping(message).await?;
                    }
                    3 => {
                        // PING_RESPONSE (PONG)
                        // Nothing to do for pong
                    }
                    4 => {
                        // CONNECT - shouldn't receive this as a client
                        warn!("Received CONNECT message as client");
                    }
                    5 => {
                        // CONNECT_ACK - shouldn't reach here, handled in connect flow
                        warn!("Received CONNECT_ACK outside of connection flow");
                    }
                    6 => {
                        // PROTOCOL_ERROR
                        error!("Received PROTOCOL_ERROR from server");
                        if let Some(error_code) = message
                            .get_header(":error-code")
                            .and_then(Header::string_value)
                        {
                            error!("Protocol error code: {}", error_code);
                        }
                        if let Some(error_message) = message
                            .get_header(":error-message")
                            .and_then(Header::string_value)
                        {
                            error!("Protocol error message: {}", error_message);
                        }

                        // Log the payload if present
                        if !message.payload.is_empty() {
                            error!(
                                "Protocol error payload: {}",
                                String::from_utf8_lossy(&message.payload)
                            );
                        }

                        return Err(Error::ProtocolError(format!(
                            "Server sent protocol error: {:?}",
                            message.headers
                        )));
                    }
                    7 => {
                        // INTERNAL_ERROR
                        error!("Received INTERNAL_ERROR from server");
                        return Err(Error::ProtocolError("Server internal error".to_string()));
                    }
                    _ => {
                        warn!("Received unknown integer message type: {}", msg_type);
                    }
                }
            }
            _ => {
                warn!("Received message with no or invalid message-type header");
                warn!(
                    "Available headers: {:?}",
                    message.headers.iter().map(|h| h.name()).collect::<Vec<_>>()
                );
            }
        }

        Ok(())
    }

    /// Allocate a unique stream ID for a new operation
    pub async fn allocate_stream_id(&self) -> Result<i32> {
        Ok(self.next_stream_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Atomically allocate a stream ID, register a response handler, and send
    /// a message.
    ///
    /// The message must NOT already contain a `:stream-id` header — one will be
    /// added under the write lock so that IDs hit the wire in allocation order.
    ///
    /// The response handler is registered under the same lock, **before** the
    /// bytes hit the wire, so the read loop can never see a response for a
    /// stream ID that has no handler yet.
    ///
    /// Returns the allocated stream ID.
    pub async fn send_operation_message(
        &self,
        message: EventStreamMessage,
        response_handler: Option<oneshot::Sender<Result<String>>>,
    ) -> Result<i32> {
        let state = *self.state.read().await;
        if state != ConnectionState::Connected && state != ConnectionState::WaitingForConnectAck {
            return Err(Error::ConnectionClosed(
                "Connection is not established".to_string(),
            ));
        }

        let mut write_guard = self.write_stream.lock().await;

        // Allocate under the write lock so IDs are ordered on the wire
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::SeqCst);
        let message = message.with_header(Header::StreamId(stream_id));

        // Register handler before writing so the read loop can never see a
        // response for this stream ID without a matching handler.
        if let Some(sender) = response_handler {
            let mut handlers = self.operation_response_handlers.write().await;
            handlers.insert(stream_id, sender);
        }

        if let Some(write_stream) = &mut *write_guard {
            let encoded = message.encode()?;
            debug!(
                "Encoded operation message size: {} bytes (stream {})",
                encoded.len(),
                stream_id
            );

            write_stream.write_all(&encoded).await.map_err(|e| {
                error!("Failed to write message: {}", e);
                Error::SendFailed(e.to_string())
            })?;

            write_stream.flush().await.map_err(|e| {
                error!("Failed to flush stream: {}", e);
                Error::SendFailed(e.to_string())
            })?;

            debug!("Operation message sent on stream {}", stream_id);
            Ok(stream_id)
        } else {
            error!("No active write stream available");
            Err(Error::ConnectionClosed("No active stream".to_string()))
        }
    }

    /// Register a response handler for an operation
    pub async fn register_response_handler(
        &self,
        stream_id: i32,
        sender: oneshot::Sender<Result<String>>,
    ) -> Result<()> {
        let mut handlers = self.operation_response_handlers.write().await;
        handlers.insert(stream_id, sender);
        Ok(())
    }

    /// Register a mapping from stream ID to operation ID for subscription handling
    pub async fn register_stream_operation_mapping(
        &self,
        stream_id: i32,
        operation_id: String,
    ) -> Result<()> {
        let mut mapping = self.stream_to_operation_map.write().await;
        mapping.insert(stream_id, operation_id);
        Ok(())
    }

    /// Handle an APPLICATION_MESSAGE (could be response to operation)
    async fn handle_application_message(&self, message: EventStreamMessage) -> Result<()> {
        // Get the stream ID from the message
        if let Some(crate::event_stream::Header::StreamId(stream_id)) =
            message.get_header(":stream-id")
        {
            // Check if this is a response to one of our operations
            let mut handlers = self.operation_response_handlers.write().await;
            if let Some(sender) = handlers.remove(stream_id) {
                // This is a response to an operation
                let payload_str = String::from_utf8_lossy(&message.payload).to_string();
                let _ = sender.send(Ok(payload_str));
                return Ok(());
            }
        }

        // If not an operation response, treat as stream event
        self.handle_stream_event(message).await
    }

    /// Handle a CONNECT_ACK message
    async fn handle_connect_ack(
        &self,
        message: EventStreamMessage,
        connect_ack_tx: oneshot::Sender<Result<()>>,
    ) -> Result<()> {
        let current_state = *self.state.read().await;

        if current_state != ConnectionState::WaitingForConnectAck {
            warn!("Received CONNECT_ACK in invalid state: {:?}", current_state);
            // Complete the channel anyway to avoid hanging
            let _ = connect_ack_tx.send(Err(Error::InvalidOperation(format!(
                "Received CONNECT_ACK in invalid state: {:?}",
                current_state
            ))));
            return Ok(());
        }

        // Debug log all headers and payload in CONNECT_ACK
        debug!("CONNECT_ACK message headers: {:?}", message.headers);
        debug!(
            "CONNECT_ACK message payload: {}",
            String::from_utf8_lossy(&message.payload)
        );

        // Check if connection was accepted by looking at message flags
        // CONNECTION_ACCEPTED flag = 0x01
        if let Some(crate::event_stream::Header::MessageFlags(flags)) =
            message.get_header(":message-flags")
        {
            debug!("CONNECT_ACK flags: 0x{:x}", flags);
            if (flags & 0x01) == 0 {
                let err = Error::AuthenticationError("Connection access denied".to_string());

                // Complete the channel with an error
                let _ = connect_ack_tx.send(Err(err.clone()));

                return Err(err);
            }
        } else {
            // No flags header found, assume connection denied
            let err = Error::AuthenticationError("No connection flags in CONNECT_ACK".to_string());
            let _ = connect_ack_tx.send(Err(err.clone()));
            return Err(err);
        }

        debug!("CONNECT_ACK received, connection established");

        // Update state to connected
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Connected;
        }

        // Signal successful connection via the oneshot channel
        let _ = connect_ack_tx.send(Ok(()));

        // Notify lifecycle handler
        if let Some(handler) = &self.lifecycle_handler {
            handler.on_connect();
        }

        Ok(())
    }

    /// Handle a stream event message
    async fn handle_stream_event(&self, message: EventStreamMessage) -> Result<()> {
        // Try to extract operation ID directly first
        let operation_id = if let Some(id) = message
            .get_header(":operation-id")
            .and_then(Header::string_value)
        {
            id.to_string()
        } else if let Some(crate::event_stream::Header::StreamId(stream_id)) =
            message.get_header(":stream-id")
        {
            // If no operation ID, try to map from stream ID (for subscriptions)
            let mapping = self.stream_to_operation_map.read().await;
            match mapping.get(stream_id) {
                Some(id) => id.clone(),
                None => {
                    warn!("Received stream event with no operation ID and no stream mapping for stream {}", stream_id);
                    return Ok(());
                }
            }
        } else {
            warn!("Received stream event with no operation ID or stream ID");
            return Ok(());
        };

        // Find the appropriate handler
        let handlers = self.response_handlers.read().await;
        if let Some(handler) = handlers.get(&operation_id) {
            handler.handle_message(message)?;
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
            crate::event_stream::Header::MessageType(5), // Use numeric PONG type
        );

        self.send_message(&pong).await?;

        // Notify lifecycle handler
        if let Some(_handler) = &self.lifecycle_handler {
            // Convert to ConnectionError type expected by the lifecycle handler
            let debug_headers: Vec<String> = message
                .headers
                .iter()
                .map(|header| format!("{}: {:?}", header.name(), header))
                .collect();

            // Call ping handler with headers and payload
            // The LifecycleHandler doesn't have an on_ping method in this implementation
            // So we just log it
            debug!("Received ping message with {} headers", debug_headers.len());
        }

        Ok(())
    }

    /// Handle disconnection - notifies stream handlers and the lifecycle handler
    async fn handle_disconnection(&self, reason: Error) {
        debug!("Handling disconnection: {}", reason);

        // Update state to disconnected if not already
        {
            let mut state = self.state.write().await;
            if *state == ConnectionState::Disconnected {
                return;
            }
            *state = ConnectionState::Disconnected;
        }

        // Notify all stream handlers
        {
            let handlers = self.response_handlers.read().await;
            for (_, handler) in handlers.iter() {
                if let Err(e) = handler.handle_closed() {
                    warn!("Error handling stream closure: {}", e);
                }
            }
        }

        // Notify lifecycle handler
        if let Some(handler) = &self.lifecycle_handler {
            let conn_error = ConnectionError::Other(reason.to_string());
            handler.on_disconnect(Some(&conn_error));
        }
    }
}
