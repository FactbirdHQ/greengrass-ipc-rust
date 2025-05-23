//! Lifecycle handler for Greengrass IPC connections
//!
//! This module defines the LifecycleHandler trait, which allows applications
//! to handle events that occur during the lifetime of a connection.

use std::fmt;

/// Handler for connection lifecycle events
///
/// Implement this trait to receive notifications about connection events,
/// such as connection establishment, disconnection, and errors.
pub trait LifecycleHandler: Send + Sync + 'static {
    /// Called when a connection is established
    ///
    /// This method is called after a connection is successfully established
    /// with the Greengrass Core IPC service.
    fn on_connect(&self) {
        // Default implementation does nothing
    }

    /// Called when a connection is closed
    ///
    /// This method is called when a connection is closed, either due to
    /// an error or a normal shutdown. If the connection was closed due to
    /// an error, the error will be provided.
    ///
    /// # Arguments
    ///
    /// * `error` - Optional error that caused the disconnection
    fn on_disconnect(&self, _error: Option<&ConnectionError>) {
        // Default implementation does nothing
    }

    /// Called when an error occurs on the connection
    ///
    /// This method is called when an error occurs on the connection, but
    /// the connection is still active.
    ///
    /// # Arguments
    ///
    /// * `error` - The error that occurred
    ///
    /// # Returns
    ///
    /// `true` if the connection should be closed, `false` if the connection
    /// should be kept open.
    fn on_error(&self, _error: &ConnectionError) -> bool {
        // Default implementation closes the connection on any error
        true
    }
}

/// Errors that can occur on a connection
#[derive(Debug)]
pub enum ConnectionError {
    /// An I/O error occurred
    Io(std::io::Error),
    /// The connection was closed by the remote endpoint
    RemoteClosed,
    /// An authentication error occurred
    AuthenticationFailed(String),
    /// A protocol error occurred
    ProtocolError(String),
    /// An unspecified error occurred
    Other(String),
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {}", err),
            Self::RemoteClosed => write!(f, "Connection closed by remote endpoint"),
            Self::AuthenticationFailed(msg) => write!(f, "Authentication failed: {}", msg),
            Self::ProtocolError(msg) => write!(f, "Protocol error: {}", msg),
            Self::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for ConnectionError {}

impl From<std::io::Error> for ConnectionError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

/// A no-op lifecycle handler that does nothing
///
/// This is useful when you don't need to handle lifecycle events.
#[derive(Debug, Default)]
pub struct NoOpLifecycleHandler;

impl LifecycleHandler for NoOpLifecycleHandler {}