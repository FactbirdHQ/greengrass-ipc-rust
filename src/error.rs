//! Error handling for the Greengrass IPC library

use thiserror::Error;

/// Result type for Greengrass IPC operations
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in the Greengrass IPC library
#[derive(Error, Debug, Clone)]
pub enum Error {
    /// Failed to connect to the Greengrass Core IPC service
    #[error("Failed to connect to the Greengrass Core IPC service: {0}")]
    ConnectionFailed(String),

    /// Connection attempt timed out
    #[error("Connection attempt timed out")]
    ConnectionTimeout,

    /// Connection was closed
    #[error("Connection was closed: {0}")]
    ConnectionClosed(String),

    /// Failed to send a message
    #[error("Failed to send message: {0}")]
    SendFailed(String),

    /// Failed to receive a message
    #[error("Failed to receive message: {0}")]
    ReceiveFailed(String),

    /// Stream was closed
    #[error("Stream was closed: {0}")]
    StreamClosed(String),

    /// Operation was unauthorized
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    /// Invalid operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Operation not implemented
    #[error("Operation not implemented: {0}")]
    NotImplemented(String),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Authentication error
    #[error("Authentication error: {0}")]
    AuthenticationError(String),

    /// Resource not found
    #[error("Resource not found: {0}")]
    ResourceNotFound(String),

    /// Operation timed out
    #[error("Operation timed out")]
    OperationTimeout,

    /// Operation was cancelled
    #[error("Operation was cancelled")]
    OperationCancelled,

    /// Service error
    #[error("Service error ({0}): {1}")]
    ServiceError(String, String),

    /// Unmapped data in response
    #[error("Unmapped data in response: {0}")]
    UnmappedData(String),

    /// Unknown error
    #[error("Unknown error: {0}")]
    Unknown(String),
    
    /// Invalid input
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    
    /// Protocol error
    #[error("Protocol error: {0}")]
    ProtocolError(String),
}
