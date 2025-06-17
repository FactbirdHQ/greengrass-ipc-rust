//! Error types for AWS IoT Device Shadow operations

use std::fmt;

/// Errors that can occur during shadow operations
#[derive(Debug)]
pub enum ShadowError {
    /// IPC communication error
    IpcError(crate::Error),

    /// JSON serialization/deserialization error
    SerializationError(serde_json::Error),

    /// File I/O error during persistence operations
    IoError(std::io::Error),

    /// Shadow operation was rejected by AWS IoT
    ShadowRejected {
        /// Error code from AWS IoT
        code: u16,
        /// Error message from AWS IoT
        message: String,
    },

    /// Operation timed out waiting for response
    Timeout,

    /// Invalid shadow document format
    InvalidDocument(String),

    /// Shadow not found
    ShadowNotFound,

    /// AWS IoT SDK error
    #[cfg(feature = "shadow-manager")]
    AwsSdkError(aws_sdk_iotdataplane::Error),

    /// Invalid topic format
    InvalidTopic(String),

    /// Subscription failed
    SubscriptionFailed(String),

    /// Client token mismatch
    ClientTokenMismatch { expected: String, received: String },
}

impl fmt::Display for ShadowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShadowError::IpcError(err) => write!(f, "IPC communication error: {}", err),
            ShadowError::SerializationError(err) => write!(f, "Serialization error: {}", err),
            ShadowError::IoError(err) => write!(f, "File I/O error: {}", err),
            ShadowError::ShadowRejected { code, message } => {
                write!(f, "Shadow operation rejected ({}): {}", code, message)
            }
            ShadowError::Timeout => write!(f, "Operation timeout"),
            ShadowError::InvalidDocument(msg) => write!(f, "Invalid shadow document: {}", msg),
            #[cfg(feature = "shadow-manager")]
            ShadowError::AwsSdkError(msg) => write!(f, "AWS SDK error: {}", msg),
            ShadowError::ShadowNotFound => write!(f, "Shadow not found"),
            ShadowError::InvalidTopic(topic) => write!(f, "Invalid topic format: {}", topic),
            ShadowError::SubscriptionFailed(msg) => write!(f, "Subscription failed: {}", msg),
            ShadowError::ClientTokenMismatch { expected, received } => {
                write!(
                    f,
                    "Client token mismatch: expected '{}', received '{}'",
                    expected, received
                )
            }
        }
    }
}

impl std::error::Error for ShadowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ShadowError::IpcError(err) => Some(err),
            ShadowError::SerializationError(err) => Some(err),
            ShadowError::IoError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<crate::Error> for ShadowError {
    fn from(err: crate::Error) -> Self {
        ShadowError::IpcError(err)
    }
}

impl From<serde_json::Error> for ShadowError {
    fn from(err: serde_json::Error) -> Self {
        ShadowError::SerializationError(err)
    }
}

impl From<std::io::Error> for ShadowError {
    fn from(err: std::io::Error) -> Self {
        ShadowError::IoError(err)
    }
}

/// Result type for shadow operations
pub type ShadowResult<T> = Result<T, ShadowError>;
