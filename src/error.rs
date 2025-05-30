//! Error handling for the Greengrass IPC library

use serde::Deserialize;
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

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Invalid operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Authentication error
    #[error("Authentication error: {0}")]
    AuthenticationError(String),

    /// Operation timed out
    #[error("Operation timed out")]
    OperationTimeout,

    /// Invalid arguments provided to the operation
    #[error("Invalid arguments: {0}")]
    InvalidArgumentsError(String),

    /// Resource not found
    #[error("Resource not found: {message}. {resource_type:?}. {resource_name:?}")]
    ResourceNotFoundError {
        message: String,
        resource_type: Option<String>,
        resource_name: Option<String>,
    },

    /// Unauthorized access
    #[error("Unauthorized: {0}")]
    UnauthorizedError(String),

    /// Component not found
    #[error("Component not found: {0}")]
    ComponentNotFoundError(String),

    /// Invalid token
    #[error("Invalid token: {0}")]
    InvalidTokenError(String),

    /// Failed update condition check
    #[error("Failed update condition check: {0}")]
    FailedUpdateConditionCheckError(String),

    /// Conflict error
    #[error("Conflict: {0}")]
    ConflictError(String),

    /// Invalid client device auth token
    #[error("Invalid client device auth token: {0}")]
    InvalidClientDeviceAuthTokenError(String),

    /// Invalid credential
    #[error("Invalid credential: {0}")]
    InvalidCredentialError(String),

    /// Invalid artifacts directory path
    #[error("Invalid artifacts directory path: {0}")]
    InvalidArtifactsDirectoryPathError(String),

    /// Invalid recipe directory path
    #[error("Invalid recipe directory path: {0}")]
    InvalidRecipeDirectoryPathError(String),

    /// Generic service error
    #[error("Service error: {0}")]
    ServiceError(String),

    /// Protocol error
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// Invalid input
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// Unknown error
    #[error("Unknown error: {0}")]
    Unknown(String),
}

/// Consolidated error response structure that handles all Greengrass IPC error types
#[derive(Debug, Deserialize)]
#[allow(unused)]
struct ErrorResponse {
    message: String,
    #[serde(rename = "_service")]
    service: Option<String>,
    #[serde(rename = "_message")]
    service_message: Option<String>,
    #[serde(rename = "_errorCode")]
    error_code: String,
    #[serde(rename = "resourceType")]
    resource_type: Option<String>,
    #[serde(rename = "resourceName")]
    resource_name: Option<String>,
    #[serde(rename = "componentName")]
    component_name: Option<String>,
}

/// Check if a JSON response contains an error and parse it into the appropriate Error type
pub fn check_and_parse_error_response(json_str: &str) -> std::result::Result<(), Error> {
    // First, try to parse as a generic JSON value to check for error code
    match serde_json::from_str::<serde_json::Value>(json_str) {
        Ok(value) => {
            if value.get("_errorCode").is_some() {
                Err(parse_error_response(json_str))
            } else {
                Ok(())
            }
        }
        Err(_) => {
            // If we can't parse the JSON at all, it's probably not an error response
            Ok(())
        }
    }
}

/// Parse a JSON error response string into the appropriate Error type using serde
pub fn parse_error_response(json_str: &str) -> Error {
    match serde_json::from_str::<ErrorResponse>(json_str) {
        Ok(error_response) => {
            match error_response.error_code.as_str() {
                "InvalidArgumentsError" => Error::InvalidArgumentsError(error_response.message),
                "ResourceNotFoundError" => Error::ResourceNotFoundError {
                    message: error_response.message,
                    resource_type: error_response.resource_type,
                    resource_name: error_response.resource_name,
                },
                "UnauthorizedError" => Error::UnauthorizedError(error_response.message),
                "ComponentNotFoundError" => Error::ComponentNotFoundError(error_response.message),
                "InvalidTokenError" => Error::InvalidTokenError(error_response.message),
                "FailedUpdateConditionCheckError" => {
                    Error::FailedUpdateConditionCheckError(error_response.message)
                }
                "ConflictError" => Error::ConflictError(error_response.message),
                "InvalidClientDeviceAuthTokenError" => {
                    Error::InvalidClientDeviceAuthTokenError(error_response.message)
                }
                "InvalidCredentialError" => Error::InvalidCredentialError(error_response.message),
                "InvalidArtifactsDirectoryPathError" => {
                    Error::InvalidArtifactsDirectoryPathError(error_response.message)
                }
                "InvalidRecipeDirectoryPathError" => {
                    Error::InvalidRecipeDirectoryPathError(error_response.message)
                }
                _ => {
                    // Unknown error code, fall back to generic service error
                    Error::ServiceError(format!(
                        "Unknown error code '{}': {}",
                        error_response.error_code, error_response.message
                    ))
                }
            }
        }
        Err(_) => {
            // Failed to parse JSON, return generic error
            Error::ServiceError(format!("Failed to parse error response: {}", json_str))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_invalid_arguments_error() {
        let json = r#"{"message":"Validation request either timed out or was never made","_service":"aws.greengrass#GreengrassCoreIPC","_message":"Validation request either timed out or was never made","_errorCode":"InvalidArgumentsError"}"#;

        match parse_error_response(json) {
            Error::InvalidArgumentsError(msg) => {
                assert_eq!(msg, "Validation request either timed out or was never made");
            }
            _ => panic!("Expected InvalidArgumentsError"),
        }
    }

    #[test]
    fn test_parse_resource_not_found_error() {
        let json = r#"{"message":"Key not found","_service":"aws.greengrass#GreengrassCoreIPC","_message":"Key not found","_errorCode":"ResourceNotFoundError"}"#;

        match parse_error_response(json) {
            Error::ResourceNotFoundError {
                message,
                resource_type,
                resource_name,
            } => {
                assert_eq!(message, "Key not found");
                assert_eq!(resource_type, None);
                assert_eq!(resource_name, None);
            }
            _ => panic!("Expected ResourceNotFoundError"),
        }
    }

    #[test]
    fn test_parse_resource_not_found_error_with_details() {
        let json = r#"{"message":"Component not found","resourceType":"Component","resourceName":"MyComponent","_errorCode":"ResourceNotFoundError"}"#;

        match parse_error_response(json) {
            Error::ResourceNotFoundError {
                message,
                resource_type,
                resource_name,
            } => {
                assert_eq!(message, "Component not found");
                assert_eq!(resource_type, Some("Component".to_string()));
                assert_eq!(resource_name, Some("MyComponent".to_string()));
            }
            _ => panic!("Expected ResourceNotFoundError"),
        }
    }

    #[test]
    fn test_parse_unknown_error_code() {
        let json = r#"{"message":"Something went wrong","_errorCode":"UnknownErrorCode"}"#;

        match parse_error_response(json) {
            Error::ServiceError(msg) => {
                assert_eq!(
                    msg,
                    "Unknown error code 'UnknownErrorCode': Something went wrong"
                );
            }
            _ => panic!("Expected ServiceError"),
        }
    }

    #[test]
    fn test_parse_invalid_json() {
        let json = "invalid json";

        match parse_error_response(json) {
            Error::ServiceError(msg) => {
                assert!(msg.starts_with("Failed to parse error response:"));
            }
            _ => panic!("Expected ServiceError"),
        }
    }

    #[test]
    fn test_deserialize_error_response() {
        let json = r#"{"message":"Validation request either timed out or was never made","_service":"aws.greengrass#GreengrassCoreIPC","_message":"Validation request either timed out or was never made","_errorCode":"InvalidArgumentsError"}"#;

        let error_response: ErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(
            error_response.message,
            "Validation request either timed out or was never made"
        );
        assert_eq!(error_response.error_code, "InvalidArgumentsError");
        assert_eq!(
            error_response.service,
            Some("aws.greengrass#GreengrassCoreIPC".to_string())
        );
    }

    #[test]
    fn test_deserialize_resource_not_found_error_response() {
        let json = r#"{"message":"Component not found","resourceType":"Component","resourceName":"MyComponent","_errorCode":"ResourceNotFoundError"}"#;

        let error_response: ErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(error_response.message, "Component not found");
        assert_eq!(error_response.error_code, "ResourceNotFoundError");
        assert_eq!(error_response.resource_type, Some("Component".to_string()));
        assert_eq!(
            error_response.resource_name,
            Some("MyComponent".to_string())
        );
    }

    #[test]
    fn test_check_and_parse_error_response_with_error() {
        let json = r#"{"message":"Key not found","_errorCode":"ResourceNotFoundError"}"#;

        match check_and_parse_error_response(json) {
            Err(Error::ResourceNotFoundError { message, .. }) => {
                assert_eq!(message, "Key not found");
            }
            _ => panic!("Expected ResourceNotFoundError"),
        }
    }

    #[test]
    fn test_check_and_parse_error_response_without_error() {
        let json = r#"{"data":"some success response"}"#;

        assert!(check_and_parse_error_response(json).is_ok());
    }
}
