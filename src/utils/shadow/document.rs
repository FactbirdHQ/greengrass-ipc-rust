//! AWS IoT Device Shadow document types
//!
//! This module contains the data structures representing AWS IoT Device Shadow
//! documents as defined in the AWS IoT Core documentation.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A complete shadow document as returned by AWS IoT
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShadowDocument<T> {
    /// The shadow state containing desired and reported values
    pub state: ShadowState<T>,
    
    /// Metadata about when each field was last updated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ShadowMetadata>,
    
    /// The current version of the shadow document
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    
    /// The timestamp when the document was last updated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    
    /// Client token for request/response correlation
    #[serde(rename = "clientToken", skip_serializing_if = "Option::is_none")]
    pub client_token: Option<String>,
}

/// The state section of a shadow document
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShadowState<T> {
    /// The desired state - what the device should become
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired: Option<T>,
    
    /// The reported state - the current state of the device
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reported: Option<T>,
    
    /// The delta - difference between desired and reported
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<T>,
}

/// Metadata about when each field in the shadow was last updated
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShadowMetadata {
    /// Metadata for the desired state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired: Option<HashMap<String, FieldMetadata>>,
    
    /// Metadata for the reported state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reported: Option<HashMap<String, FieldMetadata>>,
}

/// Metadata for a specific field in the shadow
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldMetadata {
    /// Timestamp when this field was last updated
    pub timestamp: u64,
}

/// Request structure for updating a shadow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowUpdateRequest<T> {
    /// The state to update
    pub state: ShadowUpdateState<T>,
    
    /// Client token for request/response correlation
    #[serde(rename = "clientToken", skip_serializing_if = "Option::is_none")]
    pub client_token: Option<String>,
    
    /// Version to match for conditional updates
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
}

/// State section for shadow update requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowUpdateState<T> {
    /// The desired state to set
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired: Option<T>,
    
    /// The reported state to set
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reported: Option<T>,
}

/// Response from AWS IoT when a shadow operation is accepted
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowAcceptedResponse<T> {
    /// The updated shadow state
    pub state: ShadowState<T>,
    
    /// Metadata about the update
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ShadowMetadata>,
    
    /// The new version of the shadow
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    
    /// Timestamp of the update
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    
    /// Client token from the original request
    #[serde(rename = "clientToken", skip_serializing_if = "Option::is_none")]
    pub client_token: Option<String>,
}

/// Response from AWS IoT when a shadow operation is rejected
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowRejectedResponse {
    /// HTTP status code indicating the type of error
    pub code: u16,
    
    /// Human-readable error message
    pub message: String,
    
    /// Timestamp when the error occurred
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    
    /// Client token from the original request
    #[serde(rename = "clientToken", skip_serializing_if = "Option::is_none")]
    pub client_token: Option<String>,
}

/// Delta response indicating changes to the shadow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowDeltaResponse<T> {
    /// The current state of the shadow
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<T>,
    
    /// Metadata about the delta
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, FieldMetadata>>,
    
    /// The version of the shadow that generated this delta
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    
    /// Timestamp when the delta was generated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    
    /// Client token from the update that caused this delta
    #[serde(rename = "clientToken", skip_serializing_if = "Option::is_none")]
    pub client_token: Option<String>,
}

impl<T> Default for ShadowState<T> {
    fn default() -> Self {
        Self {
            desired: None,
            reported: None,
            delta: None,
        }
    }
}

impl<T> Default for ShadowUpdateState<T> {
    fn default() -> Self {
        Self {
            desired: None,
            reported: None,
        }
    }
}

impl<T> ShadowUpdateRequest<T> {
    /// Create a new update request with only reported state
    pub fn new_reported(reported: T) -> Self {
        Self {
            state: ShadowUpdateState {
                desired: None,
                reported: Some(reported),
            },
            client_token: None,
            version: None,
        }
    }
    
    /// Create a new update request with only desired state
    pub fn new_desired(desired: T) -> Self {
        Self {
            state: ShadowUpdateState {
                desired: Some(desired),
                reported: None,
            },
            client_token: None,
            version: None,
        }
    }
    
    /// Set the client token for this request
    pub fn with_client_token(mut self, token: String) -> Self {
        self.client_token = Some(token);
        self
    }
    
    /// Set the version for conditional updates
    pub fn with_version(mut self, version: u64) -> Self {
        self.version = Some(version);
        self
    }
}

impl<T> ShadowDocument<T> {
    /// Get the current reported state if available
    pub fn reported(&self) -> Option<&T> {
        self.state.reported.as_ref()
    }
    
    /// Get the current desired state if available
    pub fn desired(&self) -> Option<&T> {
        self.state.desired.as_ref()
    }
    
    /// Get the current delta if available
    pub fn delta(&self) -> Option<&T> {
        self.state.delta.as_ref()
    }
    
    /// Check if this document has a delta (indicating desired != reported)
    pub fn has_delta(&self) -> bool {
        self.state.delta.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestState {
        temperature: f64,
        humidity: f64,
    }

    #[test]
    fn test_shadow_document_serialization() {
        let state = TestState {
            temperature: 23.5,
            humidity: 45.2,
        };
        
        let doc = ShadowDocument {
            state: ShadowState {
                desired: Some(state.clone()),
                reported: Some(state.clone()),
                delta: None,
            },
            metadata: None,
            version: Some(1),
            timestamp: Some(1234567890),
            client_token: Some("test-token".to_string()),
        };
        
        let json = serde_json::to_string(&doc).unwrap();
        let deserialized: ShadowDocument<TestState> = serde_json::from_str(&json).unwrap();
        
        assert_eq!(doc, deserialized);
    }
    
    #[test]
    fn test_shadow_update_request() {
        let state = TestState {
            temperature: 25.0,
            humidity: 50.0,
        };
        
        let request = ShadowUpdateRequest::new_reported(state.clone())
            .with_client_token("test-token".to_string())
            .with_version(2);
        
        assert_eq!(request.state.reported, Some(state));
        assert_eq!(request.state.desired, None);
        assert_eq!(request.client_token, Some("test-token".to_string()));
        assert_eq!(request.version, Some(2));
    }
    
    #[test]
    fn test_shadow_rejected_response() {
        let json = r#"{
            "code": 400,
            "message": "Invalid request",
            "timestamp": 1234567890,
            "clientToken": "test-token"
        }"#;
        
        let response: ShadowRejectedResponse = serde_json::from_str(json).unwrap();
        
        assert_eq!(response.code, 400);
        assert_eq!(response.message, "Invalid request");
        assert_eq!(response.timestamp, Some(1234567890));
        assert_eq!(response.client_token, Some("test-token".to_string()));
    }
}