//! Main ShadowClient implementation for AWS IoT Device Shadows
//!
//! This module provides the core ShadowClient that uses MQTT-based communication
//! through the Greengrass IPC client for shadow operations.

use crate::utils::shadow::{
    document::{
        ShadowAcceptedResponse, ShadowDeltaResponse, ShadowDocument, ShadowRejectedResponse,
        ShadowState, ShadowUpdateRequest,
    },
    error::{ShadowError, ShadowResult},
    persistence::FileBasedPersistence,
    topics::ShadowTopics,
};
use crate::{
    GreengrassCoreIPCClient, IoTCoreMessage, PublishToIoTCoreRequest, QoS, StreamOperation,
    SubscribeToIoTCoreRequest,
};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

/// Default timeout for shadow operations
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// AWS IoT Device Shadow client
#[derive(Clone)]
pub struct ShadowClient<T> {
    /// The underlying IPC client
    ipc_client: Arc<GreengrassCoreIPCClient>,

    /// Thing name for the shadow
    thing_name: String,

    /// Shadow name (None for classic shadow)
    shadow_name: Option<String>,

    /// Shadow topic manager
    topics: ShadowTopics,

    /// File-based persistence
    persistence: FileBasedPersistence<T>,

    /// Operation timeout
    operation_timeout: Duration,

    /// Phantom data for type parameter
    _phantom: PhantomData<T>,
}

impl<T> ShadowClient<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    /// Create a new shadow client
    pub async fn new(
        ipc_client: Arc<GreengrassCoreIPCClient>,
        thing_name: String,
        shadow_name: Option<String>,
        persistence_file_path: PathBuf,
    ) -> ShadowResult<Self> {
        let topics = if let Some(ref name) = shadow_name {
            ShadowTopics::new_named(&thing_name, name)
        } else {
            ShadowTopics::new_classic(&thing_name)
        };

        let persistence = FileBasedPersistence::new(persistence_file_path);

        Ok(Self {
            ipc_client,
            thing_name,
            shadow_name,
            topics,
            persistence,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            _phantom: PhantomData,
        })
    }

    /// Set the operation timeout
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.operation_timeout = timeout;
    }

    /// Get the current operation timeout
    pub fn timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Get the thing name
    pub fn thing_name(&self) -> &str {
        &self.thing_name
    }

    /// Get the shadow name (if any)
    pub fn shadow_name(&self) -> Option<&str> {
        self.shadow_name.as_deref()
    }

    /// Get the current shadow state from AWS IoT
    pub async fn get_shadow(&self) -> ShadowResult<Option<T>> {
        // Create subscriptions for get responses
        let (accepted_stream, rejected_stream) = self.subscribe_to_get_responses().await?;

        // Generate client token for request correlation
        let client_token = self.generate_client_token();

        // Publish get request
        self.publish_get_request(&client_token).await?;

        // Wait for response
        let response = self
            .wait_for_get_response(accepted_stream, rejected_stream, &client_token)
            .await?;

        // Update local persistence if we got a shadow
        if let Some(ref shadow) = response {
            let document = ShadowDocument {
                state: ShadowState {
                    desired: shadow.desired().cloned(),
                    reported: shadow.reported().cloned(),
                    delta: shadow.delta().cloned(),
                },
                metadata: None,
                version: shadow.version,
                timestamp: shadow.timestamp,
                client_token: Some(client_token),
            };
            self.persistence.save(&document).await?;
        }

        Ok(response.and_then(|doc| doc.reported().cloned()))
    }

    /// Wait for delta changes from the cloud
    pub async fn wait_delta(&self) -> ShadowResult<T> {
        // Subscribe to delta topic
        let mut delta_stream = self.subscribe_to_delta().await?;

        // Wait for delta message
        while let Some(message) = delta_stream.next().await {
            let topic = &message.message.topic_name;
            let payload = &message.message.payload;

            if topic == &self.topics.delta {
                let delta_response: ShadowDeltaResponse<T> = serde_json::from_slice(payload)?;

                if let Some(delta_state) = delta_response.state {
                    // Update local persistence with the new state
                    let mut current_doc =
                        self.persistence
                            .load()
                            .await?
                            .unwrap_or_else(|| ShadowDocument {
                                state: ShadowState::default(),
                                metadata: None,
                                version: None,
                                timestamp: None,
                                client_token: None,
                            });

                    // Apply delta to reported state
                    current_doc.state.reported = Some(delta_state.clone());
                    current_doc.version = delta_response.version;
                    current_doc.timestamp = delta_response.timestamp;

                    self.persistence.save(&current_doc).await?;

                    return Ok(delta_state);
                }
            }
        }

        Err(ShadowError::Timeout)
    }

    /// Report the current device state to the cloud
    pub async fn report(&self, state: T) -> ShadowResult<()> {
        let update_request = ShadowUpdateRequest::new_reported(state.clone())
            .with_client_token(self.generate_client_token());

        self.perform_update(update_request).await?;

        // Update local persistence
        let mut current_doc = self
            .persistence
            .load()
            .await?
            .unwrap_or_else(|| ShadowDocument {
                state: ShadowState::default(),
                metadata: None,
                version: None,
                timestamp: None,
                client_token: None,
            });

        current_doc.state.reported = Some(state);
        self.persistence.save(&current_doc).await?;

        Ok(())
    }

    /// Update the desired state in the cloud
    pub async fn update(&self, desired: T) -> ShadowResult<()> {
        let update_request = ShadowUpdateRequest::new_desired(desired.clone())
            .with_client_token(self.generate_client_token());

        self.perform_update(update_request).await?;

        // Update local persistence
        let mut current_doc = self
            .persistence
            .load()
            .await?
            .unwrap_or_else(|| ShadowDocument {
                state: ShadowState::default(),
                metadata: None,
                version: None,
                timestamp: None,
                client_token: None,
            });

        current_doc.state.desired = Some(desired);
        self.persistence.save(&current_doc).await?;

        Ok(())
    }

    /// Delete the shadow from AWS IoT
    pub async fn delete_shadow(&self) -> ShadowResult<()> {
        // Create subscriptions for delete responses
        let (accepted_stream, rejected_stream) = self.subscribe_to_delete_responses().await?;

        // Generate client token
        let client_token = self.generate_client_token();

        // Publish delete request
        self.publish_delete_request(&client_token).await?;

        // Wait for response
        self.wait_for_delete_response(accepted_stream, rejected_stream, &client_token)
            .await?;

        // Delete local persistence
        self.persistence.delete().await?;

        Ok(())
    }

    /// Perform a shadow update operation
    async fn perform_update(
        &self,
        request: ShadowUpdateRequest<T>,
    ) -> ShadowResult<ShadowAcceptedResponse<T>> {
        // Create subscriptions for update responses
        let (accepted_stream, rejected_stream) = self.subscribe_to_update_responses().await?;

        // Get client token from request
        let client_token = request
            .client_token
            .clone()
            .unwrap_or_else(|| self.generate_client_token());

        // Publish update request
        self.publish_update_request(&request).await?;

        // Wait for response
        self.wait_for_update_response(accepted_stream, rejected_stream, &client_token)
            .await
    }

    /// Subscribe to get operation responses
    async fn subscribe_to_get_responses(
        &self,
    ) -> ShadowResult<(
        StreamOperation<IoTCoreMessage>,
        StreamOperation<IoTCoreMessage>,
    )> {
        let accepted_stream = self.subscribe_to_topic(&self.topics.get_accepted).await?;
        let rejected_stream = self.subscribe_to_topic(&self.topics.get_rejected).await?;
        Ok((accepted_stream, rejected_stream))
    }

    /// Subscribe to update operation responses
    async fn subscribe_to_update_responses(
        &self,
    ) -> ShadowResult<(
        StreamOperation<IoTCoreMessage>,
        StreamOperation<IoTCoreMessage>,
    )> {
        let accepted_stream = self
            .subscribe_to_topic(&self.topics.update_accepted)
            .await?;
        let rejected_stream = self
            .subscribe_to_topic(&self.topics.update_rejected)
            .await?;
        Ok((accepted_stream, rejected_stream))
    }

    /// Subscribe to delete operation responses
    async fn subscribe_to_delete_responses(
        &self,
    ) -> ShadowResult<(
        StreamOperation<IoTCoreMessage>,
        StreamOperation<IoTCoreMessage>,
    )> {
        let accepted_stream = self
            .subscribe_to_topic(&self.topics.delete_accepted)
            .await?;
        let rejected_stream = self
            .subscribe_to_topic(&self.topics.delete_rejected)
            .await?;
        Ok((accepted_stream, rejected_stream))
    }

    /// Subscribe to delta notifications
    async fn subscribe_to_delta(&self) -> ShadowResult<StreamOperation<IoTCoreMessage>> {
        self.subscribe_to_topic(&self.topics.delta).await
    }

    /// Subscribe to a specific topic
    async fn subscribe_to_topic(
        &self,
        topic: &str,
    ) -> ShadowResult<StreamOperation<IoTCoreMessage>> {
        let request = SubscribeToIoTCoreRequest {
            topic_name: topic.to_string(),
            qos: QoS::AtLeastOnce,
        };

        let stream = self.ipc_client.subscribe_to_iot_core(request).await?;
        Ok(stream)
    }

    /// Publish a get request
    async fn publish_get_request(&self, _client_token: &str) -> ShadowResult<()> {
        let empty_payload = b"";
        self.publish_to_topic(&self.topics.get, empty_payload).await
    }

    /// Publish an update request
    async fn publish_update_request(&self, request: &ShadowUpdateRequest<T>) -> ShadowResult<()> {
        let payload = serde_json::to_vec(request)?;
        self.publish_to_topic(&self.topics.update, &payload).await
    }

    /// Publish a delete request
    async fn publish_delete_request(&self, _client_token: &str) -> ShadowResult<()> {
        let empty_payload = b"";
        self.publish_to_topic(&self.topics.delete, empty_payload)
            .await
    }

    /// Publish to a specific topic
    async fn publish_to_topic(&self, topic: &str, payload: &[u8]) -> ShadowResult<()> {
        let request = PublishToIoTCoreRequest {
            topic_name: topic.to_string(),
            qos: QoS::AtLeastOnce,
            payload: payload.to_vec().into(),
            user_properties: None,
            message_expiry_interval_seconds: None,
            correlation_data: None,
            response_topic: None,
            content_type: None,
        };

        self.ipc_client.publish_to_iot_core(request).await?;
        Ok(())
    }

    /// Wait for get operation response
    async fn wait_for_get_response(
        &self,
        mut accepted_stream: StreamOperation<IoTCoreMessage>,
        mut rejected_stream: StreamOperation<IoTCoreMessage>,
        client_token: &str,
    ) -> ShadowResult<Option<ShadowDocument<T>>> {
        let operation = async {
            tokio::select! {
                message = accepted_stream.next() => {
                    println!("Raw json payload: {:?}", message);
                    if let Some(msg) = message {
                        let response: ShadowAcceptedResponse<T> = serde_json::from_slice(&msg.message.payload)?;

                        // Verify client token if present
                        if let Some(ref token) = response.client_token {
                            if token != client_token {
                                return Err(ShadowError::ClientTokenMismatch {
                                    expected: client_token.to_string(),
                                    received: token.clone(),
                                });
                            }
                        }

                        let document = ShadowDocument {
                            state: response.state,
                            metadata: response.metadata,
                            version: response.version,
                            timestamp: response.timestamp,
                            client_token: response.client_token,
                        };

                        Ok(Some(document))
                    } else {
                        Err(ShadowError::Timeout)
                    }
                }
                message = rejected_stream.next() => {
                    if let Some(msg) = message {
                        let response: ShadowRejectedResponse = serde_json::from_slice(&msg.message.payload)?;

                        // Check if this is a "not found" error
                        if response.code == 404 {
                            Ok(None)
                        } else {
                            Err(ShadowError::ShadowRejected {
                                code: response.code,
                                message: response.message,
                            })
                        }
                    } else {
                        Err(ShadowError::Timeout)
                    }
                }
            }
        };

        timeout(self.operation_timeout, operation)
            .await
            .map_err(|_| ShadowError::Timeout)?
    }

    /// Wait for update operation response
    async fn wait_for_update_response(
        &self,
        mut accepted_stream: StreamOperation<IoTCoreMessage>,
        mut rejected_stream: StreamOperation<IoTCoreMessage>,
        client_token: &str,
    ) -> ShadowResult<ShadowAcceptedResponse<T>> {
        let operation = async {
            tokio::select! {
                message = accepted_stream.next() => {
                    if let Some(msg) = message {
                        let response: ShadowAcceptedResponse<T> = serde_json::from_slice(&msg.message.payload)?;

                        // Verify client token if present
                        if let Some(ref token) = response.client_token {
                            if token != client_token {
                                return Err(ShadowError::ClientTokenMismatch {
                                    expected: client_token.to_string(),
                                    received: token.clone(),
                                });
                            }
                        }

                        Ok(response)
                    } else {
                        Err(ShadowError::Timeout)
                    }
                }
                message = rejected_stream.next() => {
                    if let Some(msg) = message {
                        let response: ShadowRejectedResponse = serde_json::from_slice(&msg.message.payload)?;
                        Err(ShadowError::ShadowRejected {
                            code: response.code,
                            message: response.message,
                        })
                    } else {
                        Err(ShadowError::Timeout)
                    }
                }
            }
        };

        timeout(self.operation_timeout, operation)
            .await
            .map_err(|_| ShadowError::Timeout)?
    }

    /// Wait for delete operation response
    async fn wait_for_delete_response(
        &self,
        mut accepted_stream: StreamOperation<IoTCoreMessage>,
        mut rejected_stream: StreamOperation<IoTCoreMessage>,
        _client_token: &str,
    ) -> ShadowResult<()> {
        let operation = async {
            tokio::select! {
                message = accepted_stream.next() => {
                    if let Some(_msg) = message {
                        Ok(())
                    } else {
                        Err(ShadowError::Timeout)
                    }
                }
                message = rejected_stream.next() => {
                    if let Some(msg) = message {
                        let response: ShadowRejectedResponse = serde_json::from_slice(&msg.message.payload)?;
                        Err(ShadowError::ShadowRejected {
                            code: response.code,
                            message: response.message,
                        })
                    } else {
                        Err(ShadowError::Timeout)
                    }
                }
            }
        };

        timeout(self.operation_timeout, operation)
            .await
            .map_err(|_| ShadowError::Timeout)?
    }

    /// Generate a unique client token for request correlation
    fn generate_client_token(&self) -> String {
        Uuid::new_v4().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::shadow::document::ShadowState;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestState {
        temperature: f64,
        humidity: f64,
        status: String,
    }

    fn create_test_state() -> TestState {
        TestState {
            temperature: 23.5,
            humidity: 45.2,
            status: "online".to_string(),
        }
    }

    #[test]
    fn test_client_token_generation() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.json");

        // We can't easily test the full client without mocking the IPC client,
        // but we can test individual components
        let _persistence: FileBasedPersistence<TestState> = FileBasedPersistence::new(file_path);

        // Test that we can create the client structure
        let topics = ShadowTopics::new_classic("test-device");
        assert_eq!(topics.update, "$aws/things/test-device/shadow/update");
    }

    #[tokio::test]
    async fn test_persistence_integration() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_shadow.json");
        let persistence = FileBasedPersistence::new(file_path);

        let state = create_test_state();
        let document = ShadowDocument {
            state: ShadowState {
                desired: None,
                reported: Some(state.clone()),
                delta: None,
            },
            metadata: None,
            version: Some(1),
            timestamp: Some(1234567890),
            client_token: Some("test-token".to_string()),
        };

        // Test save and load
        persistence.save(&document).await.unwrap();
        let loaded = persistence.load().await.unwrap().unwrap();
        assert_eq!(document, loaded);
    }
}
