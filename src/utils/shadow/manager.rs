//! Multi-shadow manager with wildcard subscription support
//!
//! This module provides the `ShadowManager` which can manage multiple named shadows
//! with pattern-based filtering and efficient wildcard MQTT subscriptions.

use crate::model::ListNamedShadowsForThingRequest;
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
use futures::stream::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use uuid::Uuid;

/// Default timeout for shadow operations
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Multi-shadow manager that can handle multiple named shadows with wildcard subscriptions
#[derive(Clone)]
pub struct ShadowManager<T> {
    /// The underlying IPC client
    ipc_client: Arc<GreengrassCoreIPCClient>,

    /// Thing name for all shadows
    thing_name: String,

    /// Shadow name pattern (e.g., "flow-")
    shadow_pattern: String,

    /// Base directory for persistence files
    persistence_base_dir: PathBuf,

    /// Currently managed shadows (shadow_name -> persistence)
    shadows: Arc<RwLock<HashMap<String, FileBasedPersistence<T>>>>,

    /// Operation timeout
    operation_timeout: Duration,
}

impl<T> ShadowManager<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    /// Create a new shadow manager
    pub async fn new(
        ipc_client: Arc<GreengrassCoreIPCClient>,
        thing_name: String,
        shadow_pattern: String,
        persistence_base_dir: PathBuf,
    ) -> ShadowResult<Self> {
        Ok(Self {
            ipc_client,
            thing_name,
            shadow_pattern,
            persistence_base_dir,
            shadows: Arc::new(RwLock::new(HashMap::new())),
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        })
    }

    /// Initialize the manager by discovering existing shadows and setting up subscriptions
    pub async fn initialize(&self) -> ShadowResult<()> {
        // Discover existing shadows
        let shadow_names = self.discover_shadows().await?;

        // Load persistence for each shadow
        for shadow_name in &shadow_names {
            self.add_shadow(shadow_name.clone()).await?;
        }

        // Note: Wildcard subscriptions are set up when create_delta_stream() is called

        // Sync all shadows from cloud
        for shadow_name in &shadow_names {
            if let Err(e) = self.sync_shadow_from_cloud(shadow_name).await {
                eprintln!("Warning: Failed to sync shadow '{}': {}", shadow_name, e);
            }
        }

        Ok(())
    }

    /// Discover shadows matching the pattern
    async fn discover_shadows(&self) -> ShadowResult<Vec<String>> {
        let request = ListNamedShadowsForThingRequest {
            thing_name: self.thing_name.clone(),
            next_token: None,
            page_size: Some(100),
        };

        let response = self
            .ipc_client
            .list_named_shadows_for_thing(request)
            .await?;

        let matching_shadows = response
            .results
            .into_iter()
            .filter(|name| name.starts_with(&self.shadow_pattern))
            .collect();

        Ok(matching_shadows)
    }

    /// Add a new shadow to be managed
    pub async fn add_shadow(&self, shadow_name: String) -> ShadowResult<()> {
        let persistence_path = self
            .persistence_base_dir
            .join(format!("{}.json", shadow_name));

        let persistence = FileBasedPersistence::new(persistence_path);

        let mut shadows = self.shadows.write().await;
        shadows.insert(shadow_name, persistence);

        Ok(())
    }

    /// Remove a shadow from management
    pub async fn remove_shadow(&self, shadow_name: &str) -> ShadowResult<()> {
        let mut shadows = self.shadows.write().await;
        shadows.remove(shadow_name);
        Ok(())
    }

    /// Wait for the next delta change from any managed shadow
    pub async fn wait_delta(&self) -> ShadowResult<(String, T)> {
        // Subscribe to delta topic with wildcard
        let delta_topic = format!(
            "$aws/things/{}/shadow/name/{}+/update/delta",
            self.thing_name, self.shadow_pattern
        );

        let request = SubscribeToIoTCoreRequest {
            topic_name: delta_topic,
            qos: QoS::AtLeastOnce,
        };

        let mut delta_stream = self.ipc_client.subscribe_to_iot_core(request).await?;

        // Wait for the next delta message
        while let Some(message) = delta_stream.next().await {
            if let Ok((shadow_name, delta_state)) = self.parse_delta_message(&message).await {
                return Ok((shadow_name, delta_state));
            }
        }

        Err(ShadowError::Timeout)
    }

    /// Create a delta subscription for monitoring changes
    /// Returns a StreamOperation that can be used to monitor delta changes
    pub async fn create_delta_subscription(&self) -> ShadowResult<StreamOperation<IoTCoreMessage>> {
        // Subscribe to delta topic with wildcard
        let delta_topic = format!(
            "$aws/things/{}/shadow/name/{}+/update/delta",
            self.thing_name, self.shadow_pattern
        );

        let request = SubscribeToIoTCoreRequest {
            topic_name: delta_topic,
            qos: QoS::AtLeastOnce,
        };

        let delta_stream = self.ipc_client.subscribe_to_iot_core(request).await?;

        Ok(delta_stream)
    }

    /// Parse a delta message and extract shadow name and state
    pub async fn parse_delta_message(&self, message: &IoTCoreMessage) -> ShadowResult<(String, T)> {
        let topic = &message.message.topic_name;
        let payload = &message.message.payload;

        // Parse shadow name from topic
        // Topic format: $aws/things/{thing_name}/shadow/name/{shadow_name}/update/delta
        let expected_prefix = format!("$aws/things/{}/shadow/name/", self.thing_name);
        let expected_suffix = "/update/delta";

        if let Some(middle) = topic.strip_prefix(&expected_prefix) {
            if let Some(shadow_name) = middle.strip_suffix(expected_suffix) {
                if shadow_name.starts_with(&self.shadow_pattern) {
                    // Parse delta response
                    let delta_response: ShadowDeltaResponse<T> =
                        serde_json::from_slice(payload).map_err(ShadowError::SerializationError)?;

                    if let Some(delta_state) = delta_response.state {
                        return Ok((shadow_name.to_string(), delta_state));
                    }
                }
            }
        }

        Err(ShadowError::InvalidDocument(
            "Failed to parse delta message".to_string(),
        ))
    }

    /// Get the current state of a specific shadow
    pub async fn get_shadow(&self, shadow_name: &str) -> ShadowResult<Option<T>> {
        let shadows = self.shadows.read().await;
        let persistence = shadows.get(shadow_name).ok_or_else(|| {
            ShadowError::InvalidDocument(format!("Shadow '{}' not managed", shadow_name))
        })?;

        // First try to load from local persistence
        if let Some(document) = persistence.load().await? {
            if let Some(reported) = document.reported() {
                return Ok(Some(reported.clone()));
            }
        }

        // If not in local storage, fetch from cloud
        self.sync_shadow_from_cloud(shadow_name).await?;

        // Try loading again after sync
        if let Some(document) = persistence.load().await? {
            Ok(document.reported().cloned())
        } else {
            Ok(None)
        }
    }

    /// Sync a shadow from the cloud
    async fn sync_shadow_from_cloud(&self, shadow_name: &str) -> ShadowResult<()> {
        let topics = ShadowTopics::new_named(&self.thing_name, shadow_name);

        // Create subscriptions for get responses
        let accepted_stream = self.subscribe_to_topic(&topics.get_accepted).await?;
        let rejected_stream = self.subscribe_to_topic(&topics.get_rejected).await?;

        // Generate client token
        let client_token = self.generate_client_token();

        // Publish get request
        self.publish_to_topic(&topics.get, b"").await?;

        // Wait for response
        let response = self
            .wait_for_get_response(accepted_stream, rejected_stream, &client_token)
            .await?;

        if let Some(document) = response {
            // Update local persistence
            let shadows = self.shadows.read().await;
            if let Some(persistence) = shadows.get(shadow_name) {
                persistence.save(&document).await?;
            }
        }

        Ok(())
    }

    /// Report state for a specific shadow
    pub async fn report(&self, shadow_name: &str, state: T) -> ShadowResult<()> {
        let topics = ShadowTopics::new_named(&self.thing_name, shadow_name);

        let update_request = ShadowUpdateRequest::new_reported(state.clone())
            .with_client_token(self.generate_client_token());

        // Create subscriptions for update responses
        let accepted_stream = self.subscribe_to_topic(&topics.update_accepted).await?;
        let rejected_stream = self.subscribe_to_topic(&topics.update_rejected).await?;

        // Publish update request
        let payload = serde_json::to_vec(&update_request)?;
        self.publish_to_topic(&topics.update, &payload).await?;

        // Wait for response
        let client_token = update_request
            .client_token
            .unwrap_or_else(|| self.generate_client_token());
        self.wait_for_update_response(accepted_stream, rejected_stream, &client_token)
            .await?;

        // Update local persistence
        let shadows = self.shadows.read().await;
        if let Some(persistence) = shadows.get(shadow_name) {
            let mut current_doc = persistence.load().await?.unwrap_or_else(|| ShadowDocument {
                state: ShadowState::default(),
                metadata: None,
                version: None,
                timestamp: None,
                client_token: None,
            });

            current_doc.state.reported = Some(state);
            persistence.save(&current_doc).await?;
        }

        Ok(())
    }

    /// Update desired state for a specific shadow
    pub async fn update(&self, shadow_name: &str, desired: T) -> ShadowResult<()> {
        let topics = ShadowTopics::new_named(&self.thing_name, shadow_name);

        let update_request = ShadowUpdateRequest::new_desired(desired.clone())
            .with_client_token(self.generate_client_token());

        // Create subscriptions for update responses
        let accepted_stream = self.subscribe_to_topic(&topics.update_accepted).await?;
        let rejected_stream = self.subscribe_to_topic(&topics.update_rejected).await?;

        // Publish update request
        let payload = serde_json::to_vec(&update_request)?;
        self.publish_to_topic(&topics.update, &payload).await?;

        // Wait for response
        let client_token = update_request
            .client_token
            .unwrap_or_else(|| self.generate_client_token());
        self.wait_for_update_response(accepted_stream, rejected_stream, &client_token)
            .await?;

        // Update local persistence
        let shadows = self.shadows.read().await;
        if let Some(persistence) = shadows.get(shadow_name) {
            let mut current_doc = persistence.load().await?.unwrap_or_else(|| ShadowDocument {
                state: ShadowState::default(),
                metadata: None,
                version: None,
                timestamp: None,
                client_token: None,
            });

            current_doc.state.desired = Some(desired);
            persistence.save(&current_doc).await?;
        }

        Ok(())
    }

    /// Delete a specific shadow
    pub async fn delete_shadow(&self, shadow_name: &str) -> ShadowResult<()> {
        let topics = ShadowTopics::new_named(&self.thing_name, shadow_name);

        // Create subscriptions for delete responses
        let accepted_stream = self.subscribe_to_topic(&topics.delete_accepted).await?;
        let rejected_stream = self.subscribe_to_topic(&topics.delete_rejected).await?;

        // Generate client token
        let client_token = self.generate_client_token();

        // Publish delete request
        self.publish_to_topic(&topics.delete, b"").await?;

        // Wait for response
        self.wait_for_delete_response(accepted_stream, rejected_stream, &client_token)
            .await?;

        // Remove from local management
        let mut shadows = self.shadows.write().await;
        if let Some(persistence) = shadows.remove(shadow_name) {
            persistence.delete().await?;
        }

        Ok(())
    }

    /// Get a list of all currently managed shadow names
    pub async fn get_managed_shadows(&self) -> Vec<String> {
        let shadows = self.shadows.read().await;
        shadows.keys().cloned().collect()
    }

    /// Get the current state of all managed shadows
    pub async fn get_all_shadows(&self) -> ShadowResult<HashMap<String, Option<T>>> {
        let shadows = self.shadows.read().await;
        let mut result = HashMap::new();

        for (shadow_name, persistence) in shadows.iter() {
            let state = if let Some(document) = persistence.load().await? {
                document.reported().cloned()
            } else {
                None
            };
            result.insert(shadow_name.clone(), state);
        }

        Ok(result)
    }

    /// Set operation timeout
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.operation_timeout = timeout;
    }

    /// Get current operation timeout
    pub fn timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Get thing name
    pub fn thing_name(&self) -> &str {
        &self.thing_name
    }

    /// Get shadow pattern
    pub fn shadow_pattern(&self) -> &str {
        &self.shadow_pattern
    }

    // Helper methods (similar to ShadowClient)

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

    fn generate_client_token(&self) -> String {
        Uuid::new_v4().to_string()
    }

    // Response waiting methods (similar to ShadowClient)

    async fn wait_for_get_response(
        &self,
        mut accepted_stream: StreamOperation<IoTCoreMessage>,
        mut rejected_stream: StreamOperation<IoTCoreMessage>,
        _client_token: &str,
    ) -> ShadowResult<Option<ShadowDocument<T>>> {
        let operation = async {
            tokio::select! {
                message = accepted_stream.next() => {
                    if let Some(msg) = message {
                        let response: ShadowAcceptedResponse<T> = serde_json::from_slice(&msg.message.payload)?;

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

    async fn wait_for_update_response(
        &self,
        mut accepted_stream: StreamOperation<IoTCoreMessage>,
        mut rejected_stream: StreamOperation<IoTCoreMessage>,
        _client_token: &str,
    ) -> ShadowResult<ShadowAcceptedResponse<T>> {
        let operation = async {
            tokio::select! {
                message = accepted_stream.next() => {
                    if let Some(msg) = message {
                        let response: ShadowAcceptedResponse<T> = serde_json::from_slice(&msg.message.payload)?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct FlowState {
        flow_rate: f64,
        temperature: f64,
        active: bool,
    }

    #[test]
    fn test_shadow_name_filtering() {
        let shadow_names = vec![
            "flow-pump-01".to_string(),
            "flow-valve-02".to_string(),
            "config-main".to_string(),
            "flow-sensor-03".to_string(),
        ];

        let pattern = "flow-";
        let filtered: Vec<_> = shadow_names
            .into_iter()
            .filter(|name| name.starts_with(pattern))
            .collect();

        assert_eq!(filtered.len(), 3);
        assert!(filtered.contains(&"flow-pump-01".to_string()));
        assert!(filtered.contains(&"flow-valve-02".to_string()));
        assert!(filtered.contains(&"flow-sensor-03".to_string()));
    }

    #[tokio::test]
    async fn test_topic_parsing() {
        let thing_name = "test-device";
        let shadow_pattern = "flow-";
        let topic = "$aws/things/test-device/shadow/name/flow-pump-01/update/delta";

        // Simulate topic parsing
        let expected_prefix = format!("$aws/things/{}/shadow/name/", thing_name);
        let expected_suffix = "/update/delta";

        if let Some(middle) = topic.strip_prefix(&expected_prefix) {
            if let Some(shadow_name) = middle.strip_suffix(&expected_suffix) {
                assert_eq!(shadow_name, "flow-pump-01");
                assert!(shadow_name.starts_with(shadow_pattern));
            }
        }
    }

    #[tokio::test]
    async fn test_persistence_paths() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();
        let thing_name = "test-device";
        let shadow_name = "flow-pump-01";

        let expected_path = base_dir.join(format!("{}_{}.json", thing_name, shadow_name));
        let persistence = FileBasedPersistence::<FlowState>::new(expected_path.clone());

        assert_eq!(persistence.file_path(), expected_path);
    }
}
