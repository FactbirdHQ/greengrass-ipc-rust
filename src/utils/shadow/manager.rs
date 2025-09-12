//! Multi-shadow manager with wildcard subscription support
//!
//! This module provides the `ShadowManager` which can manage multiple named shadows
//! with pattern-based filtering and efficient wildcard MQTT subscriptions.

use crate::utils::shadow::{
    error::{ShadowError, ShadowResult},
    persistence::FileBasedPersistence,
    topics::ShadowTopics,
    ShadowState,
};
use crate::{
    GreengrassCoreIPCClient, IoTCoreMessage, PublishToIoTCoreRequest, QoS, StreamOperation,
    SubscribeToIoTCoreRequest,
};
use futures::stream::StreamExt as _;
use rustot::shadows::data_types::{
    AcceptedResponse, DeltaResponse, DeltaState, ErrorResponse, Request, RequestState,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
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
    T: ShadowState + Serialize + DeserializeOwned,
{
    /// Create a new shadow manager
    pub fn new(
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

        // Sync all shadows from cloud
        for shadow_name in &shadow_names {
            if let Some(id) = shadow_name.strip_prefix(&self.shadow_pattern) {
                if let Err(e) = self.get_shadow(id).await {
                    eprintln!("Warning: Failed to sync shadow '{}': {}", shadow_name, e);
                }
            }
        }

        Ok(())
    }

    /// Initialize with seeding from existing shadows
    /// This method reconciles persisted shadows with actual existing shadows
    /// Returns a list of shadow names that were deleted while offline
    pub async fn initialize_with_seed(
        &self,
        existing_shadow_names: Vec<String>,
    ) -> ShadowResult<Vec<String>> {
        // Discover existing shadows from cloud
        let cloud_shadows = self.discover_shadows().await?;

        // Find shadows that were deleted while offline (exist in seed but not in cloud)
        let deleted_shadows: Vec<String> = existing_shadow_names
            .iter()
            .filter(|shadow_name| !cloud_shadows.contains(shadow_name))
            .cloned()
            .collect();

        // Note: shadows_to_manage already contains all cloud shadows, including any that were seeded

        // Load persistence for each shadow that exists
        for shadow_name in &cloud_shadows {
            self.add_shadow(shadow_name.clone()).await?;
        }

        // Sync all shadows from cloud
        for shadow_name in &cloud_shadows {
            if let Some(id) = shadow_name.strip_prefix(&self.shadow_pattern) {
                if let Err(e) = self.get_shadow(id).await {
                    eprintln!("Warning: Failed to sync shadow '{}': {}", shadow_name, e);
                }
            }
        }

        Ok(deleted_shadows)
    }

    /// Discover shadows matching the pattern
    async fn discover_shadows(&self) -> ShadowResult<Vec<String>> {
        let config = aws_config::load_from_env().await;
        let iot_client = aws_sdk_iotdataplane::Client::new(&config);

        let response = iot_client
            .list_named_shadows_for_thing()
            .thing_name(self.thing_name())
            .page_size(100)
            .send()
            .await
            .map_err(|e| ShadowError::AwsSdkError(e.into()))?;

        let matching_shadows = response
            .results
            .unwrap_or_default()
            .into_iter()
            .filter(|name| name.starts_with(&self.shadow_pattern))
            .collect();

        Ok(matching_shadows)
    }

    /// Add a new shadow to be managed by ID
    pub async fn add_shadow_by_id(&self, id: &str) -> ShadowResult<()> {
        let shadow_name = format!("{}{}", self.shadow_pattern, id);
        self.add_shadow(shadow_name).await
    }

    /// Add a new shadow to be managed
    async fn add_shadow(&self, shadow_name: String) -> ShadowResult<()> {
        let persistence_path = self
            .persistence_base_dir
            .join(format!("{}.json", shadow_name));

        let persistence = FileBasedPersistence::new(persistence_path);

        let mut shadows = self.shadows.write().await;
        shadows.insert(shadow_name, persistence);

        Ok(())
    }

    /// Wait for the next delta change from any managed shadow
    pub async fn wait_delta(&self) -> ShadowResult<(String, T, Option<T::Delta>)> {
        let mut delta_stream = self.create_delta_subscription().await?;

        // Wait for the next delta message
        while let Some(message) = delta_stream.next().await {
            return self.parse_delta_message(&message).await;
        }

        Err(ShadowError::Timeout)
    }

    /// Create a delta subscription for monitoring changes
    /// Returns a StreamOperation that can be used to monitor delta changes
    pub async fn create_delta_subscription(&self) -> ShadowResult<StreamOperation<IoTCoreMessage>> {
        let topics = ShadowTopics::new_named(&self.thing_name, "+");

        let request = SubscribeToIoTCoreRequest {
            topic_name: topics.delta,
            qos: QoS::AtLeastOnce,
        };

        let delta_stream = self.ipc_client.subscribe_to_iot_core(request).await?;

        Ok(delta_stream)
    }

    /// Create a deletion subscription for monitoring shadow deletions
    /// Returns a StreamOperation that can be used to monitor deletion events
    pub async fn create_deletion_subscription(
        &self,
    ) -> ShadowResult<StreamOperation<IoTCoreMessage>> {
        let topics = ShadowTopics::new_named(&self.thing_name, "+");

        let request = SubscribeToIoTCoreRequest {
            topic_name: topics.delete_accepted,
            qos: QoS::AtLeastOnce,
        };

        let delete_stream = self.ipc_client.subscribe_to_iot_core(request).await?;

        Ok(delete_stream)
    }

    /// Parse a deletion message and handle shadow cleanup
    /// Returns the shadow ID (with pattern prefix stripped) if successful
    pub async fn handle_deletion_message(
        &self,
        message: &IoTCoreMessage,
    ) -> ShadowResult<Option<String>> {
        let topic_parts: Vec<&str> = message.message.topic_name.split('/').collect();

        if topic_parts.len() >= 6 {
            let shadow_name = topic_parts[5];

            // Check if this shadow matches our pattern
            if let Some(shadow_id) = shadow_name.strip_prefix(&self.shadow_pattern) {
                let mut shadows = self.shadows.write().await;
                if let Some(persistence) = shadows.remove(shadow_name) {
                    persistence.delete().await?;
                }
                return Ok(Some(shadow_id.to_string()));
            }
        }

        Ok(None)
    }

    /// Parse a delta message, apply it to state, and return both full state and delta
    /// Returns the shadow ID, full updated state, and delta state
    pub async fn parse_delta_message(
        &self,
        msg: &IoTCoreMessage,
    ) -> ShadowResult<(String, T, Option<T::Delta>)> {
        let topic = &msg.message.topic_name;
        let payload = &msg.message.payload;

        // Parse shadow name from topic
        // Topic format: $aws/things/{thing_name}/shadow/name/{shadow_name}/update/delta
        let expected_prefix = format!("$aws/things/{}/shadow/name/", self.thing_name);
        let expected_suffix = "/update/delta";

        if let Some(middle) = topic.strip_prefix(&expected_prefix) {
            if let Some(shadow_name) = middle.strip_suffix(expected_suffix) {
                if let Some(shadow_id) = shadow_name.strip_prefix(&self.shadow_pattern) {
                    // Parse delta response
                    let delta_response: DeltaResponse<T::Delta> =
                        serde_json::from_slice(payload).map_err(ShadowError::SerializationError)?;

                    // Get or create persistence for this shadow
                    if self.shadows.read().await.get(shadow_name).is_none() {
                        // Add shadow if not tracked
                        self.add_shadow(shadow_name.to_string()).await?;
                    } else {
                    }

                    let shadows = self.shadows.read().await;
                    let persistence = shadows.get(shadow_name).unwrap();

                    // Load existing state or use default
                    let mut state = match persistence.load().await? {
                        Some(state) => state,
                        None => {
                            let default_state = T::default();
                            persistence.save(&default_state).await?;
                            default_state
                        }
                    };

                    // Apply delta if present
                    if let Some(ref delta) = delta_response.state {
                        state.apply_patch(delta.clone());

                        // Report updated state
                        self.report(&shadow_name, state.clone().into_reported())
                            .await?;

                        // Persist updated state
                        persistence.save(&state).await?;
                    }

                    return Ok((shadow_id.to_string(), state, delta_response.state));
                }
            }
        }
        Err(ShadowError::InvalidDocument(
            "Failed to parse delta message".to_string(),
        ))
    }

    /// Get the current state of a specific shadow by ID
    pub async fn get_shadow(&self, id: &str) -> ShadowResult<T> {
        let shadow_name = format!("{}{}", self.shadow_pattern, id);
        let shadows = self.shadows.read().await;
        let persistence = shadows.get(&shadow_name).ok_or_else(|| {
            ShadowError::InvalidDocument(format!("Shadow ID '{}' not managed", id))
        })?;

        let topics = ShadowTopics::new_named(&self.thing_name, &shadow_name);

        // Create subscriptions for get responses
        let (accepted_stream, rejected_stream) = futures::try_join!(
            self.subscribe_to_topic(&topics.get_accepted),
            self.subscribe_to_topic(&topics.get_rejected)
        )?;

        // Publish get request
        self.publish_to_topic(&topics.get, b"").await?;

        // Wait for response
        let delta_state = self
            .wait_for_get_response(accepted_stream, rejected_stream)
            .await?;

        let mut state = persistence.load().await?.unwrap_or_default();
        if let Some(delta) = delta_state.delta {
            state.apply_patch(delta.clone());
            persistence.save(&state).await?;
            self.report(&shadow_name, state.clone().into_reported())
                .await?;
        }

        Ok(state)
    }

    /// Report state for a specific shadow
    async fn report(
        &self,
        shadow_name: &str,
        reported: T::Reported,
    ) -> ShadowResult<DeltaState<T::Delta, T::Delta>> {
        let client_token = self.generate_client_token();

        let request: Request<'_, T, T::Reported> = Request {
            state: RequestState {
                desired: None,
                reported: Some(reported),
            },
            client_token: Some(&client_token),
            version: None,
        };

        let topics = ShadowTopics::new_named(&self.thing_name, shadow_name);

        // Create subscriptions for update responses
        let (accepted_stream, rejected_stream) = futures::try_join!(
            self.subscribe_to_topic(&topics.update_accepted),
            self.subscribe_to_topic(&topics.update_rejected)
        )?;

        // Serialize and publish update request
        let payload = serde_json::to_vec(&request)?;

        self.publish_to_topic(&topics.update, &payload).await?;

        // Wait for response
        self.wait_for_update_response(accepted_stream, rejected_stream, Some(&client_token))
            .await
    }

    /// Report state for a specific shadow
    async fn desired(
        &self,
        shadow_name: &str,
        desired: T,
        reported: T::Reported,
    ) -> ShadowResult<DeltaState<T::Delta, T::Delta>> {
        let client_token = self.generate_client_token();

        let request: Request<'_, T, T::Reported> = Request {
            state: RequestState {
                desired: Some(desired),
                reported: Some(reported),
            },
            client_token: Some(&client_token),
            version: None,
        };

        let topics = ShadowTopics::new_named(&self.thing_name, shadow_name);

        // Create subscriptions for update responses
        let (accepted_stream, rejected_stream) = futures::try_join!(
            self.subscribe_to_topic(&topics.update_accepted),
            self.subscribe_to_topic(&topics.update_rejected)
        )?;

        // Serialize and publish update request
        let payload = serde_json::to_vec(&request)?;

        self.publish_to_topic(&topics.update, &payload).await?;

        // Wait for response
        self.wait_for_update_response(accepted_stream, rejected_stream, Some(&client_token))
            .await
    }

    /// Update desired state for a specific shadow by ID
    pub async fn update_desired<F>(&self, id: &str, f: F) -> ShadowResult<()>
    where
        F: FnOnce(&mut T, &mut T::Reported),
    {
        let shadow_name = format!("{}{}", self.shadow_pattern, id);
        let shadows = self.shadows.read().await;
        let persistence = shadows.get(&shadow_name).ok_or_else(|| {
            ShadowError::InvalidDocument(format!("Shadow ID '{}' not managed", id))
        })?;

        let mut update = T::Reported::default();

        let mut state = persistence.load().await?.unwrap_or_default();

        f(&mut state, &mut update);

        self.desired(&shadow_name, state.clone(), update).await?;

        persistence.save(&state).await?;

        Ok(())
    }

    /// Update reported state for a specific shadow by ID
    pub async fn update<F>(&self, id: &str, f: F) -> ShadowResult<T>
    where
        F: FnOnce(&T, &mut T::Reported),
    {
        let shadow_name = format!("{}{}", self.shadow_pattern, id);
        let shadows = self.shadows.read().await;
        let persistence = shadows.get(&shadow_name).ok_or_else(|| {
            ShadowError::InvalidDocument(format!("Shadow ID '{}' not managed", id))
        })?;
        let mut update = T::Reported::default();

        let mut state = persistence.load().await?.unwrap_or_default();

        f(&state, &mut update);

        let response = self.report(&shadow_name, update).await?;

        if let Some(delta) = response.delta {
            state.apply_patch(delta.clone());

            persistence.save(&state).await?;
        }

        Ok(state)
    }

    /// Delete a specific shadow by ID
    pub async fn delete_shadow(&self, id: &str) -> ShadowResult<()> {
        let shadow_name = format!("{}{}", self.shadow_pattern, id);
        let topics = ShadowTopics::new_named(&self.thing_name, &shadow_name);

        // Create subscription for delete rejected responses only
        // (accepted responses will be handled by the deletion subscription)
        let mut rejected_stream = self.subscribe_to_topic(&topics.delete_rejected).await?;

        // Publish delete request
        self.publish_to_topic(&topics.delete, b"").await?;

        // Wait for either success (via timeout) or rejection
        let result = timeout(self.operation_timeout, async {
            if let Some(rejected_msg) = rejected_stream.next().await {
                let error_response: ErrorResponse =
                    serde_json::from_slice(&rejected_msg.message.payload)?;
                return Err(ShadowError::ShadowRejected {
                    code: error_response.code,
                    message: error_response.message.to_string(),
                });
            }
            Ok(())
        })
        .await;

        rejected_stream.close().await?;

        match result {
            Ok(Ok(())) => Ok(()), // Success - deletion subscription will handle cleanup
            Ok(Err(e)) => Err(e), // Rejection
            Err(_) => Ok(()),     // Timeout means success (no rejection received)
        }
    }

    /// Get managed shadow IDs (strips the shadow pattern prefix)
    pub async fn get_managed_ids(&self) -> Vec<String> {
        let shadows = self.shadows.read().await;
        shadows
            .keys()
            .filter_map(|shadow_name| shadow_name.strip_prefix(&self.shadow_pattern))
            .map(|s| s.to_string())
            .collect()
    }

    /// Check if an ID is currently managed (alias for is_shadow_managed)
    pub async fn is_id_managed(&self, id: &str) -> bool {
        let shadow_name = format!("{}{}", self.shadow_pattern, id);
        let shadows = self.shadows.read().await;
        shadows.contains_key(&shadow_name)
    }

    /// Get the current state of all managed shadows (returns map of ID -> state)
    pub async fn get_all_shadows(&self) -> ShadowResult<HashMap<String, Option<T>>> {
        let shadows = self.shadows.read().await;
        let mut result = HashMap::new();

        for (shadow_name, persistence) in shadows.iter() {
            if let Some(id) = shadow_name.strip_prefix(&self.shadow_pattern) {
                let state = if let Some(reported) = persistence.load().await? {
                    Some(reported)
                } else {
                    None
                };
                result.insert(id.to_string(), state);
            }
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
    ) -> ShadowResult<DeltaState<T::Delta, T::Delta>> {
        let result = timeout(self.operation_timeout, async {
            tokio::select! {
                Some(msg) = accepted_stream.next() => {
                    let response: AcceptedResponse<T::Delta, T::Delta> = serde_json::from_slice(&msg.message.payload)?;
                    Ok(response.state)
                }
                Some(msg) = rejected_stream.next() => {
                    let error_response: ErrorResponse = serde_json::from_slice(&msg.message.payload)?;

                    Err(ShadowError::ShadowRejected {
                        code: error_response.code,
                        message: error_response.message.to_string(),
                    })
                }
            }
        }).await;

        match result {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ShadowError::Timeout),
        }
    }

    async fn wait_for_update_response(
        &self,
        mut accepted_stream: StreamOperation<IoTCoreMessage>,
        mut rejected_stream: StreamOperation<IoTCoreMessage>,
        client_token: Option<&str>,
    ) -> ShadowResult<DeltaState<T::Delta, T::Delta>> {
        let result = timeout(self.operation_timeout, async {
            loop {
                tokio::select! {
                    Some(msg) = accepted_stream.next() => {
                        let response: AcceptedResponse<T::Delta, T::Delta> = serde_json::from_slice(&msg.message.payload)?;

                        // Verify client token if present
                        if response.client_token.is_some() && response.client_token != client_token {
                            continue; // Not our response, but operation succeeded
                        }

                        return Ok(response.state);
                    }
                    Some(msg) = rejected_stream.next() => {
                        let error_response: ErrorResponse = serde_json::from_slice(&msg.message.payload)?;

                        // Verify client token if present
                        if error_response.client_token.is_some() && error_response.client_token != client_token {
                            continue; // Not our response, continue waiting
                        }

                        return Err(ShadowError::ShadowRejected {
                            code: error_response.code,
                            message: error_response.message.to_string(),
                        });
                    }
                }
            }
        }).await;

        match result {
            Ok(Ok(state)) => Ok(state),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ShadowError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustot::shadows::rustot_derive::shadow;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[shadow]
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
