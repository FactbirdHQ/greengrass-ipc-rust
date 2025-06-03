//! Multi-shadow manager with wildcard subscription support
//!
//! This module provides the `ShadowManager` which can manage multiple named shadows
//! with pattern-based filtering and efficient wildcard MQTT subscriptions.

use crate::model::ListNamedShadowsForThingRequest;
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

        // Sync all shadows from cloud
        for shadow_name in &shadow_names {
            if let Err(e) = self.get_shadow(shadow_name).await {
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
    pub async fn wait_delta(&self) -> ShadowResult<(String, Option<T::Delta>)> {
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
            return self.parse_delta_message(&message).await;
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
    pub async fn parse_delta_message(
        &self,
        msg: &IoTCoreMessage,
    ) -> ShadowResult<(String, Option<T::Delta>)> {
        let topic = &msg.message.topic_name;
        let payload = &msg.message.payload;

        // Parse shadow name from topic
        // Topic format: $aws/things/{thing_name}/shadow/name/{shadow_name}/update/delta
        let expected_prefix = format!("$aws/things/{}/shadow/name/", self.thing_name);
        let expected_suffix = "/update/delta";

        if let Some(middle) = topic.strip_prefix(&expected_prefix) {
            if let Some(shadow_name) = middle.strip_suffix(expected_suffix) {
                if shadow_name.starts_with(&self.shadow_pattern) {
                    // Parse delta response
                    let delta_response: DeltaResponse<T::Delta> =
                        serde_json::from_slice(payload).map_err(ShadowError::SerializationError)?;

                    return Ok((shadow_name.to_string(), delta_response.state));
                }
            }
        }

        Err(ShadowError::InvalidDocument(
            "Failed to parse delta message".to_string(),
        ))
    }

    /// Get the current state of a specific shadow
    pub async fn get_shadow(&self, shadow_name: &str) -> ShadowResult<T> {
        let shadows = self.shadows.read().await;
        let persistence = shadows.get(shadow_name).ok_or_else(|| {
            ShadowError::InvalidDocument(format!("Shadow '{}' not managed", shadow_name))
        })?;

        let topics = ShadowTopics::new_named(&self.thing_name, shadow_name);

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
            self.report(shadow_name, state.clone().into()).await?;
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

        let request: Request<'_, T::Reported> = Request {
            state: RequestState {
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

    /// Update desired state for a specific shadow
    pub async fn update<F: FnOnce(&T, &mut T::Reported)>(
        &self,
        shadow_name: &str,
        f: F,
    ) -> ShadowResult<T> {
        let shadows = self.shadows.read().await;
        let persistence = shadows.get(shadow_name).ok_or_else(|| {
            ShadowError::InvalidDocument(format!("Shadow '{}' not managed", shadow_name))
        })?;
        let mut update = T::Reported::default();

        let mut state = persistence.load().await?.unwrap_or_default();

        f(&state, &mut update);

        let response = self.report(shadow_name, update).await?;

        if let Some(delta) = response.delta {
            state.apply_patch(delta.clone());

            persistence.save(&state).await?;
        }

        Ok(state)
    }

    /// Delete a specific shadow
    pub async fn delete_shadow(&self, shadow_name: &str) -> ShadowResult<()> {
        let topics = ShadowTopics::new_named(&self.thing_name, shadow_name);

        // Create subscriptions for delete responses
        let (accepted_stream, rejected_stream) = futures::try_join!(
            self.subscribe_to_topic(&topics.delete_accepted),
            self.subscribe_to_topic(&topics.delete_rejected)
        )?;

        // Publish delete request
        self.publish_to_topic(&topics.delete, b"").await?;

        // Wait for response
        self.wait_for_delete_response(accepted_stream, rejected_stream)
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
            let state = if let Some(reported) = persistence.load().await? {
                Some(reported)
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

        futures::try_join!(accepted_stream.close(), rejected_stream.close())?;

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

        futures::try_join!(accepted_stream.close(), rejected_stream.close())?;

        match result {
            Ok(Ok(state)) => Ok(state),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ShadowError::Timeout),
        }
    }

    async fn wait_for_delete_response(
        &self,
        mut accepted_stream: StreamOperation<IoTCoreMessage>,
        mut rejected_stream: StreamOperation<IoTCoreMessage>,
    ) -> ShadowResult<()> {
        let result = timeout(self.operation_timeout, async {
            tokio::select! {
                Some(_message) = accepted_stream.next() => {
                    Ok(())
                }
                Some(msg) = rejected_stream.next() => {
                    let error_response: ErrorResponse = serde_json::from_slice(&msg.message.payload)?;

                    Err(ShadowError::ShadowRejected {
                        code: error_response.code,
                        message: error_response.message.to_string(),
                    })
                }
            }
        })
        .await;

        futures::try_join!(accepted_stream.close(), rejected_stream.close())?;

        match result {
            Ok(Ok(_)) => Ok(()),
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
