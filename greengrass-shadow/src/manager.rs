//! Multi-shadow manager with wildcard subscription support
//!
//! This module provides the `ShadowManager` which can manage multiple named shadows
//! with pattern-based filtering and efficient wildcard MQTT subscriptions.
//! It uses rustot's `FileKVStore` for persistence and `Topic` for MQTT operations.

use crate::error::{ShadowError, ShadowResult};
use crate::MultiShadowRoot;
use futures::stream::StreamExt as _;
use greengrass_ipc_rust::{
    GreengrassCoreIPCClient, IoTCoreMessage, PublishToIoTCoreRequest, QoS, StreamOperation,
    SubscribeToIoTCoreRequest,
};
use rustot::shadows::data_types::{
    AcceptedResponse, DeltaResponse, DeltaState, ErrorResponse, Request, RequestState,
};
use rustot::shadows::store::FileKVStore;
use rustot::shadows::topics::Topic;
use rustot::shadows::StateStore;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use uuid::Uuid;

/// Default timeout for shadow operations
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Multi-shadow manager that can handle multiple named shadows with wildcard subscriptions.
///
/// Uses rustot's `FileKVStore` for persistence and `Topic` for MQTT topic formatting.
/// Each shadow is stored under a prefix like `"{pattern}{id}"` in the KV store.
#[derive(Clone)]
pub struct ShadowManager<T: MultiShadowRoot> {
    /// The underlying IPC client
    ipc_client: Arc<GreengrassCoreIPCClient>,

    /// Thing name for all shadows
    thing_name: String,

    /// Base directory for persistence files
    persistence_base_dir: PathBuf,

    /// File-based KV store for persistence
    store: Arc<FileKVStore>,

    /// Currently managed shadow IDs (without pattern prefix)
    shadow_ids: Arc<RwLock<HashSet<String>>>,

    /// Operation timeout
    operation_timeout: Duration,

    /// Phantom data for type parameter
    _marker: PhantomData<T>,
}

impl<T> ShadowManager<T>
where
    T: MultiShadowRoot + Serialize + DeserializeOwned + Send + Sync,
    T::Delta: Send + Sync,
    T::Reported: Default + Send + Sync,
{
    /// Create a new shadow manager.
    pub fn new(
        ipc_client: Arc<GreengrassCoreIPCClient>,
        thing_name: String,
        persistence_base_dir: PathBuf,
    ) -> ShadowResult<Self> {
        let store = FileKVStore::new(persistence_base_dir.clone());

        Ok(Self {
            ipc_client,
            thing_name,
            persistence_base_dir,
            store: Arc::new(store),
            shadow_ids: Arc::new(RwLock::new(HashSet::new())),
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            _marker: PhantomData,
        })
    }

    /// Get the full shadow name from an ID.
    fn shadow_name(id: &str) -> String {
        format!("{}{}", T::PATTERN, id)
    }

    /// Initialize the manager by discovering existing shadows.
    pub async fn initialize(&self) -> ShadowResult<()> {
        let shadow_names = self.discover_shadows().await?;

        for shadow_name in &shadow_names {
            if let Some(id) = shadow_name.strip_prefix(T::PATTERN) {
                self.add_shadow_id(id).await?;
            }
        }

        for shadow_name in &shadow_names {
            if let Some(id) = shadow_name.strip_prefix(T::PATTERN) {
                if let Err(e) = self.get_shadow(id).await {
                    tracing::warn!("Failed to sync shadow '{}': {}", shadow_name, e);
                }
            }
        }

        Ok(())
    }

    /// Initialize with seeding from existing shadows.
    pub async fn initialize_with_seed(
        &self,
        existing_shadow_names: Vec<String>,
    ) -> ShadowResult<Vec<String>> {
        let cloud_shadows = self.discover_shadows().await?;

        let deleted_shadows: Vec<String> = existing_shadow_names
            .iter()
            .filter(|shadow_name| !cloud_shadows.contains(shadow_name))
            .cloned()
            .collect();

        for shadow_name in &cloud_shadows {
            if let Some(id) = shadow_name.strip_prefix(T::PATTERN) {
                self.add_shadow_id(id).await?;
            }
        }

        Ok(deleted_shadows)
    }

    /// Discover shadows matching the pattern from AWS IoT.
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
            .filter(|name| name.starts_with(T::PATTERN))
            .collect();

        Ok(matching_shadows)
    }

    /// Add a shadow ID to the tracked set.
    async fn add_shadow_id(&self, id: &str) -> ShadowResult<()> {
        let mut shadow_ids = self.shadow_ids.write().await;
        shadow_ids.insert(id.to_string());
        Ok(())
    }

    /// Add a new shadow to be managed by ID.
    pub async fn add_shadow_by_id(&self, id: &str) -> ShadowResult<()> {
        self.add_shadow_id(id).await
    }

    /// Wait for the next delta change from any managed shadow.
    pub async fn wait_delta(&self) -> ShadowResult<(String, T, Option<T::Delta>)> {
        let mut delta_stream = self.create_delta_subscription().await?;

        while let Some(message) = delta_stream.next().await {
            return self.parse_delta_message(&message).await;
        }

        Err(ShadowError::Timeout)
    }

    /// Create a delta subscription using wildcard.
    pub async fn create_delta_subscription(&self) -> ShadowResult<StreamOperation<IoTCoreMessage>> {
        let topic = Topic::UpdateDelta
            .format::<128>(T::PREFIX, &self.thing_name, Some("+"))
            .map_err(|_| ShadowError::InvalidTopic("Failed to format delta topic".to_string()))?
            .to_string();

        let request = SubscribeToIoTCoreRequest {
            topic_name: topic,
            qos: QoS::AtLeastOnce,
        };

        let delta_stream = self.ipc_client.subscribe_to_iot_core(request).await?;
        Ok(delta_stream)
    }

    /// Create a deletion subscription for monitoring shadow deletions.
    pub async fn create_deletion_subscription(
        &self,
    ) -> ShadowResult<StreamOperation<IoTCoreMessage>> {
        let topic = Topic::DeleteAccepted
            .format::<128>(T::PREFIX, &self.thing_name, Some("+"))
            .map_err(|_| ShadowError::InvalidTopic("Failed to format delete topic".to_string()))?
            .to_string();

        let request = SubscribeToIoTCoreRequest {
            topic_name: topic,
            qos: QoS::AtLeastOnce,
        };

        let delete_stream = self.ipc_client.subscribe_to_iot_core(request).await?;
        Ok(delete_stream)
    }

    /// Parse a deletion message and handle shadow cleanup.
    pub async fn handle_deletion_message(
        &self,
        message: &IoTCoreMessage,
    ) -> ShadowResult<Option<String>> {
        if let Some((Topic::DeleteAccepted, _thing, Some(shadow_name))) =
            Topic::from_str(T::PREFIX, &message.message.topic_name)
        {
            if let Some(id) = shadow_name.strip_prefix(T::PATTERN) {
                let mut shadow_ids = self.shadow_ids.write().await;
                shadow_ids.remove(id);
                return Ok(Some(id.to_string()));
            }
        }

        Ok(None)
    }

    /// Parse a delta message, apply it to state, and return both full state and delta.
    pub async fn parse_delta_message(
        &self,
        msg: &IoTCoreMessage,
    ) -> ShadowResult<(String, T, Option<T::Delta>)> {
        let payload = &msg.message.payload;

        if let Some((Topic::UpdateDelta, _thing, Some(shadow_name))) =
            Topic::from_str(T::PREFIX, &msg.message.topic_name)
        {
            if let Some(id) = shadow_name.strip_prefix(T::PATTERN) {
                let prefix = Self::shadow_name(id);
                let resolver = <FileKVStore as StateStore<T>>::resolver(&self.store, &prefix);
                let delta_response = DeltaResponse::parse::<T, _>(payload, &resolver)
                    .await
                    .map_err(|e| ShadowError::InvalidDocument(format!("Failed to parse delta: {:?}", e)))?;

                let state: T = if let Some(ref delta) = delta_response.state {
                    self.store.apply_delta(&prefix, delta).await.map_err(|e| {
                        ShadowError::StorageError(format!("Failed to apply delta: {:?}", e))
                    })?
                } else {
                    self.store.get_state(&prefix).await.map_err(|e| {
                        ShadowError::StorageError(format!("Failed to get state: {:?}", e))
                    })?
                };

                self.report(id, state.clone().into_reported()).await?;

                if !self.shadow_ids.read().await.contains(id) {
                    self.add_shadow_id(id).await?;
                }

                return Ok((id.to_string(), state, delta_response.state));
            }
        }

        Err(ShadowError::InvalidDocument(
            "Failed to parse delta message".to_string(),
        ))
    }

    /// Get the current state of a specific shadow by ID.
    pub async fn get_shadow(&self, id: &str) -> ShadowResult<(T, Option<T::Delta>)> {
        let shadow_name = Self::shadow_name(id);

        // Create topic strings first to avoid temporary value issues
        let get_accepted_topic = Topic::GetAccepted
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("get accepted".into()))?
            .to_string();

        let get_rejected_topic = Topic::GetRejected
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("get rejected".into()))?
            .to_string();

        let (accepted_stream, rejected_stream) = futures::try_join!(
            self.subscribe_to_topic(&get_accepted_topic),
            self.subscribe_to_topic(&get_rejected_topic)
        )?;

        let get_topic = Topic::Get
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("get".into()))?
            .to_string();

        self.publish_to_topic(&get_topic, b"").await?;

        let delta_state = self
            .wait_for_get_response(&shadow_name, accepted_stream, rejected_stream)
            .await?;

        let prefix = Self::shadow_name(id);

        let mut state: T = self.store.get_state(&prefix).await.map_err(|e| {
            ShadowError::StorageError(format!("Failed to get state: {:?}", e))
        })?;

        if let Some(ref delta) = delta_state.delta {
            state = self.store.apply_delta(&prefix, delta).await.map_err(|e| {
                ShadowError::StorageError(format!("Failed to apply delta: {:?}", e))
            })?;
            self.report(id, state.clone().into_reported()).await?;
        }

        Ok((state, delta_state.delta))
    }

    /// Report state for a specific shadow.
    async fn report(
        &self,
        id: &str,
        reported: T::Reported,
    ) -> ShadowResult<DeltaState<T::Delta, T::Delta>> {
        let shadow_name = Self::shadow_name(id);
        let client_token = self.generate_client_token();

        let request: Request<'_, T, T::Reported> = Request {
            state: RequestState {
                desired: None,
                reported: Some(reported),
            },
            client_token: Some(&client_token),
            version: None,
        };

        // Create topic strings first
        let update_accepted_topic = Topic::UpdateAccepted
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("update accepted".into()))?
            .to_string();

        let update_rejected_topic = Topic::UpdateRejected
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("update rejected".into()))?
            .to_string();

        let (accepted_stream, rejected_stream) = futures::try_join!(
            self.subscribe_to_topic(&update_accepted_topic),
            self.subscribe_to_topic(&update_rejected_topic)
        )?;

        let payload = serde_json::to_vec(&request)?;

        let update_topic = Topic::Update
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("update".into()))?
            .to_string();

        self.publish_to_topic(&update_topic, &payload).await?;

        self.wait_for_update_response(&shadow_name, accepted_stream, rejected_stream, Some(&client_token))
            .await
    }

    /// Update reported state for a specific shadow by ID.
    pub async fn update<F>(&self, id: &str, f: F) -> ShadowResult<T>
    where
        F: FnOnce(&T, &mut T::Reported),
    {
        if !self.shadow_ids.read().await.contains(id) {
            return Err(ShadowError::ShadowNotManaged(id.to_string()));
        }

        let prefix = Self::shadow_name(id);
        let state: T = self.store.get_state(&prefix).await.map_err(|e| {
            ShadowError::StorageError(format!("Failed to get state: {:?}", e))
        })?;

        let mut reported = T::Reported::default();
        f(&state, &mut reported);

        let response = self.report(id, reported).await?;

        let state = if let Some(ref delta) = response.delta {
            self.store.apply_delta(&prefix, delta).await.map_err(|e| {
                ShadowError::StorageError(format!("Failed to apply delta: {:?}", e))
            })?
        } else {
            state
        };

        Ok(state)
    }

    /// Update desired state for a specific shadow by ID.
    pub async fn update_desired<F>(&self, id: &str, f: F) -> ShadowResult<()>
    where
        F: FnOnce(&mut T, &mut T::Reported),
    {
        if !self.shadow_ids.read().await.contains(id) {
            return Err(ShadowError::ShadowNotManaged(id.to_string()));
        }

        let prefix = Self::shadow_name(id);
        let mut state: T = self.store.get_state(&prefix).await.map_err(|e| {
            ShadowError::StorageError(format!("Failed to get state: {:?}", e))
        })?;

        let mut reported = T::Reported::default();
        f(&mut state, &mut reported);

        self.desired(id, state.clone(), reported).await?;

        self.store.set_state(&prefix, &state).await.map_err(|e| {
            ShadowError::StorageError(format!("Failed to set state: {:?}", e))
        })?;

        Ok(())
    }

    /// Update both desired and reported state.
    async fn desired(
        &self,
        id: &str,
        desired: T,
        reported: T::Reported,
    ) -> ShadowResult<DeltaState<T::Delta, T::Delta>> {
        let shadow_name = Self::shadow_name(id);
        let client_token = self.generate_client_token();

        let request: Request<'_, T, T::Reported> = Request {
            state: RequestState {
                desired: Some(desired),
                reported: Some(reported),
            },
            client_token: Some(&client_token),
            version: None,
        };

        // Create topic strings first
        let update_accepted_topic = Topic::UpdateAccepted
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("update accepted".into()))?
            .to_string();

        let update_rejected_topic = Topic::UpdateRejected
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("update rejected".into()))?
            .to_string();

        let (accepted_stream, rejected_stream) = futures::try_join!(
            self.subscribe_to_topic(&update_accepted_topic),
            self.subscribe_to_topic(&update_rejected_topic)
        )?;

        let payload = serde_json::to_vec(&request)?;

        let update_topic = Topic::Update
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("update".into()))?
            .to_string();

        self.publish_to_topic(&update_topic, &payload).await?;

        self.wait_for_update_response(&shadow_name, accepted_stream, rejected_stream, Some(&client_token))
            .await
    }

    /// Delete a specific shadow by ID.
    pub async fn delete_shadow(&self, id: &str) -> ShadowResult<()> {
        let shadow_name = Self::shadow_name(id);

        let delete_rejected_topic = Topic::DeleteRejected
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("delete rejected".into()))?
            .to_string();

        let mut rejected_stream = self.subscribe_to_topic(&delete_rejected_topic).await?;

        let delete_topic = Topic::Delete
            .format::<128>(T::PREFIX, &self.thing_name, Some(&shadow_name))
            .map_err(|_| ShadowError::InvalidTopic("delete".into()))?
            .to_string();

        self.publish_to_topic(&delete_topic, b"").await?;

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
            Ok(Ok(())) | Err(_) => {
                let mut shadow_ids = self.shadow_ids.write().await;
                shadow_ids.remove(id);
                Ok(())
            }
            Ok(Err(e)) => Err(e),
        }
    }

    /// Get managed shadow IDs (without the pattern prefix).
    pub async fn get_managed_ids(&self) -> Vec<String> {
        let shadow_ids = self.shadow_ids.read().await;
        shadow_ids.iter().cloned().collect()
    }

    /// Check if an ID is currently managed.
    pub async fn is_id_managed(&self, id: &str) -> bool {
        let shadow_ids = self.shadow_ids.read().await;
        shadow_ids.contains(id)
    }

    /// Get the current state of all managed shadows.
    pub async fn get_all_shadows(&self) -> ShadowResult<std::collections::HashMap<String, T>> {
        let shadow_ids = self.shadow_ids.read().await;
        let mut result = std::collections::HashMap::new();

        for id in shadow_ids.iter() {
            let prefix = Self::shadow_name(id);
            if let Ok(state) = self.store.get_state(&prefix).await {
                result.insert(id.clone(), state);
            }
        }

        Ok(result)
    }

    /// Set operation timeout.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.operation_timeout = timeout;
    }

    /// Get current operation timeout.
    pub fn timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Get thing name.
    pub fn thing_name(&self) -> &str {
        &self.thing_name
    }

    /// Get shadow pattern.
    pub fn shadow_pattern(&self) -> &'static str {
        T::PATTERN
    }

    /// Get the base persistence directory.
    pub fn persistence_dir(&self) -> &PathBuf {
        &self.persistence_base_dir
    }

    // --- Helper methods ---

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

    async fn wait_for_get_response(
        &self,
        prefix: &str,
        accepted_stream: StreamOperation<IoTCoreMessage>,
        rejected_stream: StreamOperation<IoTCoreMessage>,
    ) -> ShadowResult<DeltaState<T::Delta, T::Delta>> {
        struct StreamGuard {
            accepted: Option<StreamOperation<IoTCoreMessage>>,
            rejected: Option<StreamOperation<IoTCoreMessage>>,
        }

        impl Drop for StreamGuard {
            fn drop(&mut self) {
                if let Some(accepted) = self.accepted.take() {
                    tokio::spawn(async move {
                        let _ = accepted.close().await;
                    });
                }
                if let Some(rejected) = self.rejected.take() {
                    tokio::spawn(async move {
                        let _ = rejected.close().await;
                    });
                }
            }
        }

        let mut guard = StreamGuard {
            accepted: Some(accepted_stream),
            rejected: Some(rejected_stream),
        };

        let resolver = <FileKVStore as StateStore<T>>::resolver(&self.store, prefix);
        let result = timeout(self.operation_timeout, async {
            tokio::select! {
                Some(msg) = guard.accepted.as_mut().unwrap().next() => {
                    let response = AcceptedResponse::parse::<T, _>(&msg.message.payload, &resolver)
                        .await
                        .map_err(|e| ShadowError::InvalidDocument(format!("Failed to parse accepted response: {:?}", e)))?;
                    Ok(response.state)
                }
                Some(msg) = guard.rejected.as_mut().unwrap().next() => {
                    let error_response: ErrorResponse = serde_json::from_slice(&msg.message.payload)?;

                    match error_response.code {
                        404 => return Err(ShadowError::ShadowNotFound),
                        _ => {}
                    }

                    Err(ShadowError::ShadowRejected {
                        code: error_response.code,
                        message: error_response.message.to_string(),
                    })
                }
            }
        })
        .await;

        if let Some(accepted) = guard.accepted.take() {
            let _ = accepted.close().await;
        }
        if let Some(rejected) = guard.rejected.take() {
            let _ = rejected.close().await;
        }

        match result {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ShadowError::Timeout),
        }
    }

    async fn wait_for_update_response(
        &self,
        prefix: &str,
        accepted_stream: StreamOperation<IoTCoreMessage>,
        rejected_stream: StreamOperation<IoTCoreMessage>,
        client_token: Option<&str>,
    ) -> ShadowResult<DeltaState<T::Delta, T::Delta>> {
        struct StreamGuard {
            accepted: Option<StreamOperation<IoTCoreMessage>>,
            rejected: Option<StreamOperation<IoTCoreMessage>>,
        }

        impl Drop for StreamGuard {
            fn drop(&mut self) {
                if let Some(accepted) = self.accepted.take() {
                    tokio::spawn(async move {
                        let _ = accepted.close().await;
                    });
                }
                if let Some(rejected) = self.rejected.take() {
                    tokio::spawn(async move {
                        let _ = rejected.close().await;
                    });
                }
            }
        }

        let mut guard = StreamGuard {
            accepted: Some(accepted_stream),
            rejected: Some(rejected_stream),
        };

        let resolver = <FileKVStore as StateStore<T>>::resolver(&self.store, prefix);
        let result = timeout(self.operation_timeout, async {
            loop {
                tokio::select! {
                    Some(msg) = guard.accepted.as_mut().unwrap().next() => {
                        let response = AcceptedResponse::parse::<T, _>(&msg.message.payload, &resolver)
                            .await
                            .map_err(|e| ShadowError::InvalidDocument(format!("Failed to parse accepted response: {:?}", e)))?;

                        if response.client_token.is_some() && response.client_token != client_token {
                            continue;
                        }

                        return Ok(response.state);
                    }
                    Some(msg) = guard.rejected.as_mut().unwrap().next() => {
                        let error_response: ErrorResponse = serde_json::from_slice(&msg.message.payload)?;

                        if error_response.client_token.is_some() && error_response.client_token != client_token {
                            continue;
                        }

                        return Err(ShadowError::ShadowRejected {
                            code: error_response.code,
                            message: error_response.message.to_string(),
                        });
                    }
                }
            }
        })
        .await;

        if let Some(accepted) = guard.accepted.take() {
            let _ = accepted.close().await;
        }
        if let Some(rejected) = guard.rejected.take() {
            let _ = rejected.close().await;
        }

        match result {
            Ok(Ok(state)) => Ok(state),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ShadowError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_shadow_name_generation() {
        let pattern = "flow-";
        let id = "pump-01";
        let shadow_name = format!("{}{}", pattern, id);
        assert_eq!(shadow_name, "flow-pump-01");
    }
}
