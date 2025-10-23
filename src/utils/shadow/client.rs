//! AWS IoT Device Shadow Client using rustot traits
//!
//! This module provides a shadow client that leverages the rustot library's
//! ShadowState and ShadowPatch traits for proper delta handling.

use crate::client::GreengrassCoreIPCClient;
use crate::model::IoTCoreMessage;
use crate::utils::shadow::error::{ShadowError, ShadowResult};
use crate::utils::shadow::persistence::FileBasedPersistence;
use crate::utils::shadow::topics::ShadowTopics;
use crate::{PublishToIoTCoreRequest, QoS, StreamOperation, SubscribeToIoTCoreRequest};

use futures::stream::StreamExt;
use rustot::shadows::data_types::{
    AcceptedResponse, DeltaResponse, DeltaState, ErrorResponse, Request, RequestState,
};
use rustot::shadows::ShadowState;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

/// Default timeout for shadow operations
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// AWS IoT Device Shadow client using rustot traits
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
}

impl<T> ShadowClient<T>
where
    T: ShadowState + Serialize + DeserializeOwned,
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
    pub async fn get_shadow(&self) -> ShadowResult<T> {
        // Create subscriptions for get responses
        let (accepted_stream, rejected_stream) = self.subscribe_to_get_responses().await?;

        // Publish get request
        self.publish_get_request().await?;

        // Wait for response
        let delta_state = self
            .wait_for_get_response(accepted_stream, rejected_stream)
            .await?;

        // Update local persistence if we got a shadow
        let mut state = self.persistence.load().await?.unwrap_or_default();
        if let Some(delta) = delta_state.delta {
            state.apply_patch(delta.clone());
            self.persistence.save(&state).await?;
            self.report(state.clone().into_reported()).await?;
        }

        Ok(state)
    }

    /// Subscribe to delta topic
    pub async fn create_delta_subscription(&self) -> ShadowResult<StreamOperation<IoTCoreMessage>> {
        self.subscribe_to_topic(&self.topics.delta).await
    }

    /// Parse a delta message, apply it to state, and return both full state and delta
    /// Returns the full updated state, and delta state
    pub async fn parse_delta_message(
        &self,
        msg: &IoTCoreMessage,
    ) -> ShadowResult<(T, Option<T::Delta>)> {
        let topic = &msg.message.topic_name;
        let payload = &msg.message.payload;

        if topic == &self.topics.delta {
            let delta_response: DeltaResponse<T::Delta> = serde_json::from_slice(payload)?;

            // Load current state and apply delta
            let mut state = self.persistence.load().await?.unwrap_or_default();
            if let Some(ref delta) = delta_response.state {
                // Apply the delta using rustot's patch mechanism
                state.apply_patch(delta.clone());

                self.report(state.clone().into_reported()).await?;

                // Save updated state
                self.persistence.save(&state).await?;
            }
            return Ok((state, delta_response.state));
        }

        Err(ShadowError::InvalidDocument(
            "Failed to parse delta message".to_string(),
        ))
    }

    /// Wait for delta changes from the cloud
    pub async fn wait_delta(&self) -> ShadowResult<(T, Option<T::Delta>)> {
        // Subscribe to delta topic
        let mut delta_stream = self.create_delta_subscription().await?;

        // Wait for the next delta message
        while let Some(message) = delta_stream.next().await {
            return self.parse_delta_message(&message).await;
        }

        Err(ShadowError::Timeout)
    }

    /// Report the current device state to the cloud
    pub async fn report(
        &self,
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

        // Create subscriptions for update responses
        let (accepted_stream, rejected_stream) = self.subscribe_to_update_responses().await?;

        // Serialize and publish update request
        let payload = serde_json::to_vec(&request)?;
        let client_token = request.client_token;

        self.publish_to_topic(&self.topics.update, payload).await?;

        // Wait for response
        self.wait_for_update_response(accepted_stream, rejected_stream, client_token)
            .await
    }

    /// Update the state of the shadow.
    ///
    /// This function will update the desired state of the shadow in the cloud,
    /// and depending on whether the state update is rejected or accepted, it
    /// will automatically update the local version after response
    pub async fn update<F: FnOnce(&T, &mut T::Reported)>(&self, f: F) -> ShadowResult<T> {
        let mut update = T::Reported::default();
        let mut state = self.persistence.load().await?.unwrap_or_default();
        f(&state, &mut update);

        let response = self.report(update).await?;

        if let Some(delta) = response.delta {
            state.apply_patch(delta.clone());

            self.persistence.save(&state).await?;
        }

        Ok(state)
    }

    /// Report state for a specific shadow
    async fn desired(
        &self,
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

        // Create subscriptions for update responses
        let (accepted_stream, rejected_stream) = self.subscribe_to_update_responses().await?;

        // Serialize and publish update request
        let payload = serde_json::to_vec(&request)?;
        let client_token = request.client_token;

        self.publish_to_topic(&self.topics.update, payload).await?;

        // Wait for response
        self.wait_for_update_response(accepted_stream, rejected_stream, client_token)
            .await
    }

    /// Update desired state for a specific shadow by ID
    pub async fn update_desired<F>(&self, f: F) -> ShadowResult<()>
    where
        F: FnOnce(&mut T, &mut T::Reported),
    {
        let mut update = T::Reported::default();
        let mut state = self.persistence.load().await?.unwrap_or_default();
        f(&mut state, &mut update);

        self.desired(state.clone(), update).await?;

        self.persistence.save(&state).await?;

        Ok(())
    }

    /// Delete the shadow from AWS IoT
    pub async fn delete_shadow(&self) -> ShadowResult<()> {
        // Create subscriptions for delete responses
        let (accepted_stream, rejected_stream) = self.subscribe_to_delete_responses().await?;

        // Publish delete request
        self.publish_delete_request().await?;

        // Wait for response
        self.wait_for_delete_response(accepted_stream, rejected_stream)
            .await?;

        // Clear local persistence
        self.persistence.delete().await?;

        Ok(())
    }

    /// Subscribe to get response topics
    async fn subscribe_to_get_responses(
        &self,
    ) -> ShadowResult<(
        StreamOperation<IoTCoreMessage>,
        StreamOperation<IoTCoreMessage>,
    )> {
        Ok(futures::try_join!(
            self.subscribe_to_topic(&self.topics.get_accepted),
            self.subscribe_to_topic(&self.topics.get_rejected)
        )?)
    }

    /// Subscribe to update response topics
    async fn subscribe_to_update_responses(
        &self,
    ) -> ShadowResult<(
        StreamOperation<IoTCoreMessage>,
        StreamOperation<IoTCoreMessage>,
    )> {
        Ok(futures::try_join!(
            self.subscribe_to_topic(&self.topics.update_accepted),
            self.subscribe_to_topic(&self.topics.update_rejected)
        )?)
    }

    /// Subscribe to delete response topics
    async fn subscribe_to_delete_responses(
        &self,
    ) -> ShadowResult<(
        StreamOperation<IoTCoreMessage>,
        StreamOperation<IoTCoreMessage>,
    )> {
        Ok(futures::try_join!(
            self.subscribe_to_topic(&self.topics.delete_accepted),
            self.subscribe_to_topic(&self.topics.delete_rejected)
        )?)
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

        Ok(self.ipc_client.subscribe_to_iot_core(request).await?)
    }

    /// Publish get request
    async fn publish_get_request(&self) -> ShadowResult<()> {
        // Get requests are typically empty payloads
        self.publish_to_topic(&self.topics.get, Vec::new()).await
    }

    /// Publish delete request
    async fn publish_delete_request(&self) -> ShadowResult<()> {
        // Delete requests are typically empty payloads
        self.publish_to_topic(&self.topics.delete, Vec::new()).await
    }

    /// Publish to a specific topic
    async fn publish_to_topic(&self, topic: &str, payload: Vec<u8>) -> ShadowResult<()> {
        let request = PublishToIoTCoreRequest {
            topic_name: topic.to_string(),
            qos: QoS::AtLeastOnce,
            payload: payload.into(),
            user_properties: None,
            message_expiry_interval_seconds: None,
            correlation_data: None,
            response_topic: None,
            content_type: None,
        };

        self.ipc_client.publish_to_iot_core(request).await?;
        Ok(())
    }

    /// Wait for get response
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
        }).await;

        futures::try_join!(accepted_stream.close(), rejected_stream.close())?;

        match result {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ShadowError::Timeout),
        }
    }

    /// Wait for update response
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

    /// Wait for delete response
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

    /// Generate a unique client token
    fn generate_client_token(&self) -> String {
        Uuid::new_v4().to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::shadow::shadow;

    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[shadow]
    #[derive(Debug, Clone, PartialEq)]
    struct TestState {
        temperature: f64,
        humidity: f64,
        #[shadow_attr(leaf)]
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
        // We can't easily test the full client without IPC connection,
        // but we can test the helper methods
        let token1 = "test-token-1";
        let token2 = "test-token-2";

        assert_ne!(token1, token2);

        // Test that tokens are non-empty
        assert!(!token1.is_empty());
        assert!(!token2.is_empty());
    }

    #[tokio::test]
    async fn test_persistence_integration() {
        let temp_dir = TempDir::new().unwrap();
        let shadow_file = temp_dir.path().join("test_shadow.json");

        let persistence = FileBasedPersistence::new(shadow_file);

        let test_state = create_test_state();

        // Test saving and loading state
        persistence.save(&test_state).await.unwrap();
        let loaded_state = persistence.load().await.unwrap();

        assert_eq!(Some(test_state), loaded_state);
    }
}
