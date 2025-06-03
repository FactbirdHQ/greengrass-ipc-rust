//! AWS IoT Device Shadow client implementation
//!
//! This module provides a simplified shadow client that uses MQTT-based communication
//! through the Greengrass IPC client. It offers a higher-level API for shadow operations
//! with file-based persistence.
//!
//! # Features
//!
//! - Type-safe shadow document handling
//! - File-based persistence of shadow state
//! - MQTT-based communication using PublishToIoTCore and SubscribeToIoTCore
//! - Support for both classic and named shadows
//! - Delta processing for efficient updates
//!
//! # Example
//!
//! ```rust,no_run
//! use greengrass_ipc_rust::utils::shadow::shadow;
//! use greengrass_ipc_rust::{utils::shadow::ShadowClient, GreengrassCoreIPCClient};
//! use serde::{Deserialize, Serialize};
//! use std::path::PathBuf;
//! use std::sync::Arc;
//!
//! #[shadow]
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct DeviceState {
//!     temperature: f64,
//!     humidity: f64,
//!     #[shadow_attr(leaf)]
//!     status: String,
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let ipc_client = Arc::new(GreengrassCoreIPCClient::connect().await?);
//!
//! let shadow_client = ShadowClient::<DeviceState>::new(
//!     ipc_client,
//!     "my-device".to_string(),
//!     None, // Classic shadow
//!     PathBuf::from("/tmp/device_shadow.json"),
//! ).await?;
//!
//! // Get current shadow
//! let current_state = shadow_client.get_shadow().await?;
//! println!("Current state: {:?}", current_state);
//!
//! // Update device reported state
//! shadow_client
//!     .update(|_current, update| {
//!         update.temperature = Some(25.0);
//!         update.humidity = Some(50.0);
//!     })
//!     .await?;
//!
//! // Wait for delta changes
//! let updated_state = shadow_client.wait_delta().await?;
//! println!("Received delta: {:?}", updated_state);
//! # Ok(())
//! # }
//! ```

mod client;
mod error;
mod manager;
mod persistence;
mod topics;

pub use client::ShadowClient;
pub use error::ShadowError;
pub use manager::ShadowManager;
pub use rustot::shadows::data_types::{
    // Re-export rustot derive macros
    AcceptedResponse,
    DeltaResponse,
    DeltaState,
    ErrorResponse,
    Patch,
    Request,
    RequestState,
};
pub use rustot::shadows::rustot_derive::{shadow, shadow_patch};
pub use rustot::shadows::{ShadowPatch, ShadowState};
