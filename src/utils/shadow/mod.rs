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
//! use greengrass_ipc_rust::{connect, utils::shadow::ShadowClient};
//! use serde::{Deserialize, Serialize};
//! use std::path::PathBuf;
//! use std::sync::Arc;
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct DeviceState {
//!     temperature: f64,
//!     humidity: f64,
//!     status: String,
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let ipc_client = Arc::new(connect().await?);
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
//! // Report device state
//! let device_state = DeviceState {
//!     temperature: 23.5,
//!     humidity: 45.2,
//!     status: "online".to_string(),
//! };
//! shadow_client.report(device_state.clone()).await?;
//!
//! // Wait for delta changes
//! let updated_state = shadow_client.wait_delta().await?;
//! println!("Received delta: {:?}", updated_state);
//! # Ok(())
//! # }
//! ```

mod client;
mod document;
mod error;
mod manager;
mod persistence;
mod topics;

pub use client::ShadowClient;
pub use document::{ShadowDocument, ShadowState, ShadowUpdateRequest, ShadowUpdateState};
pub use error::ShadowError;
pub use manager::ShadowManager;