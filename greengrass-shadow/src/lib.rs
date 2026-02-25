//! Multi-shadow manager for AWS IoT Greengrass
//!
//! This crate provides `ShadowManager` for managing multiple named AWS IoT Device Shadows
//! with pattern-based filtering and efficient wildcard MQTT subscriptions.
//!
//! It integrates with rustot's storage and derive macros while using Greengrass IPC for
//! MQTT communication.
//!
//! # Key Features
//!
//! - **Runtime shadow names**: Unlike rustot's `Shadow` which uses compile-time names,
//!   `ShadowManager` supports runtime-named shadows (e.g., "flow-pump-01", "flow-valve-02")
//!
//! - **Efficient subscriptions**: Single wildcard subscription for all shadows matching
//!   a pattern instead of one subscription per shadow
//!
//! - **rustot integration**: Uses `ShadowNode` trait for state operations, `FileKVStore`
//!   for persistence, and `Topic` for MQTT topic formatting
//!
//! # Architecture
//!
//! ```text
//! greengrass-ipc-rust (low-level IPC)
//!     ↑
//! rustot (ShadowNode trait, FileKVStore, Topic, derive macros)
//!     ↑
//! greengrass-shadow (this crate)
//!     - ShadowManager with:
//!       - Own MQTT ops (wildcard subscriptions, runtime shadow names)
//!       - rustot's FileKVStore for persistence
//!       - rustot's ShadowNode for state handling
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use greengrass_shadow::{ShadowManager, MultiShadowRoot};
//! use rustot::shadows::{ShadowNode, KVPersist};
//! use rustot::shadows::rustot_derive::shadow_node;
//! use serde::{Serialize, Deserialize};
//! use std::sync::Arc;
//!
//! // Define state type with rustot's shadow_node macro
//! #[shadow_node]
//! #[derive(Debug, Clone, Default, Serialize, Deserialize)]
//! pub struct FlowState {
//!     pub flow_rate: f64,
//!     pub temperature: f64,
//!     pub active: bool,
//! }
//!
//! // Implement MultiShadowRoot for runtime naming
//! impl MultiShadowRoot for FlowState {
//!     const PATTERN: &'static str = "flow-";
//!     const PREFIX: &'static str = "$aws";
//!     const MAX_PAYLOAD_SIZE: usize = 512;
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let ipc_client = Arc::new(greengrass_ipc_rust::GreengrassCoreIPCClient::connect().await?);
//!
//!     let manager = ShadowManager::<FlowState>::new(
//!         ipc_client,
//!         "my-device".into(),
//!         "/data/shadows".into(),
//!     )?;
//!
//!     // Discover existing shadows matching the pattern
//!     manager.initialize().await?;
//!
//!     // Add a new shadow dynamically
//!     manager.add_shadow_by_id("pump-03").await?;
//!
//!     // Update a specific shadow
//!     let state = manager.update("pump-01", |current, reported| {
//!         reported.flow_rate = Some(current.flow_rate);
//!     }).await?;
//!
//!     // Wait for delta from ANY managed shadow
//!     loop {
//!         let (shadow_id, state, delta) = manager.wait_delta().await?;
//!         println!("Shadow {} updated", shadow_id);
//!     }
//! }
//! ```
//!
//! # Single Shadow Usage
//!
//! For single shadows with compile-time names, use rustot's `Shadow` directly
//! with greengrass-ipc-rust's MQTT client:
//!
//! ```rust,ignore
//! use rustot::shadows::{Shadow, shadow_root};
//! use rustot::shadows::store::FileKVStore;
//!
//! #[shadow_root(name = "device-config")]
//! #[derive(Clone, Default)]
//! struct DeviceConfig {
//!     pub timeout: u32,
//!     pub debug_mode: bool,
//! }
//!
//! // Use rustot's Shadow directly with a greengrass MQTT adapter
//! ```

mod error;
mod manager;

pub use error::{ShadowError, ShadowResult};
pub use manager::ShadowManager;

// Re-export rustot types for convenience
pub use rustot::shadows::data_types::{
    AcceptedResponse, DeltaResponse, DeltaState, ErrorResponse, Patch, Request, RequestState,
};
pub use rustot::shadows::store::{FileKVStore, StateStore};
pub use rustot::shadows::topics::Topic;
pub use rustot::shadows::{KVPersist, ShadowNode, ShadowRoot};

// Re-export derive macros
pub use rustot::shadows::rustot_derive::{shadow_node, shadow_root};

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Trait for shadow types used with ShadowManager (runtime naming).
///
/// This is similar to `ShadowRoot` but uses `PATTERN` instead of `NAME`
/// to support runtime-named shadows like "flow-pump-01", "flow-valve-02".
///
/// # Implementation
///
/// Types implementing `MultiShadowRoot` must also implement `ShadowNode`
/// (typically via `#[shadow_node]` derive) and `KVPersist` for storage.
///
/// ```rust,ignore
/// use greengrass_shadow::MultiShadowRoot;
/// use rustot::shadows::{ShadowNode, KVPersist};
/// use rustot::shadows::rustot_derive::shadow_node;
/// use serde::{Serialize, Deserialize};
///
/// #[shadow_node]
/// #[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// pub struct FlowState {
///     pub flow_rate: f64,
///     pub temperature: f64,
///     pub active: bool,
/// }
///
/// impl MultiShadowRoot for FlowState {
///     const PATTERN: &'static str = "flow-";
/// }
/// ```
///
/// # Topic Format
///
/// Full shadow names are constructed as `PATTERN + id`:
/// - Pattern: "flow-"
/// - ID: "pump-01"
/// - Full name: "flow-pump-01"
/// - Delta topic: `$aws/things/{thing}/shadow/name/flow-pump-01/update/delta`
/// - Wildcard: `$aws/things/{thing}/shadow/name/+/update/delta`
pub trait MultiShadowRoot: ShadowNode + KVPersist + Serialize + DeserializeOwned + Clone + Send + Sync {
    /// Shadow pattern prefix (e.g., "flow-" matches "flow-pump-01", "flow-valve-02").
    ///
    /// The full shadow name is constructed as `PATTERN + id`.
    const PATTERN: &'static str;

    /// AWS IoT topic prefix (default: "$aws").
    const PREFIX: &'static str = "$aws";

    /// Maximum payload size for shadow updates (default: 512 bytes).
    const MAX_PAYLOAD_SIZE: usize = 512;
}
