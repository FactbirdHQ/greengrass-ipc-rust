//! AWS IoT Greengrass IPC client for Rust
//!
//! This crate provides an async Rust client for the AWS IoT Greengrass IPC (Inter-Process Communication) service.
//! It allows Greengrass components to communicate with the Greengrass nucleus and other components.
//!
//! Based on the Python implementation in the AWS IoT Device SDK v2.

mod client;
mod connection;
mod error;
pub mod event_stream;
mod lifecycle;
mod model;
mod operation;

use std::time::Duration;

pub use client::{GreengrassCoreIPCClient, Subscription, IoTCoreSubscription};
pub use error::{Error, Result};
pub use lifecycle::LifecycleHandler;
pub use model::{
    BinaryMessage, JsonMessage, Message, MessageContext, PublishToIoTCoreRequest,
    PublishToIoTCoreResponse, QoS, SubscribeToIoTCoreRequest, SubscribeToIoTCoreResponse,
    SubscriptionResponseMessage, IoTCoreMessage, MqttMessage,
};

/// Connect to the Greengrass Core IPC service with default parameters.
///
/// This will attempt to connect using the default Unix domain socket path and auth token,
/// which are typically provided via environment variables by the Greengrass nucleus.
///
/// # Returns
///
/// A future that resolves to a `Result<GreengrassCoreIPCClient>`.
pub async fn connect() -> Result<GreengrassCoreIPCClient> {
    GreengrassCoreIPCClient::connect().await
}

/// Connect to the Greengrass Core IPC service with custom parameters.
///
/// # Arguments
///
/// * `ipc_socket` - Optional path to the Unix domain socket of the Greengrass nucleus.
/// * `auth_token` - Optional authentication token.
/// * `lifecycle_handler` - Optional handler for connection lifecycle events.
/// * `timeout` - Optional timeout for the connection attempt in seconds.
///
/// # Returns
///
/// A future that resolves to a `Result<GreengrassCoreIPCClient>`.
pub async fn connect_with_options(
    ipc_socket: Option<std::path::PathBuf>,
    auth_token: Option<String>,
    lifecycle_handler: Option<Box<dyn LifecycleHandler>>,
    timeout: Option<Duration>,
) -> Result<GreengrassCoreIPCClient> {
    GreengrassCoreIPCClient::connect_with_options(
        ipc_socket,
        auth_token,
        lifecycle_handler,
        timeout,
    )
    .await
}

/// Version of the Greengrass IPC library.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
