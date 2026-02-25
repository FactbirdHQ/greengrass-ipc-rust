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
pub mod model;

pub use client::{GreengrassCoreIPCClient, StreamOperation};
pub use error::{Error, Result};
pub use lifecycle::{ConnectionError, LifecycleHandler};
pub use model::{
    BinaryMessage, ComponentDetails, ComponentState, ConfigurationUpdateEvent,
    ConfigurationValidityReport, ConfigurationValidityStatus, DeploymentStatus,
    DeploymentStatusDetails, GetComponentDetailsRequest, GetComponentDetailsResponse,
    GetConfigurationRequest, GetConfigurationResponse, IoTCoreMessage, JsonMessage,
    ListComponentsRequest, ListComponentsResponse, ListLocalDeploymentsRequest,
    ListLocalDeploymentsResponse, LocalDeployment, Message, MessageContext, MqttMessage,
    PauseComponentRequest, PauseComponentResponse, PublishToIoTCoreRequest,
    PublishToIoTCoreResponse, QoS, RequestStatus, RestartComponentRequest,
    RestartComponentResponse, ResumeComponentRequest, ResumeComponentResponse,
    SendConfigurationValidityReportRequest, SendConfigurationValidityReportResponse,
    StopComponentRequest, StopComponentResponse, SubscribeToConfigurationUpdateRequest,
    SubscribeToConfigurationUpdateResponse, SubscribeToIoTCoreRequest, SubscribeToIoTCoreResponse,
    SubscribeToValidateConfigurationUpdatesRequest,
    SubscribeToValidateConfigurationUpdatesResponse, SubscriptionResponseMessage,
    UpdateConfigurationRequest, UpdateConfigurationResponse, ValidateConfigurationUpdateEvent,
};
