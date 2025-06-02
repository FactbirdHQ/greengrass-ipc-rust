//! Utility modules for the Greengrass IPC client
//!
//! This module contains higher-level abstractions and utilities built on top
//! of the core Greengrass IPC functionality.

pub mod shadow;

pub use shadow::ShadowClient;