//! Example demonstrating the ShadowClient usage
//!
//! This example shows how to use the ShadowClient to interact with AWS IoT Device Shadows
//! through the Greengrass IPC service using MQTT-based communication.

use greengrass_ipc_rust::utils::shadow::shadow;
use greengrass_ipc_rust::{utils::shadow::ShadowClient, GreengrassCoreIPCClient};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[shadow]
#[derive(Debug, Clone, PartialEq)]
struct DeviceState {
    /// Current temperature reading
    temperature: f64,
    /// Current humidity reading
    humidity: f64,
    /// Device operational status
    #[shadow_attr(leaf)]
    status: String,
    /// LED brightness level (0-100)
    led_brightness: u8,
    /// Whether the device is actively monitoring
    monitoring_enabled: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Connect to Greengrass IPC
    println!("Connecting to Greengrass IPC...");
    let ipc_client = Arc::new(GreengrassCoreIPCClient::connect().await?);
    println!("Connected successfully!");

    // Create shadow client for classic shadow
    let thing_name =
        std::env::var("AWS_IOT_THING_NAME").unwrap_or_else(|_| "example-device".to_string());

    let shadow_file_path = PathBuf::from("/tmp").join(format!("{}_shadow.json", thing_name));

    let mut shadow_client = ShadowClient::<DeviceState>::new(
        ipc_client.clone(),
        thing_name.clone(),
        None, // Classic shadow (no shadow name)
        shadow_file_path,
    )
    .await?;

    // Set a custom timeout
    shadow_client.set_timeout(Duration::from_secs(10));

    println!("Shadow client created for thing: {}", thing_name);

    // Get current shadow state
    println!("\n=== Getting current shadow state ===");
    match shadow_client.get_shadow().await {
        Ok(current_state) => {
            println!("Current shadow state: {:#?}", current_state);
        }
        Err(e) => {
            println!("Failed to get shadow: {:?}", e);
        }
    }

    // Update reported state
    println!("\n=== Updating reported state ===");

    match shadow_client
        .update(|_current, update| {
            update.temperature = Some(25.0);
            update.humidity = Some(50.0);
        })
        .await
    {
        Ok(state) => {
            println!("Successfully updated desired state: {:#?}", state);
        }
        Err(e) => {
            println!("Failed to update desired state: {:?}", e);
        }
    }

    // Simulate device responding to changes
    println!("\n=== Simulating device operation ===");

    // Spawn a task to simulate sensor readings
    let sensor_client = shadow_client.clone();
    let sensor_task = tokio::spawn(async move {
        let mut current_temp = 22.5f64;
        let mut current_humidity = 48.0f64;

        for i in 0..5 {
            // Simulate sensor readings gradually approaching desired values
            current_temp += (25.0f64 - current_temp) * 0.3f64;
            current_humidity += (50.0f64 - current_humidity) * 0.3f64;

            println!(
                "Sensor reading {}: temp={:.1}°C, humidity={:.1}%",
                i + 1,
                (current_temp * 10.0f64).round() / 10.0f64,
                (current_humidity * 10.0f64).round() / 10.0f64
            );

            if let Err(e) = sensor_client
                .update(|_current, update| {
                    update.temperature = Some((current_temp * 10.0f64).round() / 10.0f64);
                    update.humidity = Some((current_humidity * 10.0f64).round() / 10.0f64);
                })
                .await
            {
                println!("Failed to report sensor data: {:?}", e);
            }

            sleep(Duration::from_secs(2)).await;
        }
    });

    // Wait for delta changes (in a separate task)
    let delta_client = shadow_client.clone();
    let delta_task = tokio::spawn(async move {
        println!("Waiting for delta changes...");

        // In a real application, this would run in a loop
        // For this example, we'll just wait for one delta
        match tokio::time::timeout(Duration::from_secs(15), delta_client.wait_delta()).await {
            Ok(Ok(delta_state)) => {
                println!("Received delta update: {:#?}", delta_state);
                println!("Device would now adjust to match desired state");
            }
            Ok(Err(e)) => {
                println!("Error waiting for delta: {:?}", e);
            }
            Err(_) => {
                println!("Timeout waiting for delta changes");
            }
        }
    });

    // Wait for sensor task to complete
    if let Err(e) = sensor_task.await {
        println!("Sensor task failed: {:?}", e);
    }

    // Wait for delta task to complete (with timeout)
    if let Err(e) = delta_task.await {
        println!("Delta task failed: {:?}", e);
    }

    // Example 6: Final shadow state check
    println!("\n=== Final shadow state ===");
    match shadow_client.get_shadow().await {
        Ok(final_state) => {
            println!("Final shadow state: {:#?}", final_state);
        }
        Err(e) => {
            println!("Failed to get final shadow: {:?}", e);
        }
    }

    // Clean up example - delete shadow (optional)
    if std::env::var("DELETE_SHADOW_ON_EXIT").is_ok() {
        println!("\n=== Cleaning up shadow ===");
        match shadow_client.delete_shadow().await {
            Ok(_) => {
                println!("Successfully deleted shadow");
            }
            Err(e) => {
                println!("Failed to delete shadow: {:?}", e);
            }
        }
    }

    println!("\nExample completed!");
    Ok(())
}

/// Configuration structure for named shadow example
#[shadow(name = "device-config")]
#[derive(Debug, Clone, PartialEq)]
struct DeviceConfig {
    update_interval_seconds: u32,
    max_temperature_threshold: f64,
    #[shadow_attr(leaf)]
    alert_email: String,
}

/// Example of running the shadow client with error handling and retries
#[allow(dead_code)]
async fn robust_shadow_operations() -> Result<(), Box<dyn std::error::Error>> {
    let ipc_client = Arc::new(GreengrassCoreIPCClient::connect().await?);
    let thing_name = "robust-device".to_string();
    let shadow_path = PathBuf::from("/tmp/robust_shadow.json");

    let shadow_client =
        ShadowClient::<DeviceState>::new(ipc_client, thing_name, None, shadow_path).await?;

    // Retry logic for critical operations
    let max_retries = 3;
    let mut retries = 0;

    while retries < max_retries {
        match shadow_client.get_shadow().await {
            Ok(state) => {
                println!("Successfully retrieved shadow: {:?}", state);
                break;
            }
            Err(e) => {
                retries += 1;
                println!("Attempt {} failed: {:?}", retries, e);

                if retries < max_retries {
                    println!("Retrying in 2 seconds...");
                    sleep(Duration::from_secs(2)).await;
                } else {
                    println!("Max retries reached, giving up");
                    return Err(e.into());
                }
            }
        }
    }

    Ok(())
}

/// Example of handling different shadow error types
#[allow(dead_code)]
async fn handle_shadow_errors(
    shadow_client: &ShadowClient<DeviceState>,
) -> Result<(), Box<dyn std::error::Error>> {
    match shadow_client.get_shadow().await {
        Ok(state) => {
            println!("Shadow retrieved: {:?}", state);
        }
        Err(e) => {
            use greengrass_ipc_rust::utils::shadow::ShadowError;

            match e {
                ShadowError::Timeout => {
                    println!("Operation timed out, check network connectivity");
                }
                ShadowError::ShadowRejected { code, message } => {
                    println!("Shadow operation rejected: {} - {}", code, message);
                    if code == 401 {
                        println!("Check AWS IoT permissions for this device");
                    }
                }
                ShadowError::IpcError(ipc_err) => {
                    println!("IPC communication error: {}", ipc_err);
                    println!("Check if Greengrass Core is running");
                }
                ShadowError::SerializationError(ser_err) => {
                    println!("Data serialization error: {}", ser_err);
                    println!("Check that your data structure is properly serializable");
                }
                ShadowError::IoError(io_err) => {
                    println!("File I/O error: {}", io_err);
                    println!("Check file permissions and disk space");
                }
                _ => {
                    println!("Other error: {}", e);
                }
            }
        }
    }

    Ok(())
}
