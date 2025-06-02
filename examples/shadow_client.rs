//! Example demonstrating the ShadowClient usage
//!
//! This example shows how to use the ShadowClient to interact with AWS IoT Device Shadows
//! through the Greengrass IPC service using MQTT-based communication.

use greengrass_ipc_rust::{GreengrassCoreIPCClient, utils::shadow::ShadowClient};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct DeviceState {
    /// Current temperature reading
    temperature: f64,
    /// Current humidity reading  
    humidity: f64,
    /// Device operational status
    status: String,
    /// LED brightness level (0-100)
    led_brightness: u8,
    /// Whether the device is actively monitoring
    monitoring_enabled: bool,
}

impl Default for DeviceState {
    fn default() -> Self {
        Self {
            temperature: 20.0,
            humidity: 45.0,
            status: "offline".to_string(),
            led_brightness: 50,
            monitoring_enabled: false,
        }
    }
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
    let thing_name = std::env::var("AWS_IOT_THING_NAME")
        .unwrap_or_else(|_| "example-device".to_string());
    
    let shadow_file_path = PathBuf::from("/tmp")
        .join(format!("{}_shadow.json", thing_name));

    let mut shadow_client = ShadowClient::<DeviceState>::new(
        ipc_client.clone(),
        thing_name.clone(),
        None, // Classic shadow (no shadow name)
        shadow_file_path,
    ).await?;

    // Set a custom timeout
    shadow_client.set_timeout(Duration::from_secs(10));

    println!("Shadow client created for thing: {}", thing_name);

    // Example 1: Get current shadow state
    println!("\n=== Getting current shadow state ===");
    match shadow_client.get_shadow().await {
        Ok(Some(current_state)) => {
            println!("Current shadow state: {:#?}", current_state);
        }
        Ok(None) => {
            println!("No shadow exists yet");
        }
        Err(e) => {
            println!("Failed to get shadow: {:?}", e);
        }
    }

    // Example 2: Report initial device state
    println!("\n=== Reporting initial device state ===");
    let initial_state = DeviceState {
        temperature: 22.5,
        humidity: 48.0,
        status: "online".to_string(),
        led_brightness: 75,
        monitoring_enabled: true,
    };

    match shadow_client.report(initial_state.clone()).await {
        Ok(_) => {
            println!("Successfully reported initial state: {:#?}", initial_state);
        }
        Err(e) => {
            println!("Failed to report state: {:?}", e);
        }
    }

    // Example 3: Update desired state (simulating cloud-side update)
    println!("\n=== Updating desired state ===");
    let desired_state = DeviceState {
        temperature: 25.0, // Desired temperature setpoint
        humidity: 50.0,    // Desired humidity setpoint
        status: "active".to_string(),
        led_brightness: 90,
        monitoring_enabled: true,
    };

    match shadow_client.update(desired_state.clone()).await {
        Ok(_) => {
            println!("Successfully updated desired state: {:#?}", desired_state);
        }
        Err(e) => {
            println!("Failed to update desired state: {:?}", e);
        }
    }

    // Example 4: Simulate device responding to changes
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
            
            let sensor_state = DeviceState {
                temperature: (current_temp * 10.0f64).round() / 10.0f64,
                humidity: (current_humidity * 10.0f64).round() / 10.0f64,
                status: "active".to_string(),
                led_brightness: 90,
                monitoring_enabled: true,
            };
            
            println!("Sensor reading {}: temp={:.1}°C, humidity={:.1}%", 
                     i + 1, sensor_state.temperature, sensor_state.humidity);
            
            if let Err(e) = sensor_client.report(sensor_state).await {
                println!("Failed to report sensor data: {:?}", e);
            }
            
            sleep(Duration::from_secs(2)).await;
        }
    });

    // Example 5: Wait for delta changes (in a separate task)
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
        Ok(Some(final_state)) => {
            println!("Final shadow state: {:#?}", final_state);
        }
        Ok(None) => {
            println!("No shadow found");
        }
        Err(e) => {
            println!("Failed to get final shadow: {:?}", e);
        }
    }

    // Example 7: Demonstrate named shadow (optional)
    if std::env::var("DEMO_NAMED_SHADOW").is_ok() {
        println!("\n=== Named Shadow Example ===");
        
        let config_shadow_path = PathBuf::from("/tmp")
            .join(format!("{}_config_shadow.json", thing_name));
            
        let config_client = ShadowClient::<DeviceConfig>::new(
            ipc_client.clone(),
            thing_name.clone(),
            Some("device-config".to_string()), // Named shadow
            config_shadow_path,
        ).await?;

        let config = DeviceConfig {
            update_interval_seconds: 30,
            max_temperature_threshold: 35.0,
            alert_email: "admin@example.com".to_string(),
        };

        match config_client.report(config.clone()).await {
            Ok(_) => {
                println!("Successfully reported config to named shadow: {:#?}", config);
            }
            Err(e) => {
                println!("Failed to report config: {:?}", e);
            }
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct DeviceConfig {
    update_interval_seconds: u32,
    max_temperature_threshold: f64,
    alert_email: String,
}

/// Example of running the shadow client with error handling and retries
#[allow(dead_code)]
async fn robust_shadow_operations() -> Result<(), Box<dyn std::error::Error>> {
    let ipc_client = Arc::new(GreengrassCoreIPCClient::connect().await?);
    let thing_name = "robust-device".to_string();
    let shadow_path = PathBuf::from("/tmp/robust_shadow.json");
    
    let shadow_client = ShadowClient::<DeviceState>::new(
        ipc_client,
        thing_name,
        None,
        shadow_path,
    ).await?;

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
    shadow_client: &ShadowClient<DeviceState>
) -> Result<(), Box<dyn std::error::Error>> {
    match shadow_client.get_shadow().await {
        Ok(Some(state)) => {
            println!("Shadow retrieved: {:?}", state);
        }
        Ok(None) => {
            println!("Shadow does not exist, creating initial state...");
            let initial_state = DeviceState::default();
            shadow_client.report(initial_state).await?;
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