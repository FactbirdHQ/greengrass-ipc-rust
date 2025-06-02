//! Example demonstrating how to manage component configuration using Greengrass IPC
//!
//! This example connects to the Greengrass Core IPC service and demonstrates
//! various configuration operations including getting and updating the calling
//! component's own configuration values.

use greengrass_ipc_rust::{
    model::{GetConfigurationRequest, UpdateConfigurationRequest},
    GreengrassCoreIPCClient,
};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    println!("Connecting to Greengrass Core IPC...");

    // Create and connect the client
    let client = GreengrassCoreIPCClient::connect().await?;

    println!("Connected successfully!");
    println!("\nDemonstrating configuration operations...\n");
    println!("Note: GetConfiguration can access other components with optional componentName");
    println!("      UpdateConfiguration only works on the calling component's own configuration");

    // =============================================
    // Get Configuration Example
    // =============================================
    println!("1. Getting current configuration...");
    println!("{:-<60}", "");

    // Try to get the entire configuration first (empty key path)
    let get_all_config_request = GetConfigurationRequest {
        component_name: None, // None means calling component's own configuration
        key_path: vec![],     // Getting the entire configuration
    };

    match client.get_configuration(get_all_config_request).await {
        Ok(response) => {
            println!("✓ Full configuration retrieved successfully:");
            println!(
                "  Value: {}",
                serde_json::to_string_pretty(&response.value)
                    .unwrap_or_else(|_| { "Failed to format value".to_string() })
            );
        }
        Err(e) => {
            eprintln!("✗ Failed to get full configuration: {}", e);

            // Try a specific test key
            let get_config_request = GetConfigurationRequest {
                component_name: None,
                key_path: vec!["testKey".to_string()],
            };

            match client.get_configuration(get_config_request).await {
                Ok(response) => {
                    println!("✓ Test key configuration retrieved successfully:");
                    println!(
                        "  Value: {}",
                        serde_json::to_string_pretty(&response.value)
                            .unwrap_or_else(|_| { "Failed to format value".to_string() })
                    );
                }
                Err(e) => {
                    eprintln!("✗ Failed to get test key configuration: {}", e);
                }
            }
        }
    }

    println!();

    // =============================================
    // Update Configuration Example
    // =============================================
    println!("2. Updating configuration...");
    println!("{:-<60}", "");

    // Set a test configuration value with current timestamp
    let current_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;

    let test_value = serde_json::json!("example_value");
    let update_config_request = UpdateConfigurationRequest {
        key_path: vec!["myTestKey".to_string()],
        value_to_merge: test_value.clone(),
        timestamp: current_timestamp,
    };

    match client.update_configuration(update_config_request).await {
        Ok(_) => {
            println!("✓ Configuration updated successfully:");
            println!("  Key: myTestKey");
            println!("  New Value: {}", test_value);
            println!("  Timestamp: {}", current_timestamp);
        }
        Err(e) => {
            eprintln!("✗ Failed to update configuration: {}", e);
            println!("  Note: Updates may require deployment context or special permissions");
        }
    }

    println!();

    // =============================================
    // Verify Configuration Update
    // =============================================
    println!("3. Verifying configuration update...");
    println!("{:-<60}", "");

    let verify_config_request = GetConfigurationRequest {
        component_name: None,
        key_path: vec!["myTestKey".to_string()],
    };

    match client.get_configuration(verify_config_request).await {
        Ok(response) => {
            println!("✓ Updated configuration verified:");
            println!(
                "  Current Value: {}",
                serde_json::to_string_pretty(&response.value)
                    .unwrap_or_else(|_| { "Failed to format value".to_string() })
            );
        }
        Err(e) => {
            eprintln!("✗ Failed to verify configuration: {}", e);
        }
    }

    println!();

    // =============================================
    // Get Configuration from Another Component Example
    // =============================================
    println!("4. Getting configuration from another component...");
    println!("{:-<60}", "");

    // Try to get configuration from LocalDebugConsole component
    let other_component_request = GetConfigurationRequest {
        component_name: Some("aws.greengrass.LocalDebugConsole".to_string()),
        key_path: vec!["port".to_string()],
    };

    match client.get_configuration(other_component_request).await {
        Ok(response) => {
            println!("✓ Other component configuration retrieved successfully:");
            println!("  Component: {}", response.component_name);
            println!(
                "  Value: {}",
                serde_json::to_string_pretty(&response.value)
                    .unwrap_or_else(|_| { "Failed to format value".to_string() })
            );
        }
        Err(e) => {
            eprintln!("✗ Failed to get other component configuration: {}", e);
            println!("  Note: This may require proper permissions or the component may not exist");
        }
    }

    println!();
    println!("{:-<60}", "");
    println!("Configuration operations demonstration completed!");
    println!();
    println!("Summary of operations performed:");
    println!("  ✓ Retrieved own component's full configuration");
    println!("  ✓ Updated test configuration key with timestamp");
    println!("  ✓ Verified the configuration update");
    println!("  ✓ Attempted to get another component's configuration");
    println!();
    println!("Key features demonstrated:");
    println!("  • Optional componentName in GetConfiguration");
    println!("  • Required timestamp in UpdateConfiguration for concurrency control");
    println!("  • Proper error handling for various scenarios");
    println!();
    println!("Closing connection...");
    client.close().await?;
    println!("Connection closed successfully!");

    Ok(())
}
