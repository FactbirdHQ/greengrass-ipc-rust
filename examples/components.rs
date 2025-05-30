//! Example demonstrating how to list components using Greengrass IPC
//!
//! This example connects to the Greengrass Core IPC service and retrieves
//! information about all installed components.

use greengrass_ipc_rust::{
    model::ComponentState,
    GreengrassCoreIPCClient,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    println!("Connecting to Greengrass Core IPC...");

    // Create and connect the client
    let client = GreengrassCoreIPCClient::connect().await?;

    println!("Connected successfully!");
    println!("\nListing components...\n");

    // List components
    match client.list_components().await {
        Ok(response) => {
            if let Some(components) = response.components {
                if components.is_empty() {
                    println!("No components found.");
                } else {
                    println!("Found {} component(s):", components.len());
                    println!("{:-<80}", "");

                    for (idx, component) in components.iter().enumerate() {
                        println!("\nComponent #{}", idx + 1);
                        println!("  Name: {}", component.component_name);
                        println!("  Version: {}", component.version);
                        println!("  State: {}", format_state(component.state));

                        if let Some(config) = &component.configuration {
                            println!("  Configuration: {}", 
                                serde_json::to_string_pretty(config)
                                    .unwrap_or_else(|_| "Failed to format configuration".to_string())
                            );
                        }

                        if idx < components.len() - 1 {
                            println!("{:-<80}", "");
                        }
                    }
                }
            } else {
                println!("No components found.");
            }
        }
        Err(e) => {
            eprintln!("Failed to list components: {}", e);
            return Err(e.into());
        }
    }

    println!("\n{:-<80}", "");
    println!("Closing connection...");
    client.close().await?;
    println!("Connection closed successfully!");

    Ok(())
}

fn format_state(state: ComponentState) -> &'static str {
    match state {
        ComponentState::New => "NEW",
        ComponentState::Installed => "INSTALLED",
        ComponentState::Starting => "STARTING",
        ComponentState::Running => "RUNNING",
        ComponentState::Stopping => "STOPPING",
        ComponentState::Errored => "ERRORED",
        ComponentState::Broken => "BROKEN",
        ComponentState::Finished => "FINISHED",
    }
}