//! Example demonstrating how to list components using Greengrass IPC
//!
//! This example connects to the Greengrass Core IPC service and retrieves
//! information about all installed components.

use greengrass_ipc_rust::{
    GreengrassCoreIPCClient,
    model::{
        ComponentState, GetComponentDetailsRequest, PauseComponentRequest, RequestStatus,
        ResumeComponentRequest,
    },
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
                        println!("  Version: {:?}", component.version);
                        println!("  State: {}", format_state(component.state));

                        if let Some(config) = &component.configuration {
                            println!(
                                "  Configuration: {}",
                                serde_json::to_string_pretty(config).unwrap_or_else(|_| {
                                    "Failed to format configuration".to_string()
                                })
                            );
                        }

                        if idx < components.len() - 1 {
                            println!("{:-<80}", "");
                        }
                    }

                    // Demonstrate get_component_details for multiple components
                    if let Some(first_component) = components.first() {
                        println!("\n{:-<80}", "");
                        println!(
                            "\nGetting detailed information for '{}'...\n",
                            first_component.component_name
                        );

                        let details_request = GetComponentDetailsRequest {
                            component_name: first_component.component_name.clone(),
                        };

                        match client.get_component_details(details_request).await {
                            Ok(details_response) => {
                                let details = &details_response.component_details;
                                println!("Detailed component information retrieved successfully:");
                                println!("  Name: {}", details.component_name);
                                println!("  Version: {:?}", details.version);
                                println!("  State: {}", format_state(details.state));

                                if let Some(config) = &details.configuration {
                                    println!(
                                        "  Configuration: {}",
                                        serde_json::to_string_pretty(config).unwrap_or_else(|_| {
                                            "Failed to format configuration".to_string()
                                        })
                                    );
                                } else {
                                    println!("  Configuration: None");
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to get component details: {}", e);
                            }
                        }
                    }

                    // Test with a component that has null configuration
                    if let Some(service_component) = components
                        .iter()
                        .find(|c| c.component_name == "UpdateSystemPolicyService")
                    {
                        println!("\n{:-<80}", "");
                        println!(
                            "\nGetting detailed information for '{}'...\n",
                            service_component.component_name
                        );

                        let details_request = GetComponentDetailsRequest {
                            component_name: service_component.component_name.clone(),
                        };

                        match client.get_component_details(details_request).await {
                            Ok(details_response) => {
                                let details = &details_response.component_details;
                                println!("Detailed component information retrieved successfully:");
                                println!("  Name: {}", details.component_name);
                                println!("  Version: {:?}", details.version);
                                println!("  State: {}", format_state(details.state));

                                if let Some(config) = &details.configuration {
                                    println!(
                                        "  Configuration: {}",
                                        serde_json::to_string_pretty(config).unwrap_or_else(|_| {
                                            "Failed to format configuration".to_string()
                                        })
                                    );
                                } else {
                                    println!("  Configuration: None");
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to get component details: {}", e);
                            }
                        }
                    }

                    if let Some(test_component) = components
                        .iter()
                        .find(|c| c.component_name == "aws.greengrass.LocalDebugConsole")
                    {
                        println!("\n{:-<80}", "");
                        println!(
                            "\nTesting lifecycle operations on {}...",
                            test_component.component_name
                        );

                        // Test pause
                        let pause_request = PauseComponentRequest {
                            component_name: test_component.component_name.clone(),
                        };

                        match client.pause_component(pause_request).await {
                            Ok(response) => {
                                println!("Pause component result:");
                                println!(
                                    "  Status: {}",
                                    format_request_status(response.pause_status)
                                );
                                if let Some(msg) = response.message {
                                    println!("  Message: {}", msg);
                                }
                            }
                            Err(e) => eprintln!("Failed to pause component: {}", e),
                        }

                        // Wait a moment
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                        // Test resume
                        let resume_request = ResumeComponentRequest {
                            component_name: test_component.component_name.clone(),
                        };

                        match client.resume_component(resume_request).await {
                            Ok(response) => {
                                println!("Resume component result:");
                                println!(
                                    "  Status: {}",
                                    format_request_status(response.resume_status)
                                );
                                if let Some(msg) = response.message {
                                    println!("  Message: {}", msg);
                                }
                            }
                            Err(e) => eprintln!("Failed to resume component: {}", e),
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

fn format_request_status(status: RequestStatus) -> &'static str {
    match status {
        RequestStatus::Succeeded => "SUCCEEDED",
        RequestStatus::Failed => "FAILED",
    }
}
