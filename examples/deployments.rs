//! Example demonstrating how to list local deployments using Greengrass IPC
//!
//! This example connects to the Greengrass Core IPC service and retrieves
//! information about the last 5 local deployments.
//! ```

use greengrass_ipc_rust::{
    GreengrassCoreIPCClient,
    model::{DeploymentStatus, GetLocalDeploymentStatusRequest},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    println!("Connecting to Greengrass Core IPC...");

    // Create and connect the client
    let client = GreengrassCoreIPCClient::connect().await?;

    println!("Connected successfully!");
    println!("\nListing local deployments...\n");

    // List local deployments
    match client.list_local_deployments().await {
        Ok(response) => {
            if let Some(deployments) = response.local_deployments {
                if deployments.is_empty() {
                    println!("No local deployments found.");
                } else {
                    println!("Found {} local deployment(s):", deployments.len());
                    println!("{:-<80}", "");

                    for (idx, deployment) in deployments.iter().enumerate() {
                        println!("\nDeployment #{}", idx + 1);
                        println!("  ID: {}", deployment.deployment_id);
                        println!("  Status: {}", format_status(deployment.status));

                        if let Some(created_on) = &deployment.created_on {
                            println!("  Created: {}", created_on);
                        }

                        if let Some(details) = &deployment.deployment_status_details {
                            if let Some(detailed_status) = &details.detailed_deployment_status {
                                println!("  Detailed Status: {}", detailed_status);
                            }

                            if let Some(failure_cause) = &details.deployment_failure_cause {
                                println!("  Failure Cause: {}", failure_cause);
                            }

                            if let Some(error_stack) = &details.deployment_error_stack {
                                if !error_stack.is_empty() {
                                    println!("  Error Stack:");
                                    for error in error_stack {
                                        println!("    - {}", error);
                                    }
                                }
                            }

                            if let Some(error_types) = &details.deployment_error_types {
                                if !error_types.is_empty() {
                                    println!("  Error Types:");
                                    for error_type in error_types {
                                        println!("    - {}", error_type);
                                    }
                                }
                            }
                        }

                        if idx < deployments.len() - 1 {
                            println!("{:-<80}", "");
                        }
                    }

                    // Get detailed status for the first deployment
                    if let Some(first_deployment) = deployments.first() {
                        println!("\n{:-<80}", "");
                        println!("\nGetting detailed status for the first deployment...\n");

                        let status_request = GetLocalDeploymentStatusRequest {
                            deployment_id: first_deployment.deployment_id.clone(),
                        };

                        match client.get_local_deployment_status(status_request).await {
                            Ok(status_response) => {
                                println!("Detailed deployment status retrieved successfully:");
                                println!("  ID: {}", status_response.deployment.deployment_id);
                                println!(
                                    "  Status: {}",
                                    format_status(status_response.deployment.status)
                                );

                                if let Some(created_on) = &status_response.deployment.created_on {
                                    println!("  Created: {}", created_on);
                                }

                                if let Some(details) =
                                    &status_response.deployment.deployment_status_details
                                {
                                    if let Some(detailed_status) =
                                        &details.detailed_deployment_status
                                    {
                                        println!("  Detailed Status: {}", detailed_status);
                                    }

                                    if let Some(failure_cause) = &details.deployment_failure_cause {
                                        println!("  Failure Cause: {}", failure_cause);
                                    }

                                    if let Some(error_stack) = &details.deployment_error_stack {
                                        if !error_stack.is_empty() {
                                            println!("  Error Stack:");
                                            for error in error_stack {
                                                println!("    - {}", error);
                                            }
                                        }
                                    }

                                    if let Some(error_types) = &details.deployment_error_types {
                                        if !error_types.is_empty() {
                                            println!("  Error Types:");
                                            for error_type in error_types {
                                                println!("    - {}", error_type);
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to get deployment status: {}", e);
                            }
                        }
                    }
                }
            } else {
                println!("No local deployments found.");
            }
        }
        Err(e) => {
            eprintln!("Failed to list local deployments: {}", e);
            return Err(e.into());
        }
    }

    println!("\n{:-<80}", "");
    println!("Closing connection...");
    client.close().await?;
    println!("Connection closed successfully!");

    Ok(())
}

fn format_status(status: DeploymentStatus) -> &'static str {
    match status {
        DeploymentStatus::InProgress => "IN_PROGRESS",
        DeploymentStatus::Queued => "QUEUED",
        DeploymentStatus::Failed => "FAILED",
        DeploymentStatus::Succeeded => "SUCCEEDED",
    }
}
