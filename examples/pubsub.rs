use greengrass_ipc_rust::{connect, Result};
use log::LevelFilter;
use std::time::Duration;
use tokio::time::sleep;

/// A simple example demonstrating the use of the Greengrass IPC client
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Trace)
        .init();

    // Connect to the Greengrass Core IPC service
    let client = match connect().await {
        Ok(client) => {
            println!("Successfully connected to Greengrass Core IPC service");
            client
        }
        Err(e) => {
            eprintln!("Failed to connect to Greengrass Core IPC service: {}", e);
            // In a real application, we might want to retry the connection
            return Err(e);
        }
    };

    // Example topic for pub/sub
    let topic = "/test";

    // Example message
    let message = format!(
        "Hello from Rust! Current time: {}",
        chrono::Local::now().to_rfc3339()
    );

    // Subscribe to the topic
    // println!("Subscribing to topic: {}", topic);
    // let subscription = client
    //     .subscribe_to_topic(topic, |msg| async move {
    //         if let Some(binary_message) = msg.binary_message {
    //             if let Some(message_bytes) = binary_message.message {
    //                 let message_str = String::from_utf8_lossy(&message_bytes);
    //                 println!("Received message: {}", message_str);
    //             }
    //         } else if let Some(json_message) = msg.json_message {
    //             if let Some(message_value) = json_message.message {
    //                 println!("Received JSON message: {}", message_value);
    //             }
    //         }
    //     })
    //     .await;

    // match subscription {
    //     Ok(_) => println!("Successfully subscribed to topic: {}", topic),
    //     Err(e) => eprintln!("Failed to subscribe to topic: {}: {}", topic, e),
    // }

    // Publish a message to the topic
    println!("Publishing message to topic: {}", topic);
    match client
        .publish_to_topic(topic, message.as_bytes().to_vec())
        .await
    {
        Ok(_) => println!("Successfully published message to topic: {}", topic),
        Err(e) => eprintln!("Failed to publish message to topic: {}: {}", topic, e),
    }

    // Keep the application running for a while to receive messages
    // println!("Waiting for messages...");
    // sleep(Duration::from_secs(10)).await;

    println!("Exiting example");
    Ok(())
}
