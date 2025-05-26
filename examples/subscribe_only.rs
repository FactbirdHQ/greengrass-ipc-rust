use greengrass_ipc_rust::{connect, Message, Result};
use futures::StreamExt;
use log::LevelFilter;

/// A simple example demonstrating subscription to a Greengrass IPC topic
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    // Connect to the Greengrass Core IPC service
    let client = match connect().await {
        Ok(client) => {
            println!("Successfully connected to Greengrass Core IPC service");
            client
        }
        Err(e) => {
            eprintln!("Failed to connect to Greengrass Core IPC service: {}", e);
            return Err(e);
        }
    };

    // Example topic for subscription
    let topic = "/test";

    // Subscribe to the topic
    println!("Subscribing to topic: {}", topic);
    let mut subscription = match client.subscribe_to_topic(topic).await {
        Ok(subscription) => {
            println!("Successfully subscribed to topic: {}", topic);
            subscription
        }
        Err(e) => {
            eprintln!("Failed to subscribe to topic: {}: {}", topic, e);
            return Err(e);
        }
    };

    // Listen for messages using the Stream API
    println!("Waiting for messages... Press Ctrl+C to exit");
    while let Some(msg) = subscription.next().await {
        match msg.message {
            Message::Json(json_message) => {
                println!("Received JSON message: {:?}", json_message.message);
            }
            Message::Binary(binary_message) => {
                let message_str = String::from_utf8_lossy(&binary_message.message);
                println!("Received message: {}", message_str);
            }
        }
    }
    
    Ok(())
}