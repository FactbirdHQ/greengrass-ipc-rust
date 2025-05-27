use greengrass_ipc_rust::{connect, PublishToIoTCoreRequest, QoS, Result};
use bytes::Bytes;
use log::LevelFilter;
use std::time::Duration;

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
    let message = "Hello from Rust! Test message in the cloud";

    // // Subscribe to the topic
    // println!("Subscribing to topic: {}", topic);
    // let mut subscription = match client.subscribe_to_topic(topic).await {
    //     Ok(subscription) => {
    //         println!("Successfully subscribed to topic: {}", topic);
    //         subscription
    //     }
    //     Err(e) => {
    //         eprintln!("Failed to subscribe to topic: {}: {}", topic, e);
    //         return Err(e);
    //     }
    // };

    let publish_req = PublishToIoTCoreRequest {
        topic_name: topic.to_string(),
        qos: QoS::AtLeastOnce,
        payload: Bytes::from(message.as_bytes().to_vec()),
        user_properties: None,
        message_expiry_interval_seconds: None,
        correlation_data: None,
        response_topic: None,
        content_type: None,
    };

    // Publish a message to the topic
    println!("Publishing message to IoT core on topic: {}", topic);
    match tokio::time::timeout(
        Duration::from_secs(5),
        client.publish_to_iot_core(publish_req),
    )
    .await
    {
        Ok(Ok(_)) => {
            println!("Successfully published message to topic: {}", topic);
            println!("Message content: {}", message);
        }
        Ok(Err(e)) => eprintln!("Failed to publish message to topic: {}: {}", topic, e),
        Err(e) => eprintln!("Failed to publish message to topic: {}: {}", topic, e),
    }

    // Listen for messages using the Stream API
    // println!("Waiting for messages...");
    // let message_timeout = timeout(Duration::from_secs(10), async {
    //     while let Some(msg) = subscription.next().await {
    //         match msg.message {
    //             Message::Json(json_message) => {
    //                 println!("Received JSON message: {:?}", json_message.message);
    //             }
    //             Message::Binary(binary_message) => {
    //                 let message_str = String::from_utf8_lossy(&binary_message.message);
    //                 println!("Received message: {}", message_str);
    //             }
    //         }
    //     }
    // });

    // if let Err(_) = message_timeout.await {
    //     println!("Timeout waiting for messages");
    // }

    println!("Exiting example");
    Ok(())
}
