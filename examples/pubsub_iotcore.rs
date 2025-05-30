use bytes::Bytes;
use futures::StreamExt;
use greengrass_ipc_rust::{
    GreengrassCoreIPCClient, PublishToIoTCoreRequest, QoS, Result, SubscribeToIoTCoreRequest,
};
use log::LevelFilter;
use std::time::Duration;
use tokio::time::timeout;

/// A simple example demonstrating the use of the Greengrass IPC client
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Trace)
        .init();

    // Connect to the Greengrass Core IPC service
    let client = match GreengrassCoreIPCClient::connect().await {
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

    // Subscribe to the topic
    println!("Subscribing to topic: {}", topic);
    let mut subscription = match client
        .subscribe_to_iot_core(SubscribeToIoTCoreRequest {
            topic_name: topic.to_string(),
            qos: QoS::AtLeastOnce,
        })
        .await
    {
        Ok(subscription) => {
            println!("Successfully subscribed to topic: {}", topic);
            subscription
        }
        Err(e) => {
            eprintln!("Failed to subscribe to topic: {}: {}", topic, e);
            return Err(e);
        }
    };

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
    println!("Waiting for messages...");
    let message_timeout = timeout(Duration::from_secs(10), async {
        while let Some(iot_msg) = subscription.next().await {
            let mqtt_msg = &iot_msg.message;
            println!("Received message on topic: {}", mqtt_msg.topic_name);
            let message_str = String::from_utf8_lossy(&mqtt_msg.payload);
            println!("Message content: {}", message_str);

            // Print additional MQTT properties if available
            if let Some(user_props) = &mqtt_msg.user_properties {
                println!("User properties: {:?}", user_props);
            }
            if let Some(content_type) = &mqtt_msg.content_type {
                println!("Content type: {}", content_type);
            }
        }
    });

    if let Err(_) = message_timeout.await {
        println!("Timeout waiting for messages");
    }

    println!("Exiting example");
    Ok(())
}
