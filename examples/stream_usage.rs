use futures::{StreamExt, TryStreamExt};
use greengrass_ipc_rust::{connect, Error, Message, Result};
use log::LevelFilter;
use std::time::Duration;
use tokio::time::timeout;

/// Advanced example demonstrating different Stream usage patterns for Greengrass IPC subscriptions
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    // Connect to the Greengrass Core IPC service
    let client = connect().await?;
    println!("Successfully connected to Greengrass Core IPC service");

    // Example 1: Basic Stream iteration
    println!("\n=== Example 1: Basic Stream iteration ===");
    let mut subscription = client.subscribe_to_topic("/example/basic").await?;

    // Publish a test message
    client
        .publish_to_topic("/example/basic", b"Basic message".to_vec())
        .await?;

    // Read first message with timeout
    if let Ok(Some(msg)) = timeout(Duration::from_secs(2), subscription.next()).await {
        println!(
            "Basic: Received message on topic {:?}",
            msg.message.get_topic()
        );
    }

    // Example 2: Filtering messages
    println!("\n=== Example 2: Filtering messages ===");
    let filtered_subscription = client.subscribe_to_topic("/example/filter").await?;

    // Publish test messages
    client
        .publish_to_topic("/example/filter", b"Important: Critical alert".to_vec())
        .await?;
    client
        .publish_to_topic("/example/filter", b"Debug: Routine log".to_vec())
        .await?;
    client
        .publish_to_topic("/example/filter", b"Important: System warning".to_vec())
        .await?;

    // Filter for only "Important" messages
    let important_messages = filtered_subscription
        .filter_map(|msg| async move {
            if let Message::Binary(binary_msg) = msg.message {
                let content = String::from_utf8_lossy(&binary_msg.message);
                if content.starts_with("Important:") {
                    Some(content.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .take(2); // Take only first 2 important messages

    let messages: Vec<String> = timeout(Duration::from_secs(3), important_messages.collect())
        .await
        .unwrap_or_default();

    println!("Filtered messages: {:?}", messages);

    // Example 3: Transforming messages
    println!("\n=== Example 3: Transforming messages ===");
    let transform_subscription = client.subscribe_to_topic("/example/transform").await?;

    // Publish JSON-like messages
    client
        .publish_to_topic("/example/transform", br#"{"value": 42}"#.to_vec())
        .await?;
    client
        .publish_to_topic("/example/transform", br#"{"value": 100}"#.to_vec())
        .await?;

    // Transform messages to extract values
    let transformed = transform_subscription
        .map(|msg| {
            if let Message::Binary(binary_msg) = msg.message {
                let content = String::from_utf8_lossy(&binary_msg.message);
                // Simple JSON value extraction (in real code, use a proper JSON parser)
                if let Some(start) = content.find("\"value\": ") {
                    let value_str = &content[start + 9..];
                    if let Some(end) = value_str.find('}') {
                        return value_str[..end].parse::<i32>().unwrap_or(0);
                    }
                }
            }
            0
        })
        .take(2);

    let values: Vec<i32> = timeout(Duration::from_secs(3), transformed.collect())
        .await
        .unwrap_or_default();

    println!("Extracted values: {:?}", values);

    // Example 4: Multiple subscriptions concurrently
    println!("\n=== Example 4: Multiple subscriptions ===");
    let subscription_a = client.subscribe_to_topic("/example/topic_a").await?;
    let subscription_b = client.subscribe_to_topic("/example/topic_b").await?;

    // Publish to both topics
    client
        .publish_to_topic("/example/topic_a", b"Message from A".to_vec())
        .await?;
    client
        .publish_to_topic("/example/topic_b", b"Message from B".to_vec())
        .await?;

    // Merge streams and handle concurrently
    let merged = futures::stream::select(
        subscription_a.map(|msg| ("Topic A", msg)),
        subscription_b.map(|msg| ("Topic B", msg)),
    );

    let results: Vec<(String, String)> = timeout(
        Duration::from_secs(3),
        merged
            .map(|(source, msg)| {
                let content = match msg.message {
                    Message::Binary(binary_msg) => {
                        String::from_utf8_lossy(&binary_msg.message).to_string()
                    }
                    Message::Json(json_msg) => json_msg.message.to_string(),
                };
                (source.to_string(), content)
            })
            .take(2)
            .collect(),
    )
    .await
    .unwrap_or_default();

    for (source, content) in results {
        println!("{}: {}", source, content);
    }

    // Example 5: Error handling with try_for_each
    println!("\n=== Example 5: Error handling ===");
    let error_subscription = client.subscribe_to_topic("/example/error").await?;

    // Publish test message
    client
        .publish_to_topic("/example/error", b"Test error handling".to_vec())
        .await?;

    let result = timeout(
        Duration::from_secs(2),
        error_subscription
            .map(|msg| -> Result<String> {
                match msg.message {
                    Message::Binary(binary_msg) => {
                        let content = String::from_utf8_lossy(&binary_msg.message);
                        if content.contains("error") {
                            Ok(format!("Processed: {}", content))
                        } else {
                            Err(Error::ValidationError("No error keyword found".to_string()))
                        }
                    }
                    _ => Ok("Non-binary message".to_string()),
                }
            })
            .try_for_each(|processed| async move {
                println!("Error handling result: {}", processed);
                Ok(())
            }),
    )
    .await;

    match result {
        Ok(Ok(())) => println!("Error handling completed successfully"),
        Ok(Err(e)) => println!("Error during processing: {}", e),
        Err(_) => println!("Timeout during error handling"),
    }

    println!("\n=== All examples completed ===");
    Ok(())
}

// Extension trait to add helper methods to Message
trait MessageExt {
    fn get_topic(&self) -> Option<&str>;
}

impl MessageExt for Message {
    fn get_topic(&self) -> Option<&str> {
        match self {
            Message::Binary(binary_msg) => binary_msg.context.as_ref()?.topic.as_deref(),
            Message::Json(json_msg) => json_msg.context.as_ref()?.topic.as_deref(),
        }
    }
}
