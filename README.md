# Greengrass IPC for Rust

A Rust implementation of the AWS IoT Greengrass Core IPC client, providing asynchronous interaction with Greengrass Core services via the EventStream protocol.

## Overview

This library is a Rust port of the [AWS IoT Device SDK v2 for Python's Greengrass IPC implementation](https://github.com/aws/aws-iot-device-sdk-python-v2). It allows Greengrass components written in Rust to communicate with the Greengrass Core, other components, and AWS IoT Core.

Key features include:
- Async/await based API compatible with Tokio
- Local publish/subscribe messaging
- AWS IoT Core MQTT messaging
- Stream-based operations for continuous data flow
- Error handling and retry mechanisms
- Type-safe request/response APIs

## Installation

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
greengrass-ipc-rust = "0.1.0"
```

## Usage

### Basic Example

```rust
use greengrass_ipc_rust::{connect, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to the Greengrass Core IPC service
    let client = connect().await?;
    
    // Publish a message to a topic
    let topic = "test/topic";
    let message = "Hello from Rust!";
    client.publish_to_topic(topic, message.as_bytes().to_vec()).await?;
    
    // Subscribe to a topic
    let subscription = client.subscribe_to_topic(topic, |msg| async move {
        if let Some(binary_message) = msg.binary_message {
            if let Some(message_bytes) = binary_message.message {
                let message_str = String::from_utf8_lossy(&message_bytes);
                println!("Received message: {}", message_str);
            }
        }
    }).await?;
    
    // Keep the application running
    tokio::signal::ctrl_c().await?;
    
    // Clean up
    subscription.close().await?;
    
    Ok(())
}
```

### Advanced Usage

For more complex use cases, the library supports:

- Custom connection parameters
- Lifecycle event handling
- JSON message publishing and receiving
- Stream operation management
- MQTT QoS control

## API Reference

See the [documentation](https://docs.rs/greengrass-ipc-rust) for detailed API reference.

## Development

The project uses standard Rust tooling:

```bash
# Run the tests
cargo test

# Run the example
cargo run

# Build documentation
cargo doc --open
```

## Testing

This library includes both unit tests and integration tests:

```bash
# Run unit tests
cargo test unit_tests

# Run integration tests 
cargo test integration_tests

# Run tests against the actual AWS IoT Device SDK v2 for Python
cargo test python_interop -- --ignored
```

### Python Interoperability Testing

For validation against the reference implementation, we include tests that run against the actual AWS IoT Device SDK v2 for Python. These tests ensure our Rust implementation is compatible with the Event Stream protocol used by Greengrass.

To run these tests:
1. Install the AWS IoT SDK for Python: `pip install awsiotsdk`
2. Run the interop tests: `cargo test python_interop -- --ignored`

See [tests/PYTHON_TEST_SETUP.md](tests/PYTHON_TEST_SETUP.md) for detailed setup instructions.

## License

This library is licensed under the Apache License 2.0.

## Contributing

Contributions are welcome! Please see the [CONTRIBUTING.md](CONTRIBUTING.md) file for details.

## Related Resources

- [AWS IoT Device SDK v2 for Python](https://github.com/aws/aws-iot-device-sdk-python-v2)
- [AWS IoT Greengrass Documentation](https://docs.aws.amazon.com/greengrass/v2/developerguide/what-is-iot-greengrass.html)
- [AWS Event Stream Protocol](https://github.com/awslabs/aws-c-event-stream)