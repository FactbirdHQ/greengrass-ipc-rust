//! Message Comparison Framework for Greengrass IPC
//!
//! This framework allows direct byte-level comparison between messages encoded by the Rust
//! library and messages encoded by the Python library, without needing a proxy server.
//! It provides a way to verify that our Rust implementation produces identical wire-format
//! messages as the Python SDK.

use std::collections::{HashMap, VecDeque};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time;

// Define helper functions for base64 serialization
mod serde_with_ext {
    use base64::{engine::general_purpose, Engine as _};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize_base64<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let base64_string = general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&base64_string)
    }

    pub fn deserialize_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let base64_string = String::deserialize(deserializer)?;
        general_purpose::STANDARD
            .decode(base64_string)
            .map_err(serde::de::Error::custom)
    }
}

/// A message comparison test harness for comparing Rust and Python message encodings
pub struct MessageComparisonHarness {
    /// Python subprocess for generating reference messages
    python_process: Option<Child>,
    /// Path to the Python script
    script_path: PathBuf,
    /// Captured Python-encoded messages
    python_messages: Arc<Mutex<VecDeque<EncodedMessage>>>,
}

/// A message encoded by either the Python or Rust implementation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncodedMessage {
    /// The name of the operation (e.g., "PublishToTopic")
    pub operation: String,
    /// The raw bytes of the message
    #[serde(
        serialize_with = "serde_with_ext::serialize_base64",
        deserialize_with = "serde_with_ext::deserialize_base64"
    )]
    pub raw_bytes: Vec<u8>,
    /// The message type (request, response, etc)
    pub message_type: String,
    /// Operation model (if applicable)
    pub operation_model: Option<String>,
    /// Any additional metadata about the message
    pub metadata: HashMap<String, String>,
}

impl MessageComparisonHarness {
    /// Create a new message comparison harness
    pub fn new() -> io::Result<Self> {
        let script_path = create_python_test_script()?;

        Ok(Self {
            python_process: None,
            script_path,
            python_messages: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    /// Start the Python process that will generate reference messages
    pub async fn start(&mut self) -> io::Result<()> {
        // Start the Python process
        let mut process = Command::new("python3")
            .arg(&self.script_path)
            .arg("--encode-only") // Tell the script to encode messages and output them
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Capture stdout and stderr
        let stdout = process
            .stdout
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to capture stdout"))?;
        let stderr = process
            .stderr
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to capture stderr"))?;

        let python_messages = self.python_messages.clone();

        // Set up stdout monitoring - this will parse the encoded messages from Python
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                if let Ok(line) = line {
                    if line.starts_with("ENCODED_MESSAGE:") {
                        // Parse the JSON message data
                        if let Some(json_data) = line.strip_prefix("ENCODED_MESSAGE:") {
                            match serde_json::from_str::<EncodedMessage>(json_data) {
                                Ok(message) => {
                                    python_messages.lock().unwrap().push_back(message);
                                }
                                Err(e) => {
                                    eprintln!("Failed to parse encoded message: {}", e);
                                }
                            }
                        }
                    } else {
                        println!("Python: {}", line);
                    }
                }
            }
        });

        // Set up stderr monitoring
        std::thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                if let Ok(line) = line {
                    eprintln!("Python error: {}", line);
                }
            }
        });

        // Store the Python process
        self.python_process = Some(process);

        // Wait for the server to be ready
        time::sleep(Duration::from_millis(500)).await;

        Ok(())
    }

    /// Get all captured Python-encoded messages
    pub fn get_python_messages(&self) -> Vec<EncodedMessage> {
        self.python_messages
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    /// Get Python-encoded messages for a specific operation
    pub fn get_messages_for_operation(&self, operation: &str) -> Vec<EncodedMessage> {
        self.python_messages
            .lock()
            .unwrap()
            .iter()
            .filter(|msg| msg.operation == operation)
            .cloned()
            .collect()
    }

    /// Get Python-encoded message for a specific operation and message type
    pub fn get_message(&self, operation: &str, message_type: &str) -> Option<EncodedMessage> {
        self.python_messages
            .lock()
            .unwrap()
            .iter()
            .find(|msg| msg.operation == operation && msg.message_type == message_type)
            .cloned()
    }

    /// Compare a Rust-encoded message with the corresponding Python-encoded message
    pub fn compare_message(
        &self,
        rust_message: &[u8],
        operation: &str,
        message_type: &str,
    ) -> ComparisonResult {
        let python_message = self.get_message(operation, message_type);

        match python_message {
            None => ComparisonResult {
                matches: false,
                python_message: None,
                differences: vec![DifferenceType::NoPythonMessage],
                rust_message: rust_message.to_vec(),
            },
            Some(py_msg) => {
                let rust_message_vec = rust_message.to_vec();

                println!("Got py message for operation {}: {:?}", operation, py_msg);

                if rust_message_vec == py_msg.raw_bytes {
                    ComparisonResult {
                        matches: true,
                        python_message: Some(py_msg),
                        differences: vec![],
                        rust_message: rust_message_vec,
                    }
                } else {
                    // Find differences
                    let mut differences = vec![];

                    // Check if lengths are different
                    if rust_message_vec.len() != py_msg.raw_bytes.len() {
                        differences.push(DifferenceType::LengthMismatch {
                            rust_len: rust_message_vec.len(),
                            python_len: py_msg.raw_bytes.len(),
                        });
                    }

                    // Find byte-level differences
                    let mut byte_diffs = vec![];
                    for (i, (rust_byte, python_byte)) in rust_message_vec
                        .iter()
                        .zip(py_msg.raw_bytes.iter())
                        .enumerate()
                    {
                        if rust_byte != python_byte {
                            byte_diffs.push(ByteDifference {
                                position: i,
                                rust_byte: *rust_byte,
                                python_byte: *python_byte,
                            });

                            // Limit the number of differences to report
                            if byte_diffs.len() >= 10 {
                                break;
                            }
                        }
                    }

                    if !byte_diffs.is_empty() {
                        differences.push(DifferenceType::ByteDifferences(byte_diffs));
                    }

                    ComparisonResult {
                        matches: false,
                        python_message: Some(py_msg),
                        differences,
                        rust_message: rust_message_vec,
                    }
                }
            }
        }
    }

    /// Request the Python process to encode a specific operation message
    pub async fn request_encoding(
        &self,
        _operation: &str,
        _params: serde_json::Value,
    ) -> io::Result<()> {
        // This would send a command to the Python process to encode a specific message
        // In a real implementation, you would need a way to communicate with the Python process
        // For now, we'll assume the Python process is pre-configured to encode common messages
        Ok(())
    }

    /// Stop the Python process
    pub fn stop(&mut self) -> io::Result<()> {
        if let Some(mut process) = self.python_process.take() {
            // Try to terminate gracefully
            process.kill()?;
            process.wait()?;
        }
        Ok(())
    }
}

impl Drop for MessageComparisonHarness {
    fn drop(&mut self) {
        if let Err(e) = self.stop() {
            eprintln!("Failed to stop Python process: {}", e);
        }
    }
}

/// Result of comparing Rust and Python message encodings
#[derive(Debug)]
pub struct ComparisonResult {
    /// Whether the messages match exactly
    pub matches: bool,
    /// The Python-encoded message, if one was found
    pub python_message: Option<EncodedMessage>,
    /// List of differences between the messages
    pub differences: Vec<DifferenceType>,
    /// The Rust-encoded message
    pub rust_message: Vec<u8>,
}

/// Types of differences that can be found between messages
#[derive(Debug)]
pub enum DifferenceType {
    /// No Python message was found for comparison
    NoPythonMessage,
    /// The length of the messages differs
    LengthMismatch {
        /// Length of the Rust message
        rust_len: usize,
        /// Length of the Python message
        python_len: usize,
    },
    /// Specific byte differences
    ByteDifferences(Vec<ByteDifference>),
}

/// A single byte difference between messages
#[derive(Debug)]
pub struct ByteDifference {
    /// Position in the message where the difference was found
    pub position: usize,
    /// The byte in the Rust message
    pub rust_byte: u8,
    /// The byte in the Python message
    pub python_byte: u8,
}

// Create a Python script that encodes messages using the AWS IoT Device SDK
fn create_python_test_script() -> io::Result<PathBuf> {
    // Use the external Python script that was created in the tests/python directory
    let script_path = Path::new(file!())
        .parent()
        .unwrap()
        .join("python")
        .join("message_encoder.py");

    // Ensure the script is executable on Unix platforms
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = std::fs::metadata(&script_path)?;
        let mut perms = metadata.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms)?;
    }

    Ok(script_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[test]
    async fn test_harness_start_stop() {
        let mut harness = MessageComparisonHarness::new().expect("Failed to create harness");
        harness.start().await.expect("Failed to start harness");
        harness.stop().expect("Failed to stop harness");
    }

    #[test]
    async fn test_message_comparison() {
        let mut harness = MessageComparisonHarness::new().expect("Failed to create harness");
        harness.start().await.expect("Failed to start harness");

        // Wait briefly for Python to generate messages
        time::sleep(Duration::from_secs(1)).await;

        // Create a sample Rust message
        let rust_message = b"Sample encoded message for PublishToTopic";

        // Compare the messages
        let _result = harness.compare_message(rust_message, "PublishToTopic", "request");

        // In this simple example with placeholder messages, they should match
        // In a real test, you'd verify specific aspects of the comparison

        harness.stop().expect("Failed to stop harness");
    }
}
