//! Event stream implementation for Greengrass IPC
//!
//! This module implements the AWS Event Stream protocol used by the Greengrass IPC service.
//! Event Stream is a binary protocol format that allows for bidirectional streaming of messages.

use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::Mutex;

use crate::error::{Error, Result};

// Constants for the Event Stream protocol
const PRELUDE_LENGTH: usize = 8;
const PRELUDE_CRC_LENGTH: usize = 4;
const TRAILER_LENGTH: usize = 4;
const MIN_MESSAGE_LENGTH: usize = PRELUDE_LENGTH + PRELUDE_CRC_LENGTH + TRAILER_LENGTH;

// Message type constants
pub const MESSAGE_TYPE_REQUEST: &str = "Request";
pub const MESSAGE_TYPE_RESPONSE: &str = "Response";
pub const MESSAGE_TYPE_STREAM: &str = "StreamEvent";
pub const MESSAGE_TYPE_ERROR: &str = "Error";
pub const MESSAGE_TYPE_PING: &str = "Ping";
pub const MESSAGE_TYPE_PONG: &str = "Pong";
pub const MESSAGE_TYPE_CONNECT: &str = "Connect";
pub const MESSAGE_TYPE_CONNECT_ACK: &str = "ConnectAck";

/// A header in an Event Stream message
#[derive(Debug, Clone, PartialEq)]
pub enum HeaderValue {
    /// A boolean value
    Bool(bool),
    /// An 8-bit signed integer
    I8(i8),
    /// An 8-bit unsigned integer
    U8(u8),
    /// A 16-bit signed integer
    I16(i16),
    /// A 16-bit unsigned integer
    U16(u16),
    /// A 32-bit signed integer
    I32(i32),
    /// A 32-bit unsigned integer
    U32(u32),
    /// A 64-bit signed integer
    I64(i64),
    /// A 64-bit unsigned integer
    U64(u64),
    /// A UTF-8 string
    String(String),
    /// A byte array
    Bytes(Vec<u8>),
    /// A 64-bit timestamp (milliseconds since epoch)
    Timestamp(u64),
    /// A UUID (128-bit)
    Uuid([u8; 16]),
}

/// A message in the Event Stream protocol
#[derive(Debug, Clone)]
pub struct EventStreamMessage {
    /// Headers for the message
    pub headers: HashMap<String, HeaderValue>,
    /// Payload for the message
    pub payload: Bytes,
}

impl EventStreamMessage {
    /// Create a new Event Stream message
    pub fn new() -> Self {
        Self {
            headers: HashMap::new(),
            payload: Bytes::new(),
        }
    }

    /// Add a header to the message
    pub fn with_header<S: Into<String>>(mut self, name: S, value: HeaderValue) -> Self {
        self.headers.insert(name.into(), value);
        self
    }

    /// Add multiple headers to the message
    pub fn with_headers<S: Into<String>, I: IntoIterator<Item = (S, HeaderValue)>>(
        mut self,
        headers: I,
    ) -> Self {
        for (name, value) in headers {
            self.headers.insert(name.into(), value);
        }
        self
    }

    /// Set the payload of the message
    pub fn with_payload<B: Into<Bytes>>(mut self, payload: B) -> Self {
        self.payload = payload.into();
        self
    }

    /// Get a header by name
    pub fn get_header(&self, name: &str) -> Option<&HeaderValue> {
        self.headers.get(name)
    }

    /// Get a string header value
    pub fn get_string_header(&self, name: &str) -> Option<&str> {
        match self.get_header(name) {
            Some(HeaderValue::String(s)) => Some(s),
            _ => None,
        }
    }

    /// Get the message type header
    pub fn message_type(&self) -> Option<&str> {
        self.get_string_header(":message-type")
    }

    /// Encode the message to a byte buffer
    pub fn encode(&self) -> Result<Bytes> {
        // Implementation of Event Stream protocol encoding
        let mut buffer = BytesMut::new();

        // Encode headers first to determine their length
        let encoded_headers = self.encode_headers()?;
        let headers_len = encoded_headers.len();
        let payload_len = self.payload.len();

        // Calculate total message length
        let total_len =
            PRELUDE_LENGTH + PRELUDE_CRC_LENGTH + headers_len + payload_len + TRAILER_LENGTH;

        // Write prelude - total message length (4 bytes) and headers length (4 bytes)
        buffer.put_u32(total_len as u32);
        buffer.put_u32(headers_len as u32);

        // Calculate prelude CRC
        let prelude_crc = calculate_crc32(&buffer[0..PRELUDE_LENGTH]);
        buffer.put_u32(prelude_crc);

        // Write headers
        buffer.extend_from_slice(&encoded_headers);

        // Write payload
        buffer.extend_from_slice(&self.payload);

        // Calculate message CRC (over everything except the trailer)
        let message_crc = calculate_crc32(&buffer);
        buffer.put_u32(message_crc);

        Ok(buffer.freeze())
    }

    /// Encode headers to a byte buffer
    fn encode_headers(&self) -> Result<Bytes> {
        let mut buffer = BytesMut::new();

        for (name, value) in &self.headers {
            buffer.extend_from_slice(self.encode_header(name, value)?.as_ref());
        }

        Ok(buffer.freeze())
    }

    /// Encode a single header to a byte buffer
    fn encode_header(&self, name: &str, value: &HeaderValue) -> Result<Bytes> {
        let mut buffer = BytesMut::new();

        // Write header name length (1 byte)
        let name_bytes = name.as_bytes();
        if name_bytes.len() > 255 {
            return Err(Error::InvalidInput("Header name too long".to_string()));
        }
        buffer.put_u8(name_bytes.len() as u8);

        // Write header name
        buffer.extend_from_slice(name_bytes);

        // Write header type and value
        match value {
            HeaderValue::Bool(val) => {
                if *val {
                    buffer.put_u8(0); // type code for boolean_true
                    // No value bytes follow for boolean true
                } else {
                    buffer.put_u8(1); // type code for boolean_false
                    // No value bytes follow for boolean false
                }
            }
            HeaderValue::I8(val) => {
                buffer.put_u8(2); // type code for byte
                buffer.put_i8(*val);
            }
            HeaderValue::U8(val) => {
                buffer.put_u8(2); // type code for byte (treating as i8)
                buffer.put_i8(*val as i8);
            }
            HeaderValue::I16(val) => {
                buffer.put_u8(3); // type code for short
                buffer.put_i16(*val);
            }
            HeaderValue::U16(val) => {
                buffer.put_u8(3); // type code for short (treating as i16)
                buffer.put_i16(*val as i16);
            }
            HeaderValue::I32(val) => {
                buffer.put_u8(4); // type code for integer
                buffer.put_i32(*val);
            }
            HeaderValue::U32(val) => {
                buffer.put_u8(4); // type code for integer (treating as i32)
                buffer.put_i32(*val as i32);
            }
            HeaderValue::I64(val) => {
                buffer.put_u8(5); // type code for long
                buffer.put_i64(*val);
            }
            HeaderValue::U64(val) => {
                buffer.put_u8(5); // type code for long (treating as i64)
                buffer.put_i64(*val as i64);
            }
            HeaderValue::String(val) => {
                buffer.put_u8(7); // type code for string
                let bytes = val.as_bytes();
                if bytes.len() > u16::MAX as usize {
                    return Err(Error::InvalidInput("String value too long".to_string()));
                }
                buffer.put_u16(bytes.len() as u16);
                buffer.extend_from_slice(bytes);
            }
            HeaderValue::Bytes(val) => {
                buffer.put_u8(6); // type code for byte_array
                if val.len() > u16::MAX as usize {
                    return Err(Error::InvalidInput("Bytes value too long".to_string()));
                }
                buffer.put_u16(val.len() as u16);
                buffer.extend_from_slice(val);
            }
            HeaderValue::Timestamp(val) => {
                buffer.put_u8(8); // type code for timestamp
                buffer.put_u64(*val);
            }
            HeaderValue::Uuid(val) => {
                buffer.put_u8(9); // type code for uuid
                buffer.extend_from_slice(val);
            }
        }

        Ok(buffer.freeze())
    }

    /// Decode a message from a byte buffer
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        // Validate minimum message length
        if bytes.len() < MIN_MESSAGE_LENGTH {
            return Err(Error::InvalidInput(format!(
                "Message too short: {} bytes, minimum is {} bytes",
                bytes.len(),
                MIN_MESSAGE_LENGTH
            )));
        }

        let mut cursor = Cursor::new(bytes);

        // Parse prelude
        let total_len = cursor.get_u32() as usize;
        let headers_len = cursor.get_u32() as usize;

        // Validate total length
        if total_len != bytes.len() {
            return Err(Error::InvalidInput(format!(
                "Invalid message length: header says {}, actual is {}",
                total_len,
                bytes.len()
            )));
        }

        // Validate prelude CRC
        let expected_prelude_crc = cursor.get_u32();
        let actual_prelude_crc = calculate_crc32(&bytes[0..PRELUDE_LENGTH]);
        if expected_prelude_crc != actual_prelude_crc {
            return Err(Error::InvalidInput(format!(
                "Invalid prelude CRC: expected {:#x}, actual {:#x}",
                expected_prelude_crc, actual_prelude_crc
            )));
        }

        // Parse headers
        let headers_start = PRELUDE_LENGTH + PRELUDE_CRC_LENGTH;
        let headers_end = headers_start + headers_len;
        let headers = Self::decode_headers(&bytes[headers_start..headers_end])?;

        // Extract payload
        let payload_start = headers_end;
        let payload_end = bytes.len() - TRAILER_LENGTH;
        let payload = Bytes::copy_from_slice(&bytes[payload_start..payload_end]);

        // Validate message CRC
        let expected_message_crc = u32::from_be_bytes([
            bytes[payload_end],
            bytes[payload_end + 1],
            bytes[payload_end + 2],
            bytes[payload_end + 3],
        ]);
        let actual_message_crc = calculate_crc32(&bytes[..payload_end]);
        if expected_message_crc != actual_message_crc {
            return Err(Error::InvalidInput(format!(
                "Invalid message CRC: expected {:#x}, actual {:#x}",
                expected_message_crc, actual_message_crc
            )));
        }

        Ok(Self { headers, payload })
    }

    /// Decode headers from a byte buffer
    fn decode_headers(bytes: &[u8]) -> Result<HashMap<String, HeaderValue>> {
        let mut headers = HashMap::new();
        let mut cursor = Cursor::new(bytes);

        while cursor.position() < bytes.len() as u64 {
            let (name, value) = Self::decode_header(&mut cursor)?;
            headers.insert(name, value);
        }

        Ok(headers)
    }

    /// Decode a single header from a byte buffer
    fn decode_header(cursor: &mut Cursor<&[u8]>) -> Result<(String, HeaderValue)> {
        // Read header name
        let name_len = cursor.get_u8() as usize;
        let mut name_bytes = vec![0; name_len];
        cursor.read_exact(&mut name_bytes).map_err(Error::from)?;
        let name = String::from_utf8(name_bytes)
            .map_err(|e| Error::InvalidInput(format!("Invalid header name: {}", e)))?;

        // Read header type
        let header_type = cursor.get_u8();

        // Read header value
        let value = match header_type {
            0 => HeaderValue::Bool(true), // boolean_true - no value bytes
            1 => HeaderValue::Bool(false), // boolean_false - no value bytes
            2 => HeaderValue::I8(cursor.get_i8()), // byte
            3 => HeaderValue::I16(cursor.get_i16()), // short
            4 => HeaderValue::I32(cursor.get_i32()), // integer
            5 => HeaderValue::I64(cursor.get_i64()), // long
            6 => {
                // byte_array
                let len = cursor.get_u16() as usize;
                let mut bytes = vec![0; len];
                cursor.read_exact(&mut bytes).map_err(Error::from)?;
                HeaderValue::Bytes(bytes)
            }
            7 => {
                // string
                let len = cursor.get_u16() as usize;
                let mut bytes = vec![0; len];
                cursor.read_exact(&mut bytes).map_err(Error::from)?;
                let string = String::from_utf8(bytes)
                    .map_err(|e| Error::InvalidInput(format!("Invalid string: {}", e)))?;
                HeaderValue::String(string)
            }
            8 => HeaderValue::Timestamp(cursor.get_u64()), // timestamp
            9 => {
                // uuid
                let mut uuid = [0; 16];
                cursor.read_exact(&mut uuid).map_err(Error::from)?;
                HeaderValue::Uuid(uuid)
            }
            _ => {
                return Err(Error::InvalidInput(format!(
                    "Unknown header type: {}",
                    header_type
                )))
            }
        };

        Ok((name, value))
    }
}

/// An event handler for streaming responses
pub trait StreamResponseHandler: Send + Sync + 'static {
    /// Called when a stream event is received
    fn on_stream_event(&self, message: &EventStreamMessage);

    /// Called when an error occurs on the stream
    ///
    /// Return true to close the stream, false to keep it open.
    fn on_stream_error(&self, error: &Error) -> bool;

    /// Called when the stream is closed
    fn on_stream_closed(&self);
}

/// A reader for Event Stream messages
pub struct EventStreamReader {
    buffer: BytesMut,
}

impl EventStreamReader {
    /// Create a new Event Stream reader
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
        }
    }

    /// Add data to the reader's buffer
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to read a message from the buffer
    pub fn try_read_message(&mut self) -> Result<Option<EventStreamMessage>> {
        // Check if we have enough data for the prelude
        if self.buffer.len() < PRELUDE_LENGTH + PRELUDE_CRC_LENGTH {
            return Ok(None);
        }

        // Read prelude to get total message size
        let total_len = u32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize;

        // Check if we have the entire message
        if self.buffer.len() < total_len {
            return Ok(None);
        }

        // Parse the message
        let message_bytes = self.buffer.split_to(total_len);
        let message = EventStreamMessage::decode(&message_bytes)?;

        Ok(Some(message))
    }
}

/// A writer for Event Stream messages
pub struct EventStreamWriter {
    output: Arc<Mutex<Vec<u8>>>,
}

impl EventStreamWriter {
    /// Create a new Event Stream writer
    pub fn new() -> Self {
        Self {
            output: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Write a message to the output
    pub async fn write_message(&self, message: &EventStreamMessage) -> Result<()> {
        // Encode the message
        let encoded = message.encode()?;

        // Write to the output
        let mut output = self.output.lock().await;
        output.extend_from_slice(&encoded);

        Ok(())
    }

    /// Get a clone of the output buffer
    pub async fn output(&self) -> Vec<u8> {
        self.output.lock().await.clone()
    }
}

// Utility functions for CRC calculation
fn calculate_crc32(data: &[u8]) -> u32 {
    // CRC-32 implementation according to the Event Stream specification
    // This uses the same polynomial as Ethernet and zip files
    const CRC32_POLYNOMIAL: u32 = 0xEDB88320;
    let mut crc = 0xFFFFFFFFu32;

    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ CRC32_POLYNOMIAL
            } else {
                crc >> 1
            }
        }
    }

    !crc // Final XOR value
}

// Extend Error to handle std::io::Error
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Unknown(error.to_string())
    }
}
