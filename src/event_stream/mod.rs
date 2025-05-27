//! Event stream implementation for Greengrass IPC
//!
//! This module implements the AWS Event Stream protocol used by the Greengrass IPC service.
//! Event Stream is a binary protocol format that allows for bidirectional streaming of messages.

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

#[derive(Debug, Clone)]
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

/// Strongly-typed headers for the Event Stream protocol
#[derive(Debug, Clone, PartialEq)]
pub enum Header {
    /// Protocol version (":version")
    Version(String),
    /// Stream identifier (":stream-id")
    StreamId(i32),
    /// Message type (":message-type")
    MessageType(i32),
    /// Message flags (":message-flags")
    MessageFlags(i32),
    /// Content type (":content-type")
    ContentType(String),
    /// Operation name ("operation")
    Operation(String),
    /// Service model type ("service-model-type")
    ServiceModelType(String),
    /// Operation identifier (":operation-id")
    OperationId(String),
}

impl Header {
    // type code for string
    const STRING_TYPE: u8 = 7;

    /// Get the header name as used in the protocol
    pub fn name(&self) -> &'static str {
        match self {
            Header::Version(_) => ":version",
            Header::StreamId(_) => ":stream-id",
            Header::MessageType(_) => ":message-type",
            Header::MessageFlags(_) => ":message-flags",
            Header::ContentType(_) => ":content-type",
            Header::Operation(_) => "operation",
            Header::ServiceModelType(_) => "service-model-type",
            Header::OperationId(_) => ":operation-id",
        }
    }

    /// Encode a single header to a byte buffer
    fn encode(&self) -> Result<Bytes> {
        let mut buffer = BytesMut::new();

        // Write header name length (1 byte)
        let name_bytes = self.name().as_bytes();
        if name_bytes.len() > 255 {
            return Err(Error::InvalidInput("Header name too long".to_string()));
        }
        buffer.put_u8(name_bytes.len() as u8);

        // Write header name
        buffer.extend_from_slice(name_bytes);

        match self {
            // String types
            Header::ContentType(val)
            | Header::Version(val)
            | Header::Operation(val)
            | Header::ServiceModelType(val)
            | Header::OperationId(val) => {
                buffer.put_u8(Self::STRING_TYPE);
                let bytes = val.as_bytes();
                if bytes.len() > u16::MAX as usize {
                    return Err(Error::InvalidInput("String value too long".to_string()));
                }
                buffer.put_u16(bytes.len() as u16);
                buffer.extend_from_slice(bytes);
            }
            // I32 types
            Header::StreamId(val) | Header::MessageType(val) | Header::MessageFlags(val) => {
                buffer.put_u8(4); // type code for integer
                buffer.put_i32(*val);
            }
        }

        Ok(buffer.freeze())
    }

    /// Decode a single header from a byte buffer
    fn decode(cursor: &mut Cursor<&[u8]>) -> Result<Self> {
        // Read header name
        let name_len = cursor.get_u8() as usize;
        let mut name_bytes = vec![0; name_len];
        cursor.read_exact(&mut name_bytes).map_err(Error::from)?;
        let name = String::from_utf8(name_bytes)
            .map_err(|e| Error::InvalidInput(format!("Invalid header name: {}", e)))?;

        // Read header type
        let header_type = cursor.get_u8();

        // First decode the value based on wire format type
        let value = match header_type {
            0 => HeaderValue::Bool(true),          // boolean_true - no value bytes
            1 => HeaderValue::Bool(false),         // boolean_false - no value bytes
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

        // Then convert to the appropriate Header variant based on name and value
        let header = match (name.as_str(), value) {
            (":version", HeaderValue::String(s)) => Header::Version(s),
            (":stream-id", HeaderValue::I32(i)) => Header::StreamId(i),
            (":message-type", HeaderValue::I32(i)) => Header::MessageType(i),
            (":message-flags", HeaderValue::I32(i)) => Header::MessageFlags(i),
            (":content-type", HeaderValue::String(s)) => Header::ContentType(s),
            ("operation", HeaderValue::String(s)) => Header::Operation(s),
            ("service-model-type", HeaderValue::String(s)) => Header::ServiceModelType(s),
            (":operation-id", HeaderValue::String(s)) => Header::OperationId(s),
            (name, value) => {
                return Err(Error::InvalidInput(format!(
                    "Unknown header: {} with value {:?}",
                    name, value
                )))
            }
        };

        Ok(header)
    }
}

/// A message in the Event Stream protocol
#[derive(Debug, Clone)]
pub struct EventStreamMessage {
    /// Headers for the message
    pub headers: Vec<Header>,
    /// Payload for the message
    pub payload: Bytes,
}

impl EventStreamMessage {
    /// Create a new Event Stream message
    /// Create a new empty message
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
            payload: Bytes::new(),
        }
    }

    /// Add a header to the message
    pub fn with_header(mut self, header: Header) -> Self {
        self.headers.push(header);
        self
    }

    /// Add multiple headers to the message
    pub fn with_headers<I: IntoIterator<Item = Header>>(mut self, headers: I) -> Self {
        self.headers.extend(headers);
        self
    }

    /// Set the payload of the message
    pub fn with_payload<B: Into<Bytes>>(mut self, payload: B) -> Self {
        self.payload = payload.into();
        self
    }

    /// Get a header by name
    pub fn get_header(&self, name: &str) -> Option<&Header> {
        self.headers.iter().find(|h| h.name() == name)
    }

    /// Get a string header value
    pub fn get_string_header(&self, name: &str) -> Option<&str> {
        match self.get_header(name) {
            Some(Header::Version(s)) => Some(s),
            Some(Header::ContentType(s)) => Some(s),
            Some(Header::Operation(s)) => Some(s),
            Some(Header::ServiceModelType(s)) => Some(s),
            Some(Header::OperationId(s)) => Some(s),
            _ => None,
        }
    }

    /// Get the message type header
    pub fn message_type(&self) -> Option<i32> {
        match self.get_header(":message-type") {
            Some(Header::MessageType(i)) => Some(*i),
            _ => None,
        }
    }

    /// Get the stream ID header
    pub fn stream_id(&self) -> Option<i32> {
        match self.get_header(":stream-id") {
            Some(Header::StreamId(i)) => Some(*i),
            _ => None,
        }
    }

    /// Get the message flags header
    pub fn message_flags(&self) -> Option<i32> {
        match self.get_header(":message-flags") {
            Some(Header::MessageFlags(i)) => Some(*i),
            _ => None,
        }
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

        for header in &self.headers {
            buffer.extend_from_slice(header.encode()?.as_ref());
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
    fn decode_headers(bytes: &[u8]) -> Result<Vec<Header>> {
        let mut headers = Vec::new();
        let mut cursor = Cursor::new(bytes);

        while cursor.position() < bytes.len() as u64 {
            let header = Header::decode(&mut cursor)?;
            headers.push(header);
        }

        Ok(headers)
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
