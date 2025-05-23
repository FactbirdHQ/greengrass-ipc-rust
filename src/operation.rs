//! Operation module for Greengrass IPC
//!
//! This module handles operations on the Greengrass IPC connection.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::event_stream::StreamResponseHandler;

/// A request/response operation
pub struct Operation<ResponseType> {
    connection: Arc<Connection>,
    operation_id: String,
    response_receiver: oneshot::Receiver<Result<ResponseType>>,
    timeout_duration: Duration,
}

impl<ResponseType> Operation<ResponseType> {
    /// Create a new operation
    pub(crate) fn new(
        connection: Arc<Connection>,
        operation_id: String,
        response_receiver: oneshot::Receiver<Result<ResponseType>>,
        timeout_duration: Duration,
    ) -> Self {
        Self {
            connection,
            operation_id,
            response_receiver,
            timeout_duration,
        }
    }

    /// Get the operation ID
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// Get the connection
    pub fn connection(&self) -> &Arc<Connection> {
        &self.connection
    }

    /// Get the result of the operation
    pub async fn get_result(self) -> Result<ResponseType> {
        match timeout(self.timeout_duration, self.response_receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::ConnectionClosed(
                "Response channel closed".to_string(),
            )),
            Err(_) => Err(Error::OperationTimeout),
        }
    }
}

impl<ResponseType> Future for Operation<ResponseType> {
    type Output = Result<ResponseType>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.response_receiver)
            .poll(cx)
            .map(|result| match result {
                Ok(response) => response,
                Err(_) => Err(Error::ConnectionClosed(
                    "Response channel closed".to_string(),
                )),
            })
    }
}

/// A streaming operation
pub struct StreamOperation<ResponseType, Handler> {
    connection: Arc<Connection>,
    operation_id: String,
    response_receiver: oneshot::Receiver<Result<ResponseType>>,
    handler: Arc<Handler>,
    close_sender: mpsc::Sender<()>,
    timeout_duration: Duration,
}

impl<ResponseType, Handler> StreamOperation<ResponseType, Handler>
where
    Handler: StreamResponseHandler + 'static,
{
    /// Create a new streaming operation
    pub(crate) fn new(
        connection: Arc<Connection>,
        operation_id: String,
        response_receiver: oneshot::Receiver<Result<ResponseType>>,
        handler: Handler,
        close_sender: mpsc::Sender<()>,
        timeout_duration: Duration,
    ) -> Self {
        Self {
            connection,
            operation_id,
            response_receiver,
            handler: Arc::new(handler),
            close_sender,
            timeout_duration,
        }
    }

    /// Get the operation ID
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// Get the connection
    pub fn connection(&self) -> &Arc<Connection> {
        &self.connection
    }

    /// Get the stream handler
    pub fn handler(&self) -> &Arc<Handler> {
        &self.handler
    }

    /// Get the result of the operation
    pub async fn get_result(self) -> Result<ResponseType> {
        match timeout(self.timeout_duration, self.response_receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::ConnectionClosed(
                "Response channel closed".to_string(),
            )),
            Err(_) => Err(Error::OperationTimeout),
        }
    }

    /// Close the stream
    pub async fn close(self) -> Result<()> {
        if let Err(_) = self.close_sender.send(()).await {
            return Err(Error::StreamClosed("Stream already closed".to_string()));
        }
        Ok(())
    }
}

impl<ResponseType, Handler> Future for StreamOperation<ResponseType, Handler>
where
    Handler: StreamResponseHandler + 'static,
{
    type Output = Result<ResponseType>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.response_receiver)
            .poll(cx)
            .map(|result| match result {
                Ok(response) => response,
                Err(_) => Err(Error::ConnectionClosed(
                    "Response channel closed".to_string(),
                )),
            })
    }
}

/// A task for handling stream events
pub(crate) struct StreamTask {
    connection: Arc<Connection>,
    operation_id: String,
    handler: Arc<dyn StreamResponseHandler>,
    close_receiver: mpsc::Receiver<()>,
}

impl StreamTask {
    /// Create a new stream task
    pub fn new(
        connection: Arc<Connection>,
        operation_id: String,
        handler: Arc<dyn StreamResponseHandler>,
        close_receiver: mpsc::Receiver<()>,
    ) -> Self {
        Self {
            connection,
            operation_id,
            handler,
            close_receiver,
        }
    }

    /// Run the stream task
    pub async fn run(mut self) {
        // This is a simplified implementation
        // In a real implementation, we would:
        // 1. Listen for messages on the connection
        // 2. Filter for messages with this operation ID
        // 3. Dispatch to the handler
        // 4. Handle close requests

        // Placeholder implementation
        tokio::select! {
            _ = self.close_receiver.recv() => {
                // Stream was closed by client
                self.handler.on_stream_closed();
            }
        }
    }
}
