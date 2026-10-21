//! A scriptable stand-in for the Greengrass core's IPC server, for tests.
//!
//! Accepts the CONNECT, then answers subscriptions per topic the way the
//! nucleus does (an accepted response, or an error response carrying
//! `TERMINATE_STREAM`), records every frame the client sends, and lets a test
//! deliver messages, end a stream from the server side, or drop the connection.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::mpsc;

use crate::event_stream::{EventStreamMessage, EventStreamReader, Header};

/// How the fake answers a subscribe to a topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    Accept,
    /// Error response with `TERMINATE_STREAM`, as for an unauthorized topic.
    Reject,
    /// Never answer (until [`FakeCore::answer`]).
    Hold,
}

/// A frame the client sent after CONNECT.
#[derive(Clone, Debug)]
pub struct Frame {
    pub stream_id: i32,
    pub flags: i32,
    pub operation: Option<String>,
    /// `topicName` / `topic` of a subscribe request.
    pub topic: Option<String>,
}

impl Frame {
    pub fn is_terminate(&self) -> bool {
        self.flags & 2 != 0
    }
}

enum Command {
    Write(EventStreamMessage),
    Disconnect,
}

#[derive(Default)]
struct State {
    answers: HashMap<String, Answer>,
    frames: Vec<Frame>,
    /// Stream ID of each subscribe, by topic (latest wins).
    streams: HashMap<String, i32>,
}

pub struct FakeCore {
    _dir: tempfile::TempDir,
    pub path: PathBuf,
    state: Arc<Mutex<State>>,
    commands: mpsc::UnboundedSender<Command>,
}

fn accepted(stream_id: i32) -> EventStreamMessage {
    EventStreamMessage::new()
        .with_header(Header::MessageType(0))
        .with_header(Header::MessageFlags(0))
        .with_header(Header::StreamId(stream_id))
        .with_payload(b"{}".to_vec())
}

fn rejected(stream_id: i32) -> EventStreamMessage {
    EventStreamMessage::new()
        .with_header(Header::MessageType(1))
        .with_header(Header::MessageFlags(2))
        .with_header(Header::StreamId(stream_id))
        .with_payload(br#"{"message":"not authorized","_errorCode":"UnauthorizedError"}"#.to_vec())
}

impl FakeCore {
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ipc.socket");
        let listener = UnixListener::bind(&path).unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let (commands, mut command_rx) = mpsc::unbounded_channel::<Command>();
        let task_state = state.clone();
        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let (mut read, mut write) = sock.into_split();
            let (out_tx, mut out_rx) = mpsc::unbounded_channel::<EventStreamMessage>();
            let writer = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = out_rx.recv() => match msg {
                            Some(msg) => {
                                if write.write_all(&msg.encode().unwrap()).await.is_err() {
                                    return;
                                }
                            }
                            None => return,
                        },
                        cmd = command_rx.recv() => match cmd {
                            Some(Command::Write(msg)) => {
                                if write.write_all(&msg.encode().unwrap()).await.is_err() {
                                    return;
                                }
                            }
                            Some(Command::Disconnect) | None => return,
                        },
                    }
                }
            });
            let mut reader = EventStreamReader::new();
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = match read.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                if writer.is_finished() {
                    break;
                }
                reader.add_data(&buf[..n]);
                while let Ok(Some(msg)) = reader.try_read_message() {
                    if let Some(Header::MessageType(4)) = msg.get_header(":message-type") {
                        let ack = EventStreamMessage::new()
                            .with_header(Header::MessageType(5))
                            .with_header(Header::MessageFlags(1));
                        let _ = out_tx.send(ack);
                        continue;
                    }
                    let stream_id = match msg.get_header(":stream-id") {
                        Some(Header::StreamId(id)) => *id,
                        _ => 0,
                    };
                    let flags = match msg.get_header(":message-flags") {
                        Some(Header::MessageFlags(f)) => *f,
                        _ => 0,
                    };
                    let operation = msg.get_header("operation").and_then(Header::string_value);
                    let topic = serde_json::from_slice::<serde_json::Value>(&msg.payload)
                        .ok()
                        .and_then(|v| {
                            v.get("topicName")
                                .or_else(|| v.get("topic"))
                                .and_then(|t| t.as_str().map(str::to_string))
                        });
                    let mut st = task_state.lock().unwrap();
                    st.frames.push(Frame {
                        stream_id,
                        flags,
                        operation: operation.clone(),
                        topic: topic.clone(),
                    });
                    if let (Some(_), Some(topic)) = (&operation, &topic) {
                        st.streams.insert(topic.clone(), stream_id);
                        match st.answers.get(topic).copied().unwrap_or(Answer::Accept) {
                            Answer::Accept => {
                                let _ = out_tx.send(accepted(stream_id));
                            }
                            Answer::Reject => {
                                let _ = out_tx.send(rejected(stream_id));
                            }
                            Answer::Hold => {}
                        }
                    }
                }
            }
        });
        Self {
            _dir: dir,
            path,
            state,
            commands,
        }
    }

    pub fn set_answer(&self, topic: &str, answer: Answer) {
        self.state
            .lock()
            .unwrap()
            .answers
            .insert(topic.to_string(), answer);
    }

    pub fn frames(&self) -> Vec<Frame> {
        self.state.lock().unwrap().frames.clone()
    }

    pub fn subscribes(&self, topic: &str) -> usize {
        self.frames()
            .iter()
            .filter(|f| f.operation.is_some() && f.topic.as_deref() == Some(topic))
            .count()
    }

    pub fn terminates(&self) -> Vec<i32> {
        self.frames()
            .iter()
            .filter(|f| f.is_terminate())
            .map(|f| f.stream_id)
            .collect()
    }

    pub fn stream_of(&self, topic: &str) -> i32 {
        *self
            .state
            .lock()
            .unwrap()
            .streams
            .get(topic)
            .expect("topic was subscribed")
    }

    /// Answer a held subscribe.
    pub fn answer(&self, topic: &str) {
        let id = self.stream_of(topic);
        let _ = self.commands.send(Command::Write(accepted(id)));
    }

    /// Deliver an IoT Core message on the topic's stream.
    pub fn deliver(&self, topic: &str, payload: &str) {
        self.deliver_with_flags(topic, payload, 0);
    }

    /// End the topic's stream from the server side: an empty message with
    /// `TERMINATE_STREAM`.
    pub fn end_stream(&self, topic: &str) {
        let msg = EventStreamMessage::new()
            .with_header(Header::MessageType(0))
            .with_header(Header::MessageFlags(2))
            .with_header(Header::StreamId(self.stream_of(topic)));
        let _ = self.commands.send(Command::Write(msg));
    }

    fn deliver_with_flags(&self, topic: &str, payload: &str, flags: i32) {
        let id = self.stream_of(topic);
        let body = serde_json::json!({
            "message": {
                "topicName": topic,
                "payload": base64::engine::general_purpose::STANDARD.encode(payload),
            }
        });
        let msg = EventStreamMessage::new()
            .with_header(Header::MessageType(0))
            .with_header(Header::MessageFlags(flags))
            .with_header(Header::StreamId(id))
            .with_header(Header::ServiceModelType(
                "aws.greengrass#IoTCoreMessage".to_string(),
            ))
            .with_payload(serde_json::to_vec(&body).unwrap());
        let _ = self.commands.send(Command::Write(msg));
    }

    pub fn disconnect(&self) {
        let _ = self.commands.send(Command::Disconnect);
    }

    /// Wait until `cond` holds over the recorded frames.
    pub async fn wait_for(&self, what: &str, cond: impl Fn(&[Frame]) -> bool) {
        for _ in 0..200 {
            if cond(&self.frames()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("fake core never saw: {what}; frames: {:?}", self.frames());
    }
}
