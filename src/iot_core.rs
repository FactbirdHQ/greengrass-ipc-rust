//! Shared, pooled IoT Core subscriptions.
//!
//! The Greengrass core maps IPC `SubscribeToIoTCore` streams onto cloud MQTT
//! subscriptions with two properties a client has to work around (nucleus
//! v2.16.0, `mqttclient/MqttClient.java`):
//!
//! - **No unsubscribe acknowledgement.** Closing a stream (`TERMINATE_STREAM`)
//!   is the only unsubscribe there is. The core then removes its local
//!   subscription at once but drops the cloud filter only on UNSUBACK, which
//!   the client never sees.
//! - **Resubscribing in that window strands the topic.** A subscribe that
//!   arrives before the UNSUBACK piggybacks on the dying filter, is answered
//!   with success, and is left without any cloud subscription once the
//!   UNSUBACK lands, permanently: it is not resubscribed on reconnect either.
//!   Terminating a subscribe whose SUBACK is still pending leaves an orphaned
//!   cloud subscription instead.
//!
//! So a client must not tie IPC streams to its own subscribe/unsubscribe
//! cycles. The pool keeps one IPC stream per topic filter per connection and
//! fans each message out to every local subscriber:
//!
//! - **Subscribing** joins the topic's stream, or opens it. Opening runs in a
//!   background task, so a caller that is dropped or gives up (its own
//!   timeout) never cancels a pending subscribe; the next caller joins it.
//! - **Unsubscribing** (dropping the [`IoTCoreSubscription`]) detaches the
//!   subscriber locally and synchronously. The stream stays, so a later
//!   subscribe to the topic reuses it and receives everything published after
//!   its subscribe returns.
//! - **Streams are only closed under pressure.** When opening a stream takes the
//!   pool past its limit, the longest-idle streams (no subscribers) are closed.
//!   A closed topic is then held back for [`DEFAULT_RESUBSCRIBE_HOLD`] before it
//!   is subscribed again, so the core has processed its UNSUBACK first. Every
//!   cloud subscription counts towards the per-connection limit of 50, past
//!   which the core opens `<thing>#N` connections.
//! - **A stream the core ends** (error response, server-side close, lost IPC
//!   connection) is removed and never terminated again; its subscribers see
//!   the end of the stream and resubscribe.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::Stream;
use tokio::sync::{mpsc, watch};

use crate::client::{StreamOperation, open_stream};
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::model::{IoTCoreMessage, SubscribeToIoTCoreRequest};

/// Default number of IoT Core streams a client keeps before it closes idle
/// ones. Each is one cloud subscription.
pub const DEFAULT_STREAM_LIMIT: usize = 40;

/// Default time a topic whose stream the pool closed is held back before it is
/// subscribed again. It only has to outlast the core's UNSUBACK round trip.
pub const DEFAULT_RESUBSCRIBE_HOLD: Duration = Duration::from_secs(30);

#[derive(Clone)]
enum Status {
    Opening,
    Open,
    Failed(Error),
    Ended,
}

struct Slot {
    topic: String,
    status: watch::Sender<Status>,
    subscribers: Mutex<HashMap<u64, mpsc::UnboundedSender<IoTCoreMessage>>>,
    /// When the last subscriber left; `None` while it has subscribers.
    idle_since: Mutex<Option<Instant>>,
    /// The task owning the IPC stream. Aborting it drops the stream, which
    /// terminates it on the core.
    task: Mutex<Option<tokio::task::AbortHandle>>,
}

impl Slot {
    fn is_closable(&self) -> bool {
        matches!(*self.status.borrow(), Status::Open) && self.subscribers.lock().unwrap().is_empty()
    }
}

pub(crate) struct IoTCorePool {
    connection: Arc<Connection>,
    slots: Mutex<HashMap<String, Arc<Slot>>>,
    /// Topics closed by the pool, with the time they may be subscribed again.
    held: Mutex<HashMap<String, Instant>>,
    stream_limit: AtomicUsize,
    resubscribe_hold: Mutex<Duration>,
    next_subscriber: AtomicU64,
}

impl IoTCorePool {
    pub(crate) fn new(connection: Arc<Connection>) -> Arc<Self> {
        Arc::new(Self {
            connection,
            slots: Mutex::new(HashMap::new()),
            held: Mutex::new(HashMap::new()),
            stream_limit: AtomicUsize::new(DEFAULT_STREAM_LIMIT),
            resubscribe_hold: Mutex::new(DEFAULT_RESUBSCRIBE_HOLD),
            next_subscriber: AtomicU64::new(0),
        })
    }

    pub(crate) fn set_stream_limit(&self, limit: usize) {
        self.stream_limit.store(limit.max(1), Ordering::Relaxed);
    }

    pub(crate) fn set_resubscribe_hold(&self, hold: Duration) {
        *self.resubscribe_hold.lock().unwrap() = hold;
    }

    pub(crate) fn stream_count(&self) -> usize {
        self.slots.lock().unwrap().len()
    }

    pub(crate) fn topics(&self) -> Vec<String> {
        self.slots.lock().unwrap().keys().cloned().collect()
    }

    pub(crate) async fn subscribe(
        self: &Arc<Self>,
        request: SubscribeToIoTCoreRequest,
        timeout: Duration,
    ) -> Result<IoTCoreSubscription> {
        let topic = request.topic_name.clone();
        let deadline = tokio::time::Instant::now() + timeout;

        // A topic the pool closed recently waits until the core is done with it.
        let held_until = self.held.lock().unwrap().get(&topic).copied();
        if let Some(until) = held_until {
            let now = Instant::now();
            if until > now {
                if tokio::time::Instant::now() + (until - now) > deadline {
                    return Err(Error::OperationTimeout);
                }
                tokio::time::sleep(until - now).await;
            }
            self.held.lock().unwrap().remove(&topic);
        }

        let (slot, opened) = {
            let mut slots = self.slots.lock().unwrap();
            match slots.get(&topic) {
                Some(slot) => (slot.clone(), false),
                None => {
                    let (status, _) = watch::channel(Status::Opening);
                    let slot = Arc::new(Slot {
                        topic: topic.clone(),
                        status,
                        subscribers: Mutex::new(HashMap::new()),
                        idle_since: Mutex::new(None),
                        task: Mutex::new(None),
                    });
                    slots.insert(topic.clone(), slot.clone());
                    (slot, true)
                }
            }
        };

        // Join before waiting: nothing the stream delivers after it is open
        // can be missed.
        let id = self.next_subscriber.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        slot.subscribers.lock().unwrap().insert(id, tx);
        *slot.idle_since.lock().unwrap() = None;
        let lease = Lease {
            slot: slot.clone(),
            id,
        };

        if opened {
            self.open(&slot, request);
            self.close_idle_over_limit();
        }

        let mut status = slot.status.subscribe();
        let ready =
            tokio::time::timeout_at(deadline, status.wait_for(|s| !matches!(s, Status::Opening)))
                .await;
        let outcome = match ready {
            // Gave up waiting; the subscribe goes on in the background.
            Err(_) => return Err(Error::OperationTimeout),
            Ok(Err(_)) => Status::Ended,
            Ok(Ok(s)) => s.clone(),
        };
        match outcome {
            Status::Open => Ok(IoTCoreSubscription { rx, _lease: lease }),
            Status::Failed(e) => Err(e),
            Status::Opening | Status::Ended => Err(Error::ConnectionClosed(format!(
                "IoT Core subscription to {topic} ended"
            ))),
        }
    }

    /// Open the slot's IPC stream in a task that also forwards its messages.
    fn open(self: &Arc<Self>, slot: &Arc<Slot>, request: SubscribeToIoTCoreRequest) {
        let connection = self.connection.clone();
        let pool = Arc::downgrade(self);
        let task_slot = slot.clone();
        let handle = tokio::spawn(async move {
            let slot = task_slot;
            // No timeout: callers bound their own wait, and abandoning a
            // pending subscribe would orphan its cloud subscription.
            let opened: Result<StreamOperation<IoTCoreMessage>> = open_stream(
                &connection,
                None,
                "SubscribeToIoTCore",
                "SubscribeToIoTCoreRequest",
                "IoTCoreMessage",
                &request,
            )
            .await;
            let mut stream = match opened {
                Ok(stream) => stream,
                Err(e) => {
                    log::warn!("Subscribe to {} failed: {}", slot.topic, e);
                    finish(&pool, &slot, Status::Failed(e));
                    return;
                }
            };
            slot.status.send_replace(Status::Open);
            while let Some(message) = stream.recv().await {
                let subscribers = slot.subscribers.lock().unwrap();
                for tx in subscribers.values() {
                    let _ = tx.send(message.clone());
                }
            }
            // Ended by the core or a lost connection; nothing to terminate.
            log::debug!("IoT Core stream for {} ended", slot.topic);
            finish(&pool, &slot, Status::Ended);
        });
        *slot.task.lock().unwrap() = Some(handle.abort_handle());
    }

    /// Close the longest-idle open streams while the pool is over its limit.
    /// Streams in use or still opening are never closed.
    fn close_idle_over_limit(&self) {
        let limit = self.stream_limit.load(Ordering::Relaxed);
        let hold = *self.resubscribe_hold.lock().unwrap();
        let mut slots = self.slots.lock().unwrap();
        while slots.len() > limit {
            let oldest = slots
                .values()
                .filter(|s| s.is_closable())
                .filter_map(|s| s.idle_since.lock().unwrap().map(|t| (t, s.topic.clone())))
                .min();
            let Some((_, topic)) = oldest else {
                break;
            };
            let slot = slots.remove(&topic).expect("picked from the map");
            log::debug!("Closing idle IoT Core stream for {}", topic);
            let now = Instant::now();
            let mut held = self.held.lock().unwrap();
            held.retain(|_, until| *until > now);
            held.insert(topic, now + hold);
            drop(held);
            slot.status.send_replace(Status::Ended);
            if let Some(task) = slot.task.lock().unwrap().take() {
                // Drops the stream, which sends TERMINATE_STREAM.
                task.abort();
            }
        }
    }
}

/// Remove a slot that failed or ended and release its subscribers.
fn finish(pool: &Weak<IoTCorePool>, slot: &Arc<Slot>, status: Status) {
    if let Some(pool) = pool.upgrade() {
        let mut slots = pool.slots.lock().unwrap();
        if slots.get(&slot.topic).is_some_and(|s| Arc::ptr_eq(s, slot)) {
            slots.remove(&slot.topic);
        }
    }
    slot.status.send_replace(status);
    // Closing the senders ends every subscriber's stream.
    slot.subscribers.lock().unwrap().clear();
}

/// A subscriber's claim on a pooled stream; released on drop. The stream stays
/// open: only pressure from new topics closes it.
struct Lease {
    slot: Arc<Slot>,
    id: u64,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut subscribers = self.slot.subscribers.lock().unwrap();
        subscribers.remove(&self.id);
        if subscribers.is_empty() {
            *self.slot.idle_since.lock().unwrap() = Some(Instant::now());
        }
    }
}

/// A subscription to an IoT Core topic filter, sharing the pooled stream for
/// that filter with every other subscriber on the same client.
///
/// Dropping it (or [`close`](Self::close)) unsubscribes at once: no further
/// message reaches it. The underlying stream stays open for reuse, so a new
/// subscription to the same filter receives everything published after its
/// subscribe returns. There is no acknowledgement from the cloud broker behind
/// this; see the module docs. The stream yields `None` when the core ended the
/// subscription or the IPC connection was lost: subscribe again.
pub struct IoTCoreSubscription {
    rx: mpsc::UnboundedReceiver<IoTCoreMessage>,
    _lease: Lease,
}

impl IoTCoreSubscription {
    /// Receive the next message, or `None` once the subscription has ended.
    pub async fn recv(&mut self) -> Option<IoTCoreMessage> {
        self.rx.recv().await
    }

    /// Unsubscribe. Equivalent to dropping the subscription.
    pub async fn close(self) -> Result<()> {
        Ok(())
    }
}

impl Stream for IoTCoreSubscription {
    type Item = IoTCoreMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
