//! IoT Core subscription pool against the fake core.

use std::time::Duration;

use futures::StreamExt;

use crate::fake_core::{Answer, FakeCore};
use crate::model::{QoS, SubscribeToIoTCoreRequest};
use crate::{Error, GreengrassCoreIPCClient, IoTCoreSubscription};

const A: &str = "$aws/things/t/shadow/name/network/update/delta";
const B: &str = "$aws/things/t/shadow/name/network/get/accepted";
const C: &str = "$aws/things/t/jobs/notify-next";

async fn client(core: &FakeCore) -> GreengrassCoreIPCClient {
    let client = connect(core).await;
    client.set_iot_core_startup_hold(Duration::ZERO);
    client
}

async fn connect(core: &FakeCore) -> GreengrassCoreIPCClient {
    GreengrassCoreIPCClient::connect_with_options(
        Some(core.path.clone()),
        Some("token".into()),
        None,
        Some(Duration::from_secs(2)),
    )
    .await
    .unwrap()
}

fn request(topic: &str) -> SubscribeToIoTCoreRequest {
    SubscribeToIoTCoreRequest {
        topic_name: topic.into(),
        qos: QoS::AtLeastOnce,
    }
}

async fn next_payload(sub: &mut IoTCoreSubscription) -> Option<String> {
    tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("no message")
        .map(|m| String::from_utf8(m.message.payload.to_vec()).unwrap())
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn subscribers_to_one_topic_share_a_stream() {
    let core = FakeCore::start().await;
    let client = client(&core).await;

    let mut first = client.subscribe_to_iot_core(request(A)).await.unwrap();
    let mut second = client.subscribe_to_iot_core(request(A)).await.unwrap();
    assert_eq!(core.subscribes(A), 1);
    assert_eq!(client.iot_core_stream_count(), 1);

    core.deliver(A, "one");
    assert_eq!(next_payload(&mut first).await.as_deref(), Some("one"));
    assert_eq!(next_payload(&mut second).await.as_deref(), Some("one"));
}

/// Unsubscribing detaches at once; the stream stays for the next subscriber,
/// so no TERMINATE_STREAM ever races a resubscribe.
#[tokio::test]
async fn unsubscribe_keeps_the_stream_for_the_next_subscribe() {
    let core = FakeCore::start().await;
    let client = client(&core).await;

    let mut kept = client.subscribe_to_iot_core(request(A)).await.unwrap();
    let dropped = client.subscribe_to_iot_core(request(A)).await.unwrap();
    drop(dropped);
    core.deliver(A, "after drop");
    assert_eq!(next_payload(&mut kept).await.as_deref(), Some("after drop"));
    drop(kept);

    settle().await;
    let mut again = client.subscribe_to_iot_core(request(A)).await.unwrap();
    core.deliver(A, "two");
    assert_eq!(next_payload(&mut again).await.as_deref(), Some("two"));
    assert_eq!(core.subscribes(A), 1, "reused, not resubscribed");
    assert!(core.terminates().is_empty());
}

/// A caller that gives up (or is dropped) does not cancel a pending subscribe;
/// the next caller joins it.
#[tokio::test]
async fn abandoned_subscribe_goes_on_and_is_joined() {
    let core = FakeCore::start().await;
    let mut client = client(&core).await;
    client.set_operation_timeout(Duration::from_millis(100));
    core.set_answer(A, Answer::Hold);

    let err = client.subscribe_to_iot_core(request(A)).await.err();
    assert!(matches!(err, Some(Error::OperationTimeout)), "{err:?}");
    let dropped = tokio::time::timeout(
        Duration::from_millis(50),
        client.subscribe_to_iot_core(request(A)),
    )
    .await;
    assert!(dropped.is_err());
    settle().await;
    assert!(
        core.terminates().is_empty(),
        "no terminate of a pending subscribe"
    );
    assert_eq!(core.subscribes(A), 1);

    client.set_operation_timeout(Duration::from_secs(2));
    let joined = {
        let client = &client;
        let answer = async {
            settle().await;
            core.answer(A);
        };
        let (sub, ()) = tokio::join!(client.subscribe_to_iot_core(request(A)), answer);
        sub
    };
    let mut sub = joined.unwrap();
    core.deliver(A, "late");
    assert_eq!(next_payload(&mut sub).await.as_deref(), Some("late"));
    assert_eq!(core.subscribes(A), 1);
}

/// The core ends a rejected stream itself; a TERMINATE_STREAM for it would make
/// the core close the whole IPC connection.
#[tokio::test]
async fn rejected_subscribe_is_never_terminated() {
    let core = FakeCore::start().await;
    let client = client(&core).await;
    core.set_answer(A, Answer::Reject);

    let err = client.subscribe_to_iot_core(request(A)).await.err();
    assert!(
        err.is_some() && !matches!(err, Some(Error::OperationTimeout)),
        "{err:?}"
    );
    settle().await;
    assert!(core.terminates().is_empty());
    assert_eq!(client.iot_core_stream_count(), 0);

    // The connection is intact and the next subscribe starts over.
    core.set_answer(A, Answer::Accept);
    client.subscribe_to_iot_core(request(B)).await.unwrap();
    client.subscribe_to_iot_core(request(A)).await.unwrap();
    assert_eq!(core.subscribes(A), 2);
}

/// A stream the core ends is not terminated again, its subscribers see the end,
/// and the next subscribe opens a fresh stream.
#[tokio::test]
async fn stream_ended_by_the_core_is_not_terminated() {
    let core = FakeCore::start().await;
    let client = client(&core).await;

    let mut sub = client.subscribe_to_iot_core(request(A)).await.unwrap();
    core.end_stream(A);
    assert_eq!(next_payload(&mut sub).await, None);
    drop(sub);
    settle().await;
    assert!(core.terminates().is_empty());
    assert_eq!(client.iot_core_stream_count(), 0);

    client.subscribe_to_iot_core(request(A)).await.unwrap();
    assert_eq!(core.subscribes(A), 2);
}

/// Idle streams are only closed when a new topic takes the pool over its
/// limit, oldest idle first, and a closed topic is held back before it is
/// subscribed again.
#[tokio::test]
async fn idle_streams_close_only_over_the_limit() {
    let core = FakeCore::start().await;
    let client = client(&core).await;
    client.set_iot_core_stream_limit(2);
    client.set_iot_core_resubscribe_hold(Duration::from_millis(300));

    drop(client.subscribe_to_iot_core(request(A)).await.unwrap());
    settle().await;
    drop(client.subscribe_to_iot_core(request(B)).await.unwrap());
    settle().await;
    assert!(core.terminates().is_empty(), "idle but within the limit");

    let _c = client.subscribe_to_iot_core(request(C)).await.unwrap();
    let a_stream = core.stream_of(A);
    core.wait_for("terminate of A", |f| {
        f.iter()
            .any(|f| f.is_terminate() && f.stream_id == a_stream)
    })
    .await;
    assert_eq!(core.terminates(), vec![a_stream], "only the oldest idle");
    let mut topics = client.iot_core_topics();
    topics.sort();
    let mut expected = vec![B.to_string(), C.to_string()];
    expected.sort();
    assert_eq!(topics, expected);

    let started = tokio::time::Instant::now();
    client.subscribe_to_iot_core(request(A)).await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "A was held back after its close"
    );
    assert_eq!(core.subscribes(A), 2);
}

/// The first subscribe after connecting waits out the startup hold before it
/// reaches the core; once the hold has passed, subscribes go out at once.
#[tokio::test]
async fn first_subscribe_waits_out_the_startup_hold() {
    let core = FakeCore::start().await;
    let client = connect(&core).await;
    client.set_iot_core_startup_hold(Duration::from_millis(300));

    let started = tokio::time::Instant::now();
    let pending = client.subscribe_to_iot_core(request(A));
    let early = async {
        tokio::time::sleep(Duration::from_millis(150)).await;
        core.subscribes(A)
    };
    let (sub, seen_early) = tokio::join!(pending, early);
    sub.unwrap();
    assert_eq!(seen_early, 0, "subscribe reached the core inside the hold");
    assert!(started.elapsed() >= Duration::from_millis(250));

    let started = tokio::time::Instant::now();
    client.subscribe_to_iot_core(request(B)).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "no hold after it passed"
    );
}

/// The default hold applies to a client that does not configure one, and a
/// caller giving up during it does not cancel the subscribe.
#[tokio::test]
async fn subscribe_abandoned_during_the_startup_hold_goes_on() {
    let core = FakeCore::start().await;
    let mut client = connect(&core).await;
    client.set_operation_timeout(Duration::from_millis(100));

    let err = client.subscribe_to_iot_core(request(A)).await.err();
    assert!(matches!(err, Some(Error::OperationTimeout)), "{err:?}");
    assert_eq!(core.subscribes(A), 0);

    client.set_operation_timeout(Duration::from_secs(5));
    client.subscribe_to_iot_core(request(A)).await.unwrap();
    assert_eq!(core.subscribes(A), 1, "joined, not resubscribed");
    assert!(core.terminates().is_empty());
}

/// Streams in use are never closed, even over the limit.
#[tokio::test]
async fn streams_in_use_are_never_closed() {
    let core = FakeCore::start().await;
    let client = client(&core).await;
    client.set_iot_core_stream_limit(1);

    let _a = client.subscribe_to_iot_core(request(A)).await.unwrap();
    let _b = client.subscribe_to_iot_core(request(B)).await.unwrap();
    settle().await;
    assert!(core.terminates().is_empty());
    assert_eq!(client.iot_core_stream_count(), 2);
}

/// A lost IPC connection ends every subscription instead of leaving readers
/// waiting forever.
#[tokio::test]
async fn lost_connection_ends_subscriptions() {
    let core = FakeCore::start().await;
    let client = client(&core).await;

    let mut sub = client.subscribe_to_iot_core(request(A)).await.unwrap();
    core.disconnect();
    assert_eq!(next_payload(&mut sub).await, None);
}
