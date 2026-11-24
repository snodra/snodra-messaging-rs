mod support;

use std::sync::Arc;
use std::time::Duration;

use snodra_messaging::MessagingError;
use snodra_messaging::hub::{
    CLIENT_VERSION, HubConnection, HubSettings, Inbound, PERMIT_FLUSH_AFTER, RegisteredTypes,
};
use snodra_messaging::proto::{
    self, ClientMessage, Heartbeat, Reply, ServerMessage, client_message, reply, server_message,
};
use support::fake_hub::{FakeHub, default_ack};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, timeout};

fn settings(address: String, backoff_min_ms: u64, backoff_max_ms: u64) -> HubSettings {
    HubSettings {
        address,
        backoff_min: Duration::from_millis(backoff_min_ms),
        backoff_max: Duration::from_millis(backoff_max_ms),
        handshake_timeout: Duration::from_secs(5),
        max_concurrent_handlers: 4,
        ..HubSettings::default()
    }
}

fn connection(settings: HubSettings) -> (Arc<HubConnection>, mpsc::Receiver<Inbound>) {
    let (tx, rx) = mpsc::channel(64);
    let conn = HubConnection::new(settings, "svc".into(), uuid::Uuid::new_v4().to_string(), tx)
        .expect("valid hub settings");
    (conn, rx)
}

fn heartbeat() -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Heartbeat(Heartbeat {
            sent_at_ms: 0,
        })),
    }
}

#[tokio::test]
async fn registers_with_declared_types() {
    let hub = FakeHub::start().await;
    let (conn, _rx) = connection(settings(hub.address(), 50, 500));
    conn.set_types(vec!["t.Cmd".into()], vec!["t.Q".into()]);
    conn.start();

    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let registers = hub.registers().await;
    assert_eq!(registers.len(), 1);
    assert_eq!(registers[0].service_name, "svc");
    assert!(!registers[0].instance_id.is_empty());
    assert_eq!(registers[0].command_types, vec!["t.Cmd"]);
    assert_eq!(registers[0].query_types, vec!["t.Q"]);
    assert!(conn.is_connected());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn registers_shared_query_types_apart() {
    let hub = FakeHub::start().await;
    let (conn, _rx) = connection(settings(hub.address(), 50, 500));
    conn.set_registered_types(RegisteredTypes {
        commands: vec!["t.Cmd".into()],
        queries: vec!["t.Q".into()],
        shared_queries: vec!["t.S2".into(), "t.S1".into()],
        // Exclusive streaming queries are query types to the hub: announced with `t.Q`.
        streaming_queries: vec!["t.A".into()],
        subscription_queries: vec![],
        shared_subscription_queries: vec![],
    });
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let r = &hub.registers().await[0];
    assert_eq!(r.command_types, vec!["t.Cmd"]);
    assert_eq!(r.query_types, vec!["t.A", "t.Q"]);
    assert_eq!(r.shared_query_types, vec!["t.S1", "t.S2"]);
    conn.shutdown().await;
    hub.stop();
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Watch {}
snodra_messaging::message_type!(Watch, "t.Watch");
impl snodra_messaging::SubscriptionQuery for Watch {
    type Initial = ();
    type Update = ();
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SharedWatch {}
snodra_messaging::message_type!(SharedWatch, "t.SharedWatch");
impl snodra_messaging::SubscriptionQuery for SharedWatch {
    type Initial = ();
    type Update = ();
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Lookup {}
snodra_messaging::message_type!(Lookup, "t.Lookup");
impl snodra_messaging::Query for Lookup {
    type Reply = ();
}

/// The types served by subscription handlers, exclusive or shared, are declared in
/// `Register.subscription_query_types` (each also in `query_types` or `shared_query_types`), on
/// the first `Register` and on every later one; plain queries and commands are not; the empty
/// `Register` of a shutdown declares none.
#[tokio::test]
async fn the_register_declares_the_subscription_types() {
    let hub = FakeHub::start().await;
    let (conn, _rx) = connection(settings(hub.address(), 50, 500));
    let mut registry = snodra_messaging::hub::HandlerRegistry::new();
    registry.subscription_query::<Watch, _, _>(|_, _| async { Ok(()) });
    registry.shared_subscription_query::<SharedWatch, _, _>(|_, _| async { Ok(()) });
    registry.query::<Lookup, _, _>(|_, _| async { Ok(()) });
    conn.set_registered_types(registry.registered_types());
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let first = hub.registers().await[0].clone();
    assert_eq!(first.subscription_query_types, ["t.SharedWatch", "t.Watch"]);
    assert_eq!(first.query_types, ["t.Lookup", "t.Watch"]);
    assert_eq!(first.shared_query_types, ["t.SharedWatch"]);
    // A reconnect registers again with the same declaration.
    hub.drop_streams();
    timeout(Duration::from_secs(5), async {
        while hub.registers().await.len() < 2 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("registered again");
    assert_eq!(
        hub.registers().await[1].subscription_query_types,
        ["t.SharedWatch", "t.Watch"]
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn reconnects_after_server_restart() {
    support::init_tracing();
    let hub = FakeHub::start().await;
    let (conn, mut rx) = connection(settings(hub.address(), 50, 1_000));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);

    hub.drop_streams();
    let disconnected = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Inbound::Disconnected { .. }) = rx.recv().await {
                break;
            }
        }
    })
    .await;
    assert!(disconnected.is_ok(), "no Disconnected notification");
    assert!(
        conn.await_registered(Duration::from_secs(3)).await,
        "did not re-register"
    );
    assert_eq!(hub.registers().await.len(), 2);
    assert!(conn.is_connected());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn already_exists_is_reported_and_retried_at_max_backoff() {
    let hub = FakeHub::start().await;
    hub.reject_next_with_already_exists();
    let (conn, _rx) = connection(settings(hub.address(), 50, 300));
    let started = Instant::now();
    conn.start();

    sleep(Duration::from_millis(120)).await;
    assert!(
        conn.last_rejection()
            .is_some_and(|r| r.contains("owned by other-service"))
    );
    assert!(
        hub.registers().await.is_empty(),
        "retried before backoff_max"
    );
    assert!(conn.await_registered(Duration::from_secs(3)).await);
    // The rejection wait is backoff_max jittered down by at most 20 %.
    assert!(started.elapsed() >= Duration::from_millis(240));
    assert_eq!(hub.registers().await.len(), 1);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn send_without_stream_is_hub_unavailable() {
    let (conn, _rx) = connection(settings("static://127.0.0.1:1".into(), 50, 500));
    assert!(matches!(
        conn.send(heartbeat()).await,
        Err(MessagingError::HubUnavailable(_))
    ));
}

#[tokio::test]
async fn invalid_argument_is_retried_at_max_backoff() {
    support::init_tracing();
    let hub = FakeHub::start().await;
    hub.reject_next_with_invalid_argument();
    let (conn, _rx) = connection(settings(hub.address(), 50, 300));
    let started = Instant::now();
    conn.start();
    sleep(Duration::from_millis(120)).await;
    assert!(
        conn.last_rejection()
            .is_some_and(|r| r.contains("service_name"))
    );
    assert!(
        hub.registers().await.is_empty(),
        "retried before backoff_max"
    );
    assert!(conn.await_registered(Duration::from_secs(3)).await);
    // The rejection wait is backoff_max jittered down by at most 20 %.
    assert!(started.elapsed() >= Duration::from_millis(240));
    conn.shutdown().await;
    hub.stop();
}

/// Like snodra-server, the fake hub closes a stream whose first `Register` offers no permits.
#[tokio::test]
async fn the_hub_refuses_a_register_without_permits() {
    let hub = FakeHub::start().await;
    let channel =
        tonic::transport::Endpoint::from_shared(hub.address().replace("static://", "http://"))
            .unwrap()
            .connect()
            .await
            .unwrap();
    let mut client = proto::hub_client::HubClient::new(channel);
    let (tx, rx) = mpsc::channel(4);
    tx.send(ClientMessage {
        message: Some(client_message::Message::Register(proto::Register {
            service_name: "svc".into(),
            instance_id: uuid::Uuid::new_v4().to_string(),
            initial_permits: 0,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut stream = client
        .connect(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let status = stream.message().await.expect_err("refused");
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    assert_eq!(
        status.message(),
        "Register.initial_permits must be at least 1"
    );
    assert!(hub.registers().await.is_empty());
    hub.stop();
}

/// `max_concurrent_handlers` is the `Register`'s `initial_permits`, which the hub refuses at 0:
/// settings built in code fail at once instead of retrying a refused registration forever.
#[tokio::test]
async fn max_concurrent_handlers_of_zero_is_refused() {
    let (tx, _rx) = mpsc::channel(1);
    let s = HubSettings {
        max_concurrent_handlers: 0,
        ..settings("static://127.0.0.1:1".into(), 50, 500)
    };
    let refused = HubConnection::new(s, "svc".into(), "i".into(), tx);
    assert!(
        matches!(refused, Err(MessagingError::InvalidMessage(ref m)) if m.contains("max_concurrent_handlers")),
        "{:?}",
        refused.err()
    );
}

/// The hub stops echoing a registered stream's heartbeats: after three heartbeat intervals of the
/// ack without an echo the stream fails (`UNAVAILABLE`, not a rejection) and the connection opens
/// a new one, which registers again.
#[tokio::test]
async fn a_registered_stream_whose_echoes_stop_reconnects() {
    let hub = FakeHub::start().await;
    hub.set_ack(Some(proto::RegisterAck {
        heartbeat_interval_ms: 100,
        ..default_ack()
    }));
    let (conn, mut rx) = connection(settings(hub.address(), 50, 100));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    hub.set_echo_heartbeats(false);
    let stopped = Instant::now();
    let lost = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Inbound::Disconnected { .. }) = rx.recv().await {
                break;
            }
        }
    })
    .await;
    assert!(lost.is_ok(), "a stream without echoes must be abandoned");
    assert!(
        stopped.elapsed() >= Duration::from_millis(290),
        "abandoned after {:?}, before three intervals without an echo",
        stopped.elapsed()
    );
    assert_eq!(
        conn.last_rejection(),
        None,
        "a missing echo is not a rejection"
    );
    hub.set_echo_heartbeats(true);
    timeout(Duration::from_secs(3), async {
        while hub.registers().await.len() < 2 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a new stream registers");
    assert!(conn.await_registered(Duration::from_secs(3)).await);
    conn.shutdown().await;
    hub.stop();
}

/// A hub that acknowledges the `Register` but never echoes a heartbeat: the silence is counted
/// from the ack, and the stream is dropped on the tick after the third unanswered heartbeat,
/// between three and four intervals after the ack, never at three.
#[tokio::test]
async fn a_hub_that_never_echoes_is_dropped_after_three_unanswered_heartbeats() {
    let hub = FakeHub::start().await;
    hub.set_ack(Some(proto::RegisterAck {
        heartbeat_interval_ms: 100,
        ..default_ack()
    }));
    hub.set_echo_heartbeats(false);
    let (conn, mut rx) = connection(settings(hub.address(), 50, 100));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let registered = Instant::now();
    let before = hub.heartbeats();
    timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Inbound::Disconnected { .. }) = rx.recv().await {
                break;
            }
        }
    })
    .await
    .expect("a stream without echoes must be abandoned");
    let after = registered.elapsed();
    assert!(
        after >= Duration::from_millis(370) && after < Duration::from_millis(900),
        "dropped {after:?} after the ack: not between three and four intervals"
    );
    assert!(
        hub.heartbeats() - before >= 3,
        "{} heartbeats sent after the ack before the drop",
        hub.heartbeats() - before
    );
    conn.shutdown().await;
    hub.stop();
}

/// A late echo starts the silence over: the hub echoes one heartbeat 1.5 intervals late, then
/// nothing; the stream is dropped more than three intervals after that late echo (not after the
/// heartbeat it answers), on the tick after the third heartbeat sent since it.
#[tokio::test]
async fn a_late_echo_starts_the_silence_over() {
    let hub = FakeHub::start().await;
    hub.set_ack(Some(proto::RegisterAck {
        heartbeat_interval_ms: 100,
        ..default_ack()
    }));
    let (conn, mut rx) = connection(settings(hub.address(), 50, 100));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    sleep(Duration::from_millis(250)).await; // a few echoed heartbeats
    hub.echo_once_late(Duration::from_millis(150));
    timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Inbound::Disconnected { .. }) = rx.recv().await {
                break;
            }
        }
    })
    .await
    .expect("the stream is dropped once the hub went silent");
    let dropped = std::time::Instant::now();
    let late = hub.late_echo_at().expect("the late echo was written");
    let after = dropped - late;
    assert!(
        after > Duration::from_millis(300) && after < Duration::from_millis(500),
        "dropped {after:?} after the late echo: not between three and four intervals"
    );
    conn.shutdown().await;
    hub.stop();
}

/// Before the `RegisterAck` nothing counts as silence: a hub that takes longer than three
/// heartbeat intervals (3 s each before the ack) to acknowledge, and echoes nothing meanwhile,
/// is waited for until the handshake timeout.
#[tokio::test]
async fn a_slow_ack_is_waited_for_until_the_handshake_timeout_not_dropped_as_silent() {
    let hub = FakeHub::start().await;
    // The fake hub handles a stream's messages in order: the heartbeats sent before the ack are
    // only echoed after it.
    hub.set_ack_delay(Duration::from_millis(9_600));
    let mut s = settings(hub.address(), 50, 100);
    s.handshake_timeout = Duration::from_secs(10);
    let (conn, mut rx) = connection(s);
    conn.start();
    let registered = conn.await_registered(Duration::from_secs(11)).await;
    assert!(
        registered,
        "the stream was dropped before its ack: {:?}",
        rx.try_recv().ok()
    );
    assert_eq!(
        hub.registers().await.len(),
        1,
        "the first stream registered"
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn hung_handshake_is_abandoned_and_reported() {
    // A server that accepts TCP but never speaks HTTP/2: the connect must give up after handshake_timeout.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hold = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        }
    });
    let mut s = settings(format!("static://{addr}"), 50, 500);
    s.handshake_timeout = Duration::from_millis(500);
    let (conn, mut rx) = connection(s);
    conn.start();
    let notified = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Inbound::Disconnected { .. }) = rx.recv().await {
                break;
            }
        }
    })
    .await;
    assert!(
        notified.is_ok(),
        "connect loop hung on a server that never answered"
    );
    conn.shutdown().await;
    hold.abort();
}

#[tokio::test]
async fn register_declares_the_client_version() {
    let hub = FakeHub::start().await;
    let (conn, _rx) = connection(settings(hub.address(), 50, 500));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let r = &hub.registers().await[0];
    assert_eq!(r.client_version, CLIENT_VERSION);
    assert!(r.client_version.starts_with("snodra-messaging-rs/"));
    assert!(r.credentials.is_none());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn ack_completes_the_handshake_and_its_interval_is_the_heartbeat_period() {
    let hub = FakeHub::start().await;
    let ack = proto::RegisterAck {
        heartbeat_interval_ms: 100,
        ..default_ack()
    };
    hub.set_ack(Some(ack.clone()));
    // Before the ack 3 s: only the announced 100 ms explains several heartbeats within a second.
    let (conn, _rx) = connection(settings(hub.address(), 50, 500));
    assert_eq!(conn.handshake(), None);
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert_eq!(conn.handshake(), Some(ack));
    let before = hub.heartbeats();
    sleep(Duration::from_secs(1)).await;
    let sent = hub.heartbeats() - before;
    assert!(sent >= 5, "{sent} heartbeats in 1 s at a 100 ms interval");
    conn.shutdown().await;
    hub.stop();
}

/// The `RegisterAck` is the only registration confirmation: a hub that echoes the heartbeat but
/// never acknowledges the `Register` leaves the stream unconfirmed, and it is abandoned at the
/// handshake timeout and opened again; once the hub acknowledges, the instance registers.
#[tokio::test]
async fn an_echo_without_an_ack_does_not_register_and_the_stream_is_abandoned() {
    let hub = FakeHub::start().await;
    hub.set_ack(None);
    let mut s = settings(hub.address(), 50, 100);
    s.handshake_timeout = Duration::from_millis(300);
    let (conn, mut rx) = connection(s);
    conn.start();
    let lost = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Inbound::Disconnected { .. }) = rx.recv().await {
                break;
            }
        }
    })
    .await;
    assert!(lost.is_ok(), "a stream without an ack must be abandoned");
    assert!(hub.heartbeats() >= 1, "the hub echoed the heartbeat");
    assert_eq!(conn.handshake(), None);
    assert!(!conn.await_registered(Duration::from_millis(10)).await);
    assert_eq!(
        conn.last_rejection(),
        None,
        "a handshake timeout is not a rejection"
    );
    hub.set_ack(Some(default_ack()));
    assert!(conn.await_registered(Duration::from_secs(3)).await);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn send_returns_the_stream_it_wrote_to() {
    let hub = FakeHub::start().await;
    let (conn, _rx) = connection(settings(hub.address(), 50, 500));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let stream = conn.send(heartbeat()).await.unwrap();
    assert_eq!(Some(stream), conn.current_stream());
    conn.shutdown().await;
    hub.stop();
}

/// Streaming writes `Flow` and `Cancel` for a request to the very stream it was written to:
/// `send_on` refuses any other stream (the request is gone with it), and the hub's `Flow` and
/// `Cancel` reach the owner as inbound messages.
#[tokio::test]
async fn send_on_writes_only_to_the_named_stream_and_flow_and_cancel_come_inbound() {
    let hub = FakeHub::start().await;
    let (conn, mut rx) = connection(settings(hub.address(), 50, 500));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let stream = conn.current_stream().unwrap();
    conn.send_on(stream, heartbeat()).await.unwrap();
    let e = conn.send_on(stream + 1_000, heartbeat()).await.unwrap_err();
    assert!(matches!(e, MessagingError::HubUnavailable(_)), "{e:?}");
    hub.send_to_clients(ServerMessage {
        message: Some(server_message::Message::Flow(proto::Flow {
            request_id: "s#1".into(),
            credits: 3,
        })),
    })
    .await;
    hub.send_to_clients(ServerMessage {
        message: Some(server_message::Message::Cancel(proto::Cancel {
            request_id: "s#1".into(),
            reason: "max results".into(),
        })),
    })
    .await;
    let flow = timeout(Duration::from_secs(2), rx.recv()).await.unwrap();
    assert!(
        matches!(flow, Some(Inbound::Flow(ref f)) if f.request_id == "s#1" && f.credits == 3),
        "{flow:?}"
    );
    let cancel = timeout(Duration::from_secs(2), rx.recv()).await.unwrap();
    assert!(
        matches!(cancel, Some(Inbound::Cancel(ref c)) if c.request_id == "s#1" && c.reason == "max results"),
        "{cancel:?}"
    );
    conn.shutdown().await;
    hub.stop();
}

fn request(id: &str) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Request(proto::Request {
            id: id.into(),
            kind: proto::RequestKind::Command as i32,
            r#type: "t.Cmd".into(),
            ..Default::default()
        })),
    }
}

fn reply_msg(id: &str) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Reply(Reply {
            request_id: id.into(),
            result: Some(reply::Result::Payload(b"{}".to_vec())),
        })),
    }
}

async fn next_request(
    rx: &mut mpsc::Receiver<Inbound>,
) -> (proto::Request, snodra_messaging::hub::ReplyTo) {
    timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Inbound::Request { request, reply_to }) = rx.recv().await {
                return (request, reply_to);
            }
        }
    })
    .await
    .expect("no request delivered")
}

#[tokio::test]
async fn reply_goes_to_the_arrival_stream_and_never_to_a_newer_one() {
    let hub = FakeHub::start().await;
    let (conn, mut rx) = connection(settings(hub.address(), 50, 500));
    conn.set_types(vec!["t.Cmd".into()], vec![]);
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);

    // A request on stream 1 whose handler is still running when stream 1 breaks.
    hub.send_to_clients(request("r1")).await;
    let (r1, reply_to_1) = next_request(&mut rx).await;
    assert_eq!(r1.id, "r1");
    let first = reply_to_1.stream();
    assert_eq!(Some(first), conn.current_stream());
    hub.drop_streams();
    timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Inbound::Disconnected { stream, .. }) = rx.recv().await {
                assert_eq!(stream, first);
                break;
            }
        }
    })
    .await
    .expect("no Disconnected for stream 1");
    assert!(conn.await_registered(Duration::from_secs(3)).await);
    let second = conn.current_stream().unwrap();
    assert_ne!(first, second);

    // The late reply is dropped: it must not travel on the new stream.
    let late = reply_to_1.send(reply_msg("r1")).await;
    assert!(
        matches!(late, Err(MessagingError::HubUnavailable(_))),
        "{late:?}"
    );

    // A request on stream 2 is answered on stream 2.
    hub.send_to_clients(request("r2")).await;
    let (r2, reply_to_2) = next_request(&mut rx).await;
    assert_eq!((r2.id.as_str(), reply_to_2.stream()), ("r2", second));
    reply_to_2.send(reply_msg("r2")).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while hub.replies().await.is_empty() && Instant::now() < deadline {
        sleep(Duration::from_millis(20)).await;
    }
    let replies = hub.replies_by_stream().await;
    assert_eq!(replies.len(), 1, "{replies:?}");
    assert_eq!(
        replies[0].0, 2,
        "reply arrived on hub stream {}",
        replies[0].0
    );
    assert_eq!(replies[0].1.request_id, "r2");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn backoff_restarts_from_the_floor_after_a_handshaken_stream() {
    // backoff_max 2 s: three drops of a handshaken stream must each reconnect within the floor's
    // jittered 120 ms (plus connect time), never at the doubled or capped value.
    let hub = FakeHub::start().await;
    let (conn, mut rx) = connection(settings(hub.address(), 100, 2_000));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    for round in 0..3 {
        hub.drop_streams();
        timeout(Duration::from_secs(2), async {
            loop {
                if let Some(Inbound::Disconnected { .. }) = rx.recv().await {
                    break;
                }
            }
        })
        .await
        .expect("no Disconnected");
        let started = Instant::now();
        assert!(conn.await_registered(Duration::from_secs(3)).await);
        let took = started.elapsed();
        assert!(
            took < Duration::from_millis(800),
            "round {round}: reconnect took {took:?}"
        );
    }
    conn.shutdown().await;
    hub.stop();
}

// ---- flow control ----

#[tokio::test]
async fn register_offers_permits_load_factor_and_tags() {
    let hub = FakeHub::start().await;
    let mut s = settings(hub.address(), 50, 500);
    s.load_factor = 250;
    s.tags = [("zone".to_string(), "eu-1".to_string())].into();
    let (conn, _rx) = connection(s);
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let r = &hub.registers().await[0];
    assert_eq!(
        r.initial_permits, 4,
        "max_concurrent_handlers permits up front"
    );
    assert_eq!(r.load_factor, 250);
    assert_eq!(r.tags.get("zone").map(String::as_str), Some("eu-1"));
    conn.shutdown().await;
    hub.stop();
}

/// Pushes `n` requests and hands back their reply handles, in order.
async fn requests_in(
    hub: &FakeHub,
    rx: &mut mpsc::Receiver<Inbound>,
    n: usize,
) -> Vec<snodra_messaging::hub::ReplyTo> {
    let mut handles = Vec::new();
    for i in 0..n {
        hub.send_to_clients(request(&format!("r{i}"))).await;
        handles.push(next_request(rx).await.1);
    }
    handles
}

/// While the hub still holds more than the low-water mark (min(16, initial / 2)) of our permits,
/// grants are batched: 16 pending go at once, fewer wait for the 50 ms timer. 64 permits and 20
/// requests leave the hub 44, so the mark (16) is never reached here.
#[tokio::test]
async fn permits_are_granted_in_batches_of_16_or_after_50_ms() {
    let hub = FakeHub::start().await;
    let mut s = settings(hub.address(), 50, 500);
    s.max_concurrent_handlers = 64;
    let (conn, mut rx) = connection(s);
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let handles = requests_in(&hub, &mut rx, 20).await;

    let started = std::time::Instant::now();
    for h in &handles[..16] {
        h.grant().await;
    }
    // The 16th grant flushes at once.
    timeout(Duration::from_millis(30), async {
        while hub.permits().is_empty() {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("a batch of 16 was not flushed at once");
    assert_eq!(hub.permits(), [(1, 16)]);

    for h in &handles[16..] {
        h.grant().await;
    }
    sleep(Duration::from_millis(20)).await;
    assert_eq!(hub.permits(), [(1, 16)], "4 grants must wait for the timer");
    sleep(Duration::from_millis(80)).await;
    assert_eq!(hub.permits(), [(1, 16), (1, 4)]);
    let (_, _, at) = hub.permits_timed()[1];
    assert!(
        at.duration_since(started) >= Duration::from_millis(45),
        "flushed too early"
    );
    conn.shutdown().await;
    hub.stop();
}

/// One permit: while the request runs the hub holds none for us, so the permit goes back as soon
/// as the request is finished, not after the flush timer (one permit would mean ~20 requests/s).
#[tokio::test]
async fn a_single_permit_goes_back_at_once() {
    let hub = FakeHub::start().await;
    let mut s = settings(hub.address(), 50, 500);
    s.max_concurrent_handlers = 1;
    let (conn, mut rx) = connection(s);
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert_eq!(hub.registers().await[0].initial_permits, 1);
    for round in 1..=3 {
        let handle = requests_in(&hub, &mut rx, 1).await.remove(0);
        let finished = std::time::Instant::now();
        handle.grant().await;
        timeout(PERMIT_FLUSH_AFTER / 2, async {
            while hub.permits().len() < round {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("round {round}: the permit waited for the flush timer"));
        let (stream, count, at) = hub.permits_timed()[round - 1];
        assert_eq!((stream, count), (1, 1));
        assert!(at.duration_since(finished) < PERMIT_FLUSH_AFTER / 2);
    }
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn permits_go_only_to_the_arrival_stream() {
    support::init_tracing();
    let hub = FakeHub::start().await;
    let (conn, mut rx) = connection(settings(hub.address(), 50, 500));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let old = requests_in(&hub, &mut rx, 1).await.remove(0);

    hub.drop_streams();
    timeout(Duration::from_secs(3), async {
        while hub.streams() < 2
            || conn.current_stream() == Some(old.stream())
            || conn.handshake().is_none()
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("no reconnect");
    let new = requests_in(&hub, &mut rx, 1).await.remove(0);
    old.grant().await; // the old stream is gone: nothing is sent, not even on the new one
    new.grant().await;
    sleep(Duration::from_millis(120)).await;
    assert_eq!(hub.permits(), [(2, 1)]);
    conn.shutdown().await;
    hub.stop();
}

// ---- Test support: the container readiness probe ----

#[tokio::test]
async fn wait_for_tcp_returns_once_the_port_accepts_and_gives_up_otherwise() {
    // Nothing listens: it gives up at the deadline.
    let free = support::containers::free_port();
    let addr = format!("127.0.0.1:{free}");
    assert!(!support::containers::wait_for_tcp(&addr, Duration::from_millis(300)).await);

    // A listener that only comes up after a while (a forward that is not ready at once).
    let late = tokio::spawn(async move {
        sleep(Duration::from_millis(400)).await;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", free))
            .await
            .unwrap();
        sleep(Duration::from_secs(3)).await;
        drop(listener);
    });
    let started = Instant::now();
    assert!(support::containers::wait_for_tcp(&addr, Duration::from_secs(5)).await);
    assert!(started.elapsed() >= Duration::from_millis(300));
    late.abort();
}

/// The application reads why a hub refuses the instance (`SnodraMessaging::hub_rejections`): by
/// the target dialled while that hub never registered it, by its hub id once it did; gone once
/// the hub accepts a registration again. `SnodraMessaging::latest_rejection` is the newest refusal
/// by any hub: replaced by a newer one only, never cleared by an acceptance.
#[tokio::test]
async fn the_application_reads_each_hubs_latest_rejection() {
    let hub = FakeHub::start().await;
    hub.reject_next_with_already_exists();
    let mut config = snodra_messaging::Config::new("svc-rejected", &hub.address());
    config.outbox_enabled = false;
    config.reconnect_backoff_min = Duration::from_millis(50);
    config.reconnect_backoff_max = Duration::from_millis(300);
    let app = snodra_messaging::SnodraMessaging::builder(config)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    let target = hub.address().trim_start_matches("static://").to_string();
    let deadline = Instant::now() + Duration::from_secs(5);
    let refused = loop {
        let rejections = app.hub_rejections();
        if !rejections.is_empty() {
            break rejections;
        }
        assert!(Instant::now() < deadline, "no rejection read");
        sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(refused.keys().collect::<Vec<_>>(), [&target]);
    assert!(
        refused[&target].contains("owned by other-service"),
        "{refused:?}"
    );
    let first = app.latest_rejection().expect("the latest rejection");
    assert_eq!(first.hub, target);
    assert_eq!(first.code, "ALREADY_EXISTS");
    assert!(
        first
            .description
            .as_deref()
            .is_some_and(|d| d.contains("owned by other-service")),
        "{first:?}"
    );
    assert!(first.at <= std::time::SystemTime::now());
    assert!(app.await_hub(Duration::from_secs(5)).await);
    assert!(app.hub_rejections().is_empty(), "accepted again");
    assert_eq!(
        app.latest_rejection(),
        Some(first.clone()),
        "an acceptance does not clear the latest rejection"
    );

    hub.reject_next_with_already_exists();
    hub.drop_streams();
    let deadline = Instant::now() + Duration::from_secs(5);
    while app.hub_rejections().is_empty() {
        assert!(Instant::now() < deadline, "no second rejection read");
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        app.hub_rejections().keys().collect::<Vec<_>>(),
        ["fake-hub"],
        "by its hub id once it registered the instance"
    );
    let second = app.latest_rejection().expect("the latest rejection");
    assert_eq!(second.hub, "fake-hub", "{second:?}");
    assert!(second.at >= first.at, "replaced by the newer refusal");
    app.shutdown().await;
    hub.stop();
}

/// Every later `Register` (here the empty one of a graceful shutdown) is followed by a
/// `Heartbeat`, as the first one is; the hub echoes it like any other.
#[tokio::test]
async fn a_later_register_is_followed_by_a_heartbeat() {
    let hub = FakeHub::start().await;
    hub.set_ack(Some(proto::RegisterAck {
        heartbeat_interval_ms: 60_000, // no tick heartbeat during the test
        ..default_ack()
    }));
    let (conn, _rx) = connection(settings(hub.address(), 50, 500));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    timeout(Duration::from_secs(3), async {
        while hub.heartbeats() < 1 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first heartbeat");
    let pool = snodra_messaging::hub::HubPool::of(Arc::clone(&conn));
    assert!(pool.deregister(Duration::from_secs(3)).await);
    timeout(Duration::from_secs(3), async {
        while hub.heartbeats() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a heartbeat after the later Register");
    assert_eq!(hub.registers().await.len(), 2);
    conn.shutdown().await;
    hub.stop();
}
