mod support;

use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::MessagingError;
use kraken_messaging::hub::{
    CAPABILITIES, CLIENT_VERSION, Handshake, HubConnection, HubSettings, Inbound, PROTOCOL_VERSION,
};
use kraken_messaging::proto::{
    self, ClientMessage, Heartbeat, Reply, ServerMessage, client_message, reply, server_message,
};
use support::fake_hub::{FakeHub, default_ack};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, timeout};

fn settings(
    address: String,
    heartbeat_ms: u64,
    backoff_min_ms: u64,
    backoff_max_ms: u64,
) -> HubSettings {
    HubSettings {
        address,
        heartbeat_interval: Duration::from_millis(heartbeat_ms),
        backoff_min: Duration::from_millis(backoff_min_ms),
        backoff_max: Duration::from_millis(backoff_max_ms),
        handshake_timeout: Duration::from_secs(5),
        max_concurrent_handlers: 4,
        ..HubSettings::default()
    }
}

fn connection(settings: HubSettings) -> (Arc<HubConnection>, mpsc::Receiver<Inbound>) {
    let (tx, rx) = mpsc::channel(64);
    let conn = HubConnection::new(settings, "svc".into(), uuid::Uuid::new_v4().to_string(), tx);
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
    let (conn, _rx) = connection(settings(hub.address(), 1_000, 50, 500));
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
async fn legacy_hub_heartbeats_at_the_configured_interval() {
    let hub = FakeHub::start().await;
    hub.set_ack(None); // a hub older than protocol v2 announces no interval
    let (conn, _rx) = connection(settings(hub.address(), 100, 50, 500));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    sleep(Duration::from_secs(1)).await;
    assert!(hub.heartbeats() >= 3, "got {} heartbeats", hub.heartbeats());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn reconnects_after_server_restart() {
    support::init_tracing();
    let hub = FakeHub::start().await;
    let (conn, mut rx) = connection(settings(hub.address(), 1_000, 50, 1_000));
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
    let (conn, _rx) = connection(settings(hub.address(), 1_000, 50, 300));
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
    let (conn, _rx) = connection(settings("static://127.0.0.1:1".into(), 1_000, 50, 500));
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
    let (conn, _rx) = connection(settings(hub.address(), 1_000, 50, 300));
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
    let mut s = settings(format!("static://{addr}"), 1_000, 50, 500);
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
async fn register_declares_protocol_v2_and_client_version() {
    let hub = FakeHub::start().await;
    let (conn, _rx) = connection(settings(hub.address(), 1_000, 50, 500));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let r = &hub.registers().await[0];
    assert_eq!(r.protocol_version, PROTOCOL_VERSION);
    assert_eq!(r.protocol_version, 2);
    assert_eq!(r.capabilities, ["flow-control"]);
    assert_eq!(r.client_version, CLIENT_VERSION);
    assert!(r.client_version.starts_with("kraken-messaging-rs/"));
    assert!(r.credentials.is_none());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn ack_completes_the_handshake_and_its_interval_replaces_the_fallback() {
    let hub = FakeHub::start().await;
    let ack = proto::RegisterAck {
        heartbeat_interval_ms: 100,
        ..default_ack()
    };
    hub.set_ack(Some(ack.clone()));
    // Fallback 10 s: only the announced 100 ms explains several heartbeats within a second.
    let (conn, _rx) = connection(settings(hub.address(), 10_000, 50, 500));
    assert_eq!(conn.handshake(), None);
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let shared = proto::RegisterAck {
        capabilities: vec!["flow-control".into()], // we declared it, the hub shares it
        ..ack
    };
    assert_eq!(conn.handshake(), Some(Handshake::V2(shared)));
    let before = hub.heartbeats();
    sleep(Duration::from_secs(1)).await;
    let sent = hub.heartbeats() - before;
    assert!(sent >= 5, "{sent} heartbeats in 1 s at a 100 ms interval");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn echo_without_ack_is_legacy_mode() {
    let hub = FakeHub::start().await;
    hub.set_ack(None);
    let (conn, _rx) = connection(settings(hub.address(), 1_000, 50, 500));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert_eq!(conn.handshake(), Some(Handshake::Legacy));
    assert!(conn.handshake().unwrap().is_legacy());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn no_ack_and_no_echo_fails_the_handshake_and_reconnects() {
    let hub = FakeHub::start().await;
    hub.set_ack(None);
    hub.set_echo_heartbeats(false);
    let mut s = settings(hub.address(), 1_000, 50, 100);
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
    assert!(lost.is_ok(), "an unconfirmed stream must be abandoned");
    assert!(!conn.await_registered(Duration::from_millis(10)).await);
    hub.set_echo_heartbeats(true);
    assert!(conn.await_registered(Duration::from_secs(3)).await);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn send_returns_the_stream_it_wrote_to() {
    let hub = FakeHub::start().await;
    let (conn, _rx) = connection(settings(hub.address(), 1_000, 50, 500));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let stream = conn.send(heartbeat()).await.unwrap();
    assert_eq!(Some(stream), conn.current_stream());
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
) -> (proto::Request, kraken_messaging::hub::ReplyTo) {
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
    let (conn, mut rx) = connection(settings(hub.address(), 1_000, 50, 500));
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
            if let Some(Inbound::Disconnected { stream }) = rx.recv().await {
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
    let (conn, mut rx) = connection(settings(hub.address(), 1_000, 100, 2_000));
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

// ---- flow control (P1) ----

#[tokio::test]
async fn register_offers_permits_load_factor_and_tags() {
    let hub = FakeHub::start().await;
    let mut s = settings(hub.address(), 1_000, 50, 500);
    s.load_factor = 250;
    s.tags = [("zone".to_string(), "eu-1".to_string())].into();
    let (conn, _rx) = connection(s);
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let r = &hub.registers().await[0];
    assert!(CAPABILITIES.contains(&"flow-control"));
    assert_eq!(
        r.initial_permits, 4,
        "max_concurrent_handlers permits up front"
    );
    assert_eq!(r.load_factor, 250);
    assert_eq!(r.tags.get("zone").map(String::as_str), Some("eu-1"));
    assert!(conn.handshake().unwrap().flow_control());
    conn.shutdown().await;
    hub.stop();
}

/// Pushes `n` requests and hands back their reply handles, in order.
async fn requests_in(
    hub: &FakeHub,
    rx: &mut mpsc::Receiver<Inbound>,
    n: usize,
) -> Vec<kraken_messaging::hub::ReplyTo> {
    let mut handles = Vec::new();
    for i in 0..n {
        hub.send_to_clients(request(&format!("r{i}"))).await;
        handles.push(next_request(rx).await.1);
    }
    handles
}

#[tokio::test]
async fn permits_are_granted_in_batches_of_16_or_after_50_ms() {
    let hub = FakeHub::start().await;
    let (conn, mut rx) = connection(settings(hub.address(), 1_000, 50, 500));
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

#[tokio::test]
async fn no_permits_to_a_hub_without_flow_control() {
    for p0_hub in [true, false] {
        let hub = FakeHub::start().await;
        if p0_hub {
            hub.set_flow_control(false); // the merged P0 hub: ack without the capability
        } else {
            hub.set_ack(None); // a v1 hub: no ack at all
        }
        let (conn, mut rx) = connection(settings(hub.address(), 1_000, 50, 500));
        conn.start();
        assert!(conn.await_registered(Duration::from_secs(5)).await);
        assert!(!conn.handshake().unwrap().flow_control());
        for h in requests_in(&hub, &mut rx, 20).await {
            h.grant().await;
        }
        sleep(Duration::from_millis(120)).await;
        assert!(
            hub.permits().is_empty(),
            "permits sent to a hub without flow control"
        );
        conn.shutdown().await;
        hub.stop();
    }
}

#[tokio::test]
async fn permits_go_only_to_the_arrival_stream() {
    support::init_tracing();
    let hub = FakeHub::start().await;
    let (conn, mut rx) = connection(settings(hub.address(), 1_000, 50, 500));
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
