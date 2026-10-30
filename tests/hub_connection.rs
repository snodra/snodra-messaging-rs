mod support;

use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::MessagingError;
use kraken_messaging::hub::{HubConnection, HubSettings, Inbound};
use kraken_messaging::proto::{ClientMessage, Heartbeat, client_message};
use support::fake_hub::FakeHub;
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
async fn heartbeats_are_sent_at_interval() {
    let hub = FakeHub::start().await;
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
            if let Some(Inbound::Disconnected) = rx.recv().await {
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

    sleep(Duration::from_millis(150)).await;
    assert!(
        conn.last_rejection()
            .is_some_and(|r| r.contains("owned by other-service"))
    );
    assert!(
        hub.registers().await.is_empty(),
        "retried before backoff_max"
    );
    assert!(conn.await_registered(Duration::from_secs(3)).await);
    assert!(started.elapsed() >= Duration::from_millis(300));
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
    sleep(Duration::from_millis(150)).await;
    assert!(
        conn.last_rejection()
            .is_some_and(|r| r.contains("service_name"))
    );
    assert!(
        hub.registers().await.is_empty(),
        "retried before backoff_max"
    );
    assert!(conn.await_registered(Duration::from_secs(3)).await);
    assert!(started.elapsed() >= Duration::from_millis(300));
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
            if let Some(Inbound::Disconnected) = rx.recv().await {
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
