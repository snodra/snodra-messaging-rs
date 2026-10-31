mod support;

use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::context::CURRENT;
use kraken_messaging::hub::{HubConnection, HubSettings, Inbound, SendOptions, Sender};
use kraken_messaging::proto::{self, ErrorCode, ServerMessage, reply, server_message};
use kraken_messaging::{Command, MessageContext, MessagingError, Query, message_type};
use serde::{Deserialize, Serialize};
use support::fake_hub::{FakeHub, default_ack};
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize)]
struct Ping {
    text: String,
}
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Pong {
    echoed: String,
}
message_type!(Ping, "t.Ping");
impl Command for Ping {
    type Reply = Pong;
}

#[derive(Serialize, Deserialize)]
struct Nop {}
message_type!(Nop, "t.Nop");
impl Command for Nop {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Count {
    name: String,
}
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Total {
    n: u64,
}
message_type!(Count, "t.Count");
impl Query for Count {
    type Reply = Total;
}

fn reply_with(req: &proto::Request, payload: &str) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Reply(proto::Reply {
            request_id: req.id.clone(),
            result: Some(reply::Result::Payload(payload.as_bytes().to_vec())),
        })),
    }
}

fn reply_error(req: &proto::Request, code: ErrorCode, message: &str) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Reply(proto::Reply {
            request_id: req.id.clone(),
            result: Some(reply::Result::Error(proto::Error {
                request_id: req.id.clone(),
                code: code as i32,
                message: message.into(),
                details: vec![],
            })),
        })),
    }
}

fn hub_error(req: &proto::Request, code: ErrorCode, message: &str) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Error(proto::Error {
            request_id: req.id.clone(),
            code: code as i32,
            message: message.into(),
            details: vec![],
        })),
    }
}

async fn sender(
    hub: &FakeHub,
    default_timeout: Option<Duration>,
) -> (Arc<Sender>, Arc<HubConnection>) {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let settings = HubSettings {
        address: hub.address(),
        heartbeat_interval: Duration::from_secs(1),
        backoff_min: Duration::from_millis(50),
        backoff_max: Duration::from_millis(500),
        ..HubSettings::default()
    };
    let conn = HubConnection::new(settings, "svc".into(), uuid::Uuid::new_v4().to_string(), tx);
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let sender = Sender::new(Arc::clone(&conn), default_timeout, true);
    let forward = Arc::clone(&sender);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            forward.on_inbound(msg);
        }
    });
    (sender, conn)
}

#[tokio::test]
async fn send_command_round_trips_reply() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_with(&req, r#"{"echoed":"x"}"#)))
        .await;
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(2))).await;
    let pong = sender
        .send_command(&Ping { text: "x".into() }, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(pong, Pong { echoed: "x".into() });
    let sent = hub.requests().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].kind, proto::RequestKind::Command as i32);
    assert_eq!(sent[0].r#type, "t.Ping");
    assert_eq!(sent[0].payload, br#"{"text":"x"}"#);
    assert_eq!(sent[0].timeout_ms, 2_000);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn send_query_round_trips_reply_with_routing_key() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_with(&req, r#"{"n":3}"#)))
        .await;
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(2))).await;
    let total = sender
        .send_query(
            &Count { name: "abc".into() },
            SendOptions {
                routing_key: Some("abc".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(total, Total { n: 3 });
    let sent = hub.requests().await;
    assert_eq!(sent[0].kind, proto::RequestKind::Query as i32);
    assert_eq!(sent[0].routing_key, "abc");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn hub_error_no_handler_maps_to_no_handler() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(hub_error(&req, ErrorCode::NoHandler, "no handler")))
        .await;
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(2))).await;
    let err = sender
        .send_command(&Ping { text: "x".into() }, SendOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, MessagingError::NoHandler(ref t) if t == "t.Ping"),
        "{err:?}"
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn hub_error_permission_denied_maps_to_access_denied() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| {
        Some(reply_error(
            &req,
            ErrorCode::PermissionDenied,
            "access denied",
        ))
    })
    .await;
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(2))).await;
    let err = sender
        .send_command(&Ping { text: "x".into() }, SendOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, MessagingError::AccessDenied(ref m) if m == "access denied"),
        "{err:?}"
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn send_decodes_unit_from_empty_object() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_with(&req, "{}"))).await;
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(2))).await;
    sender
        .send_command(&Nop {}, SendOptions::default())
        .await
        .unwrap();
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn request_carries_correlation_and_forwarded_authorization() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_with(&req, r#"{"echoed":"x"}"#)))
        .await;
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(2))).await;
    let ctx = MessageContext {
        correlation_id: "c-9".into(),
        authorization: Some("Bearer t".into()),
        ..Default::default()
    };
    CURRENT
        .scope(ctx, async {
            sender
                .send_command(&Ping { text: "x".into() }, SendOptions::default())
                .await
                .unwrap()
        })
        .await;
    let headers = &hub.requests().await[0].headers;
    assert_eq!(
        headers.get("correlationId").map(String::as_str),
        Some("c-9")
    );
    assert_eq!(
        headers.get("authorization").map(String::as_str),
        Some("Bearer t")
    );

    // Outside any handler: a fresh correlation id, no authorization.
    sender
        .send_command(&Ping { text: "y".into() }, SendOptions::default())
        .await
        .unwrap();
    let headers = &hub.requests().await[1].headers;
    assert!(
        headers
            .get("correlationId")
            .is_some_and(|c| !c.is_empty() && c != "c-9")
    );
    assert!(!headers.contains_key("authorization"));
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn disconnect_fails_written_request_with_hub_connection_lost() {
    let hub = FakeHub::start().await;
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(10))).await;
    let pending = tokio::spawn({
        let sender = Arc::clone(&sender);
        async move {
            sender
                .send_command(&Ping { text: "x".into() }, SendOptions::default())
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    hub.drop_streams();
    let err = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("not failed on disconnect")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(err, MessagingError::HubConnectionLost(_)),
        "{err:?}"
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn no_reply_times_out() {
    let hub = FakeHub::start().await;
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(10))).await;
    let started = std::time::Instant::now();
    let err = sender
        .send_command(
            &Ping { text: "x".into() },
            SendOptions {
                timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, MessagingError::Timeout(ref t) if t == "t.Ping"),
        "{err:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(hub.requests().await[0].timeout_ms, 200);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn v2_hub_gets_zero_unless_a_timeout_was_chosen() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_with(&req, "{}"))).await;
    let (sender, conn) = sender(&hub, None).await;
    sender
        .send_command(&Nop {}, SendOptions::default())
        .await
        .unwrap();
    sender
        .send_command(
            &Nop {},
            SendOptions {
                timeout: Some(Duration::from_millis(1_234)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let sent = hub.requests().await;
    assert_eq!(
        sent[0].timeout_ms, 0,
        "the hub decides (override or default)"
    );
    assert_eq!(sent[1].timeout_ms, 1_234, "the caller's choice is sent");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn legacy_hub_gets_todays_five_seconds() {
    let hub = FakeHub::start().await;
    hub.set_ack(None);
    hub.set_responder(|req| Some(reply_with(&req, "{}"))).await;
    let (sender, conn) = sender(&hub, None).await;
    sender
        .send_command(&Nop {}, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(hub.requests().await[0].timeout_ms, 5_000);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn local_guard_uses_the_acks_per_type_timeout() {
    // The hub never answers; the guard is the announced t.Nop override (300 ms) plus 1 s.
    let hub = FakeHub::start().await;
    let mut ack = default_ack();
    ack.type_timeouts_ms.insert("t.Nop".into(), 300);
    hub.set_ack(Some(ack));
    let (sender, conn) = sender(&hub, None).await;
    let started = std::time::Instant::now();
    let err = sender
        .send_command(&Nop {}, SendOptions::default())
        .await
        .unwrap_err();
    let took = started.elapsed();
    assert!(
        matches!(err, MessagingError::Timeout(ref t) if t == "t.Nop"),
        "{err:?}"
    );
    assert!(
        took >= Duration::from_millis(1_250) && took < Duration::from_millis(2_500),
        "guard fired after {took:?}"
    );
    assert_eq!(hub.requests().await[0].timeout_ms, 0);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn send_with_no_stream_is_hub_unavailable() {
    let hub = FakeHub::start().await;
    let (sender, conn) = sender(&hub, None).await;
    conn.shutdown().await; // no live stream any more: nothing can be written
    let err = sender
        .send_command(&Nop {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, MessagingError::HubUnavailable(_)), "{err:?}");
    assert!(hub.requests().await.is_empty());
    hub.stop();
}

#[tokio::test]
async fn disconnect_of_another_stream_leaves_a_pending_request_alone() {
    let hub = FakeHub::start().await;
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(10))).await;
    let stream = conn.current_stream().unwrap();
    let pending = tokio::spawn({
        let sender = Arc::clone(&sender);
        async move {
            sender
                .send_command(&Ping { text: "x".into() }, SendOptions::default())
                .await
        }
    });
    while hub.requests().await.is_empty() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // A late notification about an older stream must not fail a request written to the current one.
    sender.on_inbound(Inbound::Disconnected { stream: stream - 1 });
    sender.on_inbound(Inbound::Disconnected { stream: stream + 7 });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !pending.is_finished(),
        "failed by another stream's Disconnected"
    );
    sender.on_inbound(Inbound::Disconnected { stream });
    let err = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("not failed by its own stream's Disconnected")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(err, MessagingError::HubConnectionLost(_)),
        "{err:?}"
    );
    conn.shutdown().await;
    hub.stop();
}
