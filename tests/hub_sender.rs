mod support;

use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::context::CURRENT;
use kraken_messaging::hub::{HubConnection, HubSettings, Inbound, SendOptions, Sender};
use kraken_messaging::proto::{self, ErrorCode, ServerMessage, reply, server_message};
use kraken_messaging::{Command, MessageContext, MessagingError, Query, message_type};
use serde::{Deserialize, Serialize};
use support::fake_hub::FakeHub;
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

async fn sender(hub: &FakeHub, default_timeout: Duration) -> (Arc<Sender>, Arc<HubConnection>) {
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
    let (sender, conn) = sender(&hub, Duration::from_secs(2)).await;
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
    let (sender, conn) = sender(&hub, Duration::from_secs(2)).await;
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
    let (sender, conn) = sender(&hub, Duration::from_secs(2)).await;
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
    let (sender, conn) = sender(&hub, Duration::from_secs(2)).await;
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
    let (sender, conn) = sender(&hub, Duration::from_secs(2)).await;
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
    let (sender, conn) = sender(&hub, Duration::from_secs(2)).await;
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
async fn disconnect_fails_pending_with_hub_unavailable() {
    let hub = FakeHub::start().await;
    let (sender, conn) = sender(&hub, Duration::from_secs(10)).await;
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
    assert!(matches!(err, MessagingError::HubUnavailable(_)), "{err:?}");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn no_reply_times_out() {
    let hub = FakeHub::start().await;
    let (sender, conn) = sender(&hub, Duration::from_secs(10)).await;
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
