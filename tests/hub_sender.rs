mod support;

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use snodra_messaging::RetryPolicy;
use snodra_messaging::context::CURRENT;
use snodra_messaging::hub::{
    HubConnection, HubSettings, Inbound, SendOptions, Sender, SenderSettings,
};
use snodra_messaging::proto::{self, ErrorCode, ServerMessage, reply, server_message};
use snodra_messaging::{Command, MessageContext, MessagingError, Query, message_type};
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
    sender_with(
        hub,
        SenderSettings {
            default_timeout,
            ..SenderSettings::default()
        },
    )
    .await
}

async fn sender_with(
    hub: &FakeHub,
    sender_settings: SenderSettings,
) -> (Arc<Sender>, Arc<HubConnection>) {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let settings = HubSettings {
        address: hub.address(),
        backoff_min: Duration::from_millis(50),
        backoff_max: Duration::from_millis(500),
        ..HubSettings::default()
    };
    let conn = HubConnection::new(settings, "svc".into(), uuid::Uuid::new_v4().to_string(), tx)
        .expect("valid hub settings");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let sender = Sender::with_settings(Arc::clone(&conn), sender_settings);
    let forward = Arc::clone(&sender);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            forward.on_inbound(msg);
        }
    });
    (sender, conn)
}

/// Every command or query call through the hub is timed once, with its final outcome: a reply
/// that does not decode is `invalid_request`, as Kotlin's.
#[tokio::test]
async fn every_call_is_timed_by_kind_type_and_final_outcome() {
    use snodra_messaging::metrics::{HUB_CLIENT_REQUEST_DURATION, Metrics};
    let hub = FakeHub::start().await;
    hub.set_responder(|req| {
        Some(match req.r#type.as_str() {
            "t.Ping" => reply_with(&req, r#"{"echoed":"x"}"#),
            "t.Count" => reply_with(&req, r#"{"n":"not a number"}"#),
            _ => hub_error(&req, ErrorCode::NoHandler, "no handler"),
        })
    })
    .await;
    let meter = support::metrics::TestMeter::new();
    let (sender, conn) = sender_with(
        &hub,
        SenderSettings {
            metrics: Metrics::new(meter.meter()),
            ..SenderSettings::default()
        },
    )
    .await;
    let ping = || Ping { text: "x".into() };
    sender
        .send_command(&ping(), SendOptions::default())
        .await
        .unwrap();
    sender
        .send_command(&ping(), SendOptions::default())
        .await
        .unwrap();
    assert!(
        sender
            .send_query(&Count { name: "c".into() }, SendOptions::default())
            .await
            .is_err()
    );
    assert!(
        sender
            .send_command(&Nop {}, SendOptions::default())
            .await
            .is_err()
    );
    let calls = |kind: &str, t: &str, outcome: &str| {
        meter.value(
            HUB_CLIENT_REQUEST_DURATION,
            &[("kind", kind), ("type", t), ("outcome", outcome)],
        )
    };
    assert_eq!(calls("command", "t.Ping", "ok"), Some(2.0));
    assert_eq!(calls("query", "t.Count", "invalid_request"), Some(1.0));
    assert_eq!(calls("command", "t.Nop", "no_handler"), Some(1.0));
    let point = meter
        .point(
            HUB_CLIENT_REQUEST_DURATION,
            &[("kind", "command"), ("type", "t.Ping"), ("outcome", "ok")],
        )
        .unwrap();
    assert_eq!(point.unit, "s");
    assert!(point.value > 0.0, "{point:?}");
    conn.shutdown().await;
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
async fn the_hub_gets_zero_unless_a_timeout_was_chosen() {
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
    sender.on_inbound(Inbound::Disconnected {
        stream: stream - 1,
        unsent: vec![],
    });
    sender.on_inbound(Inbound::Disconnected {
        stream: stream + 7,
        unsent: vec![],
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !pending.is_finished(),
        "failed by another stream's Disconnected"
    );
    sender.on_inbound(Inbound::Disconnected {
        stream,
        unsent: vec![],
    });
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

/// Review fix 5: a caller that gives up (drops the future) after the request was recorded must not
/// leave its pending entry behind until the stream disconnects.
#[tokio::test]
async fn dropped_caller_future_leaves_no_pending_entry() {
    let hub = FakeHub::start().await; // swallows requests: no reply ever comes
    let (sender, conn) = sender(&hub, Some(Duration::from_secs(10))).await;
    let gave_up = tokio::time::timeout(
        Duration::from_millis(200),
        sender.send_command(&Ping { text: "x".into() }, SendOptions::default()),
    )
    .await;
    assert!(gave_up.is_err(), "the fake hub never answers");
    assert_eq!(hub.requests().await.len(), 1, "the request was written");
    assert_eq!(sender.in_flight(), 0, "abandoned request still pending");
    conn.shutdown().await;
    hub.stop();
}

// ---- priority and retry (P1) ----

#[tokio::test]
async fn priority_comes_from_send_options_then_config_then_zero() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_with(&req, r#"{"echoed":"x"}"#)))
        .await;
    let (sender, conn) = sender_with(
        &hub,
        SenderSettings {
            priorities: [("t.Ping".to_string(), 7)].into(),
            ..SenderSettings::default()
        },
    )
    .await;
    let ping = || Ping { text: "x".into() };
    sender
        .send_command(&ping(), SendOptions::default())
        .await
        .unwrap();
    sender
        .send_command(
            &ping(),
            SendOptions {
                priority: Some(-3),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hub.set_responder(|req| Some(reply_with(&req, "{}"))).await;
    sender
        .send_command(&Nop {}, SendOptions::default())
        .await
        .unwrap();
    let priorities: Vec<i32> = hub.requests().await.iter().map(|r| r.priority).collect();
    assert_eq!(priorities, [7, -3, 0]);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn resource_exhausted_maps_to_handler_busy() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(hub_error(&req, ErrorCode::ResourceExhausted, "queue full")))
        .await;
    let (sender, conn) = sender(&hub, None).await;
    let err = sender
        .send_command(&Ping { text: "x".into() }, SendOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, MessagingError::HandlerBusy(ref t) if t == "t.Ping"),
        "{err:?}"
    );
    assert_eq!(
        hub.requests().await.len(),
        1,
        "no retry unless a policy is set"
    );
    conn.shutdown().await;
    hub.stop();
}

/// Answers the first `failures` requests with hub error `code`, then replies `ok`.
async fn failing_then_ok(hub: &FakeHub, failures: usize, code: ErrorCode, ok: &'static str) {
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    hub.set_responder(move |req| {
        let n = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(if n < failures {
            hub_error(&req, code, "transient")
        } else {
            reply_with(&req, ok)
        })
    })
    .await;
}

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        initial_backoff: Duration::from_millis(20),
        ..RetryPolicy::default()
    }
}

#[tokio::test]
async fn retry_resends_a_busy_command_with_a_fresh_id() {
    let hub = FakeHub::start().await;
    failing_then_ok(&hub, 2, ErrorCode::ResourceExhausted, r#"{"echoed":"x"}"#).await;
    let (sender, conn) = sender_with(
        &hub,
        SenderSettings {
            retry: Some(fast_retry()),
            ..SenderSettings::default()
        },
    )
    .await;
    let pong = sender
        .send_command(&Ping { text: "x".into() }, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(pong, Pong { echoed: "x".into() });
    let ids: std::collections::HashSet<String> =
        hub.requests().await.into_iter().map(|r| r.id).collect();
    assert_eq!(ids.len(), 3, "three attempts, three request ids");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn retry_follows_the_classification_per_kind() {
    // (hub code, command retried, query retried)
    let cases = [
        (ErrorCode::NoHandler, true, true),
        (ErrorCode::ResourceExhausted, true, true),
        (ErrorCode::HandlerDisconnected, false, true),
        (ErrorCode::Timeout, false, true),
        (ErrorCode::PermissionDenied, false, false),
        (ErrorCode::HandlerError, false, false),
    ];
    for (code, command, query) in cases {
        let hub = FakeHub::start().await;
        let (sender, conn) = sender_with(
            &hub,
            SenderSettings {
                retry: Some(fast_retry()),
                ..SenderSettings::default()
            },
        )
        .await;
        failing_then_ok(&hub, 1, code, r#"{"echoed":"x"}"#).await;
        let c = sender
            .send_command(&Ping { text: "x".into() }, SendOptions::default())
            .await;
        assert_eq!(c.is_ok(), command, "command after {code:?}: {c:?}");
        let attempts_c = hub.requests().await.len();
        assert_eq!(
            attempts_c,
            if command { 2 } else { 1 },
            "command attempts after {code:?}"
        );

        failing_then_ok(&hub, 1, code, r#"{"n":1}"#).await;
        let q = sender
            .send_query(&Count { name: "a".into() }, SendOptions::default())
            .await;
        assert_eq!(q.is_ok(), query, "query after {code:?}: {q:?}");
        let attempts_q = hub.requests().await.len() - attempts_c;
        assert_eq!(
            attempts_q,
            if query { 2 } else { 1 },
            "query attempts after {code:?}"
        );
        conn.shutdown().await;
        hub.stop();
    }
}

/// A policy built in code is in force as written: there is no separate switch on the policy that
/// would leave `RetryPolicy { max_attempts: 5, ..RetryPolicy::default() }` silently disabled.
#[tokio::test]
async fn a_policy_built_in_code_is_in_force_as_written() {
    let hub = FakeHub::start().await;
    failing_then_ok(&hub, 2, ErrorCode::NoHandler, r#"{"echoed":"x"}"#).await;
    let (sender, conn) = sender(&hub, None).await;
    let policy = RetryPolicy {
        max_attempts: 5,
        initial_backoff: Duration::from_millis(20),
        ..RetryPolicy::default()
    };
    let pong = sender
        .send_command(
            &Ping { text: "x".into() },
            SendOptions {
                retry: Some(policy),
                ..Default::default()
            },
        )
        .await;
    assert!(pong.is_ok(), "the user-built policy was ignored: {pong:?}");
    assert_eq!(hub.requests().await.len(), 3);
    conn.shutdown().await;
    hub.stop();
}

/// A policy without a single attempt is a caller bug: the call fails at once with
/// `InvalidMessage` and nothing is sent, whether the policy came with the call or with the sender's
/// settings (built in code, so `Config::validate` never saw it).
#[tokio::test]
async fn a_policy_with_zero_attempts_fails_the_call_before_anything_is_sent() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_with(&req, r#"{"n":1}"#)))
        .await;
    let zero = RetryPolicy {
        max_attempts: 0,
        ..RetryPolicy::default()
    };
    let (sender, conn) = sender(&hub, None).await;
    let per_call = sender
        .send_command(
            &Ping { text: "x".into() },
            SendOptions {
                retry: Some(zero.clone()),
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(per_call, Err(MessagingError::InvalidMessage(_))),
        "{per_call:?}"
    );
    let (configured, conn2) = sender_with(
        &hub,
        SenderSettings {
            retry: Some(zero),
            ..SenderSettings::default()
        },
    )
    .await;
    let from_settings = configured
        .send_query(&Count { name: "a".into() }, SendOptions::default())
        .await;
    assert!(
        matches!(from_settings, Err(MessagingError::InvalidMessage(_))),
        "{from_settings:?}"
    );
    assert!(hub.requests().await.is_empty(), "nothing may be sent");
    // A valid policy for the call wins over the invalid configured one.
    let total = configured
        .send_query(
            &Count { name: "a".into() },
            SendOptions {
                retry: Some(RetryPolicy::NONE),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(total, Total { n: 1 });
    conn.shutdown().await;
    conn2.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn retry_can_be_switched_per_call() {
    let hub = FakeHub::start().await;
    failing_then_ok(&hub, 1, ErrorCode::NoHandler, r#"{"echoed":"x"}"#).await;
    let (sender, conn) = sender(&hub, None).await; // no policy in the settings
    let ok = sender
        .send_command(
            &Ping { text: "x".into() },
            SendOptions {
                retry: Some(fast_retry()),
                ..Default::default()
            },
        )
        .await;
    assert!(ok.is_ok(), "{ok:?}");
    failing_then_ok(&hub, 1, ErrorCode::NoHandler, r#"{"echoed":"x"}"#).await;
    let (sender2, conn2) = sender_with(
        &hub,
        SenderSettings {
            retry: Some(fast_retry()),
            ..SenderSettings::default()
        },
    )
    .await;
    let off = sender2
        .send_command(
            &Ping { text: "x".into() },
            SendOptions {
                retry: Some(RetryPolicy::NONE),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(off, Err(MessagingError::NoHandler(_))), "{off:?}");
    conn.shutdown().await;
    conn2.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn all_attempts_stay_within_the_callers_budget() {
    let hub = FakeHub::start().await;
    failing_then_ok(&hub, 100, ErrorCode::NoHandler, "{}").await;
    let policy = RetryPolicy {
        max_attempts: 10,
        initial_backoff: Duration::from_millis(200),
        ..RetryPolicy::default()
    };
    let (sender, conn) = sender_with(
        &hub,
        SenderSettings {
            retry: Some(policy),
            ..SenderSettings::default()
        },
    )
    .await;
    let started = std::time::Instant::now();
    let err = sender
        .send_command(
            &Ping { text: "x".into() },
            SendOptions {
                timeout: Some(Duration::from_millis(600)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    assert!(
        matches!(err, MessagingError::NoHandler(_)),
        "the last error is returned: {err:?}"
    );
    assert!(
        elapsed < Duration::from_millis(600),
        "overran the budget: {elapsed:?}"
    );
    // Attempts at ~0 and ~200 ms (waits jittered by ±20 %); after the next wait (400 ms ± 20 %)
    // less than 250 ms would be left of the 600 ms.
    let sent = hub.requests().await;
    assert_eq!(sent.len(), 2, "{sent:?}");
    // Each attempt carries what is left of the budget.
    assert!(
        sent[0].timeout_ms <= 600 && sent[0].timeout_ms > 550,
        "{}",
        sent[0].timeout_ms
    );
    assert!(
        sent[1].timeout_ms <= 440 && sent[1].timeout_ms >= 250,
        "{}",
        sent[1].timeout_ms
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn without_a_chosen_timeout_the_budget_is_the_hubs_bound_for_the_type() {
    let hub = FakeHub::start().await;
    hub.set_ack(Some(proto::RegisterAck {
        type_timeouts_ms: [("t.Ping".to_string(), 1_000)].into(),
        ..default_ack()
    }));
    failing_then_ok(&hub, 100, ErrorCode::NoHandler, "{}").await;
    let policy = RetryPolicy {
        max_attempts: 10,
        initial_backoff: Duration::from_millis(300),
        ..RetryPolicy::default()
    };
    let (sender, conn) = sender_with(
        &hub,
        SenderSettings {
            retry: Some(policy),
            ..SenderSettings::default()
        },
    )
    .await;
    sender
        .send_command(&Ping { text: "x".into() }, SendOptions::default())
        .await
        .unwrap_err();
    let sent = hub.requests().await;
    // The budget is the 1 s override. Attempts at ~0 and ~300 ms; after the next wait (600 ms;
    // both ±20 %) less than the 300 ms minimum (the initial backoff) would be left of it.
    assert_eq!(sent.len(), 2, "{sent:?}");
    assert!(
        sent[0].timeout_ms <= 1_000 && sent[0].timeout_ms > 950,
        "{}",
        sent[0].timeout_ms
    );
    assert!(
        sent[1].timeout_ms <= 760 && sent[1].timeout_ms >= 300,
        "{}",
        sent[1].timeout_ms
    );
    conn.shutdown().await;
    hub.stop();
}

/// A retry needs room: the next attempt starts only if, after its wait, at least
/// max(initial_backoff, 250 ms) of the budget is left; otherwise the last error comes back at once.
/// Budget 1 s, NO_HANDLER at once, 100 ms between attempts: no attempt is sent with less than
/// 250 ms to live.
#[tokio::test]
async fn a_retry_starts_only_with_room_left_in_the_budget() {
    let hub = FakeHub::start().await;
    failing_then_ok(&hub, usize::MAX, ErrorCode::NoHandler, "{}").await;
    let policy = RetryPolicy {
        max_attempts: 100,
        initial_backoff: Duration::from_millis(100),
        multiplier: 1.0,
        ..RetryPolicy::default()
    };
    let (sender, conn) = sender_with(
        &hub,
        SenderSettings {
            retry: Some(policy),
            ..SenderSettings::default()
        },
    )
    .await;
    let started = std::time::Instant::now();
    let err = sender
        .send_command(
            &Ping { text: "x".into() },
            SendOptions {
                timeout: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    assert!(matches!(err, MessagingError::NoHandler(_)), "{err:?}");
    assert!(
        elapsed < Duration::from_secs(1),
        "overran the budget: {elapsed:?}"
    );
    let sent: Vec<u32> = hub.requests().await.iter().map(|r| r.timeout_ms).collect();
    assert!(sent.len() >= 2, "never retried: {sent:?}");
    assert!(sent[0] <= 1_000 && sent[0] > 950, "{sent:?}");
    assert!(
        sent[1..].iter().all(|&ms| ms >= 250),
        "a retry was sent with less than 250 ms left: {sent:?}"
    );
    conn.shutdown().await;
    hub.stop();
}

/// What is left of a budget is sent as at least 1 ms: a sub-millisecond remainder never becomes
/// `timeout_ms = 0`, which would let the hub apply its default (5 s) instead.
#[tokio::test]
async fn a_sub_millisecond_budget_is_sent_as_one_millisecond_never_as_unset() {
    let hub = FakeHub::start().await;
    failing_then_ok(&hub, usize::MAX, ErrorCode::NoHandler, "{}").await;
    for (n, retry) in [Some(fast_retry()), None].into_iter().enumerate() {
        let (sender, conn) = sender(&hub, None).await;
        let err = sender
            .send_command(
                &Ping { text: "x".into() },
                SendOptions {
                    timeout: Some(Duration::from_micros(500)),
                    retry,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, MessagingError::NoHandler(_)), "{err:?}");
        let sent = hub.requests().await;
        assert_eq!(sent.len(), n + 1, "no room for a retry");
        assert_eq!(sent[n].timeout_ms, 1, "case {n}: {:?}", sent[n]);
        conn.shutdown().await;
    }
    hub.stop();
}
