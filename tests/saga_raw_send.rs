//! Request headers (`SendOptions.headers`) and the saga relay's raw, local-first command
//! sender.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use snodra_messaging::context::CURRENT;
use snodra_messaging::hub::{
    HandlerRegistry, HubConnection, HubSettings, Inbound, SendOptions, Sender, SenderSettings,
};
use snodra_messaging::proto::{self, ErrorCode, ServerMessage, reply, server_message};
use snodra_messaging::saga::{
    FailureReason, LocalFirstSender, LocalRawCommandSender, RETRYABLE_MARKER, RawCommandSender,
    RawOutcome, UNKNOWN_OUTCOME_MARKER, classify_failure,
};
use snodra_messaging::{Command, Guard, MessageContext, MessagingError, RetryPolicy, message_type};
use support::fake_hub::FakeHub;
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize)]
struct Ping {
    text: String,
}
message_type!(Ping, "t.Ping");
impl Command for Ping {
    type Reply = ();
}

const PAYLOAD: &[u8] = br#"{"text":"x"}"#;

fn saga_headers() -> std::collections::HashMap<String, String> {
    [
        ("idempotency-key".to_string(), "s-1:1001".to_string()),
        ("saga-id".to_string(), "s-1".to_string()),
    ]
    .into()
}

fn with_headers() -> SendOptions {
    SendOptions {
        headers: saga_headers(),
        ..Default::default()
    }
}

fn reply_ok(req: &proto::Request) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Reply(proto::Reply {
            request_id: req.id.clone(),
            result: Some(reply::Result::Payload(b"{}".to_vec())),
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

async fn sender(hub: &FakeHub, settings: SenderSettings) -> Arc<Sender> {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let conn = HubConnection::new(
        HubSettings {
            address: hub.address(),
            backoff_min: Duration::from_millis(50),
            backoff_max: Duration::from_millis(500),
            ..HubSettings::default()
        },
        "svc".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .expect("valid hub settings");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let sender = Sender::with_settings(conn, settings);
    let forward = Arc::clone(&sender);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            forward.on_inbound(msg);
        }
    });
    sender
}

fn quick() -> SenderSettings {
    SenderSettings {
        default_timeout: Some(Duration::from_secs(2)),
        ..SenderSettings::default()
    }
}

/// What a handler saw: its context and the text of the command.
type Seen = Arc<Mutex<Vec<(MessageContext, Vec<u8>)>>>;

/// A registry whose `t.Ping` handler records the context it was given.
fn registry(seen: &Seen) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    let seen = Arc::clone(seen);
    registry.command::<Ping, _, _>(move |ping, ctx| {
        let seen = Arc::clone(&seen);
        async move {
            seen.lock()
                .unwrap()
                .push((ctx, ping.text.clone().into_bytes()));
            Ok(())
        }
    });
    registry
}

fn seen() -> Seen {
    Arc::new(Mutex::new(Vec::new()))
}

#[tokio::test]
async fn headers_reach_a_remote_handler() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_ok(&req))).await;
    let sender = sender(&hub, quick()).await;

    sender
        .send_command(&Ping { text: "x".into() }, with_headers())
        .await
        .unwrap();

    let headers = &hub.requests().await[0].headers;
    assert_eq!(
        headers.get("idempotency-key").map(String::as_str),
        Some("s-1:1001")
    );
    assert_eq!(headers.get("saga-id").map(String::as_str), Some("s-1"));
    assert!(headers.contains_key("correlationId"), "{headers:?}");
}

#[tokio::test]
async fn headers_reach_a_local_handler() {
    let seen = seen();
    let local = LocalRawCommandSender::new(Arc::new(registry(&seen)));

    let outcome = CURRENT
        .scope(
            MessageContext {
                correlation_id: "corr-9".into(),
                ..Default::default()
            },
            local.send_raw("t.Ping", PAYLOAD.to_vec(), with_headers()),
        )
        .await;

    assert_eq!(outcome, RawOutcome::Success);
    let seen = seen.lock().unwrap();
    let (ctx, text) = &seen[0];
    assert_eq!(text, b"x");
    assert_eq!(
        ctx.headers.get("idempotency-key").map(String::as_str),
        Some("s-1:1001")
    );
    assert_eq!(ctx.headers.get("saga-id").map(String::as_str), Some("s-1"));
    assert_eq!(ctx.correlation_id, "corr-9");
    assert_eq!(
        ctx.headers.get("correlationId").map(String::as_str),
        Some("corr-9")
    );
    assert_eq!(ctx.type_name, "t.Ping");
    assert!(!ctx.message_id.is_empty());
}

#[tokio::test]
async fn a_reserved_header_is_refused() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_ok(&req))).await;
    let sender = sender(&hub, quick()).await;
    let seen = seen();
    let local = LocalRawCommandSender::new(Arc::new(registry(&seen)));
    let first = LocalFirstSender::new(Arc::new(registry(&seen)), Arc::clone(&sender));

    for name in [
        "authorization",
        "Authorization",
        "traceparent",
        "tracestate",
        "correlation-id",
        "correlationId",
        "CORRELATIONID",
    ] {
        let options = SendOptions {
            headers: [(name.to_string(), "x".to_string())].into(),
            ..Default::default()
        };
        let err = sender
            .send_command(&Ping { text: "x".into() }, options.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, MessagingError::InvalidMessage(m) if m.contains(name)),
            "{name}: {err:?}"
        );
        for outcome in [
            local
                .send_raw("t.Ping", PAYLOAD.to_vec(), options.clone())
                .await,
            first
                .send_raw("t.Nobody", PAYLOAD.to_vec(), options.clone())
                .await,
        ] {
            assert!(
                matches!(outcome, RawOutcome::Permanent(FailureReason::Invalid, ref m) if m.contains(name)),
                "{name}: {outcome:?}"
            );
        }
    }
    assert!(hub.requests().await.is_empty(), "nothing reached the hub");
    assert!(
        seen.lock().unwrap().is_empty(),
        "nothing reached the handler"
    );
}

#[tokio::test]
async fn a_command_this_instance_handles_is_delivered_locally_without_the_hub() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_ok(&req))).await;
    let sender = sender(&hub, quick()).await;
    let seen = seen();
    let first = LocalFirstSender::new(Arc::new(registry(&seen)), sender);

    let outcome = first
        .send_raw("t.Ping", PAYLOAD.to_vec(), with_headers())
        .await;

    assert_eq!(outcome, RawOutcome::Success);
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(
        hub.requests().await.is_empty(),
        "a local command never goes to the hub"
    );

    // A type it does not handle goes through the hub.
    let outcome = first
        .send_raw("t.Elsewhere", b"{}".to_vec(), with_headers())
        .await;
    assert_eq!(outcome, RawOutcome::Success);
    let requests = hub.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].r#type, "t.Elsewhere");
    assert_eq!(requests[0].kind, proto::RequestKind::Command as i32);
    assert_eq!(requests[0].payload, b"{}");
    assert_eq!(
        requests[0].headers.get("saga-id").map(String::as_str),
        Some("s-1")
    );
}

#[tokio::test]
async fn through_the_hub_no_handler_is_transient_locally_it_is_permanent() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(hub_error(&req, ErrorCode::NoHandler, "no handler")))
        .await;
    let sender = sender(&hub, quick()).await;
    let first = LocalFirstSender::new(Arc::new(HandlerRegistry::new()), sender);

    // A rolling redeploy of the target service looks the same as a mistake: back off, retry.
    let through_hub = first
        .send_raw("t.Nobody", PAYLOAD.to_vec(), SendOptions::default())
        .await;
    let RawOutcome::Transient(message) = through_hub else {
        panic!("{through_hub:?}");
    };
    assert!(message.contains("t.Nobody"), "{message}");

    // The local sender has no hub to wait for.
    let local = LocalRawCommandSender::new(Arc::new(HandlerRegistry::new()));
    let outcome = local
        .send_raw("t.Nobody", PAYLOAD.to_vec(), SendOptions::default())
        .await;
    let RawOutcome::Permanent(FailureReason::NoHandler, message) = outcome else {
        panic!("{outcome:?}");
    };
    assert!(message.contains("t.Nobody"), "{message}");
}

#[tokio::test]
async fn the_relays_sends_carry_no_authorization() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(reply_ok(&req))).await;
    let sender = sender(&hub, quick()).await;
    let seen = seen();
    let mut guarded = registry(&seen);
    guarded.requires::<Ping>(Guard::authenticated());
    let first = LocalFirstSender::new(Arc::new(guarded), Arc::clone(&sender));
    // A relay task that happens to run inside a user's handler context (it must not use it).
    let user = MessageContext {
        correlation_id: "c-9".into(),
        authorization: Some("Bearer user-token".into()),
        headers: [(
            "traceparent".to_string(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
        )]
        .into(),
        ..Default::default()
    };

    // Through the hub: the correlation id and trace parent travel, the token does not.
    let outcome = CURRENT
        .scope(
            user.clone(),
            first.send_raw("t.Elsewhere", b"{}".to_vec(), with_headers()),
        )
        .await;
    assert_eq!(outcome, RawOutcome::Success);
    let headers = &hub.requests().await[0].headers;
    assert!(!headers.contains_key("authorization"), "{headers:?}");
    assert_eq!(
        headers.get("correlationId").map(String::as_str),
        Some("c-9")
    );
    assert_eq!(
        headers.get("traceparent").map(String::as_str),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    );

    // Locally: no identity, so a handler that requires authentication denies the command.
    let outcome = CURRENT
        .scope(
            user,
            first.send_raw("t.Ping", PAYLOAD.to_vec(), with_headers()),
        )
        .await;
    assert!(
        matches!(outcome, RawOutcome::Permanent(FailureReason::Denied, _)),
        "{outcome:?}"
    );
    assert!(seen.lock().unwrap().is_empty(), "the handler never ran");

    // And a handler without a guard sees no token and no identity either.
    let open_seen = self::seen();
    let open = LocalRawCommandSender::new(Arc::new(registry(&open_seen)));
    CURRENT
        .scope(
            MessageContext {
                authorization: Some("Bearer user-token".into()),
                ..Default::default()
            },
            open.send_raw("t.Ping", PAYLOAD.to_vec(), SendOptions::default()),
        )
        .await;
    {
        let (ctx, _) = &open_seen.lock().unwrap()[0];
        assert!(ctx.authorization.is_none() && ctx.identity.is_none());
        assert!(!ctx.headers.contains_key("authorization"));
    }

    // An invalid traceparent is not forwarded, through the hub or locally.
    let invalid = MessageContext {
        correlation_id: "c-10".into(),
        headers: [("traceparent".to_string(), "00-abc-def-01".to_string())].into(),
        ..Default::default()
    };
    let outcome = CURRENT
        .scope(
            invalid.clone(),
            first.send_raw("t.Elsewhere", b"{}".to_vec(), with_headers()),
        )
        .await;
    assert_eq!(outcome, RawOutcome::Success);
    let headers = &hub.requests().await[1].headers;
    assert_eq!(
        headers.get("correlationId").map(String::as_str),
        Some("c-10")
    );
    assert!(!headers.contains_key("traceparent"), "{headers:?}");
    CURRENT
        .scope(
            invalid,
            open.send_raw("t.Ping", PAYLOAD.to_vec(), SendOptions::default()),
        )
        .await;
    let (ctx, _) = &open_seen.lock().unwrap()[1];
    assert!(
        !ctx.headers.contains_key("traceparent"),
        "{:?}",
        ctx.headers
    );
}

#[tokio::test]
async fn the_retry_policy_of_a_raw_send_is_none() {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| Some(hub_error(&req, ErrorCode::ResourceExhausted, "queue full")))
        .await;
    let sender = sender(
        &hub,
        SenderSettings {
            default_timeout: Some(Duration::from_secs(5)),
            retry: Some(RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_millis(10),
                ..RetryPolicy::default()
            }),
            ..SenderSettings::default()
        },
    )
    .await;

    // The configured policy is live: an ordinary send retries a busy handler.
    let err = sender
        .send_command(&Ping { text: "x".into() }, SendOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, MessagingError::HandlerBusy(_)), "{err:?}");
    assert_eq!(hub.requests().await.len(), 3);

    // The relay owns the backoff and the attempt limit: one request per raw send.
    let first = LocalFirstSender::new(Arc::new(HandlerRegistry::new()), Arc::clone(&sender));
    for outcome in [
        first
            .send_raw("t.Ping", PAYLOAD.to_vec(), SendOptions::default())
            .await,
        sender
            .send_raw("t.Ping", PAYLOAD.to_vec(), SendOptions::default())
            .await,
    ] {
        assert!(matches!(outcome, RawOutcome::Transient(_)), "{outcome:?}");
    }
    assert_eq!(hub.requests().await.len(), 5, "3 + one per raw send");
}

fn remote(details: &[u8]) -> MessagingError {
    MessagingError::RemoteHandler {
        type_name: "t.Ping".into(),
        message: "adapter said so".into(),
        details: details.to_vec(),
    }
}

#[test]
fn markers_in_error_details_classify_retryable_and_unknown_outcome() {
    assert_eq!(RETRYABLE_MARKER, br#"{"snodra":"retryable"}"#);
    assert_eq!(UNKNOWN_OUTCOME_MARKER, br#"{"snodra":"unknown-outcome"}"#);

    assert_eq!(
        classify_failure(&remote(RETRYABLE_MARKER)),
        RawOutcome::Transient("adapter said so".into())
    );
    assert_eq!(
        classify_failure(&remote(b"  {\"snodra\":\"retryable\"}\n")),
        RawOutcome::Transient("adapter said so".into()),
        "surrounding whitespace is ignored, as Kotlin does"
    );
    assert_eq!(
        classify_failure(&remote(UNKNOWN_OUTCOME_MARKER)),
        RawOutcome::Permanent(FailureReason::UnknownOutcome, "adapter said so".into())
    );
    // Any other details, or none, are an ordinary handler failure.
    for details in [&b""[..], b"{}", br#"{"snodra":"other"}"#, b"retryable"] {
        assert_eq!(
            classify_failure(&remote(details)),
            RawOutcome::Permanent(FailureReason::Failed, "adapter said so".into())
        );
    }
}

#[test]
fn the_retry_taxonomy_names_every_failure() {
    let transient = [
        MessagingError::HubUnavailable("no stream".into()),
        MessagingError::Timeout("t.Ping".into()),
        MessagingError::HandlerDisconnected("t.Ping".into()),
        MessagingError::HubConnectionLost("broken".into()),
        MessagingError::HandlerBusy("t.Ping".into()),
        // Not a handler's answer: something unexpected in the sender or the store.
        MessagingError::Transport("tcp".into()),
        MessagingError::Storage("mongo".into()),
    ];
    for e in transient {
        assert!(
            matches!(classify_failure(&e), RawOutcome::Transient(_)),
            "{e:?}"
        );
    }
    let permanent = [
        (
            MessagingError::NoHandler("t.Ping".into()),
            FailureReason::NoHandler,
        ),
        (
            MessagingError::Unauthenticated("no token".into()),
            FailureReason::Denied,
        ),
        (
            MessagingError::AccessDenied("no".into()),
            FailureReason::Denied,
        ),
        (
            MessagingError::InvalidMessage("bad".into()),
            FailureReason::Invalid,
        ),
        (remote(b""), FailureReason::Failed),
        // Any other messaging failure is a handler error.
        (
            MessagingError::Cancelled("cut".into()),
            FailureReason::Failed,
        ),
    ];
    for (e, reason) in permanent {
        let RawOutcome::Permanent(got, message) = classify_failure(&e) else {
            panic!("{e:?}");
        };
        assert_eq!(got, reason, "{e:?}");
        assert_eq!(message, e.to_string());
    }
}

#[tokio::test]
async fn a_local_handler_that_fails_or_cannot_read_the_payload_is_a_permanent_failure() {
    let mut failing = HandlerRegistry::new();
    failing.command::<Ping, _, _>(|_, _| async {
        Err::<(), _>(MessagingError::InvalidMessage("printer on fire".into()))
    });
    let local = LocalRawCommandSender::new(Arc::new(failing));
    // A handler's own error is a failure of the handler, whatever variant it chose.
    let outcome = local
        .send_raw("t.Ping", PAYLOAD.to_vec(), SendOptions::default())
        .await;
    let RawOutcome::Permanent(FailureReason::Failed, message) = outcome else {
        panic!("{outcome:?}");
    };
    assert!(message.contains("printer on fire"), "{message}");

    let seen = seen();
    let local = LocalRawCommandSender::new(Arc::new(registry(&seen)));
    let outcome = local
        .send_raw("t.Ping", b"not json".to_vec(), SendOptions::default())
        .await;
    assert!(
        matches!(outcome, RawOutcome::Permanent(FailureReason::Invalid, _)),
        "{outcome:?}"
    );

    let mut panicking = HandlerRegistry::new();
    panicking.command::<Ping, _, _>(|_, _| async { panic!("boom") });
    let outcome = LocalRawCommandSender::new(Arc::new(panicking))
        .send_raw("t.Ping", PAYLOAD.to_vec(), SendOptions::default())
        .await;
    assert!(
        matches!(outcome, RawOutcome::Permanent(FailureReason::Failed, _)),
        "{outcome:?}"
    );
}
