mod support;

use std::collections::HashMap;
use std::sync::Arc;

use kraken_messaging::hub::{Dispatcher, HandlerRegistry};
use kraken_messaging::identity::{Guard, IdentityResolver};
use kraken_messaging::proto::{ErrorCode, Request, RequestKind, reply};
use kraken_messaging::{Command, MessageContext, MessagingError, Query, message_type};
use serde::{Deserialize, Serialize};
use serde_json::json;

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
#[derive(Serialize, Deserialize)]
struct Total {
    n: u64,
}
message_type!(Count, "t.Count");
impl Query for Count {
    type Reply = Total;
}

#[derive(Serialize, Deserialize)]
struct Boom {}
message_type!(Boom, "t.Boom");
impl Command for Boom {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Panicky {}
message_type!(Panicky, "t.Panicky");
impl Command for Panicky {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Secret {}
message_type!(Secret, "t.Secret");
impl Command for Secret {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Nested {}
message_type!(Nested, "t.Nested");
impl Command for Nested {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Maybe {}
message_type!(Maybe, "t.Maybe");
impl Query for Maybe {
    type Reply = Option<Pong>;
}

#[derive(Serialize, Deserialize)]
struct Introspect {}
#[derive(Serialize, Deserialize, Debug)]
struct Seen {
    correlation_id: String,
    current_message_id: String,
    user_id: Option<String>,
}
message_type!(Introspect, "t.Introspect");
impl Command for Introspect {
    type Reply = Seen;
}

fn registry() -> HandlerRegistry {
    let mut r = HandlerRegistry::new();
    r.command::<Ping, _, _>(|p, _ctx| async move { Ok(Pong { echoed: p.text }) });
    r.command::<Nop, _, _>(|_, _| async move { Ok(()) });
    r.query::<Count, _, _>(|q, _| async move {
        Ok(Total {
            n: q.name.len() as u64,
        })
    });
    r.command::<Boom, _, _>(|_, _| async move { Err(MessagingError::Storage("db down".into())) });
    r.command::<Panicky, _, _>(|_, _| async move {
        if true {
            panic!("kaboom");
        }
        Ok(())
    });
    r.command::<Secret, _, _>(|_, _| async move { Ok(()) });
    r.requires::<Secret>(Guard::role("kraken-admin"));
    r.command::<Nested, _, _>(|_, _| async move {
        Err(MessagingError::NoHandler("t.Downstream".into()))
    });
    r.query::<Maybe, _, _>(|_, _| async move { Ok(None) });
    r.command::<Introspect, _, _>(|_, ctx: MessageContext| async move {
        let current = MessageContext::current().expect("task-local set");
        let user_id = ctx.user_id().map(str::to_string);
        Ok(Seen {
            correlation_id: ctx.correlation_id,
            current_message_id: current.message_id,
            user_id,
        })
    });
    r
}

fn dispatcher() -> Dispatcher {
    Dispatcher::new(
        Arc::new(registry()),
        Arc::new(IdentityResolver::new(None)),
        4,
    )
}

fn request(kind: RequestKind, type_name: &str, payload: &str, headers: &[(&str, &str)]) -> Request {
    Request {
        id: "req-1".into(),
        kind: kind as i32,
        r#type: type_name.into(),
        routing_key: String::new(),
        payload: payload.as_bytes().to_vec(),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>(),
        timeout_ms: 0,
        priority: 0,
        ..Request::default()
    }
}

fn error_code(reply: &kraken_messaging::proto::Reply) -> (ErrorCode, String) {
    match &reply.result {
        Some(reply::Result::Error(e)) => (ErrorCode::try_from(e.code).unwrap(), e.message.clone()),
        other => panic!("expected an error reply, got {other:?}"),
    }
}

fn payload(reply: &kraken_messaging::proto::Reply) -> serde_json::Value {
    match &reply.result {
        Some(reply::Result::Payload(p)) => serde_json::from_slice(p).unwrap(),
        other => panic!("expected a payload reply, got {other:?}"),
    }
}

#[tokio::test]
async fn dispatches_command_and_encodes_reply() {
    let reply = dispatcher()
        .handle(request(
            RequestKind::Command,
            "t.Ping",
            r#"{"text":"hi"}"#,
            &[],
        ))
        .await;
    assert_eq!(reply.request_id, "req-1");
    assert_eq!(payload(&reply), json!({"echoed": "hi"}));
}

#[tokio::test]
async fn dispatches_query() {
    let reply = dispatcher()
        .handle(request(
            RequestKind::Query,
            "t.Count",
            r#"{"name":"abc"}"#,
            &[],
        ))
        .await;
    assert_eq!(payload(&reply), json!({"n": 3}));
}

#[tokio::test]
async fn unit_reply_is_empty_object() {
    let reply = dispatcher()
        .handle(request(RequestKind::Command, "t.Nop", "{}", &[]))
        .await;
    assert_eq!(payload(&reply), json!({}));
}

#[tokio::test]
async fn unknown_type_is_no_handler() {
    let reply = dispatcher()
        .handle(request(RequestKind::Command, "t.Nope", "{}", &[]))
        .await;
    assert_eq!(error_code(&reply).0, ErrorCode::NoHandler);
}

#[tokio::test]
async fn query_type_sent_as_command_is_no_handler() {
    let reply = dispatcher()
        .handle(request(
            RequestKind::Command,
            "t.Count",
            r#"{"name":"x"}"#,
            &[],
        ))
        .await;
    assert_eq!(error_code(&reply).0, ErrorCode::NoHandler);
}

#[tokio::test]
async fn unspecified_kind_is_invalid_request() {
    let reply = dispatcher()
        .handle(request(
            RequestKind::Unspecified,
            "t.Ping",
            r#"{"text":"hi"}"#,
            &[],
        ))
        .await;
    let (code, message) = error_code(&reply);
    assert_eq!(code, ErrorCode::InvalidRequest);
    assert!(message.contains("unsupported request kind"));
}

#[tokio::test]
async fn undecodable_payload_is_invalid_request() {
    let reply = dispatcher()
        .handle(request(
            RequestKind::Command,
            "t.Ping",
            r#"{"text":42}"#,
            &[],
        ))
        .await;
    assert_eq!(error_code(&reply).0, ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn guarded_handler_rejects_anonymous_before_decoding() {
    let reply = dispatcher()
        .handle(request(RequestKind::Command, "t.Secret", "garbage", &[]))
        .await;
    let (code, message) = error_code(&reply);
    assert_eq!(code, ErrorCode::Unauthenticated);
    assert!(message.contains("requires authentication"));
}

#[tokio::test]
async fn guarded_handler_denies_missing_role() {
    let token = support::sign(
        support::OTHER_KEY_PEM,
        None,
        "u-1",
        &["kraken-user"],
        support::ISSUER,
        support::now() + 60,
    );
    let auth = format!("Bearer {token}");
    let reply = dispatcher()
        .handle(request(
            RequestKind::Command,
            "t.Secret",
            "{}",
            &[("authorization", &auth)],
        ))
        .await;
    assert_eq!(error_code(&reply).0, ErrorCode::PermissionDenied);

    let admin = support::sign(
        support::OTHER_KEY_PEM,
        None,
        "u-1",
        &["kraken-admin"],
        support::ISSUER,
        support::now() + 60,
    );
    let auth = format!("Bearer {admin}");
    let reply = dispatcher()
        .handle(request(
            RequestKind::Command,
            "t.Secret",
            "{}",
            &[("authorization", &auth)],
        ))
        .await;
    assert_eq!(payload(&reply), json!({}));
}

#[tokio::test]
async fn handler_error_maps_to_handler_error_code() {
    let (code, message) = error_code(
        &dispatcher()
            .handle(request(RequestKind::Command, "t.Boom", "{}", &[]))
            .await,
    );
    assert_eq!(code, ErrorCode::HandlerError);
    assert!(message.contains("db down"));
}

#[tokio::test]
async fn handler_propagating_no_handler_is_handler_error() {
    // A nested call's NO_HANDLER must not masquerade as "no handler for t.Nested" at the original caller.
    let (code, message) = error_code(
        &dispatcher()
            .handle(request(RequestKind::Command, "t.Nested", "{}", &[]))
            .await,
    );
    assert_eq!(code, ErrorCode::HandlerError);
    assert!(message.contains("t.Downstream"));
}

#[tokio::test]
async fn none_option_reply_is_null_not_empty_object() {
    let reply = dispatcher()
        .handle(request(RequestKind::Query, "t.Maybe", "{}", &[]))
        .await;
    assert_eq!(payload(&reply), serde_json::Value::Null);
}

#[tokio::test]
async fn panicking_handler_is_handler_error() {
    let (code, message) = error_code(
        &dispatcher()
            .handle(request(RequestKind::Command, "t.Panicky", "{}", &[]))
            .await,
    );
    assert_eq!(code, ErrorCode::HandlerError);
    assert!(message.contains("panicked"));
}

#[tokio::test]
async fn handler_sees_context_and_current_task_local() {
    let token = support::sign(
        support::OTHER_KEY_PEM,
        None,
        "u-7",
        &[],
        support::ISSUER,
        support::now() + 60,
    );
    let auth = format!("Bearer {token}");
    let reply = dispatcher()
        .handle(request(
            RequestKind::Command,
            "t.Introspect",
            "{}",
            &[("correlationId", "c-1"), ("authorization", &auth)],
        ))
        .await;
    let seen = payload(&reply);
    assert_eq!(seen["correlation_id"], "c-1");
    assert_eq!(seen["current_message_id"], "req-1");
    assert_eq!(seen["user_id"], "u-7");
}

#[tokio::test]
async fn correlation_defaults_to_request_id() {
    let reply = dispatcher()
        .handle(request(RequestKind::Command, "t.Introspect", "{}", &[]))
        .await;
    assert_eq!(payload(&reply)["correlation_id"], "req-1");
}

#[test]
#[should_panic(expected = "duplicate handler")]
fn duplicate_registration_panics() {
    let mut r = HandlerRegistry::new();
    r.command::<Ping, _, _>(|p, _| async move { Ok(Pong { echoed: p.text }) });
    r.command::<Ping, _, _>(|p, _| async move { Ok(Pong { echoed: p.text }) });
}

#[test]
fn registry_lists_types_sorted() {
    let r = registry();
    assert_eq!(
        r.command_types(),
        vec![
            "t.Boom",
            "t.Introspect",
            "t.Nested",
            "t.Nop",
            "t.Panicky",
            "t.Ping",
            "t.Secret"
        ]
    );
    assert_eq!(r.query_types(), vec!["t.Count", "t.Maybe"]);
    assert!(r.event_types().is_empty());
}

#[derive(Serialize, Deserialize)]
struct Search {
    term: String,
}
message_type!(Search, "t.Search");
impl Query for Search {
    type Reply = Total;
}

#[tokio::test]
async fn shared_query_is_listed_apart_and_dispatched_like_any_query() {
    let mut r = HandlerRegistry::new();
    r.query::<Count, _, _>(|q, _| async move {
        Ok(Total {
            n: q.name.len() as u64,
        })
    });
    r.shared_query::<Search, _, _>(|q, _| async move {
        Ok(Total {
            n: q.term.len() as u64 * 10,
        })
    });
    r.requires::<Search>(Guard::authenticated());
    assert_eq!(r.query_types(), vec!["t.Count"]);
    assert_eq!(r.shared_query_types(), vec!["t.Search"]);
    let types = r.registered_types();
    assert_eq!(types.queries, vec!["t.Count"]);
    assert_eq!(types.shared_queries, vec!["t.Search"]);
    assert!(types.commands.is_empty());

    // A per-target request `<id>#<n>` of a scatter-gather is an ordinary unary request.
    let mut r2 = HandlerRegistry::new();
    r2.shared_query::<Search, _, _>(|q, _| async move {
        Ok(Total {
            n: q.term.len() as u64 * 10,
        })
    });
    let d = Dispatcher::new(Arc::new(r2), Arc::new(IdentityResolver::new(None)), 4);
    let mut req = request(RequestKind::Query, "t.Search", r#"{"term":"abc"}"#, &[]);
    req.id = "g-1#2".into();
    let reply = d.handle(req).await;
    assert_eq!(reply.request_id, "g-1#2");
    assert_eq!(payload(&reply)["n"], 30);
}

#[test]
#[should_panic(expected = "t.Search registered both as an exclusive and as a shared query")]
fn exclusive_then_shared_registration_panics() {
    let mut r = HandlerRegistry::new();
    r.query::<Search, _, _>(|_, _| async move { Ok(Total { n: 0 }) });
    r.shared_query::<Search, _, _>(|_, _| async move { Ok(Total { n: 0 }) });
}

#[test]
#[should_panic(expected = "t.Search registered both as an exclusive and as a shared query")]
fn shared_then_exclusive_registration_panics() {
    let mut r = HandlerRegistry::new();
    r.shared_query::<Search, _, _>(|_, _| async move { Ok(Total { n: 0 }) });
    r.query::<Search, _, _>(|_, _| async move { Ok(Total { n: 0 }) });
}

#[test]
#[should_panic(expected = "duplicate handler")]
fn shared_query_registered_twice_panics() {
    let mut r = HandlerRegistry::new();
    r.shared_query::<Search, _, _>(|_, _| async move { Ok(Total { n: 0 }) });
    r.shared_query::<Search, _, _>(|_, _| async move { Ok(Total { n: 0 }) });
}
