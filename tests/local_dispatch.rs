//! Local dispatch: a command, or a unary query of a type this instance handles itself (not
//! shared), is answered in-process, without the hub, exactly as the handler would answer it
//! through the hub (the codec round trip, the identity of the forwarded token, the guard, the
//! error mapping); the routing key, the timeout, the retry policy and the metrics do not apply.
//! `Config::hub_always_route_through_hub` sends everything through the hub instead.
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use snodra_messaging::external::IdempotencyKey;
use snodra_messaging::proto::{self, ServerMessage, reply, server_message};
use snodra_messaging::{
    Command, Config, Guard, MessagingError, Query, RetryPolicy, SendOptions, SnodraMessaging,
    message_type,
};
use support::fake_hub::FakeHub;
use support::metrics::TestMeter;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Serialize, Deserialize)]
struct Ping {
    text: String,
}
message_type!(Ping, "local.Ping");
impl Command for Ping {
    type Reply = Pong;
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Pong {
    text: String,
    user: Option<String>,
    correlation: String,
    tenant: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Lookup {}
message_type!(Lookup, "local.Lookup");
impl Query for Lookup {
    type Reply = u32;
}

/// A query several services own: always through the hub.
#[derive(Serialize, Deserialize)]
struct Shared {}
message_type!(Shared, "local.Shared");
impl Query for Shared {
    type Reply = u32;
}

/// Another service's command.
#[derive(Serialize, Deserialize)]
struct Elsewhere {}
message_type!(Elsewhere, "local.Elsewhere");
impl Command for Elsewhere {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Fail {
    how: String,
}
message_type!(Fail, "local.Fail");
impl Command for Fail {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Admin {}
message_type!(Admin, "local.Admin");
impl Command for Admin {
    type Reply = String;
}

#[derive(Serialize, Deserialize)]
struct Slow {}
message_type!(Slow, "local.Slow");
impl Command for Slow {
    type Reply = ();
}

/// A hub that answers every request it gets with `7` (`u32`), as another owner would.
async fn hub() -> FakeHub {
    let hub = FakeHub::start().await;
    hub.set_responder(|req| {
        Some(ServerMessage {
            message: Some(server_message::Message::Reply(proto::Reply {
                request_id: req.id.clone(),
                result: Some(reply::Result::Payload(b"7".to_vec())),
            })),
        })
    })
    .await;
    hub
}

/// A JWKS endpoint serving the fixture key.
async fn jwks() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/jwks"))
        .respond_with(ResponseTemplate::new(200).set_body_string(support::JWKS_JSON))
        .mount(&server)
        .await;
    server
}

fn config(hub: &FakeHub, jwks: &MockServer) -> Config {
    let mut c = Config::new("svc-local", &hub.address());
    c.reconnect_backoff_min = Duration::from_millis(50);
    c.reconnect_backoff_max = Duration::from_millis(200);
    c.jwks_url = Some(format!("{}/jwks", jwks.uri()));
    c.issuer = Some(support::ISSUER.into());
    c
}

/// Counts of the handlers that ran.
#[derive(Default)]
struct Ran {
    ping: AtomicU32,
    lookup: AtomicU32,
    fail: AtomicU32,
}

async fn app(config: Config, ran: &Arc<Ran>, meter: Option<&TestMeter>) -> SnodraMessaging {
    let builder = match meter {
        Some(m) => SnodraMessaging::builder_with_meter(config, m.meter()).await,
        None => SnodraMessaging::builder(config).await,
    }
    .unwrap();
    let (p, l, f) = (Arc::clone(ran), Arc::clone(ran), Arc::clone(ran));
    builder
        .command::<Ping, _, _>(move |ping, ctx| {
            p.ping.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(Pong {
                    text: ping.text,
                    user: ctx.identity.and_then(|i| i.user_id),
                    correlation: ctx.correlation_id,
                    tenant: ctx.headers.get("x-tenant").cloned(),
                })
            }
        })
        .query::<Lookup, _, _>(move |_, _| {
            l.lookup.fetch_add(1, Ordering::SeqCst);
            async move { Ok(1) }
        })
        .shared_query::<Shared, _, _>(|_, _| async move { Ok(1) })
        .command::<Fail, _, _>(move |fail, _| {
            f.fail.fetch_add(1, Ordering::SeqCst);
            async move {
                match fail.how.as_str() {
                    "panic" => panic!("boom"),
                    "busy" => Err(MessagingError::HandlerBusy {
                        type_name: "full".into(),
                        reason: None,
                    }),
                    _ => Err(MessagingError::Storage("disk gone".into())),
                }
            }
        })
        .command::<Admin, _, _>(|_, ctx| async move {
            Ok(ctx.identity.and_then(|i| i.user_id).unwrap_or_default())
        })
        .requires::<Admin>(Guard::role("admin"))
        .command::<Slow, _, _>(|_, _| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(())
        })
        .start()
        .await
        .unwrap()
}

fn ping(text: &str) -> Ping {
    Ping { text: text.into() }
}

/// A command and a unary query of this instance's own types are answered by its handlers, never
/// sent to the hub, also with a routing key; the reply goes through the codec as on the hub path.
#[tokio::test]
async fn own_commands_and_queries_are_answered_in_process_without_the_hub() {
    let (hub, jwks) = (hub().await, jwks().await);
    let ran = Arc::new(Ran::default());
    let app = app(config(&hub, &jwks), &ran, None).await;
    assert!(app.await_hub(Duration::from_secs(5)).await);
    let pong = app
        .commands()
        .send_command(
            &ping("hi"),
            SendOptions {
                routing_key: Some("order-1".into()),
                headers: [("x-tenant".to_string(), "t1".to_string())].into(),
                priority: Some(9),
                ..SendOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(pong.text, "hi");
    assert_eq!(
        pong.tenant.as_deref(),
        Some("t1"),
        "headers reach the handler"
    );
    assert!(!pong.correlation.is_empty(), "a correlation id");
    assert_eq!(pong.user, None, "no caller: anonymous");
    let n: u32 = app
        .queries()
        .send_query(&Lookup {}, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 1, "this instance's handler, not the hub's 7");
    assert_eq!(ran.ping.load(Ordering::SeqCst), 1);
    assert_eq!(ran.lookup.load(Ordering::SeqCst), 1);
    assert!(hub.requests().await.is_empty(), "nothing reached the hub");
    app.shutdown().await;
}

/// A shared query type goes through the hub (which spreads it over every owner), and so does a
/// type this instance does not handle.
#[tokio::test]
async fn a_shared_query_and_another_services_types_go_through_the_hub() {
    let (hub, jwks) = (hub().await, jwks().await);
    let ran = Arc::new(Ran::default());
    let app = app(config(&hub, &jwks), &ran, None).await;
    assert!(app.await_hub(Duration::from_secs(5)).await);
    let n: u32 = app
        .queries()
        .send_query(&Shared {}, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 7, "the hub's answer");
    let requests = hub.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].r#type, "local.Shared");
    app.shutdown().await;
}

/// `hub_always_route_through_hub`: the instance's own types go through the hub too.
#[tokio::test]
async fn always_route_through_hub_sends_own_types_to_the_hub() {
    let (hub, jwks) = (hub().await, jwks().await);
    let ran = Arc::new(Ran::default());
    let mut config = config(&hub, &jwks);
    config.hub_always_route_through_hub = true;
    let app = app(config, &ran, None).await;
    assert!(app.await_hub(Duration::from_secs(5)).await);
    let n: u32 = app
        .queries()
        .send_query(&Lookup {}, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 7, "the hub's answer");
    let requests = hub.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].r#type, "local.Lookup");
    assert_eq!(ran.lookup.load(Ordering::SeqCst), 0);
    app.shutdown().await;
}

/// The builder method sets the same option.
#[tokio::test]
async fn the_builder_can_route_everything_through_the_hub() {
    let (hub, jwks) = (hub().await, jwks().await);
    let app = SnodraMessaging::builder(config(&hub, &jwks))
        .await
        .unwrap()
        .always_route_through_hub(true)
        .query::<Lookup, _, _>(|_, _| async move { Ok(1) })
        .start()
        .await
        .unwrap();
    assert!(app.await_hub(Duration::from_secs(5)).await);
    let n: u32 = app
        .queries()
        .send_query(&Lookup {}, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 7);
    app.shutdown().await;
}

/// The variable `SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB` sets it from the environment.
#[test]
fn the_environment_can_route_everything_through_the_hub() {
    // Only this test sets these variables in this binary.
    unsafe {
        std::env::set_var("SNODRA_SERVICE_NAME", "svc-env");
        std::env::set_var("SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB", "true");
    }
    let c = Config::from_env().unwrap();
    assert!(c.hub_always_route_through_hub);
    unsafe {
        std::env::remove_var("SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB");
    }
    assert!(
        !Config::from_env().unwrap().hub_always_route_through_hub,
        "off by default"
    );
}

/// A local handler's failure reaches the caller as it would through the hub: its error becomes
/// the hub's `HANDLER_ERROR` (`RemoteHandler`), a panic is "handler panicked"; nothing is retried,
/// whatever the policy says.
#[tokio::test]
async fn a_local_failure_is_mapped_as_through_the_hub_and_never_retried() {
    let (hub, jwks) = (hub().await, jwks().await);
    let ran = Arc::new(Ran::default());
    let app = app(config(&hub, &jwks), &ran, None).await;
    let retry = SendOptions {
        retry: Some(RetryPolicy {
            max_attempts: 5,
            ..RetryPolicy::default()
        }),
        ..SendOptions::default()
    };
    let e = app
        .commands()
        .send_command(&Fail { how: "io".into() }, retry.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(&e, MessagingError::RemoteHandler { type_name, message, .. } if type_name == "local.Fail" && message.contains("disk gone")),
        "{e:?}"
    );
    let e = app
        .commands()
        .send_command(&Fail { how: "busy".into() }, retry.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(&e, MessagingError::RemoteHandler { .. }),
        "a handler's own HandlerBusy is its error: {e:?}"
    );
    let e = app
        .commands()
        .send_command(
            &Fail {
                how: "panic".into(),
            },
            retry,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&e, MessagingError::RemoteHandler { message, .. } if message == "handler panicked"),
        "{e:?}"
    );
    assert_eq!(ran.fail.load(Ordering::SeqCst), 3, "one run each");
    assert!(hub.requests().await.is_empty());
    app.shutdown().await;
}

/// The identity is the forwarded token's, verified as the hub path's handler would verify it, and
/// the guard applies: an anonymous caller is refused, a token with the role passes.
#[tokio::test]
async fn the_identity_and_the_guard_apply_as_through_the_hub() {
    let (hub, jwks) = (hub().await, jwks().await);
    let ran = Arc::new(Ran::default());
    let app = app(config(&hub, &jwks), &ran, None).await;
    let anonymous = app
        .commands()
        .send_command(&Admin {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(anonymous, MessagingError::Unauthenticated(_)),
        "{anonymous:?}"
    );
    let bearer = |roles: &[&str]| {
        format!(
            "Bearer {}",
            support::token("alice", roles, support::ISSUER, support::now() + 300)
        )
    };
    let with = |auth: String| SendOptions {
        authorization: Some(auth),
        ..SendOptions::default()
    };
    let denied = app
        .commands()
        .send_command(&Admin {}, with(bearer(&["user"])))
        .await
        .unwrap_err();
    assert!(
        matches!(denied, MessagingError::AccessDenied(_)),
        "{denied:?}"
    );
    let user = app
        .commands()
        .send_command(&Admin {}, with(bearer(&["admin"])))
        .await
        .unwrap();
    assert_eq!(user, "alice");
    let bad = app
        .commands()
        .send_command(
            &ping("x"),
            with(format!("Bearer {}", support::unsigned_token("mallory"))),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(bad, MessagingError::Unauthenticated(_)),
        "a token that does not verify: {bad:?}"
    );
    let pong = app
        .commands()
        .send_command(&ping("x"), with(bearer(&[])))
        .await
        .unwrap();
    assert_eq!(pong.user.as_deref(), Some("alice"));
    app.shutdown().await;
}

/// The caller's timeout does not bound a local handler, and a local call is not timed in the
/// request metrics (it is no hub request).
#[tokio::test]
async fn a_local_call_has_no_timeout_and_no_request_metrics() {
    let (hub, jwks) = (hub().await, jwks().await);
    let ran = Arc::new(Ran::default());
    let meter = TestMeter::new();
    let app = app(config(&hub, &jwks), &ran, Some(&meter)).await;
    app.commands()
        .send_command(
            &Slow {},
            SendOptions {
                timeout: Some(Duration::from_millis(50)),
                ..SendOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(
        meter
            .points_of(snodra_messaging::metrics::HUB_CLIENT_REQUEST_DURATION)
            .is_empty(),
        "{:?}",
        meter.points()
    );
    app.shutdown().await;
}

/// Local calls take no handler permit: with one permit, two local calls run at once (a handler
/// that sends to another local handler never waits for its own permit).
#[tokio::test]
async fn local_calls_are_not_bounded_by_the_handler_permits() {
    let (hub, jwks) = (hub().await, jwks().await);
    let mut config = config(&hub, &jwks);
    config.max_concurrent_handlers = 1;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let app = SnodraMessaging::builder(config)
        .await
        .unwrap()
        .command::<Slow, _, _>(move |_, _| {
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                Ok(())
            }
        })
        .start()
        .await
        .unwrap();
    let both = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            app.commands()
                .send_command(&Slow {}, SendOptions::default()),
            app.commands()
                .send_command(&Slow {}, SendOptions::default()),
        )
    })
    .await
    .expect("both handlers ran at once");
    both.0.unwrap();
    both.1.unwrap();
    app.shutdown().await;
}

/// With the hub turned off, the instance's own handlers still answer; another service's command
/// fails at once.
#[tokio::test]
async fn with_the_hub_turned_off_own_handlers_still_answer() {
    let (hub, jwks) = (hub().await, jwks().await);
    let ran = Arc::new(Ran::default());
    let mut config = config(&hub, &jwks);
    config.hub_enabled = false;
    let app = app(config, &ran, None).await;
    let pong = app
        .commands()
        .send_command(&ping("off"), SendOptions::default())
        .await
        .unwrap();
    assert_eq!(pong.text, "off");
    let e = app
        .commands()
        .send_command(&Elsewhere {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(e, MessagingError::HubUnavailable(_)), "{e:?}");
    app.shutdown().await;
}

/// Before `start` the sender already answers the types registered so far in-process, as after
/// it (the guard included); a type registered later is answered too, and any other type goes
/// through the hub (not connected yet: it waits for one until its deadline, `HubUnavailable`).
#[tokio::test]
async fn before_start_the_sender_answers_its_own_types_in_process() {
    let (hub, jwks) = (hub().await, jwks().await);
    let builder = SnodraMessaging::builder(config(&hub, &jwks))
        .await
        .unwrap()
        .query::<Lookup, _, _>(|_, _| async move { Ok(1) });
    let sender = builder.sender();
    let n: u32 = sender
        .send_query(&Lookup {}, SendOptions::default())
        .await
        .expect("answered in-process before start");
    assert_eq!(n, 1);
    let builder = builder
        .command::<Admin, _, _>(|_, _| async move { Ok(String::new()) })
        .requires::<Admin>(Guard::role("admin"));
    let e = sender
        .send_command(&Admin {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(e, MessagingError::Unauthenticated(_)), "{e:?}");
    let short = SendOptions {
        timeout: Some(Duration::from_millis(300)),
        ..SendOptions::default()
    };
    let started = std::time::Instant::now();
    let e = sender
        .send_command(&Elsewhere {}, short.clone())
        .await
        .unwrap_err();
    assert!(matches!(e, MessagingError::HubUnavailable(_)), "{e:?}");
    assert!(started.elapsed() >= Duration::from_millis(250));
    let app = builder.start().await.unwrap();
    let n: u32 = sender
        .send_query(&Lookup {}, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 1, "the same sender answers locally once started");
    assert!(hub.requests().await.is_empty(), "nothing went to the hub");
    app.shutdown().await;
}

/// An idempotent command type of this service is not answered in-process before `start` (its call
/// records are bound by the start): it goes through the hub, not connected yet, and fails as never
/// sent (`HubUnavailable`, retry-safe) at its deadline; its handler does not run.
#[tokio::test]
async fn before_start_an_idempotent_command_goes_through_the_hub() {
    let (hub, jwks) = (hub().await, jwks().await);
    let ran = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&ran);
    let builder = SnodraMessaging::builder(config(&hub, &jwks))
        .await
        .unwrap()
        .idempotent_command::<Slow, _, _>(IdempotencyKey::default(), false, move |_, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move { Ok(()) }
        });
    let started = std::time::Instant::now();
    let e = builder
        .sender()
        .send_command(
            &Slow {},
            SendOptions {
                timeout: Some(Duration::from_millis(300)),
                ..SendOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(e, MessagingError::HubUnavailable(_)), "{e:?}");
    assert!(started.elapsed() >= Duration::from_millis(250));
    assert_eq!(ran.load(Ordering::SeqCst), 0, "the handler did not run");
}

/// With "always route through the hub" nothing is answered in-process before `start` either.
#[tokio::test]
async fn before_start_always_through_the_hub_answers_nothing_in_process() {
    let (hub, jwks) = (hub().await, jwks().await);
    let builder = SnodraMessaging::builder(config(&hub, &jwks))
        .await
        .unwrap()
        .query::<Lookup, _, _>(|_, _| async move { Ok(1) })
        .always_route_through_hub(true);
    let e = builder
        .sender()
        .send_query(
            &Lookup {},
            SendOptions {
                timeout: Some(Duration::from_millis(300)),
                ..SendOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(e, MessagingError::HubUnavailable(_)), "{e:?}");
}

#[derive(Serialize, Deserialize)]
struct Outer {}
message_type!(Outer, "local.Outer");
impl Command for Outer {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Inner {}
message_type!(Inner, "local.Inner");
impl Command for Inner {
    type Reply = ();
}

/// What the nested handler saw.
#[derive(Debug, Default, Clone, PartialEq)]
struct Nested {
    user: Option<String>,
    correlation: String,
    traceparent: Option<String>,
    authorization: Option<String>,
}

/// A handler serving a hub request sends a command of its own instance: answered in-process,
/// the callee sees the forwarded caller (verified, so its guard passes), the same correlation id
/// and the same trace parent.
#[tokio::test]
async fn a_nested_call_from_a_handler_keeps_the_caller_the_correlation_and_the_trace() {
    let (hub, jwks) = (hub().await, jwks().await);
    let seen: Arc<std::sync::Mutex<Option<Nested>>> = Arc::default();
    let builder = SnodraMessaging::builder(config(&hub, &jwks)).await.unwrap();
    let sender = builder.sender();
    let record = Arc::clone(&seen);
    let app = builder
        .command::<Outer, _, _>(move |_, _| {
            let sender = Arc::clone(&sender);
            async move { sender.send_command(&Inner {}, SendOptions::default()).await }
        })
        .command::<Inner, _, _>(move |_, ctx| {
            let record = Arc::clone(&record);
            async move {
                *record.lock().unwrap() = Some(Nested {
                    user: ctx.identity.as_ref().and_then(|i| i.user_id.clone()),
                    correlation: ctx.correlation_id.clone(),
                    traceparent: ctx.traceparent().map(str::to_string),
                    authorization: ctx.authorization.clone(),
                });
                Ok(())
            }
        })
        .requires::<Inner>(Guard::role("admin"))
        .start()
        .await
        .unwrap();
    assert!(app.await_hub(Duration::from_secs(5)).await);
    let bearer = format!(
        "Bearer {}",
        support::token("alice", &["admin"], support::ISSUER, support::now() + 300)
    );
    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    hub.send_to_clients(ServerMessage {
        message: Some(server_message::Message::Request(proto::Request {
            id: "outer-1".into(),
            kind: proto::RequestKind::Command as i32,
            r#type: "local.Outer".into(),
            payload: b"{}".to_vec(),
            headers: [
                ("authorization".to_string(), bearer.clone()),
                ("correlationId".to_string(), "corr-7".to_string()),
                ("traceparent".to_string(), traceparent.to_string()),
            ]
            .into(),
            ..Default::default()
        })),
    })
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while hub.replies().await.is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "no reply");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let reply = hub.replies().await.remove(0);
    assert!(
        matches!(reply.result, Some(reply::Result::Payload(_))),
        "{reply:?}"
    );
    assert_eq!(
        seen.lock()
            .unwrap()
            .clone()
            .expect("the nested handler ran"),
        Nested {
            user: Some("alice".into()),
            correlation: "corr-7".into(),
            traceparent: Some(traceparent.into()),
            authorization: Some(bearer),
        }
    );
    assert!(
        hub.requests().await.is_empty(),
        "the nested call never reached the hub"
    );
    app.shutdown().await;
}
