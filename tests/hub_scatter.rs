//! Scatter-gather queries and shared query types (ADR 0017) against in-process fake hubs: shared
//! registration, one part per target in arrival order and the end, failed and timed-out targets,
//! `max_results`, the request fields, the `UnsupportedByHub` gate, errors before any target,
//! parts and ends of unknown requests, a lost stream, a gather dropped while its write is
//! blocked, the retry policy over the whole gather, and the multi-hub pool (one gather to one
//! capable hub).
mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use kraken_messaging::hub::{
    HubPool, HubSettings, Inbound, RegisteredTypes, RemoteError, ReplyTo, ScatterOptions,
    ScatterPart, Sender, SenderSettings,
};
use kraken_messaging::proto::{self, ClientMessage, ErrorCode, client_message, reply};
use kraken_messaging::{MessagingError, Query, RetryPolicy, message_type};
use serde::{Deserialize, Serialize};
use support::fake_hub::FakeHub;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Find {
    term: String,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Found {
    service: String,
    instance: String,
}
message_type!(Find, "t.Find");
impl Query for Find {
    type Reply = Found;
}

fn find() -> Find {
    Find { term: "x".into() }
}

/// How a target answers `t.Find`.
#[derive(Clone, Copy, Debug)]
enum Behaviour {
    /// At once, with its service and instance.
    Answer,
    /// After a delay, with its service and instance.
    After(u64),
    /// A handler error "boom".
    Fail,
    /// Never.
    Silent,
}

struct Node {
    pool: Arc<HubPool>,
    sender: Arc<Sender>,
}

impl Node {
    async fn shutdown(self) {
        self.pool.shutdown().await;
    }
}

fn settings(address: &str) -> HubSettings {
    HubSettings {
        address: address.to_string(),
        heartbeat_interval: Duration::from_secs(1),
        backoff_min: Duration::from_millis(50),
        backoff_max: Duration::from_millis(200),
        ..HubSettings::default()
    }
}

async fn answer(
    behaviour: Behaviour,
    service: String,
    instance: String,
    request: proto::Request,
    reply_to: ReplyTo,
) {
    let result = match behaviour {
        Behaviour::Silent => return,
        Behaviour::Fail => reply::Result::Error(proto::Error {
            request_id: request.id.clone(),
            code: ErrorCode::HandlerError as i32,
            message: "boom".into(),
            details: vec![],
        }),
        Behaviour::Answer | Behaviour::After(_) => {
            if let Behaviour::After(ms) = behaviour {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            let found = Found { service, instance };
            reply::Result::Payload(serde_json::to_vec(&found).unwrap())
        }
    };
    let _ = reply_to
        .send(ClientMessage {
            message: Some(client_message::Message::Reply(proto::Reply {
                request_id: request.id,
                result: Some(result),
            })),
        })
        .await;
}

/// An instance of `service` on the hubs of `address`, registering `types`, answering `t.Find`
/// per `behaviour`; its sender sends with `sender_settings`.
async fn node_with(
    address: &str,
    service: &str,
    instance: &str,
    types: RegisteredTypes,
    behaviour: Behaviour,
    sender_settings: SenderSettings,
) -> Node {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let pool = HubPool::new(settings(address), service.into(), instance.into(), tx).unwrap();
    pool.set_registered_types(types);
    let sender = Sender::with_pool(Arc::clone(&pool), sender_settings);
    pool.start();
    let forward = Arc::clone(&sender);
    let (service, instance) = (service.to_string(), instance.to_string());
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { request, reply_to } => {
                    tokio::spawn(answer(
                        behaviour,
                        service.clone(),
                        instance.clone(),
                        request,
                        reply_to,
                    ));
                }
                other => forward.on_inbound(other),
            }
        }
    });
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    Node { pool, sender }
}

fn shared(types: &[&str]) -> RegisteredTypes {
    RegisteredTypes {
        shared_queries: types.iter().map(|t| t.to_string()).collect(),
        ..RegisteredTypes::default()
    }
}

async fn target(address: &str, service: &str, instance: &str, behaviour: Behaviour) -> Node {
    node_with(
        address,
        service,
        instance,
        shared(&["t.Find"]),
        behaviour,
        SenderSettings::default(),
    )
    .await
}

async fn caller(address: &str) -> Node {
    caller_with(address, SenderSettings::default()).await
}

async fn caller_with(address: &str, sender_settings: SenderSettings) -> Node {
    node_with(
        address,
        "caller",
        "c1",
        RegisteredTypes::default(),
        Behaviour::Silent,
        sender_settings,
    )
    .await
}

fn who<R>(parts: &[ScatterPart<R>]) -> Vec<(String, String)> {
    parts
        .iter()
        .map(|p| (p.service.clone(), p.instance.clone()))
        .collect()
}

fn pair(s: &str, i: &str) -> (String, String) {
    (s.to_string(), i.to_string())
}

/// Polls `check` every 20 ms for at most 5 s.
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Scatter-gather requests the hub received.
async fn gathers(hub: &FakeHub) -> Vec<proto::Request> {
    hub.requests()
        .await
        .into_iter()
        .filter(|r| r.mode == proto::RequestMode::ScatterGather as i32)
        .collect()
}

#[tokio::test]
async fn two_owners_with_three_instances_give_three_parts_then_the_end() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let nodes = vec![
        target(&a, "svc-a", "a1", Behaviour::Answer).await,
        target(&a, "svc-b", "b1", Behaviour::Answer).await,
        target(&a, "svc-b", "b2", Behaviour::Answer).await,
    ];
    let c = caller(&a).await;
    let result = c
        .sender
        .scatter_gather(&find(), ScatterOptions::default())
        .await
        .unwrap();
    let mut seen = who(&result.parts);
    seen.sort();
    assert_eq!(
        seen,
        [
            pair("svc-a", "a1"),
            pair("svc-b", "b1"),
            pair("svc-b", "b2")
        ]
    );
    for p in &result.parts {
        let found = p.outcome.as_ref().expect("a payload part");
        assert_eq!((&found.service, &found.instance), (&p.service, &p.instance));
    }
    assert_eq!((result.failed, result.timed_out), (0, 0));
    // The registrations: shared, not exclusive.
    let registers = hub.registers().await;
    let a1 = registers.iter().find(|r| r.instance_id == "a1").unwrap();
    assert_eq!(a1.shared_query_types, ["t.Find"]);
    assert!(a1.query_types.is_empty());
    assert_eq!(hub.open_gathers(), 0);
    for n in nodes {
        n.shutdown().await;
    }
    c.shutdown().await;
}

#[tokio::test]
async fn options_become_the_request_fields() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let t = target(&a, "svc-a", "a1", Behaviour::Answer).await;
    let c = caller(&a).await;
    c.sender
        .scatter_gather(&find(), ScatterOptions::default())
        .await
        .unwrap();
    let options = ScatterOptions {
        timeout: Some(Duration::from_millis(2_500)),
        max_results: 2,
        priority: Some(7),
    };
    c.sender.scatter_gather(&find(), options).await.unwrap();
    let sent = gathers(&hub).await;
    assert_eq!(sent.len(), 2);
    let (default, chosen) = (&sent[0], &sent[1]);
    assert_eq!(default.kind, proto::RequestKind::Query as i32);
    assert_eq!(default.r#type, "t.Find");
    assert_eq!(default.payload, br#"{"term":"x"}"#);
    assert_eq!(default.routing_key, "");
    assert_eq!(
        (default.timeout_ms, default.max_results, default.priority),
        (0, 0, 0)
    );
    assert!(
        default.headers.contains_key("correlationId"),
        "{:?}",
        default.headers.keys()
    );
    assert_eq!(chosen.mode, proto::RequestMode::ScatterGather as i32);
    assert_eq!(
        (chosen.timeout_ms, chosen.max_results, chosen.priority),
        (2_500, 2, 7)
    );
    assert_ne!(default.id, chosen.id);
    t.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn configured_timeout_and_priority_apply_when_the_options_name_none() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let t = target(&a, "svc-a", "a1", Behaviour::Answer).await;
    let c = caller_with(
        &a,
        SenderSettings {
            default_timeout: Some(Duration::from_secs(3)),
            priorities: [("t.Find".to_string(), 4)].into(),
            ..SenderSettings::default()
        },
    )
    .await;
    c.sender
        .scatter_gather(&find(), ScatterOptions::default())
        .await
        .unwrap();
    let sent = gathers(&hub).await;
    assert_eq!((sent[0].timeout_ms, sent[0].priority), (3_000, 4));
    t.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_failing_handler_is_an_error_part() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let ok = target(&a, "svc-a", "a1", Behaviour::Answer).await;
    let bad = target(&a, "svc-b", "b1", Behaviour::Fail).await;
    let c = caller(&a).await;
    let result = c
        .sender
        .scatter_gather(&find(), ScatterOptions::default())
        .await
        .unwrap();
    assert_eq!(result.parts.len(), 2);
    assert_eq!((result.failed, result.timed_out), (1, 0));
    let failed = result.parts.iter().find(|p| p.instance == "b1").unwrap();
    let error: &RemoteError = failed.outcome.as_ref().unwrap_err();
    assert_eq!(error.code, ErrorCode::HandlerError);
    assert_eq!(error.message, "boom");
    let e = error.clone().into_error("t.Find");
    assert!(
        matches!(e, MessagingError::RemoteHandler { ref message, .. } if message == "boom"),
        "{e:?}"
    );
    assert!(
        result
            .parts
            .iter()
            .any(|p| p.instance == "a1" && p.outcome.is_ok())
    );
    ok.shutdown().await;
    bad.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_target_that_does_not_answer_in_time_is_counted_timed_out() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let ok = target(&a, "svc-a", "a1", Behaviour::Answer).await;
    let mute = target(&a, "svc-b", "b1", Behaviour::Silent).await;
    let c = caller(&a).await;
    let options = ScatterOptions {
        timeout: Some(Duration::from_millis(300)),
        ..ScatterOptions::default()
    };
    let started = Instant::now();
    let result = c.sender.scatter_gather(&find(), options).await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(who(&result.parts), [pair("svc-a", "a1")]);
    assert_eq!((result.failed, result.timed_out), (0, 1));
    ok.shutdown().await;
    mute.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn max_results_ends_the_gather_early() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let nodes = vec![
        target(&a, "svc-a", "a1", Behaviour::Answer).await,
        target(&a, "svc-b", "b1", Behaviour::After(300)).await,
        target(&a, "svc-b", "b2", Behaviour::After(300)).await,
    ];
    let c = caller(&a).await;
    let options = ScatterOptions {
        max_results: 1,
        ..ScatterOptions::default()
    };
    let started = Instant::now();
    let result = c.sender.scatter_gather(&find(), options).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(who(&result.parts), [pair("svc-a", "a1")]);
    assert_eq!((result.failed, result.timed_out), (0, 0));
    // The answers after the end are dropped; nothing is left pending on the client.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(c.sender.in_flight(), 0);
    for n in nodes {
        n.shutdown().await;
    }
    c.shutdown().await;
}

#[tokio::test]
async fn the_stream_yields_parts_in_arrival_order_and_completes_on_the_end() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let nodes = vec![
        target(&a, "svc-a", "a1", Behaviour::After(300)).await,
        target(&a, "svc-b", "b1", Behaviour::Answer).await,
        target(&a, "svc-b", "b2", Behaviour::After(150)).await,
    ];
    let c = caller(&a).await;
    let stream = c
        .sender
        .scatter_gather_stream(&find(), ScatterOptions::default());
    let mut stream = Box::pin(stream);
    let mut order = vec![];
    while let Some(part) = stream.next().await {
        let part = part.unwrap();
        assert_eq!(part.outcome.as_ref().unwrap().instance, part.instance);
        order.push(part.instance);
    }
    drop(stream);
    assert_eq!(order, ["b1", "b2", "a1"]);
    assert_eq!(c.sender.in_flight(), 0);
    for n in nodes {
        n.shutdown().await;
    }
    c.shutdown().await;
}

#[tokio::test]
async fn a_hub_without_the_capability_is_unsupported_and_gets_nothing() {
    for legacy in [false, true] {
        let hub = FakeHub::start().await;
        if legacy {
            hub.set_ack(None);
        } else {
            hub.set_scatter_gather(false);
        }
        let a = hub.address();
        let c = caller(&a).await;
        let e = c
            .sender
            .scatter_gather(&find(), ScatterOptions::default())
            .await
            .unwrap_err();
        assert!(
            matches!(e, MessagingError::UnsupportedByHub(_)),
            "legacy {legacy}: {e:?}"
        );
        let stream = c
            .sender
            .scatter_gather_stream(&find(), ScatterOptions::default());
        let mut stream = Box::pin(stream);
        let first = stream.next().await.expect("one error");
        assert!(
            matches!(first, Err(MessagingError::UnsupportedByHub(_))),
            "{first:?}"
        );
        assert!(stream.next().await.is_none());
        drop(stream);
        assert!(hub.requests().await.is_empty(), "nothing is sent");
        c.shutdown().await;
    }
}

#[tokio::test]
async fn errors_before_any_target_map_to_the_unary_errors() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let c = caller(&a).await;
    // No target.
    let e = c
        .sender
        .scatter_gather(&find(), ScatterOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(e, MessagingError::NoHandler(ref t) if t == "t.Find"),
        "{e:?}"
    );
    // The type is a command.
    let owner = node_with(
        &a,
        "svc-cmd",
        "x1",
        RegisteredTypes {
            commands: vec!["t.Find".into()],
            ..RegisteredTypes::default()
        },
        Behaviour::Answer,
        SenderSettings::default(),
    )
    .await;
    let e = c
        .sender
        .scatter_gather(&find(), ScatterOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(e, MessagingError::InvalidMessage(_)), "{e:?}");
    // The hub stopped sharing the capability after the handshake: it answers UNSUPPORTED.
    hub.set_scatter_gather(false);
    let e = c
        .sender
        .scatter_gather(&find(), ScatterOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(e, MessagingError::UnsupportedByHub(_)), "{e:?}");
    assert_eq!(c.sender.in_flight(), 0);
    owner.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn parts_and_ends_of_unknown_requests_are_ignored() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let t = target(&a, "svc-a", "a1", Behaviour::Answer).await;
    let c = caller(&a).await;
    let stray_part = proto::ServerMessage {
        message: Some(proto::server_message::Message::Part(proto::ReplyPart {
            request_id: "nobody".into(),
            payload: br#"{"service":"x","instance":"y"}"#.to_vec(),
            responder_service: "x".into(),
            responder_instance: "y".into(),
            ..Default::default()
        })),
    };
    let stray_end = proto::ServerMessage {
        message: Some(proto::server_message::Message::End(proto::ReplyEnd {
            request_id: "nobody".into(),
            parts: 1,
            ..Default::default()
        })),
    };
    hub.send_to_clients(stray_part).await;
    hub.send_to_clients(stray_end).await;
    let result = c
        .sender
        .scatter_gather(&find(), ScatterOptions::default())
        .await
        .unwrap();
    assert_eq!(who(&result.parts), [pair("svc-a", "a1")]);
    assert_eq!(c.sender.in_flight(), 0);
    t.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_unary_query_ignores_parts_and_ends_carrying_its_id() {
    let hub = FakeHub::start().await;
    // The hub answers a unary query with a part and an end first (never sent by kraken-hub),
    // then the reply: only the reply completes it.
    let a = hub.address();
    let c = caller(&a).await;
    let pushed = Arc::new(std::sync::Mutex::new(None::<String>));
    let seen = Arc::clone(&pushed);
    hub.set_responder(move |req| {
        *seen.lock().unwrap() = Some(req.id.clone());
        None
    })
    .await;
    let sender = Arc::clone(&c.sender);
    let call = tokio::spawn(async move {
        sender
            .send_query(&find(), kraken_messaging::SendOptions::default())
            .await
    });
    let id = loop {
        if let Some(id) = pushed.lock().unwrap().clone() {
            break id;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    for m in [
        proto::server_message::Message::Part(proto::ReplyPart {
            request_id: id.clone(),
            payload: b"{}".to_vec(),
            ..Default::default()
        }),
        proto::server_message::Message::End(proto::ReplyEnd {
            request_id: id.clone(),
            ..Default::default()
        }),
        proto::server_message::Message::Reply(proto::Reply {
            request_id: id.clone(),
            result: Some(reply::Result::Payload(
                br#"{"service":"s","instance":"i"}"#.to_vec(),
            )),
        }),
    ] {
        hub.send_to_clients(proto::ServerMessage { message: Some(m) })
            .await;
    }
    let found = call.await.unwrap().unwrap();
    assert_eq!(found.instance, "i");
    c.shutdown().await;
}

#[tokio::test]
async fn a_stream_lost_mid_gather_fails_it_with_hub_connection_lost() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let ok = target(&a, "svc-a", "a1", Behaviour::Answer).await;
    let mute = target(&a, "svc-b", "b1", Behaviour::Silent).await;
    let c = caller(&a).await;
    let sender = Arc::clone(&c.sender);
    let call = tokio::spawn(async move {
        sender
            .scatter_gather(&find(), ScatterOptions::default())
            .await
    });
    // The part of a1 arrived, b1 holds the gather open.
    while gathers(&hub).await.is_empty() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    hub.drop_streams();
    let e = call.await.unwrap().unwrap_err();
    assert!(matches!(e, MessagingError::HubConnectionLost(_)), "{e:?}");
    assert_eq!(c.sender.in_flight(), 0);
    ok.shutdown().await;
    mute.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn the_retry_policy_repeats_the_whole_gather() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let c = caller_with(
        &a,
        SenderSettings {
            retry: Some(RetryPolicy {
                max_attempts: 20,
                initial_backoff: Duration::from_millis(100),
                multiplier: 1.0,
                max_backoff: Duration::from_millis(100),
            }),
            default_timeout: Some(Duration::from_secs(5)),
            ..SenderSettings::default()
        },
    )
    .await;
    let sender = Arc::clone(&c.sender);
    let call = tokio::spawn(async move {
        sender
            .scatter_gather(&find(), ScatterOptions::default())
            .await
    });
    // NO_HANDLER until a target registers.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let t = target(&a, "svc-a", "a1", Behaviour::Answer).await;
    let result = call.await.unwrap().unwrap();
    assert_eq!(who(&result.parts), [pair("svc-a", "a1")]);
    let sent = gathers(&hub).await;
    assert!(sent.len() >= 2, "{} attempts", sent.len());
    let mut ids: Vec<&str> = sent.iter().map(|r| r.id.as_str()).collect();
    ids.dedup();
    assert_eq!(ids.len(), sent.len(), "each attempt has a fresh id");
    t.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_pool_sends_each_gather_to_one_hub_and_only_to_capable_ones() {
    let (h1, h2) = (
        FakeHub::start_with_id("hub-1").await,
        FakeHub::start_with_id("hub-2").await,
    );
    let both = format!("{},{}", h1.address(), h2.address());
    let t = target(&both, "svc-a", "a1", Behaviour::Answer).await;
    let c = caller(&both).await;
    for _ in 0..4 {
        let r = c
            .sender
            .scatter_gather(&find(), ScatterOptions::default())
            .await
            .unwrap();
        assert_eq!(r.parts.len(), 1, "one part: the gather went to one hub");
    }
    let (n1, n2) = (gathers(&h1).await.len(), gathers(&h2).await.len());
    assert_eq!((n1, n2), (2, 2), "round-robin");
    t.shutdown().await;
    c.shutdown().await;

    // hub-3 is a P3 hub: every gather goes to hub-4.
    let (h3, h4) = (
        FakeHub::start_with_id("hub-3").await,
        FakeHub::start_with_id("hub-4").await,
    );
    h3.set_scatter_gather(false);
    let both = format!("{},{}", h3.address(), h4.address());
    let t = target(&both, "svc-a", "a1", Behaviour::Answer).await;
    let c = caller(&both).await;
    for _ in 0..4 {
        c.sender
            .scatter_gather(&find(), ScatterOptions::default())
            .await
            .unwrap();
    }
    assert_eq!((gathers(&h3).await.len(), gathers(&h4).await.len()), (0, 4));
    t.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_pool_gather_whose_hub_breaks_fails_and_the_next_one_succeeds_on_the_other() {
    let (h1, h2) = (
        FakeHub::start_with_id("hub-1").await,
        FakeHub::start_with_id("hub-2").await,
    );
    let both = format!("{},{}", h1.address(), h2.address());
    let t = target(&both, "svc-a", "a1", Behaviour::After(500)).await;
    let c = caller(&both).await;
    let sender = Arc::clone(&c.sender);
    let call = tokio::spawn(async move {
        sender
            .scatter_gather(&find(), ScatterOptions::default())
            .await
    });
    let carrier = loop {
        if !gathers(&h1).await.is_empty() {
            break &h1;
        }
        if !gathers(&h2).await.is_empty() {
            break &h2;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    carrier.set_down(true);
    let e = call.await.unwrap().unwrap_err();
    assert!(matches!(e, MessagingError::HubConnectionLost(_)), "{e:?}");
    let r = c
        .sender
        .scatter_gather(&find(), ScatterOptions::default())
        .await
        .unwrap();
    assert_eq!(r.parts.len(), 1);
    t.shutdown().await;
    c.shutdown().await;
}

/// A gather whose future is dropped while its write still waits for room in the stream's
/// outbound buffer leaves no pending entry behind (the unary path has the same guard): a leaked
/// entry would keep `in_flight` up and hold a draining connection open until its stream ends.
#[tokio::test]
async fn a_gather_dropped_while_its_write_is_blocked_leaves_nothing_pending() {
    let hub = FakeHub::start().await;
    let a = hub.address();
    let c = caller(&a).await;
    // The hub stops reading: a few 256 KiB gathers fill the HTTP/2 windows, 256 more fill the
    // stream's outbound buffer, and the rest wait in the write itself.
    hub.set_stall_reads(true);
    let big = Find {
        term: "x".repeat(256 * 1024),
    };
    let calls: Vec<_> = (0..324)
        .map(|i| {
            let sender = Arc::clone(&c.sender);
            let query = if i < 24 { big.clone() } else { find() };
            tokio::spawn(async move {
                sender
                    .scatter_gather(&query, ScatterOptions::default())
                    .await
            })
        })
        .collect();
    // (One the hub read before it stalled may have finished with NO_HANDLER already.)
    eventually("every gather recorded its pending entry", async || {
        let finished = calls.iter().filter(|c| c.is_finished()).count();
        c.sender.in_flight() + finished == calls.len()
    })
    .await;
    for call in &calls {
        call.abort();
    }
    for call in calls {
        let _ = call.await;
    }
    assert_eq!(
        c.sender.in_flight(),
        0,
        "a dropped gather leaves nothing pending"
    );
    hub.drop_streams();
    c.shutdown().await;
}
