//! Subscription queries (ADR 0017, P4c) against in-process fake hubs: the handler side (the
//! initial result answered like a unary query, the `QueryUpdateEmitter`'s parts and ends, the
//! hub's `Cancel`, a lost hub stream, the per-instance limit, no permit held) and the sender side
//! (`Sender::subscribe`: the initial result, the updates of every owner instance, credit refills,
//! the hub's buffer, `max_results`, cancel and drop, errors, retries until the initial part, the
//! capability gates, shared and exclusive owners, a pool over capable hubs only).
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use kraken_messaging::hub::{
    Dispatcher, HandlerRegistry, HubPool, HubSettings, Inbound, QueryUpdateEmitter,
    RegisteredTypes, Sender, SenderSettings,
};
use kraken_messaging::identity::{Guard, IdentityResolver};
use kraken_messaging::proto::{self, ErrorCode, ServerMessage, server_message};
use kraken_messaging::{
    Command, MessagingError, Query, StreamingQuery, SubscriptionQuery, message_type,
};
use serde::{Deserialize, Serialize};
use support::fake_hub::FakeHub;
use tokio::sync::mpsc;

/// A unary query, next to the subscription ones.
#[derive(Serialize, Deserialize)]
struct Lookup {}
message_type!(Lookup, "t.Lookup");
impl Query for Lookup {
    type Reply = ();
}

/// A subscription query: the latest quote of `symbol`, then every tick of it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Prices {
    symbol: String,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Quote {
    symbol: String,
    price: u32,
    instance: String,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Tick {
    symbol: String,
    price: u32,
}
message_type!(Prices, "t.Prices");
impl SubscriptionQuery for Prices {
    type Initial = Quote;
    type Update = Tick;
}

/// A shared subscription type: several services own it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct SharedPrices {
    symbol: String,
}
message_type!(SharedPrices, "t.SharedPrices");
impl SubscriptionQuery for SharedPrices {
    type Initial = Quote;
    type Update = Tick;
}

/// A command, next to the queries.
#[derive(Serialize, Deserialize)]
struct Fill {
    data: String,
}
message_type!(Fill, "t.Fill");
impl Command for Fill {
    type Reply = ();
}

/// A streaming query, for the mismatch tests.
#[derive(Serialize, Deserialize)]
struct Export {}
message_type!(Export, "t.Export");
impl StreamingQuery for Export {
    type Item = Tick;
}

/// A subscription type that requires the `admin` role.
#[derive(Serialize, Deserialize)]
struct Admin {}
message_type!(Admin, "t.Admin");
impl SubscriptionQuery for Admin {
    type Initial = ();
    type Update = ();
}

fn prices(symbol: &str) -> Prices {
    Prices {
        symbol: symbol.into(),
    }
}

fn tick(symbol: &str, price: u32) -> Tick {
    Tick {
        symbol: symbol.into(),
        price,
    }
}

/// A request the hub forwards to a handler: `mode = SUBSCRIPTION` (an update target) or
/// `UNARY` (the initial result, or a plain query).
fn forwarded(id: &str, type_name: &str, payload: &[u8], subscription: bool) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Request(proto::Request {
            id: id.into(),
            kind: proto::RequestKind::Query as i32,
            r#type: type_name.into(),
            payload: payload.to_vec(),
            mode: if subscription {
                proto::RequestMode::Subscription as i32
            } else {
                proto::RequestMode::Unary as i32
            },
            ..Default::default()
        })),
    }
}

fn subscribed(id: &str, query: &Prices) -> ServerMessage {
    forwarded(id, "t.Prices", &serde_json::to_vec(query).unwrap(), true)
}

fn cancel(id: &str, reason: &str) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Cancel(proto::Cancel {
            request_id: id.into(),
            reason: reason.into(),
        })),
    }
}

fn end_code(end: &proto::ReplyEnd) -> Option<ErrorCode> {
    end.error
        .as_ref()
        .map(|e| ErrorCode::try_from(e.code).unwrap())
}

fn end_message(end: &proto::ReplyEnd) -> &str {
    end.error.as_ref().map(|e| e.message.as_str()).unwrap_or("")
}

/// What the handlers of one node saw.
#[derive(Default)]
struct Probe {
    /// Initial-result handlers invoked.
    initials: AtomicU32,
}

/// The handlers of a node of `instance`: `t.Prices` (the initial quote names the instance;
/// a symbol `fail` fails the initial result), `t.Lookup` (unary).
fn registry(instance: &str, probe: &Arc<Probe>) -> HandlerRegistry {
    let mut r = HandlerRegistry::new();
    let p = Arc::clone(probe);
    let me = instance.to_string();
    r.subscription_query::<Prices, _, _>(move |q, _| {
        let probe = Arc::clone(&p);
        let me = me.clone();
        async move {
            probe.initials.fetch_add(1, Ordering::SeqCst);
            if q.symbol == "fail" {
                return Err(MessagingError::Storage("quotes gone".into()));
            }
            Ok(Quote {
                symbol: q.symbol,
                price: 100,
                instance: me,
            })
        }
    });
    r.query::<Lookup, _, _>(|_, _| async move { Ok(()) });
    r
}

struct Node {
    pool: Arc<HubPool>,
    sender: Arc<Sender>,
    dispatcher: Arc<Dispatcher>,
    emitter: QueryUpdateEmitter,
}

impl Node {
    async fn shutdown(self) {
        self.pool.shutdown().await;
    }
}

fn settings(address: &str, max_handlers: usize) -> HubSettings {
    HubSettings {
        address: address.to_string(),
        heartbeat_interval: Duration::from_secs(1),
        backoff_min: Duration::from_millis(50),
        backoff_max: Duration::from_millis(200),
        max_concurrent_handlers: max_handlers,
        ..HubSettings::default()
    }
}

/// An instance of `service` on the hubs of `address` serving `registry` with `max_handlers`
/// permits, wired as `KrakenMessaging` wires it: requests to the dispatcher, `Flow` and `Cancel`
/// to its running handlers and subscriptions, everything else to the sender.
async fn node_with(
    address: &str,
    service: &str,
    instance: &str,
    registry: HandlerRegistry,
    max_handlers: usize,
    sender_settings: SenderSettings,
    max_subscriptions: Option<usize>,
) -> Node {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let pool = HubPool::new(
        settings(address, max_handlers),
        service.into(),
        instance.into(),
        tx,
    )
    .unwrap();
    pool.set_registered_types(registry.registered_types());
    let sender = Sender::with_pool(Arc::clone(&pool), sender_settings);
    let mut dispatcher = Dispatcher::new(
        Arc::new(registry),
        Arc::new(IdentityResolver::new(None)),
        max_handlers,
    );
    if let Some(max) = max_subscriptions {
        dispatcher = dispatcher.max_subscriptions(max);
    }
    let dispatcher = Arc::new(dispatcher);
    let emitter = dispatcher.emitter();
    pool.start();
    let (forward, serve) = (Arc::clone(&sender), Arc::clone(&dispatcher));
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { request, reply_to } => {
                    tokio::spawn(serve.serve(request, reply_to));
                }
                Inbound::Flow(f) => serve.flow(&f.request_id, f.credits),
                Inbound::Cancel(c) => serve.cancel(&c.request_id, &c.reason),
                Inbound::Disconnected { stream, .. } => {
                    serve.stream_lost(stream);
                    forward.on_inbound(msg);
                }
                other => forward.on_inbound(other),
            }
        }
    });
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    Node {
        pool,
        sender,
        dispatcher,
        emitter,
    }
}

/// An owner of `t.Prices`: instance `instance` of `service`.
async fn owner(address: &str, service: &str, instance: &str, probe: &Arc<Probe>) -> Node {
    node_with(
        address,
        service,
        instance,
        registry(instance, probe),
        4,
        SenderSettings::default(),
        None,
    )
    .await
}

/// A node without handlers, sending with `sender_settings`.
async fn caller_with(address: &str, sender_settings: SenderSettings) -> Node {
    node_with(
        address,
        "caller",
        "c1",
        HandlerRegistry::new(),
        4,
        sender_settings,
        None,
    )
    .await
}

async fn caller(address: &str) -> Node {
    caller_with(address, SenderSettings::default()).await
}

/// Polls `check` every 20 ms for at most 5 s.
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// Registration: subscription types are query types to the hub, exclusive or shared.

#[test]
fn subscription_handlers_are_query_types_exclusive_or_shared() {
    let probe = Arc::new(Probe::default());
    let mut r = registry("x", &probe);
    r.shared_subscription_query::<SharedPrices, _, _>(|q, _| async move {
        Ok(Quote {
            symbol: q.symbol,
            price: 1,
            instance: "x".into(),
        })
    });
    assert_eq!(
        r.registered_types(),
        RegisteredTypes {
            commands: vec![],
            queries: vec!["t.Lookup".into()],
            shared_queries: vec!["t.SharedPrices".into()],
            streaming_queries: vec![],
            subscription_queries: vec!["t.Prices".into()],
        }
    );
    assert_eq!(r.query_types(), ["t.Lookup", "t.Prices", "t.SharedPrices"]);
    assert_eq!(r.exclusive_query_types(), ["t.Lookup"]);
    assert_eq!(r.subscription_query_types(), ["t.Prices", "t.SharedPrices"]);
    assert_eq!(r.shared_query_types(), ["t.SharedPrices"]);
    assert!(r.streaming_query_types().is_empty());
}

/// `t.Prices` as a unary query, for the mismatch tests.
#[derive(Serialize, Deserialize)]
struct PricesOnce {}
message_type!(PricesOnce, "t.Prices");
impl Query for PricesOnce {
    type Reply = Quote;
}

#[test]
#[should_panic(expected = "duplicate handler for t.Prices")]
fn a_subscription_type_has_one_handler_whatever_its_kind() {
    let probe = Arc::new(Probe::default());
    let mut r = registry("x", &probe);
    r.query::<PricesOnce, _, _>(|_, _| async move {
        Err::<Quote, _>(MessagingError::Storage("x".into()))
    });
}

#[test]
#[should_panic(expected = "registered both as an exclusive and as a shared query")]
fn a_subscription_type_cannot_be_exclusive_and_shared() {
    let mut r = HandlerRegistry::new();
    r.subscription_query::<Prices, _, _>(|_, _| async move {
        Err::<Quote, _>(MessagingError::Storage("x".into()))
    });
    r.shared_subscription_query::<Prices, _, _>(|_, _| async move {
        Err::<Quote, _>(MessagingError::Storage("x".into()))
    });
}

#[tokio::test]
async fn the_register_declares_subscription_and_announces_the_types() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let mut r = registry("a1", &probe);
    r.shared_subscription_query::<SharedPrices, _, _>(|q, _| async move {
        Ok(Quote {
            symbol: q.symbol,
            price: 1,
            instance: "a1".into(),
        })
    });
    let a = node_with(
        &hub.address(),
        "svc-a",
        "a1",
        r,
        4,
        SenderSettings::default(),
        None,
    )
    .await;
    let register = &hub.registers().await[0];
    assert!(
        register.capabilities.iter().any(|c| c == "subscription"),
        "{:?}",
        register.capabilities
    );
    assert_eq!(register.query_types, ["t.Lookup", "t.Prices"]);
    assert_eq!(register.shared_query_types, ["t.SharedPrices"]);
    let handshake = a.pool.handshake_for("").unwrap();
    assert!(handshake.subscription(), "{handshake:?}");
    assert_eq!(a.emitter.open_subscriptions(), 0);
    a.shutdown().await;
}

// Handler side, driven by messages the fake hub pushes as kraken-hub would.

#[tokio::test]
async fn a_subscription_request_registers_and_the_emitter_sends_parts_then_the_end() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    eventually("the subscription registered", async || {
        a.emitter.open_subscriptions() == 1
    })
    .await;
    assert_eq!(
        probe.initials.load(Ordering::SeqCst),
        0,
        "a subscription request runs no handler"
    );
    a.emitter
        .emit::<Prices>(|q| q.symbol == "acme", &tick("acme", 101));
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 102));
    // The filter: another symbol's update is not for this subscription.
    a.emitter
        .emit::<Prices>(|q| q.symbol == "other", &tick("other", 1));
    eventually("two parts at the hub", async || hub.parts().len() == 2).await;
    let parts = hub.parts();
    for (n, (_, part)) in parts.iter().enumerate() {
        assert_eq!(part.request_id, "s1#1");
        assert_eq!(part.seq, n as u64 + 1, "seq from 1 per subscription");
        assert!(part.error.is_none());
        let update: Tick = serde_json::from_slice(&part.payload).unwrap();
        assert_eq!(update, tick("acme", 101 + n as u32));
    }
    a.emitter.complete::<Prices>(|q| q.symbol == "acme");
    eventually("the end at the hub", async || hub.ends().len() == 1).await;
    let (_, end) = &hub.ends()[0];
    assert_eq!(end.request_id, "s1#1");
    assert_eq!(end_code(end), None, "a clean end");
    assert_eq!(a.emitter.open_subscriptions(), 0);
    // Completed: nothing more reaches it.
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 103));
    a.emitter.complete::<Prices>(|_| true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!((hub.parts().len(), hub.ends().len()), (2, 1));
    a.shutdown().await;
}

#[tokio::test]
async fn the_initial_request_is_answered_by_the_handler_like_a_unary_query() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let payload = serde_json::to_vec(&prices("acme")).unwrap();
    hub.send_to_clients(forwarded("s1#0", "t.Prices", &payload, false))
        .await;
    eventually("the initial reply", async || hub.replies().await.len() == 1).await;
    let reply = &hub.replies().await[0];
    assert_eq!(reply.request_id, "s1#0");
    let Some(proto::reply::Result::Payload(p)) = &reply.result else {
        panic!("{reply:?}");
    };
    let quote: Quote = serde_json::from_slice(p).unwrap();
    assert_eq!(
        quote,
        Quote {
            symbol: "acme".into(),
            price: 100,
            instance: "a1".into()
        }
    );
    assert_eq!(probe.initials.load(Ordering::SeqCst), 1);
    eventually("the permit back", async || {
        a.dispatcher.available_permits() == 4
    })
    .await;
    assert_eq!(
        a.emitter.open_subscriptions(),
        0,
        "no subscription registered"
    );
    // A failing initial result is the handler's error, as for any query.
    let failing = serde_json::to_vec(&prices("fail")).unwrap();
    hub.send_to_clients(forwarded("s2#0", "t.Prices", &failing, false))
        .await;
    eventually("the error reply", async || hub.replies().await.len() == 2).await;
    let Some(proto::reply::Result::Error(e)) = &hub.replies().await[1].result else {
        panic!("expected an error");
    };
    assert_eq!(e.code, ErrorCode::HandlerError as i32);
    assert!(e.message.contains("quotes gone"), "{}", e.message);
    // A unary request without the `#0` suffix used the wrong mode.
    hub.send_to_clients(forwarded("plain", "t.Prices", &payload, false))
        .await;
    eventually("the rejection", async || hub.replies().await.len() == 3).await;
    let Some(proto::reply::Result::Error(e)) = &hub.replies().await[2].result else {
        panic!("expected an error");
    };
    assert_eq!(e.code, ErrorCode::InvalidRequest as i32);
    assert_eq!(
        e.message,
        "t.Prices is a subscription query: send it with Sender::subscribe (mode SUBSCRIPTION)"
    );
    assert_eq!(probe.initials.load(Ordering::SeqCst), 2);
    a.shutdown().await;
}

#[tokio::test]
async fn complete_exceptionally_ends_the_subscription_with_handler_error() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    hub.send_to_clients(subscribed("s2#1", &prices("other")))
        .await;
    eventually("both registered", async || {
        a.emitter.open_subscriptions() == 2
    })
    .await;
    a.emitter.emit::<Prices>(|_| true, &tick("any", 1));
    a.emitter.complete_exceptionally::<Prices>(
        |q| q.symbol == "acme",
        &MessagingError::Storage("quotes gone".into()),
    );
    eventually("the error end", async || hub.ends().len() == 1).await;
    let (_, end) = &hub.ends()[0];
    assert_eq!(end.request_id, "s1#1");
    assert_eq!(end_code(end), Some(ErrorCode::HandlerError));
    assert!(end_message(end).contains("quotes gone"), "{end:?}");
    assert_eq!(a.emitter.open_subscriptions(), 1, "the other one lives on");
    assert_eq!(hub.parts().len(), 2, "the update reached both first");
    a.shutdown().await;
}

#[tokio::test]
async fn a_subscription_request_the_type_cannot_serve_is_refused_with_an_end() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let mut r = registry("a1", &probe);
    r.streaming_query::<Export, _, _, _>(|_, _| async move {
        Ok(tokio_stream::empty::<Result<Tick, MessagingError>>())
    });
    r.subscription_query::<Admin, _, _>(|_, _| async move { Ok(()) });
    r.requires::<Admin>(Guard::role("admin"));
    r.command::<Fill, _, _>(|_, _| async move { Ok(()) });
    let a = node_with(
        &hub.address(),
        "svc-a",
        "a1",
        r,
        4,
        SenderSettings::default(),
        None,
    )
    .await;
    let cases: [(&str, &str, &[u8], ErrorCode, &str); 6] = [
        (
            "u#1",
            "t.Lookup",
            b"{}",
            ErrorCode::InvalidRequest,
            "t.Lookup is not a subscription query handler (it answers mode UNARY)",
        ),
        (
            "x#1",
            "t.Export",
            b"{}",
            ErrorCode::InvalidRequest,
            "t.Export is not a subscription query handler (it answers mode STREAMING)",
        ),
        ("n#1", "t.Nope", b"{}", ErrorCode::NoHandler, "t.Nope"),
        ("g#1", "t.Admin", b"{}", ErrorCode::Unauthenticated, ""),
        (
            "d#1",
            "t.Prices",
            b"not json",
            ErrorCode::InvalidRequest,
            "cannot decode payload",
        ),
        ("c#1", "t.Fill", b"{}", ErrorCode::NoHandler, "t.Fill"),
    ];
    for (n, (id, type_name, payload, code, message)) in cases.iter().enumerate() {
        hub.send_to_clients(forwarded(id, type_name, payload, true))
            .await;
        eventually("the end", async || hub.ends().len() == n + 1).await;
        let (_, end) = &hub.ends()[n];
        assert_eq!(end.request_id, *id);
        assert_eq!(end_code(end), Some(*code), "{id}: {end:?}");
        assert!(end_message(end).contains(message), "{id}: {end:?}");
    }
    assert_eq!(a.emitter.open_subscriptions(), 0);
    // A command kind serves no subscription, whatever the type (the hub gates it before).
    let mut as_command = forwarded("k#1", "t.Prices", b"{}", true);
    if let Some(server_message::Message::Request(r)) = &mut as_command.message {
        r.kind = proto::RequestKind::Command as i32;
    }
    hub.send_to_clients(as_command).await;
    eventually("the end", async || hub.ends().len() == 7).await;
    assert_eq!(end_code(&hub.ends()[6].1), Some(ErrorCode::NoHandler));
    a.shutdown().await;
}

#[tokio::test]
async fn the_per_instance_limit_refuses_new_subscriptions_with_resource_exhausted() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = node_with(
        &hub.address(),
        "svc-a",
        "a1",
        registry("a1", &probe),
        4,
        SenderSettings::default(),
        Some(2),
    )
    .await;
    for id in ["s1#1", "s2#1", "s3#1"] {
        hub.send_to_clients(subscribed(id, &prices("acme"))).await;
    }
    eventually("the third refused", async || hub.ends().len() == 1).await;
    let (_, end) = &hub.ends()[0];
    assert_eq!(end.request_id, "s3#1");
    assert_eq!(end_code(end), Some(ErrorCode::ResourceExhausted));
    assert_eq!(end_message(end), "subscription limit (2) reached");
    assert_eq!(a.emitter.open_subscriptions(), 2);
    // One ends: there is room again.
    a.emitter.complete::<Prices>(|_| true);
    eventually("both completed", async || hub.ends().len() == 3).await;
    hub.send_to_clients(subscribed("s4#1", &prices("acme")))
        .await;
    eventually("the fourth registered", async || {
        a.emitter.open_subscriptions() == 1
    })
    .await;
    a.shutdown().await;
}

#[tokio::test]
async fn a_hub_cancel_removes_the_subscription_and_nothing_follows() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    eventually("registered", async || a.emitter.open_subscriptions() == 1).await;
    hub.send_to_clients(cancel("s1#1", "max results")).await;
    eventually("removed", async || a.emitter.open_subscriptions() == 0).await;
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
    a.emitter.complete::<Prices>(|_| true);
    // A cancel for an unknown id is ignored.
    hub.send_to_clients(cancel("nobody#1", "whatever")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(hub.parts().is_empty() && hub.ends().is_empty());
    a.shutdown().await;
}

#[tokio::test]
async fn a_lost_hub_stream_removes_the_subscriptions_it_carried() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    hub.send_to_clients(subscribed("s2#1", &prices("acme")))
        .await;
    eventually("registered", async || a.emitter.open_subscriptions() == 2).await;
    hub.drop_streams();
    eventually("removed with the stream", async || {
        a.emitter.open_subscriptions() == 0
    })
    .await;
    eventually("re-registered", async || hub.registers().await.len() == 2).await;
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(hub.parts().is_empty(), "nothing is sent on the new stream");
    a.shutdown().await;
}

#[tokio::test]
async fn a_subscription_holds_no_permit_and_grants_none_back() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let permits_before = hub.permits().len();
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    eventually("registered", async || a.emitter.open_subscriptions() == 1).await;
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
    eventually("the part", async || hub.parts().len() == 1).await;
    assert_eq!(a.dispatcher.available_permits(), 4, "no local permit held");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        hub.permits().len(),
        permits_before,
        "no permit given back for a request that took none"
    );
    // The initial request does take one, and gives it back.
    let payload = serde_json::to_vec(&prices("acme")).unwrap();
    hub.send_to_clients(forwarded("s1#0", "t.Prices", &payload, false))
        .await;
    eventually("the permit back", async || {
        hub.permits().len() == permits_before + 1
    })
    .await;
    a.shutdown().await;
}
