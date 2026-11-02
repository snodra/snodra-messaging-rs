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

use futures::stream::BoxStream;
use kraken_messaging::hub::{
    Dispatcher, HandlerRegistry, HubPool, HubSettings, Inbound, QueryUpdateEmitter,
    RegisteredTypes, SendOptions, Sender, SenderSettings, SubscriptionOptions, rendezvous_score,
};
use kraken_messaging::identity::{Guard, IdentityResolver};
use kraken_messaging::proto::{self, ErrorCode, ServerMessage, server_message};
use kraken_messaging::{
    Command, MessagingError, Query, RetryPolicy, StreamingQuery, SubscriptionQuery, message_type,
};
use serde::{Deserialize, Serialize};
use support::fake_hub::FakeHub;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

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
/// a symbol `fail` fails the initial result, `hang` never answers it), `t.Lookup` (unary).
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
            if q.symbol == "hang" {
                std::future::pending::<()>().await;
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
    node_wired(
        address,
        service,
        instance,
        registry,
        max_handlers,
        sender_settings,
        max_subscriptions,
        Wiring::default(),
    )
    .await
}

/// What a test changes in how a node is wired.
struct Wiring {
    /// Awaited by every subscription request before its identity check (a slow identity).
    admission_hook: Option<kraken_messaging::hub::AdmissionHook>,
    /// Whether a lost hub stream reaches `Dispatcher::stream_lost` (as `KrakenMessaging` does).
    stream_lost: bool,
}

impl Default for Wiring {
    fn default() -> Wiring {
        Wiring {
            admission_hook: None,
            stream_lost: true,
        }
    }
}

/// A door the subscription requests wait at, until it is opened: a slow identity check.
struct Gate(tokio::sync::watch::Sender<bool>);

impl Gate {
    fn closed() -> Gate {
        Gate(tokio::sync::watch::channel(false).0)
    }

    fn open(&self) {
        self.0.send_replace(true);
    }

    /// The hook that holds the requests whose id ends with `#1` (the update targets, not the
    /// `#0` initial request) until the gate opens.
    fn hook(&self) -> kraken_messaging::hub::AdmissionHook {
        let tx = self.0.clone();
        Arc::new(move |id| {
            let mut rx = tx.subscribe();
            let held = id.ends_with("#1");
            Box::pin(async move {
                if held {
                    let _ = rx.wait_for(|open| *open).await;
                }
            })
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn node_wired(
    address: &str,
    service: &str,
    instance: &str,
    registry: HandlerRegistry,
    max_handlers: usize,
    sender_settings: SenderSettings,
    max_subscriptions: Option<usize>,
    wiring: Wiring,
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
    if let Some(hook) = wiring.admission_hook {
        dispatcher = dispatcher.admission_hook(hook);
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
                    if wiring.stream_lost {
                        serve.stream_lost(stream);
                    }
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

/// `ReplyEnd.parts` is the number of parts this subscription sent (the hub's own count of
/// relayed parts is its own business): a clean end, an error end and a shutdown end all carry it.
#[tokio::test]
async fn the_end_of_a_subscription_counts_the_parts_it_sent() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    for id in ["s1#1", "s2#1", "s3#1", "s4#1"] {
        hub.send_to_clients(subscribed(id, &prices(&format!("sym-{id}"))))
            .await;
    }
    eventually("registered", async || a.emitter.open_subscriptions() == 4).await;
    // s1 gets 3 updates, s2 gets 2, s3 gets 1, s4 none.
    for (id, n) in [("s1#1", 3), ("s2#1", 2), ("s3#1", 1)] {
        for price in 0..n {
            a.emitter
                .emit::<Prices>(|q| q.symbol == format!("sym-{id}"), &tick("x", price));
        }
    }
    eventually("six parts", async || hub.parts().len() == 6).await;
    a.emitter.complete::<Prices>(|q| q.symbol == "sym-s1#1");
    a.emitter.complete_exceptionally::<Prices>(
        |q| q.symbol == "sym-s2#1",
        &MessagingError::Storage("gone".into()),
    );
    for pump in a.dispatcher.end_subscriptions() {
        pump.await.unwrap();
    }
    eventually("four ends", async || hub.ends().len() == 4).await;
    let parts_of = |id: &str| {
        hub.ends()
            .into_iter()
            .find(|(_, e)| e.request_id == id)
            .map(|(_, e)| e.parts)
    };
    assert_eq!(parts_of("s1#1"), Some(3), "clean end");
    assert_eq!(parts_of("s2#1"), Some(2), "error end");
    assert_eq!(parts_of("s3#1"), Some(1), "shutdown end");
    assert_eq!(parts_of("s4#1"), Some(0), "no update sent");
    a.shutdown().await;
}

/// A shutdown half-closes the hub stream and waits for the hub to finish it, instead of
/// resetting it: the updates and the `ReplyEnd` the pumps queued just before it reach the hub even
/// when the hub has not read them yet (its HTTP/2 window is full), so the subscriber gets a clean
/// end rather than HANDLER_DISCONNECTED.
#[tokio::test]
async fn a_shutdown_delivers_what_the_subscriptions_queued_before_closing_the_stream() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    eventually("registered", async || a.emitter.open_subscriptions() == 1).await;
    // The hub stops reading: its HTTP/2 stream window (hyper's 2 MiB) fills with the first two
    // updates, the rest and the end wait in the client.
    hub.set_stall_reads(true);
    let big = "x".repeat(1_000_000);
    for price in 0..4 {
        a.emitter.emit::<Prices>(|_| true, &tick(&big, price));
    }
    for pump in a.dispatcher.end_subscriptions() {
        pump.await.unwrap();
    }
    tokio::join!(a.shutdown(), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        hub.set_stall_reads(false);
    });
    eventually("the end reached the hub", async || {
        hub.ends().iter().any(|(_, e)| e.request_id == "s1#1")
    })
    .await;
    assert_eq!(hub.parts().len(), 4, "every update came before the end");
    let ends = hub.ends();
    assert_eq!(ends.len(), 1);
    assert_eq!((end_code(&ends[0].1), ends[0].1.parts), (None, 4));
}

/// The user's filter runs outside the subscriptions' lock: one that panics skips its
/// subscription (the others still get the update) and leaves the instance serving: a later hub
/// `Cancel` and a lost stream are still processed.
#[tokio::test]
async fn a_panicking_filter_skips_its_subscription_and_leaves_the_instance_serving() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    for (id, symbol) in [("s1#1", "boom"), ("s2#1", "acme"), ("s3#1", "acme")] {
        hub.send_to_clients(subscribed(id, &prices(symbol))).await;
    }
    eventually("registered", async || a.emitter.open_subscriptions() == 3).await;
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        a.emitter.emit::<Prices>(
            |q| {
                if q.symbol == "boom" {
                    panic!("filter bug");
                }
                true
            },
            &tick("acme", 1),
        );
    }));
    assert!(outcome.is_ok(), "the emitter contains the filter's panic");
    eventually("the other two get the update", async || {
        hub.parts().len() == 2
    })
    .await;
    assert!(hub.parts().iter().all(|(_, p)| p.request_id != "s1#1"));
    // The lock is usable: a hub Cancel is processed, and so are further emits and a lost stream.
    hub.send_to_clients(cancel("s2#1", "max results")).await;
    eventually("the cancel is processed", async || {
        a.emitter.open_subscriptions() == 2
    })
    .await;
    a.emitter
        .emit::<Prices>(|q| q.symbol == "acme", &tick("acme", 2));
    eventually("a later emit reaches s3", async || hub.parts().len() == 3).await;
    hub.drop_streams();
    eventually("the lost stream is processed", async || {
        a.emitter.open_subscriptions() == 0
    })
    .await;
    a.shutdown().await;
}

/// A filter that calls the emitter again (to emit, or to read the count) does not deadlock: the
/// filters run outside the subscriptions' lock.
#[tokio::test]
async fn a_filter_that_calls_the_emitter_again_completes() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = owner(&hub.address(), "svc-a", "a1", &probe).await;
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    eventually("registered", async || a.emitter.open_subscriptions() == 1).await;
    let emitter = a.emitter.clone();
    let nested = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let call = tokio::task::spawn_blocking(move || {
        let inner = emitter.clone();
        emitter.emit::<Prices>(
            move |_| {
                if !nested.swap(true, Ordering::SeqCst) {
                    assert_eq!(inner.open_subscriptions(), 1);
                    inner.emit::<Prices>(|_| true, &tick("acme", 1));
                }
                true
            },
            &tick("acme", 2),
        );
    });
    tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect("no deadlock")
        .unwrap();
    eventually("both updates sent", async || hub.parts().len() == 2).await;
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

/// A node whose subscription requests wait at `gate` before their identity check.
async fn gated_owner(hub: &FakeHub, gate: &Gate, max_subscriptions: Option<usize>) -> Node {
    node_wired(
        &hub.address(),
        "svc-a",
        "a1",
        registry("a1", &Arc::new(Probe::default())),
        4,
        SenderSettings::default(),
        max_subscriptions,
        Wiring {
            admission_hook: Some(gate.hook()),
            ..Wiring::default()
        },
    )
    .await
}

/// The subscription is registered when its request is read, before its admission (identity,
/// guard) completes: a hub `Cancel` that follows at once finds it, so nothing leaks, no slot is
/// held and nothing is sent.
#[tokio::test]
async fn a_cancel_before_admission_completes_leaves_nothing_open_and_sends_nothing() {
    let hub = FakeHub::start().await;
    let gate = Gate::closed();
    let a = gated_owner(&hub, &gate, Some(1)).await;
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    eventually("registered while pending", async || {
        a.emitter.open_subscriptions() == 1
    })
    .await;
    hub.send_to_clients(cancel("s1#1", "max results")).await;
    eventually("removed by the cancel", async || {
        a.emitter.open_subscriptions() == 0
    })
    .await;
    gate.open();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(a.emitter.open_subscriptions(), 0, "not registered late");
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
    assert!(hub.parts().is_empty() && hub.ends().is_empty());
    // No slot is held (the limit is 1): another subscription registers.
    hub.send_to_clients(subscribed("s2#1", &prices("acme")))
        .await;
    eventually("the next one registered", async || {
        a.emitter.open_subscriptions() == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(hub.ends().is_empty(), "not refused for a leaked slot");
    a.shutdown().await;
}

#[tokio::test]
async fn a_lost_stream_during_admission_leaves_nothing_open() {
    let hub = FakeHub::start().await;
    let gate = Gate::closed();
    let a = gated_owner(&hub, &gate, None).await;
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    eventually("registered while pending", async || {
        a.emitter.open_subscriptions() == 1
    })
    .await;
    hub.drop_streams();
    eventually("removed with the stream", async || {
        a.emitter.open_subscriptions() == 0
    })
    .await;
    gate.open();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(a.emitter.open_subscriptions(), 0, "not registered late");
    assert!(hub.parts().is_empty() && hub.ends().is_empty());
    a.shutdown().await;
}

/// The hub writes `<id>#n` before `<id>#0` to the same instance: an update emitted after the
/// initial result was answered, while the subscription is still being admitted, is held and
/// delivered once it is, after the initial result and in order.
#[tokio::test]
async fn an_update_emitted_during_admission_is_delivered_after_the_initial_result() {
    let hub = FakeHub::start().await;
    let gate = Gate::closed();
    let a = gated_owner(&hub, &gate, None).await;
    let payload = serde_json::to_vec(&prices("acme")).unwrap();
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    hub.send_to_clients(forwarded("s1#0", "t.Prices", &payload, false))
        .await;
    eventually("the initial result answered", async || {
        hub.replies().await.len() == 1
    })
    .await;
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 2));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(hub.parts().is_empty(), "held while the admission runs");
    gate.open();
    eventually("both delivered", async || hub.parts().len() == 2).await;
    let parts = hub.parts();
    for (n, (_, part)) in parts.iter().enumerate() {
        assert_eq!((part.request_id.as_str(), part.seq), ("s1#1", n as u64 + 1));
        let update: Tick = serde_json::from_slice(&part.payload).unwrap();
        assert_eq!(update.price, n as u32 + 1, "in emit order");
    }
    // Later emits follow.
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 3));
    eventually("the later one", async || hub.parts().len() == 3).await;
    assert_eq!(hub.parts()[2].1.seq, 3);
    a.shutdown().await;
}

/// A refused admission drops what was held: the subscriber gets the refusal only.
#[tokio::test]
async fn a_refused_admission_drops_the_held_updates_and_sends_the_refusal_only() {
    let hub = FakeHub::start().await;
    let gate = Gate::closed();
    let a = gated_owner(&hub, &gate, None).await;
    let mut request = subscribed("s1#1", &prices("acme"));
    if let Some(server_message::Message::Request(r)) = &mut request.message {
        r.headers
            .insert("authorization".into(), "not a bearer token".into());
    }
    hub.send_to_clients(request).await;
    eventually("registered while pending", async || {
        a.emitter.open_subscriptions() == 1
    })
    .await;
    a.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
    gate.open();
    eventually("the refusal", async || hub.ends().len() == 1).await;
    let (_, end) = &hub.ends()[0];
    assert_eq!(end.request_id, "s1#1");
    assert_eq!(end_code(end), Some(ErrorCode::Unauthenticated));
    assert_eq!(end.parts, 0);
    assert_eq!(a.emitter.open_subscriptions(), 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(hub.parts().is_empty(), "the held update was dropped");
    a.shutdown().await;
}

/// A pump whose write fails (its stream is gone and nobody told the dispatcher) removes its own
/// entry: it would otherwise hold its slot for ever.
#[tokio::test]
async fn a_pump_whose_write_fails_removes_its_entry() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a = node_wired(
        &hub.address(),
        "svc-a",
        "a1",
        registry("a1", &probe),
        4,
        SenderSettings::default(),
        None,
        Wiring {
            stream_lost: false,
            ..Wiring::default()
        },
    )
    .await;
    hub.send_to_clients(subscribed("s1#1", &prices("acme")))
        .await;
    eventually("registered", async || a.emitter.open_subscriptions() == 1).await;
    hub.drop_streams();
    eventually("reconnected", async || hub.registers().await.len() == 2).await;
    assert_eq!(
        a.emitter.open_subscriptions(),
        1,
        "nothing removed it with the stream"
    );
    eventually("the failed write removed it", async || {
        a.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
        a.emitter.open_subscriptions() == 0
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

// Sender side: `Sender::subscribe` against owners served by the dispatcher.

/// Subscription requests the hub received from senders.
async fn subscriptions(hub: &FakeHub) -> Vec<proto::Request> {
    hub.requests()
        .await
        .into_iter()
        .filter(|r| r.mode == proto::RequestMode::Subscription as i32)
        .collect()
}

type Updates = BoxStream<'static, Result<Tick, MessagingError>>;

/// The next update within 5 s.
async fn next_update(updates: &mut Updates) -> Result<Tick, MessagingError> {
    tokio::time::timeout(Duration::from_secs(5), updates.next())
        .await
        .expect("an update within 5 s")
        .expect("the subscription is open")
}

/// The updates until the end or the first error (bounded by 5 s).
async fn drain(updates: &mut Updates) -> (Vec<u32>, Option<MessagingError>) {
    let mut prices = vec![];
    loop {
        match tokio::time::timeout(Duration::from_secs(5), updates.next())
            .await
            .expect("the end within 5 s")
        {
            Some(Ok(tick)) => prices.push(tick.price),
            Some(Err(e)) => return (prices, Some(e)),
            None => return (prices, None),
        }
    }
}

/// The updates until a clean end: exactly `expected`, no error.
async fn ends_cleanly(updates: &mut Updates, expected: Vec<u32>) {
    let (prices, error) = drain(updates).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(prices, expected);
}

/// Two instances of `svc-a` owning `t.Prices` and a caller.
async fn two_owners_and_a_caller(hub: &FakeHub, probe: &Arc<Probe>) -> (Node, Node, Node) {
    let a1 = owner(&hub.address(), "svc-a", "a1", probe).await;
    let a2 = owner(&hub.address(), "svc-a", "a2", probe).await;
    let c = caller(&hub.address()).await;
    (a1, a2, c)
}

#[tokio::test]
async fn a_subscription_yields_the_initial_then_the_updates_of_every_owner_instance_then_ends() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let (a1, a2, c) = two_owners_and_a_caller(&hub, &probe).await;
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    assert_eq!(sub.initial.symbol, "acme");
    assert!(
        ["a1", "a2"].contains(&sub.initial.instance.as_str()),
        "{:?}",
        sub.initial
    );
    assert_eq!(
        probe.initials.load(Ordering::SeqCst),
        1,
        "one initial request"
    );
    eventually("both instances subscribed", async || {
        a1.emitter.open_subscriptions() == 1 && a2.emitter.open_subscriptions() == 1
    })
    .await;
    let request = &subscriptions(&hub).await[0];
    assert_eq!(
        (
            request.initial_credit,
            request.max_results,
            request.timeout_ms,
            request.priority
        ),
        (64, 0, 0, 0)
    );
    assert_eq!(c.sender.in_flight(), 1, "pending until the end");

    let mut updates = sub.updates;
    a1.emitter
        .emit::<Prices>(|q| q.symbol == "acme", &tick("acme", 1));
    a2.emitter
        .emit::<Prices>(|q| q.symbol == "acme", &tick("acme", 2));
    let mut got = vec![
        next_update(&mut updates).await.unwrap().price,
        next_update(&mut updates).await.unwrap().price,
    ];
    got.sort();
    assert_eq!(got, [1, 2], "one update from each instance");
    // One instance completes: the subscription lives on with the other.
    a1.emitter.complete::<Prices>(|_| true);
    eventually("a1's target removed", async || {
        hub.subscription_targets_open() == 1
    })
    .await;
    a2.emitter.emit::<Prices>(|_| true, &tick("acme", 3));
    assert_eq!(next_update(&mut updates).await.unwrap().price, 3);
    // The last one completes: the subscription ends cleanly.
    a2.emitter.complete::<Prices>(|_| true);
    ends_cleanly(&mut updates, vec![]).await;
    assert_eq!(hub.open_subscriptions(), 0);
    assert_eq!(c.sender.in_flight(), 0);
    assert!(
        hub.cancels().is_empty(),
        "nothing to cancel after a clean end"
    );
    for n in [a1, a2, c] {
        n.shutdown().await;
    }
}

#[tokio::test]
async fn options_become_the_request_fields() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let c = caller(&hub.address()).await;
    let sub = c
        .sender
        .subscribe(
            &prices("acme"),
            SubscriptionOptions {
                timeout: Some(Duration::from_secs(2)),
                max_results: 5,
                priority: Some(7),
                initial_credit: 3,
            },
        )
        .await
        .unwrap();
    let request = &subscriptions(&hub).await[0];
    assert_eq!(
        (
            request.timeout_ms,
            request.max_results,
            request.priority,
            request.initial_credit
        ),
        (2_000, 5, 7, 3)
    );
    assert_eq!(request.kind, proto::RequestKind::Query as i32);
    assert_eq!(request.r#type, "t.Prices");
    assert_eq!(
        serde_json::from_slice::<Prices>(&request.payload).unwrap(),
        prices("acme")
    );
    // 0 reads as 64, as on the hub.
    sub.cancel();
    let sub = c
        .sender
        .subscribe(
            &prices("acme"),
            SubscriptionOptions {
                initial_credit: 0,
                ..SubscriptionOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(subscriptions(&hub).await[1].initial_credit, 64);
    drop(sub);
    a1.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn the_emitter_filter_selects_the_subscriptions_by_their_query() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let c = caller(&hub.address()).await;
    let acme = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    let other = c
        .sender
        .subscribe(&prices("other"), SubscriptionOptions::default())
        .await
        .unwrap();
    eventually("both subscribed", async || {
        a1.emitter.open_subscriptions() == 2
    })
    .await;
    let (mut acme_updates, mut other_updates) = (acme.updates, other.updates);
    a1.emitter
        .emit::<Prices>(|q| q.symbol == "other", &tick("other", 9));
    a1.emitter
        .emit::<Prices>(|q| q.symbol == "acme", &tick("acme", 1));
    assert_eq!(next_update(&mut acme_updates).await.unwrap().price, 1);
    assert_eq!(next_update(&mut other_updates).await.unwrap().price, 9);
    a1.emitter.complete::<Prices>(|q| q.symbol == "acme");
    ends_cleanly(&mut acme_updates, vec![]).await;
    a1.emitter.emit::<Prices>(|_| true, &tick("other", 10));
    assert_eq!(next_update(&mut other_updates).await.unwrap().price, 10);
    assert_eq!(hub.open_subscriptions(), 1);
    drop(other_updates);
    a1.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn complete_exceptionally_fails_the_subscription_and_cancels_the_other_targets() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let (a1, a2, c) = two_owners_and_a_caller(&hub, &probe).await;
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    eventually("both subscribed", async || {
        a1.emitter.open_subscriptions() == 1 && a2.emitter.open_subscriptions() == 1
    })
    .await;
    let mut updates = sub.updates;
    a1.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
    assert_eq!(next_update(&mut updates).await.unwrap().price, 1);
    a1.emitter
        .complete_exceptionally::<Prices>(|_| true, &MessagingError::Storage("feed down".into()));
    let (rest, error) = drain(&mut updates).await;
    assert!(rest.is_empty());
    assert!(
        matches!(error, Some(MessagingError::RemoteHandler { ref type_name, ref message, .. }) if type_name == "t.Prices" && message.contains("feed down")),
        "{error:?}"
    );
    eventually("a2's target cancelled by the hub", async || {
        a2.emitter.open_subscriptions() == 0
    })
    .await;
    assert_eq!(hub.open_subscriptions(), 0);
    assert_eq!(c.sender.in_flight(), 0);
    for n in [a1, a2, c] {
        n.shutdown().await;
    }
}

#[tokio::test]
async fn updates_beyond_the_credit_wait_at_the_hub_until_the_consumer_pulls_half_the_window() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let c = caller(&hub.address()).await;
    // Window 4: the initial part takes one credit, three updates may follow at once.
    let sub = c
        .sender
        .subscribe(
            &prices("acme"),
            SubscriptionOptions {
                initial_credit: 4,
                ..SubscriptionOptions::default()
            },
        )
        .await
        .unwrap();
    let id = subscriptions(&hub).await[0].id.clone();
    eventually("subscribed", async || a1.emitter.open_subscriptions() == 1).await;
    for price in 1..=5 {
        a1.emitter.emit::<Prices>(|_| true, &tick("acme", price));
    }
    eventually("two updates wait for credit", async || {
        hub.subscription_buffered(&id) == Some(2)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(hub.flows().is_empty(), "nothing consumed yet: no refill");
    let mut updates = sub.updates;
    // The initial part counted as consumed; the first update pulled makes half the window.
    assert_eq!(next_update(&mut updates).await.unwrap().price, 1);
    eventually("a refill of half the window", async || {
        hub.flows()
            .iter()
            .any(|(_, f)| f.request_id == id && f.credits == 2)
    })
    .await;
    eventually("the buffer drained", async || {
        hub.subscription_buffered(&id) == Some(0)
    })
    .await;
    assert_eq!(next_update(&mut updates).await.unwrap().price, 2);
    assert_eq!(next_update(&mut updates).await.unwrap().price, 3);
    assert_eq!(next_update(&mut updates).await.unwrap().price, 4);
    assert_eq!(next_update(&mut updates).await.unwrap().price, 5);
    eventually("a second refill", async || {
        hub.flows()
            .iter()
            .filter(|(_, f)| f.request_id == id && f.credits == 2)
            .count()
            >= 2
    })
    .await;
    drop(updates);
    a1.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_consumer_further_behind_than_the_hubs_buffer_loses_the_subscription_as_handler_busy() {
    let hub = FakeHub::start().await;
    hub.set_subscription_buffer(4);
    let probe = Arc::new(Probe::default());
    let (a1, a2, c) = two_owners_and_a_caller(&hub, &probe).await;
    // Credit 1: the initial part takes it; consuming it refills 1 (the window's half, at least
    // one), so at most one update is relayed and the next ones wait at the hub, up to 4.
    let sub = c
        .sender
        .subscribe(
            &prices("acme"),
            SubscriptionOptions {
                initial_credit: 1,
                ..SubscriptionOptions::default()
            },
        )
        .await
        .unwrap();
    eventually("both subscribed", async || {
        a1.emitter.open_subscriptions() == 1 && a2.emitter.open_subscriptions() == 1
    })
    .await;
    for price in 1..=12 {
        a1.emitter.emit::<Prices>(|_| true, &tick("acme", price));
    }
    let mut updates = sub.updates;
    let (got, error) = drain(&mut updates).await;
    assert!(
        got.len() <= 1,
        "the buffered updates are dropped with it: {got:?}"
    );
    assert!(
        matches!(error, Some(MessagingError::HandlerBusy(ref t)) if t == "t.Prices"),
        "{error:?}"
    );
    eventually("every target cancelled", async || {
        a1.emitter.open_subscriptions() == 0 && a2.emitter.open_subscriptions() == 0
    })
    .await;
    assert_eq!(hub.open_subscriptions(), 0);
    assert_eq!(c.sender.in_flight(), 0);
    for n in [a1, a2, c] {
        n.shutdown().await;
    }
}

#[tokio::test]
async fn max_results_counts_updates_and_ends_the_subscription_from_the_hub() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let (a1, a2, c) = two_owners_and_a_caller(&hub, &probe).await;
    let sub = c
        .sender
        .subscribe(
            &prices("acme"),
            SubscriptionOptions {
                max_results: 2,
                ..SubscriptionOptions::default()
            },
        )
        .await
        .unwrap();
    eventually("both subscribed", async || {
        a1.emitter.open_subscriptions() == 1 && a2.emitter.open_subscriptions() == 1
    })
    .await;
    for price in 1..=3 {
        a1.emitter.emit::<Prices>(|_| true, &tick("acme", price));
    }
    let mut updates = sub.updates;
    ends_cleanly(&mut updates, vec![1, 2]).await;
    eventually("every target cancelled at max results", async || {
        a1.emitter.open_subscriptions() == 0 && a2.emitter.open_subscriptions() == 0
    })
    .await;
    assert_eq!(hub.open_subscriptions(), 0);
    for n in [a1, a2, c] {
        n.shutdown().await;
    }
}

#[tokio::test]
async fn cancelling_or_dropping_the_subscription_cancels_it_at_the_hub_and_at_every_target() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let (a1, a2, c) = two_owners_and_a_caller(&hub, &probe).await;
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    eventually("both subscribed", async || {
        a1.emitter.open_subscriptions() == 1 && a2.emitter.open_subscriptions() == 1
    })
    .await;
    let id = subscriptions(&hub).await[0].id.clone();
    sub.cancel();
    eventually("cancelled at the hub", async || {
        hub.cancels().iter().any(|(_, c)| c.request_id == id)
    })
    .await;
    let (_, cancel) = hub
        .cancels()
        .into_iter()
        .find(|(_, c)| c.request_id == id)
        .unwrap();
    assert_eq!(cancel.reason, "dropped by the sender");
    eventually("every target cancelled", async || {
        a1.emitter.open_subscriptions() == 0 && a2.emitter.open_subscriptions() == 0
    })
    .await;
    assert_eq!(hub.open_subscriptions(), 0);
    assert_eq!(c.sender.in_flight(), 0);
    // Dropping the subscription (its updates) does the same.
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    eventually("both subscribed again", async || {
        a1.emitter.open_subscriptions() == 1 && a2.emitter.open_subscriptions() == 1
    })
    .await;
    drop(sub);
    eventually("cancelled at the hub and at every target", async || {
        hub.cancels().len() == 2
            && a1.emitter.open_subscriptions() == 0
            && a2.emitter.open_subscriptions() == 0
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    // Not after a clean end: nothing to cancel.
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    eventually("both subscribed once more", async || {
        a1.emitter.open_subscriptions() == 1 && a2.emitter.open_subscriptions() == 1
    })
    .await;
    a1.emitter.complete::<Prices>(|_| true);
    a2.emitter.complete::<Prices>(|_| true);
    let mut updates = sub.updates;
    ends_cleanly(&mut updates, vec![]).await;
    drop(updates);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(hub.cancels().len(), 2);
    for n in [a1, a2, c] {
        n.shutdown().await;
    }
}

#[tokio::test]
async fn a_target_leaving_is_silent_unless_it_was_the_last_one() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let (a1, a2, c) = two_owners_and_a_caller(&hub, &probe).await;
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    eventually("both subscribed", async || {
        a1.emitter.open_subscriptions() == 1 && a2.emitter.open_subscriptions() == 1
    })
    .await;
    let mut updates = sub.updates;
    a1.shutdown().await;
    eventually("one target left", async || {
        hub.subscription_targets_open() == 1
    })
    .await;
    a2.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
    assert_eq!(next_update(&mut updates).await.unwrap().price, 1);
    a2.shutdown().await;
    let (rest, error) = drain(&mut updates).await;
    assert!(rest.is_empty());
    assert!(
        matches!(error, Some(MessagingError::HandlerDisconnected(ref t)) if t == "t.Prices"),
        "{error:?}"
    );
    assert_eq!(c.sender.in_flight(), 0);
    c.shutdown().await;
}

#[tokio::test]
async fn failures_of_the_initial_result_are_the_calls_error_and_cancel_the_targets() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let (a1, a2, c) = two_owners_and_a_caller(&hub, &probe).await;
    let e = c
        .sender
        .subscribe(&prices("fail"), SubscriptionOptions::default())
        .await
        .err()
        .expect("the handler's error");
    assert!(
        matches!(e, MessagingError::RemoteHandler { ref message, .. } if message.contains("quotes gone")),
        "{e:?}"
    );
    eventually("every target cancelled", async || {
        a1.emitter.open_subscriptions() == 0 && a2.emitter.open_subscriptions() == 0
    })
    .await;
    assert_eq!(hub.open_subscriptions(), 0);
    // No initial result within the timeout: the hub's TIMEOUT.
    let e = c
        .sender
        .subscribe(
            &prices("hang"),
            SubscriptionOptions {
                timeout: Some(Duration::from_millis(300)),
                ..SubscriptionOptions::default()
            },
        )
        .await
        .err()
        .expect("timed out");
    assert!(
        matches!(e, MessagingError::Timeout(ref t) if t == "t.Prices"),
        "{e:?}"
    );
    eventually("every target cancelled again", async || {
        a1.emitter.open_subscriptions() == 0 && a2.emitter.open_subscriptions() == 0
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    for n in [a1, a2, c] {
        n.shutdown().await;
    }
}

#[tokio::test]
async fn the_max_subscription_duration_ends_it_with_cancelled() {
    let hub = FakeHub::start().await;
    hub.set_subscription_max_duration(Some(Duration::from_millis(300)));
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let c = caller(&hub.address()).await;
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    let mut updates = sub.updates;
    let (_, error) = drain(&mut updates).await;
    assert!(
        matches!(error, Some(MessagingError::Cancelled(ref m)) if m.contains("max subscription duration")),
        "{error:?}"
    );
    eventually("the target cancelled", async || {
        a1.emitter.open_subscriptions() == 0
    })
    .await;
    a1.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_subscription_to_a_type_whose_handler_answers_another_mode_is_invalid_request() {
    let hub = FakeHub::start().await;
    let mut r = HandlerRegistry::new();
    r.query::<PricesOnce, _, _>(|_, _| async move {
        Ok(Quote {
            symbol: "acme".into(),
            price: 1,
            instance: "u1".into(),
        })
    });
    let unary = node_with(
        &hub.address(),
        "svc-u",
        "u1",
        r,
        4,
        SenderSettings::default(),
        None,
    )
    .await;
    let c = caller(&hub.address()).await;
    // The target's INVALID_REQUEST end and the initial result (the unary handler answers the
    // `<id>#0` request) race: the error is the call's, or the first item of the updates.
    let error = match c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
    {
        Err(e) => e,
        Ok(sub) => {
            let mut updates = sub.updates;
            drain(&mut updates).await.1.expect("the error end")
        }
    };
    assert!(
        matches!(error, MessagingError::InvalidMessage(ref m) if m.contains("not a subscription query handler")),
        "{error:?}"
    );
    assert_eq!(hub.open_subscriptions(), 0);
    assert_eq!(c.sender.in_flight(), 0);
    // The other way round (a unary query to a subscription type) is answered by the handler's
    // library: see `the_initial_request_is_answered_by_the_handler_like_a_unary_query`.
    unary.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_hub_without_the_capability_is_unsupported_and_gets_nothing() {
    for legacy in [false, true] {
        let hub = FakeHub::start().await;
        if legacy {
            hub.set_ack(None);
        } else {
            hub.set_subscription(false);
        }
        let c = caller(&hub.address()).await;
        assert!(!c.pool.handshake_for("").unwrap().subscription());
        let e = c
            .sender
            .subscribe(&prices("acme"), SubscriptionOptions::default())
            .await
            .err()
            .expect("unsupported");
        assert!(
            matches!(e, MessagingError::UnsupportedByHub(ref m) if m.contains("subscription")),
            "legacy {legacy}: {e:?}"
        );
        assert!(hub.requests().await.is_empty(), "nothing is sent");
        assert_eq!(c.sender.in_flight(), 0);
        c.shutdown().await;
    }
}

#[tokio::test]
async fn gating_errors_map_to_the_unary_errors() {
    let hub = FakeHub::start().await;
    let c = caller(&hub.address()).await;
    // No owner.
    let e = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .err()
        .unwrap();
    assert!(
        matches!(e, MessagingError::NoHandler(ref t) if t == "t.Prices"),
        "{e:?}"
    );
    // An owner whose session did not negotiate subscription (an older library).
    hub.set_subscription(false);
    let probe = Arc::new(Probe::default());
    let old = owner(&hub.address(), "svc-old", "o1", &probe).await;
    hub.set_subscription(true);
    let e = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .err()
        .unwrap();
    assert!(
        matches!(e, MessagingError::UnsupportedByHub(ref m) if m.contains("svc-old")),
        "{e:?}"
    );
    assert_eq!(probe.initials.load(Ordering::SeqCst), 0);
    old.shutdown().await;
    // The type is a command.
    let owner = node_with(
        &hub.address(),
        "svc-cmd",
        "x1",
        {
            let mut r = HandlerRegistry::new();
            r.command::<Fill, _, _>(|_, _| async move { Ok(()) });
            r
        },
        2,
        SenderSettings::default(),
        None,
    )
    .await;
    #[derive(Serialize, Deserialize)]
    struct AsSubscription {}
    message_type!(AsSubscription, "t.Fill");
    impl SubscriptionQuery for AsSubscription {
        type Initial = ();
        type Update = ();
    }
    let e = c
        .sender
        .subscribe(&AsSubscription {}, SubscriptionOptions::default())
        .await
        .err()
        .unwrap();
    assert!(matches!(e, MessagingError::InvalidMessage(_)), "{e:?}");
    // The hub stopped sharing the capability after the handshake: it answers UNSUPPORTED.
    hub.set_subscription(false);
    let e = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .err()
        .unwrap();
    assert!(matches!(e, MessagingError::UnsupportedByHub(_)), "{e:?}");
    assert_eq!(c.sender.in_flight(), 0);
    owner.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn the_retry_policy_applies_until_the_initial_part_only() {
    let hub = FakeHub::start().await;
    let c = caller_with(
        &hub.address(),
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
    // No owner yet: NO_HANDLER is retried until one registers.
    let sender = Arc::clone(&c.sender);
    let call = tokio::spawn(async move {
        sender
            .subscribe(&prices("acme"), SubscriptionOptions::default())
            .await
    });
    eventually("a few attempts", async || {
        subscriptions(&hub).await.len() >= 2
    })
    .await;
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let sub = call
        .await
        .unwrap()
        .expect("subscribed once an owner is there");
    assert_eq!(sub.initial.instance, "a1");
    let attempts = subscriptions(&hub).await;
    assert!(attempts.len() >= 2, "{}", attempts.len());
    let mut ids: Vec<&String> = attempts.iter().map(|r| &r.id).collect();
    ids.dedup();
    assert_eq!(ids.len(), attempts.len(), "a fresh id per attempt");
    // The initial request's timeout is a deadline, as a unary request's: the first attempt
    // sends the chosen one, each retry what is left of the budget.
    let timeouts: Vec<u32> = attempts.iter().map(|r| r.timeout_ms).collect();
    assert!((4_900..=5_000).contains(&timeouts[0]), "{timeouts:?}");
    assert!(
        timeouts.windows(2).all(|w| w[1] < w[0]) && *timeouts.last().unwrap() >= 1,
        "{timeouts:?}"
    );
    // After the initial part nothing is retried: the owner leaving ends the subscription.
    let before = attempts.len();
    let mut updates = sub.updates;
    a1.shutdown().await;
    let (_, error) = drain(&mut updates).await;
    assert!(
        matches!(error, Some(MessagingError::HandlerDisconnected(_))),
        "{error:?}"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(subscriptions(&hub).await.len(), before, "no new attempt");
    assert_eq!(c.sender.in_flight(), 0);
    c.shutdown().await;
}

#[tokio::test]
async fn a_gap_in_seq_fails_the_subscription_as_lost_parts_and_cancels_it() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let c = caller(&hub.address()).await;
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    let id = subscriptions(&hub).await[0].id.clone();
    // A part the hub numbered 5 after the initial (seq 1): three were lost in between.
    hub.send_to_clients(ServerMessage {
        message: Some(server_message::Message::Part(proto::ReplyPart {
            request_id: id.clone(),
            payload: serde_json::to_vec(&tick("acme", 5)).unwrap(),
            seq: 5,
            ..Default::default()
        })),
    })
    .await;
    let mut updates = sub.updates;
    let (got, error) = drain(&mut updates).await;
    assert!(got.is_empty());
    assert!(
        matches!(error, Some(MessagingError::InvalidMessage(ref m)) if m == "subscription t.Prices lost 3 part(s)"),
        "{error:?}"
    );
    eventually("cancelled at the hub, reason lost parts", async || {
        hub.cancels()
            .iter()
            .any(|(_, c)| c.request_id == id && c.reason == "lost parts")
    })
    .await;
    eventually("the target cancelled", async || {
        a1.emitter.open_subscriptions() == 0
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    a1.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_shared_subscription_type_subscribes_every_capable_instance_of_every_owner() {
    let hub = FakeHub::start().await;
    let shared = |instance: &'static str| {
        let mut r = HandlerRegistry::new();
        r.shared_subscription_query::<SharedPrices, _, _>(move |q, _| async move {
            Ok(Quote {
                symbol: q.symbol,
                price: 1,
                instance: instance.into(),
            })
        });
        r
    };
    let a = node_with(
        &hub.address(),
        "svc-a",
        "a1",
        shared("a1"),
        4,
        SenderSettings::default(),
        None,
    )
    .await;
    let b = node_with(
        &hub.address(),
        "svc-b",
        "b1",
        shared("b1"),
        4,
        SenderSettings::default(),
        None,
    )
    .await;
    let c = caller(&hub.address()).await;
    let registers = hub.registers().await;
    let b1 = registers.iter().find(|r| r.instance_id == "b1").unwrap();
    assert_eq!(b1.shared_query_types, ["t.SharedPrices"]);
    assert!(b1.query_types.is_empty());
    let mut initials = vec![];
    for _ in 0..2 {
        let sub = c
            .sender
            .subscribe(
                &SharedPrices {
                    symbol: "acme".into(),
                },
                SubscriptionOptions::default(),
            )
            .await
            .unwrap();
        initials.push(sub.initial.instance.clone());
        eventually("both owners subscribed", async || {
            a.emitter.open_subscriptions() == 1 && b.emitter.open_subscriptions() == 1
        })
        .await;
        let mut updates = sub.updates;
        a.emitter.emit::<SharedPrices>(|_| true, &tick("acme", 1));
        b.emitter.emit::<SharedPrices>(|_| true, &tick("acme", 2));
        let mut got = vec![
            next_update(&mut updates).await.unwrap().price,
            next_update(&mut updates).await.unwrap().price,
        ];
        got.sort();
        assert_eq!(got, [1, 2], "an update from each owner");
        a.emitter.complete::<SharedPrices>(|_| true);
        b.emitter.complete::<SharedPrices>(|_| true);
        ends_cleanly(&mut updates, vec![]).await;
    }
    initials.sort();
    assert_eq!(
        initials,
        ["a1", "b1"],
        "the initial result round-robins over the targets"
    );
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_pool_subscribes_through_capable_hubs_only() {
    // hub-3 is a P4b hub: every subscription goes to hub-4.
    let (h3, h4) = (
        FakeHub::start_with_id("hub-3").await,
        FakeHub::start_with_id("hub-4").await,
    );
    h3.set_subscription(false);
    let both = format!("{},{}", h3.address(), h4.address());
    let probe = Arc::new(Probe::default());
    let a1 = owner(&both, "svc-a", "a1", &probe).await;
    let c = caller(&both).await;
    for _ in 0..4 {
        let sub = c
            .sender
            .subscribe(&prices("acme"), SubscriptionOptions::default())
            .await
            .unwrap();
        assert_eq!(sub.initial.instance, "a1");
        eventually("subscribed", async || a1.emitter.open_subscriptions() == 1).await;
        a1.emitter.complete::<Prices>(|_| true);
        let mut updates = sub.updates;
        ends_cleanly(&mut updates, vec![]).await;
    }
    assert_eq!(
        (
            subscriptions(&h3).await.len(),
            subscriptions(&h4).await.len()
        ),
        (0, 4)
    );
    a1.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_subscription_is_not_written_to_a_hub_that_lost_the_capability_after_ranking() {
    let (h1, h2) = (
        FakeHub::start_with_id("hub-1").await,
        FakeHub::start_with_id("hub-2").await,
    );
    let both = format!("{},{}", h1.address(), h2.address());
    let c = caller(&both).await;
    assert_eq!(c.pool.hub_ids(), ["hub-1", "hub-2"]);
    h1.set_stall_reads(true);
    let key_on_1 = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| rendezvous_score("hub-1", k) > rendezvous_score("hub-2", k))
        .unwrap();
    let fillers: Vec<_> = (0..324)
        .map(|i| {
            let sender = Arc::clone(&c.sender);
            let opts = SendOptions {
                routing_key: Some(key_on_1.clone()),
                retry: Some(RetryPolicy::NONE),
                timeout: Some(Duration::from_secs(30)),
                ..SendOptions::default()
            };
            let size = if i < 24 { 256 * 1024 } else { 1 };
            tokio::spawn(async move {
                let fill = Fill {
                    data: "x".repeat(size),
                };
                sender.send_command(&fill, opts).await
            })
        })
        .collect();
    eventually("every filler handed to hub-1's stream", async || {
        c.sender.in_flight() == fillers.len()
    })
    .await;
    for filler in &fillers {
        filler.abort();
    }
    for filler in fillers {
        let _ = filler.await;
    }
    // Subscriptions go round-robin: the one ranked [hub-1, hub-2] waits in the write to hub-1
    // (the one ranked the other way round ends at once with NO_HANDLER on hub-2).
    let mut probes = 0;
    let waiting = loop {
        probes += 1;
        assert!(probes <= 3, "no subscription went to hub-1 first");
        let sender = Arc::clone(&c.sender);
        let call = tokio::spawn(async move {
            sender
                .subscribe(&prices("acme"), SubscriptionOptions::default())
                .await
                .map(|_| ())
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        if !call.is_finished() {
            break call;
        }
        let e = call.await.unwrap().unwrap_err();
        assert!(matches!(e, MessagingError::NoHandler(_)), "{e:?}");
    };
    assert!(
        subscriptions(&h2).await.len() < probes,
        "the waiting subscription reached no hub"
    );
    h2.set_subscription(false);
    h2.drop_streams();
    eventually("hub-2 re-handshaken without the capability", async || {
        c.pool.connections().iter().any(|conn| {
            conn.address() == h2.address() && conn.handshake().is_some_and(|h| !h.subscription())
        })
    })
    .await;
    let on_h2_before = subscriptions(&h2).await.len();
    h1.drop_streams();
    let e = tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .expect("the open ends at once, not at the local guard")
        .unwrap()
        .unwrap_err();
    assert!(matches!(e, MessagingError::UnsupportedByHub(_)), "{e:?}");
    assert_eq!(
        subscriptions(&h2).await.len(),
        on_h2_before,
        "the subscription was not written to hub-2's stream"
    );
    assert_eq!(c.sender.in_flight(), 0);
    c.shutdown().await;
}

/// The hub may drop a part it could not buffer: the sender checks `seq` and the end's count, as
/// for a stream. The end counts parts that never arrived, so it is `InvalidMessage`, not clean.
#[tokio::test]
async fn an_end_counting_more_parts_than_arrived_fails_the_subscription_as_lost_parts() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let c = caller(&hub.address()).await;
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    let id = subscriptions(&hub).await[0].id.clone();
    // The initial part (seq 1) arrived; the hub counted 3 parts before this clean end.
    hub.send_to_clients(ServerMessage {
        message: Some(server_message::Message::End(proto::ReplyEnd {
            request_id: id,
            error: None,
            parts: 3,
            ..Default::default()
        })),
    })
    .await;
    let mut updates = sub.updates;
    let (got, error) = drain(&mut updates).await;
    assert!(got.is_empty());
    assert!(
        matches!(&error, Some(MessagingError::InvalidMessage(m)) if m == "subscription t.Prices lost 2 part(s)"),
        "{error:?}"
    );
    assert_eq!(c.sender.in_flight(), 0);
    a1.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn an_undecodable_update_fails_the_subscription_and_cancels_it() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let c = caller(&hub.address()).await;
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    let id = subscriptions(&hub).await[0].id.clone();
    hub.send_to_clients(ServerMessage {
        message: Some(server_message::Message::Part(proto::ReplyPart {
            request_id: id.clone(),
            payload: b"not a tick".to_vec(),
            seq: 2,
            ..Default::default()
        })),
    })
    .await;
    let mut updates = sub.updates;
    let e = updates.next().await.unwrap().unwrap_err();
    assert!(matches!(e, MessagingError::Serialization(_)), "{e:?}");
    assert!(updates.next().await.is_none());
    eventually(
        "cancelled at the hub, reason undecodable part",
        async || {
            hub.cancels()
                .iter()
                .any(|(_, c)| c.request_id == id && c.reason == "undecodable part")
        },
    )
    .await;
    eventually("the target cancelled", async || {
        a1.emitter.open_subscriptions() == 0
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    a1.shutdown().await;
    c.shutdown().await;
}

/// A shutdown completes every subscription of the instance with a clean `ReplyEnd` (a target
/// that just vanished would end the last one with `HANDLER_DISCONNECTED`).
#[tokio::test]
async fn ending_the_subscriptions_at_shutdown_completes_them_cleanly() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let c = caller(&hub.address()).await;
    let sub = c
        .sender
        .subscribe(&prices("acme"), SubscriptionOptions::default())
        .await
        .unwrap();
    eventually("subscribed", async || a1.emitter.open_subscriptions() == 1).await;
    a1.emitter.emit::<Prices>(|_| true, &tick("acme", 1));
    for pump in a1.dispatcher.end_subscriptions() {
        pump.await.unwrap();
    }
    assert_eq!(a1.emitter.open_subscriptions(), 0);
    let mut updates = sub.updates;
    ends_cleanly(&mut updates, vec![1]).await;
    assert_eq!(c.sender.in_flight(), 0);
    a1.shutdown().await;
    c.shutdown().await;
}

/// The initial result is bounded like a unary request: the hub's bound plus the margin is the
/// local guard of a hub that never answers it (the subscription is cancelled at the hub).
#[tokio::test]
async fn the_local_guard_bounds_the_initial_result_only() {
    let hub = FakeHub::start().await;
    hub.set_stream_timeouts(false);
    let probe = Arc::new(Probe::default());
    let a1 = owner(&hub.address(), "svc-a", "a1", &probe).await;
    let c = caller(&hub.address()).await;
    let options = SubscriptionOptions {
        timeout: Some(Duration::from_secs(1)),
        ..SubscriptionOptions::default()
    };
    // No initial result ever: the open fails at the bound (1 s) plus the margin (1 s).
    let started = Instant::now();
    let e = c
        .sender
        .subscribe(&prices("hang"), options.clone())
        .await
        .err()
        .expect("the local guard");
    let took = started.elapsed();
    assert!(
        matches!(e, MessagingError::Timeout(ref t) if t == "t.Prices"),
        "{e:?}"
    );
    assert!(
        took >= Duration::from_millis(1_950) && took < Duration::from_millis(3_500),
        "the guard is bound + 1 s, took {took:?}"
    );
    eventually("cancelled at the hub", async || {
        hub.cancels()
            .iter()
            .any(|(_, c)| c.reason == "local guard timeout")
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    // An initial result at once, then silence for longer than the guard: no idle timeout, no
    // guard on the updates.
    let sub = c.sender.subscribe(&prices("acme"), options).await.unwrap();
    let mut updates = sub.updates;
    let quiet = tokio::time::timeout(Duration::from_millis(2_500), updates.next()).await;
    assert!(quiet.is_err(), "a quiet subscription stays open: {quiet:?}");
    a1.emitter.emit::<Prices>(|_| true, &tick("acme", 7));
    assert_eq!(next_update(&mut updates).await.unwrap().price, 7);
    drop(updates);
    a1.shutdown().await;
    c.shutdown().await;
}
