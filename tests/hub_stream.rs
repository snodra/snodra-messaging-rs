//! Streaming queries (ADR 0017, P4b) against in-process fake hubs: the handler side (parts under
//! credit, the end, error items, the hub's `Cancel` for streams and for unary handlers, the
//! permit held for the whole stream) and the sender side (`Sender::stream`: items in order,
//! credit refills, `max_results`, the idle timeout, errors, the drop guard's `Cancel`, retries
//! until the first part, the capability gates, shared streaming types).
mod support;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::stream::BoxStream;
use kraken_messaging::hub::{
    Dispatcher, HandlerRegistry, HubPool, HubSettings, Inbound, RegisteredTypes, Sender,
    SenderSettings,
};
use kraken_messaging::identity::{Guard, IdentityResolver};
use kraken_messaging::proto::{self, ErrorCode, ServerMessage, server_message};
use kraken_messaging::{MessagingError, Query, StreamingQuery, message_type};
use serde::{Deserialize, Serialize};
use support::fake_hub::FakeHub;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

/// A unary query, next to the streaming ones.
#[derive(Serialize, Deserialize)]
struct Lookup {}
message_type!(Lookup, "t.Lookup");
impl Query for Lookup {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct SharedLookup {}
message_type!(SharedLookup, "t.SharedLookup");
impl Query for SharedLookup {
    type Reply = ();
}

/// `n` rows, each after `delay_ms`; the handler fails at row `fail_at` (an error item).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Export {
    n: u32,
    delay_ms: u64,
    fail_at: Option<u32>,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Row {
    i: u32,
}
message_type!(Export, "t.Export");
impl StreamingQuery for Export {
    type Item = Row;
}

#[derive(Serialize, Deserialize)]
struct SharedExport {}
message_type!(SharedExport, "t.SharedExport");
impl StreamingQuery for SharedExport {
    type Item = Row;
}

fn rows(export: Export) -> impl futures::Stream<Item = Result<Row, MessagingError>> + Send {
    let Export {
        n,
        delay_ms,
        fail_at,
    } = export;
    tokio_stream::iter(0..n).then(move |i| async move {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        if fail_at == Some(i) {
            Err(MessagingError::Storage("disk gone".into()))
        } else {
            Ok(Row { i })
        }
    })
}

/// A unary query whose handler never answers (until cancelled).
#[derive(Serialize, Deserialize)]
struct Hang {}
message_type!(Hang, "t.Hang");
impl Query for Hang {
    type Reply = ();
}

/// A streaming query whose handler panics.
#[derive(Serialize, Deserialize)]
struct Kaboom {}
message_type!(Kaboom, "t.Kaboom");
impl StreamingQuery for Kaboom {
    type Item = Row;
}

/// A streaming query that requires the `admin` role.
#[derive(Serialize, Deserialize)]
struct Admin {}
message_type!(Admin, "t.Admin");
impl StreamingQuery for Admin {
    type Item = Row;
}

/// What the handlers of one node saw.
#[derive(Default)]
struct Probe {
    /// Export handlers invoked.
    started: AtomicU32,
    /// Rows the library pulled from the handlers' streams.
    polled: AtomicU32,
    /// Handler streams dropped (ended or cancelled).
    dropped: AtomicU32,
    /// Hang handlers aborted (their future dropped before completion).
    aborted: AtomicU32,
    hanging: AtomicBool,
}

/// A handler stream that counts what is pulled from it and notices its drop.
struct Watched {
    inner: BoxStream<'static, Result<Row, MessagingError>>,
    probe: Arc<Probe>,
}

impl futures::Stream for Watched {
    type Item = Result<Row, MessagingError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = self.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(_)) = &polled {
            self.probe.polled.fetch_add(1, Ordering::SeqCst);
        }
        polled
    }
}

impl Drop for Watched {
    fn drop(&mut self) {
        self.probe.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// Sets a counter when dropped before `disarm`.
struct AbortGuard(Arc<Probe>);

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.0.aborted.fetch_add(1, Ordering::SeqCst);
    }
}

/// The handlers of a node: `t.Export` (streamed rows, watched), `t.Lookup` (unary), `t.Hang`
/// (unary, never answers), `t.Kaboom` (panics), `t.Admin` (guarded).
fn registry(probe: &Arc<Probe>) -> HandlerRegistry {
    let mut r = HandlerRegistry::new();
    let p = Arc::clone(probe);
    r.streaming_query::<Export, _, _, _>(move |q, _| {
        let probe = Arc::clone(&p);
        async move {
            probe.started.fetch_add(1, Ordering::SeqCst);
            Ok(Watched {
                inner: Box::pin(rows(q)),
                probe,
            })
        }
    });
    r.query::<Lookup, _, _>(|_, _| async move { Ok(()) });
    let p = Arc::clone(probe);
    r.query::<Hang, _, _>(move |_, _| {
        let probe = Arc::clone(&p);
        async move {
            probe.hanging.store(true, Ordering::SeqCst);
            let _guard = AbortGuard(probe);
            std::future::pending::<()>().await;
            Ok(())
        }
    });
    r.streaming_query::<Kaboom, _, _, _>(|_, _| async move {
        if true {
            panic!("kaboom");
        }
        Ok(tokio_stream::empty::<Result<Row, MessagingError>>())
    });
    r.streaming_query::<Admin, _, _, _>(|_, _| async move {
        Ok(tokio_stream::empty::<Result<Row, MessagingError>>())
    });
    r.requires::<Admin>(Guard::role("admin"));
    r
}

struct Node {
    pool: Arc<HubPool>,
    #[allow(dead_code)] // the sender side of the tests
    sender: Arc<Sender>,
    dispatcher: Arc<Dispatcher>,
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
/// to its running handlers, everything else to the sender.
async fn node_with(
    address: &str,
    service: &str,
    instance: &str,
    registry: HandlerRegistry,
    max_handlers: usize,
    sender_settings: SenderSettings,
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
    let dispatcher = Arc::new(Dispatcher::new(
        Arc::new(registry),
        Arc::new(IdentityResolver::new(None)),
        max_handlers,
    ));
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
                other => forward.on_inbound(other),
            }
        }
    });
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    Node {
        pool,
        sender,
        dispatcher,
    }
}

/// A handler node of `service` with `max_handlers` permits.
async fn handler(
    address: &str,
    service: &str,
    instance: &str,
    probe: &Arc<Probe>,
    max_handlers: usize,
) -> Node {
    node_with(
        address,
        service,
        instance,
        registry(probe),
        max_handlers,
        SenderSettings::default(),
    )
    .await
}

/// Polls `check` every 20 ms for at most 5 s.
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn export(n: u32, delay_ms: u64, fail_at: Option<u32>) -> Export {
    Export {
        n,
        delay_ms,
        fail_at,
    }
}

/// A request the hub forwards to a handler: `mode = STREAMING` with `initial_credit` (0: unary).
fn forwarded(id: &str, type_name: &str, payload: &[u8], credit: u32) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Request(proto::Request {
            id: id.into(),
            kind: proto::RequestKind::Query as i32,
            r#type: type_name.into(),
            payload: payload.to_vec(),
            mode: if credit > 0 {
                proto::RequestMode::Streaming as i32
            } else {
                proto::RequestMode::Unary as i32
            },
            initial_credit: credit,
            ..Default::default()
        })),
    }
}

fn flow(id: &str, credits: u32) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Flow(proto::Flow {
            request_id: id.into(),
            credits,
        })),
    }
}

fn cancel(id: &str, reason: &str) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Cancel(proto::Cancel {
            request_id: id.into(),
            reason: reason.into(),
        })),
    }
}

fn row(i: u32) -> Vec<u8> {
    serde_json::to_vec(&Row { i }).unwrap()
}

fn end_code(end: &proto::ReplyEnd) -> Option<ErrorCode> {
    end.error
        .as_ref()
        .map(|e| ErrorCode::try_from(e.code).unwrap())
}

// Handler side, driven by messages the fake hub pushes as kraken-hub would.

#[tokio::test]
async fn a_streaming_handler_sends_parts_under_credit_then_the_end() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let payload = serde_json::to_vec(&export(5, 0, None)).unwrap();
    hub.send_to_clients(forwarded("s#1", "t.Export", &payload, 2))
        .await;
    eventually("two parts", async || hub.parts().len() == 2).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let parts = hub.parts();
    assert_eq!(parts.len(), 2, "no part beyond the credit");
    assert_eq!(
        parts
            .iter()
            .map(|(_, p)| (p.request_id.as_str(), p.seq, p.payload.clone()))
            .collect::<Vec<_>>(),
        [("s#1", 1, row(0)), ("s#1", 2, row(1))]
    );
    assert!(parts.iter().all(|(_, p)| p.error.is_none()));
    assert_eq!(
        probe.polled.load(Ordering::SeqCst),
        2,
        "the handler's stream is not polled without credit"
    );
    assert_eq!(h.dispatcher.available_permits(), 1, "the permit is held");
    assert_eq!(hub.permits_held(1), Some(1), "the hub's permit too");
    assert!(hub.ends().is_empty());

    hub.send_to_clients(flow("s#1", 3)).await;
    eventually("five parts", async || hub.parts().len() == 5).await;
    assert_eq!(
        hub.parts().iter().map(|(_, p)| p.seq).collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    // The handler's stream is never polled without credit, so its end is found by a poll that
    // needs one credit beyond the last item (the sender's refills always grant it).
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(hub.ends().is_empty(), "no credit left to find the end");
    hub.send_to_clients(flow("s#1", 1)).await;
    eventually("the end", async || !hub.ends().is_empty()).await;
    assert_eq!(hub.parts().len(), 5);
    let (_, end) = &hub.ends()[0];
    assert_eq!(end.request_id, "s#1");
    assert!(end.error.is_none(), "{end:?}");
    eventually("permits released", async || {
        h.dispatcher.available_permits() == 2 && hub.permits_held(1) == Some(2)
    })
    .await;
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(hub.replies().await.len(), 0, "no unary reply for a stream");
    h.shutdown().await;
}

#[tokio::test]
async fn an_error_item_ends_the_stream_with_the_handlers_error() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let payload = serde_json::to_vec(&export(3, 0, Some(1))).unwrap();
    hub.send_to_clients(forwarded("s#1", "t.Export", &payload, 64))
        .await;
    eventually("the end", async || !hub.ends().is_empty()).await;
    assert_eq!(hub.parts().len(), 1);
    let (_, end) = &hub.ends()[0];
    assert_eq!(end_code(end), Some(ErrorCode::HandlerError));
    assert_eq!(
        end.error.as_ref().unwrap().message,
        "storage error: disk gone",
        "the handler's error, as a unary handler's"
    );
    eventually("permits released", async || {
        h.dispatcher.available_permits() == 2 && hub.permits_held(1) == Some(2)
    })
    .await;
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    h.shutdown().await;
}

#[tokio::test]
async fn a_hub_cancel_drops_the_stream_and_nothing_follows() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let payload = serde_json::to_vec(&export(1_000, 20, None)).unwrap();
    hub.send_to_clients(forwarded("s#1", "t.Export", &payload, 64))
        .await;
    eventually("a part", async || !hub.parts().is_empty()).await;
    hub.send_to_clients(cancel("s#1", "max results")).await;
    eventually("the stream dropped", async || {
        probe.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    eventually("permits released", async || {
        h.dispatcher.available_permits() == 2 && hub.permits_held(1) == Some(2)
    })
    .await;
    let parts = hub.parts().len();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(hub.parts().len(), parts, "no part after the cancel");
    assert!(hub.ends().is_empty(), "no end after the cancel");
    h.shutdown().await;
}

#[tokio::test]
async fn a_hub_cancel_aborts_a_unary_handler() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    hub.send_to_clients(forwarded("g#0", "t.Hang", b"{}", 0))
        .await;
    eventually("the handler runs", async || {
        probe.hanging.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(h.dispatcher.available_permits(), 1);
    hub.send_to_clients(cancel("g#0", "max results")).await;
    eventually("the handler aborted", async || {
        probe.aborted.load(Ordering::SeqCst) == 1
    })
    .await;
    eventually("permits released", async || {
        h.dispatcher.available_permits() == 2 && hub.permits_held(1) == Some(2)
    })
    .await;
    assert!(hub.replies().await.is_empty(), "nothing after a cancel");
    // A cancel for an unknown id is ignored.
    hub.send_to_clients(cancel("nobody", "x")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.dispatcher.available_permits(), 2);
    h.shutdown().await;
}

#[tokio::test]
async fn a_request_whose_mode_does_not_match_the_handler_is_no_handler() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    hub.send_to_clients(forwarded("s#1", "t.Lookup", b"{}", 8))
        .await;
    eventually("the end", async || !hub.ends().is_empty()).await;
    let (_, end) = &hub.ends()[0];
    assert_eq!(end_code(end), Some(ErrorCode::NoHandler));
    let payload = serde_json::to_vec(&export(1, 0, None)).unwrap();
    hub.send_to_clients(forwarded("u", "t.Export", &payload, 0))
        .await;
    eventually("the reply", async || !hub.replies().await.is_empty()).await;
    let reply = &hub.replies().await[0];
    assert!(
        matches!(&reply.result, Some(proto::reply::Result::Error(e)) if e.code == ErrorCode::NoHandler as i32),
        "{reply:?}"
    );
    assert_eq!(probe.started.load(Ordering::SeqCst), 0);
    eventually("permits released", async || {
        h.dispatcher.available_permits() == 2 && hub.permits_held(1) == Some(2)
    })
    .await;
    h.shutdown().await;
}

#[tokio::test]
async fn failures_before_the_first_part_end_the_stream_at_once() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    hub.send_to_clients(forwarded("bad", "t.Export", b"not json", 8))
        .await;
    hub.send_to_clients(forwarded("anon", "t.Admin", b"{}", 8))
        .await;
    hub.send_to_clients(forwarded("boom", "t.Kaboom", b"{}", 8))
        .await;
    eventually("three ends", async || hub.ends().len() == 3).await;
    let code = |id: &str| {
        hub.ends()
            .iter()
            .find(|(_, e)| e.request_id == id)
            .map(|(_, e)| (end_code(e).unwrap(), e.error.clone().unwrap().message))
            .unwrap()
    };
    assert_eq!(code("bad").0, ErrorCode::InvalidRequest);
    assert_eq!(code("anon").0, ErrorCode::Unauthenticated);
    assert_eq!(
        code("boom"),
        (ErrorCode::HandlerError, "handler panicked".to_string())
    );
    assert!(hub.parts().is_empty());
    eventually("permits released", async || {
        h.dispatcher.available_permits() == 2 && hub.permits_held(1) == Some(2)
    })
    .await;
    h.shutdown().await;
}

#[test]
fn streaming_handlers_are_query_types_exclusive_or_shared() {
    let mut r = HandlerRegistry::new();
    r.query::<Lookup, _, _>(|_, _| async move { Ok(()) });
    r.shared_query::<SharedLookup, _, _>(|_, _| async move { Ok(()) });
    r.streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
    r.shared_streaming_query::<SharedExport, _, _, _>(|_, _| async move {
        Ok(tokio_stream::empty::<Result<Row, MessagingError>>())
    });
    assert_eq!(
        r.registered_types(),
        RegisteredTypes {
            commands: vec![],
            queries: vec!["t.Lookup".into()],
            shared_queries: vec!["t.SharedExport".into(), "t.SharedLookup".into()],
            streaming_queries: vec!["t.Export".into()],
        }
    );
    assert_eq!(
        r.query_types(),
        ["t.Export", "t.Lookup", "t.SharedExport", "t.SharedLookup"]
    );
    assert_eq!(r.exclusive_query_types(), ["t.Lookup"]);
    assert_eq!(r.streaming_query_types(), ["t.Export", "t.SharedExport"]);
    assert_eq!(r.shared_query_types(), ["t.SharedExport", "t.SharedLookup"]);
}

#[test]
#[should_panic(expected = "duplicate handler for t.Export")]
fn a_type_has_one_handler_whatever_its_kind() {
    let mut r = HandlerRegistry::new();
    r.streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
    r.streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
}

#[test]
#[should_panic(expected = "registered both as an exclusive and as a shared query")]
fn a_streaming_type_cannot_be_exclusive_and_shared() {
    let mut r = HandlerRegistry::new();
    r.streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
    r.shared_streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
}
