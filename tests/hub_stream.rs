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
    Dispatcher, HandlerRegistry, HubPool, HubSettings, Inbound, RegisteredTypes, SendOptions,
    Sender, SenderSettings, StreamOptions, rendezvous_score,
};
use kraken_messaging::identity::{Guard, IdentityResolver};
use kraken_messaging::proto::{self, ErrorCode, ServerMessage, server_message};
use kraken_messaging::{Command, MessagingError, Query, RetryPolicy, StreamingQuery, message_type};
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

/// `n` rows, the first at once and every other one after `delay_ms`; the handler fails at row
/// `fail_at` (an error item).
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
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        if fail_at == Some(i) {
            Err(MessagingError::Storage("disk gone".into()))
        } else {
            Ok(Row { i })
        }
    })
}

/// A command of `size` bytes, used to fill a stream.
#[derive(Serialize, Deserialize)]
struct Fill {
    data: String,
}
message_type!(Fill, "t.Fill");
impl Command for Fill {
    type Reply = ();
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
    stream_credit_timeout: Option<Duration>,
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
    if let Some(timeout) = stream_credit_timeout {
        dispatcher = dispatcher.stream_credit_timeout(timeout);
    }
    let dispatcher = Arc::new(dispatcher);
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
        None,
    )
    .await
}

/// An owner of the shared streaming query `t.SharedExport`, answering one row `Row { i: mark }`.
async fn shared_owner(address: &str, service: &str, instance: &str, mark: u32) -> Node {
    let mut r = HandlerRegistry::new();
    r.shared_streaming_query::<SharedExport, _, _, _>(move |_, _| async move {
        Ok(tokio_stream::once(Ok(Row { i: mark })))
    });
    node_with(
        address,
        service,
        instance,
        r,
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

/// Streaming requests the hub received from senders.
async fn streams(hub: &FakeHub) -> Vec<proto::Request> {
    hub.requests()
        .await
        .into_iter()
        .filter(|r| r.mode == proto::RequestMode::Streaming as i32)
        .collect()
}

/// The rows of a stream until its end or its first error.
async fn collect(
    stream: &mut (impl futures::Stream<Item = Result<Row, MessagingError>> + Unpin),
) -> (Vec<u32>, Option<MessagingError>) {
    let mut rows = vec![];
    while let Some(item) = stream.next().await {
        match item {
            Ok(row) => rows.push(row.i),
            Err(e) => return (rows, Some(e)),
        }
    }
    (rows, None)
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
        3,
        "the handler's stream is polled at most one item ahead of the credit"
    );
    assert_eq!(h.dispatcher.available_permits(), 1, "the permit is held");
    assert_eq!(hub.permits_held(1), Some(1), "the hub's permit too");
    assert!(hub.ends().is_empty());

    // Exactly the credit the remaining items need: the end is found without spare credit.
    hub.send_to_clients(flow("s#1", 3)).await;
    eventually("the end", async || !hub.ends().is_empty()).await;
    assert_eq!(
        hub.parts().iter().map(|(_, p)| p.seq).collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
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

/// A hub that forgot the stream (a race on the hub, a lost `Flow` or `Cancel`) never sends
/// credit: without a bound the handler task and its permits would leak for ever. After
/// `Config.hub_stream_credit_timeout` without credit the stream ends with `TIMEOUT` and both
/// permits go back.
#[tokio::test]
async fn a_handler_waiting_for_credit_longer_than_the_credit_timeout_ends_with_timeout() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = node_with(
        &hub.address(),
        "svc-h",
        "h1",
        registry(&probe),
        2,
        SenderSettings::default(),
        Some(Duration::from_millis(200)),
    )
    .await;
    let payload = serde_json::to_vec(&export(5, 0, None)).unwrap();
    hub.send_to_clients(forwarded("s#1", "t.Export", &payload, 1))
        .await;
    eventually("one part", async || hub.parts().len() == 1).await;
    assert_eq!(h.dispatcher.available_permits(), 1, "the permit is held");
    assert_eq!(hub.permits_held(1), Some(1));
    // Never a `Flow`: the handler holds its second item and waits for credit.
    let started = Instant::now();
    eventually("the end", async || !hub.ends().is_empty()).await;
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(150) && took < Duration::from_secs(2),
        "the credit timeout, took {took:?}"
    );
    let (_, end) = &hub.ends()[0];
    assert_eq!(end.request_id, "s#1");
    assert_eq!(end_code(end), Some(ErrorCode::Timeout), "{end:?}");
    assert_eq!(
        end.error.as_ref().unwrap().message,
        "no credit within 200ms",
        "{end:?}"
    );
    assert_eq!(hub.parts().len(), 1, "nothing after the end");
    eventually("both permits released", async || {
        h.dispatcher.available_permits() == 2 && hub.permits_held(1) == Some(2)
    })
    .await;
    assert_eq!(
        probe.dropped.load(Ordering::SeqCst),
        1,
        "the handler's stream dropped"
    );
    // Credit that arrives too late is ignored: the request is gone.
    hub.send_to_clients(flow("s#1", 10)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(hub.parts().len(), 1);
    h.shutdown().await;
}

/// A sender that grants exactly as many credits as there are items (and never a `Flow`) gets
/// the end: it must not wait for the idle timeout.
#[tokio::test]
async fn the_end_arrives_with_exactly_as_many_credits_as_items() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let payload = serde_json::to_vec(&export(5, 0, None)).unwrap();
    hub.send_to_clients(forwarded("s#1", "t.Export", &payload, 5))
        .await;
    eventually("the end", async || !hub.ends().is_empty()).await;
    assert_eq!(hub.parts().len(), 5);
    assert!(hub.ends()[0].1.error.is_none());
    assert_eq!(probe.polled.load(Ordering::SeqCst), 5);
    eventually("permits released", async || {
        h.dispatcher.available_permits() == 2 && hub.permits_held(1) == Some(2)
    })
    .await;
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

/// The hub stream a streaming request arrived on closes: its handler would wait for credit for
/// ever, holding its permit, so it is aborted (the hub forgot the stream with the session). A
/// unary handler keeps running; its reply is dropped, as before.
#[tokio::test]
async fn a_lost_hub_stream_cancels_the_streaming_handlers_it_carried_but_not_the_unary_ones() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 3).await;
    let payload = serde_json::to_vec(&export(1_000, 20, None)).unwrap();
    hub.send_to_clients(forwarded("s#1", "t.Export", &payload, 4))
        .await;
    hub.send_to_clients(forwarded("g#0", "t.Hang", b"{}", 0))
        .await;
    eventually("both handlers running", async || {
        hub.parts().len() == 4 && probe.hanging.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(h.dispatcher.available_permits(), 1);
    hub.drop_streams();
    eventually("the streaming handler aborted", async || {
        probe.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    eventually(
        "its permit released, the unary handler's held",
        async || h.dispatcher.available_permits() == 2,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        probe.aborted.load(Ordering::SeqCst),
        0,
        "the unary handler runs on"
    );
    assert_eq!(h.dispatcher.available_permits(), 2);
    assert!(
        hub.ends().is_empty(),
        "nothing is sent for a cancelled stream"
    );
    // The instance is back on a new stream and serves again.
    assert!(h.pool.await_all_registered(Duration::from_secs(5)).await);
    hub.send_to_clients(forwarded("s#2", "t.Export", &payload, 4))
        .await;
    eventually("parts on the new stream", async || {
        hub.parts()
            .iter()
            .any(|(stream, p)| *stream > 1 && p.request_id == "s#2")
    })
    .await;
    h.shutdown().await;
}

#[tokio::test]
async fn a_request_whose_mode_does_not_match_the_handler_is_invalid_request() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    // The caller used the wrong mode: not retryable, unlike NO_HANDLER (a unary query to a type
    // this instance does not serve at all stays NO_HANDLER).
    hub.send_to_clients(forwarded("s#1", "t.Lookup", b"{}", 8))
        .await;
    eventually("the end", async || !hub.ends().is_empty()).await;
    let (_, end) = &hub.ends()[0];
    assert_eq!(end_code(end), Some(ErrorCode::InvalidRequest));
    assert_eq!(
        end.error.as_ref().unwrap().message,
        "t.Lookup is not a streaming query handler (it answers mode UNARY)"
    );
    let payload = serde_json::to_vec(&export(1, 0, None)).unwrap();
    hub.send_to_clients(forwarded("u", "t.Export", &payload, 0))
        .await;
    hub.send_to_clients(forwarded("n", "t.Nobody", b"{}", 0))
        .await;
    eventually("the replies", async || hub.replies().await.len() == 2).await;
    let replies = hub.replies().await;
    let reply_of = |id: &str| {
        replies
            .iter()
            .find(|r| r.request_id == id)
            .map(|r| match &r.result {
                Some(proto::reply::Result::Error(e)) => {
                    (ErrorCode::try_from(e.code).unwrap(), e.message.clone())
                }
                other => panic!("expected an error reply, got {other:?}"),
            })
            .unwrap()
    };
    assert_eq!(
        reply_of("u"),
        (
            ErrorCode::InvalidRequest,
            "t.Export is a streaming query: send it with Sender::stream (mode STREAMING)".into()
        )
    );
    assert_eq!(reply_of("n").0, ErrorCode::NoHandler);
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

// Sender side: `Sender::stream` against handlers served by the dispatcher.

#[tokio::test]
async fn a_stream_yields_the_handlers_items_in_order_then_ends() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let options = StreamOptions {
        initial_credit: 8,
        ..StreamOptions::default()
    };
    let mut stream = c
        .sender
        .stream(&export(200, 0, None), options)
        .await
        .unwrap();
    let (rows, error) = collect(&mut stream).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(rows, (0..200).collect::<Vec<_>>());
    drop(stream);
    // Half the window back every time half of it was consumed.
    let flows = hub.flows();
    assert!(flows.len() >= 49, "{} flows", flows.len());
    assert!(flows.iter().all(|(_, f)| f.credits == 4), "{flows:?}");
    assert_eq!(c.sender.in_flight(), 0);
    assert_eq!(hub.open_streams(), 0);
    assert!(hub.cancels().is_empty(), "nothing to cancel after the end");
    eventually("the handler's permits released", async || {
        h.dispatcher.available_permits() == 2 && probe.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    h.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn options_become_the_request_fields() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    for options in [
        StreamOptions::default(),
        StreamOptions {
            timeout: Some(Duration::from_millis(2_500)),
            max_results: 3,
            priority: Some(7),
            initial_credit: 16,
        },
        StreamOptions {
            initial_credit: 0,
            ..StreamOptions::default()
        },
    ] {
        let mut stream = c.sender.stream(&export(1, 0, None), options).await.unwrap();
        assert_eq!(collect(&mut stream).await.0, [0]);
    }
    let sent = streams(&hub).await;
    assert_eq!(sent.len(), 3);
    let (default, chosen, zero) = (&sent[0], &sent[1], &sent[2]);
    assert_eq!(default.kind, proto::RequestKind::Query as i32);
    assert_eq!(default.r#type, "t.Export");
    assert_eq!(default.payload, br#"{"n":1,"delay_ms":0,"fail_at":null}"#);
    assert_eq!(default.routing_key, "");
    assert!(default.headers.contains_key("correlationId"));
    assert_eq!(
        (
            default.timeout_ms,
            default.max_results,
            default.priority,
            default.initial_credit
        ),
        (0, 0, 0, 64)
    );
    assert_eq!(chosen.mode, proto::RequestMode::Streaming as i32);
    assert_eq!(
        (
            chosen.timeout_ms,
            chosen.max_results,
            chosen.priority,
            chosen.initial_credit
        ),
        (2_500, 3, 7, 16)
    );
    assert_eq!(
        zero.initial_credit, 64,
        "0 reads as 64, as the hub reads it"
    );
    assert_ne!(default.id, chosen.id);
    h.shutdown().await;
    c.shutdown().await;
}

/// The handler stalls at credit 0 and resumes on the sender's refill, which comes once half of
/// the window was consumed by the caller (back-pressure follows the consumer, not the arrival).
#[tokio::test]
async fn the_handler_stalls_at_zero_credit_until_the_consumer_pulls_half_the_window() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let options = StreamOptions {
        initial_credit: 4,
        ..StreamOptions::default()
    };
    let mut stream = c
        .sender
        .stream(&export(100, 0, None), options)
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().i, 0);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        probe.polled.load(Ordering::SeqCst),
        5,
        "the handler sent its window, holds one item and stalled"
    );
    assert!(
        hub.flows().is_empty(),
        "one consumed of a window of 4: no refill yet"
    );
    assert_eq!(stream.next().await.unwrap().unwrap().i, 1);
    eventually("the refill", async || !hub.flows().is_empty()).await;
    assert_eq!(hub.flows()[0].1.credits, 2);
    eventually("the handler resumed", async || {
        probe.polled.load(Ordering::SeqCst) == 7
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(probe.polled.load(Ordering::SeqCst), 7, "stalled again");
    drop(stream);
    eventually("the handler cancelled", async || {
        probe.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    h.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn max_results_ends_the_stream_and_cancels_the_handler() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let options = StreamOptions {
        max_results: 3,
        ..StreamOptions::default()
    };
    let mut stream = c
        .sender
        .stream(&export(100, 0, None), options)
        .await
        .unwrap();
    let (rows, error) = collect(&mut stream).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(rows, [0, 1, 2]);
    assert_eq!(c.sender.in_flight(), 0);
    eventually(
        "the handler cancelled and its permits released",
        async || probe.dropped.load(Ordering::SeqCst) == 1 && h.dispatcher.available_permits() == 2,
    )
    .await;
    assert!(
        hub.cancels().is_empty(),
        "the hub cancelled, not the sender"
    );
    drop(stream);
    h.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn the_idle_timeout_ends_the_stream_with_timeout() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let options = StreamOptions {
        timeout: Some(Duration::from_millis(300)),
        ..StreamOptions::default()
    };
    // The first row comes at once, the second never in time: the error is the last item.
    let mut stream = c
        .sender
        .stream(&export(3, 5_000, None), options.clone())
        .await
        .unwrap();
    let (rows, error) = collect(&mut stream).await;
    assert_eq!(rows, [0]);
    assert!(
        matches!(error, Some(MessagingError::Timeout(ref t)) if t == "t.Export"),
        "{error:?}"
    );
    eventually("the handler cancelled", async || {
        probe.dropped.load(Ordering::SeqCst) == 1 && h.dispatcher.available_permits() == 2
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    // Not even the first row in time: the open fails.
    let hang = Arc::new(Probe::default());
    let slow = node_with(
        &hub.address(),
        "svc-slow",
        "s1",
        {
            let mut r = HandlerRegistry::new();
            let probe = Arc::clone(&hang);
            r.shared_streaming_query::<SharedExport, _, _, _>(move |_, _| {
                let probe = Arc::clone(&probe);
                async move {
                    Ok(Watched {
                        inner: Box::pin(tokio_stream::pending()),
                        probe,
                    })
                }
            });
            r
        },
        2,
        SenderSettings::default(),
        None,
    )
    .await;
    let e = c
        .sender
        .stream(&SharedExport {}, options)
        .await
        .err()
        .expect("no first part in time");
    assert!(matches!(e, MessagingError::Timeout(_)), "{e:?}");
    eventually("the slow handler cancelled", async || {
        hang.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    drop(stream);
    h.shutdown().await;
    slow.shutdown().await;
    c.shutdown().await;
}

/// The hub bounds a queue wait by `timeout_ms` and restarts the idle clock at the forward, so
/// the first part may take up to twice the timeout: the local guard allows 2 x the hub's bound
/// plus 1 s for it, then the bound plus 1 s reset by every part. (A hub that never ends an idle
/// stream shows which guard fired; the guard cancels the stream at the hub.)
///
/// With a 1 s timeout the guards are 3 s and 2 s apart by a second, so each window below is
/// about a second wide (a timer never fires early; under load it fires late): the two halves
/// run at once so the test still takes about 3 s.
#[tokio::test]
async fn the_local_guard_allows_twice_the_hub_bound_for_the_first_part_then_the_bound() {
    let hub = FakeHub::start().await;
    hub.set_stream_timeouts(false);
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let options = StreamOptions {
        timeout: Some(Duration::from_secs(1)),
        ..StreamOptions::default()
    };
    let hang = Arc::new(Probe::default());
    let slow = node_with(
        &hub.address(),
        "svc-slow",
        "s1",
        {
            let mut r = HandlerRegistry::new();
            let probe = Arc::clone(&hang);
            r.shared_streaming_query::<SharedExport, _, _, _>(move |_, _| {
                let probe = Arc::clone(&probe);
                async move {
                    Ok(Watched {
                        inner: Box::pin(tokio_stream::pending()),
                        probe,
                    })
                }
            });
            r
        },
        2,
        SenderSettings::default(),
        None,
    )
    .await;
    // No first part ever: the open fails at 2 x 1 s + 1 s.
    let first_part = async {
        let started = Instant::now();
        let e = c
            .sender
            .stream(&SharedExport {}, options.clone())
            .await
            .err()
            .expect("the local guard");
        (e, started.elapsed())
    };
    // A first part at once, then nothing: the idle guard is the bound + 1 s.
    let idle = async {
        let mut stream = c
            .sender
            .stream(&export(3, 30_000, None), options.clone())
            .await
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().i, 0);
        let started = Instant::now();
        let e = stream.next().await.unwrap().unwrap_err();
        let took = started.elapsed();
        assert!(stream.next().await.is_none());
        (e, took)
    };
    let ((first_error, first_took), (idle_error, idle_took)) = tokio::join!(first_part, idle);
    assert!(
        matches!(first_error, MessagingError::Timeout(_)),
        "{first_error:?}"
    );
    assert!(
        first_took >= Duration::from_millis(2_950) && first_took < Duration::from_millis(4_500),
        "the first-part guard is 2 x bound + 1 s, took {first_took:?}"
    );
    assert!(
        matches!(idle_error, MessagingError::Timeout(_)),
        "{idle_error:?}"
    );
    assert!(
        idle_took >= Duration::from_millis(1_950) && idle_took < Duration::from_millis(2_950),
        "the idle guard is bound + 1 s, took {idle_took:?}"
    );
    eventually("both handlers cancelled by the guards", async || {
        hang.dropped.load(Ordering::SeqCst) == 1 && probe.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    h.shutdown().await;
    slow.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_handler_error_item_is_the_last_item() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let mut stream = c
        .sender
        .stream(&export(5, 0, Some(2)), StreamOptions::default())
        .await
        .unwrap();
    let (rows, error) = collect(&mut stream).await;
    assert_eq!(rows, [0, 1]);
    assert!(
        matches!(error, Some(MessagingError::RemoteHandler { ref message, ref type_name, .. }) if message.contains("disk gone") && type_name == "t.Export"),
        "{error:?}"
    );
    assert!(stream.next().await.is_none(), "nothing after the error");
    assert_eq!(c.sender.in_flight(), 0);
    assert_eq!(hub.open_streams(), 0);
    // An error before the first item fails the open.
    let e = c
        .sender
        .stream(&export(5, 0, Some(0)), StreamOptions::default())
        .await
        .err()
        .expect("the handler failed at once");
    assert!(matches!(e, MessagingError::RemoteHandler { .. }), "{e:?}");
    eventually("permits released", async || {
        h.dispatcher.available_permits() == 2
    })
    .await;
    drop(stream);
    h.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn dropping_the_stream_cancels_the_handler() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let options = StreamOptions {
        initial_credit: 8,
        ..StreamOptions::default()
    };
    let mut stream = c
        .sender
        .stream(&export(1_000, 0, None), options)
        .await
        .unwrap();
    for i in 0..3 {
        assert_eq!(stream.next().await.unwrap().unwrap().i, i);
    }
    drop(stream);
    eventually("the cancel", async || !hub.cancels().is_empty()).await;
    let id = streams(&hub).await[0].id.clone();
    let (_, cancel) = &hub.cancels()[0];
    assert_eq!(
        (cancel.request_id.as_str(), cancel.reason.as_str()),
        (id.as_str(), "dropped by the sender")
    );
    eventually(
        "the handler cancelled and its permits released",
        async || probe.dropped.load(Ordering::SeqCst) == 1 && h.dispatcher.available_permits() == 2,
    )
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    assert_eq!(hub.open_streams(), 0);
    h.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn an_undecodable_part_fails_the_stream_and_cancels_the_handler() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let mut stream = c
        .sender
        .stream(&export(3, 5_000, None), StreamOptions::default())
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().i, 0);
    let id = streams(&hub).await[0].id.clone();
    hub.send_to_clients(ServerMessage {
        message: Some(server_message::Message::Part(proto::ReplyPart {
            request_id: id.clone(),
            payload: b"not a row".to_vec(),
            seq: 2,
            ..Default::default()
        })),
    })
    .await;
    let e = stream.next().await.unwrap().unwrap_err();
    assert!(matches!(e, MessagingError::Serialization(_)), "{e:?}");
    assert!(stream.next().await.is_none());
    eventually("the cancel", async || !hub.cancels().is_empty()).await;
    let (_, cancel) = &hub.cancels()[0];
    assert_eq!(
        (cancel.request_id.as_str(), cancel.reason.as_str()),
        (id.as_str(), "undecodable part")
    );
    eventually("the handler cancelled", async || {
        probe.dropped.load(Ordering::SeqCst) == 1 && h.dispatcher.available_permits() == 2
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    drop(stream);
    h.shutdown().await;
    c.shutdown().await;
}

/// The hub may drop a part it could not buffer (its outbound buffer stayed full for 5 s): the
/// sender checks `seq` (1, 2, 3, ... in arrival order) and ends the stream with
/// `InvalidMessage` at the first gap, cancelling the handler, which still streams.
#[tokio::test]
async fn a_gap_in_seq_fails_the_stream_as_lost_parts_and_cancels_the_handler() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let mut stream = c
        .sender
        .stream(&export(3, 5_000, None), StreamOptions::default())
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().i, 0);
    let id = streams(&hub).await[0].id.clone();
    // Seq 2 never arrives: seq 4 right after seq 1 is two parts lost.
    hub.send_to_clients(ServerMessage {
        message: Some(server_message::Message::Part(proto::ReplyPart {
            request_id: id.clone(),
            payload: row(3),
            seq: 4,
            ..Default::default()
        })),
    })
    .await;
    let e = stream.next().await.unwrap().unwrap_err();
    assert!(
        matches!(&e, MessagingError::InvalidMessage(m) if m == "stream t.Export lost 2 part(s)"),
        "{e:?}"
    );
    assert!(stream.next().await.is_none());
    eventually("the cancel", async || !hub.cancels().is_empty()).await;
    let (_, cancel) = &hub.cancels()[0];
    assert_eq!(
        (cancel.request_id.as_str(), cancel.reason.as_str()),
        (id.as_str(), "lost parts")
    );
    eventually("the handler cancelled", async || {
        probe.dropped.load(Ordering::SeqCst) == 1 && h.dispatcher.available_permits() == 2
    })
    .await;
    assert_eq!(c.sender.in_flight(), 0);
    drop(stream);
    h.shutdown().await;
    c.shutdown().await;
}

/// `ReplyEnd.parts` is the hub's count of the parts it sent: fewer received means the last
/// ones were lost after the sender's last check, so the end is `InvalidMessage`, not a clean end.
#[tokio::test]
async fn an_end_counting_more_parts_than_arrived_fails_the_stream_as_lost_parts() {
    let hub = FakeHub::start().await;
    let probe = Arc::new(Probe::default());
    let h = handler(&hub.address(), "svc-h", "h1", &probe, 2).await;
    let c = caller(&hub.address()).await;
    let mut stream = c
        .sender
        .stream(&export(3, 5_000, None), StreamOptions::default())
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().i, 0);
    let id = streams(&hub).await[0].id.clone();
    hub.send_to_clients(ServerMessage {
        message: Some(server_message::Message::End(proto::ReplyEnd {
            request_id: id.clone(),
            error: None,
            parts: 3,
            ..Default::default()
        })),
    })
    .await;
    let e = stream.next().await.unwrap().unwrap_err();
    assert!(
        matches!(&e, MessagingError::InvalidMessage(m) if m == "stream t.Export lost 2 part(s)"),
        "{e:?}"
    );
    assert!(stream.next().await.is_none());
    assert_eq!(c.sender.in_flight(), 0);
    assert!(
        hub.cancels().is_empty(),
        "the stream had ended at the hub: nothing to cancel"
    );
    drop(stream);
    h.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_hub_without_the_capability_is_unsupported_and_gets_nothing() {
    for legacy in [false, true] {
        let hub = FakeHub::start().await;
        if legacy {
            hub.set_ack(None);
        } else {
            hub.set_streaming(false);
        }
        let c = caller(&hub.address()).await;
        assert!(!c.pool.handshake_for("").unwrap().streaming());
        let e = c
            .sender
            .stream(&export(1, 0, None), StreamOptions::default())
            .await
            .err()
            .expect("unsupported");
        assert!(
            matches!(e, MessagingError::UnsupportedByHub(_)),
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
        .stream(&export(1, 0, None), StreamOptions::default())
        .await
        .err()
        .unwrap();
    assert!(
        matches!(e, MessagingError::NoHandler(ref t) if t == "t.Export"),
        "{e:?}"
    );
    // An owner whose session did not negotiate streaming (an older library).
    hub.set_streaming(false);
    let probe = Arc::new(Probe::default());
    let old = handler(&hub.address(), "svc-old", "o1", &probe, 2).await;
    hub.set_streaming(true);
    let e = c
        .sender
        .stream(&export(1, 0, None), StreamOptions::default())
        .await
        .err()
        .unwrap();
    assert!(
        matches!(e, MessagingError::UnsupportedByHub(ref m) if m.contains("svc-old")),
        "{e:?}"
    );
    assert_eq!(probe.started.load(Ordering::SeqCst), 0);
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
    struct AsStream {}
    message_type!(AsStream, "t.Fill");
    impl StreamingQuery for AsStream {
        type Item = Row;
    }
    let e = c
        .sender
        .stream(&AsStream {}, StreamOptions::default())
        .await
        .err()
        .unwrap();
    assert!(matches!(e, MessagingError::InvalidMessage(_)), "{e:?}");
    // The hub stopped sharing the capability after the handshake: it answers UNSUPPORTED.
    hub.set_streaming(false);
    let e = c
        .sender
        .stream(&export(1, 0, None), StreamOptions::default())
        .await
        .err()
        .unwrap();
    assert!(matches!(e, MessagingError::UnsupportedByHub(_)), "{e:?}");
    assert_eq!(c.sender.in_flight(), 0);
    owner.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn the_retry_policy_applies_until_the_first_part_only() {
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
    let probe = Arc::new(Probe::default());
    let query = export(1_000, 0, None);
    // NO_HANDLER until a handler registers: retried with fresh ids.
    let (opened, h) = tokio::join!(
        c.sender.stream(
            &query,
            StreamOptions {
                initial_credit: 8,
                ..StreamOptions::default()
            },
        ),
        async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            handler(&hub.address(), "svc-h", "h1", &probe, 2).await
        }
    );
    let mut stream = opened.unwrap();
    let sent = streams(&hub).await;
    assert!(sent.len() >= 2, "{} attempts", sent.len());
    let mut ids: Vec<&str> = sent.iter().map(|r| r.id.as_str()).collect();
    ids.dedup();
    assert_eq!(ids.len(), sent.len(), "each attempt has a fresh id");
    for i in 0..3 {
        assert_eq!(stream.next().await.unwrap().unwrap().i, i);
    }
    // After the first part a lost hub stream is the end: no new stream is opened.
    let attempts = sent.len();
    hub.drop_streams();
    let (rows, error) = collect(&mut stream).await;
    assert!(
        matches!(error, Some(MessagingError::HubConnectionLost(_))),
        "{error:?} after {} more rows",
        rows.len()
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        streams(&hub).await.len(),
        attempts,
        "not retried after a part"
    );
    assert_eq!(c.sender.in_flight(), 0);
    drop(stream);
    h.shutdown().await;
    c.shutdown().await;
}

/// A `ReplyEnd{error}` before the first part is retried like an `Error` with that code: the
/// handler leaves (HANDLER_DISCONNECTED, retryable) before its first part, then NO_HANDLER until
/// another owner registers and serves the stream. (Nothing is retried after the first part:
/// `the_retry_policy_applies_until_the_first_part_only`.)
#[tokio::test]
async fn a_reply_end_error_before_the_first_part_is_retried_like_an_error() {
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
    // An owner that never sends its first part.
    let hang = Arc::new(Probe::default());
    let mute = node_with(
        &hub.address(),
        "svc-mute",
        "m1",
        {
            let mut r = HandlerRegistry::new();
            let probe = Arc::clone(&hang);
            r.shared_streaming_query::<SharedExport, _, _, _>(move |_, _| {
                let probe = Arc::clone(&probe);
                async move {
                    probe.started.fetch_add(1, Ordering::SeqCst);
                    Ok(Watched {
                        inner: Box::pin(tokio_stream::pending()),
                        probe,
                    })
                }
            });
            r
        },
        2,
        SenderSettings::default(),
        None,
    )
    .await;
    let (opened, owner) = tokio::join!(
        c.sender.stream(&SharedExport {}, StreamOptions::default()),
        async {
            eventually("the mute owner got the stream", async || {
                hang.started.load(Ordering::SeqCst) == 1
            })
            .await;
            // It leaves: ReplyEnd{HANDLER_DISCONNECTED} to the sender, retried; then NO_HANDLER
            // (retried) until another owner registers.
            mute.shutdown().await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            shared_owner(&hub.address(), "svc-b", "b1", 7).await
        }
    );
    let mut stream = opened.unwrap();
    assert_eq!(collect(&mut stream).await.0, [7]);
    let sent = streams(&hub).await;
    assert!(sent.len() >= 3, "{} attempts", sent.len());
    let mut ids: Vec<&str> = sent.iter().map(|r| r.id.as_str()).collect();
    ids.dedup();
    assert_eq!(ids.len(), sent.len(), "each attempt has a fresh id");
    assert_eq!(c.sender.in_flight(), 0);
    drop(stream);
    owner.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_shared_streaming_type_streams_from_one_owner_instance() {
    let hub = FakeHub::start().await;
    let a = shared_owner(&hub.address(), "svc-a", "a1", 1).await;
    let b = shared_owner(&hub.address(), "svc-b", "b1", 2).await;
    let c = caller(&hub.address()).await;
    let registers = hub.registers().await;
    let a1 = registers.iter().find(|r| r.instance_id == "a1").unwrap();
    assert_eq!(a1.shared_query_types, ["t.SharedExport"]);
    assert!(a1.query_types.is_empty());
    let mut marks = vec![];
    for _ in 0..2 {
        let mut stream = c
            .sender
            .stream(&SharedExport {}, StreamOptions::default())
            .await
            .unwrap();
        let (rows, error) = collect(&mut stream).await;
        assert!(error.is_none(), "{error:?}");
        assert_eq!(rows.len(), 1, "one instance answers");
        marks.push(rows[0]);
    }
    marks.sort();
    assert_eq!(marks, [1, 2], "one stream to each owner, round-robin");
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_pool_streams_to_one_capable_hub_only() {
    // hub-3 is a P4a hub: every stream goes to hub-4.
    let (h3, h4) = (
        FakeHub::start_with_id("hub-3").await,
        FakeHub::start_with_id("hub-4").await,
    );
    h3.set_streaming(false);
    let both = format!("{},{}", h3.address(), h4.address());
    let probe = Arc::new(Probe::default());
    let h = handler(&both, "svc-h", "h1", &probe, 4).await;
    let c = caller(&both).await;
    for _ in 0..4 {
        let mut stream = c
            .sender
            .stream(&export(2, 0, None), StreamOptions::default())
            .await
            .unwrap();
        assert_eq!(collect(&mut stream).await.0, [0, 1]);
    }
    assert_eq!((streams(&h3).await.len(), streams(&h4).await.len()), (0, 4));
    h.shutdown().await;
    c.shutdown().await;
}

/// A big streaming query, to fill a stream.
#[derive(Serialize, Deserialize)]
struct BigExport {
    term: String,
}
message_type!(BigExport, "t.BigExport");
impl StreamingQuery for BigExport {
    type Item = Row;
}

/// A stream whose open is dropped while its write still waits for room in the stream's outbound
/// buffer leaves no pending entry behind (as the gather path).
#[tokio::test]
async fn a_stream_dropped_while_its_write_is_blocked_leaves_nothing_pending() {
    let hub = FakeHub::start().await;
    let c = caller(&hub.address()).await;
    hub.set_stall_reads(true);
    let calls: Vec<_> = (0..324)
        .map(|i| {
            let sender = Arc::clone(&c.sender);
            let size = if i < 24 { 256 * 1024 } else { 1 };
            tokio::spawn(async move {
                let query = BigExport {
                    term: "x".repeat(size),
                };
                sender
                    .stream(&query, StreamOptions::default())
                    .await
                    .map(|_| ())
            })
        })
        .collect();
    eventually("every stream recorded its pending entry", async || {
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
        "a dropped open leaves nothing pending"
    );
    hub.drop_streams();
    c.shutdown().await;
}

/// The capable hubs are ranked on a snapshot; the write refuses a stream that lost `streaming`
/// in between (a reconnect to an older hub) and the open moves on (`UnsupportedByHub` when no
/// capable hub is left), as the gather path does.
#[tokio::test]
async fn a_stream_is_not_written_to_a_hub_that_lost_the_capability_after_ranking() {
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
    // Streams go round-robin: the one ranked [hub-1, hub-2] waits in the write to hub-1 (the one
    // ranked the other way round ends at once with NO_HANDLER on hub-2).
    let mut probes = 0;
    let waiting = loop {
        probes += 1;
        assert!(probes <= 3, "no stream went to hub-1 first");
        let sender = Arc::clone(&c.sender);
        let call = tokio::spawn(async move {
            sender
                .stream(&export(1, 0, None), StreamOptions::default())
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
        streams(&h2).await.len() < probes,
        "the waiting stream reached no hub"
    );
    h2.set_streaming(false);
    h2.drop_streams();
    eventually("hub-2 re-handshaken without the capability", async || {
        c.pool.connections().iter().any(|conn| {
            conn.address() == h2.address() && conn.handshake().is_some_and(|h| !h.streaming())
        })
    })
    .await;
    let on_h2_before = streams(&h2).await.len();
    h1.drop_streams();
    let e = tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .expect("the open ends at once, not at the local guard")
        .unwrap()
        .unwrap_err();
    assert!(matches!(e, MessagingError::UnsupportedByHub(_)), "{e:?}");
    assert_eq!(
        streams(&h2).await.len(),
        on_h2_before,
        "the stream was not written to hub-2's stream"
    );
    assert_eq!(c.sender.in_flight(), 0);
    c.shutdown().await;
}
