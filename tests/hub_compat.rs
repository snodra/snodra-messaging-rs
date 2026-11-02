//! Compatibility suite against real hub images (roadmap rule: every phase checks new clients
//! against the new hub and against the previous hub image).
//!
//! - `kraken-hub:dev` (`KRAKEN_HUB_IMAGE`): protocol v2 with flow control. The ack carries the
//!   configured values and shares `flow-control`, per-type timeout overrides apply to requests sent
//!   with `timeout_ms` 0, a request lost with the stream is `HubConnectionLost`, one never written is
//!   `HubUnavailable`, and the client is back within the 5 s backoff cap after a hub restart. Flow
//!   control (P1): a slow handler with one permit turns a burst into latency (the topology shows the
//!   instance flow-controlled with a queue), a single permit goes back at once so a burst to a fast
//!   handler is not paced by the flush timer, a full queue answers `HandlerBusy` and a retried
//!   command gets through once it drains, retry never repeats a handler error or a timed-out
//!   command, and queued requests are served by priority.
//! - `kraken-hub:p0` (`KRAKEN_HUB_P0_IMAGE`): protocol v2 without flow control. The ack lists no
//!   capability, no `Permits` are sent, the hub forwards without waiting and everything works as in
//!   P0, reconnect included.
//! - `kraken-hub:v1-baseline` (`KRAKEN_HUB_BASELINE_IMAGE`): the hub before protocol v2. The client
//!   detects it (echo without ack) and works exactly as before, without `Permits`.
//! - P3 (ADR 0016), one target through a `HubPool`: `kraken-hub:p3-hub` (`KRAKEN_HUB_P3_IMAGE`)
//!   shares `instance-status` and gets `InstanceStatus` reports; `kraken-hub:p2`
//!   (`KRAKEN_HUB_P2_IMAGE`) and the v1 baseline do not share it and get none, and work as before.
//!
//! - P4a (ADR 0017): the new hub shares `scatter-gather`. Against `kraken-hub:p3`
//!   (`KRAKEN_HUB_P3_IMAGE`) and the v1 baseline a scatter-gather fails with `UnsupportedByHub`
//!   (nothing is sent), a unary query still works, and a shared query type is not registered
//!   there (the field is ignored: a point query to it is `NoHandler`).
//! - P4b (ADR 0017): the new hub shares `streaming`. Against `kraken-hub:p4a-hub`
//!   (`KRAKEN_HUB_P4A_IMAGE`) a streaming query fails with `UnsupportedByHub` (nothing is sent)
//!   while a unary query and a scatter-gather keep working.
//! - P4c (ADR 0017): the new hub shares `subscription`. Against `kraken-hub:p4b-hub`
//!   (`KRAKEN_HUB_P4B_IMAGE`) a subscription fails with `UnsupportedByHub` (nothing is sent)
//!   while a unary query, a scatter-gather and a streaming query keep working.
//!
//! A missing image fails the test with the command that builds it. No Kafka or MongoDB: the
//! participants are a `HubConnection` and a `Sender` each; a pass-through (`PermitTap`) records the
//! `Permits` and `InstanceStatus` messages a participant writes.
mod support;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use kraken_messaging::hub::{
    CLIENT_VERSION, Dispatcher, HandlerRegistry, Handshake, HubConnection, HubPool, HubSettings,
    Inbound, PERMIT_FLUSH_AFTER, RegisteredTypes, ScatterOptions, SendOptions, Sender,
    SenderSettings, StreamOptions, SubscriptionOptions,
};
use kraken_messaging::identity::IdentityResolver;
use kraken_messaging::proto::{self, ClientMessage, client_message, reply};
use kraken_messaging::{
    Command, MessagingError, Query, RetryPolicy, StreamingQuery, SubscriptionQuery, message_type,
};
use serde::{Deserialize, Serialize};
use support::containers::{HubContainer, HubImage, HubOptions, free_port, hub_with};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Serialize, Deserialize)]
struct Fast {}
message_type!(Fast, "compat.Fast");
impl Command for Fast {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Slow {}
message_type!(Slow, "compat.Slow");
impl Command for Slow {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Hang {}
message_type!(Hang, "compat.Hang");
impl Command for Hang {
    type Reply = ();
}

/// How long the handler side takes per type.
fn handler_delay(type_name: &str) -> Duration {
    match type_name {
        "compat.Fast" => Duration::from_millis(2_000), // under the 5 s default
        "compat.Lookup" | "compat.Shared" => Duration::ZERO,
        "compat.Slow" => Duration::from_millis(3_000), // over its 1.5 s override
        _ => Duration::from_secs(30),                  // compat.Hang: never in time
    }
}

struct Peer {
    conn: Arc<HubConnection>,
    sender: Arc<Sender>,
}

impl Peer {
    async fn shutdown(self) {
        self.conn.shutdown().await;
    }
}

/// A participant: registers `commands`, answers each after [`handler_delay`] on its arrival stream.
async fn peer(address: &str, service: &str, commands: &[&str]) -> Peer {
    peer_with(
        address,
        service,
        RegisteredTypes {
            commands: commands.iter().map(|c| c.to_string()).collect(),
            ..RegisteredTypes::default()
        },
    )
    .await
}

/// A participant registering `types`, answering like [`peer`].
async fn peer_with(address: &str, service: &str, types: RegisteredTypes) -> Peer {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let settings = HubSettings {
        address: address.to_string(),
        backoff_min: Duration::from_millis(500),
        backoff_max: Duration::from_secs(5),
        ..HubSettings::default()
    };
    let conn = HubConnection::new(
        settings,
        service.to_string(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    );
    conn.set_registered_types(types);
    let sender = Sender::new(Arc::clone(&conn), None, false);
    let forward = Arc::clone(&sender);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { request, reply_to } => {
                    tokio::spawn(async move {
                        tokio::time::sleep(handler_delay(&request.r#type)).await;
                        let _ = reply_to
                            .send(ClientMessage {
                                message: Some(client_message::Message::Reply(proto::Reply {
                                    request_id: request.id,
                                    result: Some(reply::Result::Payload(b"{}".to_vec())),
                                })),
                            })
                            .await;
                        reply_to.grant().await; // a flow-controlled hub needs the permit back
                    });
                }
                other => forward.on_inbound(other),
            }
        }
    });
    conn.start();
    assert!(
        conn.await_registered(Duration::from_secs(15)).await,
        "{service} did not register"
    );
    Peer { conn, sender }
}

/// Returns once a `Topology` RPC on `address` succeeds (bounded by 30 s).
async fn wait_until_reachable(address: &str) {
    let target = address.replace("static://", "http://");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let attempt = async {
            let channel = tonic::transport::Endpoint::from_shared(target.clone())
                .ok()?
                .connect()
                .await
                .ok()?;
            proto::hub_client::HubClient::new(channel)
                .topology(proto::TopologyRequest {})
                .await
                .ok()
        };
        if tokio::time::timeout(Duration::from_millis(500), attempt)
            .await
            .ok()
            .flatten()
            .is_some()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "hub at {address} never became reachable"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn topology(hub: &HubContainer) -> serde_json::Value {
    reqwest::get(format!("{}/topology", hub.http))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn new_hub_acks_with_its_configured_values_and_applies_type_overrides() {
    require_docker!();
    let hub = hub_with(
        HubOptions::new(HubImage::Current)
            .env("KRAKEN_HUB_ID", "compat-hub")
            .env("KRAKEN_HUB_TIMEOUT_OVERRIDES", "compat.Slow=1500"),
    )
    .await;
    let b = peer(&hub.address, "compat-b", &["compat.Fast", "compat.Slow"]).await;
    let a = peer(&hub.address, "compat-a", &[]).await;

    let Some(Handshake::V2(ack)) = a.conn.handshake() else {
        panic!("expected a v2 handshake, got {:?}", a.conn.handshake());
    };
    assert_eq!(ack.hub_id, "compat-hub");
    assert_eq!(ack.protocol_version, 2);
    assert_eq!(
        ack.capabilities,
        [
            "flow-control",
            "instance-status",
            "scatter-gather",
            "streaming",
            "subscription"
        ],
        "the hub shares what we declared"
    );
    assert_eq!(ack.heartbeat_interval_ms, 3_000);
    assert_eq!(ack.heartbeat_timeout_ms, 10_000);
    assert_eq!(ack.default_timeout_ms, 5_000);
    assert_eq!(ack.max_timeout_ms, 60_000);
    assert_eq!(ack.context, "default");
    assert_eq!(
        ack.type_timeouts_ms.get("compat.Slow").copied(),
        Some(1_500)
    );
    assert_eq!(ack.type_timeouts_ms.len(), 1);

    // No timeout chosen: compat.Fast gets the hub default (5 s) and its 2 s handler makes it.
    a.sender
        .send_command(&Fast {}, SendOptions::default())
        .await
        .expect("2 s handler within the 5 s default");
    // compat.Slow gets its 1.5 s override from the hub: TIMEOUT from the hub after ~1.5 s.
    let started = Instant::now();
    let err = a
        .sender
        .send_command(&Slow {}, SendOptions::default())
        .await
        .unwrap_err();
    let took = started.elapsed();
    assert!(
        matches!(err, MessagingError::Timeout(ref t) if t == "compat.Slow"),
        "{err:?}"
    );
    assert!(
        took >= Duration::from_millis(1_400) && took < Duration::from_millis(2_400),
        "override not applied: TIMEOUT after {took:?}"
    );
    eprintln!("compat.Slow timed out after {took:?} (override 1500 ms)");

    // The topology shows the hub id and each session's protocol and client version.
    let t = topology(&hub).await;
    assert_eq!(t["hubId"], "compat-hub", "{t}");
    let instances: Vec<&serde_json::Value> = t["services"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["instances"].as_array().unwrap())
        .collect();
    assert_eq!(instances.len(), 2, "{t}");
    for i in instances {
        assert_eq!(i["protocolVersion"], 2, "{t}");
        assert_eq!(i["clientVersion"], CLIENT_VERSION, "{t}");
    }
    a.shutdown().await;
    b.shutdown().await;
}

/// A request in flight when the hub dies (SIGKILL) is `HubConnectionLost`; one sent while no
/// stream is up is `HubUnavailable`.
async fn connection_lost_vs_unavailable(image: HubImage) {
    let hub = hub_with(HubOptions::new(image)).await;
    let b = peer(&hub.address, "compat-b", &["compat.Hang"]).await;
    let a = peer(&hub.address, "compat-a", &[]).await;

    let in_flight = tokio::spawn({
        let sender = Arc::clone(&a.sender);
        async move {
            sender
                .send_command(
                    &Hang {},
                    SendOptions {
                        timeout: Some(Duration::from_secs(30)),
                        ..Default::default()
                    },
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    // SIGKILL: a graceful stop would let the hub answer the request (HANDLER_DISCONNECTED) first.
    let killed = std::process::Command::new("docker")
        .args(["kill", hub.container.id()])
        .output()
        .expect("docker kill");
    assert!(killed.status.success(), "docker kill failed: {killed:?}");
    let lost = tokio::time::timeout(Duration::from_secs(15), in_flight)
        .await
        .expect("in-flight request not failed after the hub stopped")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(lost, MessagingError::HubConnectionLost(_)),
        "{lost:?}"
    );

    let never_sent = a
        .sender
        .send_command(&Fast {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(never_sent, MessagingError::HubUnavailable(_)),
        "{never_sent:?}"
    );
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn new_hub_lost_request_is_connection_lost_and_unsent_is_unavailable() {
    require_docker!();
    connection_lost_vs_unavailable(HubImage::Current).await;
}

#[tokio::test]
async fn baseline_hub_lost_request_is_connection_lost_and_unsent_is_unavailable() {
    require_docker!();
    connection_lost_vs_unavailable(HubImage::Baseline).await;
}

#[tokio::test]
async fn baseline_hub_runs_in_legacy_mode_without_regression() {
    require_docker!();
    let hub = hub_with(HubOptions::new(HubImage::Baseline)).await;
    let b = peer(&hub.address, "compat-b", &["compat.Fast", "compat.Slow"]).await;
    let a = peer(&hub.address, "compat-a", &[]).await;
    assert_eq!(a.conn.handshake(), Some(Handshake::Legacy));
    assert_eq!(b.conn.handshake(), Some(Handshake::Legacy));

    // Legacy: timeout_ms 5000 is sent, so the 2 s handler makes it ...
    a.sender
        .send_command(&Fast {}, SendOptions::default())
        .await
        .expect("round trip through the old hub");
    // ... and the old hub has no overrides: compat.Slow (3 s) succeeds within the 5 s as before.
    a.sender
        .send_command(&Slow {}, SendOptions::default())
        .await
        .expect("no override on the old hub");
    // A chosen timeout still reaches the old hub and is enforced by it.
    let err = a
        .sender
        .send_command(
            &Slow {},
            SendOptions {
                timeout: Some(Duration::from_millis(1_000)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, MessagingError::Timeout(_)), "{err:?}");

    // Both sessions are still registered on the old hub after the exchanges.
    let t = topology(&hub).await;
    let names: Vec<&str> = t["services"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["serviceName"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["compat-a", "compat-b"], "{t}");
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn reconnects_within_the_backoff_cap_after_a_hub_restart() {
    require_docker!();
    reconnects_within_the_backoff_cap(HubImage::Current).await;
}

#[tokio::test]
async fn p0_hub_reconnects_within_the_backoff_cap_after_a_hub_restart() {
    require_docker!();
    reconnects_within_the_backoff_cap(HubImage::P0).await;
}

#[tokio::test]
async fn baseline_hub_reconnects_within_the_backoff_cap_after_a_hub_restart() {
    require_docker!();
    reconnects_within_the_backoff_cap(HubImage::Baseline).await;
}

async fn reconnects_within_the_backoff_cap(image: HubImage) {
    support::init_tracing();
    let port = free_port();
    let hub = hub_with(HubOptions::new(image).port(port)).await;
    let b = peer(&hub.address, "compat-b", &["compat.Fast"]).await;
    let a = peer(&hub.address, "compat-a", &[]).await;

    hub.container.stop_with_timeout(Some(0)).await.unwrap();
    // Long enough for the backoff to climb to its 5 s cap (0.5 + 1 + 2 + 4 s of attempts).
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(a.conn.handshake().is_none() && b.conn.handshake().is_none());
    hub.container.start().await.unwrap();
    // `start` returns before the host port forwarding works again (Rancher Desktop takes seconds):
    // measure from the first gRPC call that reaches the restarted hub from the host.
    let started = Instant::now();
    wait_until_reachable(&hub.address).await;
    let ready = Instant::now();
    eprintln!("hub reachable {:?} after start returned", ready - started);
    let (ra, rb) = tokio::join!(
        a.conn.await_registered(Duration::from_secs(15)),
        b.conn.await_registered(Duration::from_secs(15)),
    );
    let took = ready.elapsed();
    assert!(ra && rb, "did not re-register after the hub restart");
    eprintln!("{image:?}: re-registered {took:?} after the restarted hub was reachable");
    let handshake = a.conn.handshake().unwrap();
    assert_eq!(
        handshake.is_legacy(),
        image == HubImage::Baseline,
        "{handshake:?}"
    );
    // Cap 5 s (jitter never exceeds it) plus connect and handshake time.
    assert!(
        took <= Duration::from_millis(5_500),
        "re-registered only after {took:?}"
    );
    a.sender
        .send_command(&Fast {}, SendOptions::default())
        .await
        .expect("round trip after the restart");
    a.shutdown().await;
    b.shutdown().await;
}

// ---- P1: flow control, priority, retry ----

#[derive(Serialize, Deserialize)]
struct FlowSlow {}
message_type!(FlowSlow, "flow.Slow");
impl Command for FlowSlow {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct FlowGate {}
message_type!(FlowGate, "flow.Gate");
impl Command for FlowGate {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct FlowRec {
    label: String,
}
message_type!(FlowRec, "flow.Rec");
impl Command for FlowRec {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct FlowFail {}
message_type!(FlowFail, "flow.Fail");
impl Command for FlowFail {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct FlowHang {}
message_type!(FlowHang, "flow.Hang");
impl Command for FlowHang {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct FlowZero {}
message_type!(FlowZero, "flow.Zero");
impl Command for FlowZero {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct FlowHangQuery {}
message_type!(FlowHangQuery, "flow.HangQuery");
impl Query for FlowHangQuery {
    type Reply = ();
}

const FLOW_COMMANDS: &[&str] = &[
    "flow.Slow",
    "flow.Gate",
    "flow.Rec",
    "flow.Fail",
    "flow.Hang",
    "flow.Zero",
];
const FLOW_QUERIES: &[&str] = &["flow.HangQuery"];

/// What a flow peer handled, in the order it started handling: (type, payload, priority).
type Handled = Arc<std::sync::Mutex<Vec<(String, String, i32)>>>;

struct FlowPeer {
    peer: Peer,
    handled: Handled,
    /// Opens `flow.Gate` handlers, one per permit added.
    gate: Arc<tokio::sync::Semaphore>,
    /// Most handlers that ran at the same time.
    max_in_flight: Arc<AtomicUsize>,
}

/// A participant offering `permits` concurrent handlers (`max_concurrent_handlers`, which is also
/// `Register.initial_permits`). As a handler (`handles`) it registers the `flow.*` types:
/// `flow.Slow` takes 300 ms, `flow.Gate` waits for a permit of `gate`, `flow.Rec` 20 ms,
/// `flow.Fail` answers `HANDLER_ERROR "handler busy"` (the v1 text of a full hub), `flow.Zero` at
/// once, `flow.Hang` and `flow.HangQuery` 30 s. Every finished request grants its permit back, like
/// the library does.
/// `retry` is the sender's configured default.
async fn flow_peer(
    address: &str,
    service: &str,
    handles: bool,
    permits: usize,
    retry: Option<RetryPolicy>,
) -> FlowPeer {
    let (tx, mut rx) = mpsc::channel::<Inbound>(256);
    let settings = HubSettings {
        address: address.to_string(),
        backoff_min: Duration::from_millis(500),
        backoff_max: Duration::from_secs(5),
        max_concurrent_handlers: permits,
        ..HubSettings::default()
    };
    let conn = HubConnection::new(
        settings,
        service.to_string(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    );
    if handles {
        let names = |types: &[&str]| types.iter().map(|t| t.to_string()).collect();
        conn.set_types(names(FLOW_COMMANDS), names(FLOW_QUERIES));
    }
    let sender = Sender::with_settings(
        Arc::clone(&conn),
        SenderSettings {
            forward_jwt: false,
            retry,
            ..SenderSettings::default()
        },
    );
    let handled: Handled = Arc::default();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let forward = Arc::clone(&sender);
    let (log, g, now, max) = (
        Arc::clone(&handled),
        Arc::clone(&gate),
        Arc::clone(&in_flight),
        Arc::clone(&max_in_flight),
    );
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { request, reply_to } => {
                    let (log, g, now, max) = (
                        Arc::clone(&log),
                        Arc::clone(&g),
                        Arc::clone(&now),
                        Arc::clone(&max),
                    );
                    tokio::spawn(async move {
                        max.fetch_max(now.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                        log.lock().unwrap().push((
                            request.r#type.clone(),
                            String::from_utf8_lossy(&request.payload).into_owned(),
                            request.priority,
                        ));
                        let result = match request.r#type.as_str() {
                            "flow.Slow" => {
                                tokio::time::sleep(Duration::from_millis(300)).await;
                                reply::Result::Payload(b"{}".to_vec())
                            }
                            "flow.Gate" => {
                                g.acquire().await.unwrap().forget();
                                reply::Result::Payload(b"{}".to_vec())
                            }
                            "flow.Rec" => {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                                reply::Result::Payload(b"{}".to_vec())
                            }
                            "flow.Zero" => reply::Result::Payload(b"{}".to_vec()),
                            "flow.Fail" => reply::Result::Error(proto::Error {
                                request_id: request.id.clone(),
                                code: proto::ErrorCode::HandlerError as i32,
                                message: "handler busy".into(),
                                details: vec![],
                            }),
                            _ => {
                                tokio::time::sleep(Duration::from_secs(30)).await;
                                reply::Result::Payload(b"{}".to_vec())
                            }
                        };
                        now.fetch_sub(1, Ordering::SeqCst);
                        let _ = reply_to
                            .send(ClientMessage {
                                message: Some(client_message::Message::Reply(proto::Reply {
                                    request_id: request.id,
                                    result: Some(result),
                                })),
                            })
                            .await;
                        reply_to.grant().await;
                    });
                }
                other => forward.on_inbound(other),
            }
        }
    });
    conn.start();
    assert!(
        conn.await_registered(Duration::from_secs(15)).await,
        "{service} did not register"
    );
    FlowPeer {
        peer: Peer { conn, sender },
        handled,
        gate,
        max_in_flight,
    }
}

impl FlowPeer {
    fn count(&self, type_name: &str) -> usize {
        self.handled
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _, _)| t == type_name)
            .count()
    }

    /// Waits until `n` requests reached the handler (bounded by 10 s).
    async fn handled_at_least(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.handled.lock().unwrap().len() < n {
            assert!(
                Instant::now() < deadline,
                "only {:?} handled",
                self.handled.lock().unwrap()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// A gRPC pass-through between one participant and a real hub that adds up the `Permits` the
/// participant writes: a hub image gives no view of what a client sent.
struct PermitTap {
    address: String,
    permits: Arc<AtomicU64>,
    statuses: Arc<AtomicU64>,
}

impl PermitTap {
    async fn start(upstream: &str) -> PermitTap {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tap = Tap {
            upstream: upstream.replace("static://", "http://"),
            permits: Arc::default(),
            statuses: Arc::default(),
        };
        let (permits, statuses) = (Arc::clone(&tap.permits), Arc::clone(&tap.statuses));
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(proto::hub_server::HubServer::new(tap))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        PermitTap {
            address: format!("static://{addr}"),
            permits,
            statuses,
        }
    }

    /// Permits written so far (the sum of the counts).
    fn permits(&self) -> u64 {
        self.permits.load(Ordering::SeqCst)
    }

    /// `InstanceStatus` messages written so far.
    fn statuses(&self) -> u64 {
        self.statuses.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
struct Tap {
    upstream: String,
    permits: Arc<AtomicU64>,
    statuses: Arc<AtomicU64>,
}

impl Tap {
    /// The RPC is called `Connect`, so the generated client has no `connect(url)` constructor.
    async fn hub(&self) -> Result<proto::hub_client::HubClient<tonic::transport::Channel>, Status> {
        let channel = tonic::transport::Endpoint::from_shared(self.upstream.clone())
            .map_err(|e| Status::internal(e.to_string()))?
            .connect()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        Ok(proto::hub_client::HubClient::new(channel))
    }
}

#[tonic::async_trait]
impl proto::hub_server::Hub for Tap {
    type ConnectStream =
        Pin<Box<dyn Stream<Item = Result<proto::ServerMessage, Status>> + Send + 'static>>;

    async fn connect(
        &self,
        request: tonic::Request<tonic::Streaming<ClientMessage>>,
    ) -> Result<tonic::Response<Self::ConnectStream>, Status> {
        let (permits, statuses) = (Arc::clone(&self.permits), Arc::clone(&self.statuses));
        let outbound = request.into_inner().map_while(Result::ok).map(move |m| {
            match &m.message {
                Some(client_message::Message::Permits(p)) => {
                    permits.fetch_add(p.count.into(), Ordering::SeqCst);
                }
                Some(client_message::Message::Status(_)) => {
                    statuses.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
            m
        });
        let inbound = self.hub().await?.connect(outbound).await?.into_inner();
        Ok(tonic::Response::new(Box::pin(inbound)))
    }

    async fn topology(
        &self,
        request: tonic::Request<proto::TopologyRequest>,
    ) -> Result<tonic::Response<proto::TopologyResponse>, Status> {
        self.hub().await?.topology(request.into_inner()).await
    }
}

/// `service`'s only instance in the hub's topology.
async fn instance_view(hub: &HubContainer, service: &str) -> serde_json::Value {
    let t = topology(hub).await;
    t["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["serviceName"] == service)
        .unwrap_or_else(|| panic!("{service} not in {t}"))["instances"][0]
        .clone()
}

/// Waits until the hub's topology shows `count` requests queued for `service` (bounded by 10 s).
async fn await_queued(hub: &HubContainer, service: &str, count: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let view = instance_view(hub, service).await;
        if view["queued"].as_u64().unwrap_or(0) >= count {
            return;
        }
        assert!(Instant::now() < deadline, "never {count} queued: {view}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// No retry configured on the sender.
fn no_retry() -> Option<RetryPolicy> {
    None
}

#[tokio::test]
async fn new_hub_turns_a_slow_handler_with_one_permit_into_latency_not_timeouts() {
    require_docker!();
    let hub = hub_with(HubOptions::new(HubImage::Current)).await;
    let tap = PermitTap::start(&hub.address).await;
    let b = flow_peer(&tap.address, "flow-b", true, 1, no_retry()).await;
    let a = flow_peer(&hub.address, "flow-a", false, 256, no_retry()).await;
    let handshake = b.peer.conn.handshake().unwrap();
    assert!(handshake.flow_control(), "{handshake:?}");

    let started = Instant::now();
    let calls: Vec<_> = (0..5)
        .map(|_| {
            let s = Arc::clone(&a.peer.sender);
            tokio::spawn(async move { s.send_command(&FlowSlow {}, SendOptions::default()).await })
        })
        .collect();
    // While they wait, the hub shows the instance as flow-controlled with its queue.
    let mut saw_queue = false;
    for _ in 0..20 {
        let view = instance_view(&hub, "flow-b").await;
        assert_eq!(view["flowControlled"], true, "{view}");
        assert!(view["permitsAvailable"].as_u64().unwrap() <= 1, "{view}");
        saw_queue |= view["queued"].as_u64().unwrap() > 0;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for c in calls {
        c.await.unwrap().expect("queued, not timed out");
    }
    let elapsed = started.elapsed();
    eprintln!("5 calls to a 300 ms handler with one permit took {elapsed:?}");
    assert!(saw_queue, "the hub never queued a request");
    assert!(
        elapsed >= Duration::from_millis(1_500),
        "five 300 ms requests with one permit finished in {elapsed:?}"
    );
    assert_eq!(b.max_in_flight.load(Ordering::SeqCst), 1, "one at a time");
    // With one permit every grant is written at once; give the last one time to pass the tap.
    tokio::time::sleep(PERMIT_FLUSH_AFTER * 3).await;
    assert!(
        tap.permits() >= 5,
        "one permit back per request, got {}",
        tap.permits()
    );
    a.peer.shutdown().await;
    b.peer.shutdown().await;
}

/// One permit and a handler that takes no time: while a request runs the hub holds no permit for
/// the instance, so each completion returns its permit at once. Waiting for the 50 ms flush timer
/// instead would pace the instance at ~20 requests/s (100 calls in ~5 s).
#[tokio::test]
async fn new_hub_gets_a_single_permit_back_at_once_so_a_burst_is_not_paced_by_the_timer() {
    require_docker!();
    let hub = hub_with(HubOptions::new(HubImage::Current)).await;
    let b = flow_peer(&hub.address, "flow-b", true, 1, no_retry()).await;
    let a = flow_peer(&hub.address, "flow-a", false, 256, no_retry()).await;
    assert!(b.peer.conn.handshake().unwrap().flow_control());

    let started = Instant::now();
    let calls: Vec<_> = (0..100)
        .map(|_| {
            let s = Arc::clone(&a.peer.sender);
            tokio::spawn(async move { s.send_command(&FlowZero {}, SendOptions::default()).await })
        })
        .collect();
    for c in calls {
        c.await.unwrap().expect("queued, then served");
    }
    let elapsed = started.elapsed();
    eprintln!("100 calls to a zero-time handler with one permit took {elapsed:?}");
    assert_eq!(b.count("flow.Zero"), 100);
    assert_eq!(b.max_in_flight.load(Ordering::SeqCst), 1, "one at a time");
    assert!(
        elapsed < Duration::from_secs(1),
        "100 calls through one permit took {elapsed:?}"
    );
    a.peer.shutdown().await;
    b.peer.shutdown().await;
}

#[tokio::test]
async fn new_hub_full_queue_is_handler_busy_and_a_retried_command_gets_through() {
    require_docker!();
    let hub = hub_with(
        HubOptions::new(HubImage::Current).env("KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE", "1"),
    )
    .await;
    let b = flow_peer(&hub.address, "flow-b", true, 1, no_retry()).await;
    let a = flow_peer(&hub.address, "flow-a", false, 256, no_retry()).await;
    let gated = || {
        let s = Arc::clone(&a.peer.sender);
        tokio::spawn(async move { s.send_command(&FlowGate {}, SendOptions::default()).await })
    };
    let rec = || FlowRec {
        label: "after-busy".into(),
    };

    let first = gated(); // takes the only permit
    b.handled_at_least(1).await;
    let second = gated(); // fills the queue (capacity 1)
    await_queued(&hub, "flow-b", 1).await;

    let busy = a
        .peer
        .sender
        .send_command(
            &rec(),
            SendOptions {
                retry: Some(RetryPolicy::NONE),
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(busy, Err(MessagingError::HandlerBusy(ref t)) if t == "flow.Rec"),
        "{busy:?}"
    );

    let retried = {
        let s = Arc::clone(&a.peer.sender);
        let command = rec();
        tokio::spawn(async move {
            s.send_command(
                &command,
                SendOptions {
                    timeout: Some(Duration::from_secs(10)),
                    retry: Some(RetryPolicy {
                        max_attempts: 10,
                        initial_backoff: Duration::from_millis(200),
                        ..RetryPolicy::default()
                    }),
                    ..Default::default()
                },
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    b.gate.add_permits(2); // the queue drains; a later attempt finds room
    tokio::time::timeout(Duration::from_secs(15), retried)
        .await
        .expect("retried command still pending")
        .unwrap()
        .expect("retried past the full queue");
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(
        b.count("flow.Rec"),
        1,
        "rejected attempts never reached the handler"
    );
    a.peer.shutdown().await;
    b.peer.shutdown().await;
}

#[tokio::test]
async fn new_hub_retry_never_repeats_a_handler_error_or_a_timed_out_command() {
    require_docker!();
    // The hub ends both hang types after 300 ms, well inside the 3 s budget the caller chooses, so
    // only the classification can stop a retry (a query is retried, a command is not).
    let hub = hub_with(HubOptions::new(HubImage::Current).env(
        "KRAKEN_HUB_TIMEOUT_OVERRIDES",
        "flow.Hang=300,flow.HangQuery=300",
    ))
    .await;
    let b = flow_peer(&hub.address, "flow-b", true, 8, no_retry()).await;
    let retry = RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(50),
        ..RetryPolicy::default()
    };
    let a = flow_peer(&hub.address, "flow-a", false, 256, Some(retry)).await;
    let budget = || SendOptions {
        timeout: Some(Duration::from_secs(3)),
        ..Default::default()
    };

    let failed = a.peer.sender.send_command(&FlowFail {}, budget()).await;
    assert!(
        matches!(failed, Err(MessagingError::RemoteHandler { ref message, .. }) if message == "handler busy"),
        "{failed:?}"
    );

    let started = Instant::now();
    let timed_out = a.peer.sender.send_command(&FlowHang {}, budget()).await;
    assert!(
        matches!(timed_out, Err(MessagingError::Timeout(ref t)) if t == "flow.Hang"),
        "{timed_out:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );

    let query = a.peer.sender.send_query(&FlowHangQuery {}, budget()).await;
    assert!(
        matches!(query, Err(MessagingError::Timeout(_))),
        "{query:?}"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(b.count("flow.Fail"), 1, "a handler error is not retried");
    assert_eq!(
        b.count("flow.Hang"),
        1,
        "a timed-out command is not retried"
    );
    assert_eq!(b.count("flow.HangQuery"), 3, "a timed-out query is retried");
    a.peer.shutdown().await;
    b.peer.shutdown().await;
}

#[tokio::test]
async fn new_hub_serves_queued_requests_by_priority() {
    require_docker!();
    let hub = hub_with(HubOptions::new(HubImage::Current)).await;
    let b = flow_peer(&hub.address, "flow-b", true, 1, no_retry()).await;
    let a = flow_peer(&hub.address, "flow-a", false, 256, no_retry()).await;

    // B's only permit is taken by a request that waits for the gate.
    let blocker = {
        let s = Arc::clone(&a.peer.sender);
        tokio::spawn(async move { s.send_command(&FlowGate {}, SendOptions::default()).await })
    };
    b.handled_at_least(1).await;
    // Three more queue at the hub, in this arrival order: low, low, high.
    let mut queued = Vec::new();
    for (n, (label, priority)) in [("low-1", 0), ("low-2", 0), ("high", 10)]
        .into_iter()
        .enumerate()
    {
        let s = Arc::clone(&a.peer.sender);
        queued.push(tokio::spawn(async move {
            let command = FlowRec {
                label: label.into(),
            };
            let opts = SendOptions {
                priority: Some(priority),
                ..Default::default()
            };
            s.send_command(&command, opts).await
        }));
        await_queued(&hub, "flow-b", n as u64 + 1).await;
    }
    b.gate.add_permits(1);
    blocker.await.unwrap().unwrap();
    for q in queued {
        q.await.unwrap().unwrap();
    }
    let order: Vec<(String, i32)> = b
        .handled
        .lock()
        .unwrap()
        .iter()
        .skip(1)
        .map(|(_, payload, priority)| {
            let body: serde_json::Value = serde_json::from_str(payload).unwrap();
            (body["label"].as_str().unwrap().to_string(), *priority)
        })
        .collect();
    assert_eq!(
        order,
        [
            ("high".to_string(), 10),
            ("low-1".to_string(), 0),
            ("low-2".to_string(), 0)
        ],
        "higher priority first, then arrival"
    );
    a.peer.shutdown().await;
    b.peer.shutdown().await;
}

/// Older hubs (P0: v2 without flow control; v1 baseline) ignore the new `Register` fields and
/// forward without permits: the P1 client sends no `Permits`, never stalls and works as before.
async fn older_hub_works_as_before(image: HubImage) {
    let hub = hub_with(HubOptions::new(image)).await;
    let tap = PermitTap::start(&hub.address).await;
    let b = flow_peer(&tap.address, "flow-b", true, 2, no_retry()).await;
    let a = flow_peer(&hub.address, "flow-a", false, 256, no_retry()).await;
    let handshake = b.peer.conn.handshake().unwrap();
    assert!(!handshake.flow_control(), "{handshake:?}");
    match image {
        HubImage::Baseline => assert!(handshake.is_legacy(), "{handshake:?}"),
        _ => {
            let ack = handshake.ack().expect("a v2 ack");
            assert_eq!(ack.protocol_version, 2);
            assert!(ack.capabilities.is_empty(), "{ack:?}");
        }
    }

    // More requests than the permits offered: an older hub forwards them all at once.
    let started = Instant::now();
    let calls: Vec<_> = (0..6)
        .map(|i| {
            let s = Arc::clone(&a.peer.sender);
            tokio::spawn(async move {
                let opts = SendOptions {
                    priority: Some(i % 3), // ignored by an older hub
                    ..Default::default()
                };
                s.send_command(&FlowSlow {}, opts).await
            })
        })
        .collect();
    for c in calls {
        c.await.unwrap().expect("round trip through an older hub");
    }
    eprintln!(
        "{image:?}: 6 calls to a 300 ms handler offering 2 permits took {:?}",
        started.elapsed()
    );
    assert_eq!(b.count("flow.Slow"), 6);
    assert!(
        b.max_in_flight.load(Ordering::SeqCst) > 2,
        "an older hub does not wait for permits"
    );
    let unknown = a
        .peer
        .sender
        .send_command(&Fast {}, SendOptions::default())
        .await;
    assert!(
        matches!(unknown, Err(MessagingError::NoHandler(_))),
        "{unknown:?}"
    );
    tokio::time::sleep(PERMIT_FLUSH_AFTER * 3).await;
    assert_eq!(
        tap.permits(),
        0,
        "permits sent to a hub without flow control"
    );
    a.peer.shutdown().await;
    b.peer.shutdown().await;
}

#[tokio::test]
async fn p0_hub_acks_without_flow_control_and_works_as_before() {
    require_docker!();
    older_hub_works_as_before(HubImage::P0).await;
}

#[tokio::test]
async fn baseline_hub_works_as_before_without_flow_control() {
    require_docker!();
    older_hub_works_as_before(HubImage::Baseline).await;
}

// ---- P3: multi-hub client against one hub ----

/// One target through a `HubPool` (what `KrakenMessaging` now uses) behaves as the single-hub
/// client: registered in the hub's mode, round trips work, and `InstanceStatus` is written only to
/// a hub that shares `instance-status` (the P3 hub; not P2, not v1).
async fn single_target_pool(image: HubImage) {
    let hub = hub_with(HubOptions::new(image)).await;
    let tap = PermitTap::start(&hub.address).await;
    let b = peer(&hub.address, "compat-b", &["compat.Fast"]).await;
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let pool = HubPool::new(
        HubSettings {
            address: tap.address.clone(),
            status_interval: Duration::from_millis(100),
            ..HubSettings::default()
        },
        "compat-pool".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    let sender = Sender::with_pool(Arc::clone(&pool), SenderSettings::default());
    let forward = Arc::clone(&sender);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            forward.on_inbound(msg);
        }
    });
    pool.start();
    assert!(pool.await_registered(Duration::from_secs(15)).await);
    let handshake = pool.connections()[0].handshake().unwrap();
    let shares_status = image == HubImage::P3;
    assert_eq!(
        handshake.is_legacy(),
        image == HubImage::Baseline,
        "{handshake:?}"
    );
    assert_eq!(handshake.instance_status(), shares_status, "{handshake:?}");
    let expected_id = match &handshake {
        Handshake::V2(ack) => ack.hub_id.clone(),
        // A legacy hub is known by its target as host:port (shared with kraken-messaging).
        Handshake::Legacy => tap.address.trim_start_matches("static://").to_string(),
    };
    assert_eq!(pool.hub_ids(), [expected_id]);
    sender
        .send_command(&Fast {}, SendOptions::default())
        .await
        .expect("round trip through the single hub");
    tokio::time::sleep(Duration::from_millis(600)).await;
    if shares_status {
        assert!(
            tap.statuses() >= 3,
            "{} reports to the P3 hub",
            tap.statuses()
        );
        let view = instance_view(&hub, "compat-pool").await;
        assert!(view["statusAgeMs"].as_i64().unwrap() >= 0, "{view}");
        assert_eq!(
            view["connectedHubIds"].as_array().unwrap().len(),
            1,
            "{view}"
        );
    } else {
        assert_eq!(tap.statuses(), 0, "InstanceStatus written to {image:?}");
    }
    pool.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn p3_hub_single_target_gets_instance_status() {
    require_docker!();
    single_target_pool(HubImage::P3).await;
}

#[tokio::test]
async fn p2_hub_single_target_works_and_gets_no_instance_status() {
    require_docker!();
    single_target_pool(HubImage::P2).await;
}

#[tokio::test]
async fn baseline_hub_single_target_pool_runs_in_legacy_mode_without_instance_status() {
    require_docker!();
    single_target_pool(HubImage::Baseline).await;
}

#[derive(Serialize, Deserialize)]
struct Lookup {}
message_type!(Lookup, "compat.Lookup");
impl Query for Lookup {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct SharedLookup {}
message_type!(SharedLookup, "compat.Shared");
impl Query for SharedLookup {
    type Reply = ();
}

/// A hub before P4a: no `scatter-gather` in its ack (none at all for the baseline), so a
/// scatter-gather fails with `UnsupportedByHub` before anything is sent, while a unary query to an
/// exclusive owner works as before. The hub ignores `shared_query_types`: the registration is
/// accepted, the shared type is simply not there.
async fn scatter_gather_unsupported(image: HubImage) {
    let hub = hub_with(HubOptions::new(image)).await;
    let owner = peer_with(
        &hub.address,
        "compat-owner",
        RegisteredTypes {
            queries: vec!["compat.Lookup".into()],
            shared_queries: vec!["compat.Shared".into()],
            ..RegisteredTypes::default()
        },
    )
    .await;
    let a = peer(&hub.address, "compat-a", &[]).await;
    let handshake = a.conn.handshake().unwrap();
    assert!(!handshake.scatter_gather(), "{handshake:?}");

    let e = a
        .sender
        .scatter_gather(&Lookup {}, ScatterOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(e, MessagingError::UnsupportedByHub(_)),
        "{image:?}: {e:?}"
    );
    assert_eq!(a.sender.in_flight(), 0);
    a.sender
        .send_query(&Lookup {}, SendOptions::default())
        .await
        .expect("unary query on an older hub");
    let e = a
        .sender
        .send_query(&SharedLookup {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(e, MessagingError::NoHandler(_)),
        "{image:?}: {e:?}"
    );
    a.shutdown().await;
    owner.shutdown().await;
}

#[tokio::test]
async fn p3_hub_scatter_gather_is_unsupported_by_hub_and_unary_still_works() {
    require_docker!();
    scatter_gather_unsupported(HubImage::P3).await;
}

#[tokio::test]
async fn baseline_hub_scatter_gather_is_unsupported_by_hub_and_unary_still_works() {
    require_docker!();
    scatter_gather_unsupported(HubImage::Baseline).await;
}

/// `compat.Lookup` as a streaming query: the P4a hub knows the mode and rejects it.
#[derive(Serialize, Deserialize)]
struct StreamLookup {}
message_type!(StreamLookup, "compat.Lookup");
impl StreamingQuery for StreamLookup {
    type Item = ();
}

/// A P4a hub (ADR 0017, P4b): it shares `scatter-gather` but not `streaming`, so a streaming
/// query fails with `UnsupportedByHub` before anything is sent, while a unary query and a
/// scatter-gather to the same owner work as before.
#[tokio::test]
async fn p4a_hub_streaming_is_unsupported_by_hub_and_unary_and_gathers_still_work() {
    require_docker!();
    let hub = hub_with(HubOptions::new(HubImage::P4a)).await;
    let owner = peer_with(
        &hub.address,
        "compat-owner",
        RegisteredTypes {
            queries: vec!["compat.Lookup".into()],
            ..RegisteredTypes::default()
        },
    )
    .await;
    let a = peer(&hub.address, "compat-a", &[]).await;
    let handshake = a.conn.handshake().unwrap();
    assert!(handshake.scatter_gather(), "{handshake:?}");
    assert!(!handshake.streaming(), "{handshake:?}");

    let e = a
        .sender
        .stream(&StreamLookup {}, StreamOptions::default())
        .await
        .err()
        .expect("unsupported by a P4a hub");
    assert!(matches!(e, MessagingError::UnsupportedByHub(_)), "{e:?}");
    assert_eq!(a.sender.in_flight(), 0);
    a.sender
        .send_query(&Lookup {}, SendOptions::default())
        .await
        .expect("unary query on a P4a hub");
    let gathered = a
        .sender
        .scatter_gather(&Lookup {}, ScatterOptions::default())
        .await
        .expect("scatter-gather on a P4a hub");
    assert_eq!(gathered.parts.len(), 1);
    assert_eq!(a.sender.in_flight(), 0);
    a.shutdown().await;
    owner.shutdown().await;
}

/// `compat.Rows`: a streaming query a P4b hub serves (two items).
#[derive(Serialize, Deserialize)]
struct Rows {}
message_type!(Rows, "compat.Rows");
impl StreamingQuery for Rows {
    type Item = ();
}

/// `compat.Feed`: a subscription query, which only a P4c hub routes.
#[derive(Serialize, Deserialize)]
struct Feed {}
message_type!(Feed, "compat.Feed");
impl SubscriptionQuery for Feed {
    type Initial = ();
    type Update = ();
}

/// The handlers of the owner: `compat.Lookup` (unary), `compat.Rows` (streaming) and
/// `compat.Feed` (subscription).
fn owner_registry() -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    registry.shared_query::<Lookup, _, _>(|_, _| async move { Ok(()) });
    registry.streaming_query::<Rows, _, _, _>(|_, _| async move {
        Ok(tokio_stream::iter(vec![Ok(()), Ok(())]))
    });
    registry.subscription_query::<Feed, _, _>(|_, _| async move { Ok(()) });
    registry
}

/// An instance of `service` served by the library's own dispatcher with `registry`.
async fn library_node(
    address: &str,
    service: &str,
    registry: HandlerRegistry,
) -> (Arc<HubPool>, Arc<Sender>) {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let pool = HubPool::new(
        HubSettings {
            address: address.to_string(),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(5),
            ..HubSettings::default()
        },
        service.to_string(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    pool.set_registered_types(registry.registered_types());
    let sender = Sender::with_pool(Arc::clone(&pool), SenderSettings::default());
    let dispatcher = Arc::new(Dispatcher::new(
        Arc::new(registry),
        Arc::new(IdentityResolver::new(None)),
        8,
    ));
    let forward = Arc::clone(&sender);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { request, reply_to } => {
                    tokio::spawn(dispatcher.serve(request, reply_to));
                }
                Inbound::Flow(f) => dispatcher.flow(&f.request_id, f.credits),
                Inbound::Cancel(c) => dispatcher.cancel(&c.request_id, &c.reason),
                Inbound::Disconnected { stream, .. } => {
                    dispatcher.stream_lost(stream);
                    forward.on_inbound(msg);
                }
                other => forward.on_inbound(other),
            }
        }
    });
    pool.start();
    assert!(
        pool.await_all_registered(Duration::from_secs(15)).await,
        "{service} did not register"
    );
    (pool, sender)
}

/// A P4b hub (ADR 0017, P4c): it shares `streaming` but not `subscription`, so a subscription
/// fails with `UnsupportedByHub` before anything is sent, while a unary query, a scatter-gather
/// and a streaming query to the same owner work as before.
#[tokio::test]
async fn p4b_hub_subscription_is_unsupported_by_hub_and_unary_gathers_and_streams_still_work() {
    require_docker!();
    let hub = hub_with(HubOptions::new(HubImage::P4b)).await;
    let (owner_pool, _) = library_node(&hub.address, "compat-owner", owner_registry()).await;
    let (second_pool, _) = library_node(&hub.address, "compat-owner-2", {
        // A second owner of the shared `compat.Lookup`, so the gather has two parts.
        let mut r = HandlerRegistry::new();
        r.shared_query::<Lookup, _, _>(|_, _| async move { Ok(()) });
        r
    })
    .await;
    let (caller_pool, caller) =
        library_node(&hub.address, "compat-caller", HandlerRegistry::new()).await;
    let handshake = caller_pool.handshake_for("").unwrap();
    assert!(handshake.streaming(), "{handshake:?}");
    assert!(!handshake.subscription(), "{handshake:?}");

    let e = caller
        .subscribe(&Feed {}, SubscriptionOptions::default())
        .await
        .err()
        .expect("unsupported by a P4b hub");
    assert!(
        matches!(e, MessagingError::UnsupportedByHub(ref m) if m.contains("subscription")),
        "{e:?}"
    );
    assert_eq!(caller.in_flight(), 0);
    caller
        .send_query(&Lookup {}, SendOptions::default())
        .await
        .expect("unary query on a P4b hub");
    let gathered = caller
        .scatter_gather(&Lookup {}, ScatterOptions::default())
        .await
        .expect("scatter-gather on a P4b hub");
    assert_eq!(gathered.parts.len(), 2, "both owners answer");
    let items: Vec<_> = caller
        .stream(&Rows {}, StreamOptions::default())
        .await
        .expect("streaming query on a P4b hub")
        .collect()
        .await;
    assert_eq!(items.len(), 2);
    assert!(items.iter().all(Result::is_ok), "{items:?}");
    assert_eq!(caller.in_flight(), 0);
    caller_pool.shutdown().await;
    second_pool.shutdown().await;
    owner_pool.shutdown().await;
}
