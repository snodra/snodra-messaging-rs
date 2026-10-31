//! Compatibility suite against real hub images (roadmap rule: every phase checks new clients
//! against the new hub and against the previous hub image).
//!
//! - `kraken-hub:dev` (`KRAKEN_HUB_IMAGE`): protocol v2. The ack carries the configured values,
//!   per-type timeout overrides apply to requests sent with `timeout_ms` 0, a request lost with the
//!   stream is `HubConnectionLost`, one never written is `HubUnavailable`, and the client is back
//!   within the 5 s backoff cap after a hub restart.
//! - `kraken-hub:v1-baseline` (`KRAKEN_HUB_BASELINE_IMAGE`): the hub before protocol v2. The client
//!   detects it (echo without ack) and works exactly as before.
//!
//! No Kafka or MongoDB: the participants are a `HubConnection` and a `Sender` each.
mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use kraken_messaging::hub::{
    CLIENT_VERSION, Handshake, HubConnection, HubSettings, Inbound, SendOptions, Sender,
};
use kraken_messaging::proto::{self, ClientMessage, client_message, reply};
use kraken_messaging::{Command, MessagingError, message_type};
use serde::{Deserialize, Serialize};
use support::containers::{HubContainer, HubImage, HubOptions, free_port, hub_with};
use tokio::sync::mpsc;

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
    conn.set_types(commands.iter().map(|c| c.to_string()).collect(), vec![]);
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
    assert!(ack.capabilities.is_empty());
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
    support::init_tracing();
    let port = free_port();
    let hub = hub_with(HubOptions::new(HubImage::Current).port(port)).await;
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
    eprintln!("re-registered {took:?} after the restarted hub was reachable");
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
