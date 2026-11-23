//! Multi-hub against in-process fake hubs: one connection per hub with the identical
//! registration, permits split, routing (round-robin, rendezvous by key), failover of requests
//! never written, `HubConnectionLost` for written ones, dedupe by hub id, `InstanceStatus` to every
//! hub, and `dns:///` re-resolution.
mod support;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use snodra_messaging::hub::{
    HubPool, HubSettings, Inbound, ReplyTo, SendOptions, Sender, SenderSettings, rendezvous_score,
};
use snodra_messaging::proto::{self, ServerMessage, reply, server_message};
use snodra_messaging::{Command, MessagingError, Query, RetryPolicy, message_type};
use support::fake_hub::{Event, FakeHub, default_ack};
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize)]
struct Ping {}
message_type!(Ping, "t.Ping");
impl Command for Ping {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Look {}
message_type!(Look, "t.Look");
impl Query for Look {
    type Reply = ();
}

fn ok_reply(req: &proto::Request) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Reply(proto::Reply {
            request_id: req.id.clone(),
            result: Some(reply::Result::Payload(b"{}".to_vec())),
        })),
    }
}

async fn answering(id: &str) -> FakeHub {
    let hub = FakeHub::start_with_id(id).await;
    hub.set_responder(|req| Some(ok_reply(&req))).await;
    hub
}

struct Client {
    pool: Arc<HubPool>,
    sender: Arc<Sender>,
    /// Requests pushed by the hubs, held until the test finishes them.
    held: Arc<Mutex<Vec<ReplyTo>>>,
}

impl Client {
    async fn shutdown(self) {
        self.pool.shutdown().await;
    }
}

fn settings(address: String) -> HubSettings {
    HubSettings {
        address,
        backoff_min: Duration::from_millis(50),
        backoff_max: Duration::from_millis(200),
        max_concurrent_handlers: 10,
        status_interval: Duration::from_millis(100),
        ..HubSettings::default()
    }
}

/// A started pool with a sender; requests pushed by a hub are held, not answered.
async fn client(settings: HubSettings) -> Client {
    client_with(settings, |_| {}).await
}

async fn client_with(settings: HubSettings, before_start: impl FnOnce(&HubPool)) -> Client {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let pool = HubPool::new(settings, "svc".into(), uuid::Uuid::new_v4().to_string(), tx)
        .expect("valid hub address");
    pool.set_types(vec!["t.Handled".into()], vec![]);
    before_start(&pool);
    let sender = Sender::with_pool(Arc::clone(&pool), SenderSettings::default());
    let held = Arc::new(Mutex::new(Vec::new()));
    let (forward, keep) = (Arc::clone(&sender), Arc::clone(&held));
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { reply_to, .. } => keep.lock().unwrap().push(reply_to),
                other => forward.on_inbound(other),
            }
        }
    });
    pool.start();
    Client { pool, sender, held }
}

fn addresses(hubs: &[&FakeHub]) -> String {
    hubs.iter()
        .map(|h| h.address())
        .collect::<Vec<_>>()
        .join(",")
}

/// Waits (bounded) until `check` holds.
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn all_registered(c: &Client, ids: &[&str]) {
    eventually(&format!("registered on {ids:?}"), async || {
        c.pool.hub_ids() == ids
    })
    .await;
}

fn push_request(id: &str) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Request(proto::Request {
            id: id.into(),
            kind: proto::RequestKind::Command as i32,
            r#type: "t.Handled".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        })),
    }
}

#[tokio::test]
async fn registers_identically_on_every_hub_and_splits_the_permits() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    let (ra, rb) = (a.registers().await, b.registers().await);
    assert_eq!((ra.len(), rb.len()), (1, 1));
    assert_eq!(ra[0], rb[0], "the identical Register on every hub");
    assert_eq!(ra[0].command_types, ["t.Handled"]);
    assert_eq!(ra[0].initial_permits, 5, "ceil(10 / 2)");
    assert!(c.pool.await_registered(Duration::from_millis(10)).await);
    c.shutdown().await;
}

/// Every stream offers at least one permit (the hub refuses 0): a pool without handlers to share
/// is refused when it is built.
#[test]
fn max_concurrent_handlers_of_zero_is_refused() {
    let (tx, _rx) = mpsc::channel(1);
    let s = HubSettings {
        max_concurrent_handlers: 0,
        ..settings("static://127.0.0.1:1,static://127.0.0.1:2".into())
    };
    let refused = HubPool::new(s, "svc".into(), "i".into(), tx);
    assert!(
        matches!(refused, Err(MessagingError::InvalidMessage(ref m)) if m.contains("max_concurrent_handlers")),
        "{:?}",
        refused.err()
    );
}

/// N counts the hubs live (or on their first attempt) when the Register is written: a stream
/// re-opened while the other hub is down offers all the permits; nothing is revoked.
#[tokio::test]
async fn a_stream_opened_while_the_other_hub_is_down_offers_all_permits() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    b.set_down(true);
    all_registered(&c, &["hub-a"]).await;
    a.drop_streams();
    eventually("hub-a re-registered", async || {
        a.registers().await.len() == 2
    })
    .await;
    let permits: Vec<u32> = a
        .registers()
        .await
        .iter()
        .map(|r| r.initial_permits)
        .collect();
    assert_eq!(permits, [5, 10]);
    b.set_down(false);
    all_registered(&c, &["hub-a", "hub-b"]).await;
    let permits: Vec<u32> = b
        .registers()
        .await
        .iter()
        .map(|r| r.initial_permits)
        .collect();
    assert_eq!(permits, [5, 5], "back with two live hubs: half each");
    c.shutdown().await;
}

/// `await_registered` is "any hub" (the client can send), `await_all_registered` "every hub"
/// (snodra-messaging's `awaitRegistered`): a hub that is down holds back only the latter.
#[tokio::test]
async fn await_registered_waits_for_any_hub_and_await_all_registered_for_every_hub() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    b.set_down(true);
    let c = client(settings(addresses(&[&a, &b]))).await;
    assert!(c.pool.await_registered(Duration::from_secs(5)).await);
    assert!(
        !c.pool
            .await_all_registered(Duration::from_millis(300))
            .await
    );
    b.set_down(false);
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    assert_eq!(c.pool.hub_ids(), ["hub-a", "hub-b"]);
    c.shutdown().await;
}

/// While a hub is out, the survivors may run its share too (shared with snodra-messaging): when
/// the live hubs go from N_old to N_new < N_old, every live hub gets a `Permits` top-up up to
/// `ceil(max / N_new)` (here 8 / 1 − 8 / 2 = 4) with `grant = ceil(max / N_new)`, which raises the
/// hub's permit cap (the fake hub caps like snodra-server) so the hub keeps the top-up. A returning hub
/// offers `ceil(max / N)` afresh; nothing is revoked, and a hub that already holds the larger share
/// gets no second top-up.
#[tokio::test]
async fn surviving_hubs_get_the_share_of_a_hub_that_left() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let c = client(HubSettings {
        max_concurrent_handlers: 8,
        ..settings(addresses(&[&a, &b]))
    })
    .await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    let initial = |regs: Vec<proto::Register>| -> Vec<u32> {
        regs.iter().map(|r| r.initial_permits).collect()
    };
    assert_eq!(initial(a.registers().await), [4]);
    assert_eq!(initial(b.registers().await), [4]);
    assert!(a.permits().is_empty());

    let stopped = std::time::Instant::now();
    b.set_down(true);
    eventually("hub-a topped up", async || !a.permits().is_empty()).await;
    let (stream, count, at) = a.permits_timed()[0];
    assert_eq!(
        (stream, count),
        (1, 4),
        "ceil(8 / 1) - ceil(8 / 2) on hub-a's stream"
    );
    assert!(at.duration_since(stopped) < Duration::from_secs(1));
    assert_eq!(
        a.grants(),
        [(1, 8)],
        "the top-up raises the cap to ceil(8 / 1)"
    );
    assert_eq!(a.permits_held(1), Some(8), "hub-a holds the whole share");

    b.set_down(false);
    all_registered(&c, &["hub-a", "hub-b"]).await;
    assert_eq!(
        initial(b.registers().await).last(),
        Some(&4),
        "back: ceil(8 / 2)"
    );
    // hub-b leaves again: hub-a already holds 8, the share of one hub; nothing more.
    b.set_down(true);
    all_registered(&c, &["hub-a"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(a.permits(), [(1, 4)], "one top-up, nothing else");
    assert_eq!(a.permits_held(1), Some(8), "nothing revoked");
    assert_eq!(
        initial(a.registers().await),
        [4],
        "hub-a's stream was never reopened"
    );
    c.shutdown().await;
}

/// A hub down at start counts while its first attempt runs (the split is made before anyone
/// knows); once that attempt fails, the live hubs are topped up to the share of the hubs left.
#[tokio::test]
async fn a_hub_down_at_start_leaves_its_share_to_the_others() {
    let a = answering("hub-a").await;
    // A peer that accepts TCP and never answers: the first attempt fails after the handshake
    // timeout, long after hub-a's Register split the permits.
    let mute = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mute_addr = mute.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((conn, _)) = mute.accept().await {
            held.push(conn);
        }
    });
    let c = client(HubSettings {
        handshake_timeout: Duration::from_millis(300),
        ..settings(format!("{},{mute_addr}", a.address()))
    })
    .await;
    all_registered(&c, &["hub-a"]).await;
    let offered = a.registers().await[0].initial_permits;
    assert_eq!(offered, 5, "split with the hub still on its first attempt");
    eventually("hub-a holds all 10", async || a.permits_held(1) == Some(10)).await;
    assert_eq!(a.grants(), [(1, 10)]);
    c.shutdown().await;
}

#[tokio::test]
async fn unkeyed_requests_round_robin_over_live_hubs_and_skip_a_dead_one() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    for _ in 0..10 {
        c.sender
            .send_command(&Ping {}, SendOptions::default())
            .await
            .unwrap();
    }
    assert_eq!((a.requests().await.len(), b.requests().await.len()), (5, 5));

    b.set_down(true);
    all_registered(&c, &["hub-a"]).await;
    for _ in 0..6 {
        c.sender
            .send_command(&Ping {}, SendOptions::default())
            .await
            .expect("hub-a takes everything while hub-b reconnects");
    }
    assert_eq!(
        (a.requests().await.len(), b.requests().await.len()),
        (11, 5)
    );
    c.shutdown().await;
}

#[tokio::test]
async fn keyed_requests_go_to_the_rendezvous_hub_and_move_only_when_it_leaves() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    let keys: Vec<String> = (0..40).map(|i| format!("order-{i}")).collect();
    let owner = |key: &str| {
        if rendezvous_score("hub-a", key) > rendezvous_score("hub-b", key) {
            "hub-a"
        } else {
            "hub-b"
        }
    };
    for round in 0..2 {
        for key in &keys {
            let opts = SendOptions {
                routing_key: Some(key.clone()),
                ..Default::default()
            };
            c.sender.send_command(&Ping {}, opts).await.unwrap();
        }
        let at = |reqs: Vec<proto::Request>| -> Vec<String> {
            reqs.into_iter().map(|r| r.routing_key).collect()
        };
        let (on_a, on_b) = (at(a.requests().await), at(b.requests().await));
        for key in &keys {
            let want = owner(key);
            let n_a = on_a.iter().filter(|k| *k == key).count();
            let n_b = on_b.iter().filter(|k| *k == key).count();
            let want_counts = if want == "hub-a" {
                (round + 1, 0)
            } else {
                (0, round + 1)
            };
            assert_eq!(
                (n_a, n_b),
                want_counts,
                "{key} (owner {want}), round {round}"
            );
        }
    }
    let on_b_before = b.requests().await.len();
    a.set_down(true);
    all_registered(&c, &["hub-b"]).await;
    for key in &keys {
        let opts = SendOptions {
            routing_key: Some(key.clone()),
            ..Default::default()
        };
        c.sender.send_command(&Ping {}, opts).await.unwrap();
    }
    assert_eq!(
        b.requests().await.len(),
        on_b_before + keys.len(),
        "hub-a's keys moved to hub-b"
    );
    c.shutdown().await;
}

/// The chosen hub's stream dropped and the write fails: the request was never sent, so it goes to
/// the next hub with the same id, without any retry policy.
#[tokio::test]
async fn a_request_not_written_when_its_hub_drops_is_sent_to_another_hub() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    let conns = c.pool.connections();
    conns[0].sever_writes(); // hub-a: dropped, not noticed yet
    conns[1].sever_writes(); // ... and hub-b too for one of the tries below
    let err = c
        .sender
        .send_command(&Ping {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, MessagingError::HubUnavailable(_)),
        "no hub took it: {err:?}"
    );
    assert_eq!(c.sender.in_flight(), 0, "nothing left pending");
    // Let both reconnect, then break hub-a only: unkeyed and keyed requests all go to hub-b.
    a.drop_streams();
    b.drop_streams();
    eventually("both re-registered", async || {
        a.registers().await.len() == 2 && b.registers().await.len() == 2
    })
    .await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    c.pool.connections()[0].sever_writes();
    let key_on_a = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| rendezvous_score("hub-a", k) > rendezvous_score("hub-b", k))
        .unwrap();
    for i in 0..4 {
        let opts = SendOptions {
            routing_key: (i % 2 == 0).then(|| key_on_a.clone()),
            retry: Some(RetryPolicy::NONE),
            ..Default::default()
        };
        c.sender
            .send_command(&Ping {}, opts)
            .await
            .expect("failed over to hub-b transparently");
    }
    assert!(a.requests().await.is_empty(), "nothing reached hub-a");
    assert_eq!(b.requests().await.len(), 4);
    assert_eq!(c.sender.in_flight(), 0);
    c.shutdown().await;
}

/// A request written to a hub whose stream then drops has an unknown outcome: the command fails
/// with `HubConnectionLost` and is not sent elsewhere; a query under a retry policy is retried on
/// the other hub.
#[tokio::test]
async fn a_request_written_before_its_hub_drops_is_connection_lost() {
    let a = FakeHub::start_with_id("hub-a").await; // swallows requests
    let b = answering("hub-b").await;
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    let key_on_a = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| rendezvous_score("hub-a", k) > rendezvous_score("hub-b", k))
        .unwrap();
    let opts = SendOptions {
        routing_key: Some(key_on_a.clone()),
        timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    let command = tokio::spawn({
        let (sender, opts) = (Arc::clone(&c.sender), opts.clone());
        async move { sender.send_command(&Ping {}, opts).await }
    });
    let query = tokio::spawn({
        let sender = Arc::clone(&c.sender);
        let opts = SendOptions {
            retry: Some(RetryPolicy::default()),
            ..opts
        };
        async move { sender.send_query(&Look {}, opts).await }
    });
    eventually("both written to hub-a", async || {
        a.requests().await.len() == 2
    })
    .await;
    a.set_down(true);
    let lost = tokio::time::timeout(Duration::from_secs(5), command)
        .await
        .expect("the command is failed at once, not left to its timeout")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(lost, MessagingError::HubConnectionLost(_)),
        "{lost:?}"
    );
    tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .unwrap()
        .unwrap()
        .expect("the query's retry policy sends it again, to hub-b");
    let on_b: Vec<String> = b.requests().await.into_iter().map(|r| r.r#type).collect();
    assert_eq!(on_b, ["t.Look"], "the command was not sent again");
    c.shutdown().await;
}

#[derive(Serialize, Deserialize)]
struct Blob {
    data: String,
}
message_type!(Blob, "t.Blob");
impl Command for Blob {
    type Reply = ();
}

/// A request still in the connection's outbound buffer when its stream is lost was never written:
/// it goes to another hub with the same id (as snodra-messaging does), not `HubConnectionLost`.
/// Only the few the transport had taken (possibly on the wire) have an unknown outcome.
#[tokio::test]
async fn requests_still_buffered_when_a_stream_is_lost_go_to_another_hub() {
    let a = FakeHub::start_with_id("hub-a").await;
    let b = answering("hub-b").await;
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    let key_on_a = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| rendezvous_score("hub-a", k) > rendezvous_score("hub-b", k))
        .unwrap();
    // hub-a stops reading: after a few 256 KiB requests its HTTP/2 windows are full and the rest
    // wait in the client's outbound buffer.
    a.set_stall_reads(true);
    let calls: Vec<_> = (0..40)
        .map(|_| {
            let sender = Arc::clone(&c.sender);
            let opts = SendOptions {
                routing_key: Some(key_on_a.clone()),
                retry: Some(RetryPolicy::NONE),
                timeout: Some(Duration::from_secs(10)),
                ..Default::default()
            };
            tokio::spawn(async move {
                let blob = Blob {
                    data: "x".repeat(256 * 1024),
                };
                sender.send_command(&blob, opts).await
            })
        })
        .collect();
    eventually("all 40 handed to hub-a's stream", async || {
        c.sender.in_flight() == 40
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await; // the transport takes what fits
    a.drop_streams();
    let mut lost = 0;
    for call in calls {
        match tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .expect("every call ends at once")
            .unwrap()
        {
            Ok(()) => {}
            Err(MessagingError::HubConnectionLost(_)) => lost += 1,
            Err(e) => panic!("unexpected {e:?}"),
        }
    }
    let on_b = b.requests().await;
    eprintln!(
        "{} moved to hub-b, {lost} lost with hub-a's stream",
        on_b.len()
    );
    assert_eq!(on_b.len() + lost, 40);
    assert!(
        lost <= 12,
        "{lost} of 40 lost: the buffered ones were failed, not moved"
    );
    let mut ids: Vec<String> = on_b.into_iter().map(|r| r.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len() + lost, 40, "each moved request sent once");
    assert_eq!(c.sender.in_flight(), 0);
    c.shutdown().await;
}

/// A request still buffered when its stream is lost moves to another hub only while the caller's
/// timeout has not passed: once it has, no new hop starts and the call fails as never sent.
#[tokio::test]
async fn no_move_starts_after_the_callers_timeout() {
    let a = FakeHub::start_with_id("hub-a").await;
    let b = answering("hub-b").await;
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    let key_on_a = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| rendezvous_score("hub-a", k) > rendezvous_score("hub-b", k))
        .unwrap();
    a.set_stall_reads(true);
    let calls: Vec<_> = (0..40)
        .map(|_| {
            let sender = Arc::clone(&c.sender);
            let opts = SendOptions {
                routing_key: Some(key_on_a.clone()),
                retry: Some(RetryPolicy::NONE),
                timeout: Some(Duration::from_millis(300)),
                ..Default::default()
            };
            tokio::spawn(async move {
                let blob = Blob {
                    data: "x".repeat(256 * 1024),
                };
                sender.send_command(&blob, opts).await
            })
        })
        .collect();
    eventually("all 40 handed to hub-a's stream", async || {
        c.sender.in_flight() == 40
    })
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await; // past every call's 300 ms
    a.drop_streams();
    let (mut never_sent, mut lost) = (0, 0);
    for call in calls {
        match tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .expect("every call ends at once")
            .unwrap()
        {
            Err(MessagingError::HubUnavailable(_)) => never_sent += 1,
            Err(MessagingError::HubConnectionLost(_)) => lost += 1,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(never_sent >= 28, "{never_sent} never sent, {lost} lost");
    assert!(b.requests().await.is_empty(), "nothing moved to hub-b");
    c.shutdown().await;
}

#[tokio::test]
async fn two_addresses_of_one_hub_keep_one_connection() {
    // Two hubs answering the same id stand for two addresses of one hub.
    let (a, b) = (answering("hub-x").await, answering("hub-x").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-x"]).await;
    eventually("the second connection closed", async || {
        a.connected_clients().await + b.connected_clients().await == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await; // several backoffs
    assert_eq!(
        a.streams() + b.streams(),
        2,
        "the duplicate never reconnects"
    );
    assert_eq!(c.pool.hub_ids(), ["hub-x"]);
    for _ in 0..4 {
        c.sender
            .send_command(&Ping {}, SendOptions::default())
            .await
            .unwrap();
    }
    let (na, nb) = (a.requests().await.len(), b.requests().await.len());
    assert!(na == 4 && nb == 0 || na == 0 && nb == 4, "{na} / {nb}");
    c.shutdown().await;
}

/// Of two hubs answering one id, the one whose address owns it (the other is parked).
async fn owner_of<'h>(c: &Client, a: &'h FakeHub, b: &'h FakeHub) -> (&'h FakeHub, &'h FakeHub) {
    eventually("one connection left to the shared id", async || {
        a.connected_clients().await + b.connected_clients().await == 1
            && c.pool.hub_ids() == ["hub-x"]
    })
    .await;
    if a.connected_clients().await == 1 {
        (a, b)
    } else {
        (b, a)
    }
}

/// Makes `hub` answer `id` from its next stream on, and ends its streams (a restarted hub).
fn renamed(hub: &FakeHub, id: &str) {
    hub.set_ack(Some(proto::RegisterAck {
        hub_id: id.into(),
        ..default_ack()
    }));
    hub.drop_streams();
}

/// A parked address is connected again once the address that owns its hub id answers another id
/// (its hub was replaced): the id is free, and the parked address reaches it.
#[tokio::test]
async fn a_parked_address_is_connected_again_once_its_owner_answers_another_hub_id() {
    let (a, b) = (answering("hub-x").await, answering("hub-x").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    let (owner, parked) = owner_of(&c, &a, &b).await;
    let parked_streams = parked.streams();
    renamed(owner, "hub-y");
    eventually("both ids live, each on one address", async || {
        let mut ids = c.pool.hub_ids();
        ids.sort();
        ids == ["hub-x", "hub-y"]
            && owner.connected_clients().await == 1
            && parked.connected_clients().await == 1
    })
    .await;
    assert_eq!(
        parked.streams(),
        parked_streams + 1,
        "the parked address reconnected once"
    );
    c.shutdown().await;
}

/// With `dns:///`, a parked address is connected again once its owner's address is no longer
/// resolved: the owner drains, the parked address takes the hub over.
#[tokio::test]
async fn a_parked_address_is_connected_again_once_its_owner_leaves_dns() {
    let (a, b) = (answering("hub-x").await, answering("hub-x").await);
    let resolved: Resolved = Arc::new(Mutex::new(vec![a.addr, b.addr]));
    let c = client_with(
        HubSettings {
            dns_refresh: Duration::from_millis(50),
            ..settings("dns:///hubs.test:7000".into())
        },
        |pool| {
            let resolved = Arc::clone(&resolved);
            pool.set_resolver(Arc::new(move |_, _| {
                let addrs = resolved.lock().unwrap().clone();
                Box::pin(async move { Ok(addrs) })
            }));
        },
    )
    .await;
    let (owner, parked) = owner_of(&c, &a, &b).await;
    *resolved.lock().unwrap() = vec![parked.addr];
    eventually("the parked address took the hub over", async || {
        c.pool.hub_ids() == ["hub-x"]
            && parked.connected_clients().await == 1
            && owner.connected_clients().await == 0
    })
    .await;
    c.sender
        .send_command(&Ping {}, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(parked.requests().await.len(), 1);
    c.shutdown().await;
}

/// The owner of an id can itself become a duplicate (its hub now answers an id another address
/// owns): it is parked, and the id it gave up goes to the address parked behind it.
#[tokio::test]
async fn an_id_given_up_by_an_owner_that_became_a_duplicate_goes_to_the_address_parked_behind_it() {
    let (a, b, y) = (
        answering("hub-x").await,
        answering("hub-x").await,
        answering("hub-y").await,
    );
    let c = client(settings(addresses(&[&a, &b, &y]))).await;
    eventually("hub-x and hub-y live", async || {
        a.connected_clients().await + b.connected_clients().await == 1
            && c.pool.hub_ids().len() == 2
    })
    .await;
    let (owner, parked) = if a.connected_clients().await == 1 {
        (&a, &b)
    } else {
        (&b, &a)
    };
    renamed(owner, "hub-y");
    eventually("hub-x through the other address, hub-y once", async || {
        let mut ids = c.pool.hub_ids();
        ids.sort();
        ids == ["hub-x", "hub-y"]
            && parked.connected_clients().await == 1
            && owner.connected_clients().await == 0
            && y.connected_clients().await == 1
    })
    .await;
    c.shutdown().await;
}

/// The real hub replaces the session of an instance id when a second stream registers it, ending
/// the first. Two targets of one hub (`127.0.0.1:port` and `localhost:port`) therefore evict each
/// other once at start; the second connection is closed for good when its ack shows the same hub
/// id, the first comes back, and from then on the hub holds the instance without further evictions.
#[tokio::test]
async fn two_targets_of_one_replacing_hub_settle_without_ping_pong() {
    let hub = answering("hub-x").await;
    let port = hub.addr.port();
    let c = client(settings(format!(
        "static://127.0.0.1:{port},localhost:{port}"
    )))
    .await;
    let instance = c.pool.instance_id().to_string();
    all_registered(&c, &["hub-x"]).await;
    eventually("one stream left, holding the session", async || {
        hub.connected_clients().await == 1 && hub.holds(&instance) && c.pool.hub_ids() == ["hub-x"]
    })
    .await;
    assert!(
        hub.evictions() >= 1,
        "the fake hub replaces sessions like snodra-server"
    );
    assert!(
        c.pool.await_all_registered(Duration::from_secs(1)).await,
        "a closed duplicate is not a hub to wait for"
    );
    let (evictions, streams) = (hub.evictions(), hub.streams());
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(50)).await; // 1 s: several backoffs (50-200 ms)
        assert!(
            hub.holds(&instance),
            "the instance left the hub: {:?}",
            hub.events()
        );
    }
    assert_eq!(
        (hub.evictions(), hub.streams()),
        (evictions, streams),
        "no ping-pong: {:?}",
        hub.events()
    );
    for _ in 0..4 {
        c.sender
            .send_command(&Ping {}, SendOptions::default())
            .await
            .unwrap();
    }
    c.shutdown().await;
}

/// A hub whose ack carries an empty `RegisterAck.hub_id` is known by its target as `host:port`
/// (shared with snodra-messaging): two such hubs are two hubs, not duplicates.
#[tokio::test]
async fn a_hub_without_an_id_is_known_by_its_bare_target() {
    let (a, b) = (answering("").await, answering("").await);
    let c = client(settings(format!("{},http://{}", a.address(), b.addr))).await;
    let ids = [a.addr.to_string(), b.addr.to_string()];
    eventually("three hubs", async || c.pool.hub_ids() == ids).await;
    let by_conn: Vec<Option<String>> = c.pool.connections().iter().map(|c| c.hub_id()).collect();
    assert_eq!(by_conn, ids.map(Some));
    c.shutdown().await;
}

/// A dual-stack `dns:///` name resolves to IPv6 and IPv4 addresses of the same hubs: only the
/// IPv4 ones are used, as bare `ip:port`, so no hub is registered on twice.
#[tokio::test]
async fn a_dual_stack_dns_name_reaches_each_hub_once_over_ipv4() {
    let hub = answering("hub-a").await;
    let port = hub.addr.port();
    let c = client_with(
        HubSettings {
            dns_refresh: Duration::from_millis(100),
            ..settings("dns:///hubs.test:7000".into())
        },
        |pool| {
            pool.set_resolver(Arc::new(move |_, _| {
                let addrs: Vec<SocketAddr> = vec![
                    format!("[::1]:{port}").parse().unwrap(),
                    format!("127.0.0.1:{port}").parse().unwrap(),
                ];
                Box::pin(async move { Ok(addrs) })
            }));
        },
    )
    .await;
    all_registered(&c, &["hub-a"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await; // a few re-resolutions
    let addresses: Vec<String> = c
        .pool
        .connections()
        .iter()
        .map(|c| c.address().to_string())
        .collect();
    assert_eq!(addresses, [format!("127.0.0.1:{port}")]);
    assert_eq!(hub.evictions(), 0);
    assert_eq!(hub.registers().await.len(), 1);
    c.shutdown().await;
}

/// `InstanceStatus` goes every interval to every hub; it lists the acknowledged hubs and the
/// handlers running.
#[tokio::test]
async fn instance_status_goes_to_every_hub() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    a.send_to_clients(push_request("in-1")).await;
    eventually("a handler runs", async || c.held.lock().unwrap().len() == 1).await;
    // A report made while the handler runs (the ones sent at once when the hubs came up may
    // precede it).
    let (from_a, from_b) = (a.statuses().len(), b.statuses().len());
    eventually("both hubs got a report since", async || {
        a.statuses().len() > from_a && b.statuses().len() > from_b
    })
    .await;
    for hub in [&a, &b] {
        let (_, last) = hub.statuses().last().cloned().unwrap();
        assert_eq!(last.connected_hub_ids, ["hub-a", "hub-b"]);
        assert_eq!(last.handlers_in_flight, 1);
    }
    let reply_to = c.held.lock().unwrap().pop().unwrap();
    reply_to.grant().await;
    let seen = a.statuses().len();
    eventually("a report with no handler running", async || {
        a.statuses()[seen..]
            .iter()
            .any(|(_, s)| s.handlers_in_flight == 0)
    })
    .await;
    c.shutdown().await;
}

/// The hub client gauges read what the status reports: the live hubs and the requests being
/// handled; the credential counters are read from memory too (no token fetch without OAuth2).
#[tokio::test]
async fn the_hub_client_gauges_count_live_hubs_and_running_handlers() {
    use snodra_messaging::metrics::{
        HUB_CLIENT_CONNECTED_HUBS, HUB_CLIENT_CREDENTIALS_REFRESHES_SENT,
        HUB_CLIENT_HANDLERS_IN_FLIGHT, HUB_CLIENT_TOKEN_FETCHES, Metrics,
    };
    let meter = support::metrics::TestMeter::new();
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    HubPool::observe(&c.pool, &Metrics::new(meter.meter()));
    all_registered(&c, &["hub-a", "hub-b"]).await;
    assert_eq!(meter.value(HUB_CLIENT_CONNECTED_HUBS, &[]), Some(2.0));
    assert_eq!(meter.value(HUB_CLIENT_HANDLERS_IN_FLIGHT, &[]), Some(0.0));
    a.send_to_clients(push_request("in-1")).await;
    eventually("a handler runs", async || c.held.lock().unwrap().len() == 1).await;
    assert_eq!(meter.value(HUB_CLIENT_HANDLERS_IN_FLIGHT, &[]), Some(1.0));
    assert_eq!(
        meter.value(HUB_CLIENT_CREDENTIALS_REFRESHES_SENT, &[]),
        Some(0.0)
    );
    assert!(
        meter.points_of(HUB_CLIENT_TOKEN_FETCHES).is_empty(),
        "no OAuth2, no token fetch counter"
    );
    b.set_down(true);
    eventually("one hub left", async || {
        meter.value(HUB_CLIENT_CONNECTED_HUBS, &[]) == Some(1.0)
    })
    .await;
    let reply_to = c.held.lock().unwrap().pop().unwrap();
    reply_to.grant().await;
    c.shutdown().await;
}

/// The processor status rides on every hub's `InstanceStatus`.
#[tokio::test]
async fn processor_status_goes_to_every_hub() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let c = client_with(settings(addresses(&[&a, &b])), |pool| {
        pool.set_processor_status(Arc::new(|| {
            vec![proto::ProcessorStatus {
                name: "orders".into(),
                subscription: "svc.orders".into(),
                state: proto::ProcessorState::ProcessorRunning as i32,
                ..Default::default()
            }]
        }));
    })
    .await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    eventually("both hubs got reports", async || {
        a.statuses().len() >= 2 && b.statuses().len() >= 2
    })
    .await;
    for hub in [&a, &b] {
        let (_, last) = hub.statuses().last().cloned().unwrap();
        assert_eq!(last.processors.len(), 1);
        assert_eq!(last.processors[0].name, "orders");
    }
    c.shutdown().await;
}

type Resolved = Arc<Mutex<Vec<SocketAddr>>>;

/// `dns:///`: new addresses get a connection; a vanished one leaves the routing at once and is
/// closed once its handlers are done.
#[tokio::test]
async fn a_dns_target_follows_the_resolution_and_drains_a_vanished_hub() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let resolved: Resolved = Arc::new(Mutex::new(vec![a.addr]));
    let c = client_with(
        HubSettings {
            dns_refresh: Duration::from_millis(100),
            ..settings("dns:///hubs.test:7000".into())
        },
        |pool| {
            let resolved = Arc::clone(&resolved);
            pool.set_resolver(Arc::new(move |host, port| {
                assert_eq!((host.as_str(), port), ("hubs.test", 7000));
                let addrs = resolved.lock().unwrap().clone();
                Box::pin(async move { Ok(addrs) })
            }));
        },
    )
    .await;
    all_registered(&c, &["hub-a"]).await;
    resolved.lock().unwrap().push(b.addr);
    all_registered(&c, &["hub-a", "hub-b"]).await;

    // hub-a's request is still being handled when hub-a leaves DNS.
    a.send_to_clients(push_request("in-a")).await;
    eventually("a handler runs", async || c.held.lock().unwrap().len() == 1).await;
    *resolved.lock().unwrap() = vec![b.addr];
    all_registered(&c, &["hub-b"]).await;
    for _ in 0..3 {
        c.sender
            .send_command(&Ping {}, SendOptions::default())
            .await
            .unwrap();
    }
    assert_eq!(
        b.requests().await.len(),
        3,
        "a draining hub gets no new request"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        a.connected_clients().await,
        1,
        "kept open while its handler runs"
    );
    let reply_to = c.held.lock().unwrap().pop().unwrap();
    reply_to.grant().await;
    eventually("hub-a closed once idle", async || {
        a.connected_clients().await == 0
    })
    .await;
    assert_eq!(c.pool.connections().len(), 1);
    c.shutdown().await;
}

/// A vanished address is first deregistered with an empty `Register` (the hub stops routing to the
/// instance there and hands its queue to other instances), so no new request arrives while its
/// handlers finish; only then is the stream closed.
#[tokio::test]
async fn a_vanished_dns_address_is_deregistered_before_it_is_closed() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let resolved: Resolved = Arc::new(Mutex::new(vec![a.addr, b.addr]));
    let c = client_with(
        HubSettings {
            dns_refresh: Duration::from_millis(50),
            ..settings("dns:///hubs.test:7000".into())
        },
        |pool| {
            let resolved = Arc::clone(&resolved);
            pool.set_resolver(Arc::new(move |_, _| {
                let addrs = resolved.lock().unwrap().clone();
                Box::pin(async move { Ok(addrs) })
            }));
        },
    )
    .await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    a.send_to_clients(push_request("in-a")).await;
    eventually("a handler runs", async || c.held.lock().unwrap().len() == 1).await;
    *resolved.lock().unwrap() = vec![b.addr];
    eventually("an empty Register on hub-a", async || {
        a.events().contains(&Event::Register {
            stream: 1,
            types: 0,
        })
    })
    .await;
    assert_eq!(
        a.connected_clients().await,
        1,
        "open while the handler runs"
    );
    let reply_to = c.held.lock().unwrap().pop().unwrap();
    reply_to.grant().await;
    eventually("hub-a closed once idle", async || {
        a.events().contains(&Event::Closed { stream: 1 })
    })
    .await;
    let events = a.events();
    let empty = events
        .iter()
        .position(|e| {
            *e == Event::Register {
                stream: 1,
                types: 0,
            }
        })
        .unwrap();
    let closed = events
        .iter()
        .position(|e| *e == Event::Closed { stream: 1 })
        .unwrap();
    assert!(empty < closed, "{events:?}");
    assert_eq!(a.streams(), 1, "no reconnect to a vanished address");
    c.shutdown().await;
}

/// An address that comes and goes leaves no finished drain task behind.
#[tokio::test]
async fn finished_drains_do_not_pile_up() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let resolved: Resolved = Arc::new(Mutex::new(vec![a.addr]));
    let c = client_with(
        HubSettings {
            dns_refresh: Duration::from_millis(20),
            ..settings("dns:///hubs.test:7000".into())
        },
        |pool| {
            let resolved = Arc::clone(&resolved);
            pool.set_resolver(Arc::new(move |_, _| {
                let addrs = resolved.lock().unwrap().clone();
                Box::pin(async move { Ok(addrs) })
            }));
        },
    )
    .await;
    for round in 0..6 {
        *resolved.lock().unwrap() = vec![a.addr, b.addr];
        all_registered(&c, &["hub-a", "hub-b"]).await;
        *resolved.lock().unwrap() = vec![a.addr];
        eventually(&format!("round {round}: hub-b closed"), async || {
            c.pool.connections().len() == 1
        })
        .await;
    }
    // The status loop, the resolve loop and at most the last drain.
    assert!(
        c.pool.background_tasks() <= 3,
        "{}",
        c.pool.background_tasks()
    );
    c.shutdown().await;
}

/// A failed or empty resolution keeps the hubs there are.
#[tokio::test]
async fn a_failed_resolution_keeps_the_current_hubs() {
    let a = answering("hub-a").await;
    let answer: Arc<Mutex<Option<Vec<SocketAddr>>>> = Arc::new(Mutex::new(Some(vec![a.addr])));
    let c = client_with(
        HubSettings {
            dns_refresh: Duration::from_millis(50),
            ..settings("dns:///hubs.test:7000".into())
        },
        |pool| {
            let answer = Arc::clone(&answer);
            pool.set_resolver(Arc::new(move |_, _| {
                let r = answer.lock().unwrap().clone();
                Box::pin(async move { r.ok_or_else(|| std::io::Error::other("no such host")) })
            }));
        },
    )
    .await;
    all_registered(&c, &["hub-a"]).await;
    *answer.lock().unwrap() = None;
    tokio::time::sleep(Duration::from_millis(200)).await;
    *answer.lock().unwrap() = Some(vec![]);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(c.pool.hub_ids(), ["hub-a"]);
    assert_eq!(a.connected_clients().await, 1);
    c.shutdown().await;
}

fn no_handler(req: &proto::Request) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Error(proto::Error {
            request_id: req.id.clone(),
            code: proto::ErrorCode::NoHandler as i32,
            message: format!("no handler for {}", req.r#type),
            details: vec![],
        })),
    }
}

/// A hub that has no owner for any type.
async fn without_owner(id: &str) -> FakeHub {
    let hub = FakeHub::start_with_id(id).await;
    hub.set_responder(|req| Some(no_handler(&req))).await;
    hub
}

/// A single owner instance is registered on hub B only (or deregistered on hub A, as during a
/// certificate rollover): a send whose routing picks hub A gets `NO_HANDLER` there, which means
/// the request was never delivered, so it goes to hub B, and succeeds.
#[tokio::test]
async fn no_handler_from_one_hub_fails_over_to_the_other_live_hubs() {
    let (a, b) = (without_owner("hub-a").await, answering("hub-b").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    for _ in 0..6 {
        c.sender
            .send_command(&Ping {}, SendOptions::default())
            .await
            .expect("hub-b can deliver");
    }
    assert!(
        a.requests().await.len() >= 2,
        "round robin never picked hub-a"
    );
    assert_eq!(b.requests().await.len(), 6, "every command ends on hub-b");
    // A keyed command whose preferred hub has no handler: affinity cannot be kept, it moves too.
    let key = (0..)
        .map(|i| format!("agg-{i}"))
        .find(|k| rendezvous_score("hub-a", k) > rendezvous_score("hub-b", k))
        .unwrap();
    c.sender
        .send_command(
            &Ping {},
            SendOptions {
                routing_key: Some(key),
                ..SendOptions::default()
            },
        )
        .await
        .expect("a keyed command fails over as well");
    // A query, the same.
    c.sender
        .send_query(&Look {}, SendOptions::default())
        .await
        .expect("a query fails over");
    c.shutdown().await;
}

/// With no owner on any hub the answer is the last `NO_HANDLER`, at once: each hub is tried once.
#[tokio::test]
async fn with_no_owner_anywhere_every_hub_is_tried_once_and_no_handler_comes_back_promptly() {
    let (a, b) = (without_owner("hub-a").await, without_owner("hub-b").await);
    let c = client(settings(addresses(&[&a, &b]))).await;
    all_registered(&c, &["hub-a", "hub-b"]).await;
    for round in 1..=3usize {
        let started = std::time::Instant::now();
        let error = c
            .sender
            .send_command(&Ping {}, SendOptions::default())
            .await
            .expect_err("nobody handles it");
        assert!(matches!(error, MessagingError::NoHandler(_)), "{error:?}");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(a.requests().await.len(), round, "hub-a tried once per send");
        assert_eq!(b.requests().await.len(), round, "hub-b tried once per send");
    }
    c.shutdown().await;
}

/// Once the pool deregistered (the first step of a shutdown), a connection it opens later (a new
/// DNS address during the drain) registers no types either: the hub must not route to a
/// closing instance.
#[tokio::test]
async fn after_the_deregistration_a_new_address_registers_no_types() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let resolved: Resolved = Arc::new(Mutex::new(vec![a.addr]));
    let c = client_with(
        HubSettings {
            dns_refresh: Duration::from_millis(50),
            ..settings("dns:///hubs.test:7000".into())
        },
        |pool| {
            let resolved = Arc::clone(&resolved);
            pool.set_resolver(Arc::new(move |_, _| {
                let addrs = resolved.lock().unwrap().clone();
                Box::pin(async move { Ok(addrs) })
            }));
        },
    )
    .await;
    all_registered(&c, &["hub-a"]).await;
    assert!(c.pool.deregister(Duration::from_secs(1)).await);
    resolved.lock().unwrap().push(b.addr);
    eventually("hub-b registered", async || !b.registers().await.is_empty()).await;
    let register = b.registers().await.remove(0);
    assert!(
        register.command_types.is_empty() && register.query_types.is_empty(),
        "{register:?}"
    );
    c.shutdown().await;
}

/// Besides every `status_interval`, an `InstanceStatus` goes to every live hub at once when the
/// set of live hubs changes: a hub coming up, and an address leaving DNS.
#[tokio::test]
async fn instance_status_goes_out_at_once_when_the_live_hubs_change() {
    let (a, b) = (answering("hub-a").await, answering("hub-b").await);
    let resolved: Resolved = Arc::new(Mutex::new(vec![a.addr]));
    let c = client_with(
        HubSettings {
            dns_refresh: Duration::from_millis(50),
            status_interval: Duration::from_secs(600),
            ..settings("dns:///hubs.test:7000".into())
        },
        |pool| {
            let resolved = Arc::clone(&resolved);
            pool.set_resolver(Arc::new(move |_, _| {
                let addrs = resolved.lock().unwrap().clone();
                Box::pin(async move { Ok(addrs) })
            }));
        },
    )
    .await;
    let reported = |hub: &FakeHub, ids: &[&str]| {
        hub.statuses()
            .iter()
            .any(|(_, s)| s.connected_hub_ids == ids)
    };
    eventually("hub-a told it is live", async || reported(&a, &["hub-a"])).await;
    resolved.lock().unwrap().push(b.addr);
    eventually("both told both are live", async || {
        reported(&a, &["hub-a", "hub-b"]) && reported(&b, &["hub-a", "hub-b"])
    })
    .await;
    *resolved.lock().unwrap() = vec![a.addr];
    eventually("hub-a told hub-b left", async || {
        a.statuses()
            .last()
            .is_some_and(|(_, s)| s.connected_hub_ids == ["hub-a"])
    })
    .await;
    c.shutdown().await;
}
