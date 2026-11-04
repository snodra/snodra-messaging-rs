//! End to end with two real hubs (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`, ADR 0016): every
//! instance registers on both, reports its connected hubs to both, spreads unkeyed requests over
//! both, survives one hub stopping mid-traffic and is back on it within the backoff cap once it
//! restarts, and moves a stopped hub's permits to the other one (`Permits.grant`) without revoking
//! them on its return.
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use kraken_messaging::hub::SendOptions;
use kraken_messaging::{Command, Config, KrakenMessaging, MessagingError, message_type};
use serde::{Deserialize, Serialize};
use support::containers::{
    HubContainer, HubOptions, free_port, hub_with, mongo, wait_until_reachable,
};

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Serialize, Deserialize)]
struct Work {}
message_type!(Work, "e2e.Work");
impl Command for Work {
    type Reply = ();
}

const IDS: [&str; 2] = ["e2e-hub-1", "e2e-hub-2"];

/// Bound for both hubs to show the instance's first `InstanceStatus` (5 s period) after the
/// registration: the period plus 3 s of scheduling slack.
const STATUS_SEEN_WITHIN: Duration = Duration::from_secs(8);
/// Bound for re-registering on a restarted hub once it answers `Topology`: the backoff cap (5 s,
/// jitter clamped to it) plus 7 s for an attempt still in flight against the restarting container
/// (connect and handshake), under `registered_on`'s 15 s. The point is "within the backoff cap,
/// not after a stall", not a tight latency bound.
const REJOIN_WITHIN: Duration = Duration::from_secs(12);

/// Two hubs with fixed ids, on fixed host ports (a restart keeps the address).
async fn two_hubs() -> (HubContainer, HubContainer) {
    let start =
        |id: &'static str| hub_with(HubOptions::new().port(free_port()).env("KRAKEN_HUB_ID", id));
    tokio::join!(start(IDS[0]), start(IDS[1]))
}

fn config(service: &str, hubs: &str, mongo: &str) -> Config {
    let mut c = Config::new(service, hubs, "127.0.0.1:1", mongo);
    c.outbox_enabled = false;
    c.consumer_enabled = false;
    c.reconnect_backoff_min = Duration::from_millis(100);
    c.max_concurrent_handlers = 8;
    c
}

/// The handler side: `e2e.Work` takes 20 ms.
async fn worker(cfg: Config) -> KrakenMessaging {
    KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .command::<Work, _, _>(|_, _| async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(())
        })
        .start()
        .await
        .unwrap()
}

async fn caller(cfg: Config) -> KrakenMessaging {
    KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .start()
        .await
        .unwrap()
}

/// Waits (bounded by `limit`) until the app is registered on exactly `ids`; returns how long it took.
async fn registered_on(app: &KrakenMessaging, ids: &[&str], limit: Duration) -> Duration {
    let started = Instant::now();
    loop {
        let mut now = app.hub_ids();
        now.sort();
        if now == ids {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < limit,
            "registered on {now:?}, not {ids:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn get(hub: &HubContainer, path: &str) -> serde_json::Value {
    reqwest::get(format!("{}{path}", hub.http))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// `service`'s instance in the hub's `/api/v1/topology`, if listed.
async fn instance(hub: &HubContainer, service: &str) -> Option<serde_json::Value> {
    let t = get(hub, "/api/v1/topology").await;
    t["services"]
        .as_array()?
        .iter()
        .find(|s| s["serviceName"] == service)
        .map(|s| s["instances"][0].clone())
}

/// Requests of `type_name` the hub forwarded in its statistics window.
async fn traffic(hub: &HubContainer, type_name: &str) -> u64 {
    let t = get(hub, "/api/v1/types").await;
    t["types"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["type"] == type_name)
        .and_then(|t| t["traffic"]["count"].as_u64())
        .unwrap_or(0)
}

#[tokio::test]
async fn both_hubs_list_the_instance_with_both_ids_and_share_unkeyed_requests() {
    require_docker!();
    let ((h1, h2), db) = tokio::join!(two_hubs(), mongo());
    let hubs = format!("{},{}", h1.address, h2.address);
    let b = worker(config("e2e-b", &hubs, &db.uri_for("b"))).await;
    let a = caller(config("e2e-a", &hubs, &db.uri_for("a"))).await;
    assert!(b.await_all_hubs(Duration::from_secs(15)).await);
    registered_on(&b, &IDS, Duration::from_secs(15)).await;
    let registered = Instant::now();
    registered_on(&a, &IDS, Duration::from_secs(15)).await;

    // Both hubs list the instance with both hub ids once its first report (5 s period) is in. The
    // first report goes one period after the pool starts, which is before `registered`; 8 s leaves
    // 3 s for a slow container or CI scheduler on top of the period.
    for hub in [&h1, &h2] {
        loop {
            let view = instance(hub, "e2e-b").await;
            let mut ids: Vec<String> = view
                .as_ref()
                .and_then(|v| v["connectedHubIds"].as_array().cloned())
                .unwrap_or_default()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            ids.sort();
            if ids == IDS {
                let view = view.unwrap();
                assert!(view["statusAgeMs"].as_i64().unwrap() >= 0, "{view}");
                // ceil(8 / 2) permits offered to each hub.
                assert_eq!(view["permitsAvailable"], 4, "{view}");
                break;
            }
            assert!(
                registered.elapsed() < STATUS_SEEN_WITHIN,
                "{} still shows {view:?}",
                hub.http
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    eprintln!(
        "both hubs show both ids {:?} after the registration",
        registered.elapsed()
    );

    for _ in 0..50 {
        a.commands()
            .send_command(&Work {}, SendOptions::default())
            .await
            .expect("round trip over one of the hubs");
    }
    let (c1, c2) = (
        traffic(&h1, "e2e.Work").await,
        traffic(&h2, "e2e.Work").await,
    );
    eprintln!(
        "50 unkeyed requests: {c1} through {}, {c2} through {}",
        IDS[0], IDS[1]
    );
    assert_eq!(c1 + c2, 50);
    assert!(c1 >= 20 && c2 >= 20, "not spread: {c1} / {c2}");
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn a_hub_stopped_mid_traffic_fails_over_and_is_rejoined_after_its_restart() {
    require_docker!();
    support::init_tracing();
    let ((h1, h2), db) = tokio::join!(two_hubs(), mongo());
    let hubs = format!("{},{}", h1.address, h2.address);
    let b = worker(config("e2e-b", &hubs, &db.uri_for("b"))).await;
    let a = Arc::new(caller(config("e2e-a", &hubs, &db.uri_for("a"))).await);
    registered_on(&b, &IDS, Duration::from_secs(15)).await;
    registered_on(&a, &IDS, Duration::from_secs(15)).await;

    let timeout = Duration::from_secs(2);
    let finish = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicUsize::new(0));
    let t0 = Instant::now();
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let (a, finish, done) = (Arc::clone(&a), Arc::clone(&finish), Arc::clone(&done));
            tokio::spawn(async move {
                let mut results = Vec::new();
                while !finish.load(Ordering::SeqCst) {
                    let started = t0.elapsed();
                    let opts = SendOptions {
                        timeout: Some(timeout),
                        ..Default::default()
                    };
                    let result = a.commands().send_command(&Work {}, opts).await;
                    results.push((started, t0.elapsed() - started, result));
                    done.fetch_add(1, Ordering::SeqCst);
                }
                results
            })
        })
        .collect();
    while done.load(Ordering::SeqCst) < 60 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stop_at = t0.elapsed();
    h2.container.stop().await.unwrap();
    let stopped = t0.elapsed();
    eprintln!("hub 2 stopped between {stop_at:?} and {stopped:?}");
    tokio::time::sleep(Duration::from_secs(3)).await;
    finish.store(true, Ordering::SeqCst);

    let mut results = Vec::new();
    for w in workers {
        results.extend(w.await.unwrap());
    }
    results.sort_by_key(|(started, _, _)| *started);
    let failures: Vec<_> = results.iter().filter(|(_, _, r)| r.is_err()).collect();
    for (started, took, r) in &failures {
        eprintln!("failed: started {started:?}, took {took:?}: {r:?}");
    }
    let slowest = results.iter().map(|(_, took, _)| *took).max().unwrap();
    eprintln!(
        "{} requests, {} failed, slowest {slowest:?}",
        results.len(),
        failures.len()
    );
    // Only requests in flight through hub 2 when it went away fail (at most one per worker, plus
    // any written before the loss was noticed), with an unknown outcome: the stopping hub answers
    // HANDLER_DISCONNECTED when the worker's stream closes first, else the stream to it breaks.
    assert!(failures.len() <= 8, "{} failures", failures.len());
    for (started, took, r) in &failures {
        assert!(
            matches!(
                r,
                Err(MessagingError::HubConnectionLost(_) | MessagingError::HandlerDisconnected(_))
            ),
            "{r:?}"
        );
        assert!(
            *started + *took >= stop_at && *started <= stopped + Duration::from_secs(1),
            "not in flight while hub 2 stopped: started {started:?}, took {took:?}"
        );
    }
    // Nothing waits beyond the request timeout (plus the 1 s local guard margin).
    assert!(
        slowest <= timeout + Duration::from_millis(1_500),
        "{slowest:?}"
    );
    // Everything sent once the loss is known goes to hub 1.
    let after: Vec<_> = results
        .iter()
        .filter(|(started, _, _)| *started > stopped + Duration::from_secs(1))
        .collect();
    assert!(after.len() > 100, "{} requests after the stop", after.len());
    assert!(after.iter().all(|(_, _, r)| r.is_ok()));
    assert_eq!(a.hub_ids(), [IDS[0]]);

    h2.container.start().await.unwrap();
    wait_until_reachable(&h2.address).await;
    let (ta, tb) = tokio::join!(
        registered_on(&a, &IDS, Duration::from_secs(15)),
        registered_on(&b, &IDS, Duration::from_secs(15)),
    );
    eprintln!("re-registered on the restarted hub after {ta:?} (caller) and {tb:?} (worker)");
    assert!(
        ta.max(tb) <= REJOIN_WITHIN,
        "re-registered only after {:?}",
        ta.max(tb)
    );
    let before = traffic(&h2, "e2e.Work").await;
    for _ in 0..10 {
        a.commands()
            .send_command(&Work {}, SendOptions::default())
            .await
            .expect("round trip after the restart");
    }
    assert!(
        traffic(&h2, "e2e.Work").await > before,
        "the restarted hub is used again"
    );
    Arc::into_inner(a).unwrap().shutdown().await;
    b.shutdown().await;
}

/// `permitsAvailable` of `service`'s instance on `hub`; `None` while it is not listed.
async fn permits_available(hub: &HubContainer, service: &str) -> Option<u64> {
    instance(hub, service)
        .await
        .and_then(|v| v["permitsAvailable"].as_u64())
}

/// Polls (every 25 ms, at most `limit`) until `service` shows `want` permits on `hub`.
async fn permits_reach(hub: &HubContainer, service: &str, want: u64, limit: Duration) -> Duration {
    let started = Instant::now();
    loop {
        let now = permits_available(hub, service).await;
        if now == Some(want) {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < limit,
            "{} shows {now:?} permits, not {want}",
            hub.http
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// max 8 over two hubs: 4 permits on each. Hub 2 stops: hub 1 is topped up with
/// `Permits{count = 4, grant = 8}` and shows 8 within about 2 s. Hub 2 returns: it gets 4 afresh
/// and hub 1 keeps its 8 (nothing is revoked).
#[tokio::test]
async fn a_stopped_hubs_permits_move_to_the_other_and_are_not_revoked_on_its_return() {
    require_docker!();
    let ((h1, h2), db) = tokio::join!(two_hubs(), mongo());
    let hubs = format!("{},{}", h1.address, h2.address);
    let b = worker(config("e2e-b", &hubs, &db.uri_for("b"))).await;
    registered_on(&b, &IDS, Duration::from_secs(15)).await;
    for hub in [&h1, &h2] {
        permits_reach(hub, "e2e-b", 4, Duration::from_secs(10)).await;
    }

    let stopping = Instant::now();
    h2.container.stop().await.unwrap();
    let stopped = stopping.elapsed();
    let topped_up = permits_reach(&h1, "e2e-b", 8, Duration::from_secs(10)).await;
    eprintln!(
        "hub 1 shows 8 permits {topped_up:?} after hub 2 stopped (the stop took {stopped:?})"
    );
    assert!(
        topped_up <= Duration::from_millis(2_500),
        "topped up only after {topped_up:?}"
    );

    h2.container.start().await.unwrap();
    wait_until_reachable(&h2.address).await;
    registered_on(&b, &IDS, Duration::from_secs(15)).await;
    permits_reach(&h2, "e2e-b", 4, Duration::from_secs(10)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        permits_available(&h1, "e2e-b").await,
        Some(8),
        "nothing revoked"
    );
    b.shutdown().await;
}

/// One hub reached through two targets (`127.0.0.1:port` and `localhost:port`): the hub replaces
/// an instance's session when a second stream registers the same instance id, so the pool must
/// settle on one stream and keep it. Once settled, the hub lists the instance on every poll for
/// 10 s with the same `connectedSinceMs` (no re-registration), and requests keep succeeding.
#[tokio::test]
async fn one_hub_reached_through_two_targets_keeps_the_instance_registered() {
    require_docker!();
    let (h, db) = tokio::join!(
        hub_with(
            HubOptions::new()
                .port(free_port())
                .env("KRAKEN_HUB_ID", "e2e-solo"),
        ),
        mongo()
    );
    let port = h.address.rsplit_once(':').unwrap().1;
    let twice = format!("127.0.0.1:{port},localhost:{port}");
    let b = worker(config("e2e-b", &twice, &db.uri_for("b"))).await;
    let a = caller(config("e2e-a", &h.address, &db.uri_for("a"))).await;
    registered_on(&b, &["e2e-solo"], Duration::from_secs(15)).await;
    registered_on(&a, &["e2e-solo"], Duration::from_secs(15)).await;
    // Both targets have registered by now; at most one eviction happened at start.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let since = |view: &serde_json::Value| view["connectedSinceMs"].as_i64().unwrap();
    let first = instance(&h, "e2e-b").await.expect("listed once settled");
    let started = Instant::now();
    let mut polls = 0;
    while started.elapsed() < Duration::from_secs(10) {
        let view = instance(&h, "e2e-b")
            .await
            .unwrap_or_else(|| panic!("instance absent after {:?}", started.elapsed()));
        assert_eq!(
            since(&view),
            since(&first),
            "re-registered after {:?}: {view}",
            started.elapsed()
        );
        a.commands()
            .send_command(&Work {}, SendOptions::default())
            .await
            .expect("round trip while the instance stays registered");
        polls += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!("{polls} polls over 10 s, the instance listed every time");
    assert_eq!(b.hub_ids(), ["e2e-solo"]);
    a.shutdown().await;
    b.shutdown().await;
}
