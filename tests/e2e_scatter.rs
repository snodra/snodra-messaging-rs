//! End to end with two real hubs (`SNODRA_SERVER_IMAGE`, default `snodra-server:dev`, ADR 0017) and the
//! multi-hub configuration (every instance on both hubs): a scatter-gather over three instances of
//! two services owning a shared query gets three parts, a unary query on the shared type reaches
//! every instance of both owners, and a gather whose hub dies before its end fails with
//! `HubConnectionLost` while the next one succeeds on the other hub.
mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use snodra_messaging::hub::{ScatterOptions, SendOptions};
use snodra_messaging::{Config, MessagingError, Query, SnodraMessaging, message_type};
use support::containers::{HubContainer, HubOptions, hub_with};
use tokio_stream::StreamExt;

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Serialize, Deserialize)]
struct Find {
    delay_ms: u64,
}
#[derive(Serialize, Deserialize, Debug)]
struct Found {
    service: String,
}
message_type!(Find, "e2e.Find");
impl Query for Find {
    type Reply = Found;
}

const IDS: [&str; 2] = ["e2e-p4a-1", "e2e-p4a-2"];

async fn two_hubs() -> (HubContainer, HubContainer) {
    let start = |id: &'static str| hub_with(HubOptions::new().env("SNODRA_SERVER_ID", id));
    tokio::join!(start(IDS[0]), start(IDS[1]))
}

fn config(service: &str, hubs: &str) -> Config {
    let mut c = Config::new(service, hubs);
    c.outbox_enabled = false;
    c.consumer_enabled = false;
    c.reconnect_backoff_min = Duration::from_millis(100);
    c.max_concurrent_handlers = 8;
    c
}

/// An instance of `service` owning `e2e.Find` as a shared query; it answers after the delay.
async fn owner(cfg: Config) -> SnodraMessaging {
    let service = cfg.service_name.clone();
    SnodraMessaging::builder(cfg)
        .await
        .unwrap()
        .shared_query::<Find, _, _>(move |q, _| {
            let service = service.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(q.delay_ms)).await;
                Ok(Found { service })
            }
        })
        .start()
        .await
        .unwrap()
}

async fn caller(cfg: Config) -> SnodraMessaging {
    SnodraMessaging::builder(cfg)
        .await
        .unwrap()
        .start()
        .await
        .unwrap()
}

async fn get(hub: &HubContainer, path: &str) -> serde_json::Value {
    reqwest::get(format!("{}{path}", hub.http))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// `inFlightAsSender` of `service`'s instance on `hub` (0 while it is not listed).
async fn in_flight_as_sender(hub: &HubContainer, service: &str) -> u64 {
    let t = get(hub, "/api/v1/topology").await;
    t["services"]
        .as_array()
        .and_then(|s| s.iter().find(|s| s["serviceName"] == service))
        .and_then(|s| s["instances"][0]["inFlightAsSender"].as_u64())
        .unwrap_or(0)
}

/// Three owners (one of `e2e-sa`, two of `e2e-sb`) and a caller, all registered on both hubs.
async fn cluster(hubs: &str) -> (Vec<SnodraMessaging>, SnodraMessaging) {
    let owners = vec![
        owner(config("e2e-sa", hubs)).await,
        owner(config("e2e-sb", hubs)).await,
        owner(config("e2e-sb", hubs)).await,
    ];
    let c = caller(config("e2e-caller", hubs)).await;
    for app in owners.iter().chain([&c]) {
        assert!(app.await_all_hubs(Duration::from_secs(15)).await);
    }
    (owners, c)
}

#[tokio::test]
async fn scatter_over_three_instances_of_two_owners_gets_three_parts_and_unary_reaches_them_all() {
    require_docker!();
    let (h1, h2) = two_hubs().await;
    let hubs = format!("{},{}", h1.address, h2.address);
    let (owners, c) = cluster(&hubs).await;
    let instances: BTreeSet<String> = owners.iter().map(|o| o.instance_id().to_string()).collect();

    // Twice, so both hubs carry one (unkeyed round-robin).
    for _ in 0..2 {
        let result = c
            .queries()
            .scatter_gather(&Find { delay_ms: 0 }, ScatterOptions::default())
            .await
            .unwrap();
        assert_eq!(result.parts.len(), 3, "{result:?}");
        assert_eq!((result.failed, result.timed_out), (0, 0));
        let seen: BTreeSet<String> = result.parts.iter().map(|p| p.instance.clone()).collect();
        assert_eq!(seen, instances);
        for p in &result.parts {
            assert_eq!(p.outcome.as_ref().unwrap().service, p.service);
        }
        let services: BTreeSet<&str> = result.parts.iter().map(|p| p.service.as_str()).collect();
        assert_eq!(services, BTreeSet::from(["e2e-sa", "e2e-sb"]));
    }

    // The stream: three parts, then the end.
    let stream = c
        .queries()
        .scatter_gather_stream(&Find { delay_ms: 0 }, ScatterOptions::default());
    let parts: Vec<_> = stream.collect().await;
    assert_eq!(parts.len(), 3);
    assert!(
        parts
            .iter()
            .all(|p| p.as_ref().is_ok_and(|p| p.outcome.is_ok()))
    );

    // max_results: the first successful answer ends it.
    let first = c
        .queries()
        .scatter_gather(
            &Find { delay_ms: 0 },
            ScatterOptions {
                max_results: 1,
                ..ScatterOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(first.parts.len(), 1);

    // A point query on the shared type: one pool over every instance of every owner.
    let mut services = BTreeSet::new();
    for _ in 0..30 {
        let found = c
            .queries()
            .send_query(&Find { delay_ms: 0 }, SendOptions::default())
            .await
            .unwrap();
        services.insert(found.service);
    }
    assert_eq!(
        services,
        BTreeSet::from(["e2e-sa".to_string(), "e2e-sb".to_string()])
    );
    for hub in [&h1, &h2] {
        let types = get(hub, "/api/v1/types").await;
        let find = types["types"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["type"] == "e2e.Find")
            .cloned()
            .unwrap();
        assert_eq!(find["shared"], true, "{find}");
    }
    c.shutdown().await;
    for o in owners {
        o.shutdown().await;
    }
}

#[tokio::test]
async fn a_gather_whose_hub_dies_fails_with_connection_lost_and_the_next_succeeds_on_the_other() {
    require_docker!();
    let (h1, h2) = two_hubs().await;
    let hubs = format!("{},{}", h1.address, h2.address);
    let (owners, c) = cluster(&hubs).await;
    let c = Arc::new(c);

    let sender = Arc::clone(&c);
    let gather = tokio::spawn(async move {
        sender
            .queries()
            .scatter_gather(
                &Find { delay_ms: 3_000 },
                ScatterOptions {
                    timeout: Some(Duration::from_secs(10)),
                    ..ScatterOptions::default()
                },
            )
            .await
    });
    // The hub carrying it counts it in flight for the caller.
    let started = Instant::now();
    let (carrier, other) = loop {
        if in_flight_as_sender(&h1, "e2e-caller").await > 0 {
            break (&h1, &h2);
        }
        if in_flight_as_sender(&h2, "e2e-caller").await > 0 {
            break (&h2, &h1);
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the gather never showed up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // Killed, not stopped: a graceful stop may still relay parts (HANDLER_DISCONNECTED) and an end
    // when the owners' streams close first; a dead hub sends nothing more.
    let killed = std::process::Command::new("docker")
        .args(["kill", carrier.container.id()])
        .output()
        .unwrap();
    assert!(killed.status.success(), "{killed:?}");
    let e = gather.await.unwrap().unwrap_err();
    assert!(matches!(e, MessagingError::HubConnectionLost(_)), "{e:?}");

    let result = c
        .queries()
        .scatter_gather(&Find { delay_ms: 0 }, ScatterOptions::default())
        .await
        .unwrap();
    assert_eq!(result.parts.len(), 3, "{result:?}");
    let other_id = if std::ptr::eq(other, &h1) {
        IDS[0]
    } else {
        IDS[1]
    };
    assert_eq!(c.hub_ids(), [other_id]);
    Arc::into_inner(c).unwrap().shutdown().await;
    for o in owners {
        o.shutdown().await;
    }
}
