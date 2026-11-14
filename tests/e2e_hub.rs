//! End to end against the real kraken-hub image, Kafka and MongoDB.
mod support;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use kraken_messaging::hub::SendOptions;
use kraken_messaging::identity::Guard;
use kraken_messaging::{Command, Config, Event, KrakenMessaging, MessagingError, message_type};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Serialize, Deserialize)]
struct Ping {
    text: String,
}
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Pong {
    echoed: String,
}
message_type!(Ping, "t.Ping");
impl Command for Ping {
    type Reply = Pong;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Happened {
    text: String,
    at: DateTime<Utc>,
}
message_type!(Happened, "t.Happened");
impl Event for Happened {}

#[derive(Serialize, Deserialize)]
struct Nobody {}
message_type!(Nobody, "t.Nobody");
impl Command for Nobody {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Secret {}
message_type!(Secret, "t.Secret");
impl Command for Secret {
    type Reply = ();
}

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

fn config(service: &str, hub: &str) -> Config {
    let mut c = Config::new(service, hub);
    c.outbox_poll_interval = Duration::from_millis(100);
    c.reconnect_backoff_min = Duration::from_millis(100);
    c.reconnect_backoff_max = Duration::from_secs(2);
    c
}

/// Service B: owns `t.Ping` (replies and publishes `t.Happened`, so it has a store) and the
/// guarded `t.Secret`.
async fn start_b(cfg: Config, mongo: &str, kafka: &str) -> KrakenMessaging {
    let builder = KrakenMessaging::builder(cfg)
        .await
        .expect("service b builder")
        .store(support::mongo_store(mongo).await)
        .event_store(support::kafka_events(kafka));
    let events = builder.events();
    builder
        .command::<Ping, _, _>(move |p, _ctx| {
            let events = events.clone();
            async move {
                events
                    .publish(
                        &Happened {
                            text: p.text.clone(),
                            at: Utc::now(),
                        },
                        Some(&p.text),
                    )
                    .await?;
                Ok(Pong { echoed: p.text })
            }
        })
        .command::<Secret, _, _>(|_, _| async move { Ok(()) })
        .requires::<Secret>(Guard::role("kraken-admin"))
        .start()
        .await
        .expect("service b starts")
}

#[tokio::test]
async fn two_participants_round_trip() {
    require_docker!();
    let hub = support::containers::hub().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let (tx, mut rx) = mpsc::channel::<Happened>(8);
    let a = KrakenMessaging::builder(config("svc-a", &hub.address))
        .await
        .unwrap()
        .store(support::mongo_store(&mongo.uri_for("a")).await)
        .event_store(support::kafka_events(&kafka.bootstrap))
        .event::<Happened, _, _>(move |e, _ctx| {
            let tx = tx.clone();
            async move {
                tx.send(e).await.unwrap();
                Ok(())
            }
        })
        .start()
        .await
        .expect("service a starts");
    let b = start_b(
        config("svc-b", &hub.address),
        &mongo.uri_for("b"),
        &kafka.bootstrap,
    )
    .await;
    assert!(
        a.await_hub(Duration::from_secs(15)).await && b.await_hub(Duration::from_secs(15)).await
    );

    let pong = a
        .commands()
        .send_command(&Ping { text: "hi".into() }, SendOptions::default())
        .await
        .unwrap();
    assert_eq!(
        pong,
        Pong {
            echoed: "hi".into()
        }
    );
    let event = tokio::time::timeout(Duration::from_secs(30), rx.recv())
        .await
        .expect("event within 30s")
        .unwrap();
    assert_eq!(event.text, "hi");

    let topology: serde_json::Value = reqwest::get(format!("{}/api/v1/topology", hub.http))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let owners: Vec<&str> = topology["types"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|t| t["ownerServices"].as_array().unwrap())
        .map(|s| s.as_str().unwrap())
        .collect();
    assert!(owners.contains(&"svc-b"));
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn no_handler_is_reported() {
    require_docker!();
    let hub = support::containers::hub().await;
    let mut cfg = config("svc-a", &hub.address);
    cfg.outbox_enabled = false;
    cfg.consumer_enabled = false;
    let a = KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    assert!(a.await_hub(Duration::from_secs(15)).await);
    let err = a
        .commands()
        .send_command(&Nobody {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, MessagingError::NoHandler(ref t) if t == "t.Nobody"),
        "{err:?}"
    );
    a.shutdown().await;
}

#[tokio::test]
async fn permission_denied_with_signed_token() {
    require_docker!();
    let hub = support::containers::hub().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let jwks = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/certs"))
        .respond_with(ResponseTemplate::new(200).set_body_string(support::JWKS_JSON))
        .mount(&jwks)
        .await;

    let mut cfg_b = config("svc-b", &hub.address);
    cfg_b.jwks_url = Some(format!("{}/certs", jwks.uri()));
    cfg_b.issuer = Some(support::ISSUER.to_string());
    let b = start_b(cfg_b, &mongo.uri_for("b"), &kafka.bootstrap).await;
    let mut cfg_a = config("svc-a", &hub.address);
    cfg_a.consumer_enabled = false;
    let a = KrakenMessaging::builder(cfg_a)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    assert!(
        a.await_hub(Duration::from_secs(15)).await && b.await_hub(Duration::from_secs(15)).await
    );

    let anonymous = a
        .commands()
        .send_command(&Secret {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(anonymous, MessagingError::Unauthenticated(_)),
        "{anonymous:?}"
    );

    let user = support::token(
        "u-1",
        &["kraken-user"],
        support::ISSUER,
        support::now() + 300,
    );
    let denied = a
        .commands()
        .send_command(
            &Secret {},
            SendOptions {
                authorization: Some(format!("Bearer {user}")),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(denied, MessagingError::AccessDenied(_)),
        "{denied:?}"
    );

    let admin = support::token(
        "u-1",
        &["kraken-admin"],
        support::ISSUER,
        support::now() + 300,
    );
    a.commands()
        .send_command(
            &Secret {},
            SendOptions {
                authorization: Some(format!("Bearer {admin}")),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let forged = support::sign(
        support::OTHER_KEY_PEM,
        Some("k1"),
        "u-1",
        &["kraken-admin"],
        support::ISSUER,
        support::now() + 300,
    );
    let rejected = a
        .commands()
        .send_command(
            &Secret {},
            SendOptions {
                authorization: Some(format!("Bearer {forged}")),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(rejected, MessagingError::Unauthenticated(_)),
        "{rejected:?}"
    );
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn send_before_connected_is_hub_unavailable() {
    require_docker!();
    let mut cfg = config("svc-a", "static://127.0.0.1:1");
    cfg.outbox_enabled = false;
    cfg.consumer_enabled = false;
    let a = KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .start()
        .await
        .expect("start does not wait for the hub");
    let err = a
        .commands()
        .send_command(&Nobody {}, SendOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, MessagingError::HubUnavailable(_)), "{err:?}");
    assert!(!a.await_hub(Duration::from_millis(300)).await);
    a.shutdown().await;
}

#[tokio::test]
async fn reconnects_after_hub_restart() {
    require_docker!();
    let port = support::containers::free_port();
    let hub = support::containers::hub_on(port).await;
    let mut cfg_a = config("svc-a", &hub.address);
    cfg_a.outbox_enabled = false;
    cfg_a.consumer_enabled = false;
    let a = KrakenMessaging::builder(cfg_a)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    let mut cfg_b = config("svc-b", &hub.address);
    cfg_b.outbox_enabled = false;
    cfg_b.consumer_enabled = false;
    let b = KrakenMessaging::builder(cfg_b)
        .await
        .unwrap()
        .command::<Nobody, _, _>(|_, _| async move { Ok(()) })
        .start()
        .await
        .unwrap();
    assert!(
        a.await_hub(Duration::from_secs(15)).await && b.await_hub(Duration::from_secs(15)).await
    );
    a.commands()
        .send_command(&Nobody {}, SendOptions::default())
        .await
        .unwrap();

    hub.container.stop_with_timeout(None).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(matches!(
        a.commands()
            .send_command(&Nobody {}, SendOptions::default())
            .await,
        Err(MessagingError::HubUnavailable(_))
    ));
    hub.container.start().await.unwrap();

    assert!(
        a.await_hub(Duration::from_secs(30)).await && b.await_hub(Duration::from_secs(30)).await,
        "did not re-register after the hub restart"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        match a
            .commands()
            .send_command(&Nobody {}, SendOptions::default())
            .await
        {
            Ok(()) => break,
            Err(e) if tokio::time::Instant::now() < deadline => {
                eprintln!("retrying after {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("round trip did not recover: {e}"),
        }
    }
    a.shutdown().await;
    b.shutdown().await;
}

#[allow(dead_code)]
fn _assert_send_sync<T: Send + Sync>() {}
#[allow(dead_code)]
fn _app_is_shareable() {
    _assert_send_sync::<Arc<KrakenMessaging>>();
}

#[tokio::test]
async fn shutdown_is_prompt() {
    require_docker!();
    let hub = support::containers::hub().await;
    let mut cfg = config("svc-a", &hub.address);
    cfg.outbox_enabled = false;
    cfg.consumer_enabled = false;
    let a = KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    assert!(a.await_hub(Duration::from_secs(15)).await);
    let started = std::time::Instant::now();
    a.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "shutdown took {:?}",
        started.elapsed()
    );
}

#[derive(Serialize, Deserialize)]
struct Stall {}
message_type!(Stall, "t.Stall");
impl Command for Stall {
    type Reply = ();
}

/// Review fix 1: a request of A still in flight when A shuts down fails at once with
/// `HubConnectionLost` (it was written; the outcome is unknown) instead of waiting for its local
/// guard.
#[tokio::test]
async fn shutdown_fails_outgoing_requests_in_flight_with_hub_connection_lost() {
    require_docker!();
    let hub = support::containers::hub().await;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut cfg_b = config("svc-b", &hub.address);
    cfg_b.outbox_enabled = false;
    cfg_b.consumer_enabled = false;
    let (s, r) = (Arc::clone(&started), Arc::clone(&release));
    let b = KrakenMessaging::builder(cfg_b)
        .await
        .unwrap()
        .command::<Stall, _, _>(move |_, _| {
            let (s, r) = (Arc::clone(&s), Arc::clone(&r));
            async move {
                s.notify_one();
                r.notified().await;
                Ok(())
            }
        })
        .start()
        .await
        .unwrap();
    let mut cfg_a = config("svc-a", &hub.address);
    cfg_a.outbox_enabled = false;
    cfg_a.consumer_enabled = false;
    let builder_a = KrakenMessaging::builder(cfg_a).await.unwrap();
    let sender = builder_a.sender();
    let a = builder_a.start().await.unwrap();
    assert!(
        a.await_hub(Duration::from_secs(15)).await && b.await_hub(Duration::from_secs(15)).await
    );

    let call = tokio::spawn(async move {
        sender
            .send_command(
                &Stall {},
                SendOptions {
                    timeout: Some(Duration::from_secs(30)),
                    ..Default::default()
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("B never received the request");
    a.shutdown().await;
    let result = tokio::time::timeout(Duration::from_secs(3), call)
        .await
        .expect("the in-flight request outlived shutdown")
        .unwrap();
    assert!(
        matches!(result, Err(MessagingError::HubConnectionLost(_))),
        "{result:?}"
    );
    release.notify_one();
    b.shutdown().await;
}
