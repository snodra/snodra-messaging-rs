//! End to end against the real hub image (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`): a
//! service's event processors, a parked letter and the lag reach the hub's `GET /api/v1/processors`
//! and topology (ADR 0022).

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_messaging::{Config, Event, KrakenMessaging, MessagingError, message_type};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use support::containers::{HubOptions, hub_with};

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Ordered {
    order: String,
}
message_type!(Ordered, "e2e.Ordered");
impl Event for Ordered {}

async fn json(url: &str) -> serde_json::Value {
    reqwest::get(url).await.unwrap().json().await.unwrap()
}

#[tokio::test]
async fn the_hub_shows_the_processors_their_letters_and_their_lag() {
    require_docker!();
    let hub = hub_with(HubOptions::new()).await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "e2e.events";
    let mut config = Config::new(
        "svc-e2e-proc",
        &hub.address,
        &kafka.bootstrap,
        &mongo.uri_for("e2e"),
    );
    config.kafka_topic = topic.to_string();
    config.outbox_enabled = false;
    config.consumer_max_attempts = 1;
    config.hub_status_interval = Duration::from_millis(500);
    let failing = Arc::new(Mutex::new(true));
    let fail = Arc::clone(&failing);
    let app = KrakenMessaging::builder(config)
        .await
        .unwrap()
        .event::<Ordered, _, _>(|_, _| async { Ok(()) })
        .event_in::<Ordered, _, _>("orders", move |e, _| {
            let fail = Arc::clone(&fail);
            async move {
                if *fail.lock().unwrap() && e.order == "o-1" {
                    return Err(MessagingError::Storage("handler failed: boom".into()));
                }
                Ok(())
            }
        })
        .start()
        .await
        .unwrap();
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let p: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap)
        .create()
        .unwrap();
    for order in ["o-1", "o-2"] {
        let value = serde_json::to_vec(&Ordered {
            order: order.into(),
        })
        .unwrap();
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "type",
                value: Some("e2e.Ordered"),
            })
            .insert(Header {
                key: "id",
                value: Some(order),
            });
        p.send(
            FutureRecord::to(topic)
                .key("k")
                .payload(&value)
                .headers(headers),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    }

    // The hub's aggregate: both processors, the named one FAILING with its letters (o-1
    // HANDLER_FAILED, o-2 SEQUENCED behind it) and its last error.
    let url = format!("{}/api/v1/processors", hub.http);
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let service = loop {
        let view = json(&url).await;
        let service = view["services"]
            .as_array()
            .and_then(|all| all.iter().find(|s| s["serviceName"] == "svc-e2e-proc"))
            .cloned();
        if let Some(s) = service
            && s["processors"]
                .as_array()
                .and_then(|ps| ps.iter().find(|p| p["name"] == "orders"))
                .is_some_and(|p| p["deadLetters"] == 2 && p["lag"] == 0)
        {
            break s;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the hub never showed the letters: {view}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let processors = service["processors"].as_array().unwrap();
    let names: Vec<&str> = processors
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["default", "orders"], "{service}");
    let orders = &processors[1];
    assert_eq!(orders["groupId"], "svc-e2e-proc.orders", "{orders}");
    assert_eq!(orders["topic"], topic);
    assert_eq!(orders["state"], "FAILING", "{orders}");
    assert_eq!(orders["deadLetterMode"], "collection");
    assert_eq!(orders["failedTotal"], 1);
    assert_eq!(orders["eventTypes"], serde_json::json!(["e2e.Ordered"]));
    assert!(
        orders["lastError"]["message"]
            .as_str()
            .unwrap()
            .contains("handler failed: boom"),
        "{orders}"
    );
    assert_eq!(orders["instances"][0]["instanceId"], app.instance_id());
    assert_eq!(orders["instances"][0]["partitions"][0]["committed"], 2);
    assert_eq!(service["deadLetters"], 2, "{service}");
    let default = &processors[0];
    assert_eq!(default["groupId"], "svc-e2e-proc.default");
    assert_eq!(default["state"], "RUNNING", "{default}");
    assert_eq!(default["processedTotal"], 2);

    // The topology carries the instance's raw report.
    let topology = json(&format!("{}/api/v1/topology", hub.http)).await;
    let instance = topology["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["serviceName"] == "svc-e2e-proc")
        .unwrap()["instances"][0]
        .clone();
    assert_eq!(
        instance["processors"].as_array().unwrap().len(),
        2,
        "{instance}"
    );

    // Drained from the admin API: the hub shows it on the next sample.
    *failing.lock().unwrap() = false;
    let result = app.dead_letters().retry_all("orders").await.unwrap();
    assert_eq!((result.retried, result.failed), (2, 0));
    app.shutdown().await;
}
