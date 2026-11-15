//! What the management API really answers (rabbitmq:4.4.0-management), pinned: how an unnamed
//! consumer's properties are rendered while a reader without a name is open, and what the
//! consumer list and the overview say with metrics collection switched off by each of the two
//! settings that can do it. Whatever it answers, a subscription's members are listed or the
//! call fails: never "nobody" while an instance reads.

mod support;

use std::collections::HashMap;
use std::time::Duration;

use kraken_messaging::event_store::{
    EventStore, EventStoreError, SubscriptionAdmin, SubscriptionEvent, SubscriptionSpec,
};
use kraken_messaging_rabbitmq::RabbitEventStore;
use rabbitmq_stream_client::types::OffsetSpecification;
use rabbitmq_stream_client::{Client, ClientOptions};

/// Runs a blocking admin call on the blocking pool.
async fn members(admin: Box<dyn SubscriptionAdmin>) -> Result<Vec<String>, EventStoreError> {
    tokio::task::spawn_blocking(move || admin.members())
        .await
        .unwrap()
}

/// One subscription reading `source` (assigned), and an unnamed raw reader of its first
/// partition open beside it; what the API lists and what `members()` answers.
async fn observe(
    rabbit: &support::RabbitMq,
    source: &str,
) -> (
    (u16, String),
    serde_json::Value,
    Result<Vec<String>, EventStoreError>,
) {
    rabbit.super_stream(source, 2).await;
    let store = RabbitEventStore::new(rabbit.config()).unwrap();
    let spec = SubscriptionSpec {
        name: "svc.observed".into(),
        source: source.into(),
    };
    let mut sub = store.subscribe(spec.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let SubscriptionEvent::Assigned(s) = sub.next().await.unwrap()
                && !s.is_empty()
            {
                return;
            }
        }
    })
    .await
    .expect("assigned");
    let (host, port) = rabbit.stream.split_once(':').unwrap();
    let raw = Client::connect(
        ClientOptions::builder()
            .host(host)
            .port(port.parse().unwrap())
            .build(),
    )
    .await
    .unwrap();
    let ok = raw
        .subscribe(
            9,
            &format!("{source}-0"),
            OffsetSpecification::First,
            1,
            HashMap::new(),
        )
        .await
        .unwrap();
    assert!(ok.is_ok());
    // The API lists a consumer only after a collection interval (~5 s): ask until it lists both
    // readers (the subscription's, named, and the raw one, unnamed), or answers anything but 200
    // (metrics collection off).
    let consumers = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let (status, body) = rabbit.consumers().await;
            if status != 200 || lists_both(&body) {
                return (status, body);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the API lists both readers within 60 s");
    let overview = rabbit.overview().await;
    // Asked from another store (as another process would): only the API can tell.
    let elsewhere = RabbitEventStore::new(rabbit.config()).unwrap();
    let listed = members(elsewhere.admin(&spec).unwrap()).await;
    raw.close().await.unwrap();
    sub.close().await;
    store.close().await;
    (consumers, overview, listed)
}

/// Whether the consumer list `body` holds a named consumer and an unnamed one.
fn lists_both(body: &str) -> bool {
    let Ok(serde_json::Value::Array(consumers)) = serde_json::from_str(body) else {
        return false;
    };
    let named = |c: &&serde_json::Value| c["properties"].get("name").is_some();
    consumers.iter().any(|c| named(&c)) && consumers.iter().any(|c| !named(&c))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_management_api_as_it_answers() {
    // Default settings: the unnamed reader's properties, and members listed despite it.
    let rabbit = support::rabbitmq().await;
    let ((status, body), overview, listed) = observe(&rabbit, "observed.default").await;
    assert_eq!(status, 200, "{body}");
    let consumers: serde_json::Value = serde_json::from_str(&body).unwrap();
    let unnamed: Vec<&serde_json::Value> = consumers
        .as_array()
        .expect("an array")
        .iter()
        .filter(|c| c["properties"].get("name").is_none())
        .map(|c| &c["properties"])
        .collect();
    eprintln!(
        "default: unnamed reader properties {unnamed:?}; overview disable_stats {}",
        overview["disable_stats"]
    );
    assert!(
        !unnamed.is_empty(),
        "the unnamed reader is listed: {consumers}"
    );
    let listed = listed.expect("members despite an unnamed reader");
    assert_eq!(listed.len(), 1, "{listed:?}");
    drop(rabbit);

    for setting in [
        "management.disable_stats = true",
        "management_agent.disable_metrics_collector = true",
    ] {
        let rabbit = support::rabbitmq_with(setting).await;
        let ((status, body), overview, listed) = observe(&rabbit, "observed.off").await;
        eprintln!(
            "{setting}: consumers HTTP {status} {body}; overview disable_stats {}; members {listed:?}",
            overview["disable_stats"]
        );
        assert!(
            !matches!(&listed, Ok(m) if m.is_empty()),
            "{setting}: \"nobody reads\" while an instance reads: {listed:?}"
        );
    }
}
