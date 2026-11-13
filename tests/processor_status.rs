//! The processor status (ADR 0022) against a real Kafka: the committed offset and high watermark
//! are read off the consumer task (a handler that blocks does not stop them), so the lag of
//! records produced while the service was stopped shows; the partitions follow a rebalance.

mod support;

use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::proto::{ProcessorState, ProcessorStatus};
use kraken_messaging::{Config, Event, KrakenMessaging, KrakenMessagingBuilder, message_type};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use support::fake_hub::FakeHub;
use tokio::sync::Semaphore;

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Ticked {
    n: u32,
}
message_type!(Ticked, "st.Ticked");
impl Event for Ticked {}

/// The service's builder with the MongoDB message store of `mongo`.
async fn builder(
    service: &str,
    hub: &str,
    kafka: &str,
    mongo: &str,
    topic: &str,
) -> KrakenMessagingBuilder {
    KrakenMessaging::builder(config(service, hub, kafka, topic))
        .await
        .unwrap()
        .store(support::mongo_store(mongo).await)
}

fn config(service: &str, hub: &str, kafka: &str, topic: &str) -> Config {
    let mut c = Config::new(service, hub, kafka);
    c.kafka_topic = topic.to_string();
    c.outbox_enabled = false;
    c
}

async fn eventually<F>(what: &str, within: Duration, mut check: F)
where
    F: FnMut() -> bool,
{
    let started = std::time::Instant::now();
    while !check() {
        assert!(started.elapsed() < within, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn produce(bootstrap: &str, topic: &str, from: u32, to: u32) {
    let p: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    for n in from..to {
        let value = serde_json::to_vec(&Ticked { n }).unwrap();
        let id = format!("t-{n}");
        let headers = rdkafka::message::OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: "type",
                value: Some("st.Ticked"),
            })
            .insert(rdkafka::message::Header {
                key: "id",
                value: Some(id.as_str()),
            });
        let key = format!("k-{n}");
        p.send(
            FutureRecord::to(topic)
                .key(&key)
                .payload(&value)
                .headers(headers),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    }
}

fn default_of(app: &KrakenMessaging) -> ProcessorStatus {
    app.processors()
        .into_iter()
        .find(|p| p.name == "default")
        .unwrap()
}

#[tokio::test]
async fn the_lag_of_records_produced_while_stopped_shows_while_the_handler_is_busy() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "st.events";
    let uri = mongo.uri_for("lag");
    // The first run handles three records.
    let app = builder("svc-st", &hub.address(), &kafka.bootstrap, &uri, topic)
        .await
        .event::<Ticked, _, _>(|_, _| async { Ok(()) })
        .start()
        .await
        .unwrap();
    produce(&kafka.bootstrap, topic, 0, 3).await;
    eventually(
        "three handled and committed",
        Duration::from_secs(60),
        || {
            let s = default_of(&app);
            s.processed_total == 3 && s.partitions.first().is_some_and(|p| p.committed == 3)
        },
    )
    .await;
    let s = default_of(&app);
    assert_eq!(
        ProcessorState::try_from(s.state),
        Ok(ProcessorState::ProcessorRunning)
    );
    assert_eq!(s.partitions.len(), 1);
    assert_eq!(
        (
            s.partitions[0].position,
            s.partitions[0].high_watermark,
            s.partitions[0].lag
        ),
        (3, 3, 0)
    );
    app.shutdown().await;

    // Five more while it is stopped; the next run's handler blocks on the first one.
    produce(&kafka.bootstrap, topic, 3, 8).await;
    let gate = Arc::new(Semaphore::new(0));
    let held = Arc::clone(&gate);
    let meter = support::metrics::TestMeter::new();
    let app = KrakenMessaging::builder_with_meter(
        config("svc-st", &hub.address(), &kafka.bootstrap, topic),
        meter.meter(),
    )
    .await
    .unwrap()
    .store(support::mongo_store(&uri).await)
    .event::<Ticked, _, _>(move |_, _| {
        let held = Arc::clone(&held);
        async move {
            held.acquire().await.unwrap().forget();
            Ok(())
        }
    })
    .start()
    .await
    .unwrap();
    eventually("the lag of the five shows", Duration::from_secs(60), || {
        default_of(&app)
            .partitions
            .first()
            .is_some_and(|p| p.committed == 3 && p.high_watermark == 8)
    })
    .await;
    let p = default_of(&app).partitions[0];
    assert_eq!((p.partition, p.position, p.lag), (0, -1, 5), "{p:?}");
    // The gauges read the same status.
    assert_eq!(
        meter.value(
            kraken_messaging::metrics::PROCESSOR_LAG,
            &[("processor", "default"), ("partition", "0")]
        ),
        Some(5.0)
    );
    assert_eq!(
        meter.value(
            kraken_messaging::metrics::PROCESSOR_STATE,
            &[("processor", "default"), ("state", "running")]
        ),
        Some(1.0)
    );
    assert_eq!(
        meter.value(
            kraken_messaging::metrics::DEAD_LETTERS,
            &[("processor", "default")]
        ),
        Some(0.0)
    );
    gate.add_permits(5);
    eventually("caught up", Duration::from_secs(60), || {
        default_of(&app)
            .partitions
            .first()
            .is_some_and(|p| p.committed == 8 && p.position == 8 && p.lag == 0)
    })
    .await;
    assert_eq!(default_of(&app).processed_total, 5);
    app.shutdown().await;
}

#[tokio::test]
async fn the_partitions_follow_a_rebalance() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "rb.events";
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap)
        .create()
        .unwrap();
    admin
        .create_topics(
            &[NewTopic::new(topic, 2, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    let uri = mongo.uri_for("rb");
    let start = || async {
        builder("svc-rb", &hub.address(), &kafka.bootstrap, &uri, topic)
            .await
            .event::<Ticked, _, _>(|_, _| async { Ok(()) })
            .start()
            .await
            .unwrap()
    };
    let partitions = |app: &KrakenMessaging| -> Vec<i32> {
        default_of(app)
            .partitions
            .iter()
            .map(|p| p.partition)
            .collect()
    };
    let a = start().await;
    eventually("a alone holds both", Duration::from_secs(60), || {
        partitions(&a) == [0, 1]
    })
    .await;
    assert_eq!(
        ProcessorState::try_from(default_of(&a).state),
        Ok(ProcessorState::ProcessorRunning)
    );
    let b = start().await;
    eventually(
        "one each after the rebalance",
        Duration::from_secs(90),
        || {
            let (pa, pb) = (partitions(&a), partitions(&b));
            pa.len() == 1 && pb.len() == 1 && pa != pb
        },
    )
    .await;
    b.shutdown().await;
    eventually("a holds both again", Duration::from_secs(90), || {
        partitions(&a) == [0, 1]
    })
    .await;
    a.shutdown().await;
}
