//! Processor control (ADR 0023, P6b) through `KrakenMessaging`, against a real Kafka and MongoDB
//! (and the in-process fake hub): positioned markers, pause and resume (persisted, converging,
//! surviving a restart), the leased reset (prepare leaves the group, a second operation is
//! LOCKED, the lease expires by itself, an active member is GROUP_NOT_EMPTY, the replay of a
//! range with its reset handlers, `disallow_replay` and the sagas skipped), and the dead letters
//! through control requests.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_messaging::processors::RetryResult;
use kraken_messaging::{Config, Event, KrakenMessaging, MessagingError, message_type};
use mongodb::bson::{Bson, Document, doc};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use support::fake_hub::FakeHub;

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct Noted {
    note: String,
}
message_type!(Noted, "c.Noted");
impl Event for Noted {}

fn config(service: &str, hub: &str, kafka: &str, mongo: &str, topic: &str) -> Config {
    let mut c = Config::new(service, hub, kafka, mongo);
    c.kafka_topic = topic.to_string();
    c.consumer_max_attempts = 1;
    c.outbox_enabled = false;
    c.reconnect_backoff_min = Duration::from_millis(100);
    c.reconnect_backoff_max = Duration::from_secs(1);
    c.hub_status_interval = Duration::from_millis(300);
    c
}

async fn eventually<F, Fut>(what: &str, within: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = std::time::Instant::now();
    while !check().await {
        assert!(started.elapsed() < within, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

const WITHIN: Duration = Duration::from_secs(60);

fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap()
}

/// A `c.Noted` for `note` with id `note`, on `partition` (0 unless given).
async fn produce(p: &FutureProducer, topic: &str, partition: Option<i32>, note: &str) {
    let value = serde_json::to_vec(&Noted {
        note: note.to_string(),
    })
    .unwrap();
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "type",
            value: Some("c.Noted"),
        })
        .insert(Header {
            key: "id",
            value: Some(note),
        });
    let record: FutureRecord<'_, str, [u8]> = FutureRecord::to(topic)
        .key(note)
        .payload(value.as_slice())
        .headers(headers)
        .partition(partition.unwrap_or(0));
    p.send(record, Duration::from_secs(10)).await.unwrap();
}

/// The notes a handler saw (in order, repeats included), and notes it fails for.
#[derive(Clone, Default)]
struct Handler {
    seen: Arc<Mutex<Vec<String>>>,
    failing: Arc<Mutex<Vec<String>>>,
}

impl Handler {
    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    fn count(&self, note: &str) -> usize {
        self.seen().iter().filter(|n| *n == note).count()
    }

    fn fail(&self, note: &str) {
        self.failing.lock().unwrap().push(note.to_string());
    }

    fn heal(&self) {
        self.failing.lock().unwrap().clear();
    }

    fn handle(
        &self,
    ) -> impl Fn(
        Noted,
        kraken_messaging::EventContext,
    ) -> futures::future::BoxFuture<'static, Result<(), MessagingError>>
    + Send
    + Sync
    + 'static {
        let (seen, failing) = (Arc::clone(&self.seen), Arc::clone(&self.failing));
        move |e: Noted, _ctx| {
            let (seen, failing) = (Arc::clone(&seen), Arc::clone(&failing));
            Box::pin(async move {
                seen.lock().unwrap().push(e.note.clone());
                if failing.lock().unwrap().contains(&e.note) {
                    return Err(MessagingError::Storage(format!("cannot note {}", e.note)));
                }
                Ok(())
            })
        }
    }
}

async fn marker(db: &mongodb::Database, id: &str) -> Option<Document> {
    db.collection::<Document>("processed_events")
        .find_one(doc! { "_id": id })
        .await
        .unwrap()
}

/// Every stage-1 marker a Kafka consumer writes carries its position (`processor`, `topic`,
/// `partition` Int32, `offset` Int64); a dead letter's retry is that record's stage 1 and writes
/// the letter's position.
#[tokio::test]
async fn markers_carry_the_processor_and_the_records_position() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "pos.events";
    let (default, audit) = (Handler::default(), Handler::default());
    audit.fail("n-2");
    let app = KrakenMessaging::builder(config(
        "svc-pos",
        &hub.address(),
        &kafka.bootstrap,
        &mongo.uri_for("pos"),
        topic,
    ))
    .await
    .unwrap()
    .event::<Noted, _, _>(default.handle())
    .event_in::<Noted, _, _>("audit", audit.handle())
    .start()
    .await
    .unwrap();
    let db = app.database();
    let p = producer(&kafka.bootstrap);
    for note in ["n-1", "n-2"] {
        produce(&p, topic, None, note).await;
    }
    eventually("both handled and n-2 parked", WITHIN, || async {
        marker(&db, "n-2").await.is_some()
            && app.dead_letters().count(Some("audit")).await.unwrap() == 1
    })
    .await;

    for (id, processor, offset) in [
        ("n-1", "default", 0i64),
        ("n-2", "default", 1),
        ("processor:audit:n-1", "audit", 0),
    ] {
        let m = marker(&db, id).await.expect(id);
        assert_eq!(m.get_str("processor").unwrap(), processor, "{m}");
        assert_eq!(m.get_str("topic").unwrap(), topic, "{m}");
        assert_eq!(m.get("partition"), Some(&Bson::Int32(0)), "{m}");
        assert_eq!(m.get("offset"), Some(&Bson::Int64(offset)), "{m}");
        assert_eq!(m.get_str("type").unwrap(), "c.Noted", "{m}");
        assert!(m.get_datetime("processedAt").is_ok(), "{m}");
    }
    assert!(marker(&db, "processor:audit:n-2").await.is_none());

    audit.heal();
    let letter = format!("audit:{topic}:0:1");
    assert_eq!(
        app.dead_letters().retry(&letter).await.unwrap(),
        RetryResult::Retried
    );
    let m = marker(&db, "processor:audit:n-2").await.unwrap();
    assert_eq!(m.get_str("processor").unwrap(), "audit", "{m}");
    assert_eq!(m.get_str("topic").unwrap(), topic, "{m}");
    assert_eq!(m.get("partition"), Some(&Bson::Int32(0)), "{m}");
    assert_eq!(m.get("offset"), Some(&Bson::Int64(1)), "{m}");
    app.shutdown().await;
}
