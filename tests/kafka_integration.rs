mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use kraken_messaging::config::DeadLetterMode;
use kraken_messaging::hub::HandlerRegistry;
use kraken_messaging::kafka::{
    EventBus, EventConsumer, LocalEventDispatcher, NoSagas, OutboxPublisher, ProcessorSettings,
    ensure_indexes, headers,
};
use kraken_messaging::metrics::{self, Metrics};
use kraken_messaging::{Event, message_type};
use mongodb::bson::doc;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use support::metrics::TestMeter;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Clone)]
struct Happened {
    id: String,
    at: DateTime<Utc>,
}
message_type!(Happened, "t.Happened");
impl Event for Happened {}

const TOPIC: &str = "kraken.events";

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

async fn db(uri: &str) -> (mongodb::Client, mongodb::Database) {
    let client = mongodb::Client::with_uri_str(uri).await.unwrap();
    let db = client.default_database().expect("database in uri");
    ensure_indexes(
        &db,
        Duration::from_secs(7 * 86_400),
        Duration::from_secs(30 * 86_400),
    )
    .await
    .unwrap();
    (client, db)
}

fn raw_consumer(bootstrap: &str, group: &str) -> StreamConsumer {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    c.subscribe(&[TOPIC]).unwrap();
    c
}

/// The `default` processor's consumer of `registry` in `service`'s group, a failed record logged
/// and skipped (mode `log`).
fn consumer(
    bootstrap: &str,
    service: &str,
    client: mongodb::Client,
    db: mongodb::Database,
    registry: HandlerRegistry,
    max_attempts: u32,
    metrics: &Metrics,
) -> Arc<EventConsumer> {
    EventConsumer::processor(
        ProcessorSettings {
            name: "default".into(),
            bootstrap_servers: bootstrap.into(),
            group_id: format!("{service}.default"),
            topic: TOPIC.into(),
            max_attempts,
            backoff: Duration::from_millis(50),
            dead_letter_mode: DeadLetterMode::Log,
            dead_letter_retention: Duration::from_secs(86_400),
            db: None,
            metrics: metrics.clone(),
        },
        Arc::new(LocalEventDispatcher::new(
            client,
            db,
            Arc::new(registry),
            Arc::new(NoSagas),
        )),
    )
    .unwrap()
}

#[tokio::test]
async fn outbox_relays_to_kafka_with_kotlin_headers() {
    require_docker!();
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let (_client, db) = db(&mongo.uri).await;
    let bus = EventBus::new(db.clone());
    let meter = TestMeter::new();
    let publisher = OutboxPublisher::new(
        db.clone(),
        &kafka.bootstrap,
        TOPIC,
        "svc-a",
        Duration::from_millis(100),
        100,
        10,
        &Metrics::new(meter.meter()),
    )
    .unwrap();

    bus.publish(
        &Happened {
            id: "e-1".into(),
            at: Utc::now(),
        },
        Some("k1"),
    )
    .await
    .unwrap();
    let raw = raw_consumer(&kafka.bootstrap, "raw-reader");
    assert_eq!(publisher.relay_once().await.unwrap(), 1);
    assert_eq!(
        meter.value(metrics::OUTBOX_PUBLISHED, &[("outcome", "ok")]),
        Some(1.0)
    );
    assert_eq!(meter.value(metrics::OUTBOX_PENDING, &[]), Some(0.0));
    assert_eq!(meter.value(metrics::OUTBOX_OLDEST_AGE, &[]), Some(0.0));
    assert_eq!(publisher.relay_once().await.unwrap(), 0, "already sent");

    // The topic is auto-created by the first produce; the raw consumer may see transient metadata errors before that.
    let msg = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match raw.recv().await {
                Ok(m) => break m,
                Err(e) => eprintln!("raw consumer retrying after: {e}"),
            }
        }
    })
    .await
    .expect("record within 30s");
    assert_eq!(msg.key(), Some(b"k1".as_slice()));
    let body: serde_json::Value = serde_json::from_slice(msg.payload().unwrap()).unwrap();
    assert_eq!(body["id"], "e-1");
    assert!(body["at"].as_str().unwrap().ends_with('Z'));
    let hs = msg.headers().unwrap();
    let get = |k: &str| {
        hs.iter()
            .find(|h| h.key == k)
            .and_then(|h| h.value)
            .map(|v| String::from_utf8_lossy(v).to_string())
    };
    assert_eq!(get(headers::TYPE).as_deref(), Some("t.Happened"));
    assert!(uuid::Uuid::parse_str(&get(headers::ID).unwrap()).is_ok());
    assert!(get(headers::CORRELATION_ID).is_some());
    assert!(get(headers::TIMESTAMP).unwrap().ends_with('Z'));
    assert_eq!(get(headers::SOURCE).as_deref(), Some("svc-a"));
    assert_eq!(get(headers::USER_ID).as_deref(), Some(""));

    let sent = db
        .collection::<mongodb::bson::Document>("outbox")
        .find_one(doc! {})
        .await
        .unwrap()
        .unwrap();
    assert!(
        sent.get("sentAt")
            .is_some_and(|v| !matches!(v, mongodb::bson::Bson::Null))
    );
}

#[tokio::test]
async fn consumer_handles_once_and_dedupes() {
    require_docker!();
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let (client, db) = db(&mongo.uri).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let seen_user = Arc::new(std::sync::Mutex::new(None::<String>));
    let mut registry = HandlerRegistry::new();
    let seen = Arc::clone(&calls);
    let seen_user_w = Arc::clone(&seen_user);
    registry.event::<Happened, _, _>(move |e, ctx| {
        let seen = Arc::clone(&seen);
        let seen_user_w = Arc::clone(&seen_user_w);
        async move {
            assert_eq!(e.id, "dup-1");
            assert_eq!(ctx.message.message_id, "dup-1");
            *seen_user_w.lock().unwrap() = ctx.message.user_id().map(str::to_string);
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    let meter = TestMeter::new();
    let consumer = consumer(
        &kafka.bootstrap,
        "svc-b",
        client,
        db.clone(),
        registry,
        3,
        &Metrics::new(meter.meter()),
    );
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap)
        .create()
        .unwrap();
    let payload = serde_json::to_vec(&Happened {
        id: "dup-1".into(),
        at: Utc::now(),
    })
    .unwrap();
    for type_name in ["t.Happened", "t.Happened", "x.Foreign"] {
        let hs = rdkafka::message::OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: headers::TYPE,
                value: Some(type_name),
            })
            .insert(rdkafka::message::Header {
                key: headers::ID,
                value: Some("dup-1"),
            })
            .insert(rdkafka::message::Header {
                key: headers::USER_ID,
                value: Some("u-1"),
            });
        producer
            .send(
                FutureRecord::to(TOPIC)
                    .key("k")
                    .payload(&payload)
                    .headers(hs),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("handled within 30s");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "duplicate id handled once");
    assert_eq!(
        seen_user.lock().unwrap().as_deref(),
        Some("u-1"),
        "userId header reaches the event context like Kotlin's MessageContext.userId"
    );
    let markers = db
        .collection::<mongodb::bson::Document>("processed_events")
        .count_documents(doc! {})
        .await
        .unwrap();
    assert_eq!(markers, 1);
    let consumed = |type_name: &str, outcome: &str| {
        meter.value(
            metrics::EVENTS_CONSUMED,
            &[
                ("processor", "default"),
                ("type", type_name),
                ("outcome", outcome),
            ],
        )
    };
    assert_eq!(consumed("t.Happened", "handled"), Some(1.0));
    assert_eq!(consumed("t.Happened", "duplicate"), Some(1.0));
    assert_eq!(
        consumed("other", "skipped"),
        Some(1.0),
        "a foreign type is not a label value"
    );
    assert_eq!(
        meter.value(
            metrics::EVENT_HANDLER_DURATION,
            &[("processor", "default"), ("type", "t.Happened")]
        ),
        Some(2.0),
        "every stage-1 run is timed, the duplicate's too"
    );
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

#[tokio::test]
async fn panicking_handler_is_retried_then_skipped() {
    require_docker!();
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let (client, db) = db(&mongo.uri).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let good = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    let a = Arc::clone(&attempts);
    let g = Arc::clone(&good);
    registry.event::<Happened, _, _>(move |e, _| {
        let a = Arc::clone(&a);
        let g = Arc::clone(&g);
        async move {
            if e.id == "bad" {
                a.fetch_add(1, Ordering::SeqCst);
                panic!("handler exploded");
            }
            g.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    let meter = TestMeter::new();
    let consumer = consumer(
        &kafka.bootstrap,
        "svc-c",
        client,
        db.clone(),
        registry,
        2,
        &Metrics::new(meter.meter()),
    );
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap)
        .create()
        .unwrap();
    for id in ["bad", "good"] {
        let payload = serde_json::to_vec(&Happened {
            id: id.into(),
            at: Utc::now(),
        })
        .unwrap();
        let hs = rdkafka::message::OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: headers::TYPE,
                value: Some("t.Happened"),
            })
            .insert(rdkafka::message::Header {
                key: headers::ID,
                value: Some(id),
            });
        producer
            .send(
                FutureRecord::to(TOPIC)
                    .key("k")
                    .payload(&payload)
                    .headers(hs),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while good.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the record after the poison one is handled");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "poison record tried max_attempts times"
    );
    let markers = db
        .collection::<mongodb::bson::Document>("processed_events")
        .count_documents(doc! {})
        .await
        .unwrap();
    assert_eq!(markers, 1, "only the good record is marked processed");
    let consumed = |outcome: &str| {
        meter.value(
            metrics::EVENTS_CONSUMED,
            &[
                ("processor", "default"),
                ("type", "t.Happened"),
                ("outcome", outcome),
            ],
        )
    };
    assert_eq!(
        consumed("failed"),
        Some(1.0),
        "mode log: given up, no letter"
    );
    assert_eq!(consumed("handled"), Some(1.0));
    assert_eq!(
        meter.value(
            metrics::EVENT_HANDLER_DURATION,
            &[("processor", "default"), ("type", "t.Happened")]
        ),
        Some(3.0),
        "both attempts of the poison record and the good one"
    );
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

#[tokio::test]
async fn failed_send_marks_attempts() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let (_client, db) = db(&mongo.uri).await;
    let bus = EventBus::new(db.clone());
    for id in ["e-x", "e-y", "e-z"] {
        bus.publish(
            &Happened {
                id: id.into(),
                at: Utc::now(),
            },
            None,
        )
        .await
        .unwrap();
    }
    // Unreachable broker: the delivery times out, the document stays unsent with attempts = 1
    // and the batch stops there.
    let meter = TestMeter::new();
    let publisher = OutboxPublisher::new(
        db.clone(),
        "127.0.0.1:1",
        TOPIC,
        "svc-a",
        Duration::from_millis(100),
        2,
        10,
        &Metrics::new(meter.meter()),
    )
    .unwrap();
    assert_eq!(
        meter.value(metrics::OUTBOX_PENDING, &[]),
        Some(0.0),
        "no poll yet"
    );
    assert_eq!(publisher.relay_once().await.unwrap(), 0);
    assert_eq!(
        meter.value(metrics::OUTBOX_PUBLISHED, &[("outcome", "failed")]),
        Some(1.0)
    );
    assert_eq!(
        meter.value(metrics::OUTBOX_PENDING, &[]),
        Some(3.0),
        "a full batch: every pending document counted"
    );
    let age = meter.value(metrics::OUTBOX_OLDEST_AGE, &[]).unwrap();
    assert!(
        age > 0.0,
        "the oldest document waited out the failed send: {age}"
    );
    let d = db
        .collection::<mongodb::bson::Document>("outbox")
        .find_one(doc! { "attempts": 1 })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.get_i32("attempts").unwrap(), 1);
    assert_eq!(d.get("sentAt"), Some(&mongodb::bson::Bson::Null));
    assert_eq!(d.get("failedAt"), Some(&mongodb::bson::Bson::Null));
}

#[derive(Serialize, Deserialize)]
struct Huge {
    blob: String,
}
message_type!(Huge, "t.Huge");
impl Event for Huge {}

#[tokio::test]
async fn non_retriable_send_marks_failed_immediately() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let (_client, db) = db(&mongo.uri).await;
    let bus = EventBus::new(db.clone());
    // 2 MB payload exceeds the producer's message.max.bytes (1 MB): MessageSizeTooLarge is final, not retriable.
    bus.publish(
        &Huge {
            blob: "x".repeat(2 * 1024 * 1024),
        },
        None,
    )
    .await
    .unwrap();
    bus.publish(
        &Happened {
            id: "after".into(),
            at: Utc::now(),
        },
        None,
    )
    .await
    .unwrap();
    let publisher = OutboxPublisher::new(
        db.clone(),
        "127.0.0.1:1",
        TOPIC,
        "svc-a",
        Duration::from_millis(100),
        100,
        10,
        &Metrics::global(),
    )
    .unwrap();
    let _ = publisher.relay_once().await.unwrap();
    let huge = db
        .collection::<mongodb::bson::Document>("outbox")
        .find_one(doc! { "type": "t.Huge" })
        .await
        .unwrap()
        .unwrap();
    assert!(
        huge.get("failedAt")
            .is_some_and(|v| !matches!(v, mongodb::bson::Bson::Null)),
        "oversized document marked failed on the first attempt: {huge:?}"
    );
    assert_eq!(huge.get_i32("attempts").unwrap(), 1);
}
