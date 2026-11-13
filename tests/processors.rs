//! Named event processors and dead letters through `KrakenMessaging` (ADR 0022, P6a), against a
//! real Kafka and MongoDB (and the in-process fake hub): each processor its own group and marker
//! family, one event in two processors, failure isolation, sagas only in `default`, every letter
//! reason, sequencing per key (stage 2 still runs, a miss after a remote discard handles
//! normally), retrying the head and then the rest drains the key, idempotent parking on
//! redelivery, a letter that cannot be parked keeps its offset uncommitted, and modes `topic` and
//! `log`.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::TryStreamExt;
use kraken_messaging::config::DeadLetterMode;
use kraken_messaging::processors::{DeadLetters, RetryResult};
use kraken_messaging::proto::{ProcessorState, ProcessorStatus};
use kraken_messaging::saga::store::SagaDocument;
use kraken_messaging::store::{DeadLetter, DeadLetterReason};
use kraken_messaging::{
    Config, Event, KrakenMessaging, KrakenMessagingBuilder, MessagingError, message_type,
};
use kraken_messaging_mongo::documents::DeadLetterDocument;
use kraken_messaging_mongo::documents::SagaDoc;
use mongodb::bson::{Document, doc};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
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
struct Placed {
    order: String,
}
message_type!(Placed, "p.Placed");
impl Event for Placed {}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct Tracker {
    order: Option<String>,
}

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
    c.consumer_max_attempts = 2;
    c.outbox_enabled = false;
    c.reconnect_backoff_min = Duration::from_millis(100);
    c.reconnect_backoff_max = Duration::from_secs(1);
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

/// Produces one record: `type` and `id` headers unless `None`, `value` as given.
async fn produce_raw(
    p: &FutureProducer,
    topic: &str,
    key: Option<&str>,
    type_name: Option<&str>,
    id: Option<&str>,
    value: &[u8],
) {
    let mut headers = OwnedHeaders::new().insert(Header {
        key: "correlationId",
        value: Some("corr-x"),
    });
    if let Some(t) = type_name {
        headers = headers.insert(Header {
            key: "type",
            value: Some(t),
        });
    }
    if let Some(i) = id {
        headers = headers.insert(Header {
            key: "id",
            value: Some(i),
        });
    }
    let mut record: FutureRecord<'_, str, [u8]> =
        FutureRecord::to(topic).payload(value).headers(headers);
    if let Some(k) = key {
        record = record.key(k);
    }
    p.send(record, Duration::from_secs(10)).await.unwrap();
}

/// A `p.Placed` for `order` with key `key` and id `order`.
async fn produce(p: &FutureProducer, topic: &str, key: Option<&str>, order: &str) {
    let value = serde_json::to_vec(&Placed {
        order: order.to_string(),
    })
    .unwrap();
    produce_raw(p, topic, key, Some("p.Placed"), Some(order), &value).await;
}

/// Orders a handler saw, and orders it fails for.
#[derive(Clone, Default)]
struct Handler {
    seen: Arc<Mutex<Vec<String>>>,
    failing: Arc<Mutex<Vec<String>>>,
}

impl Handler {
    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    fn fail(&self, order: &str) {
        self.failing.lock().unwrap().push(order.to_string());
    }

    fn heal(&self) {
        self.failing.lock().unwrap().clear();
    }

    fn handle(
        &self,
    ) -> impl Fn(
        Placed,
        kraken_messaging::EventContext,
    ) -> futures::future::BoxFuture<'static, Result<(), MessagingError>>
    + Send
    + Sync
    + 'static {
        let (seen, failing) = (Arc::clone(&self.seen), Arc::clone(&self.failing));
        move |e: Placed, _ctx| {
            let (seen, failing) = (Arc::clone(&seen), Arc::clone(&failing));
            Box::pin(async move {
                seen.lock().unwrap().push(e.order.clone());
                if failing.lock().unwrap().contains(&e.order) {
                    return Err(MessagingError::Storage(format!(
                        "cannot handle {}",
                        e.order
                    )));
                }
                Ok(())
            })
        }
    }
}

async fn letters(db: &mongodb::Database) -> Vec<DeadLetter> {
    let found: Vec<DeadLetterDocument> = db
        .collection::<DeadLetterDocument>("dead_letters")
        .find(doc! {})
        .sort(doc! { "offset": 1 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    found.into_iter().map(DeadLetter::from).collect()
}

async fn markers(db: &mongodb::Database) -> Vec<String> {
    let mut all: Vec<String> = db
        .collection::<Document>("processed_events")
        .distinct("_id", doc! {})
        .await
        .unwrap()
        .into_iter()
        .map(|b| b.as_str().unwrap().to_string())
        .collect();
    all.sort();
    all
}

fn status<'a>(all: &'a [ProcessorStatus], name: &str) -> &'a ProcessorStatus {
    all.iter()
        .find(|p| p.name == name)
        .unwrap_or_else(|| panic!("no processor {name} in {all:?}"))
}

fn state(p: &ProcessorStatus) -> ProcessorState {
    ProcessorState::try_from(p.state).unwrap()
}

/// The committed offset of `group` on partition 0 of `topic` (-1 none).
fn committed(bootstrap: &str, group: &str, topic: &str) -> i64 {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, 0);
    // The group coordinator may not be known yet (NotCoordinator): no commit seen.
    let Ok(got) = c.committed_offsets(tpl, Duration::from_secs(10)) else {
        return -1;
    };
    match got.find_partition(topic, 0).map(|p| p.offset()) {
        Some(Offset::Offset(o)) => o,
        _ => -1,
    }
}

#[tokio::test]
async fn an_invalid_processor_name_fails_the_start_naming_the_handler() {
    for (name, expected) in [
        ("Orders", "^[a-z0-9][a-z0-9-]{0,47}$"),
        ("default", "'default'"),
    ] {
        let builder =
            KrakenMessaging::builder(Config::new("svc", "static://127.0.0.1:1", "127.0.0.1:1"))
                .await
                .unwrap();
        let e = builder
            .event_in::<Placed, _, _>(name, |_, _| async { Ok(()) })
            .start()
            .await
            .err()
            .expect("refused");
        let e = e.to_string();
        assert!(
            e.contains("p.Placed") && e.contains(expected),
            "{name}: {e}"
        );
    }
}

/// The admin API serves the processors that have handlers here, whether or not the consumer runs
/// on this instance (an admin-only deployment with `KRAKEN_CONSUMER_ENABLED=false`).
#[tokio::test]
async fn the_admin_api_knows_the_processors_without_a_running_consumer() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let mut c = config(
        "svc-admin",
        "static://127.0.0.1:1",
        "127.0.0.1:1",
        "a.events",
    );
    c.consumer_enabled = false;
    let app = KrakenMessaging::builder(c)
        .await
        .unwrap()
        .store(support::mongo_store(&mongo.uri_for("admin")).await)
        .event::<Placed, _, _>(|_, _| async { Ok(()) })
        .event_in::<Placed, _, _>("audit", |_, _| async { Ok(()) })
        .start()
        .await
        .unwrap();
    let admin = app.dead_letters();
    assert_eq!(admin.count(Some("audit")).await.unwrap(), 0);
    assert_eq!(admin.count(Some("default")).await.unwrap(), 0);
    assert!(admin.count(Some("other")).await.is_err());
    assert!(app.processors().is_empty(), "no processor started");
    app.shutdown().await;
}

#[tokio::test]
async fn named_processors_have_their_own_group_and_markers_and_never_hold_each_other() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "p.events";
    let (default, audit, mail) = (Handler::default(), Handler::default(), Handler::default());
    audit.fail("o-2");
    let app = builder(
        "svc-p",
        &hub.address(),
        &kafka.bootstrap,
        &mongo.uri_for("named"),
        topic,
    )
    .await
    .event::<Placed, _, _>(default.handle())
    .event_in::<Placed, _, _>("audit", audit.handle())
    .event_in::<Placed, _, _>("mail", mail.handle())
    .saga::<Tracker>("p.Tracker", |d| {
        d.start_on::<Placed>(
            "order",
            |e| Some(e.order.clone()),
            |s, e, _| {
                s.order = Some(e.order.clone());
                Ok(())
            },
        )
    })
    .start()
    .await
    .unwrap();
    let db = support::mongo_db(&mongo.uri_for("named")).await;
    let p = producer(&kafka.bootstrap);
    for (key, order) in [("k1", "o-1"), ("k2", "o-2"), ("k3", "o-3")] {
        produce(&p, topic, Some(key), order).await;
    }

    eventually("every processor saw the three", WITHIN, || async {
        default.seen().len() == 3 && mail.seen().len() == 3 && audit.seen().len() >= 4
    })
    .await;
    eventually("audit parked o-2", WITHIN, || async {
        letters(&db).await.len() == 1
    })
    .await;
    let parked = letters(&db).await;
    assert_eq!(parked[0].processor, "audit");
    assert_eq!(parked[0].event_id.as_deref(), Some("o-2"));
    assert_eq!(parked[0].reason, DeadLetterReason::HandlerFailed);
    assert_eq!(parked[0].attempts, 2);
    assert_eq!(parked[0].id, format!("audit:{topic}:0:1"));
    eventually("audit went on after the failure", WITHIN, || async {
        audit.seen().contains(&"o-3".to_string())
    })
    .await;
    // Each processor its own marker family; the failed one left none for o-2.
    let m = markers(&db).await;
    for order in ["o-1", "o-2", "o-3"] {
        assert!(m.contains(&format!("processor:default:{order}")), "{m:?}");
        assert!(m.contains(&format!("processor:mail:{order}")), "{m:?}");
    }
    assert!(m.contains(&"processor:audit:o-1".to_string()));
    assert!(!m.contains(&"processor:audit:o-2".to_string()));
    assert!(m.contains(&"processor:audit:o-3".to_string()));
    // Sagas run in default only: one saga per order, the named processors add nothing.
    let sagas: Vec<SagaDoc> = db
        .collection::<SagaDoc>("sagas")
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(sagas.len(), 3);
    eventually("each group committed its three records", WITHIN, || async {
        ["svc-p.default", "svc-p.audit", "svc-p.mail"]
            .iter()
            .all(|g| committed(&kafka.bootstrap, g, topic) == 3)
    })
    .await;

    let all = app.processors();
    assert_eq!(
        all.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
        ["default", "audit", "mail"]
    );
    assert_eq!(status(&all, "default").group_id, "svc-p.default");
    assert_eq!(status(&all, "audit").group_id, "svc-p.audit");
    assert_eq!(status(&all, "audit").topic, topic);
    assert_eq!(status(&all, "audit").event_types, ["p.Placed"]);
    assert_eq!(status(&all, "audit").failed_total, 1);
    assert_eq!(status(&all, "audit").processed_total, 2);
    assert_eq!(
        state(status(&all, "audit")),
        ProcessorState::ProcessorFailing
    );
    assert!(
        status(&all, "audit")
            .last_error
            .contains("cannot handle o-2")
    );
    assert_eq!(
        state(status(&all, "mail")),
        ProcessorState::ProcessorRunning,
        "{:?}",
        status(&all, "mail")
    );
    assert_eq!(status(&all, "mail").failed_total, 0);
    assert_eq!(status(&all, "default").processed_total, 3);
    eventually("the dead-letter count is sampled", WITHIN, || async {
        status(&app.processors(), "audit").dead_letters == 1
    })
    .await;
    assert_eq!(status(&app.processors(), "mail").dead_letters, 0);
    app.shutdown().await;
}

#[tokio::test]
async fn a_failing_key_parks_its_later_records_while_other_keys_flow_until_it_is_drained() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "s.events";
    let handler = Handler::default();
    handler.fail("o-bad");
    let meter = support::metrics::TestMeter::new();
    let app = KrakenMessaging::builder_with_meter(
        config("svc-s", &hub.address(), &kafka.bootstrap, topic),
        meter.meter(),
    )
    .await
    .unwrap()
    .store(support::mongo_store(&mongo.uri_for("seq")).await)
    .event::<Placed, _, _>(handler.handle())
    .saga::<Tracker>("s.Tracker", |d| {
        d.start_on::<Placed>(
            "order",
            |e| Some(e.order.clone()),
            |s, e, _| {
                s.order = Some(e.order.clone());
                Ok(())
            },
        )
    })
    .start()
    .await
    .unwrap();
    let db = support::mongo_db(&mongo.uri_for("seq")).await;
    let admin = app.dead_letters();
    let p = producer(&kafka.bootstrap);
    produce(&p, topic, Some("k1"), "o-bad").await; // 0: HANDLER_FAILED
    produce(&p, topic, Some("k1"), "o-next").await; // 1: SEQUENCED
    produce(&p, topic, Some("k2"), "o-other").await; // 2: handled
    produce_raw(&p, topic, Some("k1"), None, Some("x-1"), b"{}").await; // 3: INVALID (no type)
    produce_raw(&p, topic, Some("k3"), Some("p.Placed"), Some("x-2"), b"[1]").await; // 4: INVALID
    produce_raw(&p, topic, Some("k1"), Some("p.Unknown"), Some("x-3"), b"{}").await; // 5: skipped
    produce(&p, topic, None, "o-keyless").await; // 6: handled
    produce(&p, topic, Some("k3"), "o-k3").await; // 7: an INVALID letter never blocks its key

    eventually("all eight records acknowledged", WITHIN, || async {
        committed(&kafka.bootstrap, "svc-s.default", topic) == 8
    })
    .await;
    let parked = letters(&db).await;
    let reasons: Vec<(i64, DeadLetterReason)> =
        parked.iter().map(|l| (l.offset, l.reason)).collect();
    assert_eq!(
        reasons,
        [
            (0, DeadLetterReason::HandlerFailed),
            (1, DeadLetterReason::Sequenced),
            (3, DeadLetterReason::Invalid),
            (4, DeadLetterReason::Invalid),
        ]
    );
    // One outcome per record: a malformed record of a handled type keeps its type, one without a
    // type header (or of a type nobody handles here) is `other`.
    let consumed = |type_name: &str, outcome: &str| {
        meter.value(
            kraken_messaging::metrics::EVENTS_CONSUMED,
            &[
                ("processor", "default"),
                ("type", type_name),
                ("outcome", outcome),
            ],
        )
    };
    assert_eq!(consumed("p.Placed", "parked"), Some(3.0));
    assert_eq!(consumed("p.Placed", "handled"), Some(3.0));
    assert_eq!(consumed("other", "parked"), Some(1.0));
    assert_eq!(consumed("other", "skipped"), Some(1.0));
    let head = parked[0].id.clone();
    assert_eq!(parked[1].blocked_by.as_deref(), Some(head.as_str()));
    assert_eq!(parked[1].attempts, 0);
    assert_eq!(parked[1].error.type_name, "Sequenced");
    assert_eq!(parked[2].error.message, "record without type header");
    assert!(parked[2].event_type.is_none());
    assert_eq!(parked[3].error.message, "payload is not a JSON object");
    assert_eq!(parked[3].payload, b"[1]");
    assert_eq!(
        handler.seen(),
        ["o-bad", "o-bad", "o-other", "o-keyless", "o-k3"],
        "o-next never reached stage 1"
    );
    // Stage 2 ran for the HANDLER_FAILED and the SEQUENCED record alike.
    let sagas: Vec<String> = db
        .collection::<SagaDoc>("sagas")
        .find(doc! {})
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(SagaDocument::from)
        .map(|s| {
            serde_json::from_str::<serde_json::Value>(&s.state).unwrap()["order"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    for order in ["o-bad", "o-next", "o-other", "o-keyless", "o-k3"] {
        assert!(sagas.contains(&order.to_string()), "{order}: {sagas:?}");
    }

    // The cause is fixed; the head retried, the next one still waits, a new record queues
    // behind it; retrying it and the new one drains the key.
    handler.heal();
    assert_eq!(
        admin.retry(&parked[1].id).await.unwrap(),
        RetryResult::Blocked(head.clone())
    );
    assert_eq!(admin.retry(&head).await.unwrap(), RetryResult::Retried);
    produce(&p, topic, Some("k1"), "o-mid").await; // 8: SEQUENCED behind o-next
    eventually("o-mid parked", WITHIN, || async {
        letters(&db).await.len() == 4
    })
    .await;
    let mid = letters(&db)
        .await
        .into_iter()
        .find(|l| l.offset == 8)
        .unwrap();
    assert_eq!(mid.reason, DeadLetterReason::Sequenced);
    assert_eq!(mid.blocked_by.as_deref(), Some(parked[1].id.as_str()));
    assert_eq!(
        admin.retry(&parked[1].id).await.unwrap(),
        RetryResult::Retried
    );
    assert_eq!(admin.retry(&mid.id).await.unwrap(), RetryResult::Retried);
    produce(&p, topic, Some("k1"), "o-after").await; // 9: the key is free again
    eventually("o-after handled", WITHIN, || async {
        handler.seen().contains(&"o-after".to_string())
    })
    .await;
    assert_eq!(letters(&db).await.len(), 2, "the two INVALID letters stay");

    // Another instance (here: its own admin API) parks nothing but discards: this consumer
    // still holds the key in its set, confirms it against the collection and handles normally.
    handler.fail("o-bad2");
    produce(&p, topic, Some("k5"), "o-bad2").await; // 10
    eventually("o-bad2 parked", WITHIN, || async {
        letters(&db).await.len() == 3
    })
    .await;
    let remote = DeadLetters::new(
        Some(
            support::mongo_store(&mongo.uri_for("seq"))
                .await
                .dead_letters(),
        ),
        DeadLetterMode::Collection,
        vec![],
    );
    let _ = remote; // no processor runs there: discard straight in the collection instead
    let bad2 = letters(&db)
        .await
        .into_iter()
        .find(|l| l.offset == 10)
        .unwrap();
    db.collection::<Document>("dead_letters")
        .delete_one(doc! { "_id": &bad2.id })
        .await
        .unwrap();
    produce(&p, topic, Some("k5"), "o-k5").await; // 11
    eventually("o-k5 handled after the miss", WITHIN, || async {
        handler.seen().contains(&"o-k5".to_string())
    })
    .await;
    assert_eq!(letters(&db).await.len(), 2);

    // A payload the handler cannot decode is not retried: HANDLER_FAILED after one attempt.
    let seen = handler.seen().len();
    produce_raw(
        &p,
        topic,
        Some("k6"),
        Some("p.Placed"),
        Some("x-6"),
        br#"{"order":5}"#,
    )
    .await; // 12
    eventually("the undecodable record parked", WITHIN, || async {
        letters(&db).await.len() == 3
    })
    .await;
    let undecodable = letters(&db)
        .await
        .into_iter()
        .find(|l| l.offset == 12)
        .unwrap();
    assert_eq!(undecodable.reason, DeadLetterReason::HandlerFailed);
    assert_eq!(undecodable.attempts, 1);
    assert_eq!(undecodable.error.type_name, "InvalidMessage");
    assert_eq!(
        handler.seen().len(),
        seen,
        "decoding fails before the handler"
    );
    let all = app.processors();
    assert_eq!(status(&all, "default").event_types, ["p.Placed"]);
    assert_eq!(status(&all, "default").failed_total, 3);
    app.shutdown().await;
}

/// A letter that cannot be written (here: the collection's validator refuses it, as a MongoDB
/// that is down would) keeps the record's offset uncommitted; it is parked once the store takes
/// it. Redelivered later (the group's offset reset), the record is found parked: one letter, no
/// second handler run.
#[tokio::test]
async fn a_letter_is_written_before_its_offset_is_committed_and_parked_once_on_redelivery() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "m.events";
    let uri = mongo.uri_for("down");
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.default_database().unwrap();
    db.create_collection("dead_letters")
        .validator(doc! { "$jsonSchema": { "required": ["neverWritten"] } })
        .await
        .unwrap();
    let handler = Handler::default();
    handler.fail("o-1");
    let start = || async {
        builder("svc-m", &hub.address(), &kafka.bootstrap, &uri, topic)
            .await
            .event::<Placed, _, _>(handler.handle())
            .start()
            .await
            .unwrap()
    };
    let app = start().await;
    let p = producer(&kafka.bootstrap);
    produce(&p, topic, Some("k"), "o-1").await;
    eventually("o-1 exhausted its attempts", WITHIN, || async {
        handler.seen().len() == 2
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(letters(&db).await.is_empty());
    assert!(
        committed(&kafka.bootstrap, "svc-m.default", topic) <= 0,
        "not acknowledged"
    );
    let s = app.processors();
    assert_eq!(
        state(status(&s, "default")),
        ProcessorState::ProcessorFailing
    );
    assert!(!status(&s, "default").last_error.is_empty());

    db.run_command(doc! { "collMod": "dead_letters", "validator": {} })
        .await
        .unwrap();
    eventually("parked once the store takes it", WITHIN, || async {
        letters(&db).await.len() == 1
    })
    .await;
    eventually("then acknowledged", WITHIN, || async {
        committed(&kafka.bootstrap, "svc-m.default", topic) == 1
    })
    .await;
    assert_eq!(handler.seen().len(), 2, "the handler did not run again");
    app.shutdown().await;

    // Redelivery: the group starts over at offset 0.
    let reset: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap)
        .set("group.id", "svc-m.default")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Offset(0))
        .unwrap();
    reset
        .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    drop(reset);
    assert_eq!(committed(&kafka.bootstrap, "svc-m.default", topic), 0);
    let app = start().await;
    eventually("acknowledged again", WITHIN, || async {
        committed(&kafka.bootstrap, "svc-m.default", topic) == 1
    })
    .await;
    let parked = letters(&db).await;
    assert_eq!(parked.len(), 1, "parked once");
    assert_eq!(parked[0].reason, DeadLetterReason::HandlerFailed);
    assert_eq!(
        handler.seen().len(),
        2,
        "its key is parked: stage 1 did not run again"
    );
    app.shutdown().await;
}

/// A letter that cannot be written (here: a validator that refuses every insert, as a store
/// that is down would) does not hold the poll loop: the record is not committed and comes back
/// (its partition sought back to it) about every second, while the other partition's records
/// flow and the later records of its own partition wait. Once the store takes the letter it is
/// parked once, from the remembered failure (the handler does not run again), and then the
/// partition moves on.
#[tokio::test]
async fn a_letter_that_cannot_be_written_holds_its_record_but_not_the_poll_loop() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "w.events";
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
    let uri = mongo.uri_for("w");
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.default_database().unwrap();
    db.create_collection("dead_letters")
        .validator(doc! { "$jsonSchema": { "required": ["neverWritten"] } })
        .await
        .unwrap();
    let handler = Handler::default();
    handler.fail("o-1");
    let app = builder("svc-w", &hub.address(), &kafka.bootstrap, &uri, topic)
        .await
        .event::<Placed, _, _>(handler.handle())
        .start()
        .await
        .unwrap();
    let p = producer(&kafka.bootstrap);
    let to = |partition: i32, key: &'static str, order: &'static str| {
        let p = p.clone();
        async move {
            let value = serde_json::to_vec(&Placed {
                order: order.to_string(),
            })
            .unwrap();
            let headers = OwnedHeaders::new()
                .insert(Header {
                    key: "type",
                    value: Some("p.Placed"),
                })
                .insert(Header {
                    key: "id",
                    value: Some(order),
                });
            let record: FutureRecord<'_, str, [u8]> = FutureRecord::to(topic)
                .partition(partition)
                .key(key)
                .payload(value.as_slice())
                .headers(headers);
            p.send(record, Duration::from_secs(10)).await.unwrap();
        }
    };
    let runs = |order: &str| handler.seen().iter().filter(|o| *o == order).count();

    to(0, "k", "o-1").await;
    eventually("o-1 exhausted its attempts", WITHIN, || async {
        runs("o-1") == 2
    })
    .await;
    to(0, "x", "o-3").await;
    to(1, "j", "o-2").await;
    eventually(
        "the other partition flows while the letter cannot be written",
        WITHIN,
        || async { runs("o-2") == 1 },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(letters(&db).await.is_empty());
    assert_eq!(runs("o-3"), 0, "a later record of the partition waits");
    assert!(
        committed(&kafka.bootstrap, "svc-w.default", topic) <= 0,
        "not acknowledged"
    );
    assert_eq!(
        state(status(&app.processors(), "default")),
        ProcessorState::ProcessorFailing
    );

    db.run_command(doc! { "collMod": "dead_letters", "validator": {} })
        .await
        .unwrap();
    eventually("then o-3 is handled", WITHIN, || async { runs("o-3") == 1 }).await;
    eventually("and partition 0 acknowledged", WITHIN, || async {
        committed(&kafka.bootstrap, "svc-w.default", topic) == 2
    })
    .await;
    let parked = letters(&db).await;
    assert_eq!(parked.len(), 1, "parked once");
    assert_eq!(parked[0].id, "default:w.events:0:0");
    assert_eq!(parked[0].reason, DeadLetterReason::HandlerFailed);
    assert_eq!(parked[0].attempts, 2);
    assert_eq!(
        runs("o-1"),
        2,
        "the redeliveries did not run the handler again"
    );
    app.shutdown().await;
}

#[tokio::test]
async fn mode_topic_produces_to_the_dlt_and_mode_log_skips() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let p = producer(&kafka.bootstrap);

    // topic
    let handler = Handler::default();
    handler.fail("o-1");
    let mut c = config("svc-t", &hub.address(), &kafka.bootstrap, "t.events");
    c.dead_letter_mode = DeadLetterMode::Topic;
    let app = KrakenMessaging::builder(c)
        .await
        .unwrap()
        .store(support::mongo_store(&mongo.uri_for("t")).await)
        .event::<Placed, _, _>(handler.handle())
        .start()
        .await
        .unwrap();
    produce(&p, "t.events", Some("k"), "o-1").await;
    produce_raw(
        &p,
        "t.events",
        Some("k"),
        Some("p.Placed"),
        Some("x"),
        b"nope",
    )
    .await;
    produce(&p, "t.events", Some("k"), "o-2").await;
    let dlt: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap)
        .set("group.id", "dlt-reader")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();
    dlt.subscribe(&["t.events.dlt"]).unwrap();
    let mut got = Vec::new();
    tokio::time::timeout(WITHIN, async {
        while got.len() < 2 {
            match dlt.recv().await {
                Ok(m) => got.push(m.detach()),
                Err(e) => eprintln!("dlt reader retrying: {e}"),
            }
        }
    })
    .await
    .expect("two records on the DLT");
    let header = |m: &rdkafka::message::OwnedMessage, name: &str| {
        m.headers()
            .unwrap()
            .iter()
            .filter(|h| h.key == name)
            .last()
            .and_then(|h| h.value)
            .map(|v| String::from_utf8_lossy(v).to_string())
    };
    assert_eq!(got[0].key(), Some(b"k".as_slice()));
    assert_eq!(
        header(&got[0], "kraken-dlt-processor").as_deref(),
        Some("default")
    );
    assert_eq!(
        header(&got[0], "kraken-dlt-reason").as_deref(),
        Some("HANDLER_FAILED")
    );
    assert!(
        header(&got[0], "kraken-dlt-error")
            .unwrap()
            .contains("cannot handle o-1")
    );
    assert_eq!(
        header(&got[0], "kraken-dlt-original-topic").as_deref(),
        Some("t.events")
    );
    assert_eq!(
        header(&got[0], "kraken-dlt-original-partition").as_deref(),
        Some("0")
    );
    assert_eq!(
        header(&got[0], "kraken-dlt-original-offset").as_deref(),
        Some("0")
    );
    assert_eq!(
        header(&got[0], "id").as_deref(),
        Some("o-1"),
        "original headers kept"
    );
    assert_eq!(
        header(&got[1], "kraken-dlt-reason").as_deref(),
        Some("INVALID")
    );
    assert_eq!(
        header(&got[1], "kraken-dlt-original-offset").as_deref(),
        Some("1")
    );
    assert_eq!(got[1].payload(), Some(b"nope".as_slice()));
    eventually(
        "o-2 handled: not sequenced in mode topic",
        WITHIN,
        || async { handler.seen().contains(&"o-2".to_string()) },
    )
    .await;
    assert!(
        letters(&support::mongo_db(&mongo.uri_for("t")).await)
            .await
            .is_empty()
    );
    let e = app.dead_letters().list(None, 100).await.unwrap_err();
    assert!(e.to_string().contains("topic"), "{e}");
    let s = app.processors();
    assert_eq!(status(&s, "default").dead_letter_mode, "topic");
    assert_eq!(status(&s, "default").failed_total, 1);
    app.shutdown().await;

    // log
    let handler = Handler::default();
    handler.fail("o-1");
    let mut c = config("svc-l", &hub.address(), &kafka.bootstrap, "l.events");
    c.dead_letter_mode = DeadLetterMode::Log;
    let app = KrakenMessaging::builder(c)
        .await
        .unwrap()
        .store(support::mongo_store(&mongo.uri_for("l")).await)
        .event::<Placed, _, _>(handler.handle())
        .start()
        .await
        .unwrap();
    produce(&p, "l.events", Some("k"), "o-1").await;
    produce(&p, "l.events", Some("k"), "o-2").await;
    eventually("both acknowledged", WITHIN, || async {
        committed(&kafka.bootstrap, "svc-l.default", "l.events") == 2
    })
    .await;
    assert_eq!(handler.seen(), ["o-1", "o-1", "o-2"]);
    assert!(
        letters(&support::mongo_db(&mongo.uri_for("l")).await)
            .await
            .is_empty()
    );
    assert_eq!(status(&app.processors(), "default").dead_letter_mode, "log");
    app.shutdown().await;
}
