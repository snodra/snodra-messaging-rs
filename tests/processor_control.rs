//! Processor control (ADR 0023, P6b) through `KrakenMessaging`, against a real Kafka and MongoDB
//! (and the in-process fake hub): positioned markers, pause and resume (persisted, converging,
//! surviving a restart), the leased reset (prepare leaves the group, a second operation is
//! LOCKED, the lease expires by itself, an active member is GROUP_NOT_EMPTY, the replay of a
//! range with its reset handlers, `disallow_replay` and the sagas skipped), and the dead letters
//! through control requests.

mod support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::TryStreamExt;
use kraken_messaging::processors::{EventOptions, ReplayRange, ResetContext, ResetTarget};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;

use kraken_messaging::processors::RetryResult;
use kraken_messaging::proto::control_reply::Result as Answer;
use kraken_messaging::proto::control_request::Action;
use kraken_messaging::proto::{
    self, ControlRefusal, ControlReply, ProcessorState, ProcessorStatus, reset_target,
};
use kraken_messaging::{Config, Event, KrakenMessaging, MessagingError, message_type};
use mongodb::bson::{Bson, Document, doc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
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

fn status(app: &KrakenMessaging, name: &str) -> ProcessorStatus {
    app.processors()
        .into_iter()
        .find(|p| p.name == name)
        .unwrap_or_else(|| panic!("no processor {name}"))
}

fn state(app: &KrakenMessaging, name: &str) -> ProcessorState {
    ProcessorState::try_from(status(app, name).state).unwrap()
}

async fn control_doc(db: &mongodb::Database, processor: &str) -> Option<Document> {
    db.collection::<Document>("processor_control")
        .find_one(doc! { "_id": processor })
        .await
        .unwrap()
}

fn pause() -> Action {
    Action::Pause(proto::ProcessorPause {})
}

fn resume() -> Action {
    Action::Resume(proto::ProcessorResume {})
}

fn prepare(op: &str, lease_ms: i64) -> Action {
    Action::ResetPrepare(proto::ResetPrepare {
        operation_id: op.to_string(),
        lease_ms,
    })
}

fn execute(op: &str, to: reset_target::To) -> Action {
    Action::ResetExecute(proto::ResetExecute {
        operation_id: op.to_string(),
        target: Some(proto::ResetTarget { to: Some(to) }),
    })
}

fn finish(op: &str, aborted: bool) -> Action {
    Action::ResetFinish(proto::ResetFinish {
        operation_id: op.to_string(),
        aborted,
    })
}

fn earliest() -> reset_target::To {
    reset_target::To::Earliest(true)
}

/// The `ok` detail of `reply` (panics on any other answer).
fn ok(reply: Option<ControlReply>) -> String {
    match reply.expect("a reply").result {
        Some(Answer::Ok(o)) => o.detail,
        other => panic!("not ok: {other:?}"),
    }
}

/// The refusal of `reply` (panics on any other answer).
fn refused(reply: Option<ControlReply>) -> (ControlRefusal, String) {
    match reply.expect("a reply").result {
        Some(Answer::Refused(r)) => (ControlRefusal::try_from(r.code).unwrap(), r.message),
        other => panic!("not refused: {other:?}"),
    }
}

/// Creates `topic` with `partitions` partitions.
async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
}

/// The `_id`s of the markers in `processed_events`, sorted.
async fn marker_ids(db: &mongodb::Database) -> Vec<String> {
    let mut all: Vec<String> = db
        .collection::<Document>("processed_events")
        .find(doc! {})
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.get_str("_id").unwrap().to_string())
        .collect();
    all.sort();
    all
}

/// A marker written before P6b: no position.
async fn legacy_marker(db: &mongodb::Database, id: &str) {
    db.collection::<Document>("processed_events")
        .insert_one(
            doc! { "_id": id, "type": "c.Noted", "processedAt": mongodb::bson::DateTime::now() },
        )
        .await
        .unwrap();
}

async fn replay(db: &mongodb::Database, processor: &str) -> Vec<Document> {
    control_doc(db, processor)
        .await
        .map(|d| {
            d.get_array("replay")
                .unwrap()
                .iter()
                .map(|r| r.as_document().unwrap().clone())
                .collect()
        })
        .unwrap_or_default()
}

/// The reset of `reply` (panics on any other answer).
fn reset(reply: Option<ControlReply>) -> proto::ResetOutcome {
    match reply.expect("a reply").result {
        Some(Answer::Reset(r)) => r,
        other => panic!("not a reset: {other:?}"),
    }
}

fn ranges(outcome: &proto::ResetOutcome) -> Vec<(i32, i64, i64)> {
    outcome
        .ranges
        .iter()
        .map(|r| (r.partition, r.from, r.until))
        .collect()
}

const ASK: Duration = Duration::from_secs(30);

/// The client ids of `group`'s members.
fn members(bootstrap: &str, group: &str) -> Vec<String> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    c.fetch_group_list(Some(group), Duration::from_secs(10))
        .unwrap()
        .groups()
        .iter()
        .flat_map(|g| g.members().iter().map(|m| m.client_id().to_string()))
        .collect()
}

/// The committed offset of `group` on `partition` of `topic` (-1 none).
fn committed(bootstrap: &str, group: &str, topic: &str, partition: i32) -> i64 {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);
    let Ok(got) = c.committed_offsets(tpl, Duration::from_secs(10)) else {
        return -1;
    };
    match got.find_partition(topic, partition).map(|p| p.offset()) {
        Some(Offset::Offset(o)) => o,
        _ => -1,
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

/// A pause is written to the document and holds the records on every instance (it stays in the
/// group, PAUSED), a restarted instance starts paused, an instance converges on a pause another
/// one wrote within a poll, and a resume lets the held records flow.
#[tokio::test]
async fn a_pause_holds_the_records_everywhere_and_survives_a_restart_until_the_resume() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "pause.events";
    let uri = mongo.uri_for("pause");
    let handler = Handler::default();
    let start = || async {
        KrakenMessaging::builder(config(
            "svc-pause",
            &hub.address(),
            &kafka.bootstrap,
            &uri,
            topic,
        ))
        .await
        .unwrap()
        .event::<Noted, _, _>(handler.handle())
        .start()
        .await
        .unwrap()
    };
    let app = start().await;
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let ack = hub.registers().await[0].capabilities.clone();
    assert!(ack.contains(&"processor-control".to_string()), "{ack:?}");
    let db = app.database();
    let p = producer(&kafka.bootstrap);
    produce(&p, topic, None, "n-1").await;
    eventually("n-1 handled", WITHIN, || async {
        handler.count("n-1") == 1
    })
    .await;

    let reply = hub.control("default", pause(), ASK).await;
    assert_eq!(ok(reply), "paused");
    let doc = control_doc(&db, "default").await.unwrap();
    assert!(doc.get_bool("paused").unwrap(), "{doc}");
    assert!(doc.get_datetime("pausedAt").is_ok(), "{doc}");
    let by = hub.control_replies().last().unwrap().1.control_id.clone();
    assert_eq!(doc.get_str("pausedBy").unwrap(), by, "{doc}");
    assert_eq!(doc.get("operation"), Some(&Bson::Null), "{doc}");
    assert_eq!(doc.get_array("replay").unwrap().len(), 0, "{doc}");
    assert_eq!(state(&app, "default"), ProcessorState::ProcessorPaused);
    produce(&p, topic, None, "n-2").await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(handler.count("n-2"), 0, "held while paused");
    assert_eq!(
        members(&kafka.bootstrap, "svc-pause").len(),
        1,
        "still in the group"
    );
    app.shutdown().await;

    // A restart reads the document before it polls: it starts paused.
    let first = start().await;
    let second = start().await;
    eventually("both PAUSED", WITHIN, || async {
        state(&first, "default") == ProcessorState::ProcessorPaused
            && state(&second, "default") == ProcessorState::ProcessorPaused
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(handler.count("n-2"), 0, "held after the restart");

    // A resume through one instance reaches the other on its poll.
    first.processor_control().resume("default").await.unwrap();
    let doc = control_doc(&db, "default").await.unwrap();
    assert!(!doc.get_bool("paused").unwrap(), "{doc}");
    assert_eq!(doc.get("pausedAt"), Some(&Bson::Null), "{doc}");
    assert_eq!(doc.get("pausedBy"), Some(&Bson::Null), "{doc}");
    eventually(
        "the other instance resumes within a poll",
        Duration::from_secs(12),
        || async { state(&second, "default") != ProcessorState::ProcessorPaused },
    )
    .await;
    eventually("n-2 flows", WITHIN, || async { handler.count("n-2") == 1 }).await;

    // A local pause converges the same way.
    second.processor_control().pause("default").await.unwrap();
    assert_eq!(
        control_doc(&db, "default")
            .await
            .unwrap()
            .get_str("pausedBy")
            .unwrap(),
        "local"
    );
    eventually(
        "the other instance pauses within a poll",
        Duration::from_secs(12),
        || async { state(&first, "default") == ProcessorState::ProcessorPaused },
    )
    .await;
    produce(&p, topic, None, "n-3").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(handler.count("n-3"), 0);
    let replies = hub.control("default", resume(), ASK).await;
    assert_eq!(ok(replies), "resumed");
    eventually("n-3 flows", WITHIN, || async { handler.count("n-3") == 1 }).await;
    assert!(
        first.processor_control().pause("nope").await.is_err(),
        "a processor that does not run here"
    );
    first.shutdown().await;
    second.shutdown().await;
}

/// A prepare takes the lock and leaves the group (STOPPED with the operation id); a second
/// operation is LOCKED, and its finish is NOT_PREPARED and leaves the lock alone; an instance
/// started meanwhile stays out of the group; without a renewal the lease passes and the
/// instance releases the lock and consumes again by itself; a finish releases it at once.
#[tokio::test]
async fn a_prepare_leaves_the_group_until_the_finish_or_the_lease_expiry() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "lease.events";
    let uri = mongo.uri_for("lease");
    let handler = Handler::default();
    let start = || async {
        KrakenMessaging::builder(config(
            "svc-lease",
            &hub.address(),
            &kafka.bootstrap,
            &uri,
            topic,
        ))
        .await
        .unwrap()
        .event_in::<Noted, _, _>("audit", handler.handle())
        .start()
        .await
        .unwrap()
    };
    let group = "svc-lease.audit";
    let app = start().await;
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let db = app.database();
    let p = producer(&kafka.bootstrap);
    produce(&p, topic, None, "n-1").await;
    eventually("n-1 handled", WITHIN, || async {
        handler.count("n-1") == 1
    })
    .await;
    eventually("a member of the group", WITHIN, || async {
        members(&kafka.bootstrap, group).len() == 1
    })
    .await;

    let detail = ok(hub.control("audit", prepare("op-1", 4000), ASK).await);
    assert!(detail.contains(group), "{detail}");
    assert!(
        members(&kafka.bootstrap, group).is_empty(),
        "left the group"
    );
    let s = status(&app, "audit");
    assert_eq!(state(&app, "audit"), ProcessorState::ProcessorStopped);
    assert_eq!(s.operation_id, "op-1");
    let op = control_doc(&db, "audit").await.unwrap();
    let op = op.get_document("operation").unwrap().clone();
    assert_eq!(op.get_str("id").unwrap(), "op-1");
    assert_eq!(op.get_str("kind").unwrap(), "reset");
    let prepared_at = op.get_datetime("preparedAt").unwrap().timestamp_millis();
    let lease_until = op.get_datetime("leaseUntil").unwrap().timestamp_millis();
    assert!((3500..=4500).contains(&(lease_until - prepared_at)), "{op}");

    // A renewal moves the lease only.
    ok(hub.control("audit", prepare("op-1", 4000), ASK).await);
    let renewed = control_doc(&db, "audit").await.unwrap();
    let renewed = renewed.get_document("operation").unwrap();
    assert_eq!(
        renewed
            .get_datetime("preparedAt")
            .unwrap()
            .timestamp_millis(),
        prepared_at
    );
    assert!(
        renewed
            .get_datetime("leaseUntil")
            .unwrap()
            .timestamp_millis()
            >= lease_until
    );

    let (code, message) = refused(hub.control("audit", prepare("op-2", 4000), ASK).await);
    assert_eq!(code, ControlRefusal::Locked);
    assert!(message.contains("op-1"), "{message}");
    let (code, _) = refused(hub.control("audit", finish("op-2", true), ASK).await);
    assert_eq!(code, ControlRefusal::NotPrepared);
    assert_eq!(
        control_doc(&db, "audit")
            .await
            .unwrap()
            .get_document("operation")
            .unwrap()
            .get_str("id")
            .unwrap(),
        "op-1",
        "another operation's finish never releases the lock"
    );
    let (code, _) = refused(hub.control("nope", prepare("op-1", 4000), ASK).await);
    assert_eq!(code, ControlRefusal::UnknownProcessor);

    // Started under the lock: out of the group too.
    let late = start().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(state(&late, "audit"), ProcessorState::ProcessorStopped);
    assert_eq!(status(&late, "audit").operation_id, "op-1");
    assert!(members(&kafka.bootstrap, group).is_empty());
    produce(&p, topic, None, "n-2").await;

    // No renewal: the lease passes, the lock is cleared and both consume again.
    eventually(
        "the lease expired and the lock is cleared",
        Duration::from_secs(20),
        || async { control_doc(&db, "audit").await.unwrap().get("operation") == Some(&Bson::Null) },
    )
    .await;
    eventually("both consume again", Duration::from_secs(20), || async {
        state(&app, "audit") != ProcessorState::ProcessorStopped
            && state(&late, "audit") != ProcessorState::ProcessorStopped
            && members(&kafka.bootstrap, group).len() == 2
    })
    .await;
    assert_eq!(status(&app, "audit").operation_id, "");
    eventually("n-2 handled", WITHIN, || async {
        handler.count("n-2") == 1
    })
    .await;

    // A finish releases at once; a repeated finish (nothing held) is ok.
    ok(hub.control("audit", prepare("op-3", 30_000), ASK).await);
    assert!(members(&kafka.bootstrap, group).is_empty());
    ok(hub
        .control_to(Some(app.instance_id()), "audit", finish("op-3", false), ASK)
        .await);
    ok(hub
        .control_to(
            Some(late.instance_id()),
            "audit",
            finish("op-3", false),
            ASK,
        )
        .await);
    assert_eq!(
        control_doc(&db, "audit").await.unwrap().get("operation"),
        Some(&Bson::Null)
    );
    ok(hub.control("audit", finish("op-3", false), ASK).await);
    eventually("both back in the group", WITHIN, || async {
        members(&kafka.bootstrap, group).len() == 2
    })
    .await;
    app.shutdown().await;
    late.shutdown().await;
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct Seen {
    note: Option<String>,
}

/// A reset to `earliest` with two instances: both leave the group, one executes (its reset
/// handler once, the markers of the range and the legacy ones of the processor deleted, the
/// ranges written before the offsets), and after the finish every record of the range is handled
/// again exactly once overall, without the handler that disallows replay and without the sagas;
/// another processor's markers and the other marker families are untouched; the ranges are
/// removed once passed.
#[tokio::test]
async fn a_reset_to_earliest_replays_the_range_once_without_sagas_or_disallowed_handlers() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "replay.events";
    create_topic(&kafka.bootstrap, topic, 2).await;
    let uri = mongo.uri_for("replay");
    let (projection, mailer, audit) = (Handler::default(), Handler::default(), Handler::default());
    let resets: Arc<Mutex<Vec<ResetContext>>> = Arc::default();
    let saga_runs = Arc::new(AtomicUsize::new(0));
    let start = || async {
        let (resets, runs) = (Arc::clone(&resets), Arc::clone(&saga_runs));
        KrakenMessaging::builder(config(
            "svc-replay",
            &hub.address(),
            &kafka.bootstrap,
            &uri,
            topic,
        ))
        .await
        .unwrap()
        .event::<Noted, _, _>(projection.handle())
        .event_with::<Noted, _, _>(EventOptions::new().disallow_replay(), mailer.handle())
        .event_in::<Noted, _, _>("audit", audit.handle())
        .saga::<Seen>("c.Seen", move |d| {
            d.start_on::<Noted>(
                "note",
                |e| Some(e.note.clone()),
                move |s, e, _| {
                    runs.fetch_add(1, Ordering::SeqCst);
                    s.note = Some(e.note.clone());
                    Ok(())
                },
            )
        })
        .reset_handler("default", move |ctx| {
            let resets = Arc::clone(&resets);
            async move {
                resets.lock().unwrap().push(ctx);
                Ok(())
            }
        })
        .start()
        .await
        .unwrap()
    };
    let (first, second) = (start().await, start().await);
    assert!(first.await_hub(Duration::from_secs(30)).await);
    assert!(second.await_hub(Duration::from_secs(30)).await);
    let db = first.database();
    let p = producer(&kafka.bootstrap);
    for (partition, note) in [(0, "n-1"), (0, "n-2"), (1, "n-3"), (1, "n-4")] {
        produce(&p, topic, Some(partition), note).await;
    }
    let notes = ["n-1", "n-2", "n-3", "n-4"];
    eventually("everything handled once", WITHIN, || async {
        notes
            .iter()
            .all(|n| projection.count(n) == 1 && mailer.count(n) == 1 && audit.count(n) == 1)
            && saga_runs.load(Ordering::SeqCst) == 4
            && (0..2).all(|p| committed(&kafka.bootstrap, "svc-replay", topic, p) == 2)
    })
    .await;
    // Were the sagas run again, their handler would count (their markers are gone).
    db.collection::<Document>("processed_events")
        .delete_many(doc! { "_id": { "$regex": "^saga:" } })
        .await
        .unwrap();
    for id in [
        "legacy-1",
        "processor:audit:legacy-2",
        "saga:c.Other:legacy-3",
        "ingress:hook:k",
        "partner:0:7",
    ] {
        legacy_marker(&db, id).await;
    }
    let audit_markers: Vec<String> = marker_ids(&db)
        .await
        .into_iter()
        .filter(|m| m.starts_with("processor:audit:"))
        .collect();
    assert_eq!(audit_markers.len(), 5, "{audit_markers:?}");

    for app in [&first, &second] {
        ok(hub
            .control_to(
                Some(app.instance_id()),
                "default",
                prepare("op-r", 30_000),
                ASK,
            )
            .await);
    }
    assert!(members(&kafka.bootstrap, "svc-replay").is_empty());
    let outcome = reset(
        hub.control_to(
            Some(first.instance_id()),
            "default",
            execute("op-r", earliest()),
            ASK,
        )
        .await,
    );
    assert_eq!(ranges(&outcome), [(0, 0, 2), (1, 0, 2)]);
    assert_eq!(outcome.markers_deleted, 5, "four positioned, one legacy");
    assert_eq!(outcome.reset_handlers, 1);
    let ctx = resets.lock().unwrap()[0].clone();
    assert_eq!(ctx.processor, "default");
    assert_eq!(ctx.operation_id, "op-r");
    assert_eq!(ctx.target, ResetTarget::Earliest);
    assert_eq!(
        ctx.ranges,
        [0, 1].map(|partition| ReplayRange {
            topic: topic.into(),
            partition,
            from: 0,
            until: 2
        })
    );
    let written = replay(&db, "default").await;
    assert_eq!(written.len(), 2, "{written:?}");
    assert_eq!(
        written[1],
        doc! { "topic": topic, "partition": 1, "from": 0i64, "until": 2i64 }
    );
    let left = marker_ids(&db).await;
    for gone in ["n-1", "n-2", "n-3", "n-4", "legacy-1"] {
        assert!(!left.contains(&gone.to_string()), "{left:?}");
    }
    for kept in ["saga:c.Other:legacy-3", "ingress:hook:k", "partner:0:7"] {
        assert!(left.contains(&kept.to_string()), "{left:?}");
    }
    assert_eq!(
        left.iter()
            .filter(|m| m.starts_with("processor:audit:"))
            .count(),
        5,
        "another processor's markers are untouched"
    );
    for p in 0..2 {
        assert_eq!(committed(&kafka.bootstrap, "svc-replay", topic, p), 0);
    }

    for app in [&first, &second] {
        ok(hub
            .control_to(
                Some(app.instance_id()),
                "default",
                finish("op-r", false),
                ASK,
            )
            .await);
    }
    eventually(
        "the range replayed and the ranges removed",
        WITHIN,
        || async {
            notes.iter().all(|n| projection.count(n) == 2)
                && replay(&db, "default").await.is_empty()
        },
    )
    .await;
    eventually("not replaying any more", WITHIN, || async {
        !status(&first, "default").replaying && !status(&second, "default").replaying
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    for n in notes {
        assert_eq!(projection.count(n), 2, "{n}: exactly once more overall");
        assert_eq!(mailer.count(n), 1, "{n}: disallows replay");
        assert_eq!(audit.count(n), 1, "{n}: another processor");
        let m = marker(&db, n).await.expect("a new marker");
        assert_eq!(m.get_str("processor").unwrap(), "default");
    }
    assert_eq!(
        saga_runs.load(Ordering::SeqCst),
        4,
        "the sagas never see a replay"
    );
    assert_eq!(
        resets.lock().unwrap().len(),
        1,
        "on the executing instance only"
    );
    first.shutdown().await;
    second.shutdown().await;
}

/// The targets on one instance: `latest` replays nothing; a time and explicit offsets replay
/// from there (legacy markers kept); an active member of the group is GROUP_NOT_EMPTY and a
/// failing reset handler RESET_HANDLER_FAILED (saying it ran), both without a marker or an offset
/// touched; a bad target is
/// INVALID and an execute without a prepare NOT_PREPARED.
#[tokio::test]
async fn reset_targets_and_refusals() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "targets.events";
    let handler = Handler::default();
    let fail_reset = Arc::new(AtomicBool::new(false));
    let resets = Arc::new(AtomicUsize::new(0));
    let (fail, count) = (Arc::clone(&fail_reset), Arc::clone(&resets));
    let app = KrakenMessaging::builder(config(
        "svc-targets",
        &hub.address(),
        &kafka.bootstrap,
        &mongo.uri_for("targets"),
        topic,
    ))
    .await
    .unwrap()
    .event_in::<Noted, _, _>("audit", handler.handle())
    .reset_handler("audit", move |_| {
        let (fail, count) = (Arc::clone(&fail), Arc::clone(&count));
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            if fail.load(Ordering::SeqCst) {
                return Err(MessagingError::Storage("projection store down".into()));
            }
            Ok(())
        }
    })
    .start()
    .await
    .unwrap();
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let db = app.database();
    let group = "svc-targets.audit";
    let p = producer(&kafka.bootstrap);
    for note in ["n-1", "n-2"] {
        produce(&p, topic, None, note).await;
    }
    eventually("n-1 and n-2 handled", WITHIN, || async {
        handler.count("n-2") == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let later = chrono::Utc::now().timestamp_millis();
    tokio::time::sleep(Duration::from_millis(200)).await;
    for note in ["n-3", "n-4"] {
        produce(&p, topic, None, note).await;
    }
    eventually("all handled and committed", WITHIN, || async {
        handler.count("n-4") == 1 && committed(&kafka.bootstrap, group, topic, 0) == 4
    })
    .await;
    legacy_marker(&db, "processor:audit:legacy-9").await;
    let counts = |notes: [usize; 4]| {
        ["n-1", "n-2", "n-3", "n-4"]
            .iter()
            .map(|n| handler.count(n))
            .collect::<Vec<_>>()
            == notes
    };

    let (code, _) = refused(hub.control("audit", execute("op-0", earliest()), ASK).await);
    assert_eq!(code, ControlRefusal::NotPrepared);

    // An active member of the group (an instance that missed the prepare): nothing changes.
    let bootstrap = kafka.bootstrap.clone();
    let (topic_s, group_s) = (topic.to_string(), group.to_string());
    let old = tokio::task::spawn_blocking(move || {
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", &group_s)
            .set("client.id", "old-instance")
            .set("enable.auto.commit", "false")
            .create()
            .unwrap();
        c.subscribe(&[topic_s.as_str()]).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !members(&bootstrap, &group_s).contains(&"old-instance".to_string()) {
            assert!(
                std::time::Instant::now() < deadline,
                "the old instance never joined"
            );
            let _ = c.poll(Duration::from_millis(100));
        }
        c
    })
    .await
    .unwrap();
    ok(hub.control("audit", prepare("op-a", 30_000), ASK).await);
    let (code, message) = refused(hub.control("audit", execute("op-a", earliest()), ASK).await);
    assert_eq!(code, ControlRefusal::GroupNotEmpty, "{message}");
    assert!(message.contains("old-instance"), "{message}");
    assert_eq!(committed(&kafka.bootstrap, group, topic, 0), 4);
    assert!(marker(&db, "processor:audit:n-1").await.is_some());
    assert!(marker(&db, "processor:audit:legacy-9").await.is_some());
    assert!(replay(&db, "audit").await.is_empty());
    assert_eq!(resets.load(Ordering::SeqCst), 0);
    tokio::task::spawn_blocking(move || drop(old))
        .await
        .unwrap();
    ok(hub.control("audit", finish("op-a", true), ASK).await);

    // A failing reset handler: it ran (the refusal says so), no marker or offset is touched.
    fail_reset.store(true, Ordering::SeqCst);
    ok(hub.control("audit", prepare("op-b", 30_000), ASK).await);
    let (code, message) = refused(hub.control("audit", execute("op-b", earliest()), ASK).await);
    assert_eq!(code, ControlRefusal::ResetHandlerFailed, "{message}");
    assert!(message.contains("projection store down"), "{message}");
    assert!(
        message.contains("reset handlers ran (1), no markers deleted, offsets not moved"),
        "{message}"
    );
    assert_eq!(committed(&kafka.bootstrap, group, topic, 0), 4);
    assert!(marker(&db, "processor:audit:n-1").await.is_some());
    assert!(replay(&db, "audit").await.is_empty());
    ok(hub.control("audit", finish("op-b", true), ASK).await);
    fail_reset.store(false, Ordering::SeqCst);
    resets.store(0, Ordering::SeqCst);

    // Bad targets.
    ok(hub.control("audit", prepare("op-c", 30_000), ASK).await);
    let offsets = |all: &[(i32, i64)]| {
        reset_target::To::Offsets(proto::PartitionOffsets {
            offsets: all
                .iter()
                .map(|&(partition, offset)| proto::PartitionOffset { partition, offset })
                .collect(),
        })
    };
    for bad in [&[(5, 0)][..], &[(0, -1)], &[(0, 99)]] {
        let (code, message) = refused(
            hub.control("audit", execute("op-c", offsets(bad)), ASK)
                .await,
        );
        assert_eq!(code, ControlRefusal::Invalid, "{bad:?}: {message}");
    }

    // Latest: nothing is replayed, no reset handler runs.
    let outcome = reset(
        hub.control(
            "audit",
            execute("op-c", reset_target::To::Latest(true)),
            ASK,
        )
        .await,
    );
    assert_eq!(
        (
            outcome.ranges.len(),
            outcome.markers_deleted,
            outcome.reset_handlers
        ),
        (0, 0, 0)
    );
    ok(hub.control("audit", finish("op-c", false), ASK).await);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(counts([1, 1, 1, 1]));
    assert_eq!(resets.load(Ordering::SeqCst), 0);

    // A time: the records from then on; the legacy marker stays (only earliest deletes them).
    ok(hub.control("audit", prepare("op-d", 30_000), ASK).await);
    let outcome = reset(
        hub.control(
            "audit",
            execute("op-d", reset_target::To::TimestampMs(later)),
            ASK,
        )
        .await,
    );
    assert_eq!(ranges(&outcome), [(0, 2, 4)]);
    assert_eq!((outcome.markers_deleted, outcome.reset_handlers), (2, 1));
    assert!(marker(&db, "processor:audit:legacy-9").await.is_some());
    ok(hub.control("audit", finish("op-d", false), ASK).await);
    eventually("n-3 and n-4 again", WITHIN, || async {
        counts([1, 1, 2, 2]) && replay(&db, "audit").await.is_empty()
    })
    .await;

    // Explicit offsets.
    ok(hub.control("audit", prepare("op-e", 30_000), ASK).await);
    let outcome = reset(
        hub.control("audit", execute("op-e", offsets(&[(0, 3)])), ASK)
            .await,
    );
    assert_eq!(ranges(&outcome), [(0, 3, 4)]);
    assert_eq!(outcome.markers_deleted, 1);
    ok(hub.control("audit", finish("op-e", false), ASK).await);
    eventually("n-4 again", WITHIN, || async {
        counts([1, 1, 2, 3]) && replay(&db, "audit").await.is_empty()
    })
    .await;
    app.shutdown().await;
}

/// A `c.Noted` for `note` with key `key`.
async fn produce_keyed(p: &FutureProducer, topic: &str, key: &str, note: &str) {
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
        .key(key)
        .payload(value.as_slice())
        .headers(headers);
    p.send(record, Duration::from_secs(10)).await.unwrap();
}

fn retried(reply: Option<ControlReply>) -> (String, String) {
    match reply.expect("a reply").result {
        Some(Answer::Retry(r)) => (r.result, r.detail),
        other => panic!("not a retry: {other:?}"),
    }
}

/// The dead letters through control requests: a page of summaries (no payload) with the total,
/// a retry's result strings (Blocked naming the head, Failed with the error, Retried, NotFound
/// for a letter that is not the processor's), retry all, discard (NOT_FOUND once gone), and the
/// refusals of a bad limit, an empty letter id and a processor that does not run here.
#[tokio::test]
async fn the_dead_letters_through_control_requests() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "dlq.events";
    let (audit, default) = (Handler::default(), Handler::default());
    for note in ["a-1", "b-1", "c-1"] {
        audit.fail(note);
    }
    default.fail("d-1");
    let app = KrakenMessaging::builder(config(
        "svc-dlq",
        &hub.address(),
        &kafka.bootstrap,
        &mongo.uri_for("dlq"),
        topic,
    ))
    .await
    .unwrap()
    .event::<Noted, _, _>(default.handle())
    .event_in::<Noted, _, _>("audit", audit.handle())
    .start()
    .await
    .unwrap();
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let p = producer(&kafka.bootstrap);
    for (key, note) in [
        ("k1", "a-1"),
        ("k1", "a-2"),
        ("k2", "b-1"),
        ("k3", "c-1"),
        ("k4", "d-1"),
    ] {
        produce_keyed(&p, topic, key, note).await;
    }
    eventually("four audit letters and one of default", WITHIN, || async {
        app.dead_letters().count(Some("audit")).await.unwrap() == 4
            && app.dead_letters().count(Some("default")).await.unwrap() == 1
    })
    .await;
    let list = |limit| Action::DeadLetterList(proto::DeadLetterList { limit });
    let page = match hub.control("audit", list(2), ASK).await.unwrap().result {
        Some(Answer::DeadLetters(page)) => page,
        other => panic!("{other:?}"),
    };
    assert_eq!(page.total, 4);
    assert_eq!(page.letters.len(), 2);
    let head = &page.letters[0];
    assert_eq!(head.id, format!("audit:{topic}:0:0"));
    assert_eq!(
        (
            head.processor.as_str(),
            head.topic.as_str(),
            head.partition,
            head.offset
        ),
        ("audit", topic, 0, 0)
    );
    assert_eq!(
        (
            head.key.as_str(),
            head.event_id.as_str(),
            head.event_type.as_str()
        ),
        ("k1", "a-1", "c.Noted")
    );
    assert_eq!(head.reason, "HANDLER_FAILED");
    assert_eq!(head.error_type, "Storage");
    assert!(head.error_message.contains("cannot note a-1"), "{head:?}");
    assert_eq!(head.attempts, 1);
    assert!(head.parked_at_ms > 0 && head.last_attempt_at_ms >= head.parked_at_ms);
    let sequenced = &page.letters[1];
    assert_eq!(sequenced.reason, "SEQUENCED");
    assert_eq!(sequenced.blocked_by, head.id);
    for bad in [0, 101] {
        let (code, _) = refused(hub.control("audit", list(bad), ASK).await);
        assert_eq!(code, ControlRefusal::Invalid, "limit {bad}");
    }

    let retry = |id: &str| {
        Action::DeadLetterRetry(proto::DeadLetterRetry {
            letter_id: id.to_string(),
        })
    };
    let discard = |id: &str| {
        Action::DeadLetterDiscard(proto::DeadLetterDiscard {
            letter_id: id.to_string(),
        })
    };
    let (result, detail) = retried(hub.control("audit", retry(&sequenced.id), ASK).await);
    assert_eq!(
        (result.as_str(), detail.as_str()),
        ("Blocked", head.id.as_str())
    );
    let (result, detail) = retried(hub.control("audit", retry(&head.id), ASK).await);
    assert_eq!(result, "Failed");
    assert!(detail.contains("cannot note a-1"), "{detail}");
    let (result, _) = retried(hub.control("audit", retry("audit:nope:0:9"), ASK).await);
    assert_eq!(result, "NotFound");
    let default_letter = format!("default:{topic}:0:4");
    let (result, _) = retried(hub.control("audit", retry(&default_letter), ASK).await);
    assert_eq!(result, "NotFound", "a letter of another processor");
    let (code, _) = refused(hub.control("audit", retry(""), ASK).await);
    assert_eq!(code, ControlRefusal::Invalid);

    audit.heal();
    let (result, detail) = retried(hub.control("audit", retry(&head.id), ASK).await);
    assert_eq!((result.as_str(), detail.as_str()), ("Retried", ""));
    let c1 = format!("audit:{topic}:0:3");
    ok(hub.control("audit", discard(&c1), ASK).await);
    let (code, _) = refused(hub.control("audit", discard(&c1), ASK).await);
    assert_eq!(code, ControlRefusal::NotFound);
    let (code, _) = refused(hub.control("audit", discard(&default_letter), ASK).await);
    assert_eq!(
        code,
        ControlRefusal::NotFound,
        "a letter of another processor"
    );
    let all = match hub
        .control(
            "audit",
            Action::DeadLetterRetryAll(proto::DeadLetterRetryAll {}),
            ASK,
        )
        .await
        .unwrap()
        .result
    {
        Some(Answer::RetryAll(r)) => r,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        (all.retried, all.failed, all.blocked, all.skipped),
        (2, 0, 0, 0)
    );
    assert_eq!(app.dead_letters().count(Some("audit")).await.unwrap(), 0);
    assert_eq!(app.dead_letters().count(Some("default")).await.unwrap(), 1);
    let (code, _) = refused(hub.control("nope", list(10), ASK).await);
    assert_eq!(code, ControlRefusal::UnknownProcessor);
    app.shutdown().await;
}

/// Control requests never hold the hub stream: while a reset handler blocks the execute, the
/// heartbeats and a command request flow and a renewal of the held operation is answered at once
/// (never queued behind the execute); a hub that does not list `processor-control` gets no
/// answer at all.
#[tokio::test]
async fn control_requests_never_hold_the_hub_stream_and_renewals_never_wait() {
    require_docker!();
    let hub = Arc::new(FakeHub::start().await);
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "stream.events";
    let handler = Handler::default();
    let release = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    let (wait, enter) = (Arc::clone(&release), Arc::clone(&entered));
    let mut c = config(
        "svc-stream",
        &hub.address(),
        &kafka.bootstrap,
        &mongo.uri_for("stream"),
        topic,
    );
    c.heartbeat_interval = Duration::from_millis(200);
    let app = KrakenMessaging::builder(c)
        .await
        .unwrap()
        .command::<Ping, _, _>(|_, _| async { Ok(Pong { pong: true }) })
        .event::<Noted, _, _>(handler.handle())
        .reset_handler("default", move |_| {
            let (wait, enter) = (Arc::clone(&wait), Arc::clone(&enter));
            async move {
                enter.notify_one();
                wait.notified().await;
                Ok(())
            }
        })
        .start()
        .await
        .unwrap();
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let p = producer(&kafka.bootstrap);
    produce(&p, topic, None, "n-1").await;
    eventually("n-1 handled and committed", WITHIN, || async {
        handler.count("n-1") == 1 && committed(&kafka.bootstrap, "svc-stream", topic, 0) == 1
    })
    .await;

    ok(hub.control("default", prepare("op-s", 30_000), ASK).await);
    let executing = {
        let hub = Arc::clone(&hub);
        tokio::spawn(async move {
            hub.control(
                "default",
                execute("op-s", earliest()),
                Duration::from_secs(60),
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(30), entered.notified())
        .await
        .expect("the reset handler runs");
    let beats = hub.heartbeats();
    let started = std::time::Instant::now();
    assert_eq!(
        ok(hub
            .control("default", prepare("op-s", 30_000), Duration::from_secs(5))
            .await),
        "lease renewed"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "answered at once"
    );
    hub.send_to_clients(proto::ServerMessage {
        message: Some(proto::server_message::Message::Request(proto::Request {
            id: "ping-1".into(),
            kind: proto::RequestKind::Command as i32,
            r#type: "c.Ping".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        })),
    })
    .await;
    eventually(
        "the command is answered meanwhile",
        Duration::from_secs(10),
        || async { hub.replies().await.iter().any(|r| r.request_id == "ping-1") },
    )
    .await;
    eventually("heartbeats go on", Duration::from_secs(10), || async {
        hub.heartbeats() >= beats + 3
    })
    .await;
    assert!(!executing.is_finished(), "the execute is still held");
    release.notify_one();
    let outcome = reset(executing.await.unwrap());
    assert_eq!(outcome.reset_handlers, 1);
    ok(hub.control("default", finish("op-s", false), ASK).await);
    eventually("n-1 replayed", WITHIN, || async {
        handler.count("n-1") == 2
    })
    .await;
    app.shutdown().await;

    // A hub that does not list the capability is never answered.
    let old = FakeHub::start().await;
    old.set_processor_control(false);
    let app = KrakenMessaging::builder(config(
        "svc-stream",
        &old.address(),
        &kafka.bootstrap,
        &mongo.uri_for("stream"),
        topic,
    ))
    .await
    .unwrap()
    .event::<Noted, _, _>(handler.handle())
    .start()
    .await
    .unwrap();
    assert!(app.await_hub(Duration::from_secs(30)).await);
    assert!(
        old.control("default", pause(), Duration::from_secs(3))
            .await
            .is_none()
    );
    assert!(old.control_replies().is_empty());
    assert_ne!(state(&app, "default"), ProcessorState::ProcessorPaused);
    app.shutdown().await;
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct Ping {}
message_type!(Ping, "c.Ping");
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct Pong {
    pong: bool,
}
impl kraken_messaging::Command for Ping {
    type Reply = Pong;
}

/// One `audit` processor of service `service` on `topic`, with `handler`, and `once` (when given)
/// registered with `disallow_replay`.
async fn audit_app(
    hub: &FakeHub,
    kafka: &str,
    uri: &str,
    service: &str,
    topic: &str,
    handler: &Handler,
    once: Option<&Handler>,
) -> KrakenMessaging {
    let mut builder = KrakenMessaging::builder(config(service, &hub.address(), kafka, uri, topic))
        .await
        .unwrap()
        .event_in::<Noted, _, _>("audit", handler.handle());
    if let Some(once) = once {
        builder = builder.event_with::<Noted, _, _>(
            EventOptions::new().processor("audit").disallow_replay(),
            once.handle(),
        );
    }
    let app = builder.start().await.unwrap();
    assert!(app.await_hub(Duration::from_secs(30)).await);
    app
}

/// A renewal that reaches the instance after the operation's finish is a prepare of an operation
/// that ended here, and one whose lock was cleared meanwhile renews nothing: both are refused
/// NOT_PREPARED and never take the lock (which would stop every instance for a whole lease).
#[tokio::test]
async fn a_late_renewal_never_takes_the_lock_again() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "late.events";
    let handler = Handler::default();
    let uri = mongo.uri_for("late");
    let app = audit_app(
        &hub,
        &kafka.bootstrap,
        &uri,
        "svc-late",
        topic,
        &handler,
        None,
    )
    .await;
    let db = app.database();
    let group = "svc-late.audit";
    produce(&producer(&kafka.bootstrap), topic, None, "n-0").await;
    eventually("n-0 handled", WITHIN, || async {
        handler.count("n-0") == 1
    })
    .await;
    eventually("a member of the group", WITHIN, || async {
        members(&kafka.bootstrap, group).len() == 1
    })
    .await;
    let lock = || async {
        control_doc(&db, "audit")
            .await
            .unwrap()
            .get("operation")
            .cloned()
    };

    ok(hub.control("audit", prepare("op-1", 30_000), ASK).await);
    ok(hub.control("audit", finish("op-1", false), ASK).await);
    let (code, message) = refused(hub.control("audit", prepare("op-1", 30_000), ASK).await);
    assert_eq!(code, ControlRefusal::NotPrepared, "{message}");
    assert!(message.contains("op-1 already finished"), "{message}");
    assert_eq!(lock().await, Some(Bson::Null));
    eventually("consuming after the finish", WITHIN, || async {
        members(&kafka.bootstrap, group).len() == 1
    })
    .await;

    // The lock is cleared under the held operation (another instance cleared its passed lease).
    ok(hub.control("audit", prepare("op-2", 30_000), ASK).await);
    db.collection::<Document>("processor_control")
        .update_one(
            doc! { "_id": "audit" },
            doc! { "$set": { "operation": null } },
        )
        .await
        .unwrap();
    let (code, message) = refused(hub.control("audit", prepare("op-2", 30_000), ASK).await);
    assert_eq!(code, ControlRefusal::NotPrepared, "{message}");
    assert!(message.contains("lock lost"), "{message}");
    assert_eq!(lock().await, Some(Bson::Null));
    let (code, _) = refused(hub.control("audit", prepare("op-2", 30_000), ASK).await);
    assert_eq!(
        code,
        ControlRefusal::NotPrepared,
        "and so is every later one"
    );
    eventually("consuming again", WITHIN, || async {
        members(&kafka.bootstrap, group).len() == 1
    })
    .await;

    // A first prepare still takes the lock.
    ok(hub.control("audit", prepare("op-3", 30_000), ASK).await);
    let held = lock().await.unwrap();
    assert_eq!(held.as_document().unwrap().get_str("id").unwrap(), "op-3");
    ok(hub.control("audit", finish("op-3", false), ASK).await);
    app.shutdown().await;
}

/// A renewal written between the poll's read of a passed lease and its conditional clear keeps
/// the lock: the instance stays prepared and out of the group.
#[tokio::test]
async fn a_lease_renewed_before_its_clear_keeps_the_instance_prepared() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "renewed.events";
    let handler = Handler::default();
    let uri = mongo.uri_for("renewed");
    let app = audit_app(
        &hub,
        &kafka.bootstrap,
        &uri,
        "svc-renewed",
        topic,
        &handler,
        None,
    )
    .await;
    let db = app.database();
    let group = "svc-renewed.audit";
    produce(&producer(&kafka.bootstrap), topic, None, "n-0").await;
    eventually("n-0 handled", WITHIN, || async {
        handler.count("n-0") == 1
    })
    .await;
    eventually("a member of the group", WITHIN, || async {
        members(&kafka.bootstrap, group).len() == 1
    })
    .await;
    ok(hub.control("audit", prepare("op-1", 1500), ASK).await);
    let (renewed_tx, renewed) = tokio::sync::oneshot::channel();
    let docs = db.collection::<Document>("processor_control");
    app.processor_control().pause_before_next_expiry(
        "audit",
        Box::pin(async move {
            let until = mongodb::bson::DateTime::from_millis(
                mongodb::bson::DateTime::now().timestamp_millis() + 60_000,
            );
            docs.update_one(
                doc! { "_id": "audit" },
                doc! { "$set": { "operation.leaseUntil": until } },
            )
            .await
            .unwrap();
            let _ = renewed_tx.send(());
        }),
    );
    tokio::time::timeout(Duration::from_secs(20), renewed)
        .await
        .expect("the poll found the lease passed")
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(state(&app, "audit"), ProcessorState::ProcessorStopped);
    assert_eq!(status(&app, "audit").operation_id, "op-1");
    assert!(members(&kafka.bootstrap, group).is_empty());
    assert_eq!(
        ok(hub.control("audit", prepare("op-1", 30_000), ASK).await),
        "lease renewed",
        "still held here"
    );
    ok(hub.control("audit", finish("op-1", false), ASK).await);
    app.shutdown().await;
}

/// The lease is checked again after the reset handlers: lost while they ran (another instance
/// cleared it and took the lock), the execute stops before any marker or offset is touched, and
/// its refusal says the reset handlers ran.
#[tokio::test]
async fn a_lease_lost_while_the_reset_handlers_run_stops_the_reset_before_the_markers() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "lost.events";
    let handler = Handler::default();
    let uri = mongo.uri_for("lost");
    let thief = mongodb::Client::with_uri_str(&uri)
        .await
        .unwrap()
        .database("lost")
        .collection::<Document>("processor_control");
    let app = KrakenMessaging::builder(config(
        "svc-lost",
        &hub.address(),
        &kafka.bootstrap,
        &uri,
        topic,
    ))
    .await
    .unwrap()
    .event_in::<Noted, _, _>("audit", handler.handle())
    .reset_handler("audit", move |_| {
        let thief = thief.clone();
        async move {
            let now = mongodb::bson::DateTime::now();
            let until = mongodb::bson::DateTime::from_millis(now.timestamp_millis() + 60_000);
            thief
                .update_one(
                    doc! { "_id": "audit" },
                    doc! { "$set": { "operation": { "id": "op-other", "kind": "reset", "leaseUntil": until, "preparedAt": now } } },
                )
                .await
                .unwrap();
            Ok(())
        }
    })
    .start()
    .await
    .unwrap();
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let db = app.database();
    let group = "svc-lost.audit";
    let p = producer(&kafka.bootstrap);
    for note in ["n-1", "n-2"] {
        produce(&p, topic, None, note).await;
    }
    eventually("handled and committed", WITHIN, || async {
        handler.count("n-2") == 1 && committed(&kafka.bootstrap, group, topic, 0) == 2
    })
    .await;
    ok(hub.control("audit", prepare("op-1", 30_000), ASK).await);
    let (code, message) = refused(hub.control("audit", execute("op-1", earliest()), ASK).await);
    assert_eq!(code, ControlRefusal::Failed, "{message}");
    for said in [
        "lease lost",
        "reset handlers ran (1)",
        "no markers deleted",
        "offsets not moved",
    ] {
        assert!(message.contains(said), "{said}: {message}");
    }
    assert_eq!(committed(&kafka.bootstrap, group, topic, 0), 2);
    assert!(marker(&db, "processor:audit:n-1").await.is_some());
    assert!(replay(&db, "audit").await.is_empty());
    app.shutdown().await;
}

/// A record parked while its processor replays it keeps that in its letter (`replay: true`), so
/// the letter's retry runs stage 1 as a replay: without the handlers that disallow replay.
#[tokio::test]
async fn a_letter_parked_during_a_replay_is_retried_as_a_replay() {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "replayed.events";
    let (handler, once) = (Handler::default(), Handler::default());
    let uri = mongo.uri_for("replayed");
    let app = audit_app(
        &hub,
        &kafka.bootstrap,
        &uri,
        "svc-replayed",
        topic,
        &handler,
        Some(&once),
    )
    .await;
    let db = app.database();
    let group = "svc-replayed.audit";
    let p = producer(&kafka.bootstrap);
    produce(&p, topic, None, "n-1").await;
    eventually("handled and committed", WITHIN, || async {
        once.count("n-1") == 1 && committed(&kafka.bootstrap, group, topic, 0) == 1
    })
    .await;
    handler.fail("n-1");
    ok(hub.control("audit", prepare("op-1", 30_000), ASK).await);
    assert_eq!(
        ranges(&reset(
            hub.control("audit", execute("op-1", earliest()), ASK).await
        )),
        [(0, 0, 1)]
    );
    ok(hub.control("audit", finish("op-1", false), ASK).await);
    let id = format!("audit:{topic}:0:0");
    eventually("parked during the replay", WITHIN, || async {
        app.dead_letters().count(Some("audit")).await.unwrap() == 1
    })
    .await;
    let letter = db
        .collection::<Document>("dead_letters")
        .find_one(doc! { "_id": &id })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(letter.get_bool("replay"), Ok(true), "{letter}");
    handler.heal();
    let retry = Action::DeadLetterRetry(proto::DeadLetterRetry {
        letter_id: id.clone(),
    });
    let (result, detail) = retried(hub.control("audit", retry, ASK).await);
    assert_eq!(result, "Retried", "{detail}");
    assert_eq!(
        handler.count("n-1"),
        3,
        "handled, failed in the replay, retried"
    );
    assert_eq!(
        once.count("n-1"),
        1,
        "the retry of a replayed record is a replay"
    );
    app.shutdown().await;
}
