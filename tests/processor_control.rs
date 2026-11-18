//! Processor control (ADR 0023, P6b) through `SnodraMessaging`, against a real Kafka and MongoDB
//! (and the in-process fake hub): positioned markers, pause and resume (persisted, converging,
//! surviving a restart), the leased reset (prepare leaves the group, a second operation is
//! LOCKED, the lease expires by itself, an active member is SUBSCRIBERS_ACTIVE, the replay of a
//! range with its reset handlers, `disallow_replay` and the sagas skipped), and the dead letters
//! through control requests.

mod support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::TryStreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use snodra_messaging::event_store::{ReplayRange, ResetTarget};
use snodra_messaging::processors::{EventOptions, ResetContext};

use mongodb::bson::{Bson, Document, doc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use snodra_messaging::processors::RetryResult;
use snodra_messaging::proto::control_reply::Result as Answer;
use snodra_messaging::proto::control_request::Action;
use snodra_messaging::proto::{
    self, ControlRefusal, ControlReply, ProcessorState, ProcessorStatus, reset_target,
};
use snodra_messaging::{
    Config, Event, MessagingError, SnodraMessaging, SnodraMessagingBuilder, message_type,
};
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

/// The service's builder with the MongoDB message store of `mongo`.
async fn builder(
    service: &str,
    hub: &str,
    kafka: &str,
    mongo: &str,
    topic: &str,
) -> SnodraMessagingBuilder {
    SnodraMessaging::builder(config(service, hub, topic))
        .await
        .unwrap()
        .store(support::mongo_store(mongo).await)
        .event_store(support::kafka_events(kafka))
}

fn config(service: &str, hub: &str, topic: &str) -> Config {
    let mut c = Config::new(service, hub);
    c.events_source = topic.to_string();
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
        snodra_messaging::EventContext,
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

fn status(app: &SnodraMessaging, name: &str) -> ProcessorStatus {
    app.processors()
        .into_iter()
        .find(|p| p.name == name)
        .unwrap_or_else(|| panic!("no processor {name}"))
}

fn state(app: &SnodraMessaging, name: &str) -> ProcessorState {
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

/// A marker without a position, which no reset deletes: another family's (a saga's, the webhook
/// ingress') or a processor's own for a record not read from the events topic (a partner topic's,
/// a failed notice's).
async fn positionless_marker(db: &mongodb::Database, id: &str) {
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

/// The ranges of `outcome` as Kafka's partitions and offsets.
fn ranges(outcome: &proto::ResetOutcome) -> Vec<(i32, i64, i64)> {
    outcome
        .ranges
        .iter()
        .map(|r| {
            (
                r.segment.parse().unwrap(),
                r.from.parse().unwrap(),
                r.until.parse().unwrap(),
            )
        })
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
    let app = builder(
        "svc-pos",
        &hub.address(),
        &kafka.bootstrap,
        &mongo.uri_for("pos"),
        topic,
    )
    .await
    .event::<Noted, _, _>(default.handle())
    .event_in::<Noted, _, _>("audit", audit.handle())
    .start()
    .await
    .unwrap();
    let db = support::mongo_db(&mongo.uri_for("pos")).await;
    let p = producer(&kafka.bootstrap);
    for note in ["n-1", "n-2"] {
        produce(&p, topic, None, note).await;
    }
    eventually("both handled and n-2 parked", WITHIN, || async {
        marker(&db, "processor:default:n-2").await.is_some()
            && app.dead_letters().count(Some("audit")).await.unwrap() == 1
    })
    .await;

    for (id, processor, offset) in [
        ("processor:default:n-1", "default", 0i64),
        ("processor:default:n-2", "default", 1),
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
        builder("svc-pause", &hub.address(), &kafka.bootstrap, &uri, topic)
            .await
            .event::<Noted, _, _>(handler.handle())
            .start()
            .await
            .unwrap()
    };
    let app = start().await;
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let db = support::mongo_db(&uri).await;
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
        members(&kafka.bootstrap, "svc-pause.default").len(),
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
        builder("svc-lease", &hub.address(), &kafka.bootstrap, &uri, topic)
            .await
            .event_in::<Noted, _, _>("audit", handler.handle())
            .start()
            .await
            .unwrap()
    };
    let group = "svc-lease.audit";
    let app = start().await;
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let db = support::mongo_db(&uri).await;
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

    // A finish releases at once; a repeated finish (nothing held) is ok. The prepare goes to
    // both instances and returns with the first reply, so the other may still be leaving.
    ok(hub.control("audit", prepare("op-3", 30_000), ASK).await);
    eventually("both left the group", WITHIN, || async {
        members(&kafka.bootstrap, group).is_empty()
    })
    .await;
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
/// handler once, the markers of the range deleted, the ranges written before the offsets), and after the finish every record of the range is handled
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
        builder("svc-replay", &hub.address(), &kafka.bootstrap, &uri, topic)
            .await
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
    let db = support::mongo_db(&uri).await;
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
            && (0..2).all(|p| committed(&kafka.bootstrap, "svc-replay.default", topic, p) == 2)
    })
    .await;
    // Were the sagas run again, their handler would count (their markers are gone).
    db.collection::<Document>("processed_events")
        .delete_many(doc! { "_id": { "$regex": "^saga:" } })
        .await
        .unwrap();
    let positionless = [
        "saga:c.Other:x-3",
        "ingress:hook:k",
        "processor:default:partner:0:7",
        "processor:default:failed:x",
    ];
    for id in positionless {
        positionless_marker(&db, id).await;
    }
    let audit_markers: Vec<String> = marker_ids(&db)
        .await
        .into_iter()
        .filter(|m| m.starts_with("processor:audit:"))
        .collect();
    assert_eq!(audit_markers.len(), 4, "{audit_markers:?}");

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
    assert!(members(&kafka.bootstrap, "svc-replay.default").is_empty());
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
    assert_eq!(outcome.markers_deleted, 4, "the four of the range");
    assert_eq!(outcome.reset_handlers, 1);
    let ctx = resets.lock().unwrap()[0].clone();
    assert_eq!(ctx.processor, "default");
    assert_eq!(ctx.operation_id, "op-r");
    assert_eq!(ctx.target, ResetTarget::Earliest);
    assert_eq!(
        ctx.ranges,
        [0, 1].map(|partition: i32| ReplayRange {
            source: topic.into(),
            segment: partition.to_string(),
            from: "0".into(),
            until: "2".into()
        })
    );
    let written = replay(&db, "default").await;
    assert_eq!(written.len(), 2, "{written:?}");
    assert_eq!(
        written[1],
        doc! { "topic": topic, "partition": 1, "from": 0i64, "until": 2i64 }
    );
    let left = marker_ids(&db).await;
    for gone in ["n-1", "n-2", "n-3", "n-4"] {
        let gone = format!("processor:default:{gone}");
        assert!(!left.contains(&gone), "{left:?}");
    }
    for kept in positionless {
        assert!(left.contains(&kept.to_string()), "{left:?}");
    }
    assert_eq!(
        left.iter()
            .filter(|m| m.starts_with("processor:audit:"))
            .count(),
        4,
        "another processor's markers are untouched"
    );
    for p in 0..2 {
        assert_eq!(
            committed(&kafka.bootstrap, "svc-replay.default", topic, p),
            0
        );
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
        let m = marker(&db, &format!("processor:default:{n}"))
            .await
            .expect("a new marker");
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
/// from there; an active member of the group is SUBSCRIBERS_ACTIVE and a
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
    let panic_reset = Arc::new(AtomicBool::new(false));
    let resets = Arc::new(AtomicUsize::new(0));
    let (fail, panics, count) = (
        Arc::clone(&fail_reset),
        Arc::clone(&panic_reset),
        Arc::clone(&resets),
    );
    let app = builder(
        "svc-targets",
        &hub.address(),
        &kafka.bootstrap,
        &mongo.uri_for("targets"),
        topic,
    )
    .await
    .event_in::<Noted, _, _>("audit", handler.handle())
    .reset_handler("audit", move |_| {
        let (fail, panics, count) = (Arc::clone(&fail), Arc::clone(&panics), Arc::clone(&count));
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            if panics.load(Ordering::SeqCst) {
                panic!("the projection reset is not written yet");
            }
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
    let db = support::mongo_db(&mongo.uri_for("targets")).await;
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
    assert_eq!(code, ControlRefusal::SubscribersActive, "{message}");
    assert!(message.contains("old-instance"), "{message}");
    assert_eq!(committed(&kafka.bootstrap, group, topic, 0), 4);
    assert!(marker(&db, "processor:audit:n-1").await.is_some());
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
        message.contains("reset handlers ran (1), no markers deleted, positions not moved"),
        "{message}"
    );
    assert_eq!(committed(&kafka.bootstrap, group, topic, 0), 4);
    assert!(marker(&db, "processor:audit:n-1").await.is_some());
    assert!(replay(&db, "audit").await.is_empty());
    ok(hub.control("audit", finish("op-b", true), ASK).await);
    fail_reset.store(false, Ordering::SeqCst);
    // A panicking one is answered the same way.
    panic_reset.store(true, Ordering::SeqCst);
    ok(hub.control("audit", prepare("op-p", 30_000), ASK).await);
    let (code, message) = refused(hub.control("audit", execute("op-p", earliest()), ASK).await);
    assert_eq!(code, ControlRefusal::ResetHandlerFailed, "{message}");
    assert!(message.contains("panicked"), "{message}");
    assert_eq!(committed(&kafka.bootstrap, group, topic, 0), 4);
    ok(hub.control("audit", finish("op-p", true), ASK).await);
    panic_reset.store(false, Ordering::SeqCst);
    resets.store(0, Ordering::SeqCst);

    // Bad targets.
    ok(hub.control("audit", prepare("op-c", 30_000), ASK).await);
    let offsets = |all: &[(i32, i64)]| {
        reset_target::To::Positions(proto::SegmentPositions {
            positions: all
                .iter()
                .map(|&(partition, offset)| proto::SegmentPosition {
                    segment: partition.to_string(),
                    position: offset.to_string(),
                })
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

    // A time: the records from then on.
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
    let app = builder(
        "svc-dlq",
        &hub.address(),
        &kafka.bootstrap,
        &mongo.uri_for("dlq"),
        topic,
    )
    .await
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
            head.source.as_str(),
            head.segment.as_str(),
            head.position.as_str()
        ),
        ("audit", topic, "0", "0")
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
/// (never queued behind the execute).
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
    let app = builder(
        "svc-stream",
        &hub.address(),
        &kafka.bootstrap,
        &mongo.uri_for("stream"),
        topic,
    )
    .await
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
        handler.count("n-1") == 1
            && committed(&kafka.bootstrap, "svc-stream.default", topic, 0) == 1
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
    // Dead letters never wait behind the execute (the hub times a request out after 10 s): the
    // list is read beside it, a change is refused at once.
    let started = std::time::Instant::now();
    let page = hub
        .control(
            "default",
            Action::DeadLetterList(proto::DeadLetterList { limit: 10 }),
            Duration::from_secs(5),
        )
        .await;
    assert!(
        matches!(
            page.as_ref().and_then(|r| r.result.as_ref()),
            Some(Answer::DeadLetters(p)) if p.total == 0
        ),
        "{page:?}"
    );
    for change in [
        Action::DeadLetterRetry(proto::DeadLetterRetry {
            letter_id: "default:stream.events:0:0".into(),
        }),
        Action::DeadLetterRetryAll(proto::DeadLetterRetryAll {}),
        Action::DeadLetterDiscard(proto::DeadLetterDiscard {
            letter_id: "default:stream.events:0:0".into(),
        }),
    ] {
        let (code, message) = refused(hub.control("default", change, Duration::from_secs(5)).await);
        assert_eq!(code, ControlRefusal::Locked, "{message}");
        assert!(
            message.contains("executing reset operation op-s"),
            "{message}"
        );
    }
    assert!(
        started.elapsed() < Duration::from_secs(4),
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
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct Ping {}
message_type!(Ping, "c.Ping");
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct Pong {
    pong: bool,
}
impl snodra_messaging::Command for Ping {
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
) -> SnodraMessaging {
    let mut builder = builder(service, &hub.address(), kafka, uri, topic)
        .await
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
    let db = support::mongo_db(&uri).await;
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
    // Whichever notices the lost lock first answers (ADR 0023 ruling 19): the renewal ("lock
    // lost"), or the control poll, which forgets op-2 so this prepare is a late one ("already
    // finished"). Either way the lock is not taken (below).
    assert!(
        message.contains("lock lost") || message.contains("op-2 already finished"),
        "{message}"
    );
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
    let db = support::mongo_db(&uri).await;
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
    let app = builder(
        "svc-lost",
        &hub.address(),
        &kafka.bootstrap,
        &uri,
        topic,
    )
    .await
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
    let db = support::mongo_db(&uri).await;
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
        "positions not moved",
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
    let db = support::mongo_db(&uri).await;
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

/// Review focus 4 (ADR 0027): segments reassigned during an open replay range keep replaying up
/// to `replay_until` on their new owner and nowhere else, on the in-memory stores (no container).
/// One instance holds both segments of the event store when it resets them to earliest (the ranges
/// end at the two records each already handled), and gets its consumer back paused; a second
/// instance joins and takes segment 1 over. On the resume each replays the range of its own
/// segment (without the handler that disallows replay) and handles what came after the reset
/// as new records; neither ever sees the other's segment.
#[tokio::test]
async fn replay_range_survives_reassignment() {
    let store = snodra_messaging::store::memory::MemoryStore::new();
    use snodra_messaging::event_store::memory::{MemoryEventStore, MemoryEventStoreConfig};
    let events = MemoryEventStore::new(MemoryEventStoreConfig {
        segments: 2,
        ..MemoryEventStoreConfig::default()
    })
    .unwrap();
    let topic = "rr.events";
    // (instance, note) of every run of the handler, and of the one that disallows replay.
    let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let once: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let start = |instance: &'static str| {
        let (seen, once, store, events) = (
            Arc::clone(&seen),
            Arc::clone(&once),
            store.clone(),
            events.clone(),
        );
        async move {
            SnodraMessaging::builder(config("svc-rr", "static://127.0.0.1:1", topic))
                .await
                .unwrap()
                .store(Arc::new(store))
                .event_store(Arc::new(events))
                .event::<Noted, _, _>(move |e, _| {
                    seen.lock()
                        .unwrap()
                        .push((instance.to_string(), e.note.clone()));
                    async { Ok(()) }
                })
                .event_with::<Noted, _, _>(EventOptions::new().disallow_replay(), move |e, _| {
                    once.lock()
                        .unwrap()
                        .push((instance.to_string(), e.note.clone()));
                    async { Ok(()) }
                })
                .start()
                .await
                .unwrap()
        }
    };
    // Without a key the event store spreads the records round robin from segment 0: n-0 and n-2
    // go to segment 0, n-1 and n-3 to segment 1, each at positions 0 and 1.
    let publish = |note: &'static str| {
        let events = events.clone();
        async move {
            use snodra_messaging::event_store::{EventStore, OutgoingEvent};
            events
                .publish(&OutgoingEvent {
                    source: topic.into(),
                    id: note.into(),
                    type_name: "c.Noted".into(),
                    key: None,
                    headers: std::collections::HashMap::from([
                        ("type".to_string(), "c.Noted".to_string()),
                        ("id".to_string(), note.to_string()),
                    ]),
                    payload: serde_json::to_vec(&Noted {
                        note: note.to_string(),
                    })
                    .unwrap(),
                })
                .await
                .unwrap();
        }
    };
    let runs = |log: &Arc<Mutex<Vec<(String, String)>>>, instance: &str| -> Vec<String> {
        let mut notes: Vec<String> = log
            .lock()
            .unwrap()
            .iter()
            .filter(|(i, _)| i == instance)
            .map(|(_, n)| n.clone())
            .collect();
        notes.sort();
        notes
    };

    let a = start("a").await;
    for note in ["n-0", "n-1", "n-2", "n-3"] {
        publish(note).await;
    }
    eventually("a handled the four", WITHIN, || async {
        runs(&seen, "a").len() == 4 && runs(&once, "a").len() == 4
    })
    .await;

    // Paused, then reset to earliest while a alone holds both segments.
    let control = a.processor_control();
    let ask = |action| {
        let control = control.clone();
        async move {
            Some(
                control
                    .handle(proto::ControlRequest {
                        control_id: "c".into(),
                        processor: "default".into(),
                        action: Some(action),
                    })
                    .await,
            )
        }
    };
    ok(ask(pause()).await);
    ok(ask(prepare("op-rr", 30_000)).await);
    let outcome = reset(ask(execute("op-rr", earliest())).await);
    assert_eq!(ranges(&outcome), [(0, 0, 2), (1, 0, 2)]);
    assert_eq!(outcome.markers_deleted, 4);
    // After the reset: new records, past the ranges (n-4 to segment 0, n-5 to segment 1).
    publish("n-4").await;
    publish("n-5").await;
    ok(ask(finish("op-rr", false)).await);

    // b joins while paused and takes segment 1 over; the ranges stay open on both owners.
    let b = start("b").await;
    let segments = |app: &SnodraMessaging| -> Vec<(String, String)> {
        status(app, "default")
            .segments
            .iter()
            .map(|s| (s.segment.clone(), s.replay_until.clone()))
            .collect()
    };
    eventually("one segment each, both replaying", WITHIN, || async {
        segments(&a) == [("0".to_string(), "2".to_string())]
            && segments(&b) == [("1".to_string(), "2".to_string())]
    })
    .await;
    assert!(status(&b, "default").replaying);

    ok(ask(resume()).await);
    eventually(
        "each replayed its range and handled the new one",
        WITHIN,
        || async { runs(&seen, "a").len() == 7 && runs(&seen, "b").len() == 3 },
    )
    .await;
    // The replay of each segment ran on its owner only; the new records once, not as a replay.
    assert_eq!(
        runs(&seen, "a"),
        ["n-0", "n-0", "n-1", "n-2", "n-2", "n-3", "n-4"]
    );
    assert_eq!(runs(&seen, "b"), ["n-1", "n-3", "n-5"]);
    assert_eq!(runs(&once, "a"), ["n-0", "n-1", "n-2", "n-3", "n-4"]);
    assert_eq!(runs(&once, "b"), ["n-5"], "only past the range");
    eventually("the ranges removed by their owners", WITHIN, || async {
        use snodra_messaging::store::MessageStore;
        let control = store.processor_control();
        control.get("default").await.unwrap().replay.is_empty()
    })
    .await;
    assert!(!status(&a, "default").replaying && !status(&b, "default").replaying);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        runs(&seen, "a").len() + runs(&seen, "b").len(),
        10,
        "nothing more"
    );
    b.shutdown().await;
    a.shutdown().await;
}
