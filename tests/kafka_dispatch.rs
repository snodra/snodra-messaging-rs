//! The local event dispatcher (S2 Task 4, ADR 0021 rulings 2 and 26): stage 1 runs the ordinary
//! `event` handlers in one transaction with the idempotency marker, stage 2 hands the event to the
//! saga stage after it committed, and stage 2 runs even when stage 1 failed.

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use kraken_messaging::config::DeadLetterMode;
use kraken_messaging::hub::HandlerRegistry;
use kraken_messaging::kafka::dispatch::{
    LocalEventDispatcher, NoSagas, Outcome, SagaStage, StageOneDone,
};
use kraken_messaging::kafka::{
    EventConsumer, ProcessorSettings, RecordDisposition, classify_with, ensure_indexes, headers,
};
use kraken_messaging::{Event, MessageContext, MessagingError, message_type};
use mongodb::Database;
use mongodb::bson::doc;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Clone)]
struct Happened {
    id: String,
    at: DateTime<Utc>,
}
message_type!(Happened, "t.Happened");
impl Event for Happened {}

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

fn payload(id: &str) -> Vec<u8> {
    serde_json::to_vec(&Happened {
        id: id.into(),
        at: Utc::now(),
    })
    .unwrap()
}

fn ctx(id: &str, type_name: &str) -> MessageContext {
    MessageContext {
        message_id: id.into(),
        type_name: type_name.into(),
        correlation_id: format!("corr-{id}"),
        headers: HashMap::from([
            ("id".to_string(), id.to_string()),
            ("type".to_string(), type_name.to_string()),
        ]),
        ..Default::default()
    }
}

struct Call {
    type_name: String,
    payload: Vec<u8>,
    ctx: MessageContext,
    /// Stage 1's marker was visible (committed) when stage 2 started.
    marker_committed: bool,
    /// What the ordinary handlers had done by then.
    handlers_seen: Vec<String>,
}

/// A saga stage that records what it was given and what the ordinary handlers had done by then.
struct RecordingStage {
    interest: bool,
    applies: bool,
    fail_with: Mutex<Option<String>>,
    db: Database,
    handlers: Arc<Mutex<Vec<String>>>,
    calls: Mutex<Vec<Call>>,
}

impl RecordingStage {
    fn new(interest: bool, db: &Database, handlers: &Arc<Mutex<Vec<String>>>) -> Arc<Self> {
        Self::with_applies(interest, true, db, handlers)
    }

    /// `applies`: whether `process` reports the event applied to a saga instance.
    fn with_applies(
        interest: bool,
        applies: bool,
        db: &Database,
        handlers: &Arc<Mutex<Vec<String>>>,
    ) -> Arc<Self> {
        Arc::new(RecordingStage {
            interest,
            applies,
            fail_with: Mutex::new(None),
            db: db.clone(),
            handlers: Arc::clone(handlers),
            calls: Mutex::new(Vec::new()),
        })
    }
}

impl SagaStage for RecordingStage {
    fn interested(&self, type_name: &str) -> bool {
        self.interest && type_name == "t.Happened"
    }

    fn process<'a>(
        &'a self,
        type_name: &'a str,
        payload: &'a [u8],
        ctx: &'a MessageContext,
    ) -> BoxFuture<'a, Result<bool, MessagingError>> {
        Box::pin(async move {
            // Outside any transaction, so a marker that is visible here has been committed.
            let committed = self
                .db
                .collection::<mongodb::bson::Document>("processed_events")
                .count_documents(doc! { "_id": format!("processor:default:{}", ctx.message_id) })
                .await
                .unwrap()
                == 1;
            self.calls.lock().unwrap().push(Call {
                type_name: type_name.into(),
                payload: payload.to_vec(),
                ctx: ctx.clone(),
                marker_committed: committed,
                handlers_seen: self.handlers.lock().unwrap().clone(),
            });
            match self.fail_with.lock().unwrap().clone() {
                Some(message) => Err(MessagingError::Storage(message)),
                None => Ok(self.applies),
            }
        })
    }
}

struct Fixture {
    db: Database,
    client: mongodb::Client,
    handlers: Arc<Mutex<Vec<String>>>,
    fail_ordinary: Arc<Mutex<Option<String>>>,
    _mongo: support::containers::MongoDb,
}

impl Fixture {
    async fn start() -> Fixture {
        let mongo = support::containers::mongo().await;
        let client = mongodb::Client::with_uri_str(&mongo.uri).await.unwrap();
        let db = client.default_database().unwrap();
        ensure_indexes(
            &db,
            Duration::from_secs(86_400),
            Duration::from_secs(86_400),
        )
        .await
        .unwrap();
        Fixture {
            db,
            client,
            handlers: Arc::new(Mutex::new(Vec::new())),
            fail_ordinary: Arc::new(Mutex::new(None)),
            _mongo: mongo,
        }
    }

    fn registry(&self, with_handler: bool) -> Arc<HandlerRegistry> {
        let mut registry = HandlerRegistry::new();
        if with_handler {
            let seen = Arc::clone(&self.handlers);
            let fail = Arc::clone(&self.fail_ordinary);
            registry.event::<Happened, _, _>(move |e, _ctx| {
                let seen = Arc::clone(&seen);
                let fail = Arc::clone(&fail);
                async move {
                    if fail.lock().unwrap().as_deref() == Some(e.id.as_str()) {
                        return Err(MessagingError::Storage("ordinary handler is broken".into()));
                    }
                    seen.lock().unwrap().push(e.id);
                    Ok(())
                }
            });
        }
        Arc::new(registry)
    }

    fn dispatcher(&self, stage: Arc<dyn SagaStage>, with_handler: bool) -> LocalEventDispatcher {
        LocalEventDispatcher::new(
            self.client.clone(),
            self.db.clone(),
            self.registry(with_handler),
            stage,
        )
    }

    async fn markers(&self) -> u64 {
        self.db
            .collection::<mongodb::bson::Document>("processed_events")
            .count_documents(doc! {})
            .await
            .unwrap()
    }
}

async fn dispatch(d: &LocalEventDispatcher, id: &str) -> Result<Outcome, MessagingError> {
    d.dispatch(id, "t.Happened", &payload(id), &ctx(id, "t.Happened"))
        .await
}

#[tokio::test]
async fn a_record_with_no_ordinary_handler_but_saga_interest_reaches_the_saga_stage() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::new(true, &f.db, &f.handlers);
    let dispatcher = f.dispatcher(stage.clone(), false);

    let outcome = dispatch(&dispatcher, "e-1").await.unwrap();

    assert_eq!(outcome, Outcome::Handled);
    {
        let calls = stage.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].type_name, "t.Happened");
        assert_eq!(calls[0].ctx.message_id, "e-1");
        assert_eq!(calls[0].ctx.correlation_id, "corr-e-1");
        let event: Happened = serde_json::from_slice(&calls[0].payload).unwrap();
        assert_eq!(event.id, "e-1");
    }
    assert!(f.handlers.lock().unwrap().is_empty());
    assert_eq!(
        f.markers().await,
        0,
        "stage 1 did not run, so it left no marker"
    );
}

#[tokio::test]
async fn a_redelivered_record_skips_stage_1_by_its_marker_and_still_runs_stage_2() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::new(true, &f.db, &f.handlers);
    let dispatcher = f.dispatcher(stage.clone(), true);

    assert_eq!(
        dispatch(&dispatcher, "r-1").await.unwrap(),
        Outcome::Handled
    );
    assert_eq!(
        dispatch(&dispatcher, "r-1").await.unwrap(),
        Outcome::Handled
    );

    assert_eq!(
        *f.handlers.lock().unwrap(),
        ["r-1"],
        "the ordinary handler ran once"
    );
    assert_eq!(
        stage.calls.lock().unwrap().len(),
        2,
        "the saga stage runs again: its own markers decide"
    );
    assert_eq!(f.markers().await, 1);
}

#[tokio::test]
async fn stage_2_runs_after_stage_1_committed() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::new(true, &f.db, &f.handlers);

    dispatch(&f.dispatcher(stage.clone(), true), "o-1")
        .await
        .unwrap();

    let calls = stage.calls.lock().unwrap();
    assert!(
        calls[0].marker_committed,
        "stage 1's marker was committed when stage 2 started"
    );
    assert_eq!(calls[0].handlers_seen, ["o-1"]);
}

#[tokio::test]
async fn a_stage_1_failure_still_runs_stage_2_then_the_error_propagates() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::new(true, &f.db, &f.handlers);
    let dispatcher = f.dispatcher(stage.clone(), true);
    *f.fail_ordinary.lock().unwrap() = Some("poison-1".into());

    let err = dispatch(&dispatcher, "poison-1").await.unwrap_err();

    assert!(
        err.to_string().contains("ordinary handler is broken"),
        "{err}"
    );
    assert_eq!(
        stage.calls.lock().unwrap().len(),
        1,
        "the sagas must not miss the event because an ordinary handler failed"
    );
    assert_eq!(
        f.markers().await,
        0,
        "the marker rolled back with the handler"
    );

    // Fixed: the redelivery runs both stages again (stage 2's own markers make that safe).
    *f.fail_ordinary.lock().unwrap() = None;
    assert_eq!(
        dispatch(&dispatcher, "poison-1").await.unwrap(),
        Outcome::Handled
    );
    assert_eq!(stage.calls.lock().unwrap().len(), 2);
    assert_eq!(f.markers().await, 1);
}

#[tokio::test]
async fn when_both_stages_fail_stage_1s_error_is_the_one_returned() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::new(true, &f.db, &f.handlers);
    *stage.fail_with.lock().unwrap() = Some("the store is down".into());
    let dispatcher = f.dispatcher(stage.clone(), true);
    *f.fail_ordinary.lock().unwrap() = Some("both".into());

    let err = dispatch(&dispatcher, "both").await.unwrap_err();

    assert!(
        err.to_string().contains("ordinary handler is broken"),
        "{err}"
    );
    assert_eq!(stage.calls.lock().unwrap().len(), 1, "stage 2 ran");
}

#[tokio::test]
async fn a_stage_2_failure_is_returned_when_stage_1_succeeded() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::new(true, &f.db, &f.handlers);
    *stage.fail_with.lock().unwrap() = Some("the store is down".into());
    let dispatcher = f.dispatcher(stage.clone(), true);

    let err = dispatch(&dispatcher, "s2-1").await.unwrap_err();

    assert!(err.to_string().contains("the store is down"), "{err}");
    assert_eq!(
        *f.handlers.lock().unwrap(),
        ["s2-1"],
        "stage 1 had committed"
    );
    assert_eq!(f.markers().await, 1);
}

#[tokio::test]
async fn the_outcome_is_duplicate_when_stage_1s_marker_exists_and_no_saga_is_interested() {
    require_docker!();
    let f = Fixture::start().await;
    let dispatcher = f.dispatcher(Arc::new(NoSagas), true);

    assert_eq!(
        dispatch(&dispatcher, "d-1").await.unwrap(),
        Outcome::Handled
    );
    assert_eq!(
        dispatch(&dispatcher, "d-1").await.unwrap(),
        Outcome::Duplicate
    );

    assert_eq!(*f.handlers.lock().unwrap(), ["d-1"]);
    assert_eq!(f.markers().await, 1);
}

#[tokio::test]
async fn a_type_no_handler_and_no_saga_wants_is_no_interest_and_leaves_no_marker() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::new(false, &f.db, &f.handlers);
    let dispatcher = f.dispatcher(stage.clone(), false);

    assert!(!dispatcher.interested("t.Happened"));
    assert!(!dispatcher.interested("t.Unknown"));
    let outcome = dispatcher
        .dispatch("u-1", "t.Unknown", b"{}", &ctx("u-1", "t.Unknown"))
        .await
        .unwrap();

    assert_eq!(outcome, Outcome::NoInterest);
    assert_eq!(f.markers().await, 0);
    assert!(stage.calls.lock().unwrap().is_empty());
    assert!(
        f.dispatcher(Arc::new(NoSagas), true)
            .interested("t.Happened")
    );
    assert!(!NoSagas.interested("t.Happened"));
}

#[tokio::test]
async fn an_event_no_saga_instance_applied_and_no_ordinary_handler_took_is_unclaimed() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::with_applies(true, false, &f.db, &f.handlers);
    let dispatcher = f.dispatcher(stage.clone(), false);

    assert_eq!(
        dispatch(&dispatcher, "x-1").await.unwrap(),
        Outcome::Unclaimed
    );

    // With an ordinary handler the event is handled even when no saga applied it.
    let with_handler = f.dispatcher(stage, true);
    assert_eq!(
        dispatch(&with_handler, "x-2").await.unwrap(),
        Outcome::Handled
    );
}

#[test]
fn a_record_only_a_saga_wants_is_handled_not_skipped() {
    let hdrs: HashMap<String, String> = [
        (headers::TYPE.to_string(), "t.Happened".to_string()),
        (headers::ID.to_string(), "e-1".to_string()),
    ]
    .into();
    let none = HandlerRegistry::new();
    assert_eq!(
        classify_with(&hdrs, b"{}", "kraken.events", 0, 7, |_| false),
        RecordDisposition::Skip("no handler for this type")
    );
    assert_eq!(
        classify_with(&hdrs, b"{}", "kraken.events", 0, 7, |t| t == "t.Happened"),
        RecordDisposition::Handle {
            type_name: "t.Happened".into(),
            id: "e-1".into()
        }
    );
    // `classify` is `classify_with` over the registry's handlers.
    assert_eq!(
        kraken_messaging::kafka::classify(&hdrs, b"{}", "kraken.events", 0, 7, &none),
        RecordDisposition::Skip("no handler for this type")
    );
}

#[tokio::test]
async fn the_consumer_hands_a_record_only_a_saga_wants_to_the_saga_stage() {
    require_docker!();
    let kafka = support::containers::kafka().await;
    let f = Fixture::start().await;
    let stage = RecordingStage::new(true, &f.db, &f.handlers);
    let consumer = EventConsumer::processor(
        ProcessorSettings {
            name: "default".into(),
            bootstrap_servers: kafka.bootstrap.clone(),
            group_id: "svc-saga.default".into(),
            topic: "kraken.events".into(),
            max_attempts: 3,
            backoff: Duration::from_millis(50),
            dead_letter_mode: DeadLetterMode::Log,
            dead_letter_retention: Duration::from_secs(86_400),
            db: None,
        },
        Arc::new(LocalEventDispatcher::new(
            f.client.clone(),
            f.db.clone(),
            f.registry(false),
            stage.clone(),
        )),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap)
        .create()
        .unwrap();
    let hs = rdkafka::message::OwnedHeaders::new()
        .insert(rdkafka::message::Header {
            key: headers::TYPE,
            value: Some("t.Happened"),
        })
        .insert(rdkafka::message::Header {
            key: headers::ID,
            value: Some("k-1"),
        })
        .insert(rdkafka::message::Header {
            key: headers::CORRELATION_ID,
            value: Some("corr-k"),
        });
    producer
        .send(
            FutureRecord::to("kraken.events")
                .key("k")
                .payload(&payload("k-1"))
                .headers(hs),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap();

    let started = std::time::Instant::now();
    while stage.calls.lock().unwrap().is_empty() && started.elapsed() < Duration::from_secs(60) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancel.cancel();
    let _ = handle.await;

    let calls = stage.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "the saga stage saw the record");
    assert_eq!(calls[0].ctx.message_id, "k-1");
    assert_eq!(calls[0].ctx.correlation_id, "corr-k");
    assert_eq!(
        calls[0].ctx.headers.get("type").map(String::as_str),
        Some("t.Happened")
    );
}

// ---- P6a (ADR 0022): a named processor's markers and the stages one by one ----

/// Every processor's dispatcher keys its markers `processor:<name>:<eventId>`, the unnamed one
/// `processor:default:<eventId>`, so two processors never share a marker.
#[tokio::test]
async fn a_named_processors_markers_are_its_own_family() {
    require_docker!();
    let f = Fixture::start().await;
    let named = f
        .dispatcher(Arc::new(NoSagas), true)
        .with_marker_prefix("processor:orders:".into());
    let default = f.dispatcher(Arc::new(NoSagas), true);

    assert_eq!(dispatch(&named, "e-1").await.unwrap(), Outcome::Handled);
    assert_eq!(dispatch(&named, "e-1").await.unwrap(), Outcome::Duplicate);
    assert_eq!(dispatch(&default, "e-1").await.unwrap(), Outcome::Handled);
    let ids: Vec<String> =
        f.db.collection::<mongodb::bson::Document>("processed_events")
            .distinct("_id", doc! {})
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.as_str().unwrap().to_string())
            .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"processor:orders:e-1".to_string()), "{ids:?}");
    assert!(
        ids.contains(&"processor:default:e-1".to_string()),
        "{ids:?}"
    );
    assert_eq!(*f.handlers.lock().unwrap(), ["e-1", "e-1"]);
}

/// `dispatch_stages` tells each stage's result apart (the consumer parks only for stage 1), and
/// without stage 1 (a SEQUENCED record) only the saga stage runs.
#[tokio::test]
async fn dispatch_stages_reports_each_stage_and_can_skip_stage_one() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::new(true, &f.db, &f.handlers);
    let dispatcher = f.dispatcher(stage.clone(), true);
    assert!(dispatcher.has_stage_one("t.Happened"));
    assert!(!dispatcher.has_stage_one("t.Other"));
    assert_eq!(dispatcher.event_types(), ["t.Happened"]);

    *f.fail_ordinary.lock().unwrap() = Some("e-1".into());
    let stages = dispatcher
        .dispatch_stages(
            "e-1",
            "t.Happened",
            &payload("e-1"),
            &ctx("e-1", "t.Happened"),
            true,
        )
        .await;
    assert!(matches!(
        stages.stage_one,
        Some(Err(MessagingError::Storage(_)))
    ));
    assert!(matches!(stages.stage_two, Some(Ok(true))));

    let stages = dispatcher
        .dispatch_stages(
            "e-2",
            "t.Happened",
            &payload("e-2"),
            &ctx("e-2", "t.Happened"),
            false,
        )
        .await;
    assert!(stages.stage_one.is_none(), "stage 1 not run");
    assert!(matches!(stages.stage_two, Some(Ok(true))));
    assert!(f.handlers.lock().unwrap().is_empty());
    assert_eq!(f.markers().await, 0);
    assert_eq!(stage.calls.lock().unwrap().len(), 2);

    *stage.fail_with.lock().unwrap() = Some("saga store down".into());
    let stages = dispatcher
        .dispatch_stages(
            "e-3",
            "t.Happened",
            &payload("e-3"),
            &ctx("e-3", "t.Happened"),
            true,
        )
        .await;
    assert!(matches!(stages.stage_one, Some(Ok(StageOneDone::Handled))));
    assert!(matches!(stages.stage_two, Some(Err(_))));
    let stages = dispatcher
        .dispatch_stages(
            "e-3",
            "t.Happened",
            &payload("e-3"),
            &ctx("e-3", "t.Happened"),
            true,
        )
        .await;
    assert!(matches!(
        stages.stage_one,
        Some(Ok(StageOneDone::Duplicate))
    ));

    let none = f.dispatcher(Arc::new(NoSagas), false);
    let stages = none
        .dispatch_stages(
            "e-4",
            "t.Happened",
            &payload("e-4"),
            &ctx("e-4", "t.Happened"),
            true,
        )
        .await;
    assert!(stages.stage_one.is_none() && stages.stage_two.is_none());
}

/// ADR 0022 (with the Kotlin slice's ruling): a local dispatch from another entry point (a partner
/// topic, a webhook, a relay notice) reaches every processor's stage 1, each in its own
/// transaction with its own marker, and the sagas once; the Kafka consumer's own path does not
/// fan out.
#[tokio::test]
async fn a_local_dispatch_fans_out_to_every_processors_stage_one_and_the_sagas_once() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = RecordingStage::new(true, &f.db, &f.handlers);
    let named = Arc::new(
        f.dispatcher(Arc::new(NoSagas), true)
            .with_marker_prefix("processor:orders:".into()),
    );
    let default = f
        .dispatcher(stage.clone(), true)
        .with_fan_out(vec![Arc::clone(&named)]);

    assert_eq!(dispatch(&default, "e-1").await.unwrap(), Outcome::Handled);
    assert_eq!(
        *f.handlers.lock().unwrap(),
        ["e-1", "e-1"],
        "both processors"
    );
    assert_eq!(stage.calls.lock().unwrap().len(), 1, "the sagas once");
    let mut ids: Vec<String> =
        f.db.collection::<mongodb::bson::Document>("processed_events")
            .distinct("_id", doc! {})
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.as_str().unwrap().to_string())
            .collect();
    ids.sort();
    assert_eq!(ids, ["processor:default:e-1", "processor:orders:e-1"]);
    // Again: every stage 1 is a duplicate, the sagas decide by their own markers.
    dispatch(&default, "e-1").await.unwrap();
    assert_eq!(f.handlers.lock().unwrap().len(), 2);

    // A failure in the named processor's stage 1 is returned (the entry point retries); the
    // default processor's marker makes the retry a no-op there.
    *f.fail_ordinary.lock().unwrap() = Some("e-2".into());
    let only_named = f
        .dispatcher(Arc::new(NoSagas), false)
        .with_fan_out(vec![Arc::clone(&named)]);
    assert!(
        only_named.interested("t.Happened"),
        "a named processor's type"
    );
    assert!(dispatch(&only_named, "e-2").await.is_err());

    // The consumer's path: its own processor only.
    let stages = default
        .dispatch_stages(
            "e-3",
            "t.Happened",
            &payload("e-3"),
            &ctx("e-3", "t.Happened"),
            true,
        )
        .await;
    assert!(matches!(stages.stage_one, Some(Ok(StageOneDone::Handled))));
    let e3 = f
        .handlers
        .lock()
        .unwrap()
        .iter()
        .filter(|h| *h == "e-3")
        .count();
    assert_eq!(e3, 1, "the consumer's own processor only");
    assert_eq!(default.event_types(), ["t.Happened"]);
}
