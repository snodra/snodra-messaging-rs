//! The deadline scheduler (S2 Task 7, ADR 0021 rulings 15 and 37): due deadlines fire at least
//! once from a leased poller, against a real MongoDB. The Rust form of the Kotlin
//! `DeadlineSchedulerTest`.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use kraken_messaging::kafka::SagaStage;
use kraken_messaging::saga::store::{DeadlineDocument, DeadlineStatus, DispatchKind, SagaDocument};
use kraken_messaging::saga::{
    DeadlineScheduler, DeadlineSettings, SagaDefinition, SagaRetry, SagaRuntime, SagaType,
};
use kraken_messaging::store::MessageStore;
use kraken_messaging::time::epoch_ms;
use kraken_messaging::{Command, Event, RetryPolicy, message_type};
use kraken_messaging_mongo::documents::DeadlineDoc;
use mongodb::bson::doc;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use support::order_saga::RecordingMetrics;
use support::saga::{Store, event_ctx, state, t0};
use tokio_util::sync::CancellationToken;

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

macro_rules! event {
    ($name:ident, $wire:literal, { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct $name { $($field: $ty),* }
        message_type!($name, $wire);
        impl Event for $name {}
    };
}

event!(Placed, "dl.Placed", { order_id: String, seconds: u64 });
event!(Schedule, "dl.Schedule", { order_id: String, name: String, seconds: u64 });
event!(CancelOne, "dl.CancelOne", { order_id: String });
event!(CancelAll, "dl.CancelAll", { order_id: String, name: String });
event!(Paid, "dl.Paid", { order_id: String });

#[derive(Clone, Serialize, Deserialize)]
struct CancelCharge {
    r#ref: String,
}
message_type!(CancelCharge, "dl.CancelCharge");
impl Command for CancelCharge {
    type Reply = ();
}

#[derive(Clone, Serialize, Deserialize)]
struct TimeoutPayload {
    reason: String,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Charge {
    order_id: Option<String>,
    timeout_deadline: Option<String>,
    fired: u32,
    reasons: Vec<String>,
    fired_with_correlation: Option<String>,
}

fn charge_saga(broken: &Arc<AtomicBool>) -> SagaType {
    let broken = Arc::clone(broken);
    SagaDefinition::<Charge>::new("dl.Charge")
        .start_on::<Placed>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, ctx| {
                s.order_id = Some(e.order_id.clone());
                s.timeout_deadline = Some(ctx.schedule_after(
                    Duration::from_secs(e.seconds),
                    "charge-timeout",
                    Some(&TimeoutPayload {
                        reason: "no payment".into(),
                    }),
                )?);
                Ok(())
            },
        )
        .on::<Schedule>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |_, e, ctx| {
                ctx.schedule_after(
                    Duration::from_secs(e.seconds),
                    &e.name,
                    Some(&TimeoutPayload {
                        reason: format!("scheduled {}", e.name),
                    }),
                )?;
                Ok(())
            },
        )
        .on::<CancelOne>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, _, ctx| {
                if let Some(id) = &s.timeout_deadline {
                    ctx.cancel(id);
                }
                Ok(())
            },
        )
        .on::<CancelAll>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |_, e, ctx| {
                ctx.cancel_all(&e.name);
                Ok(())
            },
        )
        .on::<Paid>("orderId", |e| Some(e.order_id.clone()), |_, _, _| Ok(()))
        .ending()
        .on_deadline::<TimeoutPayload>("charge-timeout", move |s, payload, ctx| {
            if broken.load(Ordering::SeqCst) {
                return Err(kraken_messaging::MessagingError::InvalidMessage(
                    "deadline handler failed on purpose".into(),
                ));
            }
            s.fired += 1;
            s.fired_with_correlation = Some(ctx.message().correlation_id.clone());
            s.reasons
                .push(payload.map(|p| p.reason).unwrap_or_default());
            ctx.send(
                &CancelCharge {
                    r#ref: format!("ref-{}", s.order_id.clone().unwrap_or_default()),
                },
                None,
            )
        })
        .on_deadline::<IgnoredAny>("end-now", |s, _, _| {
            s.fired += 1;
            Ok(())
        })
        .ending()
        .build()
        .unwrap()
}

struct Dl {
    store: Store,
    metrics: Arc<RecordingMetrics>,
    broken: Arc<AtomicBool>,
    runtime: Arc<SagaRuntime>,
}

impl Dl {
    async fn start() -> Dl {
        let store = Store::start().await;
        let broken = Arc::new(AtomicBool::new(false));
        let metrics = RecordingMetrics::new();
        let runtime = Arc::new(SagaRuntime::new(
            vec![charge_saga(&broken)],
            store.ports(),
            store.clock.clock(),
            SagaRetry::new(RetryPolicy {
                max_attempts: 2,
                initial_backoff: Duration::from_millis(1),
                multiplier: 1.0,
                max_backoff: Duration::from_millis(1),
            }),
            metrics.clone(),
        ));
        Dl {
            store,
            metrics,
            broken,
            runtime,
        }
    }

    fn scheduler(&self) -> Arc<DeadlineScheduler> {
        DeadlineScheduler::new(
            self.store.store.sagas().deadlines,
            Arc::clone(&self.runtime),
            self.store.clock.clock(),
            DeadlineSettings {
                poll_interval: Duration::from_millis(50),
                lease: Duration::from_secs(30),
                batch_size: 100,
            },
            self.metrics.clone(),
        )
        .unwrap()
    }

    async fn deliver<E: Event>(&self, event: &E) {
        self.deliver_as(event, &uuid::Uuid::new_v4().to_string())
            .await;
    }

    async fn deliver_as<E: Event>(&self, event: &E, id: &str) {
        self.runtime
            .process(
                E::TYPE,
                &serde_json::to_vec(event).unwrap(),
                &event_ctx(id, E::TYPE, &[]),
            )
            .await
            .unwrap();
    }

    fn advance(&self, seconds: u64) {
        self.store.clock.advance(Duration::from_secs(seconds));
    }

    async fn saga(&self, order_id: &str) -> SagaDocument {
        self.store
            .sagas("dl.Charge")
            .await
            .into_iter()
            .find(|s| state(s)["orderId"] == order_id)
            .unwrap_or_else(|| panic!("no saga for {order_id}"))
    }

    async fn fired(&self, order_id: &str) -> u64 {
        state(&self.saga(order_id).await)["fired"].as_u64().unwrap()
    }

    async fn deadlines_of(&self, saga_id: &str) -> Vec<DeadlineDocument> {
        self.store
            .records::<DeadlineDoc, _>("deadlines", doc! { "sagaId": saga_id })
            .await
    }
}

fn placed(order_id: &str, seconds: u64) -> Placed {
    Placed {
        order_id: order_id.into(),
        seconds,
    }
}

/// Epoch milliseconds.
fn dt(t: std::time::SystemTime) -> i64 {
    epoch_ms(t)
}

#[tokio::test]
async fn a_deadline_carries_the_correlation_id_of_the_message_that_scheduled_it_to_its_handler_and_to_the_commands_it_sends()
 {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver_as(&placed("o40", 60), "e-40").await; // delivered with the correlation id "corr-e-40"
    let stored: Vec<mongodb::bson::Document> = dl.store.all("deadlines").await;
    assert_eq!(
        stored[0].get_str("correlationId").unwrap(),
        "corr-e-40",
        "stored with the deadline"
    );

    dl.advance(61);
    dl.scheduler().poll_once().await.unwrap();

    assert_eq!(
        state(&dl.saga("o40").await)["firedWithCorrelation"],
        "corr-e-40",
        "the handler's message context"
    );
    let sent = dl.store.dispatch_of(&dl.saga("o40").await.id).await;
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].headers["correlation-id"], "corr-e-40",
        "the command it sent"
    );
}

#[tokio::test]
async fn a_due_deadline_fires_its_handler_once_with_its_payload() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 900)).await;
    let scheduler = dl.scheduler();

    scheduler.poll_once().await.unwrap();
    assert_eq!(dl.fired("o1").await, 0, "not due yet");

    dl.advance(901);
    assert_eq!(scheduler.poll_once().await.unwrap(), 1);
    scheduler.poll_once().await.unwrap();

    assert_eq!(dl.fired("o1").await, 1);
    assert_eq!(
        state(&dl.saga("o1").await)["reasons"],
        serde_json::json!(["no payment"])
    );
    let deadline = dl.store.deadlines().await.remove(0);
    assert_eq!(deadline.status, DeadlineStatus::Fired);
    assert_eq!(deadline.finished_at_ms, Some(dt(dl.store.clock.now())));
    assert_eq!(*dl.metrics.fired.lock().unwrap(), ["charge-timeout"]);
}

#[tokio::test]
async fn a_deadline_fires_after_a_restart() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 60)).await;
    let _never_polled = dl.scheduler(); // a scheduler that never got to poll: its service instance stopped

    dl.advance(61);
    dl.scheduler().poll_once().await.unwrap(); // a new instance of the service

    assert_eq!(dl.fired("o1").await, 1);
}

#[tokio::test]
async fn a_cancelled_deadline_never_fires() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 60)).await;
    dl.deliver(&CancelOne {
        order_id: "o1".into(),
    })
    .await;

    dl.advance(3600);
    dl.scheduler().poll_once().await.unwrap();

    assert_eq!(dl.fired("o1").await, 0);
    assert_eq!(
        dl.store.deadlines().await[0].status,
        DeadlineStatus::Cancelled
    );
}

#[tokio::test]
async fn cancel_all_cancels_every_scheduled_deadline_of_that_name_for_the_saga() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 60)).await;
    for (name, seconds) in [
        ("charge-timeout", 120),
        ("charge-timeout", 180),
        ("end-now", 240),
    ] {
        dl.deliver(&Schedule {
            order_id: "o1".into(),
            name: name.into(),
            seconds,
        })
        .await;
    }
    dl.deliver(&placed("o2", 60)).await;

    dl.deliver(&CancelAll {
        order_id: "o1".into(),
        name: "charge-timeout".into(),
    })
    .await;

    let of_o1 = dl.deadlines_of(&dl.saga("o1").await.id).await;
    let timeouts: Vec<_> = of_o1
        .iter()
        .filter(|d| d.name == "charge-timeout")
        .collect();
    assert_eq!(timeouts.len(), 3);
    assert!(
        timeouts
            .iter()
            .all(|d| d.status == DeadlineStatus::Cancelled)
    );
    let end_now = of_o1.iter().find(|d| d.name == "end-now").unwrap();
    assert_eq!(end_now.status, DeadlineStatus::Scheduled);
    let of_o2 = dl.deadlines_of(&dl.saga("o2").await.id).await;
    assert_eq!(
        of_o2[0].status,
        DeadlineStatus::Scheduled,
        "another saga's deadline is untouched"
    );
    dl.advance(3600);
    dl.scheduler().poll_once().await.unwrap();
    assert_eq!(dl.fired("o1").await, 1, "only end-now fired");
}

#[tokio::test]
async fn a_deadline_of_an_ended_saga_is_marked_cancelled_when_found_due() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 60)).await;
    dl.deliver(&Paid {
        order_id: "o1".into(),
    })
    .await;
    let ended = dl.saga("o1").await;
    assert!(ended.ended);
    // A deadline still scheduled for an ended saga (its write raced the saga's end) or for no
    // saga: found due, it is cancelled, not run.
    let deadline = |id: &str, saga_id: &str| DeadlineDocument {
        id: id.into(),
        saga_type: "dl.Charge".into(),
        saga_id: saga_id.into(),
        name: "charge-timeout".into(),
        payload: Some(r#"{"reason":"late"}"#.into()),
        payload_type: Some("dl.TimeoutPayload".into()),
        due_at_ms: dt(t0() + Duration::from_secs(10)),
        status: DeadlineStatus::Scheduled,
        claimed_by: None,
        claimed_until_ms: None,
        finished_at_ms: None,
        correlation_id: None,
    };
    dl.store.insert_deadline(&deadline("late", &ended.id)).await;
    dl.store
        .insert_deadline(&deadline("orphan", "no-such-saga"))
        .await;

    dl.advance(3600);
    dl.scheduler().poll_once().await.unwrap();

    for id in ["late", "orphan"] {
        let found: Vec<DeadlineDocument> = dl
            .store
            .records::<DeadlineDoc, _>("deadlines", doc! { "_id": id })
            .await;
        assert_eq!(found[0].status, DeadlineStatus::Cancelled, "{id}");
    }
    assert_eq!(dl.fired("o1").await, 0);
    assert!(dl.metrics.fired.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_deadline_whose_saga_type_has_no_handler_of_that_name_is_cancelled() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 600)).await;
    dl.deliver(&Schedule {
        order_id: "o1".into(),
        name: "nobody-handles-this".into(),
        seconds: 10,
    })
    .await;

    dl.advance(11);
    dl.scheduler().poll_once().await.unwrap();

    let deadlines = dl.store.deadlines().await;
    let unhandled = deadlines
        .iter()
        .find(|d| d.name == "nobody-handles-this")
        .unwrap();
    assert_eq!(unhandled.status, DeadlineStatus::Cancelled);
    assert_eq!(dl.fired("o1").await, 0);
}

#[tokio::test]
async fn an_expired_lease_makes_a_deadline_fire_again_at_least_once() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 60)).await;
    dl.advance(61);
    dl.broken.store(true, Ordering::SeqCst); // the first service instance claims it and fails (or crashes) before firing
    let first = dl.scheduler();

    first.poll_once().await.unwrap();
    let stuck = dl.store.deadlines().await.remove(0);
    assert_eq!(stuck.status, DeadlineStatus::Scheduled);
    assert!(stuck.claimed_until_ms.is_some());

    dl.broken.store(false, Ordering::SeqCst);
    dl.advance(10);
    dl.scheduler().poll_once().await.unwrap();
    assert_eq!(
        dl.fired("o1").await,
        0,
        "the lease of the first claim still holds"
    );

    dl.advance(30);
    dl.scheduler().poll_once().await.unwrap();

    assert_eq!(dl.fired("o1").await, 1);
    assert_eq!(dl.store.deadlines().await[0].status, DeadlineStatus::Fired);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_schedulers_fire_one_deadline_once_while_the_lease_holds() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 60)).await;
    dl.advance(61);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let polls: Vec<_> = (0..2)
        .map(|_| {
            let (scheduler, barrier) = (dl.scheduler(), Arc::clone(&barrier));
            tokio::spawn(async move {
                barrier.wait().await;
                scheduler.poll_once().await.unwrap()
            })
        })
        .collect();

    let mut claimed = 0;
    for poll in polls {
        claimed += poll.await.unwrap();
    }

    assert_eq!(dl.fired("o1").await, 1);
    assert_eq!(claimed, 1, "exactly one of the two claimed it");
    assert_eq!(dl.store.deadlines().await[0].status, DeadlineStatus::Fired);
}

#[tokio::test]
async fn an_ending_deadline_handler_ends_the_saga_and_cancels_its_other_deadlines() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 600)).await;
    dl.deliver(&Schedule {
        order_id: "o1".into(),
        name: "end-now".into(),
        seconds: 10,
    })
    .await;

    dl.advance(11);
    dl.scheduler().poll_once().await.unwrap();

    let saga = dl.saga("o1").await;
    assert!(saga.ended);
    assert_eq!(saga.ended_at_ms, Some(dt(dl.store.clock.now())));
    let deadlines = dl.deadlines_of(&saga.id).await;
    let by_name = |n: &str| deadlines.iter().find(|d| d.name == n).unwrap().status;
    assert_eq!(by_name("end-now"), DeadlineStatus::Fired);
    assert_eq!(
        by_name("charge-timeout"),
        DeadlineStatus::Cancelled,
        "ending the saga cancels its other deadlines"
    );
    assert_eq!(dl.metrics.ended(), ["dl.Charge"]);
}

#[tokio::test]
async fn a_deadline_handler_that_sends_commands_records_them_for_the_relay() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 60)).await;
    let saga_id = dl.saga("o1").await.id;
    dl.advance(61);

    dl.scheduler().poll_once().await.unwrap();

    let entries = dl.store.dispatch_of(&saga_id).await;
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.kind, DispatchKind::Command);
    assert_eq!(entry.type_name, "dl.CancelCharge");
    assert_eq!(entry.payload, r#"{"ref":"ref-o1"}"#);
    assert_eq!(entry.id, format!("{saga_id}:{}", entry.seq));
    assert_eq!(
        entry.seq,
        dl.saga("o1").await.version * 1000,
        "seq follows the saga's new version"
    );
    assert!(
        entry.dispatched_at_ms.is_none(),
        "recorded, not sent: the relay sends it"
    );
}

#[tokio::test]
async fn a_poll_reports_the_overdue_deadlines_it_could_not_fire() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 60)).await;
    dl.advance(61);
    dl.broken.store(true, Ordering::SeqCst);
    let scheduler = dl.scheduler();

    scheduler.poll_once().await.unwrap();
    assert_eq!(
        *dl.metrics.overdue.lock().unwrap(),
        std::collections::HashMap::from([("charge-timeout".to_string(), 1u64)])
    );

    dl.broken.store(false, Ordering::SeqCst);
    dl.advance(31);
    scheduler.poll_once().await.unwrap();
    assert!(dl.metrics.overdue.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_started_scheduler_fires_due_deadlines_and_stops_when_cancelled() {
    require_docker!();
    let dl = Dl::start().await;
    dl.deliver(&placed("o1", 60)).await;
    dl.advance(61);
    let cancel = CancellationToken::new();
    let task = dl.scheduler().start(cancel.clone());

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while dl.fired("o1").await == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the started scheduler never fired it"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dl.fired("o1").await, 1);
}
