//! The core over the event-store interface (ADR 0027), on the in-process event store double
//! (`support::event_store`): a broken subscription is subscribed again, a control action the event
//! store's capabilities do not cover is refused `UNSUPPORTED`, and the processor status names the
//! event store and its capabilities. No container: the message store is the in-memory one.

mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use snodra_messaging::config::DeadLetterMode;
use snodra_messaging::event_store::{Capability, EventStore, OutgoingEvent, SubscriptionSpec};
use snodra_messaging::events::{
    EventConsumer, LocalEventDispatcher, NoSagas, ProcessorSettings, RESUBSCRIBE_MIN,
};
use snodra_messaging::hub::HandlerRegistry;
use snodra_messaging::metrics::Metrics;
use snodra_messaging::proto::control_reply::Result as Answer;
use snodra_messaging::proto::control_request::Action;
use snodra_messaging::proto::{self, ControlRefusal, ControlRequest, ProcessorState, reset_target};
use snodra_messaging::store::MessageStore;
use snodra_messaging::store::memory::MemoryStore;
use snodra_messaging::{Config, Event, SnodraMessaging, message_type};
use support::event_store::Log;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Noted {
    note: String,
}
message_type!(Noted, "core.Noted");
impl Event for Noted {}

const SOURCE: &str = "core.events";

async fn publish(events: &Log, id: &str) {
    events
        .publish(&OutgoingEvent {
            source: SOURCE.into(),
            id: id.into(),
            type_name: "core.Unhandled".into(),
            key: Some(id.into()),
            headers: HashMap::from([
                ("type".to_string(), "core.Unhandled".to_string()),
                ("id".to_string(), id.to_string()),
            ]),
            payload: b"{}".to_vec(),
        })
        .await
        .unwrap();
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let started = std::time::Instant::now();
    while !check() {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A `Failed` from `next` ends the subscription (the `Subscription` contract): the processor
/// closes it, shows the error, subscribes again after a backoff and goes on from the committed
/// positions.
#[tokio::test]
async fn a_failed_subscription_is_subscribed_again_after_a_backoff() {
    let events = Log::full();
    let spec = SubscriptionSpec {
        name: "core.default".into(),
        source: SOURCE.into(),
    };
    // No handler for what is published: every record is acknowledged as skipped.
    let consumer = EventConsumer::processor(
        ProcessorSettings {
            name: "default".into(),
            subscription: spec.clone(),
            events: Arc::new(events.clone()),
            max_attempts: 1,
            backoff: Duration::from_millis(10),
            dead_letter_mode: DeadLetterMode::Log,
            dead_letter_retention: Duration::from_secs(60),
            dead_letters: None,
            metrics: Metrics::global(),
        },
        Arc::new(LocalEventDispatcher::new(
            None,
            Arc::new(HandlerRegistry::new()),
            Arc::new(NoSagas),
        )),
    )
    .unwrap();
    let monitor = consumer.monitor();
    let cancel = CancellationToken::new();
    let task = consumer.start(cancel.clone());
    for id in ["e-1", "e-2"] {
        publish(&events, id).await;
    }
    eventually("both acknowledged", || {
        monitor
            .status()
            .segments
            .iter()
            .map(|s| s.committed.parse::<u64>().unwrap_or(0))
            .sum::<u64>()
            == 2
    })
    .await;
    assert_eq!(events.subscriptions_of("core.default"), 1);

    events.fail_next(1);
    let broke = std::time::Instant::now();
    eventually("the error on the status", || {
        monitor
            .status()
            .last_error
            .contains("subscription: event store: the double broke")
    })
    .await;
    eventually("subscribed again", || {
        events.subscriptions_of("core.default") == 2
    })
    .await;
    assert!(
        broke.elapsed() >= RESUBSCRIBE_MIN - Duration::from_millis(50),
        "after the backoff: {:?}",
        broke.elapsed()
    );
    assert_eq!(events.members(&spec), 1, "the broken one was closed");
    for id in ["e-3", "e-4"] {
        publish(&events, id).await;
    }
    eventually("the later ones acknowledged too", || {
        monitor
            .status()
            .segments
            .iter()
            .map(|s| s.committed.parse::<u64>().unwrap_or(0))
            .sum::<u64>()
            == 4
    })
    .await;
    cancel.cancel();
    task.await.unwrap();
    assert_eq!(events.members(&spec), 0, "left at the stop");
}

fn request(action: Action) -> ControlRequest {
    ControlRequest {
        control_id: "c-1".into(),
        processor: "default".into(),
        action: Some(action),
    }
}

/// An event store without `replay`: a reset is refused `UNSUPPORTED` before anything changes
/// (no lock taken, the processor keeps its subscription), and the status says what the event
/// store offers.
#[tokio::test]
async fn control_action_without_capability_is_unsupported() {
    let store = MemoryStore::new();
    let mut config = Config::new("core", "static://127.0.0.1:1");
    config.events_source = SOURCE.into();
    config.outbox_enabled = false;
    let events = Log::new(vec![Capability::Lag]);
    let app = SnodraMessaging::builder(config)
        .await
        .unwrap()
        .store(Arc::new(store.clone()))
        .event_store(Arc::new(events.clone()))
        .event::<Noted, _, _>(|_, _| async { Ok(()) })
        .start()
        .await
        .unwrap();
    let spec = SubscriptionSpec {
        name: "core.default".into(),
        source: SOURCE.into(),
    };
    let running = |app: &SnodraMessaging| {
        app.processors().remove(0).state == ProcessorState::ProcessorRunning as i32
    };
    eventually("the processor reads its subscription", || {
        running(&app) && events.members(&spec) == 1
    })
    .await;
    let status = app.processors().remove(0);
    assert_eq!(status.event_store, "kit-double");
    assert_eq!(status.capabilities, ["lag"]);
    assert_eq!(status.subscription, "core.default");
    assert_eq!(status.source, SOURCE);

    let refused = |answer: Option<Answer>| match answer {
        Some(Answer::Refused(r)) => (ControlRefusal::try_from(r.code).unwrap(), r.message),
        other => panic!("not refused: {other:?}"),
    };
    let control = app.processor_control();
    for action in [
        Action::ResetPrepare(proto::ResetPrepare {
            operation_id: "op-1".into(),
            lease_ms: 30_000,
        }),
        Action::ResetExecute(proto::ResetExecute {
            operation_id: "op-1".into(),
            target: Some(proto::ResetTarget {
                to: Some(reset_target::To::Earliest(true)),
            }),
        }),
    ] {
        let (code, message) = refused(control.handle(request(action)).await.result);
        assert_eq!(code, ControlRefusal::Unsupported, "{message}");
        assert!(message.contains("replay"), "{message}");
    }
    // Nothing changed: still reading, no lock in the control document.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(running(&app), "{:?}", app.processors());
    assert_eq!(
        events.members(&spec),
        1,
        "the processor kept its subscription"
    );
    assert_eq!(app.processors().remove(0).operation_id, "");
    let control = store.processor_control().get("default").await.unwrap();
    assert_eq!(control.operation, None, "no lock: {control:?}");
    app.shutdown().await;
}

/// One processor's consumer on `events`, reading `spec`.
fn consumer_of(events: &Log, spec: &SubscriptionSpec) -> Arc<EventConsumer> {
    EventConsumer::processor(
        ProcessorSettings {
            name: "default".into(),
            subscription: spec.clone(),
            events: Arc::new(events.clone()),
            max_attempts: 1,
            backoff: Duration::from_millis(10),
            dead_letter_mode: DeadLetterMode::Log,
            dead_letter_retention: Duration::from_secs(60),
            dead_letters: None,
            metrics: Metrics::global(),
        },
        Arc::new(LocalEventDispatcher::new(
            None,
            Arc::new(HandlerRegistry::new()),
            Arc::new(NoSagas),
        )),
    )
    .unwrap()
}

/// More instances than segments on the in-memory event store: the instance that joined last
/// holds nothing, and its processor reads RUNNING (a member, idle), never STARTING, because a new
/// subscription announces its first assignment even when it is empty.
#[tokio::test]
async fn an_idle_newcomer_reads_running() {
    let events = Log::full();
    let spec = SubscriptionSpec {
        name: "core.idle".into(),
        source: SOURCE.into(),
    };
    let state = |m: &snodra_messaging::processors::ProcessorMonitor| {
        ProcessorState::try_from(m.status().state).unwrap()
    };
    let cancel = CancellationToken::new();
    let mut monitors = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..=support::event_store::SEGMENTS {
        let consumer = consumer_of(&events, &spec);
        let monitor = consumer.monitor();
        tasks.push(consumer.start(cancel.clone()));
        eventually("the newcomer reads RUNNING", || {
            state(&monitor) == ProcessorState::ProcessorRunning
        })
        .await;
        monitors.push(monitor);
    }
    eventually("every segment held by the first instances", || {
        monitors[..support::event_store::SEGMENTS]
            .iter()
            .all(|m| m.status().segments.len() == 1)
    })
    .await;
    let idle = monitors.last().unwrap();
    assert!(
        idle.status().segments.is_empty(),
        "the last one holds nothing"
    );
    assert_eq!(
        state(idle),
        ProcessorState::ProcessorRunning,
        "idle, running"
    );
    cancel.cancel();
    for task in tasks {
        task.await.unwrap();
    }
}
