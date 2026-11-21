//! Saga statistics: every counter and gauge of the Kotlin
//! meters moves on the matching action of a real runtime, scheduler, relay and idempotent adapter
//! on the in-memory message store. The Rust form of the Kotlin `SagaMetricsTest`.
#![cfg(feature = "testing")]

mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use snodra_messaging::events::{EventBus, LocalEventDispatcher, SagaStage};
use snodra_messaging::external::{ExternalCallSettings, ExternalCalls, IdempotencyKey};
use snodra_messaging::hub::{HandlerRegistry, SendOptions};
use snodra_messaging::saga::stats::{
    DEADLINES_DUE, EXTERNAL_CALLS, SAGA_COMMAND_FAILURES, SAGA_DISPATCH_PENDING, SAGA_EVENTS,
    SAGAS_ACTIVE, SAGAS_STARTED, SagaStats,
};
use snodra_messaging::saga::{
    DeadlineScheduler, DeadlineSettings, EventOutcome, ExternalOutcome, FailureReason,
    LocalRawCommandSender, RawCommandSender, RelaySettings, SagaDefinition, SagaDispatchRelay,
    SagaRetry, SagaRuntime, SagaType,
};
use snodra_messaging::{Command, Event, MessagingError, RetryPolicy, message_type};
use support::order_saga::{Bump, Finish, Gate, OrderPlaced, broken_saga, order_saga};
use support::saga::{Store, event_ctx, t0};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DlPlaced {
    order_id: String,
    seconds: u64,
}
message_type!(DlPlaced, "dl.Placed");
impl Event for DlPlaced {}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Charge {
    order_id: Option<String>,
    timed_out: bool,
}

fn charge_saga(broken: &Arc<AtomicBool>) -> SagaType {
    let broken = Arc::clone(broken);
    SagaDefinition::<Charge>::new("dl.Charge")
        .start_on::<DlPlaced>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, ctx| {
                s.order_id = Some(e.order_id.clone());
                ctx.schedule_after(
                    Duration::from_secs(e.seconds),
                    "charge-timeout",
                    None::<&()>,
                )?;
                Ok(())
            },
        )
        .on_deadline::<serde::de::IgnoredAny>("charge-timeout", move |s, _, _| {
            if broken.load(Ordering::SeqCst) {
                return Err(MessagingError::Storage("deadline handler broken".into()));
            }
            s.timed_out = true;
            Ok(())
        })
        .build()
        .unwrap()
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Print {
    job_id: String,
}
message_type!(Print, "st.Print");
impl Command for Print {
    type Reply = ();
}

struct St {
    store: Store,
    stats: SagaStats,
    gate: Arc<Gate>,
}

impl St {
    async fn start() -> St {
        let store = Store::start().await;
        let stats = SagaStats::new();
        stats.register_saga_types(["rt.Order", "dl.Charge"]);
        St {
            store,
            stats,
            gate: Gate::new(),
        }
    }

    fn runtime(&self, types: Vec<SagaType>) -> Arc<SagaRuntime> {
        Arc::new(SagaRuntime::new(
            types,
            self.store.ports(),
            self.store.clock.clock(),
            SagaRetry::new(RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_millis(2),
                multiplier: 1.0,
                max_backoff: Duration::from_millis(5),
            }),
            Arc::new(self.stats.clone()),
        ))
    }
}

async fn deliver<E: Event>(runtime: &SagaRuntime, event: &E) {
    let id = uuid::Uuid::new_v4().to_string();
    runtime
        .process(
            E::TYPE,
            &serde_json::to_vec(event).unwrap(),
            &event_ctx(&id, E::TYPE, &[]),
        )
        .await
        .unwrap();
}

fn placed(order_id: &str) -> OrderPlaced {
    OrderPlaced {
        order_id: order_id.into(),
    }
}

#[tokio::test]
async fn the_saga_lifecycle_and_event_outcomes_are_counted() {
    let st = St::start().await;
    let runtime = st.runtime(vec![order_saga(&st.gate)]);

    deliver(&runtime, &placed("o1")).await;
    // No instance and no start handler: a broken association shows up here.
    deliver(
        &runtime,
        &Bump {
            order_id: "nobody".into(),
            tag: "x".into(),
        },
    )
    .await;
    deliver(
        &runtime,
        &Finish {
            order_id: "o1".into(),
        },
    )
    .await;

    assert_eq!(st.stats.started("rt.Order"), 1);
    assert_eq!(st.stats.counter(SAGAS_STARTED, &[("type", "rt.Order")]), 1);
    assert_eq!(st.stats.ended("rt.Order"), 1);
    assert_eq!(st.stats.events("rt.Order", EventOutcome::Handled), 2);
    assert_eq!(
        st.stats.counter(
            SAGA_EVENTS,
            &[("type", "rt.Order"), ("outcome", "no_match")]
        ),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_version_conflict_and_a_parked_event_are_counted() {
    let st = St::start().await;
    let runtime = st.runtime(vec![order_saga(&st.gate)]);
    deliver(&runtime, &placed("o2")).await;
    st.gate.meet_in_pairs(2);

    let (a, b) = (Arc::clone(&runtime), Arc::clone(&runtime));
    let first = tokio::spawn(async move {
        deliver(
            &a,
            &Bump {
                order_id: "o2".into(),
                tag: "x".into(),
            },
        )
        .await
    });
    let second = tokio::spawn(async move {
        deliver(
            &b,
            &Bump {
                order_id: "o2".into(),
                tag: "y".into(),
            },
        )
        .await
    });
    first.await.unwrap();
    second.await.unwrap();
    assert!(st.stats.events("rt.Order", EventOutcome::Conflict) >= 1);

    let broken = st.runtime(vec![broken_saga(&st.gate)]);
    deliver(&broken, &placed("o3")).await;
    assert_eq!(
        st.stats
            .counter(SAGA_EVENTS, &[("type", "rt.Broken"), ("outcome", "parked")]),
        1
    );
}

#[tokio::test]
async fn deadlines_fired_and_overdue_are_counted() {
    let st = St::start().await;
    let switch = Arc::new(AtomicBool::new(false));
    let runtime = st.runtime(vec![charge_saga(&switch)]);
    deliver(
        &runtime,
        &DlPlaced {
            order_id: "d1".into(),
            seconds: 60,
        },
    )
    .await;
    let scheduler = DeadlineScheduler::new(
        st.store.message_store().sagas().deadlines,
        Arc::clone(&runtime),
        st.store.clock.clock(),
        DeadlineSettings {
            poll_interval: Duration::from_millis(50),
            lease: Duration::from_secs(30),
            ..DeadlineSettings::default()
        },
        Arc::new(st.stats.clone()),
    )
    .unwrap();

    st.store.clock.set(t0() + Duration::from_secs(61));
    switch.store(true, Ordering::SeqCst);
    scheduler.poll_once().await.unwrap();
    assert_eq!(
        st.stats.gauge(DEADLINES_DUE, &[("name", "charge-timeout")]),
        Some(1),
        "overdue and not fired"
    );
    assert_eq!(st.stats.deadlines_fired("charge-timeout"), 0);

    switch.store(false, Ordering::SeqCst);
    st.store.clock.advance(Duration::from_secs(31)); // the lease of the failed attempt expired
    scheduler.poll_once().await.unwrap();
    assert_eq!(st.stats.deadlines_fired("charge-timeout"), 1);
    assert_eq!(
        st.stats.deadlines_due("charge-timeout"),
        Some(0),
        "the gauge goes back to zero when nothing is overdue"
    );
}

#[tokio::test]
async fn the_pending_dispatch_and_active_saga_gauges_follow_the_stored_state() {
    let st = St::start().await;
    let runtime = st.runtime(vec![order_saga(&st.gate)]);
    assert_eq!(
        st.stats.active("rt.Order"),
        Some(0),
        "a known saga type has a gauge before its first instance"
    );

    deliver(&runtime, &placed("o4")).await; // records a command and an event
    deliver(&runtime, &placed("o5")).await;
    st.stats
        .sample_once(
            &*st.store.message_store().sagas().sagas,
            &*st.store.message_store().sagas().dispatch,
        )
        .await
        .unwrap();
    assert_eq!(
        st.stats.gauge(SAGAS_ACTIVE, &[("type", "rt.Order")]),
        Some(2)
    );
    assert_eq!(st.stats.gauge(SAGA_DISPATCH_PENDING, &[]), Some(4));

    deliver(
        &runtime,
        &Finish {
            order_id: "o4".into(),
        },
    )
    .await;
    let at = snodra_messaging::time::epoch_ms(t0());
    st.store.edit(|c| {
        c.dispatch
            .iter_mut()
            .for_each(|e| e.dispatched_at_ms = Some(at))
    });
    st.stats
        .sample_once(
            &*st.store.message_store().sagas().sagas,
            &*st.store.message_store().sagas().dispatch,
        )
        .await
        .unwrap();
    assert_eq!(st.stats.active("rt.Order"), Some(1));
    assert_eq!(
        st.stats.dispatch_pending(),
        Some(0),
        "dispatched entries are not pending"
    );

    // The sampler task samples at once and keeps going until cancelled.
    st.store.edit(|c| {
        c.dispatch
            .iter_mut()
            .for_each(|e| e.dispatched_at_ms = None)
    });
    let cancel = tokio_util::sync::CancellationToken::new();
    let sampler = st.stats.start_sampler(
        st.store.message_store().sagas().sagas,
        st.store.message_store().sagas().dispatch,
        Duration::from_millis(20),
        cancel.clone(),
    );
    let started = std::time::Instant::now();
    while st.stats.dispatch_pending() != Some(4) {
        assert!(started.elapsed() < Duration::from_secs(10), "no sample");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cancel.cancel();
    sampler.await.unwrap();
}

#[tokio::test]
async fn an_unhandled_command_failure_is_counted_by_command_type_and_reason() {
    let st = St::start().await;
    let runtime = st.runtime(vec![order_saga(&st.gate)]);
    let registry = Arc::new(HandlerRegistry::new()); // nobody handles rt.DoIt
    let relay = SagaDispatchRelay::new(
        st.store.message_store().tx(),
        st.store.message_store().sagas().dispatch,
        Arc::new(LocalRawCommandSender::new(Arc::clone(&registry))),
        EventBus::over(st.store.message_store().outbox()),
        Arc::new(LocalEventDispatcher::new(
            Some(st.store.message_store()),
            registry,
            runtime.clone(),
        )),
        st.store.clock.clock(),
        RelaySettings {
            poll_interval: Duration::from_millis(50),
            max_attempts: 2,
            ..RelaySettings::default()
        },
        Arc::new(st.stats.clone()),
    )
    .unwrap();
    deliver(&runtime, &placed("o6")).await;

    relay.poll_once().await.unwrap();

    assert_eq!(
        st.stats.counter(
            SAGA_COMMAND_FAILURES,
            &[("type", "rt.DoIt"), ("reason", "NO_HANDLER")]
        ),
        1
    );
    assert_eq!(
        st.stats
            .command_failures("rt.DoIt", FailureReason::NoHandler),
        1
    );
}

#[tokio::test]
async fn adapter_calls_are_counted_by_handler_and_outcome() {
    let st = St::start().await;
    let calls = ExternalCalls::new(
        st.store.message_store().external_calls(),
        st.store.clock.clock(),
        ExternalCallSettings::default(),
        Arc::new(st.stats.clone()),
    )
    .unwrap();
    let mut registry = HandlerRegistry::new();
    registry.idempotent_command::<Print, _, _>(
        &calls.handle(),
        IdempotencyKey::default(),
        false,
        |_, _| async { Ok(()) },
    );
    let sender = LocalRawCommandSender::new(Arc::new(registry));
    let options = || SendOptions {
        headers: HashMap::from([("idempotency-key".to_string(), "k-1".to_string())]),
        ..SendOptions::default()
    };

    for _ in 0..2 {
        sender
            .send_raw("st.Print", br#"{"jobId":"j1"}"#.to_vec(), options())
            .await;
    }

    assert_eq!(
        st.stats.counter(
            EXTERNAL_CALLS,
            &[("handler", "st.Print"), ("outcome", "succeeded")]
        ),
        1
    );
    assert_eq!(
        st.stats
            .external_calls("st.Print", ExternalOutcome::Replayed),
        1
    );
}
