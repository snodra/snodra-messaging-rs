//! The saga runtime (S2 Task 5, ADR 0021 rulings 1, 3-6, 25, 26, 27, 33, 35, 37): one transaction
//! per (event, saga type) with an exactly-once marker, association routing, start keys,
//! version-checked writes, dispatch entries, deadlines and parking. The Rust form of the Kotlin
//! `SagaRuntimeTest`, on the in-memory message store and its transactions (no container).
#![cfg(feature = "testing")]

mod support;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use snodra_messaging::events::SagaStage;
use snodra_messaging::saga::store::{Association, DeadlineStatus, DispatchKind};
use snodra_messaging::saga::{
    EventOutcome, FailureReason, SagaCommandFailed, SagaDefinition, SagaRetry, SagaRuntime,
    SagaType,
};
use snodra_messaging::{Event, MessageType, RetryPolicy};
use support::order_saga::*;
use support::saga::{Store, event_ctx, state, t0};

fn quick_retry() -> SagaRetry {
    SagaRetry::new(RetryPolicy {
        max_attempts: 5,
        initial_backoff: Duration::from_millis(5),
        multiplier: 1.0,
        max_backoff: Duration::from_millis(20),
    })
}

struct Rt {
    store: Store,
    metrics: Arc<RecordingMetrics>,
    gate: Arc<Gate>,
    runtime: Arc<SagaRuntime>,
}

impl Rt {
    async fn start() -> Rt {
        Rt::on(Store::start().await)
    }

    fn on(store: Store) -> Rt {
        let gate = Gate::new();
        let metrics = RecordingMetrics::new();
        let runtime = Self::build(
            &store,
            vec![order_saga(&gate), header_saga()],
            &metrics,
            quick_retry(),
        );
        Rt {
            store,
            metrics,
            gate,
            runtime,
        }
    }

    fn build(
        store: &Store,
        types: Vec<SagaType>,
        metrics: &Arc<RecordingMetrics>,
        retry: SagaRetry,
    ) -> Arc<SagaRuntime> {
        Arc::new(SagaRuntime::new(
            types,
            store.ports(),
            store.clock.clock(),
            retry,
            metrics.clone(),
        ))
    }

    fn runtime_of(&self, types: Vec<SagaType>) -> Arc<SagaRuntime> {
        Self::build(&self.store, types, &self.metrics, quick_retry())
    }

    async fn deliver<E: Event>(&self, event: &E) -> bool {
        deliver_to(&self.runtime, event, &uuid::Uuid::new_v4().to_string(), &[]).await
    }

    async fn deliver_as<E: Event>(&self, event: &E, id: &str, headers: &[(&str, &str)]) -> bool {
        deliver_to(&self.runtime, event, id, headers).await
    }

    async fn saga(&self, order_id: &str) -> snodra_messaging::saga::store::SagaDocument {
        let mut found: Vec<_> = self
            .store
            .sagas("rt.Order")
            .await
            .into_iter()
            .filter(|s| state(s)["orderId"] == order_id)
            .collect();
        assert_eq!(found.len(), 1, "one instance of order {order_id}");
        found.remove(0)
    }

    async fn log_of(&self, order_id: &str) -> Vec<String> {
        let saga = self.saga(order_id).await;
        state(&saga)["log"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    }
}

async fn deliver_to<E: Event>(
    runtime: &SagaRuntime,
    event: &E,
    id: &str,
    headers: &[(&str, &str)],
) -> bool {
    let payload = serde_json::to_vec(event).unwrap();
    runtime
        .process(E::TYPE, &payload, &event_ctx(id, E::TYPE, headers))
        .await
        .unwrap()
}

fn placed(order_id: &str) -> OrderPlaced {
    OrderPlaced {
        order_id: order_id.into(),
    }
}

fn bump(order_id: &str, tag: &str) -> Bump {
    Bump {
        order_id: order_id.into(),
        tag: tag.into(),
    }
}

/// Epoch milliseconds.
fn dt(t: std::time::SystemTime) -> i64 {
    snodra_messaging::time::epoch_ms(t)
}

#[tokio::test]
async fn state_and_dispatched_payloads_keep_a_decimal_text_and_map_keys_with_dots_or_a_leading_dollar_exactly()
 {
    let rt = Rt::start().await;
    let priced = rt.runtime_of(vec![price_saga()]);
    let tags = std::collections::BTreeMap::from([
        ("a.b".to_string(), "x".to_string()),
        ("$c".to_string(), "y".to_string()),
    ]);
    deliver_to(
        &priced,
        &Priced {
            order_id: "p1".into(),
            amount: "12.30".into(),
            tags: tags.clone(),
        },
        "e-p1",
        &[],
    )
    .await;
    deliver_to(
        &priced,
        &PriceCheck {
            order_id: "p1".into(),
        },
        "e-p2",
        &[],
    )
    .await;

    let doc = rt.store.sagas("rt.Price").await.remove(0);
    let sent = rt.store.dispatch_of(&doc.id).await;
    assert_eq!(sent.len(), 2);
    for entry in &sent {
        assert!(
            entry.payload.contains(r#""amount":"12.30""#),
            "the amount keeps its text in the dispatched command: {}",
            entry.payload
        );
        assert!(
            entry.payload.contains(r#""a.b":"x""#) && entry.payload.contains(r#""$c":"y""#),
            "the map keys survive in the dispatched command: {}",
            entry.payload
        );
    }
    // The stored text is the codec's exact output: reading and writing it again changes nothing.
    let back: Price = serde_json::from_str(&doc.state).unwrap();
    assert_eq!(serde_json::to_string(&back).unwrap(), doc.state);
    assert_eq!(back.amount, "12.30");
    assert_eq!(back.tags, tags);
}

#[tokio::test]
async fn a_start_event_creates_an_instance_with_its_association_and_state() {
    let rt = Rt::start().await;

    assert!(rt.deliver(&placed("o1")).await);

    let doc = rt.saga("o1").await;
    assert_eq!(doc.start_key.as_deref(), Some("rt.Order|orderId=o1"));
    let mut associations = doc.associations.clone();
    associations.sort_by(|a, b| a.k.cmp(&b.k));
    assert_eq!(
        associations,
        [
            Association::new("orderId", "o1"),
            Association::new("paymentRef", "pay-o1")
        ]
    );
    assert_eq!(state(&doc)["placed"], 1);
    assert!(!doc.ended);
    assert_eq!(doc.version, 0);
    assert_eq!(doc.created_at_ms, dt(t0()));
    assert_eq!(rt.metrics.started(), ["rt.Order"]);
    assert_eq!(rt.metrics.count("rt.Order", EventOutcome::Handled), 1);
}

#[tokio::test]
async fn a_later_event_is_routed_by_association_to_that_instance() {
    let rt = Rt::start().await;
    rt.deliver(&placed("o2")).await;
    rt.deliver(&bump("o2", "a")).await;

    let doc = rt.saga("o2").await;
    assert_eq!(rt.log_of("o2").await, ["a"]);
    assert_eq!(doc.version, 1, "every handled event bumps the version");
    assert_eq!(rt.store.sagas("rt.Order").await.len(), 1);
}

#[tokio::test]
async fn an_event_that_matches_an_instance_and_a_start_handler_is_handled_once_by_the_instance() {
    let rt = Rt::start().await;
    rt.deliver(&placed("o3")).await;
    rt.deliver(&placed("o3")).await;

    let doc = rt.saga("o3").await;
    assert_eq!(
        rt.store.sagas("rt.Order").await.len(),
        1,
        "no second instance"
    );
    assert_eq!(
        state(&doc)["placed"],
        2,
        "the existing instance handled the second start event, once"
    );
    assert_eq!(rt.metrics.started().len(), 1);
}

#[tokio::test]
async fn force_new_always_creates_a_new_instance_without_a_start_key() {
    let rt = Rt::start().await;
    rt.deliver(&Forced {
        order_id: "o4".into(),
    })
    .await;
    rt.deliver(&Forced {
        order_id: "o4".into(),
    })
    .await;

    let all = rt.store.sagas("rt.Order").await;
    assert_eq!(all.len(), 2);
    assert!(all.iter().all(|s| s.start_key.is_none()));
    let forced: u64 = all
        .iter()
        .map(|s| state(s)["forced"].as_u64().unwrap())
        .sum();
    assert_eq!(
        forced, 3,
        "the first instance handled both, the second only its own"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_start_events_for_one_key_end_in_one_instance_that_handled_both() {
    two_start_events_race(Rt::start().await).await;
}

/// The same race on the MongoDB add-on: its write conflicts, transient transaction errors and the
/// partial unique start-key index under two live transactions become conflicts the runtime retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_start_events_for_one_key_end_in_one_instance_on_mongodb() {
    if !support::docker_available() {
        eprintln!("docker not available, skipping");
        return;
    }
    two_start_events_race(Rt::on(Store::start_mongo().await)).await;
}

async fn two_start_events_race(rt: Rt) {
    rt.gate.meet_in_pairs(2);

    let (a, b) = (Arc::clone(&rt.runtime), Arc::clone(&rt.runtime));
    let first = tokio::spawn(async move { deliver_to(&a, &placed("o5"), "e-5a", &[]).await });
    let second = tokio::spawn(async move { deliver_to(&b, &placed("o5"), "e-5b", &[]).await });
    first.await.unwrap();
    second.await.unwrap();

    let doc = rt.saga("o5").await;
    assert_eq!(rt.store.sagas("rt.Order").await.len(), 1);
    assert_eq!(state(&doc)["placed"], 2);
    assert!(
        rt.metrics.count("rt.Order", EventOutcome::Conflict) >= 1,
        "the loser conflicted and retried"
    );
    assert_eq!(rt.store.failures().await.len(), 0);
    assert_eq!(rt.metrics.started().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_events_for_one_instance_never_interleave_and_both_apply() {
    two_events_race(Rt::start().await).await;
}

/// The same race on the MongoDB add-on (a write conflict on the saga's document).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_events_for_one_instance_never_interleave_on_mongodb() {
    if !support::docker_available() {
        eprintln!("docker not available, skipping");
        return;
    }
    two_events_race(Rt::on(Store::start_mongo().await)).await;
}

async fn two_events_race(rt: Rt) {
    rt.deliver(&placed("o6")).await;
    rt.gate.meet_in_pairs(2);

    let (a, b) = (Arc::clone(&rt.runtime), Arc::clone(&rt.runtime));
    let first = tokio::spawn(async move { deliver_to(&a, &bump("o6", "x"), "e-6x", &[]).await });
    let second = tokio::spawn(async move { deliver_to(&b, &bump("o6", "y"), "e-6y", &[]).await });
    first.await.unwrap();
    second.await.unwrap();

    let doc = rt.saga("o6").await;
    let mut log = rt.log_of("o6").await;
    log.sort();
    assert_eq!(log, ["x", "y"], "neither lost nor applied twice");
    assert_eq!(doc.version, 2);
    assert!(rt.metrics.count("rt.Order", EventOutcome::Conflict) >= 1);
    let commands = rt
        .store
        .dispatch_of(&doc.id)
        .await
        .into_iter()
        .filter(|e| e.type_name == "rt.DoIt" && !e.payload.contains(r#""o6""#))
        .count();
    assert_eq!(commands, 2, "one command per applied bump");
}

#[tokio::test]
async fn associate_with_and_remove_association_with_change_routing_for_later_events() {
    let rt = Rt::start().await;
    rt.deliver(&placed("o7")).await;
    let by_ref = |tag: &str| ByRef {
        r#ref: "r7".into(),
        tag: tag.into(),
    };
    rt.deliver(&by_ref("before")).await;
    rt.deliver(&assoc("o7", "ref", "r7", false)).await;
    rt.deliver(&by_ref("while")).await;
    rt.deliver(&assoc("o7", "ref", "r7", true)).await;
    rt.deliver(&by_ref("after")).await;

    assert_eq!(rt.log_of("o7").await, ["while"]);
    let mut associations = rt.saga("o7").await.associations;
    associations.sort_by(|a, b| a.k.cmp(&b.k));
    assert_eq!(
        associations,
        [
            Association::new("orderId", "o7"),
            Association::new("paymentRef", "pay-o7")
        ]
    );
}

#[tokio::test]
async fn an_ending_handler_ends_the_instance_and_cancels_its_scheduled_deadlines() {
    let rt = Rt::start().await;
    rt.deliver(&placed("o8")).await;
    let scheduled = rt
        .store
        .deadlines()
        .await
        .iter()
        .filter(|d| d.status == DeadlineStatus::Scheduled)
        .count();
    assert_eq!(scheduled, 2);

    rt.deliver(&PaymentTaken {
        payment_ref: "pay-o8".into(),
    })
    .await;

    let doc = rt.saga("o8").await;
    assert!(doc.ended);
    assert_eq!(doc.ended_at_ms, Some(dt(t0())));
    assert_eq!(rt.log_of("o8").await, ["paid"]);
    assert!(
        rt.store.deadlines().await.iter().all(|d| {
            d.status == DeadlineStatus::Cancelled && d.finished_at_ms == Some(dt(t0()))
        })
    );
    assert_eq!(rt.metrics.ended(), ["rt.Order"]);
}

#[tokio::test]
async fn end_from_code_ends_the_instance() {
    let rt = Rt::start().await;
    rt.deliver(&placed("o9")).await;
    rt.deliver(&Finish {
        order_id: "o9".into(),
    })
    .await;

    assert!(rt.saga("o9").await.ended);
    rt.deliver(&bump("o9", "late")).await; // an ended instance routes nothing
    assert_eq!(rt.log_of("o9").await, Vec::<String>::new());
}

#[tokio::test]
async fn commands_and_events_are_recorded_in_saga_dispatch_in_call_order_and_not_sent() {
    let rt = Rt::start().await;
    rt.deliver_as(&placed("o10"), "e-10", &[]).await;
    rt.deliver(&bump("o10", "a")).await;

    let doc = rt.saga("o10").await;
    let entries = rt.store.dispatch_of(&doc.id).await;
    assert_eq!(
        entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
        [0, 1, 1000]
    );
    assert_eq!(
        entries.iter().map(|e| e.kind).collect::<Vec<_>>(),
        [
            DispatchKind::Command,
            DispatchKind::Event,
            DispatchKind::Command
        ]
    );
    assert_eq!(
        entries
            .iter()
            .map(|e| e.type_name.as_str())
            .collect::<Vec<_>>(),
        ["rt.DoIt", "rt.Done", "rt.DoIt"]
    );
    assert_eq!(
        entries[0].id,
        format!("{}:0", doc.id),
        "the id is the idempotency key"
    );
    assert!(entries[0].payload.contains(r#""orderId":"o10""#));
    assert_eq!(entries[1].routing_key.as_deref(), Some("k-o10"));
    assert!(entries[2].payload.contains(r#""orderId":"o10-a""#));
    assert!(entries.iter().all(|e| {
        e.dispatched_at_ms.is_none()
            && e.failed_at_ms.is_none()
            && e.attempts == 0
            && e.created_at_ms == dt(t0())
    }));
    assert_eq!(
        entries[0].headers.get("correlation-id").map(String::as_str),
        Some("corr-e-10")
    );
}

#[tokio::test]
async fn deadlines_are_recorded_with_due_time_payload_and_payload_type() {
    let rt = Rt::start().await;
    rt.deliver(&placed("o11")).await;

    let deadlines = rt.store.deadlines().await;
    let saga_id = rt.saga("o11").await.id;
    let timeout = deadlines.iter().find(|d| d.name == "timeout").unwrap();
    assert_eq!(timeout.saga_id, saga_id);
    assert_eq!(timeout.saga_type, "rt.Order");
    assert_eq!(timeout.due_at_ms, dt(t0() + Duration::from_secs(600)));
    assert_eq!(timeout.payload.as_deref(), Some(r#""p-o11""#));
    assert_eq!(timeout.status, DeadlineStatus::Scheduled);
    assert_eq!(
        timeout
            .correlation_id
            .as_deref()
            .map(|c| c.starts_with("corr-")),
        Some(true)
    );
    let nudge = deadlines.iter().find(|d| d.name == "nudge").unwrap();
    assert_eq!(nudge.due_at_ms, dt(t0() + Duration::from_secs(2 * 3600)));
    assert_eq!(nudge.payload.as_deref(), Some(r#"{"orderId":"o11"}"#));
    assert!(nudge.payload_type.as_deref().unwrap().contains("Nudge"));
}

#[tokio::test]
async fn a_handler_error_rolls_back_the_state_the_dispatch_entries_and_the_deadlines_of_that_attempt()
 {
    let rt = Rt::start().await;
    rt.gate.fail_first.store(1, Ordering::SeqCst); // the first attempt fails after it sent, published and scheduled

    rt.deliver(&placed("o12")).await;

    let doc = rt.saga("o12").await;
    assert_eq!(rt.store.sagas("rt.Order").await.len(), 1);
    assert_eq!(state(&doc)["placed"], 1);
    let types: Vec<_> = rt
        .store
        .dispatch_of(&doc.id)
        .await
        .into_iter()
        .map(|e| e.type_name)
        .collect();
    assert_eq!(
        types,
        ["rt.DoIt", "rt.Done"],
        "the failed attempt left no entries"
    );
    assert_eq!(rt.store.deadlines().await.len(), 2, "nor deadlines");
    assert_eq!(rt.store.failures().await.len(), 0);
    assert_eq!(rt.metrics.count("rt.Order", EventOutcome::Handled), 1);
    assert_eq!(rt.metrics.count("rt.Order", EventOutcome::Parked), 0);
}

#[tokio::test]
async fn a_redelivered_event_is_skipped_by_its_marker() {
    let rt = Rt::start().await;
    rt.deliver(&placed("o13")).await;
    rt.deliver_as(&bump("o13", "a"), "bump-1", &[]).await;
    assert!(
        rt.deliver_as(&bump("o13", "a"), "bump-1", &[]).await,
        "an event a saga type already applied counts as applied"
    );

    let doc = rt.saga("o13").await;
    assert_eq!(rt.log_of("o13").await, ["a"]);
    assert_eq!(doc.version, 1);
    assert!(rt.store.marker("saga:rt.Order:bump-1").await);
    let commands = rt
        .store
        .dispatch_of(&doc.id)
        .await
        .into_iter()
        .filter(|e| e.type_name == "rt.DoIt")
        .count();
    assert_eq!(
        commands, 2,
        "start command and one bump command, no second bump"
    );
}

#[tokio::test]
async fn an_event_no_instance_matches_and_no_start_handler_takes_is_counted_no_match() {
    let rt = Rt::start().await;
    assert!(!rt.deliver_as(&bump("nobody", "a"), "lost-1", &[]).await);
    rt.deliver(&ByRef {
        r#ref: "unknown-ref".into(),
        tag: "a".into(),
    })
    .await;

    assert_eq!(rt.store.sagas("rt.Order").await.len(), 0);
    assert_eq!(rt.metrics.count("rt.Order", EventOutcome::NoMatch), 2);
    assert_eq!(rt.store.failures().await.len(), 0);
    assert!(
        rt.store.marker("saga:rt.Order:lost-1").await,
        "a no-match event is final: its marker is written"
    );
}

#[tokio::test]
async fn a_saga_type_that_fails_on_every_attempt_is_parked_while_another_saga_type_handles_the_same_event()
 {
    let rt = Rt::start().await;
    let both = rt.runtime_of(vec![order_saga(&rt.gate), broken_saga(&rt.gate)]);

    deliver_to(&both, &placed("o14"), "e-14", &[("userId", "u-1")]).await;

    assert_eq!(
        rt.store.sagas("rt.Order").await.len(),
        1,
        "the healthy type handled it"
    );
    assert_eq!(rt.store.sagas("rt.Broken").await.len(), 0);
    let failures = rt.store.failures().await;
    assert_eq!(failures.len(), 1);
    let failure = &failures[0];
    assert_eq!(failure.id, "rt.Broken:e-14");
    assert_eq!(failure.saga_type, "rt.Broken");
    assert_eq!(failure.event_id, "e-14");
    assert_eq!(failure.event_type, "rt.OrderPlaced");
    assert_eq!(failure.payload, r#"{"orderId":"o14"}"#);
    assert_eq!(
        failure.headers.get("userId").map(String::as_str),
        Some("u-1")
    );
    assert_eq!(
        failure.headers.get("correlationId").map(String::as_str),
        Some("corr-e-14")
    );
    assert_eq!(
        failure.attempts, 2,
        "a handler error gets two quick attempts, not the conflict backoff"
    );
    assert!(failure.error.contains("broken on o14"), "{}", failure.error);
    assert_eq!(failure.first_failed_at_ms, dt(t0()));
    assert_eq!(failure.last_failed_at_ms, dt(t0()));
    assert!(
        rt.store.marker("saga:rt.Broken:e-14").await && rt.store.marker("saga:rt.Order:e-14").await,
        "parking writes the marker with the failure"
    );
    assert_eq!(rt.metrics.count("rt.Broken", EventOutcome::Parked), 1);
    assert_eq!(rt.metrics.count("rt.Order", EventOutcome::Handled), 1);

    deliver_to(&both, &placed("o14"), "e-14", &[]).await; // redelivered: both markers hold
    assert_eq!(rt.store.failures().await.len(), 1);
    assert_eq!(state(&rt.saga("o14").await)["placed"], 1);
}

#[tokio::test]
async fn removing_the_start_association_frees_the_start_key_so_a_later_start_event_creates_a_new_instance()
 {
    let rt = Rt::start().await;
    rt.deliver(&placed("o31")).await;
    assert_eq!(
        rt.saga("o31").await.start_key.as_deref(),
        Some("rt.Order|orderId=o31")
    );

    // The start association (key orderId, value o31) goes.
    rt.deliver(&assoc("o31", "orderId", "o31", true)).await;
    assert_eq!(
        rt.store.sagas("rt.Order").await[0].start_key,
        None,
        "the start key is released with its association"
    );

    // No live instance is associated with orderId=o31 any more.
    rt.deliver(&placed("o31")).await;

    let all = rt.store.sagas("rt.Order").await;
    assert_eq!(
        all.len(),
        2,
        "a second instance started under the freed key"
    );
    assert_eq!(
        all.iter()
            .filter(|s| s.start_key.as_deref() == Some("rt.Order|orderId=o31"))
            .count(),
        1
    );
    assert_eq!(
        rt.store.failures().await.len(),
        0,
        "nothing was parked on a duplicate start key"
    );
}

#[tokio::test]
async fn a_handler_error_parks_within_well_under_a_second_after_two_attempts() {
    let rt = Rt::start().await;
    // The real policy: 5 attempts, 200 ms doubling.
    let broken = Rt::build(
        &rt.store,
        vec![broken_saga(&rt.gate)],
        &rt.metrics,
        SagaRetry::default(),
    );

    let started = Instant::now();
    deliver_to(&broken, &placed("o30"), "e-30", &[]).await;
    let took = started.elapsed();

    assert!(
        took < Duration::from_millis(1000),
        "parking took {took:?}: a handler error must not back off like a conflict"
    );
    assert_eq!(rt.gate.broken_calls.load(Ordering::SeqCst), 2);
    assert_eq!(rt.store.failures().await.len(), 1);
}

#[tokio::test]
async fn the_handler_gets_its_saga_id_and_message_context() {
    let rt = Rt::start().await;
    rt.deliver_as(&placed("o15"), "e-15", &[]).await;

    let doc = rt.saga("o15").await;
    let s = state(&doc);
    assert_eq!(s["sawSagaId"], doc.id);
    assert_eq!(s["sawMessageId"], "e-15");
    assert_eq!(s["sawCorrelationId"], "corr-e-15");
    assert_eq!(
        rt.store.dispatch_of(&doc.id).await.len(),
        2,
        "commands recorded"
    );
    assert_eq!(rt.store.deadlines().await.len(), 2, "deadlines recorded");
}

#[tokio::test]
async fn handlers_run_inside_a_span_with_saga_type_saga_id_and_correlation_id() {
    let rt = Rt::start().await;
    let logging = SagaDefinition::<Header>::new("rt.Logging")
        .start_on::<Note>(
            "x-order",
            |e| Some(e.text.clone()),
            |_, _, _| {
                tracing::info!("inside the saga handler");
                Ok(())
            },
        )
        .build()
        .unwrap();
    let runtime = rt.runtime_of(vec![logging]);
    let captured = support::logs::Captured::default();
    let _guard = captured.install();

    deliver_to(&runtime, &Note { text: "o16".into() }, "e-16", &[]).await;

    let saga_id = rt.store.sagas("rt.Logging").await.remove(0).id;
    let line = captured
        .lines()
        .into_iter()
        .find(|l| l.contains("inside the saga handler"))
        .unwrap_or_else(|| panic!("no log line from the handler: {}", captured.text()));
    for field in [
        "saga_type=rt.Logging".to_string(),
        format!("saga_id={saga_id}"),
        "correlation_id=corr-e-16".to_string(),
    ] {
        assert!(line.contains(&field), "{field} missing in: {line}");
    }
}

#[tokio::test]
async fn an_association_header_routes_by_the_message_header() {
    let rt = Rt::start().await;
    let note = |text: &str| Note { text: text.into() };
    rt.deliver_as(&note("one"), "n1", &[("x-order", "h1")])
        .await;
    rt.deliver_as(&note("two"), "n2", &[("x-order", "h1")])
        .await;
    rt.deliver_as(&note("three"), "n3", &[("x-order", "h2")])
        .await;
    rt.deliver_as(&note("no header"), "n4", &[]).await;

    let mut docs = rt.store.sagas("rt.Header").await;
    docs.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    assert_eq!(
        docs.iter()
            .map(|d| d.start_key.clone().unwrap())
            .collect::<Vec<_>>(),
        ["rt.Header|x-order=h1", "rt.Header|x-order=h2"]
    );
    assert_eq!(
        docs.iter()
            .map(|d| state(d)["notes"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert_eq!(docs[0].associations, [Association::new("x-order", "h1")]);
    assert_eq!(rt.metrics.count("rt.Header", EventOutcome::NoMatch), 1);
}

/// Handles `SagaCommandFailed` but never associates itself with its own id.
#[derive(Default, Serialize, Deserialize)]
struct Quiet {
    order_id: Option<String>,
    failures: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct QuietStart {
    order_id: String,
}
snodra_messaging::message_type!(QuietStart, "rt.QuietStart");
impl Event for QuietStart {}

fn quiet_saga() -> SagaType {
    SagaDefinition::<Quiet>::new("rt.Quiet")
        .start_on::<QuietStart>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, _| {
                s.order_id = Some(e.order_id.clone());
                Ok(())
            },
        )
        .on_command_failed(|s, e, _| {
            s.failures
                .push(format!("{}:{:?}", e.command_type, e.reason));
            Ok(())
        })
        .build()
        .unwrap()
}

fn failed(saga_id: &str, key: &str) -> SagaCommandFailed {
    SagaCommandFailed {
        saga_id: saga_id.into(),
        command_type: "rt.Nobody".into(),
        idempotency_key: key.into(),
        reason: FailureReason::NoHandler,
        message: "nobody handles it".into(),
    }
}

#[tokio::test]
async fn saga_command_failed_reaches_its_saga_by_saga_id_without_associating() {
    let rt = Rt::start().await;
    let runtime = rt.runtime_of(vec![quiet_saga()]);
    deliver_to(
        &runtime,
        &QuietStart {
            order_id: "q1".into(),
        },
        "e-q1",
        &[],
    )
    .await;
    let doc = rt.store.sagas("rt.Quiet").await.remove(0);
    assert_eq!(
        doc.associations,
        [Association::new("orderId", "q1")],
        "the saga never associated with its own id"
    );

    let applied = runtime
        .deliver_command_failed(
            &failed(&doc.id, &format!("{}:9001", doc.id)),
            &event_ctx("failed:x", SagaCommandFailed::TYPE, &[]),
        )
        .await
        .unwrap();

    assert!(applied);
    let doc = rt.store.sagas("rt.Quiet").await.remove(0);
    assert_eq!(
        state(&doc)["failures"],
        serde_json::json!(["rt.Nobody:NoHandler"])
    );
}

#[tokio::test]
async fn saga_command_failed_for_an_ended_saga_or_a_type_without_a_handler_is_not_applied() {
    let rt = Rt::start().await;
    // rt.Order has no command-failed handler; rt.Quiet has one but its instance ends.
    let runtime = rt.runtime_of(vec![order_saga(&rt.gate), quiet_saga()]);
    deliver_to(&runtime, &placed("o20"), "e-20", &[]).await;
    deliver_to(
        &runtime,
        &QuietStart {
            order_id: "q2".into(),
        },
        "e-q2",
        &[],
    )
    .await;
    let order_id = rt.saga("o20").await.id;
    let quiet_id = rt.store.sagas("rt.Quiet").await.remove(0).id;
    let ctx = event_ctx("failed:y", SagaCommandFailed::TYPE, &[]);

    assert!(
        !runtime
            .deliver_command_failed(&failed(&order_id, "k-1"), &ctx)
            .await
            .unwrap(),
        "a saga type with no handler for it: nothing applied"
    );
    rt.store.edit(|c| {
        c.sagas
            .iter_mut()
            .filter(|s| s.id == quiet_id)
            .for_each(|s| s.ended = true)
    });
    assert!(
        !runtime
            .deliver_command_failed(
                &failed(&quiet_id, "k-2"),
                &event_ctx("failed:z", SagaCommandFailed::TYPE, &[])
            )
            .await
            .unwrap(),
        "an ended instance is not reached"
    );
}

#[tokio::test]
async fn a_panicking_handler_is_a_handler_error_that_parks_the_event() {
    let rt = Rt::start().await;
    let panicking = SagaDefinition::<Header>::new("rt.Panics")
        .start_on::<Note>(
            "x",
            |e| Some(e.text.clone()),
            |_, _, _| panic!("handler bug"),
        )
        .build()
        .unwrap();
    let runtime = rt.runtime_of(vec![panicking]);

    deliver_to(&runtime, &Note { text: "p".into() }, "e-p", &[]).await;

    let failures = rt.store.failures().await;
    assert_eq!(failures.len(), 1);
    assert!(
        failures[0].error.contains("panicked"),
        "{}",
        failures[0].error
    );
}

#[tokio::test]
async fn one_handling_may_record_at_most_999_messages_across_all_its_handlers() {
    let rt = Rt::start().await;
    let chatty = SagaDefinition::<Header>::new("rt.Chatty")
        .start_on::<Note>(
            "x",
            |e| Some(e.text.clone()),
            |_, _, ctx| {
                for i in 0..600 {
                    ctx.send(
                        &DoIt {
                            order_id: format!("a{i}"),
                        },
                        None,
                    )?;
                }
                Ok(())
            },
        )
        .on::<Note>(
            "x",
            |e| Some(e.text.clone()),
            |_, _, ctx| {
                for i in 0..600 {
                    ctx.send(
                        &DoIt {
                            order_id: format!("b{i}"),
                        },
                        None,
                    )?;
                }
                Ok(())
            },
        )
        .build()
        .unwrap();
    let runtime = rt.runtime_of(vec![chatty]);

    // One instance, two handlers of the same event: 1200 messages in one handling.
    deliver_to(&runtime, &Note { text: "c".into() }, "e-c", &[]).await;

    assert_eq!(
        rt.store.failures().await.len(),
        1,
        "parked: the handling is refused"
    );
    assert!(rt.store.failures().await[0].error.contains("999"));
    assert_eq!(rt.store.sagas("rt.Chatty").await.len(), 0);
    assert_eq!(rt.store.dispatch().await.len(), 0);
}

#[tokio::test]
async fn the_clock_decides_created_at_and_deadline_due_times() {
    let rt = Rt::start().await;
    rt.store.clock.set(t0() + Duration::from_secs(3600));
    rt.deliver(&placed("o40")).await;

    let doc = rt.saga("o40").await;
    assert_eq!(doc.created_at_ms, dt(t0() + Duration::from_secs(3600)));
    let deadlines = rt.store.deadlines().await;
    let timeout = deadlines.iter().find(|d| d.name == "timeout").unwrap();
    assert_eq!(
        timeout.due_at_ms,
        dt(t0() + Duration::from_secs(3600 + 600))
    );
}

#[derive(Default, Serialize, Deserialize)]
struct Handover {
    order_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HandoverPlaced {
    order_id: String,
}
snodra_messaging::message_type!(HandoverPlaced, "rt.HandoverPlaced");
impl Event for HandoverPlaced {}

/// Its start handler moves the instance from its start association to another one at once.
fn handover_saga() -> SagaType {
    SagaDefinition::<Handover>::new("rt.Handover")
        .start_on::<HandoverPlaced>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, ctx| {
                s.order_id = Some(e.order_id.clone());
                ctx.associate_with("paymentRef", &format!("pay-{}", e.order_id));
                ctx.remove_association_with("orderId", &e.order_id);
                Ok(())
            },
        )
        .build()
        .unwrap()
}

#[tokio::test]
async fn a_start_handler_that_removes_its_own_start_association_leaves_the_start_key_free() {
    let rt = Rt::start().await;
    let runtime = rt.runtime_of(vec![handover_saga()]);
    let placed = HandoverPlaced {
        order_id: "h1".into(),
    };

    deliver_to(&runtime, &placed, "e-h1", &[]).await;
    let first = rt.store.sagas("rt.Handover").await;
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].associations,
        [Association::new("paymentRef", "pay-h1")]
    );
    assert_eq!(
        first[0].start_key, None,
        "the start key went with the start association, as on an update (ruling 35)"
    );

    deliver_to(&runtime, &placed, "e-h2", &[]).await;
    assert_eq!(
        rt.store.failures().await.len(),
        0,
        "the second start event was not parked on a duplicate start key"
    );
    assert_eq!(
        rt.store.sagas("rt.Handover").await.len(),
        2,
        "the second start event started a new instance"
    );
}
