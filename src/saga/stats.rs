//! Saga, deadline, dispatch and external-call statistics (S2-R7, ADR 0021 ruling 21): the Kotlin
//! meters as counters and gauges behind accessors, with the same names and labels, which
//! [`OtelSagaMetrics`](super::OtelSagaMetrics) also records on the meter (ADR 0026:
//! `kraken.sagas.started{type}` is what Prometheus shows as `kraken_sagas_started_total{type}`).
//! Every label value is a saga type, a deadline name, a handler label, an outcome or a reason,
//! never an id.
//!
//! - counters `kraken.sagas.started{type}`, `kraken.sagas.ended{type}`;
//! - `kraken.saga.events{type,outcome}`, outcome `handled | no_match | conflict | parked`;
//! - `kraken.deadlines.fired{name}` and the gauge `kraken.deadlines.due{name}` (the deadlines past
//!   their due time, set by every scheduler poll and back to 0 when none is overdue);
//! - `kraken.external.calls{handler,outcome}`, outcome `succeeded | replayed | failed | retryable
//!   | unknown` (the handler is its message's wire type name);
//! - `kraken.saga.command.failures{type,reason}`: a `SagaCommandFailed` nobody handled (`type` is
//!   the failed command's wire type, `reason` the `FailureReason` name, e.g. `NO_HANDLER`), or a
//!   saga event the outbox refuses (`type` the event's wire type, `reason` `INVALID`);
//! - the gauges `kraken.sagas.active{type}` and `kraken.saga.dispatch.pending`, which no hook
//!   moves: [`SagaStats::sample_once`] counts them in the message store, and
//!   [`SagaStats::start_sampler`] does so every [`SAMPLE_INTERVAL`] (15 s, as Kotlin). A failed
//!   sample is logged and keeps the last values.
//!
//! [`SagaStats`] implements [`SagaMetrics`]; the application's runtime, relay, scheduler and
//! idempotent handlers report to the [`OtelSagaMetrics`](super::OtelSagaMetrics) over it, and
//! `KrakenMessaging::saga_stats()` hands it out.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::failed::FailureReason;
use super::metrics::{EventOutcome, ExternalOutcome, SagaMetrics};
use super::ports::{SagaDispatchStore, SagaRepository};
use crate::error::MessagingError;

/// How often the sampled gauges are counted (ruling 16: a constant).
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

pub const SAGAS_STARTED: &str = "kraken.sagas.started";
pub const SAGAS_ENDED: &str = "kraken.sagas.ended";
pub const SAGA_EVENTS: &str = "kraken.saga.events";
pub const DEADLINES_FIRED: &str = "kraken.deadlines.fired";
pub const DEADLINES_DUE: &str = "kraken.deadlines.due";
pub const EXTERNAL_CALLS: &str = "kraken.external.calls";
pub const SAGA_COMMAND_FAILURES: &str = "kraken.saga.command.failures";
pub const SAGAS_ACTIVE: &str = "kraken.sagas.active";
pub const SAGA_DISPATCH_PENDING: &str = "kraken.saga.dispatch.pending";

/// Whether a [`Sample`] counts up or is set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SampleKind {
    Counter,
    Gauge,
}

/// One counter or gauge with its labels, as [`SagaStats::snapshot`] lists it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sample {
    pub name: &'static str,
    pub labels: Vec<(&'static str, String)>,
    pub kind: SampleKind,
    pub value: u64,
}

type Key = (&'static str, Vec<(&'static str, String)>);

#[derive(Default)]
struct Inner {
    counters: BTreeMap<Key, u64>,
    gauges: BTreeMap<Key, u64>,
}

/// See the module docs. Cheap to clone: every clone shares the same values.
#[derive(Clone, Default)]
pub struct SagaStats(Arc<Mutex<Inner>>);

/// A [`SagaStats`] that does not keep it alive (what an observable gauge holds).
pub(crate) struct WeakSagaStats(Weak<Mutex<Inner>>);

impl WeakSagaStats {
    pub(crate) fn upgrade(&self) -> Option<SagaStats> {
        self.0.upgrade().map(SagaStats)
    }
}

pub(crate) fn event_outcome(outcome: EventOutcome) -> &'static str {
    match outcome {
        EventOutcome::Handled => "handled",
        EventOutcome::NoMatch => "no_match",
        EventOutcome::Conflict => "conflict",
        EventOutcome::Parked => "parked",
    }
}

pub(crate) fn reason_name(reason: FailureReason) -> String {
    serde_json::to_value(reason)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

fn key(name: &'static str, labels: &[(&'static str, &str)]) -> Key {
    (
        name,
        labels.iter().map(|(k, v)| (*k, v.to_string())).collect(),
    )
}

impl SagaStats {
    pub fn new() -> SagaStats {
        SagaStats::default()
    }

    fn inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn increment(&self, name: &'static str, labels: &[(&'static str, &str)]) {
        *self.inner().counters.entry(key(name, labels)).or_default() += 1;
    }

    /// Sets the gauges `name{label}` from `values`; one seen before and absent now goes to 0.
    fn set_gauges(&self, name: &'static str, label: &'static str, values: &HashMap<String, u64>) {
        let mut inner = self.inner();
        for ((n, labels), value) in inner.gauges.iter_mut() {
            if *n == name && !labels.is_empty() {
                *value = 0;
            }
        }
        for (tag, value) in values {
            inner
                .gauges
                .insert(key(name, &[(label, tag.as_str())]), *value);
        }
    }

    pub(crate) fn downgrade(&self) -> WeakSagaStats {
        WeakSagaStats(Arc::downgrade(&self.0))
    }

    /// Every gauge named `name` with its labels and value.
    pub(crate) fn gauges(&self, name: &str) -> Vec<(Vec<(&'static str, String)>, u64)> {
        self.inner()
            .gauges
            .iter()
            .filter(|((n, _), _)| *n == name)
            .map(|((_, labels), value)| (labels.clone(), *value))
            .collect()
    }

    /// Makes the `kraken.sagas.active` gauge of each of `saga_types` exist (at 0) before its
    /// first sample, as Kotlin's.
    pub fn register_saga_types<'a>(&self, saga_types: impl IntoIterator<Item = &'a str>) {
        let mut inner = self.inner();
        for t in saga_types {
            inner
                .gauges
                .entry(key(SAGAS_ACTIVE, &[("type", t)]))
                .or_insert(0);
        }
        inner
            .gauges
            .entry(key(SAGA_DISPATCH_PENDING, &[]))
            .or_insert(0);
    }

    /// The counter `name` with exactly `labels` (in any order), 0 when it never moved.
    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        Self::find(&self.inner().counters, name, labels).unwrap_or(0)
    }

    /// The gauge `name` with exactly `labels`, `None` when it was never set.
    pub fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
        Self::find(&self.inner().gauges, name, labels)
    }

    fn find(map: &BTreeMap<Key, u64>, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
        map.iter()
            .find(|((n, l), _)| {
                *n == name
                    && l.len() == labels.len()
                    && labels
                        .iter()
                        .all(|(k, v)| l.iter().any(|(lk, lv)| lk == k && lv == v))
            })
            .map(|(_, v)| *v)
    }

    /// Every counter and gauge, sorted by kind, name and labels (what an endpoint renders).
    pub fn snapshot(&self) -> Vec<Sample> {
        let inner = self.inner();
        let mut all: Vec<Sample> = inner
            .counters
            .iter()
            .map(|((name, labels), v)| Sample {
                name,
                labels: labels.clone(),
                kind: SampleKind::Counter,
                value: *v,
            })
            .chain(inner.gauges.iter().map(|((name, labels), v)| Sample {
                name,
                labels: labels.clone(),
                kind: SampleKind::Gauge,
                value: *v,
            }))
            .collect();
        all.sort_by(|a, b| (a.kind, a.name, &a.labels).cmp(&(b.kind, b.name, &b.labels)));
        all
    }

    pub fn started(&self, saga_type: &str) -> u64 {
        self.counter(SAGAS_STARTED, &[("type", saga_type)])
    }

    pub fn ended(&self, saga_type: &str) -> u64 {
        self.counter(SAGAS_ENDED, &[("type", saga_type)])
    }

    pub fn events(&self, saga_type: &str, outcome: EventOutcome) -> u64 {
        self.counter(
            SAGA_EVENTS,
            &[("type", saga_type), ("outcome", event_outcome(outcome))],
        )
    }

    pub fn deadlines_fired(&self, name: &str) -> u64 {
        self.counter(DEADLINES_FIRED, &[("name", name)])
    }

    pub fn deadlines_due(&self, name: &str) -> Option<u64> {
        self.gauge(DEADLINES_DUE, &[("name", name)])
    }

    pub fn external_calls(&self, handler: &str, outcome: ExternalOutcome) -> u64 {
        self.counter(
            EXTERNAL_CALLS,
            &[("handler", handler), ("outcome", outcome.as_str())],
        )
    }

    pub fn command_failures(&self, command_type: &str, reason: FailureReason) -> u64 {
        self.counter(
            SAGA_COMMAND_FAILURES,
            &[("type", command_type), ("reason", &reason_name(reason))],
        )
    }

    pub fn active(&self, saga_type: &str) -> Option<u64> {
        self.gauge(SAGAS_ACTIVE, &[("type", saga_type)])
    }

    pub fn dispatch_pending(&self) -> Option<u64> {
        self.gauge(SAGA_DISPATCH_PENDING, &[])
    }

    /// Counts the live (not ended) instances per saga type ([`SagaRepository::count_live_by_type`]) and
    /// the pending dispatch entries ([`SagaDispatchStore::pending`]) and sets the two sampled gauges.
    pub async fn sample_once(
        &self,
        sagas: &dyn SagaRepository,
        queue: &dyn SagaDispatchStore,
    ) -> Result<(), MessagingError> {
        let by_type = sagas.count_live_by_type().await?;
        let pending = queue.pending().await?;
        self.set_gauges(SAGAS_ACTIVE, "type", &by_type);
        self.inner()
            .gauges
            .insert(key(SAGA_DISPATCH_PENDING, &[]), pending);
        Ok(())
    }

    /// Samples every `interval` (the first at once) until `cancel`.
    pub fn start_sampler(
        &self,
        sagas: Arc<dyn SagaRepository>,
        queue: Arc<dyn SagaDispatchStore>,
        interval: Duration,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let stats = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = stats.sample_once(&*sagas, &*queue).await {
                    tracing::warn!("sampling the saga gauges failed, keeping the last values: {e}");
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        })
    }
}

impl SagaMetrics for SagaStats {
    fn saga_started(&self, saga_type: &str) {
        self.increment(SAGAS_STARTED, &[("type", saga_type)]);
    }

    fn saga_ended(&self, saga_type: &str) {
        self.increment(SAGAS_ENDED, &[("type", saga_type)]);
    }

    fn event(&self, saga_type: &str, outcome: EventOutcome) {
        self.increment(
            SAGA_EVENTS,
            &[("type", saga_type), ("outcome", event_outcome(outcome))],
        );
    }

    fn deadline_fired(&self, name: &str) {
        self.increment(DEADLINES_FIRED, &[("name", name)]);
    }

    fn deadlines_overdue(&self, by_name: &HashMap<String, u64>) {
        self.set_gauges(DEADLINES_DUE, "name", by_name);
    }

    fn external_call(&self, handler: &str, outcome: ExternalOutcome) {
        self.increment(
            EXTERNAL_CALLS,
            &[("handler", handler), ("outcome", outcome.as_str())],
        );
    }

    fn command_failure_unhandled(&self, command_type: &str, reason: FailureReason) {
        self.increment(
            SAGA_COMMAND_FAILURES,
            &[("type", command_type), ("reason", &reason_name(reason))],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_move_by_their_labels_and_gauges_go_back_to_zero() {
        let stats = SagaStats::new();
        stats.saga_started("t.A");
        stats.saga_started("t.A");
        stats.event("t.A", EventOutcome::NoMatch);
        stats.command_failure_unhandled("t.Do", FailureReason::NoHandler);
        assert_eq!(stats.started("t.A"), 2);
        assert_eq!(stats.started("t.B"), 0);
        assert_eq!(stats.events("t.A", EventOutcome::NoMatch), 1);
        assert_eq!(
            stats.counter(SAGA_EVENTS, &[("outcome", "no_match"), ("type", "t.A")]),
            1,
            "labels in any order"
        );
        assert_eq!(stats.command_failures("t.Do", FailureReason::NoHandler), 1);
        assert_eq!(
            stats.counter(
                SAGA_COMMAND_FAILURES,
                &[("type", "t.Do"), ("reason", "NO_HANDLER")]
            ),
            1
        );

        stats.deadlines_overdue(&HashMap::from([("a".to_string(), 2), ("b".to_string(), 1)]));
        stats.deadlines_overdue(&HashMap::from([("b".to_string(), 3)]));
        assert_eq!(stats.deadlines_due("a"), Some(0));
        assert_eq!(stats.deadlines_due("b"), Some(3));
        assert_eq!(stats.deadlines_due("c"), None);

        stats.register_saga_types(["t.A"]);
        assert_eq!(stats.active("t.A"), Some(0));
        assert_eq!(stats.dispatch_pending(), Some(0));
        let snapshot = stats.snapshot();
        assert!(snapshot.iter().any(|s| s.name == SAGAS_STARTED
            && s.kind == SampleKind::Counter
            && s.value == 2
            && s.labels == [("type", "t.A".to_string())]));
        assert!(
            snapshot
                .iter()
                .position(|s| s.kind == SampleKind::Gauge)
                .unwrap()
                > snapshot
                    .iter()
                    .rposition(|s| s.kind == SampleKind::Counter)
                    .unwrap()
        );
    }
}
