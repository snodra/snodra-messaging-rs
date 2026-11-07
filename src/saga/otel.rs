//! The saga metrics on the OpenTelemetry meter (ADR 0026): [`OtelSagaMetrics`] is the
//! [`SagaMetrics`] the application's runtime, relay, scheduler and idempotent handlers report to.
//! Each hook moves the [`SagaStats`] (so `KrakenMessaging::saga_stats()` keeps its values) and
//! the counter of the same name and labels on the meter; the gauges are observed from the
//! statistics (the deadline scheduler's last poll and the sampler's last count), never from the
//! database. The series are Kotlin's (`kraken.sagas.started{type}` is
//! `kraken_sagas_started_total{type}`; see [`crate::saga::stats`]).

use std::collections::HashMap;

use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;

use super::failed::FailureReason;
use super::metrics::{EventOutcome, ExternalOutcome, SagaMetrics};
use super::stats::{
    DEADLINES_DUE, DEADLINES_FIRED, EXTERNAL_CALLS, SAGA_COMMAND_FAILURES, SAGA_DISPATCH_PENDING,
    SAGA_EVENTS, SAGAS_ACTIVE, SAGAS_ENDED, SAGAS_STARTED, SagaStats, event_outcome, reason_name,
};
use crate::metrics::Metrics;

/// See the module docs.
pub struct OtelSagaMetrics {
    stats: SagaStats,
    started: Counter<u64>,
    ended: Counter<u64>,
    events: Counter<u64>,
    deadlines_fired: Counter<u64>,
    external_calls: Counter<u64>,
    command_failures: Counter<u64>,
}

impl OtelSagaMetrics {
    /// The counters on `metrics`' meter and the gauges observed from `stats` (while it lives).
    pub fn new(metrics: &Metrics, stats: SagaStats) -> OtelSagaMetrics {
        let meter = metrics.meter();
        for (name, description) in [
            (
                SAGAS_ACTIVE,
                "Live (not ended) saga instances, per saga type",
            ),
            (
                SAGA_DISPATCH_PENDING,
                "saga_dispatch entries neither dispatched nor failed",
            ),
            (
                DEADLINES_DUE,
                "Scheduled deadlines past their due time, per deadline name",
            ),
        ] {
            let watched = stats.downgrade();
            meter
                .u64_observable_gauge(name)
                .with_description(description)
                .with_callback(move |o| {
                    let Some(stats) = watched.upgrade() else {
                        return;
                    };
                    for (labels, value) in stats.gauges(name) {
                        let labels: Vec<KeyValue> = labels
                            .into_iter()
                            .map(|(k, v)| KeyValue::new(k, v))
                            .collect();
                        o.observe(value, &labels);
                    }
                })
                .build();
        }
        let counter = |name: &'static str, description: &'static str| {
            meter
                .u64_counter(name)
                .with_description(description)
                .build()
        };
        OtelSagaMetrics {
            started: counter(SAGAS_STARTED, "Saga instances started, per saga type"),
            ended: counter(SAGAS_ENDED, "Saga instances ended, per saga type"),
            events: counter(
                SAGA_EVENTS,
                "Events handled by saga types, per type and outcome (handled, no_match, conflict, parked)",
            ),
            deadlines_fired: counter(DEADLINES_FIRED, "Deadlines whose handler ran, per name"),
            external_calls: counter(
                EXTERNAL_CALLS,
                "Idempotent handler calls, per handler and outcome (succeeded, replayed, failed, retryable, unknown)",
            ),
            command_failures: counter(
                SAGA_COMMAND_FAILURES,
                "SagaCommandFailed notices nobody handled, per command type and reason",
            ),
            stats,
        }
    }
}

impl SagaMetrics for OtelSagaMetrics {
    fn saga_started(&self, saga_type: &str) {
        self.stats.saga_started(saga_type);
        self.started
            .add(1, &[KeyValue::new("type", saga_type.to_string())]);
    }

    fn saga_ended(&self, saga_type: &str) {
        self.stats.saga_ended(saga_type);
        self.ended
            .add(1, &[KeyValue::new("type", saga_type.to_string())]);
    }

    fn event(&self, saga_type: &str, outcome: EventOutcome) {
        self.stats.event(saga_type, outcome);
        self.events.add(
            1,
            &[
                KeyValue::new("type", saga_type.to_string()),
                KeyValue::new("outcome", event_outcome(outcome)),
            ],
        );
    }

    fn deadline_fired(&self, name: &str) {
        self.stats.deadline_fired(name);
        self.deadlines_fired
            .add(1, &[KeyValue::new("name", name.to_string())]);
    }

    fn deadlines_overdue(&self, by_name: &HashMap<String, u64>) {
        self.stats.deadlines_overdue(by_name);
    }

    fn external_call(&self, handler: &str, outcome: ExternalOutcome) {
        self.stats.external_call(handler, outcome);
        self.external_calls.add(
            1,
            &[
                KeyValue::new("handler", handler.to_string()),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
    }

    fn command_failure_unhandled(&self, command_type: &str, reason: FailureReason) {
        self.stats.command_failure_unhandled(command_type, reason);
        self.command_failures.add(
            1,
            &[
                KeyValue::new("type", command_type.to_string()),
                KeyValue::new("reason", reason_name(reason)),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::test_meter::{Kind, TestMeter};

    #[test]
    fn every_hook_reaches_the_meter_and_the_statistics_with_kotlins_names_and_labels() {
        let t = TestMeter::new();
        let stats = SagaStats::new();
        let metrics = OtelSagaMetrics::new(&Metrics::new(t.meter()), stats.clone());
        metrics.saga_started("t.A");
        metrics.saga_started("t.A");
        metrics.saga_ended("t.A");
        metrics.event("t.A", EventOutcome::NoMatch);
        metrics.deadline_fired("timeout");
        metrics.external_call("t.Print", ExternalOutcome::Replayed);
        metrics.command_failure_unhandled("t.Do", FailureReason::NoHandler);
        metrics.deadlines_overdue(&HashMap::from([("timeout".to_string(), 2)]));
        stats.register_saga_types(["t.A"]);

        let counter = |name: &str, labels: &[(&str, &str)]| {
            let p = t.point(name, labels).unwrap_or_else(|| panic!("{name}"));
            assert_eq!(p.kind, Kind::Counter, "{name}");
            p.value
        };
        assert_eq!(counter(SAGAS_STARTED, &[("type", "t.A")]), 2.0);
        assert_eq!(counter(SAGAS_ENDED, &[("type", "t.A")]), 1.0);
        assert_eq!(
            counter(SAGA_EVENTS, &[("type", "t.A"), ("outcome", "no_match")]),
            1.0
        );
        assert_eq!(counter(DEADLINES_FIRED, &[("name", "timeout")]), 1.0);
        assert_eq!(
            counter(
                EXTERNAL_CALLS,
                &[("handler", "t.Print"), ("outcome", "replayed")]
            ),
            1.0
        );
        assert_eq!(
            counter(
                SAGA_COMMAND_FAILURES,
                &[("type", "t.Do"), ("reason", "NO_HANDLER")]
            ),
            1.0
        );
        let gauge = |name: &str, labels: &[(&str, &str)]| {
            let p = t.point(name, labels).unwrap_or_else(|| panic!("{name}"));
            assert_eq!(p.kind, Kind::Gauge, "{name}");
            p.value
        };
        assert_eq!(gauge(DEADLINES_DUE, &[("name", "timeout")]), 2.0);
        assert_eq!(gauge(SAGAS_ACTIVE, &[("type", "t.A")]), 0.0);
        assert_eq!(gauge(SAGA_DISPATCH_PENDING, &[]), 0.0);
        // The statistics still hold the same values.
        assert_eq!(stats.started("t.A"), 2);
        assert_eq!(stats.deadlines_due("timeout"), Some(2));
    }
}
