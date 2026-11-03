//! What the saga runtime, relay and scheduler report. The crate has no metrics registry (P5b), so
//! the hooks are a trait: the statistics of Task 12 implement it; [`NoopSagaMetrics`] is the
//! default. Every method has an empty default so hooks can be added without breaking
//! implementations.

use std::collections::HashMap;

use super::failed::FailureReason;

/// The result of one (event, saga type) handling (`kraken_saga_events_total{type,outcome}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EventOutcome {
    Handled,
    NoMatch,
    Conflict,
    Parked,
}

pub trait SagaMetrics: Send + Sync {
    fn saga_started(&self, _saga_type: &str) {}
    fn saga_ended(&self, _saga_type: &str) {}
    fn event(&self, _saga_type: &str, _outcome: EventOutcome) {}

    /// A deadline named `name` ran its handler.
    fn deadline_fired(&self, _name: &str) {}

    /// The SCHEDULED deadlines past their due time after a scheduler poll, per deadline name
    /// (empty when none): `kraken_deadlines_due{name}`.
    fn deadlines_overdue(&self, _by_name: &HashMap<String, u64>) {}

    /// A `SagaCommandFailed` for `command_type` reached no saga or handler (or could not be
    /// delivered): logged at error, `kraken_saga_command_failures_total`.
    fn command_failure_unhandled(&self, _command_type: &str, _reason: FailureReason) {}
}

/// No metrics.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopSagaMetrics;

impl SagaMetrics for NoopSagaMetrics {}
