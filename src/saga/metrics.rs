//! What the saga runtime, relay and scheduler report: a trait. The application's is
//! [`OtelSagaMetrics`](super::OtelSagaMetrics) (the meter and the [`SagaStats`](super::SagaStats)
//! behind `SnodraMessaging::saga_stats()`); [`NoopSagaMetrics`] reports nothing. Every
//! method has an empty default so hooks can be added without breaking implementations.

use std::collections::HashMap;

use super::failed::FailureReason;

/// The result of one (event, saga type) handling (`snodra_saga_events_total{type,outcome}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EventOutcome {
    Handled,
    NoMatch,
    Conflict,
    Parked,
}

/// How one call of an idempotent handler ended (`snodra_external_calls_total{handler,outcome}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExternalOutcome {
    /// The body ran and succeeded.
    Succeeded,
    /// The stored reply of an earlier success was returned.
    Replayed,
    /// The body failed for good (stored; later calls fail the same way), or a stored failure was
    /// returned again.
    Failed,
    /// The body asked for a later attempt, or another call of the same key was in flight.
    Retryable,
    /// An earlier call crashed and its outcome is unknown, or the adapter said so itself.
    Unknown,
}

impl ExternalOutcome {
    /// The tag value (`succeeded`, `replayed`, `failed`, `retryable`, `unknown`).
    pub fn as_str(self) -> &'static str {
        match self {
            ExternalOutcome::Succeeded => "succeeded",
            ExternalOutcome::Replayed => "replayed",
            ExternalOutcome::Failed => "failed",
            ExternalOutcome::Retryable => "retryable",
            ExternalOutcome::Unknown => "unknown",
        }
    }
}

pub trait SagaMetrics: Send + Sync {
    fn saga_started(&self, _saga_type: &str) {}
    fn saga_ended(&self, _saga_type: &str) {}
    fn event(&self, _saga_type: &str, _outcome: EventOutcome) {}

    /// A deadline named `name` ran its handler.
    fn deadline_fired(&self, _name: &str) {}

    /// The SCHEDULED deadlines past their due time after a scheduler poll, per deadline name
    /// (empty when none): `snodra_deadlines_due{name}`.
    fn deadlines_overdue(&self, _by_name: &HashMap<String, u64>) {}

    /// How one call of the idempotent handler `handler` (its message's wire type name) ended.
    fn external_call(&self, _handler: &str, _outcome: ExternalOutcome) {}

    /// A `SagaCommandFailed` for `command_type` reached no saga or handler (or could not be
    /// delivered), or a saga event of that type was refused by the outbox (`INVALID`): logged at
    /// error, `snodra_saga_command_failures_total`.
    fn command_failure_unhandled(&self, _command_type: &str, _reason: FailureReason) {}
}

/// No metrics.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopSagaMetrics;

impl SagaMetrics for NoopSagaMetrics {}
