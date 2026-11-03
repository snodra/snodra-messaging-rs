//! Sagas, deadlines and the integration toolkit (ADR 0021, S2): the Rust form of the Kotlin
//! `saga`, `deadline` and `external` packages, over the same MongoDB shapes.
//!
//! A saga is registered on the builder with a closure-based definition
//! ([`KrakenMessagingBuilder::saga`](crate::KrakenMessagingBuilder::saga)); its handlers are
//! synchronous closures that only record what they want done on a [`SagaContext`] (the runtime
//! does the I/O after the saga's transaction committed).

mod clock;
mod context;
mod deadlines;
mod definition;
mod failed;
mod indexes;
mod metrics;
pub mod ports;
mod raw;
mod relay;
mod retry;
mod runtime;
pub mod store;

pub use clock::{Clock, ManualClock};
pub use context::SagaContext;
pub use definition::{SagaDefinition, SagaType};
pub use failed::{FailureReason, SagaCommandFailed};
pub use indexes::{ensure_external_call_indexes, ensure_saga_indexes};
pub use metrics::{EventOutcome, NoopSagaMetrics, SagaMetrics};
pub use ports::{SagaPorts, StoreError, Tx};
pub use raw::{
    LocalFirstSender, LocalRawCommandSender, RETRYABLE_MARKER, RawCommandSender, RawOutcome,
    UNKNOWN_OUTCOME_MARKER, classify_failure,
};

pub use deadlines::{DeadlineScheduler, DeadlineSettings};
pub use relay::{RelaySettings, SagaDispatchRelay};
pub use retry::SagaRetry;
pub use runtime::SagaRuntime;

pub(crate) use definition::validate_registrations;
