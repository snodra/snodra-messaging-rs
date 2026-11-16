//! Sagas, deadlines and the integration toolkit (ADR 0021, S2): the Rust form of the Kotlin
//! `saga`, `deadline` and `external` packages, over the message store's saga ports.
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
mod failures;
mod metrics;
mod otel;
pub mod ports;
mod raw;
mod relay;
mod retry;
mod runtime;
pub mod stats;
pub mod store;
/// [`SagaTestFixture`](testing::SagaTestFixture) and the in-memory ports (feature `testing`).
#[cfg(feature = "testing")]
pub mod testing;

pub use clock::{Clock, ManualClock};
pub use context::SagaContext;
pub use definition::{SagaDefinition, SagaType};
pub use failed::{FailureReason, SagaCommandFailed};
pub use metrics::{EventOutcome, ExternalOutcome, NoopSagaMetrics, SagaMetrics};
pub use otel::OtelSagaMetrics;
pub use ports::SagaPorts;
pub use raw::{
    LocalFirstSender, LocalRawCommandSender, RETRYABLE_MARKER, RawCommandSender, RawOutcome,
    UNKNOWN_OUTCOME_MARKER, classify_failure,
};

pub use deadlines::{DeadlineScheduler, DeadlineSettings};
pub use failures::SagaFailures;
pub use relay::{RelaySettings, SagaDispatchRelay};
pub use retry::SagaRetry;
pub use runtime::SagaRuntime;
pub use stats::SagaStats;

pub(crate) use definition::validate_registrations;
