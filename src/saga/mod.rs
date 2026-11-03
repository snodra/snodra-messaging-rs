//! Sagas, deadlines and the integration toolkit (ADR 0021, S2): the Rust form of the Kotlin
//! `saga`, `deadline` and `external` packages, over the same MongoDB shapes.
//!
//! A saga is registered on the builder with a closure-based definition
//! ([`KrakenMessagingBuilder::saga`](crate::KrakenMessagingBuilder::saga)); its handlers are
//! synchronous closures that only record what they want done on a [`SagaContext`] (the runtime
//! does the I/O after the saga's transaction committed).

// The erased model is consumed by the runtime, the relay and the scheduler of the later S2 tasks.
#![allow(dead_code, unused_imports)]

mod clock;
mod context;
mod definition;
mod failed;
mod indexes;
mod metrics;
pub mod ports;
mod raw;
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

pub use retry::SagaRetry;
pub use runtime::SagaRuntime;

pub(crate) use context::{AssociationChange, DeadlineOp, Effects, RecordedDispatch};
pub(crate) use definition::{
    ErasedDeadlineHandler, ErasedHandler, ValueSource, validate_registrations,
};
pub(crate) use store::DispatchKind;
