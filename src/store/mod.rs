//! The message store (ADR 0028): everything the library keeps in the service's own database,
//! behind one interface per collection, with one add-on per database implementing all of them
//! ([`MessageStore`]). The outbox and the processed events join the service's transaction
//! ([`Tx`]), so they commit or roll back with the service's own writes.
//!
//! Payloads cross these interfaces as JSON bytes (or JSON text where the stored contract is
//! text), times as epoch milliseconds. An add-on proves itself with the conformance kit
//! (`conformance`, feature `testing`).

use std::sync::Arc;

pub mod control;
pub mod dead_letters;
pub mod external;
pub mod outbox;
pub mod processed;
mod tx;

#[cfg(feature = "testing")]
pub mod conformance;

pub use control::{
    ControlOperation, Locked, ProcessorControlState, ProcessorControlStore, RESET_KIND, ReplayRange,
};
pub use dead_letters::{
    DeadLetter, DeadLetterError, DeadLetterReason, DeadLetterStore, DeadLetterSummary, Parked,
};
pub use external::{ExternalCall, ExternalCallStatus, ExternalCallStore};
pub use outbox::{OutboxEntry, OutboxStore};
pub use processed::{ProcessedEventStore, ProcessedMarker};
pub use tx::{Tx, TxProvider};

pub use crate::saga::ports::{
    DeadlineStore, DispatchRecorder, FailureStore, MarkerStore, SagaPorts, SagaRepository,
};

use crate::error::MessagingError;

/// What a store operation or a transaction fails with.
#[derive(Debug)]
pub enum StoreError {
    /// A version check, a start-key or marker duplicate, a write conflict or a transient
    /// transaction error: another writer got there first, a retry of the whole attempt resolves it.
    Conflict(String),
    /// Anything else (a handler's error, a store outage).
    Failed(MessagingError),
}

impl StoreError {
    /// The failure as the crate's error (a conflict becomes a storage error).
    pub fn into_messaging(self) -> MessagingError {
        match self {
            StoreError::Conflict(message) => MessagingError::Storage(message),
            StoreError::Failed(e) => e,
        }
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, StoreError::Conflict(_))
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Conflict(message) => write!(f, "conflict: {message}"),
            StoreError::Failed(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<MessagingError> for StoreError {
    fn from(e: MessagingError) -> Self {
        StoreError::Failed(e)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Failed(e.into())
    }
}

/// The saga ports (ADR 0021) a store provides; their transaction is the store's
/// [`MessageStore::tx`].
#[derive(Clone)]
pub struct SagaStores {
    pub sagas: Arc<dyn SagaRepository>,
    pub dispatch: Arc<dyn DispatchRecorder>,
    pub deadlines: Arc<dyn DeadlineStore>,
    pub failures: Arc<dyn FailureStore>,
    pub markers: Arc<dyn MarkerStore>,
}

/// A store add-on: every store the library uses, in the service's own database, sharing one
/// transaction.
pub trait MessageStore: Send + Sync {
    /// The add-on's name (`mongo`, `postgres`, `mysql`, `memory`), for logs and errors.
    fn name(&self) -> &'static str;
    fn tx(&self) -> Arc<dyn TxProvider>;
    fn outbox(&self) -> Arc<dyn OutboxStore>;
    fn processed_events(&self) -> Arc<dyn ProcessedEventStore>;
    fn dead_letters(&self) -> Arc<dyn DeadLetterStore>;
    fn processor_control(&self) -> Arc<dyn ProcessorControlStore>;
    fn external_calls(&self) -> Arc<dyn ExternalCallStore>;
    fn sagas(&self) -> SagaStores;
}
