//! The processed events: the ids of the events a handler has handled, recorded in its
//! transaction, so a redelivered event is skipped (at-least-once delivery, exactly-once effects).

use futures::future::BoxFuture;

use super::{StoreError, Tx};

/// One handled event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessedMarker {
    /// `processor:<name>:<eventId>`.
    pub id: String,
    /// Where the event was read, when it was read from an event store (a partition).
    pub segment: Option<String>,
    /// Its position in `segment` (an offset).
    pub position: Option<String>,
    /// When it was handled, epoch milliseconds.
    pub at_ms: i64,
}

pub trait ProcessedEventStore: Send + Sync {
    /// Records `marker` in the handler's transaction; false when its id is recorded already (a
    /// duplicate: the caller aborts the transaction, which the store may have spoiled).
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        marker: &'a ProcessedMarker,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Removes the markers handled before `before_ms` (epoch milliseconds); how many.
    fn purge(&self, before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>>;
}
