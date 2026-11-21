//! The processed events: the ids of the events a handler has handled, recorded in its
//! transaction, so a redelivered event is skipped (at-least-once delivery, exactly-once effects).

use futures::future::BoxFuture;

use super::{StoreError, Tx};
use crate::event_store::ReplayRange;

/// The idempotency marker of a handled event. `id` is the marker id
/// (`processor:<name>:<eventId>`, `saga:<type>:<eventId>`, `ingress:<source>:<key>`, ...),
/// `type_name` the event's type, `processed_at_ms` when it was handled (the retention counts from
/// it). A stage-1 marker of a delivered event also carries its `processor`, `source`, `segment`
/// and `position` (the event store's opaque strings), so a reset deletes exactly the
/// markers of its replay range ([`ProcessedEventStore::delete_markers`]); every other marker
/// leaves them `None` and is never deleted by a reset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessedMarker {
    pub id: String,
    pub type_name: String,
    /// Epoch milliseconds.
    pub processed_at_ms: i64,
    pub processor: Option<String>,
    pub source: Option<String>,
    pub segment: Option<String>,
    pub position: Option<String>,
}

pub trait ProcessedEventStore: Send + Sync {
    /// Records `marker` in the handler's transaction; false when its id is recorded already (a
    /// duplicate: the caller aborts the transaction, which the store may have spoiled).
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        marker: &'a ProcessedMarker,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Whether a marker with `id` is recorded (committed), read outside any transaction.
    fn exists<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Deletes the stage-1 markers of `processor` in `ranges` (the range's source and segment,
    /// a position in `[from, until)` in the event store's order), outside any transaction; how
    /// many. Markers without a position are never deleted.
    fn delete_markers<'a>(
        &'a self,
        processor: &'a str,
        ranges: &'a [ReplayRange],
    ) -> BoxFuture<'a, Result<u64, StoreError>>;

    /// Removes the markers handled before `before_ms` (epoch milliseconds); how many.
    fn purge(&self, before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>>;
}
