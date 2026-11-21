//! The event store: where a service's published events are kept and read from by its
//! event processors, behind one interface, with one add-on per event store implementing it
//! ([`EventStore`]).
//!
//! Every event store provides the contract below, which the conformance kit (`conformance`,
//! feature `testing`) checks:
//!
//! - **Publish**: the outbox relay hands it one [`OutgoingEvent`]; it returns once the event store
//!   accepted it.
//! - **One subscription per processor**: every [`SubscriptionSpec::name`] receives every event of
//!   its source, whatever other processors and services do.
//! - **Competing instances**: the [`Subscription`]s of one name share its segments; adding one
//!   spreads them.
//! - **Order per key**: events with the same key go to one segment and reach a processor in
//!   publish order.
//! - **At-least-once with acknowledgement**: the core acknowledges after the handler and the
//!   processed-event record committed; what was not acknowledged comes again, and the message
//!   store's processed events skip it
//!   ([`ProcessedEventStore`](crate::store::ProcessedEventStore)).
//! - **Pause and resume**: a subscription stops delivering without losing its place.
//! - **The headers**: the seven [`headers`] travel as the event store's native metadata.
//!
//! and declares the [`Capability`]s it offers (`replay`, `browse`, `lag`, `native-dead-letter`);
//! with `browse` it gives an [`EventBrowser`] ([`browse`]).
//!
//! Segments and positions are opaque strings that only the event store reads, at most
//! [`MAX_SEGMENT_BYTES`] and [`MAX_POSITION_BYTES`] bytes; [`EventStore::compare`] orders two
//! positions of one segment. Payloads are JSON bytes, carried as given.
//!
//! A new subscription name starts at the earliest position of every segment.

use std::cmp::Ordering;
use std::sync::Arc;

use futures::future::BoxFuture;

mod admin;
pub mod browse;
pub mod memory;
mod subscription;
mod types;

#[cfg(feature = "testing")]
pub mod conformance;

pub use admin::{ReplayRange, ResetTarget, SubscriptionAdmin};
pub use browse::EventBrowser;
pub use subscription::{FIRST_ASSIGNMENT_WITHIN, Subscription};
pub use types::{
    Capability, Delivery, EventStoreError, MAX_POSITION_BYTES, MAX_SEGMENT_BYTES, OutgoingEvent,
    SegmentStatus, SubscriptionEvent, SubscriptionSpec, dead_letter_source, headers, segment_order,
};

/// An event-store add-on.
pub trait EventStore: Send + Sync {
    /// The add-on's name (`kafka`, `rabbitmq-streams`, `memory` or a third party's), reported to
    /// the hub as the processors' event store.
    fn name(&self) -> &'static str;

    /// What it offers beyond the required contract.
    fn capabilities(&self) -> &[Capability];

    /// Whether it declares `capability`.
    fn supports(&self, capability: Capability) -> bool {
        self.capabilities().contains(&capability)
    }

    /// Appends `event` to `event.source`; returns once the event store accepted it. Two publishes
    /// of one key, the second called after the first returned, are delivered in that order.
    /// [`EventStoreError::Rejected`] when the event store will never take this event (the relay
    /// gives it up at once), [`EventStoreError::Failed`] when another try may succeed.
    fn publish<'a>(
        &'a self,
        event: &'a OutgoingEvent,
    ) -> BoxFuture<'a, Result<(), EventStoreError>>;

    /// Joins the subscription `spec` as one more instance.
    fn subscribe(
        &self,
        spec: SubscriptionSpec,
    ) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>>;

    /// The reset side of the subscription `spec`, for a reset run while no instance reads it;
    /// [`EventStoreError::Unsupported`] without [`Capability::Replay`]. Never blocking; the calls
    /// of the returned admin block.
    fn admin(&self, spec: &SubscriptionSpec)
    -> Result<Box<dyn SubscriptionAdmin>, EventStoreError>;

    /// The order of two positions `a` and `b` of one segment.
    fn compare(&self, a: &str, b: &str) -> Ordering;

    /// Its event browser: `Some` exactly when it declares [`Capability::Browse`]. Never
    /// blocking, never connecting (the browser connects on its first call).
    fn browser(&self) -> Option<Arc<dyn EventBrowser>> {
        None
    }
}
