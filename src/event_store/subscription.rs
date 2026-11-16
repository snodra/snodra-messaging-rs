//! A processor's subscription on one instance.

use futures::future::BoxFuture;

use super::{EventStoreError, SegmentStatus, SubscriptionEvent};

/// The longest a new subscription takes to yield its first event, an `Assigned` (see
/// [`Subscription`]); the conformance kit checks it.
pub const FIRST_ASSIGNMENT_WITHIN: std::time::Duration = std::time::Duration::from_secs(10);

/// One instance's subscription of a processor ([`EventStore::subscribe`](super::EventStore::subscribe)):
/// the segments the event store assigns it and their events, one at a time. Not the
/// [`kraken_messaging::Subscription`](crate::Subscription) at the crate root, which is an open
/// subscription query to the hub.
///
/// - **Driven by [`next`](Self::next).** Nothing is delivered until `next` is called, and the core
///   keeps calling it while paused: that keeps the subscription's membership alive and reports
///   assignment changes.
/// - **Order per segment.** A segment's events come in their order, each after the
///   [`SubscriptionEvent::Assigned`] naming its segment and before any `Revoked` of it.
/// - **The first event is an `Assigned`** (an empty one when the subscription holds no segment),
///   within [`FIRST_ASSIGNMENT_WITHIN`] of joining: an instance that joined and holds nothing is a
///   member, idle (the core reports it `RUNNING`, not `STARTING`). An empty first `Assigned` may be
///   followed by one naming segments; an `Assigned` naming segments is never followed by a late
///   empty "first" one.
/// - **Every `Revoked` is followed by an `Assigned`** (an empty one when nothing new comes): the
///   assignment change is over only then (the core reports the processor `STARTING` in between).
///   An event store that only moves segments away (a single active consumer losing one) still
///   closes the change with `Assigned([])`.
/// - **At-least-once.** An event is acknowledged with [`ack`](Self::ack) after its handling
///   committed; one not acknowledged comes again to the next subscription of the same name that
///   gets its segment (after a [`close`](Self::close), a crash or a reassignment), or to this one
///   after [`redeliver`](Self::redeliver).
/// - **A failure ends it.** An error from `next` means the subscription is broken: the caller
///   closes it and subscribes again after a backoff (a store retries what it can recover from
///   inside `next`, and reports it through [`take_error`](Self::take_error)).
pub trait Subscription: Send {
    /// The next assignment change or delivery; waits for one. Cancel-safe: dropping the future
    /// before it completes loses nothing (the core races it with its shutdown and its timers).
    /// An error ends the subscription (see above).
    fn next(&mut self) -> BoxFuture<'_, Result<SubscriptionEvent, EventStoreError>>;

    /// Acknowledges the delivery at `position` of `segment` and every earlier one of that
    /// segment: a later subscription of this name resumes after it. May reach the event store
    /// after the call returns; [`close`](Self::close) flushes it.
    fn ack(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError>;

    /// Hands the delivery at `position` of `segment` out again, without leaving the subscription:
    /// `next` delivers it and that segment's later events again (what the core does with an event
    /// it could not finish, e.g. while its dead letters cannot take it). Other segments are
    /// unaffected.
    fn redeliver(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError>;

    /// Stops delivering without losing the place: `next` gives assignment changes only, and a
    /// segment assigned while paused is paused at once. Idempotent; called before the first
    /// `next`, nothing is ever delivered until [`resume`](Self::resume).
    fn pause(&mut self);

    /// Delivers again from where the pause stopped. Idempotent.
    fn resume(&mut self);

    /// The segments assigned now, in [`segment_order`](super::segment_order), with their
    /// positions and lag as last known (refreshed by the subscription itself; never a blocking
    /// call).
    fn segments(&self) -> Vec<SegmentStatus>;

    /// The latest trouble the subscription recovered from by itself since the last call (a lost
    /// connection, a failed reassignment, an error it retried), as text for the processor's
    /// status (`last_error`); `None` when there was none. Never blocking.
    fn take_error(&mut self) -> Option<String> {
        None
    }

    /// Leaves the subscription after flushing the acknowledgements: its segments go to the other
    /// subscriptions of the name.
    fn close(self: Box<Self>) -> BoxFuture<'static, ()>;
}
