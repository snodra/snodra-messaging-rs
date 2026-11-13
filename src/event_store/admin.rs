//! A subscription's positions, moved by a reset (ADR 0023, capability
//! [`Replay`](super::Capability::Replay)).

use super::EventStoreError;

/// Where a reset moves a subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResetTarget {
    /// Every segment's first position: everything the event store still has is a replay.
    Earliest,
    /// Every segment's head: nothing is replayed.
    Latest,
    /// The first position at or after this time (epoch milliseconds, the event store's append
    /// time) per segment, else its head.
    Timestamp(i64),
    /// These segments to these positions (`(segment, position)`); a segment not listed keeps its
    /// committed position.
    Positions(Vec<(String, String)>),
}

/// Events of `segment` of `source` at a position `from <= position < until` are a replay.
///
/// What [`SubscriptionAdmin::plan`] gives is one per segment a reset moves: `from` is the position
/// it moves to, `until` the subscription's committed position before (empty when it had none).
/// Only a range with `until` set and `from` before it (by
/// [`EventStore::compare`](super::EventStore::compare)) replays anything; the others are moves
/// without a replay (`latest`, a segment never committed). The processor control store keeps the
/// open ones.
///
/// `segment`, `from` and `until` are opaque strings of at most
/// [`MAX_SEGMENT_BYTES`](super::MAX_SEGMENT_BYTES) and
/// [`MAX_POSITION_BYTES`](super::MAX_POSITION_BYTES) bytes that only the event store reads;
/// [`EventStore::compare`](super::EventStore::compare) is the only way the core orders two
/// positions of one segment.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReplayRange {
    /// The source (topic, stream) of `segment`.
    pub source: String,
    pub segment: String,
    pub from: String,
    pub until: String,
}

/// The reset side of one subscription ([`EventStore::admin`](super::EventStore::admin)). Every
/// call blocks (it talks to the event store): run them on a blocking thread.
pub trait SubscriptionAdmin: Send {
    /// The instances reading the subscription now (their client or consumer ids); empty when
    /// none does.
    fn members(&self) -> Result<Vec<String>, EventStoreError>;

    /// Where `target` moves each segment, from the event store's current positions; nothing
    /// moves yet. Sorted by segment. A target position the segment does not have (a segment
    /// the source lacks, a position past the head) is [`EventStoreError::Invalid`].
    fn plan(&self, target: &ResetTarget) -> Result<Vec<ReplayRange>, EventStoreError>;

    /// Moves the committed position of each range's segment to its `from` (`until` is not read),
    /// all or nothing; refused with [`EventStoreError::SubscribersActive`] while an instance
    /// reads the subscription.
    fn apply(&self, ranges: &[ReplayRange]) -> Result<(), EventStoreError>;
}
