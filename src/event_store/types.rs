//! What crosses the event-store interface: the events published, the deliveries read, the
//! capabilities declared and the errors.

use std::collections::HashMap;

/// The headers every event carries (ADR 0027), as the event store's native metadata (Kafka record
/// headers, AMQP application properties, a map in memory).
pub mod headers {
    /// The event's type name.
    pub const TYPE: &str = "type";
    /// The event's id.
    pub const ID: &str = "id";
    pub const CORRELATION_ID: &str = "correlationId";
    /// When the event was recorded, RFC 3339 in UTC with milliseconds.
    pub const TIMESTAMP: &str = "timestamp";
    /// The publishing service's name (not the event store's source).
    pub const SOURCE: &str = "source";
    /// The caller's user id; empty when none.
    pub const USER_ID: &str = "userId";
    /// The W3C trace context of the message that caused the event.
    pub const TRACEPARENT: &str = "traceparent";

    /// The seven headers.
    pub const ALL: [&str; 7] = [
        TYPE,
        ID,
        CORRELATION_ID,
        TIMESTAMP,
        SOURCE,
        USER_ID,
        TRACEPARENT,
    ];
}

/// The most bytes of a segment's name (hub.proto bounds: a longer one is dropped by the hub).
pub const MAX_SEGMENT_BYTES: usize = 128;
/// The most bytes of a position (hub.proto bounds: a longer one reads as unknown on the hub).
pub const MAX_POSITION_BYTES: usize = 128;

/// The dead-letter source of `source` (`<source>.dlt`): where the dead-letter mode `event-store`
/// sends what a processor cannot handle, on an event store declaring
/// [`Capability::NativeDeadLetter`].
pub fn dead_letter_source(source: &str) -> String {
    format!("{source}.dlt")
}

/// What an event store may offer beyond the required contract (ADR 0027); the hub and the
/// dashboard offer only what an event store declares.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Capability {
    /// Reset to earliest, latest, a time or explicit positions: reset handlers, replay ranges
    /// ([`SubscriptionAdmin`](super::SubscriptionAdmin)).
    Replay,
    /// The event browser can read it: search, one record, the live tail.
    Browse,
    /// How far a subscription is behind, per segment ([`SegmentStatus::lag`]).
    Lag,
    /// A dead-letter source ([`dead_letter_source`]): the dead-letter mode `event-store`.
    NativeDeadLetter,
}

impl Capability {
    /// Every capability, in protocol order.
    pub const ALL: [Capability; 4] = [
        Capability::Replay,
        Capability::Browse,
        Capability::Lag,
        Capability::NativeDeadLetter,
    ];

    /// The protocol's name (`ProcessorStatus.capabilities`): `replay`, `browse`, `lag`,
    /// `native-dead-letter`.
    pub fn as_str(self) -> &'static str {
        match self {
            Capability::Replay => "replay",
            Capability::Browse => "browse",
            Capability::Lag => "lag",
            Capability::NativeDeadLetter => "native-dead-letter",
        }
    }

    /// The capability named `name` ([`as_str`](Self::as_str)); `None` for any other name.
    pub fn parse(name: &str) -> Option<Capability> {
        Capability::ALL.into_iter().find(|c| c.as_str() == name)
    }
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One event the outbox relay hands the event store ([`EventStore::publish`](super::EventStore::publish)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutgoingEvent {
    /// The source (topic, stream) it goes to: the service's events source, or its dead-letter
    /// source ([`dead_letter_source`]).
    pub source: String,
    /// The event's id, also in header [`headers::ID`].
    pub id: String,
    /// The event's type name, also in header [`headers::TYPE`].
    pub type_name: String,
    /// The order key: events with one key reach a processor in publish order; `None` spreads
    /// them as the event store does.
    pub key: Option<String>,
    /// Carried as given, as the event store's native metadata: the seven [`headers`] and any
    /// other (a dead letter's `kraken-dlt-*`).
    pub headers: HashMap<String, String>,
    /// JSON bytes, carried as given.
    pub payload: Vec<u8>,
}

/// One event a subscription delivers.
///
/// `segment` and `position` are opaque strings of at most [`MAX_SEGMENT_BYTES`] and
/// [`MAX_POSITION_BYTES`] bytes that only the event store reads (a partition and an offset in
/// decimal for the first-wave stores); the core orders two positions of one segment only through
/// [`EventStore::compare`](super::EventStore::compare).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    /// The segment (partition, partition stream) it was read from: a unit of order per key.
    pub segment: String,
    /// Its position in `segment`, which [`Subscription::ack`](super::Subscription::ack) takes.
    pub position: String,
    pub key: Option<String>,
    /// The headers as published.
    pub headers: HashMap<String, String>,
    /// The payload as published.
    pub payload: Vec<u8>,
    /// When the event store appended it, epoch milliseconds (its own clock).
    pub timestamp_ms: i64,
}

/// What [`Subscription::next`](super::Subscription::next) gives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionEvent {
    /// These segments became this subscription's (in addition to the ones it has).
    Assigned(Vec<String>),
    /// These segments are this subscription's no longer; what it delivered of them and did not
    /// acknowledge comes again to whichever subscription gets them.
    Revoked(Vec<String>),
    /// An event of a segment assigned to this subscription.
    Delivery(Delivery),
}

/// A processor's subscription: every subscription of one `name` shares the events of `source`
/// (competing instances), and each name receives every event (one per processor).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionSpec {
    /// `<service>.<processor>`: the consumer group, the stream consumer name.
    pub name: String,
    /// The source read: the topic, the super stream.
    pub source: String,
}

/// One segment assigned to a subscription, as [`Subscription::segments`](super::Subscription::segments)
/// reports it (hub.proto `SegmentStatus`, without the replay range, which is the core's).
/// Positions as in [`Delivery`]; `None` when unknown.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SegmentStatus {
    pub segment: String,
    /// The next position this subscription delivers.
    pub position: Option<String>,
    /// The subscription's committed position (the one after its last acknowledgement).
    pub committed: Option<String>,
    /// The position of the next event appended to the segment.
    pub head: Option<String>,
    /// How many events lie between `committed` and `head` (between `position` and `head` when
    /// nothing is committed): what is not acknowledged yet. `None` when unknown, always `None`
    /// without [`Capability::Lag`].
    pub lag: Option<u64>,
}

/// What an event store operation fails with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventStoreError {
    /// A bad argument: an unknown segment, a position the segment does not have.
    Invalid(String),
    /// Another instance still reads the subscription (its members), so its positions cannot move
    /// (`CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE`).
    SubscribersActive(String),
    /// The event store lacks the capability (`CONTROL_REFUSAL_UNSUPPORTED`).
    Unsupported(Capability),
    /// The event store failed or cannot be reached.
    Failed(String),
}

impl std::fmt::Display for EventStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventStoreError::Invalid(why) => write!(f, "invalid: {why}"),
            EventStoreError::SubscribersActive(members) => {
                write!(f, "the subscription has active subscribers: {members}")
            }
            EventStoreError::Unsupported(capability) => {
                write!(f, "the event store lacks the capability {capability}")
            }
            EventStoreError::Failed(why) => write!(f, "event store: {why}"),
        }
    }
}

impl std::error::Error for EventStoreError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_have_the_protocol_names() {
        let names: Vec<&str> = Capability::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(names, ["replay", "browse", "lag", "native-dead-letter"]);
        for c in Capability::ALL {
            assert_eq!(Capability::parse(c.as_str()), Some(c));
            assert_eq!(c.to_string(), c.as_str());
        }
        assert_eq!(Capability::parse("topic"), None);
    }

    #[test]
    fn the_dead_letter_source_is_the_source_with_dlt() {
        assert_eq!(dead_letter_source("kraken.events"), "kraken.events.dlt");
    }
}
