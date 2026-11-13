//! Events on Kafka (`kraken.events`) with the message store's outbox and the idempotent consumer,
//! byte-compatible with the Kotlin starter (`OutboxEventBus`, `OutboxPublisher`, `EventConsumer`).

mod consumer;
pub mod dispatch;
mod outbox;
mod publisher;

pub(crate) use consumer::message_context;
pub use consumer::{
    ConsumerControl, EventConsumer, ProcessorContext, ProcessorSettings, RecordDisposition,
    classify, classify_with,
};
pub use dispatch::{LocalEventDispatcher, NoSagas, SagaStage};
pub use outbox::{EventBus, new_outbox_entry};
pub(crate) use outbox::{check_payload, check_payload_text};
pub use publisher::{OUTBOX_SEND_TIMEOUT, OutboxPublisher, OutboxPublisherSettings};

pub const EVENTS_TOPIC: &str = "kraken.events";

use crate::event_store::ReplayRange;

/// A Kafka replay range of `topic`: the partition as its segment and the offsets as its
/// positions, in decimal.
pub(crate) fn replay_range(topic: &str, partition: i32, from: i64, until: i64) -> ReplayRange {
    ReplayRange {
        source: topic.to_string(),
        segment: partition.to_string(),
        from: from.to_string(),
        until: until.to_string(),
    }
}

/// `range` as Kafka's `(partition, from, until)` when it is a range of `topic` with a decimal
/// segment and positions.
pub(crate) fn kafka_range(range: &ReplayRange, topic: &str) -> Option<(i32, i64, i64)> {
    if range.source != topic {
        return None;
    }
    Some((
        range.segment.parse().ok()?,
        range.from.parse().ok()?,
        range.until.parse().ok()?,
    ))
}
