//! Events on Kafka (`kraken.events`) with the Mongo outbox and the idempotent consumer, byte- and
//! document-compatible with the Kotlin starter (`OutboxEventBus`, `OutboxPublisher`, `EventConsumer`).

mod consumer;
pub mod headers;
mod indexes;
mod outbox;
mod publisher;

pub use consumer::{EventConsumer, RecordDisposition, classify};
pub use indexes::ensure_indexes;
pub use outbox::{EventBus, OutboxDocument, new_outbox_document};
pub use publisher::OutboxPublisher;

pub const EVENTS_TOPIC: &str = "kraken.events";
pub const OUTBOX_COLLECTION: &str = "outbox";
pub const PROCESSED_EVENTS_COLLECTION: &str = "processed_events";
