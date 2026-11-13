//! Events on Kafka (`kraken.events`) with the message store's outbox and the idempotent consumer,
//! byte-compatible with the Kotlin starter (`OutboxEventBus`, `OutboxPublisher`, `EventConsumer`).

mod consumer;
pub mod dispatch;
pub mod headers;
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
