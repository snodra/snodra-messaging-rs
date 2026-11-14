//! Events through the event store (ADR 0027) with the message store's outbox (ADR 0028):
//! [`EventBus`] appends to the outbox, [`OutboxPublisher`] relays it with
//! [`EventStore::publish`](crate::event_store::EventStore::publish), and each event processor's
//! [`EventConsumer`] reads its subscription, idempotently. The records are byte-compatible with
//! the Kotlin starter's (`OutboxEventBus`, `OutboxPublisher`, `EventConsumer`).

mod consumer;
pub mod dispatch;
mod outbox;
mod publisher;
mod resubscribe;

pub(crate) use consumer::check_dead_letter_mode;
pub(crate) use consumer::message_context;
pub use consumer::{
    ConsumerControl, DLT_HEADERS, EventConsumer, ProcessorSettings, RecordDisposition, classify,
    classify_with,
};
pub use dispatch::{LocalEventDispatcher, NoSagas, SagaStage};
pub use outbox::{EventBus, new_outbox_entry};
pub(crate) use outbox::{check_payload, check_payload_text};
pub use publisher::{OUTBOX_SEND_TIMEOUT, OutboxPublisher, OutboxPublisherSettings};
pub(crate) use resubscribe::{Ended, Resubscribing, keep_subscribed};
pub use resubscribe::{RESUBSCRIBE_MAX, RESUBSCRIBE_MIN};

/// The source a service's events go to unless `KRAKEN_EVENTS_SOURCE` names another (on Kafka,
/// the topic of ADR 0003).
pub const EVENTS_SOURCE: &str = "kraken.events";
