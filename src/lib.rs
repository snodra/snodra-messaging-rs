//! # kraken_messaging
//!
//! Rust client for the kraken messaging layer (see kraken/docs/02-messaging.md):
//!
//! - `HandlerRegistry`: declare `@EventHandler`/`@CommandHandler`/`@QueryHandler` equivalents.
//! - `HubClient`: commands and queries over the gRPC hub, with JWT forwarding.
//! - `EventPublisher` / `EventConsumer`: events on the `kraken.events` Kafka topic.
//! - `Envelope`: the shared JSON envelope and header names.
//!
//! Status: skeleton. Public API is stable in shape; transports return
//! `MessagingError::NotImplemented`.

pub mod envelope;
pub mod error;
pub mod handlers;
pub mod hub_client;
pub mod kafka;

/// Generated gRPC types for `kraken.hub.v1`.
pub mod proto {
    tonic::include_proto!("kraken.hub.v1");
}

pub use envelope::Envelope;
pub use error::MessagingError;
pub use handlers::{Command, Event, HandlerContext, HandlerRegistry, MessageType, Query};
pub use hub_client::{HubClient, HubConfig, SendOptions};
pub use kafka::{EventConsumer, EventPublisher, KafkaConfig};
