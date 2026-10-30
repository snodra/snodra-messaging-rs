//! kraken messaging client for Rust: the counterpart of the Kotlin `kraken-messaging` starter.
//!
//! Wire- and storage-compatible with it: same hub protocol (`proto/hub.proto`), same Kafka record
//! format on `kraken.events`, same `outbox` and `processed_events` collections. See README.md.
pub mod app;
pub mod codec;
pub mod config;
pub mod context;
pub mod error;
pub mod hub;
pub mod identity;
pub mod kafka;
pub mod proto;
pub mod types;

pub use app::{KrakenMessaging, KrakenMessagingBuilder};
pub use config::Config;
pub use context::MessageContext;
pub use error::MessagingError;
pub use hub::{EventContext, SendOptions};
pub use identity::{Guard, Identity};
pub use types::{Command, Event, MessageType, Query};
