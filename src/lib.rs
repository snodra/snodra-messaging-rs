//! kraken messaging client for Rust: the counterpart of the Kotlin `kraken-messaging` starter.
//!
//! Wire- and storage-compatible with it: same hub protocol (`proto/hub.proto`), same events on the
//! event store (an add-on, ADR 0027: `kraken-messaging-kafka`), same documents in the message store
//! (an add-on, ADR 0028: `kraken-messaging-mongo`). See README.md.
pub mod app;
pub mod codec;
pub mod config;
pub mod context;
pub mod error;
pub mod event_store;
pub mod events;
pub mod external;
pub mod hub;
pub mod identity;
pub mod metrics;
pub mod processors;
pub mod proto;
pub mod retry;
pub mod saga;
pub mod security;
pub mod store;
#[cfg(feature = "testing")]
pub mod testing;
pub mod time;
pub mod types;

pub use app::{KrakenMessaging, KrakenMessagingBuilder};
pub use config::Config;
pub use context::MessageContext;
pub use error::MessagingError;
pub use hub::{
    EventContext, QueryUpdateEmitter, SendOptions, StreamOptions, Subscription, SubscriptionOptions,
};
pub use identity::{Guard, Identity};
pub use retry::RetryPolicy;
pub use saga::{FailureReason, SagaCommandFailed, SagaContext, SagaDefinition};
pub use security::{HubOAuth2, HubTls, SecretString};
pub use types::{Command, Event, MessageType, Query, StreamingQuery, SubscriptionQuery};
