//! The zero-infrastructure mode for a service's own tests (feature `testing`): its messaging on
//! the in-memory event store ([`MemoryEventStore`]) and message store ([`MemoryStore`]), with no
//! broker, no database and no container. The test publishes events to the service as another
//! service would, and reads back what the service published and its dead letters:
//!
//! ```ignore
//! let stores = InMemoryStores::new();
//! let app = stores
//!     .install(KrakenMessaging::builder(stores.config("orders")).await?)
//!     .event::<OrderPlaced, _, _>(place_order) // the service's own registrations
//!     .start()
//!     .await?;
//! let event_id = stores.publish(&OrderPlaced { .. }, Some("o-1")).await?;
//! let confirmed: Vec<OrderConfirmed> = stores.wait_published(1, Duration::from_secs(10)).await?;
//! assert!(stores.processed(&event_id));
//! let letters = stores.dead_letters().await?;
//! ```
//!
//! Events, processors, dead letters, sagas and idempotent handlers run as on any store; commands
//! and queries need a hub, which [`InMemoryStores::config`] does not point at (a test of those
//! uses a hub as before). The stores warn at startup that they are not durable.

use std::time::Duration;

use crate::app::KrakenMessagingBuilder;
use crate::config::Config;
use crate::error::MessagingError;
use crate::event_store::memory::MemoryEventStore;
use crate::event_store::{EventStore, OutgoingEvent, headers};
use crate::events::new_outbox_entry;
use crate::store::memory::MemoryStore;
use crate::store::{DeadLetter, MessageStore};
use crate::time::rfc3339;
use crate::types::Event;

/// The `source` header of the events [`InMemoryStores::publish`] publishes: "another service".
pub const TEST_PUBLISHER: &str = "kraken-test";

/// How often the waits look again.
const LOOK_AGAIN: Duration = Duration::from_millis(10);

/// Both in-memory stores of one test, and what the test reads back from them. Every clone is the
/// same pair: hand clones to the instances of a test to make them one deployment.
#[derive(Clone)]
pub struct InMemoryStores {
    events: MemoryEventStore,
    store: MemoryStore,
    source: String,
}

impl Default for InMemoryStores {
    fn default() -> Self {
        InMemoryStores::new()
    }
}

impl InMemoryStores {
    /// Empty stores; the events go to the source `kraken.events` (as [`Config`]'s default).
    pub fn new() -> InMemoryStores {
        InMemoryStores {
            events: MemoryEventStore::default(),
            store: MemoryStore::new(),
            source: Config::new("", "").events_source,
        }
    }

    /// A configuration for `service` in this mode: its events on this pair's source and a hub
    /// address nothing listens at (`static://127.0.0.1:1`), since events need no hub. (The
    /// relay needs no short poll: the in-memory store wakes it on every append.)
    pub fn config(&self, service: &str) -> Config {
        let mut config = Config::new(service, "static://127.0.0.1:1");
        config.events_source = self.source.clone();
        config
    }

    /// `builder` with both stores.
    pub fn install(&self, builder: KrakenMessagingBuilder) -> KrakenMessagingBuilder {
        builder
            .store(std::sync::Arc::new(self.store.clone()))
            .event_store(std::sync::Arc::new(self.events.clone()))
    }

    /// The event store.
    pub fn event_store(&self) -> &MemoryEventStore {
        &self.events
    }

    /// The message store.
    pub fn store(&self) -> &MemoryStore {
        &self.store
    }

    /// Publishes `event` straight to the event store as another service would (the seven
    /// headers, `source` [`TEST_PUBLISHER`]); its id.
    pub async fn publish<E: Event>(
        &self,
        event: &E,
        key: Option<&str>,
    ) -> Result<String, MessagingError> {
        let entry = new_outbox_entry(event, key, None, 0)?;
        let mut all = entry.headers;
        all.insert(headers::TYPE.into(), entry.type_name.clone());
        all.insert(headers::ID.into(), entry.id.clone());
        all.insert(headers::TIMESTAMP.into(), rfc3339(entry.created_at_ms));
        all.insert(headers::SOURCE.into(), TEST_PUBLISHER.into());
        let outgoing = OutgoingEvent {
            source: self.source.clone(),
            id: entry.id.clone(),
            type_name: entry.type_name,
            key: entry.key,
            headers: all,
            payload: entry.payload,
        };
        self.events
            .publish(&outgoing)
            .await
            .map_err(|e| MessagingError::Transport(e.to_string()))?;
        Ok(entry.id)
    }

    /// The events of type `E` the services published so far (not [`publish`](Self::publish)'s),
    /// oldest first; an event that does not decode as an `E` is left out.
    pub fn published<E: Event>(&self) -> Vec<E> {
        self.events
            .events(&self.source)
            .into_iter()
            .filter(|d| {
                d.headers.get(headers::TYPE).map(String::as_str) == Some(E::TYPE)
                    && d.headers.get(headers::SOURCE).map(String::as_str) != Some(TEST_PUBLISHER)
            })
            .filter_map(|d| serde_json::from_slice(&d.payload).ok())
            .collect()
    }

    /// [`published`](Self::published) once it holds at least `count` events, waiting at most
    /// `within`; an error naming the type when it does not.
    pub async fn wait_published<E: Event>(
        &self,
        count: usize,
        within: Duration,
    ) -> Result<Vec<E>, MessagingError> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let found = self.published::<E>();
            if found.len() >= count {
                return Ok(found);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(MessagingError::Timeout(format!(
                    "{count} {} published within {within:?}: {} so far",
                    E::TYPE,
                    found.len()
                )));
            }
            tokio::time::sleep(LOOK_AGAIN).await;
        }
    }

    /// Whether a handler of the service recorded the event `event_id` as processed (a processor's
    /// marker `processor:<name>:<id>` or a saga's `saga:<type>:<id>`), committed.
    pub fn processed(&self, event_id: &str) -> bool {
        let suffix = format!(":{event_id}");
        self.store
            .contents()
            .processed
            .iter()
            .any(|m| m.id.ends_with(&suffix))
    }

    /// Every dead letter, of every processor, by parking time.
    pub async fn dead_letters(&self) -> Result<Vec<DeadLetter>, MessagingError> {
        Ok(self.store.dead_letters().list(None, None).await?)
    }

    /// [`dead_letters`](Self::dead_letters) once there are at least `count`, waiting at most
    /// `within`.
    pub async fn wait_dead_letters(
        &self,
        count: usize,
        within: Duration,
    ) -> Result<Vec<DeadLetter>, MessagingError> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let found = self.dead_letters().await?;
            if found.len() >= count {
                return Ok(found);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(MessagingError::Timeout(format!(
                    "{count} dead letters within {within:?}: {} so far",
                    found.len()
                )));
            }
            tokio::time::sleep(LOOK_AGAIN).await;
        }
    }
}
