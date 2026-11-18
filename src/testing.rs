//! The zero-infrastructure mode for a service's own tests (feature `testing`): its messaging on
//! the in-memory event store ([`MemoryEventStore`]) and message store ([`MemoryStore`]), with no
//! broker, no database and no container. The test publishes events to the service as another
//! service would, and reads back what the service published and its dead letters:
//!
//! ```no_run
//! # use std::time::Duration;
//! # use snodra_messaging::testing::InMemoryStores;
//! # use snodra_messaging::{Event, SnodraMessaging, MessagingError, message_type};
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct OrderPlaced { order: String }
//! # message_type!(OrderPlaced, "orders.OrderPlaced");
//! # impl Event for OrderPlaced {}
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct OrderConfirmed { order: String }
//! # message_type!(OrderConfirmed, "orders.OrderConfirmed");
//! # impl Event for OrderConfirmed {}
//! # async fn run() -> Result<(), MessagingError> {
//! let stores = InMemoryStores::new();
//! let app = stores
//!     .install(SnodraMessaging::builder(stores.config("orders")).await?)
//!     // the service's own registrations
//!     .event::<OrderPlaced, _, _>(|_placed, _ctx| async { Ok(()) })
//!     .start()
//!     .await?;
//! let order = OrderPlaced { order: "o-1".into() };
//! let event_id = stores.publish(&order, Some("o-1")).await?;
//! let confirmed: Vec<OrderConfirmed> = stores.wait_published(1, Duration::from_secs(10)).await?;
//! assert!(stores.processed(&event_id));
//! let letters = stores.dead_letters().await?;
//! # app.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! Events, processors, dead letters, sagas and idempotent handlers run as on any store; commands
//! and queries need a hub, which [`InMemoryStores::config`] turns off (a test of those uses a hub
//! as before). The application's clock is [`Clock::tokio`], so paused time moves it. The stores
//! warn at startup that they are not durable.

use std::time::Duration;

use crate::app::SnodraMessagingBuilder;
use crate::config::Config;
use crate::error::MessagingError;
use crate::event_store::memory::MemoryEventStore;
use crate::event_store::{EventStore, headers};
use crate::events::{new_outbox_entry, outgoing_event};
use crate::saga::Clock;
use crate::store::memory::MemoryStore;
use crate::store::{DeadLetter, MessageStore};
use crate::types::Event;

/// The `source` header of the events [`InMemoryStores::publish`] publishes: "another service".
pub const TEST_PUBLISHER: &str = "snodra-test";

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
    /// Empty stores; the events go to the source `snodra.events` (as [`Config`]'s default).
    pub fn new() -> InMemoryStores {
        InMemoryStores {
            events: MemoryEventStore::default(),
            store: MemoryStore::new(),
            source: Config::new("", "").events_source,
        }
    }

    /// A configuration for `service` in this mode: its events on this pair's source and the hub
    /// turned off ([`Config::hub_enabled`] false: nothing connects or logs reconnects; every
    /// command, query and subscription sent fails at once, those to the service's own handlers
    /// included; the saga relay still delivers a command type the instance handles to its own
    /// handler, and any other saga command goes into the relay's retries). The relay needs no short poll: the in-memory
    /// store wakes it on every append.
    pub fn config(&self, service: &str) -> Config {
        let mut config = Config::new(service, "static://127.0.0.1:1");
        config.events_source = self.source.clone();
        config.hub_enabled = false;
        config
    }

    /// `builder` with both stores and [`Clock::tokio`] as the application's clock, so a test on
    /// paused time (`#[tokio::test(start_paused = true)]`, `tokio::time::advance`) moves the
    /// outbox leases and claims, the saga deadlines and every time the library stamps. A later
    /// `.clock(..)` on the builder replaces it.
    pub fn install(&self, builder: SnodraMessagingBuilder) -> SnodraMessagingBuilder {
        builder
            .store(std::sync::Arc::new(self.store.clone()))
            .event_store(std::sync::Arc::new(self.events.clone()))
            .clock(Clock::tokio())
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
        let outgoing = outgoing_event(&entry, &self.source, TEST_PUBLISHER);
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
