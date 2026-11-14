//! The Kafka event store add-on (ADR 0027): [`KafkaEventStore`] implements the core's
//! [`EventStore`] on Apache Kafka, and [`KafkaExternalSource`] reads a partner's topic.
//!
//! On Kafka a source is a topic, a segment a partition and a position an offset (both in
//! decimal); a subscription's name is its consumer group (`<service>.<processor>`). The layout
//! on the wire is the one the Kotlin starter writes: the event's headers are the record's
//! headers, its key the record's key, its payload the record's value, a dead letter goes to
//! `<topic>.dlt`.
//!
//! Capabilities: all four. `replay` (reset a group's offsets, [`admin`]), `lag` (committed
//! offsets and high watermarks, refreshed by the subscription every [`OFFSETS_REFRESH`]),
//! `native-dead-letter` (`.dlt` topics) and `browse` (the event browser reads the topic itself).
//!
//! Settings ([`KafkaConfig`]): the bootstrap servers (`KRAKEN_KAFKA_BOOTSTRAP`, default
//! `kafka:19092`) and extra librdkafka properties (security, for example) for every client the
//! store creates.

use std::cmp::Ordering;
use std::collections::HashMap;

use futures::future::BoxFuture;
use kraken_messaging::event_store::{
    Capability, EventStore, EventStoreError, OutgoingEvent, Subscription, SubscriptionAdmin,
    SubscriptionSpec,
};
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

mod admin;
mod external;
mod publish;
mod subscription;

pub use admin::KafkaAdmin;
pub use external::{ExternalSource, FIXED_KEYS, KafkaExternalSource};
pub use publish::SEND_TIMEOUT;
pub use subscription::{KafkaSubscription, OFFSETS_REFRESH};

/// The name this add-on reports as the processors' event store.
pub const NAME: &str = "kafka";

/// What the store offers.
const CAPABILITIES: [Capability; 4] = Capability::ALL;

/// How the store reaches Kafka.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaConfig {
    /// `host:port` list (`bootstrap.servers`).
    pub bootstrap_servers: String,
    /// Extra librdkafka properties for every client, never one of [`FIXED_KEYS`].
    pub properties: HashMap<String, String>,
}

impl KafkaConfig {
    /// The service's own Kafka at `bootstrap_servers`.
    pub fn new(bootstrap_servers: &str) -> KafkaConfig {
        KafkaConfig {
            bootstrap_servers: bootstrap_servers.to_string(),
            properties: HashMap::new(),
        }
    }

    /// `KRAKEN_KAFKA_BOOTSTRAP` (default `kafka:19092`).
    pub fn from_env() -> KafkaConfig {
        KafkaConfig::new(
            &std::env::var("KRAKEN_KAFKA_BOOTSTRAP")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| "kafka:19092".to_string()),
        )
    }

    /// One more librdkafka property.
    pub fn property(mut self, key: &str, value: &str) -> KafkaConfig {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }

    fn validate(&self) -> Result<(), EventStoreError> {
        if self.bootstrap_servers.trim().is_empty() {
            return Err(EventStoreError::Invalid(
                "the Kafka bootstrap servers are empty".into(),
            ));
        }
        external::reject_fixed_keys("the Kafka properties", &self.properties)
            .map_err(EventStoreError::Invalid)
    }
}

/// The Kafka event store: one producer shared by every publish; each subscription and each admin
/// gets a consumer of its own.
pub struct KafkaEventStore {
    config: KafkaConfig,
    producer: FutureProducer,
}

impl KafkaEventStore {
    /// Creates the producer (nothing connects before the first call). `Invalid` for empty
    /// bootstrap servers or a property that is one of [`FIXED_KEYS`], `Failed` when librdkafka
    /// refuses the settings.
    pub fn new(config: KafkaConfig) -> Result<KafkaEventStore, EventStoreError> {
        config.validate()?;
        let producer = publish::producer(&config)?;
        Ok(KafkaEventStore { config, producer })
    }
}

/// The consumer settings every client of the store starts from: the extra `properties`, then the
/// fixed keys (`bootstrap.servers`, `group.id`, no auto commit) on top.
pub(crate) fn consumer_config(
    bootstrap_servers: &str,
    group: &str,
    properties: &HashMap<String, String>,
) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("auto.offset.reset", "earliest");
    for (k, v) in properties {
        config.set(k, v);
    }
    config
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group)
        .set("enable.auto.commit", "false");
    config
}

pub(crate) fn failed(e: impl std::fmt::Display) -> EventStoreError {
    EventStoreError::Failed(format!("kafka: {e}"))
}

impl EventStore for KafkaEventStore {
    fn name(&self) -> &'static str {
        NAME
    }

    fn capabilities(&self) -> &[Capability] {
        &CAPABILITIES
    }

    fn publish<'a>(
        &'a self,
        event: &'a OutgoingEvent,
    ) -> BoxFuture<'a, Result<(), EventStoreError>> {
        Box::pin(publish::send(&self.producer, event))
    }

    fn subscribe(
        &self,
        spec: SubscriptionSpec,
    ) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
        Box::pin(async move {
            let subscription = KafkaSubscription::join(
                &self.config.bootstrap_servers,
                &spec.name,
                &spec.source,
                &self.config.properties,
            )?;
            Ok(Box::new(subscription) as Box<dyn Subscription>)
        })
    }

    fn admin(
        &self,
        spec: &SubscriptionSpec,
    ) -> Result<Box<dyn SubscriptionAdmin>, EventStoreError> {
        Ok(Box::new(KafkaAdmin::new(
            &self.config.bootstrap_servers,
            &spec.name,
            &spec.source,
            &self.config.properties,
        )?))
    }

    /// Offsets by value; text that is not an offset after any that is (and by text among
    /// themselves).
    fn compare(&self, a: &str, b: &str) -> Ordering {
        compare(a, b)
    }
}

fn compare(a: &str, b: &str) -> Ordering {
    match (a.parse::<i64>(), b.parse::<i64>()) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        (Ok(_), Err(_)) => Ordering::Less,
        (Err(_), Ok(_)) => Ordering::Greater,
        (Err(_), Err(_)) => a.cmp(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_compare_by_value() {
        assert_eq!(compare("9", "10"), Ordering::Less);
        assert_eq!(compare("10", "9"), Ordering::Greater);
        assert_eq!(compare("7", "7"), Ordering::Equal);
        assert_eq!(compare("7", "x"), Ordering::Less);
    }

    #[test]
    fn the_store_declares_all_four_capabilities() {
        let store = KafkaEventStore::new(KafkaConfig::new("127.0.0.1:1")).unwrap();
        assert_eq!(store.name(), "kafka");
        assert_eq!(store.capabilities(), Capability::ALL);
    }

    #[test]
    fn a_fixed_property_or_empty_servers_are_refused() {
        let refused = KafkaEventStore::new(KafkaConfig::new("h:1").property("group.id", "x"))
            .err()
            .expect("group.id is fixed");
        assert!(matches!(&refused, EventStoreError::Invalid(m) if m.contains("group.id")));
        assert!(matches!(
            KafkaEventStore::new(KafkaConfig::new(" ")),
            Err(EventStoreError::Invalid(_))
        ));
    }

    #[test]
    fn the_bootstrap_servers_default_for_the_environment() {
        // Not set in the test environment: the default (the variable is read, never written).
        if std::env::var("KRAKEN_KAFKA_BOOTSTRAP").is_err() {
            assert_eq!(KafkaConfig::from_env().bootstrap_servers, "kafka:19092");
        }
    }
}
