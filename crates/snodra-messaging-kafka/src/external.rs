//! A partner's Kafka topic as a source of records (ADR 0021 rulings 20, 40, 44a): the Kafka half
//! of the external sources. [`KafkaExternalSource`] is the core's [`ExternalSourceProvider`]: a
//! [`Subscription`] over the topic in the partner's own consumer group, with the offset acknowledged after the core dispatched the
//! record (at least once); translating a record into an event, the retries and the dedupe id
//! (`<name>:<partition>:<offset>`, from the delivery's segment and position) are the core's.
//!
//! A source's group is its `group_id`, by default `<service-name>.external.<name>`; its consumer
//! properties are the source's `properties` on top of `auto.offset.reset=earliest`, with the
//! servers, the group and auto-commit fixed by the library: a `properties` entry for a fixed key
//! is refused (ruling 40).

use std::collections::HashMap;

use futures::future::BoxFuture;
use snodra_messaging::event_store::{EventStoreError, Subscription};
use snodra_messaging::external::ExternalSourceProvider;

use crate::subscription::KafkaSubscription;

/// The consumer properties the library sets itself; a source's `properties` cannot hold them
/// (Kotlin's list: Rust has no deserializers to configure, the keys are refused all the same).
pub const FIXED_KEYS: [&str; 5] = [
    "bootstrap.servers",
    "group.id",
    "key.deserializer",
    "value.deserializer",
    "enable.auto.commit",
];

/// A partner's Kafka topic (Kotlin's `snodra.messaging.external.kafka.sources.<name>`):
/// `bootstrap_servers` defaults to the service's own Kafka, `group_id` to
/// `<service-name>.external.<name>`; `properties` are extra consumer properties (librdkafka
/// names), never one of [`FIXED_KEYS`].
/// Its `Debug` shows the properties' names, never their values.
#[derive(Clone, Default)]
pub struct ExternalSource {
    pub bootstrap_servers: Option<String>,
    pub topic: String,
    pub group_id: Option<String>,
    pub properties: HashMap<String, String>,
}

impl ExternalSource {
    /// A source reading `topic` on the service's own Kafka.
    pub fn new(topic: &str) -> ExternalSource {
        ExternalSource {
            topic: topic.to_string(),
            ..ExternalSource::default()
        }
    }
}

/// The message refusing a `properties` entry for a fixed key, naming `what` and the key.
pub(crate) fn reject_fixed_keys(
    what: &str,
    properties: &HashMap<String, String>,
) -> Result<(), String> {
    match FIXED_KEYS.iter().find(|k| properties.contains_key(**k)) {
        None => Ok(()),
        Some(key) => Err(format!(
            "{what}: properties[{key}] cannot be set: the library fixes it (use the bootstrap servers and group id settings; deserializers and auto-commit are not configurable). Fixed keys: {}",
            FIXED_KEYS.join(", ")
        )),
    }
}

impl std::fmt::Debug for ExternalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalSource")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("topic", &self.topic)
            .field("group_id", &self.group_id)
            .field("properties", &crate::names(&self.properties))
            .finish()
    }
}

/// A registered partner topic, ready to be read. Its `Debug` shows the properties' names only.
#[derive(Clone)]
pub struct KafkaExternalSource {
    name: String,
    topic: String,
    group_id: String,
    bootstrap_servers: String,
    properties: HashMap<String, String>,
}

impl std::fmt::Debug for KafkaExternalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaExternalSource")
            .field("name", &self.name)
            .field("topic", &self.topic)
            .field("group_id", &self.group_id)
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("properties", &crate::names(&self.properties))
            .finish()
    }
}

impl KafkaExternalSource {
    /// Checks the source (a name, a topic, no fixed key among its `properties`; `Invalid`
    /// otherwise, naming the source and the key) and fills its defaults: `default_bootstrap`
    /// servers and the group `<service_name>.external.<name>`. Nothing connects before
    /// [`subscribe`](Self::subscribe).
    pub fn new(
        name: &str,
        source: ExternalSource,
        service_name: &str,
        default_bootstrap: &str,
    ) -> Result<KafkaExternalSource, EventStoreError> {
        let invalid = |what: String| Err(EventStoreError::Invalid(what));
        if name.trim().is_empty() {
            return invalid("an external source needs a name".into());
        }
        if source.topic.trim().is_empty() {
            return invalid(format!("external source '{name}' needs a topic"));
        }
        reject_fixed_keys(&format!("external source '{name}'"), &source.properties)
            .and_then(|()| crate::reject_tls_without_feature(&source.properties))
            .map_err(EventStoreError::Invalid)?;
        Ok(KafkaExternalSource {
            name: name.to_string(),
            topic: source.topic,
            group_id: source
                .group_id
                .filter(|g| !g.trim().is_empty())
                .unwrap_or_else(|| format!("{service_name}.external.{name}")),
            bootstrap_servers: source
                .bootstrap_servers
                .unwrap_or_else(|| default_bootstrap.to_string()),
            properties: source.properties,
        })
    }

    /// The source's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The partner's topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The consumer group.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Joins the group as one more instance, reading the topic from the group's committed
    /// offset (earliest unless `properties` say otherwise).
    pub async fn subscribe(&self) -> Result<Box<dyn Subscription>, EventStoreError> {
        let subscription = KafkaSubscription::join(
            &self.bootstrap_servers,
            &self.group_id,
            &self.topic,
            &self.properties,
            false,
        )?;
        Ok(Box::new(subscription))
    }
}

impl ExternalSourceProvider for KafkaExternalSource {
    fn source(&self) -> &str {
        &self.topic
    }

    fn subscription(&self) -> &str {
        &self.group_id
    }

    fn subscribe(&self) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
        Box::pin(KafkaExternalSource::subscribe(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(name: &str, source: ExternalSource) -> String {
        match KafkaExternalSource::new(name, source, "svc", "k:1") {
            Err(EventStoreError::Invalid(m)) => m,
            other => panic!("expected a refusal: {other:?}"),
        }
    }

    #[test]
    fn defaults_are_the_service_kafka_and_the_external_group() {
        let s = KafkaExternalSource::new("partner", ExternalSource::new("p.topic"), "svc", "k:1")
            .unwrap();
        assert_eq!(s.group_id(), "svc.external.partner");
        assert_eq!(s.topic(), "p.topic");
        assert_eq!(s.bootstrap_servers, "k:1");
    }

    #[test]
    fn a_fixed_key_is_refused_naming_the_source_and_the_key() {
        let mut source = ExternalSource::new("p.topic");
        source.properties.insert("group.id".into(), "x".into());
        let m = refused("partner", source);
        assert!(m.contains("partner") && m.contains("group.id"), "{m}");
    }

    #[test]
    fn a_name_and_a_topic_are_needed() {
        assert!(refused(" ", ExternalSource::new("t")).contains("name"));
        assert!(refused("p", ExternalSource::new(" ")).contains("topic"));
    }
}
