//! The Kafka event store add-on (ADR 0027): [`KafkaEventStore`] implements the core's
//! [`EventStore`] on Apache Kafka, and [`KafkaExternalSource`] reads a partner's topic.
//!
//! On Kafka a source is a topic, a segment a partition and a position an offset (both in
//! decimal); a subscription's name is its consumer group (`<service>.<processor>`). The layout
//! on the wire is the one the Kotlin starter writes: the event's headers are the record's
//! headers, its key the record's key, its payload the record's value, a dead letter goes to
//! `<topic>.dlt`.
//!
//! Capabilities: all four. `replay` (reset a group's offsets, [`KafkaAdmin`]), `lag` (committed
//! offsets and high watermarks, refreshed by the subscription every [`OFFSETS_REFRESH`]),
//! `native-dead-letter` (`.dlt` topics) and `browse` ([`KafkaBrowser`]: search, one record, the
//! topics, the consumer groups' lag and the tail, read-only, never a group of its own).
//!
//! Settings ([`KafkaConfig`]): the bootstrap servers (`SNODRA_KAFKA_BOOTSTRAP`, default
//! `kafka:19092`), extra librdkafka properties (security, for example) for every client the
//! store creates, for its producer only or for its consumers only (the browser's consumers
//! included), and the browser's sources and bounds ([`KafkaConfig::browse`]).
//!
//! **Properties from the environment** ([`KafkaConfig::from_env`]): every variable
//! `SNODRA_KAFKA_PROPERTY_<NAME>` is a property for every client, its name the `<NAME>` in lower
//! case with `_` standing for `.` and `__` for a literal `_`:
//! `SNODRA_KAFKA_PROPERTY_SECURITY_PROTOCOL=SASL_SSL` is `security.protocol`,
//! `SNODRA_KAFKA_PROPERTY_SASL_MECHANISM=SCRAM-SHA-512` is `sasl.mechanism`, and
//! `SNODRA_KAFKA_PROPERTY_LOG__LEVEL=6` is `log_level` (the only string property of librdkafka
//! with an underscore; the others are callbacks set in code). The fixed keys ([`FIXED_KEYS`])
//! are refused when the store or browser opens, as when set in code. Values never appear in
//! `Debug` output or logs: [`KafkaConfig`] shows the property names only.
//!
//! **Which value wins** for one property: the environment ([`KafkaConfig::from_env`]) fills the
//! properties for every client; a [`KafkaConfig::producer_property`] or
//! [`KafkaConfig::consumer_property`] overrides them for its clients; a later
//! [`KafkaConfig::property`] (or `producer_property`, `consumer_property`) of the same name
//! replaces the earlier one, the environment's included. The fixed keys are set last, by the
//! store itself.
//!
//! **Partitions**: the producer partitions keys as the Java client's default partitioner does
//! (librdkafka `partitioner=murmur2_random`, [`DEFAULT_PARTITIONER`]), so one key from a Kotlin and
//! a Rust publisher lands on one partition and keeps its order; a `partitioner` producer property
//! (`SNODRA_KAFKA_PROPERTY_PARTITIONER`, which reaches the producer only, or
//! [`KafkaConfig::producer_property`]) chooses another.
//!
//! **TLS** (`security.protocol=SSL` or `SASL_SSL`) needs the cargo feature `ssl` of this crate,
//! which builds librdkafka with a vendored, statically linked OpenSSL (no system OpenSSL at run
//! time, so a distroless image works). Without it, a configuration asking for TLS is refused when
//! the store or browser opens, naming the feature.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use snodra_messaging::event_store::browse::BrowseSettings;
use snodra_messaging::event_store::{
    Capability, EventBrowser, EventStore, EventStoreError, OutgoingEvent, Subscription,
    SubscriptionAdmin, SubscriptionSpec,
};

mod admin;
mod browser;
mod external;
mod publish;
mod subscription;

pub use admin::KafkaAdmin;
pub use browser::{BROWSER_GROUP_PREFIX, CLOSE_GRACE, KafkaBrowser, MAX_GROUPS, TO_SKEW};
pub use external::{ExternalSource, FIXED_KEYS, KafkaExternalSource};
pub use publish::{DEFAULT_PARTITIONER, SEND_TIMEOUT};
pub use subscription::{KafkaSubscription, OFFSETS_REFRESH};

/// The name this add-on reports as the processors' event store.
pub const NAME: &str = "kafka";

/// What the store offers.
const CAPABILITIES: [Capability; 4] = Capability::ALL;

/// The prefix of the environment variables that carry extra properties
/// ([`KafkaConfig::from_env`]).
pub const PROPERTY_PREFIX: &str = "SNODRA_KAFKA_PROPERTY_";

/// The properties from the environment that only the producer reads: given to a consumer, librdkafka
/// would log them as ignored.
pub const PRODUCER_ONLY: [&str; 1] = ["partitioner"];

/// How the store reaches Kafka. Its `Debug` shows the properties' names, never their values.
#[derive(Clone, PartialEq, Eq)]
pub struct KafkaConfig {
    /// `host:port` list (`bootstrap.servers`).
    pub bootstrap_servers: String,
    /// Extra librdkafka properties for every client (security, for example), never one of
    /// [`FIXED_KEYS`].
    pub properties: HashMap<String, String>,
    /// Extra librdkafka properties for the producer only (a consumer would log them as ignored).
    pub producer_properties: HashMap<String, String>,
    /// Extra librdkafka properties for the consumers only (the subscriptions, the admins and the
    /// browser; the producer would log them as ignored), never one of [`FIXED_KEYS`].
    pub consumer_properties: HashMap<String, String>,
    /// The browser's topics (the first is the default) and bounds (ADR 0025's by default, on
    /// `snodra.events`).
    pub browse: BrowseSettings,
}

impl KafkaConfig {
    /// The service's own Kafka at `bootstrap_servers`.
    pub fn new(bootstrap_servers: &str) -> KafkaConfig {
        KafkaConfig {
            bootstrap_servers: bootstrap_servers.to_string(),
            properties: HashMap::new(),
            producer_properties: HashMap::new(),
            consumer_properties: HashMap::new(),
            browse: BrowseSettings::default(),
        }
    }

    /// `SNODRA_KAFKA_BOOTSTRAP` (default `kafka:19092`), and every `SNODRA_KAFKA_PROPERTY_<NAME>`
    /// as a property for every client (see the crate docs for the names), except the producer's
    /// own [`PRODUCER_ONLY`] ones, which go to the producer only.
    pub fn from_env() -> KafkaConfig {
        KafkaConfig::from_vars(std::env::vars())
    }

    /// [`from_env`](Self::from_env) over `vars` (name, value).
    pub fn from_vars(vars: impl IntoIterator<Item = (String, String)>) -> KafkaConfig {
        let mut bootstrap = None;
        let mut properties = HashMap::new();
        let mut producer_properties = HashMap::new();
        for (name, value) in vars {
            if name == "SNODRA_KAFKA_BOOTSTRAP" {
                bootstrap = Some(value);
            } else if let Some(property) = property_name(&name) {
                if PRODUCER_ONLY.contains(&property.as_str()) {
                    producer_properties.insert(property, value);
                } else {
                    properties.insert(property, value);
                }
            }
        }
        KafkaConfig {
            properties,
            producer_properties,
            ..KafkaConfig::new(&bootstrap_or_default(bootstrap))
        }
    }

    /// One more librdkafka property, for every client.
    pub fn property(mut self, key: &str, value: &str) -> KafkaConfig {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }

    /// One more librdkafka property, for the producer only.
    pub fn producer_property(mut self, key: &str, value: &str) -> KafkaConfig {
        self.producer_properties
            .insert(key.to_string(), value.to_string());
        self
    }

    /// One more librdkafka property, for the consumers only.
    pub fn consumer_property(mut self, key: &str, value: &str) -> KafkaConfig {
        self.consumer_properties
            .insert(key.to_string(), value.to_string());
        self
    }

    /// The browser's topics and bounds.
    pub fn browse(mut self, settings: BrowseSettings) -> KafkaConfig {
        self.browse = settings;
        self
    }

    /// The properties of the producer: every client's, then the producer's own.
    pub(crate) fn producer(&self) -> HashMap<String, String> {
        let mut all = self.properties.clone();
        all.extend(self.producer_properties.clone());
        all
    }

    /// The properties of a consumer: every client's, then the consumers' own.
    pub(crate) fn consumer(&self) -> HashMap<String, String> {
        let mut all = self.properties.clone();
        all.extend(self.consumer_properties.clone());
        all
    }

    fn validate(&self) -> Result<(), EventStoreError> {
        if self.bootstrap_servers.trim().is_empty() {
            return Err(EventStoreError::Invalid(
                "the Kafka bootstrap servers are empty".into(),
            ));
        }
        external::reject_fixed_keys("the Kafka properties", &self.consumer())
            .and_then(|()| {
                external::reject_fixed_keys("the Kafka producer properties", &self.producer())
            })
            .and_then(|()| reject_tls_without_feature(&self.producer()))
            .and_then(|()| reject_tls_without_feature(&self.consumer()))
            .and_then(|()| self.browse.validate())
            .map_err(EventStoreError::Invalid)
    }
}

impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("properties", &names(&self.properties))
            .field("producer_properties", &names(&self.producer_properties))
            .field("consumer_properties", &names(&self.consumer_properties))
            .field("browse", &self.browse)
            .finish()
    }
}

/// The names of `properties`, sorted (what `Debug` shows of them: never a value).
pub(crate) fn names(properties: &HashMap<String, String>) -> Vec<&str> {
    let mut names: Vec<&str> = properties.keys().map(String::as_str).collect();
    names.sort_unstable();
    names
}

/// The property a `SNODRA_KAFKA_PROPERTY_<NAME>` variable sets: `<NAME>` in lower case, `__` a
/// literal `_`, `_` a `.`; `None` for any other variable or an empty name.
fn property_name(variable: &str) -> Option<String> {
    let name = variable.strip_prefix(PROPERTY_PREFIX)?;
    if name.is_empty() {
        return None;
    }
    Some(
        name.to_ascii_lowercase()
            .split("__")
            .map(|part| part.replace('_', "."))
            .collect::<Vec<_>>()
            .join("_"),
    )
}

/// Whether this build can speak TLS to Kafka (the cargo feature `ssl`).
pub const TLS_SUPPORTED: bool = cfg!(feature = "ssl");

/// Refuses `security.protocol` `SSL` or `SASL_SSL` in a build without the feature `ssl`.
pub(crate) fn reject_tls_without_feature(
    properties: &HashMap<String, String>,
) -> Result<(), String> {
    let asks_tls = properties
        .get("security.protocol")
        .is_some_and(|p| p.eq_ignore_ascii_case("ssl") || p.eq_ignore_ascii_case("sasl_ssl"));
    if asks_tls && !TLS_SUPPORTED {
        return Err(
            "security.protocol asks for TLS, but snodra-messaging-kafka was built without its \
             `ssl` feature: enable it (snodra-messaging-kafka = { ..., features = [\"ssl\"] })"
                .into(),
        );
    }
    Ok(())
}

/// The Kafka event store: one producer shared by every publish; each subscription and each admin
/// gets a consumer of its own, and each browser call too.
pub struct KafkaEventStore {
    config: KafkaConfig,
    producer: FutureProducer,
    browser: Arc<KafkaBrowser>,
}

impl KafkaEventStore {
    /// Creates the producer (nothing connects before the first call). `Invalid` for empty
    /// bootstrap servers, a property that is one of [`FIXED_KEYS`] or browse settings without a
    /// topic or with a zero bound, `Failed` when librdkafka refuses the settings.
    pub fn new(config: KafkaConfig) -> Result<KafkaEventStore, EventStoreError> {
        config.validate()?;
        let producer = publish::producer(&config)?;
        let browser = Arc::new(KafkaBrowser::new(
            &config.bootstrap_servers,
            config.consumer(),
            config.browse.clone(),
        ));
        Ok(KafkaEventStore {
            config,
            producer,
            browser,
        })
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
                &self.config.consumer(),
                true,
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
            &self.config.consumer(),
        )?))
    }

    /// Offsets by value; text that is not an offset after any that is (and by text among
    /// themselves).
    fn compare(&self, a: &str, b: &str) -> Ordering {
        compare(a, b)
    }

    fn browser(&self) -> Option<Arc<dyn EventBrowser>> {
        Some(Arc::clone(&self.browser) as Arc<dyn EventBrowser>)
    }
}

/// The configured bootstrap servers, `kafka:19092` when none (or a blank one) is set.
fn bootstrap_or_default(configured: Option<String>) -> String {
    configured
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "kafka:19092".to_string())
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
        assert!(store.browser().is_some(), "browse: a browser");
    }

    #[test]
    fn properties_come_from_the_environment_by_name() {
        assert_eq!(
            property_name("SNODRA_KAFKA_PROPERTY_SECURITY_PROTOCOL").as_deref(),
            Some("security.protocol")
        );
        assert_eq!(
            property_name("SNODRA_KAFKA_PROPERTY_SASL_MECHANISM").as_deref(),
            Some("sasl.mechanism")
        );
        assert_eq!(
            property_name("SNODRA_KAFKA_PROPERTY_LOG__LEVEL").as_deref(),
            Some("log_level")
        );
        assert_eq!(property_name("SNODRA_KAFKA_PROPERTY_"), None);
        assert_eq!(property_name("SNODRA_KAFKA_BOOTSTRAP"), None);
        let vars = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>()
        };
        let config = KafkaConfig::from_vars(vars(&[
            ("SNODRA_KAFKA_BOOTSTRAP", "k1:9092"),
            ("SNODRA_KAFKA_PROPERTY_SASL_USERNAME", "svc"),
            ("SNODRA_KAFKA_PROPERTY_SASL_PASSWORD", "s3cr3t-value"),
            ("PATH", "/bin"),
        ]));
        assert_eq!(config.bootstrap_servers, "k1:9092");
        assert_eq!(config.properties["sasl.username"], "svc");
        assert_eq!(config.properties["sasl.password"], "s3cr3t-value");
        assert_eq!(config.properties.len(), 2);
        assert_eq!(
            KafkaConfig::from_vars(vec![]).bootstrap_servers,
            "kafka:19092"
        );
        // A fixed key from the environment is refused when the store opens.
        let fixed = KafkaConfig::from_vars(vars(&[("SNODRA_KAFKA_PROPERTY_GROUP_ID", "x")]));
        assert!(matches!(
            KafkaEventStore::new(fixed).err(),
            Some(EventStoreError::Invalid(m)) if m.contains("group.id")
        ));
    }

    /// `partitioner` is a producer property: from the environment it reaches the producer only
    /// (a consumer would log it as ignored).
    #[test]
    fn the_partitioner_from_the_environment_reaches_the_producer_only() {
        let config = KafkaConfig::from_vars([
            (
                "SNODRA_KAFKA_PROPERTY_PARTITIONER".to_string(),
                "fnv1a".to_string(),
            ),
            (
                "SNODRA_KAFKA_PROPERTY_SASL_USERNAME".to_string(),
                "svc".to_string(),
            ),
        ]);
        assert_eq!(config.producer()["partitioner"], "fnv1a");
        assert!(!config.consumer().contains_key("partitioner"));
        assert!(!config.properties.contains_key("partitioner"));
        assert_eq!(config.consumer()["sasl.username"], "svc");
    }

    #[test]
    fn debug_shows_property_names_never_values() {
        let config = KafkaConfig::new("h:1")
            .property("sasl.password", "s3cr3t-value")
            .producer_property("ssl.key.password", "other-s3cr3t")
            .consumer_property("sasl.username", "svc-user");
        let shown = format!("{config:?}");
        assert!(shown.contains("sasl.password") && shown.contains("ssl.key.password"));
        for value in ["s3cr3t-value", "other-s3cr3t", "svc-user"] {
            assert!(!shown.contains(value), "{shown}");
        }
        let source = ExternalSource {
            properties: HashMap::from([("sasl.password".to_string(), "s3cr3t-value".to_string())]),
            ..ExternalSource::new("partner.topic")
        };
        assert!(!format!("{source:?}").contains("s3cr3t-value"));
    }

    /// Without the `ssl` feature, asking for TLS fails at open, naming the feature.
    #[cfg(not(feature = "ssl"))]
    #[test]
    fn tls_without_the_feature_is_refused_at_open() {
        for protocol in ["SSL", "sasl_ssl"] {
            let config = KafkaConfig::new("h:1").property("security.protocol", protocol);
            for refused in [
                KafkaEventStore::new(config.clone()).err(),
                KafkaBrowser::open(&config).err(),
            ] {
                assert!(
                    matches!(&refused, Some(EventStoreError::Invalid(m)) if m.contains("`ssl` feature")),
                    "{protocol}: {refused:?}"
                );
            }
        }
        assert!(
            KafkaEventStore::new(
                KafkaConfig::new("h:1").property("security.protocol", "PLAINTEXT")
            )
            .is_ok()
        );
    }

    /// With it, a TLS configuration opens (librdkafka accepts `security.protocol=SSL`).
    #[cfg(feature = "ssl")]
    #[test]
    fn tls_with_the_feature_opens() {
        let config = KafkaConfig::new("h:1").property("security.protocol", "SSL");
        assert!(KafkaEventStore::new(config.clone()).is_ok());
        assert!(KafkaBrowser::open(&config).is_ok());
    }

    #[test]
    fn a_browser_opens_alone_and_checks_its_configuration() {
        let browser = KafkaBrowser::open(&KafkaConfig::new("127.0.0.1:1")).expect("a browser");
        let _as_trait: Arc<dyn EventBrowser> = Arc::new(browser);
        assert!(matches!(
            KafkaBrowser::open(&KafkaConfig::new(" ")).err(),
            Some(EventStoreError::Invalid(_))
        ));
        assert!(matches!(
            KafkaBrowser::open(&KafkaConfig::new("h:1").consumer_property("group.id", "x")).err(),
            Some(EventStoreError::Invalid(_))
        ));
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
        let no_topic = KafkaConfig::new("h:1").browse(BrowseSettings {
            sources: vec![],
            ..BrowseSettings::default()
        });
        assert!(matches!(
            KafkaEventStore::new(no_topic),
            Err(EventStoreError::Invalid(_))
        ));
    }

    #[test]
    fn each_client_gets_the_common_properties_and_its_own_only() {
        let config = KafkaConfig::new("h:1")
            .property("security.protocol", "SSL")
            .producer_property("linger.ms", "5")
            .consumer_property("session.timeout.ms", "30000");
        let producer = config.producer();
        assert_eq!(
            producer.get("security.protocol").map(String::as_str),
            Some("SSL")
        );
        assert_eq!(producer.get("linger.ms").map(String::as_str), Some("5"));
        assert!(!producer.contains_key("session.timeout.ms"));
        let consumer = config.consumer();
        assert_eq!(
            consumer.get("security.protocol").map(String::as_str),
            Some("SSL")
        );
        assert_eq!(
            consumer.get("session.timeout.ms").map(String::as_str),
            Some("30000")
        );
        assert!(!consumer.contains_key("linger.ms"));
        assert!(
            KafkaEventStore::new(KafkaConfig::new("h:1").consumer_property("group.id", "x"))
                .is_err()
        );
    }

    #[test]
    fn the_bootstrap_servers_default_when_none_is_configured() {
        assert_eq!(bootstrap_or_default(None), "kafka:19092");
        assert_eq!(bootstrap_or_default(Some("  ".into())), "kafka:19092");
        assert_eq!(bootstrap_or_default(Some("k:9".into())), "k:9");
    }
}
