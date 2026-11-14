//! The RabbitMQ Streams event store add-on (ADR 0027): [`RabbitEventStore`] implements the
//! core's [`EventStore`] on RabbitMQ 3.13 or later with the stream plugin, through the stream
//! protocol (`rabbitmq-stream-client`), and [`RabbitBrowser`] is its event browser.
//!
//! # The layout on the wire
//!
//! The Kotlin add-on (`com.rabbitmq:stream-client`) reads and writes the same streams the same
//! way:
//!
//! - **A source is a super stream** (`kraken.events`, partition streams `kraken.events-0` to
//!   `-N`), created by the infrastructure, never by the library
//!   (`rabbitmq-streams add_super_stream kraken.events --partitions 6`); a source that is a plain
//!   stream (a dead-letter stream) is read and written as one partition. A source that is
//!   neither fails the publish or the subscription, naming the command that creates it.
//! - **Routing** is the stream clients' hash routing: murmur3 x86_32 of the routing key's UTF-8
//!   bytes, seed 104729, as an unsigned number modulo the partition count, over the partitions in
//!   the order the broker lists them. The routing key is the event's key; an event without one is
//!   routed by its id, which spreads them.
//! - **A message** is one AMQP 1.0 message per event (never batched or compressed): the payload
//!   (JSON bytes) as one `data` section; `properties.message-id` the event id (a string);
//!   `properties.content-type` `application/json`; the key as the message annotation
//!   `x-routing-key` (a string; absent when the event has none); every header (the seven:
//!   `type`, `id`, `correlationId`, `timestamp`, `source`, `userId`, `traceparent`, and any other,
//!   a dead letter's `kraken-dlt-*`) as an application property, an AMQP string.
//! - **A segment** is the partition stream's suffix after `<source>-` (`3` for
//!   `kraken.events-3`: decimal names sort by value, as the hub lists segments, and fit the
//!   128-byte bound), `0` for a plain stream; **a position** is the offset in decimal; an
//!   event's time is its chunk's timestamp (the leader's clock when it wrote it).
//! - **A processor's subscription** `<service>.<processor>` is a consumer of that name on every
//!   partition stream, with `single-active-consumer=true` and `super-stream=<source>` (absent on a
//!   plain stream): the broker makes one consumer of the name active per partition and spreads
//!   the partitions over the instances. The subscribe property `kraken-member` (a random id per
//!   subscription instance) tells the instances apart in the management API.
//! - **Offsets are tracked on the server** under the subscription's name, per partition stream:
//!   the stored value is the next offset to read (the offset after the last acknowledged event,
//!   as Kafka commits), and a consumer that becomes active resumes at
//!   `OffsetSpecification.offset(stored)`, or at `first` when nothing is stored.
//! - **The native dead letter** is the stream `<source>.dlt` (`kraken.events.dlt`), with the
//!   headers the core writes (as the Kafka add-on's), created by the infrastructure
//!   (`rabbitmq-streams add_stream kraken.events.dlt`).
//!
//! # What it declares
//!
//! `browse`, `lag` and `native-dead-letter` always; `replay` only with the management API
//! ([`RabbitConfig::management`]): a reset is refused while an instance reads the subscription,
//! and only the management API can tell (the stream protocol lists no consumers). Without it,
//! [`EventStore::admin`] is `Unsupported(replay)` and the hub offers no reset.
//!
//! # How it differs from the Kafka add-on
//!
//! - **Redelivery** cannot seek a single active consumer in place: a subscription keeps the
//!   deliveries it handed out and not acknowledged, and hands them out again.
//! - **Lag** counts what a subscription received and has not acknowledged one by one, and what
//!   it has not received yet by offsets, which also number the streams' offset-tracking entries
//!   (every stored offset takes one): a lag not received yet may be overstated. The browser's lag
//!   view counts by offsets alone.
//! - **The stream's bounds** are read, not asked for (the client has no `StreamStats`): the
//!   first and the last chunk; a stream that sends none within 1.5 s is taken as empty.
//! - **Partitions** are read when a subscription joins: one added later is read after the
//!   instances subscribe again.
//! - **Publishing** waits for the broker's confirm of each event; there is no deduplication (no
//!   producer name): an event the outbox publishes again after a crash is a duplicate, which the
//!   processed-event markers skip. A refusal for good (`access refused`, a message over the 1 MiB
//!   frame) is `Rejected`; anything else, a missing stream included, is `Failed` and retried.
//!
//! # Settings ([`RabbitConfig`], `KRAKEN_RABBITMQ_*`)
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `KRAKEN_RABBITMQ_HOSTS` | `rabbitmq` | nodes to connect to first, `host` or `host:port`, comma-separated, tried in order |
//! | `KRAKEN_RABBITMQ_PORT` | 5552 (5551 with TLS) | the port of a host given without one |
//! | `KRAKEN_RABBITMQ_VIRTUAL_HOST` | `/` | |
//! | `KRAKEN_RABBITMQ_USERNAME` | `guest` | |
//! | `KRAKEN_RABBITMQ_PASSWORD` / `_PASSWORD_FILE` | `guest` | the password, or a file holding it (the variable wins) |
//! | `KRAKEN_RABBITMQ_TLS` | false | TLS (rustls) to every node |
//! | `KRAKEN_RABBITMQ_TLS_CA_FILE` | none | PEM CA certificates the nodes are verified against; required with TLS |
//! | `KRAKEN_RABBITMQ_LOAD_BALANCER` | false | the hosts are a load balancer: connect through it until it lands on the node needed |
//! | `KRAKEN_RABBITMQ_MANAGEMENT_URL` | none | the management API (`http://rabbitmq:15672`): declares `replay`, lists live subscriptions |
//! | `KRAKEN_RABBITMQ_MANAGEMENT_USERNAME` / `_PASSWORD` / `_PASSWORD_FILE` | the stream user's | |
//! | `KRAKEN_RABBITMQ_KNOWN_SUBSCRIPTIONS` | none | subscription names the browser's lag view always lists |
//!
//! The source and its dead-letter source are the core's (`KRAKEN_EVENTS_SOURCE`, `<source>.dlt`),
//! as the Kafka add-on's topics. Passwords never appear in `Debug` output or logs. TLS needs a
//! CA file: the client verifies the nodes only against one, and the store never connects
//! unverified (the client's "trust everything" mode is never used).

use std::cmp::Ordering;
use std::sync::Arc;

use futures::future::BoxFuture;
use kraken_messaging::event_store::{
    Capability, EventBrowser, EventStore, EventStoreError, OutgoingEvent, Subscription,
    SubscriptionAdmin, SubscriptionSpec,
};

mod admin;
mod browser;
mod config;
mod conn;
mod management;
mod publish;
mod read;
mod subscription;
mod wire;

pub use admin::RabbitAdmin;
pub use browser::{BROWSER_CONNECTION, CLOSE_GRACE, MAX_SUBSCRIPTIONS, RabbitBrowser};
pub use config::{DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TLS_PORT, Management, RabbitConfig};
pub use management::REQUEST_TIMEOUT as MANAGEMENT_TIMEOUT;
pub use publish::{MAX_FRAME, SEND_TIMEOUT};
pub use read::QUIET;
pub use subscription::{MEMBER_PROPERTY, OFFSETS_REFRESH, READY_LIMIT, RabbitSubscription};
pub use wire::{CONTENT_TYPE, HASH_SEED, KEY_ANNOTATION, partition_index};

/// The name this add-on reports as the processors' event store.
pub const NAME: &str = "rabbitmq-streams";
/// The connection name the store's connections carry.
pub const STORE_CONNECTION: &str = "kraken-messaging";

/// What the store offers without the management API.
const WITHOUT_MANAGEMENT: [Capability; 3] = [
    Capability::Browse,
    Capability::Lag,
    Capability::NativeDeadLetter,
];

/// The RabbitMQ Streams event store: one publisher per partition stream on its leader's
/// connection; each subscription and each admin call gets connections of its own, and each
/// browser call too.
pub struct RabbitEventStore {
    broker: conn::Broker,
    publisher: publish::Publisher,
    management: Option<management::Management>,
    registry: browser::Registry,
    browser: Arc<RabbitBrowser>,
}

impl RabbitEventStore {
    /// Checks `config` (nothing connects before the first call): `Invalid` for no host, a host
    /// that is not `host[:port]`, TLS without a CA file, a management URL that is not http(s),
    /// browse settings without a source or with a zero bound.
    pub fn new(config: RabbitConfig) -> Result<RabbitEventStore, EventStoreError> {
        config.validate()?;
        let config = Arc::new(config);
        let broker = conn::Broker::new(Arc::clone(&config), STORE_CONNECTION)?;
        let management = config
            .management
            .as_ref()
            .map(|m| management::Management::new(m, &config.virtual_host))
            .transpose()?;
        let registry = browser::Registry::default();
        let browser = Arc::new(RabbitBrowser::new(
            conn::Broker::new(Arc::clone(&config), BROWSER_CONNECTION)?,
            management.clone(),
            registry.clone(),
        ));
        Ok(RabbitEventStore {
            publisher: publish::Publisher::new(broker.clone()),
            broker,
            management,
            registry,
            browser,
        })
    }

    /// Closes the publishers' connections (the subscriptions close their own). A store dropped
    /// without it closes them on a task.
    pub async fn close(&self) {
        self.publisher.close().await;
    }
}

impl EventStore for RabbitEventStore {
    fn name(&self) -> &'static str {
        NAME
    }

    fn capabilities(&self) -> &[Capability] {
        if self.management.is_some() {
            &Capability::ALL
        } else {
            &WITHOUT_MANAGEMENT
        }
    }

    fn publish<'a>(
        &'a self,
        event: &'a OutgoingEvent,
    ) -> BoxFuture<'a, Result<(), EventStoreError>> {
        Box::pin(self.publisher.publish(event))
    }

    fn subscribe(
        &self,
        spec: SubscriptionSpec,
    ) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
        Box::pin(async move {
            let member = uuid::Uuid::new_v4().simple().to_string();
            let subscription =
                RabbitSubscription::join(self.broker.clone(), &spec, &member).await?;
            self.registry.add(&spec.source, &spec.name);
            Ok(Box::new(subscription) as Box<dyn Subscription>)
        })
    }

    fn admin(
        &self,
        spec: &SubscriptionSpec,
    ) -> Result<Box<dyn SubscriptionAdmin>, EventStoreError> {
        let management = self
            .management
            .clone()
            .ok_or(EventStoreError::Unsupported(Capability::Replay))?;
        Ok(Box::new(RabbitAdmin::new(
            self.broker.clone(),
            management,
            spec,
        )))
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

fn compare(a: &str, b: &str) -> Ordering {
    match (a.parse::<u64>(), b.parse::<u64>()) {
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
        assert_eq!(compare("x", "7"), Ordering::Greater);
    }

    #[test]
    fn replay_is_declared_only_with_the_management_api() {
        let without = RabbitEventStore::new(RabbitConfig::new("127.0.0.1:1")).unwrap();
        assert_eq!(without.name(), "rabbitmq-streams");
        assert_eq!(
            without.capabilities(),
            [
                Capability::Browse,
                Capability::Lag,
                Capability::NativeDeadLetter
            ]
        );
        let spec = SubscriptionSpec {
            name: "svc.p".into(),
            source: "kraken.events".into(),
        };
        assert!(matches!(
            without.admin(&spec).err(),
            Some(EventStoreError::Unsupported(Capability::Replay))
        ));
        assert!(without.browser().is_some(), "browse: a browser");

        let with = RabbitEventStore::new(
            RabbitConfig::new("127.0.0.1:1").management("http://127.0.0.1:1"),
        )
        .unwrap();
        assert_eq!(with.capabilities(), Capability::ALL);
        assert!(with.admin(&spec).is_ok());
    }

    #[test]
    fn a_bad_configuration_is_refused_by_the_store_and_the_browser() {
        let mut tls = RabbitConfig::new("h");
        tls.tls = true;
        for config in [RabbitConfig::new(" "), tls, RabbitConfig::new("h:port")] {
            assert!(matches!(
                RabbitEventStore::new(config.clone()).err(),
                Some(EventStoreError::Invalid(_))
            ));
            assert!(matches!(
                RabbitBrowser::open(&config).err(),
                Some(EventStoreError::Invalid(_))
            ));
        }
    }
}
