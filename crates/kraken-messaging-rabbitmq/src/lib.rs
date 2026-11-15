//! The RabbitMQ Streams event store add-on (ADR 0027): [`RabbitEventStore`] implements the
//! core's [`EventStore`] on RabbitMQ 3.13 or later with the stream plugin, through the stream
//! protocol (`rabbitmq-stream-client`), and [`RabbitBrowser`] is its event browser.
//!
//! # The layout on the wire
//!
//! The Kotlin add-on (`com.rabbitmq:stream-client`) reads and writes the same streams the same
//! way. These are rules, not descriptions: both libraries must follow them.
//!
//! - **A source is a super stream** (`kraken.events`, partition streams `kraken.events-0` to
//!   `-N`), created by the infrastructure, never by the library
//!   (`rabbitmq-streams add_super_stream kraken.events --partitions 6`); a source that is a plain
//!   stream (a dead-letter stream) is read and written as one partition. A source that is
//!   neither fails the publish or the subscription, naming the command that creates it.
//! - **Routing** is the stream clients' hash routing: murmur3 x86_32 of the routing key's UTF-8
//!   bytes, seed 104729, as an **unsigned** number modulo the partition count, over the
//!   partitions in the order the broker lists them. The routing key is the event's key; an event
//!   without one is routed by its id, which spreads them. Pinned with 6 partitions (a signed
//!   modulo is wrong: `order-7` would not land on 3):
//!
//!   | key | murmur3 | partition |
//!   |---|---|---|
//!   | `order-1` | `0x3163ba7a` | 0 |
//!   | `order-2` | `0x4aa7bf62` | 2 |
//!   | `customer-123` | `0x6fabe837` | 5 |
//!   | `order-7` | `0x813d1adb` | 3 |
//!   | `""` | `0xc6757d56` | 4 |
//!
//! - **A message** is one AMQP 1.0 message per event: the payload (JSON bytes) as exactly one
//!   `data` section; `properties.message-id` the event id (an AMQP string);
//!   `properties.content-type` the symbol `application/json`; the key as the message annotation
//!   whose key is the symbol `x-routing-key` and whose value is an AMQP string (absent when the
//!   event has none); every header (the seven: `type`, `id`, `correlationId`, `timestamp`,
//!   `source`, `userId`, `traceparent`, and any other, a dead letter's `kraken-dlt-*`) as an
//!   application property whose value is an AMQP string.
//! - **Producers MUST NOT use sub-entry batching or compression** on kraken streams: the Rust
//!   client cannot decode such a chunk (its decoder fails on the connection's read task, so the
//!   connection stops reading; a subscription notices on its next liveness check and ends).
//! - **A segment** is the partition stream's suffix after `<source>-` (`3` for
//!   `kraken.events-3`: decimal names sort by value, as the hub lists segments, and fit the
//!   128-byte bound), `0` for a plain stream; **a position** is the offset in decimal; an
//!   event's time is its chunk's timestamp (the leader's clock when it wrote it).
//! - **A processor's subscription** `<service>.<processor>` is a consumer of that name on every
//!   partition stream, with `single-active-consumer=true`, `name=<service>.<processor>` and
//!   `super-stream=<source>` (absent on a plain stream): the broker makes one consumer of the name
//!   active per partition and spreads the partitions over the instances. Every consumer also
//!   carries the property **`kraken-member`**, a random id per subscription instance (the Kotlin
//!   add-on sets it too): the management API tells the instances apart by it.
//! - **Offset tracking**: the tracking reference is the consumer name, per partition stream;
//!   **the stored value is the next offset to read** (the offset after the last acknowledged
//!   event, as Kafka commits: the "last processed" convention cannot say "resume at offset 0"
//!   after a reset, the protocol's offset being unsigned); a consumer with no stored value starts
//!   at `first`. A consumer that becomes active answers the broker `offset(stored)` (or `first`).
//!   The Kotlin add-on uses the Java client's manual tracking and a consumer update listener that
//!   returns the offset specification itself, never the client's automatic tracking (which would
//!   resume at stored + 1 and skip one event), and stores `offset + 1` of what it acknowledged.
//!   Stored offsets are entries in the stream itself, so a subscription stores every
//!   [`StorePolicy::messages`] acknowledgements or [`StorePolicy::interval`] (10 000 / 5 s by
//!   default, the Java client's), and always on deactivation and at close.
//! - **The native dead letter** is the stream `<source>.dlt` (`kraken.events.dlt`), with the
//!   headers the core writes (as the Kafka add-on's), created by the infrastructure: a stream
//!   queue, e.g. `rabbitmqadmin declare queue --name kraken.events.dlt --type stream --durable true`
//!   (`rabbitmqadmin` 2.x, shipped in the `-management` images; from outside a node add `--host`,
//!   `--port`, `--username` and `--password`; `rabbitmq-streams` has no command for a plain
//!   stream).
//!
//! # What it declares
//!
//! `browse`, `lag` and `native-dead-letter` always; `replay` only with the management API
//! ([`RabbitConfig::management`], plugin `rabbitmq_stream_management`, metrics collection on):
//! a reset is refused while an instance reads the subscription, and only the management API can
//! tell (the stream protocol lists no consumers). Without it, [`EventStore::admin`] is
//! `Unsupported(replay)` and the hub offers no reset. [`RabbitEventStore::open`] asks the API
//! once at start (it must answer: the setting is one URL, so with it configured `open` fails
//! while that node is down even when the rest of a cluster is healthy; put the API behind a load
//! balancer, or use [`RabbitEventStore::new`], which declares `replay` without asking; with
//! metrics collection off, `management.disable_stats` or
//! `management_agent.disable_metrics_collector`, no `replay` is declared); every
//! reset asks again and fails, never "nobody reads", on an answer it cannot trust; this
//! process's own open subscriptions count as readers too, and the offsets a reset stored are read
//! back once all are stored. The API lists a consumer only after its next metrics collection
//! (about every 5 s): an instance in another process that subscribed within the last seconds can
//! be missing from the members check.
//!
//! # How it differs from the Kafka add-on
//!
//! - **Redelivery** cannot seek a single active consumer in place: a subscription keeps the
//!   deliveries it handed out and not acknowledged, and hands them out again.
//! - **Lag** counts what a subscription received and has not acknowledged one by one, and what
//!   it has not received yet by offsets, which also number the streams' offset-tracking entries:
//!   a lag not received yet may be overstated. The browser's lag view counts by offsets alone.
//! - **The stream's bounds** are read, not asked for (neither the client nor its protocol crate
//!   has `StreamStats`): the first and the last chunk. A stream is empty only when both reads
//!   are silent for 1.5 s; one answering and the other not is a failure.
//! - **Partitions** are read when a subscription joins: one added later is read after the
//!   instances subscribe again; publishing reads them again every 30 s.
//! - **Publishing** waits for the broker's confirm of each event, one event at a time (the relay
//!   publishes sequentially), so its rate is one confirm round trip per event: the network
//!   latency to the stream's leader and the broker's write (fsync) time decide it (~1 700/s on a
//!   local broker); there is no deduplication (no
//!   producer name): an event the outbox publishes again after a crash is a duplicate, which the
//!   processed-event markers skip. A refusal for good (`access refused`, a message over the 1 MiB
//!   frame) is `Rejected`; anything else, a missing stream included, is `Failed` and retried.
//!
//! # Bounds when something stalls
//!
//! - **Hand-over.** An instance that stops or crashes hands its partitions over at once (the
//!   broker activates another consumer of the name). After the loss of a broker **node**, a
//!   partition whose active consumer was connected to that node (every partition led by it: a
//!   subscription reads each partition on its leader's connection) has no active consumer until
//!   the broker's `consumer_disconnected_timeout` passes (60 s by default on RabbitMQ 4.x;
//!   measured 60.1 s on a 3-node cluster), then another instance's consumer takes it. Nothing is
//!   lost and the order holds; the events wait.
//! - **A consuming connection that stops reading** (a frame the client cannot decode, a hung
//!   node) is found by the status refresh, which shares its loop with the head reads: measured
//!   ~33 s after a node hung (the 5 s period, then up to three 10 s request bounds: the head
//!   read's open and unsubscribe, then the liveness request). The subscription then ends and
//!   closes: it flushes the positions (up to 10 s), joins its tasks and unsubscribes (up to 10 s),
//!   measured ~20 s more. The hand-over that follows is the broker's, as above. A connection
//!   whose close is not answered has its heartbeats stopped: the broker drops it after two
//!   missed heartbeats.
//! - **A publisher connection that confirms nothing** within the confirm timeout (30 s by
//!   default) is replaced: that publish fails (the relay tries again) and the next one opens a
//!   new connection.
//! - **A consistently slow broker** (answers over 10 s) makes every request fail as if the
//!   connection were gone: subscriptions end and the core subscribes again after its backoff,
//!   publishes fail and the relay retries, browser calls answer `unavailable`. Nothing is lost;
//!   nothing moves until the broker answers in time again.
//! - **After a crash**, a subscription stored its position at most 10 000 acknowledgements or
//!   5 s ago (the store policy): up to that much per partition is delivered again (the
//!   processed-event markers skip it), and the browser's lag view, which reads the stored
//!   positions, trails by as much.
//!
//! # Settings ([`RabbitConfig`], `KRAKEN_RABBITMQ_*`)
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `KRAKEN_RABBITMQ_HOSTS` | `rabbitmq` | nodes to connect to first, `host` or `host:port`, comma-separated, tried in order |
//! | `KRAKEN_RABBITMQ_PORT` | 5552 (5551 with TLS) | the port of a host given without one |
//! | `KRAKEN_RABBITMQ_VIRTUAL_HOST` | `/` | |
//! | `KRAKEN_RABBITMQ_USERNAME` | `guest` | |
//! | `KRAKEN_RABBITMQ_PASSWORD` / `_PASSWORD_FILE` | `guest` | the password, or a file holding it (the file wins; its surrounding whitespace is trimmed, and an empty file is a startup error) |
//! | `KRAKEN_RABBITMQ_TLS` | false | TLS (rustls) to every node |
//! | `KRAKEN_RABBITMQ_TLS_CA_FILE` | none | PEM CA certificates the nodes are verified against; required with TLS |
//! | `KRAKEN_RABBITMQ_LOAD_BALANCER` | false | the hosts are a load balancer: connect through it until it lands on the node needed |
//! | `KRAKEN_RABBITMQ_CONFIRM_TIMEOUT_MS` | 30000 | the longest a publish waits for its confirm; a connection that confirms nothing in time is replaced |
//! | `KRAKEN_RABBITMQ_OFFSET_STORE_MESSAGES` | 10000 | store a subscription's position every so many acknowledgements... |
//! | `KRAKEN_RABBITMQ_OFFSET_STORE_INTERVAL_MS` | 5000 | ...or this often, whichever comes first |
//! | `KRAKEN_RABBITMQ_MANAGEMENT_URL` | none | the management API (`http://rabbitmq:15672`, no credentials in it): declares `replay`, lists live subscriptions |
//! | `KRAKEN_RABBITMQ_MANAGEMENT_USERNAME` / `_PASSWORD` / `_PASSWORD_FILE` | the stream user's | the file wins over the password |
//! | `KRAKEN_RABBITMQ_MANAGEMENT_CA_FILE` | none | PEM CA certificates of an `https` management API |
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
pub use subscription::{
    MEMBER_PROPERTY, OFFSETS_REFRESH, READY_LIMIT, RabbitSubscription, StorePolicy,
};
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
    /// The management API when `replay` is declared.
    management: Option<management::Management>,
    registry: browser::Registry,
    live: subscription::Live,
    offset_store: StorePolicy,
    browser: Arc<RabbitBrowser>,
}

impl RabbitEventStore {
    /// Checks `config` (nothing connects before the first call): `Invalid` for no host, a host
    /// that is not `host[:port]`, TLS without a CA file, a management URL that is not http(s)
    /// or carries credentials, an offset store policy of zero, browse settings without a source
    /// or with a zero bound. With a management API it declares `replay` without asking it
    /// ([`open`](Self::open) asks); every reset still fails while the API cannot tell who reads.
    pub fn new(config: RabbitConfig) -> Result<RabbitEventStore, EventStoreError> {
        config.validate()?;
        let management = management::Management::of(&config)?;
        RabbitEventStore::build(config, management)
    }

    /// [`new`](Self::new), then, with a management API, asks it once: `Failed` when it does not
    /// answer; `replay` is not declared when its metrics collection is off
    /// (`management.disable_stats` or `management_agent.disable_metrics_collector`), since its
    /// consumer list then answers nothing.
    pub async fn open(config: RabbitConfig) -> Result<RabbitEventStore, EventStoreError> {
        config.validate()?;
        let mut management = management::Management::of(&config)?;
        if let Some(m) = &management
            && !m.stats_enabled().await?
        {
            tracing::warn!(
                "the RabbitMQ management API collects no metrics (management.disable_stats or \
                 management_agent.disable_metrics_collector): \
                 the event store does not declare replay"
            );
            management = None;
        }
        RabbitEventStore::build(config, management)
    }

    fn build(
        config: RabbitConfig,
        management: Option<management::Management>,
    ) -> Result<RabbitEventStore, EventStoreError> {
        let config = Arc::new(config);
        let broker = conn::Broker::new(Arc::clone(&config), STORE_CONNECTION)?;
        let registry = browser::Registry::default();
        let browser = Arc::new(RabbitBrowser::new(
            conn::Broker::new(Arc::clone(&config), BROWSER_CONNECTION)?,
            management::Management::of(&config)?,
            registry.clone(),
        ));
        Ok(RabbitEventStore {
            publisher: publish::Publisher::new(broker.clone()),
            broker,
            management,
            registry,
            live: subscription::Live::default(),
            offset_store: config.offset_store,
            browser,
        })
    }

    /// Closes the publishers' connections (the subscriptions close their own). A store dropped
    /// without it closes them on a task.
    pub async fn close(&self) {
        self.publisher.close().await;
    }

    /// The connections the store's publishers hold now (a bounded number, whatever replacements
    /// happened). For the add-on's tests, not part of its API.
    #[doc(hidden)]
    pub async fn publishing_connections(&self) -> usize {
        self.publisher.connections().await
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
            let mut subscription =
                RabbitSubscription::join(self.broker.clone(), &spec, &member, self.offset_store)
                    .await?;
            subscription.hold(self.live.enter(&spec, &member));
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
            self.live.clone(),
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
