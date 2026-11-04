//! Partner Kafka topics as local kraken events (ADR 0021 rulings 20, 40, 44a; spec "Inbound 1"):
//! the Rust form of the Kotlin `ExternalKafkaSources`.
//!
//! Each source registered with
//! [`KrakenMessagingBuilder::external_source`](crate::KrakenMessagingBuilder::external_source) gets
//! a consumer of its own (one task: a partner topic is read in order). Every record goes through
//! the source's translator, and the event it returns is dispatched **locally** through the
//! [`LocalEventDispatcher`] (the ordinary event handlers, then the sagas of this service); it is
//! never republished to `kraken.events`. The dedupe id is `<name>:<partition>:<offset>`, or
//! `<name>:<dedupeKey>` when the translator gives one; it keys the dispatcher's markers, so a
//! record seen twice is applied once. (A partner that deletes and recreates its topic within the
//! markers' retention makes the default id skip the new records: such a partner needs a dedupe
//! key, ruling 44a.) The handlers' `MessageContext` has that id as `message_id` and in the `id`
//! header, the source's name in `source`, the event's type in `type` and the record's own headers;
//! the correlation id is the record's `correlationId` header, else the dedupe id.
//!
//! Delivery is the main consumer's: the offset is committed after the dispatch returned (at least
//! once); a translator error (or panic) or a handler failure retries the record up to
//! `max_attempts` invocations in all with a fixed wait, then logs it (never its payload) and skips
//! it. An `InvalidMessage` from the translator skips the record at once. `Ok(None)` from the
//! translator is committed and applies nothing (no marker). A source's group is its `group_id`,
//! by default `<service-name>.external.<name>`; its consumer properties are the source's
//! `properties` on top of `auto.offset.reset=earliest`, with the servers, the group and
//! auto-commit fixed by the library: a `properties` entry for a fixed key fails the startup
//! (ruling 40).

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

/// rdkafka's record type and the traits a translator reads it with (`record.payload()`,
/// `record.key()`, `record.headers()`), re-exported so a service needs no rdkafka dependency of
/// its own (and cannot end up with a second, incompatible rdkafka version).
pub use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
pub use rdkafka::message::{BorrowedMessage, Headers};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::codec;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::kafka::{LocalEventDispatcher, headers};
use crate::types::Event;

/// The consumer properties the library sets itself; a source's `properties` cannot hold them
/// (Kotlin's list: Rust has no deserializers to configure, the keys are refused all the same).
pub const FIXED_KEYS: [&str; 5] = [
    "bootstrap.servers",
    "group.id",
    "key.deserializer",
    "value.deserializer",
    "enable.auto.commit",
];

/// A partner's Kafka topic (Kotlin's `kraken.messaging.external.kafka.sources.<name>`):
/// `bootstrap_servers` defaults to the service's own Kafka, `group_id` to
/// `<service-name>.external.<name>`; `properties` are extra consumer properties (librdkafka
/// names), never one of [`FIXED_KEYS`].
#[derive(Clone, Debug, Default)]
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

/// What a partner record became: a kraken event (its wire type and JSON) and, optionally, the key
/// the partner's own id gives it. Two records with the same dedupe key are applied once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranslatedEvent {
    type_name: &'static str,
    payload: Vec<u8>,
    dedupe_key: Option<String>,
}

impl TranslatedEvent {
    pub fn new<E: Event>(
        event: &E,
        dedupe_key: Option<String>,
    ) -> Result<TranslatedEvent, MessagingError> {
        Ok(TranslatedEvent {
            type_name: E::TYPE,
            payload: codec::encode(event)?,
            dedupe_key,
        })
    }

    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn dedupe_key(&self) -> Option<&str> {
        self.dedupe_key.as_deref()
    }
}

/// Turns a partner record into a kraken event: `Ok(None)` ignores the record, an error retries it
/// (as the main consumer does) and then skips it. Kotlin's `ExternalRecordTranslator` bean.
pub type Translator = Arc<
    dyn Fn(&BorrowedMessage<'_>) -> Result<Option<TranslatedEvent>, MessagingError> + Send + Sync,
>;

/// A source as registered: its name, its settings and its translator.
pub struct SourceRegistration {
    pub name: String,
    pub source: ExternalSource,
    pub translator: Translator,
}

/// Rejects a source the library cannot run as configured: an empty name or topic, or a
/// `properties` entry for one of the [`FIXED_KEYS`] (ruling 40), naming the source and the key.
pub fn validate_source(name: &str, source: &ExternalSource) -> Result<(), MessagingError> {
    let invalid = |what: String| Err(MessagingError::InvalidMessage(what));
    if name.trim().is_empty() {
        return invalid("an external source needs a name".into());
    }
    if super::is_reserved_source_name(name) {
        return invalid(format!(
            "external source '{name}': the name is reserved (the library's marker families: {})",
            super::RESERVED_SOURCE_NAMES.join(", ")
        ));
    }
    if source.topic.trim().is_empty() {
        return invalid(format!("external source '{name}' needs a topic"));
    }
    if let Some(key) = FIXED_KEYS
        .iter()
        .find(|k| source.properties.contains_key(**k))
    {
        return invalid(format!(
            "external source '{name}': properties[{key}] cannot be set: the library fixes it (use the source's own bootstrap_servers and group_id; deserializers and auto-commit are not configurable). Fixed keys: {}",
            FIXED_KEYS.join(", ")
        ));
    }
    Ok(())
}

struct SourceConsumer {
    name: String,
    topic: String,
    group_id: String,
    consumer: StreamConsumer,
    translator: Translator,
}

/// The consumers of the registered partner topics; see the module docs.
pub struct ExternalKafkaSources {
    sources: Vec<SourceConsumer>,
    dispatcher: Arc<LocalEventDispatcher>,
    max_attempts: u32,
    backoff: Duration,
}

impl ExternalKafkaSources {
    /// Checks every source ([`validate_source`], no two of one name) and creates its consumer
    /// (nothing connects before [`start`](Self::start)). `max_attempts` invocations per record in
    /// all (`KRAKEN_CONSUMER_MAX_ATTEMPTS`), `backoff` between them.
    pub fn new(
        sources: Vec<SourceRegistration>,
        dispatcher: Arc<LocalEventDispatcher>,
        service_name: &str,
        default_bootstrap_servers: &str,
        max_attempts: u32,
        backoff: Duration,
    ) -> Result<Arc<ExternalKafkaSources>, MessagingError> {
        let mut consumers: Vec<SourceConsumer> = Vec::new();
        for SourceRegistration {
            name,
            source,
            translator,
        } in sources
        {
            validate_source(&name, &source)?;
            if consumers.iter().any(|c| c.name == name) {
                return Err(MessagingError::InvalidMessage(format!(
                    "external source '{name}' is registered twice"
                )));
            }
            let group_id = source
                .group_id
                .clone()
                .filter(|g| !g.trim().is_empty())
                .unwrap_or_else(|| format!("{service_name}.external.{name}"));
            let mut config = ClientConfig::new();
            config.set("auto.offset.reset", "earliest");
            for (k, v) in &source.properties {
                config.set(k, v);
            }
            config
                .set(
                    "bootstrap.servers",
                    source
                        .bootstrap_servers
                        .as_deref()
                        .unwrap_or(default_bootstrap_servers),
                )
                .set("group.id", &group_id)
                .set("enable.auto.commit", "false");
            let consumer: StreamConsumer = config.create().map_err(|e| {
                MessagingError::InvalidMessage(format!(
                    "external source '{name}': cannot create its consumer: {e}"
                ))
            })?;
            consumers.push(SourceConsumer {
                name,
                topic: source.topic,
                group_id,
                consumer,
                translator,
            });
        }
        Ok(Arc::new(ExternalKafkaSources {
            sources: consumers,
            dispatcher,
            max_attempts: max_attempts.max(1),
            backoff,
        }))
    }

    /// The consumer group of the source `name`.
    pub fn group_id_for(&self, name: &str) -> Option<&str> {
        self.sources
            .iter()
            .find(|s| s.name == name)
            .map(|s| s.group_id.as_str())
    }

    /// The names of the sources.
    pub fn names(&self) -> Vec<String> {
        self.sources.iter().map(|s| s.name.clone()).collect()
    }

    /// Runs one consumer task per source until `cancel`; the handle completes when all stopped.
    pub fn start(self: &Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        let tasks: Vec<JoinHandle<()>> = (0..self.sources.len())
            .map(|i| {
                let this = Arc::clone(self);
                let cancel = cancel.clone();
                tokio::spawn(async move { this.run(i, cancel).await })
            })
            .collect();
        tracing::info!(sources = ?self.sources.iter().map(|s| format!("{} -> {}", s.name, s.topic)).collect::<Vec<_>>(), "external Kafka sources started");
        tokio::spawn(async move {
            for t in tasks {
                let _ = t.await;
            }
        })
    }

    async fn run(&self, index: usize, cancel: CancellationToken) {
        let source = &self.sources[index];
        if let Err(e) = source.consumer.subscribe(&[source.topic.as_str()]) {
            tracing::error!(source = %source.name, "cannot subscribe to {}: {e}", source.topic);
            return;
        }
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                received = source.consumer.recv() => match received {
                    Err(e) => {
                        tracing::warn!(source = %source.name, "kafka receive error: {e}");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Ok(msg) => {
                        if !self.handle_record(source, &msg, &cancel).await {
                            break; // shutting down mid-retry: the record is redelivered after a restart
                        }
                        if let Err(e) = source.consumer.commit_message(&msg, CommitMode::Async) {
                            tracing::warn!(source = %source.name, "offset commit failed: {e}");
                        }
                    }
                }
            }
        }
    }

    /// Returns false when cancelled while retrying.
    async fn handle_record(
        &self,
        source: &SourceConsumer,
        msg: &BorrowedMessage<'_>,
        cancel: &CancellationToken,
    ) -> bool {
        for attempt in 1..=self.max_attempts {
            match self.translate_and_dispatch(source, msg).await {
                Ok(()) => return true,
                Err(e) => {
                    let skip = matches!(e, MessagingError::InvalidMessage(_));
                    if attempt < self.max_attempts && !skip {
                        tracing::warn!(source = %source.name, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(), attempt, "external record failed, retrying: {e}");
                        tokio::select! {
                            _ = tokio::time::sleep(self.backoff) => {}
                            _ = cancel.cancelled() => return false,
                        }
                    } else {
                        tracing::error!(source = %source.name, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(), attempts = attempt, "giving up on a record of an external source: {e}");
                        return true;
                    }
                }
            }
        }
        true
    }

    async fn translate_and_dispatch(
        &self,
        source: &SourceConsumer,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(), MessagingError> {
        let translated = catch_unwind(AssertUnwindSafe(|| (source.translator)(msg)))
            .unwrap_or_else(|_| {
                Err(MessagingError::Transport(format!(
                    "the translator of external source '{}' panicked",
                    source.name
                )))
            })?;
        let Some(translated) = translated else {
            tracing::debug!(source = %source.name, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(), "the translator ignored the record");
            return Ok(());
        };
        let dedupe_id = format!(
            "{}:{}",
            source.name,
            translated
                .dedupe_key
                .as_deref()
                .filter(|k| !k.trim().is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| format!("{}:{}", msg.partition(), msg.offset()))
        );
        let mut hdrs = record_headers(msg);
        let correlation_id = hdrs
            .get(headers::CORRELATION_ID)
            .filter(|c| !c.is_empty())
            .cloned()
            .unwrap_or_else(|| dedupe_id.clone());
        hdrs.insert(headers::ID.to_string(), dedupe_id.clone());
        hdrs.insert(headers::TYPE.to_string(), translated.type_name.to_string());
        hdrs.insert(headers::SOURCE.to_string(), source.name.clone());
        let ctx = MessageContext {
            message_id: dedupe_id.clone(),
            type_name: translated.type_name.to_string(),
            correlation_id,
            identity: None,
            authorization: None,
            headers: hdrs,
        };
        self.dispatcher
            .dispatch(&dedupe_id, translated.type_name, &translated.payload, &ctx)
            .await
            .map(|_| ())
    }
}

/// The record's headers as text.
pub(crate) fn record_headers(msg: &BorrowedMessage<'_>) -> HashMap<String, String> {
    msg.headers()
        .map(|h| {
            h.iter()
                .map(|x| {
                    (
                        x.key.to_string(),
                        String::from_utf8_lossy(x.value.unwrap_or(&[])).to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}
