//! Partner sources as local snodra events.
//!
//! Each source registered with
//! [`SnodraMessagingBuilder::external_source`](crate::SnodraMessagingBuilder::external_source)
//! comes from an [`ExternalSourceProvider`] (an event-store add-on's, for example the Kafka
//! add-on's `KafkaExternalSource` for a partner's topic), whatever the service's own event store
//! is, and gets a task of its own (one subscription: a partner source is read in order). Every
//! record ([`ExternalRecord`]) goes through the source's translator, and the event it returns is
//! dispatched **locally** through the [`LocalEventDispatcher`] (the ordinary event handlers, then
//! the sagas of this service); it is never republished to the service's events. The dedupe id is
//! `<name>:<segment>:<position>` (on Kafka `<name>:<partition>:<offset>`, as always), or
//! `<name>:<dedupeKey>` when the translator gives one; it keys the dispatcher's markers, so a
//! record seen twice is applied once. (A partner that recreates its source within the markers'
//! retention makes the default id skip the new records: such a partner needs a dedupe key.) The
//! handlers' `MessageContext` has that id as `message_id` and in the `id` header, the source's name
//! in `source`, the event's type in `type` and the record's own headers; the correlation id is the
//! record's `correlationId` header, else the dedupe id.
//!
//! Delivery is the processors': the record is acknowledged after the dispatch returned (at least
//! once); a translator error (or panic) or a handler failure retries the record up to
//! `max_attempts` invocations in all with a fixed wait, then logs it (never its payload) and skips
//! it. An `InvalidMessage` from the translator skips the record at once. `Ok(None)` from the
//! translator is acknowledged and applies nothing (no marker). A subscription whose `next` fails
//! is closed and subscribed again after a backoff.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::codec;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::event_store::{Delivery, EventStoreError, Subscription, SubscriptionEvent, headers};
use crate::events::{Ended, LocalEventDispatcher, Resubscribing, keep_subscribed};
use crate::types::Event;

/// One record of a partner source, as a translator reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalRecord {
    /// The partner's source it was read from (a topic on Kafka).
    pub source: String,
    pub key: Option<String>,
    /// Its headers as text.
    pub headers: HashMap<String, String>,
    /// Its payload as published.
    pub payload: Vec<u8>,
    /// Where it sits in the source: the event store's opaque segment and position.
    pub segment: String,
    pub position: String,
}

impl ExternalRecord {
    fn of(source: &str, delivery: Delivery) -> ExternalRecord {
        ExternalRecord {
            source: source.to_string(),
            key: delivery.key,
            headers: delivery.headers,
            payload: delivery.payload,
            segment: delivery.segment,
            position: delivery.position,
        }
    }
}

/// A partner source an event-store add-on reads (the Kafka add-on's `KafkaExternalSource`, for
/// example): a subscription of its own over the partner's source.
pub trait ExternalSourceProvider: Send + Sync {
    /// The partner's source it reads ([`ExternalRecord::source`]).
    fn source(&self) -> &str;

    /// The name of the subscription it reads in (a consumer group on Kafka), for the logs.
    fn subscription(&self) -> &str;

    /// Joins the subscription as one more instance; see the [`Subscription`] contract (a failure
    /// of `next` ends it, and the core subscribes again).
    fn subscribe(&self) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>>;
}

/// What a partner record became: a Snodra event (its wire type and JSON) and, optionally, the key
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

/// Turns a partner record into a Snodra event: `Ok(None)` ignores the record, an error retries it
/// (as the processors do) and then skips it.
pub type Translator =
    Arc<dyn Fn(&ExternalRecord) -> Result<Option<TranslatedEvent>, MessagingError> + Send + Sync>;

/// A source as registered: its name, where it is read from and its translator.
pub struct SourceRegistration {
    pub name: String,
    pub provider: Arc<dyn ExternalSourceProvider>,
    pub translator: Translator,
}

/// Rejects a source name the library cannot run: an empty one or a reserved one.
pub fn validate_source(name: &str) -> Result<(), MessagingError> {
    if name.trim().is_empty() {
        return Err(MessagingError::InvalidMessage(
            "an external source needs a name".into(),
        ));
    }
    if super::is_reserved_source_name(name) {
        return Err(MessagingError::InvalidMessage(format!(
            "external source '{name}': the name is reserved (the library's marker families: {})",
            super::RESERVED_SOURCE_NAMES.join(", ")
        )));
    }
    Ok(())
}

/// One source's reader, kept subscribed.
struct SourceReader<'s> {
    sources: &'s ExternalSources,
    source: &'s SourceRegistration,
}

impl Resubscribing for SourceReader<'_> {
    fn subscribe(&self) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
        self.source.provider.subscribe()
    }

    fn read<'a>(
        &'a self,
        subscription: &'a mut dyn Subscription,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Ended> {
        Box::pin(self.sources.read(self.source, subscription, cancel))
    }

    fn failed(&self, failure: &EventStoreError, backoff: Duration) {
        tracing::warn!(source = %self.source.name, subscription = %self.source.provider.subscription(), "the external source's subscription failed, subscribing again in {backoff:?}: {failure}");
    }
}

/// The registered partner sources' readers; see the module docs.
pub struct ExternalSources {
    sources: Vec<SourceRegistration>,
    dispatcher: Arc<LocalEventDispatcher>,
    max_attempts: u32,
    backoff: Duration,
}

impl ExternalSources {
    /// Checks every source ([`validate_source`], no two of one name); nothing connects before
    /// [`start`](Self::start). `max_attempts` invocations per record in all
    /// (`SNODRA_CONSUMER_MAX_ATTEMPTS`), `backoff` between them.
    pub fn new(
        sources: Vec<SourceRegistration>,
        dispatcher: Arc<LocalEventDispatcher>,
        max_attempts: u32,
        backoff: Duration,
    ) -> Result<Arc<ExternalSources>, MessagingError> {
        for (i, source) in sources.iter().enumerate() {
            validate_source(&source.name)?;
            if sources[..i].iter().any(|s| s.name == source.name) {
                return Err(MessagingError::InvalidMessage(format!(
                    "external source '{}' is registered twice",
                    source.name
                )));
            }
        }
        Ok(Arc::new(ExternalSources {
            sources,
            dispatcher,
            max_attempts: max_attempts.max(1),
            backoff,
        }))
    }

    /// The subscription the source `name` reads in.
    pub fn subscription_of(&self, name: &str) -> Option<&str> {
        self.sources
            .iter()
            .find(|s| s.name == name)
            .map(|s| s.provider.subscription())
    }

    /// The names of the sources.
    pub fn names(&self) -> Vec<String> {
        self.sources.iter().map(|s| s.name.clone()).collect()
    }

    /// Runs one task per source until `cancel`; the handle completes when all stopped.
    pub fn start(self: &Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        let tasks: Vec<JoinHandle<()>> = (0..self.sources.len())
            .map(|i| {
                let this = Arc::clone(self);
                let cancel = cancel.clone();
                tokio::spawn(async move { this.run(i, cancel).await })
            })
            .collect();
        tracing::info!(sources = ?self.sources.iter().map(|s| format!("{} -> {}", s.name, s.provider.source())).collect::<Vec<_>>(), "external sources started");
        tokio::spawn(async move {
            for t in tasks {
                let _ = t.await;
            }
        })
    }

    /// Subscribes, reads until `cancel`, and subscribes again after a failure.
    async fn run(&self, index: usize, cancel: CancellationToken) {
        let reader = SourceReader {
            sources: self,
            source: &self.sources[index],
        };
        keep_subscribed(&reader, &cancel).await;
    }

    /// Reads `subscription` until `cancel` or a shutdown mid-retry, or its failure.
    async fn read(
        &self,
        source: &SourceRegistration,
        subscription: &mut dyn Subscription,
        cancel: &CancellationToken,
    ) -> Ended {
        let mut delivered = false;
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => return Ended::Stopped,
                next = subscription.next() => next,
            };
            if let Some(e) = subscription.take_error() {
                tracing::warn!(source = %source.name, "external source: {e}");
            }
            let delivery = match next {
                Err(error) => return Ended::Failed { error, delivered },
                Ok(SubscriptionEvent::Delivery(d)) => d,
                Ok(change) => {
                    tracing::debug!(source = %source.name, "external source: {change:?}");
                    continue;
                }
            };
            delivered = true;
            let record = ExternalRecord::of(source.provider.source(), delivery);
            if !self.handle_record(source, &record, cancel).await {
                // Shutting down mid-retry: the record is delivered again after a restart.
                return Ended::Stopped;
            }
            if let Err(e) = subscription.ack(&record.segment, &record.position) {
                tracing::warn!(source = %source.name, segment = %record.segment, position = %record.position, "acknowledgement failed: {e}");
            }
        }
    }

    /// Returns false when cancelled while retrying.
    async fn handle_record(
        &self,
        source: &SourceRegistration,
        record: &ExternalRecord,
        cancel: &CancellationToken,
    ) -> bool {
        for attempt in 1..=self.max_attempts {
            match self.translate_and_dispatch(source, record).await {
                Ok(()) => return true,
                Err(e) => {
                    let skip = matches!(e, MessagingError::InvalidMessage(_));
                    if attempt < self.max_attempts && !skip {
                        tracing::warn!(source = %source.name, from = %record.source, segment = %record.segment, position = %record.position, attempt, "external record failed, retrying: {e}");
                        tokio::select! {
                            _ = tokio::time::sleep(self.backoff) => {}
                            _ = cancel.cancelled() => return false,
                        }
                    } else {
                        tracing::error!(source = %source.name, from = %record.source, segment = %record.segment, position = %record.position, attempts = attempt, "giving up on a record of an external source: {e}");
                        return true;
                    }
                }
            }
        }
        true
    }

    async fn translate_and_dispatch(
        &self,
        source: &SourceRegistration,
        record: &ExternalRecord,
    ) -> Result<(), MessagingError> {
        let translated = catch_unwind(AssertUnwindSafe(|| (source.translator)(record)))
            .unwrap_or_else(|_| {
                Err(MessagingError::Transport(format!(
                    "the translator of external source '{}' panicked",
                    source.name
                )))
            })?;
        let Some(translated) = translated else {
            tracing::debug!(source = %source.name, segment = %record.segment, position = %record.position, "the translator ignored the record");
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
                .unwrap_or_else(|| format!("{}:{}", record.segment, record.position))
        );
        let mut hdrs = record.headers.clone();
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
