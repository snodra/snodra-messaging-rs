//! Consumes `kraken.events` for this service's group: one record at a time; the dedupe marker is
//! inserted first, then the handlers run, all in one Mongo transaction; the offset is committed after.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mongodb::{Client, Database};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::dispatch::{LocalEventDispatcher, NoSagas, Outcome, SagaStage};
use super::headers;
use crate::codec;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::hub::HandlerRegistry;

/// What to do with a record, decided before any database work.
#[derive(Debug, PartialEq, Eq)]
pub enum RecordDisposition {
    /// Commit the offset and move on; the reason is logged (never the payload).
    Skip(&'static str),
    Handle {
        type_name: String,
        id: String,
    },
}

/// Mirrors `EventConsumer.kt`: no `type` header, a type nobody handles, or a non-object payload are
/// skipped; a missing `id` header falls back to `<topic>-<partition>-<offset>`.
pub fn classify(
    hdrs: &HashMap<String, String>,
    payload: &[u8],
    topic: &str,
    partition: i32,
    offset: i64,
    registry: &HandlerRegistry,
) -> RecordDisposition {
    classify_with(hdrs, payload, topic, partition, offset, |t| {
        registry.has_event_handler(t)
    })
}

/// [`classify`] with the interest in a type given as a predicate: the dispatcher's, which is an
/// ordinary event handler or a saga type (a record only a saga wants is handled, not skipped).
pub fn classify_with(
    hdrs: &HashMap<String, String>,
    payload: &[u8],
    topic: &str,
    partition: i32,
    offset: i64,
    interested: impl Fn(&str) -> bool,
) -> RecordDisposition {
    let Some(type_name) = hdrs.get(headers::TYPE).filter(|t| !t.is_empty()) else {
        return RecordDisposition::Skip("record without type header");
    };
    if !interested(type_name) {
        return RecordDisposition::Skip("no handler for this type");
    }
    if !codec::is_json_object(payload) {
        return RecordDisposition::Skip("payload is not a JSON object");
    }
    let id = hdrs
        .get(headers::ID)
        .filter(|i| !i.is_empty())
        .cloned()
        .unwrap_or_else(|| format!("{topic}-{partition}-{offset}"));
    RecordDisposition::Handle {
        type_name: type_name.clone(),
        id,
    }
}

pub struct EventConsumer {
    consumer: StreamConsumer,
    dispatcher: Arc<LocalEventDispatcher>,
    topic: String,
    max_attempts: u32,
    backoff: Duration,
}

impl EventConsumer {
    /// A consumer of the ordinary `event` handlers of `registry`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bootstrap_servers: &str,
        group_id: &str,
        topic: &str,
        client: Client,
        db: Database,
        registry: Arc<HandlerRegistry>,
        max_attempts: u32,
        backoff: Duration,
    ) -> Result<Arc<EventConsumer>, MessagingError> {
        Self::with_saga_stage(
            bootstrap_servers,
            group_id,
            topic,
            client,
            db,
            registry,
            Arc::new(NoSagas),
            max_attempts,
            backoff,
        )
    }

    /// [`new`](Self::new) with a saga stage: a record no ordinary handler wants but the saga stage
    /// is interested in is handled too, and every record goes through the stage after the ordinary
    /// handlers (see [`LocalEventDispatcher`]).
    #[allow(clippy::too_many_arguments)]
    pub fn with_saga_stage(
        bootstrap_servers: &str,
        group_id: &str,
        topic: &str,
        client: Client,
        db: Database,
        registry: Arc<HandlerRegistry>,
        stage: Arc<dyn SagaStage>,
        max_attempts: u32,
        backoff: Duration,
    ) -> Result<Arc<EventConsumer>, MessagingError> {
        Self::with_dispatcher(
            bootstrap_servers,
            group_id,
            topic,
            Arc::new(LocalEventDispatcher::new(client, db, registry, stage)),
            max_attempts,
            backoff,
        )
    }

    /// A consumer that dispatches through `dispatcher`, shared with the other local entry points
    /// (the partner topics, the webhook ingress, the saga relay's failure notices).
    pub fn with_dispatcher(
        bootstrap_servers: &str,
        group_id: &str,
        topic: &str,
        dispatcher: Arc<LocalEventDispatcher>,
        max_attempts: u32,
        backoff: Duration,
    ) -> Result<Arc<EventConsumer>, MessagingError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "true")
            .create()?;
        Ok(Arc::new(EventConsumer {
            consumer,
            dispatcher,
            topic: topic.to_string(),
            max_attempts: max_attempts.max(1),
            backoff,
        }))
    }

    pub fn start(self: Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            let cancel_for_records = cancel.clone();
            if let Err(e) = self.consumer.subscribe(&[self.topic.as_str()]) {
                tracing::error!("cannot subscribe to {}: {e}", self.topic);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    received = self.consumer.recv() => match received {
                        Err(e) => {
                            tracing::warn!("kafka receive error: {e}");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        Ok(msg) => {
                            if !self.handle_record(&msg, &cancel_for_records).await {
                                break; // shutting down mid-retry: leave the offset uncommitted for the next run
                            }
                            if let Err(e) = self.consumer.commit_message(&msg, CommitMode::Async) {
                                tracing::warn!("offset commit failed: {e}");
                            }
                        }
                    }
                }
            }
        })
    }

    /// Returns false when cancelled while retrying (the record is redelivered after a restart).
    async fn handle_record(&self, msg: &BorrowedMessage<'_>, cancel: &CancellationToken) -> bool {
        let hdrs: HashMap<String, String> = msg
            .headers()
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
            .unwrap_or_default();
        let payload = msg.payload().unwrap_or(&[]);
        let (type_name, id) = match classify_with(
            &hdrs,
            payload,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            |t| self.dispatcher.interested(t),
        ) {
            RecordDisposition::Skip(reason) => {
                if reason != "no handler for this type" {
                    tracing::warn!(
                        topic = msg.topic(),
                        partition = msg.partition(),
                        offset = msg.offset(),
                        "skipping record: {reason}"
                    );
                }
                return true;
            }
            RecordDisposition::Handle { type_name, id } => (type_name, id),
        };
        if !hdrs.contains_key(headers::ID) {
            tracing::warn!(r#type = %type_name, id = %id, "record has no id header, using topic-partition-offset for idempotency");
        }
        for attempt in 1..=self.max_attempts {
            let ctx = message_context(&type_name, &id, &hdrs);
            match self
                .dispatcher
                .dispatch(&id, &type_name, payload, &ctx)
                .await
            {
                Ok(Outcome::Handled | Outcome::Duplicate | Outcome::NoInterest) => return true,
                Ok(Outcome::Unclaimed) => {
                    tracing::debug!(r#type = %type_name, id = %id, "no saga instance applied the event");
                    return true;
                }
                Err(e) => {
                    if attempt < self.max_attempts {
                        tracing::warn!(r#type = %type_name, id = %id, attempt, "event handler failed, retrying: {e}");
                        tokio::select! {
                            _ = tokio::time::sleep(self.backoff) => {}
                            _ = cancel.cancelled() => return false,
                        }
                    } else {
                        tracing::error!(r#type = %type_name, id = %id, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(), attempts = attempt, "event handler failed, record skipped: {e}");
                    }
                }
            }
        }
        true
    }
}

/// What the handlers of a record see: its id and type, the correlation id header (else the id) and
/// every header.
pub(crate) fn message_context(
    type_name: &str,
    id: &str,
    hdrs: &HashMap<String, String>,
) -> MessageContext {
    MessageContext {
        message_id: id.to_string(),
        type_name: type_name.to_string(),
        correlation_id: hdrs
            .get(headers::CORRELATION_ID)
            .filter(|c| !c.is_empty())
            .cloned()
            .unwrap_or_else(|| id.to_string()),
        identity: None,
        authorization: None,
        headers: hdrs.clone(),
    }
}
