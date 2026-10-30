//! Consumes `kraken.events` for this service's group: one record at a time; the dedupe marker is
//! inserted first, then the handlers run, all in one Mongo transaction; the offset is committed after.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mongodb::bson::{DateTime, doc};
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::{Client, ClientSession, Database};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{PROCESSED_EVENTS_COLLECTION, headers};
use crate::codec;
use crate::context::{CURRENT, MessageContext};
use crate::error::MessagingError;
use crate::hub::{EventContext, HandlerRegistry};

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
    let Some(type_name) = hdrs.get(headers::TYPE).filter(|t| !t.is_empty()) else {
        return RecordDisposition::Skip("record without type header");
    };
    if !registry.has_event_handler(type_name) {
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
    client: Client,
    db: Database,
    registry: Arc<HandlerRegistry>,
    topic: String,
    max_attempts: u32,
    backoff: Duration,
}

enum Outcome {
    Handled,
    Duplicate,
}

impl EventConsumer {
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
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "true")
            .create()?;
        Ok(Arc::new(EventConsumer {
            consumer,
            client,
            db,
            registry,
            topic: topic.to_string(),
            max_attempts: max_attempts.max(1),
            backoff,
        }))
    }

    pub fn start(self: Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
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
                            self.handle_record(&msg).await;
                            if let Err(e) = self.consumer.commit_message(&msg, CommitMode::Async) {
                                tracing::warn!("offset commit failed: {e}");
                            }
                        }
                    }
                }
            }
        })
    }

    async fn handle_record(&self, msg: &BorrowedMessage<'_>) {
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
        let (type_name, id) = match classify(
            &hdrs,
            payload,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            &self.registry,
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
                return;
            }
            RecordDisposition::Handle { type_name, id } => (type_name, id),
        };
        if !hdrs.contains_key(headers::ID) {
            tracing::warn!(r#type = %type_name, id = %id, "record has no id header, using topic-partition-offset for idempotency");
        }
        for attempt in 1..=self.max_attempts {
            match self.process_once(&type_name, &id, &hdrs, payload).await {
                Ok(Outcome::Handled) => return,
                Ok(Outcome::Duplicate) => {
                    tracing::debug!(r#type = %type_name, id = %id, "duplicate event skipped");
                    return;
                }
                Err(e) => {
                    if attempt < self.max_attempts {
                        tracing::warn!(r#type = %type_name, id = %id, attempt, "event handler failed, retrying: {e}");
                        tokio::time::sleep(self.backoff).await;
                    } else {
                        tracing::error!(r#type = %type_name, id = %id, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(), attempts = attempt, "event handler failed, record skipped: {e}");
                    }
                }
            }
        }
    }

    async fn process_once(
        &self,
        type_name: &str,
        id: &str,
        hdrs: &HashMap<String, String>,
        payload: &[u8],
    ) -> Result<Outcome, MessagingError> {
        let mut session = self.client.start_session().await?;
        session.start_transaction().await?;
        let ctx = MessageContext {
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
        };
        // Marker first: a duplicate id never reaches the handler; a handler failure rolls the marker back.
        match self.mark_processed(&mut session, id, type_name).await {
            Ok(()) => {}
            Err(MarkError::Duplicate) => {
                let _ = session.abort_transaction().await;
                return Ok(Outcome::Duplicate);
            }
            Err(MarkError::Other(e)) => {
                let _ = session.abort_transaction().await;
                return Err(e);
            }
        }
        let session = Arc::new(Mutex::new(session));
        let result = self
            .run_handlers(type_name, payload, ctx, Arc::clone(&session))
            .await;
        let mut guard = session.lock().await;
        match result {
            Ok(()) => {
                guard.commit_transaction().await?;
                Ok(Outcome::Handled)
            }
            Err(e) => {
                let _ = guard.abort_transaction().await;
                Err(e)
            }
        }
    }

    async fn run_handlers(
        &self,
        type_name: &str,
        payload: &[u8],
        ctx: MessageContext,
        session: Arc<Mutex<ClientSession>>,
    ) -> Result<(), MessagingError> {
        let handlers: Vec<_> = self.registry.event_handlers(type_name).to_vec();
        for handler in handlers {
            let ectx = EventContext {
                message: ctx.clone(),
                session: Arc::clone(&session),
            };
            let bytes = payload.to_vec();
            let task =
                tokio::spawn(CURRENT.scope(ctx.clone(), async move { handler(bytes, ectx).await }));
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(join) if join.is_panic() => {
                    return Err(MessagingError::RemoteHandler {
                        type_name: type_name.to_string(),
                        message: "event handler panicked".into(),
                        details: vec![],
                    });
                }
                Err(join) => {
                    return Err(MessagingError::Transport(format!(
                        "event handler task failed: {join}"
                    )));
                }
            }
        }
        Ok(())
    }

    async fn mark_processed(
        &self,
        session: &mut ClientSession,
        id: &str,
        type_name: &str,
    ) -> Result<(), MarkError> {
        let marker = doc! { "_id": id, "type": type_name, "processedAt": DateTime::now() };
        match self
            .db
            .collection::<mongodb::bson::Document>(PROCESSED_EVENTS_COLLECTION)
            .insert_one(marker)
            .session(&mut *session)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if is_duplicate_key(&e) => Err(MarkError::Duplicate),
            Err(e) => Err(MarkError::Other(e.into())),
        }
    }
}

enum MarkError {
    Duplicate,
    Other(MessagingError),
}

fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    matches!(&*e.kind, ErrorKind::Write(WriteFailure::WriteError(w)) if w.code == 11000)
}
