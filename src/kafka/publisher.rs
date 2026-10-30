//! Relays pending outbox documents to Kafka in order, marking them sent or failed.

use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use mongodb::Database;
use mongodb::bson::{DateTime, doc};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::outbox::OutboxDocument;
use super::{OUTBOX_COLLECTION, headers};
use crate::error::MessagingError;

pub struct OutboxPublisher {
    db: Database,
    producer: FutureProducer,
    topic: String,
    source: String,
    poll_interval: Duration,
    batch_size: i64,
    max_attempts: i32,
}

impl OutboxPublisher {
    pub fn new(
        db: Database,
        bootstrap_servers: &str,
        topic: &str,
        source: &str,
        poll_interval: Duration,
        batch_size: usize,
        max_attempts: u32,
    ) -> Result<Arc<OutboxPublisher>, MessagingError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("enable.idempotence", "true")
            .create()?;
        Ok(Arc::new(OutboxPublisher {
            db,
            producer,
            topic: topic.to_string(),
            source: source.to_string(),
            poll_interval,
            batch_size: batch_size as i64,
            max_attempts: max_attempts as i32,
        }))
    }

    /// Polls until cancelled; a full batch is followed immediately by the next poll.
    pub fn start(self: Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let sent = match self.relay_once().await {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::warn!("outbox relay failed: {e}");
                        0
                    }
                };
                if sent as i64 >= self.batch_size {
                    continue;
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(self.poll_interval) => {}
                }
            }
        })
    }

    /// Sends one batch of pending documents sequentially; returns how many were sent. Stops at the
    /// first retriable failure so per-key order holds; a document failing `max_attempts` times is
    /// marked `failedAt` and skipped from then on.
    pub async fn relay_once(&self) -> Result<usize, MessagingError> {
        let coll = self.db.collection::<OutboxDocument>(OUTBOX_COLLECTION);
        let pending: Vec<OutboxDocument> = coll
            .find(doc! { "sentAt": null, "failedAt": null })
            .sort(doc! { "createdAt": 1, "seq": 1 })
            .limit(self.batch_size)
            .await?
            .try_collect()
            .await?;
        let mut sent = 0usize;
        for d in pending {
            let prepared = self.record(&d)?;
            let mut owned = OwnedHeaders::new();
            for (k, v) in &prepared.headers {
                owned = owned.insert(Header {
                    key: k,
                    value: Some(v),
                });
            }
            let mut record = FutureRecord::to(&self.topic)
                .payload(&prepared.payload)
                .headers(owned);
            if let Some(k) = &prepared.key {
                record = record.key(k);
            }
            match self
                .producer
                .send(record, Timeout::After(Duration::from_secs(10)))
                .await
            {
                Ok(_) => {
                    coll.update_one(
                        doc! { "_id": &d.id },
                        doc! { "$set": { "sentAt": DateTime::now() } },
                    )
                    .await?;
                    sent += 1;
                }
                Err((e, _)) => {
                    let attempt = d.attempts + 1;
                    if attempt >= self.max_attempts {
                        coll.update_one(doc! { "_id": &d.id }, doc! { "$inc": { "attempts": 1 }, "$set": { "failedAt": DateTime::now() } }).await?;
                        tracing::error!(id = %d.id, r#type = %d.type_name, attempts = attempt, "outbox document marked failed: {e}");
                        continue;
                    }
                    coll.update_one(doc! { "_id": &d.id }, doc! { "$inc": { "attempts": 1 } })
                        .await?;
                    tracing::warn!(id = %d.id, r#type = %d.type_name, attempt, "outbox send failed, will retry: {e}");
                    break;
                }
            }
        }
        Ok(sent)
    }

    fn record(&self, d: &OutboxDocument) -> Result<Prepared, MessagingError> {
        let payload = serde_json::to_vec(
            &mongodb::bson::Bson::Document(d.payload.clone()).into_relaxed_extjson(),
        )?;
        let mut all: Vec<(String, String)> = d
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        all.sort();
        all.retain(|(k, _)| {
            ![
                headers::TYPE,
                headers::ID,
                headers::TIMESTAMP,
                headers::SOURCE,
            ]
            .contains(&k.as_str())
        });
        all.push((headers::TYPE.into(), d.type_name.clone()));
        all.push((headers::ID.into(), d.id.clone()));
        all.push((headers::TIMESTAMP.into(), rfc3339(d.created_at)));
        all.push((headers::SOURCE.into(), self.source.clone()));
        Ok(Prepared {
            key: d.key.clone(),
            payload,
            headers: all,
        })
    }
}

struct Prepared {
    key: Option<String>,
    payload: Vec<u8>,
    headers: Vec<(String, String)>,
}

/// `createdAt` as Kotlin's `Instant.toString()` would print it (millisecond precision in BSON).
fn rfc3339(dt: DateTime) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(dt.timestamp_millis())
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_default()
}
