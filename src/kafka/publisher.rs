//! Relays pending outbox documents to Kafka in order, marking them sent or failed.
//!
//! Metrics (ADR 0026): every publish attempt counts in `kraken.outbox.published{outcome}`; the
//! gauges `kraken.outbox.pending` and `kraken.outbox.oldest.age` read what the last poll saw (the
//! batch it fetched, or a count of every pending document when the batch is full), less what it
//! relayed since: no query of their own.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
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
use crate::metrics::{Metrics, OUTBOX_OLDEST_AGE, OUTBOX_PENDING};

pub struct OutboxPublisher {
    db: Database,
    producer: FutureProducer,
    topic: String,
    source: String,
    poll_interval: Duration,
    batch_size: i64,
    max_attempts: i32,
    metrics: Metrics,
    backlog: Arc<Backlog>,
}

/// The pending documents as the relay last saw them, for the outbox gauges.
#[derive(Debug, Default)]
struct Backlog {
    pending: AtomicU64,
    /// `createdAt` of the oldest pending document, epoch milliseconds; `i64::MIN` when none.
    oldest_ms: AtomicI64,
}

impl Backlog {
    fn set(&self, pending: u64, oldest: Option<DateTime>) {
        self.pending.store(pending, Ordering::Relaxed);
        self.oldest_ms.store(
            oldest.map_or(i64::MIN, |at| at.timestamp_millis()),
            Ordering::Relaxed,
        );
    }

    /// The age of the oldest pending document at `now_ms`, in seconds; 0 when none.
    fn oldest_age(&self, now_ms: i64) -> f64 {
        match self.oldest_ms.load(Ordering::Relaxed) {
            i64::MIN => 0.0,
            at => (now_ms - at).max(0) as f64 / 1000.0,
        }
    }

    /// Registers the outbox gauges on `metrics`; they stop reporting once `backlog` is dropped.
    fn observe(backlog: Weak<Backlog>, metrics: &Metrics) {
        let pending = backlog.clone();
        metrics
            .meter()
            .u64_observable_gauge(OUTBOX_PENDING)
            .with_description(
                "Outbox documents not yet published, as the relay's last poll saw them",
            )
            .with_callback(move |o| {
                if let Some(b) = pending.upgrade() {
                    o.observe(b.pending.load(Ordering::Relaxed), &[]);
                }
            })
            .build();
        metrics
            .meter()
            .f64_observable_gauge(OUTBOX_OLDEST_AGE)
            .with_unit("s")
            .with_description("Age of the oldest outbox document not yet published, 0 when none")
            .with_callback(move |o| {
                if let Some(b) = backlog.upgrade() {
                    o.observe(b.oldest_age(DateTime::now().timestamp_millis()), &[]);
                }
            })
            .build();
    }
}

impl OutboxPublisher {
    /// The relay of `db`'s outbox to `topic`; its metrics are recorded on `metrics`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Database,
        bootstrap_servers: &str,
        topic: &str,
        source: &str,
        poll_interval: Duration,
        batch_size: usize,
        max_attempts: u32,
        metrics: &Metrics,
    ) -> Result<Arc<OutboxPublisher>, MessagingError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            // Same budget as the Kotlin producer (max.block.ms 10 s + delivery.timeout.ms 30 s): a broker
            // restart must not exhaust the 10 attempts.
            .set("message.timeout.ms", "30000")
            .set("request.timeout.ms", "20000")
            .set("enable.idempotence", "true")
            .create()?;
        let backlog = Arc::new(Backlog::default());
        Backlog::observe(Arc::downgrade(&backlog), metrics);
        Ok(Arc::new(OutboxPublisher {
            db,
            producer,
            topic: topic.to_string(),
            source: source.to_string(),
            poll_interval,
            batch_size: batch_size as i64,
            max_attempts: max_attempts as i32,
            metrics: metrics.clone(),
            backlog,
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
        let pending = doc! { "sentAt": null, "failedAt": null };
        let batch: Vec<OutboxDocument> = coll
            .find(pending.clone())
            .sort(doc! { "createdAt": 1, "seq": 1 })
            .limit(self.batch_size)
            .await?
            .try_collect()
            .await?;
        // The backlog for the gauges: the batch, or every pending document behind a full one.
        let mut backlog = batch.len() as u64;
        if batch.len() as i64 >= self.batch_size {
            match coll.count_documents(pending).await {
                Ok(n) => backlog = n.max(backlog),
                Err(e) => tracing::debug!("outbox backlog not counted: {e}"),
            }
        }
        self.backlog
            .set(backlog, batch.first().map(|d| d.created_at));
        // A document left the backlog (sent, or failed for good): the next of the batch is the
        // oldest now; past the batch, the last one stands in until the next poll.
        let left = |backlog: u64, i: usize| {
            let next = batch.get(i + 1).or(batch.get(i).filter(|_| backlog > 0));
            self.backlog.set(backlog, next.map(|d| d.created_at));
        };
        let mut sent = 0usize;
        for (i, d) in batch.iter().enumerate() {
            let prepared = self.record(d)?;
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
                .send(record, Timeout::After(Duration::from_secs(40)))
                .await
            {
                Ok(_) => {
                    self.metrics.outbox_published(true);
                    coll.update_one(
                        doc! { "_id": &d.id },
                        doc! { "$set": { "sentAt": DateTime::now() } },
                    )
                    .await?;
                    sent += 1;
                    backlog = backlog.saturating_sub(1);
                    left(backlog, i);
                }
                Err((e, _)) => {
                    self.metrics.outbox_published(false);
                    let attempt = d.attempts + 1;
                    if attempt >= self.max_attempts || !is_retriable(&e) {
                        coll.update_one(doc! { "_id": &d.id }, doc! { "$inc": { "attempts": 1 }, "$set": { "failedAt": DateTime::now() } }).await?;
                        tracing::error!(id = %d.id, r#type = %d.type_name, attempts = attempt, "outbox document marked failed: {e}");
                        backlog = backlog.saturating_sub(1);
                        left(backlog, i);
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

/// Delivery failures no retry can fix (Kotlin's NON_RETRIABLE set): oversized or invalid records,
/// unknown or unauthorized topics.
fn is_retriable(e: &rdkafka::error::KafkaError) -> bool {
    use rdkafka::types::RDKafkaErrorCode as Code;
    !matches!(
        e.rdkafka_error_code(),
        Some(
            Code::MessageSizeTooLarge
                | Code::InvalidMessage
                | Code::InvalidMessageSize
                | Code::InvalidTopic
                | Code::TopicAuthorizationFailed
                | Code::ClusterAuthorizationFailed
                | Code::InvalidRecord
        )
    )
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
