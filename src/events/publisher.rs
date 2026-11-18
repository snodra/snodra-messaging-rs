//! Relays pending outbox entries to the event store in order
//! ([`EventStore::publish`]), marking them sent or failed.
//!
//! Each poll claims a batch from the message store's outbox ([`OutboxStore::claim`]) under a
//! lease ([`OutboxPublisherSettings::lease`], `SNODRA_OUTBOX_LEASE_MS`) held by this publisher, so
//! two instances relaying one outbox never both send an entry while both live, and a crashed
//! one's entries are relayed after its lease. A batch stops before its lease could run out under
//! a send ([`OUTBOX_SEND_TIMEOUT`] before its end, by the injected clock); the next poll claims
//! again, which renews the lease of what this publisher still holds. Order holds per key for what
//! one instance relays (ADR 0028). A publish the event store rejects for good
//! ([`EventStoreError::Rejected`], or `Invalid`) marks the entry failed at once; any other failure
//! is tried again, up to the attempts.
//!
//! Metrics (ADR 0026): every publish attempt counts in `snodra.outbox.published{outcome}`; the
//! gauges `snodra.outbox.pending` and `snodra.outbox.oldest.age` read what the last poll saw (the
//! batch it claimed, or, when the batch is full, the store's count of every pending entry and its
//! oldest one, [`OutboxStore::pending`] and [`OutboxStore::oldest_pending`]), less what it relayed
//! since: no query of their own.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::error::MessagingError;
use crate::event_store::{EventStore, EventStoreError, OutgoingEvent, headers};
use crate::metrics::{Metrics, OUTBOX_OLDEST_AGE, OUTBOX_PENDING};
use crate::saga::Clock;
use crate::store::{OutboxEntry, OutboxStore};
use crate::time::{after, now_ms, rfc3339};

/// The most one publish takes (as Kotlin's `max.block.ms` 10 s + `delivery.timeout.ms` 30 s): one
/// still unanswered then counts as failed. The outbox lease must be longer.
pub const OUTBOX_SEND_TIMEOUT: Duration = Duration::from_secs(40);

/// What an [`OutboxPublisher`] relays, where to and how (`Config`'s `outbox_*` and events
/// settings, [`From<&Config>`]).
#[derive(Clone, Debug)]
pub struct OutboxPublisherSettings {
    /// The event store's source the events go to.
    pub source: String,
    /// The `source` header: the service name.
    pub service_name: String,
    pub poll_interval: Duration,
    pub batch_size: usize,
    /// Sends of one entry in all before it is given up.
    pub max_attempts: u32,
    /// How long a claimed batch stays this publisher's; longer than [`OUTBOX_SEND_TIMEOUT`].
    pub lease: Duration,
}

impl From<&Config> for OutboxPublisherSettings {
    fn from(config: &Config) -> Self {
        OutboxPublisherSettings {
            source: config.events_source.clone(),
            service_name: config.service_name.clone(),
            poll_interval: config.outbox_poll_interval,
            batch_size: config.outbox_batch_size,
            max_attempts: config.outbox_max_attempts,
            lease: config.outbox_lease,
        }
    }
}

pub struct OutboxPublisher {
    outbox: Arc<dyn OutboxStore>,
    /// This publisher's claims (a random id).
    owner: String,
    events: Arc<dyn EventStore>,
    source: String,
    service_name: String,
    poll_interval: Duration,
    batch_size: i64,
    max_attempts: i32,
    lease: Duration,
    /// What "now" is for a batch's deadline.
    clock: Clock,
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
    fn set(&self, pending: u64, oldest_ms: Option<i64>) {
        self.pending.store(pending, Ordering::Relaxed);
        self.oldest_ms
            .store(oldest_ms.unwrap_or(i64::MIN), Ordering::Relaxed);
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
                    o.observe(b.oldest_age(now_ms()), &[]);
                }
            })
            .build();
    }
}

impl OutboxPublisher {
    /// The relay of `outbox` to `events` as `settings` say, its batches timed by `clock`; its
    /// metrics are recorded on `metrics`. `InvalidMessage` for a lease not longer than a send.
    pub fn new(
        outbox: Arc<dyn OutboxStore>,
        events: Arc<dyn EventStore>,
        settings: OutboxPublisherSettings,
        clock: Clock,
        metrics: &Metrics,
    ) -> Result<Arc<OutboxPublisher>, MessagingError> {
        if settings.lease <= OUTBOX_SEND_TIMEOUT {
            return Err(MessagingError::InvalidMessage(format!(
                "the outbox lease ({:?}) must be longer than a send ({} s)",
                settings.lease,
                OUTBOX_SEND_TIMEOUT.as_secs()
            )));
        }
        let backlog = Arc::new(Backlog::default());
        Backlog::observe(Arc::downgrade(&backlog), metrics);
        Ok(Arc::new(OutboxPublisher {
            outbox,
            owner: uuid::Uuid::new_v4().to_string(),
            events,
            source: settings.source,
            service_name: settings.service_name,
            poll_interval: settings.poll_interval,
            batch_size: settings.batch_size as i64,
            max_attempts: settings.max_attempts as i32,
            lease: settings.lease,
            clock,
            metrics: metrics.clone(),
            backlog,
        }))
    }

    /// Polls until cancelled; a full batch is followed immediately by the next poll, and so is
    /// an append the store reports ([`OutboxStore::on_append`]).
    pub fn start(self: Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        let appended = Arc::new(tokio::sync::Notify::new());
        let woken = Arc::clone(&appended);
        let listener = self.outbox.on_append(Arc::new(move || woken.notify_one()));
        tokio::spawn(async move {
            // Registered for as long as the relay runs.
            let _listener = listener;
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
                    _ = appended.notified() => {}
                }
            }
        })
    }

    /// Claims one batch of pending entries and sends it sequentially; returns how many were
    /// sent. Stops at the first retriable failure so per-key order holds (the entry stays this
    /// publisher's and comes first in its next claim), and before the batch's lease could run out
    /// under the next send; an entry failing `max_attempts` times is marked failed and skipped
    /// from then on.
    pub async fn relay_once(&self) -> Result<usize, MessagingError> {
        let claimed_at_ms = self.clock.now_ms();
        let batch: Vec<OutboxEntry> = self
            .outbox
            .claim(&self.owner, self.lease, self.batch_size as usize)
            .await?;
        self.relay_batch(batch, claimed_at_ms, |event| Box::pin(self.send(event)))
            .await
    }

    /// Sends `batch`, claimed at `claimed_at_ms`, through `send`, in order; see
    /// [`relay_once`](Self::relay_once).
    async fn relay_batch<'s, F>(
        &'s self,
        batch: Vec<OutboxEntry>,
        claimed_at_ms: i64,
        mut send: F,
    ) -> Result<usize, MessagingError>
    where
        F: FnMut(OutgoingEvent) -> BoxFuture<'s, Result<(), EventStoreError>>,
    {
        // The backlog for the gauges: the batch, or every pending entry behind a full one (and the
        // oldest of them, which another instance may hold).
        let mut backlog = batch.len() as u64;
        let mut oldest = batch.first().map(|d| d.created_at_ms);
        if batch.len() as i64 >= self.batch_size {
            match self.outbox.pending().await {
                Ok(n) => backlog = n.max(backlog),
                Err(e) => tracing::debug!("outbox backlog not counted: {e}"),
            }
            match self.outbox.oldest_pending().await {
                Ok(Some(at)) => oldest = Some(oldest.map_or(at, |first| first.min(at))),
                Ok(None) => {}
                Err(e) => tracing::debug!("oldest outbox entry not read: {e}"),
            }
        }
        self.backlog.set(backlog, oldest);
        // An entry left the backlog (sent, or failed for good): the next of the batch is the
        // oldest now; past the batch, the last one stands in until the next poll.
        let left = |backlog: u64, i: usize| {
            let next = batch.get(i + 1).or(batch.get(i).filter(|_| backlog > 0));
            self.backlog.set(backlog, next.map(|d| d.created_at_ms));
        };
        // A send started from then on could outlast the lease: the rest waits for the next claim.
        let send_until_ms = after(claimed_at_ms, self.lease - OUTBOX_SEND_TIMEOUT);
        let mut sent = 0usize;
        for (i, d) in batch.iter().enumerate() {
            if self.clock.now_ms() >= send_until_ms {
                tracing::debug!(
                    sent,
                    left = batch.len() - i,
                    "outbox batch stops before its lease could run out"
                );
                break;
            }
            match send(self.event(d)).await {
                Ok(()) => {
                    self.metrics.outbox_published(true);
                    self.outbox.mark_sent(std::slice::from_ref(&d.id)).await?;
                    sent += 1;
                    backlog = backlog.saturating_sub(1);
                    left(backlog, i);
                }
                Err(e) => {
                    self.metrics.outbox_published(false);
                    let attempt = d.attempts + 1;
                    if attempt >= self.max_attempts || !is_retriable(&e) {
                        self.outbox.mark_failed(&d.id, &e.to_string()).await?;
                        tracing::error!(id = %d.id, r#type = %d.type_name, attempts = attempt, "outbox entry marked failed: {e}");
                        backlog = backlog.saturating_sub(1);
                        left(backlog, i);
                        continue;
                    }
                    self.outbox
                        .mark_attempt_failed(&d.id, &e.to_string())
                        .await?;
                    tracing::warn!(id = %d.id, r#type = %d.type_name, attempt, "outbox send failed, will retry: {e}");
                    break;
                }
            }
        }
        Ok(sent)
    }

    /// One event to the event store; no answer within [`OUTBOX_SEND_TIMEOUT`] is a failure.
    async fn send(&self, event: OutgoingEvent) -> Result<(), EventStoreError> {
        match tokio::time::timeout(OUTBOX_SEND_TIMEOUT, self.events.publish(&event)).await {
            Ok(sent) => sent,
            Err(_) => Err(EventStoreError::Failed(format!(
                "no answer within {} s",
                OUTBOX_SEND_TIMEOUT.as_secs()
            ))),
        }
    }

    /// The event of `d`: its headers, with `type`, `id`, `timestamp` and `source` (the service)
    /// set from the entry.
    fn event(&self, d: &OutboxEntry) -> OutgoingEvent {
        outgoing_event(d, &self.source, &self.service_name)
    }
}

/// The event the outbox entry `entry` of the service `service` becomes on the event store's
/// `source`: its headers, plus the type, the id, the timestamp (its creation) and the source (the
/// publishing service) headers.
pub(crate) fn outgoing_event(entry: &OutboxEntry, source: &str, service: &str) -> OutgoingEvent {
    let mut all = entry.headers.clone();
    all.insert(headers::TYPE.into(), entry.type_name.clone());
    all.insert(headers::ID.into(), entry.id.clone());
    all.insert(headers::TIMESTAMP.into(), rfc3339(entry.created_at_ms));
    all.insert(headers::SOURCE.into(), service.to_string());
    OutgoingEvent {
        source: source.to_string(),
        id: entry.id.clone(),
        type_name: entry.type_name.clone(),
        key: entry.key.clone(),
        headers: all,
        payload: entry.payload.clone(),
    }
}

/// Whether another publish of the event may succeed: not after the event store rejected it for
/// good (too large, not authorized, a destination it cannot have) or found it invalid.
fn is_retriable(e: &EventStoreError) -> bool {
    !matches!(
        e,
        EventStoreError::Rejected(_) | EventStoreError::Invalid(_)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::UNIX_EPOCH;

    use super::*;
    use crate::saga::ManualClock;
    use crate::store::{StoreError, Tx};

    /// An outbox that records which entries were marked sent, failed for good, or failed once.
    #[derive(Default)]
    struct Recorded {
        sent: Mutex<Vec<String>>,
        failed: Mutex<Vec<String>>,
        attempt_failed: Mutex<Vec<String>>,
    }

    fn done<'a, T: Send + 'a>(value: T) -> BoxFuture<'a, Result<T, StoreError>> {
        Box::pin(async move { Ok(value) })
    }

    impl OutboxStore for Recorded {
        fn append<'a>(
            &'a self,
            _: &'a mut Tx,
            _: OutboxEntry,
        ) -> BoxFuture<'a, Result<(), StoreError>> {
            done(())
        }
        fn append_now(&self, _: OutboxEntry) -> BoxFuture<'_, Result<(), StoreError>> {
            done(())
        }
        fn claim<'a>(
            &'a self,
            _: &'a str,
            _: Duration,
            _: usize,
        ) -> BoxFuture<'a, Result<Vec<OutboxEntry>, StoreError>> {
            done(Vec::new())
        }
        fn mark_sent<'a>(&'a self, ids: &'a [String]) -> BoxFuture<'a, Result<(), StoreError>> {
            self.sent.lock().unwrap().extend_from_slice(ids);
            done(())
        }
        fn mark_attempt_failed<'a>(
            &'a self,
            id: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, Result<(), StoreError>> {
            self.attempt_failed.lock().unwrap().push(id.to_string());
            done(())
        }
        fn mark_failed<'a>(
            &'a self,
            id: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, Result<(), StoreError>> {
            self.failed.lock().unwrap().push(id.to_string());
            done(())
        }
        fn purge(&self, _: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
            done(0)
        }
        fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
            done(0)
        }
        fn oldest_pending(&self) -> BoxFuture<'_, Result<Option<i64>, StoreError>> {
            done(None)
        }
    }

    fn entry(n: usize) -> OutboxEntry {
        OutboxEntry {
            id: format!("e-{n}"),
            type_name: "t.Happened".into(),
            key: None,
            headers: Default::default(),
            payload: b"{}".to_vec(),
            created_at_ms: 0,
            seq: n as i64,
            attempts: 0,
        }
    }

    /// An event store the relay never reaches in these tests: `relay_batch` gets its sends.
    struct Unreached;

    impl EventStore for Unreached {
        fn name(&self) -> &'static str {
            "unreached"
        }
        fn capabilities(&self) -> &[crate::event_store::Capability] {
            &[]
        }
        fn publish<'a>(
            &'a self,
            _: &'a OutgoingEvent,
        ) -> BoxFuture<'a, Result<(), EventStoreError>> {
            unreachable!("the tests send through relay_batch")
        }
        fn subscribe(
            &self,
            _: crate::event_store::SubscriptionSpec,
        ) -> BoxFuture<'_, Result<Box<dyn crate::event_store::Subscription>, EventStoreError>>
        {
            unreachable!()
        }
        fn admin(
            &self,
            _: &crate::event_store::SubscriptionSpec,
        ) -> Result<Box<dyn crate::event_store::SubscriptionAdmin>, EventStoreError> {
            unreachable!()
        }
        fn compare(&self, a: &str, b: &str) -> std::cmp::Ordering {
            a.cmp(b)
        }
    }

    fn settings(lease: Duration) -> OutboxPublisherSettings {
        OutboxPublisherSettings {
            source: "snodra.events".into(),
            service_name: "svc".into(),
            poll_interval: Duration::from_millis(100),
            batch_size: 10,
            max_attempts: 10,
            lease,
        }
    }

    fn publisher(outbox: Arc<Recorded>, clock: &ManualClock) -> Arc<OutboxPublisher> {
        OutboxPublisher::new(
            outbox,
            Arc::new(Unreached),
            settings(Duration::from_secs(120)),
            clock.clock(),
            &Metrics::global(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_rejected_event_is_given_up_at_once_a_failed_one_tried_again() {
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(1_793_700_000));
        let outbox = Arc::new(Recorded::default());
        let relay = publisher(Arc::clone(&outbox), &clock);
        let batch: Vec<OutboxEntry> = (0..4).map(entry).collect();
        let sent = relay
            .relay_batch(batch, clock.clock().now_ms(), |event| {
                Box::pin(async move {
                    match event.id.as_str() {
                        "e-0" => Ok(()),
                        "e-1" => Err(EventStoreError::Rejected("too large".into())),
                        "e-2" => Err(EventStoreError::Failed("broker down".into())),
                        _ => Ok(()),
                    }
                })
            })
            .await
            .unwrap();
        assert_eq!(sent, 1);
        assert_eq!(*outbox.sent.lock().unwrap(), ["e-0"]);
        assert_eq!(
            *outbox.failed.lock().unwrap(),
            ["e-1"],
            "rejected: failed at once"
        );
        assert_eq!(
            *outbox.attempt_failed.lock().unwrap(),
            ["e-2"],
            "failed: tried again, and the batch stops there (order per key)"
        );
    }

    #[tokio::test]
    async fn the_event_carries_the_entry_and_the_service_as_its_source_header() {
        let clock = ManualClock::new(UNIX_EPOCH);
        let relay = publisher(Arc::new(Recorded::default()), &clock);
        let mut e = entry(1);
        e.key = Some("k".into());
        e.headers.insert("correlationId".into(), "c-1".into());
        e.headers.insert("type".into(), "stale".into());
        let event = relay.event(&e);
        assert_eq!(event.source, "snodra.events");
        assert_eq!(
            (event.id.as_str(), event.type_name.as_str()),
            ("e-1", "t.Happened")
        );
        assert_eq!(event.key.as_deref(), Some("k"));
        assert_eq!(event.headers["type"], "t.Happened");
        assert_eq!(event.headers["id"], "e-1");
        assert_eq!(event.headers["source"], "svc");
        assert_eq!(event.headers["correlationId"], "c-1");
        assert_eq!(event.headers["timestamp"], "1970-01-01T00:00:00.000Z");
        assert_eq!(event.payload, b"{}");
    }

    #[tokio::test]
    async fn a_batch_stops_before_its_lease_could_run_out_under_a_send() {
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(1_793_700_000));
        let outbox = Arc::new(Recorded::default());
        let relay = publisher(Arc::clone(&outbox), &clock);
        let claimed_at = clock.clock().now_ms();
        let batch: Vec<OutboxEntry> = (0..5).map(entry).collect();
        // Each send takes 30 s: sends start at +0, +30 and +60 s; at +90 s a send could end past
        // the lease (120 s), so the deadline is +80 s.
        let ticking = clock.clone();
        let sent = relay
            .relay_batch(batch, claimed_at, move |_| {
                ticking.advance(Duration::from_secs(30));
                Box::pin(async { Ok(()) })
            })
            .await
            .unwrap();
        assert_eq!(sent, 3);
        assert_eq!(*outbox.sent.lock().unwrap(), ["e-0", "e-1", "e-2"]);

        // Fast sends: the whole batch.
        let claimed_at = clock.clock().now_ms();
        let batch: Vec<OutboxEntry> = (5..10).map(entry).collect();
        let sent = relay
            .relay_batch(batch, claimed_at, |_| Box::pin(async { Ok(()) }))
            .await
            .unwrap();
        assert_eq!(sent, 5);
    }

    #[test]
    fn a_lease_not_longer_than_a_send_is_refused() {
        let clock = ManualClock::new(UNIX_EPOCH);
        let e = OutboxPublisher::new(
            Arc::new(Recorded::default()),
            Arc::new(Unreached),
            settings(OUTBOX_SEND_TIMEOUT),
            clock.clock(),
            &Metrics::global(),
        )
        .err()
        .expect("refused");
        assert!(e.to_string().contains("longer than a send"), "{e}");
    }
}
