//! The saga dispatch relay: sends what sagas recorded in `saga_dispatch` after their transactions
//! committed, in order per saga (ADR 0021 rulings 7-11, 28-31, 34).
//!
//! The saga-side outbox lives in the message store, behind [`SagaDispatchStore`]. A poll finds the
//! sagas whose first pending entry (lowest `seq` neither dispatched nor failed) is due, oldest
//! work first (ruling 31), claims each one with a lease (so two service instances never dispatch
//! one saga's entries at once) and works through its entries
//! in `seq` order; different sagas go in parallel. An ended saga's entries are dispatched like any.
//!
//! - An EVENT entry is inserted into the outbox and marked dispatched **in one transaction**:
//!   exactly once. One whose payload cannot become an outbox entry (not a JSON object, or one the
//!   store cannot hold, as a number BSON cannot) fails for good (`failed_at`, `INVALID`, logged at
//!   error) and the saga's later entries continue (ruling 69).
//! - A COMMAND entry is sent as raw bytes by wire type name ([`RawCommandSender`]; the sender owns
//!   the no-retry rule, ruling 29) with the headers `idempotency-key` = `<sagaId>:<seq>` (the
//!   entry's id) and `saga-id`, the entry's routing key and the correlation id of the message that
//!   caused it as the request's correlation, with no caller identity. It is marked dispatched
//!   after the reply, so a crash in between re-sends it with the same key (an idempotent adapter
//!   then runs its body once).
//! - A transient failure counts an attempt and delays the entry (`nextAttemptAt`), holding back
//!   that saga's later entries only; after [`RelaySettings::max_attempts`] it becomes
//!   `EXHAUSTED`. A permanent failure is a known outcome: it is **persisted first** (`failedAt`,
//!   `failureReason`, `failureMessage` and `noticePending`), so the command is never sent again,
//!   and then the [`SagaCommandFailed`] is dispatched locally (dedupe `failed:<entry id>`). A
//!   notice that cannot be delivered (an ordinary handler fails, the store is down) is retried by
//!   the poll with its own bounded backoff (`noticeAttempts`, `noticeNextAt`;
//!   `max_attempts` in all, then logged at error and counted). Later entries of the saga continue
//!   after a failed one: the saga decides what the failure means (ruling 9).
//! - An error outside the send (the store is down, a mark fails) leaves the entry untouched: it is
//!   due again at the next poll.
//! - A send that is alive owns its saga (ruling 30): the lease is renewed every third of
//!   [`RelaySettings::lease`] while it runs, a saga and a notice already in flight on this
//!   instance are skipped, and the polling task never waits for sends.
//!
//! [`poll_once`](SagaDispatchRelay::poll_once) does one pass and waits for exactly the work it
//! submitted (what tests call); [`start`](SagaDispatchRelay::start) runs the loop as a tokio task
//! until its `CancellationToken` is cancelled, then waits for the sends in flight (abandoning them
//! after 15 s: their leases expire).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::clock::Clock;
use super::failed::{FailureReason, SagaCommandFailed};
use super::metrics::SagaMetrics;
use super::ports::{DispatchFailure, SagaDispatchStore};
use super::raw::{RawCommandSender, RawOutcome};
use super::runtime::DISPATCH_CORRELATION_HEADER;
use super::store::{DispatchKind, SagaDispatchDocument};
use crate::context::{CURRENT, MessageContext};
use crate::error::MessagingError;
use crate::hub::SendOptions;
use crate::kafka::{EventBus, LocalEventDispatcher, dispatch::Outcome};
use crate::retry::RetryPolicy;
use crate::store::TxProvider;
use crate::time::{after, rfc3339};
use crate::types::MessageType;

const STOP_JOIN: Duration = Duration::from_secs(15);
const MIN_RENEW_PERIOD: Duration = Duration::from_millis(10);

/// Settings of the [`SagaDispatchRelay`] (`KRAKEN_SAGAS_RELAY_*`): how often it polls, how long a
/// saga's dispatch lease holds, and the transient retry of a command (up to `max_attempts`
/// attempts in all, then `EXHAUSTED`, with `initial_backoff` growing by `multiplier` up to
/// `max_backoff`). `parallelism` sagas are dispatched at a time, at most `batch_size` of them per
/// poll.
#[derive(Clone, Debug)]
pub struct RelaySettings {
    pub poll_interval: Duration,
    pub lease: Duration,
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub multiplier: f64,
    pub max_backoff: Duration,
    pub parallelism: usize,
    pub batch_size: usize,
}

impl Default for RelaySettings {
    fn default() -> Self {
        RelaySettings {
            poll_interval: Duration::from_millis(500),
            lease: Duration::from_secs(30),
            max_attempts: 10,
            initial_backoff: Duration::from_secs(1),
            multiplier: 2.0,
            max_backoff: Duration::from_secs(300),
            parallelism: 4,
            batch_size: 100,
        }
    }
}

impl RelaySettings {
    /// `InvalidMessage` unless the intervals are positive, `parallelism` and `batch_size` are at
    /// least 1 and the retry numbers are valid.
    pub fn validate(&self) -> Result<(), MessagingError> {
        let invalid = |what: String| Err(MessagingError::InvalidMessage(what));
        if self.poll_interval.is_zero() {
            return invalid("relay poll interval must be positive".into());
        }
        if self.lease.is_zero() {
            return invalid("relay lease must be positive".into());
        }
        if self.parallelism < 1 || self.batch_size < 1 {
            return invalid("relay parallelism and batch size must be at least 1".into());
        }
        self.retry_policy().validate()
    }

    /// The backoff between the attempts of one command.
    pub fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: self.max_attempts,
            initial_backoff: self.initial_backoff,
            multiplier: self.multiplier,
            max_backoff: self.max_backoff,
        }
    }
}

/// See the module docs.
pub struct SagaDispatchRelay {
    tx: Arc<dyn TxProvider>,
    queue: Arc<dyn SagaDispatchStore>,
    commands: Arc<dyn RawCommandSender>,
    events: EventBus,
    local: Arc<LocalEventDispatcher>,
    clock: Clock,
    settings: RelaySettings,
    metrics: Arc<dyn SagaMetrics>,
    owner: String,
    in_flight: Mutex<HashMap<String, AbortHandle>>,
    permits: Arc<Semaphore>,
    tracker: TaskTracker,
    stopping: CancellationToken,
    fail_next_mark: AtomicBool,
    fail_next_notice_write: AtomicBool,
}

impl SagaDispatchRelay {
    #[allow(clippy::too_many_arguments)]
    /// The relay of `queue`, appending events to the outbox of `events` in `tx`'s transactions
    /// (one store's).
    pub fn new(
        tx: Arc<dyn TxProvider>,
        queue: Arc<dyn SagaDispatchStore>,
        commands: Arc<dyn RawCommandSender>,
        events: EventBus,
        local: Arc<LocalEventDispatcher>,
        clock: Clock,
        settings: RelaySettings,
        metrics: Arc<dyn SagaMetrics>,
    ) -> Result<Arc<SagaDispatchRelay>, MessagingError> {
        settings.validate()?;
        Ok(Arc::new(SagaDispatchRelay {
            tx,
            queue,
            commands,
            events,
            local,
            clock,
            permits: Arc::new(Semaphore::new(settings.parallelism)),
            settings,
            metrics,
            owner: uuid::Uuid::new_v4().to_string(),
            in_flight: Mutex::new(HashMap::new()),
            tracker: TaskTracker::new(),
            stopping: CancellationToken::new(),
            fail_next_mark: AtomicBool::new(false),
            fail_next_notice_write: AtomicBool::new(false),
        }))
    }

    /// Test seam: the next write that marks an entry dispatched fails (the crash between sending
    /// or publishing and marking).
    #[doc(hidden)]
    pub fn fail_next_mark(&self) {
        self.fail_next_mark.store(true, Ordering::SeqCst);
    }

    /// Test seam: the next write that settles or reschedules a failure notice fails (the store
    /// went away right after the notice was dispatched).
    #[doc(hidden)]
    pub fn fail_next_notice_write(&self) {
        self.fail_next_notice_write.store(true, Ordering::SeqCst);
    }

    fn check_notice_write_injection(&self) -> Result<(), MessagingError> {
        if self.fail_next_notice_write.swap(false, Ordering::SeqCst) {
            return Err(MessagingError::Storage(
                "injected: the failure notice write failed".into(),
            ));
        }
        Ok(())
    }

    fn now(&self) -> i64 {
        self.clock.now_ms()
    }

    /// One synchronous pass: hands the due failure notices and sagas to the workers
    /// ([`submit_due`](Self::submit_due)) and waits for exactly those to finish; returns how many
    /// entries and notices they dealt with.
    pub async fn poll_once(self: &Arc<Self>) -> Result<usize, MessagingError> {
        let mut dealt = 0;
        for handle in self.submit_due().await? {
            dealt += handle.await.unwrap_or(0);
        }
        Ok(dealt)
    }

    /// Hands every claimable due saga and every due failure notice to the workers and returns at
    /// once, **never waiting for the sends** and skipping what is already in flight on this
    /// instance: one saga whose adapter call hangs holds back only itself. While a saga's send is
    /// alive its lease is renewed (see `send_command`), so no other instance takes the saga over
    /// and sends its entry again.
    async fn submit_due(self: &Arc<Self>) -> Result<Vec<JoinHandle<usize>>, MessagingError> {
        let mut handles = Vec::new();
        for notice in self.claim_due_notices().await? {
            let this = Arc::clone(self);
            let key = format!("notice:{}", notice.id);
            handles.extend(self.submit(
                key,
                async move { this.deliver_notice_quietly(notice).await },
            ));
        }
        for saga_id in self.due_sagas().await? {
            let this = Arc::clone(self);
            let key = format!("saga:{saga_id}");
            handles.extend(self.submit(key, async move { this.dispatch_saga(&saga_id).await }));
        }
        Ok(handles)
    }

    /// Runs `work` on a worker unless `key` is already in flight (then `None`); the key is
    /// released when it finishes. At most `parallelism` run at a time.
    fn submit(
        self: &Arc<Self>,
        key: String,
        work: impl Future<Output = usize> + Send + 'static,
    ) -> Option<JoinHandle<usize>> {
        let mut in_flight = self.in_flight.lock().expect("in-flight lock");
        if in_flight.contains_key(&key) {
            return None;
        }
        let this = Arc::clone(self);
        let released = key.clone();
        let handle = self.tracker.spawn(async move {
            let _permit = this.permits.acquire().await.ok();
            let dealt = work.await;
            this.in_flight
                .lock()
                .expect("in-flight lock")
                .remove(&released);
            dealt
        });
        in_flight.insert(key, handle.abort_handle());
        Some(handle)
    }

    fn in_flight_sagas(&self) -> Vec<String> {
        self.in_flight
            .lock()
            .expect("in-flight lock")
            .keys()
            .filter_map(|k| k.strip_prefix("saga:").map(str::to_string))
            .collect()
    }

    /// The sagas whose first pending entry is due, oldest work first.
    async fn due_sagas(&self) -> Result<Vec<String>, MessagingError> {
        Ok(self
            .queue
            .due_sagas(
                self.now(),
                &self.in_flight_sagas(),
                self.settings.batch_size,
            )
            .await?)
    }

    /// Works through one saga's entries while this relay holds its lease; the number of entries
    /// dealt with.
    async fn dispatch_saga(&self, saga_id: &str) -> usize {
        match self.claim(saga_id).await {
            Ok(true) => {}
            Ok(false) => return 0,
            Err(e) => {
                tracing::warn!("Claiming the dispatch lease of saga {saga_id} failed: {e}");
                return 0;
            }
        }
        let mut dealt = 0;
        let outcome: Result<(), MessagingError> = async {
            while !self.stopping.is_cancelled() {
                let Some(entry) = self.next_pending(saga_id).await? else {
                    break;
                };
                if entry
                    .next_attempt_at_ms
                    .is_some_and(|next| next > self.now())
                {
                    break;
                }
                if !self.renew(saga_id).await? {
                    tracing::warn!("Lost the dispatch lease of saga {saga_id} while dispatching");
                    break;
                }
                dealt += 1;
                if !self.dispatch(&entry).await? {
                    break;
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = outcome {
            tracing::warn!("Dispatching saga {saga_id} failed, trying again at the next poll: {e}");
        }
        self.release(saga_id).await;
        dealt
    }

    /// Dispatches `entry`; false when this saga's turn must end (the entry waits).
    async fn dispatch(&self, entry: &SagaDispatchDocument) -> Result<bool, MessagingError> {
        match entry.kind {
            DispatchKind::Event => {
                self.publish_event(entry).await?;
                Ok(true)
            }
            DispatchKind::Command => self.send_command(entry).await,
        }
    }

    /// The outbox append and the mark, in one transaction.
    async fn publish_event(&self, entry: &SagaDispatchDocument) -> Result<(), MessagingError> {
        let correlation = entry
            .headers
            .get(DISPATCH_CORRELATION_HEADER)
            .cloned()
            .unwrap_or_else(|| entry.id.clone());
        let outbox = match self.events.raw_entry(
            &entry.type_name,
            entry.routing_key.as_deref(),
            &entry.payload,
            &correlation,
        ) {
            Ok(outbox) => outbox,
            // The payload can never become an outbox entry: retrying would hold back the saga's
            // later entries forever, so the entry fails for good (ruling 69).
            Err(e) => return self.fail_event(entry, &e.to_string()).await,
        };
        let mut tx = self.tx.begin().await?;
        // What only the store refuses (MongoDB: a number BSON cannot hold) can never be published
        // either.
        let refused = match self.events.publish_raw_in(outbox, &mut tx).await {
            Err(MessagingError::InvalidMessage(why)) => Some(why),
            Err(e) => {
                self.tx.abort(&mut tx).await;
                return Err(e);
            }
            Ok(()) => None,
        };
        if let Some(why) = refused {
            self.tx.abort(&mut tx).await;
            return self.fail_event(entry, &why).await;
        }
        let result: Result<(), MessagingError> = async {
            self.check_mark_injection()?;
            self.queue
                .mark_dispatched(&mut tx, &entry.id, self.now())
                .await?;
            self.tx.commit(&mut tx).await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.tx.abort(&mut tx).await;
        }
        result
    }

    /// Gives up an EVENT entry whose payload cannot become an outbox entry: `failed_at`, the
    /// reason `INVALID` and the message, logged at error. No failure notice: `SagaCommandFailed`
    /// is about commands. The saga's later entries continue, as after a failed command (ruling 9).
    async fn fail_event(
        &self,
        entry: &SagaDispatchDocument,
        message: &str,
    ) -> Result<(), MessagingError> {
        self.queue
            .mark_failed(
                &entry.id,
                &DispatchFailure {
                    attempts: None,
                    reason: FailureReason::Invalid,
                    message: message.to_string(),
                    failed_at_ms: self.now(),
                    notice_at_ms: None,
                },
            )
            .await?;
        tracing::error!(
            "Saga {} event {} ({}) can never be published, it is given up: {message}",
            entry.saga_id,
            entry.id,
            entry.type_name
        );
        Ok(())
    }

    fn check_mark_injection(&self) -> Result<(), MessagingError> {
        if self.fail_next_mark.swap(false, Ordering::SeqCst) {
            return Err(MessagingError::Storage(
                "crashed before marking the entry dispatched".into(),
            ));
        }
        Ok(())
    }

    async fn mark_dispatched(&self, entry: &SagaDispatchDocument) -> Result<(), MessagingError> {
        self.check_mark_injection()?;
        let mut tx = self.tx.plain().await?;
        self.queue
            .mark_dispatched(&mut tx, &entry.id, self.now())
            .await?;
        Ok(())
    }

    async fn send_command(&self, entry: &SagaDispatchDocument) -> Result<bool, MessagingError> {
        let correlation = entry
            .headers
            .get(DISPATCH_CORRELATION_HEADER)
            .cloned()
            .unwrap_or_else(|| entry.id.clone());
        let mut headers: HashMap<String, String> = entry
            .headers
            .iter()
            .filter(|(k, _)| k.as_str() != DISPATCH_CORRELATION_HEADER)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        headers.insert("idempotency-key".into(), entry.id.clone());
        headers.insert("saga-id".into(), entry.saga_id.clone());
        let options = SendOptions {
            routing_key: entry.routing_key.clone(),
            retry: Some(RetryPolicy::NONE),
            headers,
            ..Default::default()
        };
        // The correlation id of the message that caused the entry is the request's correlation;
        // nothing else of a caller (no identity, no token) is in scope.
        let ctx = MessageContext {
            correlation_id: correlation,
            ..Default::default()
        };
        let send = CURRENT.scope(
            ctx,
            self.commands.send_raw(
                &entry.type_name,
                entry.payload.clone().into_bytes(),
                options,
            ),
        );
        tokio::pin!(send);
        let period = (self.settings.lease / 3).max(MIN_RENEW_PERIOD);
        let mut beat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        let outcome = loop {
            tokio::select! {
                outcome = &mut send => break outcome,
                _ = beat.tick() => self.keep_lease(&entry.saga_id).await,
            }
        };
        match outcome {
            RawOutcome::Success => {
                self.mark_dispatched(entry).await?;
                Ok(true)
            }
            RawOutcome::Transient(message) => self.schedule_retry(entry, &message).await,
            RawOutcome::Permanent(reason, message) => {
                self.fail(entry, reason, &message, entry.attempts).await?;
                Ok(true)
            }
        }
    }

    /// A transient failure: another attempt later, or `EXHAUSTED` when the limit is reached.
    /// False: the entry waits.
    async fn schedule_retry(
        &self,
        entry: &SagaDispatchDocument,
        error: &str,
    ) -> Result<bool, MessagingError> {
        let attempts = entry.attempts + 1;
        if attempts as u32 >= self.settings.max_attempts {
            self.fail(
                entry,
                FailureReason::Exhausted,
                &format!("gave up after {attempts} attempts, the last failure was: {error}"),
                attempts,
            )
            .await?;
            return Ok(true);
        }
        let next = after(
            self.now(),
            self.settings
                .retry_policy()
                .jittered_backoff(attempts as u32),
        );
        self.queue.schedule_retry(&entry.id, attempts, next).await?;
        tracing::warn!(
            "Command {} ({}) failed attempt {attempts} of {}, next at {}: {error}",
            entry.id,
            entry.type_name,
            self.settings.max_attempts,
            rfc3339(next)
        );
        Ok(false)
    }

    /// Gives the entry up with a known outcome. The outcome is persisted first (`failedAt`, the
    /// reason and message, and a pending failure notice reserved for this instance), so nothing
    /// that goes wrong afterwards can make the command be sent again; the notice is then
    /// delivered, and retried on its own when that fails.
    async fn fail(
        &self,
        entry: &SagaDispatchDocument,
        reason: FailureReason,
        message: &str,
        attempts: i32,
    ) -> Result<(), MessagingError> {
        let now = self.now();
        let reason_text = reason_name(reason);
        self.queue
            .mark_failed(
                &entry.id,
                &DispatchFailure {
                    attempts: Some(attempts),
                    reason,
                    message: message.to_string(),
                    failed_at_ms: now,
                    notice_at_ms: Some(after(now, self.settings.lease)),
                },
            )
            .await?;
        tracing::warn!(
            "Saga {} command {} ({}) failed with {reason_text}: {message}",
            entry.saga_id,
            entry.id,
            entry.type_name
        );
        let mut failed = entry.clone();
        failed.attempts = attempts;
        failed.failed_at_ms = Some(now);
        failed.failure_reason = Some(reason);
        failed.failure_message = Some(message.to_string());
        failed.notice_pending = Some(true);
        failed.notice_attempts = Some(0);
        // The entry is failed for good: a notice that cannot be settled now is the poll's to retry,
        // never a reason to end the saga's turn as if the command had to be sent again.
        self.deliver_notice_quietly(failed).await;
        Ok(())
    }

    /// Claims the pending failure notices that are due (up to a batch), each reserved for this
    /// instance by moving `noticeNextAt` a lease ahead.
    async fn claim_due_notices(&self) -> Result<Vec<SagaDispatchDocument>, MessagingError> {
        let mut claimed = Vec::new();
        while claimed.len() < self.settings.batch_size && !self.stopping.is_cancelled() {
            let now = self.now();
            let entry = self
                .queue
                .claim_due_notice(now, after(now, self.settings.lease))
                .await?;
            match entry {
                Some(entry) => claimed.push(entry),
                None => break,
            }
        }
        Ok(claimed)
    }

    async fn deliver_notice_quietly(&self, entry: SagaDispatchDocument) -> usize {
        if let Err(e) = self.deliver_notice(&entry).await {
            tracing::warn!(
                "Settling the failure notice of {} failed (the store), it is delivered again when its claim expires (its dedupe id makes that harmless): {e}",
                entry.id
            );
        }
        1
    }

    /// Tells the saga (and any ordinary handler) that the command of `entry` failed: a
    /// [`SagaCommandFailed`] dispatched locally under the dedupe id `failed:<entry id>`, never
    /// published. Done: the pending mark is removed. A failure of the delivery counts a notice
    /// attempt and sets `noticeNextAt` with the relay's backoff; after `max_attempts` the notice
    /// is given up (logged at error and counted). The dispatcher's markers make every repeat
    /// harmless.
    async fn deliver_notice(&self, entry: &SagaDispatchDocument) -> Result<(), MessagingError> {
        let Some(reason) = entry.failure_reason else {
            return self.clear_notice(entry).await;
        };
        let failed = SagaCommandFailed {
            saga_id: entry.saga_id.clone(),
            command_type: entry.type_name.clone(),
            idempotency_key: entry.id.clone(),
            reason,
            message: entry.failure_message.clone().unwrap_or_default(),
        };
        let type_name = SagaCommandFailed::TYPE;
        let correlation = entry
            .headers
            .get(DISPATCH_CORRELATION_HEADER)
            .cloned()
            .unwrap_or_else(|| entry.id.clone());
        let dedupe = format!("failed:{}", entry.id);
        let ctx = MessageContext {
            message_id: dedupe.clone(),
            type_name: type_name.to_string(),
            correlation_id: correlation.clone(),
            headers: HashMap::from([
                ("id".to_string(), dedupe.clone()),
                ("type".to_string(), type_name.to_string()),
                ("correlationId".to_string(), correlation),
            ]),
            ..Default::default()
        };
        let outcome = self
            .local
            .dispatch(&dedupe, type_name, &serde_json::to_vec(&failed)?, &ctx)
            .await;
        match outcome {
            Err(e) => return self.schedule_notice_retry(entry, &e).await,
            Ok(unhandled @ (Outcome::NoInterest | Outcome::Unclaimed)) => {
                self.metrics
                    .command_failure_unhandled(&entry.type_name, reason);
                let why = if unhandled == Outcome::NoInterest {
                    "nothing in this service handles SagaCommandFailed (no saga type with a handler for it, no ordinary handler)"
                } else {
                    "no live saga with a SagaCommandFailed handler took it (the saga ended, its type has no such handler, or it no longer associates with its id), and no ordinary handler handles it"
                };
                tracing::error!(
                    "Saga {} command {} ({}) failed with {} and {why}: {}",
                    entry.saga_id,
                    entry.id,
                    entry.type_name,
                    reason_name(reason),
                    entry.failure_message.as_deref().unwrap_or_default()
                );
            }
            Ok(Outcome::Handled | Outcome::Duplicate) => {}
        }
        self.clear_notice(entry).await
    }

    async fn clear_notice(&self, entry: &SagaDispatchDocument) -> Result<(), MessagingError> {
        self.check_notice_write_injection()?;
        self.queue.clear_notice(&entry.id).await?;
        Ok(())
    }

    async fn schedule_notice_retry(
        &self,
        entry: &SagaDispatchDocument,
        error: &MessagingError,
    ) -> Result<(), MessagingError> {
        let attempts = entry.notice_attempts.unwrap_or(0) + 1;
        let reason = entry.failure_reason.unwrap_or(FailureReason::Failed);
        if attempts as u32 >= self.settings.max_attempts {
            self.metrics
                .command_failure_unhandled(&entry.type_name, reason);
            tracing::error!(
                "Gave up delivering SagaCommandFailed for saga {} command {} ({}, {}) after {attempts} attempts; the saga may have applied it, but the delivery kept failing: {error}",
                entry.saga_id,
                entry.id,
                entry.type_name,
                reason_name(reason)
            );
            return self.clear_notice(entry).await;
        }
        let next = after(
            self.now(),
            self.settings
                .retry_policy()
                .jittered_backoff(attempts as u32),
        );
        self.check_notice_write_injection()?;
        self.queue
            .schedule_notice_retry(&entry.id, attempts, next)
            .await?;
        tracing::warn!(
            "Delivering SagaCommandFailed for command {} failed attempt {attempts} of {}, next at {}: {error}",
            entry.id,
            self.settings.max_attempts,
            rfc3339(next)
        );
        Ok(())
    }

    async fn next_pending(
        &self,
        saga_id: &str,
    ) -> Result<Option<SagaDispatchDocument>, MessagingError> {
        Ok(self.queue.next_pending(saga_id).await?)
    }

    // ---- the saga's dispatch lease -----------------------------------------------------------

    /// Takes the lease of `saga_id` when it is free or expired.
    async fn claim(&self, saga_id: &str) -> Result<bool, MessagingError> {
        let now = self.now();
        Ok(self
            .queue
            .claim_lease(saga_id, &self.owner, now, after(now, self.settings.lease))
            .await?)
    }

    async fn renew(&self, saga_id: &str) -> Result<bool, MessagingError> {
        Ok(self
            .queue
            .renew_lease(saga_id, &self.owner, after(self.now(), self.settings.lease))
            .await?)
    }

    /// The heartbeat of a live send: extends the lease so that it never expires under an adapter
    /// call that is still running.
    async fn keep_lease(&self, saga_id: &str) {
        match self.renew(saga_id).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!("Lost the dispatch lease of saga {saga_id} while a send was running")
            }
            Err(e) => {
                tracing::debug!("Could not renew the dispatch lease of saga {saga_id}: {e}")
            }
        }
    }

    async fn release(&self, saga_id: &str) {
        if let Err(e) = self.queue.release_lease(saga_id, &self.owner).await {
            tracing::debug!(
                "Could not release the dispatch lease of saga {saga_id} (it expires): {e}"
            );
        }
    }

    // ---- lifecycle ---------------------------------------------------------------------------

    /// Runs the polling loop as a tokio task until `cancel` is cancelled, then stops starting new
    /// entries and waits for the sends in flight (abandoning them after 15 s: their leases
    /// expire). Start it after the outbox publisher; cancel it before the event consumer stops
    /// (the consumer feeds the sagas whose failures the relay delivers).
    pub fn start(self: &Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                if let Err(e) = this.submit_due().await {
                    tracing::warn!("Saga dispatch poll failed: {e}");
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(this.settings.poll_interval) => {}
                }
            }
            this.stopping.cancel();
            this.tracker.close();
            if tokio::time::timeout(STOP_JOIN, this.tracker.wait())
                .await
                .is_err()
            {
                tracing::warn!(
                    "Saga dispatch sends are still running {} s after stop; abandoning them (their leases expire)",
                    STOP_JOIN.as_secs()
                );
                for handle in this.in_flight.lock().expect("in-flight lock").values() {
                    handle.abort();
                }
            }
        })
    }
}

fn reason_name(reason: FailureReason) -> String {
    serde_json::to_value(reason)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{reason:?}"))
}
