//! The deadline scheduler: fires the due deadlines of this service's sagas, at least once
//! (ADR 0021 rulings 15, 37).
//!
//! A poll claims due deadlines one at a time ([`DeadlineStore::claim_due`]: SCHEDULED,
//! `due_at <= now`, not claimed or the claim expired; it sets `claimed_by` and
//! `claimed_until = now + lease`) and hands each to [`SagaRuntime::apply_deadline`], which fires it
//! in a transaction (loads the saga, runs the deadline handler, writes the state, marks it FIRED;
//! a deadline of an ended or missing saga is CANCELLED instead). Several service instances can run
//! a scheduler: the lease makes each deadline fire on one of them. When firing fails (or the
//! instance dies) the claim is left alone, so the deadline fires again once the lease expired: at
//! least once. Time comes from the injected [`Clock`].
//!
//! [`poll_once`](DeadlineScheduler::poll_once) does one pass; [`start`](DeadlineScheduler::start)
//! runs it every [`DeadlineSettings::poll_interval`] as a tokio task until its
//! `CancellationToken` is cancelled. Start it after the outbox publisher; cancel it before the
//! event consumer stops, like the dispatch relay.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::clock::Clock;
use super::metrics::SagaMetrics;
use super::ports::DeadlineStore;
use super::runtime::SagaRuntime;
use super::store::DeadlineDocument;
use crate::error::MessagingError;

/// Settings of the [`DeadlineScheduler`] (`KRAKEN_DEADLINES_*`): how often it polls and how long a
/// claimed deadline stays reserved for the instance that claimed it, at most `batch_size`
/// deadlines per poll.
#[derive(Clone, Debug)]
pub struct DeadlineSettings {
    pub poll_interval: Duration,
    pub lease: Duration,
    pub batch_size: usize,
}

impl Default for DeadlineSettings {
    fn default() -> Self {
        DeadlineSettings {
            poll_interval: Duration::from_secs(1),
            lease: Duration::from_secs(30),
            batch_size: 100,
        }
    }
}

impl DeadlineSettings {
    /// `InvalidMessage` unless the intervals are positive and the batch has room for one.
    pub fn validate(&self) -> Result<(), MessagingError> {
        let invalid = |what: &str| Err(MessagingError::InvalidMessage(what.to_string()));
        if self.poll_interval.is_zero() {
            return invalid("deadline poll interval must be positive");
        }
        if self.lease.is_zero() {
            return invalid("deadline lease must be positive");
        }
        if self.batch_size < 1 {
            return invalid("deadline batch size must be at least 1");
        }
        Ok(())
    }
}

/// See the module docs.
pub struct DeadlineScheduler {
    deadlines: Arc<dyn DeadlineStore>,
    runtime: Arc<SagaRuntime>,
    clock: Clock,
    settings: DeadlineSettings,
    metrics: Arc<dyn SagaMetrics>,
    owner: String,
    stopping: CancellationToken,
}

impl DeadlineScheduler {
    pub fn new(
        deadlines: Arc<dyn DeadlineStore>,
        runtime: Arc<SagaRuntime>,
        clock: Clock,
        settings: DeadlineSettings,
        metrics: Arc<dyn SagaMetrics>,
    ) -> Result<Arc<DeadlineScheduler>, MessagingError> {
        settings.validate()?;
        Ok(Arc::new(DeadlineScheduler {
            deadlines,
            runtime,
            clock,
            settings,
            metrics,
            owner: uuid::Uuid::new_v4().to_string(),
            stopping: CancellationToken::new(),
        }))
    }

    /// One pass: claims and fires the due deadlines; returns how many it claimed. Reports what is
    /// still overdue to the metrics.
    pub async fn poll_once(&self) -> Result<usize, MessagingError> {
        let mut claimed = 0;
        while claimed < self.settings.batch_size && !self.stopping.is_cancelled() {
            let Some(deadline) = self.claim_next().await? else {
                break;
            };
            claimed += 1;
            if let Err(e) = self.runtime.apply_deadline(&deadline).await {
                // The claim stays: the lease expires and the deadline fires again (at least once).
                tracing::warn!(
                    "Deadline {} '{}' of saga {} failed, it fires again when its lease expires: {e}",
                    deadline.id,
                    deadline.name,
                    deadline.saga_id
                );
            }
        }
        self.metrics.deadlines_overdue(&self.overdue().await?);
        Ok(claimed)
    }

    async fn claim_next(&self) -> Result<Option<DeadlineDocument>, MessagingError> {
        let now = self.clock.now_ms();
        let until =
            now.saturating_add(i64::try_from(self.settings.lease.as_millis()).unwrap_or(i64::MAX));
        Ok(self.deadlines.claim_due(&self.owner, now, until).await?)
    }

    /// The SCHEDULED deadlines past their due time, per name.
    async fn overdue(&self) -> Result<HashMap<String, u64>, MessagingError> {
        Ok(self
            .deadlines
            .count_overdue_by_name(self.clock.now_ms())
            .await?)
    }

    /// Runs the polling loop as a tokio task until `cancel` is cancelled; the pass in progress
    /// finishes first.
    pub fn start(self: &Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                if let Err(e) = this.poll_once().await {
                    tracing::warn!("Deadline poll failed: {e}");
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(this.settings.poll_interval) => {}
                }
            }
            this.stopping.cancel();
        })
    }
}
