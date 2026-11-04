//! One event processor under control (ADR 0023): its consumer, started and stopped by what its
//! `processor_control` document says, and the leased reset.
//!
//! The runner reads the document before the consumer first polls and then every
//! [`CONTROL_POLL`]: a pause pauses the assigned partitions (the consumer stays in the group), the
//! replay ranges reach the consumer and the status, a range is removed once this instance is past
//! its end, a held lease that passed without renewal is cleared (and the consumer starts again),
//! and an unexpired lock this instance was not prepared for keeps the consumer out of the group.
//! A [`ControlRequest`](crate::proto::ControlRequest) is the fast path to the same state.
//!
//! Leaving the group is stopping the consumer task and dropping its consumer (librdkafka's close,
//! on a blocking thread: the commits in flight are written, the group is left); starting again
//! creates a new consumer, which reads the group's committed offsets.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use mongodb::bson::{DateTime, Document};
use mongodb::{Collection, Database};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::ProcessorMonitor;
use super::control_store::{ControlStore, Locked, ReplayRange};
use super::reset::{
    GroupAdmin, Plan, ResetContext, ResetFailure, ResetHandler, ResetTarget, legacy_filter,
    range_filter,
};
use crate::error::MessagingError;
use crate::kafka::{
    ConsumerControl, EventConsumer, LocalEventDispatcher, PROCESSED_EVENTS_COLLECTION,
    ProcessorSettings,
};
use crate::proto::{self, ControlRefusal};

/// How often the runner reads its processor's control document.
pub const CONTROL_POLL: Duration = Duration::from_secs(5);
/// How often the executing instance extends its own lease while `reset_execute` runs.
pub const LEASE_KEEP: Duration = Duration::from_secs(10);
/// For how long at most it does (the hub's default `KRAKEN_HUB_RESET_TIMEOUT`): an execute that
/// hangs longer lets the lease pass, so the other instances consume again.
pub const LEASE_KEEP_MAX: Duration = Duration::from_secs(120);
/// How long a stopped consumer may still be referenced (an offset read in flight) before it is
/// left to close when the last reference goes.
const CLOSE_WAIT: Duration = Duration::from_secs(10);

/// Why a control action was refused; nothing changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub code: ControlRefusal,
    pub message: String,
}

impl Refusal {
    pub fn new(code: ControlRefusal, message: impl Into<String>) -> Refusal {
        Refusal {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn failed(e: &MessagingError) -> Refusal {
        Refusal::new(ControlRefusal::Failed, e.to_string())
    }
}

impl From<ResetFailure> for Refusal {
    fn from(f: ResetFailure) -> Refusal {
        match f {
            ResetFailure::Invalid(m) => Refusal::new(ControlRefusal::Invalid, m),
            ResetFailure::GroupNotEmpty(m) => Refusal::new(ControlRefusal::GroupNotEmpty, m),
            ResetFailure::Failed(m) => Refusal::new(ControlRefusal::Failed, m),
        }
    }
}

/// The consumer running now.
struct Run {
    consumer: Weak<EventConsumer>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

/// The reset this instance was prepared for.
#[derive(Clone, Debug)]
struct Held {
    id: String,
    lease: Duration,
}

/// See the module docs.
pub struct ProcessorRunner {
    settings: ProcessorSettings,
    dispatcher: Arc<LocalEventDispatcher>,
    monitor: Arc<ProcessorMonitor>,
    control: Arc<ConsumerControl>,
    store: ControlStore,
    markers: Collection<Document>,
    reset_handlers: Vec<ResetHandler>,
    run: tokio::sync::Mutex<Option<Run>>,
    held: Mutex<Option<Held>>,
    /// One control action of this processor at a time, the poll included (a renewal of the held
    /// operation excepted).
    serial: Arc<tokio::sync::Mutex<()>>,
    cancel: CancellationToken,
}

impl ProcessorRunner {
    /// The runner of the processor `settings` describe; nothing starts before
    /// [`start`](Self::start). `cancel` stops it (the service's shutdown).
    pub fn new(
        settings: ProcessorSettings,
        dispatcher: Arc<LocalEventDispatcher>,
        db: &Database,
        reset_handlers: Vec<ResetHandler>,
        cancel: CancellationToken,
    ) -> Arc<ProcessorRunner> {
        let monitor = Arc::new(ProcessorMonitor::new(
            &settings.name,
            &settings.group_id,
            &settings.topic,
            settings.dead_letter_mode,
            dispatcher.event_types(),
        ));
        Arc::new(ProcessorRunner {
            settings,
            dispatcher,
            monitor,
            control: Arc::new(ConsumerControl::default()),
            store: ControlStore::new(db),
            markers: db.collection(PROCESSED_EVENTS_COLLECTION),
            reset_handlers,
            run: tokio::sync::Mutex::new(None),
            held: Mutex::new(None),
            serial: Arc::new(tokio::sync::Mutex::new(())),
            cancel,
        })
    }

    pub fn name(&self) -> &str {
        &self.settings.name
    }

    pub fn monitor(&self) -> Arc<ProcessorMonitor> {
        Arc::clone(&self.monitor)
    }

    pub(crate) fn serial(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.serial)
    }

    /// Reads the control document, starts the consumer unless it says otherwise, and keeps
    /// following it every [`CONTROL_POLL`] until the cancel; then stops the consumer.
    pub fn start(self: &Arc<Self>) -> JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                {
                    let _serial = tokio::select! {
                        serial = me.serial.lock() => serial,
                        _ = me.cancel.cancelled() => break,
                    };
                    me.converge().await;
                }
                tokio::select! {
                    _ = me.cancel.cancelled() => break,
                    _ = tokio::time::sleep(CONTROL_POLL) => {}
                }
            }
            me.stop_consumer().await;
        })
    }

    /// Applies the control document (see the module docs). The caller holds `serial`. When the
    /// document cannot be read nothing changes (and a consumer that never started does not start).
    pub(crate) async fn converge(&self) {
        let name = self.name();
        let doc = match self.store.get(name).await {
            Ok(doc) => doc,
            Err(e) => {
                tracing::warn!(processor = %name, "cannot read the processor's control document: {e}");
                self.monitor
                    .error(&format!("processor control document: {e}"));
                return;
            }
        };
        let now = DateTime::now();
        self.apply_replay(&doc.replay);
        let mut held = self.held.lock().unwrap().clone();
        if let Some(h) = &held {
            let lost = match &doc.operation {
                Some(op) if op.id == h.id && !op.holds_at(now) => {
                    if let Err(e) = self.store.expire(name, &h.id, now).await {
                        tracing::warn!(processor = %name, operation = %h.id, "cannot clear the expired lock: {e}");
                        return;
                    }
                    tracing::warn!(processor = %name, operation = %h.id, "the reset's lease passed without a renewal: the lock is released and the processor consumes again");
                    true
                }
                Some(op) if op.id == h.id => false,
                _ => {
                    tracing::info!(processor = %name, operation = %h.id, "the reset no longer holds the lock: the processor consumes again");
                    true
                }
            };
            if lost {
                held = None;
                *self.held.lock().unwrap() = None;
            }
        }
        let lock = held.map(|h| h.id).or_else(|| {
            doc.operation
                .as_ref()
                .filter(|op| op.holds_at(now))
                .map(|op| op.id.clone())
        });
        self.monitor.set_operation(lock.as_deref());
        self.set_paused(doc.paused).await;
        match lock {
            Some(operation) => {
                if self.stop_consumer().await {
                    tracing::info!(processor = %name, operation = %operation, "a reset holds the processor's lock: its consumer left the group until the reset ends");
                }
            }
            None => {
                self.start_consumer().await;
                self.remove_passed_ranges(&doc.replay).await;
            }
        }
    }

    /// Pauses or resumes the running consumer (and every later one) and the status.
    async fn set_paused(&self, paused: bool) {
        let changed = self.control.paused() != paused;
        self.control.set_paused(paused);
        self.monitor.set_paused(paused);
        if let Some(consumer) = self
            .run
            .lock()
            .await
            .as_ref()
            .and_then(|r| r.consumer.upgrade())
        {
            consumer.apply_pause();
        }
        if changed {
            tracing::info!(processor = %self.name(), paused, "processor {}", if paused { "paused" } else { "resumed" });
        }
    }

    fn apply_replay(&self, ranges: &[ReplayRange]) {
        let mine: Vec<ReplayRange> = ranges
            .iter()
            .filter(|r| r.topic == self.settings.topic)
            .cloned()
            .collect();
        let mut until: BTreeMap<i32, i64> = BTreeMap::new();
        for r in &mine {
            let at = until.entry(r.partition).or_insert(r.until);
            *at = (*at).max(r.until);
        }
        self.control.set_replay(mine);
        self.monitor.set_replay(until);
    }

    /// Removes each open range of an assigned partition this instance is past the end of.
    async fn remove_passed_ranges(&self, ranges: &[ReplayRange]) {
        for r in ranges.iter().filter(|r| r.topic == self.settings.topic) {
            if !self
                .monitor
                .progress(r.partition)
                .is_some_and(|at| at >= r.until)
            {
                continue;
            }
            match self
                .store
                .remove_range(self.name(), r, DateTime::now())
                .await
            {
                Ok(true) => {
                    tracing::info!(processor = %self.name(), partition = r.partition, from = r.from, until = r.until, "replay range done")
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(processor = %self.name(), partition = r.partition, "cannot remove the replay range: {e}")
                }
            }
        }
    }

    /// Starts a consumer unless one runs (one that ended by itself is started again).
    async fn start_consumer(&self) {
        let mut run = self.run.lock().await;
        if run.as_ref().is_some_and(|r| !r.task.is_finished()) {
            return;
        }
        if let Some(ended) = run.take() {
            let _ = ended.task.await;
        }
        if self.cancel.is_cancelled() {
            return;
        }
        self.monitor.starting();
        let consumer = match EventConsumer::controlled(
            self.settings.clone(),
            Arc::clone(&self.dispatcher),
            Arc::clone(&self.monitor),
            Arc::clone(&self.control),
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(processor = %self.name(), "cannot create the processor's consumer: {e}");
                self.monitor
                    .error(&format!("cannot create the consumer: {e}"));
                self.monitor.stopped();
                return;
            }
        };
        let stop = self.cancel.child_token();
        let weak = Arc::downgrade(&consumer);
        let task = tokio::spawn(run_consumer(consumer, stop.clone()));
        *run = Some(Run {
            consumer: weak,
            stop,
            task,
        });
    }

    /// Stops the consumer and waits until it left the group; false when none ran.
    async fn stop_consumer(&self) -> bool {
        let run = self.run.lock().await.take();
        let stopped = match run {
            Some(run) => {
                run.stop.cancel();
                let _ = run.task.await;
                true
            }
            None => false,
        };
        self.monitor.stopped();
        stopped
    }

    /// A pause (`by` the control id, or `local`) or a resume: written to the document, then
    /// applied. The caller holds `serial`.
    pub(crate) async fn pause(&self, paused: bool, by: &str) -> Result<(), MessagingError> {
        self.store
            .set_paused(self.name(), paused, by, DateTime::now())
            .await?;
        self.set_paused(paused).await;
        Ok(())
    }

    /// The renewal of the operation this instance holds, answered without waiting for `serial`
    /// (ADR 0023: never queued behind an execute); `None` when it is not a renewal.
    pub(crate) async fn renew(
        &self,
        operation: &str,
        lease: Duration,
    ) -> Option<Result<String, Refusal>> {
        let held = self.held.lock().unwrap().clone()?;
        if held.id != operation {
            return None;
        }
        let now = DateTime::now();
        match self
            .store
            .extend(self.name(), operation, after(now, lease), now)
            .await
        {
            Ok(true) => {
                if let Some(h) = self.held.lock().unwrap().as_mut() {
                    h.lease = lease;
                }
                Some(Ok("lease renewed".into()))
            }
            // The lock is gone (its lease passed): taken again by an ordinary prepare.
            Ok(false) => None,
            Err(e) => Some(Err(Refusal::failed(&e))),
        }
    }

    /// `reset_prepare`: takes the lock (or renews it) and leaves the group. The caller holds
    /// `serial`.
    pub(crate) async fn prepare(
        &self,
        operation: &str,
        lease: Duration,
    ) -> Result<String, Refusal> {
        if operation.is_empty() || lease.is_zero() {
            return Err(Refusal::new(
                ControlRefusal::Invalid,
                "a prepare needs an operation id and a positive lease",
            ));
        }
        let now = DateTime::now();
        match self
            .store
            .lock(self.name(), operation, after(now, lease), now)
            .await
        {
            Err(e) => Err(Refusal::failed(&e)),
            Ok(Locked::Held(other)) => Err(Refusal::new(
                ControlRefusal::Locked,
                format!(
                    "operation {} holds the lock of processor '{}' until {}",
                    other.id,
                    self.name(),
                    other.lease_until
                ),
            )),
            Ok(Locked::Taken) => {
                *self.held.lock().unwrap() = Some(Held {
                    id: operation.to_string(),
                    lease,
                });
                self.monitor.set_operation(Some(operation));
                self.stop_consumer().await;
                Ok(format!(
                    "left the consumer group {}",
                    self.settings.group_id
                ))
            }
        }
    }

    /// The local operation that holds the lock, if any.
    fn held(&self) -> Option<Held> {
        self.held.lock().unwrap().clone()
    }

    fn not_prepared(&self, operation: &str, held: Option<&Held>) -> Refusal {
        Refusal::new(
            ControlRefusal::NotPrepared,
            match held {
                Some(h) => format!(
                    "processor '{}' is prepared for operation {}, not {operation}",
                    self.name(),
                    h.id
                ),
                None => format!(
                    "processor '{}' is not prepared for operation {operation}",
                    self.name()
                ),
            },
        )
    }

    /// `reset_execute` on the instance chosen to run it (it holds the operation): resolves the
    /// target, checks the group is empty, runs the reset handlers, deletes the markers of the
    /// ranges, writes the ranges and commits the offsets, keeping its own lease all along. The
    /// caller holds `serial`.
    pub(crate) async fn execute(
        &self,
        operation: &str,
        target: ResetTarget,
    ) -> Result<proto::ResetOutcome, Refusal> {
        let Some(held) = self.held().filter(|h| h.id == operation) else {
            return Err(self.not_prepared(operation, self.held().as_ref()));
        };
        let keeper = {
            let (store, name, op, lease) = (
                self.store.clone(),
                self.name().to_string(),
                held.id.clone(),
                held.lease,
            );
            tokio::spawn(async move {
                let until = tokio::time::Instant::now() + LEASE_KEEP_MAX;
                loop {
                    tokio::time::sleep(LEASE_KEEP.min(lease / 2)).await;
                    if tokio::time::Instant::now() >= until {
                        tracing::warn!(processor = %name, operation = %op, "the reset runs longer than {} s: its lease is no longer extended", LEASE_KEEP_MAX.as_secs());
                        return;
                    }
                    let now = DateTime::now();
                    if let Err(e) = store.extend(&name, &op, after(now, lease), now).await {
                        tracing::warn!(processor = %name, operation = %op, "cannot extend the lease while the reset runs: {e}");
                    }
                }
            })
        };
        let result = self.execute_held(operation, target).await;
        keeper.abort();
        result
    }

    async fn execute_held(
        &self,
        operation: &str,
        target: ResetTarget,
    ) -> Result<proto::ResetOutcome, Refusal> {
        let name = self.name().to_string();
        let topic = self.settings.topic.clone();
        let admin = {
            let (bootstrap, group, topic) = (
                self.settings.bootstrap_servers.clone(),
                self.settings.group_id.clone(),
                topic.clone(),
            );
            blocking(move || GroupAdmin::new(&bootstrap, &group, &topic).map(Arc::new)).await?
        };
        let (plan, members) = {
            let (admin, target) = (Arc::clone(&admin), target.clone());
            blocking(move || Ok((admin.plan(&target)?, admin.members()?))).await?
        };
        if !members.is_empty() {
            return Err(Refusal::new(
                ControlRefusal::GroupNotEmpty,
                format!(
                    "consumer group {} still has members: {}",
                    self.settings.group_id,
                    members.join(", ")
                ),
            ));
        }
        let open = self
            .store
            .get(&name)
            .await
            .map_err(|e| Refusal::failed(&e))?
            .replay;
        let ranges = plan.ranges(&topic, &target, &open);
        let mut reset_handlers = 0;
        if !ranges.is_empty() {
            let ctx = ResetContext {
                processor: name.clone(),
                operation_id: operation.to_string(),
                target: target.clone(),
                ranges: ranges.clone(),
            };
            for handler in &self.reset_handlers {
                let run = tokio::spawn(handler(ctx.clone()));
                let failure = match run.await {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e.to_string()),
                    Err(join) if join.is_panic() => Some("the reset handler panicked".into()),
                    Err(join) => Some(format!("the reset handler did not finish: {join}")),
                };
                if let Some(failure) = failure {
                    return Err(Refusal::new(
                        ControlRefusal::ResetHandlerFailed,
                        format!(
                            "reset handler {} of {}: {failure}",
                            reset_handlers + 1,
                            self.reset_handlers.len()
                        ),
                    ));
                }
                reset_handlers += 1;
            }
        }
        let mut markers_deleted = 0;
        for range in &ranges {
            markers_deleted += self
                .markers
                .delete_many(range_filter(&name, range))
                .await
                .map_err(|e| Refusal::failed(&e.into()))?
                .deleted_count;
        }
        if target == ResetTarget::Earliest {
            markers_deleted += self
                .markers
                .delete_many(legacy_filter(&name))
                .await
                .map_err(|e| Refusal::failed(&e.into()))?
                .deleted_count;
        }
        self.store
            .set_replay(&name, &Plan::merged(&open, &ranges), DateTime::now())
            .await
            .map_err(|e| Refusal::failed(&e))?;
        let offsets: Vec<(i32, i64)> = plan
            .partitions
            .iter()
            .map(|(&p, &(to, _))| (p, to))
            .collect();
        blocking(move || admin.commit(&offsets)).await?;
        tracing::info!(processor = %name, operation = %operation, target = %target.describe(), ranges = ?ranges, markers_deleted, reset_handlers, "processor reset");
        Ok(proto::ResetOutcome {
            ranges: ranges
                .iter()
                .map(|r| proto::ReplayRange {
                    partition: r.partition,
                    from: r.from,
                    until: r.until,
                })
                .collect(),
            markers_deleted,
            reset_handlers,
        })
    }

    /// `reset_finish`: releases the lock of `operation` (never another's) and consumes again,
    /// honouring the pause; idempotent when nothing is held here. The caller holds `serial`.
    pub(crate) async fn finish(&self, operation: &str, aborted: bool) -> Result<String, Refusal> {
        let held = self.held();
        if let Some(h) = held.as_ref().filter(|h| h.id != operation) {
            return Err(self.not_prepared(operation, Some(h)));
        }
        self.store
            .release(self.name(), operation, DateTime::now())
            .await
            .map_err(|e| Refusal::failed(&e))?;
        *self.held.lock().unwrap() = None;
        self.converge().await;
        Ok(match (held.is_some(), aborted) {
            (false, _) => format!("operation {operation} is not held here"),
            (true, true) => "aborted; the processor consumes again".into(),
            (true, false) => "the processor consumes again".into(),
        })
    }
}

/// A consumer's run: until `stop`, then its close.
async fn run_consumer(consumer: Arc<EventConsumer>, stop: CancellationToken) {
    let _ = Arc::clone(&consumer).start(stop).await;
    close(consumer).await;
}

/// Drops the last reference to `consumer` on a blocking thread (librdkafka's close blocks until
/// the group is left), once the offset reads still in flight let go of it.
async fn close(mut consumer: Arc<EventConsumer>) {
    let deadline = tokio::time::Instant::now() + CLOSE_WAIT;
    loop {
        match Arc::try_unwrap(consumer) {
            Ok(last) => {
                let _ = tokio::task::spawn_blocking(move || drop(last)).await;
                return;
            }
            Err(shared) if tokio::time::Instant::now() < deadline => {
                consumer = shared;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(shared) => {
                tracing::warn!("a stopped consumer is still referenced; it closes when released");
                let _ = tokio::task::spawn_blocking(move || drop(shared)).await;
                return;
            }
        }
    }
}

/// `work` on a blocking thread (librdkafka's calls block).
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ResetFailure> + Send + 'static,
) -> Result<T, Refusal> {
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result.map_err(Refusal::from),
        Err(e) => Err(Refusal::new(
            ControlRefusal::Failed,
            format!("kafka call failed: {e}"),
        )),
    }
}

fn after(now: DateTime, lease: Duration) -> DateTime {
    DateTime::from_millis(
        now.timestamp_millis()
            .saturating_add(i64::try_from(lease.as_millis()).unwrap_or(i64::MAX)),
    )
}
