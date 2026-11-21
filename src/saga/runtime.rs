//! The saga runtime: the saga stage of local event dispatch, and the deadline and parked-event
//! entry points the scheduler and the failure admin call.
//!
//! Per (event, saga type) one transaction runs: the marker `processed_events`
//! id `saga:<sagaType>:<eventId>` is inserted first (an existing one skips the event); the live
//! instances associated with the event are resolved per handler (`key = value`); when none
//! matches, a start handler creates one with `startKey = "<type>|<key>=<value>"` (the parts
//! percent-escaped, [`start_key_of`]; a force-new handler always creates one, without a start
//! key); each instance's handlers run in registration order, a handler running only on instances
//! associated with its own key and value.
//! Handlers see one [`SagaContext`] per instance that only records; once every handler returned,
//! the state is written with an optimistic version check (`version + 1`; 0 means just created),
//! the recorded dispatch entries (`seq = version * 1000 + index`) and deadline changes are
//! written, and an ended instance cancels its scheduled deadlines. A conflict (version,
//! start key, write conflict) or any handler error rolls the whole transaction back and the event
//! is retried with [`SagaRetry`]; after the last attempt it is parked in
//! `saga_failures` together with the marker and the stage moves on. The only error
//! [`SagaStage::process`] returns is a failure to park (the store is down): the consumer then
//! redelivers the record instead of losing it.
//!
//! Handlers run on the calling task inside a `tracing` span with the fields `saga_type`,
//! `saga_id` and `correlation_id` (the Rust form of the Kotlin library's MDC keys). They are
//! synchronous and must stay short: a slow one holds the consumer's partition. A handler that
//! panics is a handler error.

use std::collections::{BTreeSet, HashMap};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::SystemTime;

use futures::future::BoxFuture;

use super::clock::Clock;
use super::context::{AssociationChange, SEQ_STRIDE, SagaContext};
use super::definition::{ErasedHandler, SagaType};
use super::failed::SagaCommandFailed;
use super::metrics::{EventOutcome, SagaMetrics};
use super::ports::SagaPorts;
use super::retry::SagaRetry;
use super::store::{
    Association, DeadlineDocument, DeadlineStatus, SagaDispatchDocument, SagaDocument,
    SagaFailureDocument,
};
use crate::codec;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::events::SagaStage;
use crate::store::{StoreError, Tx};
use crate::types::MessageType;

/// The header of a `saga_dispatch` entry that carries the correlation id of the message that
/// caused it.
pub(crate) const DISPATCH_CORRELATION_HEADER: &str = "correlation-id";
/// The header of a dispatch entry that keeps the causing event's valid W3C `traceparent`.
pub(crate) const DISPATCH_TRACEPARENT_HEADER: &str = "traceparent";

const MAX_ERROR_LENGTH: usize = 2000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Handled,
    NoMatch,
    Duplicate,
    Gone,
}

struct Applied {
    kind: Kind,
    started: usize,
    ended: usize,
}

impl Applied {
    fn of(kind: Kind) -> Applied {
        Applied {
            kind,
            started: 0,
            ended: 0,
        }
    }
}

/// An attempt loop gave up after `attempts` attempts; `last` is the failure of the final one.
struct GaveUp {
    last: StoreError,
    attempts: u32,
}

enum PlannedDeadline {
    Schedule(Box<DeadlineDocument>),
    Cancel(String),
    CancelAll(String),
}

/// One instance's new document and the effects to write with it. `expected_version` is `None` for
/// a new instance.
struct Planned {
    doc: SagaDocument,
    expected_version: Option<i64>,
    entries: Vec<SagaDispatchDocument>,
    deadlines: Vec<PlannedDeadline>,
    created: bool,
}

pub struct SagaRuntime {
    types: Vec<SagaType>,
    by_name: HashMap<&'static str, usize>,
    by_event: HashMap<&'static str, Vec<usize>>,
    ports: SagaPorts,
    clock: Clock,
    retry: SagaRetry,
    metrics: Arc<dyn SagaMetrics>,
}

impl SagaRuntime {
    pub fn new(
        types: Vec<SagaType>,
        ports: SagaPorts,
        clock: Clock,
        retry: SagaRetry,
        metrics: Arc<dyn SagaMetrics>,
    ) -> SagaRuntime {
        let mut by_name = HashMap::new();
        let mut by_event: HashMap<&'static str, Vec<usize>> = HashMap::new();
        for (i, t) in types.iter().enumerate() {
            by_name.insert(t.name, i);
            let mut events: Vec<&'static str> = t.handlers.keys().copied().collect();
            if t.command_failed.is_some() {
                events.push(SagaCommandFailed::TYPE);
            }
            events.sort_unstable();
            events.dedup();
            for event in events {
                by_event.entry(event).or_default().push(i);
            }
        }
        SagaRuntime {
            types,
            by_name,
            by_event,
            ports,
            clock,
            retry,
            metrics,
        }
    }

    /// The ports the runtime reads and writes through.
    pub fn ports(&self) -> &SagaPorts {
        &self.ports
    }

    /// The names of the saga types this runtime serves.
    pub fn saga_types(&self) -> Vec<&'static str> {
        self.types.iter().map(|t| t.name).collect()
    }

    async fn process_event(
        &self,
        type_name: &str,
        payload: &[u8],
        ctx: &MessageContext,
    ) -> Result<bool, MessagingError> {
        let mut applied = false;
        for &i in self.by_event.get(type_name).into_iter().flatten() {
            let kind = self
                .deliver(&self.types[i], type_name, payload, ctx, None)
                .await?;
            // Parked (`None`): the instance got the event and its handler failed; the park says so.
            if matches!(kind, Some(Kind::Handled | Kind::Duplicate) | None) {
                applied = true;
            }
        }
        Ok(applied)
    }

    /// Delivers the `SagaCommandFailed` the relay produced to the saga that sent the command
    ///: what the stage does for the event, for callers that hold the typed event.
    pub async fn deliver_command_failed(
        &self,
        failed: &SagaCommandFailed,
        ctx: &MessageContext,
    ) -> Result<bool, MessagingError> {
        let payload = serde_json::to_vec(failed)?;
        self.process_event(SagaCommandFailed::TYPE, &payload, ctx)
            .await
    }

    /// Re-delivers a parked event (from the failure admin) to its saga type, ignoring the marker
    /// (it exists by design): in one transaction the failure is deleted and the event applied,
    /// with the saga runtime's retries. `true` when it was applied (or no longer matches anything),
    /// `false` when it failed again (the failure stays with `attempts` increased and the new
    /// error) or was no longer parked (a concurrent retry or discard: it is then not brought
    /// back). `InvalidMessage` without writing anything when its saga type does not run here
    /// (not registered, or sagas disabled): the failure keeps its error and attempts.
    pub async fn reapply(&self, failure: &SagaFailureDocument) -> Result<bool, MessagingError> {
        let Some(&i) = self.by_name.get(failure.saga_type.as_str()) else {
            return Err(not_run_here(
                &failure.saga_type,
                &format!("parked event {} is left as it was", failure.id),
            ));
        };
        let ctx = context_of(failure);
        let kind = self
            .deliver(
                &self.types[i],
                &failure.event_type,
                failure.payload.as_bytes(),
                &ctx,
                Some(failure),
            )
            .await?;
        Ok(kind.is_some_and(|k| k != Kind::Gone))
    }

    /// `InvalidMessage` unless saga type `saga_type` runs in this service; the message counts its
    /// `parked` events.
    pub(crate) fn ensure_runs(&self, saga_type: &str, parked: usize) -> Result<(), MessagingError> {
        if self.by_name.contains_key(saga_type) {
            return Ok(());
        }
        Err(not_run_here(
            saga_type,
            &format!("its parked events ({parked}) are left as they were"),
        ))
    }

    /// Fires `deadline` (claimed by the scheduler): in one transaction, a deadline no longer
    /// SCHEDULED is left alone; one whose saga is gone or ended, or whose saga type has no handler
    /// of that name, is CANCELLED; otherwise the deadline handler runs, its effects are written as
    /// for an event and the deadline becomes FIRED. Conflicts and handler errors are retried like
    /// an event's; when they persist the error is returned to the scheduler (the lease then lets
    /// the deadline fire again: at least once). An unknown saga type fails at once.
    pub async fn apply_deadline(&self, deadline: &DeadlineDocument) -> Result<(), MessagingError> {
        let Some(&i) = self.by_name.get(deadline.saga_type.as_str()) else {
            return Err(MessagingError::InvalidMessage(format!(
                "Deadline {} belongs to saga type '{}', which this service does not have",
                deadline.id, deadline.saga_type
            )));
        };
        let t = &self.types[i];
        let what = format!("Deadline {} of saga {}", deadline.id, deadline.saga_id);
        let fired = self
            .with_retries(
                &what,
                || {},
                || {
                    Box::pin(async {
                        let mut tx = self.ports.tx.begin().await?;
                        let result = self.fire_deadline(&mut tx, t, deadline).await;
                        self.finish(&mut tx, result).await
                    })
                },
            )
            .await
            .map_err(|e| e.last.into_messaging())?;
        if fired {
            self.metrics.deadline_fired(&deadline.name);
        }
        Ok(())
    }

    // ---- events ----------------------------------------------------------------------------

    /// Runs the attempts for one saga type; `None` when the event was parked (or parked again).
    async fn deliver(
        &self,
        t: &SagaType,
        event_type: &str,
        payload: &[u8],
        ctx: &MessageContext,
        retry_of: Option<&SagaFailureDocument>,
    ) -> Result<Option<Kind>, MessagingError> {
        let what = format!("Saga type {} event {}", t.name, ctx.message_id);
        let result = self
            .with_retries(
                &what,
                || self.metrics.event(t.name, EventOutcome::Conflict),
                || {
                    Box::pin(async {
                        let mut tx = self.ports.tx.begin().await?;
                        let result = self
                            .apply_event(&mut tx, t, event_type, payload, ctx, retry_of)
                            .await;
                        self.finish(&mut tx, result).await
                    })
                },
            )
            .await;
        match result {
            Ok(applied) => {
                self.report(t, &applied);
                Ok(Some(applied.kind))
            }
            Err(gave_up) => {
                let error = describe(&gave_up.last);
                match retry_of {
                    Some(failure) => self.park_again(failure, &error, gave_up.attempts).await?,
                    None => {
                        self.park(t, event_type, payload, ctx, &error, gave_up.attempts)
                            .await?
                    }
                }
                Ok(None)
            }
        }
    }

    /// Commits `result`'s transaction when it succeeded, else abandons it.
    async fn finish<T>(&self, tx: &mut Tx, result: Result<T, StoreError>) -> Result<T, StoreError> {
        match result {
            Ok(value) => match self.ports.tx.commit(tx).await {
                Ok(()) => Ok(value),
                Err(e) => {
                    self.ports.tx.abort(tx).await;
                    Err(e)
                }
            },
            Err(e) => {
                self.ports.tx.abort(tx).await;
                Err(e)
            }
        }
    }

    /// Runs `attempt` (one attempt, in its own transaction) until it succeeds, with the retry
    /// rules: a conflict is retried up to `max_attempts` attempts in all with the
    /// backoff of [`SagaRetry::conflict_pause`]; any other failure gets at most
    /// [`SagaRetry::HANDLER_ATTEMPTS`] attempts with only a quick pause between them (waiting
    /// does not cure a handler that fails).
    async fn with_retries<'f, T>(
        &self,
        what: &str,
        on_conflict: impl Fn(),
        mut attempt: impl FnMut() -> BoxFuture<'f, Result<T, StoreError>>,
    ) -> Result<T, GaveUp> {
        let mut conflicts = 0u32;
        let mut failures = 0u32;
        loop {
            let e = match attempt().await {
                Ok(value) => return Ok(value),
                Err(e) => e,
            };
            let made = conflicts + failures + 1;
            tracing::debug!("{what} failed attempt {made}: {e}");
            if e.is_conflict() {
                on_conflict();
                conflicts += 1;
                if made >= self.retry.max_attempts() {
                    return Err(GaveUp {
                        last: e,
                        attempts: made,
                    });
                }
                tokio::time::sleep(self.retry.conflict_pause(conflicts)).await;
            } else {
                failures += 1;
                if failures >= SagaRetry::HANDLER_ATTEMPTS.min(self.retry.max_attempts()) {
                    return Err(GaveUp {
                        last: e,
                        attempts: made,
                    });
                }
                tokio::time::sleep(self.retry.handler_pause()).await;
            }
        }
    }

    fn report(&self, t: &SagaType, applied: &Applied) {
        match applied.kind {
            Kind::Handled => {
                for _ in 0..applied.started {
                    self.metrics.saga_started(t.name);
                }
                for _ in 0..applied.ended {
                    self.metrics.saga_ended(t.name);
                }
                self.metrics.event(t.name, EventOutcome::Handled);
            }
            Kind::NoMatch => self.metrics.event(t.name, EventOutcome::NoMatch),
            Kind::Duplicate | Kind::Gone => {}
        }
    }

    /// One attempt, inside the transaction.
    async fn apply_event(
        &self,
        tx: &mut Tx,
        t: &SagaType,
        event_type: &str,
        payload: &[u8],
        ctx: &MessageContext,
        retry_of: Option<&SagaFailureDocument>,
    ) -> Result<Applied, StoreError> {
        let now = self.clock.now();
        let now_ms = crate::time::epoch_ms(now);
        match retry_of {
            Some(failure) => {
                if !self.ports.failures.delete(tx, &failure.id).await? {
                    return Ok(Applied::of(Kind::Gone));
                }
            }
            None => {
                let marker = marker_id(t, &ctx.message_id);
                if self.ports.markers.exists(tx, &marker).await? {
                    tracing::debug!(
                        "event {} already applied by saga type {}, skipping",
                        ctx.message_id,
                        t.name
                    );
                    return Ok(Applied::of(Kind::Duplicate));
                }
                self.ports
                    .markers
                    .insert(tx, &marker, event_type, now_ms)
                    .await?;
            }
        }

        let handlers = handlers_for(t, event_type);
        let values: Vec<Option<String>> = handlers
            .iter()
            .map(|h| h.association_value(payload, ctx))
            .collect::<Result<_, _>>()?;

        // Live instances per handler: a handler runs only on the instances associated with its
        // own key and value.
        let mut matched: Vec<SagaDocument> = Vec::new();
        let mut handled_by: HashMap<String, BTreeSet<usize>> = HashMap::new();
        for (i, value) in values.iter().enumerate() {
            let Some(value) = value else { continue };
            for doc in self
                .ports
                .sagas
                .find_live(tx, t.name, &handlers[i].key, value)
                .await?
            {
                handled_by.entry(doc.id.clone()).or_default().insert(i);
                if !matched.iter().any(|m| m.id == doc.id) {
                    matched.push(doc);
                }
            }
        }
        // A SagaCommandFailed also reaches the saga that sent the command, by its id: no
        // association needed.
        if event_type == SagaCommandFailed::TYPE {
            let failed: SagaCommandFailed = codec::decode(payload)?;
            let owner = self.ports.sagas.find_by_id(tx, &failed.saga_id).await?;
            if let Some(owner) = owner.filter(|o| o.type_name == t.name && !o.ended) {
                handled_by
                    .entry(owner.id.clone())
                    .or_default()
                    .extend(0..handlers.len());
                if !matched.iter().any(|m| m.id == owner.id) {
                    matched.push(owner);
                }
            }
        }

        let mut planned: Vec<Planned> = Vec::new();
        for doc in &matched {
            let run: Vec<&ErasedHandler> =
                handled_by[&doc.id].iter().map(|&i| handlers[i]).collect();
            planned.push(self.invoke(
                t,
                &doc.id,
                doc.state.clone(),
                Some(doc),
                doc.associations.clone(),
                &run,
                payload,
                ctx,
                now,
                None,
            )?);
        }
        for (i, value) in values.iter().enumerate() {
            let Some(value) = value else { continue };
            if handlers[i].start && handlers[i].force_new {
                planned.push(self.start_instance(
                    t,
                    handlers[i],
                    value,
                    &handlers,
                    &values,
                    payload,
                    ctx,
                    now,
                    None,
                )?);
            }
        }
        if matched.is_empty() {
            let start = values
                .iter()
                .enumerate()
                .find(|(i, v)| handlers[*i].start && !handlers[*i].force_new && v.is_some());
            if let Some((i, Some(value))) = start {
                let key = start_key_of(t.name, &handlers[i].key, value);
                planned.push(self.start_instance(
                    t,
                    handlers[i],
                    value,
                    &handlers,
                    &values,
                    payload,
                    ctx,
                    now,
                    Some(key),
                )?);
            }
        }
        if planned.is_empty() {
            tracing::debug!(
                "event {} of type {event_type} matched no instance of saga type {} and starts none",
                ctx.message_id,
                t.name
            );
            return Ok(Applied::of(Kind::NoMatch));
        }
        for p in &planned {
            self.write(tx, p, now_ms).await?;
        }
        Ok(Applied {
            kind: Kind::Handled,
            started: planned.iter().filter(|p| p.created).count(),
            ended: planned.iter().filter(|p| p.doc.ended).count(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn start_instance(
        &self,
        t: &SagaType,
        via: &ErasedHandler,
        value: &str,
        handlers: &[&ErasedHandler],
        values: &[Option<String>],
        payload: &[u8],
        ctx: &MessageContext,
        now: SystemTime,
        start_key: Option<String>,
    ) -> Result<Planned, StoreError> {
        let own: Vec<&ErasedHandler> = handlers
            .iter()
            .zip(values)
            .filter(|(h, v)| h.key == via.key && v.as_deref() == Some(value))
            .map(|(h, _)| *h)
            .collect();
        self.invoke(
            t,
            &uuid::Uuid::new_v4().to_string(),
            (t.new_state)(),
            None,
            vec![Association::new(&via.key, value)],
            &own,
            payload,
            ctx,
            now,
            start_key,
        )
    }

    /// Runs `handlers` on one instance and plans its writes; `doc` is its stored document, `None`
    /// for a new instance. One [`SagaContext`] serves all the handlers (so the limit of 999
    /// messages is per handling of the instance).
    #[allow(clippy::too_many_arguments)]
    fn invoke(
        &self,
        t: &SagaType,
        saga_id: &str,
        mut state: String,
        doc: Option<&SagaDocument>,
        associations: Vec<Association>,
        handlers: &[&ErasedHandler],
        payload: &[u8],
        ctx: &MessageContext,
        now: SystemTime,
        start_key: Option<String>,
    ) -> Result<Planned, StoreError> {
        let mut sctx = SagaContext::new(saga_id.to_string(), ctx.clone(), now);
        let span = tracing::info_span!(
            "saga",
            saga_type = %t.name,
            saga_id = %saga_id,
            correlation_id = %ctx.correlation_id
        );
        span.in_scope(|| -> Result<(), MessagingError> {
            for h in handlers {
                guarded(t, h.event_type, || {
                    (h.invoke)(&mut state, payload, &mut sctx)
                })?;
                if h.end {
                    sctx.end();
                }
                if sctx.effects().end {
                    break;
                }
            }
            Ok(())
        })?;
        Ok(plan(
            t,
            saga_id,
            doc,
            associations,
            state,
            sctx,
            ctx,
            now,
            start_key,
        ))
    }

    /// One attempt at firing `claimed`, inside the transaction; true when its handler ran.
    async fn fire_deadline(
        &self,
        tx: &mut Tx,
        t: &SagaType,
        claimed: &DeadlineDocument,
    ) -> Result<bool, StoreError> {
        let now = self.clock.now();
        let now_ms = crate::time::epoch_ms(now);
        if self
            .ports
            .deadlines
            .find_scheduled(tx, &claimed.id)
            .await?
            .is_none()
        {
            return Ok(false); // cancelled or fired since the scheduler claimed it
        }
        let doc = self.ports.sagas.find_by_id(tx, &claimed.saga_id).await?;
        let Some(doc) = doc.filter(|d| !d.ended) else {
            self.ports
                .deadlines
                .cancel(tx, &claimed.saga_id, &claimed.id, now_ms)
                .await?;
            return Ok(false);
        };
        let Some(handler) = t.deadlines.get(claimed.name.as_str()) else {
            tracing::error!(
                "Saga type {} has no deadline handler named '{}': cancelling deadline {} of saga {}",
                t.name,
                claimed.name,
                claimed.id,
                claimed.saga_id
            );
            self.ports
                .deadlines
                .cancel(tx, &claimed.saga_id, &claimed.id, now_ms)
                .await?;
            return Ok(false);
        };
        // The correlation id of the message that scheduled it (a deadline stored before it was
        // recorded falls back to its own id).
        let ctx = MessageContext {
            message_id: claimed.id.clone(),
            type_name: format!("deadline:{}", claimed.name),
            correlation_id: claimed
                .correlation_id
                .clone()
                .unwrap_or_else(|| claimed.id.clone()),
            ..Default::default()
        };
        let mut sctx = SagaContext::new(doc.id.clone(), ctx.clone(), now);
        let mut state = doc.state.clone();
        let span = tracing::info_span!(
            "saga",
            saga_type = %t.name,
            saga_id = %doc.id,
            correlation_id = %ctx.correlation_id
        );
        span.in_scope(|| -> Result<(), MessagingError> {
            guarded(t, handler.name, || {
                (handler.invoke)(&mut state, claimed.payload.as_deref(), &mut sctx)
            })?;
            if handler.end {
                sctx.end();
            }
            Ok(())
        })?;
        let planned = plan(
            t,
            &doc.id,
            Some(&doc),
            doc.associations.clone(),
            state,
            sctx,
            &ctx,
            now,
            None,
        );
        if !self
            .ports
            .deadlines
            .mark_fired(tx, &claimed.id, now_ms)
            .await?
        {
            return Err(StoreError::Conflict(format!(
                "deadline {} changed while firing",
                claimed.id
            )));
        }
        self.write(tx, &planned, now_ms).await?;
        if planned.doc.ended {
            self.metrics.saga_ended(t.name);
        }
        Ok(true)
    }

    async fn write(&self, tx: &mut Tx, planned: &Planned, now: i64) -> Result<(), StoreError> {
        let doc = &planned.doc;
        match planned.expected_version {
            None => self.ports.sagas.insert(tx, doc).await?,
            Some(expected) => {
                if !self.ports.sagas.update(tx, doc, expected).await? {
                    return Err(StoreError::Conflict(format!(
                        "saga {} is no longer at version {expected}",
                        doc.id
                    )));
                }
            }
        }
        for op in &planned.deadlines {
            match op {
                PlannedDeadline::Schedule(deadline) => {
                    self.ports.deadlines.insert(tx, deadline).await?
                }
                PlannedDeadline::Cancel(id) => {
                    self.ports.deadlines.cancel(tx, &doc.id, id, now).await?
                }
                PlannedDeadline::CancelAll(name) => {
                    self.ports
                        .deadlines
                        .cancel_by_name(tx, &doc.id, name, now)
                        .await?
                }
            }
        }
        self.ports.dispatch.record(tx, &planned.entries).await?;
        if doc.ended {
            self.ports.deadlines.cancel_all(tx, &doc.id, now).await?;
        }
        Ok(())
    }

    // ---- parking ---------------------------------------------------------------------------

    /// Parks the event for `t` with its marker; skipped when the marker exists (another instance
    /// finished it).
    async fn park(
        &self,
        t: &SagaType,
        event_type: &str,
        payload: &[u8],
        ctx: &MessageContext,
        error: &str,
        attempts: u32,
    ) -> Result<(), MessagingError> {
        let now = self.clock.now_ms();
        let marker = marker_id(t, &ctx.message_id);
        let failure = SagaFailureDocument {
            id: format!("{}:{}", t.name, ctx.message_id),
            saga_type: t.name.to_string(),
            event_id: ctx.message_id.clone(),
            event_type: event_type.to_string(),
            payload: String::from_utf8_lossy(payload).into_owned(),
            headers: failure_headers(ctx),
            error: error.to_string(),
            attempts: attempts as i32,
            first_failed_at_ms: now,
            last_failed_at_ms: now,
        };
        let mut tx = self
            .ports
            .tx
            .begin()
            .await
            .map_err(StoreError::into_messaging)?;
        let work = async {
            if self.ports.markers.exists(&mut tx, &marker).await? {
                Ok(false)
            } else {
                self.ports
                    .markers
                    .insert(&mut tx, &marker, event_type, now)
                    .await?;
                self.ports.failures.save(&mut tx, &failure).await?;
                Ok::<bool, StoreError>(true)
            }
        }
        .await;
        let parked = self
            .finish(&mut tx, work)
            .await
            .map_err(StoreError::into_messaging)?;
        if parked {
            self.metrics.event(t.name, EventOutcome::Parked);
            tracing::error!(
                "Saga type {} parked event {} of type {event_type} after {attempts} attempts: {error}",
                t.name,
                ctx.message_id
            );
        } else {
            tracing::warn!(
                "Saga type {} gave up on event {} but it was applied or parked meanwhile",
                t.name,
                ctx.message_id
            );
        }
        Ok(())
    }

    /// A reapplied failure failed again: it stays, with the attempts of this try added and the new
    /// error.
    async fn park_again(
        &self,
        failure: &SagaFailureDocument,
        error: &str,
        attempts: u32,
    ) -> Result<(), MessagingError> {
        let now = self.clock.now_ms();
        let mut tx = self
            .ports
            .tx
            .begin()
            .await
            .map_err(StoreError::into_messaging)?;
        let work = self
            .ports
            .failures
            .failed_again(&mut tx, &failure.id, error, attempts as i32, now)
            .await;
        let still_parked = self
            .finish(&mut tx, work)
            .await
            .map_err(StoreError::into_messaging)?;
        if still_parked {
            tracing::error!(
                "Saga type {} failed again on parked event {}: {error}",
                failure.saga_type,
                failure.event_id
            );
        } else {
            tracing::warn!(
                "Saga type {} failed again on parked event {}, which was discarded or retried meanwhile: {error}",
                failure.saga_type,
                failure.event_id
            );
        }
        Ok(())
    }
}

impl SagaStage for SagaRuntime {
    fn interested(&self, type_name: &str) -> bool {
        self.by_event.contains_key(type_name)
    }

    fn event_types(&self) -> Vec<String> {
        let mut all: Vec<String> = self.by_event.keys().map(|t| t.to_string()).collect();
        all.sort();
        all
    }

    fn process<'a>(
        &'a self,
        type_name: &'a str,
        payload: &'a [u8],
        ctx: &'a MessageContext,
    ) -> BoxFuture<'a, Result<bool, MessagingError>> {
        Box::pin(self.process_event(type_name, payload, ctx))
    }
}

fn not_run_here(saga_type: &str, what: &str) -> MessagingError {
    MessagingError::InvalidMessage(format!(
        "Saga type '{saga_type}' does not run in this service: {what}"
    ))
}

/// The handlers of `t` for `event_type`, in registration order; the handler of
/// `SagaCommandFailed` (not in the map) comes after the others.
fn handlers_for<'t>(t: &'t SagaType, event_type: &str) -> Vec<&'t ErasedHandler> {
    let mut handlers: Vec<&ErasedHandler> = t
        .handlers
        .get(event_type)
        .map(|hs| hs.iter().collect())
        .unwrap_or_default();
    if event_type == SagaCommandFailed::TYPE
        && let Some(h) = &t.command_failed
    {
        handlers.push(h);
    }
    handlers
}

fn marker_id(t: &SagaType, event_id: &str) -> String {
    format!("saga:{}:{event_id}", t.name)
}

/// Calls a handler; a panic is a handler error.
fn guarded(
    t: &SagaType,
    handler: &str,
    call: impl FnOnce() -> Result<(), MessagingError>,
) -> Result<(), MessagingError> {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(result) => result,
        Err(panic) => {
            let what = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "no message".to_string());
            Err(MessagingError::RemoteHandler {
                type_name: format!("{}.{handler}", t.name),
                message: format!("saga handler {}.{handler} panicked: {what}", t.name),
                details: Vec::new(),
            })
        }
    }
}

/// What a handling recorded, turned into the instance's new document and its effects.
#[allow(clippy::too_many_arguments)]
fn plan(
    t: &SagaType,
    saga_id: &str,
    doc: Option<&SagaDocument>,
    mut associations: Vec<Association>,
    state: String,
    mut sctx: SagaContext,
    ctx: &MessageContext,
    now: SystemTime,
    start_key: Option<String>,
) -> Planned {
    let now_ms = crate::time::epoch_ms(now);
    let effects = sctx.take_effects();
    for change in effects.associations {
        match change {
            AssociationChange::Add { key, value } => {
                let association = Association { k: key, v: value };
                if !associations.contains(&association) {
                    associations.push(association);
                }
            }
            AssociationChange::Remove { key, value } => {
                if let Some(at) = associations.iter().position(|a| a.k == key && a.v == value) {
                    associations.remove(at);
                }
            }
        }
    }
    let ended = effects.end;
    // The start key lives and dies with its association: remove that one and the key is free for
    // a new instance, also when the start handler itself removes it.
    let start_key = match doc {
        Some(d) => d.start_key.clone(),
        None => start_key,
    }
    .filter(|key| {
        associations
            .iter()
            .any(|a| start_key_of(t.name, &a.k, &a.v) == *key)
    });
    let version = doc.map_or(0, |d| d.version + 1);
    let next = SagaDocument {
        id: saga_id.to_string(),
        type_name: t.name.to_string(),
        state,
        associations,
        start_key,
        ended,
        ended_at_ms: ended.then_some(now_ms),
        version,
        created_at_ms: doc.map_or(now_ms, |d| d.created_at_ms),
        updated_at_ms: now_ms,
    };
    let entries = effects
        .dispatches
        .into_iter()
        .enumerate()
        .map(|(index, d)| {
            let seq = version * SEQ_STRIDE + index as i64;
            SagaDispatchDocument {
                id: format!("{saga_id}:{seq}"),
                saga_id: saga_id.to_string(),
                seq,
                kind: d.kind,
                type_name: d.type_name.to_string(),
                payload: d.payload,
                headers: dispatch_headers(ctx),
                routing_key: d.routing_key,
                created_at_ms: now_ms,
                dispatched_at_ms: None,
                attempts: 0,
                next_attempt_at_ms: None,
                failed_at_ms: None,
                failure_reason: None,
                failure_message: None,
                notice_pending: None,
                notice_attempts: None,
                notice_next_at_ms: None,
            }
        })
        .collect::<Vec<_>>();
    let deadlines = effects
        .deadlines
        .into_iter()
        .map(|op| match op {
            super::context::DeadlineOp::Schedule {
                id,
                name,
                due_at,
                payload,
                payload_type,
            } => PlannedDeadline::Schedule(Box::new(DeadlineDocument {
                id,
                saga_type: t.name.to_string(),
                saga_id: saga_id.to_string(),
                name,
                payload,
                payload_type,
                due_at_ms: crate::time::epoch_ms(due_at),
                status: DeadlineStatus::Scheduled,
                claimed_by: None,
                claimed_until_ms: None,
                finished_at_ms: None,
                correlation_id: Some(ctx.correlation_id.clone()),
            })),
            super::context::DeadlineOp::Cancel(id) => PlannedDeadline::Cancel(id),
            super::context::DeadlineOp::CancelAll(name) => PlannedDeadline::CancelAll(name),
        })
        .collect();
    Planned {
        doc: next,
        expected_version: doc.map(|d| d.version),
        entries,
        deadlines,
        created: doc.is_none(),
    }
}

/// The headers of a dispatch entry: the correlation id of the message that caused it and, when
/// that message carried a valid one, its `traceparent` (the trace floor: the relay sends the
/// command, or publishes the event, in that trace).
fn dispatch_headers(ctx: &MessageContext) -> HashMap<String, String> {
    let mut headers = HashMap::from([(
        DISPATCH_CORRELATION_HEADER.to_string(),
        ctx.correlation_id.clone(),
    )]);
    if let Some(tp) = ctx.traceparent() {
        headers.insert(DISPATCH_TRACEPARENT_HEADER.to_string(), tp.to_string());
    }
    headers
}

/// The headers kept with a parked event: enough to rebuild its message context.
fn failure_headers(ctx: &MessageContext) -> HashMap<String, String> {
    let mut headers = ctx.headers.clone();
    headers
        .entry("correlationId".to_string())
        .or_insert_with(|| ctx.correlation_id.clone());
    if let Some(user) = ctx.user_id() {
        headers
            .entry("userId".to_string())
            .or_insert_with(|| user.to_string());
    }
    headers
}

fn context_of(failure: &SagaFailureDocument) -> MessageContext {
    MessageContext {
        message_id: failure.event_id.clone(),
        type_name: failure.event_type.clone(),
        correlation_id: failure
            .headers
            .get("correlationId")
            .cloned()
            .unwrap_or_else(|| failure.event_id.clone()),
        identity: None,
        authorization: None,
        headers: failure.headers.clone(),
    }
}

fn describe(e: &StoreError) -> String {
    let mut text = e.to_string();
    if text.len() > MAX_ERROR_LENGTH {
        let mut end = MAX_ERROR_LENGTH;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

/// The start key of a saga started by `key = value`: `<type>|<key>=<value>` with `%`, `|` and `=`
/// percent-escaped in each part (`%25`, `%7C`, `%3D`), so two different pairs never build the same
/// key; the same bytes as Kotlin's `SagaRuntime.startKey`.
pub(crate) fn start_key_of(type_name: &str, key: &str, value: &str) -> String {
    fn escape(part: &str) -> String {
        part.replace('%', "%25")
            .replace('|', "%7C")
            .replace('=', "%3D")
    }
    format!("{}|{}={}", escape(type_name), escape(key), escape(value))
}

#[cfg(test)]
mod tests {
    use super::start_key_of;

    /// The same inputs and expected strings as Kotlin's `SagaRuntimeTest` (start keys whose parts
    /// contain the separators stay apart): both libraries build the same key.
    #[test]
    fn start_keys_whose_parts_contain_the_separators_stay_apart() {
        assert_eq!(start_key_of("rt.Escape", "k=x", "1"), "rt.Escape|k%3Dx=1");
        assert_eq!(start_key_of("rt.Escape", "k", "x=1"), "rt.Escape|k=x%3D1");
        assert_eq!(
            start_key_of("rt.Escape|a%b", "k", "v"),
            "rt.Escape%7Ca%25b|k=v",
            "the type name and a percent sign are escaped too"
        );
        assert_eq!(
            start_key_of("rt.Order", "orderId", "o1"),
            "rt.Order|orderId=o1"
        );
    }
}
