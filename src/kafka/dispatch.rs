//! The local event dispatcher: runs one event inside this service, first through its ordinary
//! `event` handlers (stage 1), then through the saga stage (stage 2). Shared by the Kafka
//! [`EventConsumer`](super::EventConsumer) and, in the later S2 tasks, the partner-topic
//! consumers, the webhook ingress and the relay's `SagaCommandFailed` notices (ADR 0021 ruling 2).
//!
//! Stage 1 is the consumer's own behaviour: the idempotency marker (`processed_events`, id
//! `dedupe_id`) is inserted and the handlers run inside one Mongo transaction; a duplicate marker
//! skips the stage, a handler failure rolls the marker back. Stage 2 runs after stage 1 committed,
//! with markers and transactions of its own, so a redelivered record whose stage 1 is done goes
//! straight to it. **Stage 2 runs even when stage 1 failed** (ruling 26): the failure is held,
//! stage 2 runs, and then the failure is returned, so the caller redelivers or skips the record by
//! its own rules; a record that is skipped has still been seen by the sagas, and a redelivery
//! applies nothing twice because stage 2's markers decide. Both stages finish before the caller
//! acknowledges the record.

use std::sync::Arc;

use futures::future::BoxFuture;
use mongodb::bson::{DateTime, doc};
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::{Client, ClientSession, Database};
use tokio::sync::Mutex;

use super::PROCESSED_EVENTS_COLLECTION;
use crate::context::{CURRENT, MessageContext};
use crate::error::MessagingError;
use crate::hub::{EventContext, HandlerRegistry};

/// What [`LocalEventDispatcher::dispatch`] did with an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// An ordinary handler ran now, or the saga stage ran.
    Handled,
    /// The ordinary handlers had already run for this id and no saga is interested: nothing was done.
    Duplicate,
    /// No ordinary handler and no saga wants the type: nothing was done and no marker written.
    NoInterest,
    /// No ordinary handler, and the saga stage is interested in the type but applied the event to
    /// no saga instance (ruling 27: the relay logs and counts a `SagaCommandFailed` nobody got).
    Unclaimed,
}

/// The second stage of local event dispatch: the saga runtime. `interested` says whether any saga
/// type reacts to events of `type_name` (so a record no ordinary handler wants is still processed);
/// `process` hands the event to every interested saga type, each in its own transaction. By
/// contract `process` fails only when parking a failed event itself fails (the store is down, so
/// the record is redelivered instead of lost), never for a failing saga handler: such an event is
/// parked in `saga_failures` (ruling 3). It returns whether the event was applied to at least one
/// saga instance (an event a saga type already applied counts).
pub trait SagaStage: Send + Sync {
    fn interested(&self, type_name: &str) -> bool;

    /// The event types some saga type reacts to, for the processor status (ADR 0022); none by
    /// default.
    fn event_types(&self) -> Vec<String> {
        Vec::new()
    }

    fn process<'a>(
        &'a self,
        type_name: &'a str,
        payload: &'a [u8],
        ctx: &'a MessageContext,
    ) -> BoxFuture<'a, Result<bool, MessagingError>>;
}

/// No sagas: not interested in anything.
pub struct NoSagas;

impl SagaStage for NoSagas {
    fn interested(&self, _type_name: &str) -> bool {
        false
    }

    fn process<'a>(
        &'a self,
        _type_name: &'a str,
        _payload: &'a [u8],
        _ctx: &'a MessageContext,
    ) -> BoxFuture<'a, Result<bool, MessagingError>> {
        Box::pin(async { Ok(false) })
    }
}

/// See the module docs.
pub struct LocalEventDispatcher {
    client: Client,
    db: Database,
    registry: Arc<HandlerRegistry>,
    stage: Arc<dyn SagaStage>,
    /// Put before every stage-1 marker id: `processor:<name>:` for a named processor (ADR 0022).
    marker_prefix: Option<String>,
    /// The named processors whose stage 1 a [`dispatch`](Self::dispatch) also runs (ADR 0022):
    /// the local entry points reach every processor's handlers.
    fan_out: Vec<Arc<LocalEventDispatcher>>,
}

/// How stage 1 ended when it did not fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageOneDone {
    /// The handlers ran and the marker was committed with their effects.
    Handled,
    /// The marker already existed: nothing ran.
    Duplicate,
}

/// What each stage of one [`dispatch_stages`](LocalEventDispatcher::dispatch_stages) did.
#[derive(Debug)]
pub struct Stages {
    /// `None`: stage 1 did not run (no ordinary handler for the type, or it was not asked for).
    pub stage_one: Option<Result<StageOneDone, MessagingError>>,
    /// `None`: no saga is interested in the type. `Ok(applied)` as [`SagaStage::process`].
    pub stage_two: Option<Result<bool, MessagingError>>,
}

/// Where the record of a stage-1 marker sits (ADR 0023): the Kafka consumer's markers (and a dead
/// letter's retry, which is that record's stage 1) carry it, so a replay deletes exactly the
/// markers of its range. Markers of local dispatch, sagas, ingress and failure notices have none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerPosition {
    pub processor: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// How the Kafka consumer dispatches one record ([`LocalEventDispatcher::dispatch_record`]).
#[derive(Clone, Copy, Debug)]
pub struct RecordDispatch<'a> {
    /// Written into stage 1's marker.
    pub position: Option<&'a MarkerPosition>,
    /// Run stage 1 (false for a SEQUENCED record, which only the sagas see).
    pub stage_one: bool,
    /// The record is in a replay range (ADR 0023): stage 1 runs without the handlers registered
    /// with `disallow_replay`, and stage 2 (the sagas) does not run.
    pub replay: bool,
}

/// How a retried stage 1 ([`LocalEventDispatcher::stage_one_then`]) ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StageOneThen {
    /// The handlers ran and the marker and `finish`'s writes were committed together.
    Committed,
    /// The marker already existed: nothing ran, `finish` was not called.
    Duplicate,
    /// `finish` declined: the transaction was aborted (no marker, no handler effect).
    Declined,
}

/// The work a retried stage 1 does in its transaction after the handlers succeeded; `Ok(false)`
/// aborts it.
pub(crate) type FinishInTransaction<'a> = Box<
    dyn for<'s> FnOnce(&'s mut ClientSession) -> BoxFuture<'s, Result<bool, MessagingError>>
        + Send
        + 'a,
>;

enum MarkError {
    Duplicate,
    Other(MessagingError),
}

impl LocalEventDispatcher {
    pub fn new(
        client: Client,
        db: Database,
        registry: Arc<HandlerRegistry>,
        stage: Arc<dyn SagaStage>,
    ) -> LocalEventDispatcher {
        LocalEventDispatcher {
            client,
            db,
            registry,
            stage,
            marker_prefix: None,
            fan_out: Vec::new(),
        }
    }

    /// The named processors' dispatchers (ADR 0022): [`dispatch`](Self::dispatch), the path of
    /// the partner topics, the webhook ingress and the relay's notices, runs each one's stage 1
    /// too (its own transaction and marker family), the sagas still once. The Kafka consumer's
    /// [`dispatch_stages`](Self::dispatch_stages) stays with this dispatcher's own handlers.
    pub fn with_fan_out(mut self, named: Vec<Arc<LocalEventDispatcher>>) -> LocalEventDispatcher {
        self.fan_out = named;
        self
    }

    /// Prefixes every stage-1 marker id (`processor:<name>:` for a named processor, ADR 0022);
    /// `None` (the default) keeps `<dedupeId>`.
    pub fn with_marker_prefix(mut self, prefix: Option<String>) -> LocalEventDispatcher {
        self.marker_prefix = prefix;
        self
    }

    /// Whether any ordinary handler or saga type wants events of `type_name` (the named
    /// processors of [`with_fan_out`](Self::with_fan_out) included).
    pub fn interested(&self, type_name: &str) -> bool {
        self.interested_here(type_name) || self.fan_out.iter().any(|d| d.interested(type_name))
    }

    /// [`interested`](Self::interested) for this dispatcher's own handlers and sagas only: what
    /// its processor's consumer handles.
    pub fn interested_here(&self, type_name: &str) -> bool {
        self.registry.has_event_handler(type_name) || self.stage.interested(type_name)
    }

    /// Whether an ordinary handler (stage 1) wants events of `type_name`.
    pub fn has_stage_one(&self, type_name: &str) -> bool {
        self.registry.has_event_handler(type_name)
    }

    /// Every type this dispatcher handles (the ordinary handlers' and the sagas'), sorted.
    pub fn event_types(&self) -> Vec<String> {
        let mut all = self.registry.event_types();
        all.extend(self.stage.event_types());
        all.sort();
        all.dedup();
        all
    }

    fn marker_id(&self, dedupe_id: &str) -> String {
        match &self.marker_prefix {
            Some(prefix) => format!("{prefix}{dedupe_id}"),
            None => dedupe_id.to_string(),
        }
    }

    /// Dispatches the event of `type_name` with its JSON `payload`; `dedupe_id` keys stage 1's
    /// marker and `ctx` is what the handlers see. Fails with what stage 1 fails with (the caller
    /// retries), else with stage 2's failure; see the module docs.
    pub async fn dispatch(
        &self,
        dedupe_id: &str,
        type_name: &str,
        payload: &[u8],
        ctx: &MessageContext,
    ) -> Result<Outcome, MessagingError> {
        if !self.interested(type_name) {
            return Ok(Outcome::NoInterest);
        }
        // Every processor's stage 1, each in its own transaction, then the sagas once.
        let mut stage_ones: Vec<Result<StageOneDone, MessagingError>> = Vec::new();
        for d in std::iter::once(self).chain(self.fan_out.iter().map(Arc::as_ref)) {
            if d.registry.has_event_handler(type_name) {
                stage_ones.push(
                    d.stage_one_done(dedupe_id, type_name, payload, ctx, LOCAL)
                        .await,
                );
            }
        }
        let stage_two = if self.stage.interested(type_name) {
            Some(self.stage.process(type_name, payload, ctx).await)
        } else {
            None
        };
        let saga = stage_two.is_some();
        let ordinary = !stage_ones.is_empty();
        let mut applied = false;
        let stage_two_failure = match stage_two {
            Some(Ok(done)) => {
                applied = done;
                None
            }
            Some(Err(e)) => Some(e),
            None => None,
        };
        let mut duplicate = ordinary;
        let mut stage_one_failure = None;
        for result in stage_ones {
            match result {
                Ok(StageOneDone::Duplicate) => {}
                Ok(StageOneDone::Handled) => duplicate = false,
                Err(e) => {
                    duplicate = false;
                    match &stage_one_failure {
                        None => stage_one_failure = Some(e),
                        Some(_) => {
                            tracing::error!(r#type = %type_name, id = %dedupe_id, "another processor's handlers failed as well: {e}")
                        }
                    }
                }
            }
        }
        if let Some(e) = stage_one_failure {
            if let Some(also) = stage_two_failure {
                // Both failed: the caller redelivers for stage 1's, stage 2's is kept in the log.
                tracing::error!(r#type = %type_name, id = %dedupe_id, "saga stage failed as well: {also}");
            }
            return Err(e);
        }
        if let Some(e) = stage_two_failure {
            return Err(e);
        }
        Ok(if duplicate && !saga {
            Outcome::Duplicate
        } else if !ordinary && !applied {
            Outcome::Unclaimed
        } else {
            Outcome::Handled
        })
    }

    async fn stage_one_done(
        &self,
        dedupe_id: &str,
        type_name: &str,
        payload: &[u8],
        ctx: &MessageContext,
        record: RecordDispatch<'_>,
    ) -> Result<StageOneDone, MessagingError> {
        self.stage_one(dedupe_id, type_name, payload, ctx, record, None)
            .await
            .map(|done| match done {
                StageOneThen::Duplicate => {
                    tracing::debug!(r#type = %type_name, id = %dedupe_id, "duplicate event skipped");
                    StageOneDone::Duplicate
                }
                StageOneThen::Committed | StageOneThen::Declined => StageOneDone::Handled,
            })
    }

    /// [`dispatch`](Self::dispatch) with each stage's result apart: stage 1 (the ordinary
    /// handlers, when `run_stage_one` and one wants the type), then stage 2 (the sagas, when one is
    /// interested), which runs whatever stage 1 did (ruling 26). The Kafka consumer parks a record
    /// only for stage 1's failure, and runs a SEQUENCED record through stage 2 alone (ADR 0022).
    pub async fn dispatch_stages(
        &self,
        dedupe_id: &str,
        type_name: &str,
        payload: &[u8],
        ctx: &MessageContext,
        run_stage_one: bool,
    ) -> Stages {
        self.dispatch_record(
            dedupe_id,
            type_name,
            payload,
            ctx,
            RecordDispatch {
                stage_one: run_stage_one,
                ..LOCAL
            },
        )
        .await
    }

    /// [`dispatch_stages`](Self::dispatch_stages) for a Kafka record: stage 1's marker carries
    /// the record's position, and a record of a replay range runs stage 1 without the handlers
    /// that disallow replay and skips stage 2 (ADR 0023).
    pub async fn dispatch_record(
        &self,
        dedupe_id: &str,
        type_name: &str,
        payload: &[u8],
        ctx: &MessageContext,
        record: RecordDispatch<'_>,
    ) -> Stages {
        let stage_one = if record.stage_one && self.registry.has_event_handler(type_name) {
            Some(
                self.stage_one_done(dedupe_id, type_name, payload, ctx, record)
                    .await,
            )
        } else {
            None
        };
        // Stage 1's failure is held until stage 2 ran: the sagas must not miss an event because an
        // ordinary handler failed. A replay never reaches the sagas: they saw the record already.
        let stage_two = if !record.replay && self.stage.interested(type_name) {
            Some(self.stage.process(type_name, payload, ctx).await)
        } else {
            None
        };
        Stages {
            stage_one,
            stage_two,
        }
    }

    /// Stage 1 alone, with `finish` run in its transaction after the handlers succeeded (a dead
    /// letter's retry deletes the letter there, ADR 0022): the marker, the handlers' effects and
    /// `finish`'s writes commit together or not at all. Stage 2 does not run. `record`: the
    /// letter's position, and `replay` when the record was parked while its processor replayed
    /// it, so the handlers that disallow replay are left out (ADR 0023).
    pub(crate) async fn stage_one_then(
        &self,
        dedupe_id: &str,
        type_name: &str,
        payload: &[u8],
        ctx: &MessageContext,
        record: RecordDispatch<'_>,
        finish: FinishInTransaction<'_>,
    ) -> Result<StageOneThen, MessagingError> {
        self.stage_one(dedupe_id, type_name, payload, ctx, record, Some(finish))
            .await
    }

    /// The ordinary handlers: the marker first (a duplicate id never reaches a handler), then the
    /// handlers, all in one transaction; a handler failure rolls the marker back.
    async fn stage_one(
        &self,
        dedupe_id: &str,
        type_name: &str,
        payload: &[u8],
        ctx: &MessageContext,
        record: RecordDispatch<'_>,
        finish: Option<FinishInTransaction<'_>>,
    ) -> Result<StageOneThen, MessagingError> {
        let mut session = self.client.start_session().await?;
        session.start_transaction().await?;
        match self
            .mark_processed(
                &mut session,
                &self.marker_id(dedupe_id),
                type_name,
                record.position,
            )
            .await
        {
            Ok(()) => {}
            Err(MarkError::Duplicate) => {
                let _ = session.abort_transaction().await;
                return Ok(StageOneThen::Duplicate);
            }
            Err(MarkError::Other(e)) => {
                let _ = session.abort_transaction().await;
                return Err(e);
            }
        }
        let session = Arc::new(Mutex::new(session));
        let result = self
            .run_handlers(type_name, payload, ctx, record.replay, Arc::clone(&session))
            .await;
        let mut guard = session.lock().await;
        let result = match (result, finish) {
            (Ok(()), Some(finish)) => match finish(&mut guard).await {
                Ok(true) => Ok(()),
                Ok(false) => {
                    let _ = guard.abort_transaction().await;
                    return Ok(StageOneThen::Declined);
                }
                Err(e) => Err(e),
            },
            (result, _) => result,
        };
        match result {
            Ok(()) => {
                guard.commit_transaction().await?;
                Ok(StageOneThen::Committed)
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
        ctx: &MessageContext,
        replay: bool,
        session: Arc<Mutex<ClientSession>>,
    ) -> Result<(), MessagingError> {
        // In a replay the handlers that disallow it are left out; the marker is still written, so
        // the record counts as handled for them too.
        let handlers = self.registry.event_handlers_for(type_name, replay);
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
        position: Option<&MarkerPosition>,
    ) -> Result<(), MarkError> {
        let mut marker = doc! { "_id": id, "type": type_name, "processedAt": DateTime::now() };
        if let Some(p) = position {
            marker.insert("processor", &p.processor);
            marker.insert("topic", &p.topic);
            marker.insert("partition", p.partition);
            marker.insert("offset", p.offset);
        }
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

/// A local dispatch: no position, stage 1 asked for, not a replay.
const LOCAL: RecordDispatch<'static> = RecordDispatch {
    position: None,
    stage_one: true,
    replay: false,
};

fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    matches!(&*e.kind, ErrorKind::Write(WriteFailure::WriteError(w)) if w.code == 11000)
}
