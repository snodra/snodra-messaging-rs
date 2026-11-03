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
}

/// How stage 1 ended when it did not fail.
enum StageOne {
    Handled,
    Duplicate,
}

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
        }
    }

    /// Whether any ordinary handler or saga type wants events of `type_name`.
    pub fn interested(&self, type_name: &str) -> bool {
        self.registry.has_event_handler(type_name) || self.stage.interested(type_name)
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
        let ordinary = self.registry.has_event_handler(type_name);
        let saga = self.stage.interested(type_name);
        if !ordinary && !saga {
            return Ok(Outcome::NoInterest);
        }

        let mut duplicate = false;
        let mut stage1_failure = None;
        if ordinary {
            match self.stage_one(dedupe_id, type_name, payload, ctx).await {
                Ok(StageOne::Handled) => {}
                Ok(StageOne::Duplicate) => {
                    tracing::debug!(r#type = %type_name, id = %dedupe_id, "duplicate event skipped");
                    duplicate = true;
                }
                // Held until stage 2 ran: the sagas must not miss an event because an ordinary
                // handler failed.
                Err(e) => stage1_failure = Some(e),
            }
        }
        let mut applied = false;
        if saga {
            match self.stage.process(type_name, payload, ctx).await {
                Ok(done) => applied = done,
                Err(e) if stage1_failure.is_none() => return Err(e),
                // Both failed: the caller redelivers for stage 1's, stage 2's is kept in the log.
                Err(e) => {
                    tracing::error!(r#type = %type_name, id = %dedupe_id, "saga stage failed as well: {e}")
                }
            }
        }
        if let Some(e) = stage1_failure {
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

    /// The ordinary handlers: the marker first (a duplicate id never reaches a handler), then the
    /// handlers, all in one transaction; a handler failure rolls the marker back.
    async fn stage_one(
        &self,
        dedupe_id: &str,
        type_name: &str,
        payload: &[u8],
        ctx: &MessageContext,
    ) -> Result<StageOne, MessagingError> {
        let mut session = self.client.start_session().await?;
        session.start_transaction().await?;
        match self
            .mark_processed(&mut session, dedupe_id, type_name)
            .await
        {
            Ok(()) => {}
            Err(MarkError::Duplicate) => {
                let _ = session.abort_transaction().await;
                return Ok(StageOne::Duplicate);
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
                Ok(StageOne::Handled)
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

fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    matches!(&*e.kind, ErrorKind::Write(WriteFailure::WriteError(w)) if w.code == 11000)
}
