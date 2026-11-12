//! Idempotent adapter handlers (ADR 0021 rulings 12, 17, 38, 39, 41-43): a command or event
//! handler that calls an external system, recorded in `external_calls` under its idempotency key
//! so a repeat of the key never reaches the external system twice. The Rust form of the Kotlin
//! `@IdempotentHandler` and its `IdempotencyInterceptor`.
//!
//! 1. The record is inserted `IN_PROGRESS` with `leaseUntil = now + lease`; a duplicate key means
//!    an earlier call of the key exists.
//! 2. An earlier `SUCCEEDED` call answers with its stored reply, `FAILED` with its stored error
//!    (permanent), `UNKNOWN` with [`MessagingError::UnknownOutcome`]. An `IN_PROGRESS` one within
//!    its lease is another call in flight: [`MessagingError::RetryableExternal`] (come back
//!    later). Past its lease the outcome is unknown: without `external_idempotent` the record
//!    becomes `UNKNOWN` (never called again automatically) and the call reports the unknown
//!    outcome; with it (the partner de-duplicates by the key the adapter passes it) the call takes
//!    the record over (`attempts + 1`) and runs the body.
//! 3. After the body: `SUCCEEDED` with the reply as JSON text (when that write fails the call is
//!    an unknown outcome, never a failed one, ruling 38); a `RetryableExternal` deletes the record
//!    (the next call starts afresh, the relay owns the backoff); an `UnknownOutcome` returned by
//!    the adapter itself leaves the record `UNKNOWN` (ruling 39); any other failure (a panic
//!    included) is `FAILED` with `{"code","message"}`.
//!
//! 4. Ownership (ruling 70, amended in H1): every write after `begin` filters on the key,
//!    `IN_PROGRESS` and the `owner` token this call wrote (a random id, at the insert or the
//!    takeover), so a call whose lease ran out never overwrites or deletes the record of the call
//!    that took it over, gave it up as `UNKNOWN`, or began it afresh after a retryable reset (the
//!    `attempts` count starts at 1 again then; a token never repeats). A settled record has no
//!    `owner`. A `SUCCEEDED` write that matches nothing is logged at warn and never reported as
//!    success (`RetryableExternal` with `external_idempotent`, else `UnknownOutcome`); a failure
//!    write that matches nothing is logged at warn and the call still returns its own error. The
//!    expired-lease change to `UNKNOWN` reads the record again when it matched nothing (the slow
//!    call finished meanwhile).
//!
//! Every write of the record is a single-document write of its own, outside any session: a
//! rollback of the caller's transaction (an event handler runs inside stage 1's) never forgets a
//! call that happened. The body itself runs where the caller runs it: an event handler's inside
//! stage 1's transaction (ruling 43, so a slow call there holds that transaction open), a command
//! handler's (what the saga relay calls) in none. Every handler of a saga command must be
//! idempotent by its `idempotency-key` (ruling 42): the relay re-sends after a timeout or a crash.
//! `idempotency-key` is a plain header any sender can set, so adapters must stay internal.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use mongodb::bson::{Bson, DateTime, doc};
use mongodb::options::ReturnDocument;
use mongodb::{Collection, Database};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::saga::store::{EXTERNAL_CALLS_COLLECTION, ExternalCallDocument, ExternalCallStatus};
use crate::saga::{Clock, ExternalOutcome, SagaMetrics};

/// The header the saga relay puts the idempotency key (`<sagaId>:<seq>`) in.
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

const MAX_BEGIN_ROUNDS: usize = 3;
const MAX_ERROR_LENGTH: usize = 2000;

/// Settings of `external_calls` (`KRAKEN_EXTERNAL_CALLS_LEASE_MS`): how long an `IN_PROGRESS`
/// call may take before its outcome counts as unknown. Default 5 minutes (ruling 16).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalCallSettings {
    pub lease: Duration,
}

impl Default for ExternalCallSettings {
    fn default() -> Self {
        ExternalCallSettings {
            lease: Duration::from_secs(300),
        }
    }
}

impl ExternalCallSettings {
    /// `InvalidMessage` unless the lease is positive.
    pub fn validate(&self) -> Result<(), MessagingError> {
        if self.lease.is_zero() {
            return Err(MessagingError::InvalidMessage(
                "external calls lease must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Where an idempotent handler's key comes from (Kotlin's SpEL `key`): a message header, by
/// default `idempotency-key` (what the saga relay sets), or a function of the message and its
/// context, e.g. a polling adapter's `poll:<jobId>:<attempt>`. A key that comes out empty (the
/// header is missing) fails the call naming the handler; the body does not run.
pub enum IdempotencyKey<M> {
    Header(&'static str),
    From(fn(&M, &MessageContext) -> String),
}

impl<M> Default for IdempotencyKey<M> {
    fn default() -> Self {
        IdempotencyKey::Header(IDEMPOTENCY_KEY_HEADER)
    }
}

impl<M> Clone for IdempotencyKey<M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M> Copy for IdempotencyKey<M> {}

impl<M> IdempotencyKey<M> {
    /// The key of `message` handled with `ctx`; a handler error naming `label` when it is empty.
    pub fn resolve(
        &self,
        message: &M,
        ctx: &MessageContext,
        label: &str,
    ) -> Result<String, MessagingError> {
        let (key, source) = match self {
            IdempotencyKey::Header(name) => (
                ctx.headers.get(*name).cloned().unwrap_or_default(),
                format!("header '{name}'"),
            ),
            IdempotencyKey::From(f) => (f(message, ctx), "key function".to_string()),
        };
        if key.trim().is_empty() {
            return Err(handler_error(
                label,
                format!(
                    "Handler {label}: the idempotency key ({source}) is empty for this message (a missing header?)"
                ),
            ));
        }
        Ok(key)
    }
}

fn handler_error(label: &str, message: String) -> MessagingError {
    MessagingError::RemoteHandler {
        type_name: label.to_string(),
        message,
        details: Vec::new(),
    }
}

/// What `begin` decided: run the body under the ownership token `owner` (written on the record
/// with `attempts`), or the call is answered from the record.
enum Begin {
    Run { attempts: i32, owner: String },
    Replay(Option<String>),
}

/// The bookkeeping of the idempotent handlers over `external_calls`; see the module docs. One per
/// service, shared by its idempotent handlers through an [`ExternalCallsHandle`].
pub struct ExternalCalls {
    calls: Collection<ExternalCallDocument>,
    clock: Clock,
    settings: ExternalCallSettings,
    metrics: Arc<dyn SagaMetrics>,
    fail_next_succeeded_write: AtomicBool,
    pause_before_expired_write: Mutex<Option<BoxFuture<'static, ()>>>,
}

impl ExternalCalls {
    pub fn new(
        db: Database,
        clock: Clock,
        settings: ExternalCallSettings,
        metrics: Arc<dyn SagaMetrics>,
    ) -> Result<Arc<ExternalCalls>, MessagingError> {
        settings.validate()?;
        Ok(Arc::new(ExternalCalls {
            calls: db.collection(EXTERNAL_CALLS_COLLECTION),
            clock,
            settings,
            metrics,
            fail_next_succeeded_write: AtomicBool::new(false),
            pause_before_expired_write: Mutex::new(None),
        }))
    }

    /// A handle bound to these calls, for [`HandlerRegistry`](crate::hub::HandlerRegistry)'s
    /// idempotent registrations.
    pub fn handle(self: &Arc<Self>) -> ExternalCallsHandle {
        let handle = ExternalCallsHandle::new();
        handle.bind(Arc::clone(self));
        handle
    }

    /// Test seam: the next write of a `SUCCEEDED` record fails (the store went away right after
    /// the external call succeeded).
    #[doc(hidden)]
    pub fn fail_next_succeeded_write(&self) {
        self.fail_next_succeeded_write.store(true, Ordering::SeqCst);
    }

    /// Test seam: the next call that finds an `IN_PROGRESS` record past its lease awaits `pause`
    /// after reading the record and before writing it (another call acts in between).
    #[doc(hidden)]
    pub fn pause_before_next_expired_write(&self, pause: BoxFuture<'static, ()>) {
        *self.pause_before_expired_write.lock().unwrap() = Some(pause);
    }

    async fn maybe_pause_before_expired_write(&self) {
        let pause = self.pause_before_expired_write.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.await;
        }
    }

    /// Runs `body` (the handler's call to the external system) under the idempotency rules for
    /// `key`; returns its result, or the stored reply. `label` names the handler (its message's
    /// wire type name) in errors, logs and metrics.
    pub async fn execute<R, Fut>(
        &self,
        label: &str,
        key: &str,
        external_idempotent: bool,
        body: Fut,
    ) -> Result<R, MessagingError>
    where
        R: Serialize + DeserializeOwned + 'static,
        Fut: Future<Output = Result<R, MessagingError>>,
    {
        let (attempts, owner) = match self.begin(key, external_idempotent, label).await? {
            Begin::Replay(reply) => {
                self.metrics.external_call(label, ExternalOutcome::Replayed);
                let reply = reply.unwrap_or_else(|| "{}".to_string());
                return codec::decode_reply::<R>(reply.as_bytes()).map_err(|e| {
                    handler_error(
                        label,
                        format!("Handler {label}: the stored reply of the call with key '{key}' cannot be read: {e}"),
                    )
                });
            }
            Begin::Run { attempts, owner } => (attempts, owner),
        };
        let owned = Owned {
            key,
            attempts,
            owner: &owner,
        };
        // A dropped future (the request was cancelled) leaves the record IN_PROGRESS: whether the
        // external system was reached is unknown, and its lease decides.
        let result = match AssertUnwindSafe(body).catch_unwind().await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                self.settle_failure(&owned, &e, label).await;
                return Err(e);
            }
            Err(_) => {
                let e = handler_error(label, format!("Handler {label}: handler panicked"));
                self.settle_failure(&owned, &e, label).await;
                return Err(e);
            }
        };
        let reply = codec::encode_reply(&result).and_then(|bytes| {
            String::from_utf8(bytes).map_err(|e| MessagingError::Storage(e.to_string()))
        });
        match self.record_success(&owned, reply).await {
            Ok(true) => {}
            Ok(false) => return Err(self.taken_over(&owned, label, external_idempotent)),
            Err(e) => return Err(self.unrecorded(&owned, label, external_idempotent, e).await),
        }
        self.metrics
            .external_call(label, ExternalOutcome::Succeeded);
        Ok(result)
    }

    fn now(&self) -> DateTime {
        self.clock.now_bson()
    }

    fn lease_from_now(&self) -> DateTime {
        DateTime::from_system_time(self.clock.now() + self.settings.lease)
    }

    async fn begin(
        &self,
        key: &str,
        external_idempotent: bool,
        label: &str,
    ) -> Result<Begin, MessagingError> {
        // The body has not run yet, so a store failure here is safe to retry.
        let store_down = |e: mongodb::error::Error| {
            self.metrics
                .external_call(label, ExternalOutcome::Retryable);
            MessagingError::RetryableExternal(format!(
                "The call with key '{key}' (handler {label}) could not be recorded, the body did not run: {e}"
            ))
        };
        for _ in 0..MAX_BEGIN_ROUNDS {
            let now = self.now();
            let owner = uuid::Uuid::new_v4().to_string();
            let record = ExternalCallDocument {
                id: key.to_string(),
                status: ExternalCallStatus::InProgress,
                reply: None,
                error: None,
                attempts: 1,
                updated_at: now,
                lease_until: Some(self.lease_from_now()),
                owner: Some(owner.clone()),
            };
            match self.calls.insert_one(&record).await {
                Ok(_) => return Ok(Begin::Run { attempts: 1, owner }),
                Err(e) if crate::saga::ports::is_duplicate_key(&e) => {} // an earlier call exists
                Err(e) => return Err(store_down(e)),
            }
            let Some(existing) = self
                .calls
                .find_one(doc! { "_id": key })
                .await
                .map_err(store_down)?
            else {
                continue; // reset or expired meanwhile: insert again
            };
            match existing.status {
                ExternalCallStatus::Succeeded => return Ok(Begin::Replay(existing.reply)),
                ExternalCallStatus::Failed => {
                    self.metrics.external_call(label, ExternalOutcome::Failed);
                    return Err(handler_error(label, stored_message(&existing)));
                }
                ExternalCallStatus::Unknown => return Err(self.unknown(key, label)),
                ExternalCallStatus::InProgress => {
                    let lease = existing.lease_until;
                    if lease.is_some_and(|until| until > now) {
                        self.metrics
                            .external_call(label, ExternalOutcome::Retryable);
                        return Err(MessagingError::RetryableExternal(format!(
                            "The call with key '{key}' is already in progress (handler {label})"
                        )));
                    }
                    self.maybe_pause_before_expired_write().await;
                    let same_record = doc! {
                        "_id": key,
                        "status": "IN_PROGRESS",
                        "owner": existing.owner.map(Bson::String).unwrap_or(Bson::Null),
                        "leaseUntil": lease.map(Bson::DateTime).unwrap_or(Bson::Null),
                    };
                    if external_idempotent {
                        let taken = self
                            .calls
                            .find_one_and_update(
                                same_record,
                                doc! {
                                    "$set": {
                                        "leaseUntil": self.lease_from_now(),
                                        "updatedAt": now,
                                        "owner": &owner,
                                    },
                                    "$inc": { "attempts": 1 },
                                },
                            )
                            .return_document(ReturnDocument::After)
                            .await
                            .map_err(store_down)?;
                        if let Some(taken) = taken {
                            return Ok(Begin::Run {
                                attempts: taken.attempts,
                                owner,
                            });
                        }
                    } else {
                        let changed = self
                            .calls
                            .update_one(
                                same_record,
                                doc! {
                                    "$set": { "status": "UNKNOWN", "updatedAt": now },
                                    "$unset": { "leaseUntil": "", "owner": "" },
                                },
                            )
                            .await
                            .map_err(store_down)?;
                        if changed.matched_count == 1 {
                            return Err(self.unknown(key, label));
                        }
                    }
                    // The record changed since it was read (the slow call finished, another call
                    // took it over or settled it): read it again.
                }
            }
        }
        self.metrics
            .external_call(label, ExternalOutcome::Retryable);
        Err(MessagingError::RetryableExternal(format!(
            "The call with key '{key}' kept changing under this call (handler {label})"
        )))
    }

    /// Writes `SUCCEEDED` with `reply` (the encoded reply, or why it could not be encoded) on the
    /// record this call still owns; false when it owns it no longer (logged at warn).
    async fn record_success(
        &self,
        owned: &Owned<'_>,
        reply: Result<String, MessagingError>,
    ) -> Result<bool, MessagingError> {
        if self.fail_next_succeeded_write.swap(false, Ordering::SeqCst) {
            return Err(MessagingError::Storage(
                "injected: the SUCCEEDED write failed".into(),
            ));
        }
        let reply = reply?;
        let written = self
            .calls
            .update_one(
                owned.filter(),
                doc! {
                    "$set": { "status": "SUCCEEDED", "reply": reply, "updatedAt": self.now() },
                    "$unset": { "leaseUntil": "", "owner": "", "error": "" },
                },
            )
            .await?;
        Ok(owned.wrote(written.matched_count, "SUCCEEDED"))
    }

    /// The body succeeded but the record is no longer this call's (its lease ran out and another
    /// call took it over or settled it): the success cannot be proven by the record, so it is not
    /// reported. With `external_idempotent` a repeat is safe (retryable); otherwise unknown.
    fn taken_over(
        &self,
        owned: &Owned<'_>,
        label: &str,
        external_idempotent: bool,
    ) -> MessagingError {
        let key = owned.key;
        if external_idempotent {
            self.metrics
                .external_call(label, ExternalOutcome::Retryable);
            return MessagingError::RetryableExternal(format!(
                "The call with key '{key}' (handler {label}) ran past its lease and another call took its record over; a repeat is safe because the partner de-duplicates by the key"
            ));
        }
        self.unknown(key, label)
    }

    fn unknown(&self, key: &str, label: &str) -> MessagingError {
        self.metrics.external_call(label, ExternalOutcome::Unknown);
        MessagingError::UnknownOutcome(format!(
            "The outcome of the call with key '{key}' is unknown: an earlier call of handler {label} did not finish and the external system may have acted"
        ))
    }

    /// The body returned but its `SUCCEEDED` could not be written: the external system acted and
    /// the record cannot say so, an unknown outcome and never a failed call (ruling 38). With
    /// `external_idempotent` the partner de-duplicates a repeat, so it is retryable (the record
    /// stays `IN_PROGRESS`, its lease settles it); otherwise the record is moved to `UNKNOWN` (best
    /// effort: if that fails too the lease does it later).
    async fn unrecorded(
        &self,
        owned: &Owned<'_>,
        label: &str,
        external_idempotent: bool,
        cause: MessagingError,
    ) -> MessagingError {
        let key = owned.key;
        tracing::error!(key = %key, handler = %label, "the call ran but its outcome could not be recorded: {cause}");
        if external_idempotent {
            self.metrics
                .external_call(label, ExternalOutcome::Retryable);
            return MessagingError::RetryableExternal(format!(
                "The call with key '{key}' (handler {label}) ran but its outcome could not be recorded; a repeat is safe because the partner de-duplicates by the key: {cause}"
            ));
        }
        self.mark_unknown(owned).await;
        self.unknown(key, label)
    }

    async fn mark_unknown(&self, owned: &Owned<'_>) {
        match self
            .calls
            .update_one(
                owned.filter(),
                doc! {
                    "$set": { "status": "UNKNOWN", "updatedAt": self.now() },
                    "$unset": { "leaseUntil": "", "owner": "" },
                },
            )
            .await
        {
            Ok(written) => {
                owned.wrote(written.matched_count, "UNKNOWN");
            }
            Err(e) => {
                tracing::warn!(key = %owned.key, "could not mark the call UNKNOWN (its lease will): {e}")
            }
        }
    }

    /// The record after the body failed: reset for a retryable failure, `UNKNOWN` for an unknown
    /// outcome, `FAILED` with the error for any other. Never masks the failure.
    async fn settle_failure(&self, owned: &Owned<'_>, e: &MessagingError, label: &str) {
        let written = match e {
            MessagingError::RetryableExternal(_) => {
                self.metrics
                    .external_call(label, ExternalOutcome::Retryable);
                self.calls.delete_one(owned.filter()).await.map(|deleted| {
                    owned.wrote(deleted.deleted_count, "deleted");
                })
            }
            MessagingError::UnknownOutcome(_) => {
                // The adapter cannot tell whether the partner acted: not FAILED (a redelivery would
                // report a plain failure and a person would think it safe to call again).
                self.mark_unknown(owned).await;
                self.metrics.external_call(label, ExternalOutcome::Unknown);
                Ok(())
            }
            other => {
                self.metrics.external_call(label, ExternalOutcome::Failed);
                let message: String = other.to_string().chars().take(MAX_ERROR_LENGTH).collect();
                let error =
                    serde_json::json!({ "code": "HANDLER_ERROR", "message": message }).to_string();
                self.calls
                    .update_one(
                        owned.filter(),
                        doc! {
                            "$set": { "status": "FAILED", "error": error, "updatedAt": self.now() },
                            "$unset": { "leaseUntil": "", "owner": "" },
                        },
                    )
                    .await
                    .map(|written| {
                        owned.wrote(written.matched_count, "FAILED");
                    })
            }
        };
        if let Err(bookkeeping) = written {
            tracing::warn!(key = %owned.key, handler = %label, "could not record the failure of the call: {bookkeeping}");
        }
    }
}

/// The record a running call owns: its key and the `owner` token the call wrote when it inserted
/// or took the record over (`attempts`, its count then, is for the log). Every write after `begin`
/// filters on the token and on `IN_PROGRESS`, so a call whose lease ran out never writes over (or
/// deletes) the record of the call that took it over, settled it or began it afresh (ruling 70).
struct Owned<'a> {
    key: &'a str,
    attempts: i32,
    owner: &'a str,
}

impl Owned<'_> {
    fn filter(&self) -> mongodb::bson::Document {
        doc! { "_id": self.key, "status": "IN_PROGRESS", "owner": self.owner }
    }

    /// Whether a write matched the owned record; when it did not, says so at warn.
    fn wrote(&self, matched: u64, what: &str) -> bool {
        if matched == 0 {
            tracing::warn!(
                key = %self.key,
                attempts = self.attempts,
                "the call's record is no longer its own (its lease ran out and another call took it over, settled it or began it afresh): not {what}"
            );
        }
        matched > 0
    }
}

/// The message of a stored `FAILED` call's error.
fn stored_message(doc: &ExternalCallDocument) -> String {
    doc.error
        .as_deref()
        .and_then(|e| serde_json::from_str::<serde_json::Value>(e).ok())
        .and_then(|v| {
            v.get("message")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("The call with key '{}' failed earlier", doc.id))
}

/// Where an idempotent handler finds the [`ExternalCalls`] it records in: registered handlers
/// hold the handle, and [`KrakenMessagingBuilder::start`](crate::KrakenMessagingBuilder::start)
/// binds it (with the configured lease, the clock and the statistics), so a handler registered
/// before the clock was set still uses it. A call through a handle that was never bound fails
/// naming the handler (Kotlin's missing interceptor, ruling 41: with the builder it cannot happen).
#[derive(Clone, Default)]
pub struct ExternalCallsHandle(Arc<OnceLock<Arc<ExternalCalls>>>);

impl ExternalCallsHandle {
    /// An unbound handle.
    pub fn new() -> ExternalCallsHandle {
        ExternalCallsHandle::default()
    }

    /// Binds the handle; a second bind is ignored (the first stays).
    pub fn bind(&self, calls: Arc<ExternalCalls>) {
        let _ = self.0.set(calls);
    }

    pub fn is_bound(&self) -> bool {
        self.0.get().is_some()
    }

    /// The bound calls, or a handler error naming `label`.
    pub fn get(&self, label: &str) -> Result<Arc<ExternalCalls>, MessagingError> {
        self.0.get().cloned().ok_or_else(|| {
            handler_error(
                label,
                format!(
                    "Handler {label} is idempotent but no external calls are bound to it (KrakenMessagingBuilder::start binds them; with a bare HandlerRegistry pass ExternalCalls::handle())"
                ),
            )
        })
    }
}
