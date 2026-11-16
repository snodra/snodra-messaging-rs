//! Processor control (ADR 0023): [`ProcessorControl`], from
//! [`KrakenMessaging::processor_control`](crate::KrakenMessaging::processor_control), answers the
//! hub's [`ControlRequest`]s and pauses or resumes a processor
//! locally. One action per processor at a time on an instance; a renewal of the reset this
//! instance holds is answered at once, never behind another action, and so is a dead-letter
//! request while a reset of the processor executes here: a list is read beside the execute, a
//! retry, retry-all or discard is refused `LOCKED` (the hub would time it out while it waited).
//! Every action is logged at INFO with its `control_id`, processor, action and result.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use super::DeadLetters;
use super::admin::RetryResult;
use super::monitor::cut_bytes;
use super::runner::{ProcessorRunner, Refusal};
use crate::config::DeadLetterMode;
use crate::error::MessagingError;
use crate::event_store::ResetTarget;
use crate::proto::control_reply::Result as Answer;
use crate::proto::control_request::Action;
use crate::proto::{self, ControlRefusal, ControlReply, ControlRequest};
use crate::store::DeadLetter;

/// Who paused a processor through [`ProcessorControl::pause`] (the control state's `paused_by`).
pub const PAUSED_LOCALLY: &str = "local";

/// The most bytes of a refusal's message, an `ok` detail, a retry's error and a letter's error
/// message (the hub cuts there too).
pub const MAX_CONTROL_TEXT_BYTES: usize = 512;
/// The most letters one `dead_letter_list` returns.
pub const MAX_LIST_LIMIT: u32 = 100;
/// The shortest lease a `reset_prepare` may ask for (the hub asks for 30 s): a shorter one would
/// lapse before anything happened and have the executor extend it against the store in a spin.
pub const MIN_RESET_LEASE_MS: i64 = 1000;
/// The refusal of a prepare without an operation id or with a lease under [`MIN_RESET_LEASE_MS`].
pub(crate) const BAD_PREPARE: &str =
    "a prepare needs an operation id and a lease of at least 1000 ms";

/// See the module docs. Cheap to clone.
#[derive(Clone)]
pub struct ProcessorControl {
    inner: Arc<Inner>,
}

struct Inner {
    /// The processors whose consumer runs here.
    runners: BTreeMap<String, Arc<ProcessorRunner>>,
    dead_letters: DeadLetters,
    /// Per processor this instance knows (a runner's, or a dead-letter processor's own).
    serial: BTreeMap<String, Arc<tokio::sync::Mutex<()>>>,
}

impl ProcessorControl {
    /// The control of `runners` (the processors consuming here) and of the letters of the
    /// processors `dead_letters` serves.
    pub(crate) fn new(
        runners: Vec<Arc<ProcessorRunner>>,
        dead_letters: DeadLetters,
    ) -> ProcessorControl {
        let mut serial: BTreeMap<String, Arc<tokio::sync::Mutex<()>>> = dead_letters
            .processor_names()
            .into_iter()
            .map(|p| (p, Arc::new(tokio::sync::Mutex::new(()))))
            .collect();
        for r in &runners {
            serial.insert(r.name().to_string(), r.serial());
        }
        ProcessorControl {
            inner: Arc::new(Inner {
                runners: runners
                    .into_iter()
                    .map(|r| (r.name().to_string(), r))
                    .collect(),
                dead_letters,
                serial,
            }),
        }
    }

    /// The processors whose consumer runs on this instance, by name.
    pub fn processors(&self) -> Vec<String> {
        self.inner.runners.keys().cloned().collect()
    }

    /// Pauses `processor` on every instance (`pausedBy: "local"`): it is written to its control
    /// document, applied here at once and by the other instances within their next poll (5 s).
    /// An error names a processor that does not run here, or the database's failure.
    pub async fn pause(&self, processor: &str) -> Result<(), MessagingError> {
        self.set_paused(processor, true).await
    }

    /// Resumes `processor` on every instance; see [`pause`](Self::pause).
    pub async fn resume(&self, processor: &str) -> Result<(), MessagingError> {
        self.set_paused(processor, false).await
    }

    async fn set_paused(&self, processor: &str, paused: bool) -> Result<(), MessagingError> {
        let runner = self.inner.runners.get(processor).ok_or_else(|| {
            MessagingError::InvalidMessage(format!(
                "processor '{processor}' does not run on this instance"
            ))
        })?;
        let _serial = runner.serial().lock_owned().await;
        runner.pause(paused, PAUSED_LOCALLY).await?;
        tracing::info!(processor = %processor, action = if paused { "pause" } else { "resume" }, by = PAUSED_LOCALLY, "processor control");
        Ok(())
    }

    /// Test seam: the next poll of `processor` that finds its held lease passed awaits `pause`
    /// between reading the control document and clearing the lock.
    #[doc(hidden)]
    pub fn pause_before_next_expiry(
        &self,
        processor: &str,
        pause: futures::future::BoxFuture<'static, ()>,
    ) {
        if let Some(runner) = self.inner.runners.get(processor) {
            runner.pause_before_next_expiry(pause);
        }
    }

    /// Answers one control request (exactly one reply, with its `control_id`).
    pub async fn handle(&self, request: ControlRequest) -> ControlReply {
        let ControlRequest {
            control_id,
            processor,
            action,
        } = request;
        let started = std::time::Instant::now();
        let name = action.as_ref().map(action_name).unwrap_or("none");
        let answer = self.answer(&control_id, &processor, action).await;
        tracing::info!(control_id = %control_id, processor = %processor, action = name, result = %describe(&answer), elapsed_ms = started.elapsed().as_millis() as u64, "processor control");
        ControlReply {
            control_id,
            result: Some(answer),
        }
    }

    async fn answer(&self, control_id: &str, processor: &str, action: Option<Action>) -> Answer {
        let Some(action) = action else {
            return refused(Refusal::new(
                ControlRefusal::Invalid,
                "the request names no action",
            ));
        };
        if let Action::ResetPrepare(p) = &action
            && p.lease_ms < MIN_RESET_LEASE_MS
        {
            return refused(Refusal::new(ControlRefusal::Invalid, BAD_PREPARE));
        }
        let runner = self.inner.runners.get(processor);
        // A renewal of the reset this instance holds: at once (ADR 0023), never behind an execute.
        // One whose lock is gone is refused, never turned into a prepare that takes the lock.
        if let (Action::ResetPrepare(p), Some(runner)) = (&action, runner)
            && let Some(renewed) = runner.renew(&p.operation_id, lease(p.lease_ms)).await
        {
            return ok_or_refused(renewed);
        }
        let Some(serial) = self.inner.serial.get(processor) else {
            return unknown(processor);
        };
        let free = serial.try_lock().ok();
        if free.is_none()
            && let Some(operation) = runner.and_then(|r| r.executing())
        {
            match action {
                Action::DeadLetterList(l) => return self.list(processor, l.limit).await,
                Action::DeadLetterRetry(_)
                | Action::DeadLetterRetryAll(_)
                | Action::DeadLetterDiscard(_) => {
                    return refused(Refusal::new(
                        ControlRefusal::Locked,
                        format!(
                            "processor '{processor}' is executing reset operation {operation}; try again when it finished"
                        ),
                    ));
                }
                _ => {}
            }
        }
        let _serial = match free {
            Some(serial) => serial,
            None => serial.lock().await,
        };
        match action {
            Action::Pause(_) | Action::Resume(_) => {
                let Some(runner) = runner else {
                    return unknown(processor);
                };
                let paused = matches!(action, Action::Pause(_));
                match runner.pause(paused, control_id).await {
                    Ok(()) => ok(if paused { "paused" } else { "resumed" }),
                    Err(e) => refused(Refusal::failed(&e)),
                }
            }
            Action::ResetPrepare(p) => match runner {
                Some(runner) => {
                    ok_or_refused(runner.prepare(&p.operation_id, lease(p.lease_ms)).await)
                }
                None => unknown(processor),
            },
            Action::ResetExecute(e) => {
                let Some(runner) = runner else {
                    return unknown(processor);
                };
                let Some(target) = ResetTarget::from_proto(e.target.as_ref()) else {
                    return refused(Refusal::new(
                        ControlRefusal::Invalid,
                        "the reset names no target",
                    ));
                };
                match runner.execute(&e.operation_id, target).await {
                    Ok(outcome) => Answer::Reset(outcome),
                    Err(r) => refused(r),
                }
            }
            Action::ResetFinish(f) => match runner {
                Some(runner) => ok_or_refused(runner.finish(&f.operation_id, f.aborted).await),
                None => unknown(processor),
            },
            Action::DeadLetterList(l) => self.list(processor, l.limit).await,
            Action::DeadLetterRetry(r) => self.retry(processor, &r.letter_id).await,
            Action::DeadLetterRetryAll(_) => self.retry_all(processor).await,
            Action::DeadLetterDiscard(d) => self.discard(processor, &d.letter_id).await,
        }
    }

    /// `INVALID` unless the letters are kept in the collection.
    fn collection_mode(&self) -> Result<(), Answer> {
        match self.inner.dead_letters.mode() {
            DeadLetterMode::Collection => Ok(()),
            mode => Err(refused(Refusal::new(
                ControlRefusal::Invalid,
                format!(
                    "dead letters are not kept in mode {} (KRAKEN_EVENTS_DEAD_LETTER_MODE)",
                    mode.as_str()
                ),
            ))),
        }
    }

    async fn list(&self, processor: &str, limit: u32) -> Answer {
        if let Err(answer) = self.collection_mode() {
            return answer;
        }
        if !(1..=MAX_LIST_LIMIT).contains(&limit) {
            return refused(Refusal::new(
                ControlRefusal::Invalid,
                format!("limit must be 1 to {MAX_LIST_LIMIT}, was {limit}"),
            ));
        }
        let letters = &self.inner.dead_letters;
        let page = async {
            let listed = letters.list(Some(processor), limit as usize).await?;
            let total = letters.count(Some(processor)).await?;
            Ok::<_, MessagingError>(proto::DeadLetterPage {
                letters: listed.iter().map(summary).collect(),
                total,
            })
        };
        match page.await {
            Ok(page) => Answer::DeadLetters(page),
            Err(e) => refused(Refusal::failed(&e)),
        }
    }

    /// The letter `id` when it is one of `processor`'s; `Ok(None)` when there is none.
    async fn letter_of(&self, processor: &str, id: &str) -> Result<Option<DeadLetter>, Answer> {
        self.collection_mode()?;
        if id.is_empty() {
            return Err(refused(Refusal::new(
                ControlRefusal::Invalid,
                "no letter id",
            )));
        }
        match self.inner.dead_letters.get(id).await {
            Ok(letter) => Ok(letter.filter(|l| l.processor == processor)),
            Err(e) => Err(refused(Refusal::failed(&e))),
        }
    }

    async fn retry(&self, processor: &str, id: &str) -> Answer {
        let letter = match self.letter_of(processor, id).await {
            Ok(letter) => letter,
            Err(answer) => return answer,
        };
        let result = match letter {
            Some(_) => match self.inner.dead_letters.retry(id).await {
                Ok(result) => result,
                Err(e) => return refused(Refusal::failed(&e)),
            },
            None => RetryResult::NotFound,
        };
        let (result, detail) = match result {
            RetryResult::Retried => ("Retried", String::new()),
            RetryResult::Duplicate => ("Duplicate", String::new()),
            RetryResult::Failed(error) => ("Failed", error),
            RetryResult::Blocked(head) => ("Blocked", head),
            RetryResult::NotRetriable => ("NotRetriable", String::new()),
            RetryResult::NotFound => ("NotFound", String::new()),
        };
        Answer::Retry(proto::DeadLetterRetryOutcome {
            result: result.to_string(),
            detail: cut_bytes(&detail, MAX_CONTROL_TEXT_BYTES),
        })
    }

    async fn retry_all(&self, processor: &str) -> Answer {
        if let Err(answer) = self.collection_mode() {
            return answer;
        }
        match self.inner.dead_letters.retry_all(processor).await {
            Ok(r) => Answer::RetryAll(proto::DeadLetterRetryAllOutcome {
                retried: r.retried,
                failed: r.failed,
                blocked: r.blocked,
                skipped: r.skipped,
            }),
            Err(e) => refused(Refusal::failed(&e)),
        }
    }

    async fn discard(&self, processor: &str, id: &str) -> Answer {
        let not_found = || {
            refused(Refusal::new(
                ControlRefusal::NotFound,
                format!("processor '{processor}' has no letter {id}"),
            ))
        };
        match self.letter_of(processor, id).await {
            Ok(Some(_)) => match self.inner.dead_letters.discard(id).await {
                Ok(true) => ok("discarded"),
                Ok(false) => not_found(),
                Err(e) => refused(Refusal::failed(&e)),
            },
            Ok(None) => not_found(),
            Err(answer) => answer,
        }
    }
}

/// A letter without its record: no payload, no headers.
fn summary(l: &DeadLetter) -> proto::DeadLetterSummary {
    proto::DeadLetterSummary {
        id: l.id.clone(),
        processor: l.processor.clone(),
        source: l.source.clone(),
        segment: l.segment.clone(),
        position: l.position.clone(),
        key: l.key.clone().unwrap_or_default(),
        event_id: l.event_id.clone().unwrap_or_default(),
        event_type: l.event_type.clone().unwrap_or_default(),
        reason: l.reason.as_str().to_string(),
        error_type: l.error.type_name.clone(),
        error_message: cut_bytes(&l.error.message, MAX_CONTROL_TEXT_BYTES),
        blocked_by: l.blocked_by.clone().unwrap_or_default(),
        attempts: u32::try_from(l.attempts).unwrap_or(0),
        parked_at_ms: l.parked_at_ms,
        last_attempt_at_ms: l.last_attempt_at_ms,
    }
}

fn lease(ms: i64) -> Duration {
    Duration::from_millis(u64::try_from(ms).unwrap_or(0))
}

fn ok(detail: &str) -> Answer {
    Answer::Ok(proto::ControlOk {
        detail: cut_bytes(detail, MAX_CONTROL_TEXT_BYTES),
    })
}

fn refused(r: Refusal) -> Answer {
    Answer::Refused(proto::ControlRefused {
        code: r.code as i32,
        message: cut_bytes(&r.message, MAX_CONTROL_TEXT_BYTES),
    })
}

fn ok_or_refused(result: Result<String, Refusal>) -> Answer {
    match result {
        Ok(detail) => ok(&detail),
        Err(r) => refused(r),
    }
}

fn unknown(processor: &str) -> Answer {
    refused(Refusal::new(
        ControlRefusal::UnknownProcessor,
        format!("processor '{processor}' does not run on this instance"),
    ))
}

fn action_name(action: &Action) -> &'static str {
    match action {
        Action::Pause(_) => "pause",
        Action::Resume(_) => "resume",
        Action::ResetPrepare(_) => "reset_prepare",
        Action::ResetExecute(_) => "reset_execute",
        Action::ResetFinish(_) => "reset_finish",
        Action::DeadLetterList(_) => "dead_letter_list",
        Action::DeadLetterRetry(_) => "dead_letter_retry",
        Action::DeadLetterRetryAll(_) => "dead_letter_retry_all",
        Action::DeadLetterDiscard(_) => "dead_letter_discard",
    }
}

/// The result for the log: never a payload.
fn describe(answer: &Answer) -> String {
    match answer {
        Answer::Ok(o) => format!("ok: {}", o.detail),
        Answer::Refused(r) => format!(
            "refused {}: {}",
            ControlRefusal::try_from(r.code)
                .map(|c| c.as_str_name())
                .unwrap_or("?"),
            r.message
        ),
        Answer::Reset(r) => format!(
            "reset: {} ranges, {} markers deleted, {} reset handlers",
            r.ranges.len(),
            r.markers_deleted,
            r.reset_handlers
        ),
        Answer::DeadLetters(p) => format!("{} of {} letters", p.letters.len(), p.total),
        Answer::Retry(r) => r.result.clone(),
        Answer::RetryAll(r) => format!(
            "retried {}, failed {}, blocked {}, skipped {}",
            r.retried, r.failed, r.blocked, r.skipped
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A control without runners and without a store.
    async fn control() -> ProcessorControl {
        ProcessorControl::new(
            Vec::new(),
            DeadLetters::new(None, DeadLetterMode::Collection, vec![]),
        )
    }

    fn prepare(lease_ms: i64) -> ControlRequest {
        ControlRequest {
            control_id: "c-1".into(),
            processor: "audit".into(),
            action: Some(Action::ResetPrepare(proto::ResetPrepare {
                operation_id: "op-1".into(),
                lease_ms,
            })),
        }
    }

    fn refusal(reply: ControlReply) -> (ControlRefusal, String) {
        match reply.result {
            Some(Answer::Refused(r)) => (ControlRefusal::try_from(r.code).unwrap(), r.message),
            other => panic!("not refused: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_prepare_or_renewal_with_a_lease_under_a_second_is_invalid() {
        let control = control().await;
        for lease_ms in [-1, 0, 1, 999] {
            let (code, message) = refusal(control.handle(prepare(lease_ms)).await);
            assert_eq!(code, ControlRefusal::Invalid, "lease {lease_ms} ms");
            assert!(message.contains("at least 1000 ms"), "{message}");
        }
        // A second is a lease: what comes next is about the processor, not the lease.
        let (code, _) = refusal(control.handle(prepare(1000)).await);
        assert_eq!(code, ControlRefusal::UnknownProcessor);
    }
}
