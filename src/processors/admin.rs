//! The dead-letter admin API: [`DeadLetters`], from
//! [`SnodraMessaging::dead_letters`](crate::SnodraMessaging::dead_letters), on any instance of the
//! service that runs the letter's processor.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::config::DeadLetterMode;
use crate::error::MessagingError;
use crate::events::dispatch::{FinishInTransaction, MarkerPosition, RecordDispatch, StageOneThen};
use crate::events::{LocalEventDispatcher, message_context};
use crate::store::{
    DeadLetter, DeadLetterError, DeadLetterReason, DeadLetterStore, DeadLetterSummary,
};

/// The default `limit` of [`DeadLetters::list`] (Kotlin's `limit: Int = 100`).
pub const DEFAULT_LIST_LIMIT: usize = 100;

/// What one [`DeadLetters::retry`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryResult {
    /// The processor's stage 1 ran and committed with its marker and the letter's deletion.
    Retried,
    /// The marker already existed (the record was handled meanwhile): the letter was deleted.
    Duplicate,
    /// Stage 1 failed again: the letter was updated (`attempts + 1`, `error`, `lastAttemptAt`,
    /// reason HANDLER_FAILED). The error's message.
    Failed(String),
    /// An earlier letter of its key is parked (its `_id`): retry or discard that one first.
    Blocked(String),
    /// An INVALID letter: it can only be discarded.
    NotRetriable,
    /// No such letter (or it was removed while the retry ran).
    NotFound,
}

/// What one [`DeadLetters::retry_all`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetryAllResult {
    /// Retried or found to be duplicates (deleted either way).
    pub retried: u64,
    /// Failed again (updated, kept).
    pub failed: u64,
    /// Behind a failure of their key, or behind a head that is not in this run.
    pub blocked: u64,
    /// INVALID letters, and letters removed while the run went on.
    pub skipped: u64,
}

/// The admin API over `dead_letters`; see the module docs. Cheap to clone.
#[derive(Clone)]
pub struct DeadLetters {
    inner: Arc<Inner>,
}

struct Inner {
    mode: DeadLetterMode,
    store: Option<Arc<dyn DeadLetterStore>>,
    processors: BTreeMap<String, Arc<LocalEventDispatcher>>,
}

impl DeadLetters {
    /// The API over the letters in `store` (none: a service without a message store) for the processors this instance runs (name and its
    /// dispatcher, whose stage 1 a retry runs). In modes `event-store` and `log` every call fails.
    pub fn new(
        store: Option<Arc<dyn DeadLetterStore>>,
        mode: DeadLetterMode,
        processors: Vec<(String, Arc<LocalEventDispatcher>)>,
    ) -> DeadLetters {
        DeadLetters {
            inner: Arc::new(Inner {
                mode,
                store,
                processors: processors.into_iter().collect(),
            }),
        }
    }

    /// The dead-letter mode of the service.
    pub fn mode(&self) -> DeadLetterMode {
        self.inner.mode
    }

    /// The processors whose letters this API serves (those with handlers here), by name.
    pub fn processor_names(&self) -> Vec<String> {
        self.inner.processors.keys().cloned().collect()
    }

    /// Up to `limit` letters ([`DEFAULT_LIST_LIMIT`] is the usual), sorted by `parkedAt`, then
    /// `_id`: of `processor`, or of every processor of the service with `None`. A `limit` of 0 is
    /// an error (as Kotlin's `require(limit >= 1)`), never "no limit".
    pub async fn list(
        &self,
        processor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DeadLetter>, MessagingError> {
        self.collection_mode()?;
        if limit < 1 {
            return Err(MessagingError::InvalidMessage(format!(
                "limit must be >= 1, was {limit}"
            )));
        }
        if let Some(p) = processor {
            self.dispatcher(p)?;
        }
        Ok(self.store()?.list(processor, Some(limit)).await?)
    }

    /// The letter `id`, if it exists.
    pub async fn get(&self, id: &str) -> Result<Option<DeadLetter>, MessagingError> {
        self.collection_mode()?;
        let letter = self.store()?.get(id).await?;
        if let Some(l) = &letter {
            self.dispatcher(&l.processor)?;
        }
        Ok(letter)
    }

    /// The letters of `processor`, or of every processor with `None`.
    pub async fn count(&self, processor: Option<&str>) -> Result<u64, MessagingError> {
        self.collection_mode()?;
        if let Some(p) = processor {
            self.dispatcher(p)?;
        }
        Ok(self.store()?.count(processor).await?)
    }

    /// Retries the letter `id` once (no back-off): its processor's stage 1 (every handler of its
    /// type) in the marker transaction, the letter's deletion in the same transaction. Stage 2
    /// (the sagas) is not re-run: it saw the record when it was parked.
    pub async fn retry(&self, id: &str) -> Result<RetryResult, MessagingError> {
        self.collection_mode()?;
        let Some(letter) = self.store()?.get(id).await? else {
            return Ok(RetryResult::NotFound);
        };
        self.retry_letter(&letter).await
    }

    async fn retry_letter(&self, letter: &DeadLetter) -> Result<RetryResult, MessagingError> {
        let dispatcher = self.dispatcher(&letter.processor)?;
        let Some(type_name) = letter
            .event_type
            .clone()
            .filter(|_| letter.reason != DeadLetterReason::Invalid)
        else {
            return Ok(RetryResult::NotRetriable);
        };
        if let Some(key) = &letter.key
            && let Some(head) = self
                .store()?
                .earlier(&letter.processor, key, &letter.position)
                .await?
        {
            return Ok(RetryResult::Blocked(head));
        }
        if !dispatcher.has_stage_one(&type_name) {
            let message = format!(
                "no handler for {type_name} in processor {}",
                letter.processor
            );
            return self
                .failed(
                    &letter.id,
                    DeadLetterError::new("NoHandler", &message),
                    message,
                )
                .await;
        }
        let dedupe_id = letter.dedupe_id();
        let headers: HashMap<String, String> = letter
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let ctx = message_context(&type_name, &dedupe_id, &headers);
        let letters = Arc::clone(self.store()?);
        let letter_id = letter.id.clone();
        let finish: FinishInTransaction<'_> = Box::new(move |tx| {
            Box::pin(async move { Ok(letters.delete_in(tx, &letter_id).await?) })
        });
        let payload = letter.payload.clone();
        // The retry is the record's stage 1: its marker carries the record's position.
        let position = MarkerPosition {
            processor: letter.processor.clone(),
            source: letter.source.clone(),
            segment: letter.segment.clone(),
            position: letter.position.clone(),
        };
        match dispatcher
            .stage_one_then(
                &dedupe_id,
                &type_name,
                &payload,
                &ctx,
                RecordDispatch {
                    position: Some(&position),
                    stage_one: true,
                    replay: letter.replay,
                },
                finish,
            )
            .await
        {
            Ok(StageOneThen::Committed) => Ok(RetryResult::Retried),
            Ok(StageOneThen::Declined) => Ok(RetryResult::NotFound),
            Ok(StageOneThen::Duplicate) => {
                self.store()?.delete(&letter.id).await?;
                Ok(RetryResult::Duplicate)
            }
            // Also a write conflict with another instance's removal of the letter, seen as an
            // error: the failure update then matches nothing (NotFound).
            Err(e) => {
                self.failed(&letter.id, DeadLetterError::from_error(&e), e.to_string())
                    .await
            }
        }
    }

    /// Records a failed retry on the letter: `Failed(message)`, or `NotFound` when the letter was
    /// removed meanwhile (the update matched nothing), as Kotlin answers.
    async fn failed(
        &self,
        id: &str,
        error: DeadLetterError,
        message: String,
    ) -> Result<RetryResult, MessagingError> {
        if !self
            .store()?
            .record_failure(id, &error, crate::time::now_ms())
            .await?
        {
            return Ok(RetryResult::NotFound);
        }
        tracing::warn!(letter = %id, "dead letter retry failed: {}: {}", error.type_name, error.message);
        Ok(RetryResult::Failed(message))
    }

    /// Retries every letter of `processor`: per key in position order (the keys in the order of their
    /// head letters' `parkedAt`, then `_id`), a key stopping at its first failure (its later
    /// letters count as blocked); then the keyless letters in `parkedAt` order; INVALID letters
    /// skipped. The run is ordered from the letters' summaries (no record is loaded); each letter
    /// is read just before its retry, and one gone by then is skipped.
    pub async fn retry_all(&self, processor: &str) -> Result<RetryAllResult, MessagingError> {
        self.collection_mode()?;
        self.dispatcher(processor)?;
        let mut result = RetryAllResult::default();
        let mut keyed: BTreeMap<String, Vec<DeadLetterSummary>> = BTreeMap::new();
        let mut keyless = Vec::new();
        for letter in self.store()?.summaries(processor).await? {
            if letter.reason == DeadLetterReason::Invalid {
                result.skipped += 1;
                continue;
            }
            match &letter.key {
                Some(key) => keyed.entry(key.clone()).or_default().push(letter),
                None => keyless.push(letter),
            }
        }
        // Each key's letters in the store's order (position, then id); the keys in the order of
        // their head letter by (`parkedAt`, `_id`).
        let mut keyed: Vec<Vec<DeadLetterSummary>> = keyed.into_values().collect();
        keyed.sort_by(|a, b| (a[0].parked_at_ms, &a[0].id).cmp(&(b[0].parked_at_ms, &b[0].id)));
        keyless.sort_by(|a, b| (a.parked_at_ms, &a.id).cmp(&(b.parked_at_ms, &b.id)));
        for letters in keyed {
            let total = letters.len() as u64;
            for (done, letter) in letters.iter().enumerate() {
                let rest = total - done as u64 - 1;
                match self.retry_summary(letter).await? {
                    RetryResult::Retried | RetryResult::Duplicate => result.retried += 1,
                    RetryResult::NotFound | RetryResult::NotRetriable => result.skipped += 1,
                    RetryResult::Failed(_) => {
                        result.failed += 1;
                        result.blocked += rest;
                        break;
                    }
                    RetryResult::Blocked(_) => {
                        result.blocked += rest + 1;
                        break;
                    }
                }
            }
        }
        for letter in &keyless {
            match self.retry_summary(letter).await? {
                RetryResult::Retried | RetryResult::Duplicate => result.retried += 1,
                RetryResult::NotFound | RetryResult::NotRetriable => result.skipped += 1,
                RetryResult::Failed(_) => result.failed += 1,
                RetryResult::Blocked(_) => result.blocked += 1,
            }
        }
        Ok(result)
    }

    /// The letter of `summary` as it is now, retried; `NotFound` when it is gone.
    async fn retry_summary(
        &self,
        summary: &DeadLetterSummary,
    ) -> Result<RetryResult, MessagingError> {
        match self.store()?.get(&summary.id).await? {
            Some(letter) => self.retry_letter(&letter).await,
            None => Ok(RetryResult::NotFound),
        }
    }

    /// Deletes the letter `id` (false when there is none); discarding a key's head unblocks its
    /// next letter.
    pub async fn discard(&self, id: &str) -> Result<bool, MessagingError> {
        self.collection_mode()?;
        let Some(letter) = self.store()?.get(id).await? else {
            return Ok(false);
        };
        self.dispatcher(&letter.processor)?;
        Ok(self.store()?.delete(id).await?)
    }

    fn store(&self) -> Result<&Arc<dyn DeadLetterStore>, MessagingError> {
        self.inner.store.as_ref().ok_or_else(|| {
            MessagingError::InvalidMessage(
                "the dead letters need a message store: add .store(...)".into(),
            )
        })
    }

    fn collection_mode(&self) -> Result<(), MessagingError> {
        match self.inner.mode {
            DeadLetterMode::Collection => Ok(()),
            mode => Err(MessagingError::InvalidMessage(format!(
                "dead letters are not kept in mode {} (SNODRA_EVENTS_DEAD_LETTER_MODE): there is no dead-letter admin API",
                mode.as_str()
            ))),
        }
    }

    fn dispatcher(&self, processor: &str) -> Result<&Arc<LocalEventDispatcher>, MessagingError> {
        self.inner.processors.get(processor).ok_or_else(|| {
            MessagingError::InvalidMessage(format!(
                "processor '{processor}' does not run on this instance"
            ))
        })
    }
}
