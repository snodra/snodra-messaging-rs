//! The dead letters (mode `collection`): records that exhausted their attempts, parked
//! and sequenced per (processor, key). Every operation is a write of its own, outside any
//! transaction, but a retry's [`delete_in`](DeadLetterStore::delete_in).

use std::collections::{BTreeMap, HashSet};

use futures::future::BoxFuture;

use super::{StoreError, Tx};
use crate::error::MessagingError;

/// The most characters of a letter's `error.message` (and of `snodra-dlt-error`).
pub const MAX_ERROR_MESSAGE_CHARS: usize = 2000;

/// Why a record was parked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeadLetterReason {
    /// Stage 1 failed on every attempt.
    HandlerFailed,
    /// An earlier letter of the record's key was parked; stage 1 did not run.
    Sequenced,
    /// The record cannot be handled (no type header, a value that is not a JSON object); it
    /// blocks nothing and can only be discarded.
    Invalid,
}

impl DeadLetterReason {
    /// The stored name: `HANDLER_FAILED`, `SEQUENCED`, `INVALID`.
    pub fn as_str(self) -> &'static str {
        match self {
            DeadLetterReason::HandlerFailed => "HANDLER_FAILED",
            DeadLetterReason::Sequenced => "SEQUENCED",
            DeadLetterReason::Invalid => "INVALID",
        }
    }

    /// The reason of a stored name; `None` for any other text.
    pub fn parse(name: &str) -> Option<DeadLetterReason> {
        [
            DeadLetterReason::HandlerFailed,
            DeadLetterReason::Sequenced,
            DeadLetterReason::Invalid,
        ]
        .into_iter()
        .find(|r| r.as_str() == name)
    }
}

/// A letter's `error`: `type_name` is the error variant ([`MessagingError::variant_name`]),
/// Kotlin's exception simple name; `message` at most [`MAX_ERROR_MESSAGE_CHARS`] characters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetterError {
    pub type_name: String,
    pub message: String,
}

impl DeadLetterError {
    pub fn new(type_name: &str, message: &str) -> DeadLetterError {
        DeadLetterError {
            type_name: type_name.to_string(),
            message: cut_chars(message, MAX_ERROR_MESSAGE_CHARS),
        }
    }

    /// A handler's failure.
    pub fn from_error(e: &MessagingError) -> DeadLetterError {
        DeadLetterError::new(e.variant_name(), &e.to_string())
    }

    /// A SEQUENCED letter's error, naming the key's head letter.
    pub fn sequenced(head_id: &str) -> DeadLetterError {
        DeadLetterError::new(
            "Sequenced",
            &format!("an earlier letter for this key is parked: {head_id}"),
        )
    }

    /// An INVALID letter's error.
    pub fn invalid(message: &str) -> DeadLetterError {
        DeadLetterError::new("InvalidMessage", message)
    }
}

/// `text` cut to at most `max` characters (never inside one).
pub(crate) fn cut_chars(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((end, _)) => text[..end].to_string(),
        None => text.to_string(),
    }
}

/// A letter's id: `<processor>:<source>:<segment>:<position>` (on Kafka
/// `<processor>:<topic>:<partition>:<offset>`, as always).
pub fn letter_id(processor: &str, source: &str, segment: &str, position: &str) -> String {
    format!("{processor}:{source}:{segment}:{position}")
}

/// One parked record. `id` is `<processor>:<source>:<segment>:<position>` ([`letter_id`]), so
/// parking a redelivered record finds the first letter. `segment` and `position` are the event
/// store's opaque strings (decimal numbers for the first-wave stores).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetter {
    pub id: String,
    pub processor: String,
    /// The source the record was read from.
    pub source: String,
    pub segment: String,
    pub position: String,
    /// The record's key as lossy UTF-8; `None` when it had none.
    pub key: Option<String>,
    /// The `id` header; `None` when the record had none or a blank one.
    pub event_id: Option<String>,
    /// The `type` header; `None` when the record had none or a blank one.
    pub event_type: Option<String>,
    /// Every record header, lossy UTF-8.
    pub headers: BTreeMap<String, String>,
    /// The record's value, byte for byte (JSON, or anything an INVALID record carried).
    pub payload: Vec<u8>,
    pub reason: DeadLetterReason,
    pub error: DeadLetterError,
    /// SEQUENCED only: the key's head letter when this one was parked (display only).
    pub blocked_by: Option<String>,
    pub attempts: i32,
    /// Epoch milliseconds, as every `_ms` field.
    pub parked_at_ms: i64,
    pub last_attempt_at_ms: i64,
    /// `parked_at_ms` + retention: the store removes the letter then (which unblocks its key).
    pub expire_at_ms: i64,
    /// The record was parked while its processor replayed it.
    pub replay: bool,
}

impl DeadLetter {
    /// The id the record's stage 1 is deduplicated by: its `id` header, else (none or blank)
    /// `<source>-<segment>-<position>` (the consumer's fallback).
    pub fn dedupe_id(&self) -> String {
        self.event_id
            .clone()
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| format!("{}-{}-{}", self.source, self.segment, self.position))
    }
}

/// What [`DeadLetterStore::summaries`] reads of a letter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetterSummary {
    pub id: String,
    pub key: Option<String>,
    pub position: String,
    pub reason: DeadLetterReason,
    pub parked_at_ms: i64,
}

/// Whether [`DeadLetterStore::park`] inserted the letter or found it parked already.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parked {
    New,
    Already,
}

pub trait DeadLetterStore: Send + Sync {
    /// Inserts `letter`; [`Parked::Already`] when a letter of that id exists (a redelivery).
    fn park<'a>(&'a self, letter: &'a DeadLetter) -> BoxFuture<'a, Result<Parked, StoreError>>;

    /// The id of the head letter of (`processor`, `key`): the earliest position among its
    /// letters that are not INVALID (then the lowest id). The letters of one key come from one
    /// segment; positions are ordered as the store orders them, decimal ones (every first-wave
    /// event store's) by value.
    fn head<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>>;

    /// The id of a letter of (`processor`, `key`) at a position before `position`, not INVALID
    /// (the earliest): what blocks a retry.
    fn earlier<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
        position: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>>;

    /// The keys with a letter (not INVALID) of `processor` on the given segments of `source`;
    /// none for no segments.
    fn parked_keys<'a>(
        &'a self,
        processor: &'a str,
        source: &'a str,
        segments: &'a [String],
    ) -> BoxFuture<'a, Result<HashSet<String>, StoreError>>;

    fn get<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<DeadLetter>, StoreError>>;

    /// Letters by `parked_at_ms`, then id; every processor's when `processor` is `None`; at most
    /// `limit` when given.
    fn list<'a>(
        &'a self,
        processor: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxFuture<'a, Result<Vec<DeadLetter>, StoreError>>;

    /// Every letter of `processor`, without its record, by position as [`head`](Self::head)
    /// orders them, then id.
    fn summaries<'a>(
        &'a self,
        processor: &'a str,
    ) -> BoxFuture<'a, Result<Vec<DeadLetterSummary>, StoreError>>;

    /// How many letters; every processor's when `processor` is `None`.
    fn count<'a>(&'a self, processor: Option<&'a str>) -> BoxFuture<'a, Result<u64, StoreError>>;

    /// Deletes the letter; false when there was none.
    fn delete<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Deletes the letter in `tx`: a retry's, which commits with the record's marker and its
    /// handlers' effects or not at all. False when there was none.
    fn delete_in<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// A retry failed again: `attempts + 1`, `error`, `last_attempt_at_ms = now_ms`, reason
    /// HANDLER_FAILED, no `blocked_by` any more. False when the letter is gone.
    fn record_failure<'a>(
        &'a self,
        id: &'a str,
        error: &'a DeadLetterError,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;
}
