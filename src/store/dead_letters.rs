//! The dead letters (ADR 0022, mode `collection`): records that exhausted their attempts, parked
//! and sequenced per (processor, key). Every operation is a write of its own, outside any
//! transaction.

use std::collections::{BTreeMap, HashSet};

use futures::future::BoxFuture;

pub use crate::processors::dead_letters::{DeadLetterError, DeadLetterReason, letter_id};

use super::StoreError;

/// One parked record. `id` is `<processor>:<topic>:<partition>:<offset>` ([`letter_id`]), so
/// parking a redelivered record finds the first letter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetter {
    pub id: String,
    pub processor: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
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
    /// The record was parked while its processor replayed it (ADR 0023).
    pub replay: bool,
}

/// What [`DeadLetterStore::summaries`] reads of a letter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetterSummary {
    pub id: String,
    pub key: Option<String>,
    pub offset: i64,
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

    /// The id of the head letter of (`processor`, `key`): the lowest offset among its letters
    /// that are not INVALID (then the lowest id).
    fn head<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>>;

    /// The id of a letter of (`processor`, `key`) with an offset below `offset`, not INVALID (the
    /// lowest): what blocks a retry.
    fn earlier<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
        offset: i64,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>>;

    /// The keys with a letter (not INVALID) of `processor` on the given partitions of `topic`;
    /// none for no partitions.
    fn parked_keys<'a>(
        &'a self,
        processor: &'a str,
        topic: &'a str,
        partitions: &'a [i32],
    ) -> BoxFuture<'a, Result<HashSet<String>, StoreError>>;

    fn get<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<DeadLetter>, StoreError>>;

    /// Letters by `parked_at_ms`, then id; every processor's when `processor` is `None`; at most
    /// `limit` when given.
    fn list<'a>(
        &'a self,
        processor: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxFuture<'a, Result<Vec<DeadLetter>, StoreError>>;

    /// Every letter of `processor`, without its record (in no particular order).
    fn summaries<'a>(
        &'a self,
        processor: &'a str,
    ) -> BoxFuture<'a, Result<Vec<DeadLetterSummary>, StoreError>>;

    /// How many letters; every processor's when `processor` is `None`.
    fn count<'a>(&'a self, processor: Option<&'a str>) -> BoxFuture<'a, Result<u64, StoreError>>;

    /// Deletes the letter; false when there was none.
    fn delete<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// A retry failed again: `attempts + 1`, `error`, `last_attempt_at_ms = now_ms`, reason
    /// HANDLER_FAILED, no `blocked_by` any more. False when the letter is gone.
    fn record_failure<'a>(
        &'a self,
        id: &'a str,
        error: &'a DeadLetterError,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;
}
