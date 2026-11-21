//! Parking a record (mode `collection`): the letter a record the processor gave up on
//! becomes, kept by the message store's [`DeadLetterStore`](crate::store::DeadLetterStore).
//!
//! A letter's id is `<processor>:<source>:<segment>:<position>` ([`letter_id`]), so parking the
//! same record twice (a redelivery) finds the first letter. On Kafka that is
//! `<processor>:<topic>:<partition>:<offset>`, as it always was.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::event_store::headers;
use crate::store::{DeadLetter, DeadLetterError, DeadLetterReason, letter_id};

/// The record a letter is made of, as the processor read it from the event store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetterRecord {
    pub processor: String,
    /// The source it was read from.
    pub source: String,
    /// Its segment and position, the event store's opaque strings.
    pub segment: String,
    pub position: String,
    pub key: Option<String>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
}

impl DeadLetterRecord {
    /// `<processor>:<source>:<segment>:<position>`.
    pub fn letter_id(&self) -> String {
        letter_id(&self.processor, &self.source, &self.segment, &self.position)
    }
}

/// The letter for `record`, parked at `now_ms` (epoch milliseconds) and kept for `retention`.
pub fn park(
    record: DeadLetterRecord,
    reason: DeadLetterReason,
    error: DeadLetterError,
    blocked_by: Option<String>,
    attempts: u32,
    now_ms: i64,
    retention: Duration,
) -> DeadLetter {
    let id = record.letter_id();
    let headers: BTreeMap<String, String> = record.headers.into_iter().collect();
    // A blank `id` or `type` is no id or type (as the processor reads it): absent, not "".
    let header = |name: &str| headers.get(name).filter(|v| !v.trim().is_empty()).cloned();
    let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
    DeadLetter {
        id,
        event_id: header(headers::ID),
        event_type: header(headers::TYPE),
        processor: record.processor,
        source: record.source,
        segment: record.segment,
        position: record.position,
        key: record.key,
        headers,
        payload: record.payload,
        reason,
        error,
        blocked_by,
        attempts: i32::try_from(attempts).unwrap_or(i32::MAX),
        parked_at_ms: now_ms,
        last_attempt_at_ms: now_ms,
        expire_at_ms: now_ms.saturating_add(retention_ms),
        replay: false,
    }
}
