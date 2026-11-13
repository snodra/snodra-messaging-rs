//! Parking a record (ADR 0022, mode `collection`): the letter a record the consumer gave up on
//! becomes, kept by the message store's [`DeadLetterStore`](crate::store::DeadLetterStore).
//!
//! A letter's id is `<processor>:<topic>:<partition>:<offset>` ([`letter_id`]), so parking the
//! same record twice (a redelivery) finds the first letter.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::event_store::headers;
use crate::store::{DeadLetter, DeadLetterError, DeadLetterReason, letter_id};

/// The record a letter is made of, as the consumer read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetterRecord {
    pub processor: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    /// Every header in record order (a repeated name keeps its last value in the letter).
    pub headers: Vec<(String, Vec<u8>)>,
    pub payload: Vec<u8>,
}

impl DeadLetterRecord {
    /// `<processor>:<topic>:<partition>:<offset>`.
    pub fn letter_id(&self) -> String {
        letter_id(&self.processor, &self.topic, self.partition, self.offset)
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
    let headers: BTreeMap<String, String> = record
        .headers
        .into_iter()
        .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
        .collect();
    // A blank `id` or `type` is no id or type (as the consumer reads it): absent, not "".
    let header = |name: &str| headers.get(name).filter(|v| !v.trim().is_empty()).cloned();
    let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
    DeadLetter {
        id,
        event_id: header(headers::ID),
        event_type: header(headers::TYPE),
        processor: record.processor,
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        key: record.key.map(|k| String::from_utf8_lossy(&k).into_owned()),
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
