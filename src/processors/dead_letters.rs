//! The `dead_letters` collection (ADR 0022, mode `collection`): the document, field for field the
//! contract both libraries write, and its indexes.
//!
//! A letter's `_id` is `<processor>:<topic>:<partition>:<offset>`, so parking the same record
//! twice (a redelivery) finds the first letter (a duplicate key means "already parked"). Dates are
//! BSON datetimes, `partition` an Int32, `offset` an Int64; an absent field is absent (never
//! null) and no `_class` is written.

use std::collections::BTreeMap;
use std::time::Duration;

use mongodb::bson::{Binary, DateTime, Document, doc, spec::BinarySubtype};
use mongodb::options::IndexOptions;
use mongodb::{Database, IndexModel};
use serde::{Deserialize, Serialize};

use crate::error::MessagingError;
use crate::kafka::headers;

pub const DEAD_LETTERS_COLLECTION: &str = "dead_letters";
/// The most characters of a letter's `error.message` (and of `kraken-dlt-error`).
pub const MAX_ERROR_MESSAGE_CHARS: usize = 2000;

/// Why a record was parked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
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
    pub fn as_str(self) -> &'static str {
        match self {
            DeadLetterReason::HandlerFailed => "HANDLER_FAILED",
            DeadLetterReason::Sequenced => "SEQUENCED",
            DeadLetterReason::Invalid => "INVALID",
        }
    }
}

/// A letter's `error`: `type` is the error variant ([`MessagingError::variant_name`]), Kotlin's
/// exception simple name; `message` at most 2000 characters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetterError {
    #[serde(rename = "type")]
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

/// A letter's `_id`.
pub fn letter_id(processor: &str, topic: &str, partition: i32, offset: i64) -> String {
    format!("{processor}:{topic}:{partition}:{offset}")
}

/// One parked record (`dead_letters`); see the module docs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadLetterDocument {
    #[serde(rename = "_id")]
    pub id: String,
    pub processor: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// The record's key as lossy UTF-8; absent when the record had none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// The `id` header; absent when the record had none or a blank one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// The `type` header; absent when the record had none or a blank one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    /// Every record header, lossy UTF-8.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// The value as text when it is valid UTF-8 (absent when empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    /// The value when it is not valid UTF-8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_binary: Option<Binary>,
    pub reason: DeadLetterReason,
    pub error: DeadLetterError,
    /// SEQUENCED only: the key's head letter when this one was parked (display only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_by: Option<String>,
    pub attempts: i32,
    pub parked_at: DateTime,
    pub last_attempt_at: DateTime,
    /// `parked_at` + retention; the TTL index removes the letter then.
    pub expire_at: DateTime,
    /// The record was parked while its processor replayed it (ADR 0023): its retry is a replay
    /// too (no handler that disallows replay). Written only when true; absent is false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub replay: bool,
}

impl DeadLetterDocument {
    /// The letter for `record`, parked `now` and kept for `retention`.
    pub fn park(
        record: DeadLetterRecord,
        reason: DeadLetterReason,
        error: DeadLetterError,
        blocked_by: Option<String>,
        attempts: u32,
        now: DateTime,
        retention: Duration,
    ) -> DeadLetterDocument {
        let id = record.letter_id();
        let headers: BTreeMap<String, String> = record
            .headers
            .into_iter()
            .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
            .collect();
        // A blank `id` or `type` is no id or type (as the consumer reads it): absent, not "".
        let header = |name: &str| headers.get(name).filter(|v| !v.trim().is_empty()).cloned();
        let (payload, payload_binary) = if record.payload.is_empty() {
            (None, None)
        } else {
            match String::from_utf8(record.payload) {
                Ok(text) => (Some(text), None),
                Err(e) => (
                    None,
                    Some(Binary {
                        subtype: BinarySubtype::Generic,
                        bytes: e.into_bytes(),
                    }),
                ),
            }
        };
        let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
        DeadLetterDocument {
            id,
            event_id: header(headers::ID),
            event_type: header(headers::TYPE),
            processor: record.processor,
            topic: record.topic,
            partition: record.partition,
            offset: record.offset,
            key: record.key.map(|k| String::from_utf8_lossy(&k).into_owned()),
            headers,
            payload,
            payload_binary,
            reason,
            error,
            blocked_by,
            attempts: i32::try_from(attempts).unwrap_or(i32::MAX),
            parked_at: now,
            last_attempt_at: now,
            expire_at: DateTime::from_millis(now.timestamp_millis().saturating_add(retention_ms)),
            replay: false,
        }
    }

    /// The record's value: `payload`'s bytes, else `payloadBinary`'s, else empty.
    pub fn payload_bytes(&self) -> Vec<u8> {
        match (&self.payload, &self.payload_binary) {
            (Some(text), _) => text.as_bytes().to_vec(),
            (None, Some(binary)) => binary.bytes.clone(),
            (None, None) => Vec::new(),
        }
    }

    /// The id the record's stage 1 is deduplicated by: its `id` header, else (none or blank)
    /// `<topic>-<partition>-<offset>` (the consumer's fallback).
    pub fn dedupe_id(&self) -> String {
        self.event_id
            .clone()
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| format!("{}-{}-{}", self.topic, self.partition, self.offset))
    }
}

/// `text` cut to at most `max` characters (never inside one).
pub(crate) fn cut_chars(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((end, _)) => text[..end].to_string(),
        None => text.to_string(),
    }
}

/// The contract's three indexes: `dead_letters_processor_key_offset` (`processor`, `key`,
/// `offset`: sequencing and retry order), `dead_letters_processor_parked` (`processor`,
/// `parkedAt`: listing) and `dead_letters_expire` (TTL on `expireAt`, `expireAfterSeconds: 0`:
/// an expired letter disappears and unblocks its key).
pub async fn ensure_dead_letter_indexes(db: &Database) -> Result<(), MessagingError> {
    let letters = db.collection::<Document>(DEAD_LETTERS_COLLECTION);
    let named = |keys: Document, name: &str| {
        IndexModel::builder()
            .keys(keys)
            .options(IndexOptions::builder().name(name.to_string()).build())
            .build()
    };
    letters
        .create_index(named(
            doc! { "processor": 1, "key": 1, "offset": 1 },
            "dead_letters_processor_key_offset",
        ))
        .await?;
    letters
        .create_index(named(
            doc! { "processor": 1, "parkedAt": 1 },
            "dead_letters_processor_parked",
        ))
        .await?;
    letters
        .create_index(
            IndexModel::builder()
                .keys(doc! { "expireAt": 1 })
                .options(
                    IndexOptions::builder()
                        .name("dead_letters_expire".to_string())
                        .expire_after(Duration::ZERO)
                        .build(),
                )
                .build(),
        )
        .await?;
    Ok(())
}
