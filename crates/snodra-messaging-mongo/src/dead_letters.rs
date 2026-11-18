//! The `dead_letters` collection (ADR 0022, mode `collection`): the document, field for field the
//! contract both libraries write. A letter's `_id` is `<processor>:<source>:<segment>:<position>`
//! (`<processor>:<topic>:<partition>:<offset>` on Kafka), so parking the same record twice finds
//! the first letter (a duplicate key means "already parked"). Dates are BSON datetimes; `topic` is
//! the source, `partition` the segment and `offset` the position, as
//! [`positions`](crate::positions) stores them (an Int32 and an Int64 for decimal ones, as
//! always); an absent field is absent (never null) and no `_class` is written. The value is `payload` (text) when it is valid
//! UTF-8, `payloadBinary` when it is not, neither when it is empty.

use std::collections::{BTreeMap, HashSet};

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{Binary, Bson, DateTime, Document, doc, spec::BinarySubtype};
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};
use snodra_messaging::store::{
    DeadLetter, DeadLetterError, DeadLetterReason, DeadLetterStore, DeadLetterSummary, Parked,
    StoreError, Tx,
};

use crate::{OrStoreError, date, is_duplicate_key, positions, session, store_error};

pub(crate) const DEAD_LETTERS_COLLECTION: &str = "dead_letters";

/// A letter's `reason`, by its Kotlin name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ReasonDocument {
    HandlerFailed,
    Sequenced,
    Invalid,
}

impl From<DeadLetterReason> for ReasonDocument {
    fn from(r: DeadLetterReason) -> Self {
        match r {
            DeadLetterReason::HandlerFailed => ReasonDocument::HandlerFailed,
            DeadLetterReason::Sequenced => ReasonDocument::Sequenced,
            DeadLetterReason::Invalid => ReasonDocument::Invalid,
        }
    }
}

impl From<ReasonDocument> for DeadLetterReason {
    fn from(r: ReasonDocument) -> Self {
        match r {
            ReasonDocument::HandlerFailed => DeadLetterReason::HandlerFailed,
            ReasonDocument::Sequenced => DeadLetterReason::Sequenced,
            ReasonDocument::Invalid => DeadLetterReason::Invalid,
        }
    }
}

/// A letter's `error`: `{type, message}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ErrorDocument {
    #[serde(rename = "type")]
    type_name: String,
    message: String,
}

impl From<&DeadLetterError> for ErrorDocument {
    fn from(e: &DeadLetterError) -> Self {
        ErrorDocument {
            type_name: e.type_name.clone(),
            message: e.message.clone(),
        }
    }
}

impl From<ErrorDocument> for DeadLetterError {
    fn from(e: ErrorDocument) -> Self {
        DeadLetterError {
            type_name: e.type_name,
            message: e.message,
        }
    }
}

/// One parked record (`dead_letters`); see the module docs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadLetterDocument {
    #[serde(rename = "_id")]
    id: String,
    processor: String,
    topic: String,
    partition: Bson,
    offset: Bson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_type: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload_binary: Option<Binary>,
    reason: ReasonDocument,
    error: ErrorDocument,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    blocked_by: Option<String>,
    attempts: i32,
    parked_at: DateTime,
    last_attempt_at: DateTime,
    /// The TTL index removes the letter then.
    expire_at: DateTime,
    /// Written only when true; absent is false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    replay: bool,
}

impl DeadLetterDocument {
    fn from_letter(letter: &DeadLetter) -> DeadLetterDocument {
        let (payload, payload_binary) = if letter.payload.is_empty() {
            (None, None)
        } else {
            match String::from_utf8(letter.payload.clone()) {
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
        DeadLetterDocument {
            id: letter.id.clone(),
            processor: letter.processor.clone(),
            topic: letter.source.clone(),
            partition: positions::segment(&letter.segment),
            offset: positions::position(&letter.position),
            key: letter.key.clone(),
            event_id: letter.event_id.clone(),
            event_type: letter.event_type.clone(),
            headers: letter.headers.clone(),
            payload,
            payload_binary,
            reason: letter.reason.into(),
            error: (&letter.error).into(),
            blocked_by: letter.blocked_by.clone(),
            attempts: letter.attempts,
            parked_at: date(letter.parked_at_ms),
            last_attempt_at: date(letter.last_attempt_at_ms),
            expire_at: date(letter.expire_at_ms),
            replay: letter.replay,
        }
    }

    fn into_letter(self) -> DeadLetter {
        let payload = match (self.payload, self.payload_binary) {
            (Some(text), _) => text.into_bytes(),
            (None, Some(binary)) => binary.bytes,
            (None, None) => Vec::new(),
        };
        DeadLetter {
            id: self.id,
            processor: self.processor,
            source: self.topic,
            segment: positions::text(&self.partition).unwrap_or_default(),
            position: positions::text(&self.offset).unwrap_or_default(),
            key: self.key,
            event_id: self.event_id,
            event_type: self.event_type,
            headers: self.headers,
            payload,
            reason: self.reason.into(),
            error: self.error.into(),
            blocked_by: self.blocked_by,
            attempts: self.attempts,
            parked_at_ms: self.parked_at.timestamp_millis(),
            last_attempt_at_ms: self.last_attempt_at.timestamp_millis(),
            expire_at_ms: self.expire_at.timestamp_millis(),
            replay: self.replay,
        }
    }
}

impl From<&DeadLetter> for DeadLetterDocument {
    fn from(letter: &DeadLetter) -> Self {
        DeadLetterDocument::from_letter(letter)
    }
}

impl From<DeadLetterDocument> for DeadLetter {
    fn from(doc: DeadLetterDocument) -> Self {
        doc.into_letter()
    }
}

/// What [`DeadLetterStore::summaries`] projects of a letter.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SummaryDocument {
    #[serde(rename = "_id")]
    id: String,
    #[serde(default)]
    key: Option<String>,
    offset: Bson,
    reason: ReasonDocument,
    parked_at: DateTime,
}

pub(crate) struct MongoDeadLetterStore {
    letters: Collection<DeadLetterDocument>,
}

impl MongoDeadLetterStore {
    pub(crate) fn new(db: &Database) -> MongoDeadLetterStore {
        MongoDeadLetterStore {
            letters: db.collection(DEAD_LETTERS_COLLECTION),
        }
    }

    fn raw(&self) -> Collection<Document> {
        self.letters.clone_with_type()
    }

    /// The `_id` of the first letter matching `filter`, by position then `_id`.
    async fn first(&self, filter: Document) -> Result<Option<String>, StoreError> {
        let found = self
            .raw()
            .find_one(filter)
            .sort(doc! { "offset": 1, "_id": 1 })
            .projection(doc! { "_id": 1 })
            .await
            .store()?;
        Ok(found.and_then(|d| d.get_str("_id").ok().map(str::to_string)))
    }
}

fn by_processor(processor: Option<&str>) -> Document {
    match processor {
        Some(p) => doc! { "processor": p },
        None => doc! {},
    }
}

impl DeadLetterStore for MongoDeadLetterStore {
    fn park<'a>(&'a self, letter: &'a DeadLetter) -> BoxFuture<'a, Result<Parked, StoreError>> {
        Box::pin(async move {
            match self
                .letters
                .insert_one(DeadLetterDocument::from_letter(letter))
                .await
            {
                Ok(_) => Ok(Parked::New),
                Err(e) if is_duplicate_key(&e) => Ok(Parked::Already),
                Err(e) => Err(store_error(e)),
            }
        })
    }

    fn head<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>> {
        Box::pin(self.first(doc! {
            "processor": processor,
            "key": key,
            "reason": { "$ne": DeadLetterReason::Invalid.as_str() },
        }))
    }

    fn earlier<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
        position: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>> {
        Box::pin(self.first(doc! {
            "processor": processor,
            "key": key,
            "offset": { "$lt": positions::position(position) },
            "reason": { "$ne": DeadLetterReason::Invalid.as_str() },
        }))
    }

    fn parked_keys<'a>(
        &'a self,
        processor: &'a str,
        source: &'a str,
        segments: &'a [String],
    ) -> BoxFuture<'a, Result<HashSet<String>, StoreError>> {
        Box::pin(async move {
            if segments.is_empty() {
                return Ok(HashSet::new());
            }
            let segments: Vec<Bson> = segments.iter().map(|s| positions::segment(s)).collect();
            let keys = self
                .letters
                .distinct(
                    "key",
                    doc! {
                        "processor": processor,
                        "topic": source,
                        "partition": { "$in": segments },
                        "reason": { "$ne": DeadLetterReason::Invalid.as_str() },
                        "key": { "$exists": true },
                    },
                )
                .await
                .store()?;
            Ok(keys
                .into_iter()
                .filter_map(|k| match k {
                    Bson::String(s) => Some(s),
                    _ => None,
                })
                .collect())
        })
    }

    fn get<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<DeadLetter>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .letters
                .find_one(doc! { "_id": id })
                .await
                .store()?
                .map(DeadLetterDocument::into_letter))
        })
    }

    fn list<'a>(
        &'a self,
        processor: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxFuture<'a, Result<Vec<DeadLetter>, StoreError>> {
        Box::pin(async move {
            let mut find = self
                .letters
                .find(by_processor(processor))
                .sort(doc! { "parkedAt": 1, "_id": 1 });
            if let Some(limit) = limit {
                find = find.limit(i64::try_from(limit).unwrap_or(i64::MAX));
            }
            let docs: Vec<DeadLetterDocument> = find.await.store()?.try_collect().await.store()?;
            Ok(docs
                .into_iter()
                .map(DeadLetterDocument::into_letter)
                .collect())
        })
    }

    fn summaries<'a>(
        &'a self,
        processor: &'a str,
    ) -> BoxFuture<'a, Result<Vec<DeadLetterSummary>, StoreError>> {
        Box::pin(async move {
            let docs: Vec<SummaryDocument> = self
                .letters
                .clone_with_type::<SummaryDocument>()
                .find(doc! { "processor": processor })
                .projection(doc! { "_id": 1, "key": 1, "offset": 1, "reason": 1, "parkedAt": 1 })
                .sort(doc! { "offset": 1, "_id": 1 })
                .await
                .store()?
                .try_collect()
                .await
                .store()?;
            Ok(docs
                .into_iter()
                .map(|d| DeadLetterSummary {
                    id: d.id,
                    key: d.key,
                    position: positions::text(&d.offset).unwrap_or_default(),
                    reason: d.reason.into(),
                    parked_at_ms: d.parked_at.timestamp_millis(),
                })
                .collect())
        })
    }

    fn count<'a>(&'a self, processor: Option<&'a str>) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            self.letters
                .count_documents(by_processor(processor))
                .await
                .store()
        })
    }

    fn delete<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            Ok(self
                .letters
                .delete_one(doc! { "_id": id })
                .await
                .store()?
                .deleted_count
                == 1)
        })
    }

    fn delete_in<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            Ok(self
                .letters
                .delete_one(doc! { "_id": id })
                .session(session(tx)?)
                .await
                .store()?
                .deleted_count
                == 1)
        })
    }

    fn record_failure<'a>(
        &'a self,
        id: &'a str,
        error: &'a DeadLetterError,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            Ok(self
                .letters
                .update_one(
                    doc! { "_id": id },
                    doc! {
                        "$inc": { "attempts": 1 },
                        "$set": {
                            "reason": DeadLetterReason::HandlerFailed.as_str(),
                            "error": { "type": &error.type_name, "message": &error.message },
                            "lastAttemptAt": date(now_ms),
                        },
                        "$unset": { "blockedBy": "" },
                    },
                )
                .await
                .store()?
                .matched_count
                == 1)
        })
    }
}
