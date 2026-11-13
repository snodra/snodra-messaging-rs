//! The `dead_letters` collection (ADR 0022, mode `collection`): the document, field for field the
//! contract both libraries write. A letter's `_id` is `<processor>:<topic>:<partition>:<offset>`,
//! so parking the same record twice finds the first letter (a duplicate key means "already
//! parked"). Dates are BSON datetimes, `partition` an Int32, `offset` an Int64; an absent field is
//! absent (never null) and no `_class` is written. The value is `payload` (text) when it is valid
//! UTF-8, `payloadBinary` when it is not, neither when it is empty.

use std::collections::{BTreeMap, HashSet};

use futures::TryStreamExt;
use futures::future::BoxFuture;
use kraken_messaging::store::{
    DeadLetter, DeadLetterError, DeadLetterReason, DeadLetterStore, DeadLetterSummary, Parked,
    StoreError,
};
use mongodb::bson::{Binary, Bson, DateTime, Document, doc, spec::BinarySubtype};
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};

use crate::{OrStoreError, date, is_duplicate_key, store_error};

pub(crate) const DEAD_LETTERS_COLLECTION: &str = "dead_letters";

/// One parked record; see the module docs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeadLetterDocument {
    #[serde(rename = "_id")]
    id: String,
    processor: String,
    topic: String,
    partition: i32,
    offset: i64,
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
    reason: DeadLetterReason,
    error: DeadLetterError,
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
            topic: letter.topic.clone(),
            partition: letter.partition,
            offset: letter.offset,
            key: letter.key.clone(),
            event_id: letter.event_id.clone(),
            event_type: letter.event_type.clone(),
            headers: letter.headers.clone(),
            payload,
            payload_binary,
            reason: letter.reason,
            error: letter.error.clone(),
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
            topic: self.topic,
            partition: self.partition,
            offset: self.offset,
            key: self.key,
            event_id: self.event_id,
            event_type: self.event_type,
            headers: self.headers,
            payload,
            reason: self.reason,
            error: self.error,
            blocked_by: self.blocked_by,
            attempts: self.attempts,
            parked_at_ms: self.parked_at.timestamp_millis(),
            last_attempt_at_ms: self.last_attempt_at.timestamp_millis(),
            expire_at_ms: self.expire_at.timestamp_millis(),
            replay: self.replay,
        }
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
    offset: i64,
    reason: DeadLetterReason,
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

    /// The `_id` of the first letter matching `filter`, by offset then `_id`.
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
        offset: i64,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>> {
        Box::pin(self.first(doc! {
            "processor": processor,
            "key": key,
            "offset": { "$lt": offset },
            "reason": { "$ne": DeadLetterReason::Invalid.as_str() },
        }))
    }

    fn parked_keys<'a>(
        &'a self,
        processor: &'a str,
        topic: &'a str,
        partitions: &'a [i32],
    ) -> BoxFuture<'a, Result<HashSet<String>, StoreError>> {
        Box::pin(async move {
            if partitions.is_empty() {
                return Ok(HashSet::new());
            }
            let keys = self
                .letters
                .distinct(
                    "key",
                    doc! {
                        "processor": processor,
                        "topic": topic,
                        "partition": { "$in": partitions.to_vec() },
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
                    offset: d.offset,
                    reason: d.reason,
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
