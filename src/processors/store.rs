//! The `dead_letters` operations the consumer and the admin API share.

use std::collections::HashSet;

use futures::TryStreamExt;
use mongodb::bson::{Bson, DateTime, Document, doc};
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::{Collection, Database};
use serde::Deserialize;

use super::dead_letters::{
    DEAD_LETTERS_COLLECTION, DeadLetterDocument, DeadLetterError, DeadLetterReason,
};
use crate::error::MessagingError;
use crate::store::Parked;

/// What [`DeadLetterStore::summaries`] reads of a letter.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LetterSummary {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(default)]
    pub key: Option<String>,
    pub offset: i64,
    pub reason: DeadLetterReason,
    pub parked_at: DateTime,
}

#[derive(Clone)]
pub struct DeadLetterStore {
    letters: Collection<DeadLetterDocument>,
}

impl DeadLetterStore {
    pub fn new(db: &Database) -> DeadLetterStore {
        DeadLetterStore {
            letters: db.collection(DEAD_LETTERS_COLLECTION),
        }
    }

    pub(crate) fn collection(&self) -> Collection<DeadLetterDocument> {
        self.letters.clone()
    }

    fn raw(&self) -> Collection<Document> {
        self.letters.clone_with_type()
    }

    /// A single-document insert outside any transaction; a duplicate `_id` (a redelivered record)
    /// is [`Parked::Already`].
    pub async fn park(&self, letter: &DeadLetterDocument) -> Result<Parked, MessagingError> {
        match self.letters.insert_one(letter).await {
            Ok(_) => Ok(Parked::New),
            Err(e) if is_duplicate_key(&e) => Ok(Parked::Already),
            Err(e) => Err(e.into()),
        }
    }

    /// The `_id` of the head letter of (`processor`, `key`): the lowest offset among its letters
    /// that are not INVALID.
    pub async fn head(&self, processor: &str, key: &str) -> Result<Option<String>, MessagingError> {
        self.first(doc! { "processor": processor, "key": key, "reason": { "$ne": DeadLetterReason::Invalid.as_str() } })
            .await
    }

    /// The `_id` of a letter of (`processor`, `key`) with an offset below `offset` (not INVALID):
    /// what blocks a retry.
    pub async fn earlier(
        &self,
        processor: &str,
        key: &str,
        offset: i64,
    ) -> Result<Option<String>, MessagingError> {
        self.first(doc! {
            "processor": processor,
            "key": key,
            "offset": { "$lt": offset },
            "reason": { "$ne": DeadLetterReason::Invalid.as_str() },
        })
        .await
    }

    async fn first(&self, filter: Document) -> Result<Option<String>, MessagingError> {
        let found = self
            .raw()
            .find_one(filter)
            .sort(doc! { "offset": 1, "_id": 1 })
            .projection(doc! { "_id": 1 })
            .await?;
        Ok(found.and_then(|d| d.get_str("_id").ok().map(str::to_string)))
    }

    /// The keys with a letter (not INVALID) of `processor` on the given partitions of `topic`:
    /// the consumer's in-memory sequencing set after an assignment.
    pub async fn parked_keys(
        &self,
        processor: &str,
        topic: &str,
        partitions: &[i32],
    ) -> Result<HashSet<String>, MessagingError> {
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
            .await?;
        Ok(keys
            .into_iter()
            .filter_map(|k| match k {
                Bson::String(s) => Some(s),
                _ => None,
            })
            .collect())
    }

    pub async fn get(&self, id: &str) -> Result<Option<DeadLetterDocument>, MessagingError> {
        Ok(self.letters.find_one(doc! { "_id": id }).await?)
    }

    /// Letters sorted by `parkedAt`, then `_id`; every processor's when `processor` is `None`.
    pub async fn list(
        &self,
        processor: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DeadLetterDocument>, MessagingError> {
        let filter = match processor {
            Some(p) => doc! { "processor": p },
            None => doc! {},
        };
        let mut find = self
            .letters
            .find(filter)
            .sort(doc! { "parkedAt": 1, "_id": 1 });
        if let Some(limit) = limit {
            find = find.limit(i64::try_from(limit).unwrap_or(i64::MAX));
        }
        Ok(find.await?.try_collect().await?)
    }

    /// Every letter of `processor` without its record (`_id`, `key`, `offset`, `reason`,
    /// `parkedAt` only): what orders a `retry_all`, which then reads each letter when its turn
    /// comes.
    pub(crate) async fn summaries(
        &self,
        processor: &str,
    ) -> Result<Vec<LetterSummary>, MessagingError> {
        Ok(self
            .letters
            .clone_with_type::<LetterSummary>()
            .find(doc! { "processor": processor })
            .projection(doc! { "_id": 1, "key": 1, "offset": 1, "reason": 1, "parkedAt": 1 })
            .await?
            .try_collect()
            .await?)
    }

    pub async fn count(&self, processor: Option<&str>) -> Result<u64, MessagingError> {
        let filter = match processor {
            Some(p) => doc! { "processor": p },
            None => doc! {},
        };
        Ok(self.letters.count_documents(filter).await?)
    }

    pub async fn delete(&self, id: &str) -> Result<bool, MessagingError> {
        Ok(self
            .letters
            .delete_one(doc! { "_id": id })
            .await?
            .deleted_count
            == 1)
    }

    /// A retry failed again: `attempts + 1`, the new error, `lastAttemptAt`, reason
    /// HANDLER_FAILED, no `blockedBy` any more. False when the letter is gone (retried or
    /// discarded meanwhile, possibly on another instance).
    pub async fn record_failure(
        &self,
        id: &str,
        error: &DeadLetterError,
        now: DateTime,
    ) -> Result<bool, MessagingError> {
        Ok(self
            .letters
            .update_one(
                doc! { "_id": id },
                doc! {
                    "$inc": { "attempts": 1 },
                    "$set": {
                        "reason": DeadLetterReason::HandlerFailed.as_str(),
                        "error": { "type": &error.type_name, "message": &error.message },
                        "lastAttemptAt": now,
                    },
                    "$unset": { "blockedBy": "" },
                },
            )
            .await?
            .matched_count
            == 1)
    }
}

fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    matches!(&*e.kind, ErrorKind::Write(WriteFailure::WriteError(w)) if w.code == 11000)
}
