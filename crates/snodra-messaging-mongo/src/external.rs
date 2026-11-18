//! The `external_calls` collection (ADR 0021): one record per idempotency key (`_id`), with
//! `status` (`IN_PROGRESS`, `SUCCEEDED`, `FAILED`, `UNKNOWN`), `reply` and `error` as JSON text,
//! `attempts`, `updatedAt`, and `leaseUntil` and `owner` while a call runs. An absent field is
//! absent (never null). Every write is a single-document write of its own, outside any session.

use futures::future::BoxFuture;
use mongodb::bson::{Bson, DateTime, Document, doc};
use mongodb::options::ReturnDocument;
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};
use snodra_messaging::store::{ExternalCall, ExternalCallStatus, ExternalCallStore, StoreError};

use crate::{OrStoreError, date, is_duplicate_key, store_error};

pub(crate) const EXTERNAL_CALLS_COLLECTION: &str = "external_calls";

const IN_PROGRESS: &str = "IN_PROGRESS";
const SUCCEEDED: &str = "SUCCEEDED";
const FAILED: &str = "FAILED";
const UNKNOWN: &str = "UNKNOWN";

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StatusDocument {
    InProgress,
    Succeeded,
    Failed,
    Unknown,
}

/// One `external_calls` document.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalCallDocument {
    #[serde(rename = "_id")]
    id: String,
    status: StatusDocument,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reply: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(default)]
    attempts: i32,
    updated_at: DateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lease_until: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
}

impl ExternalCallDocument {
    fn from_call(call: &ExternalCall) -> ExternalCallDocument {
        ExternalCallDocument {
            id: call.key.clone(),
            status: match call.status {
                ExternalCallStatus::InProgress => StatusDocument::InProgress,
                ExternalCallStatus::Succeeded => StatusDocument::Succeeded,
                ExternalCallStatus::Failed => StatusDocument::Failed,
                ExternalCallStatus::Unknown => StatusDocument::Unknown,
            },
            reply: call.reply.clone(),
            error: call.error.clone(),
            attempts: call.attempts,
            updated_at: date(call.updated_at_ms),
            lease_until: call.lease_until_ms.map(date),
            owner: call.owner.clone(),
        }
    }

    fn into_call(self) -> ExternalCall {
        ExternalCall {
            key: self.id,
            status: match self.status {
                StatusDocument::InProgress => ExternalCallStatus::InProgress,
                StatusDocument::Succeeded => ExternalCallStatus::Succeeded,
                StatusDocument::Failed => ExternalCallStatus::Failed,
                StatusDocument::Unknown => ExternalCallStatus::Unknown,
            },
            reply: self.reply,
            error: self.error,
            attempts: self.attempts,
            updated_at_ms: self.updated_at.timestamp_millis(),
            lease_until_ms: self.lease_until.map(|t| t.timestamp_millis()),
            owner: self.owner,
        }
    }
}

impl From<&ExternalCall> for ExternalCallDocument {
    fn from(call: &ExternalCall) -> Self {
        ExternalCallDocument::from_call(call)
    }
}

impl From<ExternalCallDocument> for ExternalCall {
    fn from(doc: ExternalCallDocument) -> Self {
        doc.into_call()
    }
}

/// The record a call owns: its key, `IN_PROGRESS` and its `owner` token.
fn owned(key: &str, owner: &str) -> Document {
    doc! { "_id": key, "status": IN_PROGRESS, "owner": owner }
}

/// The record a call read: its key, `IN_PROGRESS`, `owner` and `leaseUntil` as read.
fn as_read(seen: &ExternalCall) -> Document {
    doc! {
        "_id": &seen.key,
        "status": IN_PROGRESS,
        "owner": seen.owner.clone().map(Bson::String).unwrap_or(Bson::Null),
        "leaseUntil": seen.lease_until_ms.map(|ms| Bson::DateTime(date(ms))).unwrap_or(Bson::Null),
    }
}

/// `$set` `status` and `updatedAt` (and `extra`), `$unset` the owner and the lease (and the
/// fields of `unset`): a settled record.
fn settle(status: &str, now_ms: i64, extra: Document, unset: &[&str]) -> Document {
    let mut set = doc! { "status": status, "updatedAt": date(now_ms) };
    set.extend(extra);
    let mut removed = doc! { "leaseUntil": "", "owner": "" };
    for field in unset {
        removed.insert(*field, "");
    }
    doc! { "$set": set, "$unset": removed }
}

pub(crate) struct MongoExternalCallStore {
    calls: Collection<ExternalCallDocument>,
}

impl MongoExternalCallStore {
    pub(crate) fn new(db: &Database) -> MongoExternalCallStore {
        MongoExternalCallStore {
            calls: db.collection(EXTERNAL_CALLS_COLLECTION),
        }
    }

    async fn update(&self, filter: Document, update: Document) -> Result<bool, StoreError> {
        Ok(self
            .calls
            .update_one(filter, update)
            .await
            .store()?
            .matched_count
            == 1)
    }
}

impl ExternalCallStore for MongoExternalCallStore {
    fn insert<'a>(&'a self, call: &'a ExternalCall) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            match self
                .calls
                .insert_one(ExternalCallDocument::from_call(call))
                .await
            {
                Ok(_) => Ok(true),
                Err(e) if is_duplicate_key(&e) => Ok(false),
                Err(e) => Err(store_error(e)),
            }
        })
    }

    fn find<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ExternalCall>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .calls
                .find_one(doc! { "_id": key })
                .await
                .store()?
                .map(ExternalCallDocument::into_call))
        })
    }

    fn take_over<'a>(
        &'a self,
        seen: &'a ExternalCall,
        owner: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<Option<ExternalCall>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .calls
                .find_one_and_update(
                    as_read(seen),
                    doc! {
                        "$set": {
                            "leaseUntil": date(lease_until_ms),
                            "updatedAt": date(now_ms),
                            "owner": owner,
                        },
                        "$inc": { "attempts": 1 },
                    },
                )
                .return_document(ReturnDocument::After)
                .await
                .store()?
                .map(ExternalCallDocument::into_call))
        })
    }

    fn expire<'a>(
        &'a self,
        seen: &'a ExternalCall,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(self.update(as_read(seen), settle(UNKNOWN, now_ms, doc! {}, &[])))
    }

    fn succeed<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        reply: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(self.update(
            owned(key, owner),
            settle(SUCCEEDED, now_ms, doc! { "reply": reply }, &["error"]),
        ))
    }

    fn fail<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        error: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(self.update(
            owned(key, owner),
            settle(FAILED, now_ms, doc! { "error": error }, &[]),
        ))
    }

    fn mark_unknown<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(self.update(owned(key, owner), settle(UNKNOWN, now_ms, doc! {}, &[])))
    }

    fn reset<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            Ok(self
                .calls
                .delete_one(owned(key, owner))
                .await
                .store()?
                .deleted_count
                == 1)
        })
    }
}
