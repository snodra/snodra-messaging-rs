//! The `processor_control` collection: one document per processor, `_id` its name.
//! Every field is always written (`null` or `[]` when there is nothing), as Kotlin writes it; a
//! reader takes a missing field for `null`, `false` or `[]`, and a missing document for a
//! processor that is not paused, not locked and not replaying.

use futures::future::BoxFuture;
use mongodb::bson::{Bson, DateTime, Document, doc};
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};
use snodra_messaging::error::MessagingError;
use snodra_messaging::event_store::ReplayRange;
use snodra_messaging::store::{
    ControlOperation, Locked, ProcessorControlState, ProcessorControlStore, RESET_KIND, StoreError,
};

use crate::{OrStoreError, date, is_duplicate_key, positions, store_error};

pub(crate) const PROCESSOR_CONTROL_COLLECTION: &str = "processor_control";

/// One processor's document; see the module docs.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlDocument {
    #[serde(rename = "_id")]
    id: String,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    paused_at: Option<DateTime>,
    #[serde(default)]
    paused_by: Option<String>,
    #[serde(default)]
    operation: Option<OperationDocument>,
    #[serde(default)]
    replay: Vec<RangeDocument>,
    #[serde(default)]
    updated_at: Option<DateTime>,
}

impl ControlDocument {
    fn into_state(self) -> ProcessorControlState {
        ProcessorControlState {
            processor: self.id,
            paused: self.paused,
            paused_at_ms: self.paused_at.map(|t| t.timestamp_millis()),
            paused_by: self.paused_by,
            operation: self.operation.map(OperationDocument::into_operation),
            replay: self.replay.into_iter().map(ReplayRange::from).collect(),
            updated_at_ms: self.updated_at.map(|t| t.timestamp_millis()),
        }
    }
}

/// The reset holding a processor's lock.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationDocument {
    id: String,
    kind: String,
    lease_until: DateTime,
    prepared_at: DateTime,
}

impl OperationDocument {
    fn into_operation(self) -> ControlOperation {
        ControlOperation {
            id: self.id,
            kind: self.kind,
            lease_until_ms: self.lease_until.timestamp_millis(),
            prepared_at_ms: self.prepared_at.timestamp_millis(),
        }
    }
}

/// A replay range: `{topic, partition, from, until}`, the source as the topic and the segment and
/// positions as [`positions`](crate::positions) stores them (an Int32 and Int64s for decimal
/// ones, as always).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RangeDocument {
    topic: String,
    partition: Bson,
    from: Bson,
    until: Bson,
}

impl From<RangeDocument> for ReplayRange {
    fn from(r: RangeDocument) -> Self {
        let text = |b: &Bson| positions::text(b).unwrap_or_default();
        ReplayRange {
            source: r.topic,
            segment: text(&r.partition),
            from: text(&r.from),
            until: text(&r.until),
        }
    }
}

fn range_document(range: &ReplayRange) -> Document {
    doc! {
        "topic": &range.source,
        "partition": positions::segment(&range.segment),
        "from": positions::position(&range.from),
        "until": positions::position(&range.until),
    }
}

pub(crate) struct MongoProcessorControlStore {
    docs: Collection<ControlDocument>,
}

impl MongoProcessorControlStore {
    pub(crate) fn new(db: &Database) -> MongoProcessorControlStore {
        MongoProcessorControlStore {
            docs: db.collection(PROCESSOR_CONTROL_COLLECTION),
        }
    }

    fn raw(&self) -> Collection<Document> {
        self.docs.clone_with_type()
    }

    /// What an upsert sets when it creates the document, besides the fields it sets itself.
    fn on_insert(skip: &[&str]) -> Document {
        let mut fields = doc! {
            "paused": false,
            "pausedAt": null,
            "pausedBy": null,
            "operation": null,
            "replay": [],
        };
        for field in skip {
            fields.remove(*field);
        }
        fields
    }

    async fn state(&self, processor: &str) -> Result<ProcessorControlState, StoreError> {
        let found = self
            .docs
            .find_one(doc! { "_id": processor })
            .await
            .store()?;
        Ok(found
            .unwrap_or_else(|| ControlDocument {
                id: processor.to_string(),
                ..ControlDocument::default()
            })
            .into_state())
    }

    async fn extend_lease(
        &self,
        processor: &str,
        operation: &str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        Ok(self
            .raw()
            .update_one(
                doc! { "_id": processor, "operation.id": operation },
                doc! { "$set": {
                    "operation.leaseUntil": date(lease_until_ms),
                    "updatedAt": date(now_ms),
                } },
            )
            .await
            .store()?
            .matched_count
            == 1)
    }
}

impl ProcessorControlStore for MongoProcessorControlStore {
    fn get<'a>(
        &'a self,
        processor: &'a str,
    ) -> BoxFuture<'a, Result<ProcessorControlState, StoreError>> {
        Box::pin(self.state(processor))
    }

    fn set_paused<'a>(
        &'a self,
        processor: &'a str,
        paused: bool,
        by: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let now = date(now_ms);
            let set = if paused {
                doc! { "paused": true, "pausedAt": now, "pausedBy": by, "updatedAt": now }
            } else {
                doc! { "paused": false, "pausedAt": null, "pausedBy": null, "updatedAt": now }
            };
            self.raw()
                .update_one(
                    doc! { "_id": processor },
                    doc! {
                        "$set": set,
                        "$setOnInsert": Self::on_insert(&["paused", "pausedAt", "pausedBy"]),
                    },
                )
                .upsert(true)
                .await
                .store()?;
            Ok(())
        })
    }

    /// One conditional upsert: when nothing holds the lock or the holder's lease has passed. A
    /// duplicate key on the upsert means another operation won (or this one did on another
    /// instance: then it is a renewal).
    fn lock<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<Locked, StoreError>> {
        Box::pin(async move {
            if self
                .extend_lease(processor, operation, lease_until_ms, now_ms)
                .await?
            {
                return Ok(Locked::Taken);
            }
            let now = date(now_ms);
            let taken = self
                .raw()
                .update_one(
                    doc! {
                        "_id": processor,
                        "$or": [
                            { "operation": null },
                            { "operation.leaseUntil": { "$lte": now } },
                        ],
                    },
                    doc! {
                        "$set": {
                            "operation": {
                                "id": operation,
                                "kind": RESET_KIND,
                                "leaseUntil": date(lease_until_ms),
                                "preparedAt": now,
                            },
                            "updatedAt": now,
                        },
                        "$setOnInsert": Self::on_insert(&["operation"]),
                    },
                )
                .upsert(true)
                .await;
            match taken {
                Ok(_) => Ok(Locked::Taken),
                Err(e) if is_duplicate_key(&e) => {
                    if self
                        .extend_lease(processor, operation, lease_until_ms, now_ms)
                        .await?
                    {
                        return Ok(Locked::Taken);
                    }
                    match self.state(processor).await?.operation {
                        Some(held) => Ok(Locked::Held(held)),
                        // Released meanwhile: the caller may try again.
                        None => Err(StoreError::Failed(MessagingError::Storage(format!(
                            "the lock of processor '{processor}' changed while it was taken"
                        )))),
                    }
                }
                Err(e) => Err(store_error(e)),
            }
        })
    }

    fn extend<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(self.extend_lease(processor, operation, lease_until_ms, now_ms))
    }

    fn release<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            Ok(self
                .raw()
                .update_one(
                    doc! { "_id": processor, "operation.id": operation },
                    doc! { "$set": { "operation": null, "updatedAt": date(now_ms) } },
                )
                .await
                .store()?
                .matched_count
                == 1)
        })
    }

    fn expire<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let now = date(now_ms);
            Ok(self
                .raw()
                .update_one(
                    doc! {
                        "_id": processor,
                        "operation.id": operation,
                        "operation.leaseUntil": { "$lte": now },
                    },
                    doc! { "$set": { "operation": null, "updatedAt": now } },
                )
                .await
                .store()?
                .matched_count
                == 1)
        })
    }

    fn set_replay<'a>(
        &'a self,
        processor: &'a str,
        ranges: &'a [ReplayRange],
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let ranges: Vec<Document> = ranges.iter().map(range_document).collect();
            self.raw()
                .update_one(
                    doc! { "_id": processor },
                    doc! {
                        "$set": { "replay": ranges, "updatedAt": date(now_ms) },
                        "$setOnInsert": Self::on_insert(&["replay"]),
                    },
                )
                .upsert(true)
                .await
                .store()?;
            Ok(())
        })
    }

    fn remove_range<'a>(
        &'a self,
        processor: &'a str,
        range: &'a ReplayRange,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let range = range_document(range);
            Ok(self
                .raw()
                .update_one(
                    doc! { "_id": processor, "replay": { "$elemMatch": range.clone() } },
                    doc! {
                        "$pull": { "replay": range },
                        "$set": { "updatedAt": date(now_ms) },
                    },
                )
                .await
                .store()?
                .modified_count
                == 1)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(segment: &str, from: &str, until: &str) -> ReplayRange {
        ReplayRange {
            source: "snodra.events".into(),
            segment: segment.into(),
            from: from.into(),
            until: until.into(),
        }
    }

    #[test]
    fn a_range_is_stored_as_topic_partition_and_offsets_and_read_back() {
        let stored = range_document(&range("3", "10", "20"));
        assert_eq!(
            stored,
            doc! { "topic": "snodra.events", "partition": 3_i32, "from": 10_i64, "until": 20_i64 }
        );
        let read: RangeDocument = mongodb::bson::from_document(stored).expect("read");
        assert_eq!(ReplayRange::from(read), range("3", "10", "20"));
    }

    #[test]
    fn a_range_that_is_not_decimal_is_stored_as_text_and_read_back() {
        let odd = range("p-0", "a1", "b2");
        let stored = range_document(&odd);
        assert_eq!(
            stored,
            doc! { "topic": "snodra.events", "partition": "p-0", "from": "a1", "until": "b2" }
        );
        let read: RangeDocument = mongodb::bson::from_document(stored).expect("read");
        assert_eq!(ReplayRange::from(read), odd);
    }
}
