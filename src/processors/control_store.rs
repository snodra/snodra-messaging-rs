//! The `processor_control` collection (ADR 0023): one document per processor, `_id` its name,
//! the source of truth of its pause, its reset lock and its replay ranges (the hub keeps none of
//! them). Every field is always written (`null` or `[]` when there is nothing), as Kotlin writes
//! it; a reader takes a missing field for `null`, `false` or `[]`, and a missing document for a
//! processor that is not paused, not locked and not replaying.

use mongodb::bson::{DateTime, Document, doc};
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};

use crate::error::MessagingError;

pub const PROCESSOR_CONTROL_COLLECTION: &str = "processor_control";
/// `operation.kind` of a reset, the only kind there is.
pub const RESET_KIND: &str = "reset";
/// `pausedBy` of a pause through [`ProcessorControl::pause`](super::ProcessorControl::pause).
pub const PAUSED_LOCALLY: &str = "local";

/// One processor's document; see the module docs.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlDocument {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub paused_at: Option<DateTime>,
    /// The `control_id` of the request that paused it, or `local`.
    #[serde(default)]
    pub paused_by: Option<String>,
    /// The reset holding the processor's lock.
    #[serde(default)]
    pub operation: Option<Operation>,
    /// The open replay ranges.
    #[serde(default)]
    pub replay: Vec<ReplayRange>,
    #[serde(default)]
    pub updated_at: Option<DateTime>,
}

/// The lock of a reset: held until `leaseUntil` unless renewed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub id: String,
    pub kind: String,
    pub lease_until: DateTime,
    /// The first prepare's time; a renewal keeps it.
    pub prepared_at: DateTime,
}

impl Operation {
    /// Whether the lease has not passed at `now`.
    pub fn holds_at(&self, now: DateTime) -> bool {
        self.lease_until > now
    }
}

/// Records of `partition` of `topic` with `from <= offset < until` are a replay.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplayRange {
    pub topic: String,
    pub partition: i32,
    pub from: i64,
    pub until: i64,
}

impl ReplayRange {
    fn document(&self) -> Document {
        doc! { "topic": &self.topic, "partition": self.partition, "from": self.from, "until": self.until }
    }
}

/// Whether [`ControlStore::lock`] took (or renewed) the lock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Locked {
    /// The operation holds the lock now.
    Taken,
    /// Another operation holds it (unexpired).
    Held(Operation),
}

/// The `processor_control` operations of the runner and the executor.
#[derive(Clone)]
pub struct ControlStore {
    docs: Collection<ControlDocument>,
}

impl ControlStore {
    pub fn new(db: &Database) -> ControlStore {
        ControlStore {
            docs: db.collection(PROCESSOR_CONTROL_COLLECTION),
        }
    }

    fn raw(&self) -> Collection<Document> {
        self.docs.clone_with_type()
    }

    /// The document of `processor`; an absent one is the default (not paused, nothing held).
    pub async fn get(&self, processor: &str) -> Result<ControlDocument, MessagingError> {
        Ok(self
            .docs
            .find_one(doc! { "_id": processor })
            .await?
            .unwrap_or_else(|| ControlDocument {
                id: processor.to_string(),
                ..ControlDocument::default()
            }))
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

    /// Pauses (`by` the control id, or `local`) or resumes `processor`; idempotent. A resume
    /// clears `pausedAt` and `pausedBy`.
    pub async fn set_paused(
        &self,
        processor: &str,
        paused: bool,
        by: &str,
        now: DateTime,
    ) -> Result<(), MessagingError> {
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
            .await?;
        Ok(())
    }

    /// Takes the lock of `processor` for `operation` until `lease_until`, with one conditional
    /// update: when nothing holds it or the holder's lease has passed; a renewal (the operation
    /// holds it already) moves `leaseUntil` only. A duplicate key on the upsert means another
    /// operation won (or this one did on another instance: then it is a renewal).
    pub async fn lock(
        &self,
        processor: &str,
        operation: &str,
        lease_until: DateTime,
        now: DateTime,
    ) -> Result<Locked, MessagingError> {
        if self.extend(processor, operation, lease_until, now).await? {
            return Ok(Locked::Taken);
        }
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
                            "leaseUntil": lease_until,
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
                if self.extend(processor, operation, lease_until, now).await? {
                    return Ok(Locked::Taken);
                }
                match self.get(processor).await?.operation {
                    Some(held) => Ok(Locked::Held(held)),
                    // Released meanwhile: the caller may try again.
                    None => Err(MessagingError::Storage(format!(
                        "the lock of processor '{processor}' changed while it was taken"
                    ))),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Moves `leaseUntil` of `operation` (a renewal, or the executor keeping its own lease);
    /// false when the operation does not hold the lock.
    pub async fn extend(
        &self,
        processor: &str,
        operation: &str,
        lease_until: DateTime,
        now: DateTime,
    ) -> Result<bool, MessagingError> {
        Ok(self
            .raw()
            .update_one(
                doc! { "_id": processor, "operation.id": operation },
                doc! { "$set": { "operation.leaseUntil": lease_until, "updatedAt": now } },
            )
            .await?
            .matched_count
            == 1)
    }

    /// Clears the lock when `operation` holds it (never another operation's); false when it
    /// did not.
    pub async fn release(
        &self,
        processor: &str,
        operation: &str,
        now: DateTime,
    ) -> Result<bool, MessagingError> {
        Ok(self
            .raw()
            .update_one(
                doc! { "_id": processor, "operation.id": operation },
                doc! { "$set": { "operation": null, "updatedAt": now } },
            )
            .await?
            .matched_count
            == 1)
    }

    /// Clears the lock when `operation` holds it and its lease has passed at `now` (a renewal
    /// that came meanwhile keeps it).
    pub async fn expire(
        &self,
        processor: &str,
        operation: &str,
        now: DateTime,
    ) -> Result<bool, MessagingError> {
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
            .await?
            .matched_count
            == 1)
    }

    /// Replaces the replay ranges of `processor`.
    pub async fn set_replay(
        &self,
        processor: &str,
        ranges: &[ReplayRange],
        now: DateTime,
    ) -> Result<(), MessagingError> {
        let ranges: Vec<Document> = ranges.iter().map(ReplayRange::document).collect();
        self.raw()
            .update_one(
                doc! { "_id": processor },
                doc! {
                    "$set": { "replay": ranges, "updatedAt": now },
                    "$setOnInsert": Self::on_insert(&["replay"]),
                },
            )
            .upsert(true)
            .await?;
        Ok(())
    }

    /// Removes `range` (that exact item) from the replay ranges of `processor`; false when it
    /// was not there (another instance removed it, or a reset replaced it).
    pub async fn remove_range(
        &self,
        processor: &str,
        range: &ReplayRange,
        now: DateTime,
    ) -> Result<bool, MessagingError> {
        Ok(self
            .raw()
            .update_one(
                doc! { "_id": processor, "replay": { "$elemMatch": range.document() } },
                doc! { "$pull": { "replay": range.document() }, "$set": { "updatedAt": now } },
            )
            .await?
            .modified_count
            == 1)
    }
}

fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    matches!(&*e.kind, ErrorKind::Write(WriteFailure::WriteError(w)) if w.code == 11000)
}
