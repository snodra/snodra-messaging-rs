//! The saga collections (ADR 0021's storage contract, field for field, [`crate::documents`]):
//! `sagas`, `saga_dispatch`, `saga_dispatch_leases`, `deadlines`, `saga_failures`, and the saga
//! stage's markers in `processed_events`. The runtime's ports join its transaction (the [`Tx`]'s
//! session); the scheduler's, the relay's and the statistics' write on their own.

use std::collections::HashMap;

use futures::TryStreamExt;
use futures::future::BoxFuture;
use kraken_messaging::saga::store::{
    DeadlineDocument, DispatchKind, SagaDispatchDocument, SagaDocument, SagaFailureDocument,
};
use kraken_messaging::store::{
    DeadlineStore, DispatchFailure, FailureStore, MarkerStore, SagaDispatchStore, SagaRepository,
    StoreError, Tx,
};
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::ReturnDocument;
use mongodb::{Collection, Database};

use crate::documents::{DeadlineDoc, DispatchDoc, FailureDoc, LeaseDoc, SagaDoc};
use crate::processed::PROCESSED_EVENTS_COLLECTION;
use crate::{OrStoreError, date, is_duplicate_key, session};

pub(crate) const SAGAS_COLLECTION: &str = "sagas";
pub(crate) const SAGA_DISPATCH_COLLECTION: &str = "saga_dispatch";
pub(crate) const SAGA_DISPATCH_LEASES_COLLECTION: &str = "saga_dispatch_leases";
pub(crate) const DEADLINES_COLLECTION: &str = "deadlines";
pub(crate) const SAGA_FAILURES_COLLECTION: &str = "saga_failures";

/// Every saga port.
pub(crate) struct MongoSagaStore {
    db: Database,
}

impl MongoSagaStore {
    pub(crate) fn new(db: &Database) -> MongoSagaStore {
        MongoSagaStore { db: db.clone() }
    }

    fn sagas(&self) -> Collection<SagaDoc> {
        self.db.collection(SAGAS_COLLECTION)
    }

    fn dispatch(&self) -> Collection<DispatchDoc> {
        self.db.collection(SAGA_DISPATCH_COLLECTION)
    }

    fn leases(&self) -> Collection<LeaseDoc> {
        self.db.collection(SAGA_DISPATCH_LEASES_COLLECTION)
    }

    fn deadlines(&self) -> Collection<DeadlineDoc> {
        self.db.collection(DEADLINES_COLLECTION)
    }

    fn failures(&self) -> Collection<FailureDoc> {
        self.db.collection(SAGA_FAILURES_COLLECTION)
    }

    /// `$set` on one `saga_dispatch` entry, outside any transaction.
    async fn set_on_entry(&self, id: &str, update: Document) -> Result<(), StoreError> {
        self.dispatch()
            .update_one(doc! { "_id": id }, update)
            .await
            .store()?;
        Ok(())
    }

    fn markers(&self) -> Collection<Document> {
        self.db.collection(PROCESSED_EVENTS_COLLECTION)
    }

    async fn cancel_where(
        &self,
        tx: &mut Tx,
        mut filter: Document,
        now: i64,
    ) -> Result<(), StoreError> {
        filter.insert("status", "SCHEDULED");
        self.deadlines()
            .update_many(
                filter,
                doc! { "$set": { "status": "CANCELLED", "finishedAt": date(now) } },
            )
            .session(session(tx)?)
            .await
            .store()?;
        Ok(())
    }
}

impl SagaRepository for MongoSagaStore {
    fn find_by_id<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDocument>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .sagas()
                .find_one(doc! { "_id": id })
                .session(session(tx)?)
                .await
                .store()?
                .map(SagaDocument::from))
        })
    }

    fn find_live<'a>(
        &'a self,
        tx: &'a mut Tx,
        type_name: &'a str,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<Vec<SagaDocument>, StoreError>> {
        Box::pin(async move {
            let session = session(tx)?;
            let mut cursor = self
                .sagas()
                .find(doc! {
                    "type": type_name,
                    "ended": false,
                    "associations": { "$elemMatch": { "k": key, "v": value } },
                })
                .session(&mut *session)
                .await
                .store()?;
            let found: Vec<SagaDoc> = cursor.stream(session).try_collect().await.store()?;
            Ok(found.into_iter().map(SagaDocument::from).collect())
        })
    }

    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.sagas()
                .insert_one(SagaDoc::from(saga))
                .session(session(tx)?)
                .await
                .store()?;
            Ok(())
        })
    }

    fn update<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
        expected_version: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let result = self
                .sagas()
                .replace_one(
                    doc! { "_id": &saga.id, "version": expected_version },
                    SagaDoc::from(saga),
                )
                .session(session(tx)?)
                .await
                .store()?;
            Ok(result.matched_count == 1)
        })
    }

    fn count_live_by_type(&self) -> BoxFuture<'_, Result<HashMap<String, u64>, StoreError>> {
        Box::pin(async move {
            let groups: Vec<Document> = self
                .db
                .collection::<Document>(SAGAS_COLLECTION)
                .aggregate([
                    doc! { "$match": { "ended": false } },
                    doc! { "$group": { "_id": "$type", "n": { "$sum": 1 } } },
                ])
                .await
                .store()?
                .try_collect()
                .await
                .store()?;
            Ok(counts(groups))
        })
    }
}

/// `{_id: <name>, n: <count>}` groups as a map.
fn counts(groups: Vec<Document>) -> HashMap<String, u64> {
    groups
        .into_iter()
        .filter_map(|g| {
            let name = g.get_str("_id").ok()?.to_string();
            let n = match g.get("n") {
                Some(Bson::Int32(n)) => u64::try_from(*n).ok()?,
                Some(Bson::Int64(n)) => u64::try_from(*n).ok()?,
                _ => return None,
            };
            Some((name, n))
        })
        .collect()
}

impl DeadlineStore for MongoSagaStore {
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        deadline: &'a DeadlineDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.deadlines()
                .insert_one(DeadlineDoc::from(deadline))
                .session(session(tx)?)
                .await
                .store()?;
            Ok(())
        })
    }

    fn find_scheduled<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .deadlines()
                .find_one(doc! { "_id": id, "status": "SCHEDULED" })
                .session(session(tx)?)
                .await
                .store()?
                .map(DeadlineDocument::from))
        })
    }

    fn cancel<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        deadline_id: &'a str,
        now: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.cancel_where(tx, doc! { "_id": deadline_id, "sagaId": saga_id }, now)
                .await
        })
    }

    fn cancel_by_name<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        name: &'a str,
        now: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.cancel_where(tx, doc! { "sagaId": saga_id, "name": name }, now)
                .await
        })
    }

    fn cancel_all<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        now: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move { self.cancel_where(tx, doc! { "sagaId": saga_id }, now).await })
    }

    fn mark_fired<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let result = self
                .deadlines()
                .update_one(
                    doc! { "_id": id, "status": "SCHEDULED" },
                    doc! { "$set": { "status": "FIRED", "finishedAt": date(now) } },
                )
                .session(session(tx)?)
                .await
                .store()?;
            Ok(result.modified_count == 1)
        })
    }

    fn claim_due<'a>(
        &'a self,
        owner: &'a str,
        now_ms: i64,
        claimed_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>> {
        Box::pin(async move {
            let now = date(now_ms);
            Ok(self
                .deadlines()
                .find_one_and_update(
                    doc! {
                        "status": "SCHEDULED",
                        "dueAt": { "$lte": now },
                        "$or": [ { "claimedUntil": null }, { "claimedUntil": { "$lt": now } } ],
                    },
                    doc! { "$set": { "claimedBy": owner, "claimedUntil": date(claimed_until_ms) } },
                )
                .sort(doc! { "dueAt": 1 })
                .return_document(ReturnDocument::After)
                .await
                .store()?
                .map(DeadlineDocument::from))
        })
    }

    fn count_overdue_by_name(
        &self,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<HashMap<String, u64>, StoreError>> {
        Box::pin(async move {
            let groups: Vec<Document> = self
                .db
                .collection::<Document>(DEADLINES_COLLECTION)
                .aggregate(vec![
                    doc! { "$match": { "status": "SCHEDULED", "dueAt": { "$lte": date(now_ms) } } },
                    doc! { "$group": { "_id": "$name", "n": { "$sum": 1 } } },
                ])
                .await
                .store()?
                .try_collect()
                .await
                .store()?;
            Ok(counts(groups))
        })
    }
}

impl FailureStore for MongoSagaStore {
    fn find<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaFailureDocument>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .failures()
                .find_one(doc! { "_id": id })
                .session(session(tx)?)
                .await
                .store()?
                .map(SagaFailureDocument::from))
        })
    }

    fn save<'a>(
        &'a self,
        tx: &'a mut Tx,
        failure: &'a SagaFailureDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.failures()
                .replace_one(doc! { "_id": &failure.id }, FailureDoc::from(failure))
                .upsert(true)
                .session(session(tx)?)
                .await
                .store()?;
            Ok(())
        })
    }

    fn delete<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let result = self
                .failures()
                .delete_one(doc! { "_id": id })
                .session(session(tx)?)
                .await
                .store()?;
            Ok(result.deleted_count == 1)
        })
    }

    fn failed_again<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        error: &'a str,
        attempts: i32,
        now: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let result = self
                .failures()
                .update_one(
                    doc! { "_id": id },
                    doc! {
                        "$inc": { "attempts": attempts },
                        "$set": { "error": error, "lastFailedAt": date(now) },
                    },
                )
                .session(session(tx)?)
                .await
                .store()?;
            Ok(result.matched_count == 1)
        })
    }

    fn list<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<SagaFailureDocument>, StoreError>> {
        Box::pin(async move {
            let filter = match saga_type {
                Some(t) => doc! { "sagaType": t },
                None => doc! {},
            };
            let session = session(tx)?;
            let mut cursor = self
                .failures()
                .find(filter)
                .sort(doc! { "lastFailedAt": 1 })
                .session(&mut *session)
                .await
                .store()?;
            let found: Vec<FailureDoc> = cursor.stream(session).try_collect().await.store()?;
            Ok(found.into_iter().map(SagaFailureDocument::from).collect())
        })
    }
}

impl MarkerStore for MongoSagaStore {
    fn exists<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            Ok(self
                .markers()
                .find_one(doc! { "_id": id })
                .session(session(tx)?)
                .await
                .store()?
                .is_some())
        })
    }

    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        type_name: &'a str,
        now: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.markers()
                .insert_one(doc! { "_id": id, "type": type_name, "processedAt": date(now) })
                .session(session(tx)?)
                .await
                .store()?;
            Ok(())
        })
    }
}

/// Pending: neither dispatched nor failed (a missing field is null).
fn pending() -> Document {
    doc! { "dispatchedAt": null, "failedAt": null }
}

impl SagaDispatchStore for MongoSagaStore {
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        entries: &'a [SagaDispatchDocument],
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            // What the outbox would refuse, refused now: the saga's attempt fails, nothing commits.
            for e in entries.iter().filter(|e| e.kind == DispatchKind::Event) {
                crate::outbox::payload_document(&e.type_name, e.payload.as_bytes())?;
            }
            if !entries.is_empty() {
                self.dispatch()
                    .insert_many(entries.iter().map(DispatchDoc::from))
                    .session(session(tx)?)
                    .await
                    .store()?;
            }
            Ok(())
        })
    }

    fn due_sagas<'a>(
        &'a self,
        now_ms: i64,
        except: &'a [String],
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async move {
            let mut first = pending();
            first.insert("sagaId", doc! { "$nin": except.to_vec() });
            let pipeline = vec![
                doc! { "$match": first },
                doc! { "$sort": { "sagaId": 1, "seq": 1 } },
                doc! { "$group": {
                    "_id": "$sagaId",
                    "next": { "$first": "$nextAttemptAt" },
                    "since": { "$first": "$createdAt" },
                } },
                doc! { "$match": { "$or": [ { "next": null }, { "next": { "$lte": date(now_ms) } } ] } },
                // Oldest work first: the first pending entry of each saga, by when it was recorded.
                doc! { "$sort": { "since": 1, "_id": 1 } },
                doc! { "$limit": i64::try_from(limit).unwrap_or(i64::MAX) },
            ];
            let docs: Vec<Document> = self
                .db
                .collection::<Document>(SAGA_DISPATCH_COLLECTION)
                .aggregate(pipeline)
                .allow_disk_use(true)
                .await
                .store()?
                .try_collect()
                .await
                .store()?;
            Ok(docs
                .into_iter()
                .filter_map(|d| d.get_str("_id").ok().map(str::to_string))
                .collect())
        })
    }

    fn next_pending<'a>(
        &'a self,
        saga_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>> {
        Box::pin(async move {
            let mut filter = pending();
            filter.insert("sagaId", saga_id);
            Ok(self
                .dispatch()
                .find_one(filter)
                .sort(doc! { "seq": 1 })
                .await
                .store()?
                .map(SagaDispatchDocument::from))
        })
    }

    fn mark_dispatched<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.dispatch()
                .update_one(
                    doc! { "_id": id },
                    doc! { "$set": { "dispatchedAt": date(now_ms) } },
                )
                .session(session(tx)?)
                .await
                .store()?;
            Ok(())
        })
    }

    fn schedule_retry<'a>(
        &'a self,
        id: &'a str,
        attempts: i32,
        next_attempt_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.set_on_entry(
            id,
            doc! { "$set": { "attempts": attempts, "nextAttemptAt": date(next_attempt_at_ms) } },
        ))
    }

    fn mark_failed<'a>(
        &'a self,
        id: &'a str,
        failure: &'a DispatchFailure,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let reason = serde_json::to_value(failure.reason)?
                .as_str()
                .unwrap_or_default()
                .to_string();
            let mut set = doc! {
                "failedAt": date(failure.failed_at_ms),
                "failureReason": reason,
                "failureMessage": &failure.message,
            };
            if let Some(attempts) = failure.attempts {
                set.insert("attempts", attempts);
            }
            if let Some(at) = failure.notice_at_ms {
                set.insert("noticePending", true);
                set.insert("noticeAttempts", 0);
                set.insert("noticeNextAt", date(at));
            }
            self.set_on_entry(id, doc! { "$set": set }).await
        })
    }

    fn claim_due_notice<'a>(
        &'a self,
        now_ms: i64,
        reserve_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>> {
        Box::pin(async move {
            let now = date(now_ms);
            Ok(self
                .dispatch()
                .find_one_and_update(
                    doc! {
                        "noticePending": true,
                        "$or": [ { "noticeNextAt": null }, { "noticeNextAt": { "$lte": now } } ],
                    },
                    doc! { "$set": { "noticeNextAt": date(reserve_until_ms) } },
                )
                .sort(doc! { "noticeNextAt": 1 })
                .return_document(ReturnDocument::After)
                .await
                .store()?
                .map(SagaDispatchDocument::from))
        })
    }

    fn clear_notice<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.set_on_entry(
            id,
            doc! { "$unset": { "noticePending": "", "noticeAttempts": "", "noticeNextAt": "" } },
        ))
    }

    fn schedule_notice_retry<'a>(
        &'a self,
        id: &'a str,
        notice_attempts: i32,
        next_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.set_on_entry(
            id,
            doc! { "$set": { "noticeAttempts": notice_attempts, "noticeNextAt": date(next_at_ms) } },
        ))
    }

    /// An upsert on `{_id, until < now}`: it fails on the duplicate `_id` while another holds it.
    fn claim_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        now_ms: i64,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            match self
                .leases()
                .find_one_and_update(
                    doc! { "_id": saga_id, "until": { "$lt": date(now_ms) } },
                    doc! { "$set": { "owner": owner, "until": date(until_ms) } },
                )
                .upsert(true)
                .return_document(ReturnDocument::After)
                .await
            {
                Ok(_) => Ok(true),
                Err(e) if is_duplicate_key(&e) => Ok(false),
                Err(e) => Err(crate::store_error(e)),
            }
        })
    }

    fn renew_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let result = self
                .leases()
                .update_one(
                    doc! { "_id": saga_id, "owner": owner },
                    doc! { "$set": { "until": date(until_ms) } },
                )
                .await
                .store()?;
            Ok(result.matched_count == 1)
        })
    }

    fn release_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.leases()
                .delete_one(doc! { "_id": saga_id, "owner": owner })
                .await
                .store()?;
            Ok(())
        })
    }

    fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move { self.dispatch().count_documents(pending()).await.store() })
    }
}
