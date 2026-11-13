//! The saga collections (ADR 0021's storage contract, field for field): `sagas`, `saga_dispatch`,
//! `deadlines`, `saga_failures`, and the saga stage's markers in `processed_events`. Every port
//! joins the runtime's transaction (the [`Tx`]'s session).

use futures::TryStreamExt;
use futures::future::BoxFuture;
use kraken_messaging::saga::store::{
    DeadlineDocument, SagaDispatchDocument, SagaDocument, SagaFailureDocument,
};
use kraken_messaging::store::{
    DeadlineStore, DispatchRecorder, FailureStore, MarkerStore, SagaRepository, StoreError, Tx,
};
use mongodb::bson::{DateTime, Document, doc};
use mongodb::{Collection, Database};

use crate::processed::PROCESSED_EVENTS_COLLECTION;
use crate::{OrStoreError, session};

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

    fn sagas(&self) -> Collection<SagaDocument> {
        self.db.collection(SAGAS_COLLECTION)
    }

    fn dispatch(&self) -> Collection<SagaDispatchDocument> {
        self.db.collection(SAGA_DISPATCH_COLLECTION)
    }

    fn deadlines(&self) -> Collection<DeadlineDocument> {
        self.db.collection(DEADLINES_COLLECTION)
    }

    fn failures(&self) -> Collection<SagaFailureDocument> {
        self.db.collection(SAGA_FAILURES_COLLECTION)
    }

    fn markers(&self) -> Collection<Document> {
        self.db.collection(PROCESSED_EVENTS_COLLECTION)
    }

    async fn cancel_where(
        &self,
        tx: &mut Tx,
        mut filter: Document,
        now: DateTime,
    ) -> Result<(), StoreError> {
        filter.insert("status", "SCHEDULED");
        self.deadlines()
            .update_many(
                filter,
                doc! { "$set": { "status": "CANCELLED", "finishedAt": now } },
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
            self.sagas()
                .find_one(doc! { "_id": id })
                .session(session(tx)?)
                .await
                .store()
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
            cursor.stream(session).try_collect().await.store()
        })
    }

    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.sagas()
                .insert_one(saga)
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
                .replace_one(doc! { "_id": &saga.id, "version": expected_version }, saga)
                .session(session(tx)?)
                .await
                .store()?;
            Ok(result.matched_count == 1)
        })
    }
}

impl DispatchRecorder for MongoSagaStore {
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        entries: &'a [SagaDispatchDocument],
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if !entries.is_empty() {
                self.dispatch()
                    .insert_many(entries)
                    .session(session(tx)?)
                    .await
                    .store()?;
            }
            Ok(())
        })
    }
}

impl DeadlineStore for MongoSagaStore {
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        deadline: &'a DeadlineDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.deadlines()
                .insert_one(deadline)
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
            self.deadlines()
                .find_one(doc! { "_id": id, "status": "SCHEDULED" })
                .session(session(tx)?)
                .await
                .store()
        })
    }

    fn cancel<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        deadline_id: &'a str,
        now: DateTime,
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
        now: DateTime,
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
        now: DateTime,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move { self.cancel_where(tx, doc! { "sagaId": saga_id }, now).await })
    }

    fn mark_fired<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now: DateTime,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let result = self
                .deadlines()
                .update_one(
                    doc! { "_id": id, "status": "SCHEDULED" },
                    doc! { "$set": { "status": "FIRED", "finishedAt": now } },
                )
                .session(session(tx)?)
                .await
                .store()?;
            Ok(result.modified_count == 1)
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
            self.failures()
                .find_one(doc! { "_id": id })
                .session(session(tx)?)
                .await
                .store()
        })
    }

    fn save<'a>(
        &'a self,
        tx: &'a mut Tx,
        failure: &'a SagaFailureDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.failures()
                .replace_one(doc! { "_id": &failure.id }, failure)
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
        now: DateTime,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let result = self
                .failures()
                .update_one(
                    doc! { "_id": id },
                    doc! {
                        "$inc": { "attempts": attempts },
                        "$set": { "error": error, "lastFailedAt": now },
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
            cursor.stream(session).try_collect().await.store()
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
        now: DateTime,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.markers()
                .insert_one(doc! { "_id": id, "type": type_name, "processedAt": now })
                .session(session(tx)?)
                .await
                .store()?;
            Ok(())
        })
    }
}
