//! The persistence the saga runtime needs, as small ports: the MongoDB implementations below are
//! the default, and the test fixture (Task 14) supplies in-memory ones. Every method takes the
//! [`Tx`] of the attempt it runs in, so the runtime decides what commits together; an in-memory
//! implementation ignores it ([`Tx::none`]). [`Tx`], [`TxProvider`] and [`StoreError`] are the
//! message store's ([`crate::store`]), which provides these ports too.
//!
//! A port reports a [`StoreError`]: a *conflict* (a duplicate key, a Mongo write conflict, a
//! transient transaction error: the kind a retry resolves) apart from any other failure.

use std::sync::Arc;

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{DateTime, Document, doc};
use mongodb::error::{ErrorKind, TRANSIENT_TRANSACTION_ERROR, WriteFailure};
use mongodb::{Client, ClientSession, Database};

use super::store::{
    DEADLINES_COLLECTION, DeadlineDocument, SAGA_DISPATCH_COLLECTION, SAGA_FAILURES_COLLECTION,
    SAGAS_COLLECTION, SagaDispatchDocument, SagaDocument, SagaFailureDocument,
};
use crate::error::MessagingError;
use crate::kafka::PROCESSED_EVENTS_COLLECTION;
use crate::store::{StoreError, Tx, TxProvider};

const WRITE_CONFLICT: i32 = 112;
const DUPLICATE_KEY: i32 = 11000;

/// The server error codes a driver error carries (command, write and bulk write errors).
fn error_codes(e: &mongodb::error::Error) -> Vec<i32> {
    match &*e.kind {
        ErrorKind::Command(c) => vec![c.code],
        ErrorKind::Write(WriteFailure::WriteError(w)) => vec![w.code],
        ErrorKind::InsertMany(m) => m
            .write_errors
            .iter()
            .flatten()
            .map(|w| w.code)
            .chain(m.write_concern_error.iter().map(|w| w.code))
            .collect(),
        _ => Vec::new(),
    }
}

/// Whether the driver error is a duplicate key.
pub(crate) fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    error_codes(e).contains(&DUPLICATE_KEY)
}

impl From<mongodb::error::Error> for StoreError {
    fn from(e: mongodb::error::Error) -> Self {
        let codes = error_codes(&e);
        if e.contains_label(TRANSIENT_TRANSACTION_ERROR)
            || codes.contains(&WRITE_CONFLICT)
            || codes.contains(&DUPLICATE_KEY)
        {
            StoreError::Conflict(e.to_string())
        } else {
            StoreError::Failed(MessagingError::from(e))
        }
    }
}

/// Transitional (O0-3 removes it with the MongoDB code of the core; the MongoDB add-on's
/// `MongoTxExt` replaces it): the [`Tx`] of a Mongo session, as the saga code builds and reads it.
impl Tx {
    pub fn from_session(session: ClientSession) -> Tx {
        Tx::new(session)
    }

    /// The session of this transaction; an error for [`Tx::none`] (a Mongo port was given an
    /// in-memory transaction).
    pub fn session(&mut self) -> Result<&mut ClientSession, StoreError> {
        self.get_mut::<ClientSession>().ok_or_else(|| {
            StoreError::Failed(MessagingError::Storage(
                "a MongoDB saga port needs a transaction with a session".into(),
            ))
        })
    }
}

/// The `sagas` collection.
pub trait SagaRepository: Send + Sync {
    fn find_by_id<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDocument>, StoreError>>;

    /// The live (not ended) instances of `type_name` associated with `key = value`.
    fn find_live<'a>(
        &'a self,
        tx: &'a mut Tx,
        type_name: &'a str,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<Vec<SagaDocument>, StoreError>>;

    /// Inserts a new instance; a [`StoreError::Conflict`] when its `startKey` is held by a live
    /// instance.
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Replaces the stored instance by `saga` when the stored `version` is `expected_version`;
    /// false when it is not.
    fn update<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
        expected_version: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;
}

/// The saga-side outbox `saga_dispatch`: where the runtime records what a saga sends (the relay
/// reads it).
pub trait DispatchRecorder: Send + Sync {
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        entries: &'a [SagaDispatchDocument],
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}

/// The `deadlines` collection, as far as the runtime writes it.
pub trait DeadlineStore: Send + Sync {
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        deadline: &'a DeadlineDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// The deadline `id` when it is still SCHEDULED (not fired, not cancelled), else `None`.
    fn find_scheduled<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>>;

    /// Cancels the SCHEDULED deadline `deadline_id` of `saga_id`; no effect on any other.
    fn cancel<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        deadline_id: &'a str,
        now: DateTime,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Cancels the SCHEDULED deadlines of `saga_id` named `name`.
    fn cancel_by_name<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        name: &'a str,
        now: DateTime,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Cancels every SCHEDULED deadline of `saga_id`.
    fn cancel_all<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        now: DateTime,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Sets the SCHEDULED deadline `id` to FIRED; false when it was not SCHEDULED.
    fn mark_fired<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now: DateTime,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;
}

/// The `saga_failures` collection: parked events.
pub trait FailureStore: Send + Sync {
    fn find<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaFailureDocument>, StoreError>>;

    /// Inserts or replaces the failure with that id.
    fn save<'a>(
        &'a self,
        tx: &'a mut Tx,
        failure: &'a SagaFailureDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Deletes the failure; false when there was none.
    fn delete<'a>(&'a self, tx: &'a mut Tx, id: &'a str)
    -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Records that a retry of the failure `id` failed again: `attempts` grows by `attempts`,
    /// `error` and `lastFailedAt` are replaced. **Never inserts**: false when the failure no longer
    /// exists (a `discard` or a concurrent retry got there first), so the bookkeeping of a failed
    /// retry cannot bring a discarded failure back (ruling 36).
    fn failed_again<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        error: &'a str,
        attempts: i32,
        now: DateTime,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// The failures of `saga_type` (every type when `None`), oldest `lastFailedAt` first.
    fn list<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<SagaFailureDocument>, StoreError>>;
}

/// The exactly-once markers of the saga stage: `processed_events` ids `saga:<sagaType>:<eventId>`.
pub trait MarkerStore: Send + Sync {
    fn exists<'a>(&'a self, tx: &'a mut Tx, id: &'a str)
    -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Inserts the marker; a [`StoreError::Conflict`] when it exists.
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        type_name: &'a str,
        now: DateTime,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}

/// The six ports together.
#[derive(Clone)]
pub struct SagaPorts {
    pub tx: Arc<dyn TxProvider>,
    pub sagas: Arc<dyn SagaRepository>,
    pub dispatch: Arc<dyn DispatchRecorder>,
    pub deadlines: Arc<dyn DeadlineStore>,
    pub failures: Arc<dyn FailureStore>,
    pub markers: Arc<dyn MarkerStore>,
}

impl SagaPorts {
    /// The MongoDB implementations of all of them.
    pub fn mongo(client: Client, db: Database) -> SagaPorts {
        let mongo = Arc::new(MongoSagaStore { client, db });
        SagaPorts {
            tx: mongo.clone(),
            sagas: mongo.clone(),
            dispatch: mongo.clone(),
            deadlines: mongo.clone(),
            failures: mongo.clone(),
            markers: mongo,
        }
    }
}

/// The MongoDB implementation of every port.
pub struct MongoSagaStore {
    client: Client,
    db: Database,
}

impl MongoSagaStore {
    fn sagas(&self) -> mongodb::Collection<SagaDocument> {
        self.db.collection(SAGAS_COLLECTION)
    }

    fn dispatch(&self) -> mongodb::Collection<SagaDispatchDocument> {
        self.db.collection(SAGA_DISPATCH_COLLECTION)
    }

    fn deadlines(&self) -> mongodb::Collection<DeadlineDocument> {
        self.db.collection(DEADLINES_COLLECTION)
    }

    fn failures(&self) -> mongodb::Collection<SagaFailureDocument> {
        self.db.collection(SAGA_FAILURES_COLLECTION)
    }

    fn markers(&self) -> mongodb::Collection<Document> {
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
            .session(tx.session()?)
            .await?;
        Ok(())
    }
}

impl TxProvider for MongoSagaStore {
    fn begin(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(async move {
            let mut session = self.client.start_session().await?;
            session.start_transaction().await?;
            Ok(Tx::from_session(session))
        })
    }

    fn plain(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(async move { Ok(Tx::from_session(self.client.start_session().await?)) })
    }

    fn commit<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            tx.session()?.commit_transaction().await?;
            Ok(())
        })
    }

    fn abort<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(session) = tx.get_mut::<ClientSession>() {
                let _ = session.abort_transaction().await;
            }
        })
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
                .session(tx.session()?)
                .await?)
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
            let session = tx.session()?;
            let mut cursor = self
                .sagas()
                .find(doc! {
                    "type": type_name,
                    "ended": false,
                    "associations": { "$elemMatch": { "k": key, "v": value } },
                })
                .session(&mut *session)
                .await?;
            Ok(cursor.stream(session).try_collect().await?)
        })
    }

    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.sagas().insert_one(saga).session(tx.session()?).await?;
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
                .session(tx.session()?)
                .await?;
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
                    .session(tx.session()?)
                    .await?;
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
                .session(tx.session()?)
                .await?;
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
                .session(tx.session()?)
                .await?)
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
                .session(tx.session()?)
                .await?;
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
            Ok(self
                .failures()
                .find_one(doc! { "_id": id })
                .session(tx.session()?)
                .await?)
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
                .session(tx.session()?)
                .await?;
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
                .session(tx.session()?)
                .await?;
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
                .session(tx.session()?)
                .await?;
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
            let session = tx.session()?;
            let mut cursor = self
                .failures()
                .find(filter)
                .sort(doc! { "lastFailedAt": 1 })
                .session(&mut *session)
                .await?;
            Ok(cursor.stream(session).try_collect().await?)
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
                .session(tx.session()?)
                .await?
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
                .session(tx.session()?)
                .await?;
            Ok(())
        })
    }
}
