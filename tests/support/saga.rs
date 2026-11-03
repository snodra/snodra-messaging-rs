//! Shared helpers of the saga integration tests: a MongoDB with the saga collections and indexes,
//! a clock the test moves, and small readers over the collections.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use kraken_messaging::MessageContext;
use kraken_messaging::saga::store::{
    DeadlineDocument, SagaDispatchDocument, SagaDocument, SagaFailureDocument,
};
use kraken_messaging::saga::{ManualClock, SagaPorts, ensure_saga_indexes};
use mongodb::bson::{Document, doc};
use mongodb::{Client, Database};
use serde::de::DeserializeOwned;

use super::containers::{self, MongoDb};

/// 2026-11-03T10:00:00Z.
pub fn t0() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_793_700_000)
}

pub struct Store {
    pub client: Client,
    pub db: Database,
    pub clock: ManualClock,
    _mongo: MongoDb,
}

impl Store {
    pub async fn start() -> Store {
        let mongo = containers::mongo().await;
        let client = Client::with_uri_str(&mongo.uri).await.unwrap();
        let db = client.default_database().unwrap();
        ensure_saga_indexes(
            &db,
            Duration::from_secs(30 * 86_400),
            Duration::from_secs(7 * 86_400),
            Duration::from_secs(7 * 86_400),
        )
        .await
        .unwrap();
        // Transactions need the collections to exist.
        for name in ["processed_events", "saga_failures", "outbox"] {
            let _ = db.create_collection(name).await;
        }
        Store {
            client,
            db,
            clock: ManualClock::new(t0()),
            _mongo: mongo,
        }
    }

    pub fn ports(&self) -> SagaPorts {
        SagaPorts::mongo(self.client.clone(), self.db.clone())
    }

    pub async fn all<T: DeserializeOwned + Send + Sync + Unpin>(&self, collection: &str) -> Vec<T> {
        self.find(collection, doc! {}).await
    }

    pub async fn find<T: DeserializeOwned + Send + Sync + Unpin>(
        &self,
        collection: &str,
        filter: Document,
    ) -> Vec<T> {
        self.db
            .collection::<T>(collection)
            .find(filter)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }

    pub async fn count(&self, collection: &str, filter: Document) -> u64 {
        self.db
            .collection::<Document>(collection)
            .count_documents(filter)
            .await
            .unwrap()
    }

    pub async fn marker(&self, id: &str) -> bool {
        self.count("processed_events", doc! { "_id": id }).await == 1
    }

    pub async fn clear(&self) {
        for name in [
            "sagas",
            "saga_dispatch",
            "saga_dispatch_leases",
            "deadlines",
            "saga_failures",
            "processed_events",
            "outbox",
        ] {
            self.db
                .collection::<Document>(name)
                .delete_many(doc! {})
                .await
                .unwrap();
        }
    }

    pub async fn sagas(&self, type_name: &str) -> Vec<SagaDocument> {
        self.find("sagas", doc! { "type": type_name }).await
    }

    pub async fn dispatch_of(&self, saga_id: &str) -> Vec<SagaDispatchDocument> {
        let mut entries: Vec<SagaDispatchDocument> =
            self.find("saga_dispatch", doc! { "sagaId": saga_id }).await;
        entries.sort_by_key(|e| e.seq);
        entries
    }

    pub async fn deadlines(&self) -> Vec<DeadlineDocument> {
        self.all("deadlines").await
    }

    pub async fn failures(&self) -> Vec<SagaFailureDocument> {
        self.all("saga_failures").await
    }
}

/// The message context of an event as the dispatcher builds it: `id` and `type` headers, the
/// correlation id `corr-<id>`.
pub fn event_ctx(id: &str, type_name: &str, extra: &[(&str, &str)]) -> MessageContext {
    let mut headers: HashMap<String, String> = extra
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    headers.insert("id".into(), id.into());
    headers.insert("type".into(), type_name.into());
    MessageContext {
        message_id: id.into(),
        type_name: type_name.into(),
        correlation_id: format!("corr-{id}"),
        headers,
        ..Default::default()
    }
}

/// The state of a stored saga as JSON, for assertions (it is stored as text).
pub fn state(saga: &SagaDocument) -> serde_json::Value {
    serde_json::from_str(&saga.state).unwrap()
}
