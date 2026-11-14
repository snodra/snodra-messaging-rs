//! Shared helpers of the saga integration tests: the in-memory message store (no container: the
//! saga runtime, relay and scheduler need a store, not a database; the MongoDB layout is the
//! store kit's and `tests/saga_store.rs`'s), a clock the test moves, and readers over what the
//! store holds.
#![allow(dead_code)]

use std::collections::HashMap;
#[cfg(feature = "testing")]
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kraken_messaging::MessageContext;
use kraken_messaging::saga::store::SagaDocument;
#[cfg(feature = "testing")]
use kraken_messaging::saga::store::{DeadlineDocument, SagaDispatchDocument, SagaFailureDocument};
#[cfg(feature = "testing")]
use kraken_messaging::saga::{ManualClock, SagaPorts};
#[cfg(feature = "testing")]
use kraken_messaging::store::memory::{MemoryContents, MemoryStore, OutboxRecord};
#[cfg(feature = "testing")]
use kraken_messaging::store::{ExternalCall, MessageStore, Tx};

/// 2026-11-03T10:00:00Z.
pub fn t0() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_793_700_000)
}

/// The store of a saga test (feature `testing`: it reads [`MemoryStore::contents`]): the
/// in-memory message store, or MongoDB ([`Store::start_mongo`]) for the tests that race real
/// transactions of the MongoDB add-on.
#[cfg(feature = "testing")]
pub struct Store {
    /// The in-memory message store (unused on MongoDB).
    pub store: Arc<MemoryStore>,
    pub clock: ManualClock,
    mongo: Option<Mongo>,
}

/// The MongoDB side of a [`Store`]: the add-on's store over a database of its own container.
#[cfg(feature = "testing")]
struct Mongo {
    store: Arc<kraken_messaging_mongo::MongoStore>,
    db: mongodb::Database,
    _container: super::containers::MongoDb,
}

#[cfg(feature = "testing")]
impl Store {
    pub async fn start() -> Store {
        // The store stamps the outbox with its own clock (the wall clock, as a service's
        // does): the manual clock is the saga runtime's, relay's and scheduler's.
        Store {
            store: Arc::new(MemoryStore::new()),
            clock: ManualClock::new(t0()),
            mongo: None,
        }
    }

    /// The MongoDB add-on's store (needs Docker), its indexes in place. The readers below that
    /// tests on MongoDB use: `sagas`, `dispatch_of`, `failures`.
    pub async fn start_mongo() -> Store {
        let container = super::containers::mongo().await;
        let client = mongodb::Client::with_uri_str(&container.uri).await.unwrap();
        let db = client.default_database().unwrap();
        let store = kraken_messaging_mongo::MongoStore::new(client, db.clone());
        store
            .init_schema(&kraken_messaging::Config::new("saga-tests", "127.0.0.1:1"))
            .await
            .unwrap();
        Store {
            store: Arc::new(MemoryStore::new()),
            clock: ManualClock::new(t0()),
            mongo: Some(Mongo {
                store: Arc::new(store),
                db,
                _container: container,
            }),
        }
    }

    pub fn ports(&self) -> SagaPorts {
        match &self.mongo {
            Some(mongo) => SagaPorts::of(&*mongo.store),
            None => SagaPorts::of(&*self.store),
        }
    }

    /// The store as the core takes it.
    pub fn message_store(&self) -> Arc<dyn MessageStore> {
        match &self.mongo {
            Some(mongo) => mongo.store.clone(),
            None => self.store.clone(),
        }
    }

    /// The documents of `collection` on MongoDB, as the add-on's document type `D`.
    async fn mongo_find<D>(
        &self,
        collection: &str,
        filter: mongodb::bson::Document,
    ) -> Option<Vec<D>>
    where
        D: serde::de::DeserializeOwned + Send + Sync,
    {
        use futures::TryStreamExt;
        let mongo = self.mongo.as_ref()?;
        Some(
            mongo
                .db
                .collection::<D>(collection)
                .find(filter)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap(),
        )
    }

    /// What the store holds (the in-memory store only).
    pub fn contents(&self) -> MemoryContents {
        assert!(self.mongo.is_none(), "contents() reads the in-memory store");
        self.store.contents()
    }

    /// Changes what the store holds behind the library's back.
    pub fn edit(&self, change: impl FnOnce(&mut MemoryContents)) {
        self.store.edit(change);
    }

    /// Writes `deadline` as the store does, outside any saga.
    pub async fn insert_deadline(&self, deadline: &DeadlineDocument) {
        self.ports()
            .deadlines
            .insert(&mut Tx::none(), deadline)
            .await
            .unwrap();
    }

    pub async fn marker(&self, id: &str) -> bool {
        self.contents().processed.iter().any(|m| m.id == id)
    }

    pub async fn sagas(&self, type_name: &str) -> Vec<SagaDocument> {
        use kraken_messaging_mongo::documents::SagaDoc;
        if let Some(found) = self
            .mongo_find::<SagaDoc>("sagas", mongodb::bson::doc! { "type": type_name })
            .await
        {
            return found.into_iter().map(SagaDocument::from).collect();
        }
        let mut found: Vec<SagaDocument> = self
            .contents()
            .sagas
            .into_iter()
            .filter(|s| s.type_name == type_name)
            .collect();
        found.sort_by_key(|s| s.created_at_ms);
        found
    }

    /// Every saga dispatch entry, by saga and seq.
    pub async fn dispatch(&self) -> Vec<SagaDispatchDocument> {
        let mut entries = self.contents().dispatch;
        entries.sort_by(|a, b| (&a.saga_id, a.seq).cmp(&(&b.saga_id, b.seq)));
        entries
    }

    pub async fn dispatch_of(&self, saga_id: &str) -> Vec<SagaDispatchDocument> {
        use kraken_messaging_mongo::documents::DispatchDoc;
        if let Some(found) = self
            .mongo_find::<DispatchDoc>("saga_dispatch", mongodb::bson::doc! { "sagaId": saga_id })
            .await
        {
            let mut entries: Vec<SagaDispatchDocument> =
                found.into_iter().map(SagaDispatchDocument::from).collect();
            entries.sort_by_key(|e| e.seq);
            return entries;
        }
        let mut entries: Vec<SagaDispatchDocument> = self
            .contents()
            .dispatch
            .into_iter()
            .filter(|e| e.saga_id == saga_id)
            .collect();
        entries.sort_by_key(|e| e.seq);
        entries
    }

    pub async fn deadlines(&self) -> Vec<DeadlineDocument> {
        self.contents().deadlines
    }

    pub async fn failures(&self) -> Vec<SagaFailureDocument> {
        use kraken_messaging_mongo::documents::FailureDoc;
        if let Some(found) = self
            .mongo_find::<FailureDoc>("saga_failures", mongodb::bson::doc! {})
            .await
        {
            return found.into_iter().map(SagaFailureDocument::from).collect();
        }
        self.contents().failures
    }

    /// The outbox entries, in `(created_at, seq)` order.
    pub async fn outbox(&self) -> Vec<OutboxRecord> {
        let mut entries = self.contents().outbox;
        entries.sort_by_key(|r| (r.entry.created_at_ms, r.entry.seq));
        entries
    }

    pub async fn external_calls(&self) -> Vec<ExternalCall> {
        self.contents().external_calls
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
