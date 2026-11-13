//! The MongoDB message store (ADR 0028): every store of `kraken_messaging::store` over the
//! service's own MongoDB database, in the collections and with the document fields the library
//! has always written (`outbox`, `processed_events`, `dead_letters`, `processor_control`,
//! `external_calls`, `sagas`, `saga_dispatch`, `deadlines`, `saga_failures`), so existing data
//! stays readable without a migration. Transactions are MongoDB sessions: they need a replica set.
//!
//! ```ignore
//! let app = KrakenMessaging::builder(config).await?
//!     .store(Arc::new(MongoStore::new(client, db))) /* ... */;
//! ```
//!
//! `start()` creates the indexes ([`MessageStore::init_schema`], with the retentions of the
//! `Config`) unless `KRAKEN_STORE_INIT_SCHEMA=false`; [`MongoStore::ensure_indexes`] does it by hand.
//!
//! A handler that writes its own documents in the transaction of the event it handles gets the
//! session with [`MongoTxExt::session`] (`ctx.tx.lock().await.session()`).
//!
//! Times a store stamps itself (an outbox entry's `sentAt`, `failedAt` and lease) are the
//! application's clock (the builder's, [`MessageStore::use_clock`]; the system clock until then),
//! never the database's.

use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;
use kraken_messaging::Config;
use kraken_messaging::error::MessagingError;
use kraken_messaging::saga::Clock;
use kraken_messaging::store::{
    DeadLetterStore, ExternalCallStore, MessageStore, OutboxStore, ProcessedEventStore,
    ProcessorControlStore, SagaStores, StoreError, Tx, TxProvider,
};
use mongodb::bson::DateTime;
use mongodb::error::{ErrorKind, TRANSIENT_TRANSACTION_ERROR, WriteFailure};
use mongodb::{Client, ClientSession, Database};

mod control;
mod dead_letters;
#[doc(hidden)]
pub mod documents;
mod external;
mod indexes;
mod outbox;
mod processed;
mod sagas;

pub use indexes::MongoIndexSettings;

/// The MongoDB message store: see the crate docs.
#[derive(Clone)]
pub struct MongoStore {
    db: Database,
    /// What the outbox stamps its times with.
    clock: Arc<RwLock<Clock>>,
    tx: Arc<MongoTx>,
    outbox: Arc<outbox::MongoOutboxStore>,
    processed: Arc<processed::MongoProcessedEventStore>,
    dead_letters: Arc<dead_letters::MongoDeadLetterStore>,
    control: Arc<control::MongoProcessorControlStore>,
    external: Arc<external::MongoExternalCallStore>,
    sagas: Arc<sagas::MongoSagaStore>,
}

impl MongoStore {
    /// The stores over `db`; `client` (the one `db` comes from) starts the sessions.
    pub fn new(client: Client, db: Database) -> MongoStore {
        let clock = Arc::new(RwLock::new(Clock::system()));
        MongoStore {
            tx: Arc::new(MongoTx { client }),
            outbox: Arc::new(outbox::MongoOutboxStore::new(&db, Arc::clone(&clock))),
            clock,
            processed: Arc::new(processed::MongoProcessedEventStore::new(&db)),
            dead_letters: Arc::new(dead_letters::MongoDeadLetterStore::new(&db)),
            control: Arc::new(control::MongoProcessorControlStore::new(&db)),
            external: Arc::new(external::MongoExternalCallStore::new(&db)),
            sagas: Arc::new(sagas::MongoSagaStore::new(&db)),
            db,
        }
    }

    /// Creates the indexes of every collection (idempotent): the outbox relay index, the dead
    /// letters' three, the saga indexes and the TTL indexes of `settings`.
    pub async fn ensure_indexes(&self, settings: &MongoIndexSettings) -> Result<(), StoreError> {
        indexes::ensure_indexes(&self.db, settings).await
    }
}

impl MessageStore for MongoStore {
    fn name(&self) -> &'static str {
        "mongo"
    }

    /// The indexes of every collection ([`ensure_indexes`](MongoStore::ensure_indexes)) with the
    /// retentions of `config`; creating them creates every collection a transaction writes to
    /// (older servers create none inside one).
    fn init_schema<'a>(&'a self, config: &'a Config) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move { self.ensure_indexes(&MongoIndexSettings::from(config)).await })
    }

    fn use_clock(&self, clock: Clock) {
        *self
            .clock
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = clock;
    }

    fn tx(&self) -> Arc<dyn TxProvider> {
        self.tx.clone()
    }

    fn outbox(&self) -> Arc<dyn OutboxStore> {
        self.outbox.clone()
    }

    fn processed_events(&self) -> Arc<dyn ProcessedEventStore> {
        self.processed.clone()
    }

    fn dead_letters(&self) -> Arc<dyn DeadLetterStore> {
        self.dead_letters.clone()
    }

    fn processor_control(&self) -> Arc<dyn ProcessorControlStore> {
        self.control.clone()
    }

    fn external_calls(&self) -> Arc<dyn ExternalCallStore> {
        self.external.clone()
    }

    fn sagas(&self) -> SagaStores {
        SagaStores {
            sagas: self.sagas.clone(),
            dispatch: self.sagas.clone(),
            deadlines: self.sagas.clone(),
            failures: self.sagas.clone(),
            markers: self.sagas.clone(),
        }
    }
}

/// The session of a [`Tx`] of the MongoDB store, for a handler's own writes in its transaction.
pub trait MongoTxExt {
    /// The session.
    ///
    /// # Panics
    ///
    /// When the transaction is not the MongoDB store's (another store's, or [`Tx::none`]).
    fn session(&mut self) -> &mut ClientSession;
}

impl MongoTxExt for Tx {
    fn session(&mut self) -> &mut ClientSession {
        self.get_mut::<ClientSession>()
            .expect("the transaction is not the MongoDB store's: it holds no session")
    }
}

/// Transactions are sessions of `client`.
struct MongoTx {
    client: Client,
}

impl TxProvider for MongoTx {
    fn begin(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(async move {
            let mut session = self.client.start_session().await.store()?;
            session.start_transaction().await.store()?;
            Ok(Tx::new(session))
        })
    }

    fn plain(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(async move { Ok(Tx::new(self.client.start_session().await.store()?)) })
    }

    fn commit<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move { session(tx)?.commit_transaction().await.store() })
    }

    fn abort<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(session) = tx.get_mut::<ClientSession>() {
                let _ = session.abort_transaction().await;
            }
        })
    }
}

/// The session of `tx`; an error when it holds none (another store's transaction).
pub(crate) fn session(tx: &mut Tx) -> Result<&mut ClientSession, StoreError> {
    tx.get_mut::<ClientSession>().ok_or_else(|| {
        StoreError::Failed(MessagingError::Storage(
            "the MongoDB store was given a transaction that holds no session".into(),
        ))
    })
}

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

/// A driver error as a store error: a duplicate key, a write conflict or a transient transaction
/// error is a [`StoreError::Conflict`] (a retry of the whole attempt resolves it).
pub(crate) fn store_error(e: mongodb::error::Error) -> StoreError {
    let codes = error_codes(&e);
    if e.contains_label(TRANSIENT_TRANSACTION_ERROR)
        || codes.contains(&WRITE_CONFLICT)
        || codes.contains(&DUPLICATE_KEY)
    {
        StoreError::Conflict(e.to_string())
    } else {
        StoreError::Failed(MessagingError::Storage(e.to_string()))
    }
}

/// `.store()?` on a driver result.
pub(crate) trait OrStoreError<T> {
    fn store(self) -> Result<T, StoreError>;
}

impl<T> OrStoreError<T> for Result<T, mongodb::error::Error> {
    fn store(self) -> Result<T, StoreError> {
        self.map_err(store_error)
    }
}

/// Epoch milliseconds as a BSON date.
pub(crate) fn date(ms: i64) -> DateTime {
    DateTime::from_millis(ms)
}
