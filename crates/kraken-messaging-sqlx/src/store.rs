//! [`SqlxStore`]: the stores together, their transactions, and [`SqlxTxExt`].

use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;
use kraken_messaging::Config;
use kraken_messaging::saga::Clock;
use kraken_messaging::store::{
    DeadLetterStore, ExternalCallStore, MessageStore, OutboxStore, ProcessedEventStore,
    ProcessorControlStore, SagaStores, StoreError, Tx, TxProvider,
};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "mysql")]
use sqlx::mysql::{MySql, MySqlPool};
#[cfg(feature = "postgres")]
use sqlx::postgres::{PgPool, Postgres};

use crate::SqlxSettings;
use crate::db::{Db, Held, Pool};

/// The SQL message store: see the crate docs. Every clone is the same store.
#[derive(Clone)]
pub struct SqlxStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: Arc<Db>,
    settings: SqlxSettings,
    clock: Arc<RwLock<Clock>>,
    outbox: Arc<crate::outbox::SqlOutboxStore>,
    processed: Arc<crate::outbox::SqlProcessedEventStore>,
    dead_letters: Arc<crate::dead_letters::SqlDeadLetterStore>,
    control: Arc<crate::control::SqlProcessorControlStore>,
    external: Arc<crate::external::SqlExternalCallStore>,
    sagas: Arc<crate::sagas::SqlSagaStore>,
}

impl std::fmt::Debug for SqlxStore {
    /// The dialect and the settings: never the pool's connect options (they hold the password).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlxStore")
            .field("dialect", &self.inner.db.dialect().name())
            .field("settings", &self.inner.settings)
            .finish()
    }
}

impl SqlxStore {
    /// The store on a PostgreSQL pool (at least 2 connections), with the default settings.
    #[cfg(feature = "postgres")]
    pub fn postgres(pool: PgPool) -> SqlxStore {
        Self::build(Pool::Pg(pool), SqlxSettings::default())
    }

    /// The store on a MySQL pool (at least 2 connections), with the default settings.
    #[cfg(feature = "mysql")]
    pub fn mysql(pool: MySqlPool) -> SqlxStore {
        Self::build(Pool::My(pool), SqlxSettings::default())
    }

    /// The same store with `settings` (e.g. [`SqlxSettings::from_env`]).
    pub fn with_settings(self, settings: SqlxSettings) -> SqlxStore {
        let clock = self
            .inner
            .clock
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let store = Self::build(self.inner.db.pool.clone(), settings);
        store.use_clock(clock);
        store
    }

    fn build(pool: Pool, settings: SqlxSettings) -> SqlxStore {
        let clock = Arc::new(RwLock::new(Clock::system()));
        let db = Arc::new(Db::new(pool, Arc::clone(&clock)));
        SqlxStore {
            inner: Arc::new(Inner {
                outbox: Arc::new(crate::outbox::SqlOutboxStore::new(
                    Arc::clone(&db),
                    settings.notify,
                )),
                processed: Arc::new(crate::outbox::SqlProcessedEventStore::new(Arc::clone(&db))),
                dead_letters: Arc::new(crate::dead_letters::SqlDeadLetterStore::new(Arc::clone(
                    &db,
                ))),
                control: Arc::new(crate::control::SqlProcessorControlStore::new(Arc::clone(
                    &db,
                ))),
                external: Arc::new(crate::external::SqlExternalCallStore::new(Arc::clone(&db))),
                sagas: Arc::new(crate::sagas::SqlSagaStore::new(Arc::clone(&db))),
                db,
                settings,
                clock,
            }),
        }
    }

    /// The store's settings.
    pub fn settings(&self) -> &SqlxSettings {
        &self.inner.settings
    }

    /// Checks the server and applies schema version 1 unless the database has it: what
    /// [`MessageStore::init_schema`] does.
    pub async fn apply_schema(&self) -> Result<(), StoreError> {
        crate::schema::verify(&self.inner.db.pool).await?;
        crate::schema::apply(&self.inner.db.pool, self.inner.db.dialect()).await
    }

    /// For a service that applies the schema itself: checks the server, and fails unless the
    /// database has the schema version this library needs, or later.
    pub async fn require_schema(&self) -> Result<(), StoreError> {
        crate::schema::verify(&self.inner.db.pool).await?;
        crate::schema::require(&self.inner.db.pool, self.inner.db.dialect()).await
    }

    /// Deletes what the retentions of `config` let go, the expired dead letters and the dispatch
    /// leases that passed, in batches of 1000 (each its own transaction); how many rows went. The
    /// background task of [`MessageStore::background`] runs it every
    /// [`SqlxSettings::purge_interval`].
    pub async fn purge_expired(&self, config: &Config) -> Result<u64, StoreError> {
        crate::purge::run(&self.inner.db, config).await
    }

    /// How many times the relay wake-up established `LISTEN kraken_outbox` and woke the relays
    /// (tests: a NOTIFY committed after this grew reaches the listener).
    #[doc(hidden)]
    pub fn listens(&self) -> u64 {
        self.inner.outbox.listens()
    }
}

impl MessageStore for SqlxStore {
    fn name(&self) -> &'static str {
        match self.inner.db.dialect() {
            #[cfg(feature = "postgres")]
            crate::db::Dialect::Postgres => "postgres",
            #[cfg(feature = "mysql")]
            crate::db::Dialect::MySql => "mysql",
        }
    }

    /// The server checked, then schema version 1 applied unless the database has it.
    fn init_schema<'a>(&'a self, _config: &'a Config) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.apply_schema())
    }

    /// The server checked, and the schema at version 1 or later.
    fn check_schema(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(self.require_schema())
    }

    /// The purge every [`SqlxSettings::purge_interval`] (none when it is zero).
    fn background(
        &self,
        config: &Config,
        stop: CancellationToken,
    ) -> Option<BoxFuture<'static, ()>> {
        let interval = self.inner.settings.purge_interval;
        if interval.is_zero() {
            return None;
        }
        let (store, config) = (self.clone(), config.clone());
        Some(Box::pin(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(interval) => {}
                }
                match store.purge_expired(&config).await {
                    Ok(0) => {}
                    Ok(n) => tracing::debug!(rows = n, "purged the message store"),
                    Err(e) => tracing::warn!(
                        "the message store purge failed (retried at the next interval): {e}"
                    ),
                }
            }
        }))
    }

    fn use_clock(&self, clock: Clock) {
        *self
            .inner
            .clock
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = clock;
    }

    fn tx(&self) -> Arc<dyn TxProvider> {
        Arc::new(SqlTx {
            db: Arc::clone(&self.inner.db),
        })
    }

    fn outbox(&self) -> Arc<dyn OutboxStore> {
        self.inner.outbox.clone()
    }

    fn processed_events(&self) -> Arc<dyn ProcessedEventStore> {
        self.inner.processed.clone()
    }

    fn dead_letters(&self) -> Arc<dyn DeadLetterStore> {
        self.inner.dead_letters.clone()
    }

    fn processor_control(&self) -> Arc<dyn ProcessorControlStore> {
        self.inner.control.clone()
    }

    fn external_calls(&self) -> Arc<dyn ExternalCallStore> {
        self.inner.external.clone()
    }

    fn sagas(&self) -> SagaStores {
        let sagas = &self.inner.sagas;
        SagaStores {
            sagas: sagas.clone(),
            dispatch: sagas.clone(),
            deadlines: sagas.clone(),
            failures: sagas.clone(),
            markers: sagas.clone(),
        }
    }
}

/// The connection of a [`Tx`] of the SQL store, in its transaction, for a handler's own writes in
/// the transaction of the message it handles:
///
/// ```ignore
/// let mut tx = ctx.tx.lock().await;
/// sqlx::query("INSERT INTO orders (id) VALUES ($1)")
///     .bind(&order.id)
///     .execute(tx.connection::<sqlx::Postgres>())
///     .await?;
/// ```
///
/// The transaction is the store's (`TxProvider::begin`, `commit`, `abort`): never run `BEGIN`,
/// `COMMIT` or `ROLLBACK` on the connection by raw SQL. A nested transaction through sqlx
/// (`connection.begin()`, `Connection::transaction`) is a savepoint of the store's transaction:
/// nothing commits before the store's commit, and one left open fails it. Do not change the
/// session either (`SET`, `LISTEN`, advisory locks): the connection goes back to the pool.
///
/// Return a statement's error rather than going on: on PostgreSQL a failed statement aborts the
/// whole transaction, and the store's commit then fails (nothing of it is committed); on MySQL a
/// failed statement is undone alone, but a deadlock rolls the whole transaction back unseen
/// (see the crate README, "Transactions").
pub trait SqlxTxExt {
    /// The connection.
    ///
    /// # Panics
    ///
    /// When the transaction is not an open one of the SQL store on `DB` (another store's,
    /// [`Tx::none`], one that ended, or one of the other database).
    fn connection<DB: sqlx::Database>(&mut self) -> &mut DB::Connection;

    /// The connection; `None` where [`connection`](Self::connection) panics.
    fn try_connection<DB: sqlx::Database>(&mut self) -> Option<&mut DB::Connection>;
}

impl SqlxTxExt for Tx {
    fn connection<DB: sqlx::Database>(&mut self) -> &mut DB::Connection {
        self.try_connection::<DB>().unwrap_or_else(|| {
            panic!(
                "the transaction is not an open one of the SQL message store on {}",
                DB::NAME
            )
        })
    }

    fn try_connection<DB: sqlx::Database>(&mut self) -> Option<&mut DB::Connection> {
        self.get_mut::<Held<DB>>().and_then(Held::connection)
    }
}

/// Transactions on connections of the pool, at the database's default isolation.
struct SqlTx {
    db: Arc<Db>,
}

impl TxProvider for SqlTx {
    fn begin(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(async move {
            Ok(match &self.db.pool {
                #[cfg(feature = "postgres")]
                Pool::Pg(p) => Tx::new(Held::<Postgres>::begin(p, crate::db::BEGIN).await?),
                #[cfg(feature = "mysql")]
                Pool::My(p) => Tx::new(Held::<MySql>::begin(p, crate::db::START).await?),
            })
        })
    }

    /// No transaction: each store call runs in a short transaction of its own.
    fn plain(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(async { Ok(Tx::none()) })
    }

    fn commit<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let ended = || crate::db::storage("the transaction has ended already".into());
            #[cfg(feature = "postgres")]
            if let Some(held) = tx.get_mut::<Held<Postgres>>() {
                if !held.is_open() {
                    return Err(ended());
                }
                return held.commit_checked().await;
            }
            #[cfg(feature = "mysql")]
            if let Some(held) = tx.get_mut::<Held<MySql>>() {
                if !held.is_open() {
                    return Err(ended());
                }
                return held.end(true).await;
            }
            let _ = ended;
            // No transaction (`plain`, `Tx::none`): every write committed already.
            Ok(())
        })
    }

    fn abort<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            #[cfg(feature = "postgres")]
            if let Some(held) = tx.get_mut::<Held<Postgres>>() {
                let _ = held.end(false).await;
            }
            #[cfg(feature = "mysql")]
            if let Some(held) = tx.get_mut::<Held<MySql>>() {
                let _ = held.end(false).await;
            }
        })
    }
}
