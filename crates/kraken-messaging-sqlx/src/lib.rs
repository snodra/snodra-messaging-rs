//! The SQL message store (ADR 0028): every store of `kraken_messaging::store` over the service's
//! own PostgreSQL (13 or later, feature `postgres`) or MySQL (8.0.17 or later, InnoDB, feature
//! `mysql`) database through [`sqlx`], in the tables kraken-messaging-jdbc (the Kotlin twin)
//! writes: same tables, same values, same meanings, so a Kotlin service and a Rust service can
//! share one database.
//!
//! ```ignore
//! let pool = sqlx::postgres::PgPoolOptions::new().max_connections(10).connect(&url).await?;
//! let app = KrakenMessaging::builder(config).await?
//!     .store(Arc::new(SqlxStore::postgres(pool).with_settings(SqlxSettings::from_env()?)))
//!     /* ... */;
//! ```
//!
//! - **Schema**: `db/kraken/<dialect>/V1__kraken.sql`, the files kraken-messaging-jdbc ships, byte
//!   for byte ([`schema::POSTGRESQL_V1`], [`schema::MYSQL_V1`]). `start()` applies it
//!   ([`MessageStore::init_schema`]: the server checked, then V1 under a database lock unless
//!   `kraken_schema` holds version 1 or later) unless `KRAKEN_STORE_INIT_SCHEMA=false`; then it
//!   checks that the database has version 1 or later ([`MessageStore::check_schema`]).
//! - **Transactions**: the store's transactions are transactions on connections of the pool. A
//!   handler writes its own rows in the transaction of the message it handles with [`SqlxTxExt`]
//!   (`ctx.tx.lock().await.connection::<Postgres>()`): its rows, the outbox entries and the
//!   processed-event markers commit or roll back together. What commits on its own (dead letters,
//!   processor control, the relays' and schedulers' claims, the purge) runs in a transaction of
//!   its own at READ COMMITTED; a store call outside any transaction in a short one of its own. A
//!   connection whose transaction did not end cleanly is closed, never returned to the pool.
//! - **Wake-up** (PostgreSQL): every append runs `SELECT pg_notify('kraken_outbox', '')` in its
//!   transaction, and the relay listens (`LISTEN kraken_outbox`) on a connection of its own while
//!   it runs, so it publishes as soon as an append commits. MySQL: the relay polls.
//! - **Retention**: what the MongoDB add-on's TTL indexes do, a purge does every
//!   [`SqlxSettings::purge_interval`], with the retentions of the `Config`.
//!
//! Times are the application's clock (the builder's, [`MessageStore::use_clock`]), at
//! millisecond precision, UTC; never the database's.
//!
//! [`MessageStore::init_schema`]: kraken_messaging::store::MessageStore::init_schema
//! [`MessageStore::check_schema`]: kraken_messaging::store::MessageStore::check_schema
//! [`MessageStore::use_clock`]: kraken_messaging::store::MessageStore::use_clock

#[cfg(any(feature = "postgres", feature = "mysql"))]
mod control;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod db;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod dead_letters;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod external;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod limits;
#[cfg(feature = "postgres")]
mod notify;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod outbox;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod purge;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod sagas;
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub mod schema;
mod settings;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod store;

pub use settings::SqlxSettings;
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub use store::{SqlxStore, SqlxTxExt};
