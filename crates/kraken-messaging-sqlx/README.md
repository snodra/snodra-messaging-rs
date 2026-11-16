# kraken-messaging-sqlx

The SQL message store (ADR 0028) of kraken-messaging-rs: the outbox, the processed events, the dead letters, the processor
control, the external calls and the sagas in the service's own **PostgreSQL** (feature `postgres`) or **MySQL** (feature
`mysql`) database, through [`sqlx`](https://docs.rs/sqlx) 0.9. It writes the tables of the Kotlin module
`kraken-messaging-jdbc` with the same values and the same meanings, so a Kotlin service and a Rust service can share one
database.

```toml
kraken-messaging-sqlx = { git = "...", default-features = false, features = ["postgres"] }
sqlx = { version = "0.9.0", default-features = false, features = ["runtime-tokio", "tls-rustls-ring-native-roots", "postgres"] }
```

```rust
use kraken_messaging_sqlx::{SqlxSettings, SqlxStore, SqlxTxExt};

let pool = sqlx::postgres::PgPoolOptions::new().max_connections(10).connect(&url).await?;
let app = KrakenMessaging::builder(Config::from_env()?).await?
    .store(Arc::new(SqlxStore::postgres(pool).with_settings(SqlxSettings::from_env()?)))
    .event_store(events)
    .event::<OrderPlaced, _, _>(|placed, ctx| async move {
        let mut tx = ctx.tx.lock().await;
        sqlx::query("UPDATE orders SET status = 'CONFIRMED' WHERE id = $1")
            .bind(&placed.id)
            .execute(tx.connection::<sqlx::Postgres>())
            .await
            .map_err(|e| MessagingError::Storage(e.to_string()))?;
        Ok(())
    })
    .start()
    .await?;
```

`SqlxStore::mysql(pool)` is the same on a `MySqlPool`. `examples/orders-sql` is a complete small service.

## Requirements

| Database | Minimum | Checked at startup |
|---|---|---|
| PostgreSQL | 13.0 (`jsonb`, `GENERATED ... STORED`) | `version()` must name PostgreSQL itself (a server speaking its protocol, such as CockroachDB, is refused); `server_version_num` |
| MySQL | 8.0.17, InnoDB (`utf8mb4_0900_bin`, functional `CAST(... AS JSON)`) | `VERSION()`; **MariaDB is refused** |
| MySQL `sql_mode` | `STRICT_TRANS_TABLES` or `STRICT_ALL_TABLES` | `@@SESSION.sql_mode`: a value too long is refused, never truncated |
| Pool | at least 2 connections (refused below); size it as below | `max_connections` of the pool |

Each failure stops the startup with a message naming what is supported.

**Pool size.** Every open transaction holds one connection until it ends: each handler running in its transaction (up
to `KRAKEN_MAX_CONCURRENT_HANDLERS` hub handlers, plus each event processor's handler and the saga runtime's attempts),
and each store call that commits on its own takes a second one while it runs, sometimes from inside such a transaction
(a dead letter, the outbox relay's claim, a lease). Size the pool to the transactions that can be open together plus
one at least (plus the service's own use of the pool). A smaller pool does not fail: a call waits for a free connection
and, past the pool's `acquire_timeout` (30 s by default), fails with a storage error that the caller retries. The
relay's LISTEN connection (PostgreSQL) is its own, outside the pool.

**One database, one service.** A database (or schema) with these tables belongs to one logical service: its instances,
in Kotlin, in Rust or both (two implementations of the same service share it, which is what the shared layout is for).
Two different services keep separate databases or schemas: the processed-event markers, the processor control, the
dead letters' key blocking and the deadline and failure-notice claims carry no service name.

TLS to the database goes through sqlx's rustls (`tls-rustls-ring-native-roots`, no OpenSSL): set it in the connect options
or the URL (`sslmode=verify-full`, `ssl-mode=VERIFY_IDENTITY`). The store never logs the pool's connect options, and its
`Debug` shows the dialect and the settings only (the options hold the password).

## Settings

| Setting | Variable | Default | What it does |
|---|---|---|---|
| `SqlxSettings::notify` | `KRAKEN_STORE_SQL_NOTIFY` | `true` | PostgreSQL: appends notify `kraken_outbox` and the relay listens (below); off, the relay polls |
| `SqlxSettings::purge_interval` | `KRAKEN_STORE_SQL_PURGE_INTERVAL_MS` | 60 000 (1 min) | how often the purge runs; `0` turns it off |
| `Config::store_init_schema` | `KRAKEN_STORE_INIT_SCHEMA` | `true` | apply the schema at startup; `false`: check it is there |
| retentions | `Config::outbox_retention`, `idempotency_retention` (by code); `KRAKEN_EXTERNAL_CALLS_RETENTION_MS`, `KRAKEN_SAGAS_ENDED_RETENTION_MS`, `KRAKEN_SAGAS_DISPATCH_RETENTION_MS`, `KRAKEN_SAGAS_FAILED_RETENTION_MS`, `KRAKEN_DEADLINES_RETENTION_MS` | outbox 7 d, markers 30 d, external calls 30 d, ended sagas 30 d, dispatched 7 d, failed 30 d, deadlines 7 d | what the purge keeps |

They mirror the Kotlin module's `kraken.messaging.store.jdbc.notify`, `kraken.messaging.store.jdbc.purge-interval`,
`kraken.messaging.store.init-schema` and `kraken.messaging.*.retention`. `SqlxSettings::from_env()` reads the two variables;
the retentions and `KRAKEN_STORE_INIT_SCHEMA` are the core `Config`'s.

## Schema

The schema files are `kraken-messaging-jdbc`'s, byte for byte (a test pins their SHA-256), embedded in the crate:
`schema::POSTGRESQL_V1` (`db/kraken/postgresql/V1__kraken.sql`) and `schema::MYSQL_V1` (`db/kraken/mysql/V1__kraken.sql`).
The tables: `kraken_schema`, `kraken_outbox`, `kraken_processed_events`, `kraken_dead_letters`, `kraken_processor_control`,
`kraken_external_calls`, `kraken_sagas`, `kraken_saga_associations`, `kraken_saga_dispatch`, `kraken_saga_dispatch_leases`,
`kraken_deadlines`, `kraken_saga_failures`; every column, index and constraint is in the files and in the Kotlin module's
README (the shared reference).

- **Applying it.** `start()` checks the server, then applies V1 unless `kraken_schema` holds version 1 or later, under a
  database lock so instances starting together apply it once: PostgreSQL `pg_try_advisory_lock(5427483300711319629)`
  (polled every 200 ms for at most 60 s; a session lock, so a direct connection, not PgBouncer in transaction mode), the
  file in one transaction; MySQL `GET_LOCK('kraken_schema', 60)` (MySQL commits each DDL statement; the file is idempotent).
  A database user without the right to create tables gets a message saying to apply the file and set
  `KRAKEN_STORE_INIT_SCHEMA=false`. `SqlxStore::apply_schema()` does the same by hand.
- **Applying it yourself** (Flyway, Liquibase, a DBA): run the file, set `KRAKEN_STORE_INIT_SCHEMA=false`; `start()` then
  fails unless the database is at version 1 or later (`SqlxStore::require_schema()`).
- **The version rule** (both libraries): versions are additive. `V1__kraken.sql` is frozen once released; a change is
  `V2__kraken.sql`, applied over V1. Code that knows version n works on a database at version n or later, and refuses one
  below n.

## Values

The same as `kraken-messaging-jdbc`'s, so either library reads the other's rows:

- **Times**: the application's clock (`Config`'s, never the database's `now()`), at millisecond precision, UTC: PostgreSQL
  `TIMESTAMPTZ(3)`, MySQL `DATETIME(3)` holding the UTC wall clock.
- **JSON columns**: the outbox payload, every header map (a JSON object of strings) and the replay ranges (a JSON array of
  `{"source","segment","from","until"}` strings) are `jsonb` / `JSON`; both keep 64-bit integers exact, MySQL stores
  decimals as doubles. What must read back exactly as written is text: the saga state, the dispatch, deadline and failure
  payloads, the external calls' reply and error, a dead letter's payload (`payload`, or `payload_binary` for a value that is
  not UTF-8).
- **Hashes**: values without a length bound (marker ids, idempotency keys, saga failure ids, saga start keys, dead-letter
  record keys) are found by the lowercase hex SHA-256 of their UTF-8 bytes (`*_hash`, `CHAR(64)`), kept whole next to it;
  an association's hash is SHA-256 of key, a zero byte, value. The start key is the saga runtime's `<type>|<key>=<value>`,
  each part percent-escaped (`%` → `%25`, `|` → `%7C`, `=` → `%3D`).
- **Positions**: `position_num` is the position as a number when it is a canonical decimal (`41`, not `041`), else NULL;
  decimal positions are ordered by value, the others by their bytes.
- **Enums** by their Kotlin names (`IN_PROGRESS`, `SCHEDULED`, `COMMAND`, `HANDLER_FAILED`, `NO_HANDLER`, `reset`, ...).

## Limits

Lengths in characters (Unicode code points). Over its bound a value is refused before anything is written, with an
`InvalidMessage` naming the field and its length (never the value), not retried; free text is cut, not refused.

| Value | Bound |
|---|---|
| processor name | 64 |
| source | 255 |
| segment, position | 128 |
| type names (event, message, saga, deadline) | 255 |
| ids the library builds (outbox entry, saga, dispatch entry, deadline) | 255 |
| dead-letter id `<processor>:<source>:<segment>:<position>` | 578 |
| free text (`last_error`, dead-letter error type and message, `failure_message`, saga failure `error`) | cut to 16 KiB of UTF-8 |
| hashed values (above) | none |

The character U+0000 is refused in every text value and JSON string (PostgreSQL cannot store it), on both databases; a
dead letter's payload holding one is kept as `payload_binary`. Identifiers are compared by their bytes on both databases
(`COLLATE "C"`, `utf8mb4_0900_bin`): case, accents and trailing spaces matter.

## Transactions

- `TxProvider::begin` takes a connection from the pool and begins a transaction on it through sqlx's own transaction
  manager, at the database's default isolation (the session is checked to be in a transaction: a statement-mode pooler
  that cannot keep one fails here, clearly). A handler writes its own rows in the transaction of the message it handles
  on that connection, `SqlxTxExt::connection::<DB>()`; its rows, the outbox appends and the processed-event markers commit
  or roll back together. A command handler (which gets no transaction) begins one with `store.tx().begin()` and passes it
  to `EventBus::publish_in` (see `examples/orders-sql`).
- On that connection, never `BEGIN`, `COMMIT` or `ROLLBACK` by raw SQL: the store ends the transaction. A nested
  transaction through sqlx (`connection.begin()`, `Connection::transaction`, `Acquire::begin`) is a **savepoint** of the
  store's transaction: its commit releases the savepoint, its rollback undoes only what it did, and nothing commits before
  the store's commit. A nested transaction left open fails the store's commit: nothing is committed.
- **A statement that fails on that connection.** On PostgreSQL it aborts the whole transaction: every later statement
  fails, and a `COMMIT` is answered with a rollback that sqlx reports as a success. So the store's commit of a service
  transaction first runs `SELECT 1` (one round trip; it fails with `25P02` in an aborted transaction): a handler that ignored
  a failed statement and returned `Ok` gets a failed commit, nothing of the transaction is committed (its rows, the outbox
  appends, the marker), the connection is closed, and the record is not acknowledged: the core handles it again. The
  probe costs one more round trip per commit of a service transaction on PostgreSQL (the store's own transactions run
  none); sqlx exposes neither the `COMMIT` answer's tag nor the session's transaction status, which would spare it. MySQL has
  no aborted state: a failed statement is undone alone and the transaction goes on, so the rest of it commits; except a
  deadlock (`1213`), and a lock wait timeout with `innodb_rollback_on_timeout=ON`, which roll the whole transaction back and
  leave the session outside it (later statements commit one by one, the commit commits nothing, and sqlx does not tell):
  return such an error from the handler, never swallow it.
- A handler must not change the session of that connection either (`SET`, `LISTEN`, advisory locks, auto-commit): it goes
  back to the pool for the next user.
- The connection goes back to the pool only when its transaction ended cleanly; it is closed when the transaction was
  dropped while open, its end failed, a nested transaction was left open (on a commit and on an abort), the transaction was
  found aborted (above), or the session was changed (the MySQL saga start's lock wait until it is put back).
- A commit cancelled while it runs (its future dropped) and then aborted is reported as failed, although the server may have
  committed it: its outcome is unknown, as with any commit whose answer is lost. The core's retries and the processed-event
  markers make a repeated attempt harmless.
- What commits on its own (dead letters but a retry's delete, processor control, the outbox, deadline, notice and lease
  claims, the purge) runs in a transaction of its own at **READ COMMITTED** (on MySQL a locking read at REPEATABLE READ would
  keep the rows it scanned locked, hiding one relay's rows from another's `SKIP LOCKED` claim). A store call outside any
  transaction (or with a transaction that ended) runs in a short transaction of its own: never on a bare pooled connection.
  A transaction of another store is refused.
- **Claims** take rows `FOR UPDATE SKIP LOCKED` in `(created_at, seq)` order (the outbox), due order (deadlines, failure
  notices), with leases as the MongoDB add-on's.
- **Two transactions on one saga** never wait for each other: the loser fails at once with a `StoreError::Conflict` the saga
  runtime retries. An update locks the row `FOR UPDATE NOWAIT`; a start takes, on PostgreSQL,
  `pg_try_advisory_xact_lock(1263684430, hashtext(start_key_hash))`, and on MySQL inserts first with
  `innodb_lock_wait_timeout` at 1 s for that statement (put back after; with `innodb_rollback_on_timeout=ON` the whole
  transaction rolls back, still a retried conflict). Under READ COMMITTED on MySQL a concurrent start of one key waits for the
  other's commit (at most 1 s), then conflicts.
- **Errors**: a duplicate key (PostgreSQL `23505`; MySQL `1062`, `1586`), a serialization failure or deadlock (`40001`,
  `40P01`; `1213`) and a lock not available (`55P03`; `1205`, `3572`) are `StoreError::Conflict`; anything else
  `StoreError::Failed(Storage)`. On PostgreSQL a failed statement aborts the caller's transaction: the store never lets a
  duplicate happen there where it expects one (`ON CONFLICT DO NOTHING`), and the core aborts the attempt on any error.

## The relay wake-up (PostgreSQL)

Every append runs `SELECT pg_notify('kraken_outbox', '')` in its transaction (delivered at the commit, dropped by a
rollback). While a relay runs, the store listens (`LISTEN kraken_outbox`, sqlx's `PgListener`) on a connection of its own
(a pool of one from the store pool's connect options) and wakes the relay on every notification, so it publishes as soon as
an append commits, from this instance or another. A failure reconnects with a backoff from 200 ms to 30 s (logged once);
the relay wakes whenever listening starts again and keeps polling meanwhile. A connection quiet for 30 s is checked with
`SELECT 1` under a 10 s timeout. NOTIFY serialises the notifying commits briefly (a PostgreSQL queue lock), and PgBouncer
in transaction mode cannot carry the LISTEN: there, set `KRAKEN_STORE_SQL_NOTIFY=false`. **MySQL** has no notification: the
relay polls (`KRAKEN_OUTBOX_POLL_INTERVAL_MS`).

## Retention

A purge every `purge_interval` deletes, in batches of 1000 (each its own transaction): sent outbox entries, markers,
external calls, ended sagas (their associations by cascade), dispatched and given-up dispatch entries, fired or cancelled
deadlines and parked saga failures older than their retention, the expired dead letters and the dispatch leases that passed.
Unsent entries, live sagas and scheduled deadlines are never deleted. A dead letter past its `expire_at` is gone at once for
every read. `SqlxStore::purge_expired(&config)` runs it by hand.

## Tests

`tests/conformance.rs` runs the whole store conformance kit on `postgres:17.7` and `mysql:8.4.7`; `tests/layout.rs` pins the
stored values (golden values, hashes computed independently) and reads rows written in the Kotlin layout by plain SQL;
`tests/behaviour.rs` the transactions with the service, the bounds, the schema, the purge and the concurrency rules on both;
`tests/notify.rs` the PostgreSQL wake-up. They need Docker (`DOCKER_HOST=unix://$HOME/.rd/docker.sock` on Rancher Desktop).
