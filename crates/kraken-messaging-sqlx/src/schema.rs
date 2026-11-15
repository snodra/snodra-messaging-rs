//! The schema of the SQL message store: the server check and the versioned schema files, the same
//! files kraken-messaging-jdbc ships (`db/kraken/<dialect>/V1__kraken.sql`, byte for byte).
//!
//! **The version rule** (a contract of both libraries): schema versions are additive.
//! `V1__kraken.sql` is frozen once released; a later change is `V2__kraken.sql`, `V3__...`, each
//! applied over the one before. Code that knows version n works on a database at version n **or
//! later** (a newer library may have added tables or columns it does not read), and refuses one
//! below n. `kraken_schema` lists the versions applied.

use std::time::Duration;

use kraken_messaging::store::StoreError;
#[cfg(feature = "postgres")]
use sqlx::Connection;
use sqlx::Row as _;

use crate::db::{Dialect, Pool, storage};

/// The schema version this library reads and writes.
pub const SCHEMA_VERSION: i64 = 1;

/// Schema version 1 for PostgreSQL, as kraken-messaging-jdbc ships it.
pub const POSTGRESQL_V1: &str = include_str!("db/kraken/postgresql/V1__kraken.sql");

/// Schema version 1 for MySQL, as kraken-messaging-jdbc ships it.
pub const MYSQL_V1: &str = include_str!("db/kraken/mysql/V1__kraken.sql");

/// The session-level advisory lock that serialises the schema's application on PostgreSQL (one
/// bigint key: 'KRKN' 'SCHM').
#[cfg(feature = "postgres")]
const PG_LOCK: i64 = 0x4B52_4B4E_5343_484D;
/// The named lock that serialises it on MySQL.
#[cfg(feature = "mysql")]
const MYSQL_LOCK: &str = "kraken_schema";
/// How long an instance waits for another applying the schema.
const LOCK_WAIT: Duration = Duration::from_secs(60);
#[cfg(feature = "postgres")]
const PG_LOCK_POLL: Duration = Duration::from_millis(200);

#[cfg(feature = "mysql")]
const SUPPORTED: &str = "PostgreSQL 13 or later, MySQL 8.0.17 or later";

/// The file of schema version 1 of `dialect`, by its path in both libraries.
pub(crate) fn v1(dialect: Dialect) -> (&'static str, &'static str) {
    match dialect {
        #[cfg(feature = "postgres")]
        Dialect::Postgres => ("db/kraken/postgresql/V1__kraken.sql", POSTGRESQL_V1),
        #[cfg(feature = "mysql")]
        Dialect::MySql => ("db/kraken/mysql/V1__kraken.sql", MYSQL_V1),
    }
}

/// Fails unless the pool reaches a server this store runs on: PostgreSQL 13 or later; MySQL
/// 8.0.17 or later (not MariaDB) with a STRICT `sql_mode`, so the database refuses rather than
/// truncates a value; and a pool of at least 2 connections (a write that commits on its own inside
/// the service's transaction takes a second one).
pub(crate) async fn verify(pool: &Pool) -> Result<(), StoreError> {
    let max = match pool {
        #[cfg(feature = "postgres")]
        Pool::Pg(p) => p.options().get_max_connections(),
        #[cfg(feature = "mysql")]
        Pool::My(p) => p.options().get_max_connections(),
    };
    if max < 2 {
        return Err(storage(format!(
            "The kraken message store needs a connection pool of at least 2 (this one has {max}): \
             a write that commits on its own inside the service's transaction takes a second connection"
        )));
    }
    match pool {
        #[cfg(feature = "postgres")]
        Pool::Pg(p) => {
            let mut c = p.acquire().await.map_err(failed)?;
            let row = sqlx::query(
                "SELECT current_setting('server_version_num') AS num, current_setting('server_version') AS version",
            )
            .fetch_one(&mut *c)
            .await
            .map_err(failed)?;
            let num: i64 = row
                .try_get::<String, _>("num")
                .map_err(failed)?
                .trim()
                .parse()
                .unwrap_or(0);
            let version: String = row.try_get("version").map_err(failed)?;
            if num < 130_000 {
                return Err(storage(format!(
                    "The kraken message store needs PostgreSQL 13.0 or later; this server is PostgreSQL {version}"
                )));
            }
            Ok(())
        }
        #[cfg(feature = "mysql")]
        Pool::My(p) => {
            let mut c = p.acquire().await.map_err(failed)?;
            let row = sqlx::query("SELECT VERSION() AS version, @@SESSION.sql_mode AS mode")
                .fetch_one(&mut *c)
                .await
                .map_err(failed)?;
            let version: String = row.try_get("version").map_err(failed)?;
            let mode: String = row
                .try_get::<Option<String>, _>("mode")
                .map_err(failed)?
                .unwrap_or_default();
            check_mysql(&version, &mode)
        }
    }
}

/// The MySQL server's version text and session `sql_mode` accepted, else why not.
#[cfg(feature = "mysql")]
pub(crate) fn check_mysql(version: &str, mode: &str) -> Result<(), StoreError> {
    if version.to_ascii_lowercase().contains("mariadb") {
        return Err(storage(format!(
            "The kraken message store does not support MariaDB yet (MySQL {version}): it supports {SUPPORTED}"
        )));
    }
    let numbers: Vec<u32> = version
        .split(|c: char| !c.is_ascii_digit())
        .take(3)
        .map(|n| n.parse().unwrap_or(0))
        .collect();
    let at = |i: usize| numbers.get(i).copied().unwrap_or(0);
    if (at(0), at(1), at(2)) < (8, 0, 17) {
        return Err(storage(format!(
            "The kraken message store needs MySQL 8.0.17 or later; this server is MySQL {version}"
        )));
    }
    if !mode.contains("STRICT_TRANS_TABLES") && !mode.contains("STRICT_ALL_TABLES") {
        return Err(storage(format!(
            "The kraken message store needs a strict sql_mode on MySQL (STRICT_TRANS_TABLES or \
             STRICT_ALL_TABLES), so a value too long is refused rather than truncated; this \
             session's sql_mode is '{mode}'"
        )));
    }
    Ok(())
}

fn failed(e: sqlx::Error) -> StoreError {
    storage(format!("cannot check the database server: {e}"))
}

/// The schema version of the database `pool` reaches: the highest in `kraken_schema`, 0 when
/// there is none (no table yet). Read in a transaction rolled back after.
pub(crate) async fn version(pool: &Pool) -> Result<i64, StoreError> {
    const SQL: &str = "SELECT MAX(version) AS v FROM kraken_schema";
    let read = match pool {
        #[cfg(feature = "postgres")]
        Pool::Pg(p) => {
            let mut tx = p.begin().await.map_err(crate::db::store_error)?;
            let read = sqlx::query(SQL).fetch_one(&mut *tx).await;
            let _ = tx.rollback().await;
            read.map(|row| {
                row.try_get::<Option<i32>, _>("v")
                    .ok()
                    .flatten()
                    .map(i64::from)
            })
        }
        #[cfg(feature = "mysql")]
        Pool::My(p) => {
            let mut tx = p.begin().await.map_err(crate::db::store_error)?;
            let read = sqlx::query(SQL).fetch_one(&mut *tx).await;
            let _ = tx.rollback().await;
            read.map(|row| {
                row.try_get::<Option<i64>, _>("v")
                    .ok()
                    .flatten()
                    .or_else(|| {
                        row.try_get::<Option<i32>, _>("v")
                            .ok()
                            .flatten()
                            .map(i64::from)
                    })
            })
        }
    };
    // An error is the missing table: version 0.
    Ok(read.ok().flatten().unwrap_or(0))
}

/// For `KRAKEN_STORE_INIT_SCHEMA=false`: fails unless the database has the schema at
/// [`SCHEMA_VERSION`] or later.
pub(crate) async fn require(pool: &Pool, dialect: Dialect) -> Result<(), StoreError> {
    let found = version(pool).await?;
    if found < SCHEMA_VERSION {
        return Err(storage(format!(
            "The kraken message store's schema is at version {found}, it needs {SCHEMA_VERSION} or \
             later: apply {} (and the versions after it), or set KRAKEN_STORE_INIT_SCHEMA=true",
            v1(dialect).0
        )));
    }
    Ok(())
}

/// Applies the schema up to [`SCHEMA_VERSION`] unless the database has it (then it is only read),
/// under a database lock so instances starting together apply it once (PostgreSQL in one
/// transaction; MySQL commits each DDL statement, and the file is idempotent).
pub(crate) async fn apply(pool: &Pool, dialect: Dialect) -> Result<(), StoreError> {
    if version(pool).await? >= SCHEMA_VERSION {
        return Ok(());
    }
    let (path, script) = v1(dialect);
    let refused = |e: sqlx::Error| {
        storage(format!(
            "Could not apply the kraken message store's schema ({path}): apply that file yourself \
             and set KRAKEN_STORE_INIT_SCHEMA=false, or grant the service's database user the \
             right to create tables. Cause: {e}"
        ))
    };
    let timed_out = || {
        storage(format!(
            "Timed out after {} s waiting for another instance to apply the kraken message store schema",
            LOCK_WAIT.as_secs()
        ))
    };
    match pool {
        #[cfg(feature = "postgres")]
        Pool::Pg(p) => {
            let mut c = p.acquire().await.map_err(failed)?;
            // A session lock: it needs a direct connection (not PgBouncer in transaction mode).
            let deadline = tokio::time::Instant::now() + LOCK_WAIT;
            loop {
                let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                    .bind(PG_LOCK)
                    .fetch_one(&mut *c)
                    .await
                    .map_err(failed)?;
                if got {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(timed_out());
                }
                tokio::time::sleep(PG_LOCK_POLL).await;
            }
            let applied = async {
                if version(pool).await? >= SCHEMA_VERSION {
                    return Ok(()); // another instance applied it while this one waited
                }
                tracing::info!(
                    file = path,
                    "applying the kraken message store schema version 1"
                );
                let mut tx = c.begin().await.map_err(refused)?;
                sqlx::raw_sql(script)
                    .execute(&mut *tx)
                    .await
                    .map_err(refused)?;
                tx.commit().await.map_err(refused)
            }
            .await;
            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(PG_LOCK)
                .execute(&mut *c)
                .await;
            applied
        }
        #[cfg(feature = "mysql")]
        Pool::My(p) => {
            let mut c = p.acquire().await.map_err(failed)?;
            let got: Option<i64> = sqlx::query_scalar("SELECT GET_LOCK(?, ?)")
                .bind(MYSQL_LOCK)
                .bind(LOCK_WAIT.as_secs() as i64)
                .fetch_one(&mut *c)
                .await
                .map_err(failed)?;
            if got != Some(1) {
                return Err(timed_out());
            }
            let applied = async {
                if version(pool).await? >= SCHEMA_VERSION {
                    return Ok(());
                }
                tracing::info!(
                    file = path,
                    "applying the kraken message store schema version 1"
                );
                sqlx::raw_sql(script)
                    .execute(&mut *c)
                    .await
                    .map_err(refused)?;
                Ok(())
            }
            .await;
            let _ = sqlx::query("SELECT RELEASE_LOCK(?)")
                .bind(MYSQL_LOCK)
                .execute(&mut *c)
                .await;
            applied
        }
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    fn sha256(text: &str) -> String {
        Sha256::digest(text.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    /// The schema files are kraken-messaging-jdbc's, byte for byte: a Kotlin service and a Rust
    /// service share one database. A change to either file costs both libraries.
    #[test]
    fn the_schema_files_are_the_shared_ones_byte_for_byte() {
        assert_eq!(
            sha256(POSTGRESQL_V1),
            "62a34cb682974335e40e68d0cd584a2ce002c38c7e3a0fb3289c2ef8bf8f1b95"
        );
        assert_eq!(
            sha256(MYSQL_V1),
            "2c47c1e245556a576c69abe7811acca4f7574775d669bdb4b5ccac517db76e99"
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn the_schema_lock_key_is_the_kotlin_one() {
        assert_eq!(PG_LOCK, 5_427_483_300_711_319_629);
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_needs_8_0_17_strict_and_not_mariadb() {
        let strict = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";
        assert!(check_mysql("8.4.7", strict).is_ok());
        assert!(check_mysql("8.0.17", "STRICT_ALL_TABLES").is_ok());
        assert!(check_mysql("9.1.0-commercial", strict).is_ok());
        let refusal = |v: &str, m: &str| check_mysql(v, m).unwrap_err().to_string();
        assert!(refusal("8.0.16", strict).contains("needs MySQL 8.0.17 or later"));
        assert!(refusal("5.7.44-log", strict).contains("needs MySQL 8.0.17 or later"));
        assert!(refusal("10.11.6-MariaDB", strict).contains("does not support MariaDB"));
        assert!(refusal("8.4.7", "NO_ENGINE_SUBSTITUTION").contains("strict sql_mode"));
    }
}
