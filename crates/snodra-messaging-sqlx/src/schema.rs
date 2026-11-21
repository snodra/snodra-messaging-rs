//! The schema of the SQL message store: the server check and the versioned schema files, the same
//! files snodra-messaging-jdbc ships (`db/snodra/<dialect>/V1__snodra.sql`, byte for byte).
//!
//! **The version rule** (a contract of both libraries): schema versions are additive.
//! `V1__snodra.sql` is frozen once released; a later change is `V2__snodra.sql`, `V3__...`, each
//! applied over the one before. Code that knows version n works on a database at version n **or
//! later** (a newer library may have added tables or columns it does not read), and refuses one
//! below n. `snodra_schema` lists the versions applied.

use std::time::Duration;

use snodra_messaging::store::StoreError;
#[cfg(feature = "postgres")]
use sqlx::Connection;
use sqlx::Row as _;

use crate::db::{Dialect, Pool, storage};

/// The schema version this library reads and writes.
pub const SCHEMA_VERSION: i64 = 1;

/// Schema version 1 for PostgreSQL, as snodra-messaging-jdbc ships it.
pub const POSTGRESQL_V1: &str = include_str!("db/snodra/postgresql/V1__snodra.sql");

/// Schema version 1 for MySQL, as snodra-messaging-jdbc ships it.
pub const MYSQL_V1: &str = include_str!("db/snodra/mysql/V1__snodra.sql");

/// The session-level advisory lock that serialises the schema's application on PostgreSQL (one
/// bigint key, 0x4B52_4B4E_5343_484D = 5427483300711319629; shared with the Kotlin library).
#[cfg(feature = "postgres")]
const PG_LOCK: i64 = 0x4B52_4B4E_5343_484D;
/// The named lock that serialises it on MySQL.
#[cfg(feature = "mysql")]
const MYSQL_LOCK: &str = "snodra_schema";
/// How long an instance waits for another applying the schema.
const LOCK_WAIT: Duration = Duration::from_secs(60);
#[cfg(feature = "postgres")]
const PG_LOCK_POLL: Duration = Duration::from_millis(200);

const SUPPORTED: &str = "PostgreSQL 13 or later, MySQL 8.0.17 or later";

/// The file of schema version 1 of `dialect`, by its path in both libraries.
pub(crate) fn v1(dialect: Dialect) -> (&'static str, &'static str) {
    match dialect {
        #[cfg(feature = "postgres")]
        Dialect::Postgres => ("db/snodra/postgresql/V1__snodra.sql", POSTGRESQL_V1),
        #[cfg(feature = "mysql")]
        Dialect::MySql => ("db/snodra/mysql/V1__snodra.sql", MYSQL_V1),
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
            "The Snodra message store needs a connection pool of at least 2 (this one has {max}): \
             a write that commits on its own inside the service's transaction takes a second connection"
        )));
    }
    match pool {
        #[cfg(feature = "postgres")]
        Pool::Pg(p) => {
            let mut c = p.acquire().await.map_err(failed)?;
            let row = sqlx::query(
                "SELECT version() AS product, current_setting('server_version_num') AS num, \
                 current_setting('server_version') AS version",
            )
            .fetch_one(&mut *c)
            .await
            .map_err(failed)?;
            let product: String = row.try_get("product").map_err(failed)?;
            let num: String = row.try_get("num").map_err(failed)?;
            let version: String = row.try_get("version").map_err(failed)?;
            check_postgres(&product, &num, &version)
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

/// The PostgreSQL server (`version()`, `server_version_num`, `server_version`) accepted, else why
/// not: PostgreSQL itself (not a server speaking its protocol, such as CockroachDB), 13 or later.
#[cfg(feature = "postgres")]
pub(crate) fn check_postgres(product: &str, num: &str, version: &str) -> Result<(), StoreError> {
    if !product.starts_with("PostgreSQL") {
        return Err(storage(format!(
            "The Snodra message store supports {SUPPORTED}; this server is {product}"
        )));
    }
    if num.trim().parse::<i64>().unwrap_or(0) < 130_000 {
        return Err(storage(format!(
            "The Snodra message store needs PostgreSQL 13.0 or later; this server is PostgreSQL {version}"
        )));
    }
    Ok(())
}

/// The MySQL server's version text and session `sql_mode` accepted, else why not.
#[cfg(feature = "mysql")]
pub(crate) fn check_mysql(version: &str, mode: &str) -> Result<(), StoreError> {
    if version.to_ascii_lowercase().contains("mariadb") {
        return Err(storage(format!(
            "The Snodra message store does not support MariaDB yet (MySQL {version}): it supports {SUPPORTED}"
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
            "The Snodra message store needs MySQL 8.0.17 or later; this server is MySQL {version}"
        )));
    }
    if !mode.contains("STRICT_TRANS_TABLES") && !mode.contains("STRICT_ALL_TABLES") {
        return Err(storage(format!(
            "The Snodra message store needs a strict sql_mode on MySQL (STRICT_TRANS_TABLES or \
             STRICT_ALL_TABLES), so a value too long is refused rather than truncated; this \
             session's sql_mode is '{mode}'"
        )));
    }
    Ok(())
}

fn failed(e: sqlx::Error) -> StoreError {
    storage(format!("cannot check the database server: {e}"))
}

/// The schema version of the database `pool` reaches: the highest in `snodra_schema`, 0 when
/// there is none (no table yet). Read in a transaction rolled back after. Any other failure (the
/// connection, a missing right) is reported as it is.
pub(crate) async fn version(pool: &Pool) -> Result<i64, StoreError> {
    const SQL: &str = "SELECT CAST(MAX(version) AS SIGNED) AS v FROM snodra_schema";
    const PG_SQL: &str = "SELECT CAST(MAX(version) AS BIGINT) AS v FROM snodra_schema";
    let unreadable = |e: sqlx::Error| storage(format!("cannot read the schema version: {e}"));
    let read = match pool {
        #[cfg(feature = "postgres")]
        Pool::Pg(p) => {
            let _ = SQL;
            let mut tx = p.begin().await.map_err(unreadable)?;
            let read = sqlx::query(PG_SQL).fetch_one(&mut *tx).await;
            let _ = tx.rollback().await;
            read.and_then(|row| row.try_get::<Option<i64>, _>("v"))
        }
        #[cfg(feature = "mysql")]
        Pool::My(p) => {
            let _ = PG_SQL;
            let mut tx = p.begin().await.map_err(unreadable)?;
            let read = sqlx::query(SQL).fetch_one(&mut *tx).await;
            let _ = tx.rollback().await;
            read.and_then(|row| row.try_get::<Option<i64>, _>("v"))
        }
    };
    match read {
        Ok(v) => Ok(v.unwrap_or(0)),
        Err(e) if missing_table(&e) => Ok(0),
        Err(e) => Err(unreadable(e)),
    }
}

/// Whether `e` is "no such table" (PostgreSQL 42P01, MySQL 1146).
fn missing_table(e: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db) = e else {
        return false;
    };
    #[cfg(feature = "postgres")]
    if let Some(pg) = db.try_downcast_ref::<sqlx::postgres::PgDatabaseError>() {
        return pg.code() == "42P01";
    }
    #[cfg(feature = "mysql")]
    if let Some(my) = db.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>() {
        return my.number() == 1146;
    }
    false
}

/// For `SNODRA_STORE_INIT_SCHEMA=false`: fails unless the database has the schema at
/// [`SCHEMA_VERSION`] or later.
pub(crate) async fn require(pool: &Pool, dialect: Dialect) -> Result<(), StoreError> {
    let found = version(pool).await?;
    if found < SCHEMA_VERSION {
        return Err(storage(format!(
            "The Snodra message store's schema is at version {found}, it needs {SCHEMA_VERSION} or \
             later: apply {} (and the versions after it), or set SNODRA_STORE_INIT_SCHEMA=true",
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
            "Could not apply the Snodra message store's schema ({path}): apply that file yourself \
             and set SNODRA_STORE_INIT_SCHEMA=false, or grant the service's database user the \
             right to create tables. Cause: {e}"
        ))
    };
    let timed_out = || {
        storage(format!(
            "Timed out after {} s waiting for another instance to apply the Snodra message store schema",
            LOCK_WAIT.as_secs()
        ))
    };
    match pool {
        #[cfg(feature = "postgres")]
        Pool::Pg(p) => {
            let mut c = p.acquire().await.map_err(failed)?;
            // The connection holding the lock is closed when it goes (never back to the pool
            // holding a session lock, even when this future is dropped mid-way).
            c.close_on_drop();
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
                    "applying the Snodra message store schema version 1"
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
            // As on PostgreSQL: never back to the pool holding the named lock.
            c.close_on_drop();
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
                    "applying the Snodra message store schema version 1"
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

    /// The schema files are snodra-messaging-jdbc's, byte for byte: a Kotlin service and a Rust
    /// service share one database. A change to either file costs both libraries.
    #[test]
    fn the_schema_files_are_the_shared_ones_byte_for_byte() {
        assert_eq!(
            sha256(POSTGRESQL_V1),
            "0b2d179a18580092cd7622d11e46451a08b64fad7d6d585450a0919f9148ca6a"
        );
        assert_eq!(
            sha256(MYSQL_V1),
            "f6e28f7c12c0e7db148b534c6ac9d04e5a375bd3b873d63a1c8080707af0cf31"
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn the_schema_lock_key_is_the_kotlin_one() {
        assert_eq!(PG_LOCK, 5_427_483_300_711_319_629);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgresql_13_itself_and_not_a_compatible_server() {
        let pg = "PostgreSQL 17.7 (Debian 17.7-3.pgdg130+1) on x86_64-pc-linux-gnu";
        assert!(check_postgres(pg, "170007", "17.7 (Debian 17.7-3.pgdg130+1)").is_ok());
        assert!(check_postgres("PostgreSQL 13.0 on x86_64", "130000", "13.0").is_ok());
        let refusal = |p: &str, n: &str| check_postgres(p, n, "x").unwrap_err().to_string();
        assert!(refusal("PostgreSQL 12.18 on x86_64", "120018").contains("needs PostgreSQL 13.0"));
        let cockroach = refusal("CockroachDB CCL v24.1.0 (x86_64-pc-linux-gnu)", "130000");
        assert!(
            cockroach.contains("supports PostgreSQL 13 or later")
                && cockroach.contains("this server is CockroachDB"),
            "{cockroach}"
        );
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
