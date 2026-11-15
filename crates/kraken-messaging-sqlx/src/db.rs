//! How the stores reach the database: one connection type over both databases ([`Conn`]), the
//! statements with their bind parameters ([`Stmt`]), the rows read back ([`Row`]), and the two
//! ways a store call runs, by what it must commit with:
//!
//! - [`Db::scope`]: with the caller's [`Tx`] (the service's transaction) when it holds one of this
//!   store's open transactions, else in a short transaction of its own (a [`Tx::none`], or a
//!   transaction that ended): never on a bare pooled connection, whose auto-commit the server
//!   decides.
//! - [`Db::own`]: in a transaction of its own at READ COMMITTED, whatever the caller has: what
//!   commits on its own (dead letters, processor control, the relays' and schedulers' claims, the
//!   purge). On MySQL a locking read at REPEATABLE READ keeps every row it scanned locked, so one
//!   relay's claim would hide the rows another holds from that relay's own `SKIP LOCKED` claim.
//!
//! Every statement is a fixed text with bind parameters (`?`, numbered `$n` for PostgreSQL); the
//! only parts chosen at run time are fixed fragments and the number of parameters of an `IN`
//! list. Nothing in a statement comes from input.

use std::borrow::Cow;
use std::sync::{Arc, RwLock};

use kraken_messaging::error::MessagingError;
use kraken_messaging::saga::Clock;
use kraken_messaging::store::{StoreError, Tx};
use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, Row as _, SqlSafeStr};
use sqlx_core::transaction::TransactionManager;

#[cfg(feature = "mysql")]
use sqlx::mysql::{MySql, MySqlConnection, MySqlDatabaseError, MySqlPool, MySqlRow};
#[cfg(feature = "postgres")]
use sqlx::postgres::{PgConnection, PgDatabaseError, PgPool, PgRow, Postgres};

use crate::limits::refuse_nul;

/// The database the store runs on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Dialect {
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "mysql")]
    MySql,
}

impl Dialect {
    /// `postgresql` or `mysql`: the folder of its schema files.
    pub(crate) fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            Dialect::Postgres => "postgresql",
            #[cfg(feature = "mysql")]
            Dialect::MySql => "mysql",
        }
    }
}

/// The pool the store was given.
#[derive(Clone)]
pub(crate) enum Pool {
    #[cfg(feature = "postgres")]
    Pg(PgPool),
    #[cfg(feature = "mysql")]
    My(MySqlPool),
}

/// A transaction of the store's database (a [`Tx`] holds one, and so does [`OwnTx`]): its pooled
/// connection while it is open. It begins and ends through sqlx's own transaction manager, so the
/// connection knows it is in a transaction (depth 1): a nested transaction a handler opens on it
/// through sqlx is a savepoint, never a `BEGIN` / `COMMIT` of its own.
///
/// A connection goes back to the pool only once its transaction ended cleanly (`COMMIT` or
/// `ROLLBACK` succeeded at depth 1) with the session as it found it. Otherwise it is closed: a
/// transaction dropped while open (a future dropped mid-way, a `Tx` never ended), a failed end, a
/// nested transaction left open (the commit then fails), or a session variable changed for a
/// statement and not yet put back ([`Conn::session_changed`]). The server rolls back what a closed
/// connection left open; nothing reaches the next user of the pool. Not an `sqlx::Transaction`:
/// that hands its connection back to the pool whatever its session holds.
pub(crate) struct Held<DB: sqlx::Database> {
    conn: Option<PoolConnection<DB>>,
    session_changed: bool,
}

impl<DB: sqlx::Database> Drop for Held<DB> {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            conn.close_on_drop();
        }
    }
}

impl<DB: sqlx::Database> Held<DB> {
    /// The connection while the transaction is open.
    pub(crate) fn connection(&mut self) -> Option<&mut DB::Connection> {
        self.conn.as_deref_mut()
    }

    pub(crate) fn is_open(&self) -> bool {
        self.conn.is_some()
    }

    /// A connection of `pool` in a transaction begun by `statement` (through sqlx's transaction
    /// manager, which checks that the session is in a transaction afterwards: a statement-mode
    /// pooler fails here).
    pub(crate) async fn begin(
        pool: &sqlx::Pool<DB>,
        statement: &'static str,
    ) -> Result<Held<DB>, StoreError> {
        let mut held = Held {
            conn: Some(pool.acquire().await.map_err(store_error)?),
            session_changed: false,
        };
        // Inside `held` already: dropped here (an error, or this future dropped), the connection
        // is closed.
        if let Some(conn) = held.conn.as_mut() {
            DB::TransactionManager::begin(&mut **conn, Some(statement.into_sql_str()))
                .await
                .map_err(|e| match e {
                    sqlx::Error::BeginFailed => storage(format!(
                        "`{statement}` did not leave the session in a transaction (a pooler in \
                         statement mode?): the message store needs a connection that keeps one"
                    )),
                    other => store_error(other),
                })?;
        }
        Ok(held)
    }

    /// Commits (or rolls back) and lets go of the connection: back to the pool when that ended
    /// cleanly with the session untouched, else closed. A commit with a nested transaction still
    /// open (sqlx's depth above 1) commits nothing: the connection is closed (the server rolls it
    /// back) and the commit fails; so does one whose transaction was ended through sqlx's
    /// transaction manager by someone else (depth 0). A raw `COMMIT` or `ROLLBACK` run on the
    /// connection does not change sqlx's depth and is not seen here: it stays forbidden
    /// ([`SqlxTxExt`](crate::SqlxTxExt)). Nothing for an ended one.
    pub(crate) async fn end(&mut self, commit: bool) -> Result<(), StoreError> {
        let Some(conn) = self.conn.as_mut() else {
            return Ok(());
        };
        let depth = DB::TransactionManager::get_transaction_depth(conn);
        if depth != 1 {
            if let Some(mut conn) = self.conn.take() {
                conn.close_on_drop();
            }
            return if commit {
                Err(storage(if depth == 0 {
                    "the store's transaction was ended by someone else through sqlx's \
                     transaction manager (sqlx's depth is 0): it committed or rolled back then, \
                     not at the store's commit, and the connection is closed (never run COMMIT or \
                     ROLLBACK on the store's connection: a raw one is not even seen)"
                        .to_string()
                } else {
                    format!(
                        "a nested transaction was left open on the store's connection ({} \
                         levels): the store's transaction is rolled back, nothing of it is committed",
                        depth - 1
                    )
                }))
            } else {
                Ok(())
            };
        }
        let ended = if commit {
            DB::TransactionManager::commit(&mut **conn).await
        } else {
            DB::TransactionManager::rollback(&mut **conn).await
        };
        if let Some(mut conn) = self.conn.take()
            && (ended.is_err() || self.session_changed)
        {
            conn.close_on_drop();
        }
        ended.map_err(store_error)
    }
}

#[cfg(feature = "postgres")]
impl Held<Postgres> {
    /// [`end`](Self::end) with a commit, for a transaction foreign code ran statements in (the
    /// service's): PostgreSQL aborts the whole transaction on a failed statement and answers a
    /// later `COMMIT` with a rollback that sqlx reports as a success, so a statement that failed
    /// on the connection and whose error was ignored (a handler going on as if it had not) would
    /// pass for a commit. A probe first (`SELECT 1`, one round trip; it fails with `25P02` in an
    /// aborted transaction) finds it: the connection is closed (the server rolls back) and the
    /// commit fails, nothing of it committed.
    pub(crate) async fn commit_checked(&mut self) -> Result<(), StoreError> {
        let Some(conn) = self.conn.as_mut() else {
            return Ok(());
        };
        if <Postgres as sqlx::Database>::TransactionManager::get_transaction_depth(conn) == 1
            && let Err(e) = sqlx::raw_sql("SELECT 1").execute(&mut **conn).await
        {
            if let Some(mut conn) = self.conn.take() {
                conn.close_on_drop();
            }
            return Err(match &e {
                sqlx::Error::Database(db) if db.code().as_deref() == Some("25P02") => storage(
                    "the store's transaction was aborted by a statement that failed on its \
                     connection (an error ignored by a handler?): it is rolled back, nothing of it \
                     is committed"
                        .to_string(),
                ),
                _ => store_error(e),
            });
        }
        self.end(true).await
    }
}

macro_rules! held {
    ($db:ty, $variant:ident) => {
        impl Held<$db> {
            pub(crate) fn conn(&mut self) -> Option<Conn<'_>> {
                let Held {
                    conn,
                    session_changed,
                } = self;
                conn.as_mut().map(|c| Conn {
                    c: ConnRef::$variant(&mut **c),
                    session_changed,
                })
            }
        }
    };
}

#[cfg(feature = "postgres")]
held!(Postgres, Pg);
#[cfg(feature = "mysql")]
held!(MySql, My);

/// A connection of either database.
enum ConnRef<'a> {
    #[cfg(feature = "postgres")]
    Pg(&'a mut PgConnection),
    #[cfg(feature = "mysql")]
    My(&'a mut MySqlConnection),
}

/// A connection in a transaction, and its transaction's session flag.
pub(crate) struct Conn<'a> {
    c: ConnRef<'a>,
    session_changed: &'a mut bool,
}

/// A bind parameter.
#[derive(Clone, Debug)]
pub(crate) enum Arg {
    Text(Option<String>),
    Int(Option<i64>),
    Int32(Option<i32>),
    Bool(Option<bool>),
    /// Epoch milliseconds, bound as a UTC timestamp (PostgreSQL `timestamptz`, MySQL `DATETIME`
    /// holding UTC).
    Time(Option<i64>),
    Bytes(Option<Vec<u8>>),
}

impl From<&str> for Arg {
    fn from(v: &str) -> Arg {
        Arg::Text(Some(v.to_string()))
    }
}
impl From<String> for Arg {
    fn from(v: String) -> Arg {
        Arg::Text(Some(v))
    }
}
impl From<&String> for Arg {
    fn from(v: &String) -> Arg {
        Arg::Text(Some(v.clone()))
    }
}
impl From<Option<&str>> for Arg {
    fn from(v: Option<&str>) -> Arg {
        Arg::Text(v.map(str::to_string))
    }
}
impl From<Option<String>> for Arg {
    fn from(v: Option<String>) -> Arg {
        Arg::Text(v)
    }
}
impl From<&Option<String>> for Arg {
    fn from(v: &Option<String>) -> Arg {
        Arg::Text(v.clone())
    }
}
impl From<i64> for Arg {
    fn from(v: i64) -> Arg {
        Arg::Int(Some(v))
    }
}
impl From<Option<i64>> for Arg {
    fn from(v: Option<i64>) -> Arg {
        Arg::Int(v)
    }
}
impl From<i32> for Arg {
    fn from(v: i32) -> Arg {
        Arg::Int32(Some(v))
    }
}
impl From<Option<i32>> for Arg {
    fn from(v: Option<i32>) -> Arg {
        Arg::Int32(v)
    }
}
impl From<bool> for Arg {
    fn from(v: bool) -> Arg {
        Arg::Bool(Some(v))
    }
}
impl From<Option<bool>> for Arg {
    fn from(v: Option<bool>) -> Arg {
        Arg::Bool(v)
    }
}

/// A statement and its parameters, each named for the error that refuses it.
pub(crate) struct Stmt {
    sql: Cow<'static, str>,
    args: Vec<(&'static str, Arg)>,
}

impl Stmt {
    /// A fixed statement: `?` for each parameter, `{json}` for the JSON type (`jsonb` / `JSON`)
    /// and `{text}` for the text type a JSON column is read as (`TEXT` / `CHAR`).
    pub(crate) fn new(sql: &'static str) -> Stmt {
        Stmt {
            sql: Cow::Borrowed(sql),
            args: Vec::new(),
        }
    }

    /// A statement put together from fixed fragments (an `IN` list's parameters).
    pub(crate) fn built(sql: String) -> Stmt {
        Stmt {
            sql: Cow::Owned(sql),
            args: Vec::new(),
        }
    }

    pub(crate) fn arg(mut self, name: &'static str, value: impl Into<Arg>) -> Stmt {
        self.args.push((name, value.into()));
        self
    }

    /// Epoch milliseconds as a timestamp.
    pub(crate) fn time(mut self, name: &'static str, ms: i64) -> Stmt {
        self.args.push((name, Arg::Time(Some(ms))));
        self
    }

    pub(crate) fn opt_time(mut self, name: &'static str, ms: Option<i64>) -> Stmt {
        self.args.push((name, Arg::Time(ms)));
        self
    }

    pub(crate) fn bytes(mut self, name: &'static str, bytes: Option<Vec<u8>>) -> Stmt {
        self.args.push((name, Arg::Bytes(bytes)));
        self
    }

    /// One parameter per value, for an `IN (?, ?, ...)` list.
    pub(crate) fn args<V: Into<Arg>>(
        mut self,
        name: &'static str,
        values: impl IntoIterator<Item = V>,
    ) -> Stmt {
        self.args
            .extend(values.into_iter().map(|v| (name, v.into())));
        self
    }

    /// U+0000 in a text parameter is refused (PostgreSQL cannot store it; neither database is
    /// given one), and so is an instant no timestamp holds, naming the parameter.
    fn checked(self) -> Result<Stmt, StoreError> {
        for (name, arg) in &self.args {
            match arg {
                Arg::Text(Some(text)) => refuse_nul(&format!("the parameter {name}"), text)?,
                Arg::Time(Some(ms)) if chrono::DateTime::from_timestamp_millis(*ms).is_none() => {
                    return Err(StoreError::Failed(MessagingError::InvalidMessage(format!(
                        "The message store cannot keep the instant {ms} ms from the epoch (the parameter {name}): out of range"
                    ))));
                }
                _ => {}
            }
        }
        Ok(self)
    }

    /// The statement as `dialect` writes it.
    fn render(&self, dialect: Dialect) -> String {
        match dialect {
            #[cfg(feature = "postgres")]
            Dialect::Postgres => {
                let sql = self
                    .sql
                    .replace("{json}", "jsonb")
                    .replace("{text}", "TEXT");
                let mut out = String::with_capacity(sql.len() + 16);
                let mut n = 0;
                for c in sql.chars() {
                    if c == '?' {
                        n += 1;
                        out.push('$');
                        out.push_str(&n.to_string());
                    } else {
                        out.push(c);
                    }
                }
                out
            }
            #[cfg(feature = "mysql")]
            Dialect::MySql => self.sql.replace("{json}", "JSON").replace("{text}", "CHAR"),
        }
    }
}

/// `?, ?, ...`: `n` parameters for an `IN` list (at least one).
pub(crate) fn placeholders(n: usize) -> String {
    vec!["?"; n.max(1)].join(", ")
}

#[cfg(feature = "postgres")]
fn pg_time(ms: i64) -> chrono::DateTime<chrono::Utc> {
    // In range: `Stmt::checked` refused any other.
    chrono::DateTime::from_timestamp_millis(ms).unwrap_or_default()
}

#[cfg(feature = "mysql")]
fn my_time(ms: i64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .naive_utc()
}

#[cfg(feature = "postgres")]
fn pg_query(
    sql: String,
    args: Vec<(&'static str, Arg)>,
) -> sqlx::query::Query<'static, Postgres, sqlx::postgres::PgArguments> {
    let mut q = sqlx::query(AssertSqlSafe(sql));
    for (_, arg) in args {
        q = match arg {
            Arg::Text(v) => q.bind(v),
            Arg::Int(v) => q.bind(v),
            Arg::Int32(v) => q.bind(v),
            Arg::Bool(v) => q.bind(v),
            Arg::Time(v) => q.bind(v.map(pg_time)),
            Arg::Bytes(v) => q.bind(v),
        };
    }
    q
}

#[cfg(feature = "mysql")]
fn my_query(
    sql: String,
    args: Vec<(&'static str, Arg)>,
) -> sqlx::query::Query<'static, MySql, sqlx::mysql::MySqlArguments> {
    let mut q = sqlx::query(AssertSqlSafe(sql));
    for (_, arg) in args {
        q = match arg {
            Arg::Text(v) => q.bind(v),
            Arg::Int(v) => q.bind(v),
            Arg::Int32(v) => q.bind(v),
            Arg::Bool(v) => q.bind(v),
            Arg::Time(v) => q.bind(v.map(my_time)),
            Arg::Bytes(v) => q.bind(v),
        };
    }
    q
}

impl Conn<'_> {
    pub(crate) fn dialect(&self) -> Dialect {
        match self.c {
            #[cfg(feature = "postgres")]
            ConnRef::Pg(_) => Dialect::Postgres,
            #[cfg(feature = "mysql")]
            ConnRef::My(_) => Dialect::MySql,
        }
    }

    /// Marks the session changed (a variable set for a statement) until it is put back: a
    /// transaction ending meanwhile closes its connection rather than returning it to the pool.
    #[cfg_attr(not(feature = "mysql"), allow(dead_code))] // only MySQL changes one today
    pub(crate) fn session_changed(&mut self, changed: bool) {
        *self.session_changed = changed;
    }

    /// Runs `stmt`; the rows it changed (MySQL: the rows it found, as Connector/J counts them),
    /// or the database's error as it is (outer: a parameter refused before anything ran).
    pub(crate) async fn try_execute(
        &mut self,
        stmt: Stmt,
    ) -> Result<Result<u64, sqlx::Error>, StoreError> {
        let stmt = stmt.checked()?;
        let sql = stmt.render(self.dialect());
        Ok(match &mut self.c {
            #[cfg(feature = "postgres")]
            ConnRef::Pg(c) => pg_query(sql, stmt.args)
                .execute(&mut **c)
                .await
                .map(|r| r.rows_affected()),
            #[cfg(feature = "mysql")]
            ConnRef::My(c) => my_query(sql, stmt.args)
                .execute(&mut **c)
                .await
                .map(|r| r.rows_affected()),
        })
    }

    pub(crate) async fn execute(&mut self, stmt: Stmt) -> Result<u64, StoreError> {
        self.try_execute(stmt).await?.map_err(store_error)
    }

    pub(crate) async fn fetch_all(&mut self, stmt: Stmt) -> Result<Vec<Row>, StoreError> {
        let stmt = stmt.checked()?;
        let sql = stmt.render(self.dialect());
        match &mut self.c {
            #[cfg(feature = "postgres")]
            ConnRef::Pg(c) => Ok(pg_query(sql, stmt.args)
                .fetch_all(&mut **c)
                .await
                .map_err(store_error)?
                .into_iter()
                .map(Row::Pg)
                .collect()),
            #[cfg(feature = "mysql")]
            ConnRef::My(c) => Ok(my_query(sql, stmt.args)
                .fetch_all(&mut **c)
                .await
                .map_err(store_error)?
                .into_iter()
                .map(Row::My)
                .collect()),
        }
    }

    pub(crate) async fn fetch_optional(&mut self, stmt: Stmt) -> Result<Option<Row>, StoreError> {
        Ok(self.fetch_all(stmt).await?.into_iter().next())
    }

    /// The one row `stmt` returns.
    pub(crate) async fn fetch_one(&mut self, stmt: Stmt) -> Result<Row, StoreError> {
        self.fetch_optional(stmt)
            .await?
            .ok_or_else(|| storage("a statement returned no row".into()))
    }
}

/// A row of either database, read by column name.
pub(crate) enum Row {
    #[cfg(feature = "postgres")]
    Pg(PgRow),
    #[cfg(feature = "mysql")]
    My(MySqlRow),
}

macro_rules! get {
    ($row:expr, $ty:ty, $col:expr) => {
        match $row {
            #[cfg(feature = "postgres")]
            Row::Pg(r) => r.try_get::<$ty, _>($col),
            #[cfg(feature = "mysql")]
            Row::My(r) => r.try_get::<$ty, _>($col),
        }
        .map_err(|e| decode_error($col, e))
    };
}

fn decode_error(column: &str, e: sqlx::Error) -> StoreError {
    storage(format!("cannot read column {column}: {e}"))
}

impl Row {
    pub(crate) fn text(&self, col: &str) -> Result<String, StoreError> {
        get!(self, String, col)
    }

    pub(crate) fn opt_text(&self, col: &str) -> Result<Option<String>, StoreError> {
        get!(self, Option<String>, col)
    }

    /// An integer column of any width.
    pub(crate) fn int(&self, col: &str) -> Result<i64, StoreError> {
        self.opt_int(col)?
            .ok_or_else(|| storage(format!("column {col} is null")))
    }

    pub(crate) fn opt_int(&self, col: &str) -> Result<Option<i64>, StoreError> {
        match get!(self, Option<i64>, col) {
            Ok(v) => Ok(v),
            Err(_) => get!(self, Option<i32>, col).map(|v| v.map(i64::from)),
        }
    }

    pub(crate) fn int32(&self, col: &str) -> Result<i32, StoreError> {
        let n = self.int(col)?;
        i32::try_from(n).map_err(|_| storage(format!("column {col} holds {n}, beyond i32")))
    }

    pub(crate) fn opt_int32(&self, col: &str) -> Result<Option<i32>, StoreError> {
        self.opt_int(col)?
            .map(|n| {
                i32::try_from(n).map_err(|_| storage(format!("column {col} holds {n}, beyond i32")))
            })
            .transpose()
    }

    pub(crate) fn bool(&self, col: &str) -> Result<bool, StoreError> {
        self.opt_bool(col)?
            .ok_or_else(|| storage(format!("column {col} is null")))
    }

    pub(crate) fn opt_bool(&self, col: &str) -> Result<Option<bool>, StoreError> {
        get!(self, Option<bool>, col)
    }

    pub(crate) fn opt_bytes(&self, col: &str) -> Result<Option<Vec<u8>>, StoreError> {
        get!(self, Option<Vec<u8>>, col)
    }

    /// A timestamp column as epoch milliseconds.
    pub(crate) fn ms(&self, col: &str) -> Result<i64, StoreError> {
        self.opt_ms(col)?
            .ok_or_else(|| storage(format!("column {col} is null")))
    }

    pub(crate) fn opt_ms(&self, col: &str) -> Result<Option<i64>, StoreError> {
        match self {
            #[cfg(feature = "postgres")]
            Row::Pg(r) => r
                .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(col)
                .map(|t| t.map(|t| t.timestamp_millis())),
            #[cfg(feature = "mysql")]
            Row::My(r) => r
                .try_get::<Option<chrono::NaiveDateTime>, _>(col)
                .map(|t| t.map(|t| t.and_utc().timestamp_millis())),
        }
        .map_err(|e| decode_error(col, e))
    }
}

/// How a transaction at the database's default isolation begins.
#[cfg(feature = "postgres")]
pub(crate) const BEGIN: &str = "BEGIN";
#[cfg(feature = "mysql")]
pub(crate) const START: &str = "START TRANSACTION";

/// What the stores share: the pool, the application's clock and the dialect.
pub(crate) struct Db {
    pub(crate) pool: Pool,
    clock: Arc<RwLock<Clock>>,
}

/// The store's own transaction of [`Db::own`] or [`Db::scope`].
pub(crate) enum OwnTx {
    #[cfg(feature = "postgres")]
    Pg(Held<Postgres>),
    #[cfg(feature = "mysql")]
    My(Held<MySql>),
}

impl OwnTx {
    pub(crate) fn conn(&mut self) -> Conn<'_> {
        match self {
            #[cfg(feature = "postgres")]
            OwnTx::Pg(t) => t.conn(),
            #[cfg(feature = "mysql")]
            OwnTx::My(t) => t.conn(),
        }
        .expect("the store's own transaction is open until it is finished")
    }

    /// Commits when `result` is a success, else rolls back; `result` either way (a failed commit
    /// is the failure).
    pub(crate) async fn finish<T>(
        mut self,
        result: Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let commit = result.is_ok();
        let ended = match &mut self {
            #[cfg(feature = "postgres")]
            OwnTx::Pg(t) => t.end(commit).await,
            #[cfg(feature = "mysql")]
            OwnTx::My(t) => t.end(commit).await,
        };
        match result {
            Ok(value) => ended.map(|()| value),
            Err(e) => Err(e),
        }
    }
}

/// Where a store call runs: in the caller's transaction, or in one of its own.
pub(crate) enum Scope<'t> {
    Joined(Conn<'t>),
    Own(OwnTx),
}

impl Scope<'_> {
    pub(crate) fn conn(&mut self) -> Conn<'_> {
        match self {
            Scope::Joined(c) => c.reborrow(),
            Scope::Own(t) => t.conn(),
        }
    }

    /// Commits the store's own transaction (when it ran in one) on success, else rolls it back;
    /// the caller's transaction is the caller's to end.
    pub(crate) async fn finish<T>(self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        match self {
            Scope::Joined(_) => result,
            Scope::Own(t) => t.finish(result).await,
        }
    }
}

impl Conn<'_> {
    pub(crate) fn reborrow(&mut self) -> Conn<'_> {
        Conn {
            c: match &mut self.c {
                #[cfg(feature = "postgres")]
                ConnRef::Pg(c) => ConnRef::Pg(c),
                #[cfg(feature = "mysql")]
                ConnRef::My(c) => ConnRef::My(c),
            },
            session_changed: &mut *self.session_changed,
        }
    }
}

impl Db {
    pub(crate) fn new(pool: Pool, clock: Arc<RwLock<Clock>>) -> Db {
        Db { pool, clock }
    }

    pub(crate) fn dialect(&self) -> Dialect {
        match self.pool {
            #[cfg(feature = "postgres")]
            Pool::Pg(_) => Dialect::Postgres,
            #[cfg(feature = "mysql")]
            Pool::My(_) => Dialect::MySql,
        }
    }

    /// The application clock's now, epoch milliseconds.
    pub(crate) fn now(&self) -> i64 {
        self.clock
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .now_ms()
    }

    /// A transaction at the database's default isolation (the service's own transactions, and
    /// what a store call outside one runs in).
    pub(crate) async fn begin(&self) -> Result<OwnTx, StoreError> {
        Ok(match &self.pool {
            #[cfg(feature = "postgres")]
            Pool::Pg(p) => OwnTx::Pg(Held::<Postgres>::begin(p, BEGIN).await?),
            #[cfg(feature = "mysql")]
            Pool::My(p) => OwnTx::My(Held::<MySql>::begin(p, START).await?),
        })
    }

    /// A transaction of the store's own at READ COMMITTED.
    pub(crate) async fn own(&self) -> Result<OwnTx, StoreError> {
        Ok(match &self.pool {
            #[cfg(feature = "postgres")]
            Pool::Pg(p) => {
                OwnTx::Pg(Held::<Postgres>::begin(p, "BEGIN ISOLATION LEVEL READ COMMITTED").await?)
            }
            // SET TRANSACTION applies to the next transaction only: the one started right after.
            #[cfg(feature = "mysql")]
            Pool::My(p) => OwnTx::My(
                Held::<MySql>::begin(
                    p,
                    "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION",
                )
                .await?,
            ),
        })
    }

    /// The caller's transaction when `tx` holds an open one of this store's, else a short one of
    /// its own (a [`Tx::none`], or a transaction that ended). A transaction of another store is
    /// refused: the writes would not commit or roll back with it.
    pub(crate) async fn scope<'t>(&self, tx: &'t mut Tx) -> Result<Scope<'t>, StoreError> {
        if tx.is_none() {
            return Ok(Scope::Own(self.begin().await?));
        }
        let dialect = self.dialect();
        let conn = match dialect {
            #[cfg(feature = "postgres")]
            Dialect::Postgres => tx.get_mut::<Held<Postgres>>().map(|held| held.conn()),
            #[cfg(feature = "mysql")]
            Dialect::MySql => tx.get_mut::<Held<MySql>>().map(|held| held.conn()),
        };
        match conn {
            Some(Some(conn)) => Ok(Scope::Joined(conn)),
            // Ended: what is written now commits on its own.
            Some(None) => Ok(Scope::Own(self.begin().await?)),
            None => Err(StoreError::Failed(MessagingError::Storage(format!(
                "the SQL message store ({}) was given a transaction that is not one of its own: \
                 the writes would not commit or roll back with it",
                dialect.name()
            )))),
        }
    }
}

pub(crate) fn storage(message: String) -> StoreError {
    StoreError::Failed(MessagingError::Storage(message))
}

/// What a database error means to the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    /// A duplicate key (PostgreSQL 23505; MySQL 1062, 1586).
    Duplicate,
    /// A serialization failure or a deadlock (PostgreSQL 40001, 40P01; MySQL 1213).
    Transient,
    /// A lock not available at once or within its wait (PostgreSQL 55P03; MySQL 1205, 3572).
    LockNotAvailable,
    Other,
}

pub(crate) fn error_kind(e: &sqlx::Error) -> ErrorKind {
    let sqlx::Error::Database(db) = e else {
        return ErrorKind::Other;
    };
    #[cfg(feature = "postgres")]
    if let Some(pg) = db.try_downcast_ref::<PgDatabaseError>() {
        return match pg.code() {
            "23505" => ErrorKind::Duplicate,
            "40001" | "40P01" => ErrorKind::Transient,
            "55P03" => ErrorKind::LockNotAvailable,
            _ => ErrorKind::Other,
        };
    }
    #[cfg(feature = "mysql")]
    if let Some(my) = db.try_downcast_ref::<MySqlDatabaseError>() {
        return match my.number() {
            1062 | 1586 => ErrorKind::Duplicate,
            1213 => ErrorKind::Transient,
            1205 | 3572 => ErrorKind::LockNotAvailable,
            _ => ErrorKind::Other,
        };
    }
    ErrorKind::Other
}

/// A database error as a store error: a duplicate key, a serialization failure, a deadlock or a
/// lock not available is a [`StoreError::Conflict`] (a retry of the whole attempt resolves it).
pub(crate) fn store_error(e: sqlx::Error) -> StoreError {
    match error_kind(&e) {
        ErrorKind::Other => storage(e.to_string()),
        _ => StoreError::Conflict(e.to_string()),
    }
}
