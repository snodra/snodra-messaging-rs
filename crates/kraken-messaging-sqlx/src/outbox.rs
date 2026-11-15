//! `kraken_outbox` and `kraken_processed_events`.
//!
//! An append joins the caller's transaction (and, on PostgreSQL with notify on, runs
//! `pg_notify('kraken_outbox', '')` in it: delivered at the commit, dropped by a rollback). A claim
//! runs in a transaction of its own: `SELECT ... FOR UPDATE SKIP LOCKED` in `(created_at, seq)`
//! order picks the entries free, already the owner's or whose lease passed, then `owner` and
//! `lease_until` mark them the owner's; a concurrent claim skips the rows this one locked and,
//! once it committed, no longer matches them.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use kraken_messaging::event_store::ReplayRange;
use kraken_messaging::store::{
    AppendListener, OutboxEntry, OutboxStore, ProcessedEventStore, ProcessedMarker, StoreError, Tx,
};

use crate::db::{Conn, Db, Dialect, Row, Stmt, placeholders};
#[cfg(feature = "mysql")]
use crate::db::{ErrorKind, error_kind, store_error};
use crate::limits;

/// The relays' claim: the entries free, already the owner's or whose lease passed, in order,
/// skipping those another claim locked. At READ COMMITTED a locking read on MySQL still locks each
/// row it scans until it has evaluated the row's filter (then releases the ones that do not
/// match): for that instant a concurrent claim can skip a row it could have taken, which it then
/// takes at its next claim.
const CLAIM: &str = "SELECT id, type, event_key, CAST(headers AS {text}) AS headers, \
     CAST(payload AS {text}) AS payload, created_at, seq, attempts FROM kraken_outbox \
     WHERE sent_at IS NULL AND failed_at IS NULL AND (owner IS NULL OR owner = ? OR lease_until <= ?) \
     ORDER BY created_at, seq, id LIMIT ? FOR UPDATE SKIP LOCKED";

/// The notification of an append, in its transaction (PostgreSQL).
#[cfg(feature = "postgres")]
const NOTIFY: &str = "SELECT pg_notify('kraken_outbox', '')";

pub(crate) struct SqlOutboxStore {
    db: Arc<Db>,
    #[cfg(feature = "postgres")]
    notifications: Option<Arc<crate::notify::Notifications>>,
}

impl SqlOutboxStore {
    /// The outbox of `db`; on PostgreSQL with `notify`, appends notify and relays listen.
    pub(crate) fn new(db: Arc<Db>, notify: bool) -> SqlOutboxStore {
        #[cfg(feature = "postgres")]
        let notifications = match &db.pool {
            crate::db::Pool::Pg(pool) if notify => Some(crate::notify::Notifications::new(pool)),
            _ => None,
        };
        #[cfg(not(feature = "postgres"))]
        let _ = notify;
        SqlOutboxStore {
            db,
            #[cfg(feature = "postgres")]
            notifications,
        }
    }

    pub(crate) fn listens(&self) -> u64 {
        #[cfg(feature = "postgres")]
        if let Some(n) = &self.notifications {
            return n.listens();
        }
        0
    }

    async fn insert(&self, conn: &mut Conn<'_>, entry: &OutboxEntry) -> Result<(), StoreError> {
        limits::check("an outbox entry's id", &entry.id, limits::ID)?;
        limits::check("an outbox entry's type", &entry.type_name, limits::NAME)?;
        limits::require_object(&entry.type_name, &entry.payload)?;
        let headers = limits::headers_json("an outbox entry's headers", &entry.headers)?;
        let payload = String::from_utf8(entry.payload.clone()).map_err(|e| {
            invalid(format!(
                "the payload of event {} is not UTF-8: {e}",
                entry.type_name
            ))
        })?;
        conn.execute(
            Stmt::new(
                "INSERT INTO kraken_outbox (id, type, event_key, headers, payload, created_at, seq, attempts) \
                 VALUES (?, ?, ?, CAST(? AS {json}), CAST(? AS {json}), ?, ?, ?)",
            )
            .arg("id", &entry.id)
            .arg("type", &entry.type_name)
            .arg("key", &entry.key)
            .arg("headers", headers)
            .arg("payload", payload)
            .time("createdAt", entry.created_at_ms)
            .arg("seq", entry.seq)
            .arg("attempts", entry.attempts),
        )
        .await?;
        #[cfg(feature = "postgres")]
        if self.notifications.is_some() && conn.dialect() == Dialect::Postgres {
            conn.fetch_all(Stmt::new(NOTIFY)).await?;
        }
        Ok(())
    }

    async fn update(&self, stmt: Stmt) -> Result<u64, StoreError> {
        let mut t = self.db.begin().await?;
        let result = t.conn().execute(stmt).await;
        t.finish(result).await
    }
}

fn invalid(message: String) -> StoreError {
    StoreError::Failed(kraken_messaging::error::MessagingError::InvalidMessage(
        message,
    ))
}

fn read_entry(row: &Row) -> Result<OutboxEntry, StoreError> {
    Ok(OutboxEntry {
        id: row.text("id")?,
        type_name: row.text("type")?,
        key: row.opt_text("event_key")?,
        headers: limits::strings(row.opt_text("headers")?.as_deref())?,
        payload: row.text("payload")?.into_bytes(),
        created_at_ms: row.ms("created_at")?,
        seq: row.int("seq")?,
        attempts: row.int32("attempts")?,
    })
}

impl OutboxStore for SqlOutboxStore {
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx,
        entry: OutboxEntry,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = self.insert(&mut scope.conn(), &entry).await;
            scope.finish(result).await
        })
    }

    fn append_now(&self, entry: OutboxEntry) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = self.insert(&mut t.conn(), &entry).await;
            t.finish(result).await
        })
    }

    fn claim<'a>(
        &'a self,
        owner: &'a str,
        lease: Duration,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<OutboxEntry>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = async {
                let mut conn = t.conn();
                let now = self.db.now();
                let rows = conn
                    .fetch_all(
                        Stmt::new(CLAIM)
                            .arg("owner", owner)
                            .time("now", now)
                            .arg("limit", i64::try_from(limit).unwrap_or(i64::MAX)),
                    )
                    .await?;
                let entries = rows.iter().map(read_entry).collect::<Result<Vec<_>, _>>()?;
                if !entries.is_empty() {
                    let lease_ms = i64::try_from(lease.as_millis()).unwrap_or(i64::MAX);
                    conn.execute(
                        Stmt::built(format!(
                            "UPDATE kraken_outbox SET owner = ?, lease_until = ? WHERE id IN ({})",
                            placeholders(entries.len())
                        ))
                        .arg("owner", owner)
                        .time("until", now.saturating_add(lease_ms))
                        .args("ids", entries.iter().map(|e| e.id.as_str())),
                    )
                    .await?;
                }
                Ok(entries)
            }
            .await;
            t.finish(result).await
        })
    }

    fn mark_sent<'a>(&'a self, ids: &'a [String]) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if ids.is_empty() {
                return Ok(());
            }
            self.update(
                Stmt::built(format!(
                    "UPDATE kraken_outbox SET sent_at = ? WHERE id IN ({})",
                    placeholders(ids.len())
                ))
                .time("now", self.db.now())
                .args("ids", ids),
            )
            .await?;
            Ok(())
        })
    }

    fn mark_attempt_failed<'a>(
        &'a self,
        id: &'a str,
        error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.update(
                Stmt::new(
                    "UPDATE kraken_outbox SET attempts = attempts + 1, last_error = ? WHERE id = ?",
                )
                .arg("error", limits::cut(error))
                .arg("id", id),
            )
            .await?;
            Ok(())
        })
    }

    fn mark_failed<'a>(
        &'a self,
        id: &'a str,
        error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.update(
                Stmt::new(
                    "UPDATE kraken_outbox SET attempts = attempts + 1, last_error = ?, failed_at = ? WHERE id = ?",
                )
                .arg("error", limits::cut(error))
                .time("now", self.db.now())
                .arg("id", id),
            )
            .await?;
            Ok(())
        })
    }

    fn purge(&self, sent_before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(crate::purge::batched(
            &self.db,
            "kraken_outbox",
            "id",
            "sent_at < ?",
            sent_before_ms,
        ))
    }

    fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = async {
                let row = t
                    .conn()
                    .fetch_one(Stmt::new(
                        "SELECT COUNT(*) AS n FROM kraken_outbox WHERE sent_at IS NULL AND failed_at IS NULL",
                    ))
                    .await?;
                Ok(u64::try_from(row.int("n")?).unwrap_or(0))
            }
            .await;
            t.finish(result).await
        })
    }

    /// Registers `wake` with the PostgreSQL notifications; nothing on MySQL or with notify off
    /// (the relay polls).
    fn on_append(&self, wake: Arc<dyn Fn() + Send + Sync>) -> AppendListener {
        #[cfg(feature = "postgres")]
        if let Some(n) = &self.notifications {
            return n.register(wake);
        }
        let _ = wake;
        AppendListener::none()
    }

    fn oldest_pending(&self) -> BoxFuture<'_, Result<Option<i64>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = async {
                t.conn()
                    .fetch_optional(Stmt::new(
                        "SELECT created_at FROM kraken_outbox WHERE sent_at IS NULL AND failed_at IS NULL \
                         ORDER BY created_at, seq LIMIT 1",
                    ))
                    .await?
                    .map(|row| row.ms("created_at"))
                    .transpose()
            }
            .await;
            t.finish(result).await
        })
    }
}

/// `kraken_processed_events`: one row per marker id (by its hash), written in the caller's
/// transaction. A stage-1 marker also keeps its processor, source, segment and position (and the
/// position as a number when it is a decimal one, for resets by range).
pub(crate) struct SqlProcessedEventStore {
    db: Arc<Db>,
}

impl SqlProcessedEventStore {
    pub(crate) fn new(db: Arc<Db>) -> SqlProcessedEventStore {
        SqlProcessedEventStore { db }
    }
}

/// Inserts a row unless one with its key exists; false when it existed. On PostgreSQL without an
/// error (`ON CONFLICT ... DO NOTHING`: a duplicate key would abort the caller's transaction), on
/// MySQL by the duplicate-key error (which leaves the transaction usable).
pub(crate) async fn insert_once(
    conn: &mut Conn<'_>,
    insert: &'static str,
    pg_conflict: &'static str,
    build: impl Fn(Stmt) -> Stmt,
) -> Result<bool, StoreError> {
    match conn.dialect() {
        #[cfg(feature = "postgres")]
        Dialect::Postgres => {
            let sql = format!("{insert} {pg_conflict}");
            Ok(conn.execute(build(Stmt::built(sql))).await? == 1)
        }
        #[cfg(feature = "mysql")]
        Dialect::MySql => {
            let _ = pg_conflict;
            match conn.try_execute(build(Stmt::new(insert))).await? {
                Ok(_) => Ok(true),
                Err(e) if error_kind(&e) == ErrorKind::Duplicate => Ok(false),
                Err(e) => Err(store_error(e)),
            }
        }
    }
}

/// Whether the marker `id` is recorded (by its hash).
pub(crate) async fn marker_exists(conn: &mut Conn<'_>, id: &str) -> Result<bool, StoreError> {
    let row = conn
        .fetch_one(
            Stmt::new("SELECT COUNT(*) AS n FROM kraken_processed_events WHERE id_hash = ?")
                .arg("idHash", limits::sha256(id)),
        )
        .await?;
    Ok(row.int("n")? > 0)
}

/// A bound on a position compares only positions of its kind: decimal ones by value, the others
/// by their bytes. The SQL fragment and its parameter.
pub(crate) fn position_bound(op: &str, position: &str) -> (String, crate::db::Arg) {
    match limits::position_number(position) {
        Some(n) => (format!("position_num {op} ?"), n.into()),
        None => (
            format!("position_num IS NULL AND position {op} ?"),
            position.into(),
        ),
    }
}

impl ProcessedEventStore for SqlProcessedEventStore {
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        marker: &'a ProcessedMarker,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            limits::check_opt(
                "a marker's processor",
                marker.processor.as_deref(),
                limits::PROCESSOR,
            )?;
            limits::check_opt(
                "a marker's source",
                marker.source.as_deref(),
                limits::SOURCE,
            )?;
            limits::check_opt(
                "a marker's segment",
                marker.segment.as_deref(),
                limits::SEGMENT,
            )?;
            limits::check_opt(
                "a marker's position",
                marker.position.as_deref(),
                limits::POSITION,
            )?;
            let mut scope = self.db.scope(tx).await?;
            let result = insert_once(
                &mut scope.conn(),
                "INSERT INTO kraken_processed_events \
                 (id_hash, id, type, processed_at, processor, source, segment, position, position_num) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                "ON CONFLICT (id_hash) DO NOTHING",
                |s| {
                    s.arg("idHash", limits::sha256(&marker.id))
                        .arg("id", &marker.id)
                        .arg("type", &marker.type_name)
                        .time("processedAt", marker.processed_at_ms)
                        .arg("processor", &marker.processor)
                        .arg("source", &marker.source)
                        .arg("segment", &marker.segment)
                        .arg("position", &marker.position)
                        .arg(
                            "positionNum",
                            marker.position.as_deref().and_then(limits::position_number),
                        )
                },
            )
            .await;
            scope.finish(result).await
        })
    }

    fn exists<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = marker_exists(&mut t.conn(), id).await;
            t.finish(result).await
        })
    }

    fn delete_markers<'a>(
        &'a self,
        processor: &'a str,
        ranges: &'a [ReplayRange],
    ) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            if ranges.is_empty() {
                return Ok(0);
            }
            let mut t = self.db.own().await?;
            let result = async {
                let mut deleted = 0;
                for range in ranges {
                    let (from, from_arg) = position_bound(">=", &range.from);
                    let (until, until_arg) = position_bound("<", &range.until);
                    deleted += t
                        .conn()
                        .execute(
                            Stmt::built(format!(
                                "DELETE FROM kraken_processed_events WHERE processor = ? AND source = ? \
                                 AND segment = ? AND {from} AND {until}"
                            ))
                            .arg("processor", processor)
                            .arg("source", &range.source)
                            .arg("segment", &range.segment)
                            .arg("from", from_arg)
                            .arg("until", until_arg),
                        )
                        .await?;
                }
                Ok(deleted)
            }
            .await;
            t.finish(result).await
        })
    }

    fn purge(&self, before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(crate::purge::batched(
            &self.db,
            "kraken_processed_events",
            "id_hash",
            "processed_at < ?",
            before_ms,
        ))
    }
}
