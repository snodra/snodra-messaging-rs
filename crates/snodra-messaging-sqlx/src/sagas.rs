//! The saga ports over `snodra_sagas` (+ `snodra_saga_associations`), `snodra_saga_dispatch`,
//! `snodra_saga_dispatch_leases`, `snodra_deadlines`, `snodra_saga_failures` and
//! `snodra_processed_events`. What the runtime calls joins its transaction; the relays' and the
//! scheduler's claims (notices, leases, deadlines) commit on their own.
//!
//! Two transactions writing one saga never wait long for each other: the loser fails with a
//! [`StoreError::Conflict`] the saga runtime retries (a database would otherwise make it wait for
//! the winner's commit). An update locks the saga's row with `FOR UPDATE NOWAIT`. A start (an
//! insert with a live start key) takes, on PostgreSQL, the transaction-scoped advisory lock
//! `pg_try_advisory_xact_lock(1397638226, hashtext(start_key_hash))` (another transaction's insert
//! is invisible until it commits); on MySQL the insert itself meets the other transaction's
//! uncommitted row in the unique index on `live_start_key_hash` and waits for it at most
//! [`MYSQL_START_WAIT_SECONDS`] (`innodb_lock_wait_timeout`, for that statement): no locking read
//! on a missing row, so no gap lock, so no deadlock between starts of different keys. Messages
//! name a saga's type and a key's hash, never a start key or an association (personal data).

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use snodra_messaging::saga::FailureReason;
use snodra_messaging::saga::store::{
    Association, DeadlineDocument, DeadlineStatus, DispatchKind, SagaDispatchDocument,
    SagaDocument, SagaFailureDocument,
};
use snodra_messaging::store::{
    DeadlineStore, DispatchFailure, FailureStore, MarkerStore, SagaDispatchStore, SagaRepository,
    StoreError, Tx,
};

use crate::db::{
    Conn, Db, Dialect, ErrorKind, Row, Stmt, error_kind, placeholders, storage, store_error,
};
use crate::limits;
use crate::outbox::marker_exists;

/// The first key of the transaction-scoped advisory lock that serialises the start of sagas per
/// start key on PostgreSQL (0x534E_4452 = 1397638226, ASCII "SNDR"; the same in every
/// library).
#[cfg(feature = "postgres")]
const SAGA_START_LOCK: i32 = 0x534E_4452;

/// How long a saga start waits on MySQL for another transaction's uncommitted start with the same
/// key (then a conflict, retried).
#[cfg(feature = "mysql")]
const MYSQL_START_WAIT_SECONDS: i64 = 1;

const SAGA_COLUMNS: &str =
    "id, type, state, start_key, ended, ended_at, version, created_at, updated_at";
const DISPATCH_COLUMNS: &str = "id, saga_id, seq, kind, type, payload, CAST(headers AS {text}) AS headers, \
     routing_key, created_at, dispatched_at, attempts, next_attempt_at, failed_at, failure_reason, \
     failure_message, notice_pending, notice_attempts, notice_next_at";
const DEADLINE_COLUMNS: &str = "id, saga_type, saga_id, name, payload, payload_type, due_at, \
     correlation_id, status, claimed_by, claimed_until, finished_at";
const FAILURE_COLUMNS: &str = "id, saga_type, event_id, event_type, payload, \
     CAST(headers AS {text}) AS headers, error, attempts, first_failed_at, last_failed_at";

/// Every saga port.
pub(crate) struct SqlSagaStore {
    db: Arc<Db>,
}

impl SqlSagaStore {
    pub(crate) fn new(db: Arc<Db>) -> SqlSagaStore {
        SqlSagaStore { db }
    }

    /// `stmt` in a short transaction of its own; the rows it changed.
    async fn own_update(&self, stmt: Stmt) -> Result<u64, StoreError> {
        let mut t = self.db.begin().await?;
        let result = t.conn().execute(stmt).await;
        t.finish(result).await
    }
}

/// An enum's stored name, as serde writes it.
fn enum_name<T: serde::Serialize>(value: &T) -> Result<String, StoreError> {
    Ok(serde_json::to_value(value)?
        .as_str()
        .unwrap_or_default()
        .to_string())
}

fn enum_of<T: serde::de::DeserializeOwned>(name: &str) -> Result<T, StoreError> {
    serde_json::from_value(serde_json::Value::String(name.to_string()))
        .map_err(|_| storage(format!("unknown stored name {name}")))
}

fn read_saga(row: &Row) -> Result<SagaDocument, StoreError> {
    Ok(SagaDocument {
        id: row.text("id")?,
        type_name: row.text("type")?,
        state: row.text("state")?,
        associations: Vec::new(),
        start_key: row.opt_text("start_key")?,
        ended: row.bool("ended")?,
        ended_at_ms: row.opt_ms("ended_at")?,
        version: row.int("version")?,
        created_at_ms: row.ms("created_at")?,
        updated_at_ms: row.ms("updated_at")?,
    })
}

fn read_dispatch(row: &Row) -> Result<SagaDispatchDocument, StoreError> {
    Ok(SagaDispatchDocument {
        id: row.text("id")?,
        saga_id: row.text("saga_id")?,
        seq: row.int("seq")?,
        kind: enum_of::<DispatchKind>(&row.text("kind")?)?,
        type_name: row.text("type")?,
        payload: row.text("payload")?,
        headers: limits::strings(row.opt_text("headers")?.as_deref())?,
        routing_key: row.opt_text("routing_key")?,
        created_at_ms: row.ms("created_at")?,
        dispatched_at_ms: row.opt_ms("dispatched_at")?,
        attempts: row.int32("attempts")?,
        next_attempt_at_ms: row.opt_ms("next_attempt_at")?,
        failed_at_ms: row.opt_ms("failed_at")?,
        failure_reason: row
            .opt_text("failure_reason")?
            .map(|r| enum_of::<FailureReason>(&r))
            .transpose()?,
        failure_message: row.opt_text("failure_message")?,
        notice_pending: row.opt_bool("notice_pending")?,
        notice_attempts: row.opt_int32("notice_attempts")?,
        notice_next_at_ms: row.opt_ms("notice_next_at")?,
    })
}

/// A deadline's `correlation_id` is NOT NULL in the shared layout: a deadline
/// without one is stored as the empty text, read back as none.
fn read_deadline(row: &Row) -> Result<DeadlineDocument, StoreError> {
    Ok(DeadlineDocument {
        id: row.text("id")?,
        saga_type: row.text("saga_type")?,
        saga_id: row.text("saga_id")?,
        name: row.text("name")?,
        payload: row.opt_text("payload")?,
        payload_type: row.opt_text("payload_type")?,
        due_at_ms: row.ms("due_at")?,
        status: enum_of::<DeadlineStatus>(&row.text("status")?)?,
        claimed_by: row.opt_text("claimed_by")?,
        claimed_until_ms: row.opt_ms("claimed_until")?,
        finished_at_ms: row.opt_ms("finished_at")?,
        correlation_id: row.opt_text("correlation_id")?.filter(|c| !c.is_empty()),
    })
}

fn read_failure(row: &Row) -> Result<SagaFailureDocument, StoreError> {
    Ok(SagaFailureDocument {
        id: row.text("id")?,
        saga_type: row.text("saga_type")?,
        event_id: row.text("event_id")?,
        event_type: row.text("event_type")?,
        payload: row.text("payload")?,
        headers: limits::strings(row.opt_text("headers")?.as_deref())?,
        error: row.text("error")?,
        attempts: row.int32("attempts")?,
        first_failed_at_ms: row.ms("first_failed_at")?,
        last_failed_at_ms: row.ms("last_failed_at")?,
    })
}

fn check_saga(saga: &SagaDocument) -> Result<(), StoreError> {
    limits::check("a saga's id", &saga.id, limits::ID)?;
    limits::check("a saga's type", &saga.type_name, limits::NAME)
}

/// The associations of `ids`, in order, by saga id.
async fn associations(
    conn: &mut Conn<'_>,
    ids: &[String],
) -> Result<HashMap<String, Vec<Association>>, StoreError> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = conn
        .fetch_all(
            Stmt::built(format!(
                "SELECT saga_id, assoc_key, assoc_value FROM snodra_saga_associations \
                 WHERE saga_id IN ({}) ORDER BY saga_id, idx",
                placeholders(ids.len())
            ))
            .args("ids", ids),
        )
        .await?;
    let mut by_id: HashMap<String, Vec<Association>> = HashMap::new();
    for row in &rows {
        by_id
            .entry(row.text("saga_id")?)
            .or_default()
            .push(Association {
                k: row.text("assoc_key")?,
                v: row.text("assoc_value")?,
            });
    }
    Ok(by_id)
}

async fn with_associations(
    conn: &mut Conn<'_>,
    mut sagas: Vec<SagaDocument>,
) -> Result<Vec<SagaDocument>, StoreError> {
    let ids: Vec<String> = sagas.iter().map(|s| s.id.clone()).collect();
    let mut by_id = associations(conn, &ids).await?;
    for saga in &mut sagas {
        saga.associations = by_id.remove(&saga.id).unwrap_or_default();
    }
    Ok(sagas)
}

async fn insert_associations(conn: &mut Conn<'_>, saga: &SagaDocument) -> Result<(), StoreError> {
    if saga.associations.is_empty() {
        return Ok(());
    }
    let rows = vec!["(?, ?, ?, ?, ?)"; saga.associations.len()].join(", ");
    let mut stmt = Stmt::built(format!(
        "INSERT INTO snodra_saga_associations (saga_id, idx, assoc_key, assoc_value, assoc_hash) VALUES {rows}"
    ));
    for (i, a) in saga.associations.iter().enumerate() {
        stmt = stmt
            .arg("sagaId", &saga.id)
            .arg("idx", i32::try_from(i).unwrap_or(i32::MAX))
            .arg("key", &a.k)
            .arg("value", &a.v)
            .arg("hash", limits::association_hash(&a.k, &a.v));
    }
    conn.execute(stmt).await?;
    Ok(())
}

fn saga_values(stmt: Stmt, saga: &SagaDocument) -> Stmt {
    stmt.arg("type", &saga.type_name)
        .arg("state", &saga.state)
        .arg("startKey", &saga.start_key)
        .arg(
            "startKeyHash",
            saga.start_key.as_deref().map(limits::sha256),
        )
        .arg("ended", saga.ended)
        .opt_time("endedAt", saga.ended_at_ms)
        .arg("version", saga.version)
        .time("createdAt", saga.created_at_ms)
        .time("updatedAt", saga.updated_at_ms)
}

/// The insert of a saga, on MySQL with `innodb_lock_wait_timeout` at
/// [`MYSQL_START_WAIT_SECONDS`] for that statement, then the session's value put back. A timeout
/// rolls back that statement only (`innodb_rollback_on_timeout` OFF, MySQL's default; ON rolls
/// back the whole transaction, still a retryable conflict).
async fn insert_saga_row(
    conn: &mut Conn<'_>,
    saga: &SagaDocument,
    live: bool,
) -> Result<Result<u64, sqlx::Error>, StoreError> {
    let insert = || {
        saga_values(
            Stmt::new(
                "INSERT INTO snodra_sagas (id, type, state, start_key, start_key_hash, ended, ended_at, \
                 version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            // The id first: `saga_values` binds from `type` on.
            .arg("id", &saga.id),
            saga,
        )
    };
    match conn.dialect() {
        #[cfg(feature = "mysql")]
        Dialect::MySql if live => {
            let previous = conn
                .fetch_one(Stmt::new(
                    "SELECT CAST(@@SESSION.innodb_lock_wait_timeout AS SIGNED) AS t",
                ))
                .await?
                .int("t")?;
            // Until it is put back, a transaction ending (or dropped) closes the connection
            // rather than return it to the pool with a 1 s lock wait.
            conn.session_changed(true);
            conn.execute(
                Stmt::new("SET SESSION innodb_lock_wait_timeout = ?")
                    .arg("seconds", MYSQL_START_WAIT_SECONDS),
            )
            .await?;
            let inserted = conn.try_execute(insert()).await;
            let restored = conn
                .execute(
                    Stmt::new("SET SESSION innodb_lock_wait_timeout = ?").arg("seconds", previous),
                )
                .await;
            if restored.is_ok() {
                conn.session_changed(false);
            }
            match (inserted, restored) {
                // The statement's own failure first: a failure to put the value back never hides
                // it (it is logged).
                (Err(refused), Err(restore)) => {
                    tracing::warn!("could not restore innodb_lock_wait_timeout: {restore}");
                    Err(refused)
                }
                (Ok(Err(e)), Err(restore)) => {
                    tracing::warn!("could not restore innodb_lock_wait_timeout: {restore}");
                    Ok(Err(e))
                }
                (inserted, Ok(_)) => inserted,
                (Ok(Ok(_)), Err(restore)) => Err(restore),
            }
        }
        _ => {
            let _ = live;
            conn.try_execute(insert()).await
        }
    }
}

impl SagaRepository for SqlSagaStore {
    fn find_by_id<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDocument>, StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = async {
                let mut conn = scope.conn();
                let Some(row) = conn
                    .fetch_optional(
                        Stmt::built(format!(
                            "SELECT {SAGA_COLUMNS} FROM snodra_sagas WHERE id = ?"
                        ))
                        .arg("id", id),
                    )
                    .await?
                else {
                    return Ok(None);
                };
                Ok(with_associations(&mut conn, vec![read_saga(&row)?])
                    .await?
                    .pop())
            }
            .await;
            scope.finish(result).await
        })
    }

    fn find_live<'a>(
        &'a self,
        tx: &'a mut Tx,
        type_name: &'a str,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<Vec<SagaDocument>, StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = async {
                let mut conn = scope.conn();
                let rows = conn
                    .fetch_all(
                        Stmt::new(
                            "SELECT s.id, s.type, s.state, s.start_key, s.ended, s.ended_at, s.version, \
                             s.created_at, s.updated_at FROM snodra_sagas s WHERE s.type = ? AND s.ended = FALSE \
                             AND EXISTS (SELECT 1 FROM snodra_saga_associations a WHERE a.saga_id = s.id \
                             AND a.assoc_hash = ? AND a.assoc_key = ? AND a.assoc_value = ?) \
                             ORDER BY s.created_at, s.id",
                        )
                        .arg("type", type_name)
                        .arg("hash", limits::association_hash(key, value))
                        .arg("key", key)
                        .arg("value", value),
                    )
                    .await?;
                let sagas = rows.iter().map(read_saga).collect::<Result<Vec<_>, _>>()?;
                with_associations(&mut conn, sagas).await
            }
            .await;
            scope.finish(result).await
        })
    }

    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            check_saga(saga)?;
            let mut scope = self.db.scope(tx).await?;
            let result = async {
                let mut conn = scope.conn();
                let live = saga.start_key.is_some() && !saga.ended;
                let key_hash = saga.start_key.as_deref().map(limits::sha256);
                #[cfg(feature = "postgres")]
                if live && conn.dialect() == Dialect::Postgres {
                    let got = conn
                        .fetch_one(
                            Stmt::new("SELECT pg_try_advisory_xact_lock(?, hashtext(?)) AS got")
                                .arg("class", SAGA_START_LOCK)
                                .arg("hash", &key_hash),
                        )
                        .await?
                        .bool("got")?;
                    if !got {
                        return Err(StoreError::Conflict(format!(
                            "sagas: another transaction is starting a saga of type {} with the start key {} (hash)",
                            saga.type_name,
                            key_hash.as_deref().unwrap_or_default()
                        )));
                    }
                }
                match insert_saga_row(&mut conn, saga, live).await? {
                    Ok(_) => {}
                    Err(e) if live && error_kind(&e) == ErrorKind::Duplicate => {
                        return Err(StoreError::Conflict(format!(
                            "sagas: a live saga of type {} holds the start key {} (hash)",
                            saga.type_name,
                            key_hash.as_deref().unwrap_or_default()
                        )));
                    }
                    Err(e) if live && error_kind(&e) == ErrorKind::LockNotAvailable => {
                        return Err(StoreError::Conflict(format!(
                            "sagas: another transaction is starting a saga of type {} with the start key {} (hash)",
                            saga.type_name,
                            key_hash.as_deref().unwrap_or_default()
                        )));
                    }
                    Err(e) => return Err(store_error(e)),
                }
                insert_associations(&mut conn, saga).await
            }
            .await;
            scope.finish(result).await
        })
    }

    fn update<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga: &'a SagaDocument,
        expected_version: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            check_saga(saga)?;
            let mut scope = self.db.scope(tx).await?;
            let result = async {
                let mut conn = scope.conn();
                let stored = conn
                    .fetch_optional(
                        Stmt::new("SELECT version FROM snodra_sagas WHERE id = ? FOR UPDATE NOWAIT")
                            .arg("id", &saga.id),
                    )
                    .await?
                    .map(|row| row.int("version"))
                    .transpose()?;
                if stored != Some(expected_version) {
                    return Ok(false);
                }
                conn.execute(
                    saga_values(
                        Stmt::new(
                            "UPDATE snodra_sagas SET type = ?, state = ?, start_key = ?, start_key_hash = ?, \
                             ended = ?, ended_at = ?, version = ?, created_at = ?, updated_at = ? \
                             WHERE id = ? AND version = ?",
                        ),
                        saga,
                    )
                    .arg("id", &saga.id)
                    .arg("expected", expected_version),
                )
                .await?;
                let stored = associations(&mut conn, std::slice::from_ref(&saga.id))
                    .await?
                    .remove(&saga.id)
                    .unwrap_or_default();
                if stored != saga.associations {
                    conn.execute(
                        Stmt::new("DELETE FROM snodra_saga_associations WHERE saga_id = ?")
                            .arg("id", &saga.id),
                    )
                    .await?;
                    insert_associations(&mut conn, saga).await?;
                }
                Ok(true)
            }
            .await;
            scope.finish(result).await
        })
    }

    fn count_live_by_type(&self) -> BoxFuture<'_, Result<HashMap<String, u64>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = async {
                let rows = t
                    .conn()
                    .fetch_all(Stmt::new(
                        "SELECT type, COUNT(*) AS n FROM snodra_sagas WHERE ended = FALSE GROUP BY type",
                    ))
                    .await?;
                rows.iter()
                    .map(|r| Ok((r.text("type")?, u64::try_from(r.int("n")?).unwrap_or(0))))
                    .collect()
            }
            .await;
            t.finish(result).await
        })
    }
}

impl SqlSagaStore {
    async fn cancel_where(
        &self,
        tx: &mut Tx,
        condition: &'static str,
        args: Vec<&str>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let mut scope = self.db.scope(tx).await?;
        let mut stmt = Stmt::built(format!(
            "UPDATE snodra_deadlines SET status = 'CANCELLED', finished_at = ? WHERE {condition} AND status = 'SCHEDULED'"
        ))
        .time("now", now_ms);
        for arg in args {
            stmt = stmt.arg("condition", arg);
        }
        let result = scope.conn().execute(stmt).await.map(|_| ());
        scope.finish(result).await
    }
}

impl DeadlineStore for SqlSagaStore {
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        deadline: &'a DeadlineDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            limits::check("a deadline's id", &deadline.id, limits::ID)?;
            limits::check("a deadline's saga id", &deadline.saga_id, limits::ID)?;
            limits::check("a deadline's saga type", &deadline.saga_type, limits::NAME)?;
            limits::check("a deadline's name", &deadline.name, limits::NAME)?;
            let mut scope = self.db.scope(tx).await?;
            let result = scope
                .conn()
                .execute(
                    Stmt::new(
                        "INSERT INTO snodra_deadlines (id, saga_type, saga_id, name, payload, payload_type, due_at, \
                         correlation_id, status, claimed_by, claimed_until, finished_at) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    )
                    .arg("id", &deadline.id)
                    .arg("sagaType", &deadline.saga_type)
                    .arg("sagaId", &deadline.saga_id)
                    .arg("name", &deadline.name)
                    .arg("payload", &deadline.payload)
                    .arg("payloadType", &deadline.payload_type)
                    .time("dueAt", deadline.due_at_ms)
                    .arg(
                        "correlationId",
                        deadline.correlation_id.clone().unwrap_or_default(),
                    )
                    .arg("status", enum_name(&deadline.status)?)
                    .arg("claimedBy", &deadline.claimed_by)
                    .opt_time("claimedUntil", deadline.claimed_until_ms)
                    .opt_time("finishedAt", deadline.finished_at_ms),
                )
                .await
                .map(|_| ());
            scope.finish(result).await
        })
    }

    fn find_scheduled<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = async {
                scope
                    .conn()
                    .fetch_optional(
                        Stmt::built(format!(
                            "SELECT {DEADLINE_COLUMNS} FROM snodra_deadlines WHERE id = ? AND status = 'SCHEDULED'"
                        ))
                        .arg("id", id),
                    )
                    .await?
                    .map(|row| read_deadline(&row))
                    .transpose()
            }
            .await;
            scope.finish(result).await
        })
    }

    fn cancel<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        deadline_id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.cancel_where(
            tx,
            "id = ? AND saga_id = ?",
            vec![deadline_id, saga_id],
            now_ms,
        ))
    }

    fn cancel_by_name<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        name: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.cancel_where(tx, "saga_id = ? AND name = ?", vec![saga_id, name], now_ms))
    }

    fn cancel_all<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.cancel_where(tx, "saga_id = ?", vec![saga_id], now_ms))
    }

    fn mark_fired<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = scope
                .conn()
                .execute(
                    Stmt::new(
                        "UPDATE snodra_deadlines SET status = 'FIRED', finished_at = ? WHERE id = ? AND status = 'SCHEDULED'",
                    )
                    .time("now", now_ms)
                    .arg("id", id),
                )
                .await
                .map(|n| n == 1);
            scope.finish(result).await
        })
    }

    /// The scheduled deadline due first that nobody holds, skipping one another claim locked.
    fn claim_due<'a>(
        &'a self,
        owner: &'a str,
        now_ms: i64,
        claimed_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = async {
                let mut conn = t.conn();
                let Some(row) = conn
                    .fetch_optional(
                        Stmt::built(format!(
                            "SELECT {DEADLINE_COLUMNS} FROM snodra_deadlines WHERE status = 'SCHEDULED' AND due_at <= ? \
                             AND (claimed_until IS NULL OR claimed_until < ?) ORDER BY due_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
                        ))
                        .time("now", now_ms)
                        .time("now", now_ms),
                    )
                    .await?
                else {
                    return Ok(None);
                };
                let due = read_deadline(&row)?;
                conn.execute(
                    Stmt::new("UPDATE snodra_deadlines SET claimed_by = ?, claimed_until = ? WHERE id = ?")
                        .arg("owner", owner)
                        .time("until", claimed_until_ms)
                        .arg("id", &due.id),
                )
                .await?;
                Ok(Some(DeadlineDocument {
                    claimed_by: Some(owner.to_string()),
                    claimed_until_ms: Some(claimed_until_ms),
                    ..due
                }))
            }
            .await;
            t.finish(result).await
        })
    }

    fn count_overdue_by_name(
        &self,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<HashMap<String, u64>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = async {
                let rows = t
                    .conn()
                    .fetch_all(
                        Stmt::new(
                            "SELECT name, COUNT(*) AS n FROM snodra_deadlines WHERE status = 'SCHEDULED' \
                             AND due_at <= ? GROUP BY name",
                        )
                        .time("now", now_ms),
                    )
                    .await?;
                rows.iter()
                    .map(|r| Ok((r.text("name")?, u64::try_from(r.int("n")?).unwrap_or(0))))
                    .collect()
            }
            .await;
            t.finish(result).await
        })
    }
}

impl FailureStore for SqlSagaStore {
    fn find<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaFailureDocument>, StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = async {
                scope
                    .conn()
                    .fetch_optional(
                        Stmt::built(format!(
                            "SELECT {FAILURE_COLUMNS} FROM snodra_saga_failures WHERE id_hash = ?"
                        ))
                        .arg("idHash", limits::sha256(id)),
                    )
                    .await?
                    .map(|row| read_failure(&row))
                    .transpose()
            }
            .await;
            scope.finish(result).await
        })
    }

    /// Inserts or replaces the failure in one statement: `INSERT ... ON CONFLICT (id_hash) DO
    /// UPDATE` on PostgreSQL, `INSERT ... ON DUPLICATE KEY UPDATE` on MySQL (an exclusive lock on
    /// a duplicate at once, so two concurrent re-saves never deadlock); never a delete first.
    fn save<'a>(
        &'a self,
        tx: &'a mut Tx,
        failure: &'a SagaFailureDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            limits::check(
                "a saga failure's saga type",
                &failure.saga_type,
                limits::NAME,
            )?;
            let headers = limits::headers_json("a saga failure's headers", &failure.headers)?;
            let upsert = match self.db.dialect() {
                #[cfg(feature = "postgres")]
                Dialect::Postgres => {
                    "ON CONFLICT (id_hash) DO UPDATE SET id = EXCLUDED.id, saga_type = EXCLUDED.saga_type, \
                     event_id = EXCLUDED.event_id, event_type = EXCLUDED.event_type, payload = EXCLUDED.payload, \
                     headers = EXCLUDED.headers, error = EXCLUDED.error, attempts = EXCLUDED.attempts, \
                     first_failed_at = EXCLUDED.first_failed_at, last_failed_at = EXCLUDED.last_failed_at"
                }
                #[cfg(feature = "mysql")]
                Dialect::MySql => {
                    "ON DUPLICATE KEY UPDATE id = VALUES(id), saga_type = VALUES(saga_type), \
                     event_id = VALUES(event_id), event_type = VALUES(event_type), payload = VALUES(payload), \
                     headers = VALUES(headers), error = VALUES(error), attempts = VALUES(attempts), \
                     first_failed_at = VALUES(first_failed_at), last_failed_at = VALUES(last_failed_at)"
                }
            };
            let mut scope = self.db.scope(tx).await?;
            let result = scope
                .conn()
                .execute(
                    Stmt::built(format!(
                        "INSERT INTO snodra_saga_failures (id_hash, id, saga_type, event_id, event_type, payload, \
                         headers, error, attempts, first_failed_at, last_failed_at) \
                         VALUES (?, ?, ?, ?, ?, ?, CAST(? AS {{json}}), ?, ?, ?, ?) {upsert}"
                    ))
                    .arg("idHash", limits::sha256(&failure.id))
                    .arg("id", &failure.id)
                    .arg("sagaType", &failure.saga_type)
                    .arg("eventId", &failure.event_id)
                    .arg("eventType", &failure.event_type)
                    .arg("payload", &failure.payload)
                    .arg("headers", headers)
                    .arg("error", limits::cut(&failure.error))
                    .arg("attempts", failure.attempts)
                    .time("first", failure.first_failed_at_ms)
                    .time("last", failure.last_failed_at_ms),
                )
                .await
                .map(|_| ());
            scope.finish(result).await
        })
    }

    fn delete<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = scope
                .conn()
                .execute(
                    Stmt::new("DELETE FROM snodra_saga_failures WHERE id_hash = ?")
                        .arg("idHash", limits::sha256(id)),
                )
                .await
                .map(|n| n == 1);
            scope.finish(result).await
        })
    }

    fn failed_again<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        error: &'a str,
        attempts: i32,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = scope
                .conn()
                .execute(
                    Stmt::new(
                        "UPDATE snodra_saga_failures SET attempts = attempts + ?, error = ?, last_failed_at = ? \
                         WHERE id_hash = ?",
                    )
                    .arg("attempts", attempts)
                    .arg("error", limits::cut(error))
                    .time("now", now_ms)
                    .arg("idHash", limits::sha256(id)),
                )
                .await
                .map(|n| n == 1);
            scope.finish(result).await
        })
    }

    fn list<'a>(
        &'a self,
        tx: &'a mut Tx,
        saga_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<SagaFailureDocument>, StoreError>> {
        Box::pin(async move {
            let stmt = match saga_type {
                Some(t) => Stmt::built(format!(
                    "SELECT {FAILURE_COLUMNS} FROM snodra_saga_failures WHERE saga_type = ? \
                     ORDER BY last_failed_at, id_hash"
                ))
                .arg("sagaType", t),
                None => Stmt::built(format!(
                    "SELECT {FAILURE_COLUMNS} FROM snodra_saga_failures ORDER BY last_failed_at, id_hash"
                )),
            };
            let mut scope = self.db.scope(tx).await?;
            let result = async {
                let rows = scope.conn().fetch_all(stmt).await?;
                rows.iter().map(read_failure).collect()
            }
            .await;
            scope.finish(result).await
        })
    }
}

impl MarkerStore for SqlSagaStore {
    fn exists<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = marker_exists(&mut scope.conn(), id).await;
            scope.finish(result).await
        })
    }

    /// A plain insert: a duplicate is a [`StoreError::Conflict`].
    fn insert<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        type_name: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = scope
                .conn()
                .execute(
                    Stmt::new(
                        "INSERT INTO snodra_processed_events (id_hash, id, type, processed_at) VALUES (?, ?, ?, ?)",
                    )
                    .arg("idHash", limits::sha256(id))
                    .arg("id", id)
                    .arg("type", type_name)
                    .time("now", now_ms),
                )
                .await
                .map(|_| ());
            scope.finish(result).await
        })
    }
}

impl SagaDispatchStore for SqlSagaStore {
    fn record<'a>(
        &'a self,
        tx: &'a mut Tx,
        entries: &'a [SagaDispatchDocument],
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if entries.is_empty() {
                return Ok(());
            }
            let mut stmt = Stmt::built(format!(
                "INSERT INTO snodra_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, routing_key, \
                 created_at, dispatched_at, attempts, next_attempt_at, failed_at, failure_reason, failure_message, \
                 notice_pending, notice_attempts, notice_next_at) VALUES {}",
                vec![
                    "(?, ?, ?, ?, ?, ?, CAST(? AS {json}), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    entries.len()
                ]
                .join(", ")
            ));
            for e in entries {
                limits::check("a saga-dispatch entry's id", &e.id, limits::ID)?;
                limits::check("a saga-dispatch entry's saga id", &e.saga_id, limits::ID)?;
                limits::check("a saga-dispatch entry's type", &e.type_name, limits::NAME)?;
                // An event the outbox would refuse is refused here, inside the saga's
                // transaction: never committed, then stuck.
                if e.kind == DispatchKind::Event {
                    limits::require_object(&e.type_name, e.payload.as_bytes())?;
                }
                stmt = stmt
                    .arg("id", &e.id)
                    .arg("sagaId", &e.saga_id)
                    .arg("seq", e.seq)
                    .arg("kind", enum_name(&e.kind)?)
                    .arg("type", &e.type_name)
                    .arg("payload", &e.payload)
                    .arg(
                        "headers",
                        limits::headers_json("a saga-dispatch entry's headers", &e.headers)?,
                    )
                    .arg("routingKey", &e.routing_key)
                    .time("createdAt", e.created_at_ms)
                    .opt_time("dispatchedAt", e.dispatched_at_ms)
                    .arg("attempts", e.attempts)
                    .opt_time("nextAttemptAt", e.next_attempt_at_ms)
                    .opt_time("failedAt", e.failed_at_ms)
                    .arg(
                        "failureReason",
                        e.failure_reason.as_ref().map(enum_name).transpose()?,
                    )
                    .arg(
                        "failureMessage",
                        e.failure_message.as_deref().map(limits::cut),
                    )
                    .arg("noticePending", e.notice_pending)
                    .arg("noticeAttempts", e.notice_attempts)
                    .opt_time("noticeNextAt", e.notice_next_at_ms);
            }
            let mut scope = self.db.scope(tx).await?;
            let result = scope.conn().execute(stmt).await.map(|_| ());
            scope.finish(result).await
        })
    }

    fn due_sagas<'a>(
        &'a self,
        now_ms: i64,
        except: &'a [String],
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async move {
            let excluded = if except.is_empty() {
                String::new()
            } else {
                format!("AND d.saga_id NOT IN ({}) ", placeholders(except.len()))
            };
            let stmt = Stmt::built(format!(
                "SELECT d.saga_id FROM snodra_saga_dispatch d WHERE d.dispatched_at IS NULL AND d.failed_at IS NULL {excluded}\
                 AND d.seq = (SELECT MIN(x.seq) FROM snodra_saga_dispatch x WHERE x.saga_id = d.saga_id \
                 AND x.dispatched_at IS NULL AND x.failed_at IS NULL) \
                 AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?) ORDER BY d.created_at, d.saga_id LIMIT ?"
            ))
            .args("exclude", except)
            .time("now", now_ms)
            .arg("limit", i64::try_from(limit).unwrap_or(i64::MAX));
            let mut t = self.db.begin().await?;
            let result = async {
                let rows = t.conn().fetch_all(stmt).await?;
                rows.iter().map(|r| r.text("saga_id")).collect()
            }
            .await;
            t.finish(result).await
        })
    }

    fn next_pending<'a>(
        &'a self,
        saga_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = async {
                t.conn()
                    .fetch_optional(
                        Stmt::built(format!(
                            "SELECT {DISPATCH_COLUMNS} FROM snodra_saga_dispatch WHERE saga_id = ? \
                             AND dispatched_at IS NULL AND failed_at IS NULL ORDER BY seq LIMIT 1"
                        ))
                        .arg("sagaId", saga_id),
                    )
                    .await?
                    .map(|row| read_dispatch(&row))
                    .transpose()
            }
            .await;
            t.finish(result).await
        })
    }

    fn mark_dispatched<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = scope
                .conn()
                .execute(
                    Stmt::new("UPDATE snodra_saga_dispatch SET dispatched_at = ? WHERE id = ?")
                        .time("now", now_ms)
                        .arg("id", id),
                )
                .await
                .map(|_| ());
            scope.finish(result).await
        })
    }

    fn schedule_retry<'a>(
        &'a self,
        id: &'a str,
        attempts: i32,
        next_attempt_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.own_update(
                Stmt::new("UPDATE snodra_saga_dispatch SET attempts = ?, next_attempt_at = ? WHERE id = ?")
                    .arg("attempts", attempts)
                    .time("next", next_attempt_at_ms)
                    .arg("id", id),
            )
            .await
            .map(|_| ())
        })
    }

    /// `failed_at`, the reason and the message, with `attempts` when given, and a pending notice
    /// when given (a command's failure has one, an event's none).
    fn mark_failed<'a>(
        &'a self,
        id: &'a str,
        failure: &'a DispatchFailure,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut set = String::from("failed_at = ?, failure_reason = ?, failure_message = ?");
            if failure.attempts.is_some() {
                set.push_str(", attempts = ?");
            }
            if failure.notice_at_ms.is_some() {
                set.push_str(", notice_pending = TRUE, notice_attempts = 0, notice_next_at = ?");
            }
            let mut stmt = Stmt::built(format!(
                "UPDATE snodra_saga_dispatch SET {set} WHERE id = ?"
            ))
            .time("failedAt", failure.failed_at_ms)
            .arg("reason", enum_name(&failure.reason)?)
            .arg("message", limits::cut(&failure.message));
            if let Some(attempts) = failure.attempts {
                stmt = stmt.arg("attempts", attempts);
            }
            if let Some(at) = failure.notice_at_ms {
                stmt = stmt.time("noticeNextAt", at);
            }
            self.own_update(stmt.arg("id", id)).await.map(|_| ())
        })
    }

    fn claim_due_notice<'a>(
        &'a self,
        now_ms: i64,
        reserve_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = async {
                let mut conn = t.conn();
                let Some(row) = conn
                    .fetch_optional(
                        Stmt::built(format!(
                            "SELECT {DISPATCH_COLUMNS} FROM snodra_saga_dispatch WHERE notice_pending = TRUE \
                             AND (notice_next_at IS NULL OR notice_next_at <= ?) \
                             ORDER BY CASE WHEN notice_next_at IS NULL THEN 0 ELSE 1 END, notice_next_at, id \
                             LIMIT 1 FOR UPDATE SKIP LOCKED"
                        ))
                        .time("now", now_ms),
                    )
                    .await?
                else {
                    return Ok(None);
                };
                let due = read_dispatch(&row)?;
                conn.execute(
                    Stmt::new("UPDATE snodra_saga_dispatch SET notice_next_at = ? WHERE id = ?")
                        .time("reserve", reserve_until_ms)
                        .arg("id", &due.id),
                )
                .await?;
                Ok(Some(SagaDispatchDocument {
                    notice_next_at_ms: Some(reserve_until_ms),
                    ..due
                }))
            }
            .await;
            t.finish(result).await
        })
    }

    fn clear_notice<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.own_update(
                Stmt::new(
                    "UPDATE snodra_saga_dispatch SET notice_pending = NULL, notice_attempts = NULL, \
                     notice_next_at = NULL WHERE id = ?",
                )
                .arg("id", id),
            )
            .await
            .map(|_| ())
        })
    }

    fn schedule_notice_retry<'a>(
        &'a self,
        id: &'a str,
        notice_attempts: i32,
        next_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.own_update(
                Stmt::new(
                    "UPDATE snodra_saga_dispatch SET notice_attempts = ?, notice_next_at = ? WHERE id = ?",
                )
                .arg("attempts", notice_attempts)
                .time("next", next_at_ms)
                .arg("id", id),
            )
            .await
            .map(|_| ())
        })
    }

    /// PostgreSQL: one upsert, conditional on the lease having passed. MySQL: the takeover first
    /// (an `UPDATE` conditional on the lease having passed), else an insert; a duplicate means
    /// another claim got it. A lock conflict with another claim (a deadlock) is that claim
    /// winning: false.
    fn claim_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        now_ms: i64,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            limits::check("a saga id", saga_id, limits::ID)?;
            let mut t = self.db.own().await?;
            let result = async {
                let mut conn = t.conn();
                match conn.dialect() {
                    #[cfg(feature = "postgres")]
                    Dialect::Postgres => Ok(conn
                        .execute(
                            Stmt::new(
                                "INSERT INTO snodra_saga_dispatch_leases (saga_id, owner, lease_until) VALUES (?, ?, ?) \
                                 ON CONFLICT (saga_id) DO UPDATE SET owner = EXCLUDED.owner, lease_until = EXCLUDED.lease_until \
                                 WHERE snodra_saga_dispatch_leases.lease_until < ?",
                            )
                            .arg("sagaId", saga_id)
                            .arg("owner", owner)
                            .time("until", until_ms)
                            .time("now", now_ms),
                        )
                        .await?
                        == 1),
                    #[cfg(feature = "mysql")]
                    Dialect::MySql => {
                        let taken = conn
                            .execute(
                                Stmt::new(
                                    "UPDATE snodra_saga_dispatch_leases SET owner = ?, lease_until = ? \
                                     WHERE saga_id = ? AND lease_until < ?",
                                )
                                .arg("owner", owner)
                                .time("until", until_ms)
                                .arg("sagaId", saga_id)
                                .time("now", now_ms),
                            )
                            .await?
                            == 1;
                        if taken {
                            return Ok(true);
                        }
                        crate::outbox::insert_once(
                            &mut conn,
                            "INSERT INTO snodra_saga_dispatch_leases (saga_id, owner, lease_until) VALUES (?, ?, ?)",
                            "",
                            |s| {
                                s.arg("sagaId", saga_id)
                                    .arg("owner", owner)
                                    .time("until", until_ms)
                            },
                        )
                        .await
                    }
                }
            }
            .await;
            match t.finish(result).await {
                // Another claim locked the row first: it won.
                Err(StoreError::Conflict(_)) => Ok(false),
                other => other,
            }
        })
    }

    fn renew_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            Ok(self
                .own_update(
                    Stmt::new(
                        "UPDATE snodra_saga_dispatch_leases SET lease_until = ? WHERE saga_id = ? AND owner = ?",
                    )
                    .time("until", until_ms)
                    .arg("sagaId", saga_id)
                    .arg("owner", owner),
                )
                .await?
                == 1)
        })
    }

    fn release_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.own_update(
                Stmt::new(
                    "DELETE FROM snodra_saga_dispatch_leases WHERE saga_id = ? AND owner = ?",
                )
                .arg("sagaId", saga_id)
                .arg("owner", owner),
            )
            .await
            .map(|_| ())
        })
    }

    fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = async {
                let row = t
                    .conn()
                    .fetch_one(Stmt::new(
                        "SELECT COUNT(*) AS n FROM snodra_saga_dispatch WHERE dispatched_at IS NULL AND failed_at IS NULL",
                    ))
                    .await?;
                Ok(u64::try_from(row.int("n")?).unwrap_or(0))
            }
            .await;
            t.finish(result).await
        })
    }
}
