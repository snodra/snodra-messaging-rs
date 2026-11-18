//! `snodra_external_calls`, keyed by the SHA-256 of the idempotency key (`id_hash`; the key itself
//! in `id`): `reply` and `error` as the JSON text written, `status` by its Kotlin name. Every write
//! is a short transaction of its own, and every write after the insert is conditional on the
//! record the caller read or owns.

use std::sync::Arc;

use futures::future::BoxFuture;
use snodra_messaging::store::{ExternalCall, ExternalCallStatus, ExternalCallStore, StoreError};

use crate::db::{Db, Row, Stmt, storage};
use crate::limits;
use crate::outbox::insert_once;

const COLUMNS: &str = "id, status, reply, error, attempts, updated_at, lease_until, owner";
/// A settled record: no owner, no lease, updated now.
const SETTLE: &str = "updated_at = ?, lease_until = NULL, owner = NULL";
/// The record in progress, owned by the caller.
const OWNED: &str = "id_hash = ? AND status = 'IN_PROGRESS' AND owner = ?";

fn status_name(status: ExternalCallStatus) -> &'static str {
    match status {
        ExternalCallStatus::InProgress => "IN_PROGRESS",
        ExternalCallStatus::Succeeded => "SUCCEEDED",
        ExternalCallStatus::Failed => "FAILED",
        ExternalCallStatus::Unknown => "UNKNOWN",
    }
}

fn read(row: &Row) -> Result<ExternalCall, StoreError> {
    let status = match row.text("status")?.as_str() {
        "IN_PROGRESS" => ExternalCallStatus::InProgress,
        "SUCCEEDED" => ExternalCallStatus::Succeeded,
        "FAILED" => ExternalCallStatus::Failed,
        "UNKNOWN" => ExternalCallStatus::Unknown,
        other => return Err(storage(format!("unknown external call status {other}"))),
    };
    Ok(ExternalCall {
        key: row.text("id")?,
        status,
        reply: row.opt_text("reply")?,
        error: row.opt_text("error")?,
        attempts: row.int32("attempts")?,
        updated_at_ms: row.ms("updated_at")?,
        lease_until_ms: row.opt_ms("lease_until")?,
        owner: row.opt_text("owner")?,
    })
}

pub(crate) struct SqlExternalCallStore {
    db: Arc<Db>,
}

impl SqlExternalCallStore {
    pub(crate) fn new(db: Arc<Db>) -> SqlExternalCallStore {
        SqlExternalCallStore { db }
    }

    async fn update(&self, stmt: Stmt) -> Result<bool, StoreError> {
        let mut t = self.db.begin().await?;
        let result = t.conn().execute(stmt).await.map(|n| n == 1);
        t.finish(result).await
    }
}

/// `{id, IN_PROGRESS, owner, lease_until}` as read (a null one as null): the condition and its
/// parameters, added to `stmt`.
fn as_read(seen: &ExternalCall) -> String {
    format!(
        "id_hash = ? AND status = 'IN_PROGRESS' AND {} AND {}",
        if seen.owner.is_some() {
            "owner = ?"
        } else {
            "owner IS NULL"
        },
        if seen.lease_until_ms.is_some() {
            "lease_until = ?"
        } else {
            "lease_until IS NULL"
        }
    )
}

fn as_read_args(stmt: Stmt, seen: &ExternalCall) -> Stmt {
    let mut stmt = stmt.arg("idHash", limits::sha256(&seen.key));
    if let Some(owner) = &seen.owner {
        stmt = stmt.arg("expectedOwner", owner);
    }
    if let Some(lease) = seen.lease_until_ms {
        stmt = stmt.time("expectedLease", lease);
    }
    stmt
}

impl ExternalCallStore for SqlExternalCallStore {
    fn insert<'a>(&'a self, call: &'a ExternalCall) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = insert_once(
                &mut t.conn(),
                "INSERT INTO snodra_external_calls (id_hash, id, status, reply, error, attempts, updated_at, lease_until, owner) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                "ON CONFLICT (id_hash) DO NOTHING",
                |s| {
                    s.arg("idHash", limits::sha256(&call.key))
                        .arg("id", &call.key)
                        .arg("status", status_name(call.status))
                        .arg("reply", &call.reply)
                        .arg("error", &call.error)
                        .arg("attempts", call.attempts)
                        .time("updatedAt", call.updated_at_ms)
                        .opt_time("leaseUntil", call.lease_until_ms)
                        .arg("owner", &call.owner)
                },
            )
            .await;
            t.finish(result).await
        })
    }

    fn find<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ExternalCall>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = async {
                t.conn()
                    .fetch_optional(
                        Stmt::built(format!(
                            "SELECT {COLUMNS} FROM snodra_external_calls WHERE id_hash = ?"
                        ))
                        .arg("idHash", limits::sha256(key)),
                    )
                    .await?
                    .map(|row| read(&row))
                    .transpose()
            }
            .await;
            t.finish(result).await
        })
    }

    fn take_over<'a>(
        &'a self,
        seen: &'a ExternalCall,
        owner: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<Option<ExternalCall>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.begin().await?;
            let result = async {
                let mut conn = t.conn();
                let stmt = Stmt::built(format!(
                    "UPDATE snodra_external_calls SET lease_until = ?, updated_at = ?, owner = ?, \
                     attempts = attempts + 1 WHERE {}",
                    as_read(seen)
                ))
                .time("leaseUntil", lease_until_ms)
                .time("now", now_ms)
                .arg("owner", owner);
                if conn.execute(as_read_args(stmt, seen)).await? != 1 {
                    return Ok(None);
                }
                // The row is this transaction's until it ends: the read sees the update.
                conn.fetch_optional(
                    Stmt::built(format!(
                        "SELECT {COLUMNS} FROM snodra_external_calls WHERE id_hash = ?"
                    ))
                    .arg("idHash", limits::sha256(&seen.key)),
                )
                .await?
                .map(|row| read(&row))
                .transpose()
            }
            .await;
            t.finish(result).await
        })
    }

    fn expire<'a>(
        &'a self,
        seen: &'a ExternalCall,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let stmt = Stmt::built(format!(
                "UPDATE snodra_external_calls SET {SETTLE}, status = 'UNKNOWN' WHERE {}",
                as_read(seen)
            ))
            .time("now", now_ms);
            self.update(as_read_args(stmt, seen)).await
        })
    }

    fn succeed<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        reply: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(self.update(
            Stmt::built(format!(
                "UPDATE snodra_external_calls SET {SETTLE}, status = 'SUCCEEDED', reply = ?, error = NULL WHERE {OWNED}"
            ))
            .time("now", now_ms)
            .arg("reply", reply)
            .arg("idHash", limits::sha256(key))
            .arg("expectedOwner", owner),
        ))
    }

    fn fail<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        error: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(self.update(
            Stmt::built(format!(
                "UPDATE snodra_external_calls SET {SETTLE}, status = 'FAILED', error = ? WHERE {OWNED}"
            ))
            .time("now", now_ms)
            .arg("error", error)
            .arg("idHash", limits::sha256(key))
            .arg("expectedOwner", owner),
        ))
    }

    fn mark_unknown<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(
            self.update(
                Stmt::built(format!(
                    "UPDATE snodra_external_calls SET {SETTLE}, status = 'UNKNOWN' WHERE {OWNED}"
                ))
                .time("now", now_ms)
                .arg("idHash", limits::sha256(key))
                .arg("expectedOwner", owner),
            ),
        )
    }

    fn reset<'a>(
        &'a self,
        key: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(
            self.update(
                Stmt::built(format!("DELETE FROM snodra_external_calls WHERE {OWNED}"))
                    .arg("idHash", limits::sha256(key))
                    .arg("expectedOwner", owner),
            ),
        )
    }
}
