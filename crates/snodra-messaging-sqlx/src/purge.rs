//! The retention of the SQL message store (what the MongoDB add-on's TTL indexes do): deletes, in
//! batches, the sent outbox entries, the processed-event markers, the external calls, the ended
//! sagas (and, by cascade, their associations), the dispatched and the failed saga-dispatch
//! entries, the fired or cancelled deadlines and the parked saga failures older than their
//! retention, the expired dead letters and the saga-dispatch leases that passed. Unsent outbox
//! entries, live sagas and scheduled deadlines are never deleted.

use std::time::Duration;

use snodra_messaging::Config;
use snodra_messaging::store::StoreError;

use crate::db::{Db, Dialect, Stmt};

/// Rows deleted per statement, each batch in a transaction of its own.
pub(crate) const BATCH: u64 = 1000;

/// Deletes the rows of `table` (primary key `key`) matching `condition` (one `?`: `before`, epoch
/// milliseconds), [`BATCH`] at a time; how many went.
pub(crate) async fn batched(
    db: &Db,
    table: &'static str,
    key: &'static str,
    condition: &'static str,
    before: i64,
) -> Result<u64, StoreError> {
    let sql = match db.dialect() {
        #[cfg(feature = "postgres")]
        Dialect::Postgres => format!(
            "DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM {table} WHERE {condition} LIMIT {BATCH})"
        ),
        #[cfg(feature = "mysql")]
        Dialect::MySql => {
            let _ = key;
            format!("DELETE FROM {table} WHERE {condition} LIMIT {BATCH}")
        }
    };
    let mut total = 0;
    loop {
        let mut t = db.own().await?;
        let result = t
            .conn()
            .execute(Stmt::built(sql.clone()).time("before", before))
            .await;
        let n = t.finish(result).await?;
        total += n;
        if n < BATCH {
            return Ok(total);
        }
    }
}

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// Everything the retentions of `config` let go; how many rows went.
pub(crate) async fn run(db: &Db, config: &Config) -> Result<u64, StoreError> {
    let now = db.now();
    let before = |retention: Duration| now.saturating_sub(ms(retention));
    let failed = before(config.sagas_failed_retention);
    let mut total = 0;
    for (table, key, condition, bound) in [
        (
            "snodra_outbox",
            "id",
            "sent_at < ?",
            before(config.outbox_retention),
        ),
        (
            "snodra_processed_events",
            "id_hash",
            "processed_at < ?",
            before(config.idempotency_retention),
        ),
        (
            "snodra_external_calls",
            "id_hash",
            "updated_at < ?",
            before(config.external_calls_retention),
        ),
        (
            "snodra_sagas",
            "id",
            "ended = TRUE AND ended_at < ?",
            before(config.sagas_ended_retention),
        ),
        (
            "snodra_saga_dispatch",
            "id",
            "dispatched_at < ?",
            before(config.sagas_dispatch_retention),
        ),
        ("snodra_saga_dispatch", "id", "failed_at < ?", failed),
        (
            "snodra_saga_dispatch_leases",
            "saga_id",
            "lease_until < ?",
            now,
        ),
        (
            "snodra_deadlines",
            "id",
            "finished_at < ?",
            before(config.deadlines_retention),
        ),
        (
            "snodra_saga_failures",
            "id_hash",
            "last_failed_at < ?",
            failed,
        ),
        ("snodra_dead_letters", "id", "expire_at <= ?", now),
    ] {
        total += batched(db, table, key, condition, bound).await?;
    }
    Ok(total)
}
