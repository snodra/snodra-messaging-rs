//! `snodra_processor_control`: one row per processor, created by the first write that
//! needs it (not paused, no operation, no replay). Every write commits on its own (READ
//! COMMITTED); each method is one transaction, so a lock taken, renewed or released is atomic (a
//! conditional `UPDATE` on the row). The replay ranges are a JSON array of
//! `{"source","segment","from","until"}` strings, in order.

use std::sync::Arc;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use snodra_messaging::event_store::ReplayRange;
use snodra_messaging::store::{
    ControlOperation, Locked, ProcessorControlState, ProcessorControlStore, RESET_KIND, StoreError,
};

use crate::db::{Conn, Db, Dialect, Row, Stmt, storage};
use crate::limits;

const COLUMNS: &str = "processor, paused, paused_at, paused_by, op_id, op_kind, op_lease_until, \
     op_prepared_at, CAST(replay AS {text}) AS replay, updated_at";

/// A replay range as the `replay` column holds it.
#[derive(Serialize, Deserialize)]
struct RangeJson {
    source: String,
    segment: String,
    from: String,
    until: String,
}

fn ranges_json(ranges: &[ReplayRange]) -> Result<String, StoreError> {
    let json: Vec<RangeJson> = ranges
        .iter()
        .map(|r| RangeJson {
            source: r.source.clone(),
            segment: r.segment.clone(),
            from: r.from.clone(),
            until: r.until.clone(),
        })
        .collect();
    for r in &json {
        for part in [&r.source, &r.segment, &r.from, &r.until] {
            limits::refuse_nul("a replay range", part)?;
        }
    }
    Ok(serde_json::to_string(&json)?)
}

fn ranges(text: Option<&str>) -> Result<Vec<ReplayRange>, StoreError> {
    let Some(text) = text else {
        return Ok(Vec::new());
    };
    // A range missing a field is skipped.
    let values: Vec<serde_json::Value> = serde_json::from_str(text)?;
    Ok(values
        .into_iter()
        .filter_map(|v| serde_json::from_value::<RangeJson>(v).ok())
        .map(|r| ReplayRange {
            source: r.source,
            segment: r.segment,
            from: r.from,
            until: r.until,
        })
        .collect())
}

fn read(row: &Row) -> Result<ProcessorControlState, StoreError> {
    let operation = match row.opt_text("op_id")? {
        Some(id) => Some(ControlOperation {
            id,
            kind: row
                .opt_text("op_kind")?
                .unwrap_or_else(|| RESET_KIND.to_string()),
            lease_until_ms: row.ms("op_lease_until")?,
            prepared_at_ms: row.ms("op_prepared_at")?,
        }),
        None => None,
    };
    Ok(ProcessorControlState {
        processor: row.text("processor")?,
        paused: row.bool("paused")?,
        paused_at_ms: row.opt_ms("paused_at")?,
        paused_by: row.opt_text("paused_by")?,
        operation,
        replay: ranges(row.opt_text("replay")?.as_deref())?,
        updated_at_ms: row.opt_ms("updated_at")?,
    })
}

pub(crate) struct SqlProcessorControlStore {
    db: Arc<Db>,
}

impl SqlProcessorControlStore {
    pub(crate) fn new(db: Arc<Db>) -> SqlProcessorControlStore {
        SqlProcessorControlStore { db }
    }
}

/// Creates `processor`'s row (not paused, no operation, no replay) unless it exists.
async fn ensure(conn: &mut Conn<'_>, processor: &str) -> Result<(), StoreError> {
    limits::check("a processor's name", processor, limits::PROCESSOR)?;
    let stmt = match conn.dialect() {
        #[cfg(feature = "postgres")]
        Dialect::Postgres => Stmt::new(
            "INSERT INTO snodra_processor_control (processor, paused, replay) \
             VALUES (?, FALSE, CAST(? AS {json})) ON CONFLICT (processor) DO NOTHING",
        ),
        #[cfg(feature = "mysql")]
        Dialect::MySql => Stmt::new(
            "INSERT INTO snodra_processor_control (processor, paused, replay) \
             VALUES (?, FALSE, CAST(? AS {json})) ON DUPLICATE KEY UPDATE processor = processor",
        ),
    };
    conn.execute(stmt.arg("processor", processor).arg("replay", "[]"))
        .await?;
    Ok(())
}

async fn renew(
    conn: &mut Conn<'_>,
    processor: &str,
    operation: &str,
    lease_until_ms: i64,
    now_ms: i64,
) -> Result<bool, StoreError> {
    Ok(conn
        .execute(
            Stmt::new(
                "UPDATE snodra_processor_control SET op_lease_until = ?, updated_at = ? \
                 WHERE processor = ? AND op_id = ?",
            )
            .time("until", lease_until_ms)
            .time("now", now_ms)
            .arg("processor", processor)
            .arg("op", operation),
        )
        .await?
        == 1)
}

async fn state(
    conn: &mut Conn<'_>,
    processor: &str,
) -> Result<Option<ProcessorControlState>, StoreError> {
    conn.fetch_optional(
        Stmt::built(format!(
            "SELECT {COLUMNS} FROM snodra_processor_control WHERE processor = ?"
        ))
        .arg("processor", processor),
    )
    .await?
    .map(|row| read(&row))
    .transpose()
}

async fn write_replay(
    conn: &mut Conn<'_>,
    processor: &str,
    ranges: &[ReplayRange],
    now_ms: i64,
) -> Result<(), StoreError> {
    conn.execute(
        Stmt::new(
            "UPDATE snodra_processor_control SET replay = CAST(? AS {json}), updated_at = ? WHERE processor = ?",
        )
        .arg("replay", ranges_json(ranges)?)
        .time("now", now_ms)
        .arg("processor", processor),
    )
    .await?;
    Ok(())
}

impl ProcessorControlStore for SqlProcessorControlStore {
    fn get<'a>(
        &'a self,
        processor: &'a str,
    ) -> BoxFuture<'a, Result<ProcessorControlState, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = state(&mut t.conn(), processor).await;
            Ok(t.finish(result)
                .await?
                .unwrap_or_else(|| ProcessorControlState {
                    processor: processor.to_string(),
                    ..ProcessorControlState::default()
                }))
        })
    }

    fn set_paused<'a>(
        &'a self,
        processor: &'a str,
        paused: bool,
        by: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = async {
                let mut conn = t.conn();
                ensure(&mut conn, processor).await?;
                conn.execute(
                    Stmt::new(
                        "UPDATE snodra_processor_control SET paused = ?, paused_at = ?, paused_by = ?, \
                         updated_at = ? WHERE processor = ?",
                    )
                    .arg("paused", paused)
                    .opt_time("pausedAt", paused.then_some(now_ms))
                    .arg("by", paused.then_some(by))
                    .time("now", now_ms)
                    .arg("processor", processor),
                )
                .await?;
                Ok(())
            }
            .await;
            t.finish(result).await
        })
    }

    /// The row first (on MySQL an `UPDATE` that finds no row takes a gap lock, and two of those
    /// followed by inserts deadlock), then a renewal, then the take when nothing holds the lock or
    /// its lease passed.
    fn lock<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<Locked, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = async {
                let mut conn = t.conn();
                ensure(&mut conn, processor).await?;
                if renew(&mut conn, processor, operation, lease_until_ms, now_ms).await? {
                    return Ok(Locked::Taken);
                }
                let taken = conn
                    .execute(
                        Stmt::new(
                            "UPDATE snodra_processor_control SET op_id = ?, op_kind = ?, op_lease_until = ?, \
                             op_prepared_at = ?, updated_at = ? \
                             WHERE processor = ? AND (op_id IS NULL OR op_lease_until <= ?)",
                        )
                        .arg("op", operation)
                        .arg("kind", RESET_KIND)
                        .time("until", lease_until_ms)
                        .time("preparedAt", now_ms)
                        .time("now", now_ms)
                        .arg("processor", processor)
                        .time("now", now_ms),
                    )
                    .await?
                    == 1;
                if taken {
                    return Ok(Locked::Taken);
                }
                match state(&mut conn, processor).await?.and_then(|s| s.operation) {
                    Some(held) => Ok(Locked::Held(held)),
                    None => Err(storage(format!(
                        "the lock of processor '{processor}' changed while it was taken"
                    ))),
                }
            }
            .await;
            t.finish(result).await
        })
    }

    fn extend<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = renew(&mut t.conn(), processor, operation, lease_until_ms, now_ms).await;
            t.finish(result).await
        })
    }

    fn release<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = t
                .conn()
                .execute(
                    Stmt::new(
                        "UPDATE snodra_processor_control SET op_id = NULL, op_kind = NULL, op_lease_until = NULL, \
                         op_prepared_at = NULL, updated_at = ? WHERE processor = ? AND op_id = ?",
                    )
                    .time("now", now_ms)
                    .arg("processor", processor)
                    .arg("op", operation),
                )
                .await
                .map(|n| n == 1);
            t.finish(result).await
        })
    }

    fn expire<'a>(
        &'a self,
        processor: &'a str,
        operation: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = t
                .conn()
                .execute(
                    Stmt::new(
                        "UPDATE snodra_processor_control SET op_id = NULL, op_kind = NULL, op_lease_until = NULL, \
                         op_prepared_at = NULL, updated_at = ? WHERE processor = ? AND op_id = ? AND op_lease_until <= ?",
                    )
                    .time("now", now_ms)
                    .arg("processor", processor)
                    .arg("op", operation)
                    .time("now", now_ms),
                )
                .await
                .map(|n| n == 1);
            t.finish(result).await
        })
    }

    fn set_replay<'a>(
        &'a self,
        processor: &'a str,
        ranges: &'a [ReplayRange],
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = async {
                let mut conn = t.conn();
                ensure(&mut conn, processor).await?;
                write_replay(&mut conn, processor, ranges, now_ms).await
            }
            .await;
            t.finish(result).await
        })
    }

    /// The ranges read under a row lock (`FOR UPDATE`); every range equal to `range` removed.
    fn remove_range<'a>(
        &'a self,
        processor: &'a str,
        range: &'a ReplayRange,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = async {
                let mut conn = t.conn();
                let Some(row) = conn
                    .fetch_optional(
                        Stmt::new(
                            "SELECT CAST(replay AS {text}) AS replay FROM snodra_processor_control \
                             WHERE processor = ? FOR UPDATE",
                        )
                        .arg("processor", processor),
                    )
                    .await?
                else {
                    return Ok(false);
                };
                let stored = ranges(row.opt_text("replay")?.as_deref())?;
                if !stored.contains(range) {
                    return Ok(false);
                }
                let kept: Vec<ReplayRange> = stored.into_iter().filter(|r| r != range).collect();
                write_replay(&mut conn, processor, &kept, now_ms).await?;
                Ok(true)
            }
            .await;
            t.finish(result).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The column holds `[{"source","segment","from","until"}]` strings, in order (the shared
    /// layout), and reads it back.
    #[test]
    fn replay_ranges_are_a_json_array_of_string_objects_in_order() {
        let r = |s: &str| ReplayRange {
            source: "snodra.events".into(),
            segment: s.into(),
            from: "0".into(),
            until: "10".into(),
        };
        let json = ranges_json(&[r("2"), r("1")]).unwrap();
        assert_eq!(
            json,
            r#"[{"source":"snodra.events","segment":"2","from":"0","until":"10"},{"source":"snodra.events","segment":"1","from":"0","until":"10"}]"#
        );
        assert_eq!(ranges(Some(&json)).unwrap(), vec![r("2"), r("1")]);
        // Key order as MySQL or PostgreSQL normalise it is read the same.
        assert_eq!(
            ranges(Some(
                r#"[{"from":"0","until":"10","source":"snodra.events","segment":"2"}]"#
            ))
            .unwrap(),
            vec![r("2")]
        );
        assert_eq!(ranges(Some("[]")).unwrap(), vec![]);
    }
}
