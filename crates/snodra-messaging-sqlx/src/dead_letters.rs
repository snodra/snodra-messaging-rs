//! `snodra_dead_letters`. Every write but [`DeadLetterStore::delete_in`] commits on
//! its own (READ COMMITTED). A letter whose `expire_at` passed is gone at once for every read and
//! write here (it no longer blocks its key), and the purge deletes it. Positions are ordered as
//! [`POSITION_ORDER`] says: decimal ones by value, then the others by their bytes; a bound
//! compares only positions of its own kind. The record key is found by its hash
//! (`event_key_hash`), kept whole in `event_key`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use futures::future::BoxFuture;
use snodra_messaging::store::{
    DeadLetter, DeadLetterError, DeadLetterReason, DeadLetterStore, DeadLetterSummary, Parked,
    StoreError, Tx,
};

use crate::db::{Conn, Db, Row, Stmt, placeholders, storage};
use crate::limits;
use crate::outbox::{insert_once, position_bound};

/// `ORDER BY` of positions as the stores order them: decimal ones by value first, then the others
/// by their bytes.
const POSITION_ORDER: &str =
    "CASE WHEN position_num IS NULL THEN 1 ELSE 0 END, position_num, position";

const COLUMNS: &str = "id, processor, source, segment, position, event_key, event_id, event_type, \
     CAST(headers AS {text}) AS headers, payload, payload_binary, reason, error_type, error_message, \
     blocked_by, attempts, parked_at, last_attempt_at, expire_at, replay";

pub(crate) struct SqlDeadLetterStore {
    db: Arc<Db>,
}

impl SqlDeadLetterStore {
    pub(crate) fn new(db: Arc<Db>) -> SqlDeadLetterStore {
        SqlDeadLetterStore { db }
    }

    /// The head among the blocking letters of (processor, key) matching `condition`.
    async fn first_id(
        &self,
        processor: &str,
        key: &str,
        condition: Option<(String, crate::db::Arg)>,
    ) -> Result<Option<String>, StoreError> {
        let (extra, bound) = match condition {
            Some((sql, arg)) => (format!(" AND {sql}"), Some(arg)),
            None => (String::new(), None),
        };
        let mut stmt = Stmt::built(format!(
            "SELECT id FROM snodra_dead_letters WHERE processor = ? AND event_key_hash = ? \
             AND reason <> 'INVALID' AND expire_at > ?{extra} ORDER BY {POSITION_ORDER}, id LIMIT 1"
        ))
        .arg("processor", processor)
        .arg("keyHash", limits::sha256(key))
        .time("now", self.db.now());
        if let Some(arg) = bound {
            stmt = stmt.arg("bound", arg);
        }
        let mut t = self.db.own().await?;
        let result = async {
            t.conn()
                .fetch_optional(stmt)
                .await?
                .map(|row| row.text("id"))
                .transpose()
        }
        .await;
        t.finish(result).await
    }

    async fn delete_live(&self, conn: &mut Conn<'_>, id: &str) -> Result<bool, StoreError> {
        Ok(conn
            .execute(
                Stmt::new("DELETE FROM snodra_dead_letters WHERE id = ? AND expire_at > ?")
                    .arg("id", id)
                    .time("now", self.db.now()),
            )
            .await?
            > 0)
    }
}

fn reason(row: &Row) -> Result<DeadLetterReason, StoreError> {
    let text = row.text("reason")?;
    DeadLetterReason::parse(&text)
        .ok_or_else(|| storage(format!("unknown dead-letter reason {text}")))
}

fn read_letter(row: &Row) -> Result<DeadLetter, StoreError> {
    let payload = match (row.opt_text("payload")?, row.opt_bytes("payload_binary")?) {
        (Some(text), _) => text.into_bytes(),
        (None, Some(bytes)) => bytes,
        (None, None) => Vec::new(),
    };
    Ok(DeadLetter {
        id: row.text("id")?,
        processor: row.text("processor")?,
        source: row.text("source")?,
        segment: row.text("segment")?,
        position: row.text("position")?,
        key: row.opt_text("event_key")?,
        event_id: row.opt_text("event_id")?,
        event_type: row.opt_text("event_type")?,
        headers: limits::strings(row.opt_text("headers")?.as_deref())?
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        payload,
        reason: reason(row)?,
        error: DeadLetterError {
            type_name: row.text("error_type")?,
            message: row.text("error_message")?,
        },
        blocked_by: row.opt_text("blocked_by")?,
        attempts: row.int32("attempts")?,
        parked_at_ms: row.ms("parked_at")?,
        last_attempt_at_ms: row.ms("last_attempt_at")?,
        expire_at_ms: row.ms("expire_at")?,
        replay: row.bool("replay")?,
    })
}

/// A record's value as the columns hold it: `payload` (text) when it is UTF-8 without U+0000,
/// `payload_binary` otherwise, neither when it is empty.
fn payload_columns(payload: &[u8]) -> (Option<String>, Option<Vec<u8>>) {
    if payload.is_empty() {
        return (None, None);
    }
    match std::str::from_utf8(payload) {
        Ok(text) if !text.contains('\0') => (Some(text.to_string()), None),
        _ => (None, Some(payload.to_vec())),
    }
}

impl DeadLetterStore for SqlDeadLetterStore {
    fn park<'a>(&'a self, letter: &'a DeadLetter) -> BoxFuture<'a, Result<Parked, StoreError>> {
        Box::pin(async move {
            limits::check("a dead letter's id", &letter.id, limits::DEAD_LETTER_ID)?;
            limits::check(
                "a dead letter's processor",
                &letter.processor,
                limits::PROCESSOR,
            )?;
            limits::check("a dead letter's source", &letter.source, limits::SOURCE)?;
            limits::check("a dead letter's segment", &letter.segment, limits::SEGMENT)?;
            limits::check(
                "a dead letter's position",
                &letter.position,
                limits::POSITION,
            )?;
            let headers = limits::headers_json("a dead letter's headers", &letter.headers)?;
            let (payload, payload_binary) = payload_columns(&letter.payload);
            let now = self.db.now();
            let mut t = self.db.own().await?;
            let result = async {
                let mut conn = t.conn();
                conn.execute(
                    Stmt::new("DELETE FROM snodra_dead_letters WHERE id = ? AND expire_at <= ?")
                        .arg("id", &letter.id)
                        .time("now", now),
                )
                .await?;
                insert_once(
                    &mut conn,
                    "INSERT INTO snodra_dead_letters (id, processor, source, segment, position, position_num, \
                     event_key, event_key_hash, event_id, event_type, headers, payload, payload_binary, reason, \
                     error_type, error_message, blocked_by, attempts, parked_at, last_attempt_at, expire_at, replay) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS {json}), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    "ON CONFLICT (id) DO NOTHING",
                    |s| {
                        s.arg("id", &letter.id)
                            .arg("processor", &letter.processor)
                            .arg("source", &letter.source)
                            .arg("segment", &letter.segment)
                            .arg("position", &letter.position)
                            .arg("positionNum", limits::position_number(&letter.position))
                            .arg("key", &letter.key)
                            .arg("keyHash", letter.key.as_deref().map(limits::sha256))
                            .arg("eventId", &letter.event_id)
                            .arg("eventType", &letter.event_type)
                            .arg("headers", headers.clone())
                            .arg("payload", payload.clone())
                            .bytes("payloadBinary", payload_binary.clone())
                            .arg("reason", letter.reason.as_str())
                            .arg("errorType", limits::cut(&letter.error.type_name))
                            .arg("errorMessage", limits::cut(&letter.error.message))
                            .arg("blockedBy", &letter.blocked_by)
                            .arg("attempts", letter.attempts)
                            .time("parkedAt", letter.parked_at_ms)
                            .time("lastAttemptAt", letter.last_attempt_at_ms)
                            .time("expireAt", letter.expire_at_ms)
                            .arg("replay", letter.replay)
                    },
                )
                .await
            }
            .await;
            Ok(if t.finish(result).await? {
                Parked::New
            } else {
                Parked::Already
            })
        })
    }

    fn head<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>> {
        Box::pin(self.first_id(processor, key, None))
    }

    fn earlier<'a>(
        &'a self,
        processor: &'a str,
        key: &'a str,
        position: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, StoreError>> {
        Box::pin(self.first_id(processor, key, Some(position_bound("<", position))))
    }

    fn parked_keys<'a>(
        &'a self,
        processor: &'a str,
        source: &'a str,
        segments: &'a [String],
    ) -> BoxFuture<'a, Result<HashSet<String>, StoreError>> {
        Box::pin(async move {
            if segments.is_empty() {
                return Ok(HashSet::new());
            }
            let mut t = self.db.own().await?;
            let result = async {
                let rows = t
                    .conn()
                    .fetch_all(
                        Stmt::built(format!(
                            "SELECT DISTINCT event_key FROM snodra_dead_letters WHERE processor = ? AND source = ? \
                             AND segment IN ({}) AND event_key IS NOT NULL AND reason <> 'INVALID' AND expire_at > ?",
                            placeholders(segments.len())
                        ))
                        .arg("processor", processor)
                        .arg("source", source)
                        .args("segments", segments)
                        .time("now", self.db.now()),
                    )
                    .await?;
                rows.iter().map(|r| r.text("event_key")).collect()
            }
            .await;
            t.finish(result).await
        })
    }

    fn get<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<DeadLetter>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = async {
                t.conn()
                    .fetch_optional(
                        Stmt::built(format!(
                            "SELECT {COLUMNS} FROM snodra_dead_letters WHERE id = ? AND expire_at > ?"
                        ))
                        .arg("id", id)
                        .time("now", self.db.now()),
                    )
                    .await?
                    .map(|row| read_letter(&row))
                    .transpose()
            }
            .await;
            t.finish(result).await
        })
    }

    fn list<'a>(
        &'a self,
        processor: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxFuture<'a, Result<Vec<DeadLetter>, StoreError>> {
        Box::pin(async move {
            let mut sql = format!("SELECT {COLUMNS} FROM snodra_dead_letters WHERE expire_at > ?");
            if processor.is_some() {
                sql.push_str(" AND processor = ?");
            }
            sql.push_str(" ORDER BY parked_at, id");
            if limit.is_some() {
                sql.push_str(" LIMIT ?");
            }
            let mut stmt = Stmt::built(sql).time("now", self.db.now());
            if let Some(p) = processor {
                stmt = stmt.arg("processor", p);
            }
            if let Some(n) = limit {
                stmt = stmt.arg("limit", i64::try_from(n).unwrap_or(i64::MAX));
            }
            let mut t = self.db.own().await?;
            let result = async {
                let rows = t.conn().fetch_all(stmt).await?;
                rows.iter().map(read_letter).collect()
            }
            .await;
            t.finish(result).await
        })
    }

    fn summaries<'a>(
        &'a self,
        processor: &'a str,
    ) -> BoxFuture<'a, Result<Vec<DeadLetterSummary>, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = async {
                let rows = t
                    .conn()
                    .fetch_all(
                        Stmt::built(format!(
                            "SELECT id, event_key, position, reason, parked_at FROM snodra_dead_letters \
                             WHERE processor = ? AND expire_at > ? ORDER BY {POSITION_ORDER}, id"
                        ))
                        .arg("processor", processor)
                        .time("now", self.db.now()),
                    )
                    .await?;
                rows.iter()
                    .map(|row| {
                        Ok(DeadLetterSummary {
                            id: row.text("id")?,
                            key: row.opt_text("event_key")?,
                            position: row.text("position")?,
                            reason: reason(row)?,
                            parked_at_ms: row.ms("parked_at")?,
                        })
                    })
                    .collect()
            }
            .await;
            t.finish(result).await
        })
    }

    fn count<'a>(&'a self, processor: Option<&'a str>) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            let stmt = match processor {
                Some(p) => Stmt::new(
                    "SELECT COUNT(*) AS n FROM snodra_dead_letters WHERE expire_at > ? AND processor = ?",
                )
                .time("now", self.db.now())
                .arg("processor", p),
                None => Stmt::new("SELECT COUNT(*) AS n FROM snodra_dead_letters WHERE expire_at > ?")
                    .time("now", self.db.now()),
            };
            let mut t = self.db.own().await?;
            let result =
                async { Ok(u64::try_from(t.conn().fetch_one(stmt).await?.int("n")?).unwrap_or(0)) }
                    .await;
            t.finish(result).await
        })
    }

    fn delete<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = self.delete_live(&mut t.conn(), id).await;
            t.finish(result).await
        })
    }

    fn delete_in<'a>(
        &'a self,
        tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut scope = self.db.scope(tx).await?;
            let result = self.delete_live(&mut scope.conn(), id).await;
            scope.finish(result).await
        })
    }

    fn record_failure<'a>(
        &'a self,
        id: &'a str,
        error: &'a DeadLetterError,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let mut t = self.db.own().await?;
            let result = t
                .conn()
                .execute(
                    Stmt::new(
                        "UPDATE snodra_dead_letters SET attempts = attempts + 1, error_type = ?, error_message = ?, \
                         last_attempt_at = ?, reason = 'HANDLER_FAILED', blocked_by = NULL WHERE id = ? AND expire_at > ?",
                    )
                    .arg("type", limits::cut(&error.type_name))
                    .arg("message", limits::cut(&error.message))
                    .time("lastAttemptAt", now_ms)
                    .arg("id", id)
                    .time("now", self.db.now()),
                )
                .await
                .map(|n| n > 0);
            t.finish(result).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::payload_columns;

    #[test]
    fn a_value_is_text_when_it_can_be_and_bytes_otherwise() {
        assert_eq!(payload_columns(b""), (None, None));
        assert_eq!(
            payload_columns(b"{\"a\":1}"),
            (Some("{\"a\":1}".into()), None)
        );
        assert_eq!(
            payload_columns(&[0xff, 0x00]),
            (None, Some(vec![0xff, 0x00]))
        );
        assert_eq!(payload_columns(b"a\0b"), (None, Some(b"a\0b".to_vec())));
    }
}
