//! The `outbox` collection: `{_id, type, key, headers, payload (a document), createdAt, seq,
//! sentAt, attempts, failedAt}` as the library has always written it, plus `owner` and
//! `leaseUntil` while a relay holds an entry (absent: nobody holds it).

use std::collections::HashMap;
use std::time::Duration;

use futures::TryStreamExt;
use futures::future::BoxFuture;
use kraken_messaging::error::MessagingError;
use kraken_messaging::saga::Clock;
use kraken_messaging::store::{OutboxEntry, OutboxStore, StoreError, Tx};
use mongodb::bson::{Bson, DateTime, Document, doc};
use mongodb::options::ReturnDocument;
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};

use crate::{OrStoreError, date, session};

pub(crate) const OUTBOX_COLLECTION: &str = "outbox";

/// One outbox document. Field names are the Kotlin ones.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OutboxDocument {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "type")]
    type_name: String,
    key: Option<String>,
    headers: HashMap<String, String>,
    payload: Document,
    created_at: DateTime,
    /// Per-process counter breaking `createdAt` millisecond ties, so per-key order holds.
    seq: i64,
    sent_at: Option<DateTime>,
    #[serde(default)]
    attempts: i32,
    failed_at: Option<DateTime>,
    /// The relay holding the entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    /// Until when `owner` holds it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lease_until: Option<DateTime>,
}

impl OutboxDocument {
    fn from_entry(entry: OutboxEntry) -> Result<OutboxDocument, StoreError> {
        Ok(OutboxDocument {
            payload: payload_document(&entry.type_name, &entry.payload)?,
            id: entry.id,
            type_name: entry.type_name,
            key: entry.key,
            headers: entry.headers,
            created_at: date(entry.created_at_ms),
            seq: entry.seq,
            sent_at: None,
            attempts: entry.attempts,
            failed_at: None,
            owner: None,
            lease_until: None,
        })
    }

    fn into_entry(self) -> Result<OutboxEntry, StoreError> {
        let payload = serde_json::to_vec(&Bson::Document(self.payload).into_relaxed_extjson())?;
        Ok(OutboxEntry {
            id: self.id,
            type_name: self.type_name,
            key: self.key,
            headers: self.headers,
            payload,
            created_at_ms: self.created_at.timestamp_millis(),
            seq: self.seq,
            attempts: self.attempts,
        })
    }
}

/// The BSON document an event's JSON payload becomes: it must be a JSON object whose numbers BSON
/// can hold (an integer above `i64::MAX` it cannot). Anything else is `InvalidMessage` naming the
/// event type: no retry can change it.
fn payload_document(type_name: &str, payload: &[u8]) -> Result<Document, StoreError> {
    let invalid = |why: String| {
        StoreError::Failed(MessagingError::InvalidMessage(format!(
            "the payload of event {type_name} {why}"
        )))
    };
    let json: serde_json::Value = serde_json::from_slice(payload).map_err(|e| {
        invalid(format!(
            "is not JSON, so it cannot be an outbox document: {e}"
        ))
    })?;
    if !json.is_object() {
        return Err(invalid(
            "is not a JSON object, so it cannot be an outbox document".into(),
        ));
    }
    mongodb::bson::to_document(&json)
        .map_err(|e| invalid(format!("cannot be an outbox document: {e}")))
}

pub(crate) struct MongoOutboxStore {
    docs: Collection<OutboxDocument>,
    /// What stamps `sentAt`, `failedAt` and the leases: the application's clock.
    clock: Clock,
}

impl MongoOutboxStore {
    pub(crate) fn new(db: &Database, clock: Clock) -> MongoOutboxStore {
        MongoOutboxStore {
            docs: db.collection(OUTBOX_COLLECTION),
            clock,
        }
    }

    fn now(&self) -> DateTime {
        date(self.clock.now_ms())
    }

    /// Neither sent nor given up on (a missing field is null).
    fn unfinished() -> Document {
        doc! { "sentAt": null, "failedAt": null }
    }

    /// Nobody holds it, `owner` does, or its holder's lease has passed at `at`.
    fn claimable_by(owner: &str, at: DateTime) -> Document {
        doc! { "$or": [
            { "owner": null },
            { "owner": owner },
            { "leaseUntil": { "$lte": at } },
        ] }
    }
}

impl OutboxStore for MongoOutboxStore {
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx,
        entry: OutboxEntry,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let doc = OutboxDocument::from_entry(entry)?;
            self.docs
                .insert_one(doc)
                .session(session(tx)?)
                .await
                .store()?;
            Ok(())
        })
    }

    fn append_now(&self, entry: OutboxEntry) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            self.docs
                .insert_one(OutboxDocument::from_entry(entry)?)
                .await
                .store()?;
            Ok(())
        })
    }

    /// Finds candidates in order, then takes each with a conditional update (still unfinished
    /// and claimable): of two relays racing for an entry exactly one wins it. Repeats until
    /// `limit` entries are won or none is left to try.
    fn claim<'a>(
        &'a self,
        owner: &'a str,
        lease: Duration,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<OutboxEntry>, StoreError>> {
        Box::pin(async move {
            let mut won: Vec<OutboxDocument> = Vec::new();
            while won.len() < limit {
                let at = self.now();
                let won_ids: Vec<&str> = won.iter().map(|d| d.id.as_str()).collect();
                let mut filter = Self::unfinished();
                filter.extend(Self::claimable_by(owner, at));
                filter.insert("_id", doc! { "$nin": won_ids });
                let candidates: Vec<OutboxDocument> = self
                    .docs
                    .find(filter)
                    .sort(doc! { "createdAt": 1, "seq": 1 })
                    .limit(i64::try_from(limit - won.len()).unwrap_or(i64::MAX))
                    .await
                    .store()?
                    .try_collect()
                    .await
                    .store()?;
                if candidates.is_empty() {
                    break;
                }
                for candidate in candidates {
                    let at = self.now();
                    let lease_ms = i64::try_from(lease.as_millis()).unwrap_or(i64::MAX);
                    let until = date(at.timestamp_millis().saturating_add(lease_ms));
                    let mut filter = Self::unfinished();
                    filter.extend(Self::claimable_by(owner, at));
                    filter.insert("_id", &candidate.id);
                    let claimed = self
                        .docs
                        .find_one_and_update(
                            filter,
                            doc! { "$set": { "owner": owner, "leaseUntil": until } },
                        )
                        .return_document(ReturnDocument::After)
                        .await
                        .store()?;
                    won.extend(claimed);
                }
            }
            won.sort_by_key(|d| (d.created_at, d.seq));
            won.into_iter().map(OutboxDocument::into_entry).collect()
        })
    }

    fn mark_sent<'a>(&'a self, ids: &'a [String]) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if ids.is_empty() {
                return Ok(());
            }
            self.docs
                .update_many(
                    doc! { "_id": { "$in": ids.to_vec() } },
                    doc! { "$set": { "sentAt": self.now() } },
                )
                .await
                .store()?;
            Ok(())
        })
    }

    fn mark_attempt_failed<'a>(
        &'a self,
        id: &'a str,
        _error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.docs
                .update_one(doc! { "_id": id }, doc! { "$inc": { "attempts": 1 } })
                .await
                .store()?;
            Ok(())
        })
    }

    fn mark_failed<'a>(
        &'a self,
        id: &'a str,
        _error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.docs
                .update_one(
                    doc! { "_id": id },
                    doc! { "$inc": { "attempts": 1 }, "$set": { "failedAt": self.now() } },
                )
                .await
                .store()?;
            Ok(())
        })
    }

    fn purge(&self, sent_before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            Ok(self
                .docs
                .delete_many(doc! { "sentAt": { "$lt": date(sent_before_ms) } })
                .await
                .store()?
                .deleted_count)
        })
    }

    fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move { self.docs.count_documents(Self::unfinished()).await.store() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refusal(payload: &[u8]) -> String {
        match payload_document("t.Happened", payload) {
            Err(StoreError::Failed(MessagingError::InvalidMessage(why))) => why,
            other => panic!("expected InvalidMessage, got {other:?}"),
        }
    }

    #[test]
    fn a_payload_that_is_not_json_is_invalid() {
        let why = refusal(b"{not json");
        assert!(
            why.starts_with("the payload of event t.Happened is not JSON"),
            "{why}"
        );
    }

    #[test]
    fn a_payload_that_is_not_an_object_is_invalid() {
        for payload in [&b"[1,2]"[..], b"null", b"\"text\"", b"42"] {
            let why = refusal(payload);
            assert!(why.contains("is not a JSON object"), "{why}");
        }
    }

    #[test]
    fn a_number_bson_cannot_hold_is_invalid() {
        let why = refusal(format!("{{\"n\":{}}}", u64::MAX).as_bytes());
        assert!(
            why.starts_with("the payload of event t.Happened cannot be an outbox document"),
            "{why}"
        );
    }

    #[test]
    fn an_entry_is_written_in_the_kotlin_shape() {
        let entry = OutboxEntry {
            id: "e-1".into(),
            type_name: "t.Happened".into(),
            key: Some("k1".into()),
            headers: HashMap::from([("userId".to_string(), String::new())]),
            payload: br#"{"id":"e-1"}"#.to_vec(),
            created_at_ms: 1_762_250_000_000,
            seq: 42,
            attempts: 0,
        };
        let doc = mongodb::bson::to_document(&OutboxDocument::from_entry(entry).unwrap()).unwrap();
        let mut keys: Vec<&str> = doc.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "_id",
                "attempts",
                "createdAt",
                "failedAt",
                "headers",
                "key",
                "payload",
                "sentAt",
                "seq",
                "type"
            ],
            "no owner nor lease until a relay claims it"
        );
        assert!(matches!(doc.get("createdAt"), Some(Bson::DateTime(_))));
        assert_eq!(doc.get_document("payload").unwrap(), &doc! { "id": "e-1" });
        assert_eq!(doc.get_i64("seq"), Ok(42));
        assert_eq!(doc.get_i32("attempts"), Ok(0));
        assert_eq!(doc.get("sentAt"), Some(&Bson::Null));
        assert_eq!(doc.get("failedAt"), Some(&Bson::Null));
    }

    #[test]
    fn a_json_object_becomes_a_document() {
        let doc = payload_document("t.Happened", br#"{"n":1,"s":"x"}"#).expect("a document");
        assert_eq!(doc, doc! { "n": 1_i64, "s": "x" });
    }
}
