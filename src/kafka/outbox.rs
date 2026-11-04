//! `EventBus::publish` writes an `outbox` document; the publisher relays it to Kafka.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mongodb::bson::{DateTime, Document};
use mongodb::{ClientSession, Database};
use serde::{Deserialize, Serialize};

use super::{OUTBOX_COLLECTION, headers};
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::types::Event;

/// One event waiting in (or relayed from) the `outbox` collection. Field names are the Kotlin ones.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutboxDocument {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub key: Option<String>,
    pub headers: HashMap<String, String>,
    pub payload: Document,
    pub created_at: DateTime,
    /// Per-process counter breaking `createdAt` millisecond ties, so per-key order holds.
    pub seq: i64,
    pub sent_at: Option<DateTime>,
    pub attempts: i32,
    pub failed_at: Option<DateTime>,
}

/// Builds the document for `event`: headers from the handler context when there is one.
pub fn new_outbox_document<E: Event>(
    event: &E,
    key: Option<&str>,
    ctx: Option<&MessageContext>,
    seq: i64,
) -> Result<OutboxDocument, MessagingError> {
    let json = serde_json::to_value(event)?;
    let payload: Document = mongodb::bson::to_document(&json).map_err(|e| {
        MessagingError::Serialization(serde_json::Error::io(std::io::Error::other(e.to_string())))
    })?;
    let mut hdrs = HashMap::new();
    hdrs.insert(
        headers::CORRELATION_ID.to_string(),
        ctx.map(|c| c.correlation_id.clone())
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
    );
    hdrs.insert(
        headers::USER_ID.to_string(),
        ctx.and_then(|c| c.user_id()).unwrap_or("").to_string(),
    );
    if let Some(tp) = ctx.and_then(|c| c.headers.get(headers::TRACEPARENT)) {
        hdrs.insert(headers::TRACEPARENT.to_string(), tp.clone());
    }
    Ok(OutboxDocument {
        id: uuid::Uuid::new_v4().to_string(),
        type_name: E::TYPE.to_string(),
        key: key.map(str::to_string),
        headers: hdrs,
        payload,
        created_at: DateTime::now(),
        seq,
        sent_at: None,
        attempts: 0,
        failed_at: None,
    })
}

/// The BSON document an event's JSON payload becomes in the outbox: it must be a JSON object
/// whose numbers BSON can hold (an integer above `i64::MAX` it cannot). Anything else (a unit
/// struct's `null`, an array, a bare value) is `InvalidMessage` naming the event type: no retry
/// can change it.
pub(crate) fn payload_document(
    type_name: &str,
    json: &serde_json::Value,
) -> Result<Document, MessagingError> {
    if !json.is_object() {
        let found = match json {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "a boolean",
            serde_json::Value::Number(_) => "a number",
            serde_json::Value::String(_) => "a string",
            serde_json::Value::Array(_) => "an array",
            serde_json::Value::Object(_) => "an object",
        };
        return Err(MessagingError::InvalidMessage(format!(
            "the payload of event {type_name} is {found}, not a JSON object, so it cannot be an outbox document"
        )));
    }
    mongodb::bson::to_document(json).map_err(|e| {
        MessagingError::InvalidMessage(format!(
            "the payload of event {type_name} cannot be an outbox document: {e}"
        ))
    })
}

/// The publishing side of events: inserts into the outbox, inside the caller's session when given.
#[derive(Clone, Debug)]
pub struct EventBus {
    db: Database,
    seq: std::sync::Arc<AtomicI64>,
}

impl EventBus {
    pub fn new(db: Database) -> EventBus {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        EventBus {
            db,
            seq: std::sync::Arc::new(AtomicI64::new(now_ms << 20)),
        }
    }

    pub async fn publish<E: Event>(
        &self,
        event: &E,
        key: Option<&str>,
    ) -> Result<(), MessagingError> {
        let doc = self.document(event, key)?;
        self.db
            .collection::<OutboxDocument>(OUTBOX_COLLECTION)
            .insert_one(doc)
            .await?;
        Ok(())
    }

    /// Same as [`publish`](Self::publish) but inside `session`, so the event commits with the caller's writes.
    pub async fn publish_in<E: Event>(
        &self,
        event: &E,
        key: Option<&str>,
        session: &mut ClientSession,
    ) -> Result<(), MessagingError> {
        let doc = self.document(event, key)?;
        self.db
            .collection::<OutboxDocument>(OUTBOX_COLLECTION)
            .insert_one(doc)
            .session(session)
            .await?;
        Ok(())
    }

    /// The outbox document of an event known only by its wire type name and its JSON payload text
    /// (what the saga relay holds for an `EVENT` entry of `saga_dispatch`): the headers carry
    /// `correlation_id` and no user, as an event published outside any handler.
    pub fn raw_document(
        &self,
        type_name: &str,
        key: Option<&str>,
        payload_json: &str,
        correlation_id: &str,
    ) -> Result<OutboxDocument, MessagingError> {
        let json: serde_json::Value = serde_json::from_str(payload_json).map_err(|e| {
            MessagingError::InvalidMessage(format!(
                "the payload of event {type_name} is not JSON, so it cannot be an outbox document: {e}"
            ))
        })?;
        let payload = payload_document(type_name, &json)?;
        let mut hdrs = HashMap::new();
        hdrs.insert(
            headers::CORRELATION_ID.to_string(),
            correlation_id.to_string(),
        );
        hdrs.insert(headers::USER_ID.to_string(), String::new());
        Ok(OutboxDocument {
            id: uuid::Uuid::new_v4().to_string(),
            type_name: type_name.to_string(),
            key: key.map(str::to_string),
            headers: hdrs,
            payload,
            created_at: DateTime::now(),
            seq: self.seq.fetch_add(1, Ordering::SeqCst) + 1,
            sent_at: None,
            attempts: 0,
            failed_at: None,
        })
    }

    /// Inserts a document built by [`raw_document`](Self::raw_document) inside `session`.
    pub async fn publish_raw_in(
        &self,
        doc: OutboxDocument,
        session: &mut ClientSession,
    ) -> Result<(), MessagingError> {
        self.db
            .collection::<OutboxDocument>(OUTBOX_COLLECTION)
            .insert_one(doc)
            .session(session)
            .await?;
        Ok(())
    }

    pub fn document<E: Event>(
        &self,
        event: &E,
        key: Option<&str>,
    ) -> Result<OutboxDocument, MessagingError> {
        let ctx = MessageContext::current();
        new_outbox_document(
            event,
            key,
            ctx.as_ref(),
            self.seq.fetch_add(1, Ordering::SeqCst) + 1,
        )
    }
}
