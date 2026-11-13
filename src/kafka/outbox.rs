//! `EventBus::publish` appends to the message store's outbox; the publisher relays it to Kafka.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};

use super::headers;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::store::{OutboxEntry, OutboxStore, Tx};
use crate::time::now_ms;
use crate::types::Event;

/// What publishing without a store fails with.
pub(crate) const PUBLISH_NEEDS_STORE: &str =
    "publishing an event needs a message store: add .store(...)";

/// Builds the outbox entry for `event`: headers from the handler context when there is one.
pub fn new_outbox_entry<E: Event>(
    event: &E,
    key: Option<&str>,
    ctx: Option<&MessageContext>,
    seq: i64,
) -> Result<OutboxEntry, MessagingError> {
    let json = serde_json::to_value(event)?;
    check_payload(E::TYPE, &json)?;
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
    Ok(OutboxEntry {
        id: uuid::Uuid::new_v4().to_string(),
        type_name: E::TYPE.to_string(),
        key: key.map(str::to_string),
        headers: hdrs,
        payload: serde_json::to_vec(&json)?,
        created_at_ms: now_ms(),
        seq,
        attempts: 0,
    })
}

/// An outbox payload must be a JSON object whose integers fit in a signed 64-bit integer (the
/// outbox contract, [`OutboxEntry::payload`]). Anything else (a unit struct's `null`, an array,
/// a bare value, an integer above `i64::MAX`) is `InvalidMessage` naming the event type: no retry
/// can change it.
pub(crate) fn check_payload(
    type_name: &str,
    json: &serde_json::Value,
) -> Result<(), MessagingError> {
    let found = match json {
        serde_json::Value::Object(_) => {
            return match too_big(json) {
                None => Ok(()),
                Some(n) => Err(MessagingError::InvalidMessage(format!(
                    "the payload of event {type_name} holds the integer {n}, above i64::MAX, so it cannot be an outbox entry"
                ))),
            };
        }
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
    };
    Err(MessagingError::InvalidMessage(format!(
        "the payload of event {type_name} is {found}, not a JSON object, so it cannot be an outbox entry"
    )))
}

/// The first integer of `json` no `i64` holds.
fn too_big(json: &serde_json::Value) -> Option<&serde_json::Number> {
    match json {
        serde_json::Value::Number(n) if n.is_u64() && !n.is_i64() => Some(n),
        serde_json::Value::Array(items) => items.iter().find_map(too_big),
        serde_json::Value::Object(fields) => fields.values().find_map(too_big),
        _ => None,
    }
}

/// [`check_payload`] of a JSON text.
pub(crate) fn check_payload_text(type_name: &str, payload: &[u8]) -> Result<(), MessagingError> {
    let json: serde_json::Value = serde_json::from_slice(payload).map_err(|e| {
        MessagingError::InvalidMessage(format!(
            "the payload of event {type_name} is not JSON, so it cannot be an outbox entry: {e}"
        ))
    })?;
    check_payload(type_name, &json)
}

/// The publishing side of events: appends to the message store's outbox, inside the caller's
/// transaction when given. Cheap to clone; every clone shares the store, bound once by
/// [`KrakenMessagingBuilder::store`](crate::KrakenMessagingBuilder::store), so a bus taken before
/// the store was given publishes there too. Without a store every publish fails.
#[derive(Clone)]
pub struct EventBus {
    outbox: Arc<OnceLock<Arc<dyn OutboxStore>>>,
    seq: Arc<AtomicI64>,
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus")
            .field("bound", &self.outbox.get().is_some())
            .finish()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        EventBus::new()
    }
}

impl EventBus {
    /// A bus with no outbox yet ([`bind`](Self::bind)).
    pub fn new() -> EventBus {
        EventBus {
            outbox: Arc::new(OnceLock::new()),
            seq: Arc::new(AtomicI64::new(now_ms() << 20)),
        }
    }

    /// A bus over `outbox`.
    pub fn over(outbox: Arc<dyn OutboxStore>) -> EventBus {
        let bus = EventBus::new();
        bus.bind(outbox);
        bus
    }

    /// Gives the bus (and its every clone) its outbox; a second bind is ignored.
    pub fn bind(&self, outbox: Arc<dyn OutboxStore>) {
        let _ = self.outbox.set(outbox);
    }

    fn outbox(&self) -> Result<&Arc<dyn OutboxStore>, MessagingError> {
        self.outbox
            .get()
            .ok_or_else(|| MessagingError::InvalidMessage(PUBLISH_NEEDS_STORE.into()))
    }

    /// Appends `event` to the outbox on its own, outside any transaction.
    pub async fn publish<E: Event>(
        &self,
        event: &E,
        key: Option<&str>,
    ) -> Result<(), MessagingError> {
        let outbox = self.outbox()?;
        let entry = self.entry(event, key)?;
        Ok(outbox.append_now(entry).await?)
    }

    /// Same as [`publish`](Self::publish) but inside `tx` (the store's transaction, e.g. an event
    /// handler's `EventContext::tx`), so the event commits with the caller's writes.
    pub async fn publish_in<E: Event>(
        &self,
        event: &E,
        key: Option<&str>,
        tx: &mut Tx,
    ) -> Result<(), MessagingError> {
        let outbox = self.outbox()?;
        let entry = self.entry(event, key)?;
        Ok(outbox.append(tx, entry).await?)
    }

    /// The outbox entry of an event known only by its wire type name and its JSON payload text
    /// (what the saga relay holds for an `EVENT` entry of `saga_dispatch`): the headers carry
    /// `correlation_id` and no user, as an event published outside any handler. A payload that is
    /// not JSON or not a JSON object is `InvalidMessage`.
    pub fn raw_entry(
        &self,
        type_name: &str,
        key: Option<&str>,
        payload_json: &str,
        correlation_id: &str,
    ) -> Result<OutboxEntry, MessagingError> {
        check_payload_text(type_name, payload_json.as_bytes())?;
        let mut hdrs = HashMap::new();
        hdrs.insert(
            headers::CORRELATION_ID.to_string(),
            correlation_id.to_string(),
        );
        hdrs.insert(headers::USER_ID.to_string(), String::new());
        Ok(OutboxEntry {
            id: uuid::Uuid::new_v4().to_string(),
            type_name: type_name.to_string(),
            key: key.map(str::to_string),
            headers: hdrs,
            payload: payload_json.as_bytes().to_vec(),
            created_at_ms: now_ms(),
            seq: self.next_seq(),
            attempts: 0,
        })
    }

    /// Appends an entry built by [`raw_entry`](Self::raw_entry) inside `tx`.
    pub async fn publish_raw_in(
        &self,
        entry: OutboxEntry,
        tx: &mut Tx,
    ) -> Result<(), MessagingError> {
        Ok(self.outbox()?.append(tx, entry).await?)
    }

    /// The outbox entry of `event`, with the current handler's context.
    pub fn entry<E: Event>(
        &self,
        event: &E,
        key: Option<&str>,
    ) -> Result<OutboxEntry, MessagingError> {
        let ctx = MessageContext::current();
        new_outbox_entry(event, key, ctx.as_ref(), self.next_seq())
    }

    fn next_seq(&self) -> i64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }
}
