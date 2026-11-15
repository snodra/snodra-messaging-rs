//! `EventBus::publish` appends to the message store's outbox; the publisher relays it to the event
//! store.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::event_store::headers;
use crate::store::{OutboxEntry, OutboxStore, Tx};
use crate::time::now_ms;
use crate::types::Event;

/// What publishing without a store fails with.
pub(crate) const PUBLISH_NEEDS_STORE: &str = "publishing an event needs a message store: add .store(...) (store::memory::MemoryStore runs the in-memory one, not durable: tests and local runs)";
/// What publishing without an event store fails with.
pub(crate) const PUBLISH_NEEDS_EVENT_STORE: &str = "publishing an event needs an event store: add .event_store(...) (event_store::memory::MemoryEventStore runs the in-memory one, not durable: tests and local runs)";

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
    if let Some(tp) = ctx.and_then(MessageContext::traceparent) {
        hdrs.insert(headers::TRACEPARENT.to_string(), tp.to_string());
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

/// The deepest outbox payload, as MySQL's `JSON_DEPTH` counts: a scalar or an empty object or
/// array is 1, a non-empty one 1 + its deepest member (`{}` 1, `{"a":1}` 2, `{"a":[1]}` 3). MySQL
/// keeps no document nesting more than 100 objects and arrays, so the shared bound is depth 100
/// (both libraries: one under MySQL's own limit for a document ending in a scalar).
pub const MAX_PAYLOAD_DEPTH: usize = 100;

/// An outbox payload must be a JSON object whose integers fit in a signed 64-bit integer, at most
/// [`MAX_PAYLOAD_DEPTH`] deep (the outbox contract, [`OutboxEntry::payload`]). Anything else (a
/// unit struct's `null`, an array, a bare value, an integer above `i64::MAX`, deeper) is
/// `InvalidMessage` naming the event type: no retry can change it.
pub(crate) fn check_payload(
    type_name: &str,
    json: &serde_json::Value,
) -> Result<(), MessagingError> {
    let found = match json {
        serde_json::Value::Object(_) => {
            if let Some(n) = too_big(json) {
                return Err(MessagingError::InvalidMessage(format!(
                    "the payload of event {type_name} holds the integer {n}, above i64::MAX, so it cannot be an outbox entry"
                )));
            }
            let deep = depth(json);
            if deep > MAX_PAYLOAD_DEPTH {
                return Err(MessagingError::InvalidMessage(format!(
                    "the payload of event {type_name} is {deep} deep (JSON depth), deeper than the \
                     {MAX_PAYLOAD_DEPTH} every store keeps, so it cannot be an outbox entry"
                )));
            }
            return Ok(());
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

/// MySQL's `JSON_DEPTH` of `json`: a scalar or an empty object or array 1, a non-empty one 1 +
/// its deepest member.
fn depth(json: &serde_json::Value) -> usize {
    match json {
        serde_json::Value::Array(items) if !items.is_empty() => {
            1 + items.iter().map(depth).max().unwrap_or(0)
        }
        serde_json::Value::Object(fields) if !fields.is_empty() => {
            1 + fields.values().map(depth).max().unwrap_or(0)
        }
        _ => 1,
    }
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

/// The outbox contract every store applies, checked on a JSON text
/// ([`OutboxEntry::payload`](crate::store::OutboxEntry::payload)): JSON (UTF-8, no byte order
/// mark, no lone surrogate escape, no number a double cannot hold), an object at most
/// [`MAX_PAYLOAD_DEPTH`] deep, and every integer in it (a number written without `.`, `e` or
/// `E`) within the signed 64-bit range. The
/// integers are read from the text itself: a JSON parser reads an integer below `i64::MIN` or
/// above `u64::MAX` as a float, which a store would then keep changed.
pub fn check_outbox_payload(type_name: &str, payload: &[u8]) -> Result<(), MessagingError> {
    let json: serde_json::Value = serde_json::from_slice(payload).map_err(|e| {
        MessagingError::InvalidMessage(format!(
            "the payload of event {type_name} is not JSON, so it cannot be an outbox entry: {e}"
        ))
    })?;
    check_payload(type_name, &json)?;
    // Valid JSON, hence UTF-8.
    let text = std::str::from_utf8(payload).unwrap_or_default();
    match integer_out_of_range(text) {
        None => Ok(()),
        Some(n) => {
            // The token is ASCII: cut on any byte, at most 64 of them in the message.
            let shown = if n.len() > MAX_SHOWN_INTEGER {
                format!("{}... ({} characters)", &n[..MAX_SHOWN_INTEGER], n.len())
            } else {
                n.to_string()
            };
            Err(MessagingError::InvalidMessage(format!(
                "the payload of event {type_name} holds the integer {shown}, {}, so it cannot be an outbox entry",
                if n.starts_with('-') {
                    "below i64::MIN"
                } else {
                    "above i64::MAX"
                }
            )))
        }
    }
}

pub(crate) use check_outbox_payload as check_payload_text;

/// The most characters of a refused integer an error message quotes.
const MAX_SHOWN_INTEGER: usize = 64;

/// The first integer token of the JSON `text` (outside strings, without `.`, `e` or `E`) that
/// no `i64` holds. `text` is valid JSON.
fn integer_out_of_range(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_string {
            match c {
                b'\\' => i += 2,
                b'"' => {
                    in_string = false;
                    i += 1;
                }
                _ => i += 1,
            }
            continue;
        }
        match c {
            b'"' => {
                in_string = true;
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while i < bytes.len()
                    && matches!(bytes[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    i += 1;
                }
                let token = &text[start..i];
                if !token.contains(['.', 'e', 'E']) && token.parse::<i64>().is_err() {
                    return Some(token);
                }
            }
            _ => i += 1,
        }
    }
    None
}

/// The publishing side of events: appends to the message store's outbox, inside the caller's
/// transaction when given. Cheap to clone; every clone shares the store, bound once by
/// [`KrakenMessagingBuilder::store`](crate::KrakenMessagingBuilder::store), so a bus taken before
/// the store was given publishes there too. Without a store every publish fails.
#[derive(Clone)]
pub struct EventBus {
    outbox: Arc<OnceLock<Arc<dyn OutboxStore>>>,
    /// Set by a start without an event store.
    no_event_store: Arc<AtomicBool>,
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
            no_event_store: Arc::new(AtomicBool::new(false)),
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

    /// The service started without an event store: nothing relays the outbox, so publishing
    /// fails (on this bus and its every clone), naming the missing add-on.
    pub(crate) fn without_event_store(&self) {
        self.no_event_store.store(true, Ordering::SeqCst);
    }

    fn outbox(&self) -> Result<&Arc<dyn OutboxStore>, MessagingError> {
        let outbox = self
            .outbox
            .get()
            .ok_or_else(|| MessagingError::InvalidMessage(PUBLISH_NEEDS_STORE.into()))?;
        if self.no_event_store.load(Ordering::SeqCst) {
            return Err(MessagingError::InvalidMessage(
                PUBLISH_NEEDS_EVENT_STORE.into(),
            ));
        }
        Ok(outbox)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(payload: &str) -> Option<String> {
        match check_outbox_payload("t.E", payload.as_bytes()) {
            Ok(()) => None,
            Err(MessagingError::InvalidMessage(why)) => Some(why),
            Err(other) => panic!("{other:?}"),
        }
    }

    /// At most JSON depth 100, as MySQL's `JSON_DEPTH` counts (`{}` 1, `{"a":1}` 2, `{"a":[1]}`
    /// 3); arrays count as objects do, an empty one as a scalar; the message names the depth.
    #[test]
    fn a_payload_deeper_than_every_store_keeps_is_refused() {
        // `n` objects around a scalar: depth n + 1.
        let objects = |n: usize| format!("{}1{}", "{\"a\":".repeat(n), "}".repeat(n));
        // An object holding n - 1 arrays around a scalar: depth n + 1.
        let arrays = |n: usize| format!("{{\"a\":{}1{}}}", "[".repeat(n - 1), "]".repeat(n - 1));
        // `n` objects, the innermost empty: depth n.
        let empty = |n: usize| format!("{}{{}}{}", "{\"a\":".repeat(n - 1), "}".repeat(n - 1));
        assert_eq!(depth(&serde_json::json!({})), 1);
        assert_eq!(depth(&serde_json::json!({"a": 1})), 2);
        assert_eq!(depth(&serde_json::json!({"a": [1]})), 3);
        assert_eq!(depth(&serde_json::json!({"a": []})), 2);
        for deep in [objects(99), arrays(99), empty(100)] {
            assert_eq!(refused(&deep), None, "depth 100 is kept");
        }
        for deep in [objects(100), arrays(100), empty(101)] {
            let why = refused(&deep).expect("depth 101");
            assert!(why.contains("is 101 deep") && why.contains("100"), "{why}");
        }
        // Each refused before the store: not JSON a Rust consumer reads, whatever the store.
        for bad in [
            "{\"n\":1e400}",
            "{\"n\":-1e400}",
            "{\"s\":\"\\ud800\"}",
            "{\"\\udc00\":1}",
            "\u{feff}{}",
        ] {
            assert!(refused(bad).is_some(), "{bad}");
        }
        assert!(check_outbox_payload("t.E", b"{\"s\":\"\xff\"}").is_err());
        assert_eq!(refused("{\"n\":1e-400,\"s\":\"\\ud83d\\ude00\"}"), None);
    }

    /// Every integer of the text must fit an `i64`, whatever a JSON parser makes of it: below
    /// `i64::MIN` and above `u64::MAX` are read as floats, and were let through.
    #[test]
    fn integers_outside_i64_are_refused_wherever_they_are() {
        for (payload, n) in [
            (r#"{"n":-9223372036854775809}"#, "-9223372036854775809"),
            (r#"{"n":18446744073709551616}"#, "18446744073709551616"),
            (r#"{"n":18446744073709551615}"#, "18446744073709551615"),
            (
                r#"{"n":123456789012345678901234567890}"#,
                "123456789012345678901234567890",
            ),
            (
                r#"{"a":[1,{"b":[2,99999999999999999999]}]}"#,
                "99999999999999999999",
            ),
            (
                r#"{"s":"x\"","n":-99999999999999999999}"#,
                "-99999999999999999999",
            ),
        ] {
            let why = refused(payload).unwrap_or_else(|| panic!("accepted {payload}"));
            assert!(why.contains(&format!("holds the integer {n}, ")), "{why}");
            let side = if n.starts_with('-') {
                "below i64::MIN"
            } else {
                "above i64::MAX"
            };
            assert!(why.contains(side), "{why}");
        }
    }

    #[test]
    fn a_long_refused_integer_is_quoted_cut_to_64_characters() {
        let digits = "9".repeat(100);
        let why = refused(&format!("{{\"n\":{digits}}}")).expect("refused");
        assert!(
            why.contains(&format!(
                "holds the integer {}... (100 characters), above i64::MAX",
                "9".repeat(64)
            )),
            "{why}"
        );
        assert!(why.len() < 200, "{why}");
    }

    #[test]
    fn what_fits_and_what_is_not_an_integer_is_accepted() {
        for payload in [
            r#"{"n":9223372036854775807,"m":-9223372036854775808}"#,
            r#"{"s":"123456789012345678901234567890"}"#,
            r#"{"s":"a\"123456789012345678901234567890\"b"}"#,
            r#"{"d":1.5,"e":1e30,"f":-2.5E-3,"g":123456789012345678901234567890.0}"#,
            r#"{"z":-0,"a":[0,[-1,{"b":2}]]}"#,
            r#"{"k123456789012345678901234567890":1}"#,
        ] {
            assert_eq!(refused(payload), None, "{payload}");
        }
    }
}
