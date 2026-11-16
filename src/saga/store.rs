//! The saga records (ADR 0021): what the saga ports store, field for field the storage contract
//! both libraries keep.
//!
//! Times are epoch milliseconds (`_ms`), enums their Kotlin names, ids strings, and the saga
//! `state`, the dispatch and failure `payload` and a deadline `payload` are JSON **text** (the
//! codec's exact output, ruling 25). How a store keeps them is the store's own: the MongoDB
//! add-on writes the documents both libraries have always written (dates as BSON datetimes, an
//! absent field for `None`), so Kotlin and Rust read each other's records.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::failed::FailureReason;

/// An association `k = v` of a saga instance: events whose association value is `v` for key `k`
/// are routed to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Association {
    pub k: String,
    pub v: String,
}

impl Association {
    pub fn new(k: &str, v: &str) -> Association {
        Association {
            k: k.to_string(),
            v: v.to_string(),
        }
    }
}

/// One saga instance (`sagas`). `state` is the saga object as JSON text, `version` the optimistic
/// version every write checks (0 when created, +1 per write). While `ended` is false the instance
/// is live: its `associations` route events to it and its `start_key`
/// (`"<type>|<key>=<value>"`, absent for `forceNew` instances) is unique. Ended instances expire
/// after the store's retention, counted from `ended_at_ms`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaDocument {
    pub id: String,
    /// The saga's wire name (`sagas.type`).
    pub type_name: String,
    pub state: String,
    pub associations: Vec<Association>,
    pub start_key: Option<String>,
    pub ended: bool,
    pub ended_at_ms: Option<i64>,
    pub version: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Whether a `saga_dispatch` entry is a command (through the hub or the service's own handler)
/// or an event (through the outbox).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DispatchKind {
    Command,
    Event,
}

/// One command or event a saga recorded in its transaction, waiting in the saga-side outbox
/// (`saga_dispatch`). `seq` orders the entries of one saga (`version * 1000 + index`); the id
/// `"<sagaId>:<seq>"` is the command's idempotency key. A command given up on carries its
/// outcome (`failure_reason`, `failure_message`, written with `failed_at_ms`), and
/// `notice_pending` is true until the `SagaCommandFailed` was delivered locally (ruling 28).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaDispatchDocument {
    pub id: String,
    pub saga_id: String,
    pub seq: i64,
    pub kind: DispatchKind,
    /// The message's wire type name.
    pub type_name: String,
    pub payload: String,
    /// `correlation-id`: the correlation id of the message that caused the entry.
    pub headers: HashMap<String, String>,
    /// A command's routing key or an event's key.
    pub routing_key: Option<String>,
    pub created_at_ms: i64,
    pub dispatched_at_ms: Option<i64>,
    pub attempts: i32,
    pub next_attempt_at_ms: Option<i64>,
    pub failed_at_ms: Option<i64>,
    pub failure_reason: Option<FailureReason>,
    pub failure_message: Option<String>,
    pub notice_pending: Option<bool>,
    pub notice_attempts: Option<i32>,
    /// The notice's own retry; also reserves its delivery to the instance that claimed it.
    pub notice_next_at_ms: Option<i64>,
}

/// Lifecycle of a [`DeadlineDocument`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeadlineStatus {
    Scheduled,
    Fired,
    Cancelled,
}

/// A deadline of a saga instance (`deadlines`). `payload` is the JSON text of the payload (any
/// JSON value) and `payload_type` its type name, both absent when there is none. A scheduler
/// claims a due deadline with `claimed_by` and `claimed_until_ms` (a lease: an expired one lets it
/// fire again); `finished_at_ms` is set when it is `FIRED` or `CANCELLED`; `correlation_id` is the
/// correlation id of the message that scheduled it (ruling 37).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadlineDocument {
    pub id: String,
    pub saga_type: String,
    pub saga_id: String,
    pub name: String,
    pub payload: Option<String>,
    pub payload_type: Option<String>,
    pub due_at_ms: i64,
    pub status: DeadlineStatus,
    pub claimed_by: Option<String>,
    pub claimed_until_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub correlation_id: Option<String>,
}

/// A saga event that failed all its attempts, parked in `saga_failures` (id
/// `<sagaType>:<eventId>`): the event as received (enough to rebuild its message context), the
/// last error and how many attempts were made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaFailureDocument {
    pub id: String,
    pub saga_type: String,
    pub event_id: String,
    pub event_type: String,
    pub payload: String,
    /// The message headers, with `correlationId` and `userId` when known.
    pub headers: HashMap<String, String>,
    pub error: String,
    pub attempts: i32,
    pub first_failed_at_ms: i64,
    pub last_failed_at_ms: i64,
}
