//! The documents of the saga collections: the storage contract of ADR 0021, field for field.
//!
//! Dates are BSON datetimes, enums are their Kotlin names, `_id`s are strings, and the saga
//! `state`, the dispatch and failure `payload`, a deadline `payload` and an external call's
//! `reply` and `error` are JSON **text** (the codec's exact output, ruling 25). A field that is
//! absent reads as `None`; `None` is never written (no nulls), and no `_class` is written or
//! required (readers ignore the one Spring Data writes). Kotlin and Rust therefore read each
//! other's documents.

use std::collections::HashMap;

use mongodb::bson::DateTime;
use serde::{Deserialize, Serialize};

use super::failed::FailureReason;

pub const SAGAS_COLLECTION: &str = "sagas";
pub const SAGA_DISPATCH_COLLECTION: &str = "saga_dispatch";
pub const SAGA_DISPATCH_LEASES_COLLECTION: &str = "saga_dispatch_leases";
pub const DEADLINES_COLLECTION: &str = "deadlines";
pub const SAGA_FAILURES_COLLECTION: &str = "saga_failures";
pub const EXTERNAL_CALLS_COLLECTION: &str = "external_calls";

/// An association `k = v` of a saga instance: events whose association value is `v` for key `k`
/// are routed to it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
/// through the TTL index on `ended_at`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SagaDocument {
    #[serde(rename = "_id")]
    pub id: String,
    /// The saga's wire name (`sagas.type`).
    #[serde(rename = "type")]
    pub type_name: String,
    pub state: String,
    #[serde(default)]
    pub associations: Vec<Association>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_key: Option<String>,
    #[serde(default)]
    pub ended: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime>,
    #[serde(default)]
    pub version: i64,
    pub created_at: DateTime,
    pub updated_at: DateTime,
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
/// (`saga_dispatch`). `seq` orders the entries of one saga (`version * 1000 + index`); the `_id`
/// `"<sagaId>:<seq>"` is the command's idempotency key. A command given up on carries its
/// outcome (`failure_reason`, `failure_message`, written with `failed_at`), and `notice_pending`
/// is true until the `SagaCommandFailed` was delivered locally (ruling 28).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SagaDispatchDocument {
    #[serde(rename = "_id")]
    pub id: String,
    pub saga_id: String,
    pub seq: i64,
    pub kind: DispatchKind,
    /// The message's wire type name.
    #[serde(rename = "type")]
    pub type_name: String,
    pub payload: String,
    /// `correlation-id`: the correlation id of the message that caused the entry.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// A command's routing key or an event's key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
    pub created_at: DateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<DateTime>,
    #[serde(default)]
    pub attempts: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<FailureReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice_pending: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice_attempts: Option<i32>,
    /// The notice's own retry; also reserves its delivery to the instance that claimed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice_next_at: Option<DateTime>,
}

/// The lease of one saga's dispatch order (`saga_dispatch_leases`): the relay instance holding it
/// dispatches that saga's entries one after the other. An expired lease is as good as none; the
/// TTL index only tidies up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaDispatchLeaseDocument {
    /// The saga id.
    #[serde(rename = "_id")]
    pub id: String,
    pub owner: String,
    pub until: DateTime,
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
/// claims a due deadline with `claimed_by` and `claimed_until` (a lease: an expired one lets it
/// fire again); `finished_at` is set when it is `FIRED` or `CANCELLED`; `correlation_id` is the
/// correlation id of the message that scheduled it (ruling 37).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadlineDocument {
    #[serde(rename = "_id")]
    pub id: String,
    pub saga_type: String,
    pub saga_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_type: Option<String>,
    pub due_at: DateTime,
    pub status: DeadlineStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_until: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

/// A saga event that failed all its attempts, parked in `saga_failures` (`_id`
/// `<sagaType>:<eventId>`): the event as received (enough to rebuild its message context), the
/// last error and how many attempts were made.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SagaFailureDocument {
    #[serde(rename = "_id")]
    pub id: String,
    pub saga_type: String,
    pub event_id: String,
    pub event_type: String,
    pub payload: String,
    /// The message headers, with `correlationId` and `userId` when known.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    pub error: String,
    pub attempts: i32,
    pub first_failed_at: DateTime,
    pub last_failed_at: DateTime,
}

/// Where an external call stands in `external_calls`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExternalCallStatus {
    InProgress,
    Succeeded,
    Failed,
    Unknown,
}

/// The record of one idempotent external call (`external_calls`), keyed by its idempotency key.
/// `reply` is the JSON text of the handler's reply and `error` the JSON text of a `FAILED` call's
/// error (`{"code","message"}`); an `IN_PROGRESS` record whose `lease_until` has passed means the
/// outcome is unknown. Expires through the TTL index on `updated_at`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalCallDocument {
    #[serde(rename = "_id")]
    pub id: String,
    pub status: ExternalCallStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub attempts: i32,
    pub updated_at: DateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_until: Option<DateTime>,
    /// The ownership token of the call running it (a random id written at the insert and at a
    /// take-over), unset when the call is settled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}
