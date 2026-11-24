//! The documents the store writes, field for field the storage contract every library keeps, and
//! their mapping to the core's records: public for tools and tests that read the collections
//! (`collection::<SagaDoc>("sagas")`, then `SagaDocument::from`). The saga collections' are here
//!; `dead_letters`' and `external_calls`' are re-exported.
//!
//! Dates are BSON datetimes, enums their stored names, `_id`s strings, and the saga `state`, the
//! dispatch and failure `payload` and a deadline `payload` JSON **text**. A field
//! that is absent reads as `None`; `None` is never written (no nulls), and no `_class` is written
//! or required (a reader ignores one). Services written with any Snodra library therefore read
//! each other's documents (the specification's MongoDB layout).

use std::collections::HashMap;

use mongodb::bson::DateTime;
use serde::{Deserialize, Serialize};
use snodra_messaging::saga::FailureReason;
use snodra_messaging::saga::store::{
    Association, DeadlineDocument, DeadlineStatus, DispatchKind, SagaDispatchDocument,
    SagaDocument, SagaFailureDocument,
};

use crate::date;

pub use crate::dead_letters::DeadLetterDocument;
pub use crate::external::ExternalCallDocument;

fn ms(at: DateTime) -> i64 {
    at.timestamp_millis()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssociationDocument {
    k: String,
    v: String,
}

/// `sagas`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SagaDoc {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "type")]
    type_name: String,
    state: String,
    #[serde(default)]
    associations: Vec<AssociationDocument>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    start_key: Option<String>,
    #[serde(default)]
    ended: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ended_at: Option<DateTime>,
    #[serde(default)]
    version: i64,
    created_at: DateTime,
    updated_at: DateTime,
}

impl From<&SagaDocument> for SagaDoc {
    fn from(s: &SagaDocument) -> Self {
        SagaDoc {
            id: s.id.clone(),
            type_name: s.type_name.clone(),
            state: s.state.clone(),
            associations: s
                .associations
                .iter()
                .map(|a| AssociationDocument {
                    k: a.k.clone(),
                    v: a.v.clone(),
                })
                .collect(),
            start_key: s.start_key.clone(),
            ended: s.ended,
            ended_at: s.ended_at_ms.map(date),
            version: s.version,
            created_at: date(s.created_at_ms),
            updated_at: date(s.updated_at_ms),
        }
    }
}

impl From<SagaDoc> for SagaDocument {
    fn from(d: SagaDoc) -> Self {
        SagaDocument {
            id: d.id,
            type_name: d.type_name,
            state: d.state,
            associations: d
                .associations
                .into_iter()
                .map(|a| Association { k: a.k, v: a.v })
                .collect(),
            start_key: d.start_key,
            ended: d.ended,
            ended_at_ms: d.ended_at.map(ms),
            version: d.version,
            created_at_ms: ms(d.created_at),
            updated_at_ms: ms(d.updated_at),
        }
    }
}

/// `saga_dispatch`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DispatchDoc {
    #[serde(rename = "_id")]
    id: String,
    saga_id: String,
    seq: i64,
    kind: DispatchKind,
    #[serde(rename = "type")]
    type_name: String,
    payload: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    routing_key: Option<String>,
    created_at: DateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dispatched_at: Option<DateTime>,
    #[serde(default)]
    attempts: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_attempt_at: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failed_at: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure_reason: Option<FailureReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    notice_pending: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    notice_attempts: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    notice_next_at: Option<DateTime>,
}

impl From<&SagaDispatchDocument> for DispatchDoc {
    fn from(e: &SagaDispatchDocument) -> Self {
        DispatchDoc {
            id: e.id.clone(),
            saga_id: e.saga_id.clone(),
            seq: e.seq,
            kind: e.kind,
            type_name: e.type_name.clone(),
            payload: e.payload.clone(),
            headers: e.headers.clone(),
            routing_key: e.routing_key.clone(),
            created_at: date(e.created_at_ms),
            dispatched_at: e.dispatched_at_ms.map(date),
            attempts: e.attempts,
            next_attempt_at: e.next_attempt_at_ms.map(date),
            failed_at: e.failed_at_ms.map(date),
            failure_reason: e.failure_reason,
            failure_message: e.failure_message.clone(),
            notice_pending: e.notice_pending,
            notice_attempts: e.notice_attempts,
            notice_next_at: e.notice_next_at_ms.map(date),
        }
    }
}

impl From<DispatchDoc> for SagaDispatchDocument {
    fn from(d: DispatchDoc) -> Self {
        SagaDispatchDocument {
            id: d.id,
            saga_id: d.saga_id,
            seq: d.seq,
            kind: d.kind,
            type_name: d.type_name,
            payload: d.payload,
            headers: d.headers,
            routing_key: d.routing_key,
            created_at_ms: ms(d.created_at),
            dispatched_at_ms: d.dispatched_at.map(ms),
            attempts: d.attempts,
            next_attempt_at_ms: d.next_attempt_at.map(ms),
            failed_at_ms: d.failed_at.map(ms),
            failure_reason: d.failure_reason,
            failure_message: d.failure_message,
            notice_pending: d.notice_pending,
            notice_attempts: d.notice_attempts,
            notice_next_at_ms: d.notice_next_at.map(ms),
        }
    }
}

/// `deadlines`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadlineDoc {
    #[serde(rename = "_id")]
    id: String,
    saga_type: String,
    saga_id: String,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload_type: Option<String>,
    due_at: DateTime,
    status: DeadlineStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    claimed_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    claimed_until: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    finished_at: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    correlation_id: Option<String>,
}

impl From<&DeadlineDocument> for DeadlineDoc {
    fn from(d: &DeadlineDocument) -> Self {
        DeadlineDoc {
            id: d.id.clone(),
            saga_type: d.saga_type.clone(),
            saga_id: d.saga_id.clone(),
            name: d.name.clone(),
            payload: d.payload.clone(),
            payload_type: d.payload_type.clone(),
            due_at: date(d.due_at_ms),
            status: d.status,
            claimed_by: d.claimed_by.clone(),
            claimed_until: d.claimed_until_ms.map(date),
            finished_at: d.finished_at_ms.map(date),
            // An empty correlation id is none: left out, as the SQL store keeps it empty.
            correlation_id: d.correlation_id.clone().filter(|c| !c.is_empty()),
        }
    }
}

impl From<DeadlineDoc> for DeadlineDocument {
    fn from(d: DeadlineDoc) -> Self {
        DeadlineDocument {
            id: d.id,
            saga_type: d.saga_type,
            saga_id: d.saga_id,
            name: d.name,
            payload: d.payload,
            payload_type: d.payload_type,
            due_at_ms: ms(d.due_at),
            status: d.status,
            claimed_by: d.claimed_by,
            claimed_until_ms: d.claimed_until.map(ms),
            finished_at_ms: d.finished_at.map(ms),
            correlation_id: d.correlation_id.filter(|c| !c.is_empty()),
        }
    }
}

/// `saga_failures`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureDoc {
    #[serde(rename = "_id")]
    id: String,
    saga_type: String,
    event_id: String,
    event_type: String,
    payload: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    error: String,
    attempts: i32,
    first_failed_at: DateTime,
    last_failed_at: DateTime,
}

impl From<&SagaFailureDocument> for FailureDoc {
    fn from(f: &SagaFailureDocument) -> Self {
        FailureDoc {
            id: f.id.clone(),
            saga_type: f.saga_type.clone(),
            event_id: f.event_id.clone(),
            event_type: f.event_type.clone(),
            payload: f.payload.clone(),
            headers: f.headers.clone(),
            error: f.error.clone(),
            attempts: f.attempts,
            first_failed_at: date(f.first_failed_at_ms),
            last_failed_at: date(f.last_failed_at_ms),
        }
    }
}

impl From<FailureDoc> for SagaFailureDocument {
    fn from(d: FailureDoc) -> Self {
        SagaFailureDocument {
            id: d.id,
            saga_type: d.saga_type,
            event_id: d.event_id,
            event_type: d.event_type,
            payload: d.payload,
            headers: d.headers,
            error: d.error,
            attempts: d.attempts,
            first_failed_at_ms: ms(d.first_failed_at),
            last_failed_at_ms: ms(d.last_failed_at),
        }
    }
}

/// `saga_dispatch_leases`: the lease of one saga's dispatch order. An expired lease is as good as
/// none; the TTL index only tidies up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseDoc {
    /// The saga id.
    #[serde(rename = "_id")]
    id: String,
    owner: String,
    until: DateTime,
}

impl LeaseDoc {
    /// The saga whose dispatch order it leases.
    pub fn saga_id(&self) -> &str {
        &self.id
    }

    /// The relay holding it.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Until when, epoch milliseconds.
    pub fn until_ms(&self) -> i64 {
        ms(self.until)
    }
}
