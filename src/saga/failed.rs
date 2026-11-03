//! `SagaCommandFailed`: what the dispatch relay tells a saga when a command it recorded failed.

use serde::{Deserialize, Serialize};

use crate::message_type;
use crate::types::Event;

/// What the dispatch relay tells a saga when a command it recorded could not be delivered or
/// failed: a saga does not wait for replies, so a failure comes back as this event, dispatched
/// locally (never published to `kraken.events`) with the dedupe id `failed:<sagaId>:<seq>`. A saga
/// handles it with [`SagaDefinition::on_command_failed`](super::SagaDefinition::on_command_failed),
/// which the runtime routes by `saga_id` (ADR 0021 ruling 27); unhandled, it is logged at error and
/// counted. `idempotency_key` is the failed command's `<sagaId>:<seq>`, `command_type` its wire
/// type name. The JSON field names are the Kotlin class's.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SagaCommandFailed {
    pub saga_id: String,
    pub command_type: String,
    pub idempotency_key: String,
    pub reason: FailureReason,
    pub message: String,
}
message_type!(SagaCommandFailed, "kraken.SagaCommandFailed");
impl Event for SagaCommandFailed {}

/// Why a command failed (the retry taxonomy of ADR 0021 rulings 9 and 10): the permanent kinds at
/// once, [`Exhausted`](Self::Exhausted) after the transient retries. Serialised by name, as the
/// Kotlin enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FailureReason {
    /// Nobody handles the command type.
    NoHandler,
    /// The handler requires an authenticated caller and a saga command has none.
    Denied,
    /// The handler could not read the payload.
    Invalid,
    /// The handler failed (any other error).
    Failed,
    /// An idempotent adapter crashed mid-call and cannot tell whether the call happened.
    UnknownOutcome,
    /// A transient failure persisted through all the relay's attempts.
    Exhausted,
}
