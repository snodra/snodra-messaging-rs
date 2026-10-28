//! The JSON envelope used on Kafka (`kraken.events`) and inside hub requests.
//! Header names match the table in kraken/docs/02-messaging.md.

use serde::{Deserialize, Serialize};

/// Kafka record header names.
pub mod headers {
    pub const TYPE: &str = "type";
    pub const ID: &str = "id";
    pub const CORRELATION_ID: &str = "correlationId";
    pub const TIMESTAMP: &str = "timestamp";
    pub const SOURCE: &str = "source";
    pub const USER_ID: &str = "userId";
    pub const TRACEPARENT: &str = "traceparent";
    pub const TRACESTATE: &str = "tracestate";
}

/// Hub `Request.headers` keys.
pub mod hub_headers {
    pub const AUTHORIZATION: &str = "authorization";
    pub const CORRELATION_ID: &str = "correlationId";
    pub const TRACEPARENT: &str = "traceparent";
    pub const TRACESTATE: &str = "tracestate";
}

/// One message as it travels on the wire: metadata plus a JSON payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Envelope {
    /// Wire type name, for example `files.FileUploaded`.
    #[serde(rename = "type")]
    pub message_type: String,
    /// Message id (UUID v7). Used for idempotency by consumers.
    pub id: String,
    /// Propagated from the originating request; new UUID when absent.
    pub correlation_id: String,
    /// RFC 3339 UTC, when the message was created.
    pub timestamp: String,
    /// Publishing service name.
    pub source: String,
    /// Keycloak subject of the acting user; `None` for system messages.
    pub user_id: Option<String>,
    /// The message body as produced by the owning service's contract.
    pub payload: serde_json::Value,
}

impl Envelope {
    pub fn new(
        message_type: impl Into<String>,
        source: impl Into<String>,
        user_id: Option<String>,
        payload: serde_json::Value,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        Self {
            message_type: message_type.into(),
            id: uuid::Uuid::now_v7().to_string(),
            correlation_id: uuid::Uuid::now_v7().to_string(),
            // TODO: format as RFC 3339 once a time crate is chosen (Builder call: `time` or `jiff`).
            timestamp: now.to_string(),
            source: source.into(),
            user_id,
            payload,
        }
    }
}
