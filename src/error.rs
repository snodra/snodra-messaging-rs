//! Errors seen by callers, mirroring `MessagingExceptions.kt` and the hub `ErrorCode`s.

use crate::proto::ErrorCode;

#[derive(Debug, thiserror::Error)]
pub enum MessagingError {
    #[error("No handler registered for '{0}'")]
    NoHandler(String),
    #[error("Timed out waiting for reply to '{0}'")]
    Timeout(String),
    #[error("Handler for '{0}' disconnected")]
    HandlerDisconnected(String),
    /// Hub code RESOURCE_EXHAUSTED: the hub's queue or session buffer for the target instance is
    /// full (flow control). The handler never saw the request; a retry after a backoff is safe.
    #[error("Handler for '{0}' is busy")]
    HandlerBusy(String),
    #[error("{message}")]
    RemoteHandler {
        type_name: String,
        message: String,
        details: Vec<u8>,
    },
    #[error("{0}")]
    Unauthenticated(String),
    #[error("{0}")]
    AccessDenied(String),
    #[error("{0}")]
    InvalidMessage(String),
    /// The request was never written to a live hub stream (not connected, or the write failed):
    /// the hub never saw it, a retry is safe.
    #[error("{0}")]
    HubUnavailable(String),
    /// The request was written to a hub stream that broke before the answer arrived: the outcome
    /// is unknown (the handler may have run).
    #[error("{0}")]
    HubConnectionLost(String),
    #[error("{0}")]
    HubRegistrationRejected(String),
    /// The feature needs a newer hub: no hub this client is registered on lists its capability in
    /// the `RegisterAck` (or the hub answered `UNSUPPORTED`). Nothing was sent; not retried.
    #[error("{0}")]
    UnsupportedByHub(String),
    /// Hub code CANCELLED (ADR 0017, P4b): the hub ended a streaming query (it outlived
    /// `KRAKEN_HUB_MAX_STREAM_DURATION`). Not retried.
    #[error("{0}")]
    Cancelled(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("transport error: {0}")]
    Transport(String),
    /// An adapter's external call failed in a way a later attempt may cure (a timeout, a 503, a
    /// rate limit): Kotlin's `RetryableExternalException`. Adapters never retry internally; the
    /// saga relay owns the backoff. It crosses the hub as `HANDLER_ERROR` with the
    /// [`RETRYABLE_MARKER`](crate::saga::RETRYABLE_MARKER) in `details` (ADR 0021 ruling 12), and an
    /// idempotent handler resets its call record for it, so the next attempt runs the body.
    #[error("{0}")]
    RetryableExternal(String),
    /// Whether the external system acted is unknown (a connection broke after the request went
    /// out, or an earlier call of the key crashed mid-way): Kotlin's
    /// `ExternalCallUnknownOutcomeException`. It crosses the hub as `HANDLER_ERROR` with the
    /// [`UNKNOWN_OUTCOME_MARKER`](crate::saga::UNKNOWN_OUTCOME_MARKER) in `details`; the relay
    /// reports it as `SagaCommandFailed(UNKNOWN_OUTCOME)`, and an idempotent handler never calls the
    /// key's body again (ruling 39).
    #[error("{0}")]
    UnknownOutcome(String),
}

impl MessagingError {
    /// Maps a hub `Error` to the exception a caller gets, exactly as `HubErrors.kt`.
    pub fn from_hub(type_name: &str, code: i32, message: &str, details: &[u8]) -> Self {
        match ErrorCode::try_from(code).unwrap_or(ErrorCode::Unspecified) {
            ErrorCode::NoHandler => Self::NoHandler(type_name.to_string()),
            ErrorCode::Timeout => Self::Timeout(type_name.to_string()),
            ErrorCode::HandlerDisconnected => Self::HandlerDisconnected(type_name.to_string()),
            ErrorCode::Unauthenticated => Self::Unauthenticated(message.to_string()),
            ErrorCode::InvalidRequest => Self::InvalidMessage(message.to_string()),
            ErrorCode::PermissionDenied => Self::AccessDenied(message.to_string()),
            ErrorCode::ResourceExhausted => Self::HandlerBusy(type_name.to_string()),
            // A request mode the hub does not implement or share (ADR 0017).
            ErrorCode::Unsupported => Self::UnsupportedByHub(format!("'{type_name}': {message}")),
            // The hub ended a streaming query (P4b).
            ErrorCode::Cancelled => Self::Cancelled(format!("'{type_name}': {message}")),
            ErrorCode::HandlerError | ErrorCode::Unspecified => Self::RemoteHandler {
                type_name: type_name.to_string(),
                message: message.to_string(),
                details: details.to_vec(),
            },
        }
    }

    /// The `details` a handler's error carries inside a `Reply`: an adapter's failure kind
    /// (ruling 12), or what a remote handler sent.
    pub fn reply_details(&self) -> Vec<u8> {
        match self {
            Self::RemoteHandler { details, .. } => details.clone(),
            Self::RetryableExternal(_) => crate::saga::RETRYABLE_MARKER.to_vec(),
            Self::UnknownOutcome(_) => crate::saga::UNKNOWN_OUTCOME_MARKER.to_vec(),
            _ => Vec::new(),
        }
    }

    /// The variant's name (`"Storage"`, `"RemoteHandler"`, ...): a dead letter's `error.type`
    /// (ADR 0022), the Rust form of Kotlin's exception simple name.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::NoHandler(_) => "NoHandler",
            Self::Timeout(_) => "Timeout",
            Self::HandlerDisconnected(_) => "HandlerDisconnected",
            Self::HandlerBusy(_) => "HandlerBusy",
            Self::RemoteHandler { .. } => "RemoteHandler",
            Self::Unauthenticated(_) => "Unauthenticated",
            Self::AccessDenied(_) => "AccessDenied",
            Self::InvalidMessage(_) => "InvalidMessage",
            Self::HubUnavailable(_) => "HubUnavailable",
            Self::HubConnectionLost(_) => "HubConnectionLost",
            Self::HubRegistrationRejected(_) => "HubRegistrationRejected",
            Self::UnsupportedByHub(_) => "UnsupportedByHub",
            Self::Cancelled(_) => "Cancelled",
            Self::Serialization(_) => "Serialization",
            Self::Storage(_) => "Storage",
            Self::Transport(_) => "Transport",
            Self::RetryableExternal(_) => "RetryableExternal",
            Self::UnknownOutcome(_) => "UnknownOutcome",
        }
    }

    /// The code a handler's error becomes inside a `Reply`.
    pub fn to_reply_code(&self) -> ErrorCode {
        match self {
            Self::NoHandler(_) => ErrorCode::NoHandler,
            Self::Unauthenticated(_) => ErrorCode::Unauthenticated,
            Self::AccessDenied(_) => ErrorCode::PermissionDenied,
            Self::InvalidMessage(_) => ErrorCode::InvalidRequest,
            _ => ErrorCode::HandlerError,
        }
    }
}

impl From<tonic::Status> for MessagingError {
    fn from(status: tonic::Status) -> Self {
        Self::Transport(status.to_string())
    }
}

impl From<mongodb::error::Error> for MessagingError {
    fn from(e: mongodb::error::Error) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<rdkafka::error::KafkaError> for MessagingError {
    fn from(e: rdkafka::error::KafkaError) -> Self {
        Self::Transport(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_variant_name_is_the_dead_letter_error_type() {
        assert_eq!(MessagingError::Storage("x".into()).variant_name(), "Storage");
        assert_eq!(
            MessagingError::InvalidMessage("x".into()).variant_name(),
            "InvalidMessage"
        );
        assert_eq!(
            MessagingError::RemoteHandler {
                type_name: "t".into(),
                message: "m".into(),
                details: vec![],
            }
            .variant_name(),
            "RemoteHandler"
        );
        let json = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        assert_eq!(MessagingError::from(json).variant_name(), "Serialization");
        assert_eq!(
            MessagingError::RetryableExternal("x".into()).variant_name(),
            "RetryableExternal"
        );
    }

    use super::*;

    #[test]
    fn resource_exhausted_is_handler_busy() {
        let e = MessagingError::from_hub(
            "t.X",
            ErrorCode::ResourceExhausted as i32,
            "queue full",
            b"",
        );
        assert!(
            matches!(e, MessagingError::HandlerBusy(ref t) if t == "t.X"),
            "{e:?}"
        );
        assert_eq!(e.to_string(), "Handler for 't.X' is busy");
        // A handler that returns it itself answers HANDLER_ERROR like any other failure.
        assert_eq!(e.to_reply_code(), ErrorCode::HandlerError);
    }

    #[test]
    fn unsupported_is_unsupported_by_hub_and_never_retried() {
        let e = MessagingError::from_hub(
            "t.X",
            ErrorCode::Unsupported as i32,
            "request mode scatter_gather is not supported by this hub",
            b"",
        );
        assert!(matches!(e, MessagingError::UnsupportedByHub(_)), "{e:?}");
        assert!(e.to_string().contains("not supported"), "{e}");
        assert!(!crate::retry::is_retryable(
            crate::proto::RequestKind::Query,
            &e
        ));
    }

    #[test]
    fn cancelled_is_its_own_error_and_never_retried() {
        let e = MessagingError::from_hub(
            "t.X",
            ErrorCode::Cancelled as i32,
            "max stream duration (10m) exceeded",
            b"",
        );
        assert!(matches!(e, MessagingError::Cancelled(_)), "{e:?}");
        assert!(e.to_string().contains("max stream duration"), "{e}");
        assert!(!crate::retry::is_retryable(
            crate::proto::RequestKind::Query,
            &e
        ));
        assert_eq!(e.to_reply_code(), ErrorCode::HandlerError);
    }

    #[test]
    fn handler_error_busy_from_an_old_hub_stays_remote_handler() {
        let e =
            MessagingError::from_hub("t.X", ErrorCode::HandlerError as i32, "handler busy", b"");
        assert!(matches!(e, MessagingError::RemoteHandler { .. }), "{e:?}");
    }
}
