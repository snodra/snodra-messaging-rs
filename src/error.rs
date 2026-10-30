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
    #[error("{0}")]
    HubUnavailable(String),
    #[error("{0}")]
    HubRegistrationRejected(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("transport error: {0}")]
    Transport(String),
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
            ErrorCode::HandlerError | ErrorCode::Unspecified => Self::RemoteHandler {
                type_name: type_name.to_string(),
                message: message.to_string(),
                details: details.to_vec(),
            },
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
