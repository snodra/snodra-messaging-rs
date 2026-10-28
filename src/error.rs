//! Error type shared by the hub client and the Kafka transport. Codes mirror `ErrorCode` in
//! `proto/hub.proto` and the error table in kraken/docs/02-messaging.md.

#[derive(Debug, thiserror::Error)]
pub enum MessagingError {
    /// No connected instance handles the message type (hub `NO_HANDLER`).
    #[error("no handler registered for message type {0}")]
    NoHandler(String),
    /// No reply within the request timeout (hub `TIMEOUT`).
    #[error("request {0} timed out")]
    Timeout(String),
    /// The handling instance disconnected before replying (hub `HANDLER_DISCONNECTED`).
    #[error("handler disconnected while processing request {0}")]
    HandlerDisconnected(String),
    /// The handler returned an application error (hub `HANDLER_ERROR`, `UNAUTHENTICATED`,
    /// `INVALID_REQUEST`); `code` is the proto enum number.
    #[error("handler error {code}: {message}")]
    Handler { code: i32, message: String, details: Vec<u8> },
    /// gRPC or Kafka transport failure.
    #[error("transport error: {0}")]
    Transport(String),
    /// JSON (de)serialisation failure.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// The code path is not written yet. Exists only while this crate is a skeleton.
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

impl From<tonic::Status> for MessagingError {
    fn from(status: tonic::Status) -> Self {
        MessagingError::Transport(status.to_string())
    }
}

impl From<rdkafka::error::KafkaError> for MessagingError {
    fn from(err: rdkafka::error::KafkaError) -> Self {
        MessagingError::Transport(err.to_string())
    }
}
