//! Metadata of the message currently being handled, and the task-local that carries it so nested
//! sends re-forward the caller's identity and correlation id (the Kotlin coroutine context does this).

use std::collections::HashMap;

use crate::identity::Identity;

/// Hub `Request.headers` keys.
pub mod hub_headers {
    pub const AUTHORIZATION: &str = "authorization";
    pub const CORRELATION_ID: &str = "correlationId";
    pub const TRACEPARENT: &str = "traceparent";
    pub const TRACESTATE: &str = "tracestate";
}

#[derive(Clone, Debug, Default)]
pub struct MessageContext {
    /// `Request.id` on the hub path, the `id` header on the Kafka path.
    pub message_id: String,
    pub type_name: String,
    pub correlation_id: String,
    /// Resolved from the forwarded token; `None` for anonymous callers and for events.
    pub identity: Option<Identity>,
    /// Raw `Bearer <jwt>` as received, re-forwarded by nested sends.
    pub authorization: Option<String>,
    pub headers: HashMap<String, String>,
}

impl MessageContext {
    pub fn user_id(&self) -> Option<&str> {
        self.identity.as_ref().and_then(|i| i.user_id.as_deref())
    }

    pub fn roles(&self) -> &[String] {
        self.identity
            .as_ref()
            .map(|i| i.roles.as_slice())
            .unwrap_or(&[])
    }

    /// The context of the handler the current task runs in, if any.
    pub fn current() -> Option<MessageContext> {
        CURRENT.try_with(|c| c.clone()).ok()
    }
}

tokio::task_local! {
    /// Set by the dispatcher and the consumer around every handler invocation.
    pub static CURRENT: MessageContext;
}
