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

#[derive(Clone, Default)]
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
    /// The acting user: from the resolved identity on the hub path, from the `userId` header on
    /// the Kafka path (events carry no token), as Kotlin's `MessageContext.userId` does.
    pub fn user_id(&self) -> Option<&str> {
        self.identity
            .as_ref()
            .and_then(|i| i.user_id.as_deref())
            .or_else(|| {
                self.headers
                    .get("userId")
                    .map(String::as_str)
                    .filter(|u| !u.is_empty())
            })
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

impl std::fmt::Debug for MessageContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Bearer tokens never reach the logs.
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str(),
                    if k.eq_ignore_ascii_case(hub_headers::AUTHORIZATION) {
                        "<redacted>"
                    } else {
                        v.as_str()
                    },
                )
            })
            .collect();
        f.debug_struct("MessageContext")
            .field("message_id", &self.message_id)
            .field("type_name", &self.type_name)
            .field("correlation_id", &self.correlation_id)
            .field("identity", &self.identity)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .field("headers", &headers)
            .finish()
    }
}

tokio::task_local! {
    /// Set by the dispatcher and the consumer around every handler invocation.
    pub static CURRENT: MessageContext;
}
