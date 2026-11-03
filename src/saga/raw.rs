//! Raw command sending for the saga relay (ADR 0021 rulings 8, 10, 12, 14, 29 and 34): a command
//! known only by its wire type name and JSON bytes, delivered to this instance's own handler
//! directly (no hub: spec decision 5) or through the hub, and answered with a classified
//! [`RawOutcome`] instead of a decoded reply. Kept apart from the public command API.

use std::sync::Arc;

use futures::future::BoxFuture;

use super::failed::FailureReason;
use crate::context::{CURRENT, MessageContext, hub_headers};
use crate::error::MessagingError;
use crate::hub::sender::reject_reserved_headers;
use crate::hub::{AbortOnDrop, HandlerRegistry, SendOptions, Sender, invocation_error};

/// What an adapter's error carries in `details` when the external call failed for a reason a
/// later attempt may cure: the relay reads it to tell a transient failure from an ordinary
/// handler failure, whether the adapter ran in this service or in another (ruling 12; the Kotlin
/// marker, byte for byte).
pub const RETRYABLE_MARKER: &[u8] = br#"{"kraken":"retryable"}"#;
/// What an adapter's error carries in `details` when the call's outcome is unknown (ruling 12).
pub const UNKNOWN_OUTCOME_MARKER: &[u8] = br#"{"kraken":"unknown-outcome"}"#;

/// What a raw send reports, classified for the relay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RawOutcome {
    /// The handler ran (or the idempotent adapter replayed its stored reply).
    Success,
    /// A failure a later attempt may cure: the relay backs off and retries.
    Transient(String),
    /// A failure no retry cures: it becomes a `SagaCommandFailed` with this reason at once.
    Permanent(FailureReason, String),
}

fn has_marker(details: &[u8], marker: &[u8]) -> bool {
    String::from_utf8_lossy(details).trim().as_bytes() == marker
}

/// The retry taxonomy of ADR 0021 (rulings 9 and 10; the relay does not share the buses' retry
/// classification): a send that may not have reached the handler, or whose outcome the
/// idempotency key makes safe to repeat, is transient; the handler's own answers are permanent.
/// Anything that is not a messaging failure (the sender's or the store's own) is transient and
/// bounded by the relay's attempt limit.
pub fn classify_failure(error: &MessagingError) -> RawOutcome {
    let message = error.to_string();
    match error {
        MessagingError::HubUnavailable(_)
        | MessagingError::Timeout(_)
        | MessagingError::HandlerDisconnected(_)
        | MessagingError::HubConnectionLost(_)
        | MessagingError::HandlerBusy(_)
        | MessagingError::Transport(_)
        | MessagingError::Storage(_) => RawOutcome::Transient(message),
        MessagingError::NoHandler(_) => RawOutcome::Permanent(FailureReason::NoHandler, message),
        MessagingError::Unauthenticated(_) | MessagingError::AccessDenied(_) => {
            RawOutcome::Permanent(FailureReason::Denied, message)
        }
        MessagingError::InvalidMessage(_) | MessagingError::Serialization(_) => {
            RawOutcome::Permanent(FailureReason::Invalid, message)
        }
        MessagingError::RetryableExternal(_) => RawOutcome::Transient(message),
        MessagingError::UnknownOutcome(_) => {
            RawOutcome::Permanent(FailureReason::UnknownOutcome, message)
        }
        MessagingError::RemoteHandler { details, .. } => {
            if has_marker(details, RETRYABLE_MARKER) {
                RawOutcome::Transient(message)
            } else if has_marker(details, UNKNOWN_OUTCOME_MARKER) {
                RawOutcome::Permanent(FailureReason::UnknownOutcome, message)
            } else {
                RawOutcome::Permanent(FailureReason::Failed, message)
            }
        }
        // Any other messaging failure is a handler error.
        MessagingError::HubRegistrationRejected(_)
        | MessagingError::UnsupportedByHub(_)
        | MessagingError::Cancelled(_) => RawOutcome::Permanent(FailureReason::Failed, message),
    }
}

/// Sends a command it knows only by wire type name and JSON bytes: the relay has no Rust type for
/// a stored command, and a saga never reads a command's reply. A send carries no caller (no
/// token, no identity), so a handler registered with a [`Guard`](crate::Guard) refuses it
/// (`DENIED`), and no retry of its own (the relay owns the backoff).
pub trait RawCommandSender: Send + Sync {
    /// `options.headers` reach the handler's `MessageContext.headers`; `options.routing_key` is
    /// the hub's routing key. The correlation id is the current handler context's, else a fresh
    /// one.
    fn send_raw<'a>(
        &'a self,
        type_name: &'a str,
        payload: Vec<u8>,
        options: SendOptions,
    ) -> BoxFuture<'a, RawOutcome>;
}

/// Delivers to this service's own command handlers only: what a service with no hub does (an
/// unknown type is a permanent `NO_HANDLER`, ruling 34), and the local half of
/// [`LocalFirstSender`]. The handler runs on a task of its own (a panic is its failure), outside
/// the dispatcher's concurrency permits.
pub struct LocalRawCommandSender {
    registry: Arc<HandlerRegistry>,
}

impl LocalRawCommandSender {
    pub fn new(registry: Arc<HandlerRegistry>) -> LocalRawCommandSender {
        LocalRawCommandSender { registry }
    }

    /// Whether this service handles the command type.
    pub fn handles(&self, type_name: &str) -> bool {
        self.registry.command_handler(type_name).is_some()
    }

    async fn deliver(&self, type_name: &str, payload: Vec<u8>, options: SendOptions) -> RawOutcome {
        if let Err(e) = reject_reserved_headers(&options.headers) {
            return classify_failure(&e);
        }
        let Some(handler) = self.registry.command_handler(type_name).cloned() else {
            return classify_failure(&MessagingError::NoHandler(type_name.to_string()));
        };
        let current = MessageContext::current();
        let correlation_id = current
            .as_ref()
            .map(|c| c.correlation_id.clone())
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let mut headers = options.headers;
        headers.insert(
            hub_headers::CORRELATION_ID.to_string(),
            correlation_id.clone(),
        );
        if let Some(traceparent) = current
            .as_ref()
            .and_then(|c| c.headers.get(hub_headers::TRACEPARENT))
        {
            headers.insert(hub_headers::TRACEPARENT.to_string(), traceparent.clone());
        }
        // No identity and no token on purpose, whoever's handler this runs in.
        let ctx = MessageContext {
            message_id: uuid::Uuid::new_v4().to_string(),
            type_name: type_name.to_string(),
            correlation_id,
            identity: None,
            authorization: None,
            headers,
        };
        if let Some(guard) = self.registry.guard(type_name) {
            // Before decoding, as the dispatcher does: an anonymous caller cannot probe payloads.
            if let Err(e) = guard.check(None, &ctx) {
                return classify_failure(&e);
            }
        }
        let mut task = AbortOnDrop(tokio::spawn(
            CURRENT.scope(ctx.clone(), async move { handler(payload, ctx).await }),
        ));
        let failure = |message: &str| {
            RawOutcome::Permanent(FailureReason::Failed, format!("{type_name}: {message}"))
        };
        match (&mut task.0).await {
            Ok(Ok(_reply)) => RawOutcome::Success,
            Ok(Err(invocation)) => classify_failure(&invocation_error(type_name, invocation)),
            Err(join) if join.is_panic() => failure("handler panicked"),
            Err(join) => failure(&format!("handler task failed: {join}")),
        }
    }
}

impl RawCommandSender for LocalRawCommandSender {
    fn send_raw<'a>(
        &'a self,
        type_name: &'a str,
        payload: Vec<u8>,
        options: SendOptions,
    ) -> BoxFuture<'a, RawOutcome> {
        Box::pin(self.deliver(type_name, payload, options))
    }
}

/// What the saga relay sends with (S2-R5): a command type this instance handles is delivered to
/// its own handler directly, with no hub (spec decision 5: the adapter lives in the service that
/// owns the saga); every other type goes through the hub ([`Sender::send_raw`]), where `NO_HANDLER`
/// is transient. The crate's ordinary senders never short-circuit; only the relay does.
pub struct LocalFirstSender {
    local: LocalRawCommandSender,
    hub: Arc<Sender>,
}

impl LocalFirstSender {
    pub fn new(registry: Arc<HandlerRegistry>, hub: Arc<Sender>) -> LocalFirstSender {
        LocalFirstSender {
            local: LocalRawCommandSender::new(registry),
            hub,
        }
    }
}

impl RawCommandSender for LocalFirstSender {
    fn send_raw<'a>(
        &'a self,
        type_name: &'a str,
        payload: Vec<u8>,
        options: SendOptions,
    ) -> BoxFuture<'a, RawOutcome> {
        if self.local.handles(type_name) {
            Box::pin(self.local.deliver(type_name, payload, options))
        } else {
            Box::pin(self.hub.send_raw(type_name, payload, options))
        }
    }
}
