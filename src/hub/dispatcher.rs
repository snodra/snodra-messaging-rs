//! Incoming hub requests: identity, lookup, guard, decode, handler, reply.

use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::context::{CURRENT, MessageContext, hub_headers};
use crate::error::MessagingError;
use crate::identity::IdentityResolver;
use crate::proto::{self, ErrorCode, Reply, RequestKind, reply};

use super::registry::{HandlerRegistry, RequestHandler};

/// Turns a `Request` into a `Reply`, never into a dropped stream. Every outcome, including a
/// rejected token, is a reply the hub forwards to the sender.
pub struct Dispatcher {
    registry: Arc<HandlerRegistry>,
    resolver: Arc<IdentityResolver>,
    permits: Arc<Semaphore>,
}

impl Dispatcher {
    pub fn new(
        registry: Arc<HandlerRegistry>,
        resolver: Arc<IdentityResolver>,
        max_concurrent_handlers: usize,
    ) -> Dispatcher {
        Dispatcher {
            registry,
            resolver,
            permits: Arc::new(Semaphore::new(max_concurrent_handlers.max(1))),
        }
    }

    pub async fn handle(&self, req: proto::Request) -> Reply {
        let request_id = req.id.clone();
        match self.run(req).await {
            Ok(payload) => Reply {
                request_id,
                result: Some(reply::Result::Payload(payload)),
            },
            Err(e) => {
                let (code, details) = match &e {
                    MessagingError::RemoteHandler { details, .. } => {
                        (ErrorCode::HandlerError, details.clone())
                    }
                    other => (other.to_reply_code(), vec![]),
                };
                tracing::debug!(request = %request_id, code = ?code, "request rejected: {e}");
                Reply {
                    request_id: request_id.clone(),
                    result: Some(reply::Result::Error(proto::Error {
                        request_id,
                        code: code as i32,
                        message: e.to_string(),
                        details,
                    })),
                }
            }
        }
    }

    async fn run(&self, req: proto::Request) -> Result<Vec<u8>, MessagingError> {
        let authorization = req.headers.get(hub_headers::AUTHORIZATION).cloned();
        let identity = self.resolver.resolve(authorization.as_deref()).await?;
        let handler: RequestHandler =
            match RequestKind::try_from(req.kind).unwrap_or(RequestKind::Unspecified) {
                RequestKind::Command => self.registry.command_handler(&req.r#type),
                RequestKind::Query => self.registry.query_handler(&req.r#type),
                RequestKind::Unspecified => {
                    return Err(MessagingError::InvalidMessage(format!(
                        "unsupported request kind {}",
                        req.kind
                    )));
                }
            }
            .cloned()
            .ok_or_else(|| MessagingError::NoHandler(req.r#type.clone()))?;
        let ctx = MessageContext {
            message_id: req.id.clone(),
            type_name: req.r#type.clone(),
            correlation_id: req
                .headers
                .get(hub_headers::CORRELATION_ID)
                .cloned()
                .unwrap_or_else(|| req.id.clone()),
            identity,
            authorization,
            headers: req.headers,
        };
        if let Some(guard) = self.registry.guard(&req.r#type) {
            guard.check(ctx.identity.as_ref(), &ctx)?; // before decoding: anonymous callers cannot probe payload shapes
        }
        let _permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MessagingError::HubUnavailable("dispatcher closed".into()))?;
        let payload = req.payload;
        let task =
            tokio::spawn(CURRENT.scope(ctx.clone(), async move { handler(payload, ctx).await }));
        match task.await {
            Ok(result) => result,
            Err(join) if join.is_panic() => Err(MessagingError::RemoteHandler {
                type_name: req.r#type,
                message: "handler panicked".into(),
                details: vec![],
            }),
            Err(join) => Err(MessagingError::RemoteHandler {
                type_name: req.r#type,
                message: format!("handler task failed: {join}"),
                details: vec![],
            }),
        }
    }
}
