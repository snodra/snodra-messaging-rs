//! Incoming hub requests: identity, lookup, guard, decode, handler, reply. Streaming queries
//! (ADR 0017, P4b): the handler's stream pulled under the sender's credits, one `ReplyPart` per
//! item, then the `ReplyEnd`. The hub's `Flow` and `Cancel` reach the running request by id.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::context::{CURRENT, MessageContext, hub_headers};
use crate::error::MessagingError;
use crate::identity::{Identity, IdentityResolver};
use crate::proto::{
    self, ClientMessage, ErrorCode, Reply, RequestKind, RequestMode, client_message, reply,
};

use super::connection::ReplyTo;
use super::registry::{HandlerRegistry, Invocation, ItemStream};

/// How the hub reaches a running request: its cancel token and, for a stream, its credits.
struct Control {
    cancel: CancellationToken,
    credits: mpsc::UnboundedSender<u32>,
}

/// A request registered as running ([`Dispatcher::serve`]); unregistered on drop.
struct Running {
    dispatcher: Arc<Dispatcher>,
    id: String,
    cancel: CancellationToken,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.dispatcher.running.lock().unwrap().remove(&self.id);
    }
}

/// Aborts the task when dropped (a cancelled request must not leave its handler running).
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Turns a `Request` into a `Reply` (or, for a streaming query, into parts and an end), never
/// into a dropped stream. Every outcome, including a rejected token, is a message the hub
/// forwards to the sender; the one exception is the hub's own `Cancel`, after which nothing
/// more is sent.
pub struct Dispatcher {
    registry: Arc<HandlerRegistry>,
    resolver: Arc<IdentityResolver>,
    permits: Arc<Semaphore>,
    /// Requests being served, by id.
    running: Mutex<HashMap<String, Control>>,
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
            running: Mutex::new(HashMap::new()),
        }
    }

    /// Handler permits not held now (`max_concurrent_handlers` at rest). A streaming query holds
    /// its permit until its end, whatever the end.
    pub fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }

    /// The hub's `Flow{request_id, credits}` for a streaming query served here: its handler's
    /// stream may be pulled for `credits` more items. Ignored for an unknown id.
    pub fn flow(&self, request_id: &str, credits: u32) {
        let running = self.running.lock().unwrap();
        match running.get(request_id) {
            Some(control) => {
                let _ = control.credits.send(credits);
            }
            None => tracing::debug!(
                request = request_id,
                credits,
                "flow for an unknown request ignored"
            ),
        }
    }

    /// The hub's `Cancel{request_id, reason}`: the running handler (unary, a scatter-gather
    /// target, or streaming) is aborted and nothing more is sent for the request; its permits go
    /// back. Ignored for an unknown id (it already ended).
    pub fn cancel(&self, request_id: &str, reason: &str) {
        let control = self.running.lock().unwrap().remove(request_id);
        match control {
            Some(control) => {
                tracing::debug!(request = request_id, reason, "request cancelled by the hub");
                control.cancel.cancel();
            }
            None => tracing::debug!(
                request = request_id,
                reason,
                "cancel for an unknown request ignored"
            ),
        }
    }

    /// Serves `request` on `reply_to`, the stream it arrived on (never on whatever stream is
    /// current: a reply on another stream, after a reconnect or to another hub, would be ignored
    /// by the hub). The request is registered before the returned future runs, so a `Flow` or
    /// `Cancel` for its id that follows it at once still finds it. A unary request gets its
    /// `Reply`; a streaming one (`mode = STREAMING`) its parts under the credits and its end.
    /// One permit goes back to the hub at the end, whatever the end.
    pub fn serve(
        self: &Arc<Self>,
        request: proto::Request,
        reply_to: ReplyTo,
    ) -> impl Future<Output = ()> + Send + 'static {
        let (running, credits) = self.accept(&request.id);
        let me = Arc::clone(self);
        async move {
            let request_id = request.id.clone();
            if request.mode == RequestMode::Streaming as i32 {
                me.stream(&running, request, credits, &reply_to).await;
            } else if let Some(reply) = me.unary(request, Some(&running.cancel)).await
                && let Err(e) = reply_to
                    .send(ClientMessage {
                        message: Some(client_message::Message::Reply(reply)),
                    })
                    .await
            {
                tracing::warn!(request = %request_id, stream = reply_to.stream(), "reply dropped: {e}");
            }
            drop(running);
            // The request is finished either way: one permit back to the hub it came from.
            reply_to.grant().await;
        }
    }

    /// One unary request to its `Reply`, as [`serve`](Self::serve) does it but never cancelled.
    pub async fn handle(&self, req: proto::Request) -> Reply {
        self.unary(req, None)
            .await
            .expect("a request without a cancel token always gets a reply")
    }

    /// Registers `request_id` as running: its control for [`flow`](Self::flow) and
    /// [`cancel`](Self::cancel), and the receiving end of its credits.
    fn accept(self: &Arc<Self>, request_id: &str) -> (Running, mpsc::UnboundedReceiver<u32>) {
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::unbounded_channel();
        self.running.lock().unwrap().insert(
            request_id.to_string(),
            Control {
                cancel: cancel.clone(),
                credits: tx,
            },
        );
        (
            Running {
                dispatcher: Arc::clone(self),
                id: request_id.to_string(),
                cancel,
            },
            rx,
        )
    }

    /// The reply to a unary request; `None` when `cancel` fired first (the handler is aborted,
    /// nothing is to be sent).
    async fn unary(
        &self,
        req: proto::Request,
        cancel: Option<&CancellationToken>,
    ) -> Option<Reply> {
        let request_id = req.id.clone();
        let run = self.run(req);
        let outcome = match cancel {
            Some(token) => tokio::select! {
                _ = token.cancelled() => return None,
                outcome = run => outcome,
            },
            None => run.await,
        };
        let result = match outcome {
            Ok(payload) => reply::Result::Payload(payload),
            Err(e) => {
                let error = hub_error(&request_id, &e);
                tracing::debug!(request = %request_id, code = error.code, "request rejected: {e}");
                reply::Result::Error(error)
            }
        };
        Some(Reply {
            request_id,
            result: Some(result),
        })
    }

    async fn run(&self, req: proto::Request) -> Result<Vec<u8>, MessagingError> {
        let identity = self.identity(&req).await?;
        let handler = match kind(&req)? {
            RequestKind::Command => self.registry.command_handler(&req.r#type),
            _ => self.registry.query_handler(&req.r#type),
        }
        .cloned()
        .ok_or_else(|| MessagingError::NoHandler(req.r#type.clone()))?;
        let type_name = req.r#type.clone();
        let (ctx, _permit, payload) = self.admit(req, identity).await?;
        // Spawned so that a panic is this request's error, not the dispatcher's; aborted when the
        // caller drops the run (the hub cancelled the request).
        let mut task = AbortOnDrop(tokio::spawn(
            CURRENT.scope(ctx.clone(), async move { handler(payload, ctx).await }),
        ));
        let handler_error = |message: String| MessagingError::RemoteHandler {
            type_name: type_name.clone(),
            message,
            details: vec![],
        };
        match (&mut task.0).await {
            Ok(outcome) => outcome.map_err(|e| invocation_error(&type_name, e)),
            Err(join) if join.is_panic() => Err(handler_error("handler panicked".into())),
            Err(join) => Err(handler_error(format!("handler task failed: {join}"))),
        }
    }

    /// The caller's identity, from the forwarded token.
    async fn identity(&self, req: &proto::Request) -> Result<Option<Identity>, MessagingError> {
        let authorization = req.headers.get(hub_headers::AUTHORIZATION);
        self.resolver
            .resolve(authorization.map(String::as_str))
            .await
    }

    /// What every request goes through between its lookup and its handler: the context, the
    /// guard (before decoding: anonymous callers cannot probe payload shapes) and a permit.
    async fn admit(
        &self,
        req: proto::Request,
        identity: Option<Identity>,
    ) -> Result<(MessageContext, OwnedSemaphorePermit, Vec<u8>), MessagingError> {
        let authorization = req.headers.get(hub_headers::AUTHORIZATION).cloned();
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
            guard.check(ctx.identity.as_ref(), &ctx)?;
        }
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MessagingError::HubUnavailable("dispatcher closed".into()))?;
        Ok((ctx, permit, req.payload))
    }

    /// A streaming request: its parts under the credits (`initial_credit` plus every `Flow`;
    /// the handler's stream is never pulled without credit), then its end. A hub `Cancel` aborts
    /// the pump (the handler's stream is dropped, nothing more is sent). The handler permit is
    /// held until the end, whatever the end.
    async fn stream(
        self: &Arc<Self>,
        running: &Running,
        req: proto::Request,
        credits: mpsc::UnboundedReceiver<u32>,
        reply_to: &ReplyTo,
    ) {
        let request_id = req.id.clone();
        let type_name = req.r#type.clone();
        // Spawned so that a panic in the handler or its stream is this stream's error.
        let mut pump = AbortOnDrop(tokio::spawn(Arc::clone(self).pump(
            req,
            credits,
            reply_to.clone(),
        )));
        tokio::select! {
            _ = running.cancel.cancelled() => {}
            joined = &mut pump.0 => match joined {
                Ok(()) => {}
                Err(join) if join.is_panic() => {
                    let e = MessagingError::RemoteHandler {
                        type_name,
                        message: "handler panicked".into(),
                        details: vec![],
                    };
                    send_end(reply_to, &request_id, Some(hub_error(&request_id, &e))).await;
                }
                Err(_) => {}
            },
        }
    }

    async fn pump(
        self: Arc<Self>,
        req: proto::Request,
        mut credits: mpsc::UnboundedReceiver<u32>,
        reply_to: ReplyTo,
    ) {
        let request_id = req.id.clone();
        let type_name = req.r#type.clone();
        let mut credit = req.initial_credit;
        let (_permit, ctx, mut items) = match self.open(req).await {
            Ok(opened) => opened,
            Err(e) => {
                tracing::debug!(request = %request_id, "streaming request rejected: {e}");
                send_end(&reply_to, &request_id, Some(hub_error(&request_id, &e))).await;
                return;
            }
        };
        let pumping = async move {
            let mut seq: u64 = 0;
            loop {
                while credit == 0 {
                    match credits.recv().await {
                        Some(more) => credit = credit.saturating_add(more),
                        None => return,
                    }
                }
                while let Ok(more) = credits.try_recv() {
                    credit = credit.saturating_add(more);
                }
                match items.next().await {
                    None => {
                        send_end(&reply_to, &request_id, None).await;
                        return;
                    }
                    Some(Ok(payload)) => {
                        seq += 1;
                        credit -= 1;
                        let part = ClientMessage {
                            message: Some(client_message::Message::Part(proto::ReplyPart {
                                request_id: request_id.clone(),
                                payload,
                                seq,
                                ..Default::default()
                            })),
                        };
                        if let Err(e) = reply_to.send(part).await {
                            tracing::warn!(request = %request_id, stream = reply_to.stream(), "part dropped, stream abandoned: {e}");
                            return;
                        }
                    }
                    Some(Err(invocation)) => {
                        let e = invocation_error(&type_name, invocation);
                        tracing::debug!(request = %request_id, "stream failed: {e}");
                        send_end(&reply_to, &request_id, Some(hub_error(&request_id, &e))).await;
                        return;
                    }
                }
            }
        };
        CURRENT.scope(ctx, pumping).await;
    }

    /// Admits a streaming request and runs its handler to the stream of items.
    async fn open(
        &self,
        req: proto::Request,
    ) -> Result<(OwnedSemaphorePermit, MessageContext, ItemStream), MessagingError> {
        let identity = self.identity(&req).await?;
        let handler = match kind(&req)? {
            RequestKind::Query => self.registry.stream_handler(&req.r#type),
            _ => None,
        }
        .cloned()
        .ok_or_else(|| MessagingError::NoHandler(req.r#type.clone()))?;
        let type_name = req.r#type.clone();
        let (ctx, permit, payload) = self.admit(req, identity).await?;
        let items = CURRENT
            .scope(ctx.clone(), handler(payload, ctx.clone()))
            .await
            .map_err(|e| invocation_error(&type_name, e))?;
        Ok((permit, ctx, items))
    }
}

/// The request's kind; `InvalidMessage` when it names none.
fn kind(req: &proto::Request) -> Result<RequestKind, MessagingError> {
    match RequestKind::try_from(req.kind).unwrap_or(RequestKind::Unspecified) {
        RequestKind::Unspecified => Err(MessagingError::InvalidMessage(format!(
            "unsupported request kind {}",
            req.kind
        ))),
        kind => Ok(kind),
    }
}

/// Why an erased handler did not produce its payload (or an item), as this handler's error.
/// Only authorization outcomes keep their code; anything else a handler returns (including a
/// nested call's NoHandler or a decode error of its own) is this handler's error.
fn invocation_error(type_name: &str, invocation: Invocation) -> MessagingError {
    let handler_error = |message: String| MessagingError::RemoteHandler {
        type_name: type_name.to_string(),
        message,
        details: vec![],
    };
    match invocation {
        Invocation::Decode(m) => MessagingError::InvalidMessage(m),
        Invocation::Encode(m) => handler_error(m),
        Invocation::Handler(
            e @ (MessagingError::Unauthenticated(_) | MessagingError::AccessDenied(_)),
        ) => e,
        Invocation::Handler(MessagingError::RemoteHandler {
            message, details, ..
        }) => MessagingError::RemoteHandler {
            type_name: type_name.to_string(),
            message,
            details,
        },
        Invocation::Handler(e) => handler_error(e.to_string()),
    }
}

/// The hub `Error` a handler's failure becomes (inside a `Reply` or a `ReplyEnd`).
fn hub_error(request_id: &str, e: &MessagingError) -> proto::Error {
    let (code, details) = match e {
        MessagingError::RemoteHandler { details, .. } => (ErrorCode::HandlerError, details.clone()),
        other => (other.to_reply_code(), vec![]),
    };
    proto::Error {
        request_id: request_id.to_string(),
        code: code as i32,
        message: e.to_string(),
        details,
    }
}

/// Writes the `ReplyEnd` of a streaming request to its arrival stream.
async fn send_end(reply_to: &ReplyTo, request_id: &str, error: Option<proto::Error>) {
    let end = ClientMessage {
        message: Some(client_message::Message::End(proto::ReplyEnd {
            request_id: request_id.to_string(),
            error,
            ..Default::default()
        })),
    };
    if let Err(e) = reply_to.send(end).await {
        tracing::warn!(
            request = request_id,
            stream = reply_to.stream(),
            "end dropped: {e}"
        );
    }
}
