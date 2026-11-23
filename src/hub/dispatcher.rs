//! Incoming hub requests: identity, lookup, guard, decode, handler, reply. Streaming queries
//!: the handler's stream pulled under the sender's credits, one `ReplyPart` per
//! item, then the `ReplyEnd`. The hub's `Flow` and `Cancel` reach the running request by id.
//! Subscription queries: `mode = SUBSCRIPTION` registers a subscription (see
//! [`super::subscriptions`]), the initial result (`<id>#0`, `mode = UNARY`) is served like any
//! unary request.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::context::{CURRENT, MessageContext, hub_headers};
use crate::error::MessagingError;
use crate::identity::{Identity, IdentityResolver};
use crate::proto::{
    self, ClientMessage, ErrorCode, Reply, RequestKind, RequestMode, client_message, reply,
};

use super::connection::{ReplyTo, StreamId};
use super::registry::SubscriptionHandler;
use super::registry::{HandlerRegistry, Invocation, ItemStream};
use super::subscriptions::{
    Admission, DEFAULT_MAX_SUBSCRIPTIONS, QueryUpdateEmitter, Subscriptions,
};

/// How the hub reaches a running request: its cancel token and, for a stream, its credits.
struct Control {
    cancel: CancellationToken,
    credits: mpsc::UnboundedSender<u32>,
    /// The hub stream the request arrived on.
    stream: StreamId,
    /// A streaming query (aborted when its stream is lost; a unary one runs on).
    streaming: bool,
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
pub(crate) struct AbortOnDrop<T>(pub(crate) JoinHandle<T>);

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
    /// The longest a streaming handler waits for credit (`Config.hub_stream_credit_timeout`).
    stream_credit_timeout: Duration,
    /// The subscriptions served here, shared with the [`QueryUpdateEmitter`]s.
    subscriptions: Arc<Subscriptions>,
    /// Awaited by a subscription request before its identity is resolved (tests: a slow
    /// identity check, to hold requests in admission).
    admission_hook: Option<AdmissionHook>,
}

/// What a test runs at the start of a subscription's admission, with the request id.
#[doc(hidden)]
pub type AdmissionHook = Arc<dyn Fn(&str) -> BoxFuture<'static, ()> + Send + Sync>;

/// Why a subscription request is refused.
enum Rejected {
    /// The instance already serves that many subscriptions.
    Limit(usize),
    Error(MessagingError),
}

impl From<MessagingError> for Rejected {
    fn from(e: MessagingError) -> Self {
        Rejected::Error(e)
    }
}

/// A subscription request as [`Dispatcher::serve`] read it, before its admission: registered
/// (pending) when it could be, else what is wrong with it. The flaws are reported in the order
/// the admission always checked: lookup, then the guard, then the decode, then the limit.
struct Prepared {
    admission: Option<Admission>,
    flaw: Flaw,
}

enum Flaw {
    None,
    /// No handler of this mode for the type, or not a query.
    Lookup(MessagingError),
    /// The query does not decode.
    Decode(MessagingError),
    /// The instance serves its limit already.
    Limit(usize),
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
            stream_credit_timeout: crate::config::DEFAULT_STREAM_CREDIT_TIMEOUT,
            subscriptions: Arc::new(Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS)),
            admission_hook: None,
        }
    }

    /// The most subscriptions this instance serves at once (`Config.hub_max_subscriptions`;
    /// default [`DEFAULT_MAX_SUBSCRIPTIONS`], at least 1): a `Request{mode = SUBSCRIPTION}`
    /// beyond it is answered `ReplyEnd{error = RESOURCE_EXHAUSTED "subscription limit (N)
    /// reached"}`.
    pub fn max_subscriptions(self, max: usize) -> Dispatcher {
        self.subscriptions.set_max(max);
        self
    }

    /// Serves the subscriptions `emitter` feeds (one created before this dispatcher, so
    /// handlers could capture it) instead of its own; its limit is kept.
    pub fn with_emitter(mut self, emitter: QueryUpdateEmitter) -> Dispatcher {
        self.subscriptions = Arc::clone(emitter.inner());
        self
    }

    /// Test hook: `hook(request_id)` is awaited by every subscription request before its
    /// identity is resolved, standing in for a slow identity check.
    #[doc(hidden)]
    pub fn admission_hook(mut self, hook: AdmissionHook) -> Dispatcher {
        self.admission_hook = Some(hook);
        self
    }

    /// An emitter for the subscriptions this dispatcher serves.
    pub fn emitter(&self) -> QueryUpdateEmitter {
        QueryUpdateEmitter::from_inner(Arc::clone(&self.subscriptions))
    }

    /// Subscriptions open on this instance now.
    pub fn open_subscriptions(&self) -> usize {
        self.subscriptions.len()
    }

    /// Completes every subscription cleanly with a `ReplyEnd` (shutdown), best effort: the
    /// returned tasks finish once the ends are written (or their streams are gone).
    pub fn end_subscriptions(&self) -> Vec<JoinHandle<()>> {
        self.subscriptions.end_all()
    }

    /// The longest a streaming handler waits for credit before its stream ends with `TIMEOUT`
    /// `no credit within <d>` and its permits go back (`Config.hub_stream_credit_timeout`;
    /// default [`crate::config::DEFAULT_STREAM_CREDIT_TIMEOUT`]). A hub that forgot the stream
    /// (a lost `Flow` or `Cancel`) would otherwise leave the handler task waiting for ever.
    pub fn stream_credit_timeout(mut self, timeout: Duration) -> Dispatcher {
        self.stream_credit_timeout = timeout;
        self
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
    /// back. A subscription (`<id>#n`) is removed: nothing more is emitted to it. Ignored
    /// for an unknown id (it already ended).
    pub fn cancel(&self, request_id: &str, reason: &str) {
        let control = self.running.lock().unwrap().remove(request_id);
        match control {
            Some(control) => {
                tracing::debug!(request = request_id, reason, "request cancelled by the hub");
                control.cancel.cancel();
            }
            None if self.subscriptions.cancel(request_id) => {
                tracing::debug!(
                    request = request_id,
                    reason,
                    "subscription cancelled by the hub"
                );
            }
            None => tracing::debug!(
                request = request_id,
                reason,
                "cancel for an unknown request ignored"
            ),
        }
    }

    /// The hub stream `stream` was lost: the streaming queries that arrived on it are cancelled
    /// (the hub forgot them with the session; their handlers would otherwise wait for credit for
    /// ever, holding their permits) and the subscriptions that arrived on it are removed. Unary
    /// handlers run on, their replies dropped.
    pub fn stream_lost(&self, stream: StreamId) {
        let subscriptions = self.subscriptions.stream_lost(stream);
        if subscriptions > 0 {
            tracing::debug!(
                stream,
                subscriptions,
                "subscriptions removed: their hub stream was lost"
            );
        }
        let lost: Vec<(String, Control)> = {
            let mut running = self.running.lock().unwrap();
            let ids: Vec<String> = running
                .iter()
                .filter(|(_, c)| c.stream == stream && c.streaming)
                .map(|(id, _)| id.clone())
                .collect();
            ids.into_iter()
                .filter_map(|id| running.remove(&id).map(|c| (id, c)))
                .collect()
        };
        for (id, control) in lost {
            tracing::debug!(request = %id, stream, "streaming query cancelled: its hub stream was lost");
            control.cancel.cancel();
        }
    }

    /// Serves `request` on `reply_to`, the stream it arrived on (never on whatever stream is
    /// current: a reply on another stream, after a reconnect or to another hub, would be ignored
    /// by the hub). The request is registered before the returned future runs, so a `Flow` or
    /// `Cancel` for its id that follows it at once still finds it. A unary request gets its
    /// `Reply`; a streaming one (`mode = STREAMING`) its parts under the credits and its end.
    /// One permit goes back to the hub at the end, whatever the end. A subscription request
    /// (`mode = SUBSCRIPTION`) registers a subscription instead, also before the returned
    /// future runs, pending its admission: it is passive (no running handler, no permit here or
    /// at the hub) and gets a `ReplyEnd{error}` only when refused.
    pub fn serve(
        self: &Arc<Self>,
        request: proto::Request,
        reply_to: ReplyTo,
    ) -> impl Future<Output = ()> + Send + 'static {
        let streaming = request.mode == RequestMode::Streaming as i32;
        let subscription = request.mode == RequestMode::Subscription as i32;
        let accepted =
            (!subscription).then(|| self.accept(&request.id, reply_to.stream(), streaming));
        // Registered here, before the spawn, like a running request: a hub `Cancel` or a lost
        // stream that follows the request finds it, whatever the admission is doing.
        let prepared = subscription.then(|| self.register_subscription(&request, &reply_to));
        let me = Arc::clone(self);
        async move {
            let request_id = request.id.clone();
            if let Some(prepared) = prepared {
                me.subscribe(request, &reply_to, prepared).await;
                return;
            }
            let Some((running, credits)) = accepted else {
                return;
            };
            if streaming {
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
    fn accept(
        self: &Arc<Self>,
        request_id: &str,
        stream: StreamId,
        streaming: bool,
    ) -> (Running, mpsc::UnboundedReceiver<u32>) {
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::unbounded_channel();
        self.running.lock().unwrap().insert(
            request_id.to_string(),
            Control {
                cancel: cancel.clone(),
                credits: tx,
                stream,
                streaming,
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
            RequestKind::Command => self.registry.command_handler(&req.r#type).cloned(),
            _ => match self.registry.query_handler(&req.r#type) {
                Some(handler) => Some(handler.clone()),
                // The caller used the wrong mode: not retryable, unlike NO_HANDLER.
                None if self.registry.stream_handler(&req.r#type).is_some() => {
                    return Err(MessagingError::InvalidMessage(format!(
                        "{} is a streaming query: send it with Sender::stream (mode STREAMING)",
                        req.r#type
                    )));
                }
                // A subscription's initial result is the unary request `<id>#0`: the
                // subscription handler answers it; any other unary request used the wrong mode.
                //
                // Recognised by the id's `#0` suffix alone, which collides with scatter-gather:
                // its targets are numbered from `#0` too, so a gather of a subscription type
                // sent by a raw client would have its target `<id>#0` answered with the initial
                // result. Only raw clients can hit it: the typed APIs never gather a
                // subscription type.
                None => match self.registry.subscription_handler(&req.r#type) {
                    Some(handler) if req.id.ends_with("#0") => Some(handler.initial.clone()),
                    Some(_) => {
                        return Err(MessagingError::InvalidMessage(format!(
                            "{} is a subscription query: send it with Sender::subscribe (mode SUBSCRIPTION)",
                            req.r#type
                        )));
                    }
                    None => None,
                },
            },
        }
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
        let (ctx, payload) = self.context(req, identity)?;
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MessagingError::HubUnavailable("dispatcher closed".into()))?;
        Ok((ctx, permit, payload))
    }

    /// The request's context and its guard's verdict (before decoding), without a permit.
    fn context(
        &self,
        req: proto::Request,
        identity: Option<Identity>,
    ) -> Result<(MessageContext, Vec<u8>), MessagingError> {
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
        Ok((ctx, req.payload))
    }

    /// Registers a subscription request (`<id>#n`, `mode = SUBSCRIPTION`) as pending, as far
    /// as it can be before its identity is known: its handler is looked up and its query decoded
    /// (the guard decides later whether either matters). What stops it is kept to be reported by
    /// [`subscribe`](Self::subscribe) in the order its admission always reported it.
    fn register_subscription(&self, req: &proto::Request, reply_to: &ReplyTo) -> Prepared {
        let flawed = |flaw| Prepared {
            admission: None,
            flaw,
        };
        let handler = match self.subscription_handler(req) {
            Ok(handler) => handler,
            Err(e) => return flawed(Flaw::Lookup(e)),
        };
        let query = match (handler.decode)(&req.payload) {
            Ok(query) => query,
            Err(e) => return flawed(Flaw::Decode(invocation_error(&req.r#type, e))),
        };
        match self.subscriptions.register(
            &req.id,
            &req.r#type,
            query,
            req.payload.clone(),
            reply_to.clone(),
        ) {
            Ok(admission) => Prepared {
                admission: Some(admission),
                flaw: Flaw::None,
            },
            Err(max) => flawed(Flaw::Limit(max)),
        }
    }

    /// The handler of a subscription request, the type's query handler whatever the request's
    /// kind: `INVALID_REQUEST` when the type's handler answers another mode (a unary or streaming
    /// handler: the sender used the wrong call, which no retry fixes), and `INVALID_REQUEST` too
    /// for a type with no subscription handler here (a command, or not served at all).
    fn subscription_handler(
        &self,
        req: &proto::Request,
    ) -> Result<SubscriptionHandler, MessagingError> {
        let type_name = &req.r#type;
        match self.registry.subscription_handler(type_name) {
            Some(handler) => Ok(handler.clone()),
            None if self.registry.query_handler(type_name).is_some() => {
                Err(MessagingError::InvalidMessage(format!(
                    "{type_name} is not a subscription query handler (it answers mode UNARY)"
                )))
            }
            None if self.registry.stream_handler(type_name).is_some() => {
                Err(MessagingError::InvalidMessage(format!(
                    "{type_name} is not a subscription query handler (it answers mode STREAMING)"
                )))
            }
            None => Err(MessagingError::InvalidMessage(format!(
                "no subscription handler for {type_name} on this instance"
            ))),
        }
    }

    /// The admission of a subscription request (`<id>#n`, `mode = SUBSCRIPTION`) that
    /// [`register_subscription`](Self::register_subscription) read: admitted, so what emits
    /// matched meanwhile is delivered, or refused with a `ReplyEnd{error}`: `INVALID_REQUEST`
    /// when the type's handler answers another mode or the type has no subscription handler here,
    /// `RESOURCE_EXHAUSTED` `subscription limit (N) reached` beyond `max_subscriptions`, the
    /// guard's code, or `INVALID_REQUEST` for a query that does not decode. A hub `Cancel` or a
    /// lost stream meanwhile removed it: the outcome is dropped and nothing is sent.
    async fn subscribe(&self, req: proto::Request, reply_to: &ReplyTo, prepared: Prepared) {
        let request_id = req.id.clone();
        let Prepared { admission, flaw } = prepared;
        let error = match self.vet_subscription(req, flaw).await {
            Ok(()) => {
                if let Some(admission) = admission {
                    admission.admit();
                    tracing::debug!(request = %request_id, stream = reply_to.stream(), "subscription registered");
                }
                return;
            }
            Err(Rejected::Limit(max)) => proto::Error {
                request_id: request_id.clone(),
                code: ErrorCode::ResourceExhausted as i32,
                message: format!("subscription limit ({max}) reached"),
                details: vec![],
            },
            Err(Rejected::Error(e)) => hub_error(&request_id, &e),
        };
        tracing::debug!(request = %request_id, code = error.code, "subscription refused: {}", error.message);
        match admission {
            // Registered: its pump writes the refusal (unless the hub cancelled it).
            Some(admission) => admission.refuse(error),
            None => send_end(reply_to, &request_id, Some(error)).await,
        }
    }

    /// Identity, then what `register_subscription` found wrong with the request, then the guard
    /// (before the decode's verdict: anonymous callers cannot probe payload shapes).
    async fn vet_subscription(&self, req: proto::Request, flaw: Flaw) -> Result<(), Rejected> {
        if let Some(hook) = &self.admission_hook {
            hook(&req.id).await;
        }
        let identity = self.identity(&req).await?;
        if let Flaw::Lookup(e) = flaw {
            return Err(e.into());
        }
        let (_ctx, _payload) = self.context(req, identity)?;
        match flaw {
            Flaw::Decode(e) => Err(e.into()),
            Flaw::Limit(max) => Err(Rejected::Limit(max)),
            Flaw::Lookup(_) | Flaw::None => Ok(()),
        }
    }

    /// A streaming request: its parts under the credits (`initial_credit` plus every `Flow`;
    /// the handler's stream is pulled at most one item ahead of the credit), then its end. A hub
    /// `Cancel` aborts the pump (the handler's stream is dropped, nothing more is sent); no
    /// credit within [`stream_credit_timeout`](Self::stream_credit_timeout) ends it with
    /// `TIMEOUT`. The handler permit is held until the end, whatever the end.
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
        let credit_timeout = self.stream_credit_timeout;
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
                // The stream is polled at most one item ahead of the credit: the item is held
                // until credit arrives, so its end (or failure) is found without spare credit.
                let payload = match items.next().await {
                    None => {
                        send_end(&reply_to, &request_id, None).await;
                        return;
                    }
                    Some(Err(invocation)) => {
                        let e = invocation_error(&type_name, invocation);
                        tracing::debug!(request = %request_id, "stream failed: {e}");
                        send_end(&reply_to, &request_id, Some(hub_error(&request_id, &e))).await;
                        return;
                    }
                    Some(Ok(payload)) => payload,
                };
                // Bounded: a hub that forgot the stream never sends credit, and the permits
                // (and the held item) must not be kept for ever. The error is the hub's own
                // idle code, so the sender (if any still listens) sees a `Timeout`.
                while credit == 0 {
                    match tokio::time::timeout(credit_timeout, credits.recv()).await {
                        Ok(Some(more)) => credit = credit.saturating_add(more),
                        Ok(None) => return,
                        Err(_) => {
                            tracing::debug!(request = %request_id, "stream ended: no credit within {credit_timeout:?}");
                            let error = proto::Error {
                                request_id: request_id.clone(),
                                code: ErrorCode::Timeout as i32,
                                message: format!("no credit within {credit_timeout:?}"),
                                details: vec![],
                            };
                            send_end(&reply_to, &request_id, Some(error)).await;
                            return;
                        }
                    }
                }
                while let Ok(more) = credits.try_recv() {
                    credit = credit.saturating_add(more);
                }
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
        };
        CURRENT.scope(ctx, pumping).await;
    }

    /// Admits a streaming request and runs its handler to the stream of items. The handler is the
    /// type's query handler whatever the request's kind: `INVALID_REQUEST` when it answers another
    /// mode (unary or subscription), `NO_HANDLER` when the type has no query handler here.
    async fn open(
        &self,
        req: proto::Request,
    ) -> Result<(OwnedSemaphorePermit, MessageContext, ItemStream), MessagingError> {
        let identity = self.identity(&req).await?;
        let type_name = &req.r#type;
        let handler = match self.registry.stream_handler(type_name) {
            Some(handler) => handler.clone(),
            // The caller used the wrong mode: not retryable, unlike NO_HANDLER.
            None if self.registry.query_handler(type_name).is_some() => {
                return Err(MessagingError::InvalidMessage(format!(
                    "{type_name} is not a streaming query handler (it answers mode UNARY)"
                )));
            }
            None if self.registry.subscription_handler(type_name).is_some() => {
                return Err(MessagingError::InvalidMessage(format!(
                    "{type_name} is not a streaming query handler (it answers mode SUBSCRIPTION)"
                )));
            }
            None => return Err(MessagingError::NoHandler(type_name.clone())),
        };
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
pub(crate) fn invocation_error(type_name: &str, invocation: Invocation) -> MessagingError {
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
        // An adapter's failure kind travels as a handler error with its marker.
        Invocation::Handler(
            e @ (MessagingError::RetryableExternal(_) | MessagingError::UnknownOutcome(_)),
        ) => MessagingError::RemoteHandler {
            type_name: type_name.to_string(),
            message: e.to_string(),
            details: e.reply_details(),
        },
        Invocation::Handler(e) => handler_error(e.to_string()),
    }
}

/// The hub `Error` a handler's failure becomes (inside a `Reply` or a `ReplyEnd`).
fn hub_error(request_id: &str, e: &MessagingError) -> proto::Error {
    let (code, details) = (e.to_reply_code(), e.reply_details());
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
