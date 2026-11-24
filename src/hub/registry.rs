//! Typed handler registration, erased to byte-level closures the dispatcher and consumer call.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::{BoxStream, Stream, StreamExt};
use tokio::sync::Mutex;

use crate::codec;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::external::{ExternalCallsHandle, IdempotencyKey};
use crate::identity::Guard;
use crate::types::{Command, Event, MessageType, Query, StreamingQuery, SubscriptionQuery};

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
/// Why an erased request handler did not produce a payload: the dispatcher maps each case to its
/// own reply code (decode → INVALID_REQUEST; the handler's error → HANDLER_ERROR unless it is an
/// authorization outcome, the guard's included).
pub(crate) enum Invocation {
    Decode(String),
    Handler(MessagingError),
    Encode(String),
}

/// An erased request handler: decodes the payload, applies the type's guard (if any) to the
/// decoded request, runs the handler and encodes its reply.
pub(crate) type RequestHandler = Arc<
    dyn Fn(Vec<u8>, MessageContext, Option<Guard>) -> BoxFut<Result<Vec<u8>, Invocation>>
        + Send
        + Sync,
>;
/// The items of an erased streaming handler, each encoded (or why it could not be).
pub(crate) type ItemStream = BoxStream<'static, Result<Vec<u8>, Invocation>>;
pub(crate) type StreamHandler = Arc<
    dyn Fn(Vec<u8>, MessageContext, Option<Guard>) -> BoxFut<Result<ItemStream, Invocation>>
        + Send
        + Sync,
>;
pub(crate) type EventHandler =
    Arc<dyn Fn(Vec<u8>, EventContext) -> BoxFut<Result<(), MessagingError>> + Send + Sync>;
/// Decodes a subscription query's payload to the registered `Q`, erased for the emitter's
/// filters to downcast.
pub(crate) type QueryDecoder =
    Arc<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send + Sync>, Invocation> + Send + Sync>;

/// A subscription query's handler: the initial result, answered like a unary
/// query, and the decoder of the query a `Request{mode = SUBSCRIPTION}` registers.
#[derive(Clone)]
pub(crate) struct SubscriptionHandler {
    pub initial: RequestHandler,
    pub decode: QueryDecoder,
}

/// What an event handler receives: the message metadata plus the transaction of the message
/// store the consumer opened, so the handler's own writes commit together with the dedupe marker.
/// The store add-on gives the handler its own handle on it (the MongoDB add-on:
/// `ctx.tx.lock().await.session()` with `MongoTxExt`), and [`EventBus::publish_in`] appends to the
/// outbox in it.
///
/// [`EventBus::publish_in`]: crate::events::EventBus::publish_in
#[derive(Clone)]
pub struct EventContext {
    pub message: MessageContext,
    pub tx: Arc<Mutex<crate::store::Tx>>,
}

#[derive(Default)]
pub struct HandlerRegistry {
    commands: HashMap<&'static str, RequestHandler>,
    queries: HashMap<&'static str, RequestHandler>,
    /// Streaming queries, served only for `mode = STREAMING`.
    streams: HashMap<&'static str, StreamHandler>,
    /// Subscription queries: the initial result for `<id>#0` (`mode = UNARY`),
    /// the subscription for `mode = SUBSCRIPTION`.
    subscriptions: HashMap<&'static str, SubscriptionHandler>,
    /// The query types registered as shared (their handlers are in `queries`, `streams` or
    /// `subscriptions`).
    shared: HashSet<&'static str>,
    /// Each with whether it runs in a replay (false for `disallow_replay`).
    events: HashMap<&'static str, Vec<(EventHandler, bool)>>,
    guards: HashMap<&'static str, Guard>,
    /// The labels (wire type names) of the idempotent handlers, sorted on read.
    idempotent: Vec<&'static str>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn command<C, F, Fut>(&mut self, handler: F)
    where
        C: Command,
        F: Fn(C, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<C::Reply, MessagingError>> + Send + 'static,
    {
        self.assert_unowned(C::TYPE, None);
        self.commands
            .insert(C::TYPE, erase_request::<C, C::Reply, F, Fut>(handler));
    }

    /// An exclusive query: this service is its only owner (`Register.query_types`).
    pub fn query<Q, F, Fut>(&mut self, handler: F)
    where
        Q: Query,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Q::Reply, MessagingError>> + Send + 'static,
    {
        self.assert_unowned(Q::TYPE, Some(false));
        self.queries
            .insert(Q::TYPE, erase_request::<Q, Q::Reply, F, Fut>(handler));
    }

    /// A shared query (`Register.shared_query_types`): several services may own it. A
    /// point query goes to one instance of any owner; a scatter-gather to every instance of every
    /// owner. Each instance of each owner serves it like any query (a scatter-gather target gets
    /// an ordinary request with id `<id>#<n>`).
    ///
    /// Every registrant of the type, in every service, must register it shared: a hub rejects a
    /// `Register` that disagrees with an earlier one (`ALREADY_EXISTS`, the connection keeps
    /// retrying).
    ///
    /// # Panics
    /// When the type already has a handler here, shared or exclusive.
    pub fn shared_query<Q, F, Fut>(&mut self, handler: F)
    where
        Q: Query,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Q::Reply, MessagingError>> + Send + 'static,
    {
        self.assert_unowned(Q::TYPE, Some(true));
        self.queries
            .insert(Q::TYPE, erase_request::<Q, Q::Reply, F, Fut>(handler));
        self.shared.insert(Q::TYPE);
    }

    /// An exclusive streaming query: the handler returns a stream of items the
    /// library sends one `ReplyPart` at a time under the sender's credits (the stream is polled
    /// at most one item ahead of the credit; its end is sent at once, with or without credit),
    /// then a `ReplyEnd`. An `Err` item ends the stream with
    /// that error (`ReplyEnd{error}`, the stream dropped); a hub `Cancel` drops it silently. The
    /// handler permit is held for the whole stream. The type is announced in
    /// `Register.query_types`; only a streaming request (`Sender::stream`) reaches the handler, a
    /// unary query to the type is answered `INVALID_REQUEST` here (the wrong mode is the
    /// caller's mistake, not retryable).
    ///
    /// # Panics
    /// When the type already has a handler here.
    pub fn streaming_query<Q, F, Fut, S>(&mut self, handler: F)
    where
        Q: StreamingQuery,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, MessagingError>> + Send + 'static,
        S: Stream<Item = Result<Q::Item, MessagingError>> + Send + 'static,
    {
        self.assert_unowned(Q::TYPE, Some(false));
        self.streams
            .insert(Q::TYPE, erase_stream::<Q, F, Fut, S>(handler));
    }

    /// A shared streaming query (`Register.shared_query_types`): several services may own it;
    /// a stream goes to one instance of any owner. See
    /// [`streaming_query`](Self::streaming_query) and [`shared_query`](Self::shared_query).
    ///
    /// # Panics
    /// When the type already has a handler here, shared or exclusive.
    pub fn shared_streaming_query<Q, F, Fut, S>(&mut self, handler: F)
    where
        Q: StreamingQuery,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, MessagingError>> + Send + 'static,
        S: Stream<Item = Result<Q::Item, MessagingError>> + Send + 'static,
    {
        self.assert_unowned(Q::TYPE, Some(true));
        self.streams
            .insert(Q::TYPE, erase_stream::<Q, F, Fut, S>(handler));
        self.shared.insert(Q::TYPE);
    }

    /// An exclusive subscription query: `handler` answers the initial result
    /// (the hub asks one instance with `Request{id = <id>#0, mode = UNARY}`, served like
    /// any unary query, permit included); every instance listing the type also gets
    /// `Request{id = <id>#n, mode = SUBSCRIPTION}`, which registers a subscription here (the
    /// decoded query, the request id, the arrival stream; no permit) that the
    /// [`QueryUpdateEmitter`](super::QueryUpdateEmitter) feeds: one `ReplyPart` per `emit`, a
    /// `ReplyEnd` on `complete` / `complete_exceptionally`. A hub `Cancel` (the subscriber
    /// cancelled or left, the hub's limits) removes it. The type is announced in
    /// `Register.query_types`; a unary query to it (an id without the `#0` suffix) is answered
    /// `INVALID_REQUEST` (the wrong mode is the caller's mistake, not retryable).
    ///
    /// # Panics
    /// When the type already has a handler here.
    pub fn subscription_query<Q, F, Fut>(&mut self, handler: F)
    where
        Q: SubscriptionQuery,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Q::Initial, MessagingError>> + Send + 'static,
    {
        self.assert_unowned(Q::TYPE, Some(false));
        self.subscriptions
            .insert(Q::TYPE, erase_subscription::<Q, F, Fut>(handler));
    }

    /// A shared subscription query (`Register.shared_query_types`): several services may own
    /// it; a subscription reaches every instance of every owner. See
    /// [`subscription_query`](Self::subscription_query) and [`shared_query`](Self::shared_query).
    ///
    /// # Panics
    /// When the type already has a handler here, shared or exclusive.
    pub fn shared_subscription_query<Q, F, Fut>(&mut self, handler: F)
    where
        Q: SubscriptionQuery,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Q::Initial, MessagingError>> + Send + 'static,
    {
        self.assert_unowned(Q::TYPE, Some(true));
        self.subscriptions
            .insert(Q::TYPE, erase_subscription::<Q, F, Fut>(handler));
        self.shared.insert(Q::TYPE);
    }

    /// Several handlers may react to the same event type.
    pub fn event<E, F, Fut>(&mut self, handler: F)
    where
        E: Event,
        F: Fn(E, EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        self.event_replayed::<E, F, Fut>(true, handler);
    }

    /// [`event`](Self::event) for a handler that does not run again when its processor replays a
    /// range after a reset: a handler with effects outside
    /// the service (an e-mail, a payment) that must happen once per record.
    pub fn event_disallowing_replay<E, F, Fut>(&mut self, handler: F)
    where
        E: Event,
        F: Fn(E, EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        self.event_replayed::<E, F, Fut>(false, handler);
    }

    fn event_replayed<E, F, Fut>(&mut self, replayed: bool, handler: F)
    where
        E: Event,
        F: Fn(E, EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let erased: EventHandler = Arc::new(move |payload: Vec<u8>, ctx: EventContext| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                // A payload that does not fit the event's type cannot get better by retrying:
                // the consumer parks it after one attempt.
                let event: E = codec::decode(&payload).map_err(|e| {
                    MessagingError::InvalidMessage(format!("cannot read {}: {e}", E::TYPE))
                })?;
                handler(event, ctx).await
            })
        });
        self.events
            .entry(E::TYPE)
            .or_default()
            .push((erased, replayed));
    }

    /// An idempotent command handler: an adapter to an external system. Each call is recorded in `external_calls`
    /// under `key` (by default the `idempotency-key` header the saga relay sets), so a repeat of
    /// the key never runs `handler` twice: a success replays the stored reply, a failure returns
    /// the stored error, a [`MessagingError::RetryableExternal`] lets the next call run again, and
    /// a call that crashed mid-way is an unknown outcome
    /// ([`MessagingError::UnknownOutcome`]) unless `external_idempotent` says the partner
    /// de-duplicates by the key (then it is called again after the lease). See
    /// [`crate::external::calls`]. `calls` is bound by the builder's `start()`; the handler is
    /// labelled with `C::TYPE` in errors and metrics.
    ///
    /// # Panics
    /// When `C` already has a handler here.
    pub fn idempotent_command<C, F, Fut>(
        &mut self,
        calls: &ExternalCallsHandle,
        key: IdempotencyKey<C>,
        external_idempotent: bool,
        handler: F,
    ) where
        C: Command,
        F: Fn(C, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<C::Reply, MessagingError>> + Send + 'static,
    {
        let calls = calls.clone();
        let handler = Arc::new(handler);
        self.command::<C, _, _>(move |command, ctx| {
            let calls = calls.clone();
            let handler = Arc::clone(&handler);
            async move {
                let label = C::TYPE;
                let calls = calls.get(label)?;
                let key = key.resolve(&command, &ctx, label)?;
                calls
                    .execute(label, &key, external_idempotent, handler(command, ctx))
                    .await
            }
        });
        self.idempotent.push(C::TYPE);
    }

    /// An idempotent event handler (an `@IdempotentHandler @EventHandler`): as
    /// [`idempotent_command`](Self::idempotent_command), with the event's reply always `()`. Its
    /// body runs inside stage 1's transaction (a slow external call there holds the
    /// transaction open, prefer a command handler for those); the call record is written apart, so
    /// a rolled-back stage 1 does not forget a call that happened and the redelivery replays it.
    pub fn idempotent_event<E, F, Fut>(
        &mut self,
        calls: &ExternalCallsHandle,
        key: IdempotencyKey<E>,
        external_idempotent: bool,
        handler: F,
    ) where
        E: Event,
        F: Fn(E, EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        let calls = calls.clone();
        let handler = Arc::new(handler);
        self.event::<E, _, _>(move |event, ctx| {
            let calls = calls.clone();
            let handler = Arc::clone(&handler);
            async move {
                let label = E::TYPE;
                let calls = calls.get(label)?;
                let key = key.resolve(&event, &ctx.message, label)?;
                calls
                    .execute(label, &key, external_idempotent, handler(event, ctx))
                    .await
            }
        });
        self.idempotent.push(E::TYPE);
    }

    /// A copy of the command and query handlers registered so far, with their guards (no event
    /// handlers): what the builder's sender answers in-process before the start. The idempotent
    /// command handlers are left out: their call records are bound by the start, so before it
    /// their types go through the hub.
    pub(crate) fn requests(&self) -> HandlerRegistry {
        HandlerRegistry {
            commands: self
                .commands
                .iter()
                .filter(|(t, _)| !self.idempotent.contains(t))
                .map(|(t, h)| (*t, Arc::clone(h)))
                .collect(),
            queries: self.queries.clone(),
            streams: self.streams.clone(),
            subscriptions: self.subscriptions.clone(),
            shared: self.shared.clone(),
            events: HashMap::new(),
            guards: self.guards.clone(),
            idempotent: Vec::new(),
        }
    }

    /// The labels (wire type names) of the idempotent handlers registered here, sorted.
    pub fn idempotent_handlers(&self) -> Vec<String> {
        let mut labels: Vec<String> = self.idempotent.iter().map(|l| l.to_string()).collect();
        labels.sort();
        labels.dedup();
        labels
    }

    /// Opt-in authorization for a registered command or query.
    pub fn requires<M: MessageType>(&mut self, guard: Guard) {
        assert!(
            self.commands.contains_key(M::TYPE)
                || self.queries.contains_key(M::TYPE)
                || self.streams.contains_key(M::TYPE)
                || self.subscriptions.contains_key(M::TYPE),
            "requires::<{}>: register the command or query handler first",
            M::TYPE
        );
        self.guards.insert(M::TYPE, guard);
    }

    pub fn command_types(&self) -> Vec<String> {
        sorted(self.commands.keys())
    }

    /// Every query type served here, unary, streaming and subscription, exclusive and shared.
    pub fn query_types(&self) -> Vec<String> {
        sorted(
            self.queries
                .keys()
                .chain(self.streams.keys())
                .chain(self.subscriptions.keys()),
        )
    }

    /// The exclusive unary query types (`RegisteredTypes.queries`).
    pub fn exclusive_query_types(&self) -> Vec<String> {
        sorted(self.queries.keys().filter(|t| !self.shared.contains(*t)))
    }

    /// Every streaming query type served here, exclusive and shared.
    pub fn streaming_query_types(&self) -> Vec<String> {
        sorted(self.streams.keys())
    }

    /// Every subscription query type served here, exclusive and shared.
    pub fn subscription_query_types(&self) -> Vec<String> {
        sorted(self.subscriptions.keys())
    }

    /// The shared query types, unary, streaming and subscription (`Register.shared_query_types`).
    pub fn shared_query_types(&self) -> Vec<String> {
        sorted(self.shared.iter())
    }

    /// Everything the `Register` announces: the exclusive unary, the exclusive streaming, the
    /// exclusive subscription and the shared query types apart (see [`super::RegisteredTypes`]).
    pub fn registered_types(&self) -> super::RegisteredTypes {
        super::RegisteredTypes {
            commands: self.command_types(),
            queries: self.exclusive_query_types(),
            shared_queries: self.shared_query_types(),
            streaming_queries: sorted(self.streams.keys().filter(|t| !self.shared.contains(*t))),
            subscription_queries: sorted(
                self.subscriptions
                    .keys()
                    .filter(|t| !self.shared.contains(*t)),
            ),
            shared_subscription_queries: sorted(
                self.subscriptions
                    .keys()
                    .filter(|t| self.shared.contains(*t)),
            ),
        }
    }

    pub fn event_types(&self) -> Vec<String> {
        sorted(self.events.keys())
    }

    pub fn has_event_handler(&self, type_name: &str) -> bool {
        self.events.contains_key(type_name)
    }

    pub(crate) fn command_handler(&self, type_name: &str) -> Option<&RequestHandler> {
        self.commands.get(type_name)
    }

    pub(crate) fn query_handler(&self, type_name: &str) -> Option<&RequestHandler> {
        self.queries.get(type_name)
    }

    pub(crate) fn stream_handler(&self, type_name: &str) -> Option<&StreamHandler> {
        self.streams.get(type_name)
    }

    pub(crate) fn subscription_handler(&self, type_name: &str) -> Option<&SubscriptionHandler> {
        self.subscriptions.get(type_name)
    }

    /// The handlers of `type_name` in registration order; in a `replay` without those that
    /// disallow it.
    pub(crate) fn event_handlers_for(&self, type_name: &str, replay: bool) -> Vec<EventHandler> {
        self.events
            .get(type_name)
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .filter(|(_, replayed)| !replay || *replayed)
            .map(|(h, _)| Arc::clone(h))
            .collect()
    }

    /// Whether a unary request of `kind` for `type_name` sent by this instance is answered by its
    /// own handler in-process ([`super::Sender`]'s local dispatch): a command it handles, or a
    /// query with an exclusive handler here (unary, streaming or subscription: the handler answers
    /// a unary request in the wrong mode as it would through the hub). A shared query is not: the
    /// hub spreads it over every owner.
    pub(crate) fn answers_locally(&self, kind: crate::proto::RequestKind, type_name: &str) -> bool {
        match kind {
            crate::proto::RequestKind::Command => self.commands.contains_key(type_name),
            crate::proto::RequestKind::Query => {
                (self.queries.contains_key(type_name)
                    || self.streams.contains_key(type_name)
                    || self.subscriptions.contains_key(type_name))
                    && !self.shared.contains(type_name)
            }
            crate::proto::RequestKind::Unspecified => false,
        }
    }

    /// The types registered with a guard ([`requires`](Self::requires)), sorted.
    pub fn guarded_types(&self) -> Vec<String> {
        sorted(self.guards.keys())
    }

    pub(crate) fn guard(&self, type_name: &str) -> Option<&Guard> {
        self.guards.get(type_name)
    }

    /// `shared`: the flavour of the query being registered (`None` for a command).
    fn assert_unowned(&self, type_name: &str, shared: Option<bool>) {
        let query = self.queries.contains_key(type_name)
            || self.streams.contains_key(type_name)
            || self.subscriptions.contains_key(type_name);
        if let Some(shared) = shared {
            assert!(
                !query || self.shared.contains(type_name) == shared,
                "{type_name} registered both as an exclusive and as a shared query; every \
                 registrant must agree on one"
            );
        }
        assert!(
            !self.commands.contains_key(type_name) && !query,
            "duplicate handler for {type_name}"
        );
    }
}

fn sorted<'a>(keys: impl Iterator<Item = &'a &'static str>) -> Vec<String> {
    let mut v: Vec<String> = keys.map(|k| k.to_string()).collect();
    v.sort();
    v
}

/// The guard's rule, applied once the payload is decoded (an anonymous caller was refused before).
fn check_decoded(guard: Option<&Guard>, ctx: &MessageContext) -> Result<(), Invocation> {
    match guard {
        Some(guard) => guard
            .check(ctx.identity.as_ref(), ctx)
            .map_err(Invocation::Handler),
        None => Ok(()),
    }
}

fn erase_request<M, R, F, Fut>(handler: F) -> RequestHandler
where
    M: serde::de::DeserializeOwned + Send + 'static,
    R: serde::Serialize + Send + 'static,
    F: Fn(M, MessageContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, MessagingError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    Arc::new(
        move |payload: Vec<u8>, ctx: MessageContext, guard: Option<Guard>| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let message: M = codec::decode(&payload)
                    .map_err(|e| Invocation::Decode(format!("cannot decode payload: {e}")))?;
                check_decoded(guard.as_ref(), &ctx)?;
                let reply = handler(message, ctx).await.map_err(Invocation::Handler)?;
                codec::encode_reply(&reply)
                    .map_err(|e| Invocation::Encode(format!("cannot encode reply: {e}")))
            })
        },
    )
}

/// Erases a streaming handler: the query decoded, the handler's stream boxed with each item
/// encoded with the reply codec (an `Err` item, or one that does not encode, ends it).
fn erase_stream<Q, F, Fut, S>(handler: F) -> StreamHandler
where
    Q: StreamingQuery,
    F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<S, MessagingError>> + Send + 'static,
    S: Stream<Item = Result<Q::Item, MessagingError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    Arc::new(
        move |payload: Vec<u8>, ctx: MessageContext, guard: Option<Guard>| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let query: Q = codec::decode(&payload)
                    .map_err(|e| Invocation::Decode(format!("cannot decode payload: {e}")))?;
                check_decoded(guard.as_ref(), &ctx)?;
                let items = handler(query, ctx).await.map_err(Invocation::Handler)?;
                let items: ItemStream = items
                    .map(|item| match item {
                        Ok(item) => codec::encode_reply(&item)
                            .map_err(|e| Invocation::Encode(format!("cannot encode item: {e}"))),
                        Err(e) => Err(Invocation::Handler(e)),
                    })
                    .boxed();
                Ok(items)
            })
        },
    )
}

/// Erases a subscription handler: the initial result as a unary handler, plus the decoder of
/// the query a subscription registers (boxed for the emitter's typed filters).
fn erase_subscription<Q, F, Fut>(handler: F) -> SubscriptionHandler
where
    Q: SubscriptionQuery,
    F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Q::Initial, MessagingError>> + Send + 'static,
{
    SubscriptionHandler {
        initial: erase_request::<Q, Q::Initial, F, Fut>(handler),
        decode: Arc::new(|payload: &[u8]| {
            let query: Q = codec::decode(payload)
                .map_err(|e| Invocation::Decode(format!("cannot decode payload: {e}")))?;
            Ok(Box::new(query) as Box<dyn Any + Send + Sync>)
        }),
    }
}
