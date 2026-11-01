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
use crate::identity::Guard;
use crate::types::{Command, Event, MessageType, Query, StreamingQuery, SubscriptionQuery};

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
/// Why an erased request handler did not produce a payload: the dispatcher maps each case to its
/// own reply code (decode → INVALID_REQUEST; the handler's error → HANDLER_ERROR unless it is an
/// authorization outcome).
pub(crate) enum Invocation {
    Decode(String),
    Handler(MessagingError),
    Encode(String),
}

pub(crate) type RequestHandler =
    Arc<dyn Fn(Vec<u8>, MessageContext) -> BoxFut<Result<Vec<u8>, Invocation>> + Send + Sync>;
/// The items of an erased streaming handler, each encoded (or why it could not be).
pub(crate) type ItemStream = BoxStream<'static, Result<Vec<u8>, Invocation>>;
pub(crate) type StreamHandler =
    Arc<dyn Fn(Vec<u8>, MessageContext) -> BoxFut<Result<ItemStream, Invocation>> + Send + Sync>;
pub(crate) type EventHandler =
    Arc<dyn Fn(Vec<u8>, EventContext) -> BoxFut<Result<(), MessagingError>> + Send + Sync>;
/// Decodes a subscription query's payload to the registered `Q`, erased for the emitter's
/// filters to downcast.
pub(crate) type QueryDecoder =
    Arc<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send + Sync>, Invocation> + Send + Sync>;

/// A subscription query's handler (ADR 0017, P4c): the initial result, answered like a unary
/// query, and the decoder of the query a `Request{mode = SUBSCRIPTION}` registers.
#[derive(Clone)]
pub(crate) struct SubscriptionHandler {
    pub initial: RequestHandler,
    pub decode: QueryDecoder,
}

/// What an event handler receives: the message metadata plus the Mongo session of the transaction
/// the consumer opened, so the handler's own writes commit together with the dedupe marker.
#[derive(Clone)]
pub struct EventContext {
    pub message: MessageContext,
    pub session: Arc<Mutex<mongodb::ClientSession>>,
}

#[derive(Default)]
pub struct HandlerRegistry {
    commands: HashMap<&'static str, RequestHandler>,
    queries: HashMap<&'static str, RequestHandler>,
    /// Streaming queries (ADR 0017, P4b), served only for `mode = STREAMING`.
    streams: HashMap<&'static str, StreamHandler>,
    /// Subscription queries (ADR 0017, P4c): the initial result for `<id>#0` (`mode = UNARY`),
    /// the subscription for `mode = SUBSCRIPTION`.
    subscriptions: HashMap<&'static str, SubscriptionHandler>,
    /// The query types registered as shared (their handlers are in `queries`, `streams` or
    /// `subscriptions`).
    shared: HashSet<&'static str>,
    events: HashMap<&'static str, Vec<EventHandler>>,
    guards: HashMap<&'static str, Guard>,
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

    /// A shared query (`Register.shared_query_types`, ADR 0017): several services may own it. A
    /// point query goes to one instance of any owner; a scatter-gather to every instance of every
    /// owner. Each instance of each owner serves it like any query (a scatter-gather target gets
    /// an ordinary request with id `<id>#<n>`).
    ///
    /// Every registrant of the type, in every service, must register it shared: a hub rejects a
    /// `Register` that disagrees with an earlier one (`ALREADY_EXISTS`, the connection keeps
    /// retrying). Shared types need a hub that lists `scatter-gather` in its ack; an older hub
    /// ignores the field, so the type is not registered there.
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

    /// An exclusive streaming query (ADR 0017, P4b): the handler returns a stream of items the
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
    /// a stream goes to one capable instance of any owner. See
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

    /// An exclusive subscription query (ADR 0017, P4c): `handler` answers the initial result
    /// (the hub asks one capable instance with `Request{id = <id>#0, mode = UNARY}`, served like
    /// any unary query, permit included); every capable instance listing the type also gets
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
    /// it; a subscription reaches every capable instance of every owner. See
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
        let handler = Arc::new(handler);
        let erased: EventHandler = Arc::new(move |payload: Vec<u8>, ctx: EventContext| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let event: E = codec::decode(&payload)?;
                handler(event, ctx).await
            })
        });
        self.events.entry(E::TYPE).or_default().push(erased);
    }

    /// Opt-in authorization for a registered command or query (the `@RequiresAuthentication` of Rust).
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

    /// Every query type served here, unary, streaming and subscription, exclusive and shared
    /// (as kraken-messaging's `queryTypes()`).
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

    pub(crate) fn event_handlers(&self, type_name: &str) -> &[EventHandler] {
        self.events.get(type_name).map(Vec::as_slice).unwrap_or(&[])
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

fn erase_request<M, R, F, Fut>(handler: F) -> RequestHandler
where
    M: serde::de::DeserializeOwned + Send + 'static,
    R: serde::Serialize + Send + 'static,
    F: Fn(M, MessageContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, MessagingError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    Arc::new(move |payload: Vec<u8>, ctx: MessageContext| {
        let handler = Arc::clone(&handler);
        Box::pin(async move {
            let message: M = codec::decode(&payload)
                .map_err(|e| Invocation::Decode(format!("cannot decode payload: {e}")))?;
            let reply = handler(message, ctx).await.map_err(Invocation::Handler)?;
            codec::encode_reply(&reply)
                .map_err(|e| Invocation::Encode(format!("cannot encode reply: {e}")))
        })
    })
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
    Arc::new(move |payload: Vec<u8>, ctx: MessageContext| {
        let handler = Arc::clone(&handler);
        Box::pin(async move {
            let query: Q = codec::decode(&payload)
                .map_err(|e| Invocation::Decode(format!("cannot decode payload: {e}")))?;
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
    })
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
