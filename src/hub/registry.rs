//! Typed handler registration, erased to byte-level closures the dispatcher and consumer call.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::codec;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::identity::Guard;
use crate::types::{Command, Event, MessageType, Query};

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
pub(crate) type EventHandler =
    Arc<dyn Fn(Vec<u8>, EventContext) -> BoxFut<Result<(), MessagingError>> + Send + Sync>;

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
    /// The query types registered as shared (their handlers are in `queries`).
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
            self.commands.contains_key(M::TYPE) || self.queries.contains_key(M::TYPE),
            "requires::<{}>: register the command or query handler first",
            M::TYPE
        );
        self.guards.insert(M::TYPE, guard);
    }

    pub fn command_types(&self) -> Vec<String> {
        sorted(self.commands.keys())
    }

    /// Every query type served here, exclusive and shared (as kraken-messaging's `queryTypes()`).
    pub fn query_types(&self) -> Vec<String> {
        sorted(self.queries.keys())
    }

    /// The exclusive query types (`Register.query_types`).
    pub fn exclusive_query_types(&self) -> Vec<String> {
        sorted(self.queries.keys().filter(|t| !self.shared.contains(*t)))
    }

    /// The shared query types (`Register.shared_query_types`).
    pub fn shared_query_types(&self) -> Vec<String> {
        sorted(self.shared.iter())
    }

    /// Everything the `Register` announces: the exclusive and the shared query types apart.
    pub fn registered_types(&self) -> super::RegisteredTypes {
        super::RegisteredTypes {
            commands: self.command_types(),
            queries: self.exclusive_query_types(),
            shared_queries: self.shared_query_types(),
            ..super::RegisteredTypes::default()
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

    pub(crate) fn event_handlers(&self, type_name: &str) -> &[EventHandler] {
        self.events.get(type_name).map(Vec::as_slice).unwrap_or(&[])
    }

    pub(crate) fn guard(&self, type_name: &str) -> Option<&Guard> {
        self.guards.get(type_name)
    }

    /// `shared`: the flavour of the query being registered (`None` for a command).
    fn assert_unowned(&self, type_name: &str, shared: Option<bool>) {
        if let Some(shared) = shared {
            assert!(
                !self.queries.contains_key(type_name) || self.shared.contains(type_name) == shared,
                "{type_name} registered both as an exclusive and as a shared query; every \
                 registrant must agree on one"
            );
        }
        assert!(
            !self.commands.contains_key(type_name) && !self.queries.contains_key(type_name),
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
