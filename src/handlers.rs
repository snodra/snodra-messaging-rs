//! Message traits and the handler registry builder.
//!
//! ```ignore
//! let registry = HandlerRegistry::new("files-service")
//!     .query::<GetFileMetadata, _>(|q, ctx| async move { Ok(lookup(q).await) })
//!     .command::<DeleteFile, _>(|c, ctx| async move { delete(c).await })
//!     .event::<UserDeleted, _>(|e, ctx| async move { purge(e).await });
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::envelope::Envelope;
use crate::error::MessagingError;

/// Every message class declares its stable wire type name.
pub trait MessageType {
    const TYPE_NAME: &'static str;
}

/// A domain event published on `kraken.events`.
pub trait Event: MessageType + Serialize + DeserializeOwned + Send + Sync + 'static {}

/// A command routed through the hub to exactly one handler.
pub trait Command: MessageType + Serialize + DeserializeOwned + Send + Sync + 'static {
    type Response: Serialize + DeserializeOwned + Send + 'static;
}

/// A query routed through the hub to one handler.
pub trait Query: MessageType + Serialize + DeserializeOwned + Send + Sync + 'static {
    type Response: Serialize + DeserializeOwned + Send + 'static;
}

/// What a handler learns about the message besides its payload.
#[derive(Debug, Clone, Default)]
pub struct HandlerContext {
    pub correlation_id: String,
    /// Raw `Bearer <JWT>` forwarded over the hub, if any. Validation is the handler's job
    /// (see kraken/docs/06-security.md).
    pub authorization: Option<String>,
    pub user_id: Option<String>,
    pub traceparent: Option<String>,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Type-erased handler: raw JSON in, raw JSON (or error) out.
pub type ErasedHandler = Arc<
    dyn Fn(serde_json::Value, HandlerContext) -> BoxFuture<Result<serde_json::Value, MessagingError>>
        + Send
        + Sync,
>;

/// Type-erased event handler: envelope in, unit out.
pub type ErasedEventHandler =
    Arc<dyn Fn(Envelope, HandlerContext) -> BoxFuture<Result<(), MessagingError>> + Send + Sync>;

/// Collects handlers by wire type name. Consumed by `HubClient` (commands, queries) and
/// `EventConsumer` (events).
#[derive(Clone)]
pub struct HandlerRegistry {
    pub service_name: String,
    pub commands: HashMap<&'static str, ErasedHandler>,
    pub queries: HashMap<&'static str, ErasedHandler>,
    pub events: HashMap<&'static str, ErasedEventHandler>,
}

impl HandlerRegistry {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            commands: HashMap::new(),
            queries: HashMap::new(),
            events: HashMap::new(),
        }
    }

    pub fn command<C, F, Fut>(mut self, handler: F) -> Self
    where
        C: Command,
        F: Fn(C, HandlerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<C::Response, MessagingError>> + Send + 'static,
    {
        self.commands.insert(C::TYPE_NAME, erase::<C, C::Response, F, Fut>(handler));
        self
    }

    pub fn query<Q, F, Fut>(mut self, handler: F) -> Self
    where
        Q: Query,
        F: Fn(Q, HandlerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Q::Response, MessagingError>> + Send + 'static,
    {
        self.queries.insert(Q::TYPE_NAME, erase::<Q, Q::Response, F, Fut>(handler));
        self
    }

    pub fn event<E, F, Fut>(mut self, handler: F) -> Self
    where
        E: Event,
        F: Fn(E, HandlerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let erased: ErasedEventHandler = Arc::new(move |env: Envelope, ctx: HandlerContext| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let event: E = serde_json::from_value(env.payload)?;
                handler(event, ctx).await
            })
        });
        self.events.insert(E::TYPE_NAME, erased);
        self
    }

    pub fn command_types(&self) -> Vec<String> {
        self.commands.keys().map(|k| k.to_string()).collect()
    }

    pub fn query_types(&self) -> Vec<String> {
        self.queries.keys().map(|k| k.to_string()).collect()
    }

    pub fn event_types(&self) -> Vec<String> {
        self.events.keys().map(|k| k.to_string()).collect()
    }
}

fn erase<M, R, F, Fut>(handler: F) -> ErasedHandler
where
    M: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(M, HandlerContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, MessagingError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    Arc::new(move |payload: serde_json::Value, ctx: HandlerContext| {
        let handler = Arc::clone(&handler);
        Box::pin(async move {
            let message: M = serde_json::from_value(payload)?;
            let response = handler(message, ctx).await?;
            Ok(serde_json::to_value(response)?)
        })
    })
}
