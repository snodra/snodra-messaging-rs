//! `KrakenMessaging`: everything a service needs, wired from a [`Config`].
//!
//! ```ignore
//! let builder = KrakenMessaging::builder(Config::from_env()?).await?;
//! let events = builder.events();                      // clone into handlers that publish
//! let app = builder
//!     .command::<Shout, _, _>(move |cmd, ctx| { let events = events.clone(); async move { ... } })
//!     .query::<Reverse, _, _>(|q, ctx| async move { ... })
//!     .event::<GreetingDelivered, _, _>(|e, ctx| async move { ... })
//!     .start()
//!     .await?;
//! app.await_hub(Duration::from_secs(30)).await;
//! ```

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::Config;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::hub::{
    Dispatcher, EventContext, HandlerRegistry, HubConnection, HubSettings, Inbound, Sender,
    SenderSettings,
};
use crate::identity::{Guard, IdentityResolver, JwksVerifier};
use crate::kafka::{EventBus, EventConsumer, OutboxPublisher, ensure_indexes};
use crate::proto::{ClientMessage, client_message};

/// Replies go back on the stream the request arrived on, never on whatever stream is current: a
/// reply on another stream (after a reconnect, or to another hub later) would be ignored by the hub.
async fn reply_on_arrival_stream(
    dispatcher: Arc<Dispatcher>,
    request: crate::proto::Request,
    reply_to: crate::hub::ReplyTo,
) {
    let reply = dispatcher.handle(request).await;
    let request_id = reply.request_id.clone();
    if let Err(e) = reply_to
        .send(ClientMessage {
            message: Some(client_message::Message::Reply(reply)),
        })
        .await
    {
        tracing::warn!(request = %request_id, stream = reply_to.stream(), "reply dropped: {e}");
    }
    // The request is finished either way: one permit back to the hub it came from.
    reply_to.grant().await;
}
use crate::types::{Command, Event, MessageType, Query};

pub struct KrakenMessagingBuilder {
    config: Config,
    registry: HandlerRegistry,
    client: mongodb::Client,
    db: mongodb::Database,
    events: EventBus,
    conn: Arc<HubConnection>,
    inbound: mpsc::Receiver<Inbound>,
    sender: Arc<Sender>,
}

pub struct KrakenMessaging {
    conn: Arc<HubConnection>,
    sender: Arc<Sender>,
    events: EventBus,
    client: mongodb::Client,
    db: mongodb::Database,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    handlers: TaskTracker,
}

impl KrakenMessaging {
    /// Prepares the connections (nothing starts yet) so handlers can capture `events()` and the senders.
    pub async fn builder(config: Config) -> Result<KrakenMessagingBuilder, MessagingError> {
        config.validate()?;
        let client = mongodb::Client::with_uri_str(&config.mongodb_uri).await?;
        let db = client
            .default_database()
            .ok_or_else(|| MessagingError::Storage("mongodb_uri must name a database".into()))?;
        let events = EventBus::new(db.clone());
        let (tx, inbound) = mpsc::channel::<Inbound>(1024);
        let settings = HubSettings {
            address: config.hub_address.clone(),
            heartbeat_interval: config.heartbeat_interval,
            backoff_min: config.reconnect_backoff_min,
            backoff_max: config.reconnect_backoff_max,
            handshake_timeout: Duration::from_secs(10),
            max_concurrent_handlers: config.max_concurrent_handlers,
            load_factor: config.hub_load_factor,
            tags: config.hub_tags.clone(),
        };
        let conn = HubConnection::new(
            settings,
            config.service_name.clone(),
            uuid::Uuid::new_v4().to_string(),
            tx,
        );
        let sender = Sender::with_settings(
            Arc::clone(&conn),
            SenderSettings {
                default_timeout: config.hub_request_timeout,
                forward_jwt: config.forward_jwt,
                priorities: config.hub_priorities.clone(),
                retry: config.hub_retry.clone(),
            },
        );
        Ok(KrakenMessagingBuilder {
            config,
            registry: HandlerRegistry::new(),
            client,
            db,
            events,
            conn,
            inbound,
            sender,
        })
    }

    /// Sends commands over the hub.
    pub fn commands(&self) -> &Sender {
        &self.sender
    }

    /// Sends queries over the hub.
    pub fn queries(&self) -> &Sender {
        &self.sender
    }

    /// Publishes events through the outbox.
    pub fn events(&self) -> &EventBus {
        &self.events
    }

    pub fn instance_id(&self) -> &str {
        self.conn.instance_id()
    }

    /// The crate's Mongo client; sessions for `events().publish_in` must come from this client.
    pub fn client(&self) -> mongodb::Client {
        self.client.clone()
    }

    /// The service's database (the one named in `mongodb_uri`).
    pub fn database(&self) -> mongodb::Database {
        self.db.clone()
    }

    /// True once the hub confirmed this instance's registration.
    pub async fn await_hub(&self, timeout: Duration) -> bool {
        self.conn.await_registered(timeout).await
    }

    /// Drains in-flight handlers (bounded), closes the hub stream (the hub drops this instance at once),
    /// stops the outbox publisher and the consumer. Pending outgoing requests fail with `HubConnectionLost`.
    pub async fn shutdown(self) {
        self.handlers.close();
        if tokio::time::timeout(Duration::from_secs(10), self.handlers.wait())
            .await
            .is_err()
        {
            tracing::warn!("handlers still running after 10 s, closing the hub stream anyway");
        }
        // Close the stream before stopping the inbound loop, then fail whatever is still pending:
        // those requests were written, so the outcome is unknown (the Disconnected of the closed
        // stream may still sit in the inbound queue when the loop stops).
        self.conn.shutdown().await;
        self.sender.fail_all_lost("hub stream closed by shutdown");
        self.cancel.cancel();
        for t in self.tasks {
            if tokio::time::timeout(Duration::from_secs(5), t)
                .await
                .is_err()
            {
                tracing::warn!("background task did not stop in 5 s");
            }
        }
    }
}

impl KrakenMessagingBuilder {
    pub fn command<C, F, Fut>(mut self, handler: F) -> Self
    where
        C: Command,
        F: Fn(C, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<C::Reply, MessagingError>> + Send + 'static,
    {
        self.registry.command::<C, F, Fut>(handler);
        self
    }

    pub fn query<Q, F, Fut>(mut self, handler: F) -> Self
    where
        Q: Query,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Q::Reply, MessagingError>> + Send + 'static,
    {
        self.registry.query::<Q, F, Fut>(handler);
        self
    }

    pub fn event<E, F, Fut>(mut self, handler: F) -> Self
    where
        E: Event,
        F: Fn(E, EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        self.registry.event::<E, F, Fut>(handler);
        self
    }

    /// Opt-in authorization for a command or query registered before this call.
    pub fn requires<M: MessageType>(mut self, guard: Guard) -> Self {
        self.registry.requires::<M>(guard);
        self
    }

    /// The event bus, to capture in handlers that publish.
    pub fn events(&self) -> EventBus {
        self.events.clone()
    }

    /// The command/query sender, to capture in handlers that send.
    pub fn sender(&self) -> Arc<Sender> {
        Arc::clone(&self.sender)
    }

    /// The crate's Mongo client; handlers that write transactionally start their sessions here.
    pub fn client(&self) -> mongodb::Client {
        self.client.clone()
    }

    /// The service's database.
    pub fn database(&self) -> mongodb::Database {
        self.db.clone()
    }

    /// Ensures the Mongo indexes, registers with the hub (in the background), starts the outbox
    /// publisher and the consumer. Does not wait for the hub: see [`KrakenMessaging::await_hub`].
    pub async fn start(self) -> Result<KrakenMessaging, MessagingError> {
        let KrakenMessagingBuilder {
            config,
            registry,
            client,
            db,
            events,
            conn,
            mut inbound,
            sender,
        } = self;
        ensure_indexes(&db, config.outbox_retention, config.idempotency_retention).await?;
        let registry = Arc::new(registry);
        let verifier = match (&config.jwks_url, &config.issuer) {
            (Some(url), Some(issuer)) => Some(JwksVerifier::new(url.clone(), issuer.clone())),
            _ => None,
        };
        let dispatcher = Arc::new(Dispatcher::new(
            Arc::clone(&registry),
            Arc::new(IdentityResolver::new(verifier)),
            config.max_concurrent_handlers,
        ));
        let cancel = CancellationToken::new();
        let handlers = TaskTracker::new();
        let mut tasks = Vec::new();

        conn.set_types(registry.command_types(), registry.query_types());
        conn.start();
        let loop_sender = Arc::clone(&sender);
        let loop_cancel = cancel.clone();
        let loop_handlers = handlers.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    _ = loop_cancel.cancelled() => break,
                    msg = inbound.recv() => match msg { Some(m) => m, None => break },
                };
                match msg {
                    Inbound::Request { request, reply_to } => {
                        loop_handlers.spawn(reply_on_arrival_stream(
                            Arc::clone(&dispatcher),
                            request,
                            reply_to,
                        ));
                    }
                    other => loop_sender.on_inbound(other),
                }
            }
        }));

        if config.outbox_enabled {
            let publisher = OutboxPublisher::new(
                db.clone(),
                &config.kafka_bootstrap,
                &config.kafka_topic,
                &config.service_name,
                config.outbox_poll_interval,
                config.outbox_batch_size,
                config.outbox_max_attempts,
            )?;
            tasks.push(publisher.start(cancel.clone()));
        }
        if config.consumer_enabled && !registry.event_types().is_empty() {
            let consumer = EventConsumer::new(
                &config.kafka_bootstrap,
                &config.service_name,
                &config.kafka_topic,
                client.clone(),
                db.clone(),
                Arc::clone(&registry),
                config.consumer_max_attempts,
                Duration::from_secs(1),
            )?;
            tasks.push(consumer.start(cancel.clone()));
        }
        tracing::info!(service = %config.service_name, instance = %conn.instance_id(), commands = ?registry.command_types(), queries = ?registry.query_types(), events = ?registry.event_types(), "kraken messaging started");
        Ok(KrakenMessaging {
            conn,
            sender,
            events,
            client,
            db,
            cancel,
            tasks,
            handlers,
        })
    }
}
