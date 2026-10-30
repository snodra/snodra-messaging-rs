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

use crate::config::Config;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::hub::{
    Dispatcher, EventContext, HandlerRegistry, HubConnection, HubSettings, Inbound, Sender,
};
use crate::identity::{Guard, IdentityResolver, JwksVerifier};
use crate::kafka::{EventBus, EventConsumer, OutboxPublisher, ensure_indexes};
use crate::proto::{ClientMessage, client_message};
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
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
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
        };
        let conn = HubConnection::new(
            settings,
            config.service_name.clone(),
            uuid::Uuid::new_v4().to_string(),
            tx,
        );
        let sender = Sender::new(
            Arc::clone(&conn),
            config.hub_request_timeout,
            config.forward_jwt,
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

    /// True once the hub confirmed this instance's registration.
    pub async fn await_hub(&self, timeout: Duration) -> bool {
        self.conn.await_registered(timeout).await
    }

    /// Stops the hub stream, the outbox publisher and the consumer. Pending requests fail.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        self.conn.shutdown().await;
        for t in self.tasks {
            let _ = tokio::time::timeout(Duration::from_secs(10), t).await;
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
        let mut tasks = Vec::new();

        conn.set_types(registry.command_types(), registry.query_types());
        conn.start();
        let loop_conn = Arc::clone(&conn);
        let loop_sender = Arc::clone(&sender);
        tasks.push(tokio::spawn(async move {
            while let Some(msg) = inbound.recv().await {
                match msg {
                    Inbound::Request(req) => {
                        let dispatcher = Arc::clone(&dispatcher);
                        let conn = Arc::clone(&loop_conn);
                        tokio::spawn(async move {
                            let reply = dispatcher.handle(req).await;
                            if let Err(e) = conn
                                .send(ClientMessage {
                                    message: Some(client_message::Message::Reply(reply)),
                                })
                                .await
                            {
                                tracing::warn!("reply dropped, hub stream gone: {e}");
                            }
                        });
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
                client,
                db,
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
            cancel,
            tasks,
        })
    }
}
