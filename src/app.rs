//! `KrakenMessaging`: everything a service needs, wired from a [`Config`].
//!
//! ```ignore
//! let builder = KrakenMessaging::builder(Config::from_env()?).await?;
//! let events = builder.events();                      // clone into handlers that publish
//! let app = builder
//!     .command::<Shout, _, _>(move |cmd, ctx| { let events = events.clone(); async move { ... } })
//!     .query::<Reverse, _, _>(|q, ctx| async move { ... })
//!     .shared_query::<Search, _, _>(|q, ctx| async move { ... }) // many services may own it
//!     .streaming_query::<Export, _, _, _>(|q, ctx| async move { Ok(rows(q)) }) // a stream of items
//!     .subscription_query::<Prices, _, _>(|q, ctx| async move { Ok(latest(q)) }) // initial result; updates via `emitter`
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
    Dispatcher, EventContext, HandlerRegistry, HubPool, HubSettings, Inbound, QueryUpdateEmitter,
    Sender, SenderSettings, TokenProvider,
};
use crate::identity::{Guard, IdentityResolver, JwksVerifier};
use crate::kafka::{EventBus, EventConsumer, OutboxPublisher, ensure_indexes};
use crate::types::{Command, Event, MessageType, Query, StreamingQuery, SubscriptionQuery};

/// How long a shutdown waits for the subscriptions' `ReplyEnd`s to be written.
const SUBSCRIPTION_END_TIMEOUT: Duration = Duration::from_secs(2);

pub struct KrakenMessagingBuilder {
    config: Config,
    registry: HandlerRegistry,
    client: mongodb::Client,
    db: mongodb::Database,
    events: EventBus,
    hubs: Arc<HubPool>,
    inbound: mpsc::Receiver<Inbound>,
    sender: Arc<Sender>,
    emitter: QueryUpdateEmitter,
}

pub struct KrakenMessaging {
    hubs: Arc<HubPool>,
    sender: Arc<Sender>,
    dispatcher: Arc<Dispatcher>,
    emitter: QueryUpdateEmitter,
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
        let settings = hub_settings(&config)?;
        let hubs = HubPool::new(
            settings,
            config.service_name.clone(),
            uuid::Uuid::new_v4().to_string(),
            tx,
        )?;
        let sender = Sender::with_pool(
            Arc::clone(&hubs),
            SenderSettings {
                default_timeout: config.hub_request_timeout,
                forward_jwt: config.forward_jwt,
                priorities: config.hub_priorities.clone(),
                retry: config.hub_retry.clone(),
            },
        );
        Ok(KrakenMessagingBuilder {
            registry: HandlerRegistry::new(),
            client,
            db,
            events,
            hubs,
            inbound,
            sender,
            emitter: QueryUpdateEmitter::new(config.hub_max_subscriptions),
            config,
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

    /// The same sender as [`commands`](Self::commands) and [`queries`](Self::queries), shared:
    /// for [`Sender::stream_owned`] (a `'static` stream, e.g. an HTTP streaming body) and for
    /// tasks that outlive a borrow of the app. The builder's `sender()` is the same `Arc`.
    pub fn sender(&self) -> Arc<Sender> {
        Arc::clone(&self.sender)
    }

    /// Publishes events through the outbox.
    pub fn events(&self) -> &EventBus {
        &self.events
    }

    /// The emitter that pushes updates to the subscription queries this instance serves (ADR
    /// 0017, P4c): the same one the builder's `emitter()` gave to handlers. See
    /// [`QueryUpdateEmitter`].
    pub fn emitter(&self) -> QueryUpdateEmitter {
        self.emitter.clone()
    }

    pub fn instance_id(&self) -> &str {
        self.hubs.instance_id()
    }

    /// The ids of the hubs this instance is registered on now (a legacy hub by its address).
    pub fn hub_ids(&self) -> Vec<String> {
        self.hubs.hub_ids()
    }

    /// How this instance authenticates to the hubs: `none`, a static `token` or a `jwt`
    /// (Keycloak client credentials, ADR 0019), the current access token's expiry, the last
    /// token fetch error and the token fetch counts by result
    /// (`kraken_messaging_hub_token_fetches_total{result}`).
    pub fn hub_auth(&self) -> crate::hub::AuthStatus {
        self.hubs.auth()
    }

    /// The credential refreshes sent to the hubs so far
    /// (`kraken_messaging_hub_credentials_refreshes_sent_total`).
    pub fn hub_credentials_refreshes_sent(&self) -> u64 {
        self.hubs.credentials_refreshes_sent()
    }

    /// The crate's Mongo client; sessions for `events().publish_in` must come from this client.
    pub fn client(&self) -> mongodb::Client {
        self.client.clone()
    }

    /// The service's database (the one named in `mongodb_uri`).
    pub fn database(&self) -> mongodb::Database {
        self.db.clone()
    }

    /// True once a hub (with several: at least one, so requests can be sent) confirmed this
    /// instance's registration. kraken-messaging's `HubConnections.awaitRegistered` waits for
    /// every hub; that is [`await_all_hubs`](Self::await_all_hubs) here.
    pub async fn await_hub(&self, timeout: Duration) -> bool {
        self.hubs.await_registered(timeout).await
    }

    /// True once every hub of `hub_address` confirmed this instance's registration (with
    /// `dns:///`: every address of the latest resolution; a second target of a hub already reached
    /// does not count). Use it when every hub must be able to route to this instance before
    /// going on, e.g. before taking traffic in a rollout.
    pub async fn await_all_hubs(&self, timeout: Duration) -> bool {
        self.hubs.await_all_registered(timeout).await
    }

    /// Drains in-flight handlers (bounded), completes the subscriptions served here with a
    /// `ReplyEnd` (best effort, bounded), closes every hub stream (each hub drops this instance
    /// at once), stops the outbox publisher and the consumer. Pending outgoing requests fail
    /// with `HubConnectionLost`.
    pub async fn shutdown(self) {
        self.handlers.close();
        if tokio::time::timeout(Duration::from_secs(10), self.handlers.wait())
            .await
            .is_err()
        {
            tracing::warn!("handlers still running after 10 s, closing the hub stream anyway");
        }
        let ends = futures::future::join_all(self.dispatcher.end_subscriptions());
        if tokio::time::timeout(SUBSCRIPTION_END_TIMEOUT, ends)
            .await
            .is_err()
        {
            tracing::warn!("subscription ends not written in time, closing the hub stream anyway");
        }
        // Close the stream before stopping the inbound loop, then fail whatever is still pending:
        // those requests were written, so the outcome is unknown (the Disconnected of the closed
        // stream may still sit in the inbound queue when the loop stops).
        self.hubs.shutdown().await;
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

    /// A shared query handler (ADR 0017): several services may own `Q`; a point query goes to one
    /// instance of any owner, a scatter-gather ([`Sender::scatter_gather`]) to all of them. Every
    /// service registering `Q` must register it shared, or the hub rejects the later registration
    /// with `ALREADY_EXISTS`. See [`HandlerRegistry::shared_query`].
    ///
    /// # Panics
    /// When `Q` already has a handler here (shared or exclusive).
    pub fn shared_query<Q, F, Fut>(mut self, handler: F) -> Self
    where
        Q: Query,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Q::Reply, MessagingError>> + Send + 'static,
    {
        self.registry.shared_query::<Q, F, Fut>(handler);
        self
    }

    /// A streaming query handler (ADR 0017, P4b): `Q` is served to [`Sender::stream`] callers
    /// as a stream of `Q::Item`s under their credits; this service is its only owner. See
    /// [`HandlerRegistry::streaming_query`].
    ///
    /// # Panics
    /// When `Q` already has a handler here.
    pub fn streaming_query<Q, F, Fut, S>(mut self, handler: F) -> Self
    where
        Q: StreamingQuery,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, MessagingError>> + Send + 'static,
        S: futures::Stream<Item = Result<Q::Item, MessagingError>> + Send + 'static,
    {
        self.registry.streaming_query::<Q, F, Fut, S>(handler);
        self
    }

    /// A shared streaming query handler: several services may own `Q`; a stream goes to one
    /// capable instance of any owner. See [`HandlerRegistry::shared_streaming_query`].
    ///
    /// # Panics
    /// When `Q` already has a handler here (shared or exclusive).
    pub fn shared_streaming_query<Q, F, Fut, S>(mut self, handler: F) -> Self
    where
        Q: StreamingQuery,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, MessagingError>> + Send + 'static,
        S: futures::Stream<Item = Result<Q::Item, MessagingError>> + Send + 'static,
    {
        self.registry
            .shared_streaming_query::<Q, F, Fut, S>(handler);
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

    /// A subscription query handler (ADR 0017, P4c): `handler` answers the initial result of
    /// `Q` for [`Sender::subscribe`] callers; this service is its only owner. The updates come
    /// from the [`QueryUpdateEmitter`] (`emitter()`), called by whatever changes the data. See
    /// [`HandlerRegistry::subscription_query`].
    ///
    /// # Panics
    /// When `Q` already has a handler here.
    pub fn subscription_query<Q, F, Fut>(mut self, handler: F) -> Self
    where
        Q: SubscriptionQuery,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Q::Initial, MessagingError>> + Send + 'static,
    {
        self.registry.subscription_query::<Q, F, Fut>(handler);
        self
    }

    /// A shared subscription query handler: several services may own `Q`; a subscription
    /// reaches every capable instance of every owner. See
    /// [`HandlerRegistry::shared_subscription_query`].
    ///
    /// # Panics
    /// When `Q` already has a handler here (shared or exclusive).
    pub fn shared_subscription_query<Q, F, Fut>(mut self, handler: F) -> Self
    where
        Q: SubscriptionQuery,
        F: Fn(Q, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Q::Initial, MessagingError>> + Send + 'static,
    {
        self.registry
            .shared_subscription_query::<Q, F, Fut>(handler);
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

    /// The emitter that pushes updates to the subscription queries this instance serves (ADR
    /// 0017, P4c), to capture in the code that changes the data: `app.emitter()` is the same
    /// one. See [`QueryUpdateEmitter`].
    pub fn emitter(&self) -> QueryUpdateEmitter {
        self.emitter.clone()
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
            hubs,
            mut inbound,
            sender,
            emitter,
        } = self;
        ensure_indexes(&db, config.outbox_retention, config.idempotency_retention).await?;
        let registry = Arc::new(registry);
        let verifier = match (&config.jwks_url, &config.issuer) {
            (Some(url), Some(issuer)) => Some(JwksVerifier::new(url.clone(), issuer.clone())),
            _ => None,
        };
        let dispatcher = Arc::new(
            Dispatcher::new(
                Arc::clone(&registry),
                Arc::new(IdentityResolver::new(verifier)),
                config.max_concurrent_handlers,
            )
            .stream_credit_timeout(config.hub_stream_credit_timeout)
            .with_emitter(emitter.clone()),
        );
        let cancel = CancellationToken::new();
        let handlers = TaskTracker::new();
        let mut tasks = Vec::new();

        hubs.set_registered_types(registry.registered_types());
        hubs.start();
        let loop_sender = Arc::clone(&sender);
        let loop_cancel = cancel.clone();
        let loop_handlers = handlers.clone();
        let loop_dispatcher = Arc::clone(&dispatcher);
        tasks.push(tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    _ = loop_cancel.cancelled() => break,
                    msg = inbound.recv() => match msg { Some(m) => m, None => break },
                };
                match msg {
                    // Served on the stream it arrived on. `serve` registers the request (a
                    // running request, or a subscription pending its admission) synchronously,
                    // before the spawn, so that a Flow or Cancel right behind it finds it.
                    Inbound::Request { request, reply_to } => {
                        loop_handlers.spawn(loop_dispatcher.serve(request, reply_to));
                    }
                    Inbound::Flow(f) => loop_dispatcher.flow(&f.request_id, f.credits),
                    Inbound::Cancel(c) => loop_dispatcher.cancel(&c.request_id, &c.reason),
                    // A lost stream fails what was sent on it and cancels the streaming
                    // queries that arrived on it.
                    Inbound::Disconnected { stream, .. } => {
                        loop_dispatcher.stream_lost(stream);
                        loop_sender.on_inbound(msg);
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
        tracing::info!(service = %config.service_name, instance = %hubs.instance_id(), hubs = ?hubs.targets(), commands = ?registry.command_types(), queries = ?registry.exclusive_query_types(), shared_queries = ?registry.shared_query_types(), streaming_queries = ?registry.streaming_query_types(), subscription_queries = ?registry.subscription_query_types(), events = ?registry.event_types(), "kraken messaging started");
        Ok(KrakenMessaging {
            hubs,
            sender,
            dispatcher,
            emitter,
            events,
            client,
            db,
            cancel,
            tasks,
            handlers,
        })
    }
}

/// The hub connection settings a [`Config`] describes, access control included.
///
/// With `hub_oauth2` this is where the one token provider of the process is made; it is shared by
/// every hub connection through the settings.
fn hub_settings(config: &Config) -> Result<HubSettings, MessagingError> {
    let token_provider = config
        .hub_oauth2
        .clone()
        .map(TokenProvider::new)
        .transpose()?;
    Ok(HubSettings {
        address: config.hub_address.clone(),
        heartbeat_interval: config.heartbeat_interval,
        backoff_min: config.reconnect_backoff_min,
        backoff_max: config.reconnect_backoff_max,
        handshake_timeout: Duration::from_secs(10),
        max_concurrent_handlers: config.max_concurrent_handlers,
        load_factor: config.hub_load_factor,
        tags: config.hub_tags.clone(),
        status_interval: config.hub_status_interval,
        dns_refresh: crate::hub::DNS_REFRESH,
        token: config.hub_token.clone(),
        token_file: config.hub_token_file.clone(),
        tls: config.hub_tls.clone(),
        token_provider,
    })
}

#[cfg(test)]
mod hub_settings_tests {
    use super::*;
    use crate::security::{HubTls, SecretString};

    #[test]
    fn the_config_reaches_the_hub_settings() {
        let mut config = Config::new("svc", "static://hub:7000", "kafka:19092", "mongodb://m/db");
        assert!(hub_settings(&config).unwrap().token.is_none());
        assert!(hub_settings(&config).unwrap().tls.is_none());
        assert!(hub_settings(&config).unwrap().token_provider.is_none());
        config.hub_token = Some(SecretString::new("tok"));
        config.hub_token_file = Some("/run/secrets/hub-token".into());
        config.hub_tls = Some(HubTls {
            server_name: Some("kraken-hub".into()),
            ..HubTls::default()
        });
        let settings = hub_settings(&config).unwrap();
        assert_eq!(
            settings.token.as_ref().map(SecretString::expose),
            Some("tok")
        );
        assert_eq!(
            settings.token_file.as_deref(),
            Some(std::path::Path::new("/run/secrets/hub-token"))
        );
        assert_eq!(settings.tls, config.hub_tls);
    }

    #[test]
    fn oauth2_in_the_config_becomes_one_token_provider_for_the_settings() {
        let mut config = Config::new("svc", "a:7000,b:7000", "kafka:19092", "mongodb://m/db");
        config.hub_oauth2 = Some(crate::security::HubOAuth2 {
            token_uri: "http://keycloak:8080/token".into(),
            client_id: "svc".into(),
            client_secret: Some(SecretString::new("s3cr3t")),
            ..Default::default()
        });
        let settings = hub_settings(&config).unwrap();
        let provider = settings.token_provider.as_ref().expect("a provider");
        // The pool clones the settings for each hub: they all share the provider.
        assert!(Arc::ptr_eq(
            provider,
            settings.clone().token_provider.as_ref().unwrap()
        ));
        assert!(!format!("{settings:?}").contains("s3cr3t"));
        config.hub_oauth2.as_mut().unwrap().token_uri = "ftp://nope".into();
        assert!(hub_settings(&config).is_err());
    }
}
