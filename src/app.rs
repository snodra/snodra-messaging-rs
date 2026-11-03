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

use rdkafka::message::BorrowedMessage;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::Config;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::external::kafka::{SourceRegistration, validate_source};
use crate::external::{
    ExternalCallSettings, ExternalCalls, ExternalCallsHandle, ExternalEventIngress,
    ExternalKafkaSources, ExternalSource, IdempotencyKey, TranslatedEvent,
};
use crate::hub::{
    Dispatcher, EventContext, HandlerRegistry, HubPool, HubSettings, Inbound, QueryUpdateEmitter,
    Sender, SenderSettings, TokenProvider,
};
use crate::identity::{Guard, IdentityResolver, JwksVerifier};
use crate::kafka::{
    EventBus, EventConsumer, LocalEventDispatcher, NoSagas, OutboxPublisher, ensure_indexes,
};
use crate::saga::{
    Clock, SagaDefinition, SagaStats, SagaType, ensure_external_call_indexes,
    validate_registrations,
};
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
    /// The saga registrations, checked by `start()`.
    sagas: Vec<Result<SagaType, MessagingError>>,
    /// What the idempotent handlers record their calls in, bound by `start()`.
    external_calls: ExternalCallsHandle,
    /// The partner topics, consumed from `start()`.
    sources: Vec<SourceRegistration>,
    /// What the runtime, relay, scheduler and idempotent handlers report to.
    stats: SagaStats,
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
    ingress: ExternalEventIngress,
    stats: SagaStats,
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
            sagas: Vec::new(),
            external_calls: ExternalCallsHandle::new(),
            sources: Vec::new(),
            stats: SagaStats::new(),
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

    /// The webhook ingress (ADR 0021 ruling 18): a service's webhook endpoint verifies the
    /// request ([`webhook::verify`](crate::external::webhook::verify)), turns the body into an
    /// event and hands it to [`ExternalEventIngress::accept`], which dispatches it locally (the
    /// event handlers, then the sagas) before returning.
    ///
    /// ```ignore
    /// async fn webhook(app: &KrakenMessaging, headers: &HashMap<String, String>, body: &[u8]) -> u16 {
    ///     if !webhook::verify(&secret, None, headers, body) { return 401; }
    ///     let reply: PrinterReply = match serde_json::from_slice(body) { Ok(r) => r, Err(_) => return 400 };
    ///     match app.ingress().accept("printer-banner", &reply.event_id, &BannerPrinted::from(reply), headers).await {
    ///         Ok(_) => 202,  // Accepted or Duplicate
    ///         Err(_) => 500, // the partner retries
    ///     }
    /// }
    /// ```
    pub fn ingress(&self) -> ExternalEventIngress {
        self.ingress.clone()
    }

    /// The saga, deadline, dispatch and external-call statistics (S2-R7): the Kotlin meters'
    /// names and labels as counters and gauges, see [`SagaStats`] and
    /// [`crate::saga::stats`]. All zero in a service without sagas or idempotent handlers.
    pub fn saga_stats(&self) -> SagaStats {
        self.stats.clone()
    }

    pub fn instance_id(&self) -> &str {
        self.hubs.instance_id()
    }

    /// The ids of the hubs this instance is registered on now (a legacy hub by its address).
    pub fn hub_ids(&self) -> Vec<String> {
        self.hubs.hub_ids()
    }

    /// How this instance authenticates to the hubs: `none`, a static `token`, a `jwt`
    /// (Keycloak client credentials, ADR 0019) or a client `certificate` (mutual TLS, ADR 0020),
    /// the current access token's expiry, the last token fetch error and the token fetch counts by
    /// result (`kraken_messaging_hub_token_fetches_total{result}`); for a certificate, the one
    /// presented (fingerprint, `notAfter`, rotations).
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

    /// Registers a saga (ADR 0021): `name` is its wire type name in `sagas.type`, `S` its state
    /// (stored as JSON text, so it must be `Serialize + Deserialize` and round-trip from its
    /// `Default`), `define` its handlers. A definition error (no start handler, two sagas of one
    /// name, a state that does not round-trip, `ending()` before any handler) is returned by
    /// [`start`](Self::start), naming the saga and the handler.
    ///
    /// ```ignore
    /// builder.saga::<PaymentState>("payment", |d| {
    ///     d.start_on::<OrderPlaced>("orderId", |e| Some(e.order_id.clone()), |s, e, ctx| {
    ///         s.order_id = Some(e.order_id.clone());
    ///         ctx.associate_with("paymentRef", &format!("pay-{}", e.order_id));
    ///         ctx.send(&RequestCharge { amount: e.amount }, None)?; // dispatched after commit
    ///         Ok(())
    ///     })
    ///     .on::<ChargeSucceeded>("paymentRef", |e| Some(e.payment_ref.clone()), |s, e, ctx| Ok(()))
    ///     .ending()
    ///     .on_deadline::<()>("charge-timeout", |s, _, ctx| { ctx.end(); Ok(()) })
    /// })
    /// ```
    pub fn saga<S>(
        mut self,
        name: &'static str,
        define: impl FnOnce(SagaDefinition<S>) -> SagaDefinition<S>,
    ) -> Self
    where
        S: Serialize + DeserializeOwned + Default + Send + 'static,
    {
        self.sagas
            .push(define(SagaDefinition::<S>::new(name)).build());
        self
    }

    /// An idempotent command handler, an adapter to an external system (ADR 0021 ruling 17, the
    /// `@IdempotentHandler @CommandHandler` of Rust): each call is recorded in `external_calls`
    /// under `key` (`IdempotencyKey::default()` is the `idempotency-key` header the saga relay
    /// sets), so a repeat of the key never runs `handler` twice. Return
    /// [`MessagingError::RetryableExternal`] for a failure a later attempt may cure and
    /// [`MessagingError::UnknownOutcome`] when the partner may have acted; never retry inside the
    /// handler (the relay owns the backoff). `external_idempotent`: the partner de-duplicates by
    /// the key the adapter passes it, so a call that crashed mid-way is called again after the
    /// lease instead of reporting an unknown outcome. See [`crate::external::calls`].
    ///
    /// ```ignore
    /// builder.idempotent_command::<PrintBanner, _, _>(IdempotencyKey::default(), false, |c, ctx| async move {
    ///     printer.print(&c.text, &ctx.headers["idempotency-key"]).await
    /// })
    /// ```
    ///
    /// # Panics
    /// When `C` already has a handler here.
    pub fn idempotent_command<C, F, Fut>(
        mut self,
        key: IdempotencyKey<C>,
        external_idempotent: bool,
        handler: F,
    ) -> Self
    where
        C: Command,
        F: Fn(C, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<C::Reply, MessagingError>> + Send + 'static,
    {
        self.registry.idempotent_command::<C, F, Fut>(
            &self.external_calls,
            key,
            external_idempotent,
            handler,
        );
        self
    }

    /// An idempotent event handler (an `@IdempotentHandler @EventHandler`): see
    /// [`idempotent_command`](Self::idempotent_command). Its body runs inside the ordinary
    /// handlers' transaction (ruling 43), so a slow external call holds it open: prefer a command
    /// handler for slow calls.
    pub fn idempotent_event<E, F, Fut>(
        mut self,
        key: IdempotencyKey<E>,
        external_idempotent: bool,
        handler: F,
    ) -> Self
    where
        E: Event,
        F: Fn(E, EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        self.registry.idempotent_event::<E, F, Fut>(
            &self.external_calls,
            key,
            external_idempotent,
            handler,
        );
        self
    }

    /// A partner's Kafka topic whose records become local kraken events (ADR 0021 ruling 20,
    /// spec "Inbound 1"): `translator` turns each record into an event (`Ok(None)` ignores it, an
    /// error retries it up to `KRAKEN_CONSUMER_MAX_ATTEMPTS` times and then skips it), which is
    /// dispatched locally to this service's event handlers and sagas, never published to
    /// `kraken.events`. The dedupe id is `<name>:<partition>:<offset>`, or `<name>:<dedupeKey>`
    /// when the translation carries one. A `properties` entry for a key the library fixes, an
    /// empty topic or two sources of one name fail [`start`](Self::start). See
    /// [`crate::external::kafka`].
    ///
    /// ```ignore
    /// builder.external_source("printer-banner", ExternalSource::new("partner.printer.banner"), |record| {
    ///     let reply: PrinterReply = serde_json::from_slice(record.payload().unwrap_or_default())?;
    ///     Ok(Some(TranslatedEvent::new(&BannerPrinted { job_id: reply.job_id }, Some(reply.event_id))?))
    /// })
    /// ```
    pub fn external_source<T>(mut self, name: &str, source: ExternalSource, translator: T) -> Self
    where
        T: Fn(&BorrowedMessage<'_>) -> Result<Option<TranslatedEvent>, MessagingError>
            + Send
            + Sync
            + 'static,
    {
        self.sources.push(SourceRegistration {
            name: name.to_string(),
            source,
            translator: Arc::new(translator),
        });
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
            sagas,
            external_calls,
            sources,
            stats,
        } = self;
        // Definition errors first: nothing is connected or created for a saga that cannot run.
        let _sagas = validate_registrations(sagas)?;
        for source in &sources {
            validate_source(&source.name, &source.source)?;
        }
        ensure_indexes(&db, config.outbox_retention, config.idempotency_retention).await?;
        if !registry.idempotent_handlers().is_empty() {
            ensure_external_call_indexes(&db, Duration::from_secs(30 * 86_400)).await?;
            external_calls.bind(ExternalCalls::new(
                db.clone(),
                Clock::system(),
                ExternalCallSettings::default(),
                Arc::new(stats.clone()),
            )?);
        }
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
        // The local dispatch of partner records and webhook events: the ordinary handlers, then
        // the sagas.
        let local = Arc::new(LocalEventDispatcher::new(
            client.clone(),
            db.clone(),
            Arc::clone(&registry),
            Arc::new(NoSagas),
        ));
        let ingress = ExternalEventIngress::new(Arc::clone(&local), db.clone(), Clock::system());
        if !sources.is_empty() {
            let sources = ExternalKafkaSources::new(
                sources,
                Arc::clone(&local),
                &config.service_name,
                &config.kafka_bootstrap,
                config.consumer_max_attempts,
                Duration::from_secs(1),
            )?;
            tasks.push(sources.start(cancel.clone()));
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
            ingress,
            stats,
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
        ..HubSettings::default()
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
