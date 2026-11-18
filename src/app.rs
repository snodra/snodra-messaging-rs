//! `SnodraMessaging`: everything a service needs, wired from a [`Config`].
//!
//! ```ignore
//! let builder = SnodraMessaging::builder(Config::from_env()?).await?
//!     .store(Arc::new(store))                         // a message store add-on (events, sagas)
//!     .event_store(Arc::new(events));                 // an event-store add-on (events, sagas)
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

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::Config;
use crate::config::DeadLetterMode;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::event_store::{EventStore, SubscriptionSpec};
use crate::events::{
    EventBus, LocalEventDispatcher, NoSagas, OutboxPublisher, OutboxPublisherSettings,
    ProcessorSettings, SagaStage, check_dead_letter_mode,
};
use crate::external::sources::{SourceRegistration, validate_source};
use crate::external::{
    ExternalCalls, ExternalCallsHandle, ExternalEventIngress, ExternalRecord,
    ExternalSourceProvider, ExternalSources, IdempotencyKey, TranslatedEvent,
};
use crate::hub::{
    Dispatcher, EventContext, HandlerRegistry, HubPool, HubSettings, Inbound, QueryUpdateEmitter,
    Sender, SenderSettings, TokenProvider,
};
use crate::identity::{Guard, IdentityResolver, JwksVerifier};
use crate::metrics::Metrics;
use crate::processors::reset::ResetHandler;
use crate::processors::{
    DEFAULT_PROCESSOR, DeadLetters, EventOptions, ProcessorControl, ProcessorMonitor,
    ProcessorRunner, ResetContext, marker_prefix, subscription_name, validate_processor_name,
};
use crate::proto::ProcessorStatus;
use crate::saga::{
    Clock, DeadlineScheduler, LocalFirstSender, OtelSagaMetrics, RawCommandSender, SagaDefinition,
    SagaDispatchRelay, SagaFailures, SagaMetrics, SagaPorts, SagaRetry, SagaRuntime, SagaStats,
    SagaType, stats::SAMPLE_INTERVAL, validate_registrations,
};
use crate::store::MessageStore;
use crate::types::{Command, Event, MessageType, Query, StreamingQuery, SubscriptionQuery};

/// How long a shutdown waits for the subscriptions' `ReplyEnd`s to be written.
const SUBSCRIPTION_END_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a graceful shutdown waits for the hubs to ack its empty registration (as
/// snodra-messaging's `HubLifecycle.stop`).
pub const DEREGISTER_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a shutdown waits for the saga relay, the deadline scheduler and the sampler (ruling 64).
const SAGA_STOP_TIMEOUT: Duration = Duration::from_secs(20);

pub struct SnodraMessagingBuilder {
    config: Config,
    registry: HandlerRegistry,
    /// The message store (ADR 0028); none for a service with commands and queries only.
    store: Option<Arc<dyn MessageStore>>,
    /// The event store (ADR 0027); none for a service that neither publishes nor handles events.
    event_store: Option<Arc<dyn EventStore>>,
    events: EventBus,
    hubs: Arc<HubPool>,
    inbound: mpsc::Receiver<Inbound>,
    sender: Arc<Sender>,
    emitter: QueryUpdateEmitter,
    /// The saga registrations, checked by `start()`.
    sagas: Vec<Result<SagaType, MessagingError>>,
    /// What the idempotent handlers record their calls in, bound by `start()`.
    external_calls: ExternalCallsHandle,
    /// The partner sources, read from `start()`.
    sources: Vec<SourceRegistration>,
    /// What the runtime, relay, scheduler and idempotent handlers report to.
    stats: SagaStats,
    /// "Now" for the saga runtime, relay, scheduler, idempotent handlers and ingress.
    clock: Clock,
    /// What the library records its metrics on (ADR 0026).
    metrics: Metrics,
    /// The named event processors' handlers (ADR 0022), by processor name.
    processors: std::collections::BTreeMap<String, HandlerRegistry>,
    /// Invalid processor names given to `event_in`, returned by `start()`.
    processor_errors: Vec<MessagingError>,
    /// The reset handlers (ADR 0023), by processor name, in registration order.
    reset_handlers: Vec<(String, ResetHandler)>,
}

pub struct SnodraMessaging {
    hubs: Arc<HubPool>,
    sender: Arc<Sender>,
    dispatcher: Arc<Dispatcher>,
    emitter: QueryUpdateEmitter,
    events: EventBus,
    store: Option<Arc<dyn MessageStore>>,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    handlers: TaskTracker,
    ingress: ExternalEventIngress,
    stats: SagaStats,
    /// What the saga runtime, relay, scheduler and idempotent handlers report to: `stats` and
    /// the meter ([`OtelSagaMetrics`]).
    saga_metrics: Arc<dyn SagaMetrics>,
    /// The saga runtime, when a saga is registered (and enabled).
    runtime: Option<Arc<SagaRuntime>>,
    /// Stops the relay, the deadline scheduler and the gauge sampler.
    saga_cancel: CancellationToken,
    saga_tasks: Vec<JoinHandle<()>>,
    clock: Clock,
    /// One per started event processor, `default` first.
    monitors: Vec<Arc<ProcessorMonitor>>,
    dead_letters: DeadLetters,
    processor_control: ProcessorControl,
}

impl SnodraMessaging {
    /// Prepares the connections (nothing starts yet) so handlers can capture `events()` and the
    /// senders. The metrics (ADR 0026) are recorded on
    /// `opentelemetry::global::meter("snodra-messaging-rs")` as the global meter provider is now:
    /// install the application's provider before this call; with none, nothing is recorded.
    pub async fn builder(config: Config) -> Result<SnodraMessagingBuilder, MessagingError> {
        Self::builder_with_meter(
            config,
            opentelemetry::global::meter(crate::metrics::METER_NAME),
        )
        .await
    }

    /// [`builder`](Self::builder) recording the metrics on `meter` instead of the global one.
    pub async fn builder_with_meter(
        config: Config,
        meter: opentelemetry::metrics::Meter,
    ) -> Result<SnodraMessagingBuilder, MessagingError> {
        config.validate()?;
        let metrics = Metrics::new(meter);
        let events = EventBus::new();
        let (tx, inbound) = mpsc::channel::<Inbound>(1024);
        let settings = hub_settings(&config)?;
        let hubs = HubPool::new(
            settings,
            config.service_name.clone(),
            config.instance_id.clone(),
            tx,
        )?;
        let sender = Sender::with_pool(
            Arc::clone(&hubs),
            SenderSettings {
                default_timeout: config.hub_request_timeout,
                forward_jwt: config.forward_jwt,
                priorities: config.hub_priorities.clone(),
                retry: config.hub_retry.clone(),
                metrics: metrics.clone(),
            },
        );
        HubPool::observe(&hubs, &metrics);
        Ok(SnodraMessagingBuilder {
            registry: HandlerRegistry::new(),
            store: None,
            event_store: None,
            events,
            hubs,
            inbound,
            sender,
            emitter: QueryUpdateEmitter::new(config.hub_max_subscriptions),
            sagas: Vec::new(),
            external_calls: ExternalCallsHandle::new(),
            sources: Vec::new(),
            stats: SagaStats::new(),
            clock: Clock::system(),
            metrics,
            processors: std::collections::BTreeMap::new(),
            processor_errors: Vec::new(),
            reset_handlers: Vec::new(),
            config,
        })
    }

    /// The dead-letter admin API (ADR 0022): list, get, count, retry, retry_all and discard the
    /// letters in `dead_letters` of the processors this instance runs. In modes `event-store` and `log`
    /// every call is an error naming the mode. See [`DeadLetters`].
    pub fn dead_letters(&self) -> DeadLetters {
        self.dead_letters.clone()
    }

    /// Processor control (ADR 0023): pause and resume a processor on every instance (through its
    /// `processor_control` document), and the handling of the hub's control requests. See
    /// [`ProcessorControl`].
    pub fn processor_control(&self) -> ProcessorControl {
        self.processor_control.clone()
    }

    /// The status of every event processor this instance started (`default` first, then the
    /// named ones by name), as the hubs get it every status interval. Empty when the consumer is
    /// disabled or no processor has handlers.
    pub fn processors(&self) -> Vec<ProcessorStatus> {
        self.monitors.iter().map(|m| m.status()).collect()
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
    /// async fn webhook(app: &SnodraMessaging, headers: &HashMap<String, String>, body: &[u8]) -> u16 {
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

    /// The admin API over the parked saga events (the store's saga failures, ADR 0021 rulings 3,
    /// 36): list, retry and discard. In a service without sagas (or with them disabled) it lists
    /// what the store holds and a retry is refused (`InvalidMessage`), leaving the event as it
    /// was; without a store every call fails.
    pub fn saga_failures(&self) -> SagaFailures {
        if let Some(runtime) = &self.runtime {
            return SagaFailures::new(Arc::clone(runtime));
        }
        match &self.store {
            Some(store) => SagaFailures::new(Arc::new(SagaRuntime::new(
                Vec::new(),
                SagaPorts::of(&**store),
                self.clock.clone(),
                SagaRetry::default(),
                Arc::clone(&self.saga_metrics),
            ))),
            None => SagaFailures::without_store(),
        }
    }

    /// The saga, deadline, dispatch and external-call statistics (S2-R7): the values the saga
    /// metrics export (ADR 0026), as counters and gauges in memory, see [`SagaStats`] and
    /// [`crate::saga::stats`]. All zero in a service without sagas or idempotent handlers.
    pub fn saga_stats(&self) -> SagaStats {
        self.stats.clone()
    }

    /// `Config.instance_id`: the id the hubs know this instance by.
    pub fn instance_id(&self) -> &str {
        self.hubs.instance_id()
    }

    /// The ids of the hubs this instance is registered on now.
    pub fn hub_ids(&self) -> Vec<String> {
        self.hubs.hub_ids()
    }

    /// Why hubs refuse this instance now: the latest refusal of its registration by each hub that
    /// refuses it (a `Register` rejected, e.g. a type another service owns or an identity the hub
    /// does not accept, or no access token for it), by hub id (the target dialled, `host:port`,
    /// for a hub that never registered this instance: a refusal carries no hub id). Empty while
    /// every hub accepts it; a hub leaves the map once it accepts a registration again. The
    /// library also logs each distinct refusal once at error and retries at the backoff ceiling.
    pub fn hub_rejections(&self) -> std::collections::BTreeMap<String, String> {
        self.hubs.rejections()
    }

    /// The newest refusal of this instance's registration by any hub (which hub, the status
    /// code, the hub's message, when): replaced only by a newer refusal, never cleared by a later
    /// acceptance, so it still says why after the hub accepted again. `None` while no hub ever
    /// refused. [`hub_rejections`](Self::hub_rejections) is what refuses it now.
    pub fn latest_rejection(&self) -> Option<crate::hub::HubRejection> {
        self.hubs.latest_rejection()
    }

    /// How this instance authenticates to the hubs: `none`, a static `token`, a `jwt`
    /// (Keycloak client credentials, ADR 0019) or a client `certificate` (mutual TLS, ADR 0020),
    /// the current access token's expiry, the last token fetch error and the token fetch counts by
    /// result (`snodra_hub_client_token_fetches_total{result}`); for a certificate, the one
    /// presented (fingerprint, `notAfter`, rotations).
    pub fn hub_auth(&self) -> crate::hub::AuthStatus {
        self.hubs.auth()
    }

    /// The credential refreshes sent to the hubs so far
    /// (`snodra_hub_client_credentials_refreshes_sent_total`).
    pub fn hub_credentials_refreshes_sent(&self) -> u64 {
        self.hubs.credentials_refreshes_sent()
    }

    /// True once a hub (with several: at least one, so requests can be sent) confirmed this
    /// instance's registration. snodra-messaging's `HubConnections.awaitRegistered` waits for
    /// every hub; that is [`await_all_hubs`](Self::await_all_hubs) here. False at once when the
    /// hub is turned off ([`Config::hub_enabled`](crate::Config::hub_enabled)).
    pub async fn await_hub(&self, timeout: Duration) -> bool {
        self.hubs.await_registered(timeout).await
    }

    /// True once every hub of `hub_address` confirmed this instance's registration (with
    /// `dns:///`: every address of the latest resolution; a second target of a hub already reached
    /// does not count). Use it when every hub must be able to route to this instance before
    /// going on, e.g. before taking traffic in a rollout. False at once when the hub is turned
    /// off.
    pub async fn await_all_hubs(&self, timeout: Duration) -> bool {
        self.hubs.await_all_registered(timeout).await
    }

    /// First registers no types with every hub and waits for their acks (at most
    /// [`DEREGISTER_TIMEOUT`]): each hub stops routing to this instance and hands the requests
    /// it queued for it to the other instances at once (snodra-messaging's `HubLifecycle.stop`).
    /// Then stops the saga relay (its sends in flight finish, bounded), the deadline scheduler
    /// and the gauge sampler, drains in-flight handlers (bounded; their replies still go
    /// through), completes the subscriptions served here with a `ReplyEnd` (best effort,
    /// bounded), closes every hub stream (each hub drops this instance at once), stops the
    /// outbox publisher and the consumer. Pending outgoing requests fail with
    /// `HubConnectionLost`.
    pub async fn shutdown(self) {
        if !self.hubs.deregister(DEREGISTER_TIMEOUT).await {
            tracing::debug!("not every hub confirmed the empty registration before shutdown");
        }
        // The saga relay and the deadline scheduler first, while the hub and the consumer still
        // run: the relay's sends in flight finish (or are abandoned after 15 s, their leases
        // expire and another instance takes the sagas over).
        self.saga_cancel.cancel();
        stop_saga_tasks(self.saga_tasks, SAGA_STOP_TIMEOUT).await;
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

/// Waits for the (cancelled) saga tasks to stop, all of them within one `bound` (not one each).
async fn stop_saga_tasks(tasks: Vec<JoinHandle<()>>, bound: Duration) {
    if tokio::time::timeout(bound, futures::future::join_all(tasks))
        .await
        .is_err()
    {
        tracing::warn!("the saga tasks did not all stop in {} s", bound.as_secs());
    }
}

impl SnodraMessagingBuilder {
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

    /// An event handler in the unnamed processor `default` (with the sagas): the subscription
    /// `<service>.default` and the markers `processor:default:<eventId>`.
    pub fn event<E, F, Fut>(self, handler: F) -> Self
    where
        E: Event,
        F: Fn(E, EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        self.event_with::<E, F, Fut>(EventOptions::new(), handler)
    }

    /// An event handler registered with `options`: its processor (`default` unless
    /// [`EventOptions::processor`], as [`event_in`](Self::event_in)) and whether it runs again for
    /// a record its processor replays after a reset ([`EventOptions::disallow_replay`], ADR 0023,
    /// Kotlin's `@DisallowReplay`).
    ///
    /// ```ignore
    /// builder.event_with::<OrderPlaced, _, _>(EventOptions::new().disallow_replay(), |e, ctx| async move { ... })
    /// ```
    pub fn event_with<E, F, Fut>(mut self, options: EventOptions, handler: F) -> Self
    where
        E: Event,
        F: Fn(E, EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        let registry = match options.processor_name() {
            None => &mut self.registry,
            Some(processor) => {
                match validate_processor_name(processor, &format!("event handler for {}", E::TYPE))
                {
                    Ok(()) => self.processors.entry(processor.to_string()).or_default(),
                    Err(e) => {
                        self.processor_errors.push(e);
                        return self;
                    }
                }
            }
        };
        if options.disallows_replay() {
            registry.event_disallowing_replay::<E, F, Fut>(handler);
        } else {
            registry.event::<E, F, Fut>(handler);
        }
        self
    }

    /// A reset handler of processor `processor` (`default`, or a name given to `event_in`; ADR
    /// 0023, Kotlin's `@ResetHandler`): runs once per reset that replays a range, on the instance
    /// executing it, before the markers and the positions change, with the target and the ranges.
    /// The handlers of a processor run in registration order; an error (or a panic) refuses the
    /// reset (`RESET_HANDLER_FAILED`) with nothing changed. A processor without event handlers
    /// here fails [`start`](Self::start).
    ///
    /// ```ignore
    /// builder.reset_handler("projections", |ctx: ResetContext| async move { clear_projection(&ctx).await })
    /// ```
    pub fn reset_handler<F, Fut>(mut self, processor: &str, handler: F) -> Self
    where
        F: Fn(ResetContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        let handler: ResetHandler = Arc::new(move |ctx| Box::pin(handler(ctx)));
        self.reset_handlers.push((processor.to_string(), handler));
        self
    }

    /// An event handler in the named processor `processor` (ADR 0022):
    /// its own subscription `<service>.<processor>` of the event store, its own markers
    /// (`processor:<processor>:<eventId>`), position, dead letters and failure domain, so a slow
    /// or failing handler there never holds the other processors. Stage 1 only: sagas run in
    /// `default`. The name matches `^[a-z0-9][a-z0-9-]{0,47}$` and is not `default`; otherwise
    /// [`start`](Self::start) fails naming the handler. One event type may be handled in several
    /// processors; each gets every record.
    ///
    /// ```ignore
    /// builder.event_in::<OrderPlaced, _, _>("projections", |e, ctx| async move { ... })
    /// ```
    pub fn event_in<E, F, Fut>(self, processor: &str, handler: F) -> Self
    where
        E: Event,
        F: Fn(E, EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MessagingError>> + Send + 'static,
    {
        self.event_with::<E, F, Fut>(EventOptions::new().processor(processor), handler)
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
    ///     // A deadline without a payload, or one whose payload the handler ignores, reads it as
    ///     // `IgnoredAny` (`()` cannot read a payload that is present).
    ///     .on_deadline::<serde::de::IgnoredAny>("charge-timeout", |s, _, ctx| { ctx.end(); Ok(()) })
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

    /// A partner source whose records become local snodra events (ADR 0021 ruling 20, spec
    /// "Inbound 1"), read through an event-store add-on's `provider` (the Kafka add-on's
    /// `KafkaExternalSource` for a partner's topic), whatever the service's own event store is:
    /// `translator` turns each record into an event (`Ok(None)` ignores it, an error retries it up
    /// to `SNODRA_CONSUMER_MAX_ATTEMPTS` times and then skips it, an `InvalidMessage` skips it at
    /// once: map a record that cannot be read to it), which is dispatched locally to this
    /// service's event handlers and sagas, never published to its events. The dedupe id is
    /// `<name>:<segment>:<position>`, or `<name>:<dedupeKey>` when the translation carries one.
    /// An empty or reserved name, or two sources of one name, fail [`start`](Self::start). See
    /// [`crate::external::sources`].
    ///
    /// ```ignore
    /// let partner = KafkaExternalSource::new("printer-banner", ExternalSource::new("partner.printer.banner"), "svc", "kafka:19092")?;
    /// builder.external_source("printer-banner", partner, |record: &ExternalRecord| {
    ///     // A record that is not a reply never will be: InvalidMessage skips it at once (a bare
    ///     // `?` would make it Serialization, retried up to the attempt limit first).
    ///     let reply: PrinterReply = serde_json::from_slice(&record.payload)
    ///         .map_err(|e| MessagingError::InvalidMessage(format!("not a printer reply: {e}")))?;
    ///     Ok(Some(TranslatedEvent::new(&BannerPrinted { job_id: reply.job_id }, Some(reply.event_id))?))
    /// })
    /// ```
    pub fn external_source<P, T>(mut self, name: &str, provider: P, translator: T) -> Self
    where
        P: ExternalSourceProvider + 'static,
        T: Fn(&ExternalRecord) -> Result<Option<TranslatedEvent>, MessagingError>
            + Send
            + Sync
            + 'static,
    {
        self.sources.push(SourceRegistration {
            name: name.to_string(),
            provider: Arc::new(provider),
            translator: Arc::new(translator),
        });
        self
    }

    /// The clock of the saga runtime, the relay, the deadline scheduler, the idempotent handlers
    /// and the webhook ingress (default [`Clock::system`]): what "now" is for deadlines, leases
    /// and backoffs. Tests move a [`ManualClock`](crate::saga::ManualClock).
    pub fn clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
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

    /// The message store (ADR 0028): the service's own database, through a store add-on (e.g.
    /// `snodra-messaging-mongo`'s `MongoStore`), where the outbox, the processed events, the dead
    /// letters, the processor control, the external calls and the sagas live; the event bus
    /// (also one taken before this call) appends to its outbox. Publishing an event, an event
    /// handler, a saga, an idempotent handler and a partner source need one
    /// ([`start`](Self::start) fails naming what needs it); commands and queries do not. The
    /// store's schema (its add-on's own: indexes, tables) is prepared by `start` unless
    /// `Config::store_init_schema` is false (`SNODRA_STORE_INIT_SCHEMA`), and the store stamps its
    /// own times with the builder's [`clock`](Self::clock).
    pub fn store(mut self, store: Arc<dyn MessageStore>) -> Self {
        self.events.bind(store.outbox());
        self.store = Some(store);
        self
    }

    /// The event store (ADR 0027): where the service's events go (the outbox relay publishes
    /// them there) and where its event processors read them, through an event-store add-on (e.g.
    /// `snodra-messaging-kafka`'s `KafkaEventStore`). An event handler and a saga need one
    /// ([`start`](Self::start) fails naming what needs it), and so does publishing an event;
    /// commands and queries do not.
    pub fn event_store(mut self, events: Arc<dyn EventStore>) -> Self {
        self.event_store = Some(events);
        self
    }

    /// Prepares the message store's schema (unless `Config::store_init_schema` is false), registers
    /// with the hub (in the background), starts the outbox publisher and the event processors.
    /// Does not wait for the hub: see [`SnodraMessaging::await_hub`].
    pub async fn start(self) -> Result<SnodraMessaging, MessagingError> {
        let SnodraMessagingBuilder {
            config,
            registry,
            store,
            event_store,
            events,
            hubs,
            mut inbound,
            sender,
            emitter,
            sagas,
            external_calls,
            sources,
            stats,
            clock,
            metrics,
            processors: named_registries,
            processor_errors,
            reset_handlers,
        } = self;
        // Definition errors first: nothing is connected or created for a saga that cannot run.
        if let Some(e) = processor_errors.into_iter().next() {
            return Err(e);
        }
        let saga_types = validate_registrations(sagas)?;
        for source in &sources {
            validate_source(&source.name)?;
        }
        if store.is_none() {
            needs_store(&registry, &named_registries, &saga_types, &sources)?;
        }
        match &event_store {
            None => {
                needs_event_store(&registry, &named_registries, &saga_types)?;
                // Nothing would relay the outbox: publishing fails, naming the missing add-on.
                events.without_event_store();
            }
            Some(events) => check_dead_letter_mode(config.dead_letter_mode, &**events)?,
        }
        if !config.sagas_enabled && !saga_types.is_empty() {
            tracing::warn!(sagas = ?saga_types.iter().map(SagaType::name).collect::<Vec<_>>(), "sagas are registered but SNODRA_SAGAS_ENABLED=false: they do not run");
        }
        let saga_types = if config.sagas_enabled {
            saga_types
        } else {
            Vec::new()
        };
        // The in-memory stores lose everything when the process ends: say so (ADR 0028), named
        // by their reserved name, whatever the profile.
        if store
            .as_ref()
            .is_some_and(|s| s.name() == crate::store::memory::NAME)
        {
            tracing::warn!("in-memory message store: not durable");
        }
        if event_store
            .as_ref()
            .is_some_and(|e| e.name() == crate::event_store::memory::NAME)
        {
            tracing::warn!("in-memory event store: not durable");
        }
        // The store stamps its own times with the application's clock, and gets its schema.
        if let Some(store) = &store {
            store.use_clock(clock.clone());
            if config.store_init_schema {
                store.init_schema(&config).await?;
            } else {
                store.check_schema().await?;
            }
        }
        let saga_metrics: Arc<dyn SagaMetrics> =
            Arc::new(OtelSagaMetrics::new(&metrics, stats.clone()));
        if let Some(store) = store
            .as_ref()
            .filter(|_| !registry.idempotent_handlers().is_empty())
        {
            external_calls.bind(ExternalCalls::new(
                store.external_calls(),
                clock.clone(),
                config.external_call_settings(),
                Arc::clone(&saga_metrics),
            )?);
        }
        let registry = Arc::new(registry);
        // The saga runtime only when a saga is registered (and enabled); it is the second stage of
        // every local dispatch.
        let runtime = match &store {
            Some(store) if !saga_types.is_empty() => {
                stats.register_saga_types(saga_types.iter().map(SagaType::name));
                Some(Arc::new(SagaRuntime::new(
                    saga_types,
                    SagaPorts::of(&**store),
                    clock.clone(),
                    config.saga_retry(),
                    Arc::clone(&saga_metrics),
                )))
            }
            _ => None,
        };
        let stage: Arc<dyn SagaStage> = match &runtime {
            Some(runtime) => runtime.clone(),
            None => Arc::new(NoSagas),
        };
        // The named event processors' dispatchers (ADR 0022): their own handlers and marker
        // family, no sagas.
        let named: Vec<(String, Arc<LocalEventDispatcher>)> = named_registries
            .into_iter()
            .map(|(name, handlers)| {
                let dispatcher =
                    LocalEventDispatcher::new(store.clone(), Arc::new(handlers), Arc::new(NoSagas))
                        .with_marker_prefix(marker_prefix(&name));
                (name, Arc::new(dispatcher))
            })
            .collect();
        // One local dispatch for every entry point: the processor `default`, the partner
        // sources, the webhook ingress and the relay's failure notices (the ordinary handlers, then
        // the sagas). Outside the consumer it also runs every named processor's handlers.
        let local = Arc::new(
            LocalEventDispatcher::new(store.clone(), Arc::clone(&registry), stage)
                .with_fan_out(named.iter().map(|(_, d)| Arc::clone(d)).collect()),
        );
        let ingress = ExternalEventIngress::new(Arc::clone(&local), store.clone(), clock.clone());
        let verifier = match (&config.jwks_url, &config.issuer) {
            (Some(url), Some(issuer)) => Some(JwksVerifier::new(url.clone(), issuer.clone())),
            _ => None,
        };
        let dispatcher = Arc::new(
            Dispatcher::new(
                Arc::clone(&registry),
                Arc::new(
                    IdentityResolver::new(verifier).with_roles_claim(&config.security_roles_claim),
                ),
                config.max_concurrent_handlers,
            )
            .stream_credit_timeout(config.hub_stream_credit_timeout)
            .with_emitter(emitter.clone()),
        );
        let cancel = CancellationToken::new();
        let handlers = TaskTracker::new();
        let mut tasks = Vec::new();
        // What the store runs on its own (e.g. the SQL add-on's purge), until the shutdown.
        if let Some(background) = store
            .as_ref()
            .and_then(|store| store.background(&config, cancel.clone()))
        {
            tasks.push(tokio::spawn(background));
        }

        // The event processors (ADR 0022): `default` (the ordinary handlers and the sagas; the
        // consumer also runs for a service whose events only sagas handle), then the named ones.
        let mut planned: Vec<(String, Arc<LocalEventDispatcher>)> = Vec::new();
        if !registry.event_types().is_empty() || runtime.is_some() {
            planned.push((DEFAULT_PROCESSOR.to_string(), Arc::clone(&local)));
        }
        planned.extend(named);
        if let Some((processor, _)) = reset_handlers
            .iter()
            .find(|(p, _)| !planned.iter().any(|(name, _)| name == p))
        {
            return Err(MessagingError::InvalidMessage(format!(
                "reset handler of processor '{processor}': the processor has no event handler in this service"
            )));
        }
        // The admin API serves every processor with handlers here, whether or not this instance
        // consumes (`SNODRA_CONSUMER_ENABLED=false`).
        let dead_letters = DeadLetters::new(
            store.as_ref().map(|s| s.dead_letters()),
            config.dead_letter_mode,
            planned.clone(),
        );
        // Each processor's consumer runs under its control document (ADR 0023).
        let mut runners = Vec::new();
        // A processor exists only with event handlers or sagas, which have both stores.
        if let (Some(store), Some(events)) = (
            store.as_ref().filter(|_| config.consumer_enabled),
            event_store.as_ref(),
        ) {
            for (name, dispatcher) in planned {
                let handlers = reset_handlers
                    .iter()
                    .filter(|(p, _)| *p == name)
                    .map(|(_, h)| Arc::clone(h))
                    .collect();
                let runner = ProcessorRunner::new(
                    ProcessorSettings {
                        subscription: SubscriptionSpec {
                            name: subscription_name(&config.service_name, &name),
                            source: config.events_source.clone(),
                        },
                        name,
                        events: Arc::clone(events),
                        max_attempts: config.consumer_max_attempts,
                        backoff: Duration::from_secs(1),
                        dead_letter_mode: config.dead_letter_mode,
                        dead_letter_retention: config.dead_letter_retention,
                        dead_letters: (config.dead_letter_mode == DeadLetterMode::Collection)
                            .then(|| store.dead_letters()),
                        metrics: metrics.clone(),
                    },
                    dispatcher,
                    store.processor_control(),
                    store.processed_events(),
                    handlers,
                    cancel.clone(),
                );
                runners.push(runner);
            }
        }
        let monitors: Vec<Arc<ProcessorMonitor>> = runners.iter().map(|r| r.monitor()).collect();
        ProcessorMonitor::observe(&monitors, &metrics);
        let processor_control = ProcessorControl::new(runners.clone(), dead_letters.clone());
        let reported = monitors.clone();
        hubs.set_processor_status(Arc::new(move || {
            reported.iter().map(|m| m.status()).collect()
        }));

        hubs.set_registered_types(registry.registered_types());
        if !config.hub_enabled {
            hubs.turn_off();
            tracing::info!("the hub is turned off (SNODRA_SERVER_ENABLED=false): not connecting");
        }
        hubs.start();
        let loop_sender = Arc::clone(&sender);
        let loop_cancel = cancel.clone();
        let loop_handlers = handlers.clone();
        let loop_dispatcher = Arc::clone(&dispatcher);
        let loop_control = processor_control.clone();
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
                    // Off the read loop and this one (ADR 0023): a reset may take minutes. The
                    // answer goes back on the stream the request arrived on.
                    Inbound::Control { request, reply_to } => {
                        let control = loop_control.clone();
                        tokio::spawn(async move {
                            let reply = control.handle(request).await;
                            let control_id = reply.control_id.clone();
                            if let Err(e) = reply_to
                                .send(crate::proto::ClientMessage {
                                    message: Some(crate::proto::client_message::Message::ControlReply(reply)),
                                })
                                .await
                            {
                                tracing::warn!(control_id = %control_id, stream = reply_to.stream(), "control reply dropped: {e}");
                            }
                        });
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

        if let (Some(store), Some(events)) = (
            store.as_ref().filter(|_| config.outbox_enabled),
            event_store.as_ref(),
        ) {
            let publisher = OutboxPublisher::new(
                store.outbox(),
                Arc::clone(events),
                OutboxPublisherSettings::from(&config),
                clock.clone(),
                &metrics,
            )?;
            tasks.push(publisher.start(cancel.clone()));
        }
        for runner in &runners {
            tasks.push(runner.start());
        }
        // The relay, the deadline scheduler and the gauge sampler stop on their own token, before
        // the consumer and the hub streams (`shutdown`).
        let saga_cancel = CancellationToken::new();
        let mut saga_tasks = Vec::new();
        if let (Some(runtime), Some(store)) = (&runtime, &store) {
            let sagas = store.sagas();
            let commands: Arc<dyn RawCommandSender> = Arc::new(LocalFirstSender::new(
                Arc::clone(&registry),
                Arc::clone(&sender),
            ));
            let relay = SagaDispatchRelay::new(
                store.tx(),
                Arc::clone(&sagas.dispatch),
                commands,
                events.clone(),
                Arc::clone(&local),
                clock.clone(),
                config.relay_settings(),
                Arc::clone(&saga_metrics),
            )?;
            saga_tasks.push(relay.start(saga_cancel.clone()));
            let scheduler = DeadlineScheduler::new(
                Arc::clone(&sagas.deadlines),
                Arc::clone(runtime),
                clock.clone(),
                config.deadline_settings(),
                Arc::clone(&saga_metrics),
            )?;
            saga_tasks.push(scheduler.start(saga_cancel.clone()));
            saga_tasks.push(stats.start_sampler(
                sagas.sagas,
                sagas.dispatch,
                SAMPLE_INTERVAL,
                saga_cancel.clone(),
            ));
        }
        if !sources.is_empty() {
            let sources = ExternalSources::new(
                sources,
                Arc::clone(&local),
                config.consumer_max_attempts,
                Duration::from_secs(1),
            )?;
            tasks.push(sources.start(cancel.clone()));
        }
        let hub_targets = if config.hub_enabled {
            format!("{:?}", hubs.targets())
        } else {
            "off".to_string()
        };
        tracing::info!(service = %config.service_name, instance = %hubs.instance_id(), hubs = %hub_targets, commands = ?registry.command_types(), queries = ?registry.exclusive_query_types(), shared_queries = ?registry.shared_query_types(), streaming_queries = ?registry.streaming_query_types(), subscription_queries = ?registry.subscription_query_types(), events = ?registry.event_types(), sagas = ?runtime.as_ref().map(|r| r.saga_types()).unwrap_or_default(), idempotent = ?registry.idempotent_handlers(), processors = ?monitors.iter().map(|m| m.name().to_string()).collect::<Vec<_>>(), dead_letter_mode = config.dead_letter_mode.as_str(), store = store.as_ref().map_or("none", |s| s.name()), event_store = event_store.as_ref().map_or("none", |e| e.name()), "snodra messaging started");
        Ok(SnodraMessaging {
            hubs,
            sender,
            dispatcher,
            emitter,
            events,
            store,
            cancel,
            tasks,
            handlers,
            ingress,
            stats,
            saga_metrics,
            runtime,
            saga_cancel,
            saga_tasks,
            clock,
            monitors,
            dead_letters,
            processor_control,
        })
    }
}

/// What the missing-store messages add: the in-memory store and what it is for.
const MEMORY_STORE_HINT: &str =
    "store::memory::MemoryStore runs the in-memory one, not durable: tests and local runs";
/// What the missing-event-store messages add.
const MEMORY_EVENT_STORE_HINT: &str = "event_store::memory::MemoryEventStore runs the in-memory one, not durable: tests and local runs";

/// Without a store: the first registration that needs one, as the error naming it (ADR 0028:
/// commands, queries and subscription queries need none).
fn needs_store(
    registry: &HandlerRegistry,
    named: &std::collections::BTreeMap<String, HandlerRegistry>,
    sagas: &[SagaType],
    sources: &[SourceRegistration],
) -> Result<(), MessagingError> {
    let needs = |what: String| {
        Err(MessagingError::InvalidMessage(format!(
            "{what} needs a message store: add .store(...) ({MEMORY_STORE_HINT})"
        )))
    };
    if let Some(t) = registry.event_types().first() {
        return needs(format!("the event handler for {t}"));
    }
    if let Some((processor, t)) = named
        .iter()
        .find_map(|(p, r)| r.event_types().first().map(|t| (p.clone(), t.clone())))
    {
        return needs(format!(
            "the event handler for {t} in processor '{processor}'"
        ));
    }
    if let Some(saga) = sagas.first() {
        return needs(format!("saga '{}'", saga.name()));
    }
    if let Some(label) = registry.idempotent_handlers().first() {
        return needs(format!("the idempotent handler {label}"));
    }
    if let Some(source) = sources.first() {
        return needs(format!("external source '{}'", source.name));
    }
    Ok(())
}

/// Without an event store: the first registration that needs one, as the error naming it (ADR
/// 0027: commands, queries and subscription queries need none).
fn needs_event_store(
    registry: &HandlerRegistry,
    named: &std::collections::BTreeMap<String, HandlerRegistry>,
    sagas: &[SagaType],
) -> Result<(), MessagingError> {
    let needs = |what: String| {
        Err(MessagingError::InvalidMessage(format!(
            "{what} needs an event store: add .event_store(...) ({MEMORY_EVENT_STORE_HINT})"
        )))
    };
    if let Some(t) = registry.event_types().first() {
        return needs(format!("the event handler for {t}"));
    }
    if let Some((processor, t)) = named
        .iter()
        .find_map(|(p, r)| r.event_types().first().map(|t| (p.clone(), t.clone())))
    {
        return needs(format!(
            "the event handler for {t} in processor '{processor}'"
        ));
    }
    if let Some(saga) = sagas.first() {
        return needs(format!("saga '{}'", saga.name()));
    }
    Ok(())
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
        let mut config = Config::new("svc", "static://hub:7000");
        assert!(hub_settings(&config).unwrap().token.is_none());
        assert!(hub_settings(&config).unwrap().tls.is_none());
        assert!(hub_settings(&config).unwrap().token_provider.is_none());
        config.hub_token = Some(SecretString::new("tok"));
        config.hub_token_file = Some("/run/secrets/hub-token".into());
        config.hub_tls = Some(HubTls {
            server_name: Some("snodra-server".into()),
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
        let mut config = Config::new("svc", "a:7000,b:7000");
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

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn stuck_saga_tasks_hold_a_shutdown_for_one_bound_in_all_not_one_each() {
        // The relay, the scheduler and the sampler, none of which stops.
        let tasks = (0..3)
            .map(|_| tokio::spawn(std::future::pending::<()>()))
            .collect();
        let started = tokio::time::Instant::now();

        stop_saga_tasks(tasks, SAGA_STOP_TIMEOUT).await;

        assert_eq!(
            started.elapsed(),
            SAGA_STOP_TIMEOUT,
            "one shared deadline (ruling 64)"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn saga_tasks_that_stop_end_the_wait_at_once() {
        let tasks = (0..3).map(|_| tokio::spawn(async {})).collect();
        let started = tokio::time::Instant::now();

        stop_saga_tasks(tasks, SAGA_STOP_TIMEOUT).await;

        assert_eq!(started.elapsed(), Duration::ZERO);
    }
}
