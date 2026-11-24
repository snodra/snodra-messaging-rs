//! Connections to every hub (multi-hub high availability).
//!
//! The hubs never talk to each other; availability comes from every instance registering on every
//! hub. A [`HubPool`] holds one [`HubConnection`] per target of `hub_address` ([`HubTargets`]),
//! each with its own handshake, heartbeat and reconnect backoff, all sending the identical
//! `Register`, and all feeding one inbound channel (replies go on the arrival stream).
//!
//! - **Dedupe.** Two targets whose acks carry the same `hub_id` are one hub: the connection
//!   acknowledged first owns the id for as long as it is kept (live or reconnecting), the other is
//!   closed and parked (logged once): no stream, out of the routing, the status reports and the
//!   permit split. A hub has no say in this: its second `Register` already replaced the first
//!   stream's session, so that stream is ended by the hub and reconnects, once. A parked target is
//!   connected again once its owner is gone: its address is no longer resolved (it drains), or it
//!   answers another hub id (also when that makes the owner itself a parked duplicate of another
//!   id). With `dns:///` a parked target that is no longer resolved is dropped. An empty
//!   `RegisterAck.hub_id` (a real hub always names itself) stands for the target as `host:port`
//!   (prefix stripped).
//! - **Routing.** An unkeyed request goes round-robin over the hubs whose stream is handshaken
//!   (reconnecting ones are skipped); a keyed one to the live hub with the highest
//!   [`rendezvous_score`](super::rendezvous_score) for `(hub_id, routing key)`. A request that
//!   could not be written to the chosen hub's stream (it dropped meanwhile) goes to the next hub in
//!   that order with the same request id: it was never sent. One already written fails with
//!   `HubConnectionLost` when its stream drops (the caller's retry policy decides). While no hub
//!   can take a request (none is live yet, a hub is restarting) the call waits for one until its
//!   deadline: its timeout (the caller's, else the configured default), else 5 s, from the moment
//!   the call began; then it fails as never sent (`HubUnavailable`). No new hop starts after the
//!   deadline. A pool turned off fails at once.
//! - **Permits.** A stream offers `ceil(max_concurrent_handlers / N)` permits
//!   ([`permit_share`]), N being the hubs live when its `Register` is written, itself included
//!   (also a hub reconnecting for a client certificate rollover);
//!   until a hub's first connect attempt is over it counts as live, so hubs that come up together
//!   split from the start. When N shrinks (a hub is lost, closed as a duplicate or leaves DNS),
//!   every live stream is topped up to `ceil(max / N)` with one `Permits` message whose `grant`
//!   (`ceil(max / N)`) raises the hub's permit cap on the session, so the survivors can run the
//!   share of the hub that left; a stream that already offered that many
//!   gets nothing. Permits are never revoked when N grows. The local concurrency guard stays the
//!   total.
//! - **Status.** Every `status_interval`, and at once whenever the set of live hubs changes (a
//!   stream acknowledged or lost, an address leaving DNS), an `InstanceStatus` (acknowledged hub
//!   ids, handlers in flight, the event processors' status) goes to every live hub.
//! - **DNS.** A `dns:///name:port` target is resolved at start and every `dns_refresh` into
//!   `ip:port` targets of one family (IPv4 when any, else IPv6, [`resolved_targets`]): a new
//!   address gets a connection; a vanished one leaves the routing at once, gets an empty
//!   `Register` (the hub stops routing to the instance there) and is closed once idle (no request
//!   of ours written to it unanswered, no handler of its requests running), at most
//!   [`DRAIN_LIMIT`] later. A failed or empty resolution keeps the current hubs.
//! - **Client certificate rotation** (mutual TLS). With `hub_tls.client_cert_file` the pool
//!   reads the files every `HubSettings::certificate_check_interval` (3 s). When they hold a valid pair (not expired, and past its notBefore) that a live
//!   connection is not presenting (the content changed), it waits a random delay (uniform in
//!   `[0, min(certificate_roll_delay_max (30 s), 10 % of the presented certificate's remaining
//!   validity)]`, once per pair, so replicas that see the renewal together do not all leave the
//!   routing at once), reads the files again, and rolls the connections over one hub at a time,
//!   in target order: the hub is told the instance is stopping (a `Register` with no types; a hub
//!   that has another live hub is left out of the routing at once, the only hub once its handlers
//!   have finished), the pool waits (at most [`ROLL_IDLE_LIMIT`]) for the requests written to it
//!   and the handlers of its requests to finish (the streaming queries and subscriptions this
//!   instance itself consumes are not waited for: they end when their owners decide, and the roll
//!   cuts them, the consumer sees the loss), closes the stream gracefully (half-close, the hub
//!   gets [`SHUTDOWN_CLOSE_TIMEOUT`](super::connection::SHUTDOWN_CLOSE_TIMEOUT) to finish, and
//!   what it sends meanwhile, late replies included, is still handled), and the connection
//!   reconnects with the new pair; only when it is registered again (at most
//!   [`ROLL_RECONNECT_LIMIT`]) is the next hub rolled. While no hub is routable (a single hub
//!   being rolled), a send waits for one within its own deadline, as it always does. A pair
//!   that does not load (a rotation caught half way, a key that is not the certificate's, a
//!   certificate for another service, an expired one) leaves every connection as it is and is
//!   logged at `error` once per distinct problem (when it persists for a second period); one
//!   that is not valid yet is waited for. A hub that does not take the new pair back in time
//!   stops the rollover (logged at `error`, the other hubs keep what they have). Requests
//!   already written to a hub when its stream is closed fail like on any lost stream. A
//!   certificate with less than a tenth of its lifetime (at most 24 h) left, and no renewed file,
//!   is warned about once.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use futures::FutureExt;
use opentelemetry::KeyValue;
use opentelemetry::metrics::AsyncInstrument;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::MessagingError;
use crate::metrics::{
    HUB_CLIENT_CERTIFICATE_ROTATIONS, HUB_CLIENT_CONNECTED_HUBS,
    HUB_CLIENT_CREDENTIALS_REFRESHES_SENT, HUB_CLIENT_HANDLERS_IN_FLIGHT, HUB_CLIENT_TOKEN_FETCHES,
    Metrics,
};
use crate::proto::{ClientMessage, InstanceStatus, ProcessorStatus, client_message};
use crate::retry::random_unit;
use crate::security::HubTls;

use super::auth::{AuthKind, AuthStatus};
use super::connection::{
    Hooks, HubConnection, HubRejection, HubSettings, Inbound, RegisteredTypes, Sequenced, StreamId,
    hub_id,
};
use super::mtls::{self, ClientIdentity};
use super::targets::{HubTargets, permit_share, rank_keyed, rank_round_robin, resolved_targets};
use crate::proto::RegisterAck;

/// The longest a vanished DNS address stays open to finish its requests.
pub const DRAIN_LIMIT: Duration = Duration::from_secs(65);
/// The longest a certificate rollover waits for the requests of a hub's stream to finish before
/// closing it.
pub const ROLL_IDLE_LIMIT: Duration = Duration::from_secs(10);
/// The longest a certificate rollover waits for a hub's connection to register again with the new
/// pair before it gives up on the remaining hubs.
pub const ROLL_RECONNECT_LIMIT: Duration = Duration::from_secs(30);
/// A request a sender had already routed to a hub when it was taken out of the routing is written
/// within this; the rollover looks for unanswered requests only after it.
const ROLL_SETTLE: Duration = Duration::from_millis(50);
/// How long one `InstanceStatus` write may wait for room on a stream.
const STATUS_WRITE_TIMEOUT: Duration = Duration::from_secs(1);
const DRAIN_POLL: Duration = Duration::from_millis(100);

/// Resolves `(host, port)` to socket addresses (the system resolver unless replaced in tests).
pub type Resolver = Arc<
    dyn Fn(String, u16) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<SocketAddr>>> + Send>>
        + Send
        + Sync,
>;

/// Picks the random delay before a rollover from the bound it may not exceed (tests replace it).
pub type RollDelaySource = Arc<dyn Fn(Duration) -> Duration + Send + Sync>;

/// Tells whether requests written to a stream are still unanswered (registered by a [`super::Sender`]).
type PendingProbe = Box<dyn Fn(StreamId, Consumers) -> bool + Send + Sync>;

/// Whether the requests whose results this instance itself consumes (streaming queries and
/// subscriptions: they end when their owners decide, not when the work is done) count as work
/// to wait for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Consumers {
    Count,
    Ignore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Active,
    /// Its hub is reached by another connection: closed and parked ([`Member::parked`]), connected
    /// again (a new member at its address) once that connection is gone.
    Duplicate,
    /// Its address vanished from DNS; out of the routing, closed once idle.
    Draining,
    /// Being rolled over to a renewed client certificate (another hub serves meanwhile); out of
    /// the routing until its stream is back.
    Rolling,
}

struct Member {
    conn: Arc<HubConnection>,
    role: Mutex<Role>,
    /// A [`Role::Duplicate`]: the hub id it answered, owned by another connection.
    parked: Mutex<Option<String>>,
}

impl Member {
    fn new(conn: Arc<HubConnection>) -> Member {
        Member {
            conn,
            role: Mutex::new(Role::Active),
            parked: Mutex::new(None),
        }
    }

    /// Closed and parked behind `hub_id`, which another connection owns.
    fn park(&self, hub_id: String) {
        *self.parked.lock().unwrap() = Some(hub_id);
        self.set_role(Role::Duplicate);
    }

    /// The hub id a parked member waits for; `None` unless it is parked.
    fn parked_behind(&self) -> Option<String> {
        if self.role() != Role::Duplicate {
            return None;
        }
        self.parked.lock().unwrap().clone()
    }

    /// Kept by the pool: routed to (or about to be again), owning the hub id it answered.
    fn kept(&self) -> bool {
        matches!(self.role(), Role::Active | Role::Rolling)
    }

    fn role(&self) -> Role {
        *self.role.lock().unwrap()
    }

    fn set_role(&self, role: Role) {
        *self.role.lock().unwrap() = role;
    }

    /// Sets `to` only while the role is `from`; true when it was.
    fn change_role(&self, from: Role, to: Role) -> bool {
        let mut role = self.role.lock().unwrap();
        if *role == from {
            *role = to;
            true
        } else {
            false
        }
    }

    fn address(&self) -> &str {
        self.conn.address()
    }
}

/// A live hub at one moment: its connection, id and ack.
struct Live {
    conn: Arc<HubConnection>,
    hub_id: String,
    ack: RegisterAck,
}

/// What the certificate watch has seen and said (see the module docs).
#[derive(Default)]
struct CertificateWatch {
    /// The problem with the files seen on the last period, and whether it was logged.
    problem: Option<(String, bool)>,
    /// The certificate (fingerprint) warned about as near its end.
    warned: Option<String>,
    /// The digest of the pair a hub did not take back in time: not tried again until it changes.
    refused: Option<[u8; 32]>,
    /// The digest of the pair that is not valid yet (logged once).
    waiting: Option<[u8; 32]>,
    /// The digest of the pair the random delay before a roll was waited for.
    delayed: Option<[u8; 32]>,
    /// The failure of the last tick that failed as a whole (logged once).
    tick_failure: Option<String>,
}

/// What the status tick reads the event processors' status from: an in-memory
/// snapshot, never a Kafka or Mongo call.
pub type ProcessorStatusSource = Arc<dyn Fn() -> Vec<ProcessorStatus> + Send + Sync>;

pub struct HubPool {
    settings: HubSettings,
    targets: HubTargets,
    service_name: String,
    instance_id: String,
    inbound: mpsc::Sender<Inbound>,
    types: Mutex<RegisteredTypes>,
    members: Mutex<Vec<Arc<Member>>>,
    /// hub id → the address of the connection that reaches it.
    owners: Mutex<HashMap<String, String>>,
    turn: AtomicUsize,
    in_flight: Arc<AtomicUsize>,
    /// Credential refreshes written to any hub (monotonic: members come and go).
    refreshes_sent: Arc<AtomicU64>,
    /// The newest refusal by any hub (kept: members come and go).
    latest_rejection: Arc<Mutex<Option<Sequenced>>>,
    changes: watch::Sender<u64>,
    /// The client certificate watch (mTLS only).
    certificate: Mutex<CertificateWatch>,
    probes: Mutex<Vec<PendingProbe>>,
    resolver: Mutex<Resolver>,
    roll_delay: Mutex<RollDelaySource>,
    /// The event processors' status, for the hubs that share `processors`.
    processor_status: Mutex<Option<ProcessorStatusSource>>,
    /// Built around one connection the caller owns ([`HubPool::of`]): no tasks of its own.
    external: bool,
    /// Turned off ([`turn_off`](Self::turn_off), `Config::hub_enabled` false): never connects.
    off: AtomicBool,
    started: AtomicBool,
    cancel: CancellationToken,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl HubPool {
    /// One connection per target of `settings.address` (nothing starts yet). `InvalidMessage`
    /// when the address names no hub or is malformed ([`HubTargets::parse`]), or when
    /// `settings.max_concurrent_handlers` is 0.
    pub fn new(
        settings: HubSettings,
        service_name: String,
        instance_id: String,
        inbound: mpsc::Sender<Inbound>,
    ) -> Result<Arc<HubPool>, MessagingError> {
        settings.check()?;
        let targets = HubTargets::parse(&settings.address)?;
        let pool = Arc::new(Self::empty(
            settings,
            targets.clone(),
            service_name,
            instance_id,
            inbound,
            false,
        ));
        if let HubTargets::Static(addresses) = targets {
            for address in addresses {
                pool.add(address);
            }
        }
        Ok(pool)
    }

    /// A pool of exactly `conn`, which the caller starts and shuts down.
    pub fn of(conn: Arc<HubConnection>) -> Arc<HubPool> {
        let (tx, _) = mpsc::channel(1);
        let settings = HubSettings {
            address: conn.address().to_string(),
            ..HubSettings::default()
        };
        let pool = Self::empty(
            settings,
            HubTargets::Static(vec![conn.address().to_string()]),
            String::new(),
            conn.instance_id().to_string(),
            tx,
            true,
        );
        pool.members
            .lock()
            .unwrap()
            .push(Arc::new(Member::new(conn)));
        Arc::new(pool)
    }

    fn empty(
        settings: HubSettings,
        targets: HubTargets,
        service_name: String,
        instance_id: String,
        inbound: mpsc::Sender<Inbound>,
        external: bool,
    ) -> HubPool {
        let (changes, _) = watch::channel(0);
        HubPool {
            settings,
            targets,
            service_name,
            instance_id,
            inbound,
            types: Mutex::new(RegisteredTypes::default()),
            members: Mutex::new(vec![]),
            owners: Mutex::new(HashMap::new()),
            turn: AtomicUsize::new(0),
            in_flight: Arc::new(AtomicUsize::new(0)),
            refreshes_sent: Arc::new(AtomicU64::new(0)),
            latest_rejection: Arc::new(Mutex::new(None)),
            changes,
            certificate: Mutex::new(CertificateWatch::default()),
            probes: Mutex::new(vec![]),
            resolver: Mutex::new(Arc::new(|host, port| {
                Box::pin(async move {
                    tokio::net::lookup_host((host.as_str(), port))
                        .await
                        .map(|addrs| addrs.collect())
                })
            })),
            roll_delay: Mutex::new(Arc::new(|bound| roll_delay(bound, random_unit()))),
            processor_status: Mutex::new(None),
            external,
            off: AtomicBool::new(false),
            started: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            tasks: Mutex::new(vec![]),
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn targets(&self) -> &HubTargets {
        &self.targets
    }

    /// The command and exclusive query types announced in every `Register` on every hub, no
    /// shared query types; set before [`start`](Self::start).
    pub fn set_types(&self, command_types: Vec<String>, query_types: Vec<String>) {
        self.set_registered_types(RegisteredTypes {
            commands: command_types,
            queries: query_types,
            ..RegisteredTypes::default()
        });
    }

    /// The types announced in every `Register` on every hub; set before [`start`](Self::start).
    pub fn set_registered_types(&self, types: RegisteredTypes) {
        *self.types.lock().unwrap() = types.clone();
        for m in self.members() {
            m.conn.set_registered_types(types.clone());
        }
    }

    /// Where every status tick reads the event processors' status. Without one (the
    /// default) the field is always empty.
    pub fn set_processor_status(&self, source: ProcessorStatusSource) {
        *self.processor_status.lock().unwrap() = Some(source);
    }

    /// Replaces the resolver of a `dns:///` target (tests); set before [`start`](Self::start).
    #[doc(hidden)]
    pub fn set_resolver(&self, resolver: Resolver) {
        *self.resolver.lock().unwrap() = resolver;
    }

    /// Replaces how the random delay before a certificate rollover is picked from its bound
    /// (tests); set before [`start`](Self::start).
    #[doc(hidden)]
    pub fn set_roll_delay_source(&self, source: RollDelaySource) {
        *self.roll_delay.lock().unwrap() = source;
    }

    /// Turns the pool off before [`start`](Self::start): it never connects, and a send fails at
    /// once with `HubUnavailable` naming `SNODRA_SERVER_ENABLED`, also one already waiting for a
    /// hub.
    pub fn turn_off(&self) {
        self.off.store(true, Ordering::SeqCst);
        self.changes.send_modify(|n| *n += 1);
    }

    /// What a send fails with when no hub took it.
    fn unavailable(&self) -> MessagingError {
        MessagingError::HubUnavailable(if self.off.load(Ordering::SeqCst) {
            "the hub is turned off (SNODRA_SERVER_ENABLED=false, Config::hub_enabled): nothing is \
             sent to other services"
                .into()
        } else {
            "not connected to the hub".into()
        })
    }

    /// Starts every connection, the status reports and (for `dns:///`) the resolution loop;
    /// nothing when turned off.
    pub fn start(self: &Arc<Self>) {
        if self.external
            || self.off.load(Ordering::SeqCst)
            || self.started.swap(true, Ordering::SeqCst)
        {
            return;
        }
        for m in self.members() {
            m.conn.start();
        }
        let mut tasks = self.tasks.lock().unwrap();
        let me = Arc::clone(self);
        tasks.push(tokio::spawn(async move { me.status_loop().await }));
        if let HubTargets::Dns { host, port } = self.targets.clone() {
            let me = Arc::clone(self);
            tasks.push(tokio::spawn(
                async move { me.resolve_loop(host, port).await },
            ));
        }
        if self
            .settings
            .tls
            .as_ref()
            .is_some_and(HubTls::has_client_certificate)
        {
            let me = Arc::clone(self);
            tasks.push(tokio::spawn(async move { me.certificate_loop().await }));
        }
    }

    /// True once at least one hub confirmed the registration (the client can send); see
    /// [`await_all_registered`](Self::await_all_registered) for every hub. False at once when the
    /// pool is turned off ([`turn_off`](Self::turn_off)): no hub ever will.
    pub async fn await_registered(&self, timeout: Duration) -> bool {
        if self.off.load(Ordering::SeqCst) {
            return false;
        }
        let mut changes = self.changes.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if !self.live().is_empty() {
                return true;
            }
            tokio::select! {
                _ = changes.changed() => {}
                // A pool of one external connection hears of no change: poll.
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                _ = tokio::time::sleep_until(deadline) => return !self.live().is_empty(),
            }
        }
    }

    /// True once every hub of the pool confirmed the registration: there is at least one connection, and every connection still in use
    /// (not a closed duplicate, not draining) has a handshaken stream. With `dns:///`, the hubs of
    /// the latest resolution. False at once when the pool is turned off.
    pub async fn await_all_registered(&self, timeout: Duration) -> bool {
        if self.off.load(Ordering::SeqCst) {
            return false;
        }
        let mut changes = self.changes.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.all_registered() {
                return true;
            }
            tokio::select! {
                _ = changes.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                _ = tokio::time::sleep_until(deadline) => return self.all_registered(),
            }
        }
    }

    fn all_registered(&self) -> bool {
        let active: Vec<_> = self
            .members()
            .into_iter()
            .filter(|m| matches!(m.role(), Role::Active | Role::Rolling))
            .collect();
        !active.is_empty()
            && active
                .iter()
                .all(|m| m.role() == Role::Active && m.conn.handshake().is_some())
    }

    /// The ids of the hubs live now (handshaken streams), in target order; an empty
    /// `RegisterAck.hub_id` by its target as `host:port`.
    pub fn hub_ids(&self) -> Vec<String> {
        self.live().into_iter().map(|l| l.hub_id).collect()
    }

    /// How the pool authenticates: the kind, and for a JWT the expiry of the current
    /// access token and the last token fetch error. The same for every hub.
    pub fn auth(&self) -> AuthStatus {
        match self.members().first() {
            Some(m) => m.conn.auth(),
            None => HubConnection::auth_of(&self.settings),
        }
    }

    /// The credential refreshes (`ClientMessage.credentials`) written to any hub so far
    /// (`snodra_hub_client_credentials_refreshes_sent_total`).
    pub fn credentials_refreshes_sent(&self) -> u64 {
        self.refreshes_sent.load(Ordering::Relaxed)
    }

    /// Registers the hub client gauges and counters on `metrics`, read from memory
    /// when the meter collects, while the pool lives: `snodra.hub.client.connected.hubs` (the
    /// live hubs), `snodra.hub.client.handlers.in.flight` ([`handlers_in_flight`]),
    /// `snodra.hub.client.credentials.refreshes.sent`, and with OAuth2
    /// `snodra.hub.client.token.fetches{result}`, with a client certificate
    /// `snodra.hub.client.certificate.rotations`.
    ///
    /// [`handlers_in_flight`]: Self::handlers_in_flight
    pub fn observe(pool: &Arc<HubPool>, metrics: &Metrics) {
        let meter = metrics.meter();
        let each = |read: fn(&HubPool, &dyn AsyncInstrument<u64>)| {
            let pool = Arc::downgrade(pool);
            move |o: &dyn AsyncInstrument<u64>| {
                if let Some(pool) = pool.upgrade() {
                    read(&pool, o);
                }
            }
        };
        meter
            .u64_observable_gauge(HUB_CLIENT_CONNECTED_HUBS)
            .with_description("Hubs with an acknowledged stream")
            .with_callback(each(|p, o| o.observe(p.live().len() as u64, &[])))
            .build();
        meter
            .u64_observable_gauge(HUB_CLIENT_HANDLERS_IN_FLIGHT)
            .with_description("Incoming hub requests being handled or waiting for a handler permit")
            .with_callback(each(|p, o| o.observe(p.handlers_in_flight() as u64, &[])))
            .build();
        meter
            .u64_observable_counter(HUB_CLIENT_CREDENTIALS_REFRESHES_SENT)
            .with_description("ClientMessage.credentials written to the hubs")
            .with_callback(each(|p, o| o.observe(p.credentials_refreshes_sent(), &[])))
            .build();
        match pool.auth().kind {
            AuthKind::Jwt => {
                meter
                    .u64_observable_counter(HUB_CLIENT_TOKEN_FETCHES)
                    .with_description("Access token fetches by result")
                    .with_callback(each(|p, o| {
                        let fetches = p.auth().token_fetches;
                        for (result, n) in [
                            ("ok", fetches.ok),
                            ("http_error", fetches.http_error),
                            ("oauth_error", fetches.oauth_error),
                            ("io_error", fetches.io_error),
                            ("bad_response", fetches.bad_response),
                        ] {
                            o.observe(n, &[KeyValue::new("result", result)]);
                        }
                    }))
                    .build();
            }
            AuthKind::Certificate => {
                meter
                    .u64_observable_counter(HUB_CLIENT_CERTIFICATE_ROTATIONS)
                    .with_description("Client certificate pairs adopted from the files")
                    .with_callback(each(|p, o| {
                        let rotations = p.auth().certificate.map_or(0, |c| c.rotations);
                        o.observe(rotations, &[]);
                    }))
                    .build();
            }
            AuthKind::None | AuthKind::Token => {}
        }
    }

    /// Every connection of the pool, closed duplicates and draining ones included.
    pub fn connections(&self) -> Vec<Arc<HubConnection>> {
        self.members().iter().map(|m| Arc::clone(&m.conn)).collect()
    }

    /// The `RegisterAck` of the hub a request with `routing_key` would be sent to now (the first
    /// choice), without taking a round-robin turn.
    pub fn handshake_for(&self, routing_key: &str) -> Option<RegisterAck> {
        let live = self.live();
        if routing_key.is_empty() {
            return live.into_iter().next().map(|l| l.ack);
        }
        let ids: Vec<&str> = live.iter().map(|l| l.hub_id.as_str()).collect();
        let first = *rank_keyed(&ids, routing_key).first()?;
        Some(live[first].ack.clone())
    }

    /// The latest refusal of this instance's registration by each hub that refuses it now
    /// ([`HubConnection::last_rejection`]), by hub: its id when it registered this instance
    /// before ([`HubConnection::last_hub_id`]), else the target dialled as `host:port` (a refused
    /// `Register` carries no hub id). A hub that accepts a registration again leaves the map.
    pub fn rejections(&self) -> BTreeMap<String, String> {
        self.members()
            .iter()
            .filter_map(|m| {
                let rejection = m.conn.last_rejection()?;
                let hub = m
                    .conn
                    .last_hub_id()
                    .unwrap_or_else(|| super::targets::target_id(m.address()));
                Some((hub, rejection))
            })
            .collect()
    }

    /// The newest refusal of this instance's registration by any hub
    /// ([`HubConnection::latest_rejection`]): replaced by a newer refusal only, never cleared by a
    /// later acceptance, kept when its hub leaves the pool.
    pub fn latest_rejection(&self) -> Option<HubRejection> {
        if self.external {
            return self
                .members()
                .iter()
                .filter_map(|m| m.conn.latest_rejection_sequenced())
                .max_by_key(|(seq, _)| *seq)
                .map(|(_, r)| r);
        }
        self.latest_rejection
            .lock()
            .unwrap()
            .clone()
            .map(|(_, r)| r)
    }

    /// Requests that arrived on any hub and whose handler has not finished.
    pub fn handlers_in_flight(&self) -> usize {
        if self.external {
            return self
                .members()
                .iter()
                .map(|m| m.conn.inbound_in_flight())
                .sum();
        }
        self.in_flight.load(Ordering::SeqCst)
    }

    /// Writes a message built for the chosen hub's stream: round-robin without a `routing_key`,
    /// rendezvous with one. When the write fails or `build` refuses the stream (nothing was
    /// written) the next hub in that order is tried, so `build` may run more than once, each time
    /// for another stream. While no hub takes it (none is live, or none took the write) it waits
    /// for the live hubs to change, until `deadline`; then it fails with `HubUnavailable` ("no hub
    /// connected within `wait`", or "no hub took the request within `wait`" once a hub was tried).
    /// At once when the pool is turned off. `build` sees the ack of the stream current at the
    /// write.
    pub(crate) async fn send_where<T>(
        &self,
        routing_key: &str,
        deadline: tokio::time::Instant,
        wait: Duration,
        build: impl FnMut(StreamId, &RegisterAck) -> Result<(ClientMessage, T), MessagingError>,
    ) -> Result<(StreamId, T), MessagingError> {
        match self
            .send_excluding(routing_key, &[], deadline, wait, build)
            .await?
        {
            Some((stream, extra, _)) => Ok((stream, extra)),
            None => Err(self.unavailable()),
        }
    }

    /// Like [`send_where`](Self::send_where) over the live hubs whose address is not in `tried`,
    /// which also returns the address of the hub that took the write. For a request a hub
    /// declined with `NO_HANDLER` (it was never delivered): the next hub in the routing order
    /// gets it. `None` when `tried` is not empty and every other live hub refused the write: the
    /// declines are the answer, nothing is waited for.
    pub(crate) async fn send_excluding<T>(
        &self,
        routing_key: &str,
        tried: &[String],
        deadline: tokio::time::Instant,
        wait: Duration,
        mut build: impl FnMut(StreamId, &RegisterAck) -> Result<(ClientMessage, T), MessagingError>,
    ) -> Result<Option<(StreamId, T, String)>, MessagingError> {
        let mut changes = self.changes.subscribe();
        // The hubs that did not take the write, with the stream each had then: a hub with a new
        // stream is tried again.
        let mut refused: Vec<(String, Option<StreamId>)> = Vec::new();
        loop {
            if self.off.load(Ordering::SeqCst) {
                return Err(self.unavailable());
            }
            // The live hubs not tried yet: neither declined nor refused the write on their
            // current stream.
            let untried: Vec<Arc<HubConnection>> = self
                .ranked(routing_key)
                .into_iter()
                .filter(|c| !tried.iter().any(|a| a == c.address()))
                .filter(|c| {
                    let current = c.current_stream();
                    !refused
                        .iter()
                        .any(|(a, s)| a == c.address() && *s == current)
                })
                .collect();
            if untried.is_empty() && !tried.is_empty() {
                return Ok(None);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(MessagingError::HubUnavailable(
                    if tried.is_empty() && refused.is_empty() {
                        format!("no hub connected within {wait:?}")
                    } else {
                        format!("no hub took the request within {wait:?}")
                    },
                ));
            }
            for conn in &untried {
                let current = conn.current_stream();
                match conn.send_with(|stream, ack| build(stream, ack)).await {
                    Ok((stream, extra)) => {
                        return Ok(Some((stream, extra, conn.address().to_string())));
                    }
                    Err(e) => {
                        tracing::debug!(hub = %conn.address(), "not written, trying the next hub: {e}");
                        refused.push((conn.address().to_string(), current));
                    }
                }
            }
            if !untried.is_empty() {
                // Every hub tried in this pass refused the write: look again (they are skipped now)
                // before waiting, so declines plus refusals are answered at once.
                continue;
            }
            // No hub took it: wait for the live hubs to change, within the deadline. Every
            // handshake, lost stream and return to the routing signals `changes`; a pool of one
            // external connection hears of none, so it polls.
            tokio::select! {
                _ = changes.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(10)), if self.external => {}
                _ = tokio::time::sleep_until(deadline) => {}
                _ = self.cancel.cancelled() => return Err(self.unavailable()),
            }
        }
    }

    /// Writes `msg` to the hub stream `stream`, if it is still the current stream of its
    /// connection ([`HubConnection::send_on`]); `HubUnavailable` otherwise (nothing written).
    pub async fn send_on(
        &self,
        stream: StreamId,
        msg: ClientMessage,
    ) -> Result<(), MessagingError> {
        let Some(conn) = self
            .members()
            .into_iter()
            .map(|m| m.conn.clone())
            .find(|c| c.current_stream() == Some(stream))
        else {
            return Err(MessagingError::HubUnavailable(format!(
                "hub stream {stream} is gone"
            )));
        };
        conn.send_on(stream, msg).await
    }

    /// Background tasks held (status and resolve loops, drains); tests.
    #[doc(hidden)]
    pub fn background_tasks(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    /// Lets the pool see whether requests written to a stream are unanswered (draining).
    pub(crate) fn add_pending_probe(&self, probe: PendingProbe) {
        self.probes.lock().unwrap().push(probe);
    }

    /// The first step of a graceful shutdown: registers no types with every hub (each stops
    /// routing to this instance and hands its queued requests to the other instances at once;
    /// replies to what it already forwarded still go through) and waits at most `within` for
    /// every hub's ack. True when every connected hub acked in time; nothing to do (and true)
    /// when the pool is turned off.
    pub async fn deregister(&self, within: Duration) -> bool {
        if self.off.load(Ordering::SeqCst) {
            return true;
        }
        // A connection opened from now on (a new DNS address, a revived parked one) registers no
        // types either.
        *self.types.lock().unwrap() = RegisteredTypes::default();
        let members = self.members();
        futures::future::join_all(
            members
                .iter()
                .map(|m| m.conn.deregister_for_shutdown(within)),
        )
        .await
        .into_iter()
        .all(|acked| acked)
    }

    /// Stops the background tasks and closes every stream (each hub drops this instance at once).
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let tasks: Vec<_> = self.tasks.lock().unwrap().drain(..).collect();
        for t in tasks {
            let _ = t.await;
        }
        let members = self.members();
        futures::future::join_all(members.iter().map(|m| m.conn.shutdown())).await;
        // The token provider is the application's, shared by every hub; a `Sender` that outlives
        // the shutdown must not keep it polling the IdP.
        if let Some(provider) = &self.settings.token_provider {
            provider.close();
        }
    }

    fn members(&self) -> Vec<Arc<Member>> {
        self.members.lock().unwrap().clone()
    }

    fn live(&self) -> Vec<Live> {
        self.members()
            .into_iter()
            .filter(|m| m.role() == Role::Active)
            .filter_map(|m| {
                let ack = m.conn.handshake()?;
                Some(Live {
                    conn: Arc::clone(&m.conn),
                    hub_id: hub_id(&ack, m.address()),
                    ack,
                })
            })
            .collect()
    }

    /// The live hubs in the order a request with `routing_key` tries them.
    fn ranked(&self, routing_key: &str) -> Vec<Arc<HubConnection>> {
        let live = self.live();
        if live.is_empty() {
            return vec![];
        }
        let order = if routing_key.is_empty() {
            rank_round_robin(live.len(), self.turn.fetch_add(1, Ordering::Relaxed))
        } else {
            let ids: Vec<&str> = live.iter().map(|l| l.hub_id.as_str()).collect();
            rank_keyed(&ids, routing_key)
        };
        order
            .into_iter()
            .map(|i| Arc::clone(&live[i].conn))
            .collect()
    }

    fn add(self: &Arc<Self>, address: String) -> Arc<Member> {
        let member = {
            // Built (its types read) under the members lock: `deregister` empties the types before
            // it takes its snapshot of the members, so a member added meanwhile either reads the
            // empty types or is in that snapshot and gets the empty Register.
            let mut members = self.members.lock().unwrap();
            let member = self.member(address);
            members.push(Arc::clone(&member));
            member
        };
        if self.started.load(Ordering::SeqCst) {
            member.conn.start();
        }
        member
    }

    /// A new member for `address`, not started and not in [`members`](Self::members) yet.
    fn member(self: &Arc<Self>, address: String) -> Arc<Member> {
        let mut settings = HubSettings {
            address: address.clone(),
            ..self.settings.clone()
        };
        // `dns:///name:port` resolves to `ip:port`, and tonic would check the certificate against
        // the IP. Check it against the DNS name unless `server_name` says otherwise (snodra-messaging
        // does the same).
        if let (HubTargets::Dns { host, .. }, Some(tls)) = (&self.targets, settings.tls.as_mut())
            && tls
                .server_name
                .as_deref()
                .is_none_or(|n| n.trim().is_empty())
            && host.parse::<std::net::IpAddr>().is_err()
        {
            tls.server_name = Some(host.clone());
        }
        let conn = HubConnection::new(
            settings,
            self.service_name.clone(),
            self.instance_id.clone(),
            self.inbound.clone(),
        )
        .expect("the settings were checked by HubPool::new");
        conn.set_registered_types(self.types.lock().unwrap().clone());
        let weak = Arc::downgrade(self);
        let max = self.settings.max_concurrent_handlers;
        conn.set_hooks(Hooks {
            initial_permits: Box::new({
                let (weak, address) = (weak.clone(), address.clone());
                move || match weak.upgrade() {
                    Some(pool) => pool.initial_permits(&address),
                    None => permit_share(max, 1),
                }
            }),
            accept: Box::new({
                let (weak, address) = (weak.clone(), address.clone());
                move |ack| weak.upgrade().is_none_or(|p| p.accept(&address, ack))
            }),
            changed: Box::new(move || {
                if let Some(pool) = weak.upgrade() {
                    pool.changes.send_modify(|n| *n += 1);
                    pool.top_up();
                    pool.report_soon();
                }
            }),
            in_flight: Arc::clone(&self.in_flight),
            refreshes_sent: Arc::clone(&self.refreshes_sent),
            latest_rejection: Arc::clone(&self.latest_rejection),
        });
        Arc::new(Member::new(conn))
    }

    /// `ceil(max / N)`, N = the hubs live now or still on their first connect attempt, the one at
    /// `address` included.
    fn initial_permits(&self, address: &str) -> u32 {
        permit_share(
            self.settings.max_concurrent_handlers,
            self.hubs(Some(address)),
        )
    }

    /// N of the split: the active connections whose stream is live or that are still on their
    /// first connect attempt, plus the one at `including`, also while it is being rolled over to a
    /// renewed client certificate (its new stream offers the same share as the one it replaces).
    fn hubs(&self, including: Option<&str>) -> usize {
        self.members()
            .iter()
            .filter(|m| {
                if Some(m.address()) == including {
                    return m.kept();
                }
                m.role() == Role::Active && (m.conn.handshake().is_some() || !m.conn.failed_once())
            })
            .count()
    }

    /// Fewer hubs now than when some live stream offered its permits: that stream is topped up to
    /// `ceil(max / N)`. More hubs: nothing (permits are never
    /// revoked; the local concurrency guard stays the total).
    fn top_up(&self) {
        let share = permit_share(self.settings.max_concurrent_handlers, self.hubs(None));
        let live: Vec<_> = self.live().into_iter().map(|l| l.conn).collect();
        if live.is_empty() {
            return;
        }
        tokio::spawn(async move {
            futures::future::join_all(live.iter().map(|c| c.top_up(share))).await;
        });
    }

    /// The connection at `address` completed its handshake: it keeps its hub unless another
    /// kept connection reached the same hub id first; then it is parked (and its connection
    /// closed by the caller). Either way an id this address gave up (it reaches another hub
    /// than before) frees the addresses parked behind it.
    fn accept(self: &Arc<Self>, address: &str, ack: &RegisterAck) -> bool {
        let hub_id = hub_id(ack, address);
        let (accepted, revived) = {
            let mut owners = self.owners.lock().unwrap();
            // This connection may reach another hub than before (a restarted hub gets a new id).
            owners.retain(|_, owner| owner != address);
            let members = self.members();
            let kept = |a: &str| members.iter().any(|m| m.address() == a && m.kept());
            let accepted = match owners.get(&hub_id) {
                Some(owner) if kept(owner) => {
                    tracing::warn!(service = %self.service_name, hub = %hub_id, address, kept = %owner, "two hub addresses reach the same hub; closing and parking the second");
                    if let Some(m) = members.iter().find(|m| m.address() == address) {
                        m.park(hub_id);
                    }
                    false
                }
                _ => {
                    owners.insert(hub_id, address.to_string());
                    true
                }
            };
            (accepted, self.revive_locked(&mut owners))
        };
        self.start_revived(revived);
        accepted
    }

    /// Replaces every parked member whose hub id no kept connection owns any more (its owner left
    /// DNS, or answers another id) with a new member at its address, in its place, and returns
    /// them to start. Ownerships of connections no longer kept (draining) are dropped first.
    /// Caller holds `owners` (taken before `members`).
    fn revive_locked(self: &Arc<Self>, owners: &mut HashMap<String, String>) -> Vec<Arc<Member>> {
        let mut members = self.members.lock().unwrap();
        // Looked at under the lock: `shutdown` cancels before it takes its snapshot of the members,
        // so either this sees the cancel, or the snapshot holds what is put in place here.
        if self.cancel.is_cancelled() || !self.started.load(Ordering::SeqCst) {
            return vec![];
        }
        owners.retain(|_, owner| members.iter().any(|m| m.address() == owner && m.kept()));
        let mut revived = Vec::new();
        for slot in members.iter_mut() {
            let Some(id) = slot.parked_behind() else {
                continue;
            };
            if owners.contains_key(&id) {
                continue;
            }
            let fresh = self.member(slot.address().to_string());
            let parked = std::mem::replace(slot, Arc::clone(&fresh));
            // Its connection was cancelled when it was parked; let it finish.
            tokio::spawn(async move { parked.conn.shutdown().await });
            revived.push(fresh);
        }
        revived
    }

    /// Starts the members [`revive_locked`](Self::revive_locked) put in place of parked ones.
    fn start_revived(&self, revived: Vec<Arc<Member>>) {
        for m in revived {
            tracing::info!(service = %self.service_name, address = %m.address(), "the hub this address was parked behind has no other address any more; connecting again");
            m.conn.start();
        }
    }

    /// [`revive_locked`](Self::revive_locked) with the lock taken, then the start.
    fn revive(self: &Arc<Self>) {
        let revived = self.revive_locked(&mut self.owners.lock().unwrap());
        self.start_revived(revived);
    }

    async fn status_loop(self: Arc<Self>) {
        let period = self.settings.status_interval.max(Duration::from_millis(1));
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = ticker.tick() => self.report_status().await,
            }
        }
    }

    /// An `InstanceStatus` to every live hub now, off the caller's thread: the set of live hubs
    /// changed (besides the `status_interval` reports).
    fn report_soon(self: &Arc<Self>) {
        if self.external || self.cancel.is_cancelled() {
            return;
        }
        let me = Arc::clone(self);
        tokio::spawn(async move { me.report_status().await });
    }

    /// One `InstanceStatus` to every live hub.
    pub(crate) async fn report_status(&self) {
        let live = self.live();
        let mut ids: Vec<String> = Vec::new();
        for l in &live {
            if !ids.contains(&l.hub_id) {
                ids.push(l.hub_id.clone());
            }
        }
        // The processors' status, read once.
        let source = self.processor_status.lock().unwrap().clone();
        let status = InstanceStatus {
            connected_hub_ids: ids,
            handlers_in_flight: self.handlers_in_flight().min(u32::MAX as usize) as u32,
            processors: source.map(|source| source()).unwrap_or_default(),
        };
        let writes = live.iter().map(|l| {
            let msg = ClientMessage {
                message: Some(client_message::Message::Status(status.clone())),
            };
            async move {
                let _ = tokio::time::timeout(STATUS_WRITE_TIMEOUT, l.conn.send(msg)).await;
            }
        });
        futures::future::join_all(writes).await;
    }

    async fn resolve_loop(self: Arc<Self>, host: String, port: u16) {
        loop {
            let resolver = self.resolver.lock().unwrap().clone();
            let resolved = tokio::select! {
                _ = self.cancel.cancelled() => return,
                r = resolver(host.clone(), port) => r,
            };
            match resolved {
                Ok(addrs) if !addrs.is_empty() => self.apply_resolution(&resolved_targets(&addrs)),
                Ok(_) => {
                    tracing::warn!(service = %self.service_name, host, "dns:/// name resolved to no address; keeping the current hubs")
                }
                Err(e) => {
                    tracing::warn!(service = %self.service_name, host, "dns:/// name not resolved ({e}); keeping the current hubs")
                }
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = tokio::time::sleep(self.settings.dns_refresh) => {}
            }
        }
    }

    /// Connects to resolved addresses that have no connection and drains the ones that vanished.
    fn apply_resolution(self: &Arc<Self>, resolved: &[String]) {
        let members = self.members();
        let current: Vec<String> = members
            .iter()
            .filter(|m| m.role() != Role::Draining)
            .map(|m| m.address().to_string())
            .collect();
        let (added, removed) = plan(&current, resolved);
        for address in added {
            match members.iter().find(|m| m.address() == address) {
                Some(m) => {
                    tracing::info!(service = %self.service_name, address, "hub address is back in DNS; kept");
                    m.set_role(Role::Active);
                    // Back in the routing: a call waiting for a hub may take it.
                    self.changes.send_modify(|n| *n += 1);
                    // Its hub may be reached through another address meanwhile (a parked one
                    // revived when this one drained): deduplicated as at a handshake.
                    if let Some(ack) = m.conn.handshake()
                        && !self.accept(&address, &ack)
                    {
                        let conn = Arc::clone(&m.conn);
                        tokio::spawn(async move { conn.shutdown().await });
                        continue;
                    }
                    // Announce the types again (the drain had withdrawn them).
                    m.conn
                        .set_registered_types(self.types.lock().unwrap().clone());
                    let conn = Arc::clone(&m.conn);
                    tokio::spawn(async move {
                        if let Err(e) = conn.reregister().await {
                            tracing::debug!(address = %conn.address(), "types not announced again now: {e}");
                        }
                    });
                }
                None => {
                    tracing::info!(service = %self.service_name, address, "new hub address in DNS; connecting");
                    self.add(address);
                }
            }
        }
        let mut left = false;
        for address in removed {
            if let Some(m) = members.iter().find(|m| m.address() == address) {
                tracing::info!(service = %self.service_name, address, "hub address left DNS; closing once idle");
                left |= m.role() == Role::Active && m.conn.handshake().is_some();
                self.drain(Arc::clone(m));
            }
        }
        if left {
            // Out of the live hubs at once: the others hear of it now.
            self.report_soon();
        }
        // An owner that drains frees its hub id for the addresses parked behind it.
        self.revive();
        self.top_up();
    }

    /// Takes a vanished address out of the routing and closes it once idle: an empty `Register`
    /// first (the hub stops routing to this instance there and hands its queue to other
    /// instances; a reconnect registers no types either), then the stream is closed when no
    /// request of ours written to it is unanswered and no handler of its requests runs, at most
    /// [`DRAIN_LIMIT`] later.
    fn drain(self: &Arc<Self>, member: Arc<Member>) {
        let was = member.role();
        member.set_role(Role::Draining);
        if was != Role::Duplicate {
            member.conn.set_registered_types(RegisteredTypes::default());
        }
        let me = Arc::clone(self);
        let task = tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + DRAIN_LIMIT;
            if was != Role::Duplicate {
                if let Err(e) = member.conn.reregister().await {
                    tracing::debug!(address = %member.address(), "no stream to deregister on: {e}");
                }
                while !me.idle(&member.conn, Consumers::Count)
                    && tokio::time::Instant::now() < deadline
                {
                    tokio::select! {
                        _ = me.cancel.cancelled() => return,
                        _ = tokio::time::sleep(DRAIN_POLL) => {}
                    }
                    if member.role() != Role::Draining {
                        return; // back in DNS
                    }
                }
            }
            {
                let mut members = me.members.lock().unwrap();
                if member.role() != Role::Draining {
                    return;
                }
                members.retain(|m| !Arc::ptr_eq(m, &member));
            }
            me.owners
                .lock()
                .unwrap()
                .retain(|_, owner| owner != member.address());
            member.conn.flushed(Duration::from_secs(1)).await;
            member.conn.shutdown().await;
            tracing::info!(service = %me.service_name, address = %member.address(), "hub connection closed (address left DNS)");
        });
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|t| !t.is_finished());
        tasks.push(task);
    }

    /// Once per `certificate_check_interval`: look at the client certificate files (see the module docs).
    async fn certificate_loop(self: Arc<Self>) {
        let period = self
            .settings
            .certificate_check_interval
            .max(Duration::from_millis(1));
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = ticker.tick() => {}
            }
            // An unexpected failure (a panic) in one tick must not end the watch silently.
            if let Err(panic) = std::panic::AssertUnwindSafe(self.check_certificate())
                .catch_unwind()
                .await
            {
                self.note_tick_failure(&panic_message(panic.as_ref()));
            }
        }
    }

    async fn check_certificate(self: &Arc<Self>) {
        let Some(tls) = &self.settings.tls else {
            return;
        };
        let (Some(cert_file), Some(key_file)) = (&tls.client_cert_file, &tls.client_key_file)
        else {
            return;
        };
        let now = SystemTime::now();
        // A pair that is not valid yet is not adopted (the hub would refuse its handshake):
        // the roll waits for its notBefore; it still counts as a renewal on disk.
        let (disk, valid) = match mtls::load(cert_file, key_file, &self.service_name) {
            Ok(identity) => match mtls::check_valid_at(&identity, now) {
                Ok(()) => (Some(identity), true),
                Err(mtls::LoadError::NotYetValid(reason)) => {
                    self.note_waiting_for_validity(&identity, &reason);
                    (Some(identity), false)
                }
                Err(e) => {
                    self.note_certificate_problem(e.message());
                    (None, false)
                }
            },
            Err(e) => {
                self.note_certificate_problem(e.message());
                (None, false)
            }
        };
        if disk.is_some() {
            self.certificate.lock().unwrap().problem = None;
        }
        let live = self.live();
        self.warn_near_expiry(&live, disk.as_ref(), cert_file, now);
        let Some(disk) = disk.filter(|_| valid) else {
            return;
        };
        {
            let mut watch = self.certificate.lock().unwrap();
            if watch.refused.is_some_and(|d| d != disk.digest) {
                watch.refused = None;
            }
            if watch.refused == Some(disk.digest) {
                return;
            }
        }
        // The replicas of a service all see a renewal within one tick; rolled together they would
        // all be out of the routing at once. Each waits its own random time first, once per pair.
        let Some(disk) = self.wait_before_rolling(disk, cert_file, key_file).await else {
            return;
        };
        for member in self.members() {
            if self.cancel.is_cancelled() {
                return;
            }
            let stale = member.role() == Role::Active
                && member.conn.handshake().is_some()
                && member
                    .conn
                    .presented_certificate()
                    .is_some_and(|p| p.digest != disk.digest);
            if stale && !self.roll_over(&member, &disk.fingerprint).await {
                self.certificate.lock().unwrap().refused = Some(disk.digest);
                tracing::error!(service = %self.service_name, address = %member.address(), fingerprint = %disk.fingerprint, "the hub did not register again with the renewed client certificate in time; the other hubs keep the certificate they have until the files change again");
                return;
            }
        }
    }

    /// Before the first roll to `disk` (once per pair, and only while a live connection presents
    /// another one): waits a random time, uniform in `[0, min(the setting, 10 % of what the
    /// shortest-lived stale connection's certificate has left)]` (nothing when it has ended
    /// already), then reads the files again and returns the pair they hold then (valid, and for
    /// this service), to roll to without another delay. `None` when the roll is not to go on: the
    /// pool is shutting down, or the files no longer hold a usable pair (the next tick decides).
    /// Without a stale connection, or after the pair's delay, `disk` itself.
    async fn wait_before_rolling(
        &self,
        disk: ClientIdentity,
        cert_file: &std::path::Path,
        key_file: &std::path::Path,
    ) -> Option<ClientIdentity> {
        let remaining = {
            let mut watch = self.certificate.lock().unwrap();
            if watch.delayed == Some(disk.digest) {
                return Some(disk);
            }
            let presented: Vec<_> = self
                .members()
                .iter()
                .filter_map(|m| m.conn.presented_certificate())
                .map(|p| (p.digest, p.status.not_after))
                .collect();
            let Some(remaining) =
                shortest_stale_remaining(&presented, disk.digest, SystemTime::now())
            else {
                return Some(disk); // nothing to roll
            };
            watch.delayed = Some(disk.digest);
            remaining
        };
        let bound = roll_delay_bound(self.settings.certificate_roll_delay_max, remaining);
        let source = self.roll_delay.lock().unwrap().clone();
        let delay = source(bound).min(bound);
        if delay.is_zero() {
            return Some(disk);
        }
        tracing::info!(service = %self.service_name, fingerprint = %disk.fingerprint, delay = ?delay, "a renewed client certificate is in the files; rolling the hub connections over to it after a random delay");
        tokio::select! {
            _ = self.cancel.cancelled() => return None,
            _ = tokio::time::sleep(delay) => {}
        }
        // Whatever the files hold now is what is rolled to, without a second delay.
        let current = mtls::load(cert_file, key_file, &self.service_name)
            .and_then(|identity| {
                mtls::check_valid_at(&identity, SystemTime::now())?;
                Ok(identity)
            })
            .ok()?;
        self.certificate.lock().unwrap().delayed = Some(current.digest);
        Some(current)
    }

    /// A failure of a whole tick is logged at `error` once per distinct message, then at `debug`;
    /// the watch goes on with the next tick.
    fn note_tick_failure(&self, message: &str) {
        let mut watch = self.certificate.lock().unwrap();
        if watch.tick_failure.as_deref() == Some(message) {
            tracing::debug!(service = %self.service_name, reason = message, "the client certificate check failed again; trying again next tick");
        } else {
            tracing::error!(service = %self.service_name, reason = message, "the client certificate check failed; the connections keep the certificate they present and the check goes on with the next tick");
            watch.tick_failure = Some(message.to_string());
        }
    }

    /// A renewed pair that is not valid yet is waited for: logged at `info` once per pair.
    fn note_waiting_for_validity(&self, identity: &ClientIdentity, reason: &str) {
        let mut watch = self.certificate.lock().unwrap();
        if watch.waiting != Some(identity.digest) {
            watch.waiting = Some(identity.digest);
            tracing::info!(service = %self.service_name, reason, "the renewed client certificate is not valid yet; the connections keep the one they present until it is");
        }
    }

    /// A problem with the files is logged at `error` once, when it is still there a period later
    /// (a pair replaced in two writes is caught half way for a moment); the same problem again
    /// goes to `debug`.
    fn note_certificate_problem(&self, message: &str) {
        let mut watch = self.certificate.lock().unwrap();
        match &mut watch.problem {
            Some((seen, logged)) if seen == message => {
                if !*logged {
                    *logged = true;
                    tracing::error!(service = %self.service_name, reason = message, "the client certificate files cannot be used; the connections keep the certificate they present");
                }
            }
            problem => {
                tracing::debug!(service = %self.service_name, reason = message, "the client certificate files cannot be used right now");
                *problem = Some((message.to_string(), false));
            }
        }
    }

    /// Warns once per certificate when the one in use is in the last tenth of its life (at most
    /// the last 24 h) and the files hold no other one.
    fn warn_near_expiry(
        &self,
        live: &[Live],
        disk: Option<&ClientIdentity>,
        cert_file: &std::path::Path,
        now: SystemTime,
    ) {
        let Some(presented) = live.iter().find_map(|l| l.conn.presented_certificate()) else {
            return;
        };
        if disk.is_some_and(|d| d.digest != presented.digest) {
            return; // a renewal is on disk and about to be rolled over
        }
        let status = &presented.status;
        if !mtls::expiry_warning_due(presented.not_before, status.not_after, now) {
            return;
        }
        let mut watch = self.certificate.lock().unwrap();
        if watch.warned.as_deref() == Some(status.fingerprint.as_str()) {
            return;
        }
        watch.warned = Some(status.fingerprint.clone());
        let left = status.not_after.duration_since(now).unwrap_or_default();
        tracing::warn!(service = %self.service_name, fingerprint = %status.fingerprint, not_after = ?status.not_after, left = ?left, file = %cert_file.display(), "the client certificate expires soon and the file holds no renewed one; the hub closes the session at notAfter");
    }

    /// Rolls the connection of `member` over to the pair now on disk, see the module docs; true
    /// when it is registered again with it (or there was nothing to roll).
    async fn roll_over(&self, member: &Arc<Member>, fingerprint: &str) -> bool {
        let conn = &member.conn;
        let Some(stream) = conn.current_stream() else {
            return true;
        };
        // Another live hub serves meanwhile; the only hub keeps taking requests until the end.
        let others = self.members().iter().any(|m| {
            !Arc::ptr_eq(m, member) && m.role() == Role::Active && m.conn.handshake().is_some()
        });
        if others {
            member.change_role(Role::Active, Role::Rolling);
        }
        tracing::info!(service = %self.service_name, address = %member.address(), fingerprint, "rolling the hub connection over to the renewed client certificate");
        if let Err(e) = conn.deregister().await {
            tracing::debug!(address = %member.address(), "no stream to deregister on: {e}");
        }
        let deadline = tokio::time::Instant::now() + ROLL_IDLE_LIMIT;
        if !self.settle_and_wait_idle(member, deadline).await {
            return true;
        }
        // The only hub kept taking requests while its handlers finished; now it is out of the
        // routing, and what was written to it in the meantime is waited for too.
        if member.change_role(Role::Active, Role::Rolling)
            && !self.settle_and_wait_idle(member, deadline).await
        {
            return true;
        }
        conn.roll_over(stream);
        let deadline = tokio::time::Instant::now() + ROLL_RECONNECT_LIMIT;
        let back = loop {
            if conn.current_stream().is_some_and(|s| s != stream) {
                break true;
            }
            if tokio::time::Instant::now() >= deadline {
                break false;
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return true,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        };
        member.change_role(Role::Rolling, Role::Active);
        self.changes.send_modify(|n| *n += 1);
        back
    }

    /// Waits for the requests of `member`'s stream to finish, until `deadline` (logged at `warn`
    /// when it runs out); false when the pool is shutting down.
    async fn settle_and_wait_idle(
        &self,
        member: &Arc<Member>,
        deadline: tokio::time::Instant,
    ) -> bool {
        tokio::select! {
            _ = self.cancel.cancelled() => return false,
            _ = tokio::time::sleep(ROLL_SETTLE) => {}
        }
        while !self.idle(&member.conn, Consumers::Ignore) {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(service = %self.service_name, address = %member.address(), "requests still running after {ROLL_IDLE_LIMIT:?}, closing the hub stream for the certificate rollover anyway");
                break;
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return false,
                _ = tokio::time::sleep(DRAIN_POLL) => {}
            }
        }
        true
    }

    /// No handler of its requests running and none of our requests written to it unanswered.
    fn idle(&self, conn: &HubConnection, consumers: Consumers) -> bool {
        let Some(stream) = conn.current_stream() else {
            return true;
        };
        conn.inbound_in_flight() == 0
            && !self
                .probes
                .lock()
                .unwrap()
                .iter()
                .any(|p| p(stream, consumers))
    }
}

/// The text of a caught panic.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|m| m.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unexpected failure".into())
}

/// The upper bound of the random delay before a rollover starts: `max`, or a tenth of what the
/// presented certificate has left, whichever is smaller (zero when it has expired).
pub(crate) fn roll_delay_bound(max: Duration, remaining: Duration) -> Duration {
    max.min(remaining / 10)
}

/// What the shortest-lived of the connections that do not present the pair `disk` has left at
/// `now`: `None` when no connection is stale. `presented` is each live connection's
/// `(digest, notAfter)`.
pub(crate) fn shortest_stale_remaining(
    presented: &[([u8; 32], SystemTime)],
    disk: [u8; 32],
    now: SystemTime,
) -> Option<Duration> {
    presented
        .iter()
        .filter(|(digest, _)| *digest != disk)
        .map(|(_, not_after)| not_after.duration_since(now).unwrap_or_default())
        .min()
}

/// The delay for a uniform `unit` in `[0, 1)`: that share of `bound`.
pub(crate) fn roll_delay(bound: Duration, unit: f64) -> Duration {
    bound.mul_f64(unit.clamp(0.0, 1.0))
}

/// `(to add, to remove)`: resolved addresses without a connection, and connected ones no longer
/// resolved; both in input order.
pub(crate) fn plan(current: &[String], resolved: &[String]) -> (Vec<String>, Vec<String>) {
    let mut added: Vec<String> = Vec::new();
    for r in resolved {
        if !current.contains(r) && !added.contains(r) {
            added.push(r.clone());
        }
    }
    let removed = current
        .iter()
        .filter(|c| !resolved.contains(c))
        .cloned()
        .collect();
    (added, removed)
}

/// Keeps a [`super::Sender`] out of the pool's lifetime.
pub(crate) fn weak_probe<S: Send + Sync + 'static>(
    sender: Weak<S>,
    pending_on: fn(&S, StreamId, Consumers) -> bool,
) -> PendingProbe {
    Box::new(move |stream, consumers| {
        sender
            .upgrade()
            .is_some_and(|s| pending_on(&s, stream, consumers))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// Once the pool is shut down nothing is revived: a parked member stays as it is.
    #[tokio::test]
    async fn nothing_is_revived_after_the_shutdown() {
        let (tx, _rx) = mpsc::channel(8);
        let pool = HubPool::new(
            HubSettings {
                address: "static://127.0.0.1:1,static://127.0.0.1:2".into(),
                ..HubSettings::default()
            },
            "svc".into(),
            "i".into(),
            tx,
        )
        .unwrap();
        pool.started.store(true, Ordering::SeqCst);
        let parked = Arc::clone(&pool.members()[1]);
        parked.park("hub-x".into());
        pool.cancel.cancel();
        pool.revive();
        assert!(Arc::ptr_eq(&pool.members()[1], &parked), "not replaced");
    }

    #[test]
    fn the_roll_delay_is_bounded_by_the_setting_and_a_tenth_of_what_is_left() {
        let (max, day) = (Duration::from_secs(30), Duration::from_secs(86_400));
        assert_eq!(roll_delay_bound(max, 7 * day), max);
        assert_eq!(
            roll_delay_bound(max, Duration::from_secs(100)),
            Duration::from_secs(10)
        );
        assert_eq!(roll_delay_bound(max, Duration::ZERO), Duration::ZERO);
        assert_eq!(roll_delay_bound(Duration::ZERO, 7 * day), Duration::ZERO);
    }

    #[test]
    fn the_bound_follows_the_shortest_lived_stale_connection() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let at = |secs: u64| now + Duration::from_secs(secs);
        let (new, old_a, old_b) = ([1; 32], [2; 32], [3; 32]);
        // Nothing presented, or only the new pair: nobody is stale.
        assert_eq!(shortest_stale_remaining(&[], new, now), None);
        assert_eq!(shortest_stale_remaining(&[(new, at(50))], new, now), None);
        // The connection that already presents the new pair does not count.
        assert_eq!(
            shortest_stale_remaining(
                &[(old_a, at(500)), (new, at(5)), (old_b, at(200))],
                new,
                now
            ),
            Some(Duration::from_secs(200))
        );
        // Already ended: zero.
        assert_eq!(
            shortest_stale_remaining(&[(old_a, now - Duration::from_secs(1))], new, now),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn the_roll_delay_is_uniform_over_zero_to_the_bound() {
        let bound = Duration::from_secs(30);
        assert_eq!(roll_delay(bound, 0.0), Duration::ZERO);
        assert_eq!(roll_delay(bound, 0.5), Duration::from_secs(15));
        assert!(roll_delay(bound, 0.999_999) < bound);
        assert_eq!(roll_delay(Duration::ZERO, 0.7), Duration::ZERO);
    }

    #[test]
    fn a_new_resolution_adds_new_addresses_and_removes_vanished_ones() {
        let (added, removed) = plan(&strings(&["a", "b"]), &strings(&["b", "c", "c"]));
        assert_eq!(added, ["c"]);
        assert_eq!(removed, ["a"]);
        let (added, removed) = plan(&strings(&["a"]), &strings(&["a"]));
        assert!(added.is_empty() && removed.is_empty());
        let (added, removed) = plan(&[], &strings(&["a", "b"]));
        assert_eq!((added, removed), (strings(&["a", "b"]), vec![]));
    }
}
