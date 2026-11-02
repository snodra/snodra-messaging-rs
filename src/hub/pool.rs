//! Connections to every hub (multi-hub high availability, ADR 0016).
//!
//! The hubs never talk to each other; availability comes from every instance registering on every
//! hub. A [`HubPool`] holds one [`HubConnection`] per target of `hub_address` ([`HubTargets`]),
//! each with its own handshake, heartbeat, reconnect backoff and legacy detection, all sending the
//! identical `Register`, and all feeding one inbound channel (replies go on the arrival stream).
//!
//! - **Dedupe.** Two targets whose acks carry the same `hub_id` are one hub: the connection
//!   acknowledged first keeps it, the other is closed for good (logged once). A hub has no say in
//!   this: its second `Register` already replaced the first stream's session, so that stream is
//!   ended by the hub and reconnects, once. A hub without an id (legacy, or an empty
//!   `RegisterAck.hub_id`) is identified by its target as `host:port` (prefix stripped).
//! - **Routing.** An unkeyed request goes round-robin over the hubs whose stream is handshaken
//!   (reconnecting ones are skipped); a keyed one to the live hub with the highest
//!   [`rendezvous_score`](super::rendezvous_score) for `(hub_id, routing key)`. A request that
//!   could not be written to the chosen hub's stream (it dropped meanwhile) goes to the next hub in
//!   that order with the same request id: it was never sent. One already written fails with
//!   `HubConnectionLost` when its stream drops (the caller's retry policy decides).
//! - **Permits.** A stream offers `ceil(max_concurrent_handlers / N)` permits
//!   ([`permit_share`]), N being the hubs live when its `Register` is written, itself included;
//!   until a hub's first connect attempt is over it counts as live, so hubs that come up together
//!   split from the start. When N shrinks (a hub is lost, closed as a duplicate or leaves DNS),
//!   every live stream is topped up to `ceil(max / N)` with one `Permits` message whose `grant`
//!   (`ceil(max / N)`) raises the hub's permit cap on the session, so the survivors can run the
//!   share of the hub that left; a stream that already offered that many
//!   gets nothing. Permits are never revoked when N grows. The local concurrency guard stays the
//!   total.
//! - **Status.** Every `status_interval` an `InstanceStatus` (acknowledged hub ids, handlers in
//!   flight) goes to every hub whose ack lists `instance-status`, and never to another.
//! - **DNS.** A `dns:///name:port` target is resolved at start and every `dns_refresh` into
//!   `ip:port` targets of one family (IPv4 when any, else IPv6, [`resolved_targets`]): a new
//!   address gets a connection; a vanished one leaves the routing at once, gets an empty
//!   `Register` (the hub stops routing to the instance there) and is closed once idle (no request
//!   of ours written to it unanswered, no handler of its requests running), at most
//!   [`DRAIN_LIMIT`] later. A failed or empty resolution keeps the current hubs.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::MessagingError;
use crate::proto::{ClientMessage, InstanceStatus, client_message};

use super::connection::{
    Handshake, Hooks, HubConnection, HubSettings, Inbound, RegisteredTypes, StreamId,
};
use super::targets::{HubTargets, permit_share, rank_keyed, rank_round_robin, resolved_targets};

/// The longest a vanished DNS address stays open to finish its requests.
pub const DRAIN_LIMIT: Duration = Duration::from_secs(65);
/// How long one `InstanceStatus` write may wait for room on a stream.
const STATUS_WRITE_TIMEOUT: Duration = Duration::from_secs(1);
const DRAIN_POLL: Duration = Duration::from_millis(100);

/// Resolves `(host, port)` to socket addresses (the system resolver unless replaced in tests).
pub type Resolver = Arc<
    dyn Fn(String, u16) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<SocketAddr>>> + Send>>
        + Send
        + Sync,
>;

/// Tells whether requests written to a stream are still unanswered (registered by a [`super::Sender`]).
type PendingProbe = Box<dyn Fn(StreamId) -> bool + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Active,
    /// Its hub is reached by another connection; closed for good.
    Duplicate,
    /// Its address vanished from DNS; out of the routing, closed once idle.
    Draining,
}

struct Member {
    conn: Arc<HubConnection>,
    role: Mutex<Role>,
}

impl Member {
    fn role(&self) -> Role {
        *self.role.lock().unwrap()
    }

    fn set_role(&self, role: Role) {
        *self.role.lock().unwrap() = role;
    }

    fn address(&self) -> &str {
        self.conn.address()
    }
}

/// A live hub at one moment: its connection, id and handshake.
struct Live {
    conn: Arc<HubConnection>,
    hub_id: String,
    handshake: Handshake,
}

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
    changes: watch::Sender<u64>,
    probes: Mutex<Vec<PendingProbe>>,
    resolver: Mutex<Resolver>,
    /// Built around one connection the caller owns ([`HubPool::of`]): no tasks of its own.
    external: bool,
    started: AtomicBool,
    cancel: CancellationToken,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl HubPool {
    /// One connection per target of `settings.address` (nothing starts yet). `InvalidMessage`
    /// when the address names no hub or is malformed ([`HubTargets::parse`]).
    pub fn new(
        settings: HubSettings,
        service_name: String,
        instance_id: String,
        inbound: mpsc::Sender<Inbound>,
    ) -> Result<Arc<HubPool>, MessagingError> {
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

    /// A pool of exactly `conn`, which the caller starts and shuts down (as before multi-hub).
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
        pool.members.lock().unwrap().push(Arc::new(Member {
            conn,
            role: Mutex::new(Role::Active),
        }));
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
            changes,
            probes: Mutex::new(vec![]),
            resolver: Mutex::new(Arc::new(|host, port| {
                Box::pin(async move {
                    tokio::net::lookup_host((host.as_str(), port))
                        .await
                        .map(|addrs| addrs.collect())
                })
            })),
            external,
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

    /// Replaces the resolver of a `dns:///` target (tests); set before [`start`](Self::start).
    #[doc(hidden)]
    pub fn set_resolver(&self, resolver: Resolver) {
        *self.resolver.lock().unwrap() = resolver;
    }

    /// Starts every connection, the status reports and (for `dns:///`) the resolution loop.
    pub fn start(self: &Arc<Self>) {
        if self.external || self.started.swap(true, Ordering::SeqCst) {
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
    }

    /// True once at least one hub confirmed the registration (the client can send); see
    /// [`await_all_registered`](Self::await_all_registered) for every hub.
    pub async fn await_registered(&self, timeout: Duration) -> bool {
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

    /// True once every hub of the pool confirmed the registration (kraken-messaging's
    /// `HubConnections.awaitRegistered`): there is at least one connection, and every connection still in use
    /// (not a closed duplicate, not draining) has a handshaken stream. With `dns:///`, the hubs of
    /// the latest resolution.
    pub async fn await_all_registered(&self, timeout: Duration) -> bool {
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
            .filter(|m| m.role() == Role::Active)
            .collect();
        !active.is_empty() && active.iter().all(|m| m.conn.handshake().is_some())
    }

    /// The ids of the hubs live now (handshaken streams), in target order; a hub without an id
    /// (legacy, or an empty `RegisterAck.hub_id`) by its target as `host:port`.
    pub fn hub_ids(&self) -> Vec<String> {
        self.live().into_iter().map(|l| l.hub_id).collect()
    }

    /// Every connection of the pool, closed duplicates and draining ones included.
    pub fn connections(&self) -> Vec<Arc<HubConnection>> {
        self.members().iter().map(|m| Arc::clone(&m.conn)).collect()
    }

    /// The handshake a request with `routing_key` would be sent with now (the first choice),
    /// without taking a round-robin turn.
    pub fn handshake_for(&self, routing_key: &str) -> Option<Handshake> {
        let live = self.live();
        if routing_key.is_empty() {
            return live.into_iter().next().map(|l| l.handshake);
        }
        let ids: Vec<&str> = live.iter().map(|l| l.hub_id.as_str()).collect();
        let first = *rank_keyed(&ids, routing_key).first()?;
        Some(live[first].handshake.clone())
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
    /// for another stream; the last error (`HubUnavailable`, or `build`'s) is returned when no hub
    /// took it.
    pub(crate) async fn send_with<T>(
        &self,
        routing_key: &str,
        build: impl FnMut(StreamId, &Handshake) -> Result<(ClientMessage, T), MessagingError>,
    ) -> Result<(StreamId, T), MessagingError> {
        self.send_where(routing_key, None, build).await
    }

    /// [`send_with`](Self::send_with) over the live hubs whose ack lists `capability` only
    /// (`None`: every live hub). `UnsupportedByHub` when hubs are live but none lists it (nothing
    /// was written); `HubUnavailable` when none is live. The hubs are ranked on a snapshot of
    /// their handshakes; `build` sees the handshake of the stream current at the write and should
    /// check the capability again (a hub re-handshaken in between may have lost it).
    pub(crate) async fn send_where<T>(
        &self,
        routing_key: &str,
        capability: Option<&str>,
        mut build: impl FnMut(StreamId, &Handshake) -> Result<(ClientMessage, T), MessagingError>,
    ) -> Result<(StreamId, T), MessagingError> {
        let mut last = None;
        let ranked = self.ranked(routing_key, capability);
        if ranked.is_empty()
            && let Some(capability) = capability
            && !self.live().is_empty()
        {
            return Err(MessagingError::UnsupportedByHub(format!(
                "no hub this instance is registered on lists the \"{capability}\" capability; it needs a newer kraken-hub"
            )));
        }
        for conn in ranked {
            match conn
                .send_with(|stream, handshake| build(stream, handshake))
                .await
            {
                Ok(sent) => return Ok(sent),
                Err(e) => {
                    tracing::debug!(hub = %conn.address(), "not written, trying the next hub: {e}");
                    last = Some(e);
                }
            }
        }
        Err(last
            .unwrap_or_else(|| MessagingError::HubUnavailable("not connected to the hub".into())))
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

    /// Stops the background tasks and closes every stream (each hub drops this instance at once).
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let tasks: Vec<_> = self.tasks.lock().unwrap().drain(..).collect();
        for t in tasks {
            let _ = t.await;
        }
        let members = self.members();
        futures::future::join_all(members.iter().map(|m| m.conn.shutdown())).await;
    }

    fn members(&self) -> Vec<Arc<Member>> {
        self.members.lock().unwrap().clone()
    }

    fn live(&self) -> Vec<Live> {
        self.members()
            .into_iter()
            .filter(|m| m.role() == Role::Active)
            .filter_map(|m| {
                let handshake = m.conn.handshake()?;
                let hub_id = handshake.hub_id(m.address());
                Some(Live {
                    conn: Arc::clone(&m.conn),
                    hub_id,
                    handshake,
                })
            })
            .collect()
    }

    /// The live hubs (those whose ack lists `capability`, when given) in the order a request with
    /// `routing_key` tries them.
    fn ranked(&self, routing_key: &str, capability: Option<&str>) -> Vec<Arc<HubConnection>> {
        let mut live = self.live();
        if let Some(capability) = capability {
            live.retain(|l| l.handshake.shares(capability));
        }
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
        let mut settings = HubSettings {
            address: address.clone(),
            ..self.settings.clone()
        };
        // `dns:///name:port` resolves to `ip:port`, and tonic would check the certificate against
        // the IP. Check it against the DNS name unless `server_name` says otherwise (kraken-messaging
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
        );
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
                move |handshake| weak.upgrade().is_none_or(|p| p.accept(&address, handshake))
            }),
            changed: Box::new(move || {
                if let Some(pool) = weak.upgrade() {
                    pool.changes.send_modify(|n| *n += 1);
                    pool.top_up();
                }
            }),
            in_flight: Arc::clone(&self.in_flight),
        });
        let member = Arc::new(Member {
            conn,
            role: Mutex::new(Role::Active),
        });
        self.members.lock().unwrap().push(Arc::clone(&member));
        if self.started.load(Ordering::SeqCst) {
            member.conn.start();
        }
        member
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
    /// first connect attempt, plus the one at `including`.
    fn hubs(&self, including: Option<&str>) -> usize {
        self.members()
            .iter()
            .filter(|m| m.role() == Role::Active)
            .filter(|m| {
                Some(m.address()) == including
                    || m.conn.handshake().is_some()
                    || !m.conn.failed_once()
            })
            .count()
    }

    /// Fewer hubs now than when some live stream offered its permits: that stream is topped up to
    /// `ceil(max / N)` (shared with kraken-messaging). More hubs: nothing (permits are never
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
    /// active connection reached the same hub id first.
    fn accept(&self, address: &str, handshake: &Handshake) -> bool {
        let hub_id = handshake.hub_id(address);
        let mut owners = self.owners.lock().unwrap();
        // This connection may reach another hub than before (a restarted hub gets a new id).
        owners.retain(|_, owner| owner != address);
        let members = self.members();
        let active = |a: &str| {
            members
                .iter()
                .any(|m| m.address() == a && m.role() == Role::Active)
        };
        match owners.get(&hub_id) {
            Some(owner) if active(owner) => {
                tracing::warn!(service = %self.service_name, hub = %hub_id, address, kept = %owner, "two hub addresses reach the same hub; closing the second");
                if let Some(m) = members.iter().find(|m| m.address() == address) {
                    m.set_role(Role::Duplicate);
                }
                false
            }
            _ => {
                owners.insert(hub_id, address.to_string());
                true
            }
        }
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

    /// One `InstanceStatus` to every live hub sharing `instance-status`.
    pub(crate) async fn report_status(&self) {
        let live = self.live();
        let mut ids: Vec<String> = Vec::new();
        for l in live.iter().filter(|l| !l.handshake.is_legacy()) {
            if !ids.contains(&l.hub_id) {
                ids.push(l.hub_id.clone());
            }
        }
        let msg = ClientMessage {
            message: Some(client_message::Message::Status(InstanceStatus {
                connected_hub_ids: ids,
                handlers_in_flight: self.handlers_in_flight().min(u32::MAX as usize) as u32,
            })),
        };
        let writes = live
            .iter()
            .filter(|l| l.handshake.instance_status())
            .map(|l| {
                let msg = msg.clone();
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
        for address in removed {
            if let Some(m) = members.iter().find(|m| m.address() == address) {
                tracing::info!(service = %self.service_name, address, "hub address left DNS; closing once idle");
                self.drain(Arc::clone(m));
            }
        }
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
                while !me.idle(&member.conn) && tokio::time::Instant::now() < deadline {
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

    /// No handler of its requests running and none of our requests written to it unanswered.
    fn idle(&self, conn: &HubConnection) -> bool {
        let Some(stream) = conn.current_stream() else {
            return true;
        };
        conn.inbound_in_flight() == 0 && !self.probes.lock().unwrap().iter().any(|p| p(stream))
    }
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
    pending_on: fn(&S, StreamId) -> bool,
) -> PendingProbe {
    Box::new(move |stream| sender.upgrade().is_some_and(|s| pending_on(&s, stream)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
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
