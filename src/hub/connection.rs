//! The stream to the hub and its lifecycle: register, handshake, heartbeat, reconnect.
//!
//! Every stream starts with `Register` (protocol v2: `protocol_version`, `capabilities`,
//! `client_version`) followed by a `Heartbeat`. The hub handles a stream's messages in order:
//!
//! - a hub that speaks protocol v2 answers the `Register` with a `RegisterAck` before it echoes the
//!   heartbeat, so an ack seen first means a v2 hub ([`Handshake::V2`]): its heartbeat interval and
//!   timeout limits apply to this stream;
//! - an older hub never sends an ack, so the echo arriving first means a v1 hub
//!   ([`Handshake::Legacy`]): the configured heartbeat interval and today's timeouts apply.
//!
//! Either one completes the handshake (the registration is confirmed); only then does the stream
//! accept outgoing requests ([`HubConnection::send_with`]). A request that cannot be written to a
//! handshaken stream fails with `HubUnavailable`.
//!
//! Any failure closes the stream, notifies the owner with [`Inbound::Disconnected`] (carrying the
//! stream id and the requests still in its outbound buffer, never written) and reconnects with exponential backoff (`backoff_min` doubling up to `backoff_max`,
//! each wait jittered by ±20 % and clamped to `backoff_max`; at the cap 80-100 % of it), reset to `backoff_min` once an attempt
//! reached the handshake. An `ALREADY_EXISTS` or `INVALID_ARGUMENT` rejection is kept in
//! [`HubConnection::last_rejection`] and retried at `backoff_max`.
//!
//! Flow control (capability `flow-control`): the `Register` offers `max_concurrent_handlers`
//! permits; a hub whose ack shares the capability forwards only that many requests and queues the
//! rest. Each completed inbound request gives one permit back ([`ReplyTo::grant`]), on the stream
//! it arrived on. Per stream the client counts `initial` (the permits its `Register` offered),
//! `received` (requests that arrived), `returned` (permits written) and `pending` (completed, not
//! written yet); the hub still holds `initial - received + returned` permits for it. A completion
//! writes the pending grants in one `Permits` message at once when that count is at or below the
//! low-water mark `min(PERMIT_BATCH, initial / 2)` (with one permit: every completion) or when
//! [`PERMIT_BATCH`] grants are pending; otherwise they go [`PERMIT_FLUSH_AFTER`] after the first
//! one not sent yet. Grants of requests completed before the handshake follow the same rule once
//! the ack shares the capability. To a hub without the capability (P0 or v1) nothing is sent.
//!
//! The channel sends HTTP/2 keepalive pings (every 10 s, 5 s to answer, also while idle) so a
//! one-way partition is noticed by the transport too, not only by the missing heartbeat echoes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tonic::{Code, Status};

use crate::error::MessagingError;
use crate::proto::hub_client::HubClient;
use crate::proto::{
    self, ClientMessage, Heartbeat, Permits, Register, RegisterAck, client_message, server_message,
};
use crate::retry::{jittered, random_unit};

/// The hub protocol version this client speaks (`Register.protocol_version`).
pub const PROTOCOL_VERSION: u32 = 2;
/// The flow-control capability: permits, per-instance queues at the hub, RESOURCE_EXHAUSTED.
pub const FLOW_CONTROL: &str = "flow-control";
/// The multi-hub status capability (ADR 0016): `InstanceStatus` reports go only to a hub whose ack
/// lists it.
pub const INSTANCE_STATUS: &str = "instance-status";
/// The scatter-gather capability (ADR 0017): `Request.mode = SCATTER_GATHER`, answered with
/// `ReplyPart`s and a `ReplyEnd`; sent only to a hub whose ack lists it.
pub const SCATTER_GATHER: &str = "scatter-gather";
/// The streaming capability (ADR 0017, P4b): `Request.mode = STREAMING` with credits, answered
/// with `ReplyPart`s and a `ReplyEnd`; `Flow` and `Cancel` both ways. Declared by senders and by
/// handlers alike: the hub streams only to an instance that negotiated it.
pub const STREAMING: &str = "streaming";
/// The subscription capability (ADR 0017, P4c): `Request.mode = SUBSCRIPTION`, answered with the
/// initial result and every owner instance's updates as `ReplyPart`s and one `ReplyEnd`; `Flow`
/// and `Cancel` both ways. Declared by senders and by handlers alike: the hub subscribes only
/// instances that negotiated it.
pub const SUBSCRIPTION: &str = "subscription";
/// Optional protocol features this client declares (`Register.capabilities`).
pub const CAPABILITIES: &[&str] = &[
    FLOW_CONTROL,
    INSTANCE_STATUS,
    SCATTER_GATHER,
    STREAMING,
    SUBSCRIPTION,
];
/// A `Permits` message is sent as soon as this many grants are pending, or as soon as the hub holds
/// no more than `min(PERMIT_BATCH, initial_permits / 2)` permits for the stream (the low-water mark)...
pub const PERMIT_BATCH: u32 = 16;
/// ... else this long after the first grant not sent yet.
pub const PERMIT_FLUSH_AFTER: Duration = Duration::from_millis(50);
/// `Register.client_version`, shown in the hub's topology view.
pub const CLIENT_VERSION: &str = concat!("kraken-messaging-rs/", env!("CARGO_PKG_VERSION"));

/// HTTP/2 PING period on the hub channel.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// How long a PING may stay unanswered before the channel is dropped.
pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Identifies one stream (one connect attempt); strictly increasing and unique in the process, so
/// streams of different hubs never share an id.
pub type StreamId = u64;

static NEXT_STREAM: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct HubSettings {
    /// `host:port`, optionally prefixed with `static://` or `http://`. For a [`super::HubPool`]:
    /// the whole `hub_address` (a list or `dns:///name:port`, see [`super::HubTargets`]).
    pub address: String,
    /// Heartbeat period used until, and unless, the hub announces one in its `RegisterAck`
    /// (old hubs do not; they evict after 30 s, so this must stay well below that).
    pub heartbeat_interval: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// How long a new stream may take to complete the handshake (ack or first echo).
    pub handshake_timeout: Duration,
    /// Also `Register.initial_permits` (the hub forwards at most this many requests at once to a
    /// flow-controlled instance).
    pub max_concurrent_handlers: usize,
    /// `Register.load_factor` (100 = normal capacity).
    pub load_factor: u32,
    /// `Register.tags`.
    pub tags: HashMap<String, String>,
    /// [`super::HubPool`] only: how often `InstanceStatus` goes to every hub sharing
    /// `instance-status`.
    pub status_interval: Duration,
    /// [`super::HubPool`] only: how often a `dns:///` address is resolved again.
    pub dns_refresh: Duration,
}

impl Default for HubSettings {
    fn default() -> Self {
        HubSettings {
            address: "static://kraken-hub:7000".into(),
            heartbeat_interval: Duration::from_secs(10),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(10),
            max_concurrent_handlers: 256,
            load_factor: 100,
            tags: HashMap::new(),
            status_interval: Duration::from_secs(5),
            dns_refresh: super::targets::DNS_REFRESH,
        }
    }
}

/// What the hub told us about itself on the current stream.
#[derive(Clone, Debug, PartialEq)]
pub enum Handshake {
    /// The hub answered the `Register` with this ack before echoing the heartbeat (protocol v2).
    V2(RegisterAck),
    /// The echo arrived without an ack: a hub older than protocol v2.
    Legacy,
}

impl Handshake {
    pub fn is_legacy(&self) -> bool {
        matches!(self, Handshake::Legacy)
    }

    /// The id of the hub reached at `address`: `RegisterAck.hub_id`, or, for a hub that names
    /// none (legacy, or an empty id), the target as `host:port` ([`super::targets::target_id`]).
    pub fn hub_id(&self, address: &str) -> String {
        match self.ack() {
            Some(ack) if !ack.hub_id.is_empty() => ack.hub_id.clone(),
            _ => super::targets::target_id(address),
        }
    }

    pub fn ack(&self) -> Option<&RegisterAck> {
        match self {
            Handshake::V2(ack) => Some(ack),
            Handshake::Legacy => None,
        }
    }

    /// True when the hub shares `flow-control`: it forwards only with permits.
    pub fn flow_control(&self) -> bool {
        self.shares(FLOW_CONTROL)
    }

    /// True when the hub shares `instance-status`: it keeps our `InstanceStatus` reports.
    pub fn instance_status(&self) -> bool {
        self.shares(INSTANCE_STATUS)
    }

    /// True when the hub shares `scatter-gather`: it fans a scatter-gather query out and knows
    /// `Register.shared_query_types` (an older hub ignores that field).
    pub fn scatter_gather(&self) -> bool {
        self.shares(SCATTER_GATHER)
    }

    /// True when the hub shares `streaming`: it routes streaming queries, relays their parts
    /// under the sender's credits and sends `Flow` and `Cancel`.
    pub fn streaming(&self) -> bool {
        self.shares(STREAMING)
    }

    /// True when the hub shares `subscription`: it routes subscription queries to every capable
    /// owner instance, relays their updates under the sender's credit and sends `Cancel`.
    pub fn subscription(&self) -> bool {
        self.shares(SUBSCRIPTION)
    }

    pub(crate) fn shares(&self, capability: &str) -> bool {
        self.ack()
            .is_some_and(|a| a.capabilities.iter().any(|c| c == capability))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GrantMode {
    /// Before the handshake: grants are held until the hub says whether it wants them.
    Undecided,
    On,
    Off,
}

/// The permit accounting of one stream.
#[derive(Debug)]
struct GrantState {
    mode: GrantMode,
    /// `Register.initial_permits` sent on this stream.
    initial: u32,
    /// Requests that arrived on this stream (whatever the mode).
    received: u64,
    /// Permits written in `Permits` messages on this stream.
    returned: u64,
    /// Requests completed whose permit is not written yet.
    pending: u32,
    /// The flush timer is armed.
    timer: bool,
}

/// What a grant (or the handshake) leaves to do once the lock is released.
enum Flush {
    /// Write a `Permits` message with this count now.
    Now(u32),
    /// Spawn the flush timer.
    Arm,
    Nothing,
}

impl GrantState {
    /// Permits the hub still holds for this stream: `initial - received + returned`.
    fn hub_held(&self) -> i128 {
        i128::from(self.initial) - i128::from(self.received) + i128::from(self.returned)
    }

    /// The flush rule for pending grants: at once when the hub is down to the low-water mark
    /// (`min(PERMIT_BATCH, initial / 2)` permits) or [`PERMIT_BATCH`] grants are pending, else
    /// with the timer (armed once).
    fn next(&mut self) -> Flush {
        if self.pending == 0 {
            return Flush::Nothing;
        }
        let low_water = i128::from(PERMIT_BATCH.min(self.initial / 2));
        if self.hub_held() <= low_water || self.pending >= PERMIT_BATCH {
            Flush::Now(self.take())
        } else if !self.timer {
            self.timer = true;
            Flush::Arm
        } else {
            Flush::Nothing
        }
    }

    /// Takes the pending grants for one `Permits` message.
    fn take(&mut self) -> u32 {
        let count = std::mem::take(&mut self.pending);
        self.returned += u64::from(count);
        count
    }
}

/// Permit grants of one stream: returned at once when the hub runs low, else batched.
#[derive(Debug)]
pub(crate) struct Grants {
    tx: mpsc::WeakSender<ClientMessage>,
    state: Mutex<GrantState>,
    /// Requests that arrived and are not finished yet (the connection's and the pool's count).
    in_flight: Vec<Arc<AtomicUsize>>,
}

impl Grants {
    #[cfg(test)]
    fn new(tx: mpsc::WeakSender<ClientMessage>, initial: u32) -> Arc<Grants> {
        Self::counting(tx, initial, vec![])
    }

    fn counting(
        tx: mpsc::WeakSender<ClientMessage>,
        initial: u32,
        in_flight: Vec<Arc<AtomicUsize>>,
    ) -> Arc<Grants> {
        Arc::new(Grants {
            in_flight,
            tx,
            state: Mutex::new(GrantState {
                mode: GrantMode::Undecided,
                initial,
                received: 0,
                returned: 0,
                pending: 0,
                timer: false,
            }),
        })
    }

    /// The permits this stream offered so far (its `Register` plus top-ups).
    fn initial(&self) -> u32 {
        self.state.lock().unwrap().initial
    }

    /// Raises the permits this stream offered to `share` when it offered fewer (a flow-controlled
    /// stream only); returns the top-up to write. Never lowers them.
    fn raise(&self, share: u32) -> u32 {
        let mut s = self.state.lock().unwrap();
        if s.mode != GrantMode::On || share <= s.initial {
            return 0;
        }
        let count = share - s.initial;
        s.initial = share;
        count
    }

    /// A request arrived on this stream (the hub spent one of our permits on it).
    fn received(&self) {
        self.state.lock().unwrap().received += 1;
        for c in &self.in_flight {
            c.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The request is finished: one less in flight (never below zero).
    fn finished(&self) {
        for c in &self.in_flight {
            let _ = c.try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
        }
    }

    /// The hub decided (handshake): with flow control, grants held so far follow the flush rule;
    /// without, they are dropped and later grants are no-ops.
    async fn decide(self: &Arc<Self>, flow_control: bool) {
        let next = {
            let mut s = self.state.lock().unwrap();
            if s.mode != GrantMode::Undecided {
                return;
            }
            if flow_control {
                s.mode = GrantMode::On;
                s.next()
            } else {
                s.mode = GrantMode::Off;
                s.pending = 0;
                Flush::Nothing
            }
        };
        self.run(next).await;
    }

    async fn grant(self: &Arc<Self>) {
        self.finished();
        let next = {
            let mut s = self.state.lock().unwrap();
            match s.mode {
                GrantMode::Off => return,
                GrantMode::Undecided => {
                    s.pending += 1;
                    return;
                }
                GrantMode::On => {
                    s.pending += 1;
                    s.next()
                }
            }
        };
        self.run(next).await;
    }

    async fn run(self: &Arc<Self>, next: Flush) {
        match next {
            Flush::Now(count) => self.send(count, 0).await,
            Flush::Arm => {
                let me = Arc::clone(self);
                tokio::spawn(async move { me.flush_later().await });
            }
            Flush::Nothing => {}
        }
    }

    async fn flush_later(self: Arc<Self>) {
        tokio::time::sleep(PERMIT_FLUSH_AFTER).await;
        let count = {
            let mut s = self.state.lock().unwrap();
            s.timer = false;
            s.take()
        };
        if count > 0 {
            self.send(count, 0).await;
        }
    }

    /// Writes `Permits{count, grant}` (`grant` 0: plain grants; see [`HubConnection::top_up`]).
    /// Best effort: a stream that is gone takes its permits with it (the hub resets them).
    async fn send(&self, count: u32, grant: u32) {
        if let Some(tx) = self.tx.upgrade() {
            let _ = tx
                .send(ClientMessage {
                    message: Some(client_message::Message::Permits(Permits { count, grant })),
                })
                .await;
        }
    }
}

/// The stream an inbound request arrived on. Holds only a weak handle, so an in-flight handler
/// never keeps a dead stream's request body open; replies to a stream that is gone are dropped.
#[derive(Clone, Debug)]
pub struct ReplyTo {
    stream: StreamId,
    tx: mpsc::WeakSender<ClientMessage>,
    grants: Arc<Grants>,
}

impl ReplyTo {
    pub fn stream(&self) -> StreamId {
        self.stream
    }

    /// Writes `msg` to the arrival stream; `HubUnavailable` when that stream has closed.
    pub async fn send(&self, msg: ClientMessage) -> Result<(), MessagingError> {
        let tx = self.tx.upgrade().ok_or_else(|| {
            MessagingError::HubUnavailable(format!("hub stream {} is gone", self.stream))
        })?;
        tx.send(msg).await.map_err(|_| {
            MessagingError::HubUnavailable(format!("hub stream {} closed", self.stream))
        })
    }

    /// One permit back to the arrival stream's hub, once the request is finished (its reply
    /// written or dropped). Written at once when the hub runs low on our permits, else batched
    /// (see the module docs); a no-op when that hub has no flow control or the stream is gone.
    pub async fn grant(&self) {
        self.grants.grant().await;
    }
}

/// The types an instance announces in every `Register`.
///
/// `queries` are exclusive (one owning service), `shared_queries` may be owned by many services
/// (ADR 0017). Every registrant of a type must agree on exclusive vs shared: the hub rejects the
/// later `Register` with `ALREADY_EXISTS` otherwise.
///
/// A streaming query (P4b) is a query type to the hub: the mode is the sender's choice and the
/// hub streams only to instances that negotiated `streaming`. `streaming_queries` are the
/// exclusive ones, announced in `Register.query_types` next to `queries`; a shared streaming
/// query is listed in `shared_queries` like any shared query. A subscription query (P4c) is the
/// same: `subscription_queries` are the exclusive ones, a shared one is in `shared_queries`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegisteredTypes {
    pub commands: Vec<String>,
    /// Exclusive unary queries.
    pub queries: Vec<String>,
    /// Shared queries, unary, streaming or subscription.
    pub shared_queries: Vec<String>,
    /// Exclusive streaming queries.
    pub streaming_queries: Vec<String>,
    /// Exclusive subscription queries.
    pub subscription_queries: Vec<String>,
}

impl RegisteredTypes {
    fn sorted(mut self) -> Self {
        self.commands.sort();
        self.queries.sort();
        self.shared_queries.sort();
        self.streaming_queries.sort();
        self.subscription_queries.sort();
        self
    }

    /// `Register.query_types`: the exclusive queries, unary, streaming and subscription, sorted.
    fn exclusive_query_types(&self) -> Vec<String> {
        let mut all = self.queries.clone();
        all.extend(self.streaming_queries.iter().cloned());
        all.extend(self.subscription_queries.iter().cloned());
        all.sort();
        all.dedup();
        all
    }
}

/// What the hub sends us, handed to the owner (dispatcher and sender) in order.
pub enum Inbound {
    /// A request for one of our handlers; the reply goes to `reply_to`, the stream it arrived on.
    Request {
        request: proto::Request,
        reply_to: ReplyTo,
    },
    Reply(proto::Reply),
    Error(proto::Error),
    /// One answer of a scatter-gather, or one part of a streaming query, this client sent
    /// (ADR 0017).
    Part(proto::ReplyPart),
    /// The end of a scatter-gather or of a streaming query this client sent.
    End(proto::ReplyEnd),
    /// Credits for a streaming query this instance handles (P4b): `request_id` is the
    /// handler-side id (`<id>#1`).
    Flow(proto::Flow),
    /// The hub cancels a request this instance handles (P4b): a streaming query (the sender
    /// left or stopped reading, the hub's limits), or a unary / scatter-gather target whose
    /// gather ended early. Nothing is to be sent for it any more.
    Cancel(proto::Cancel),
    /// That stream was lost. `unsent`: the ids of requests still in its outbound buffer when it
    /// was lost (never taken by the transport, so never written): they may go to another hub with
    /// the same id. Every other request written to it and still pending has an unknown outcome.
    Disconnected {
        stream: StreamId,
        unsent: Vec<String>,
    },
}

impl std::fmt::Debug for Inbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print payloads or headers (they may carry a bearer token).
        match self {
            Inbound::Request { request, reply_to } => write!(
                f,
                "Request(id={}, type={}, stream={})",
                request.id, request.r#type, reply_to.stream
            ),
            Inbound::Reply(r) => write!(f, "Reply(request_id={})", r.request_id),
            Inbound::Error(e) => write!(f, "Error(request_id={}, code={})", e.request_id, e.code),
            Inbound::Part(p) => write!(f, "Part(request_id={}, seq={})", p.request_id, p.seq),
            Inbound::End(e) => write!(f, "End(request_id={}, parts={})", e.request_id, e.parts),
            Inbound::Flow(x) => write!(
                f,
                "Flow(request_id={}, credits={})",
                x.request_id, x.credits
            ),
            Inbound::Cancel(c) => write!(
                f,
                "Cancel(request_id={}, reason={})",
                c.request_id, c.reason
            ),
            Inbound::Disconnected { stream, unsent } => {
                write!(f, "Disconnected(stream={stream}, unsent={})", unsent.len())
            }
        }
    }
}

/// A handshaken stream that accepts outgoing messages.
struct Live {
    stream: StreamId,
    tx: mpsc::Sender<ClientMessage>,
    handshake: Handshake,
    grants: Arc<Grants>,
}

pub struct HubConnection {
    settings: HubSettings,
    service_name: String,
    instance_id: String,
    inbound: mpsc::Sender<Inbound>,
    types: Mutex<RegisteredTypes>,
    live: Mutex<Option<Live>>,
    connected: AtomicBool,
    handshake: watch::Sender<Option<Handshake>>,
    last_rejection: Mutex<Option<String>>,
    /// A stream of this connection failed or ended at least once.
    failed_once: AtomicBool,
    /// Requests that arrived on this connection's streams and are not finished yet.
    in_flight: Arc<AtomicUsize>,
    hooks: OnceLock<Hooks>,
    reregistering: tokio::sync::Mutex<()>,
    /// The outbound buffer of the current stream, also read by the transport ([`Outbound`]).
    outbound: Mutex<Option<OutboundBuffer>>,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

type OutboundBuffer = Arc<Mutex<mpsc::Receiver<ClientMessage>>>;

/// The request body of a stream: the messages of its outbound buffer, taken one at a time by the
/// transport. The buffer is shared so that, once the stream is lost, what the transport never
/// took can be told apart ([`HubConnection::take_unsent`]).
struct Outbound(OutboundBuffer);

impl tokio_stream::Stream for Outbound {
    type Item = ClientMessage;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<ClientMessage>> {
        self.0.lock().unwrap().poll_recv(cx)
    }
}

/// How a [`super::HubPool`] takes part in a member connection's handshake.
pub(crate) struct Hooks {
    /// `Register.initial_permits` for the stream being opened.
    pub initial_permits: Box<dyn Fn() -> u32 + Send + Sync>,
    /// Called with the handshake before the stream goes live; `false` closes the connection for
    /// good (another connection already reaches that hub).
    pub accept: Box<dyn Fn(&Handshake) -> bool + Send + Sync>,
    /// A stream went live or was lost, or the first connect attempt failed.
    pub changed: Box<dyn Fn() + Send + Sync>,
    /// Also counts requests that arrived and are not finished (all connections of the pool).
    pub in_flight: Arc<AtomicUsize>,
}

impl HubConnection {
    pub fn new(
        settings: HubSettings,
        service_name: String,
        instance_id: String,
        inbound: mpsc::Sender<Inbound>,
    ) -> Arc<HubConnection> {
        let (handshake, _) = watch::channel(None);
        Arc::new(HubConnection {
            settings,
            service_name,
            instance_id,
            inbound,
            types: Mutex::new(RegisteredTypes::default()),
            live: Mutex::new(None),
            connected: AtomicBool::new(false),
            handshake,
            last_rejection: Mutex::new(None),
            failed_once: AtomicBool::new(false),
            in_flight: Arc::new(AtomicUsize::new(0)),
            hooks: OnceLock::new(),
            reregistering: tokio::sync::Mutex::new(()),
            outbound: Mutex::new(None),
            cancel: CancellationToken::new(),
            task: Mutex::new(None),
        })
    }

    /// The command and exclusive query types announced in every `Register`, no shared query
    /// types; set before [`start`](Self::start). See
    /// [`set_registered_types`](Self::set_registered_types).
    pub fn set_types(&self, command_types: Vec<String>, query_types: Vec<String>) {
        self.set_registered_types(RegisteredTypes {
            commands: command_types,
            queries: query_types,
            ..RegisteredTypes::default()
        });
    }

    /// The types announced in every `Register`; set before [`start`](Self::start).
    pub fn set_registered_types(&self, types: RegisteredTypes) {
        *self.types.lock().unwrap() = types.sorted();
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// The target this connection dials (`HubSettings.address`).
    pub fn address(&self) -> &str {
        &self.settings.address
    }

    /// The id of the hub behind the current handshaken stream ([`Handshake::hub_id`]): its
    /// `RegisterAck.hub_id`, else its target as `host:port`. `None` while there is no handshaken
    /// stream.
    pub fn hub_id(&self) -> Option<String> {
        self.handshake().map(|h| h.hub_id(&self.settings.address))
    }

    /// Requests that arrived on this connection and whose handler has not finished
    /// ([`ReplyTo::grant`] not called yet).
    pub fn inbound_in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// True once a stream of this connection failed or ended (the first connect attempt is over
    /// without a lasting stream).
    pub(crate) fn failed_once(&self) -> bool {
        self.failed_once.load(Ordering::SeqCst)
    }

    pub(crate) fn set_hooks(&self, hooks: Hooks) {
        let _ = self.hooks.set(hooks);
    }

    fn changed(&self) {
        if let Some(h) = self.hooks.get() {
            (h.changed)();
        }
    }

    /// Spawns the connect loop. Returns immediately; use [`await_registered`](Self::await_registered).
    pub fn start(self: &Arc<Self>) {
        let me = Arc::clone(self);
        let handle = tokio::spawn(async move { me.connect_loop().await });
        *self.task.lock().unwrap() = Some(handle);
    }

    /// True once the current stream completed its handshake (ack or first echo), which confirms
    /// the registration.
    pub async fn await_registered(&self, timeout: Duration) -> bool {
        let mut rx = self.handshake.subscribe();
        if rx.borrow().is_some() {
            return true;
        }
        tokio::time::timeout(timeout, async {
            while rx.changed().await.is_ok() {
                if rx.borrow().is_some() {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false)
    }

    /// The handshake of the current stream, `None` while there is no handshaken stream.
    pub fn handshake(&self) -> Option<Handshake> {
        self.handshake.borrow().clone()
    }

    /// The id of the current handshaken stream.
    pub fn current_stream(&self) -> Option<StreamId> {
        self.live.lock().unwrap().as_ref().map(|l| l.stream)
    }

    /// True while a stream is open (the `Register` was written), handshaken or not.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub fn last_rejection(&self) -> Option<String> {
        self.last_rejection.lock().unwrap().clone()
    }

    /// Writes to the current handshaken stream and returns its id; `HubUnavailable` when there is
    /// none or the write fails.
    pub async fn send(&self, msg: ClientMessage) -> Result<StreamId, MessagingError> {
        self.send_with(|_, _| Ok((msg, ()))).await.map(|(s, ())| s)
    }

    /// Builds a message for the current handshaken stream and writes it there. `build` runs while
    /// the stream is still current, with its id and handshake, so whatever it records (a pending
    /// request tagged with the stream id) happens before that stream's
    /// [`Inbound::Disconnected`] can be emitted; it may refuse the stream (its handshake lacks
    /// what the message needs) with an error, which is returned and nothing is written. Returns
    /// the stream id and `build`'s extra value; `HubUnavailable` when there is no handshaken
    /// stream or the write fails (nothing was written).
    pub async fn send_with<T>(
        &self,
        build: impl FnOnce(StreamId, &Handshake) -> Result<(ClientMessage, T), MessagingError>,
    ) -> Result<(StreamId, T), MessagingError> {
        let (stream, tx, msg, extra) = {
            let live = self.live.lock().unwrap();
            let Some(live) = live.as_ref() else {
                return Err(MessagingError::HubUnavailable(
                    "not connected to the hub".into(),
                ));
            };
            let (msg, extra) = build(live.stream, &live.handshake)?;
            (live.stream, live.tx.clone(), msg, extra)
        };
        tx.send(msg)
            .await
            .map_err(|_| MessagingError::HubUnavailable("hub stream closed".into()))?;
        Ok((stream, extra))
    }

    /// Writes `msg` to the handshaken stream `stream` only: `HubUnavailable` when that stream is
    /// not the current one (it was lost, and so was whatever `msg` refers to) or the write
    /// fails. For a `Flow` or `Cancel` of a request written to `stream`: a newer stream is
    /// another session, which knows nothing of the request.
    pub async fn send_on(
        &self,
        stream: StreamId,
        msg: ClientMessage,
    ) -> Result<(), MessagingError> {
        let tx = {
            let live = self.live.lock().unwrap();
            match live.as_ref() {
                Some(live) if live.stream == stream => live.tx.clone(),
                _ => {
                    return Err(MessagingError::HubUnavailable(format!(
                        "hub stream {stream} is gone"
                    )));
                }
            }
        };
        tx.send(msg)
            .await
            .map_err(|_| MessagingError::HubUnavailable("hub stream closed".into()))
    }

    /// Tops the current stream up to `share` permits in all (the initial offer plus earlier
    /// top-ups) with one `Permits` message, when its hub shares flow control and the stream
    /// offered fewer; never lowers them ([`super::HubPool`]: a hub left, the others take its share).
    /// The message is `Permits{count = share - offered, grant = share}`: the grant raises the
    /// hub's permit cap on the session (`initial_permits` until then) so the hub keeps the top-up;
    /// a hub that predates `Permits.grant` ignores it and clamps the top-up away.
    pub(crate) async fn top_up(&self, share: u32) {
        let Some((stream, grants)) = self
            .live
            .lock()
            .unwrap()
            .as_ref()
            .map(|l| (l.stream, Arc::clone(&l.grants)))
        else {
            return;
        };
        let count = grants.raise(share);
        if count > 0 {
            tracing::debug!(service = %self.service_name, address = %self.settings.address, stream, count, share, "permits topped up");
            grants.send(count, share).await;
        }
    }

    /// Writes the `Register` again, with the types set now ([`set_types`](Self::set_types)), to
    /// the current handshaken stream. With no types the hub stops routing to this instance there
    /// and hands its queue to other instances, while replies to requests it already forwarded
    /// still go through. `HubUnavailable` when there is no handshaken stream.
    pub(crate) async fn reregister(&self) -> Result<(), MessagingError> {
        // One at a time: the message built last (with the types set last) is written last.
        let _order = self.reregistering.lock().await;
        let (tx, initial) = {
            let live = self.live.lock().unwrap();
            let live = live
                .as_ref()
                .ok_or_else(|| MessagingError::HubUnavailable("not connected to the hub".into()))?;
            (live.tx.clone(), live.grants.initial())
        };
        tx.send(self.register(initial))
            .await
            .map_err(|_| MessagingError::HubUnavailable("hub stream closed".into()))
    }

    /// Waits (at most `timeout`) until everything queued for the current stream has been taken by
    /// the transport, so closing it does not drop a last reply or `Register`.
    pub(crate) async fn flushed(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let pending = self
                .live
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|l| l.tx.capacity() < l.tx.max_capacity());
            if !pending || tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Makes writes to the current stream fail as if it had just dropped and the connection had
    /// not noticed yet (tests of the failover of requests that were never written).
    #[doc(hidden)]
    pub fn sever_writes(&self) {
        if let Some(live) = self.live.lock().unwrap().as_mut() {
            let (closed, _) = mpsc::channel(1);
            live.tx = closed;
        }
    }

    /// Stops the loop and closes the stream (the hub then drops this instance immediately).
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let handle = self.task.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        *self.live.lock().unwrap() = None;
        self.connected.store(false, Ordering::SeqCst);
        self.handshake.send_replace(None);
    }

    async fn connect_loop(self: Arc<Self>) {
        let mut backoff = self.settings.backoff_min;
        let mut failures: u32 = 0;
        while !self.cancel.is_cancelled() {
            let stream = NEXT_STREAM.fetch_add(1, Ordering::SeqCst);
            let outcome = self.run_stream(stream).await;
            self.connected.store(false, Ordering::SeqCst);
            // The first attempt is over: the pool no longer counts this hub in its split.
            let first_failure = !self.failed_once.swap(true, Ordering::SeqCst);
            // Clear the live stream before announcing the loss: a request is tagged with a stream
            // only while that stream is live, so it is always failed by this Disconnected.
            *self.live.lock().unwrap() = None;
            let unsent = self.take_unsent();
            let reached_handshake = self.handshake.send_replace(None).is_some();
            let _ = self
                .inbound
                .send(Inbound::Disconnected { stream, unsent })
                .await;
            if reached_handshake || first_failure {
                self.changed();
            }
            if self.cancel.is_cancelled() {
                break;
            }
            let status = match outcome {
                Ok(()) => Status::unavailable("hub closed the stream"),
                Err(s) => s,
            };
            // A hub that answered the Register (ack or echo) is healthy: come back quickly. One that
            // accepts the stream and then rejects it never reaches the handshake.
            if reached_handshake {
                backoff = self.settings.backoff_min;
                failures = 0;
            }
            failures += 1;
            let max = self.settings.backoff_max;
            let wait = if matches!(status.code(), Code::AlreadyExists | Code::InvalidArgument) {
                *self.last_rejection.lock().unwrap() = Some(status.message().to_string());
                let wait = jittered(max, max, random_unit());
                tracing::error!(service = %self.service_name, instance = %self.instance_id, address = %self.settings.address, reason = status.message(), retry_in = ?wait, "hub rejected the registration");
                wait
            } else {
                let wait = jittered(backoff, max, random_unit());
                if failures == 1 {
                    tracing::warn!(service = %self.service_name, instance = %self.instance_id, address = %self.settings.address, reason = %describe(&status), retry_in = ?wait, "hub stream closed; reconnecting");
                } else {
                    tracing::debug!(service = %self.service_name, instance = %self.instance_id, address = %self.settings.address, reason = %describe(&status), retry_in = ?wait, "hub stream closed; reconnecting");
                }
                backoff = (backoff * 2).min(max);
                wait
            };
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.cancel.cancelled() => break,
            }
        }
    }

    /// One stream from connect to failure. `Ok(())` when the hub ended it or we were cancelled.
    async fn run_stream(&self, stream_id: StreamId) -> Result<(), Status> {
        let endpoint = Endpoint::from_shared(grpc_target(&self.settings.address))
            .map_err(|e| Status::invalid_argument(format!("bad hub address: {e}")))?
            .connect_timeout(self.settings.handshake_timeout)
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true);
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let mut client = HubClient::new(channel);
        let (tx, rx) = mpsc::channel::<ClientMessage>(256);
        let rx = Arc::new(Mutex::new(rx));
        *self.outbound.lock().unwrap() = Some(Arc::clone(&rx));
        // A peer that accepts TCP but never answers the HTTP/2 handshake must not hang the loop.
        let mut stream = tokio::time::timeout(
            self.settings.handshake_timeout,
            client.connect(Outbound(rx)),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded("hub did not answer the connect handshake in time")
        })??
        .into_inner();

        // The Register is the first message of every stream; the ack or the echo of the heartbeat
        // after it completes the handshake.
        let initial_permits = match self.hooks.get() {
            Some(h) => (h.initial_permits)(),
            None => self.settings.max_concurrent_handlers.min(u32::MAX as usize) as u32,
        };
        tx.send(self.register(initial_permits))
            .await
            .map_err(|_| Status::unavailable("stream closed before Register"))?;
        tx.send(heartbeat())
            .await
            .map_err(|_| Status::unavailable("stream closed before first heartbeat"))?;
        self.connected.store(true, Ordering::SeqCst);
        let mut counters = vec![Arc::clone(&self.in_flight)];
        counters.extend(self.hooks.get().map(|h| Arc::clone(&h.in_flight)));
        let grants = Grants::counting(tx.downgrade(), initial_permits, counters);
        let reply_to = ReplyTo {
            stream: stream_id,
            tx: tx.downgrade(),
            grants: Arc::clone(&grants),
        };

        let mut interval = self.settings.heartbeat_interval;
        let mut ticker = ticker(interval);
        let mut last_echo = tokio::time::Instant::now();
        let handshake_deadline = tokio::time::Instant::now() + self.settings.handshake_timeout;
        let mut handshake: Option<Handshake> = None;
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep_until(handshake_deadline), if handshake.is_none() => {
                    return Err(Status::deadline_exceeded("registration not confirmed in time"));
                }
                _ = ticker.tick() => {
                    let echo_timeout = interval * 3;
                    if last_echo.elapsed() > echo_timeout {
                        return Err(Status::unavailable(format!("no heartbeat echo for {echo_timeout:?}")));
                    }
                    tx.send(heartbeat()).await.map_err(|_| Status::unavailable("stream closed"))?;
                }
                msg = stream.message() => match msg? {
                    None => return Ok(()),
                    Some(proto::ServerMessage { message: None }) => {}
                    Some(proto::ServerMessage { message: Some(m) }) => match m {
                        server_message::Message::RegisterAck(ack) => match &handshake {
                            None => {
                                if ack.heartbeat_interval_ms > 0 {
                                    interval = Duration::from_millis(ack.heartbeat_interval_ms.into());
                                    ticker = self::ticker(interval);
                                }
                                let h = Handshake::V2(ack);
                                if !self.accept(&h) {
                                    return Ok(());
                                }
                                grants.decide(h.flow_control()).await;
                                let ack = h.ack().expect("v2");
                                tracing::info!(service = %self.service_name, instance = %self.instance_id, hub = %ack.hub_id, address = %self.settings.address, protocol = ack.protocol_version, heartbeat = ?interval, flow_control = h.flow_control(), "registered with the hub");
                                self.go_live(stream_id, &tx, &h, &grants);
                                handshake = Some(h);
                            }
                            // Every later Register is acked too; the values do not change.
                            Some(Handshake::V2(_)) => {
                                let h = Handshake::V2(ack);
                                self.go_live(stream_id, &tx, &h, &grants);
                                handshake = Some(h);
                            }
                            Some(Handshake::Legacy) => {
                                tracing::debug!(service = %self.service_name, "RegisterAck after the echo on a legacy stream ignored");
                            }
                        },
                        server_message::Message::Heartbeat(_) => {
                            last_echo = tokio::time::Instant::now();
                            if handshake.is_none() {
                                tracing::info!(service = %self.service_name, instance = %self.instance_id, address = %self.settings.address, heartbeat = ?interval, "registered with the hub (legacy hub without protocol v2)");
                                let h = Handshake::Legacy;
                                if !self.accept(&h) {
                                    return Ok(());
                                }
                                grants.decide(false).await;
                                self.go_live(stream_id, &tx, &h, &grants);
                                handshake = Some(h);
                            }
                        }
                        server_message::Message::Request(request) => {
                            grants.received();
                            self.forward(Inbound::Request { request, reply_to: reply_to.clone() }).await?
                        }
                        server_message::Message::Reply(r) => self.forward(Inbound::Reply(r)).await?,
                        server_message::Message::Error(e) => self.forward(Inbound::Error(e)).await?,
                        server_message::Message::Part(p) => self.forward(Inbound::Part(p)).await?,
                        server_message::Message::End(e) => self.forward(Inbound::End(e)).await?,
                        server_message::Message::Flow(x) => self.forward(Inbound::Flow(x)).await?,
                        server_message::Message::Cancel(c) => self.forward(Inbound::Cancel(c)).await?,
                    },
                },
            }
        }
    }

    /// Closes the lost stream's outbound buffer (later writes fail and go to another hub) and
    /// returns the ids of the requests still in it: the transport never took them, so they were
    /// never written. One the transport took may be on the wire or lost with it: unknown.
    fn take_unsent(&self) -> Vec<String> {
        let Some(buffer) = self.outbound.lock().unwrap().take() else {
            return vec![];
        };
        let mut rx = buffer.lock().unwrap();
        rx.close();
        let mut unsent = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let Some(client_message::Message::Request(r)) = msg.message {
                unsent.push(r.id);
            }
        }
        unsent
    }

    /// The `Register` of this instance with the types set now.
    fn register(&self, initial_permits: u32) -> ClientMessage {
        let types = self.types.lock().unwrap().clone();
        let register = Register {
            service_name: self.service_name.clone(),
            instance_id: self.instance_id.clone(),
            command_types: types.commands.clone(),
            query_types: types.exclusive_query_types(),
            protocol_version: PROTOCOL_VERSION,
            capabilities: CAPABILITIES.iter().map(|c| c.to_string()).collect(),
            credentials: None,
            client_version: CLIENT_VERSION.to_string(),
            initial_permits,
            load_factor: self.settings.load_factor,
            tags: self.settings.tags.clone(),
            shared_query_types: types.shared_queries.clone(),
        };
        ClientMessage {
            message: Some(client_message::Message::Register(register)),
        }
    }

    fn go_live(
        &self,
        stream: StreamId,
        tx: &mpsc::Sender<ClientMessage>,
        handshake: &Handshake,
        grants: &Arc<Grants>,
    ) {
        *self.live.lock().unwrap() = Some(Live {
            stream,
            tx: tx.clone(),
            handshake: handshake.clone(),
            grants: Arc::clone(grants),
        });
        let first = self
            .handshake
            .send_replace(Some(handshake.clone()))
            .is_none();
        if first {
            self.changed();
        }
    }

    /// Asks the pool (if any) whether this stream may go live; a refusal stops the connection.
    fn accept(&self, handshake: &Handshake) -> bool {
        let accepted = self.hooks.get().is_none_or(|h| (h.accept)(handshake));
        if !accepted {
            self.cancel.cancel();
        }
        accepted
    }

    async fn forward(&self, msg: Inbound) -> Result<(), Status> {
        self.inbound
            .send(msg)
            .await
            .map_err(|_| Status::cancelled("owner dropped the inbound channel"))
    }
}

/// A ticker whose first tick is one `period` from now (the first heartbeat is sent with the
/// `Register`).
fn ticker(period: Duration) -> tokio::time::Interval {
    let mut t = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    t
}

/// `static://host:port` or `host:port` → `http://host:port`; explicit `http(s)://` kept.
pub(crate) fn grpc_target(address: &str) -> String {
    let stripped = address.strip_prefix("static://").unwrap_or(address);
    if stripped.starts_with("http://") || stripped.starts_with("https://") {
        stripped.to_string()
    } else {
        format!("http://{stripped}")
    }
}

fn heartbeat() -> ClientMessage {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    ClientMessage {
        message: Some(client_message::Message::Heartbeat(Heartbeat {
            sent_at_ms: now,
        })),
    }
}

fn describe(status: &Status) -> String {
    if status.message().is_empty() {
        format!("{:?}", status.code())
    } else {
        format!("{:?}: {}", status.code(), status.message())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_forms() {
        assert_eq!(grpc_target("static://hub:7000"), "http://hub:7000");
        assert_eq!(grpc_target("hub:7000"), "http://hub:7000");
        assert_eq!(
            grpc_target("http://localhost:7002"),
            "http://localhost:7002"
        );
    }

    fn permits_in(rx: &mut mpsc::Receiver<ClientMessage>) -> Vec<u32> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let Some(client_message::Message::Permits(p)) = m.message {
                out.push(p.count);
            }
        }
        out
    }

    /// Just past the flush timer.
    const AFTER_TIMER: Duration = PERMIT_FLUSH_AFTER.saturating_add(Duration::from_millis(1));

    /// Grants of a handshaken flow-controlled stream that offered `initial` permits and on which
    /// `received` requests arrived. The sender keeps the stream open.
    async fn decided(
        initial: u32,
        received: u32,
    ) -> (
        Arc<Grants>,
        mpsc::Sender<ClientMessage>,
        mpsc::Receiver<ClientMessage>,
    ) {
        let (tx, rx) = mpsc::channel(64);
        let grants = Grants::new(tx.downgrade(), initial);
        grants.decide(true).await;
        for _ in 0..received {
            grants.received();
        }
        (grants, tx, rx)
    }

    /// One permit: while the request runs the hub holds none for us, so the permit goes back on
    /// completion, not after the flush timer (which would pace the instance at ~20 requests/s).
    #[tokio::test(start_paused = true)]
    async fn with_one_initial_permit_every_completion_is_returned_at_once() {
        let (grants, _tx, mut rx) = decided(1, 0).await;
        for round in 0..3 {
            grants.received();
            grants.grant().await;
            assert_eq!(permits_in(&mut rx), [1], "round {round}: not sent at once");
        }
    }

    /// 64 permits, low-water mark min(16, 64 / 2) = 16: grants are batched while the hub still
    /// holds more than 16 permits for us; 16 pending go at once; once the hub is down to the mark a
    /// completion goes back at once; what the rule leaves pending goes with the timer.
    #[tokio::test(start_paused = true)]
    async fn a_larger_initial_batches_until_the_hub_runs_low_or_16_are_pending() {
        let (grants, _tx, mut rx) = decided(64, 20).await; // the hub holds 44
        for _ in 0..15 {
            grants.grant().await;
        }
        assert!(
            permits_in(&mut rx).is_empty(),
            "15 pending while the hub holds 44: batched"
        );
        grants.grant().await;
        assert_eq!(permits_in(&mut rx), [16], "16 pending go at once");
        // The hub holds 60 again; 44 more requests arrive: it holds 16, the mark.
        for _ in 0..44 {
            grants.received();
        }
        grants.grant().await;
        assert_eq!(permits_in(&mut rx), [1], "at the mark: at once");
        // That permit put the hub at 17, above the mark: batched again, the timer sends it.
        grants.grant().await;
        assert!(permits_in(&mut rx).is_empty(), "above the mark: batched");
        tokio::time::sleep(AFTER_TIMER).await;
        assert_eq!(permits_in(&mut rx), [1], "the timer flushes the rest");
    }

    /// Below 32 permits the mark is half of them: 8 permits, mark 4.
    #[tokio::test(start_paused = true)]
    async fn the_low_water_mark_is_half_of_a_small_initial() {
        let (grants, _tx, mut rx) = decided(8, 3).await; // the hub holds 5
        grants.grant().await;
        assert!(permits_in(&mut rx).is_empty(), "the hub holds 5, above 4");
        grants.received(); // the hub holds 4
        grants.grant().await;
        assert_eq!(
            permits_in(&mut rx),
            [2],
            "both pending go once the hub is at the mark"
        );
    }

    /// Requests can arrive before the ack; their grants are held until the hub's answer says
    /// whether it wants permits (the hub already counted those forwards against our permits). With
    /// flow control the held grants follow the same rule as later ones; without, they are dropped
    /// and later grants are no-ops.
    #[tokio::test(start_paused = true)]
    async fn grants_before_the_handshake_are_held_then_sent_or_dropped() {
        /// (initial, requests completed before the ack, flow control, sent at the ack, by the timer)
        type Case = (u32, u32, bool, &'static [u32], &'static [u32]);
        let cases: [Case; 5] = [
            (1, 1, true, &[1], &[]),    // the hub holds none: at once
            (64, 2, true, &[], &[2]),   // the hub holds 62: the timer
            (64, 20, true, &[20], &[]), // 16 or more pending: at once
            (1, 1, false, &[], &[]),    // no flow control: dropped
            (64, 20, false, &[], &[]),
        ];
        for (initial, held, flow_control, at_once, by_timer) in cases {
            let case = format!("initial={initial} held={held} flow_control={flow_control}");
            let (tx, mut rx) = mpsc::channel(8);
            let grants = Grants::new(tx.downgrade(), initial);
            for _ in 0..held {
                grants.received();
                grants.grant().await;
            }
            assert!(
                permits_in(&mut rx).is_empty(),
                "{case}: nothing before the handshake"
            );
            grants.decide(flow_control).await;
            assert_eq!(permits_in(&mut rx), at_once, "{case}: at the ack");
            tokio::time::sleep(AFTER_TIMER).await;
            assert_eq!(permits_in(&mut rx), by_timer, "{case}: by the timer");
            grants.received();
            grants.grant().await;
            tokio::time::sleep(AFTER_TIMER).await;
            let want: &[u32] = if flow_control { &[1] } else { &[] };
            assert_eq!(permits_in(&mut rx), want, "{case}: after the handshake");
            drop(tx);
        }
    }

    #[test]
    fn client_identity_constants() {
        assert_eq!(PROTOCOL_VERSION, 2);
        assert_eq!(
            CAPABILITIES,
            [
                "flow-control",
                "instance-status",
                "scatter-gather",
                "streaming",
                "subscription"
            ]
        );
        assert!(CLIENT_VERSION.starts_with("kraken-messaging-rs/"));
        assert_eq!(
            HubSettings::default().backoff_max,
            Duration::from_secs(5),
            "F2: reconnect backoff capped at 5 s"
        );
    }
}
