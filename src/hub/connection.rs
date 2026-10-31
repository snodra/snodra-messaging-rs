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
//! stream id) and reconnects with exponential backoff (`backoff_min` doubling up to `backoff_max`,
//! each wait jittered by ±20 % and clamped to `backoff_max`; at the cap 80-100 % of it), reset to `backoff_min` once an attempt
//! reached the handshake. An `ALREADY_EXISTS` or `INVALID_ARGUMENT` rejection is kept in
//! [`HubConnection::last_rejection`] and retried at `backoff_max`.
//!
//! The channel sends HTTP/2 keepalive pings (every 10 s, 5 s to answer, also while idle) so a
//! one-way partition is noticed by the transport too, not only by the missing heartbeat echoes.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tonic::{Code, Status};

use crate::error::MessagingError;
use crate::proto::hub_client::HubClient;
use crate::proto::{
    self, ClientMessage, Heartbeat, Register, RegisterAck, client_message, server_message,
};

/// The hub protocol version this client speaks (`Register.protocol_version`).
pub const PROTOCOL_VERSION: u32 = 2;
/// Optional protocol features this client declares (`Register.capabilities`); none yet.
pub const CAPABILITIES: &[&str] = &[];
/// `Register.client_version`, shown in the hub's topology view.
pub const CLIENT_VERSION: &str = concat!("kraken-messaging-rs/", env!("CARGO_PKG_VERSION"));

/// HTTP/2 PING period on the hub channel.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// How long a PING may stay unanswered before the channel is dropped.
pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Identifies one stream (one connect attempt) of a [`HubConnection`]; strictly increasing.
pub type StreamId = u64;

#[derive(Clone, Debug)]
pub struct HubSettings {
    /// `host:port`, optionally prefixed with `static://` or `http://`.
    pub address: String,
    /// Heartbeat period used until, and unless, the hub announces one in its `RegisterAck`
    /// (old hubs do not; they evict after 30 s, so this must stay well below that).
    pub heartbeat_interval: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// How long a new stream may take to complete the handshake (ack or first echo).
    pub handshake_timeout: Duration,
    pub max_concurrent_handlers: usize,
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

    pub fn ack(&self) -> Option<&RegisterAck> {
        match self {
            Handshake::V2(ack) => Some(ack),
            Handshake::Legacy => None,
        }
    }
}

/// The stream an inbound request arrived on. Holds only a weak handle, so an in-flight handler
/// never keeps a dead stream's request body open; replies to a stream that is gone are dropped.
#[derive(Clone, Debug)]
pub struct ReplyTo {
    stream: StreamId,
    tx: mpsc::WeakSender<ClientMessage>,
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
    /// That stream was lost; every request written to it and still pending should fail.
    Disconnected {
        stream: StreamId,
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
            Inbound::Disconnected { stream } => write!(f, "Disconnected(stream={stream})"),
        }
    }
}

/// A handshaken stream that accepts outgoing messages.
struct Live {
    stream: StreamId,
    tx: mpsc::Sender<ClientMessage>,
    handshake: Handshake,
}

pub struct HubConnection {
    settings: HubSettings,
    service_name: String,
    instance_id: String,
    inbound: mpsc::Sender<Inbound>,
    types: Mutex<(Vec<String>, Vec<String>)>,
    live: Mutex<Option<Live>>,
    connected: AtomicBool,
    handshake: watch::Sender<Option<Handshake>>,
    last_rejection: Mutex<Option<String>>,
    next_stream: AtomicU64,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
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
            types: Mutex::new((vec![], vec![])),
            live: Mutex::new(None),
            connected: AtomicBool::new(false),
            handshake,
            last_rejection: Mutex::new(None),
            next_stream: AtomicU64::new(1),
            cancel: CancellationToken::new(),
            task: Mutex::new(None),
        })
    }

    /// The types announced in every `Register`; set before [`start`](Self::start).
    pub fn set_types(&self, command_types: Vec<String>, query_types: Vec<String>) {
        let mut c = command_types;
        let mut q = query_types;
        c.sort();
        q.sort();
        *self.types.lock().unwrap() = (c, q);
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
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
        self.send_with(|_, _| (msg, ())).await.map(|(s, ())| s)
    }

    /// Builds a message for the current handshaken stream and writes it there. `build` runs while
    /// the stream is still current, with its id and handshake, so whatever it records (a pending
    /// request tagged with the stream id) happens before that stream's
    /// [`Inbound::Disconnected`] can be emitted. Returns the stream id and `build`'s extra value;
    /// `HubUnavailable` when there is no handshaken stream or the write fails (nothing was written).
    pub async fn send_with<T>(
        &self,
        build: impl FnOnce(StreamId, &Handshake) -> (ClientMessage, T),
    ) -> Result<(StreamId, T), MessagingError> {
        let (stream, tx, msg, extra) = {
            let live = self.live.lock().unwrap();
            let Some(live) = live.as_ref() else {
                return Err(MessagingError::HubUnavailable(
                    "not connected to the hub".into(),
                ));
            };
            let (msg, extra) = build(live.stream, &live.handshake);
            (live.stream, live.tx.clone(), msg, extra)
        };
        tx.send(msg)
            .await
            .map_err(|_| MessagingError::HubUnavailable("hub stream closed".into()))?;
        Ok((stream, extra))
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
            let stream = self.next_stream.fetch_add(1, Ordering::SeqCst);
            let outcome = self.run_stream(stream).await;
            self.connected.store(false, Ordering::SeqCst);
            // Clear the live stream before announcing the loss: a request is tagged with a stream
            // only while that stream is live, so it is always failed by this Disconnected.
            *self.live.lock().unwrap() = None;
            let reached_handshake = self.handshake.send_replace(None).is_some();
            let _ = self.inbound.send(Inbound::Disconnected { stream }).await;
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
                tracing::error!(service = %self.service_name, instance = %self.instance_id, reason = status.message(), retry_in = ?wait, "hub rejected the registration");
                wait
            } else {
                let wait = jittered(backoff, max, random_unit());
                if failures == 1 {
                    tracing::warn!(service = %self.service_name, instance = %self.instance_id, reason = %describe(&status), retry_in = ?wait, "hub stream closed; reconnecting");
                } else {
                    tracing::debug!(service = %self.service_name, instance = %self.instance_id, reason = %describe(&status), retry_in = ?wait, "hub stream closed; reconnecting");
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
        // A peer that accepts TCP but never answers the HTTP/2 handshake must not hang the loop.
        let mut stream = tokio::time::timeout(
            self.settings.handshake_timeout,
            client.connect(ReceiverStream::new(rx)),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded("hub did not answer the connect handshake in time")
        })??
        .into_inner();

        // The Register is the first message of every stream; the ack or the echo of the heartbeat
        // after it completes the handshake.
        let (commands, queries) = self.types.lock().unwrap().clone();
        let register = Register {
            service_name: self.service_name.clone(),
            instance_id: self.instance_id.clone(),
            command_types: commands,
            query_types: queries,
            protocol_version: PROTOCOL_VERSION,
            capabilities: CAPABILITIES.iter().map(|c| c.to_string()).collect(),
            credentials: None,
            client_version: CLIENT_VERSION.to_string(),
            initial_permits: 0,
            load_factor: 0,
            tags: Default::default(),
        };
        tx.send(ClientMessage {
            message: Some(client_message::Message::Register(register)),
        })
        .await
        .map_err(|_| Status::unavailable("stream closed before Register"))?;
        tx.send(heartbeat())
            .await
            .map_err(|_| Status::unavailable("stream closed before first heartbeat"))?;
        self.connected.store(true, Ordering::SeqCst);
        let reply_to = ReplyTo {
            stream: stream_id,
            tx: tx.downgrade(),
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
                                tracing::info!(service = %self.service_name, instance = %self.instance_id, hub = %ack.hub_id, protocol = ack.protocol_version, heartbeat = ?interval, "registered with the hub");
                                let h = Handshake::V2(ack);
                                self.go_live(stream_id, &tx, &h);
                                handshake = Some(h);
                            }
                            // Every later Register is acked too; the values do not change.
                            Some(Handshake::V2(_)) => {
                                let h = Handshake::V2(ack);
                                self.go_live(stream_id, &tx, &h);
                                handshake = Some(h);
                            }
                            Some(Handshake::Legacy) => {
                                tracing::debug!(service = %self.service_name, "RegisterAck after the echo on a legacy stream ignored");
                            }
                        },
                        server_message::Message::Heartbeat(_) => {
                            last_echo = tokio::time::Instant::now();
                            if handshake.is_none() {
                                tracing::info!(service = %self.service_name, instance = %self.instance_id, heartbeat = ?interval, "registered with the hub (legacy hub without protocol v2)");
                                let h = Handshake::Legacy;
                                self.go_live(stream_id, &tx, &h);
                                handshake = Some(h);
                            }
                        }
                        server_message::Message::Request(request) => {
                            self.forward(Inbound::Request { request, reply_to: reply_to.clone() }).await?
                        }
                        server_message::Message::Reply(r) => self.forward(Inbound::Reply(r)).await?,
                        server_message::Message::Error(e) => self.forward(Inbound::Error(e)).await?,
                    },
                },
            }
        }
    }

    fn go_live(&self, stream: StreamId, tx: &mpsc::Sender<ClientMessage>, handshake: &Handshake) {
        *self.live.lock().unwrap() = Some(Live {
            stream,
            tx: tx.clone(),
            handshake: handshake.clone(),
        });
        self.handshake.send_replace(Some(handshake.clone()));
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

/// `base` scaled by a factor in [0.8, 1.2) chosen by `unit` (in [0, 1)), never above `max`.
pub(crate) fn jittered(base: Duration, max: Duration, unit: f64) -> Duration {
    let unit = unit.clamp(0.0, 1.0);
    if base >= max {
        // At the cap jitter downward only, over [0.8, 1.0] x max: clamping a ±20 % wait would put
        // about half of a reconnecting fleet on exactly the maximum.
        max.mul_f64(0.8 + 0.2 * unit)
    } else {
        base.mul_f64(0.8 + 0.4 * unit).min(max)
    }
}

/// A uniformly distributed value in [0, 1) from the v4 UUID generator (no extra dependency).
fn random_unit() -> f64 {
    let bits = (uuid::Uuid::new_v4().as_u128() >> 64) as u64;
    (bits >> 11) as f64 / (1u64 << 53) as f64
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

    #[test]
    fn jitter_stays_within_twenty_percent_and_never_exceeds_max() {
        let max = Duration::from_secs(5);
        let base = Duration::from_secs(1);
        assert_eq!(jittered(base, max, 0.0), Duration::from_millis(800));
        assert_eq!(jittered(base, max, 0.5), Duration::from_millis(1000));
        assert!(jittered(base, max, 0.999_999) < Duration::from_millis(1200));
        assert!(jittered(base, max, 0.999_999) > Duration::from_millis(1199));
        // At the cap the wait is jittered downward over [0.8, 1.0] x max, so a fleet does not all
        // land on exactly the maximum.
        assert_eq!(jittered(max, max, 0.0), Duration::from_secs(4));
        assert_eq!(jittered(max, max, 0.5), Duration::from_millis(4500));
        let top = jittered(max, max, 0.999_999);
        assert!(top < max && top > Duration::from_millis(4999), "{top:?}");
        // Below the cap the ±20 % never exceeds the maximum either.
        assert!(jittered(Duration::from_millis(4500), max, 0.999) <= max);
    }

    #[test]
    fn random_unit_is_in_range_and_varies() {
        let values: Vec<f64> = (0..200).map(|_| random_unit()).collect();
        assert!(values.iter().all(|v| (0.0..1.0).contains(v)));
        let distinct: std::collections::HashSet<u64> = values.iter().map(|v| v.to_bits()).collect();
        assert!(distinct.len() > 190);
    }

    #[test]
    fn client_identity_constants() {
        assert_eq!(PROTOCOL_VERSION, 2);
        assert!(CAPABILITIES.is_empty());
        assert!(CLIENT_VERSION.starts_with("kraken-messaging-rs/"));
        assert_eq!(
            HubSettings::default().backoff_max,
            Duration::from_secs(5),
            "F2: reconnect backoff capped at 5 s"
        );
    }
}
