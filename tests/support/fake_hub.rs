//! In-process stand-in for kraken-hub: records registrations, answers a v2 `Register` with a
//! `RegisterAck` (new-hub behaviour, the default) or not at all (old-hub behaviour, `set_ack(None)`).
//! The ack lists "flow-control" when the client declared it, unless `set_flow_control(false)`
//! (the P0 hub), and "instance-status" likewise unless `set_instance_status(false)` (hubs before
//! P3). Permits and InstanceStatus messages are recorded per stream. It
//! echoes heartbeats, can drop streams, reject a registration, push requests to clients and answer
//! their requests.
//!
//! Like kraken-hub, a `Register` for an instance id that another open stream holds replaces that
//! stream's session: the older stream is ended (its forwarded requests can no longer be answered)
//! unless `set_replace_sessions(false)`. A stream that ends while it holds the session removes the
//! instance. Registrations and stream ends are logged in order ([`FakeHub::events`]).
//!
//! Permit accounting like kraken-hub (it never holds requests back, it only counts): a stream whose
//! ack shares "flow-control" and whose first `Register` offers `initial_permits > 0` holds that
//! many permits ([`FakeHub::permits_held`]); each request pushed to it spends one, each `Permits`
//! adds `count`, never past the stream's cap. The cap starts at `initial_permits` and a
//! `Permits.grant` above it raises it first (never lowers it). `set_permit_cap(false)` drops the
//! cap; `set_permit_grants(false)` emulates a hub that predates `Permits.grant`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use kraken_messaging::proto::hub_server::{Hub, HubServer};
use kraken_messaging::proto::{self, ClientMessage, ServerMessage, client_message, server_message};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};

type Responder = Box<dyn Fn(proto::Request) -> Option<ServerMessage> + Send + Sync>;
type Outbound = mpsc::Sender<Result<ServerMessage, Status>>;

/// The ack a default kraken-hub sends.
pub fn default_ack() -> proto::RegisterAck {
    proto::RegisterAck {
        hub_id: "fake-hub".into(),
        protocol_version: 2,
        capabilities: vec![],
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 10_000,
        default_timeout_ms: 5_000,
        max_timeout_ms: 60_000,
        context: "default".into(),
        type_timeouts_ms: Default::default(),
    }
}

pub struct State {
    pub registers: Mutex<Vec<proto::Register>>,
    /// Replies with the number of the stream (1-based, in accept order) they arrived on.
    pub replies: Mutex<Vec<(usize, proto::Reply)>>,
    pub requests: Mutex<Vec<proto::Request>>,
    heartbeats: AtomicUsize,
    reject_next: AtomicBool,
    reject_invalid_next: AtomicBool,
    echo_heartbeats: AtomicBool,
    drop_tx: broadcast::Sender<()>,
    /// Open streams: (stream number, outbound).
    clients: Mutex<Vec<(usize, Outbound)>>,
    responder: Mutex<Option<Responder>>,
    ack: StdMutex<Option<proto::RegisterAck>>,
    streams: AtomicUsize,
    flow_control: AtomicBool,
    /// Permits messages: (stream number, count, when).
    permits: StdMutex<Vec<(usize, u32, std::time::Instant)>>,
    /// Permits messages' grants: (stream number, grant).
    grants: StdMutex<Vec<(usize, u32)>>,
    /// Flow-controlled streams: stream number → (permits held, cap).
    held: StdMutex<HashMap<usize, (u32, u32)>>,
    permit_cap: AtomicBool,
    permit_grants: AtomicBool,
    instance_status: AtomicBool,
    /// InstanceStatus messages: (stream number, report).
    statuses: StdMutex<Vec<(usize, proto::InstanceStatus)>>,
    down: AtomicBool,
    replace_sessions: AtomicBool,
    stall_reads: AtomicBool,
    /// instance id → (stream number, that stream's cancel token).
    sessions: StdMutex<HashMap<String, (usize, CancellationToken)>>,
    evictions: AtomicUsize,
    events: StdMutex<Vec<(Event, std::time::Instant)>>,
}

/// What happened on the fake hub, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A `Register` on stream `stream` announcing `types` command and query types.
    Register { stream: usize, types: usize },
    /// Stream `stream` ended (either side).
    Closed { stream: usize },
    /// Stream `stream` lost its session to a newer stream of the same instance id.
    Evicted { stream: usize },
}

impl State {
    fn log(&self, event: Event) {
        self.events
            .lock()
            .unwrap()
            .push((event, std::time::Instant::now()));
    }
}

#[derive(Clone)]
struct Service(Arc<State>);

#[tonic::async_trait]
impl Hub for Service {
    type ConnectStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

    async fn connect(
        &self,
        request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let state = self.0.clone();
        if state.down.load(Ordering::SeqCst) {
            return Err(Status::unavailable("hub down (test)"));
        }
        if state.reject_next.swap(false, Ordering::SeqCst) {
            return Err(Status::already_exists(
                "type t.Cmd is owned by other-service",
            ));
        }
        if state.reject_invalid_next.swap(false, Ordering::SeqCst) {
            return Err(Status::invalid_argument("service_name must not be empty"));
        }
        let mut inbound = request.into_inner();
        let stream_no = state.streams.fetch_add(1, Ordering::SeqCst) + 1;
        let (tx, rx) = mpsc::channel::<Result<ServerMessage, Status>>(64);
        state.clients.lock().await.push((stream_no, tx.clone()));
        let mut drop_rx = state.drop_tx.subscribe();
        let cancel = CancellationToken::new();
        tokio::spawn(async move {
            let mut instance: Option<String> = None;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = drop_rx.recv() => {
                        let _ = tx.send(Err(Status::unavailable("dropped by test"))).await;
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)), if state.stall_reads.load(Ordering::SeqCst) => {}
                    msg = inbound.next(), if !state.stall_reads.load(Ordering::SeqCst) => match msg {
                        None | Some(Err(_)) => break,
                        Some(Ok(ClientMessage { message: Some(m) })) => match m {
                            client_message::Message::Register(r) => {
                                let v2 = r.protocol_version >= 2;
                                let wants_flow = r.capabilities.iter().any(|c| c == "flow-control");
                                let wants_status = r.capabilities.iter().any(|c| c == "instance-status");
                                state.log(Event::Register { stream: stream_no, types: r.command_types.len() + r.query_types.len() });
                                instance = Some(r.instance_id.clone());
                                let replaced = state.sessions.lock().unwrap().insert(r.instance_id.clone(), (stream_no, cancel.clone()));
                                if let Some((old, token)) = replaced
                                    && old != stream_no
                                    && state.replace_sessions.load(Ordering::SeqCst)
                                {
                                    state.evictions.fetch_add(1, Ordering::SeqCst);
                                    state.log(Event::Evicted { stream: old });
                                    token.cancel();
                                }
                                let flow = wants_flow && state.flow_control.load(Ordering::SeqCst) && state.ack.lock().unwrap().is_some() && v2;
                                if flow && r.initial_permits > 0 {
                                    // Like the real hub: the first Register of a stream fixes its permits.
                                    state.held.lock().unwrap().entry(stream_no).or_insert((r.initial_permits, r.initial_permits));
                                }
                                state.registers.lock().await.push(r);
                                // Like the real hub: the ack is written before the echo of the next heartbeat.
                                let ack = state.ack.lock().unwrap().clone().map(|mut ack| {
                                    if wants_flow && state.flow_control.load(Ordering::SeqCst) {
                                        ack.capabilities.push("flow-control".to_string());
                                    }
                                    if wants_status && state.instance_status.load(Ordering::SeqCst) {
                                        ack.capabilities.push("instance-status".to_string());
                                    }
                                    ack
                                });
                                if let (true, Some(ack)) = (v2, ack) {
                                    let msg = ServerMessage { message: Some(server_message::Message::RegisterAck(ack)) };
                                    if tx.send(Ok(msg)).await.is_err() { break; }
                                }
                            }
                            client_message::Message::Heartbeat(h) => {
                                state.heartbeats.fetch_add(1, Ordering::SeqCst);
                                if state.echo_heartbeats.load(Ordering::SeqCst) {
                                    let echo = ServerMessage { message: Some(server_message::Message::Heartbeat(h)) };
                                    if tx.send(Ok(echo)).await.is_err() { break; }
                                }
                            }
                            client_message::Message::Reply(r) => state.replies.lock().await.push((stream_no, r)),
                            client_message::Message::Permits(p) => {
                                state.permits.lock().unwrap().push((stream_no, p.count, std::time::Instant::now()));
                                state.grants.lock().unwrap().push((stream_no, p.grant));
                                if let Some((held, cap)) = state.held.lock().unwrap().get_mut(&stream_no) {
                                    if state.permit_grants.load(Ordering::SeqCst) {
                                        *cap = (*cap).max(p.grant);
                                    }
                                    *held = held.saturating_add(p.count);
                                    if state.permit_cap.load(Ordering::SeqCst) {
                                        *held = (*held).min(*cap);
                                    }
                                }
                            }
                            client_message::Message::Status(st) => state.statuses.lock().unwrap().push((stream_no, st)),
                            client_message::Message::Request(req) => {
                                state.requests.lock().await.push(req.clone());
                                let response = state.responder.lock().await.as_ref().and_then(|f| f(req));
                                if let Some(resp) = response && tx.send(Ok(resp)).await.is_err() { break; }
                            }
                        },
                        Some(Ok(ClientMessage { message: None })) => {}
                    }
                }
            }
            if let Some(id) = instance {
                let mut sessions = state.sessions.lock().unwrap();
                if sessions.get(&id).is_some_and(|(s, _)| *s == stream_no) {
                    sessions.remove(&id);
                }
            }
            state.log(Event::Closed { stream: stream_no });
            let mut clients = state.clients.lock().await;
            clients.retain(|(_, c)| !c.same_channel(&tx));
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn topology(
        &self,
        _: Request<proto::TopologyRequest>,
    ) -> Result<Response<proto::TopologyResponse>, Status> {
        Err(Status::unimplemented("fake hub has no topology"))
    }
}

pub struct FakeHub {
    pub addr: SocketAddr,
    pub state: Arc<State>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl FakeHub {
    pub async fn start() -> FakeHub {
        let (drop_tx, _) = broadcast::channel(8);
        let state = Arc::new(State {
            registers: Mutex::new(vec![]),
            replies: Mutex::new(vec![]),
            requests: Mutex::new(vec![]),
            heartbeats: AtomicUsize::new(0),
            reject_next: AtomicBool::new(false),
            reject_invalid_next: AtomicBool::new(false),
            echo_heartbeats: AtomicBool::new(true),
            drop_tx,
            clients: Mutex::new(vec![]),
            responder: Mutex::new(None),
            ack: StdMutex::new(Some(default_ack())),
            streams: AtomicUsize::new(0),
            flow_control: AtomicBool::new(true),
            permits: StdMutex::new(vec![]),
            grants: StdMutex::new(vec![]),
            held: StdMutex::new(HashMap::new()),
            permit_cap: AtomicBool::new(true),
            permit_grants: AtomicBool::new(true),
            instance_status: AtomicBool::new(true),
            statuses: StdMutex::new(vec![]),
            down: AtomicBool::new(false),
            replace_sessions: AtomicBool::new(true),
            stall_reads: AtomicBool::new(false),
            sessions: StdMutex::new(HashMap::new()),
            evictions: AtomicUsize::new(0),
            events: StdMutex::new(vec![]),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let svc = Service(state.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HubServer::new(svc))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    shutdown_rx.await.ok();
                })
                .await
                .unwrap();
        });
        FakeHub {
            addr,
            state,
            shutdown: Some(shutdown_tx),
        }
    }

    /// Address in the form services configure: `static://host:port`.
    pub fn address(&self) -> String {
        format!("static://{}", self.addr)
    }

    pub async fn registers(&self) -> Vec<proto::Register> {
        self.state.registers.lock().await.clone()
    }

    pub fn heartbeats(&self) -> usize {
        self.state.heartbeats.load(Ordering::SeqCst)
    }

    pub async fn replies(&self) -> Vec<proto::Reply> {
        self.state
            .replies
            .lock()
            .await
            .iter()
            .map(|(_, r)| r.clone())
            .collect()
    }

    /// Replies with the 1-based number of the stream they arrived on.
    pub async fn replies_by_stream(&self) -> Vec<(usize, proto::Reply)> {
        self.state.replies.lock().await.clone()
    }

    /// Streams accepted so far.
    pub fn streams(&self) -> usize {
        self.state.streams.load(Ordering::SeqCst)
    }

    /// The ack sent for every v2 `Register`; `None` behaves like a hub older than protocol v2.
    pub fn set_ack(&self, ack: Option<proto::RegisterAck>) {
        *self.state.ack.lock().unwrap() = ack;
    }

    /// Whether the ack shares "flow-control" with clients that declare it (`false`: the P0 hub).
    pub fn set_flow_control(&self, on: bool) {
        self.state.flow_control.store(on, Ordering::SeqCst);
    }

    /// A hub whose ack carries `hub_id` (the default ack otherwise).
    pub async fn start_with_id(hub_id: &str) -> FakeHub {
        let hub = FakeHub::start().await;
        hub.set_ack(Some(proto::RegisterAck {
            hub_id: hub_id.to_string(),
            ..default_ack()
        }));
        hub
    }

    /// Whether the ack shares "instance-status" with clients that declare it (`false`: a hub
    /// before P3).
    pub fn set_instance_status(&self, on: bool) {
        self.state.instance_status.store(on, Ordering::SeqCst);
    }

    /// InstanceStatus messages received: (1-based stream number, report).
    pub fn statuses(&self) -> Vec<(usize, proto::InstanceStatus)> {
        self.state.statuses.lock().unwrap().clone()
    }

    /// A down hub refuses every new stream (UNAVAILABLE) and drops the open ones.
    pub fn set_down(&self, down: bool) {
        self.state.down.store(down, Ordering::SeqCst);
        if down {
            self.drop_streams();
        }
    }

    /// Permits messages received: (1-based stream number, count).
    pub fn permits(&self) -> Vec<(usize, u32)> {
        self.state
            .permits
            .lock()
            .unwrap()
            .iter()
            .map(|(s, n, _)| (*s, *n))
            .collect()
    }

    /// The `Permits.grant` of each Permits message received: (1-based stream number, grant).
    pub fn grants(&self) -> Vec<(usize, u32)> {
        self.state.grants.lock().unwrap().clone()
    }

    /// Permits the hub holds for stream `stream` (1-based); `None` unless that stream is
    /// flow-controlled with `initial_permits > 0`.
    pub fn permits_held(&self, stream: usize) -> Option<u32> {
        self.state
            .held
            .lock()
            .unwrap()
            .get(&stream)
            .map(|(held, _)| *held)
    }

    /// Whether permits stop at the stream's cap (default `true`, as kraken-hub).
    pub fn set_permit_cap(&self, on: bool) {
        self.state.permit_cap.store(on, Ordering::SeqCst);
    }

    /// Whether `Permits.grant` raises the cap (default `true`; `false`: a hub that predates it).
    pub fn set_permit_grants(&self, on: bool) {
        self.state.permit_grants.store(on, Ordering::SeqCst);
    }

    /// Permits messages with the instant each arrived.
    pub fn permits_timed(&self) -> Vec<(usize, u32, std::time::Instant)> {
        self.state.permits.lock().unwrap().clone()
    }

    pub async fn requests(&self) -> Vec<proto::Request> {
        self.state.requests.lock().await.clone()
    }

    /// Fails every open stream with UNAVAILABLE, as a hub restart would.
    pub fn drop_streams(&self) {
        let _ = self.state.drop_tx.send(());
    }

    pub fn reject_next_with_already_exists(&self) {
        self.state.reject_next.store(true, Ordering::SeqCst);
    }

    pub fn reject_next_with_invalid_argument(&self) {
        self.state.reject_invalid_next.store(true, Ordering::SeqCst);
    }

    pub fn set_echo_heartbeats(&self, echo: bool) {
        self.state.echo_heartbeats.store(echo, Ordering::SeqCst);
    }

    /// Pushes a server message (typically a Request) to every connected client.
    /// A Request spends one permit of each flow-controlled stream it is pushed to.
    pub async fn send_to_clients(&self, msg: ServerMessage) {
        let request = matches!(msg.message, Some(server_message::Message::Request(_)));
        for (stream, c) in self.state.clients.lock().await.iter() {
            if c.send(Ok(msg.clone())).await.is_ok()
                && request
                && let Some((held, _)) = self.state.held.lock().unwrap().get_mut(stream)
            {
                *held = held.saturating_sub(1);
            }
        }
    }

    pub async fn connected_clients(&self) -> usize {
        self.state.clients.lock().await.len()
    }

    /// How the fake hub answers requests sent by clients (None = swallow).
    pub async fn set_responder(
        &self,
        f: impl Fn(proto::Request) -> Option<ServerMessage> + Send + Sync + 'static,
    ) {
        *self.state.responder.lock().await = Some(Box::new(f));
    }

    /// Whether a `Register` for an instance id held by another open stream ends that stream (the
    /// default, as kraken-hub does) or both streams stay (`false`).
    pub fn set_replace_sessions(&self, on: bool) {
        self.state.replace_sessions.store(on, Ordering::SeqCst);
    }

    /// Stops reading what clients write (`true`): once the HTTP/2 windows are full, a client's
    /// messages stay in its own outbound buffer, never written.
    pub fn set_stall_reads(&self, on: bool) {
        self.state.stall_reads.store(on, Ordering::SeqCst);
    }

    /// Sessions replaced by a newer stream of the same instance id so far.
    pub fn evictions(&self) -> usize {
        self.state.evictions.load(Ordering::SeqCst)
    }

    /// True while an open stream holds `instance_id`'s session (the hub routes to it).
    pub fn holds(&self, instance_id: &str) -> bool {
        self.state
            .sessions
            .lock()
            .unwrap()
            .contains_key(instance_id)
    }

    /// Registrations, evictions and stream ends in the order they happened.
    pub fn events(&self) -> Vec<Event> {
        self.events_timed().into_iter().map(|(e, _)| e).collect()
    }

    pub fn events_timed(&self) -> Vec<(Event, std::time::Instant)> {
        self.state.events.lock().unwrap().clone()
    }

    pub fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}
