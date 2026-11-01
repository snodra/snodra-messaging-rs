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
//!
//! Scatter-gather like kraken-hub (ADR 0017): the ack lists "scatter-gather" when the client
//! declared it, unless `set_scatter_gather(false)` (a P3 hub). A `Request` with
//! `mode = SCATTER_GATHER` on a stream whose ack never listed it is served like a unary request
//! (a P3 hub does not know `mode`: the responder answers, or nobody); on a stream whose ack listed
//! it while the flag is off now it is rejected with `UNSUPPORTED`. Otherwise it
//! goes to every stream whose `Register` lists the type in `query_types` or
//! `shared_query_types` (ordered by service, then instance id) as a unary request `<id>#<n>`; each
//! answer goes back to the sender as a `ReplyPart` in arrival order, then one `ReplyEnd` once every
//! target answered, after `max_results` successful parts, or at the deadline (`timeout_ms`, else
//! 5 s) with the unanswered targets as `timed_out`. A target whose stream ends is a failed part
//! (`HANDLER_DISCONNECTED`). No target: `NO_HANDLER`; a command: `INVALID_REQUEST`; a session
//! without the capability: `UNSUPPORTED`. Unary requests still go to the responder only.

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

/// A stream's registration, for scatter-gather targeting.
struct Route {
    service: String,
    instance: String,
    commands: Vec<String>,
    /// Exclusive and shared query types.
    queries: Vec<String>,
    /// The session declared "scatter-gather" and the hub shares it.
    scatter: bool,
    tx: Outbound,
}

/// A scatter-gather in progress.
struct Gather {
    sender: Outbound,
    /// target id → (stream number, service, instance).
    outstanding: HashMap<String, (usize, String, String)>,
    parts: u32,
    ok: u32,
    failed: u32,
    max_results: u32,
}

fn server(m: server_message::Message) -> ServerMessage {
    ServerMessage { message: Some(m) }
}

fn hub_error(id: &str, code: proto::ErrorCode, message: &str) -> ServerMessage {
    server(server_message::Message::Error(proto::Error {
        request_id: id.to_string(),
        code: code as i32,
        message: message.to_string(),
        details: vec![],
    }))
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
    scatter_gather: AtomicBool,
    streaming: AtomicBool,
    /// stream number → its registration.
    routes: StdMutex<HashMap<usize, Route>>,
    /// gather id → the gather; target id → gather id.
    gathers: StdMutex<HashMap<String, Gather>>,
    target_of: StdMutex<HashMap<String, String>>,
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

    /// A scatter-gather `Request` from stream `stream_no`: rejected, or fanned out. `false` when
    /// that stream's ack never listed the capability: the request is to be served as unary (an
    /// older hub does not know `mode`).
    async fn scatter(
        self: &Arc<Self>,
        stream_no: usize,
        req: &proto::Request,
        tx: &Outbound,
    ) -> bool {
        use proto::ErrorCode;
        let reject = |code, message: &str| hub_error(&req.id, code, message);
        let listed = self
            .routes
            .lock()
            .unwrap()
            .get(&stream_no)
            .is_some_and(|r| r.scatter);
        if !listed {
            return false;
        }
        if !self.scatter_gather.load(Ordering::SeqCst) {
            let _ = tx
                .send(Ok(reject(
                    ErrorCode::Unsupported,
                    "scatter-gather not supported",
                )))
                .await;
            return true;
        }
        let (is_command, mut targets) = {
            let routes = self.routes.lock().unwrap();
            let is_command = req.kind == proto::RequestKind::Command as i32
                || routes.values().any(|r| r.commands.contains(&req.r#type));
            let targets: Vec<(usize, String, String, Outbound)> = routes
                .iter()
                .filter(|(_, r)| r.queries.contains(&req.r#type))
                .map(|(n, r)| (*n, r.service.clone(), r.instance.clone(), r.tx.clone()))
                .collect();
            (is_command, targets)
        };
        if is_command {
            let _ = tx
                .send(Ok(reject(
                    ErrorCode::InvalidRequest,
                    "invalid request: a command",
                )))
                .await;
            return true;
        }
        if targets.is_empty() {
            let _ = tx
                .send(Ok(reject(ErrorCode::NoHandler, "no handler")))
                .await;
            return true;
        }
        targets.sort_by(|a, b| (&a.1, &a.2).cmp(&(&b.1, &b.2)));
        let ids: Vec<String> = (0..targets.len())
            .map(|n| format!("{}#{n}", req.id))
            .collect();
        {
            let mut gathers = self.gathers.lock().unwrap();
            let mut target_of = self.target_of.lock().unwrap();
            gathers.insert(
                req.id.clone(),
                Gather {
                    sender: tx.clone(),
                    outstanding: ids
                        .iter()
                        .zip(&targets)
                        .map(|(id, (n, s, i, _))| (id.clone(), (*n, s.clone(), i.clone())))
                        .collect(),
                    parts: 0,
                    ok: 0,
                    failed: 0,
                    max_results: req.max_results,
                },
            );
            for id in &ids {
                target_of.insert(id.clone(), req.id.clone());
            }
        }
        let timeout = if req.timeout_ms > 0 {
            req.timeout_ms
        } else {
            5_000
        };
        let me = Arc::clone(self);
        let gather = req.id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(timeout.into())).await;
            let end = me.close(&gather, true);
            if let Some((tx, end)) = end {
                let _ = tx.send(Ok(end)).await;
            }
        });
        for (id, (_, _, _, target)) in ids.into_iter().zip(targets) {
            let forwarded = proto::Request {
                id,
                mode: proto::RequestMode::Unary as i32,
                max_results: 0,
                ..req.clone()
            };
            let _ = target
                .send(Ok(server(server_message::Message::Request(forwarded))))
                .await;
        }
        true
    }

    /// The answer of scatter-gather target `id`, if it is one: its part, and the end when done.
    async fn answer(&self, id: &str, result: Result<Vec<u8>, proto::Error>) {
        let out = {
            let Some(parent) = self.target_of.lock().unwrap().remove(id) else {
                return;
            };
            let mut gathers = self.gathers.lock().unwrap();
            let Some(g) = gathers.get_mut(&parent) else {
                return;
            };
            let Some((_, service, instance)) = g.outstanding.remove(id) else {
                return;
            };
            let seq = u64::from(g.parts);
            g.parts += 1;
            let (payload, error) = match result {
                Ok(p) => {
                    g.ok += 1;
                    (p, None)
                }
                Err(mut e) => {
                    g.failed += 1;
                    e.request_id = parent.clone();
                    (vec![], Some(e))
                }
            };
            let part = server(server_message::Message::Part(proto::ReplyPart {
                request_id: parent.clone(),
                payload,
                error,
                responder_service: service,
                responder_instance: instance,
                seq,
            }));
            let done = g.outstanding.is_empty() || (g.max_results > 0 && g.ok >= g.max_results);
            let sender = g.sender.clone();
            drop(gathers);
            let end = if done {
                self.close(&parent, false).map(|(_, e)| e)
            } else {
                None
            };
            (sender, part, end)
        };
        let (sender, part, end) = out;
        let _ = sender.send(Ok(part)).await;
        if let Some(end) = end {
            let _ = sender.send(Ok(end)).await;
        }
    }

    /// Ends gather `id` (if still open): its `ReplyEnd`, the unanswered targets timed out when
    /// `deadline`.
    fn close(&self, id: &str, deadline: bool) -> Option<(Outbound, ServerMessage)> {
        let g = self.gathers.lock().unwrap().remove(id)?;
        let mut target_of = self.target_of.lock().unwrap();
        for t in g.outstanding.keys() {
            target_of.remove(t);
        }
        let end = server(server_message::Message::End(proto::ReplyEnd {
            request_id: id.to_string(),
            error: None,
            parts: g.parts,
            failed: g.failed,
            timed_out: if deadline {
                g.outstanding.len() as u32
            } else {
                0
            },
        }));
        Some((g.sender, end))
    }

    /// Stream `stream_no` (written through `tx`) ended: its gathers end silently, it is no longer
    /// a target and its unanswered targets fail as parts (unless every stream was `dropped`).
    async fn stream_closed(&self, stream_no: usize, tx: &Outbound, dropped: bool) {
        self.routes.lock().unwrap().remove(&stream_no);
        let gone: Vec<String> = self
            .gathers
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, g)| g.sender.same_channel(tx))
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            self.close(&id, false);
        }
        if dropped {
            return;
        }
        let lost: Vec<String> = self
            .gathers
            .lock()
            .unwrap()
            .values()
            .flat_map(|g| {
                g.outstanding
                    .iter()
                    .filter(|(_, (n, _, _))| *n == stream_no)
                    .map(|(id, _)| id.clone())
            })
            .collect();
        for id in lost {
            let error = proto::Error {
                request_id: id.clone(),
                code: proto::ErrorCode::HandlerDisconnected as i32,
                message: "handler disconnected".into(),
                details: vec![],
            };
            self.answer(&id, Err(error)).await;
        }
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
            // Dropped with every other stream (a hub restart): nothing more is delivered.
            let mut dropped = false;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = drop_rx.recv() => {
                        dropped = true;
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
                                let wants_scatter = r.capabilities.iter().any(|c| c == "scatter-gather");
                                let wants_streaming = r.capabilities.iter().any(|c| c == "streaming");
                                state.log(Event::Register { stream: stream_no, types: r.command_types.len() + r.query_types.len() + r.shared_query_types.len() });
                                let scatter = wants_scatter && v2 && state.scatter_gather.load(Ordering::SeqCst) && state.ack.lock().unwrap().is_some();
                                state.routes.lock().unwrap().insert(stream_no, Route {
                                    service: r.service_name.clone(),
                                    instance: r.instance_id.clone(),
                                    commands: r.command_types.clone(),
                                    queries: r.query_types.iter().chain(&r.shared_query_types).cloned().collect(),
                                    scatter,
                                    tx: tx.clone(),
                                });
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
                                    if wants_scatter && state.scatter_gather.load(Ordering::SeqCst) {
                                        ack.capabilities.push("scatter-gather".to_string());
                                    }
                                    if wants_streaming && state.streaming.load(Ordering::SeqCst) {
                                        ack.capabilities.push("streaming".to_string());
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
                            client_message::Message::Reply(r) => {
                                state.replies.lock().await.push((stream_no, r.clone()));
                                let result = match r.result {
                                    Some(proto::reply::Result::Error(e)) => Err(e),
                                    Some(proto::reply::Result::Payload(p)) => Ok(p),
                                    None => Ok(b"{}".to_vec()),
                                };
                                state.answer(&r.request_id, result).await;
                            }
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
                                let scattered = req.mode == proto::RequestMode::ScatterGather as i32
                                    && state.scatter(stream_no, &req, &tx).await;
                                if !scattered {
                                    let response = state.responder.lock().await.as_ref().and_then(|f| f(req));
                                    if let Some(resp) = response && tx.send(Ok(resp)).await.is_err() { break; }
                                }
                            }
                            // Multi-part messages (ADR 0017): this client sends none yet.
                            client_message::Message::Part(_)
                            | client_message::Message::End(_)
                            | client_message::Message::Flow(_)
                            | client_message::Message::Cancel(_) => {}
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
            state.stream_closed(stream_no, &tx, dropped).await;
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
            scatter_gather: AtomicBool::new(true),
            streaming: AtomicBool::new(true),
            routes: StdMutex::new(HashMap::new()),
            gathers: StdMutex::new(HashMap::new()),
            target_of: StdMutex::new(HashMap::new()),
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

    /// Whether the ack shares "scatter-gather" with clients that declare it (`false`: a P3 hub).
    pub fn set_scatter_gather(&self, on: bool) {
        self.state.scatter_gather.store(on, Ordering::SeqCst);
    }

    /// Whether the ack shares "streaming" with clients that declare it (`false`: a P4a hub).
    pub fn set_streaming(&self, on: bool) {
        self.state.streaming.store(on, Ordering::SeqCst);
    }

    /// Scatter-gathers still open.
    pub fn open_gathers(&self) -> usize {
        self.state.gathers.lock().unwrap().len()
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
