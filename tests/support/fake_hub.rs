//! In-process stand-in for kraken-hub: records registrations, echoes heartbeats, can drop streams,
//! reject a registration, push requests to clients and answer their requests.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use kraken_messaging::proto::hub_server::{Hub, HubServer};
use kraken_messaging::proto::{self, ClientMessage, ServerMessage, client_message, server_message};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

type Responder = Box<dyn Fn(proto::Request) -> Option<ServerMessage> + Send + Sync>;
type Outbound = mpsc::Sender<Result<ServerMessage, Status>>;

pub struct State {
    pub registers: Mutex<Vec<proto::Register>>,
    pub replies: Mutex<Vec<proto::Reply>>,
    pub requests: Mutex<Vec<proto::Request>>,
    heartbeats: AtomicUsize,
    reject_next: AtomicBool,
    reject_invalid_next: AtomicBool,
    echo_heartbeats: AtomicBool,
    drop_tx: broadcast::Sender<()>,
    clients: Mutex<Vec<Outbound>>,
    responder: Mutex<Option<Responder>>,
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
        if state.reject_next.swap(false, Ordering::SeqCst) {
            return Err(Status::already_exists(
                "type t.Cmd is owned by other-service",
            ));
        }
        if state.reject_invalid_next.swap(false, Ordering::SeqCst) {
            return Err(Status::invalid_argument("service_name must not be empty"));
        }
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<ServerMessage, Status>>(64);
        state.clients.lock().await.push(tx.clone());
        let mut drop_rx = state.drop_tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = drop_rx.recv() => {
                        let _ = tx.send(Err(Status::unavailable("dropped by test"))).await;
                        break;
                    }
                    msg = inbound.next() => match msg {
                        None | Some(Err(_)) => break,
                        Some(Ok(ClientMessage { message: Some(m) })) => match m {
                            client_message::Message::Register(r) => state.registers.lock().await.push(r),
                            client_message::Message::Heartbeat(h) => {
                                state.heartbeats.fetch_add(1, Ordering::SeqCst);
                                if state.echo_heartbeats.load(Ordering::SeqCst) {
                                    let echo = ServerMessage { message: Some(server_message::Message::Heartbeat(h)) };
                                    if tx.send(Ok(echo)).await.is_err() { break; }
                                }
                            }
                            client_message::Message::Reply(r) => state.replies.lock().await.push(r),
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
            let mut clients = state.clients.lock().await;
            clients.retain(|c| !c.same_channel(&tx));
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
        self.state.replies.lock().await.clone()
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
    pub async fn send_to_clients(&self, msg: ServerMessage) {
        for c in self.state.clients.lock().await.iter() {
            let _ = c.send(Ok(msg.clone())).await;
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

    pub fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}
