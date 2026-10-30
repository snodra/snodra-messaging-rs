//! The stream to the hub and its lifecycle: register, heartbeat, reconnect.
//!
//! Every stream starts with `Register` followed by a `Heartbeat`; the hub handles a stream's
//! messages in order and fails the stream on a rejected registration, so the echo of that heartbeat
//! confirms the registration. Any failure closes the stream, notifies the owner with
//! [`Inbound::Disconnected`] and reconnects with exponential backoff (`backoff_min` doubling up to
//! `backoff_max`, reset after a confirmed registration). An `ALREADY_EXISTS` rejection (type owned
//! by another service) is kept in [`HubConnection::last_rejection`] and retried at `backoff_max`.

use std::sync::atomic::{AtomicBool, Ordering};
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
use crate::proto::{self, ClientMessage, Heartbeat, Register, client_message, server_message};

#[derive(Clone, Debug)]
pub struct HubSettings {
    /// `host:port`, optionally prefixed with `static://` or `http://`.
    pub address: String,
    pub heartbeat_interval: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// How long a new stream may take to confirm the registration.
    pub handshake_timeout: Duration,
    pub max_concurrent_handlers: usize,
}

impl Default for HubSettings {
    fn default() -> Self {
        HubSettings {
            address: "static://kraken-hub:7000".into(),
            heartbeat_interval: Duration::from_secs(10),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
            handshake_timeout: Duration::from_secs(10),
            max_concurrent_handlers: 256,
        }
    }
}

/// What the hub sends us, handed to the owner (dispatcher and sender) in order.
#[derive(Debug)]
pub enum Inbound {
    Request(proto::Request),
    Reply(proto::Reply),
    Error(proto::Error),
    /// The stream was lost; every pending request should fail.
    Disconnected,
}

pub struct HubConnection {
    settings: HubSettings,
    service_name: String,
    instance_id: String,
    inbound: mpsc::Sender<Inbound>,
    types: Mutex<(Vec<String>, Vec<String>)>,
    outbound: Mutex<Option<mpsc::Sender<ClientMessage>>>,
    connected: AtomicBool,
    registered: watch::Sender<bool>,
    last_rejection: Mutex<Option<String>>,
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
        let (registered, _) = watch::channel(false);
        Arc::new(HubConnection {
            settings,
            service_name,
            instance_id,
            inbound,
            types: Mutex::new((vec![], vec![])),
            outbound: Mutex::new(None),
            connected: AtomicBool::new(false),
            registered,
            last_rejection: Mutex::new(None),
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

    /// True once the hub echoed the heartbeat that follows our `Register` on the current stream.
    pub async fn await_registered(&self, timeout: Duration) -> bool {
        let mut rx = self.registered.subscribe();
        if *rx.borrow() {
            return true;
        }
        tokio::time::timeout(timeout, async {
            while rx.changed().await.is_ok() {
                if *rx.borrow() {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false)
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub fn last_rejection(&self) -> Option<String> {
        self.last_rejection.lock().unwrap().clone()
    }

    /// Writes to the current stream; `HubUnavailable` when there is none.
    pub async fn send(&self, msg: ClientMessage) -> Result<(), MessagingError> {
        let tx = self.outbound.lock().unwrap().clone();
        match tx {
            Some(tx) => tx
                .send(msg)
                .await
                .map_err(|_| MessagingError::HubUnavailable("hub stream closed".into())),
            None => Err(MessagingError::HubUnavailable(
                "not connected to the hub".into(),
            )),
        }
    }

    /// Stops the loop and closes the stream (the hub then drops this instance immediately).
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let handle = self.task.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        *self.outbound.lock().unwrap() = None;
        self.connected.store(false, Ordering::SeqCst);
        self.registered.send_replace(false);
    }

    async fn connect_loop(self: Arc<Self>) {
        let mut backoff = self.settings.backoff_min;
        let mut failures: u32 = 0;
        while !self.cancel.is_cancelled() {
            let outcome = self.run_stream().await;
            let was_connected = self.connected.swap(false, Ordering::SeqCst);
            self.registered.send_replace(false);
            *self.outbound.lock().unwrap() = None;
            let _ = self.inbound.send(Inbound::Disconnected).await;
            if self.cancel.is_cancelled() {
                break;
            }
            let status = match outcome {
                Ok(()) => Status::unavailable("hub closed the stream"),
                Err(s) => s,
            };
            if was_connected {
                backoff = self.settings.backoff_min;
                failures = 0;
            }
            failures += 1;
            let wait = if status.code() == Code::AlreadyExists {
                *self.last_rejection.lock().unwrap() = Some(status.message().to_string());
                tracing::error!(service = %self.service_name, instance = %self.instance_id, reason = status.message(), retry_in = ?self.settings.backoff_max, "hub rejected the registration");
                self.settings.backoff_max
            } else {
                if failures == 1 {
                    tracing::warn!(service = %self.service_name, instance = %self.instance_id, reason = %describe(&status), retry_in = ?backoff, "hub stream closed; reconnecting");
                } else {
                    tracing::debug!(service = %self.service_name, instance = %self.instance_id, reason = %describe(&status), retry_in = ?backoff, "hub stream closed; reconnecting");
                }
                let current = backoff;
                backoff = (backoff * 2).min(self.settings.backoff_max);
                current
            };
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.cancel.cancelled() => break,
            }
        }
    }

    /// One stream from connect to failure. `Ok(())` when the hub ended it or we were cancelled.
    async fn run_stream(&self) -> Result<(), Status> {
        let endpoint = Endpoint::from_shared(grpc_target(&self.settings.address))
            .map_err(|e| Status::invalid_argument(format!("bad hub address: {e}")))?
            .connect_timeout(self.settings.handshake_timeout);
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let mut client = HubClient::new(channel);
        let (tx, rx) = mpsc::channel::<ClientMessage>(256);
        let mut stream = client.connect(ReceiverStream::new(rx)).await?.into_inner();

        // The Register is the first message of every stream, confirmed by the echo of the heartbeat after it.
        let (commands, queries) = self.types.lock().unwrap().clone();
        let register = Register {
            service_name: self.service_name.clone(),
            instance_id: self.instance_id.clone(),
            command_types: commands,
            query_types: queries,
        };
        tx.send(ClientMessage {
            message: Some(client_message::Message::Register(register)),
        })
        .await
        .map_err(|_| Status::unavailable("stream closed before Register"))?;
        tx.send(heartbeat())
            .await
            .map_err(|_| Status::unavailable("stream closed before first heartbeat"))?;
        *self.outbound.lock().unwrap() = Some(tx.clone());
        self.connected.store(true, Ordering::SeqCst);

        let mut ticker = tokio::time::interval(self.settings.heartbeat_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // the first tick fires immediately; the first heartbeat was already sent
        let heartbeat_timeout = self.settings.heartbeat_interval * 3;
        let mut last_echo = tokio::time::Instant::now();
        let handshake_deadline = tokio::time::Instant::now() + self.settings.handshake_timeout;
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    if last_echo.elapsed() > heartbeat_timeout {
                        return Err(Status::unavailable(format!("no heartbeat echo for {:?}", heartbeat_timeout)));
                    }
                    if !*self.registered.borrow() && tokio::time::Instant::now() > handshake_deadline {
                        return Err(Status::deadline_exceeded("registration not confirmed in time"));
                    }
                    tx.send(heartbeat()).await.map_err(|_| Status::unavailable("stream closed"))?;
                }
                msg = stream.message() => match msg? {
                    None => return Ok(()),
                    Some(proto::ServerMessage { message: None }) => {}
                    Some(proto::ServerMessage { message: Some(m) }) => match m {
                        server_message::Message::Heartbeat(_) => {
                            last_echo = tokio::time::Instant::now();
                            if !*self.registered.borrow() {
                                tracing::info!(service = %self.service_name, instance = %self.instance_id, "registered with the hub");
                                self.registered.send_replace(true);
                            }
                        }
                        server_message::Message::Request(r) => self.forward(Inbound::Request(r)).await?,
                        server_message::Message::Reply(r) => self.forward(Inbound::Reply(r)).await?,
                        server_message::Message::Error(e) => self.forward(Inbound::Error(e)).await?,
                    },
                },
            }
        }
    }

    async fn forward(&self, msg: Inbound) -> Result<(), Status> {
        self.inbound
            .send(msg)
            .await
            .map_err(|_| Status::cancelled("owner dropped the inbound channel"))
    }
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
    use super::grpc_target;

    #[test]
    fn target_forms() {
        assert_eq!(grpc_target("static://hub:7000"), "http://hub:7000");
        assert_eq!(grpc_target("hub:7000"), "http://hub:7000");
        assert_eq!(
            grpc_target("http://localhost:7002"),
            "http://localhost:7002"
        );
    }
}
