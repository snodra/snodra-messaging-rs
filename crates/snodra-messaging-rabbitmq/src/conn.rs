//! Connections to the nodes, on the client's raw [`Client`]: one handler per connection routes
//! what the broker pushes (chunks, consumer updates, publish confirms) to whoever asked for it,
//! and [`Nodes`] keeps one connection per node, reaching each stream on its leader.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rabbitmq_stream_client::error::ClientError;
use rabbitmq_stream_client::types::{Message, Response, ResponseCode, ResponseKind};
use rabbitmq_stream_client::{Client, ClientOptions, TlsConfiguration};
use snodra_messaging::event_store::EventStoreError;
use tokio::sync::{mpsc, oneshot};

use crate::config::RabbitConfig;
use crate::wire;

/// The longest one request on a connection may wait for its answer.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest a connection's open or close may take.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// With a load balancer: how many connections through it may land on another node before the
/// store gives up reaching the one it needs.
const BALANCER_TRIES: usize = 32;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// A failure that another try may get past.
pub(crate) fn failed(e: impl std::fmt::Display) -> EventStoreError {
    EventStoreError::Failed(format!("rabbitmq: {e}"))
}

/// A delivered chunk: its messages are at `first_offset`, `first_offset + 1`, ...
#[derive(Debug)]
pub(crate) struct Chunk {
    pub first_offset: u64,
    pub timestamp_ms: i64,
    pub messages: Vec<Message>,
}

/// What the broker pushes for one subscription.
#[derive(Debug)]
pub(crate) enum Frame {
    Chunk(Chunk),
    /// Single active consumer: this subscription becomes the stream's active one (or stops
    /// being it); the broker waits for the answer.
    Update {
        correlation_id: u32,
        active: bool,
    },
    /// The connection is gone, or the stream moved or was deleted: the subscription is over.
    Closed(String),
}

type Confirm = oneshot::Sender<Result<(), ResponseCode>>;

/// Where the pushes of one connection go.
#[derive(Default)]
struct Routes {
    /// Per subscription id: its stream and its frames.
    subs: Mutex<HashMap<u8, (String, mpsc::UnboundedSender<Frame>)>>,
    /// Per publishing id: who waits for its confirm.
    confirms: Mutex<HashMap<u64, Confirm>>,
    closed: AtomicBool,
}

impl Routes {
    fn send(&self, sub: u8, frame: Frame) {
        if let Some((_, tx)) = lock(&self.subs).get(&sub) {
            let _ = tx.send(frame);
        }
    }

    fn close(&self, why: &str) {
        self.closed.store(true, Ordering::SeqCst);
        for (_, (_, tx)) in lock(&self.subs).drain() {
            let _ = tx.send(Frame::Closed(why.to_string()));
        }
        // Dropping the senders fails every waiting publish.
        lock(&self.confirms).clear();
    }
}

/// What the handler of a connection does with one push. The client runs its handler on a task
/// of its own per push, so pushes can be handled out of order: a subscription keeps one chunk in
/// flight (one credit), and each push is only queued here.
fn route(routes: &Routes, item: Option<Result<Response, ClientError>>) {
    match item {
        Some(Ok(response)) => match response.kind_ref() {
            ResponseKind::Deliver(deliver) => match wire::chunk_header(deliver) {
                Some(header) => routes.send(
                    header.subscription_id,
                    Frame::Chunk(Chunk {
                        first_offset: header.first_offset,
                        timestamp_ms: header.timestamp_ms,
                        messages: deliver.messages.clone(),
                    }),
                ),
                // Never waiting on a chunk that could not be read: its subscription ends.
                None => routes.send(
                    deliver.subscription_id,
                    Frame::Closed("a delivered chunk could not be read".into()),
                ),
            },
            ResponseKind::ConsumerUpdate(update) => match wire::consumer_update(update) {
                Some((correlation_id, sub, active)) => routes.send(
                    sub,
                    Frame::Update {
                        correlation_id,
                        active,
                    },
                ),
                None => routes.close("a consumer update could not be read"),
            },
            ResponseKind::PublishConfirm(confirm) => {
                let mut waiting = lock(&routes.confirms);
                for id in &confirm.publishing_ids {
                    if let Some(tx) = waiting.remove(id) {
                        let _ = tx.send(Ok(()));
                    }
                }
            }
            ResponseKind::PublishError(errors) => {
                let mut waiting = lock(&routes.confirms);
                for e in &errors.publishing_errors {
                    if let Some(tx) = waiting.remove(&e.publishing_id) {
                        let _ = tx.send(Err(e.error_code.clone()));
                    }
                }
            }
            ResponseKind::MetadataUpdate(update) => {
                let stream = wire::metadata_update_stream(update);
                let subs = lock(&routes.subs);
                for (s, tx) in subs.values() {
                    // A stream that cannot be read from the update: every subscription ends.
                    if stream.as_ref().is_none_or(|stream| s == stream) {
                        let _ = tx.send(Frame::Closed(format!(
                            "the stream {} changed (deleted or moved)",
                            stream.as_deref().unwrap_or(s)
                        )));
                    }
                }
            }
            _ => {}
        },
        Some(Err(e)) => routes.close(&format!("the connection failed: {e}")),
        None => routes.close("the connection closed"),
    }
}

/// One connection to a node.
pub(crate) struct Conn {
    pub client: Client,
    routes: Arc<Routes>,
    /// The node as the broker advertises it.
    pub node: (String, u32),
    next_sub: AtomicU8,
    /// A close was started on a task (at most one per connection).
    closing: AtomicBool,
    next_publisher: std::sync::atomic::AtomicU16,
}

impl Conn {
    async fn open(options: ClientOptions) -> Result<Conn, ClientError> {
        let client = Client::connect(options).await?;
        let routes = Arc::new(Routes::default());
        let handler_routes = Arc::clone(&routes);
        client
            .set_handler(move |item: Option<Result<Response, ClientError>>| {
                let routes = Arc::clone(&handler_routes);
                async move {
                    route(&routes, item);
                    Ok(())
                }
            })
            .await;
        let properties = client.connection_properties().await;
        let node = (
            properties
                .get("advertised_host")
                .cloned()
                .unwrap_or_default(),
            properties
                .get("advertised_port")
                .and_then(|p| p.parse().ok())
                .unwrap_or(0),
        );
        Ok(Conn {
            client,
            routes,
            node,
            next_sub: AtomicU8::new(0),
            closing: AtomicBool::new(false),
            next_publisher: std::sync::atomic::AtomicU16::new(0),
        })
    }

    /// Whether the connection answers a request within [`REQUEST_TIMEOUT`].
    ///
    /// The request is a query of an offset (on a stream this connection reads): its answer
    /// carries no text. The client's decoder panics on a frame split across reads when the frame
    /// holds text (a metadata answer listing many streams, for one), which would stop the very
    /// connection being checked.
    pub async fn answers(&self) -> bool {
        let stream = lock(&self.routes.subs)
            .values()
            .next()
            .map(|(s, _)| s.clone());
        let Some(stream) = stream else {
            return true;
        };
        let query = self
            .client
            .query_offset("snodra-liveness".to_string(), &stream);
        matches!(
            tokio::time::timeout(REQUEST_TIMEOUT, query).await,
            Ok(Ok(_) | Err(ClientError::RequestError(_)))
        )
    }

    /// `request` bounded by [`REQUEST_TIMEOUT`]. A connection that does not answer in time is
    /// taken as gone (its subscriptions end, the next call opens another): the client reads
    /// every frame on one task, which its decoder stops on some frames (see `answers`).
    pub async fn timed<T>(
        &self,
        request: impl std::future::Future<Output = Result<T, ClientError>>,
    ) -> Result<T, ClientError> {
        match tokio::time::timeout(REQUEST_TIMEOUT, request).await {
            Ok(answer) => answer,
            Err(_) => {
                self.routes.close(&format!(
                    "the connection gave no answer within {REQUEST_TIMEOUT:?}"
                ));
                Err(ClientError::ConnectionClosed)
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.routes.closed.load(Ordering::SeqCst) || self.client.is_closed()
    }

    /// A free subscription id and the frames the broker pushes for it, reading `stream`.
    pub fn route(
        &self,
        stream: &str,
    ) -> Result<(u8, mpsc::UnboundedReceiver<Frame>), EventStoreError> {
        let mut subs = lock(&self.routes.subs);
        for _ in 0..=u8::MAX as usize {
            let id = self.next_sub.fetch_add(1, Ordering::SeqCst);
            if let std::collections::hash_map::Entry::Vacant(free) = subs.entry(id) {
                let (tx, rx) = mpsc::unbounded_channel();
                free.insert((stream.to_string(), tx));
                return Ok((id, rx));
            }
        }
        Err(failed("256 subscriptions on one connection"))
    }

    pub fn unroute(&self, sub: u8) {
        lock(&self.routes.subs).remove(&sub);
    }

    /// The next publisher id of this connection (each stream published to gets one); `None`
    /// once the 256 ids of the protocol are used (the caller opens another connection, never
    /// reusing an id that may still be declared).
    pub fn publisher_id(&self) -> Option<u8> {
        u8::try_from(self.next_publisher.fetch_add(1, Ordering::SeqCst)).ok()
    }

    /// Waits for the confirm (or the error) of `publishing_id`; closed without one when the
    /// connection goes.
    pub fn confirm(&self, publishing_id: u64) -> oneshot::Receiver<Result<(), ResponseCode>> {
        let (tx, rx) = oneshot::channel();
        lock(&self.routes.confirms).insert(publishing_id, tx);
        if self.routes.closed.load(Ordering::SeqCst) {
            lock(&self.routes.confirms).remove(&publishing_id);
        }
        rx
    }

    pub fn forget_confirm(&self, publishing_id: u64) {
        lock(&self.routes.confirms).remove(&publishing_id);
    }

    /// Closes the connection, bounded by [`CONNECT_TIMEOUT`]. A connection that does not answer
    /// its close (its read task stopped) cannot be closed through the client, so its heartbeats
    /// are stopped instead: the broker then drops it after two missed heartbeats (2 x 60 s), and
    /// nothing of the add-on waits on it any longer.
    pub async fn close(&self) {
        self.routes.close("closed");
        if self.client.is_closed() {
            return;
        }
        if tokio::time::timeout(CONNECT_TIMEOUT, self.client.close())
            .await
            .is_err()
        {
            let _ = tokio::time::timeout(CONNECT_TIMEOUT, self.client.set_heartbeat(0)).await;
        }
    }

    /// Closes the connection on a task of its own (bounded as [`close`](Self::close)).
    pub fn close_detached(self: &Arc<Self>) {
        if self.closing.swap(true, Ordering::SeqCst) {
            return;
        }
        let conn = Arc::clone(self);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move { conn.close().await });
        }
    }

    /// Takes the connection as gone without waiting: what waits on it fails, and it is closed
    /// on a task of its own.
    pub fn abandon(self: &Arc<Self>, why: &str) {
        self.routes.close(why);
        self.close_detached();
    }

    /// The stored offset of `reference` on `stream` (the next offset it reads); `None` when it
    /// has none.
    pub async fn stored_offset(
        &self,
        reference: &str,
        stream: &str,
    ) -> Result<Option<u64>, EventStoreError> {
        let query = self.client.query_offset(reference.to_string(), stream);
        match self.timed(query).await {
            Ok(offset) => Ok(Some(offset)),
            Err(ClientError::RequestError(ResponseCode::OffsetNotFound)) => Ok(None),
            Err(e) => Err(failed(format!(
                "the stored offset of {reference} on {stream}: {e}"
            ))),
        }
    }

    /// Stores `offset` for `reference` on `stream`, and waits (at most `within`) until a query
    /// reads it back: the broker applies a stored offset asynchronously.
    pub async fn store_offset(
        &self,
        reference: &str,
        stream: &str,
        offset: u64,
        within: Duration,
    ) -> Result<(), EventStoreError> {
        self.client
            .store_offset(reference, stream, offset)
            .await
            .map_err(|e| {
                failed(format!(
                    "storing the offset of {reference} on {stream}: {e}"
                ))
            })?;
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if self.stored_offset(reference, stream).await? == Some(offset) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failed(format!(
                    "the offset {offset} of {reference} on {stream} was not stored within {within:?}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// How to reach the nodes.
#[derive(Clone)]
pub(crate) struct Broker {
    config: Arc<RabbitConfig>,
    addresses: Vec<(String, u16)>,
    /// The connection name the broker shows (`client_provided_name`).
    name: String,
}

impl Broker {
    /// Checked by the caller (`RabbitConfig::validate`).
    pub fn new(config: Arc<RabbitConfig>, name: &str) -> Result<Broker, EventStoreError> {
        Ok(Broker {
            addresses: config.addresses()?,
            config,
            name: name.to_string(),
        })
    }

    pub fn config(&self) -> &RabbitConfig {
        &self.config
    }

    fn options(&self, host: &str, port: u16) -> Result<ClientOptions, EventStoreError> {
        let tls = if self.config.tls {
            install_tls_provider();
            let ca = self
                .config
                .tls_ca_file
                .clone()
                .ok_or_else(|| EventStoreError::Invalid("TLS needs a CA file".into()))?;
            TlsConfiguration::builder()
                .add_root_certificates(ca)
                .build()
                .map_err(|e| EventStoreError::Invalid(format!("TLS: {e}")))?
        } else {
            TlsConfiguration::Disabled
        };
        Ok(ClientOptions::builder()
            .host(host)
            .port(port)
            .user(&self.config.username)
            .password(&self.config.password)
            .v_host(&self.config.virtual_host)
            .tls(tls)
            .client_provided_name(self.name.clone())
            .build())
    }

    async fn open(&self, host: &str, port: u16) -> Result<Conn, EventStoreError> {
        let options = self.options(host, port)?;
        match tokio::time::timeout(CONNECT_TIMEOUT, Conn::open(options)).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(e)) => Err(failed(format!("cannot connect to {host}:{port}: {e}"))),
            Err(_) => Err(failed(format!(
                "cannot connect to {host}:{port} within {CONNECT_TIMEOUT:?}"
            ))),
        }
    }

    /// A connection to the first configured host that answers.
    async fn any(&self) -> Result<Conn, EventStoreError> {
        let mut last = failed("no host is configured");
        for (host, port) in &self.addresses {
            match self.open(host, *port).await {
                Ok(conn) => return Ok(conn),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    /// A connection to `node` (as advertised): directly, or through the configured hosts until
    /// one lands on it (load balancer).
    async fn to_node(&self, node: &(String, u32)) -> Result<Conn, EventStoreError> {
        if !self.config.load_balancer {
            return self.open(&node.0, node.1 as u16).await;
        }
        for _ in 0..BALANCER_TRIES {
            let conn = self.any().await?;
            if &conn.node == node {
                return Ok(conn);
            }
            conn.close().await;
        }
        Err(failed(format!(
            "the load balancer never led to node {}:{} in {BALANCER_TRIES} connections",
            node.0, node.1
        )))
    }
}

/// Install ring as the process's TLS provider when none is (the client's TLS takes the
/// process's default; with two providers compiled in, rustls has none until one is installed).
fn install_tls_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

/// One partition stream of a source, and its segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Part {
    pub segment: String,
    pub stream: String,
}

/// Why a stream has no leader to talk to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NoLeader {
    /// The metadata answer names no leader for it: a leader election under way (a node lost),
    /// or a stream that does not exist; the answer does not tell them apart.
    NotInMetadata,
    /// The broker answered that the stream does not exist.
    DoesNotExist,
}

impl NoLeader {
    pub fn error(self, stream: &str) -> EventStoreError {
        match self {
            NoLeader::NotInMetadata => failed(format!(
                "the stream {stream} is unavailable right now: the broker's metadata names no \
                 leader for it (it may have none during a leader election, or the stream may not \
                 exist); the call is retried"
            )),
            NoLeader::DoesNotExist => failed(format!(
                "the broker says the stream {stream} does not exist"
            )),
        }
    }
}

/// One connection per node, opened as needed.
pub(crate) struct Nodes {
    broker: Broker,
    seed: Option<Arc<Conn>>,
    conns: HashMap<(String, u32), Arc<Conn>>,
}

impl Nodes {
    pub fn new(broker: Broker) -> Nodes {
        Nodes {
            broker,
            seed: None,
            conns: HashMap::new(),
        }
    }

    /// A connection to any node, for the metadata and partition queries only.
    pub async fn seed(&mut self) -> Result<Arc<Conn>, EventStoreError> {
        if let Some(seed) = self.seed.as_ref().filter(|c| !c.is_closed()) {
            return Ok(Arc::clone(seed));
        }
        // The seed carries the metadata and partition queries only, never a stream's data: the
        // client's decoder panics on an answer holding text (a metadata answer) that a read
        // splits, which a connection busy with chunks makes likely, and stops reading.
        let conn = Arc::new(self.broker.any().await?);
        self.seed = Some(Arc::clone(&conn));
        Ok(conn)
    }

    /// A connection to the leader of `stream`, or why there is none: [`NoLeader`].
    pub async fn leader(
        &mut self,
        stream: &str,
    ) -> Result<Result<Arc<Conn>, NoLeader>, EventStoreError> {
        let seed = self.seed().await?;
        let metadata = seed
            .timed(seed.client.metadata(vec![stream.to_string()]))
            .await
            .map_err(failed)?;
        // The client leaves out a stream whose answer names no leader, whatever its code: a
        // stream electing one and a stream that does not exist look the same here.
        let Some(found) = metadata.get(stream) else {
            return Ok(Err(NoLeader::NotInMetadata));
        };
        if found.response_code != ResponseCode::Ok {
            return match found.response_code {
                ResponseCode::StreamDoesNotExist => Ok(Err(NoLeader::DoesNotExist)),
                ref code => Err(failed(format!("the stream {stream}: {code:?}"))),
            };
        }
        let node = (found.leader.host.clone(), found.leader.port);
        if let Some(conn) = self.conns.get(&node).filter(|c| !c.is_closed()) {
            return Ok(Ok(Arc::clone(conn)));
        }
        let conn = Arc::new(self.broker.to_node(&node).await?);
        self.conns.insert(node, Arc::clone(&conn));
        Ok(Ok(conn))
    }

    /// [`leader`](Self::leader), with no leader a `Failed` saying why (retried by the caller).
    pub async fn require_leader(&mut self, stream: &str) -> Result<Arc<Conn>, EventStoreError> {
        self.leader(stream).await?.map_err(|no| no.error(stream))
    }

    /// The streams of `source`, in segment order of the broker's partition order: the
    /// partitions of the super stream `source`, else the stream `source` alone (segment `0`);
    /// `None` when neither exists.
    pub async fn layout(&mut self, source: &str) -> Result<Option<Vec<Part>>, EventStoreError> {
        let seed = self.seed().await?;
        let partitions = seed
            .timed(seed.client.partitions(source.to_string()))
            .await
            .map_err(failed)?;
        if partitions.is_ok() && !partitions.streams.is_empty() {
            let mut parts = Vec::with_capacity(partitions.streams.len());
            for stream in &partitions.streams {
                let segment = wire::segment_of(source, stream);
                if !wire::segment_fits(&segment) {
                    return Err(EventStoreError::Invalid(format!(
                        "the partition {stream} of {source} names a segment over 128 bytes"
                    )));
                }
                parts.push(Part {
                    segment,
                    stream: stream.clone(),
                });
            }
            return Ok(Some(parts));
        }
        let metadata = seed
            .timed(seed.client.metadata(vec![source.to_string()]))
            .await
            .map_err(failed)?;
        Ok(metadata
            .get(source)
            .filter(|m| m.response_code == ResponseCode::Ok)
            .map(|_| {
                vec![Part {
                    segment: wire::SINGLE_SEGMENT.to_string(),
                    stream: source.to_string(),
                }]
            }))
    }

    /// Forgets `conn` (the next call opens another to its node) and closes it on a task.
    pub fn forget(&mut self, conn: &Arc<Conn>) {
        self.conns.retain(|_, c| !Arc::ptr_eq(c, conn));
        if self.seed.as_ref().is_some_and(|c| Arc::ptr_eq(c, conn)) {
            self.seed = None;
        }
        conn.close_detached();
    }

    /// Forgets the connections that are gone (they reconnect on the next call).
    pub fn prune(&mut self) {
        self.conns.retain(|_, c| {
            let gone = c.is_closed();
            if gone {
                c.close_detached();
            }
            !gone
        });
        if let Some(seed) = self.seed.take_if(|c| c.is_closed()) {
            seed.close_detached();
        }
    }

    /// The connections held now (the seed included).
    pub fn held(&self) -> usize {
        self.conns.len() + usize::from(self.seed.is_some())
    }

    /// Closes every connection.
    pub async fn close(&mut self) {
        let mut conns: Vec<Arc<Conn>> = self.conns.drain().map(|(_, c)| c).collect();
        conns.extend(self.seed.take());
        futures::future::join_all(conns.iter().map(|c| c.close())).await;
    }
}

impl Drop for Nodes {
    /// Connections left open (a caller that never closed) are closed on a task: the client keeps
    /// a connection alive as long as its own tasks run.
    fn drop(&mut self) {
        let mut conns: Vec<Arc<Conn>> = self.conns.drain().map(|(_, c)| c).collect();
        conns.extend(self.seed.take());
        if conns.iter().all(|c| c.is_closed()) {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                futures::future::join_all(conns.iter().map(|c| c.close())).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stream without a leader in the metadata is said to be unavailable right now (an election
    /// looks the same), never flatly "does not exist"; only the broker's own code says that.
    #[test]
    fn no_leader_says_why() {
        let EventStoreError::Failed(m) = NoLeader::NotInMetadata.error("kraken.events-0") else {
            panic!("Failed")
        };
        assert!(
            m.contains("kraken.events-0 is unavailable right now") && m.contains("no leader"),
            "{m}"
        );
        let EventStoreError::Failed(m) = NoLeader::DoesNotExist.error("kraken.events-0") else {
            panic!("Failed")
        };
        assert!(
            m.contains("the broker says the stream kraken.events-0 does not exist"),
            "{m}"
        );
    }

    /// TLS options build with the CA file (read when a connection opens) and a provider
    /// installed for the process; without TLS nothing is installed.
    #[test]
    fn tls_options_take_the_ca_file() {
        let config = Arc::new(RabbitConfig::new("r").tls("/etc/snodra/ca.pem"));
        let broker = Broker::new(config, "test").unwrap();
        assert!(broker.options("r", 5551).is_ok());
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        let plain = Broker::new(Arc::new(RabbitConfig::new("r")), "test").unwrap();
        assert!(plain.options("r", 5552).is_ok());
    }

    /// A host that does not answer fails the call, naming it, within the connect bound.
    #[tokio::test]
    async fn an_unreachable_host_fails_naming_it() {
        let broker = Broker::new(Arc::new(RabbitConfig::new("127.0.0.1:1")), "test").unwrap();
        let mut nodes = Nodes::new(broker);
        let failed = nodes.layout("kraken.events").await;
        assert!(
            matches!(&failed, Err(EventStoreError::Failed(m)) if m.contains("127.0.0.1:1")),
            "{failed:?}"
        );
    }
}

#[cfg(test)]
mod ids {
    /// The publisher ids of a connection end after 256, never wrapping onto one in use.
    #[test]
    fn publisher_ids_never_wrap() {
        let next = std::sync::atomic::AtomicU16::new(0);
        let id = || u8::try_from(next.fetch_add(1, std::sync::atomic::Ordering::SeqCst)).ok();
        let all: Vec<Option<u8>> = (0..257).map(|_| id()).collect();
        assert_eq!(all[0], Some(0));
        assert_eq!(all[255], Some(255));
        assert_eq!(all[256], None);
    }
}
