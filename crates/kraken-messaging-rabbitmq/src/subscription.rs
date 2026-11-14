//! A subscription on RabbitMQ Streams: one consumer named `<service>.<processor>` per partition
//! stream of the source, each a single active consumer (with the property `super-stream` for a
//! super stream, so the broker spreads the partitions over the consumers sharing the name).
//!
//! - **Assignment** is the broker's consumer updates: a partition whose consumer becomes active
//!   is `Assigned`, one that stops being active is `Revoked` followed by an empty `Assigned`
//!   (the change is over). On activation the subscription
//!   reads the offset stored under its name and answers the broker with it (the first offset
//!   when none is stored); on deactivation it stores what was acknowledged first.
//! - **Acknowledgement** stores the offset after the acknowledged delivery under the
//!   subscription's name (server-side offset tracking), asynchronously; [`close`] stores what is
//!   left and waits until the broker reads it back.
//! - **Flow**: each partition has one chunk in flight (one credit); the next is asked for once
//!   the deliveries waiting are under [`READY_LIMIT`], never while paused.
//! - **Redelivery** cannot seek a single active consumer in place: the deliveries handed out and
//!   not acknowledged are kept, and [`redeliver`] hands them out again ahead of the later ones.
//! - **Pause** hands nothing out and asks for nothing more; the consumers stay active, so the
//!   partitions stay this subscription's.
//! - **Status**: every [`OFFSETS_REFRESH`] a task of the subscription reads the head of each
//!   active partition (on connections of its own); [`segments`] reads what it last found.
//! - **Errors**: a lost connection, or a partition stream deleted or moved, ends the
//!   subscription: `next` fails (the caller subscribes again).
//!
//! [`close`]: Subscription::close
//! [`redeliver`]: Subscription::redeliver
//! [`segments`]: Subscription::segments

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use kraken_messaging::event_store::{
    Delivery, EventStoreError, SegmentStatus, Subscription, SubscriptionEvent, SubscriptionSpec,
    segment_order,
};
use rabbitmq_stream_client::types::OffsetSpecification;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::conn::{Broker, Chunk, Conn, Frame, Nodes, failed, lock};
use crate::read;
use crate::wire;

/// How long the status refresher waits between two reads of the heads (ADR 0022: at most every
/// 5 s).
pub const OFFSETS_REFRESH: Duration = Duration::from_secs(5);
/// Until every active partition's head is known: how long between two reads.
const OFFSETS_FIRST: Duration = Duration::from_millis(500);
/// Deliveries waiting per partition above which no further chunk is asked for.
pub const READY_LIMIT: usize = 1_000;
/// How long a stored offset may take to be read back (at close, at deactivation).
const STORE_WAIT: Duration = Duration::from_secs(5);
/// The subscribe property that tells the members of one subscription apart (the management
/// API shows it).
pub const MEMBER_PROPERTY: &str = "kraken-member";

/// What a partition task is asked to do.
enum Cmd {
    /// Store the acknowledged offset (if it moved).
    Store,
    /// Ask for the next chunk (owed since a pause or a full buffer).
    Credit,
    /// Store the acknowledged offset and wait until the broker reads it back.
    Flush(oneshot::Sender<()>),
}

struct Part {
    segment: String,
    stream: String,
    active: bool,
    /// The next offset taken from the broker (a chunk may start before it).
    next: u64,
    /// Received, not handed out.
    ready: VecDeque<Delivery>,
    /// Handed out, not acknowledged.
    inflight: VecDeque<Delivery>,
    /// The offset after the last acknowledged delivery.
    acked: Option<u64>,
    /// The offset stored under the name when the partition became active.
    stored: Option<u64>,
    head: Option<u64>,
    /// A chunk was received and the next not asked for yet.
    owed: bool,
    cmds: mpsc::UnboundedSender<Cmd>,
}

#[derive(Default)]
struct State {
    changes: VecDeque<SubscriptionEvent>,
    /// In segment order.
    parts: Vec<Part>,
    paused: bool,
    broken: Option<EventStoreError>,
    error: Option<String>,
}

impl State {
    fn part(&mut self, segment: &str) -> Option<&mut Part> {
        self.parts.iter_mut().find(|p| p.segment == segment)
    }
}

struct Shared {
    state: Mutex<State>,
    notify: Notify,
    name: String,
    source: String,
}

impl Shared {
    fn wake(&self) {
        self.notify.notify_one();
    }

    fn broken(&self, why: String) {
        tracing::warn!(source = %self.source, subscription = %self.name, "the subscription ends: {why}");
        lock(&self.state).broken.get_or_insert(failed(why));
        self.wake();
    }
}

fn offset(d: &Delivery) -> u64 {
    d.position.parse().unwrap_or(0)
}

/// One instance's subscription of a name on a source. Use [`close`](Subscription::close): it
/// stores the acknowledged offsets and leaves; dropping the subscription stores nothing (an
/// unacknowledged delivery comes again) and closes its connections on a task.
pub struct RabbitSubscription {
    shared: Arc<Shared>,
    nodes: Nodes,
    subs: Vec<(Arc<Conn>, u8)>,
    tasks: Vec<JoinHandle<()>>,
    refresher: Option<JoinHandle<()>>,
    cancel: CancellationToken,
    _stop: DropGuard,
    /// Where `next` looks first, so every partition gets its turn.
    turn: usize,
}

impl RabbitSubscription {
    /// Joins `spec` as one more member (`member` tells it apart); must run inside a tokio
    /// runtime. `Failed` when the source does not exist.
    pub(crate) async fn join(
        broker: Broker,
        spec: &SubscriptionSpec,
        member: &str,
    ) -> Result<RabbitSubscription, EventStoreError> {
        if spec.source.trim().is_empty() {
            return Err(EventStoreError::Invalid("the source is empty".into()));
        }
        let mut nodes = Nodes::new(broker.clone());
        let layout = match nodes.layout(&spec.source).await {
            Ok(Some(layout)) => layout,
            Ok(None) => {
                nodes.close().await;
                return Err(failed(format!(
                    "neither a super stream nor a stream named {} exists: create it \
                     (rabbitmq-streams add_super_stream {} --partitions 6)",
                    spec.source, spec.source
                )));
            }
            Err(e) => {
                nodes.close().await;
                return Err(e);
            }
        };
        let super_stream = !(layout.len() == 1 && layout[0].stream == spec.source);
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            notify: Notify::new(),
            name: spec.name.clone(),
            source: spec.source.clone(),
        });
        let cancel = CancellationToken::new();
        let mut subscription = RabbitSubscription {
            shared: Arc::clone(&shared),
            nodes,
            subs: Vec::new(),
            tasks: Vec::new(),
            refresher: None,
            _stop: cancel.clone().drop_guard(),
            cancel: cancel.clone(),
            turn: 0,
        };
        let mut parts = layout;
        parts.sort_by(|a, b| segment_order(&a.segment, &b.segment));
        for part in parts {
            if let Err(e) = subscription
                .consume(&part.segment, &part.stream, member, super_stream)
                .await
            {
                Box::new(subscription).close().await;
                return Err(e);
            }
        }
        subscription.refresher = Some(tokio::spawn(refresh(
            Nodes::new(broker),
            Arc::clone(&shared),
            cancel,
        )));
        Ok(subscription)
    }

    /// One single active consumer of `stream`.
    async fn consume(
        &mut self,
        segment: &str,
        stream: &str,
        member: &str,
        super_stream: bool,
    ) -> Result<(), EventStoreError> {
        let conn = self
            .nodes
            .leader(stream)
            .await?
            .ok_or_else(|| failed(format!("the stream {stream} does not exist")))?;
        let (sub, frames) = conn.route(stream)?;
        let (cmds, cmd_rx) = mpsc::unbounded_channel();
        lock(&self.shared.state).parts.push(Part {
            segment: segment.to_string(),
            stream: stream.to_string(),
            active: false,
            next: 0,
            ready: VecDeque::new(),
            inflight: VecDeque::new(),
            acked: None,
            stored: None,
            head: None,
            owed: false,
            cmds,
        });
        self.tasks.push(tokio::spawn(
            PartTask {
                shared: Arc::clone(&self.shared),
                conn: Arc::clone(&conn),
                sub,
                segment: segment.to_string(),
                stream: stream.to_string(),
                last_stored: None,
            }
            .run(frames, cmd_rx, self.cancel.clone()),
        ));
        self.subs.push((Arc::clone(&conn), sub));
        let mut properties = HashMap::from([
            ("single-active-consumer".to_string(), "true".to_string()),
            ("name".to_string(), self.shared.name.clone()),
            (MEMBER_PROPERTY.to_string(), member.to_string()),
        ]);
        if super_stream {
            properties.insert("super-stream".to_string(), self.shared.source.clone());
        }
        match conn
            .client
            .subscribe(sub, stream, OffsetSpecification::Next, 1, properties)
            .await
        {
            Ok(response) if response.is_ok() => Ok(()),
            Ok(response) => Err(failed(format!(
                "cannot consume {stream}: {:?}",
                response.code()
            ))),
            Err(e) => Err(failed(format!("cannot consume {stream}: {e}"))),
        }
    }
}

/// The task of one partition: the broker's pushes for it and the subscription's requests, one
/// at a time and in order.
struct PartTask {
    shared: Arc<Shared>,
    conn: Arc<Conn>,
    sub: u8,
    segment: String,
    stream: String,
    /// What this task last stored (or found stored).
    last_stored: Option<u64>,
}

impl PartTask {
    async fn run(
        mut self,
        mut frames: mpsc::UnboundedReceiver<Frame>,
        mut cmds: mpsc::UnboundedReceiver<Cmd>,
        cancel: CancellationToken,
    ) {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                cmd = cmds.recv() => match cmd {
                    None => return,
                    Some(Cmd::Store) => self.store().await,
                    Some(Cmd::Credit) => self.credit().await,
                    Some(Cmd::Flush(done)) => {
                        self.flush().await;
                        let _ = done.send(());
                    }
                },
                frame = frames.recv() => match frame {
                    None => return self.shared.broken(format!("{}: the connection closed", self.stream)),
                    Some(Frame::Closed(why)) => return self.shared.broken(format!("{}: {why}", self.stream)),
                    Some(Frame::Chunk(chunk)) => self.chunk(chunk).await,
                    Some(Frame::Update { correlation_id, active: true }) => self.activate(correlation_id).await,
                    Some(Frame::Update { correlation_id, active: false }) => self.deactivate(correlation_id).await,
                },
            }
        }
    }

    fn acked(&self) -> Option<u64> {
        let mut state = lock(&self.shared.state);
        state
            .part(&self.segment)
            .filter(|p| p.active)
            .and_then(|p| p.acked)
    }

    /// Stores the acknowledged offset if it moved (the broker applies it asynchronously).
    async fn store(&mut self) {
        let Some(acked) = self.acked() else { return };
        if self.last_stored.is_some_and(|s| s >= acked) {
            return;
        }
        match self
            .conn
            .client
            .store_offset(&self.shared.name, &self.stream, acked)
            .await
        {
            Ok(()) => self.last_stored = Some(acked),
            Err(e) => {
                tracing::warn!(stream = %self.stream, "the acknowledged offset was not stored: {e}");
                lock(&self.shared.state).error = Some(format!("storing the offset: {e}"));
            }
        }
    }

    /// Stores the acknowledged offset and waits until the broker reads it back.
    async fn flush(&mut self) {
        let Some(acked) = self.acked() else { return };
        if self.last_stored.is_some_and(|s| s > acked) {
            return;
        }
        match self
            .conn
            .store_offset(&self.shared.name, &self.stream, acked, STORE_WAIT)
            .await
        {
            Ok(()) => self.last_stored = Some(acked),
            Err(e) => {
                tracing::warn!(stream = %self.stream, "the acknowledged offset was not stored: {e}")
            }
        }
    }

    async fn credit(&self) {
        if let Err(e) = self.conn.client.credit(self.sub, 1).await {
            self.shared
                .broken(format!("{}: asking for the next chunk: {e}", self.stream));
        }
    }

    async fn chunk(&mut self, chunk: Chunk) {
        let credit = {
            let mut state = lock(&self.shared.state);
            let paused = state.paused;
            let Some(part) = state.part(&self.segment) else {
                return;
            };
            if part.active {
                for (i, message) in chunk.messages.iter().enumerate() {
                    let offset = chunk.first_offset + i as u64;
                    if offset >= part.next {
                        part.ready.push_back(wire::delivery(
                            &self.segment,
                            offset,
                            chunk.timestamp_ms,
                            message,
                        ));
                        part.next = offset + 1;
                    }
                }
                part.head = Some(part.head.unwrap_or(0).max(part.next));
            }
            // A chunk that came while not active (sent just before a deactivation) is dropped;
            // its credit is given back so the broker can send again once this is active.
            let credit = !part.active || (!paused && part.ready.len() < READY_LIMIT);
            part.owed = !credit;
            credit
        };
        self.shared.wake();
        if credit {
            self.credit().await;
        }
    }

    async fn activate(&mut self, correlation_id: u32) {
        let stored = match self
            .conn
            .stored_offset(&self.shared.name, &self.stream)
            .await
        {
            Ok(stored) => stored,
            Err(e) => {
                // Read again from the first offset: duplicates are skipped, a loss is not.
                tracing::warn!(stream = %self.stream, "the stored offset was not read, reading from the first: {e}");
                lock(&self.shared.state).error = Some(e.to_string());
                None
            }
        };
        self.last_stored = stored;
        let credit = {
            let mut state = lock(&self.shared.state);
            let paused = state.paused;
            let mut credit = false;
            if let Some(part) = state.part(&self.segment) {
                // A credit held back (a pause, a full buffer) before the last deactivation: the
                // broker has none left to send with.
                credit = part.owed && !paused;
                part.owed &= !credit;
                part.active = true;
                part.next = stored.unwrap_or(0);
                part.stored = stored;
                part.acked = None;
                part.ready.clear();
                part.inflight.clear();
                part.head = part.head.max(stored);
            }
            state
                .changes
                .push_back(SubscriptionEvent::Assigned(vec![self.segment.clone()]));
            credit
        };
        tracing::info!(stream = %self.stream, subscription = %self.shared.name, "partition assigned");
        self.shared.wake();
        if credit {
            self.credit().await;
        }
        self.answer(
            correlation_id,
            stored.map_or(OffsetSpecification::First, OffsetSpecification::Offset),
        );
    }

    async fn deactivate(&mut self, correlation_id: u32) {
        self.flush().await;
        let owed = {
            let mut state = lock(&self.shared.state);
            let mut owed = false;
            let was_active = state.part(&self.segment).is_some_and(|part| {
                let was = part.active;
                owed = std::mem::take(&mut part.owed);
                part.active = false;
                part.acked = None;
                part.ready.clear();
                part.inflight.clear();
                was
            });
            if was_active {
                // The partition's move is over once it is revoked here (the other instance's
                // activation is its own): an empty assignment closes the change, as a Kafka
                // rebalance closes with one, so the core's status leaves `STARTING`.
                state
                    .changes
                    .push_back(SubscriptionEvent::Revoked(vec![self.segment.clone()]));
                state.changes.push_back(SubscriptionEvent::Assigned(vec![]));
            }
            owed
        };
        tracing::info!(stream = %self.stream, subscription = %self.shared.name, "partition revoked");
        self.shared.wake();
        // An inactive consumer always holds a credit, so the broker can send once it is active.
        if owed {
            self.credit().await;
        }
        self.answer(correlation_id, OffsetSpecification::Next);
    }

    /// Answers a consumer update. The client's call waits for a response the broker never
    /// sends, so it runs on a task of its own, bounded.
    fn answer(&self, correlation_id: u32, from: OffsetSpecification) {
        let conn = Arc::clone(&self.conn);
        tokio::spawn(async move {
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                conn.client.consumer_update(correlation_id, from),
            )
            .await;
        });
    }
}

impl Subscription for RabbitSubscription {
    fn next(&mut self) -> BoxFuture<'_, Result<SubscriptionEvent, EventStoreError>> {
        Box::pin(async move {
            loop {
                let notified = self.shared.notify.notified();
                {
                    let mut state = lock(&self.shared.state);
                    if let Some(broken) = &state.broken {
                        return Err(broken.clone());
                    }
                    if let Some(change) = state.changes.pop_front() {
                        return Ok(change);
                    }
                    if !state.paused {
                        let n = state.parts.len();
                        for k in 0..n {
                            let i = (self.turn + k) % n;
                            let part = &mut state.parts[i];
                            if !part.active {
                                continue;
                            }
                            if let Some(d) = part.ready.pop_front() {
                                part.inflight.push_back(d.clone());
                                if part.owed && part.ready.len() < READY_LIMIT {
                                    part.owed = false;
                                    let _ = part.cmds.send(Cmd::Credit);
                                }
                                self.turn = i + 1;
                                return Ok(SubscriptionEvent::Delivery(d));
                            }
                        }
                    }
                }
                notified.await;
            }
        })
    }

    fn ack(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError> {
        let offset = parse(position)?;
        let mut state = lock(&self.shared.state);
        let Some(part) = state.part(segment).filter(|p| p.active) else {
            // Revoked meanwhile: the delivery comes again to whoever has the partition now.
            return Ok(());
        };
        let next = offset + 1;
        if part.acked.is_some_and(|done| done >= next) {
            return Ok(());
        }
        part.acked = Some(next);
        part.inflight.retain(|d| self::offset(d) > offset);
        let _ = part.cmds.send(Cmd::Store);
        Ok(())
    }

    fn redeliver(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError> {
        let from = parse(position)?;
        let mut state = lock(&self.shared.state);
        let Some(part) = state.part(segment).filter(|p| p.active) else {
            return Ok(());
        };
        let again: Vec<Delivery> = part
            .inflight
            .iter()
            .filter(|d| offset(d) >= from)
            .cloned()
            .collect();
        part.inflight.retain(|d| offset(d) < from);
        for d in again.into_iter().rev() {
            part.ready.push_front(d);
        }
        drop(state);
        self.shared.wake();
        Ok(())
    }

    fn pause(&mut self) {
        lock(&self.shared.state).paused = true;
    }

    fn resume(&mut self) {
        let mut state = lock(&self.shared.state);
        state.paused = false;
        for part in &mut state.parts {
            if part.owed && part.ready.len() < READY_LIMIT {
                part.owed = false;
                let _ = part.cmds.send(Cmd::Credit);
            }
        }
        drop(state);
        self.shared.wake();
    }

    fn segments(&self) -> Vec<SegmentStatus> {
        let state = lock(&self.shared.state);
        state
            .parts
            .iter()
            .filter(|p| p.active)
            .map(|p| {
                let position = p.ready.front().map(offset).unwrap_or(p.next);
                let committed = p.acked.or(p.stored);
                // A stream's offsets also number its offset-tracking entries, so the lag counts
                // the deliveries received and not acknowledged one by one, and only what is not
                // received yet by offsets.
                let unacked = (p.inflight.len() + p.ready.len()) as u64;
                SegmentStatus {
                    segment: p.segment.clone(),
                    position: Some(position.to_string()),
                    committed: committed.map(|c| c.to_string()),
                    head: p.head.map(|h| h.to_string()),
                    lag: p.head.map(|h| unacked + h.saturating_sub(p.next)),
                }
            })
            .collect()
    }

    fn take_error(&mut self) -> Option<String> {
        lock(&self.shared.state).error.take()
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let RabbitSubscription {
                shared,
                mut nodes,
                subs,
                tasks,
                refresher,
                cancel,
                ..
            } = *self;
            // Everything acknowledged is stored (and read back) before the consumers leave, so
            // the next active consumer of a partition resumes after it.
            let flushes: Vec<oneshot::Receiver<()>> = lock(&shared.state)
                .parts
                .iter()
                .filter_map(|p| {
                    let (tx, rx) = oneshot::channel();
                    p.cmds.send(Cmd::Flush(tx)).ok().map(|()| rx)
                })
                .collect();
            let _ = tokio::time::timeout(STORE_WAIT * 2, futures::future::join_all(flushes)).await;
            cancel.cancel();
            for task in tasks.into_iter().chain(refresher) {
                let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
            }
            futures::future::join_all(subs.iter().map(|(conn, sub)| async move {
                conn.unroute(*sub);
                if !conn.is_closed() {
                    let _ = tokio::time::timeout(
                        crate::conn::CONNECT_TIMEOUT,
                        conn.client.unsubscribe(*sub),
                    )
                    .await;
                }
            }))
            .await;
            nodes.close().await;
        })
    }
}

fn parse(position: &str) -> Result<u64, EventStoreError> {
    position
        .parse()
        .map_err(|_| EventStoreError::Invalid(format!("position {position} is not an offset")))
}

/// Every [`OFFSETS_REFRESH`] (sooner until every active partition's head is known): the head of
/// each active partition, read on connections of the refresher's own.
async fn refresh(mut nodes: Nodes, shared: Arc<Shared>, cancel: CancellationToken) {
    loop {
        let active: Vec<(String, String)> = lock(&shared.state)
            .parts
            .iter()
            .filter(|p| p.active)
            .map(|p| (p.segment.clone(), p.stream.clone()))
            .collect();
        let mut conns = Vec::with_capacity(active.len());
        for (segment, stream) in &active {
            match nodes.leader(stream).await {
                Ok(Some(conn)) => conns.push((segment.clone(), stream.clone(), conn)),
                Ok(None) => {}
                Err(e) => {
                    tracing::debug!(%stream, "the head was not read: {e}");
                    nodes.prune();
                }
            }
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        let heads = tokio::select! {
            _ = cancel.cancelled() => break,
            heads = futures::future::join_all(conns.iter().map(|(segment, stream, conn)| async move {
                (segment.clone(), read::bounds(conn, stream, deadline).await)
            })) => heads,
        };
        let mut all_known = !active.is_empty();
        {
            let mut state = lock(&shared.state);
            for (segment, bounds) in heads {
                match (bounds, state.part(&segment)) {
                    (Ok(bounds), Some(part)) if part.active => {
                        part.head = Some(part.head.unwrap_or(0).max(bounds.head));
                    }
                    (Err(e), _) => {
                        tracing::debug!(%segment, "the head was not read: {e}");
                        all_known = false;
                    }
                    _ => {}
                }
            }
            all_known &= state
                .parts
                .iter()
                .filter(|p| p.active)
                .all(|p| p.head.is_some());
        }
        nodes.prune();
        let wait = if all_known {
            OFFSETS_REFRESH
        } else {
            OFFSETS_FIRST
        };
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(wait) => {}
        }
    }
    nodes.close().await;
}
