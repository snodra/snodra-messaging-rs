//! A subscription on RabbitMQ Streams: one consumer named `<service>.<processor>` per partition
//! stream of the source, each a single active consumer (with the property `super-stream` for a
//! super stream, so the broker spreads the partitions over the consumers sharing the name).
//!
//! - **Assignment** is the broker's consumer updates: a partition whose consumer becomes active
//!   is `Assigned`, one that stops being active is `Revoked` followed by an empty `Assigned`
//!   (the change is over). A subscription active on no partition [`FIRST_SETTLE`] after it
//!   subscribed to all of them announces `Assigned([])` (a member, idle); an activation before
//!   that is the first `Assigned` instead, so an empty one never follows a real one as if
//!   everything were lost (an activation after it is an ordinary later `Assigned`). On activation
//!   the subscription
//!   reads the offset stored under its name and answers the broker with it (the first offset
//!   when none is stored); on deactivation it stores what was acknowledged first.
//! - **Acknowledgement** moves the position stored under the subscription's name (server-side
//!   offset tracking: the offset after the acknowledged delivery). Every stored offset is an entry
//!   in the stream itself, so it is stored every [`StorePolicy::messages`] acknowledgements or
//!   [`StorePolicy::interval`], whichever comes first, and always on deactivation and at
//!   [`close`], which waits until the broker reads it back. An acknowledgement of an offset not
//!   handed out since the partition last became active is ignored.
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

use std::collections::{BTreeSet, HashMap, VecDeque};
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
/// How long after subscribing to every partition a subscription active on none announces its
/// first assignment, empty (the broker activates a lone or first consumer at once).
pub const FIRST_SETTLE: Duration = Duration::from_secs(1);
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
    /// One past the last offset handed out since the partition became active (`None`: nothing
    /// handed out yet in this activation).
    handed: Option<u64>,
    cmds: mpsc::UnboundedSender<Cmd>,
}

impl Part {
    /// Acknowledges `offset` and every earlier one; `false` when it does not move the
    /// acknowledged position: already acknowledged, or not handed out in this activation (an
    /// acknowledgement from an earlier activation, whose deliveries may have been handled by
    /// another instance since).
    fn ack(&mut self, offset: u64) -> bool {
        let next = offset + 1;
        let handed_now = self.handed.is_some_and(|h| offset < h)
            && self.stored.is_none_or(|start| offset >= start);
        if !self.active || !handed_now || self.acked.is_some_and(|done| done >= next) {
            return false;
        }
        self.acked = Some(next);
        self.inflight.retain(|d| self::offset(d) > offset);
        true
    }

    /// Hands out again, ahead of the rest, the deliveries handed out at or after `from` and not
    /// acknowledged.
    fn redeliver(&mut self, from: u64) {
        let again: Vec<Delivery> = self
            .inflight
            .iter()
            .filter(|d| offset(d) >= from)
            .cloned()
            .collect();
        self.inflight.retain(|d| offset(d) < from);
        for d in again.into_iter().rev() {
            self.ready.push_front(d);
        }
    }

    /// The next delivery to hand out.
    fn hand_out(&mut self) -> Option<Delivery> {
        let d = self.ready.pop_front()?;
        self.handed = Some(self.handed.unwrap_or(0).max(offset(&d) + 1));
        self.inflight.push_back(d.clone());
        Some(d)
    }
}

#[derive(Default)]
struct State {
    changes: VecDeque<SubscriptionEvent>,
    /// In segment order.
    parts: Vec<Part>,
    paused: bool,
    broken: Option<EventStoreError>,
    error: Option<String>,
    /// An `Assigned` was queued: the subscription's first one is behind it.
    announced: bool,
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

/// The subscriptions of this process that are open now, per (source, name), by member id (the
/// `kraken-member` property): a reset must not move a subscription this very process reads,
/// whatever the management API says.
#[derive(Clone, Default)]
pub(crate) struct Live(Arc<Mutex<HashMap<(String, String), Members>>>);

/// The member ids of one subscription open in this process.
type Members = BTreeSet<String>;

impl Live {
    pub fn enter(&self, spec: &SubscriptionSpec, member: &str) -> LiveGuard {
        let key = (spec.source.clone(), spec.name.clone());
        lock(&self.0)
            .entry(key.clone())
            .or_default()
            .insert(member.to_string());
        LiveGuard {
            live: self.clone(),
            key,
            member: member.to_string(),
        }
    }

    /// The member ids of this process's open subscriptions of `spec`.
    pub fn members(&self, spec: &SubscriptionSpec) -> BTreeSet<String> {
        lock(&self.0)
            .get(&(spec.source.clone(), spec.name.clone()))
            .cloned()
            .unwrap_or_default()
    }
}

/// One open subscription in [`Live`], until dropped.
pub(crate) struct LiveGuard {
    live: Live,
    key: (String, String),
    member: String,
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        let mut all = lock(&self.live.0);
        if let Some(members) = all.get_mut(&self.key) {
            members.remove(&self.member);
            if members.is_empty() {
                all.remove(&self.key);
            }
        }
    }
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
    store: StorePolicy,
    /// Counts this subscription among the process's open ones until it is closed or dropped.
    live: Option<LiveGuard>,
}

impl RabbitSubscription {
    /// Joins `spec` as one more member (`member` tells it apart); must run inside a tokio
    /// runtime. `Failed` when the source does not exist.
    pub(crate) async fn join(
        broker: Broker,
        spec: &SubscriptionSpec,
        member: &str,
        store: StorePolicy,
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
            store,
            live: None,
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
        // Active on no partition once the broker had its moment: a member holding nothing.
        let first = Arc::clone(&shared);
        let settle = cancel.clone();
        subscription.tasks.push(tokio::spawn(async move {
            tokio::select! {
                _ = settle.cancelled() => {}
                _ = tokio::time::sleep(FIRST_SETTLE) => {
                    let mut state = lock(&first.state);
                    if !state.announced {
                        state.announced = true;
                        state.changes.push_back(SubscriptionEvent::Assigned(vec![]));
                        drop(state);
                        first.wake();
                    }
                }
            }
        }));
        let mut conns: Vec<Arc<Conn>> = Vec::new();
        for (conn, _) in &subscription.subs {
            if !conns.iter().any(|c| Arc::ptr_eq(c, conn)) {
                conns.push(Arc::clone(conn));
            }
        }
        subscription.refresher = Some(tokio::spawn(refresh(
            Nodes::new(broker),
            conns,
            Arc::clone(&shared),
            cancel,
        )));
        Ok(subscription)
    }

    pub(crate) fn hold(&mut self, guard: LiveGuard) {
        self.live = Some(guard);
    }

    /// One single active consumer of `stream`.
    async fn consume(
        &mut self,
        segment: &str,
        stream: &str,
        member: &str,
        super_stream: bool,
    ) -> Result<(), EventStoreError> {
        let conn = self.nodes.require_leader(stream).await?;
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
            handed: None,
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
                store: self.store,
                unstored: 0,
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
            .timed(
                conn.client
                    .subscribe(sub, stream, OffsetSpecification::Next, 1, properties),
            )
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
    /// When the acknowledged offset is stored: every so many acknowledgements, or this often.
    store: StorePolicy,
    /// Acknowledgements since the last store.
    unstored: u64,
}

/// When a subscription stores its acknowledged offset: every offset stored is an entry in the
/// stream itself, so not on every acknowledgement but every `messages` acknowledgements or
/// every `interval`, whichever comes first, and always on deactivation and close.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorePolicy {
    pub messages: u64,
    pub interval: Duration,
}

impl Default for StorePolicy {
    /// The Java stream client's automatic tracking: 10 000 messages, 5 s.
    fn default() -> Self {
        StorePolicy {
            messages: 10_000,
            interval: Duration::from_secs(5),
        }
    }
}

impl PartTask {
    async fn run(
        mut self,
        mut frames: mpsc::UnboundedReceiver<Frame>,
        mut cmds: mpsc::UnboundedReceiver<Cmd>,
        cancel: CancellationToken,
    ) {
        let mut every = tokio::time::interval(self.store.interval);
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                cmd = cmds.recv() => match cmd {
                    None => return,
                    Some(Cmd::Store) => {
                        self.unstored += 1;
                        if self.unstored >= self.store.messages {
                            self.store().await;
                        }
                    }
                    Some(Cmd::Credit) => self.credit().await,
                    Some(Cmd::Flush(done)) => {
                        self.flush().await;
                        let _ = done.send(());
                    }
                },
                _ = every.tick() => self.store().await,
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
            Ok(()) => {
                self.last_stored = Some(acked);
                self.unstored = 0;
            }
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
            Ok(()) => {
                self.last_stored = Some(acked);
                self.unstored = 0;
            }
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
        let read = self
            .conn
            .stored_offset(&self.shared.name, &self.stream)
            .await;
        let (stored, from) = match resume_from(read) {
            Ok(resume) => resume,
            Err(e) => {
                // Never from the first offset on a failed read (the whole partition again, its
                // markers maybe purged): the subscription ends and the core subscribes again.
                self.shared.broken(format!(
                    "{}: the stored offset was not read on activation: {e}",
                    self.stream
                ));
                return;
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
                part.handed = None;
                part.ready.clear();
                part.inflight.clear();
                part.head = part.head.max(stored);
            }
            state.announced = true;
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
        self.answer(correlation_id, from);
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
    /// sends, so it runs on a task of its own, bounded by 5 s: the tasks alive at once are at
    /// most the consumer updates of the last 5 s, and the broker sends one per partition per
    /// activation or deactivation of this consumer (a rebalance), never per message. The client
    /// also keeps one entry of its request map per call until the connection closes (an
    /// upstream defect; the same number, per connection lifetime).
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
                            if let Some(d) = part.hand_out() {
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
        // A segment revoked meanwhile, or an offset not handed out in this activation: ignored
        // (the delivery comes again to whoever reads the partition).
        if let Some(part) = state.part(segment)
            && part.ack(offset)
        {
            let _ = part.cmds.send(Cmd::Store);
        }
        Ok(())
    }

    fn redeliver(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError> {
        let from = parse(position)?;
        let mut state = lock(&self.shared.state);
        if let Some(part) = state.part(segment).filter(|p| p.active) {
            part.redeliver(from);
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

/// Where an activated consumer resumes, from the read of its stored offset: after it, or at the
/// first offset when the broker answers that none is stored; a failed read is an error (never
/// the first offset).
fn resume_from(
    read: Result<Option<u64>, EventStoreError>,
) -> Result<(Option<u64>, OffsetSpecification), EventStoreError> {
    let stored = read?;
    Ok((
        stored,
        stored.map_or(OffsetSpecification::First, OffsetSpecification::Offset),
    ))
}

fn parse(position: &str) -> Result<u64, EventStoreError> {
    position
        .parse()
        .map_err(|_| EventStoreError::Invalid(format!("position {position} is not an offset")))
}

/// Every [`OFFSETS_REFRESH`] (sooner until every active partition's head is known): the head of
/// each active partition, read on connections of the refresher's own.
async fn refresh(
    mut nodes: Nodes,
    consuming: Vec<Arc<Conn>>,
    shared: Arc<Shared>,
    cancel: CancellationToken,
) {
    loop {
        // The consumers' connections still answer: the client reads every frame on a task of
        // its own, and one it cannot decode (a chunk with sub-entries or compression, which
        // kraken streams never carry) stops that task without a word.
        for conn in &consuming {
            if !conn.is_closed() && !conn.answers().await {
                shared.broken(format!(
                    "the connection to {}:{} stopped answering (a chunk the client cannot read? \
                     kraken streams carry no sub-entries and no compression)",
                    conn.node.0, conn.node.1
                ));
                nodes.close().await;
                return;
            }
        }
        // (segment, stream, known to hold events: then its last chunk alone gives the head).
        let active: Vec<(String, String, bool)> = lock(&shared.state)
            .parts
            .iter()
            .filter(|p| p.active)
            .map(|p| {
                let known = p.next > 0 || p.head.is_some_and(|h| h > 0);
                (p.segment.clone(), p.stream.clone(), known)
            })
            .collect();
        let mut conns = Vec::with_capacity(active.len());
        for (segment, stream, known) in &active {
            match nodes.leader(stream).await {
                Ok(Ok(conn)) => conns.push((segment.clone(), stream.clone(), *known, conn)),
                Ok(Err(_)) => {}
                Err(e) => {
                    tracing::debug!(%stream, "the head was not read: {e}");
                    nodes.prune();
                }
            }
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        let heads = tokio::select! {
            _ = cancel.cancelled() => break,
            heads = futures::future::join_all(conns.iter().map(|(segment, stream, known, conn)| async move {
                let head = if *known {
                    // Its last chunk only, never its first every few seconds.
                    match read::head_of_known(conn, stream, deadline).await {
                        Ok(Some(head)) => Ok(read::Bounds { head, ..Default::default() }),
                        Ok(None) => Err(failed(format!("the last chunk of {stream} did not come"))),
                        Err(e) => Err(e),
                    }
                } else {
                    read::head(conn, stream, deadline).await
                };
                (segment.clone(), head)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn part(stored: Option<u64>, offsets: &[u64]) -> Part {
        let (cmds, _) = mpsc::unbounded_channel();
        Part {
            segment: "0".into(),
            stream: "s-0".into(),
            active: true,
            next: offsets.last().map_or(0, |o| o + 1),
            ready: offsets
                .iter()
                .map(|o| Delivery {
                    segment: "0".into(),
                    position: o.to_string(),
                    key: None,
                    headers: HashMap::new(),
                    payload: vec![],
                    timestamp_ms: 1,
                })
                .collect(),
            inflight: VecDeque::new(),
            acked: None,
            stored,
            head: None,
            owed: false,
            handed: None,
            cmds,
        }
    }

    /// An acknowledgement moves the position only for an offset handed out in this activation.
    #[test]
    fn an_ack_of_an_offset_not_handed_out_now_is_ignored() {
        let mut p = part(Some(10), &[10, 11, 12]);
        assert!(!p.ack(10), "nothing handed out yet");
        p.hand_out();
        p.hand_out();
        assert!(!p.ack(12), "12 is not handed out yet");
        assert!(!p.ack(9), "before this activation started");
        assert!(p.ack(11));
        assert_eq!(p.acked, Some(12));
        assert!(!p.ack(10), "already acknowledged");
        assert!(p.inflight.is_empty());
        // A new activation forgets what the earlier one handed out.
        p.handed = None;
        assert!(!p.ack(12));
    }

    #[test]
    fn redeliver_hands_out_again_from_the_offset_in_order() {
        let mut p = part(None, &[0, 1, 2, 3]);
        for _ in 0..3 {
            p.hand_out();
        }
        p.redeliver(1);
        let order: Vec<u64> = std::iter::from_fn(|| p.hand_out().map(|d| offset(&d))).collect();
        assert_eq!(order, [1, 2, 3]);
    }

    /// The broker's "no offset stored" means the first offset; a failed read is an error.
    #[test]
    fn a_failed_read_of_the_stored_offset_never_means_the_first() {
        assert!(matches!(
            resume_from(Ok(None)),
            Ok((None, OffsetSpecification::First))
        ));
        assert!(matches!(
            resume_from(Ok(Some(7))),
            Ok((Some(7), OffsetSpecification::Offset(7)))
        ));
        assert!(resume_from(Err(failed("timeout"))).is_err());
    }

    /// This process's open subscriptions of a name count as members until closed.
    #[test]
    fn open_subscriptions_are_members_until_dropped() {
        let live = Live::default();
        let spec = SubscriptionSpec {
            name: "svc.p".into(),
            source: "s".into(),
        };
        let a = live.enter(&spec, "m-1");
        let b = live.enter(&spec, "m-2");
        assert_eq!(live.members(&spec).len(), 2);
        drop(a);
        assert_eq!(live.members(&spec), BTreeSet::from(["m-2".to_string()]));
        drop(b);
        assert!(live.members(&spec).is_empty());
    }
}
