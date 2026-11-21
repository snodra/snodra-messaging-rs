//! A subscription on Kafka: one consumer of the processor's group (the subscription's name),
//! which Kafka assigns partitions to.
//!
//! - **Assignment** comes from librdkafka's rebalance callbacks, queued in order with the records
//!   and handed out by [`next`](Subscription::next) as `Revoked` then `Assigned`. A record is
//!   never handed out before the `Assigned` naming its partition: the callbacks run inside the
//!   poll that fetches it.
//! - **Acknowledgement** commits the offset after the record (asynchronously, as the consumer
//!   always did), and [`close`](Subscription::close) commits them all (synchronously, on a
//!   blocking thread) before leaving the group.
//! - **Redelivery** seeks the partition back; records fetched before the seek that still arrive
//!   are dropped (and the partition sought back again) until the sought record comes.
//! - **Pause** pauses the assigned partitions and every partition assigned later; a record
//!   fetched before the pause is not handed out, its partition is sought back to it.
//! - **Status** is read by a task of the subscription itself every [`OFFSETS_REFRESH`]
//!   (committed offsets and high watermarks, on a blocking thread); [`segments`] only reads what
//!   it last found, plus the acknowledgements made since.
//! - **Errors** librdkafka recovers from (a client error, a failed rebalance, a receive error of a
//!   partition) are logged and kept for [`take_error`]; only a fatal consumer error ends the
//!   subscription: `next` fails, at once and on every later call (the caller subscribes again).
//!
//! [`segments`]: Subscription::segments
//! [`take_error`]: Subscription::take_error

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures::future::BoxFuture;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Headers, OwnedMessage};
use rdkafka::{ClientContext, Message, Offset, Timestamp, TopicPartitionList};
use snodra_messaging::event_store::{
    Delivery, EventStoreError, SegmentStatus, Subscription, SubscriptionEvent,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::external::reject_fixed_keys;
use crate::{consumer_config, failed};

/// How long the status refresher waits between two reads of the committed offsets and the high
/// watermarks (at most every 5 s).
pub const OFFSETS_REFRESH: Duration = Duration::from_secs(5);
/// Until the first read of an assignment's offsets: how long between two tries.
const OFFSETS_FIRST: Duration = Duration::from_millis(500);
/// How long one committed-offsets or watermark request may take.
const OFFSETS_TIMEOUT: Duration = Duration::from_secs(2);
/// Between a record that arrived while paused (fetched before the pause) and the next poll.
const PAUSED_POLL: Duration = Duration::from_millis(100);
/// Between a receive error and the next poll.
const ERROR_BACKOFF: Duration = Duration::from_millis(500);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// What the rebalance callbacks and the subscription share.
#[derive(Default)]
struct Shared {
    /// Assignment changes not handed out yet, in order.
    changes: Mutex<VecDeque<SubscriptionEvent>>,
    /// Woken when `changes` gets one (the callbacks run inside a poll that may deliver nothing).
    notify: Notify,
    topic: String,
    /// The store's own topics are auto-created by their first record; a partner's never are.
    own_topic: bool,
    /// The partitions assigned now.
    assigned: Mutex<BTreeSet<i32>>,
    /// Per partition: the offset the subscription delivers next.
    positions: Mutex<HashMap<i32, i64>>,
    /// Per partition: the offset after the last acknowledged record (what is committed).
    acked: Mutex<HashMap<i32, i64>>,
    /// Per partition: what the status refresher last read.
    broker: Mutex<HashMap<i32, BrokerOffsets>>,
    paused: AtomicBool,
    /// Counts the rebalances: a seek-back is forgotten by the next one.
    generation: AtomicU64,
    /// The latest error recovered from, until [`Subscription::take_error`] takes it.
    error: Mutex<Option<String>>,
}

#[derive(Clone, Copy, Default)]
struct BrokerOffsets {
    committed: Option<i64>,
    high: Option<i64>,
}

impl Shared {
    fn new(topic: &str, own_topic: bool) -> Shared {
        Shared {
            topic: topic.to_string(),
            own_topic,
            ..Shared::default()
        }
    }

    fn push(&self, change: SubscriptionEvent) {
        lock(&self.changes).push_back(change);
        self.notify.notify_one();
    }

    fn pop(&self) -> Option<SubscriptionEvent> {
        lock(&self.changes).pop_front()
    }

    fn has_changes(&self) -> bool {
        !lock(&self.changes).is_empty()
    }

    /// Keeps `error` for the status (the latest wins).
    fn error(&self, error: String) {
        *lock(&self.error) = Some(error);
    }

    fn assigned(&self) -> Vec<i32> {
        lock(&self.assigned).iter().copied().collect()
    }

    fn partitions(&self, ps: &[i32]) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for &p in ps {
            tpl.add_partition(&self.topic, p);
        }
        tpl
    }

    /// The acknowledged offsets as a list to commit.
    fn to_commit(&self) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for (&p, &next) in lock(&self.acked).iter() {
            let _ = tpl.add_partition_offset(&self.topic, p, Offset::Offset(next));
        }
        tpl
    }
}

/// Commits everything acknowledged and waits for the broker (blocking).
fn commit_acknowledged<C: ConsumerContext>(consumer: &impl Consumer<C>, shared: &Shared) {
    let tpl = shared.to_commit();
    if tpl.count() == 0 {
        return;
    }
    if let Err(e) = consumer.commit(&tpl, CommitMode::Sync) {
        tracing::warn!(topic = %shared.topic, "the acknowledged offsets were not committed: {e}");
    }
}

/// The consumer's callbacks: they run on the task polling `next`, so they never block (the
/// acknowledgements already sent their asynchronous commits).
struct SubscriptionContext {
    shared: Arc<Shared>,
}

impl ClientContext for SubscriptionContext {
    fn error(&self, error: KafkaError, reason: &str) {
        // An idle connection the broker closed is reconnected by librdkafka, and one of the
        // store's own topics that does not exist yet is auto-created by its first record:
        // neither is a failure. A partner's topic that is missing is worth a warning.
        let missing_topic = matches!(
            error,
            KafkaError::Global(
                RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic
            )
        );
        if matches!(
            error,
            KafkaError::Global(RDKafkaErrorCode::BrokerTransportFailure)
        ) || (missing_topic && self.shared.own_topic)
        {
            tracing::debug!(topic = %self.shared.topic, "kafka: {error}: {reason}");
            return;
        }
        tracing::warn!(topic = %self.shared.topic, "kafka error: {error}: {reason}");
        self.shared.error(format!("kafka: {error}: {reason}"));
    }
}

impl ConsumerContext for SubscriptionContext {
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let shared = &self.shared;
        let partitions = |tpl: &TopicPartitionList| -> Vec<i32> {
            tpl.elements().iter().map(|e| e.partition()).collect()
        };
        match rebalance {
            Rebalance::Assign(tpl) => {
                let assigned = partitions(tpl);
                tracing::info!(topic = %shared.topic, partitions = ?assigned, "partitions assigned");
                shared.generation.fetch_add(1, Ordering::SeqCst);
                if shared.paused.load(Ordering::SeqCst)
                    && !assigned.is_empty()
                    && let Err(e) = consumer.pause(tpl)
                {
                    tracing::warn!(topic = %shared.topic, "cannot pause the assigned partitions: {e}");
                }
                lock(&shared.assigned).extend(assigned.iter().copied());
                // Every assignment, an empty one included: it closes the rebalance (the
                // event-store contract: a `Revoked` is always followed by an `Assigned`).
                shared.push(SubscriptionEvent::Assigned(
                    assigned.iter().map(i32::to_string).collect(),
                ));
            }
            Rebalance::Revoke(tpl) => {
                let revoked = partitions(tpl);
                tracing::info!(topic = %shared.topic, partitions = ?revoked, "partitions revoked");
                shared.generation.fetch_add(1, Ordering::SeqCst);
                for p in &revoked {
                    lock(&shared.assigned).remove(p);
                    lock(&shared.positions).remove(p);
                    lock(&shared.acked).remove(p);
                    lock(&shared.broker).remove(p);
                }
                if !revoked.is_empty() {
                    shared.push(SubscriptionEvent::Revoked(
                        revoked.iter().map(i32::to_string).collect(),
                    ));
                }
            }
            Rebalance::Error(e) => {
                tracing::warn!(topic = %shared.topic, "rebalance error: {e}");
                shared.error(format!("rebalance: {e}"));
            }
        }
    }
}

type Consumer_ = StreamConsumer<SubscriptionContext>;

/// One instance's subscription of a group on a topic. Use [`close`](Subscription::close): it
/// flushes the acknowledgements and leaves the group off the runtime's threads; dropping the
/// subscription instead flushes nothing (an unflushed record comes again) and leaves the group
/// from whichever thread drops the consumer last, which blocks it for a moment.
pub struct KafkaSubscription {
    consumer: Arc<Consumer_>,
    shared: Arc<Shared>,
    /// A record received together with an assignment change: handed out after the change.
    stash: Option<OwnedMessage>,
    /// Per partition: the generation and offset it was sought back to, until that record is
    /// delivered again.
    rewound: HashMap<i32, (u64, i64)>,
    cancel: CancellationToken,
    refresher: Option<JoinHandle<()>>,
    _stop: DropGuard,
    /// The fatal error that ended the subscription: every later `next` returns it.
    broken: Option<EventStoreError>,
}

/// What became of a received record.
enum Received {
    Deliver(Delivery),
    /// Not handed out (an overtaking record, a partition no longer ours).
    Dropped,
    /// Not handed out because the subscription is paused.
    Paused,
}

impl KafkaSubscription {
    /// Joins `group` reading `topic`; must run inside a tokio runtime (it starts the status
    /// refresher). `own_topic`: a topic of the store itself, created by its first record when
    /// missing; a partner's topic (`false`) is never created.
    pub(crate) fn join(
        bootstrap_servers: &str,
        group: &str,
        topic: &str,
        properties: &HashMap<String, String>,
        own_topic: bool,
    ) -> Result<KafkaSubscription, EventStoreError> {
        reject_fixed_keys("the Kafka properties", properties).map_err(EventStoreError::Invalid)?;
        if topic.trim().is_empty() {
            return Err(EventStoreError::Invalid("the source is empty".into()));
        }
        let shared = Arc::new(Shared::new(topic, own_topic));
        let consumer: Consumer_ =
            subscription_config(bootstrap_servers, group, properties, own_topic)
                .create_with_context(SubscriptionContext {
                    shared: Arc::clone(&shared),
                })
                .map_err(failed)?;
        consumer.subscribe(&[topic]).map_err(failed)?;
        let consumer = Arc::new(consumer);
        let cancel = CancellationToken::new();
        let refresher = tokio::spawn(refresh(
            Arc::clone(&consumer),
            Arc::clone(&shared),
            cancel.clone(),
        ));
        Ok(KafkaSubscription {
            consumer,
            shared,
            stash: None,
            rewound: HashMap::new(),
            _stop: cancel.clone().drop_guard(),
            cancel,
            refresher: Some(refresher),
            broken: None,
        })
    }

    fn generation(&self) -> u64 {
        self.shared.generation.load(Ordering::SeqCst)
    }

    /// Seeks `partition` back to `offset`: the next polls deliver it again (the partition's later
    /// records wait behind it).
    fn rewind(&mut self, partition: i32, offset: i64) {
        self.rewound.insert(partition, (self.generation(), offset));
        lock(&self.shared.positions).insert(partition, offset);
        // Asynchronous: librdkafka drops the partition's records fetched before the seek at once.
        if let Err(e) = self.consumer.seek(
            &self.shared.topic,
            partition,
            Offset::Offset(offset),
            Duration::ZERO,
        ) {
            tracing::warn!(topic = %self.shared.topic, partition, offset, "cannot seek back: {e}");
        }
    }

    /// True for a record that comes after the one its partition was sought back to (a seek that
    /// did not take effect): it is dropped, and the partition sought back again. A rebalance
    /// since forgets the rewind (the group's committed offset decides then).
    fn overtakes(&mut self, msg: &OwnedMessage) -> bool {
        match self.rewound.get(&msg.partition()).copied() {
            Some((generation, offset))
                if generation == self.generation() && msg.offset() > offset =>
            {
                self.rewind(msg.partition(), offset);
                true
            }
            Some(_) => {
                self.rewound.remove(&msg.partition());
                false
            }
            None => false,
        }
    }

    fn receive(&mut self, msg: OwnedMessage) -> Received {
        let partition = msg.partition();
        if !lock(&self.shared.assigned).contains(&partition) || self.overtakes(&msg) {
            return Received::Dropped;
        }
        // Fetched before the pause: not handed out, and its partition sought back to it, so the
        // resume starts there.
        if self.shared.paused.load(Ordering::SeqCst) {
            self.rewind(partition, msg.offset());
            return Received::Paused;
        }
        lock(&self.shared.positions).insert(partition, msg.offset() + 1);
        Received::Deliver(delivery(&msg))
    }
}

/// The consumer settings of a subscription: the store's own topics may be created by their first
/// record, a partner's are never created (whatever its `properties` say).
fn subscription_config(
    bootstrap_servers: &str,
    group: &str,
    properties: &HashMap<String, String>,
    own_topic: bool,
) -> ClientConfig {
    let mut config = consumer_config(bootstrap_servers, group, properties);
    config.set(
        "allow.auto.create.topics",
        if own_topic { "true" } else { "false" },
    );
    config
}

fn delivery(msg: &OwnedMessage) -> Delivery {
    Delivery {
        segment: msg.partition().to_string(),
        position: msg.offset().to_string(),
        key: msg.key().map(|k| String::from_utf8_lossy(k).into_owned()),
        headers: msg
            .headers()
            .map(|h| {
                h.iter()
                    .map(|x| {
                        (
                            x.key.to_string(),
                            String::from_utf8_lossy(x.value.unwrap_or(&[])).into_owned(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default(),
        payload: msg.payload().map(<[u8]>::to_vec).unwrap_or_default(),
        timestamp_ms: match msg.timestamp() {
            Timestamp::CreateTime(ms) | Timestamp::LogAppendTime(ms) => ms,
            Timestamp::NotAvailable => 0,
        },
    }
}

/// `segment` and `position` as a partition and an offset.
fn parse(segment: &str, position: &str) -> Result<(i32, i64), EventStoreError> {
    let partition = segment
        .parse()
        .map_err(|_| EventStoreError::Invalid(format!("segment {segment} is not a partition")))?;
    let offset = position
        .parse()
        .map_err(|_| EventStoreError::Invalid(format!("position {position} is not an offset")))?;
    Ok((partition, offset))
}

impl Subscription for KafkaSubscription {
    fn next(&mut self) -> BoxFuture<'_, Result<SubscriptionEvent, EventStoreError>> {
        Box::pin(async move {
            loop {
                if let Some(broken) = &self.broken {
                    return Err(broken.clone());
                }
                if let Some(change) = self.shared.pop() {
                    return Ok(change);
                }
                let msg = match self.stash.take() {
                    Some(msg) => msg,
                    None => {
                        let received = tokio::select! {
                            _ = self.shared.notify.notified() => continue,
                            received = self.consumer.recv() => received,
                        };
                        match received {
                            Ok(msg) => {
                                let msg = msg.detach();
                                // The callbacks of the poll that fetched it come first.
                                if self.shared.has_changes() {
                                    self.stash = Some(msg);
                                    continue;
                                }
                                msg
                            }
                            // The topic does not exist yet (auto-created by its first record): the
                            // subscription has no assignment yet, it is not failing.
                            Err(KafkaError::MessageConsumption(
                                RDKafkaErrorCode::UnknownTopicOrPartition
                                | RDKafkaErrorCode::UnknownTopic,
                            )) => {
                                if self.shared.own_topic {
                                    tracing::debug!(topic = %self.shared.topic, "topic not available yet");
                                } else {
                                    tracing::warn!(topic = %self.shared.topic, "kafka receive error: the partner topic does not exist");
                                }
                                tokio::time::sleep(ERROR_BACKOFF).await;
                                continue;
                            }
                            // A partition's error: librdkafka goes on fetching (the consumer
                            // always did too); the status shows it.
                            Err(e @ KafkaError::MessageConsumption(_)) => {
                                tracing::warn!(topic = %self.shared.topic, "kafka receive error: {e}");
                                self.shared.error(format!("kafka receive: {e}"));
                                tokio::time::sleep(ERROR_BACKOFF).await;
                                continue;
                            }
                            // The consumer cannot go on: the caller subscribes again (after its own
                            // backoff). Kept before anything else, with no wait in between, so a
                            // cancelled `next` cannot lose it.
                            Err(e) => {
                                tracing::warn!(topic = %self.shared.topic, "kafka receive error, the subscription ends: {e}");
                                self.broken = Some(failed(e));
                                continue;
                            }
                        }
                    }
                };
                match self.receive(msg) {
                    Received::Deliver(d) => return Ok(SubscriptionEvent::Delivery(d)),
                    Received::Dropped => {}
                    Received::Paused => {
                        tokio::select! {
                            _ = tokio::time::sleep(PAUSED_POLL) => {}
                            _ = self.shared.notify.notified() => {}
                        }
                    }
                }
            }
        })
    }

    fn ack(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError> {
        let (partition, offset) = parse(segment, position)?;
        if !lock(&self.shared.assigned).contains(&partition) {
            // Revoked meanwhile: the record comes again to whoever has the partition now.
            return Ok(());
        }
        let next = offset + 1;
        {
            let mut acked = lock(&self.shared.acked);
            if acked.get(&partition).is_some_and(|&done| done >= next) {
                return Ok(());
            }
            acked.insert(partition, next);
        }
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.shared.topic, partition, Offset::Offset(next))
            .map_err(failed)?;
        self.consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(failed)
    }

    fn redeliver(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError> {
        let (partition, offset) = parse(segment, position)?;
        if self
            .stash
            .as_ref()
            .is_some_and(|m| m.partition() == partition)
        {
            self.stash = None;
        }
        self.rewind(partition, offset);
        Ok(())
    }

    fn pause(&mut self) {
        self.shared.paused.store(true, Ordering::SeqCst);
        let assigned = self.shared.assigned();
        if !assigned.is_empty()
            && let Err(e) = self.consumer.pause(&self.shared.partitions(&assigned))
        {
            tracing::warn!(topic = %self.shared.topic, "cannot pause the assigned partitions: {e}");
        }
    }

    fn resume(&mut self) {
        self.shared.paused.store(false, Ordering::SeqCst);
        let assigned = self.shared.assigned();
        if !assigned.is_empty()
            && let Err(e) = self.consumer.resume(&self.shared.partitions(&assigned))
        {
            tracing::warn!(topic = %self.shared.topic, "cannot resume the assigned partitions: {e}");
        }
    }

    fn segments(&self) -> Vec<SegmentStatus> {
        let positions = lock(&self.shared.positions).clone();
        let acked = lock(&self.shared.acked).clone();
        let broker = lock(&self.shared.broker).clone();
        let text = |n: i64| n.to_string();
        // Partitions by number: their names in segment order.
        self.shared
            .assigned()
            .into_iter()
            .map(|p| (p.to_string(), p))
            .map(|(segment, p)| {
                let read = broker.get(&p).copied().unwrap_or_default();
                let committed = [read.committed, acked.get(&p).copied()]
                    .into_iter()
                    .flatten()
                    .filter(|&o| o >= 0)
                    .max();
                let position = positions.get(&p).copied().or(committed);
                let lag = read
                    .high
                    .zip(committed.or(position))
                    .map(|(high, base)| (high - base).max(0) as u64);
                SegmentStatus {
                    segment,
                    position: position.map(text),
                    committed: committed.map(text),
                    head: read.high.map(text),
                    lag,
                }
            })
            .collect()
    }

    fn take_error(&mut self) -> Option<String> {
        lock(&self.shared.error).take()
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let KafkaSubscription {
                consumer,
                shared,
                cancel,
                refresher,
                ..
            } = *self;
            cancel.cancel();
            if let Some(refresher) = refresher {
                let _ = refresher.await;
            }
            // The commit and the departure from the group block: off the runtime's threads.
            let _ = tokio::task::spawn_blocking(move || {
                commit_acknowledged(&*consumer, &shared);
                drop(consumer);
            })
            .await;
        })
    }
}

/// Every [`OFFSETS_REFRESH`] (sooner until an assignment's first read): the group's committed
/// offsets and the high watermarks of the assigned partitions, on a blocking thread (librdkafka's
/// calls block; the subscription never waits for them).
async fn refresh(consumer: Arc<Consumer_>, shared: Arc<Shared>, cancel: CancellationToken) {
    loop {
        let partitions = shared.assigned();
        let mut wait = OFFSETS_FIRST;
        if !partitions.is_empty() {
            let read = {
                let (consumer, shared, partitions) = (
                    Arc::clone(&consumer),
                    Arc::clone(&shared),
                    partitions.clone(),
                );
                {
                    let cancel = cancel.clone();
                    tokio::task::spawn_blocking(move || {
                        read_offsets(&consumer, &shared, &partitions, &cancel)
                    })
                }
            };
            let _ = read.await;
            let known = lock(&shared.broker);
            if partitions
                .iter()
                .all(|p| known.get(p).is_some_and(|o| o.high.is_some()))
            {
                wait = OFFSETS_REFRESH;
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

/// Stops between two broker calls once `cancel` is cancelled (a close waits for one call at most).
fn read_offsets(
    consumer: &Consumer_,
    shared: &Shared,
    partitions: &[i32],
    cancel: &CancellationToken,
) {
    if cancel.is_cancelled() {
        return;
    }
    let committed = match consumer.committed_offsets(shared.partitions(partitions), OFFSETS_TIMEOUT)
    {
        Ok(list) => Some(list),
        Err(e) => {
            tracing::debug!(topic = %shared.topic, "committed offsets not read: {e}");
            None
        }
    };
    for &p in partitions {
        if cancel.is_cancelled() {
            return;
        }
        let committed = committed.as_ref().and_then(|list| {
            match list.find_partition(&shared.topic, p)?.offset() {
                Offset::Offset(o) if o >= 0 => Some(o),
                _ => None,
            }
        });
        let high = match consumer.fetch_watermarks(&shared.topic, p, OFFSETS_TIMEOUT) {
            Ok((_, high)) => Some(high),
            Err(e) => {
                tracing::debug!(topic = %shared.topic, partition = p, "watermarks not read: {e}");
                None
            }
        };
        // A partition revoked meanwhile is not recorded again.
        if lock(&shared.assigned).contains(&p) {
            let mut broker = lock(&shared.broker);
            let known = broker.entry(p).or_default();
            if high.is_some() {
                known.high = high;
            }
            if committed.is_some() {
                known.committed = committed;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use snodra_messaging::event_store::segment_order;

    use super::*;

    /// A broken subscription stays broken: a `next` dropped right after the error was kept (the
    /// core races `next` with its timers) gives the error to the next call, and every call after.
    #[tokio::test]
    async fn a_fatal_error_is_returned_by_every_next_even_after_a_dropped_one() {
        let mut sub =
            KafkaSubscription::join("127.0.0.1:1", "g", "t", &HashMap::new(), true).unwrap();
        sub.broken = Some(EventStoreError::Failed("kafka: fatal".into()));
        for _ in 0..2 {
            let next = tokio::time::timeout(Duration::from_secs(5), sub.next()).await;
            assert_eq!(
                next.expect("at once"),
                Err(EventStoreError::Failed("kafka: fatal".into()))
            );
        }
        Box::new(sub).close().await;
    }

    #[test]
    fn partition_names_by_number_are_in_segment_order() {
        let mut names: Vec<String> = [0, 2, 10, 11, 1].iter().map(i32::to_string).collect();
        let by_number: Vec<String> = BTreeSet::from([0, 2, 10, 11, 1])
            .iter()
            .map(i32::to_string)
            .collect();
        names.sort_by(|a, b| segment_order(a, b));
        assert_eq!(names, by_number);
    }

    #[test]
    fn the_stores_own_topics_may_be_created_a_partners_never() {
        let own = subscription_config("h:1", "g", &HashMap::new(), true);
        assert_eq!(own.get("allow.auto.create.topics"), Some("true"));
        // Even a property saying otherwise does not create a partner's topic.
        let props = HashMap::from([("allow.auto.create.topics".to_string(), "true".to_string())]);
        let partner = subscription_config("h:1", "g", &props, false);
        assert_eq!(partner.get("allow.auto.create.topics"), Some("false"));
        assert_eq!(partner.get("group.id"), Some("g"));
        assert_eq!(partner.get("enable.auto.commit"), Some("false"));
    }
}
