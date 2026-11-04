//! Consumes `kraken.events` for one event processor's group (ADR 0022): one record at a time;
//! the dedupe marker is inserted first, then the handlers run, all in one Mongo transaction; the
//! offset is committed after. A record whose handlers fail on every attempt, a malformed one and,
//! in mode `collection`, one whose key has a parked letter go to the processor's dead letters
//! before the offset is committed. When the dead letters cannot take it (MongoDB or the DLT
//! unreachable), the record is not committed: its partition is sought back to it and the consumer
//! polls again, so the record comes back after a second while the poll loop keeps the group
//! membership (and the other partitions flow).
//!
//! Processor control (ADR 0023) reaches it through its [`ConsumerControl`]: paused, it pauses
//! its assigned partitions (and every partition assigned later) and keeps polling, so it stays in
//! the group; a record of a replay range is handled without the handlers that disallow replay
//! and without the sagas.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mongodb::Database;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::dispatch::{LocalEventDispatcher, MarkerPosition, RecordDispatch};
use super::headers;
use crate::codec;
use crate::config::DeadLetterMode;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::hub::HandlerRegistry;
use crate::processors::ProcessorMonitor;
use crate::processors::control_store::ReplayRange;
use crate::processors::dead_letters::{
    DeadLetterDocument, DeadLetterError, DeadLetterReason, DeadLetterRecord,
    MAX_ERROR_MESSAGE_CHARS, cut_chars,
};
use crate::processors::store::{DeadLetterStore, Parked};

/// What to do with a record, decided before any database work.
#[derive(Debug, PartialEq, Eq)]
pub enum RecordDisposition {
    /// Commit the offset and move on; the reason is logged (never the payload).
    Skip(&'static str),
    Handle {
        type_name: String,
        id: String,
    },
}

/// Mirrors `EventConsumer.kt`: no `type` header (or a blank one), a type nobody handles, or a
/// non-object payload are skipped; a missing or blank `id` header falls back to
/// `<topic>-<partition>-<offset>`.
pub fn classify(
    hdrs: &HashMap<String, String>,
    payload: &[u8],
    topic: &str,
    partition: i32,
    offset: i64,
    registry: &HandlerRegistry,
) -> RecordDisposition {
    classify_with(hdrs, payload, topic, partition, offset, |t| {
        registry.has_event_handler(t)
    })
}

/// [`classify`] with the interest in a type given as a predicate: the dispatcher's, which is an
/// ordinary event handler or a saga type (a record only a saga wants is handled, not skipped).
pub fn classify_with(
    hdrs: &HashMap<String, String>,
    payload: &[u8],
    topic: &str,
    partition: i32,
    offset: i64,
    interested: impl Fn(&str) -> bool,
) -> RecordDisposition {
    let Some(type_name) = hdrs.get(headers::TYPE).filter(|t| !t.trim().is_empty()) else {
        return RecordDisposition::Skip("record without type header");
    };
    if !interested(type_name) {
        return RecordDisposition::Skip(NO_HANDLER);
    }
    if !codec::is_json_object(payload) {
        return RecordDisposition::Skip("payload is not a JSON object");
    }
    let id = hdrs
        .get(headers::ID)
        .filter(|i| !i.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| format!("{topic}-{partition}-{offset}"));
    RecordDisposition::Handle {
        type_name: type_name.clone(),
        id,
    }
}

/// How long the offset refresher waits between two reads of the committed offsets and the high
/// watermarks (ADR 0022: at most every 5 s).
pub const OFFSETS_REFRESH: Duration = Duration::from_secs(5);
/// How long one committed-offsets or watermark request may take.
const OFFSETS_TIMEOUT: Duration = Duration::from_secs(2);
/// How often the processor's dead letters are counted (ADR 0022: every 30 s).
pub const DEAD_LETTERS_SAMPLE: Duration = Duration::from_secs(30);
/// Between a record the dead letters could not take (a park, a DLT produce or a sequencing read
/// failed) and its redelivery.
const PARK_RETRY: Duration = Duration::from_secs(1);

/// Between a record that arrived while the processor is paused (fetched before the pause) and the
/// next poll.
const PAUSED_POLL: Duration = Duration::from_millis(100);

/// What a processor's consumer is (ADR 0022): its name, group, dead-letter mode and the database
/// its letters go to.
#[derive(Clone)]
pub struct ProcessorSettings {
    /// `default` or the name given to `event_in`.
    pub name: String,
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topic: String,
    /// Handler invocations per record in all (`KRAKEN_CONSUMER_MAX_ATTEMPTS`), at least 1.
    pub max_attempts: u32,
    /// Between two invocations.
    pub backoff: Duration,
    pub dead_letter_mode: DeadLetterMode,
    pub dead_letter_retention: Duration,
    /// Where `dead_letters` lives (mode `collection`).
    pub db: Option<Database>,
}

/// What processor control (ADR 0023) tells a processor's consumer, shared by its every run: the
/// pause and the open replay ranges.
#[derive(Debug, Default)]
pub struct ConsumerControl {
    paused: AtomicBool,
    replay: Mutex<Vec<ReplayRange>>,
}

impl ConsumerControl {
    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    pub(crate) fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::SeqCst);
    }

    pub(crate) fn set_replay(&self, ranges: Vec<ReplayRange>) {
        *self.replay.lock().unwrap() = ranges;
    }

    /// Whether the record at `offset` of `partition` of `topic` is in an open replay range.
    pub fn replays(&self, topic: &str, partition: i32, offset: i64) -> bool {
        self.replay.lock().unwrap().iter().any(|r| {
            r.topic == topic && r.partition == partition && r.from <= offset && offset < r.until
        })
    }
}

/// The consumer's librdkafka callbacks: the assignment and errors go to the processor's monitor
/// (never blocking: they run inside the consumer's poll); a partition assigned while the processor
/// is paused is paused at once.
pub struct ProcessorContext {
    monitor: Arc<ProcessorMonitor>,
    control: Arc<ConsumerControl>,
}

impl ClientContext for ProcessorContext {
    fn error(&self, error: KafkaError, reason: &str) {
        // An idle connection the broker closed is reconnected by librdkafka, and a subscribed topic
        // that does not exist yet is auto-created by its first record: neither is a failure (the
        // latter leaves the processor STARTING, without an assignment).
        if matches!(
            error,
            KafkaError::Global(
                RDKafkaErrorCode::BrokerTransportFailure
                    | RDKafkaErrorCode::UnknownTopicOrPartition
                    | RDKafkaErrorCode::UnknownTopic
            )
        ) {
            tracing::debug!(processor = %self.monitor.name(), "kafka: {error}: {reason}");
            return;
        }
        tracing::warn!(processor = %self.monitor.name(), "kafka error: {error}: {reason}");
        self.monitor.error(&format!("kafka: {error}: {reason}"));
    }
}

impl ConsumerContext for ProcessorContext {
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let partitions = |tpl: &TopicPartitionList| {
            tpl.elements()
                .iter()
                .map(|e| e.partition())
                .collect::<Vec<_>>()
        };
        match rebalance {
            Rebalance::Assign(tpl) => {
                let assigned = partitions(tpl);
                tracing::info!(processor = %self.monitor.name(), partitions = ?assigned, "partitions assigned");
                if self.control.paused()
                    && !assigned.is_empty()
                    && let Err(e) = consumer.pause(tpl)
                {
                    tracing::warn!(processor = %self.monitor.name(), "cannot pause the assigned partitions: {e}");
                }
                self.monitor.assigned(&assigned);
            }
            Rebalance::Revoke(tpl) => {
                let revoked = partitions(tpl);
                tracing::info!(processor = %self.monitor.name(), partitions = ?revoked, "partitions revoked");
                self.monitor.revoked(&revoked);
            }
            Rebalance::Error(e) => {
                tracing::warn!(processor = %self.monitor.name(), "rebalance error: {e}");
                self.monitor.error(&format!("rebalance: {e}"));
            }
        }
    }
}

/// Where a record goes that the processor cannot handle.
enum Sink {
    Log,
    Topic {
        producer: FutureProducer,
        topic: String,
    },
    Collection {
        store: DeadLetterStore,
        retention: Duration,
    },
}

/// The keys this processor has a letter for on its partitions (sequencing, ADR 0022), loaded
/// for the assignment of `generation`.
#[derive(Default)]
struct ParkedKeys {
    generation: Option<u64>,
    keys: HashSet<String>,
}

/// A record that failed on every attempt and whose letter could not be written yet, remembered
/// for the assignment of `generation`: its redelivery goes straight to the dead letters, without
/// running the handlers again (Kotlin's ruling 4).
struct PendingLetter {
    generation: u64,
    error: DeadLetterError,
    attempts: u32,
}

pub struct EventConsumer {
    consumer: StreamConsumer<ProcessorContext>,
    dispatcher: Arc<LocalEventDispatcher>,
    topic: String,
    name: String,
    max_attempts: u32,
    backoff: Duration,
    sink: Sink,
    monitor: Arc<ProcessorMonitor>,
    control: Arc<ConsumerControl>,
    parked: Mutex<ParkedKeys>,
    /// Per (partition, offset): what the dead letters still have to take.
    pending: Mutex<HashMap<(i32, i64), PendingLetter>>,
    /// Per partition: the generation and offset it was sought back to, until that record is
    /// delivered again.
    rewound: Mutex<HashMap<i32, (u64, i64)>>,
}

/// Whether a record was acknowledged, and how it counts in the status.
enum Done {
    /// Acknowledge; `processed`: its dispatch completed without a stage-1 failure.
    Ack { processed: bool },
    /// The dead letters could not take it (or the sequencing reads failed): leave the offset
    /// uncommitted and deliver the record again after [`PARK_RETRY`].
    Again,
    /// Shutting down: leave the offset uncommitted for the next run.
    Stop,
}

impl EventConsumer {
    /// The consumer of one event processor (ADR 0022): its group, its dispatcher (whose marker
    /// prefix and handlers are the processor's) and its dead letters.
    pub fn processor(
        settings: ProcessorSettings,
        dispatcher: Arc<LocalEventDispatcher>,
    ) -> Result<Arc<EventConsumer>, MessagingError> {
        let monitor = Arc::new(ProcessorMonitor::new(
            &settings.name,
            &settings.group_id,
            &settings.topic,
            settings.dead_letter_mode,
            dispatcher.event_types(),
        ));
        Self::controlled(
            settings,
            dispatcher,
            monitor,
            Arc::new(ConsumerControl::default()),
        )
    }

    /// One run of a controlled processor's consumer (ADR 0023): its monitor and its control
    /// outlive the run, which ends (and leaves the group) when the consumer is dropped.
    pub fn controlled(
        settings: ProcessorSettings,
        dispatcher: Arc<LocalEventDispatcher>,
        monitor: Arc<ProcessorMonitor>,
        control: Arc<ConsumerControl>,
    ) -> Result<Arc<EventConsumer>, MessagingError> {
        let consumer: StreamConsumer<ProcessorContext> = ClientConfig::new()
            .set("bootstrap.servers", &settings.bootstrap_servers)
            .set("group.id", &settings.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "true")
            .create_with_context(ProcessorContext {
                monitor: Arc::clone(&monitor),
                control: Arc::clone(&control),
            })?;
        let sink = match settings.dead_letter_mode {
            DeadLetterMode::Log => Sink::Log,
            DeadLetterMode::Topic => Sink::Topic {
                producer: ClientConfig::new()
                    .set("bootstrap.servers", &settings.bootstrap_servers)
                    .set("enable.idempotence", "true")
                    .set("message.timeout.ms", "30000")
                    .create()?,
                topic: format!("{}.dlt", settings.topic),
            },
            DeadLetterMode::Collection => Sink::Collection {
                store: DeadLetterStore::new(settings.db.as_ref().ok_or_else(|| {
                    MessagingError::Storage(
                        "dead-letter mode collection needs the service's database".into(),
                    )
                })?),
                retention: settings.dead_letter_retention,
            },
        };
        Ok(Arc::new(EventConsumer {
            consumer,
            dispatcher,
            topic: settings.topic,
            name: settings.name,
            max_attempts: settings.max_attempts.max(1),
            backoff: settings.backoff,
            sink,
            monitor,
            control,
            parked: Mutex::new(ParkedKeys::default()),
            pending: Mutex::new(HashMap::new()),
            rewound: Mutex::new(HashMap::new()),
        }))
    }

    /// What this processor reports in `InstanceStatus.processors`.
    pub fn monitor(&self) -> Arc<ProcessorMonitor> {
        Arc::clone(&self.monitor)
    }

    /// Pauses (or resumes) every partition assigned now; a partition assigned later is paused
    /// by the rebalance callback while [`ConsumerControl`] says paused.
    pub fn apply_pause(&self) {
        let paused = self.control.paused();
        let assignment = match self.consumer.assignment() {
            Ok(tpl) => tpl,
            Err(e) => {
                tracing::warn!(processor = %self.name, "cannot read the assignment: {e}");
                return;
            }
        };
        if assignment.count() == 0 {
            return;
        }
        let applied = if paused {
            self.consumer.pause(&assignment)
        } else {
            self.consumer.resume(&assignment)
        };
        if let Err(e) = applied {
            tracing::warn!(processor = %self.name, paused, "cannot pause or resume the assigned partitions: {e}");
        }
    }

    /// Consumes until `cancel`; also refreshes the committed offsets and high watermarks (on a
    /// blocking thread, every [`OFFSETS_REFRESH`]) and, in mode `collection`, counts the
    /// processor's letters (every [`DEAD_LETTERS_SAMPLE`] and after a park).
    pub fn start(self: Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            let helpers = cancel.child_token();
            let refresher = tokio::spawn(Arc::clone(&self).refresh_offsets(helpers.clone()));
            let sampler = match &self.sink {
                Sink::Collection { store, .. } => Some(tokio::spawn(
                    Arc::clone(&self).sample_dead_letters(store.clone(), helpers.clone()),
                )),
                _ => None,
            };
            self.consume(&cancel).await;
            self.monitor.stopped();
            helpers.cancel();
            let _ = refresher.await;
            if let Some(sampler) = sampler {
                let _ = sampler.await;
            }
        })
    }

    async fn consume(&self, cancel: &CancellationToken) {
        if let Err(e) = self.consumer.subscribe(&[self.topic.as_str()]) {
            tracing::error!(processor = %self.name, "cannot subscribe to {}: {e}", self.topic);
            self.monitor
                .error(&format!("cannot subscribe to {}: {e}", self.topic));
            return;
        }
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                received = self.consumer.recv() => match received {
                    // The events topic does not exist yet (auto-created by its first record): the
                    // processor stays STARTING (no assignment), it is not failing.
                    Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic,
                    )) => {
                        tracing::debug!(processor = %self.name, "topic {} not available yet", self.topic);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(e) => {
                        tracing::warn!(processor = %self.name, "kafka receive error: {e}");
                        self.monitor.error(&format!("kafka receive: {e}"));
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Ok(msg) => {
                        if self.overtakes(&msg) {
                            continue;
                        }
                        // Fetched before the pause: not handled, and its partition sought back to
                        // it, so the resume starts there.
                        if self.control.paused() {
                            self.rewind(msg.partition(), msg.offset());
                            tokio::select! {
                                _ = tokio::time::sleep(PAUSED_POLL) => {}
                                _ = cancel.cancelled() => break,
                            }
                            continue;
                        }
                        let processed = match self.handle_record(&msg, cancel).await {
                            Done::Ack { processed } => processed,
                            // Not committed: back to the poll after a second, which delivers the
                            // record again (never holding the poll loop for longer).
                            Done::Again => {
                                self.rewind(msg.partition(), msg.offset());
                                tokio::select! {
                                    _ = tokio::time::sleep(PARK_RETRY) => {}
                                    _ = cancel.cancelled() => break,
                                }
                                continue;
                            }
                            Done::Stop => break, // leave the offset uncommitted for the next run
                        };
                        if let Err(e) = self.consumer.commit_message(&msg, CommitMode::Async) {
                            tracing::warn!(processor = %self.name, "offset commit failed: {e}");
                        }
                        self.monitor.acknowledged(msg.partition(), msg.offset(), processed);
                    }
                }
            }
        }
        self.monitor.stopped();
    }

    /// Seeks `partition` back to `offset`, the record that could not be acknowledged: the next
    /// polls deliver it again (the partition's later records wait behind it).
    fn rewind(&self, partition: i32, offset: i64) {
        self.rewound
            .lock()
            .unwrap()
            .insert(partition, (self.monitor.generation(), offset));
        // Asynchronous: librdkafka drops the partition's records fetched before the seek at once.
        if let Err(e) = self.consumer.seek(
            &self.topic,
            partition,
            Offset::Offset(offset),
            Duration::ZERO,
        ) {
            tracing::warn!(processor = %self.name, partition, offset, "cannot seek back to an unacknowledged record: {e}");
        }
    }

    /// True for a record that comes after the one its partition was sought back to (a seek that
    /// did not take effect): it is not handled, and the partition is sought back again. A
    /// rebalance since forgets the rewind (the group's committed offset decides then).
    fn overtakes(&self, msg: &BorrowedMessage<'_>) -> bool {
        let rewound = self.rewound.lock().unwrap().get(&msg.partition()).copied();
        match rewound {
            Some((generation, offset))
                if generation == self.monitor.generation() && msg.offset() > offset =>
            {
                self.rewind(msg.partition(), offset);
                true
            }
            Some(_) => {
                self.rewound.lock().unwrap().remove(&msg.partition());
                false
            }
            None => false,
        }
    }

    async fn handle_record(&self, msg: &BorrowedMessage<'_>, cancel: &CancellationToken) -> Done {
        if !self.load_parked_keys().await {
            return Done::Again;
        }
        let raw_headers: Vec<(String, Vec<u8>)> = msg
            .headers()
            .map(|h| {
                h.iter()
                    .map(|x| (x.key.to_string(), x.value.unwrap_or(&[]).to_vec()))
                    .collect()
            })
            .unwrap_or_default();
        let hdrs: HashMap<String, String> = raw_headers
            .iter()
            .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).to_string()))
            .collect();
        let generation = self.monitor.generation();
        let pending = self
            .pending
            .lock()
            .unwrap()
            .remove(&(msg.partition(), msg.offset()))
            .filter(|p| p.generation == generation);
        if let Some(pending) = pending {
            // Its attempts are done: only the letter is missing.
            return self
                .letter_after_attempts(msg, &raw_headers, pending.error, pending.attempts)
                .await;
        }
        let payload = msg.payload().unwrap_or(&[]);
        let (type_name, id) = match classify_with(
            &hdrs,
            payload,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            |t| self.dispatcher.interested_here(t),
        ) {
            RecordDisposition::Skip(NO_HANDLER) => return Done::Ack { processed: false },
            // Malformed: a letter that blocks nothing (ADR 0022 INVALID).
            RecordDisposition::Skip(reason) => {
                tracing::warn!(processor = %self.name, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(), "invalid record: {reason}");
                let parked = self
                    .dead_letter(
                        msg,
                        &raw_headers,
                        DeadLetterReason::Invalid,
                        DeadLetterError::invalid(reason),
                        None,
                        0,
                    )
                    .await;
                return if parked {
                    Done::Ack { processed: false }
                } else {
                    Done::Again
                };
            }
            RecordDisposition::Handle { type_name, id } => (type_name, id),
        };
        if !hdrs.get(headers::ID).is_some_and(|i| *i == id) {
            tracing::warn!(processor = %self.name, r#type = %type_name, id = %id, "record has no id header, using topic-partition-offset for idempotency");
        }
        let ctx = message_context(&type_name, &id, &hdrs);
        let stage_one = self.dispatcher.has_stage_one(&type_name);
        if stage_one && let Some(key) = msg.key().map(|k| String::from_utf8_lossy(k).into_owned()) {
            match self.head_of(&key).await {
                None => return Done::Again,
                Some(Some(head)) => {
                    let parked = self
                        .dead_letter(
                            msg,
                            &raw_headers,
                            DeadLetterReason::Sequenced,
                            DeadLetterError::sequenced(&head),
                            Some(head),
                            0,
                        )
                        .await;
                    if !parked {
                        return Done::Again;
                    }
                    // A replay never reaches the sagas (ADR 0023).
                    if self
                        .control
                        .replays(msg.topic(), msg.partition(), msg.offset())
                    {
                        return Done::Ack { processed: false };
                    }
                    // Stage 2 still sees it (ADR 0021 ruling 26).
                    return self
                        .stage_two_only(&type_name, &id, payload, &ctx, cancel)
                        .await;
                }
                Some(None) => {}
            }
        }
        let position = self.position(msg);
        let replay = self
            .control
            .replays(msg.topic(), msg.partition(), msg.offset());
        for attempt in 1..=self.max_attempts {
            let stages = self
                .dispatcher
                .dispatch_record(
                    &id,
                    &type_name,
                    payload,
                    &ctx,
                    RecordDispatch {
                        position: Some(&position),
                        stage_one: true,
                        replay,
                    },
                )
                .await;
            let (failure, in_stage_one) = match (stages.stage_one, stages.stage_two) {
                (Some(Err(e)), _) => (e, true),
                (_, Some(Err(e))) => (e, false),
                _ => return Done::Ack { processed: true },
            };
            // A payload the handler cannot read (or a handler's own InvalidMessage) cannot get
            // better by retrying: parked after this attempt.
            let hopeless = in_stage_one && matches!(failure, MessagingError::InvalidMessage(_));
            if attempt < self.max_attempts && !hopeless {
                tracing::warn!(processor = %self.name, r#type = %type_name, id = %id, attempt, "event handler failed, retrying: {failure}");
                tokio::select! {
                    _ = tokio::time::sleep(self.backoff) => {}
                    _ = cancel.cancelled() => return Done::Stop,
                }
                continue;
            }
            self.monitor
                .exhausted(&format!("{}: {failure}", failure.variant_name()));
            if !in_stage_one {
                // Stage 1 is done; the saga stage could not even park its failure (ADR 0021): logged
                // and skipped, no letter (a retry would find the marker and do nothing).
                tracing::error!(processor = %self.name, r#type = %type_name, id = %id, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(), attempts = attempt, "saga stage failed, record skipped: {failure}");
                return Done::Ack { processed: false };
            }
            tracing::error!(processor = %self.name, r#type = %type_name, id = %id, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(), attempts = attempt, "event handler failed on every attempt: {failure}");
            return self
                .letter_after_attempts(
                    msg,
                    &raw_headers,
                    DeadLetterError::from_error(&failure),
                    attempt,
                )
                .await;
        }
        Done::Ack { processed: false }
    }

    /// What this processor's stage-1 marker of `msg` records (ADR 0023).
    fn position(&self, msg: &BorrowedMessage<'_>) -> MarkerPosition {
        MarkerPosition {
            processor: self.name.clone(),
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
        }
    }

    /// The HANDLER_FAILED letter of a record whose attempts are done. When the dead letters cannot
    /// take it, the failure is remembered for the record's redelivery (which then comes straight
    /// here) as long as the partition stays assigned.
    async fn letter_after_attempts(
        &self,
        msg: &BorrowedMessage<'_>,
        raw_headers: &[(String, Vec<u8>)],
        error: DeadLetterError,
        attempts: u32,
    ) -> Done {
        if self
            .dead_letter(
                msg,
                raw_headers,
                DeadLetterReason::HandlerFailed,
                error.clone(),
                None,
                attempts,
            )
            .await
        {
            return Done::Ack { processed: false };
        }
        let generation = self.monitor.generation();
        let mut pending = self.pending.lock().unwrap();
        pending.retain(|_, p| p.generation == generation);
        pending.insert(
            (msg.partition(), msg.offset()),
            PendingLetter {
                generation,
                error,
                attempts,
            },
        );
        Done::Again
    }

    /// A SEQUENCED record through the saga stage alone, with the usual attempts; a failure that
    /// outlives them is logged (the record is parked already).
    async fn stage_two_only(
        &self,
        type_name: &str,
        id: &str,
        payload: &[u8],
        ctx: &MessageContext,
        cancel: &CancellationToken,
    ) -> Done {
        for attempt in 1..=self.max_attempts {
            let stages = self
                .dispatcher
                .dispatch_stages(id, type_name, payload, ctx, false)
                .await;
            match stages.stage_two {
                Some(Err(e)) if attempt < self.max_attempts => {
                    tracing::warn!(processor = %self.name, r#type = %type_name, id = %id, attempt, "saga stage failed for a sequenced record, retrying: {e}");
                    tokio::select! {
                        _ = tokio::time::sleep(self.backoff) => {}
                        _ = cancel.cancelled() => return Done::Stop,
                    }
                }
                Some(Err(e)) => {
                    tracing::error!(processor = %self.name, r#type = %type_name, id = %id, "saga stage failed for a sequenced record: {e}");
                    self.monitor
                        .exhausted(&format!("{}: {e}", e.variant_name()));
                    break;
                }
                _ => break,
            }
        }
        Done::Ack { processed: false }
    }

    /// In mode `collection`, the head letter of `key` when the key is in the parked set and the
    /// collection confirms it (a miss removes the key: another instance retried or discarded its
    /// letters). `None` when the store cannot be read (the record is delivered again).
    async fn head_of(&self, key: &str) -> Option<Option<String>> {
        let Sink::Collection { store, .. } = &self.sink else {
            return Some(None);
        };
        if !self.parked.lock().unwrap().keys.contains(key) {
            return Some(None);
        }
        match store.head(&self.name, key).await {
            Ok(Some(head)) => Some(Some(head)),
            Ok(None) => {
                self.parked.lock().unwrap().keys.remove(key);
                Some(None)
            }
            Err(e) => {
                tracing::warn!(processor = %self.name, "cannot check the dead letters of a key, the record comes again: {e}");
                self.monitor.error(&format!("dead letters: {e}"));
                None
            }
        }
    }

    /// After an assignment (mode `collection`): the keys of the processor's letters on the
    /// assigned partitions. False when they cannot be read (the record is delivered again, and
    /// the load tried again).
    async fn load_parked_keys(&self) -> bool {
        let Sink::Collection { store, .. } = &self.sink else {
            return true;
        };
        let generation = self.monitor.generation();
        if self.parked.lock().unwrap().generation == Some(generation) {
            return true;
        }
        let partitions = self.monitor.assigned_partitions();
        match store
            .parked_keys(&self.name, &self.topic, &partitions)
            .await
        {
            Ok(keys) => {
                *self.parked.lock().unwrap() = ParkedKeys {
                    generation: Some(generation),
                    keys,
                };
                true
            }
            Err(e) => {
                tracing::warn!(processor = %self.name, "cannot load the parked keys, the record comes again: {e}");
                self.monitor.error(&format!("dead letters: {e}"));
                false
            }
        }
    }

    /// Hands the record to the sink: logged (mode `log`), produced to `<topic>.dlt` (mode `topic`,
    /// SEQUENCED never happens there) or parked in `dead_letters` (mode `collection`). False when
    /// the sink could not take it: the record is then not acknowledged and comes again.
    async fn dead_letter(
        &self,
        msg: &BorrowedMessage<'_>,
        raw_headers: &[(String, Vec<u8>)],
        reason: DeadLetterReason,
        error: DeadLetterError,
        blocked_by: Option<String>,
        attempts: u32,
    ) -> bool {
        match &self.sink {
            Sink::Log => {
                tracing::error!(processor = %self.name, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(), reason = reason.as_str(), "record skipped: {}", error.message);
                true
            }
            Sink::Topic { producer, topic } => {
                let mut headers = OwnedHeaders::new();
                for (k, v) in raw_headers
                    .iter()
                    .filter(|(k, _)| !DLT_HEADERS.contains(&k.as_str()))
                {
                    headers = headers.insert(Header {
                        key: k,
                        value: Some(v.as_slice()),
                    });
                }
                let (partition, offset) = (msg.partition().to_string(), msg.offset().to_string());
                let dlt_error = cut_chars(
                    &format!("{}: {}", error.type_name, error.message),
                    MAX_ERROR_MESSAGE_CHARS,
                );
                for (k, v) in DLT_HEADERS.into_iter().zip([
                    self.name.as_str(),
                    reason.as_str(),
                    dlt_error.as_str(),
                    msg.topic(),
                    partition.as_str(),
                    offset.as_str(),
                ]) {
                    headers = headers.insert(Header {
                        key: k,
                        value: Some(v),
                    });
                }
                let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic)
                    .payload(msg.payload().unwrap_or(&[]))
                    .headers(headers);
                if let Some(key) = msg.key() {
                    record = record.key(key);
                }
                match producer
                    .send(record, Timeout::After(Duration::from_secs(30)))
                    .await
                {
                    Ok(_) => true,
                    Err((e, _)) => {
                        tracing::warn!(processor = %self.name, dlt = %topic, "cannot produce to the dead-letter topic, the record comes again: {e}");
                        self.monitor
                            .error(&format!("dead-letter topic {topic}: {e}"));
                        false
                    }
                }
            }
            Sink::Collection { store, retention } => {
                // Parked in a replay: its retry must be one too (ADR 0023).
                let replay = self
                    .control
                    .replays(msg.topic(), msg.partition(), msg.offset());
                let letter = DeadLetterDocument {
                    replay,
                    ..DeadLetterDocument::park(
                        DeadLetterRecord {
                            processor: self.name.clone(),
                            topic: msg.topic().to_string(),
                            partition: msg.partition(),
                            offset: msg.offset(),
                            key: msg.key().map(<[u8]>::to_vec),
                            headers: raw_headers.to_vec(),
                            payload: msg.payload().unwrap_or(&[]).to_vec(),
                        },
                        reason,
                        error,
                        blocked_by,
                        attempts,
                        mongodb::bson::DateTime::now(),
                        *retention,
                    )
                };
                match store.park(&letter).await {
                    Ok(parked) => {
                        if parked == Parked::Already {
                            tracing::info!(processor = %self.name, letter = %letter.id, "record already parked");
                        } else {
                            tracing::warn!(processor = %self.name, letter = %letter.id, reason = reason.as_str(), "record parked in dead_letters");
                        }
                        if reason != DeadLetterReason::Invalid
                            && let Some(key) = &letter.key
                        {
                            self.parked.lock().unwrap().keys.insert(key.clone());
                        }
                        self.monitor.sample_dead_letters_soon();
                        true
                    }
                    Err(e) => {
                        tracing::warn!(processor = %self.name, letter = %letter.id, "cannot park the record, it comes again (its offset stays uncommitted): {e}");
                        self.monitor
                            .error(&format!("cannot park {}: {e}", letter.id));
                        false
                    }
                }
            }
        }
    }

    /// Every [`OFFSETS_REFRESH`]: the group's committed offsets and the high watermarks of the
    /// assigned partitions, read on a blocking thread (librdkafka's calls block; the consumer task
    /// and the hub stream never wait for them).
    async fn refresh_offsets(self: Arc<Self>, cancel: CancellationToken) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(OFFSETS_REFRESH) => {}
            }
            let partitions = self.monitor.assigned_partitions();
            if partitions.is_empty() {
                continue;
            }
            let me = Arc::clone(&self);
            let read = tokio::task::spawn_blocking(move || me.read_offsets(&partitions));
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = read => {}
            }
        }
    }

    fn read_offsets(&self, partitions: &[i32]) {
        let mut tpl = TopicPartitionList::new();
        for &p in partitions {
            tpl.add_partition(&self.topic, p);
        }
        let committed = match self.consumer.committed_offsets(tpl, OFFSETS_TIMEOUT) {
            Ok(list) => list,
            Err(e) => {
                tracing::debug!(processor = %self.name, "committed offsets not read: {e}");
                TopicPartitionList::new()
            }
        };
        for &p in partitions {
            let offset = committed
                .find_partition(&self.topic, p)
                .and_then(|e| match e.offset() {
                    Offset::Offset(o) => Some(o),
                    _ => None,
                })
                .unwrap_or(-1);
            let high = match self
                .consumer
                .fetch_watermarks(&self.topic, p, OFFSETS_TIMEOUT)
            {
                Ok((_, high)) => high,
                Err(e) => {
                    tracing::debug!(processor = %self.name, partition = p, "watermarks not read: {e}");
                    -1
                }
            };
            self.monitor.offsets(p, offset, high);
        }
    }

    /// `countDocuments({processor})` at start, every [`DEAD_LETTERS_SAMPLE`] and after a park.
    async fn sample_dead_letters(
        self: Arc<Self>,
        store: DeadLetterStore,
        cancel: CancellationToken,
    ) {
        loop {
            match store.count(Some(&self.name)).await {
                Ok(n) => self.monitor.set_dead_letters(n),
                Err(e) => tracing::debug!(processor = %self.name, "dead letters not counted: {e}"),
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(DEAD_LETTERS_SAMPLE) => {}
                _ = self.monitor.sample_requested() => {}
            }
        }
    }
}

/// The reason [`classify_with`] gives for a type nobody handles: skipped, not a letter.
const NO_HANDLER: &str = "no handler for this type";
/// The headers mode `topic` adds; a record's own header of one of these names is replaced.
const DLT_HEADERS: [&str; 6] = [
    "kraken-dlt-processor",
    "kraken-dlt-reason",
    "kraken-dlt-error",
    "kraken-dlt-original-topic",
    "kraken-dlt-original-partition",
    "kraken-dlt-original-offset",
];

/// What the handlers of a record see: its id and type, the correlation id header (else the id) and
/// every header.
pub(crate) fn message_context(
    type_name: &str,
    id: &str,
    hdrs: &HashMap<String, String>,
) -> MessageContext {
    MessageContext {
        message_id: id.to_string(),
        type_name: type_name.to_string(),
        correlation_id: hdrs
            .get(headers::CORRELATION_ID)
            .filter(|c| !c.is_empty())
            .cloned()
            .unwrap_or_else(|| id.to_string()),
        identity: None,
        authorization: None,
        headers: hdrs.clone(),
    }
}
