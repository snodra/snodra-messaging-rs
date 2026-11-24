//! Reads one event processor's subscription of the event store: one record
//! at a time; the dedupe marker is recorded first, then the handlers run, all in one transaction
//! of the message store; the record is acknowledged after. A record whose handlers fail on every
//! attempt, a malformed one and, in mode `collection`, one whose key has a parked letter go to the
//! processor's dead letters before it is acknowledged. When the dead letters cannot take it (the
//! store or the event store unreachable), the record is not acknowledged: the subscription hands
//! it out again ([`Subscription::redeliver`]) after a second, and keeps its other segments
//! flowing.
//!
//! A subscription whose `next` fails is broken: it is closed and the processor subscribes again
//! after a backoff ([`keep_subscribed`](super::keep_subscribed)), showing the error on its
//! status. What the subscription recovered from by itself ([`Subscription::take_error`]) shows
//! there too.
//!
//! Processor control reaches it through its [`ConsumerControl`]: paused, it pauses its
//! subscription (which keeps its segments, and pauses every segment assigned later); a record of a
//! replay range is handled without the handlers that disallow replay and without the sagas.

use std::cmp::Ordering as Order;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use opentelemetry::Value;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::dispatch::{LocalEventDispatcher, MarkerPosition, RecordDispatch, StageOneDone};
use super::publisher::OUTBOX_SEND_TIMEOUT;
use super::resubscribe::{Ended, Resubscribing, keep_subscribed};
use crate::codec;
use crate::config::DeadLetterMode;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::event_store::{
    Capability, Delivery, EventStore, EventStoreError, OutgoingEvent, ReplayRange, Subscription,
    SubscriptionEvent, SubscriptionSpec, dead_letter_source, headers, segment_order,
};
use crate::metrics::{Consumed, Metrics, OTHER, TypeLabels};
use crate::processors::ProcessorMonitor;
use crate::processors::dead_letters::{DeadLetterRecord, park};
use crate::store::dead_letters::cut_chars;
use crate::store::{
    DeadLetterError, DeadLetterReason, DeadLetterStore, MAX_ERROR_MESSAGE_CHARS, Parked,
};
use crate::time::now_ms;

/// What to do with a record, decided before any database work.
#[derive(Debug, PartialEq, Eq)]
pub enum RecordDisposition {
    /// Acknowledge it and move on; the reason is logged (never the payload).
    Skip(&'static str),
    Handle {
        type_name: String,
        id: String,
    },
}

/// Mirrors `EventConsumer.kt`: no `type` header (or a blank one), a type nobody handles, or a
/// non-object payload are skipped; a missing or blank `id` header falls back to
/// `<source>-<segment>-<position>`.
pub fn classify(
    hdrs: &HashMap<String, String>,
    payload: &[u8],
    source: &str,
    segment: &str,
    position: &str,
    registry: &crate::hub::HandlerRegistry,
) -> RecordDisposition {
    classify_with(hdrs, payload, source, segment, position, |t| {
        registry.has_event_handler(t)
    })
}

/// [`classify`] with the interest in a type given as a predicate: the dispatcher's, which is an
/// ordinary event handler or a saga type (a record only a saga wants is handled, not skipped).
pub fn classify_with(
    hdrs: &HashMap<String, String>,
    payload: &[u8],
    source: &str,
    segment: &str,
    position: &str,
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
        .unwrap_or_else(|| format!("{source}-{segment}-{position}"));
    RecordDisposition::Handle {
        type_name: type_name.clone(),
        id,
    }
}

/// How often the processor's dead letters are counted (every 30 s).
pub const DEAD_LETTERS_SAMPLE: Duration = Duration::from_secs(30);
/// How often the subscription's segment statuses are copied into the processor's status (they
/// also are after every acknowledgement).
const STATUS_REFRESH: Duration = Duration::from_secs(1);
/// Between a record the dead letters could not take (a park, a dead-letter publish or a
/// sequencing read failed) and its redelivery.
const PARK_RETRY: Duration = Duration::from_secs(1);
/// What a processor's consumer is: its name, its subscription on the event store,
/// its dead-letter mode and where its letters go.
#[derive(Clone)]
pub struct ProcessorSettings {
    /// `default` or the name given to `event_in`.
    pub name: String,
    /// `<service>.<processor>` on the events source.
    pub subscription: SubscriptionSpec,
    /// The event store the processor reads (and its dead letters go to in mode `event-store`).
    pub events: Arc<dyn EventStore>,
    /// Handler invocations per record in all (`SNODRA_CONSUMER_MAX_ATTEMPTS`), at least 1.
    pub max_attempts: u32,
    /// Between two invocations.
    pub backoff: Duration,
    pub dead_letter_mode: DeadLetterMode,
    pub dead_letter_retention: Duration,
    /// The message store's dead letters (mode `collection`).
    pub dead_letters: Option<Arc<dyn DeadLetterStore>>,
    /// What the consumer records `snodra.events.consumed` and `snodra.event.handler.duration`
    /// on.
    pub metrics: Metrics,
}

/// What processor control tells a processor's consumer, shared by its every run: the
/// pause and the open replay ranges.
pub struct ConsumerControl {
    paused: AtomicBool,
    replay: Mutex<Vec<ReplayRange>>,
    /// Woken when the pause changes.
    changed: Notify,
    /// The event store's order of positions.
    events: Arc<dyn EventStore>,
}

impl ConsumerControl {
    /// Not paused, nothing replaying; positions ordered by `events`.
    pub fn new(events: Arc<dyn EventStore>) -> ConsumerControl {
        ConsumerControl {
            paused: AtomicBool::new(false),
            replay: Mutex::new(Vec::new()),
            changed: Notify::new(),
            events,
        }
    }

    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    pub(crate) fn set_paused(&self, paused: bool) {
        if self.paused.swap(paused, Ordering::SeqCst) != paused {
            self.changed.notify_one();
        }
    }

    pub(crate) fn set_replay(&self, ranges: Vec<ReplayRange>) {
        *self.replay.lock().unwrap() = ranges;
    }

    /// Whether the record at `position` of `segment` of `source` is in an open replay range
    /// (`from <= position < until`, by the event store's order).
    pub fn replays(&self, source: &str, segment: &str, position: &str) -> bool {
        self.replay.lock().unwrap().iter().any(|r| {
            r.source == source
                && r.segment == segment
                && self.events.compare(&r.from, position) != Order::Greater
                && self.events.compare(position, &r.until) == Order::Less
        })
    }
}

/// Where a record goes that the processor cannot handle.
enum Sink {
    Log,
    /// The event store's dead-letter source (`<source>.dlt`).
    EventStore {
        events: Arc<dyn EventStore>,
        source: String,
    },
    Collection {
        store: Arc<dyn DeadLetterStore>,
        retention: Duration,
    },
}

/// The keys this processor has a letter for on its segments (sequencing), loaded for
/// the assignment of `generation`.
#[derive(Default)]
struct ParkedKeys {
    generation: Option<u64>,
    keys: HashSet<String>,
}

/// A record that failed on every attempt and whose letter could not be written yet, remembered
/// for the assignment of `generation`: its redelivery goes straight to the dead letters, without
/// running the handlers again.
struct PendingLetter {
    generation: u64,
    error: DeadLetterError,
    attempts: u32,
}

pub struct EventConsumer {
    events: Arc<dyn EventStore>,
    subscription: SubscriptionSpec,
    dispatcher: Arc<LocalEventDispatcher>,
    name: String,
    max_attempts: u32,
    backoff: Duration,
    sink: Sink,
    monitor: Arc<ProcessorMonitor>,
    control: Arc<ConsumerControl>,
    parked: Mutex<ParkedKeys>,
    /// Per (segment, position): what the dead letters still have to take.
    pending: Mutex<HashMap<(String, String), PendingLetter>>,
    metrics: Metrics,
    /// The `processor` label.
    processor_label: Value,
    /// The `type` labels: the types this processor handles, anything else `other`.
    types: TypeLabels,
}

/// Whether a record was acknowledged, and how it counts in the status.
enum Done {
    /// Acknowledge; `processed`: its dispatch completed without a stage-1 failure.
    Ack { processed: bool },
    /// The dead letters could not take it (or the sequencing reads failed): leave it
    /// unacknowledged and deliver it again after [`PARK_RETRY`].
    Again,
    /// Shutting down: leave it unacknowledged for the next run.
    Stop,
}

/// What one turn of a subscription's loop got.
enum Step {
    Cancelled,
    PauseChanged,
    Refresh,
    Next(Result<SubscriptionEvent, EventStoreError>),
}

impl EventConsumer {
    /// The consumer of one event processor: its subscription, its dispatcher (whose
    /// marker prefix and handlers are the processor's) and its dead letters.
    pub fn processor(
        settings: ProcessorSettings,
        dispatcher: Arc<LocalEventDispatcher>,
    ) -> Result<Arc<EventConsumer>, MessagingError> {
        let monitor = Arc::new(ProcessorMonitor::new(
            &settings.name,
            &settings.subscription,
            &settings.events,
            settings.dead_letter_mode,
            dispatcher.event_types(),
        ));
        let control = Arc::new(ConsumerControl::new(Arc::clone(&settings.events)));
        Self::controlled(settings, dispatcher, monitor, control)
    }

    /// One run of a controlled processor's consumer: its monitor and its control
    /// outlive the run, which ends (and leaves the subscription) when its task ends.
    pub fn controlled(
        settings: ProcessorSettings,
        dispatcher: Arc<LocalEventDispatcher>,
        monitor: Arc<ProcessorMonitor>,
        control: Arc<ConsumerControl>,
    ) -> Result<Arc<EventConsumer>, MessagingError> {
        let sink = match settings.dead_letter_mode {
            DeadLetterMode::Log => Sink::Log,
            DeadLetterMode::EventStore => {
                check_dead_letter_mode(DeadLetterMode::EventStore, &*settings.events)?;
                Sink::EventStore {
                    events: Arc::clone(&settings.events),
                    source: dead_letter_source(&settings.subscription.source),
                }
            }
            DeadLetterMode::Collection => Sink::Collection {
                store: settings.dead_letters.clone().ok_or_else(|| {
                    MessagingError::InvalidMessage(
                        "dead-letter mode collection needs a message store: add .store(...)".into(),
                    )
                })?,
                retention: settings.dead_letter_retention,
            },
        };
        let types = TypeLabels::new(dispatcher.event_types());
        Ok(Arc::new(EventConsumer {
            events: settings.events,
            subscription: settings.subscription,
            processor_label: Value::from(Arc::<str>::from(settings.name.as_str())),
            types,
            metrics: settings.metrics,
            dispatcher,
            name: settings.name,
            max_attempts: settings.max_attempts.max(1),
            backoff: settings.backoff,
            sink,
            monitor,
            control,
            parked: Mutex::new(ParkedKeys::default()),
            pending: Mutex::new(HashMap::new()),
        }))
    }

    /// What this processor reports in `InstanceStatus.processors`.
    pub fn monitor(&self) -> Arc<ProcessorMonitor> {
        Arc::clone(&self.monitor)
    }

    /// Consumes until `cancel` (then leaves the subscription); in mode `collection` also counts
    /// the processor's letters (every 30 s and after a park).
    pub fn start(self: Arc<Self>, cancel: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            let helpers = cancel.child_token();
            let sampler = match &self.sink {
                Sink::Collection { store, .. } => Some(tokio::spawn(
                    Arc::clone(&self).sample_dead_letters(store.clone(), helpers.clone()),
                )),
                _ => None,
            };
            // A panic ends the reader (an event store's subscription that panicked): STOPPED
            // with the reason until the processor is started again, never RUNNING while nothing
            // reads.
            if let Err(panic) = AssertUnwindSafe(self.consume(&cancel)).catch_unwind().await {
                let cause = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("(no message)");
                tracing::error!(processor = %self.name, "the processor stopped reading until it is started again: its reader panicked: {cause}");
                self.monitor
                    .error(&format!("reader stopped: panicked: {cause}"));
            }
            self.monitor.stopped();
            helpers.cancel();
            if let Some(sampler) = sampler {
                let _ = sampler.await;
            }
        })
    }

    /// Subscribes, reads until `cancel` or a failure, and subscribes again after a failure.
    async fn consume(&self, cancel: &CancellationToken) {
        keep_subscribed(self, cancel).await;
        self.monitor.stopped();
    }

    /// Reads `subscription` until `cancel` or its failure.
    async fn read(&self, subscription: &mut dyn Subscription, cancel: &CancellationToken) -> Ended {
        let mut paused = false;
        self.apply_pause(subscription, &mut paused);
        let mut delivered = false;
        let mut refresh = tokio::time::interval(STATUS_REFRESH);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let step = tokio::select! {
                _ = cancel.cancelled() => Step::Cancelled,
                _ = self.control.changed.notified() => Step::PauseChanged,
                _ = refresh.tick() => Step::Refresh,
                next = subscription.next() => Step::Next(next),
            };
            match step {
                Step::Cancelled => return Ended::Stopped,
                Step::PauseChanged => self.apply_pause(subscription, &mut paused),
                Step::Refresh => self.refresh(subscription),
                Step::Next(Err(error)) => {
                    if let Some(e) = subscription.take_error() {
                        self.monitor.error(&e);
                    }
                    return Ended::Failed { error, delivered };
                }
                Step::Next(Ok(SubscriptionEvent::Assigned(segments))) => {
                    tracing::info!(processor = %self.name, segments = ?in_order(&segments), "segments assigned");
                    self.monitor.assigned(&segments);
                    self.monitor.segments(subscription.segments());
                }
                Step::Next(Ok(SubscriptionEvent::Revoked(segments))) => {
                    tracing::info!(processor = %self.name, segments = ?in_order(&segments), "segments revoked");
                    self.monitor.revoked(&segments);
                }
                Step::Next(Ok(SubscriptionEvent::Delivery(record))) => {
                    // Paused since the last turn: not handled, handed out again on the resume
                    // (and no delivery as far as the resubscribe backoff goes).
                    self.apply_pause(subscription, &mut paused);
                    if paused {
                        redeliver(subscription, &record, &self.name);
                        continue;
                    }
                    delivered = true;
                    // While a handler runs (it may take long), the status still follows the
                    // subscription and a pause still reaches it.
                    let handled = self.handle_record(&record, cancel);
                    tokio::pin!(handled);
                    let done = loop {
                        tokio::select! {
                            done = &mut handled => break done,
                            _ = refresh.tick() => self.refresh(subscription),
                            _ = self.control.changed.notified() => {
                                self.apply_pause(subscription, &mut paused);
                            }
                        }
                    };
                    match done {
                        Done::Ack { processed } => {
                            if let Err(e) = subscription.ack(&record.segment, &record.position) {
                                tracing::warn!(processor = %self.name, segment = %record.segment, position = %record.position, "acknowledgement failed: {e}");
                            }
                            self.monitor.acknowledged(processed);
                            self.monitor.segments(subscription.segments());
                        }
                        // Not acknowledged: handed out again after a second (never holding
                        // the subscription's other segments for longer).
                        Done::Again => {
                            redeliver(subscription, &record, &self.name);
                            tokio::select! {
                                _ = tokio::time::sleep(PARK_RETRY) => {}
                                _ = cancel.cancelled() => return Ended::Stopped,
                            }
                        }
                        Done::Stop => return Ended::Stopped,
                    }
                }
            }
            if let Some(e) = subscription.take_error() {
                self.monitor.error(&e);
            }
        }
    }

    /// Copies what `subscription` reports of its segments, and the trouble it recovered from,
    /// into the status.
    fn refresh(&self, subscription: &mut dyn Subscription) {
        self.monitor.segments(subscription.segments());
        if let Some(e) = subscription.take_error() {
            self.monitor.error(&e);
        }
    }

    /// Pauses or resumes `subscription` when the control's pause differs from `paused`.
    fn apply_pause(&self, subscription: &mut dyn Subscription, paused: &mut bool) {
        let wanted = self.control.paused();
        if wanted == *paused {
            return;
        }
        if wanted {
            subscription.pause();
        } else {
            subscription.resume();
        }
        *paused = wanted;
    }

    async fn handle_record(&self, record: &Delivery, cancel: &CancellationToken) -> Done {
        if !self.load_parked_keys().await {
            return Done::Again;
        }
        let hdrs = &record.headers;
        let type_label = match hdrs.get(headers::TYPE) {
            Some(t) => self.types.label(t),
            None => Value::from(OTHER),
        };
        let generation = self.monitor.generation();
        let pending = self
            .pending
            .lock()
            .unwrap()
            .remove(&(record.segment.clone(), record.position.clone()))
            .filter(|p| p.generation == generation);
        if let Some(pending) = pending {
            // Its attempts are done: only the letter is missing.
            return self
                .letter_after_attempts(record, &type_label, pending.error, pending.attempts)
                .await;
        }
        let source = &self.subscription.source;
        let payload = record.payload.as_slice();
        let (type_name, id) = match classify_with(
            hdrs,
            payload,
            source,
            &record.segment,
            &record.position,
            |t| self.dispatcher.interested_here(t),
        ) {
            RecordDisposition::Skip(NO_HANDLER) => {
                self.consumed(&type_label, Consumed::Skipped);
                return Done::Ack { processed: false };
            }
            // Malformed: a letter that blocks nothing.
            RecordDisposition::Skip(reason) => {
                tracing::warn!(processor = %self.name, source = %source, segment = %record.segment, position = %record.position, "invalid record: {reason}");
                let taken = self
                    .dead_letter(
                        record,
                        DeadLetterReason::Invalid,
                        DeadLetterError::invalid(reason),
                        None,
                        0,
                    )
                    .await;
                return match taken {
                    Some(outcome) => {
                        self.consumed(&type_label, outcome);
                        Done::Ack { processed: false }
                    }
                    None => Done::Again,
                };
            }
            RecordDisposition::Handle { type_name, id } => (type_name, id),
        };
        if !hdrs.get(headers::ID).is_some_and(|i| *i == id) {
            tracing::warn!(processor = %self.name, r#type = %type_name, id = %id, "record has no id header, using source-segment-position for idempotency");
        }
        let ctx = message_context(&type_name, &id, hdrs);
        let stage_one = self.dispatcher.has_stage_one(&type_name);
        let replay = self
            .control
            .replays(source, &record.segment, &record.position);
        if stage_one && let Some(key) = &record.key {
            match self.head_of(key).await {
                None => return Done::Again,
                Some(Some(head)) => {
                    let taken = self
                        .dead_letter(
                            record,
                            DeadLetterReason::Sequenced,
                            DeadLetterError::sequenced(&head),
                            Some(head),
                            0,
                        )
                        .await;
                    let Some(outcome) = taken else {
                        return Done::Again;
                    };
                    self.consumed(&type_label, outcome);
                    // A replay never reaches the sagas.
                    if replay {
                        return Done::Ack { processed: false };
                    }
                    // Stage 2 still sees it.
                    return self
                        .stage_two_only(&type_name, &id, payload, &ctx, cancel)
                        .await;
                }
                Some(None) => {}
            }
        }
        let position = MarkerPosition {
            processor: self.name.clone(),
            source: source.clone(),
            segment: record.segment.clone(),
            position: record.position.clone(),
        };
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
            if let Some(took) = stages.stage_one_took {
                self.metrics
                    .event_handled(self.processor_label.clone(), type_label.clone(), took);
            }
            let (failure, in_stage_one) = match (stages.stage_one, stages.stage_two) {
                (Some(Err(e)), _) => (e, true),
                (_, Some(Err(e))) => (e, false),
                (Some(Ok(StageOneDone::Duplicate)), None) => {
                    self.consumed(&type_label, Consumed::Duplicate);
                    return Done::Ack { processed: true };
                }
                _ => {
                    self.consumed(&type_label, Consumed::Handled);
                    return Done::Ack { processed: true };
                }
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
                // Stage 1 is done; the saga stage could not even park its failure: logged
                // and skipped, no letter (a retry would find the marker and do nothing).
                tracing::error!(processor = %self.name, r#type = %type_name, id = %id, source = %source, segment = %record.segment, position = %record.position, attempts = attempt, "saga stage failed, record skipped: {failure}");
                self.consumed(&type_label, Consumed::Failed);
                return Done::Ack { processed: false };
            }
            tracing::error!(processor = %self.name, r#type = %type_name, id = %id, source = %source, segment = %record.segment, position = %record.position, attempts = attempt, "event handler failed on every attempt: {failure}");
            return self
                .letter_after_attempts(
                    record,
                    &type_label,
                    DeadLetterError::from_error(&failure),
                    attempt,
                )
                .await;
        }
        Done::Ack { processed: false }
    }

    /// How a record ended for this processor (`snodra.events.consumed`).
    fn consumed(&self, type_label: &Value, outcome: Consumed) {
        self.metrics
            .event_consumed(self.processor_label.clone(), type_label.clone(), outcome);
    }

    /// The HANDLER_FAILED letter of a record whose attempts are done. When the dead letters cannot
    /// take it, the failure is remembered for the record's redelivery (which then comes straight
    /// here) as long as the segment stays assigned.
    async fn letter_after_attempts(
        &self,
        record: &Delivery,
        type_label: &Value,
        error: DeadLetterError,
        attempts: u32,
    ) -> Done {
        if let Some(outcome) = self
            .dead_letter(
                record,
                DeadLetterReason::HandlerFailed,
                error.clone(),
                None,
                attempts,
            )
            .await
        {
            self.consumed(type_label, outcome);
            return Done::Ack { processed: false };
        }
        let generation = self.monitor.generation();
        let mut pending = self.pending.lock().unwrap();
        pending.retain(|_, p| p.generation == generation);
        pending.insert(
            (record.segment.clone(), record.position.clone()),
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
    /// assigned segments. False when they cannot be read (the record is delivered again, and the
    /// load tried again).
    async fn load_parked_keys(&self) -> bool {
        let Sink::Collection { store, .. } = &self.sink else {
            return true;
        };
        let generation = self.monitor.generation();
        if self.parked.lock().unwrap().generation == Some(generation) {
            return true;
        }
        let segments = self.monitor.assigned_segments();
        match store
            .parked_keys(&self.name, &self.subscription.source, &segments)
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

    /// Hands the record to the sink: logged (mode `log`, the record [`Consumed::Failed`]),
    /// published to the event store's dead-letter source (mode `event-store`, SEQUENCED never
    /// happens there) or parked in `dead_letters` (mode `collection`), the record
    /// [`Consumed::Parked`] (or [`Consumed::Duplicate`]: its letter was there already). `None`
    /// when the sink could not take it: the record is then not acknowledged and comes again.
    async fn dead_letter(
        &self,
        record: &Delivery,
        reason: DeadLetterReason,
        error: DeadLetterError,
        blocked_by: Option<String>,
        attempts: u32,
    ) -> Option<Consumed> {
        let source = &self.subscription.source;
        match &self.sink {
            Sink::Log => {
                tracing::error!(processor = %self.name, source = %source, segment = %record.segment, position = %record.position, reason = reason.as_str(), "record skipped: {}", error.message);
                Some(Consumed::Failed)
            }
            Sink::EventStore {
                events,
                source: dlt,
            } => {
                let letter = dead_letter_event(&self.name, source, dlt, record, reason, &error);
                let published = match tokio::time::timeout(
                    OUTBOX_SEND_TIMEOUT,
                    events.publish(&letter),
                )
                .await
                {
                    Ok(published) => published,
                    Err(_) => Err(EventStoreError::Failed(format!(
                        "no answer within {} s",
                        OUTBOX_SEND_TIMEOUT.as_secs()
                    ))),
                };
                match published {
                    Ok(()) => Some(Consumed::Parked),
                    Err(e) => {
                        tracing::warn!(processor = %self.name, dlt = %dlt, "cannot publish to the dead-letter source, the record comes again: {e}");
                        self.monitor
                            .error(&format!("dead-letter source {dlt}: {e}"));
                        None
                    }
                }
            }
            Sink::Collection { store, retention } => {
                // Parked in a replay: its retry must be one too.
                let replay = self
                    .control
                    .replays(source, &record.segment, &record.position);
                let letter = crate::store::DeadLetter {
                    replay,
                    ..park(
                        DeadLetterRecord {
                            processor: self.name.clone(),
                            source: source.clone(),
                            segment: record.segment.clone(),
                            position: record.position.clone(),
                            key: record.key.clone(),
                            headers: record.headers.clone(),
                            payload: record.payload.clone(),
                        },
                        reason,
                        error,
                        blocked_by,
                        attempts,
                        now_ms(),
                        *retention,
                    )
                };
                match store.park(&letter).await {
                    Ok(parked) => {
                        let outcome = if parked == Parked::Already {
                            tracing::info!(processor = %self.name, letter = %letter.id, "record already parked");
                            Consumed::Duplicate
                        } else {
                            tracing::warn!(processor = %self.name, letter = %letter.id, reason = reason.as_str(), "record parked in dead_letters");
                            Consumed::Parked
                        };
                        if reason != DeadLetterReason::Invalid
                            && let Some(key) = &letter.key
                        {
                            self.parked.lock().unwrap().keys.insert(key.clone());
                        }
                        self.monitor.sample_dead_letters_soon();
                        Some(outcome)
                    }
                    Err(e) => {
                        tracing::warn!(processor = %self.name, letter = %letter.id, "cannot park the record, it comes again (unacknowledged): {e}");
                        self.monitor
                            .error(&format!("cannot park {}: {e}", letter.id));
                        None
                    }
                }
            }
        }
    }

    /// `countDocuments({processor})` at start, every [`DEAD_LETTERS_SAMPLE`] and after a park.
    async fn sample_dead_letters(
        self: Arc<Self>,
        store: Arc<dyn DeadLetterStore>,
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

impl Resubscribing for EventConsumer {
    fn subscribe(&self) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
        self.events.subscribe(self.subscription.clone())
    }

    fn read<'a>(
        &'a self,
        subscription: &'a mut dyn Subscription,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Ended> {
        Box::pin(EventConsumer::read(self, subscription, cancel))
    }

    fn failed(&self, failure: &EventStoreError, backoff: Duration) {
        // Its segments are gone with it: STARTING until the next one's assignment.
        let assigned = self.monitor.assigned_segments();
        self.monitor.revoked(&assigned);
        tracing::warn!(processor = %self.name, subscription = %self.subscription.name, "the subscription failed, subscribing again in {backoff:?}: {failure}");
        self.monitor.error(&format!("subscription: {failure}"));
    }
}

/// `segments` in [`segment_order`], for the logs.
fn in_order(segments: &[String]) -> Vec<&str> {
    let mut all: Vec<&str> = segments.iter().map(String::as_str).collect();
    all.sort_by(|a, b| segment_order(a, b));
    all
}

/// Whether `events` can take dead-letter `mode`: `event-store` needs the capability
/// `native-dead-letter`; `InvalidMessage` naming it otherwise.
pub(crate) fn check_dead_letter_mode(
    mode: DeadLetterMode,
    events: &dyn EventStore,
) -> Result<(), MessagingError> {
    if mode == DeadLetterMode::EventStore && !events.supports(Capability::NativeDeadLetter) {
        return Err(MessagingError::InvalidMessage(format!(
            "dead-letter mode event-store needs an event store with the capability {}: {} lacks it",
            Capability::NativeDeadLetter,
            events.name()
        )));
    }
    Ok(())
}

/// Hands `record` out again (its segment's later records wait behind it).
fn redeliver(subscription: &mut dyn Subscription, record: &Delivery, processor: &str) {
    if let Err(e) = subscription.redeliver(&record.segment, &record.position) {
        tracing::warn!(processor = %processor, segment = %record.segment, position = %record.position, "cannot hand an unacknowledged record out again: {e}");
    }
}

/// The reason [`classify_with`] gives for a type nobody handles: skipped, not a letter.
const NO_HANDLER: &str = "no handler for this type";
/// The headers mode `event-store` adds (names unchanged since they named Kafka's topic,
/// partition and offset: the source, segment and position); a record's own header of one of
/// these names is replaced.
pub const DLT_HEADERS: [&str; 6] = [
    "snodra-dlt-processor",
    "snodra-dlt-reason",
    "snodra-dlt-error",
    "snodra-dlt-original-topic",
    "snodra-dlt-original-partition",
    "snodra-dlt-original-offset",
];

/// The event mode `event-store` publishes to `dlt` for `record` of `source`: its key, payload
/// and headers, plus the [`DLT_HEADERS`].
fn dead_letter_event(
    processor: &str,
    source: &str,
    dlt: &str,
    record: &Delivery,
    reason: DeadLetterReason,
    error: &DeadLetterError,
) -> OutgoingEvent {
    let mut all: HashMap<String, String> = record
        .headers
        .iter()
        .filter(|(k, _)| !DLT_HEADERS.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let dlt_error = cut_chars(
        &format!("{}: {}", error.type_name, error.message),
        MAX_ERROR_MESSAGE_CHARS,
    );
    for (k, v) in DLT_HEADERS.into_iter().zip([
        processor,
        reason.as_str(),
        dlt_error.as_str(),
        source,
        record.segment.as_str(),
        record.position.as_str(),
    ]) {
        all.insert(k.to_string(), v.to_string());
    }
    let header = |name: &str| record.headers.get(name).cloned().unwrap_or_default();
    OutgoingEvent {
        source: dlt.to_string(),
        // No id (or a blank one): the record's position names it.
        id: record
            .headers
            .get(headers::ID)
            .filter(|id| !id.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| format!("{source}-{}-{}", record.segment, record.position)),
        type_name: header(headers::TYPE),
        key: record.key.clone(),
        headers: all,
        payload: record.payload.clone(),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn delivery() -> Delivery {
        Delivery {
            segment: "3".into(),
            position: "41".into(),
            key: Some("k".into()),
            headers: HashMap::from([
                ("type".to_string(), "t.Happened".to_string()),
                ("id".to_string(), "e-1".to_string()),
                ("snodra-dlt-reason".to_string(), "stale".to_string()),
            ]),
            payload: b"{}".to_vec(),
            timestamp_ms: 0,
        }
    }

    #[test]
    fn a_dead_letter_event_keeps_the_record_and_names_where_it_came_from() {
        let e = dead_letter_event(
            "orders",
            "snodra.events",
            "snodra.events.dlt",
            &delivery(),
            DeadLetterReason::HandlerFailed,
            &DeadLetterError::new("Storage", "down"),
        );
        assert_eq!(e.source, "snodra.events.dlt");
        assert_eq!((e.id.as_str(), e.type_name.as_str()), ("e-1", "t.Happened"));
        assert_eq!(e.key.as_deref(), Some("k"));
        assert_eq!(e.payload, b"{}");
        let h = |name: &str| e.headers[name].as_str();
        assert_eq!(h("type"), "t.Happened");
        assert_eq!(h("snodra-dlt-processor"), "orders");
        assert_eq!(
            h("snodra-dlt-reason"),
            "HANDLER_FAILED",
            "the record's own is replaced"
        );
        assert_eq!(h("snodra-dlt-error"), "Storage: down");
        assert_eq!(h("snodra-dlt-original-topic"), "snodra.events");
        assert_eq!(h("snodra-dlt-original-partition"), "3");
        assert_eq!(h("snodra-dlt-original-offset"), "41");
    }

    /// A record without an `id` header (or a blank one) gets `<source>-<segment>-<position>` as
    /// its dead-letter event's id; its headers stay as they were.
    #[test]
    fn a_dead_letter_event_of_a_record_without_an_id_is_named_by_its_position() {
        for id in [None, Some(" ")] {
            let mut record = delivery();
            match id {
                None => record.headers.remove("id"),
                Some(blank) => record.headers.insert("id".into(), blank.into()),
            };
            let e = dead_letter_event(
                "orders",
                "snodra.events",
                "snodra.events.dlt",
                &record,
                DeadLetterReason::Invalid,
                &DeadLetterError::new("Invalid", "no id"),
            );
            assert_eq!(e.id, "snodra.events-3-41", "{id:?}");
            assert_eq!(
                e.headers.get("id").map(String::as_str),
                id,
                "headers as they were"
            );
        }
    }

    /// What a scripted subscription does on each `next`.
    #[derive(Clone)]
    enum Turn {
        Assigned(&'static [&'static str]),
        /// Hands out the record at this position of segment `0`.
        Deliver(&'static str),
        Fail,
        Panic,
    }

    /// An event store whose subscriptions play a script each (the `n`th subscription the `n`th
    /// script; then nothing more comes), ignore `pause`, and whose publish (the dead-letter
    /// source's) fails while `publish_fails`. It records when each subscription was made.
    struct Scripted {
        scripts: Mutex<std::collections::VecDeque<Vec<Turn>>>,
        subscribed: Arc<Mutex<Vec<tokio::time::Instant>>>,
        publish_fails: AtomicBool,
    }

    impl Scripted {
        fn new(scripts: Vec<Vec<Turn>>) -> Arc<Scripted> {
            Arc::new(Scripted {
                scripts: Mutex::new(scripts.into()),
                subscribed: Arc::default(),
                publish_fails: AtomicBool::new(true),
            })
        }
    }

    impl EventStore for Scripted {
        fn name(&self) -> &'static str {
            "scripted"
        }
        fn capabilities(&self) -> &[Capability] {
            &[Capability::NativeDeadLetter]
        }
        fn publish<'a>(
            &'a self,
            _: &'a OutgoingEvent,
        ) -> BoxFuture<'a, Result<(), EventStoreError>> {
            let fails = self.publish_fails.load(Ordering::SeqCst);
            Box::pin(async move {
                if fails {
                    Err(EventStoreError::Failed(
                        "the dead-letter source is down".into(),
                    ))
                } else {
                    Ok(())
                }
            })
        }
        fn subscribe(
            &self,
            _: SubscriptionSpec,
        ) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
            self.subscribed
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
            Box::pin(async move {
                Ok(Box::new(Played {
                    turns: script.into(),
                }) as Box<dyn Subscription>)
            })
        }
        fn admin(
            &self,
            _: &SubscriptionSpec,
        ) -> Result<Box<dyn crate::event_store::SubscriptionAdmin>, EventStoreError> {
            Err(EventStoreError::Unsupported(Capability::Replay))
        }
        fn compare(&self, a: &str, b: &str) -> Order {
            a.cmp(b)
        }
    }

    struct Played {
        turns: std::collections::VecDeque<Turn>,
    }

    impl Subscription for Played {
        /// Cancel-safe: the turn is taken when the future is polled, and it is ready then.
        fn next(&mut self) -> BoxFuture<'_, Result<SubscriptionEvent, EventStoreError>> {
            Box::pin(async move {
                match self.turns.pop_front() {
                    Some(Turn::Assigned(segments)) => Ok(SubscriptionEvent::Assigned(
                        segments.iter().map(|s| s.to_string()).collect(),
                    )),
                    Some(Turn::Deliver(position)) => Ok(SubscriptionEvent::Delivery(Delivery {
                        segment: "0".into(),
                        position: position.into(),
                        ..delivery()
                    })),
                    Some(Turn::Fail) => Err(EventStoreError::Failed("the script broke".into())),
                    Some(Turn::Panic) => panic!("the script panicked"),
                    None => std::future::pending().await,
                }
            })
        }
        fn ack(&mut self, _: &str, _: &str) -> Result<(), EventStoreError> {
            Ok(())
        }
        fn redeliver(&mut self, _: &str, _: &str) -> Result<(), EventStoreError> {
            Ok(())
        }
        fn pause(&mut self) {}
        fn resume(&mut self) {}
        fn segments(&self) -> Vec<crate::event_store::SegmentStatus> {
            Vec::new()
        }
        fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Happened {}
    crate::message_type!(Happened, "t.Happened");
    impl crate::types::Event for Happened {}

    /// A consumer of `events` in mode `event-store`, whose one handler fails every time (counted
    /// in `runs`), one attempt per record; its control.
    fn consumer(
        events: &Arc<Scripted>,
        runs: &Arc<std::sync::atomic::AtomicU32>,
    ) -> (Arc<EventConsumer>, Arc<ConsumerControl>) {
        let events: Arc<dyn EventStore> = Arc::clone(events) as Arc<dyn EventStore>;
        let mut registry = crate::hub::HandlerRegistry::new();
        let counted = Arc::clone(runs);
        registry.event::<Happened, _, _>(move |_, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Err(MessagingError::Storage("the handler fails".into())) }
        });
        let store: Arc<dyn crate::store::MessageStore> =
            Arc::new(crate::store::memory::MemoryStore::new());
        let dispatcher = Arc::new(LocalEventDispatcher::new(
            Some(store),
            Arc::new(registry),
            Arc::new(super::super::NoSagas),
        ));
        let settings = ProcessorSettings {
            name: "default".into(),
            subscription: SubscriptionSpec {
                name: "svc.default".into(),
                source: "snodra.events".into(),
            },
            events: Arc::clone(&events),
            max_attempts: 1,
            backoff: Duration::from_millis(10),
            dead_letter_mode: DeadLetterMode::EventStore,
            dead_letter_retention: Duration::from_secs(60),
            dead_letters: None,
            metrics: Metrics::global(),
        };
        let monitor = Arc::new(ProcessorMonitor::new(
            "default",
            &settings.subscription,
            &events,
            DeadLetterMode::EventStore,
            vec!["t.Happened".into()],
        ));
        let control = Arc::new(ConsumerControl::new(Arc::clone(&events)));
        let consumer =
            EventConsumer::controlled(settings, dispatcher, monitor, Arc::clone(&control)).unwrap();
        (consumer, control)
    }

    async fn eventually(what: &str, check: impl Fn() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        while !check() {
            assert!(tokio::time::Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// When a subscription ends, what its records left pending ends with it: a record whose
    /// dead letter could not be written is handled again (its handlers run) by the next
    /// subscription, as on another instance, not sent straight to the dead letters.
    #[tokio::test(start_paused = true)]
    async fn a_pending_letter_is_forgotten_with_its_subscription() {
        let events = Scripted::new(vec![
            vec![Turn::Assigned(&["0"]), Turn::Deliver("1"), Turn::Fail],
            vec![Turn::Assigned(&["0"]), Turn::Deliver("1")],
        ]);
        let runs = Arc::default();
        let (consumer, _control) = consumer(&events, &runs);
        let cancel = CancellationToken::new();
        let task = consumer.start(cancel.clone());
        eventually("subscribed again", || {
            events.subscribed.lock().unwrap().len() == 2
        })
        .await;
        eventually("the handlers ran again", || {
            runs.load(Ordering::SeqCst) == 2
        })
        .await;
        cancel.cancel();
        task.await.unwrap();
    }

    /// A delivery handed back because the processor is paused is no delivery for the
    /// resubscribe backoff: subscriptions that only hand back records and fail keep backing off.
    #[tokio::test(start_paused = true)]
    async fn a_delivery_handed_back_for_a_pause_does_not_reset_the_backoff() {
        let broken = || vec![Turn::Assigned(&["0"]), Turn::Deliver("1"), Turn::Fail];
        let events = Scripted::new(vec![broken(), broken(), broken(), broken()]);
        let runs = Arc::default();
        let (consumer, control) = consumer(&events, &runs);
        control.set_paused(true);
        let cancel = CancellationToken::new();
        let task = consumer.start(cancel.clone());
        eventually("four subscriptions", || {
            events.subscribed.lock().unwrap().len() == 4
        })
        .await;
        let at = events.subscribed.lock().unwrap().clone();
        let waits: Vec<u64> = at.windows(2).map(|w| (w[1] - w[0]).as_secs()).collect();
        assert_eq!(waits, [1, 2, 4], "it keeps backing off");
        assert_eq!(runs.load(Ordering::SeqCst), 0, "paused: nothing handled");
        cancel.cancel();
        task.await.unwrap();
    }

    /// A reader ended by a panic leaves the processor STOPPED with the reason, never RUNNING
    /// (or STARTING) while nothing reads.
    #[tokio::test(start_paused = true)]
    async fn a_reader_ended_by_a_panic_reads_stopped_with_the_reason() {
        let events = Scripted::new(vec![vec![Turn::Assigned(&["0"]), Turn::Panic]]);
        let runs = Arc::default();
        let (consumer, _control) = consumer(&events, &runs);
        let monitor = consumer.monitor();
        let task = consumer.start(CancellationToken::new());
        let ended = task.await;
        assert!(ended.is_ok(), "the panic is the reader's, not the task's");
        let status = monitor.status();
        assert_eq!(
            crate::proto::ProcessorState::try_from(status.state).unwrap(),
            crate::proto::ProcessorState::ProcessorStopped
        );
        assert!(
            status.last_error.contains("the script panicked"),
            "{}",
            status.last_error
        );
    }

    #[test]
    fn replay_ranges_compare_positions_by_the_event_stores_order() {
        let control = ConsumerControl::new(crate::processors::monitor::tests::decimal());
        control.set_replay(vec![ReplayRange {
            source: "s".into(),
            segment: "1".into(),
            from: "9".into(),
            until: "11".into(),
        }]);
        assert!(!control.replays("s", "1", "8"));
        assert!(control.replays("s", "1", "9"));
        assert!(
            control.replays("s", "1", "10"),
            "10 is after 9, though not as text"
        );
        assert!(!control.replays("s", "1", "11"));
        assert!(!control.replays("s", "2", "10"));
        assert!(!control.replays("t", "1", "10"));
    }
}
