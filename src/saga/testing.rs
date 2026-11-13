//! Testing a saga without Kafka or a database (S2-R6, feature `testing`): [`SagaTestFixture`] drives
//! one saga definition with events and time over the crate's own [`SagaRuntime`] and the
//! in-memory ports of [`InMemorySagaStore`]. The Rust form of the Kotlin
//! `kraken-messaging-test` `SagaTestFixture`.
//!
//! ```ignore
//! SagaTestFixture::new("examples.Payment", payment_saga) // the closure given to builder.saga(..)
//!     .given_current_time(t0)
//!     .when_event(&OrderPlaced { order_id: "o1".into(), amount: 42 })
//!     .expect_active_sagas(1)
//!     .expect_association("paymentRef", "pay-o1")
//!     .expect_dispatched_commands(&[&RequestCharge { payment_ref: "pay-o1".into(), amount: 42 }])
//!     .expect_scheduled_deadline("charge-timeout", t0 + Duration::from_secs(15 * 60));
//! ```

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::clock::ManualClock;
use super::definition::SagaDefinition;
use super::failed::{FailureReason, SagaCommandFailed};
use super::metrics::NoopSagaMetrics;
use super::ports::{
    DeadlineStore, DispatchFailure, FailureStore, MarkerStore, SagaDispatchStore, SagaPorts,
    SagaRepository,
};
use super::retry::SagaRetry;
use super::runtime::SagaRuntime;
use super::store::{
    Association, DeadlineDocument, DeadlineStatus, DispatchKind, SagaDispatchDocument,
    SagaDocument, SagaFailureDocument,
};
use crate::context::MessageContext;
use crate::kafka::SagaStage;
use crate::retry::RetryPolicy;
use crate::store::{StoreError, Tx, TxProvider};
use crate::types::{Command, Event, MessageType};

/// An event as the fixture takes it: any [`Event`], behind `&dyn` so one slice holds several
/// types (`&[&OrderPlaced { .. }, &ChargeSucceeded { .. }]`).
pub trait FixtureEvent {
    fn wire_type(&self) -> &'static str;
    fn json(&self) -> Value;
}

impl<E: Event> FixtureEvent for E {
    fn wire_type(&self) -> &'static str {
        E::TYPE
    }

    fn json(&self) -> Value {
        to_json(self, E::TYPE)
    }
}

/// A command as the fixture's expectations take it: any [`Command`], behind `&dyn`.
pub trait FixtureCommand {
    fn wire_type(&self) -> &'static str;
    fn json(&self) -> Value;
}

impl<C: Command> FixtureCommand for C {
    fn wire_type(&self) -> &'static str {
        C::TYPE
    }

    fn json(&self) -> Value {
        to_json(self, C::TYPE)
    }
}

fn to_json<M: Serialize>(message: &M, type_name: &str) -> Value {
    serde_json::to_value(message)
        .unwrap_or_else(|e| panic!("the fixture cannot encode the {type_name} it was given: {e}"))
}

/// Drives one saga definition with events and time, in memory: no Kafka, no database, no hub.
///
/// It runs the crate's own [`SagaRuntime`] (the same association lookup, start keys,
/// exactly-once markers, version checks and deadline firing) over [`InMemorySagaStore`], so a saga
/// that behaves here behaves the same on a real store. The state is stored as its JSON text as in
/// production: every step decodes it before the handlers run and encodes it after, so state that
/// `serde_json` cannot carry fails the step instead of passing in memory. A step is atomic: a
/// handler that fails leaves nothing behind (the in-memory transaction rolls back).
///
/// Steps **panic** when the saga fails (a handler returned an error or panicked, the state is not
/// JSON-serialisable, a deadline handler failed), at once and without retries; expectations panic
/// saying what was expected and what was found. Both name the saga. Every method returns the
/// fixture, so a test reads as one chain.
///
/// Time is the fixture's own clock, set by [`given_current_time`](Self::given_current_time)
/// (default 2026-01-01T00:00:00Z). [`when_time_elapses`](Self::when_time_elapses) advances it and
/// fires every deadline that falls due, each at its own due time and in due order, so a deadline
/// that reschedules itself (the polling pattern) fires once per interval.
///
/// What the dispatch expectations see is what the saga sent since the last `given_*` step: the
/// commands and events of all `when_*` steps after it, in order.
///
/// The fixture's methods are synchronous (they drive the runtime with a local executor): call
/// them from a plain `#[test]`, or from an async test without holding the fixture across an
/// `.await`.
pub struct SagaTestFixture<S> {
    name: &'static str,
    runtime: SagaRuntime,
    store: Arc<InMemorySagaStore>,
    clock: ManualClock,
    observed_from: usize,
    state: PhantomData<fn() -> S>,
}

/// The fixture's clock before [`SagaTestFixture::given_current_time`]: 2026-01-01T00:00:00Z.
pub const DEFAULT_TIME_SECS: u64 = 1_767_225_600;

/// The most deadlines one `when_time_elapses` fires: more means a deadline keeps rescheduling
/// itself without time moving.
const MAX_DEADLINES_PER_STEP: usize = 10_000;

impl<S: Serialize + DeserializeOwned + Default + Send + 'static> SagaTestFixture<S> {
    /// A fixture for the saga `name` defined by `define`, the closure a service passes to
    /// [`KrakenMessagingBuilder::saga`](crate::KrakenMessagingBuilder::saga).
    ///
    /// # Panics
    /// When the definition fails the startup checks (S2-R2), with their message.
    pub fn new(
        name: &'static str,
        define: impl FnOnce(SagaDefinition<S>) -> SagaDefinition<S>,
    ) -> SagaTestFixture<S> {
        let saga_type = define(SagaDefinition::new(name))
            .build()
            .unwrap_or_else(|e| panic!("Saga {name} cannot be registered: {e}"));
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(DEFAULT_TIME_SECS));
        let store = Arc::new(InMemorySagaStore::default());
        // One attempt, no backoff: a test sees a failure at once.
        let retry = SagaRetry::new(RetryPolicy {
            max_attempts: 1,
            initial_backoff: Duration::ZERO,
            multiplier: 1.0,
            max_backoff: Duration::ZERO,
        });
        let runtime = SagaRuntime::new(
            vec![saga_type],
            store.ports(),
            clock.clock(),
            retry,
            Arc::new(NoopSagaMetrics),
        );
        SagaTestFixture {
            name,
            runtime,
            store,
            clock,
            observed_from: 0,
            state: PhantomData,
        }
    }

    // ---- given -------------------------------------------------------------------------------

    /// Delivers `events` to the saga without observing what they cause: the expectations start
    /// after them.
    pub fn given_events(&mut self, events: &[&dyn FixtureEvent]) -> &mut Self {
        for event in events {
            self.deliver(event.wire_type(), &event.json(), &[], None);
        }
        self.observed_from = self.store.data().dispatch.len();
        self
    }

    /// Sets the fixture's clock to `now`; deadlines are scheduled against it.
    pub fn given_current_time(&mut self, now: SystemTime) -> &mut Self {
        self.clock.set(now);
        self.observed_from = self.store.data().dispatch.len();
        self
    }

    // ---- when --------------------------------------------------------------------------------

    /// Delivers `event` to the saga.
    pub fn when_event<E: Event>(&mut self, event: &E) -> &mut Self {
        self.when_event_with_headers(event, &[])
    }

    /// Delivers `event` with message `headers` (what `start_on_header`/`on_header` and
    /// `ctx.message().headers` see).
    pub fn when_event_with_headers<E: Event>(
        &mut self,
        event: &E,
        headers: &[(&str, &str)],
    ) -> &mut Self {
        self.deliver(E::TYPE, &FixtureEvent::json(event), headers, None);
        self
    }

    /// Tells the saga that `command`, which it sent, failed with `reason`, as the dispatch relay
    /// does: delivers a [`SagaCommandFailed`] for the instance that sent it, with its idempotency
    /// key and the relay's dedupe id `failed:<sagaId>:<seq>`. `command` is matched by type and
    /// JSON payload against everything the saga recorded (the last such entry when several are
    /// equal).
    ///
    /// # Panics
    /// When the saga never sent such a command.
    pub fn when_command_failed<C: Command>(
        &mut self,
        command: &C,
        reason: FailureReason,
        message: &str,
    ) -> &mut Self {
        let wanted = FixtureCommand::json(command);
        let sent = {
            let data = self.store.data();
            let commands: Vec<&SagaDispatchDocument> = data
                .dispatch
                .iter()
                .filter(|d| d.kind == DispatchKind::Command)
                .collect();
            match commands
                .iter()
                .rev()
                .find(|d| d.type_name == C::TYPE && parse(&d.payload) == wanted)
            {
                Some(sent) => (*sent).clone(),
                None => panic!(
                    "{}: no saga sent {} {wanted}, so it cannot have failed; the commands sent are {}",
                    self.name,
                    C::TYPE,
                    show(
                        commands
                            .iter()
                            .map(|d| (d.type_name.clone(), parse(&d.payload)))
                    )
                ),
            }
        };
        let failed = SagaCommandFailed {
            saga_id: sent.saga_id.clone(),
            command_type: sent.type_name.clone(),
            idempotency_key: sent.id.clone(),
            reason,
            message: message.to_string(),
        };
        let payload = serde_json::to_value(&failed).expect("SagaCommandFailed encodes");
        self.deliver(
            SagaCommandFailed::TYPE,
            &payload,
            &[],
            Some(format!("failed:{}", sent.id)),
        );
        self
    }

    /// Advances the clock by `duration`, firing the deadlines that fall due on the way, each at
    /// its due time.
    ///
    /// # Panics
    /// When a deadline handler fails, or more than 10 000 deadlines fall due in one step.
    pub fn when_time_elapses(&mut self, duration: Duration) -> &mut Self {
        let target = self.clock.now() + duration;
        let target_ms = crate::time::epoch_ms(target);
        let mut fired = 0usize;
        loop {
            let next = {
                let data = self.store.data();
                data.deadlines
                    .iter()
                    .filter(|d| d.status == DeadlineStatus::Scheduled && d.due_at_ms <= target_ms)
                    .min_by_key(|d| d.due_at_ms)
                    .cloned()
            };
            let Some(next) = next else { break };
            fired += 1;
            assert!(
                fired <= MAX_DEADLINES_PER_STEP,
                "{}: more than {MAX_DEADLINES_PER_STEP} deadlines fell due in one step: a deadline keeps rescheduling itself",
                self.name
            );
            let due = std::time::UNIX_EPOCH
                + Duration::from_millis(u64::try_from(next.due_at_ms).unwrap_or(0));
            if due > self.clock.now() {
                self.clock.set(due);
            }
            if let Err(e) = futures::executor::block_on(self.runtime.apply_deadline(&next)) {
                panic!(
                    "Saga {} failed firing its deadline '{}': {e}",
                    self.name, next.name
                );
            }
        }
        self.clock.set(target);
        self
    }

    // ---- expect ------------------------------------------------------------------------------

    /// The number of live (not ended) instances is `count`.
    pub fn expect_active_sagas(&mut self, count: usize) -> &mut Self {
        let live = self.store.data().sagas.iter().filter(|s| !s.ended).count();
        if live != count {
            self.fail(format!("expected {count} active sagas but found {live}"));
        }
        self
    }

    /// Some live instance is associated with `key = value`.
    pub fn expect_association(&mut self, key: &str, value: &str) -> &mut Self {
        let wanted = Association::new(key, value);
        let found: Vec<Vec<Association>> = self
            .store
            .data()
            .sagas
            .iter()
            .filter(|s| !s.ended)
            .map(|s| s.associations.clone())
            .collect();
        if !found.iter().any(|a| a.contains(&wanted)) {
            let found: Vec<String> = found
                .iter()
                .map(|a| {
                    let pairs: Vec<String> = a.iter().map(|a| format!("{}={}", a.k, a.v)).collect();
                    format!("[{}]", pairs.join(", "))
                })
                .collect();
            self.fail(format!(
                "expected a live saga associated with {key}={value} but found [{}]",
                found.join(", ")
            ));
        }
        self
    }

    /// Exactly these commands were sent, in this order (compared by wire type and JSON payload);
    /// none for `&[]`.
    pub fn expect_dispatched_commands(&mut self, commands: &[&dyn FixtureCommand]) -> &mut Self {
        let wanted = commands
            .iter()
            .map(|c| (c.wire_type().to_string(), c.json()));
        self.expect_dispatched(DispatchKind::Command, "commands", wanted.collect())
    }

    /// Exactly these events were published, in this order (compared by wire type and JSON
    /// payload); none for `&[]`.
    pub fn expect_published_events(&mut self, events: &[&dyn FixtureEvent]) -> &mut Self {
        let wanted = events.iter().map(|e| (e.wire_type().to_string(), e.json()));
        self.expect_dispatched(DispatchKind::Event, "events", wanted.collect())
    }

    /// A deadline `name` is scheduled to fire at exactly `at` (to the millisecond, as stored).
    pub fn expect_scheduled_deadline(&mut self, name: &str, at: SystemTime) -> &mut Self {
        let at = crate::time::epoch_ms(at);
        let scheduled = self.scheduled();
        if !scheduled
            .iter()
            .any(|d| d.name == name && d.due_at_ms == at)
        {
            self.fail(format!(
                "expected a deadline '{name}' at {} but found {}",
                crate::time::rfc3339(at),
                describe_deadlines(&scheduled)
            ));
        }
        self
    }

    /// No deadline is scheduled any more (every one fired or was cancelled).
    pub fn expect_no_scheduled_deadlines(&mut self) -> &mut Self {
        let scheduled = self.scheduled();
        if !scheduled.is_empty() {
            self.fail(format!(
                "expected no scheduled deadlines but found {}",
                describe_deadlines(&scheduled)
            ));
        }
        self
    }

    /// The saga ended: at least one instance was started and none is live.
    pub fn expect_saga_ended(&mut self) -> &mut Self {
        let (all, live) = {
            let data = self.store.data();
            (
                data.sagas.len(),
                data.sagas.iter().filter(|s| !s.ended).count(),
            )
        };
        if all == 0 {
            self.fail("expected the saga to have ended but no instance was ever started".into());
        }
        if live > 0 {
            self.fail(format!(
                "expected the saga to have ended but {live} of {all} instances are not ended"
            ));
        }
        self
    }

    /// The store the fixture runs on, for assertions the expectations do not cover (the stored
    /// documents, their state text).
    pub fn store(&self) -> &InMemorySagaStore {
        &self.store
    }

    // ---- internals ---------------------------------------------------------------------------

    fn deliver(
        &mut self,
        type_name: &str,
        payload: &Value,
        headers: &[(&str, &str)],
        message_id: Option<String>,
    ) {
        if !self.runtime.interested(type_name) {
            return;
        }
        let message_id = message_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let ctx = MessageContext {
            message_id: message_id.clone(),
            type_name: type_name.to_string(),
            correlation_id: message_id,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        };
        let payload = serde_json::to_vec(payload).expect("a JSON value encodes");
        if let Err(e) = futures::executor::block_on(self.runtime.process(type_name, &payload, &ctx))
        {
            panic!("Saga {} failed handling {type_name}: {e}", self.name);
        }
        // The runtime parks an event whose handler keeps failing and moves on; a test must see
        // that as a failure.
        let parked = std::mem::take(&mut self.store.data().failures);
        if let Some(failure) = parked.into_iter().next() {
            panic!(
                "Saga {} failed handling {type_name}: {}. A step fails when a handler returns an \
                 error or panics, or when the saga state cannot be written to or read from JSON \
                 (the library stores a saga as its serde_json text).",
                failure.saga_type, failure.error
            );
        }
    }

    fn expect_dispatched(
        &mut self,
        kind: DispatchKind,
        label: &str,
        wanted: Vec<(String, Value)>,
    ) -> &mut Self {
        let found: Vec<(String, Value)> = self
            .store
            .data()
            .dispatch
            .iter()
            .skip(self.observed_from)
            .filter(|d| d.kind == kind)
            .map(|d| (d.type_name.clone(), parse(&d.payload)))
            .collect();
        if found != wanted {
            let found = show(found.into_iter());
            if wanted.is_empty() {
                self.fail(format!("expected no {label} but found {found}"));
            } else {
                self.fail(format!(
                    "expected {label} {} but found {found}",
                    show(wanted.into_iter())
                ));
            }
        }
        self
    }

    fn scheduled(&self) -> Vec<DeadlineDocument> {
        self.store
            .data()
            .deadlines
            .iter()
            .filter(|d| d.status == DeadlineStatus::Scheduled)
            .cloned()
            .collect()
    }

    fn fail(&self, message: String) -> ! {
        panic!("{}: {message}", self.name)
    }
}

fn parse(payload: &str) -> Value {
    serde_json::from_str(payload).unwrap_or_else(|_| Value::String(payload.to_string()))
}

fn show(messages: impl Iterator<Item = (String, Value)>) -> String {
    let shown: Vec<String> = messages.map(|(t, v)| format!("{t} {v}")).collect();
    format!("[{}]", shown.join(", "))
}

fn describe_deadlines(deadlines: &[DeadlineDocument]) -> String {
    if deadlines.is_empty() {
        return "none".to_string();
    }
    let shown: Vec<String> = deadlines
        .iter()
        .map(|d| format!("'{}' at {}", d.name, crate::time::rfc3339(d.due_at_ms)))
        .collect();
    format!("[{}]", shown.join(", "))
}

// ---- the in-memory ports ---------------------------------------------------------------------

/// Neither dispatched nor failed.
fn pending(e: &SagaDispatchDocument) -> bool {
    e.dispatched_at_ms.is_none() && e.failed_at_ms.is_none()
}

/// What [`InMemorySagaStore`] holds: the documents of the saga collections, in insertion order.
#[derive(Clone, Debug, Default)]
pub struct InMemoryData {
    pub sagas: Vec<SagaDocument>,
    pub dispatch: Vec<SagaDispatchDocument>,
    pub deadlines: Vec<DeadlineDocument>,
    pub failures: Vec<SagaFailureDocument>,
    pub markers: HashSet<String>,
    /// The dispatch leases: saga id to (owner, until epoch ms).
    pub leases: std::collections::HashMap<String, (String, i64)>,
}

/// The saga persistence ports in memory, and their transaction: [`TxProvider::begin`] takes a
/// snapshot that [`TxProvider::abort`] restores, as a rolled-back store transaction would (the
/// exactly-once marker included, so a failing event is parked, not skipped). One transaction at a
/// time: the store is for sequential tests (the fixture), not for concurrent writers.
#[derive(Default)]
pub struct InMemorySagaStore {
    data: Mutex<InMemoryData>,
    snapshot: Mutex<Option<InMemoryData>>,
}

impl InMemorySagaStore {
    /// The six ports over this store.
    pub fn ports(self: &Arc<Self>) -> SagaPorts {
        SagaPorts {
            tx: self.clone(),
            sagas: self.clone(),
            dispatch: self.clone(),
            deadlines: self.clone(),
            failures: self.clone(),
            markers: self.clone(),
        }
    }

    /// The documents, locked for reading (or changing) them.
    pub fn data(&self) -> MutexGuard<'_, InMemoryData> {
        self.data.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn update_entry(&self, id: &str, change: impl FnOnce(&mut SagaDispatchDocument)) {
        if let Some(e) = self.data().dispatch.iter_mut().find(|e| e.id == id) {
            change(e);
        }
    }

    fn cancel_where(&self, now: i64, matches: impl Fn(&DeadlineDocument) -> bool) {
        for d in self.data().deadlines.iter_mut() {
            if d.status == DeadlineStatus::Scheduled && matches(d) {
                d.status = DeadlineStatus::Cancelled;
                d.finished_at_ms = Some(now);
            }
        }
    }
}

fn ready<'a, T: Send + 'a>(value: Result<T, StoreError>) -> BoxFuture<'a, Result<T, StoreError>> {
    Box::pin(std::future::ready(value))
}

impl TxProvider for InMemorySagaStore {
    fn begin(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        let copy = self.data().clone();
        *self.snapshot.lock().unwrap_or_else(|p| p.into_inner()) = Some(copy);
        ready(Ok(Tx::none()))
    }

    fn plain(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        ready(Ok(Tx::none()))
    }

    fn commit<'a>(&'a self, _tx: &'a mut Tx) -> BoxFuture<'a, Result<(), StoreError>> {
        self.snapshot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        ready(Ok(()))
    }

    fn abort<'a>(&'a self, _tx: &'a mut Tx) -> BoxFuture<'a, ()> {
        let before = self
            .snapshot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(before) = before {
            *self.data() = before;
        }
        Box::pin(std::future::ready(()))
    }
}

impl SagaRepository for InMemorySagaStore {
    fn find_by_id<'a>(
        &'a self,
        _tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDocument>, StoreError>> {
        ready(Ok(self.data().sagas.iter().find(|s| s.id == id).cloned()))
    }

    fn find_live<'a>(
        &'a self,
        _tx: &'a mut Tx,
        type_name: &'a str,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<Vec<SagaDocument>, StoreError>> {
        let wanted = Association::new(key, value);
        ready(Ok(self
            .data()
            .sagas
            .iter()
            .filter(|s| s.type_name == type_name && !s.ended && s.associations.contains(&wanted))
            .cloned()
            .collect()))
    }

    fn insert<'a>(
        &'a self,
        _tx: &'a mut Tx,
        saga: &'a SagaDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let mut data = self.data();
        if data.sagas.iter().any(|s| s.id == saga.id) {
            return ready(Err(StoreError::Conflict(format!(
                "saga {} exists",
                saga.id
            ))));
        }
        if let Some(key) = &saga.start_key
            && data
                .sagas
                .iter()
                .any(|s| !s.ended && s.start_key.as_ref() == Some(key))
        {
            return ready(Err(StoreError::Conflict(format!(
                "a live saga holds the start key {key}"
            ))));
        }
        data.sagas.push(saga.clone());
        ready(Ok(()))
    }

    fn update<'a>(
        &'a self,
        _tx: &'a mut Tx,
        saga: &'a SagaDocument,
        expected_version: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.data();
        let updated = match data.sagas.iter_mut().find(|s| s.id == saga.id) {
            Some(stored) if stored.version == expected_version => {
                *stored = saga.clone();
                true
            }
            _ => false,
        };
        ready(Ok(updated))
    }

    fn count_live_by_type(
        &self,
    ) -> BoxFuture<'_, Result<std::collections::HashMap<String, u64>, StoreError>> {
        let mut counts = std::collections::HashMap::new();
        for saga in self.data().sagas.iter().filter(|s| !s.ended) {
            *counts.entry(saga.type_name.clone()).or_insert(0) += 1;
        }
        ready(Ok(counts))
    }
}

impl SagaDispatchStore for InMemorySagaStore {
    fn record<'a>(
        &'a self,
        _tx: &'a mut Tx,
        entries: &'a [SagaDispatchDocument],
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        // What the outbox would refuse, as every store refuses it.
        for e in entries.iter().filter(|e| e.kind == DispatchKind::Event) {
            if let Err(refused) =
                crate::kafka::check_payload_text(&e.type_name, e.payload.as_bytes())
            {
                return ready(Err(StoreError::Failed(refused)));
            }
        }
        self.data().dispatch.extend_from_slice(entries);
        ready(Ok(()))
    }

    fn due_sagas<'a>(
        &'a self,
        now_ms: i64,
        except: &'a [String],
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<String>, StoreError>> {
        let data = self.data();
        let mut first: std::collections::HashMap<&str, &SagaDispatchDocument> =
            std::collections::HashMap::new();
        for e in data.dispatch.iter().filter(|e| pending(e)) {
            let slot = first.entry(e.saga_id.as_str()).or_insert(e);
            if e.seq < slot.seq {
                *slot = e;
            }
        }
        let mut due: Vec<&SagaDispatchDocument> = first
            .into_values()
            .filter(|e| !except.contains(&e.saga_id))
            .filter(|e| e.next_attempt_at_ms.is_none_or(|next| next <= now_ms))
            .collect();
        due.sort_by(|a, b| (a.created_at_ms, &a.saga_id).cmp(&(b.created_at_ms, &b.saga_id)));
        ready(Ok(due
            .into_iter()
            .take(limit)
            .map(|e| e.saga_id.clone())
            .collect()))
    }

    fn next_pending<'a>(
        &'a self,
        saga_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>> {
        ready(Ok(self
            .data()
            .dispatch
            .iter()
            .filter(|e| e.saga_id == saga_id && pending(e))
            .min_by_key(|e| e.seq)
            .cloned()))
    }

    fn mark_dispatched<'a>(
        &'a self,
        _tx: &'a mut Tx,
        id: &'a str,
        now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.update_entry(id, |e| e.dispatched_at_ms = Some(now_ms));
        ready(Ok(()))
    }

    fn schedule_retry<'a>(
        &'a self,
        id: &'a str,
        attempts: i32,
        next_attempt_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.update_entry(id, |e| {
            e.attempts = attempts;
            e.next_attempt_at_ms = Some(next_attempt_at_ms);
        });
        ready(Ok(()))
    }

    fn mark_failed<'a>(
        &'a self,
        id: &'a str,
        failure: &'a DispatchFailure,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.update_entry(id, |e| {
            e.failed_at_ms = Some(failure.failed_at_ms);
            e.failure_reason = Some(failure.reason);
            e.failure_message = Some(failure.message.clone());
            if let Some(attempts) = failure.attempts {
                e.attempts = attempts;
            }
            if let Some(at) = failure.notice_at_ms {
                e.notice_pending = Some(true);
                e.notice_attempts = Some(0);
                e.notice_next_at_ms = Some(at);
            }
        });
        ready(Ok(()))
    }

    fn claim_due_notice<'a>(
        &'a self,
        now_ms: i64,
        reserve_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<SagaDispatchDocument>, StoreError>> {
        let mut data = self.data();
        let claimed = data
            .dispatch
            .iter_mut()
            .filter(|e| {
                e.notice_pending == Some(true) && e.notice_next_at_ms.is_none_or(|at| at <= now_ms)
            })
            .min_by_key(|e| e.notice_next_at_ms)
            .map(|e| {
                e.notice_next_at_ms = Some(reserve_until_ms);
                e.clone()
            });
        ready(Ok(claimed))
    }

    fn clear_notice<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), StoreError>> {
        self.update_entry(id, |e| {
            e.notice_pending = None;
            e.notice_attempts = None;
            e.notice_next_at_ms = None;
        });
        ready(Ok(()))
    }

    fn schedule_notice_retry<'a>(
        &'a self,
        id: &'a str,
        notice_attempts: i32,
        next_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.update_entry(id, |e| {
            e.notice_attempts = Some(notice_attempts);
            e.notice_next_at_ms = Some(next_at_ms);
        });
        ready(Ok(()))
    }

    fn claim_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        now_ms: i64,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.data();
        let free = data
            .leases
            .get(saga_id)
            .is_none_or(|(_, until)| *until < now_ms);
        if free {
            data.leases
                .insert(saga_id.to_string(), (owner.to_string(), until_ms));
        }
        ready(Ok(free))
    }

    fn renew_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
        until_ms: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.data();
        let renewed = match data.leases.get_mut(saga_id) {
            Some((holder, until)) if holder == owner => {
                *until = until_ms;
                true
            }
            _ => false,
        };
        ready(Ok(renewed))
    }

    fn release_lease<'a>(
        &'a self,
        saga_id: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let mut data = self.data();
        if data
            .leases
            .get(saga_id)
            .is_some_and(|(holder, _)| holder == owner)
        {
            data.leases.remove(saga_id);
        }
        ready(Ok(()))
    }

    fn pending(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        ready(Ok(
            self.data().dispatch.iter().filter(|e| pending(e)).count() as u64,
        ))
    }
}

impl DeadlineStore for InMemorySagaStore {
    fn insert<'a>(
        &'a self,
        _tx: &'a mut Tx,
        deadline: &'a DeadlineDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let mut data = self.data();
        data.deadlines.retain(|d| d.id != deadline.id);
        data.deadlines.push(deadline.clone());
        ready(Ok(()))
    }

    fn find_scheduled<'a>(
        &'a self,
        _tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>> {
        ready(Ok(self
            .data()
            .deadlines
            .iter()
            .find(|d| d.id == id && d.status == DeadlineStatus::Scheduled)
            .cloned()))
    }

    fn cancel<'a>(
        &'a self,
        _tx: &'a mut Tx,
        saga_id: &'a str,
        deadline_id: &'a str,
        now: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.cancel_where(now, |d| d.saga_id == saga_id && d.id == deadline_id);
        ready(Ok(()))
    }

    fn cancel_by_name<'a>(
        &'a self,
        _tx: &'a mut Tx,
        saga_id: &'a str,
        name: &'a str,
        now: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.cancel_where(now, |d| d.saga_id == saga_id && d.name == name);
        ready(Ok(()))
    }

    fn cancel_all<'a>(
        &'a self,
        _tx: &'a mut Tx,
        saga_id: &'a str,
        now: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.cancel_where(now, |d| d.saga_id == saga_id);
        ready(Ok(()))
    }

    fn mark_fired<'a>(
        &'a self,
        _tx: &'a mut Tx,
        id: &'a str,
        now: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.data();
        let fired = match data
            .deadlines
            .iter_mut()
            .find(|d| d.id == id && d.status == DeadlineStatus::Scheduled)
        {
            Some(d) => {
                d.status = DeadlineStatus::Fired;
                d.finished_at_ms = Some(now);
                true
            }
            None => false,
        };
        ready(Ok(fired))
    }

    fn claim_due<'a>(
        &'a self,
        owner: &'a str,
        now: i64,
        claimed_until_ms: i64,
    ) -> BoxFuture<'a, Result<Option<DeadlineDocument>, StoreError>> {
        let mut data = self.data();
        let claimed = data
            .deadlines
            .iter_mut()
            .filter(|d| {
                d.status == DeadlineStatus::Scheduled
                    && d.due_at_ms <= now
                    && d.claimed_until_ms.is_none_or(|until| until < now)
            })
            .min_by_key(|d| d.due_at_ms)
            .map(|d| {
                d.claimed_by = Some(owner.to_string());
                d.claimed_until_ms = Some(claimed_until_ms);
                d.clone()
            });
        ready(Ok(claimed))
    }

    fn count_overdue_by_name(
        &self,
        now: i64,
    ) -> BoxFuture<'_, Result<std::collections::HashMap<String, u64>, StoreError>> {
        let mut counts = std::collections::HashMap::new();
        for d in self
            .data()
            .deadlines
            .iter()
            .filter(|d| d.status == DeadlineStatus::Scheduled && d.due_at_ms <= now)
        {
            *counts.entry(d.name.clone()).or_insert(0) += 1;
        }
        ready(Ok(counts))
    }
}

impl FailureStore for InMemorySagaStore {
    fn find<'a>(
        &'a self,
        _tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<SagaFailureDocument>, StoreError>> {
        ready(Ok(self
            .data()
            .failures
            .iter()
            .find(|f| f.id == id)
            .cloned()))
    }

    fn save<'a>(
        &'a self,
        _tx: &'a mut Tx,
        failure: &'a SagaFailureDocument,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let mut data = self.data();
        data.failures.retain(|f| f.id != failure.id);
        data.failures.push(failure.clone());
        ready(Ok(()))
    }

    fn delete<'a>(
        &'a self,
        _tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.data();
        let before = data.failures.len();
        data.failures.retain(|f| f.id != id);
        ready(Ok(data.failures.len() != before))
    }

    fn failed_again<'a>(
        &'a self,
        _tx: &'a mut Tx,
        id: &'a str,
        error: &'a str,
        attempts: i32,
        now: i64,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let mut data = self.data();
        let found = match data.failures.iter_mut().find(|f| f.id == id) {
            Some(f) => {
                f.attempts += attempts;
                f.error = error.to_string();
                f.last_failed_at_ms = now;
                true
            }
            None => false,
        };
        ready(Ok(found))
    }

    fn list<'a>(
        &'a self,
        _tx: &'a mut Tx,
        saga_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<SagaFailureDocument>, StoreError>> {
        let mut found: Vec<SagaFailureDocument> = self
            .data()
            .failures
            .iter()
            .filter(|f| saga_type.is_none_or(|t| f.saga_type == t))
            .cloned()
            .collect();
        found.sort_by_key(|f| f.last_failed_at_ms);
        ready(Ok(found))
    }
}

impl MarkerStore for InMemorySagaStore {
    fn exists<'a>(
        &'a self,
        _tx: &'a mut Tx,
        id: &'a str,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        ready(Ok(self.data().markers.contains(id)))
    }

    fn insert<'a>(
        &'a self,
        _tx: &'a mut Tx,
        id: &'a str,
        _type_name: &'a str,
        _now_ms: i64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        if self.data().markers.insert(id.to_string()) {
            ready(Ok(()))
        } else {
            ready(Err(StoreError::Conflict(format!("marker {id} exists"))))
        }
    }
}
