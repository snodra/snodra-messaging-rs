//! What one processor reports in `InstanceStatus.processors` (ADR 0022): kept in memory by the
//! processor's consumer (the assignment changes its subscription reports, and the subscription's
//! segment statuses it copies), and read by the hub pool's status tick under a short lock (never
//! an event-store or store call).
//!
//! State: STOPPED while its consumer does not run (stopping at shutdown, or out of its
//! subscription for a reset, ADR 0023); else PAUSED while its control document says so; else
//! FAILING for 60 s after a record exhausted its attempts, a letter could not be parked or the
//! subscription reported an error; else STARTING before the first assignment (an empty one
//! counts) and between a revoke and the next assign; else RUNNING. `operation_id` names the reset
//! holding the processor's lock, `replaying` and `replay_until` its open replay ranges on the
//! assigned segments that the subscription's committed position (its position while nothing is
//! committed) has not passed yet: a segment stops replaying as its last replayed record is
//! acknowledged, not when the poll removes the range from the document. Segments are listed in
//! [`segment_order`]; positions are compared with the event store's
//! [`compare`](EventStore::compare) only.
//!
//! The processor gauges (ADR 0026) read the same status when the meter collects:
//! `snodra.processor.state{processor,state}` (every state, 1 for the current one),
//! `snodra.processor.lag{processor,segment}` (each assigned segment whose lag is known) and
//! `snodra.dead.letters{processor}`.

use std::cmp::Ordering as Order;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use opentelemetry::metrics::AsyncInstrument;
use tokio::sync::Notify;

use crate::config::DeadLetterMode;
use crate::event_store::{EventStore, SegmentStatus, SubscriptionSpec, segment_order};
use crate::metrics::{DEAD_LETTERS, Metrics, PROCESSOR_LAG, PROCESSOR_STATE};
use crate::proto::{self, ProcessorState, ProcessorStatus};

/// The states a processor reports, with their `state` label (the `ProcessorState` name without
/// `PROCESSOR_`, lower case, as the hub's `snodra_hub_processor_state`).
const STATES: [(ProcessorState, &str); 5] = [
    (ProcessorState::ProcessorStarting, "starting"),
    (ProcessorState::ProcessorRunning, "running"),
    (ProcessorState::ProcessorPaused, "paused"),
    (ProcessorState::ProcessorFailing, "failing"),
    (ProcessorState::ProcessorStopped, "stopped"),
];

/// How long a failure keeps a processor FAILING.
pub const FAILING_FOR: Duration = Duration::from_secs(60);
/// The most bytes of `last_error` (cut at a UTF-8 boundary).
pub const MAX_LAST_ERROR_BYTES: usize = 512;
/// The most bytes of `event_store` and of each capability, and the most capabilities (the hub's
/// bounds).
pub const MAX_EVENT_STORE_BYTES: usize = 64;
pub const MAX_CAPABILITY_BYTES: usize = 64;
pub const MAX_CAPABILITIES: usize = 16;

/// The order of two positions of one segment: the event store's.
type Compare = Arc<dyn Fn(&str, &str) -> Order + Send + Sync>;

#[derive(Default)]
struct State {
    assigned_once: bool,
    rebalancing: bool,
    stopped: bool,
    generation: u64,
    /// The assigned segments, with what the subscription last reported of each.
    segments: HashMap<String, SegmentStatus>,
    last_error: Option<(String, i64)>,
    failing_since: Option<Instant>,
    paused: bool,
    operation: Option<String>,
    /// Segment → the `until` of its open replay range.
    replay: HashMap<String, String>,
}

/// See the module docs.
pub struct ProcessorMonitor {
    name: String,
    subscription: String,
    source: String,
    event_store: String,
    capabilities: Vec<String>,
    compare: Compare,
    mode: DeadLetterMode,
    event_types: Vec<String>,
    state: Mutex<State>,
    processed: AtomicU64,
    failed: AtomicU64,
    dead_letters: AtomicU64,
    sample: Notify,
}

fn lag_attrs(processor: &str, segment: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("processor", processor.to_string()),
        KeyValue::new("segment", segment.to_string()),
    ]
}

impl ProcessorMonitor {
    /// The monitor of processor `name` reading `spec` on `events`.
    pub fn new(
        name: &str,
        spec: &SubscriptionSpec,
        events: &Arc<dyn EventStore>,
        mode: DeadLetterMode,
        mut event_types: Vec<String>,
    ) -> ProcessorMonitor {
        event_types.sort();
        event_types.dedup();
        let mut capabilities: Vec<String> = events
            .capabilities()
            .iter()
            .map(|c| cut_bytes(c.as_str(), MAX_CAPABILITY_BYTES))
            .collect();
        capabilities.sort();
        capabilities.dedup();
        capabilities.truncate(MAX_CAPABILITIES);
        let store = Arc::clone(events);
        ProcessorMonitor {
            name: name.to_string(),
            subscription: spec.name.clone(),
            source: spec.source.clone(),
            event_store: cut_bytes(events.name(), MAX_EVENT_STORE_BYTES),
            capabilities,
            compare: Arc::new(move |a, b| store.compare(a, b)),
            mode,
            event_types,
            state: Mutex::new(State::default()),
            processed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            dead_letters: AtomicU64::new(0),
            sample: Notify::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Registers the processor gauges of `monitors` on `metrics`; a monitor reports while it
    /// lives. The lag of a segment that left the assignment (or whose monitor is gone) is
    /// observed once more as 0, then not at all: Prometheus keeps a series that stops at its last
    /// value for minutes (no staleness over OTLP), which next to the new owner's would read as a
    /// lag nobody has. An assigned segment whose lag is unknown is not observed (0 means caught
    /// up).
    pub(crate) fn observe(monitors: &[Arc<ProcessorMonitor>], metrics: &Metrics) {
        let watched: Arc<[Weak<ProcessorMonitor>]> = monitors.iter().map(Arc::downgrade).collect();
        let lag = {
            let watched = Arc::clone(&watched);
            let last = Mutex::new(HashSet::<(String, String)>::new());
            move |o: &dyn AsyncInstrument<u64>| {
                let statuses: Vec<ProcessorStatus> = watched
                    .iter()
                    .filter_map(Weak::upgrade)
                    .map(|m| m.status())
                    .collect();
                let assigned: HashSet<(String, String)> = statuses
                    .iter()
                    .flat_map(|s| {
                        s.segments
                            .iter()
                            .map(|p| (s.name.clone(), p.segment.clone()))
                    })
                    .collect();
                let mut last = last.lock().unwrap();
                for (processor, segment) in last.difference(&assigned) {
                    o.observe(0, &lag_attrs(processor, segment));
                }
                for s in &statuses {
                    for p in s.segments.iter().filter(|p| p.lag >= 0) {
                        o.observe(p.lag as u64, &lag_attrs(&s.name, &p.segment));
                    }
                }
                *last = assigned;
            }
        };
        let each = move |report: fn(&ProcessorStatus, &dyn AsyncInstrument<u64>)| {
            let watched = Arc::clone(&watched);
            move |o: &dyn AsyncInstrument<u64>| {
                for m in watched.iter().filter_map(Weak::upgrade) {
                    report(&m.status(), o);
                }
            }
        };
        let meter = metrics.meter();
        meter
            .u64_observable_gauge(PROCESSOR_STATE)
            .with_description("1 for the processor's state now (starting, running, paused, failing, stopped), 0 for the others")
            .with_callback(each(|s, o| {
                for (state, label) in STATES {
                    o.observe(
                        u64::from(s.state == state as i32),
                        &[
                            KeyValue::new("processor", s.name.clone()),
                            KeyValue::new("state", label),
                        ],
                    );
                }
            }))
            .build();
        meter
            .u64_observable_gauge(PROCESSOR_LAG)
            .with_description("Records behind on each segment assigned to this instance whose lag is known, as the processor status (a segment that left the assignment reads 0 once)")
            .with_callback(lag)
            .build();
        meter
            .u64_observable_gauge(DEAD_LETTERS)
            .with_description("The processor's letters in dead_letters as last sampled (0 when the dead-letter mode is not collection)")
            .with_callback(each(|s, o| {
                o.observe(
                    s.dead_letters,
                    &[KeyValue::new("processor", s.name.clone())],
                );
            }))
            .build();
    }

    /// The source the processor reads.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The status now (wall clock for `last_error_at_ms`).
    pub fn status(&self) -> ProcessorStatus {
        self.status_at(Instant::now())
    }

    fn status_at(&self, now: Instant) -> ProcessorStatus {
        let st = self.state.lock().unwrap();
        let failing = st
            .failing_since
            .is_some_and(|at| now.saturating_duration_since(at) < FAILING_FOR);
        let state = if st.stopped {
            ProcessorState::ProcessorStopped
        } else if st.paused {
            ProcessorState::ProcessorPaused
        } else if failing {
            ProcessorState::ProcessorFailing
        } else if !st.assigned_once || st.rebalancing {
            ProcessorState::ProcessorStarting
        } else {
            ProcessorState::ProcessorRunning
        };
        let text = |p: &Option<String>| p.clone().unwrap_or_default();
        let segments: Vec<proto::SegmentStatus> = sorted(st.segments.keys())
            .into_iter()
            .map(|segment| {
                let o = &st.segments[&segment];
                let replay_until = st
                    .replay
                    .get(&segment)
                    .filter(|until| !self.passed(progress(o), until))
                    .cloned()
                    .unwrap_or_default();
                proto::SegmentStatus {
                    position: text(&o.position),
                    committed: text(&o.committed),
                    head: text(&o.head),
                    lag: o
                        .lag
                        .map_or(-1, |lag| i64::try_from(lag).unwrap_or(i64::MAX)),
                    replay_until,
                    segment,
                }
            })
            .collect();
        let replaying = segments.iter().any(|p| !p.replay_until.is_empty());
        let (last_error, last_error_at_ms) = st.last_error.clone().unwrap_or_default();
        ProcessorStatus {
            name: self.name.clone(),
            subscription: self.subscription.clone(),
            source: self.source.clone(),
            state: state as i32,
            segments,
            processed_total: self.processed.load(Ordering::Relaxed),
            failed_total: self.failed.load(Ordering::Relaxed),
            dead_letters: if self.mode == DeadLetterMode::Collection {
                self.dead_letters.load(Ordering::Relaxed)
            } else {
                0
            },
            last_error,
            last_error_at_ms,
            event_types: self.event_types.clone(),
            dead_letter_mode: self.mode.as_str().to_string(),
            replaying,
            operation_id: st.operation.clone().unwrap_or_default(),
            event_store: self.event_store.clone(),
            capabilities: self.capabilities.clone(),
        }
    }

    /// Whether `at` (a position of a segment, `None` unknown) is at or past `until`.
    fn passed(&self, at: Option<&str>, until: &str) -> bool {
        at.is_some_and(|at| (self.compare)(at, until) != Order::Less)
    }

    /// The subscription was assigned `segments` (an empty assignment counts).
    pub fn assigned(&self, segments: &[String]) {
        let mut st = self.state.lock().unwrap();
        for s in segments {
            st.segments.insert(
                s.clone(),
                SegmentStatus {
                    segment: s.clone(),
                    ..SegmentStatus::default()
                },
            );
        }
        st.assigned_once = true;
        st.rebalancing = false;
        st.generation += 1;
    }

    /// The subscription lost `segments`: STARTING until the next assignment.
    pub fn revoked(&self, segments: &[String]) {
        let mut st = self.state.lock().unwrap();
        for s in segments {
            st.segments.remove(s);
        }
        st.rebalancing = true;
    }

    /// Bumped by every assignment: the consumer reloads its parked keys when it changed.
    pub fn generation(&self) -> u64 {
        self.state.lock().unwrap().generation
    }

    /// The segments assigned now, in [`segment_order`].
    pub fn assigned_segments(&self) -> Vec<String> {
        sorted(self.state.lock().unwrap().segments.keys())
    }

    /// A record was acknowledged; `processed` counts it in `processed_total` (its stage 1
    /// completed).
    pub fn acknowledged(&self, processed: bool) {
        if processed {
            self.processed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// What the subscription reports of its segments (positions and lag); a segment not
    /// assigned is ignored.
    pub fn segments(&self, statuses: Vec<SegmentStatus>) {
        let mut st = self.state.lock().unwrap();
        for status in statuses {
            if let Some(known) = st.segments.get_mut(&status.segment) {
                *known = status;
            }
        }
    }

    /// A record exhausted its attempts: `failed_total` + 1, FAILING, `last_error`.
    pub fn exhausted(&self, message: &str) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.error(message);
    }

    /// The subscription (or parking) reported an error: FAILING, `last_error`.
    pub fn error(&self, message: &str) {
        let at_ms = chrono::Utc::now().timestamp_millis();
        let mut st = self.state.lock().unwrap();
        st.last_error = Some((cut_bytes(message, MAX_LAST_ERROR_BYTES), at_ms));
        st.failing_since = Some(Instant::now());
    }

    /// The sampled `countDocuments({processor})` of `dead_letters`.
    pub fn set_dead_letters(&self, count: u64) {
        self.dead_letters.store(count, Ordering::Relaxed);
    }

    /// Asks the dead-letter sampler for a sample now (after this instance parked a letter).
    pub fn sample_dead_letters_soon(&self) {
        self.sample.notify_one();
    }

    pub(crate) async fn sample_requested(&self) {
        self.sample.notified().await
    }

    /// The processor is stopping or stopped; its segments are forgotten.
    pub fn stopped(&self) {
        let mut st = self.state.lock().unwrap();
        st.stopped = true;
        st.segments.clear();
    }

    /// Its consumer starts (again): STARTING until the first assignment.
    pub fn starting(&self) {
        let mut st = self.state.lock().unwrap();
        st.stopped = false;
        st.assigned_once = false;
        st.rebalancing = false;
        st.segments.clear();
    }

    /// The control document pauses it (PAUSED while its consumer runs) or not.
    pub fn set_paused(&self, paused: bool) {
        self.state.lock().unwrap().paused = paused;
    }

    /// The reset holding the processor's lock, if any (`operation_id`).
    pub fn set_operation(&self, operation: Option<&str>) {
        self.state.lock().unwrap().operation = operation.map(str::to_string);
    }

    /// The open replay ranges of its source: segment → `until`.
    pub fn set_replay(&self, ranges: HashMap<String, String>) {
        self.state.lock().unwrap().replay = ranges;
    }

    /// Whether this instance is at or past `until` on the assigned `segment`: the subscription's
    /// committed position (its position while nothing is committed) is (`false` unknown).
    pub fn passed_on(&self, segment: &str, until: &str) -> bool {
        let st = self.state.lock().unwrap();
        st.segments
            .get(segment)
            .is_some_and(|o| self.passed(progress(o), until))
    }
}

/// How far a segment is: its committed position, else its position.
fn progress(o: &SegmentStatus) -> Option<&str> {
    o.committed.as_deref().or(o.position.as_deref())
}

/// `names` in [`segment_order`].
fn sorted<'a>(names: impl Iterator<Item = &'a String>) -> Vec<String> {
    let mut all: Vec<String> = names.cloned().collect();
    all.sort_by(|a, b| segment_order(a, b));
    all
}

/// `text` cut to at most `max` bytes at a UTF-8 boundary.
pub(crate) fn cut_bytes(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

#[cfg(test)]
pub(crate) mod tests {
    use futures::future::BoxFuture;

    use super::*;
    use crate::event_store::{
        Capability, EventStoreError, OutgoingEvent, Subscription, SubscriptionAdmin,
    };

    /// An event store with decimal positions that is never reached: what a monitor reads of one.
    pub(crate) struct Decimal(pub Vec<Capability>);

    impl EventStore for Decimal {
        fn name(&self) -> &'static str {
            "decimal"
        }
        fn capabilities(&self) -> &[Capability] {
            &self.0
        }
        fn publish<'a>(
            &'a self,
            _: &'a OutgoingEvent,
        ) -> BoxFuture<'a, Result<(), EventStoreError>> {
            unreachable!()
        }
        fn subscribe(
            &self,
            _: SubscriptionSpec,
        ) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
            unreachable!()
        }
        fn admin(
            &self,
            _: &SubscriptionSpec,
        ) -> Result<Box<dyn SubscriptionAdmin>, EventStoreError> {
            unreachable!()
        }
        fn compare(&self, a: &str, b: &str) -> Order {
            a.parse::<i64>().unwrap().cmp(&b.parse::<i64>().unwrap())
        }
    }

    pub(crate) fn decimal() -> Arc<dyn EventStore> {
        Arc::new(Decimal(Capability::ALL.to_vec()))
    }

    fn spec() -> SubscriptionSpec {
        SubscriptionSpec {
            name: "svc.orders".into(),
            source: "snodra.events".into(),
        }
    }

    fn monitor() -> ProcessorMonitor {
        ProcessorMonitor::new(
            "orders",
            &spec(),
            &decimal(),
            DeadLetterMode::Collection,
            vec!["t.B".into(), "t.A".into(), "t.B".into()],
        )
    }

    fn names(all: &[&str]) -> Vec<String> {
        all.iter().map(|s| s.to_string()).collect()
    }

    /// What the subscription reports of `segment`: committed, position and head (-1 unknown),
    /// the lag from them as a store with `lag` would.
    fn seg(segment: &str, position: i64, committed: i64, head: i64) -> SegmentStatus {
        let known = |n: i64| (n >= 0).then(|| n.to_string());
        let base = if committed >= 0 { committed } else { position };
        SegmentStatus {
            segment: segment.into(),
            position: known(position),
            committed: known(committed),
            head: known(head),
            lag: (head >= 0 && base >= 0).then(|| (head - base).max(0) as u64),
        }
    }

    fn state(m: &ProcessorMonitor, now: Instant) -> ProcessorState {
        ProcessorState::try_from(m.status_at(now).state).unwrap()
    }

    /// An instance that loses every segment is running, idle, once the revocation is closed
    /// by an empty assignment (the event-store contract), not starting.
    #[test]
    fn an_instance_left_without_segments_is_running_after_the_empty_assignment() {
        let m = monitor();
        let t0 = Instant::now();
        m.assigned(&names(&["0", "1"]));
        m.revoked(&names(&["0", "1"]));
        assert_eq!(state(&m, t0), ProcessorState::ProcessorStarting);
        m.assigned(&[]);
        assert_eq!(state(&m, t0), ProcessorState::ProcessorRunning);
        assert!(m.status_at(t0).segments.is_empty());
    }

    #[test]
    fn the_gauges_read_the_status_one_state_at_one_and_only_known_lags() {
        use crate::metrics::test_meter::TestMeter;
        use crate::metrics::{DEAD_LETTERS, Metrics, PROCESSOR_LAG, PROCESSOR_STATE};
        let t = TestMeter::new();
        let m = Arc::new(monitor());
        ProcessorMonitor::observe(&[Arc::clone(&m)], &Metrics::new(t.meter()));
        let state_of = |t: &TestMeter| {
            let on: Vec<String> = t
                .points_of(PROCESSOR_STATE)
                .into_iter()
                .filter(|p| p.attributes["processor"] == "orders" && p.value == 1.0)
                .map(|p| p.attributes["state"].clone())
                .collect();
            on
        };
        assert_eq!(state_of(&t), ["starting"]);
        assert_eq!(
            t.points_of(PROCESSOR_STATE).len(),
            5,
            "every state, the others at 0"
        );

        m.assigned(&names(&["0", "1"]));
        m.segments(vec![seg("0", -1, 5, 12)]);
        m.set_dead_letters(3);
        assert_eq!(state_of(&t), ["running"]);
        assert_eq!(
            t.value(PROCESSOR_LAG, &[("processor", "orders"), ("segment", "0")]),
            Some(7.0)
        );
        assert_eq!(
            t.points_of(PROCESSOR_LAG).len(),
            1,
            "segment 1's lag is not known yet"
        );
        assert_eq!(t.value(DEAD_LETTERS, &[("processor", "orders")]), Some(3.0));

        // Prometheus keeps a series that stops at its last value (no staleness over OTLP): a
        // revoked segment reads 0 once, then is gone; its new owner reports it from then on.
        m.revoked(&names(&["0"]));
        let lag = |t: &TestMeter| {
            let mut lag: Vec<(String, f64)> = t
                .points_of(PROCESSOR_LAG)
                .into_iter()
                .map(|p| (p.attributes["segment"].clone(), p.value))
                .collect();
            lag.sort_by(|a, b| a.0.cmp(&b.0));
            lag
        };
        assert_eq!(lag(&t), [("0".to_string(), 0.0)], "revoked: 0 once");
        assert_eq!(lag(&t), [], "then no longer reported");
        // Segment 1 is still assigned, its lag unknown: absent, not 0 (0 means caught up).
        assert_eq!(m.assigned_segments(), ["1"]);
        drop(m);
        assert_eq!(
            lag(&t),
            [("1".to_string(), 0.0)],
            "a dropped monitor's segments read 0 once"
        );
        assert!(
            t.points_of(PROCESSOR_STATE).is_empty(),
            "a dropped monitor reports nothing"
        );
        assert_eq!(lag(&t), []);
    }

    #[test]
    fn identity_fields_event_store_capabilities_and_sorted_event_types() {
        let s = monitor().status();
        assert_eq!(s.name, "orders");
        assert_eq!(s.subscription, "svc.orders");
        assert_eq!(s.source, "snodra.events");
        assert_eq!(s.event_store, "decimal");
        assert_eq!(
            s.capabilities,
            ["browse", "lag", "native-dead-letter", "replay"]
        );
        assert_eq!(s.event_types, ["t.A", "t.B"]);
        assert_eq!(s.dead_letter_mode, "collection");
        assert_eq!(
            (s.processed_total, s.failed_total, s.dead_letters),
            (0, 0, 0)
        );
        assert_eq!((s.last_error.as_str(), s.last_error_at_ms), ("", 0));
        assert!(s.segments.is_empty());
    }

    #[test]
    fn states_follow_assignment_failures_and_stop() {
        let m = monitor();
        let t0 = Instant::now();
        assert_eq!(
            state(&m, t0),
            ProcessorState::ProcessorStarting,
            "not assigned yet"
        );
        m.assigned(&[]);
        assert_eq!(
            state(&m, t0),
            ProcessorState::ProcessorRunning,
            "an empty assignment counts"
        );
        m.revoked(&[]);
        assert_eq!(
            state(&m, t0),
            ProcessorState::ProcessorStarting,
            "rebalancing"
        );
        m.assigned(&names(&["1", "0"]));
        assert_eq!(state(&m, t0), ProcessorState::ProcessorRunning);
        let g = m.generation();
        m.exhausted("handler failed: boom");
        let failed_at = Instant::now();
        assert_eq!(state(&m, failed_at), ProcessorState::ProcessorFailing);
        assert_eq!(
            state(&m, failed_at + FAILING_FOR + Duration::from_secs(1)),
            ProcessorState::ProcessorRunning,
            "60 s later"
        );
        m.revoked(&names(&["0", "1"]));
        assert_eq!(
            state(&m, failed_at),
            ProcessorState::ProcessorFailing,
            "failing wins"
        );
        m.assigned(&names(&["0"]));
        assert!(m.generation() > g);
        m.error("all brokers down");
        let s = m.status_at(Instant::now());
        assert_eq!(s.last_error, "all brokers down");
        assert_eq!(s.failed_total, 1, "an error is not a failed record");
        m.stopped();
        assert_eq!(state(&m, Instant::now()), ProcessorState::ProcessorStopped);
    }

    #[test]
    fn stopped_beats_paused_which_beats_failing_and_starting() {
        let m = monitor();
        m.set_paused(true);
        assert_eq!(state(&m, Instant::now()), ProcessorState::ProcessorPaused);
        m.exhausted("boom");
        assert_eq!(
            state(&m, Instant::now()),
            ProcessorState::ProcessorPaused,
            "paused beats failing"
        );
        m.set_operation(Some("op-1"));
        m.stopped();
        let s = m.status();
        assert_eq!(state(&m, Instant::now()), ProcessorState::ProcessorStopped);
        assert_eq!(s.operation_id, "op-1");
        m.set_operation(None);
        m.starting();
        m.set_paused(false);
        assert_eq!(state(&m, Instant::now()), ProcessorState::ProcessorFailing);
        assert_eq!(m.status().operation_id, "");
    }

    fn replay(all: &[(&str, &str)]) -> HashMap<String, String> {
        all.iter()
            .map(|(s, u)| (s.to_string(), u.to_string()))
            .collect()
    }

    #[test]
    fn replay_ranges_of_the_assigned_segments_and_progress() {
        let m = monitor();
        m.assigned(&names(&["0", "1"]));
        m.set_replay(replay(&[("1", "40"), ("5", "9")]));
        let s = m.status();
        assert!(s.replaying);
        assert_eq!(
            s.segments
                .iter()
                .map(|p| (p.segment.as_str(), p.replay_until.as_str()))
                .collect::<Vec<_>>(),
            [("0", ""), ("1", "40")]
        );
        m.set_replay(replay(&[("5", "9")]));
        assert!(!m.status().replaying, "not one of its segments");
        assert!(!m.passed_on("1", "10"), "nothing known");
        m.segments(vec![seg("1", 10, -1, 40)]);
        assert!(
            m.passed_on("1", "10"),
            "the position while nothing is committed"
        );
        m.segments(vec![seg("1", 31, 30, 40)]);
        assert!(
            m.passed_on("1", "30") && !m.passed_on("1", "31"),
            "the committed one"
        );
        assert!(!m.passed_on("7", "0"), "not assigned");
    }

    #[test]
    fn a_segment_stops_replaying_as_soon_as_its_committed_position_passes_the_range() {
        let m = monitor();
        m.assigned(&names(&["1", "2"]));
        m.set_replay(replay(&[("1", "40"), ("2", "9")]));
        let until = |m: &ProcessorMonitor| {
            m.status()
                .segments
                .iter()
                .map(|p| (p.segment.clone(), p.replay_until.clone()))
                .collect::<Vec<_>>()
        };
        m.segments(vec![seg("1", 39, 39, 50)]);
        assert!(m.status().replaying);
        assert_eq!(
            until(&m),
            [("1".into(), "40".into()), ("2".into(), "9".into())]
        );

        // The last record of the range is acknowledged: not replaying from now on, without
        // waiting for the poll to remove the range from the document.
        m.segments(vec![seg("1", 40, 40, 50)]);
        assert_eq!(
            until(&m),
            [("1".into(), "".into()), ("2".into(), "9".into())]
        );
        assert!(m.status().replaying, "segment 2 still replays");
        // Compared by the event store's order, not as text ("10" < "9" as text).
        m.segments(vec![seg("2", -1, 10, 50)]);
        assert_eq!(
            until(&m),
            [("1".into(), "".into()), ("2".into(), "".into())]
        );
        assert!(!m.status().replaying);
    }

    #[test]
    fn segments_in_segment_order_with_positions_and_lag() {
        let m = monitor();
        m.assigned(&names(&["10", "2", "0", "1"]));
        assert_eq!(m.assigned_segments(), ["0", "1", "2", "10"]);
        m.segments(vec![
            seg("1", 10, 8, 20),
            seg("2", 5, -1, 15),
            seg("7", 1, 1, 2), // not assigned: ignored
        ]);
        let s = m.status();
        let p: Vec<(&str, &str, &str, &str, i64)> = s
            .segments
            .iter()
            .map(|p| {
                (
                    p.segment.as_str(),
                    p.position.as_str(),
                    p.committed.as_str(),
                    p.head.as_str(),
                    p.lag,
                )
            })
            .collect();
        assert_eq!(
            p,
            [
                ("0", "", "", "", -1),
                ("1", "10", "8", "20", 12),
                ("2", "5", "", "15", 10),
                ("10", "", "", "", -1)
            ]
        );
        m.acknowledged(true);
        m.acknowledged(false);
        assert_eq!(m.status().processed_total, 1);
        m.revoked(&names(&["1"]));
        assert_eq!(m.assigned_segments(), ["0", "2", "10"]);
        m.assigned(&names(&["1"]));
        assert_eq!(
            m.status().segments[1].position,
            "",
            "fresh after a reassignment"
        );
    }

    #[test]
    fn last_error_is_cut_at_512_bytes_on_a_char_boundary_with_its_time() {
        let m = monitor();
        let before = chrono::Utc::now().timestamp_millis();
        m.exhausted(&"é".repeat(400));
        let s = m.status();
        assert!(s.last_error.len() <= MAX_LAST_ERROR_BYTES);
        assert_eq!(s.last_error.len(), 512);
        assert!(s.last_error_at_ms >= before);
        assert_eq!(cut_bytes("aé", 2), "a");
        assert_eq!(cut_bytes("abc", 5), "abc");
    }

    #[test]
    fn dead_letters_count_and_mode() {
        let m = ProcessorMonitor::new(
            "default",
            &spec(),
            &decimal(),
            DeadLetterMode::EventStore,
            vec![],
        );
        m.set_dead_letters(3);
        let s = m.status();
        assert_eq!(s.dead_letters, 0, "0 unless mode collection");
        assert_eq!(s.dead_letter_mode, "event-store");
        let m = monitor();
        m.set_dead_letters(3);
        assert_eq!(m.status().dead_letters, 3);
    }
}
