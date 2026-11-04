//! What one processor reports in `InstanceStatus.processors` (ADR 0022): kept in memory by the
//! consumer, its rebalance callbacks and its offset refresher, and read by the hub pool's status
//! tick under a short lock (never a Kafka or Mongo call).
//!
//! State: STOPPED while its consumer does not run (stopping at shutdown, or out of the group for a
//! reset, ADR 0023); else PAUSED while its control document says so; else FAILING for 60 s after
//! a record exhausted its attempts, a letter could not be parked or the consumer reported an
//! error; else STARTING before the first assignment (an empty one counts) and between a revoke
//! and the next assign; else RUNNING. `operation_id` names the reset holding the processor's
//! lock, `replaying` and `replay_until` its open replay ranges on the assigned partitions.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::config::DeadLetterMode;
use crate::proto::{PartitionStatus, ProcessorState, ProcessorStatus};

/// How long a failure keeps a processor FAILING.
pub const FAILING_FOR: Duration = Duration::from_secs(60);
/// The most bytes of `last_error` (cut at a UTF-8 boundary).
pub const MAX_LAST_ERROR_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Offsets {
    position: i64,
    committed: i64,
    high_watermark: i64,
}

impl Default for Offsets {
    fn default() -> Self {
        Offsets {
            position: -1,
            committed: -1,
            high_watermark: -1,
        }
    }
}

#[derive(Default)]
struct State {
    assigned_once: bool,
    rebalancing: bool,
    stopped: bool,
    generation: u64,
    partitions: BTreeMap<i32, Offsets>,
    last_error: Option<(String, i64)>,
    failing_since: Option<Instant>,
    paused: bool,
    operation: Option<String>,
    /// Partition → the `until` of its open replay range.
    replay: BTreeMap<i32, i64>,
}

/// See the module docs.
pub struct ProcessorMonitor {
    name: String,
    group_id: String,
    topic: String,
    mode: DeadLetterMode,
    event_types: Vec<String>,
    state: Mutex<State>,
    processed: AtomicU64,
    failed: AtomicU64,
    dead_letters: AtomicU64,
    sample: Notify,
}

impl ProcessorMonitor {
    pub fn new(
        name: &str,
        group_id: &str,
        topic: &str,
        mode: DeadLetterMode,
        mut event_types: Vec<String>,
    ) -> ProcessorMonitor {
        event_types.sort();
        event_types.dedup();
        ProcessorMonitor {
            name: name.to_string(),
            group_id: group_id.to_string(),
            topic: topic.to_string(),
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

    pub fn topic(&self) -> &str {
        &self.topic
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
        let partitions = st
            .partitions
            .iter()
            .map(|(&partition, o)| PartitionStatus {
                partition,
                position: o.position,
                committed: o.committed,
                high_watermark: o.high_watermark,
                lag: lag(o),
                replay_until: st.replay.get(&partition).copied().unwrap_or(0),
            })
            .collect();
        let (last_error, last_error_at_ms) = st.last_error.clone().unwrap_or_default();
        ProcessorStatus {
            name: self.name.clone(),
            group_id: self.group_id.clone(),
            topic: self.topic.clone(),
            state: state as i32,
            partitions,
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
            replaying: st.partitions.keys().any(|p| st.replay.contains_key(p)),
            operation_id: st.operation.clone().unwrap_or_default(),
        }
    }

    /// A rebalance assigned `partitions` (an empty assignment counts).
    pub fn assigned(&self, partitions: &[i32]) {
        let mut st = self.state.lock().unwrap();
        for &p in partitions {
            st.partitions.insert(p, Offsets::default());
        }
        st.assigned_once = true;
        st.rebalancing = false;
        st.generation += 1;
    }

    /// A rebalance revoked `partitions`: STARTING until the next assignment.
    pub fn revoked(&self, partitions: &[i32]) {
        let mut st = self.state.lock().unwrap();
        for p in partitions {
            st.partitions.remove(p);
        }
        st.rebalancing = true;
    }

    /// Bumped by every assignment: the consumer reloads its parked keys when it changed.
    pub fn generation(&self) -> u64 {
        self.state.lock().unwrap().generation
    }

    /// The partitions assigned now, sorted.
    pub fn assigned_partitions(&self) -> Vec<i32> {
        self.state
            .lock()
            .unwrap()
            .partitions
            .keys()
            .copied()
            .collect()
    }

    /// The record at `offset` of `partition` was acknowledged; `processed` counts it in
    /// `processed_total` (its stage 1 completed).
    pub fn acknowledged(&self, partition: i32, offset: i64, processed: bool) {
        if processed {
            self.processed.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(o) = self.state.lock().unwrap().partitions.get_mut(&partition) {
            o.position = offset.saturating_add(1);
        }
    }

    /// A record exhausted its attempts: `failed_total` + 1, FAILING, `last_error`.
    pub fn exhausted(&self, message: &str) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.error(message);
    }

    /// The consumer (or parking) reported an error: FAILING, `last_error`.
    pub fn error(&self, message: &str) {
        let at_ms = chrono::Utc::now().timestamp_millis();
        let mut st = self.state.lock().unwrap();
        st.last_error = Some((cut_bytes(message, MAX_LAST_ERROR_BYTES), at_ms));
        st.failing_since = Some(Instant::now());
    }

    /// The committed offsets and high watermarks read off the consumer thread (-1 unknown); a
    /// partition no longer assigned is ignored.
    pub fn offsets(&self, partition: i32, committed: i64, high_watermark: i64) {
        if let Some(o) = self.state.lock().unwrap().partitions.get_mut(&partition) {
            o.committed = committed.max(-1);
            o.high_watermark = high_watermark.max(-1);
        }
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

    /// The processor is stopping or stopped; its partitions are forgotten.
    pub fn stopped(&self) {
        let mut st = self.state.lock().unwrap();
        st.stopped = true;
        st.partitions.clear();
    }

    /// Its consumer starts (again): STARTING until the first assignment.
    pub fn starting(&self) {
        let mut st = self.state.lock().unwrap();
        st.stopped = false;
        st.assigned_once = false;
        st.rebalancing = false;
        st.partitions.clear();
    }

    /// The control document pauses it (PAUSED while its consumer runs) or not.
    pub fn set_paused(&self, paused: bool) {
        self.state.lock().unwrap().paused = paused;
    }

    /// The reset holding the processor's lock, if any (`operation_id`).
    pub fn set_operation(&self, operation: Option<&str>) {
        self.state.lock().unwrap().operation = operation.map(str::to_string);
    }

    /// The open replay ranges of its topic: partition → `until`.
    pub fn set_replay(&self, ranges: BTreeMap<i32, i64>) {
        self.state.lock().unwrap().replay = ranges;
    }

    /// How far this instance is on an assigned `partition`: the higher of the next offset after
    /// the last record it acknowledged and the group's committed offset (`None` unknown).
    pub fn progress(&self, partition: i32) -> Option<i64> {
        let st = self.state.lock().unwrap();
        let o = st.partitions.get(&partition)?;
        Some(o.position.max(o.committed)).filter(|&at| at >= 0)
    }
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

/// `high_watermark - committed` (with no commit: `high_watermark - position`); -1 unknown; never
/// negative.
fn lag(o: &Offsets) -> i64 {
    if o.high_watermark < 0 {
        return -1;
    }
    let base = if o.committed >= 0 {
        o.committed
    } else if o.position >= 0 {
        o.position
    } else {
        return -1;
    };
    (o.high_watermark - base).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn monitor() -> ProcessorMonitor {
        ProcessorMonitor::new(
            "orders",
            "svc.orders",
            "kraken.events",
            DeadLetterMode::Collection,
            vec!["t.B".into(), "t.A".into(), "t.B".into()],
        )
    }

    fn state(m: &ProcessorMonitor, now: Instant) -> ProcessorState {
        ProcessorState::try_from(m.status_at(now).state).unwrap()
    }

    #[test]
    fn identity_fields_and_sorted_event_types() {
        let s = monitor().status();
        assert_eq!(s.name, "orders");
        assert_eq!(s.group_id, "svc.orders");
        assert_eq!(s.topic, "kraken.events");
        assert_eq!(s.event_types, ["t.A", "t.B"]);
        assert_eq!(s.dead_letter_mode, "collection");
        assert_eq!(
            (s.processed_total, s.failed_total, s.dead_letters),
            (0, 0, 0)
        );
        assert_eq!((s.last_error.as_str(), s.last_error_at_ms), ("", 0));
        assert!(s.partitions.is_empty());
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
        m.assigned(&[1, 0]);
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
        m.revoked(&[0, 1]);
        assert_eq!(
            state(&m, failed_at),
            ProcessorState::ProcessorFailing,
            "failing wins"
        );
        m.assigned(&[0]);
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

    #[test]
    fn replay_ranges_of_the_assigned_partitions_and_progress() {
        let m = monitor();
        m.assigned(&[0, 1]);
        m.set_replay(BTreeMap::from([(1, 40), (5, 9)]));
        let s = m.status();
        assert!(s.replaying);
        assert_eq!(
            s.partitions
                .iter()
                .map(|p| (p.partition, p.replay_until))
                .collect::<Vec<_>>(),
            [(0, 0), (1, 40)]
        );
        m.set_replay(BTreeMap::from([(5, 9)]));
        assert!(!m.status().replaying, "not one of its partitions");
        assert_eq!(m.progress(1), None);
        m.acknowledged(1, 9, true);
        assert_eq!(m.progress(1), Some(10));
        m.offsets(1, 30, 40);
        assert_eq!(m.progress(1), Some(30));
        assert_eq!(m.progress(7), None);
    }

    #[test]
    fn partitions_positions_offsets_and_lag() {
        let m = monitor();
        m.assigned(&[2, 0, 1]);
        assert_eq!(m.assigned_partitions(), [0, 1, 2]);
        // 0: nothing known; 1: processed and committed known; 2: no commit yet, position known.
        m.acknowledged(1, 9, true);
        m.offsets(1, 8, 20);
        m.acknowledged(2, 4, false);
        m.offsets(2, -1, 15);
        m.offsets(7, 1, 2); // not assigned: ignored
        let s = m.status();
        let p: Vec<(i32, i64, i64, i64, i64)> = s
            .partitions
            .iter()
            .map(|p| {
                (
                    p.partition,
                    p.position,
                    p.committed,
                    p.high_watermark,
                    p.lag,
                )
            })
            .collect();
        assert_eq!(
            p,
            [(0, -1, -1, -1, -1), (1, 10, 8, 20, 12), (2, 5, -1, 15, 10)]
        );
        assert_eq!(s.processed_total, 1);
        // A committed offset above the (older) watermark never gives a negative lag.
        m.offsets(1, 25, 20);
        assert_eq!(m.status().partitions[1].lag, 0);
        m.revoked(&[1]);
        assert_eq!(m.assigned_partitions(), [0, 2]);
        m.assigned(&[1]);
        assert_eq!(
            m.status().partitions[1].position,
            -1,
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
        let m = ProcessorMonitor::new("default", "svc", "t", DeadLetterMode::Topic, vec![]);
        m.set_dead_letters(3);
        let s = m.status();
        assert_eq!(s.dead_letters, 0, "0 unless mode collection");
        assert_eq!(s.dead_letter_mode, "topic");
        let m = monitor();
        m.set_dead_letters(3);
        assert_eq!(m.status().dead_letters, 3);
    }
}
