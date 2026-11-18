//! The in-memory event store (ADR 0027): the whole event-store contract inside one process, for
//! tests, local runs and single-process demos, with no infrastructure.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use snodra_messaging::event_store::memory::MemoryEventStore;
//! # use snodra_messaging::store::memory::MemoryStore;
//! # use snodra_messaging::{Config, SnodraMessaging, MessagingError};
//! # async fn run(config: Config) -> Result<(), MessagingError> {
//! let app = SnodraMessaging::builder(config)
//!     .await?
//!     .event_store(Arc::new(MemoryEventStore::default()))
//!     .store(Arc::new(MemoryStore::new()))
//!     // the service's handlers
//!     .start()
//!     .await?;
//! # app.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! - **One log per source**, split into a fixed number of segments
//!   ([`MemoryEventStoreConfig::segments`], four by default) named `0` to `n-1`; an event's key
//!   goes to segment `FNV-1a 64(UTF-8 bytes) mod n` (events without a key go round the segments
//!   from `0`), so events of one key keep their publish order. Positions are the decimal indexes
//!   of a segment's events, from `0`. Member ids are `memory-<n>`.
//! - **A subscription per name**, shared by every [`subscribe`](EventStore::subscribe) of that
//!   name in the process (competing instances): its segments are spread over its members, and a
//!   segment moves to another member only after the one holding it saw it revoked (its next
//!   [`next`](super::Subscription::next)), was closed or was dropped. The committed positions are
//!   kept per name in memory.
//! - **Capabilities** `replay`, `browse` and `lag`; not `native-dead-letter` (the dead letters
//!   stay in the message store, mode `collection`).
//! - **Not durable**: it keeps every event it is given until the process ends, and loses them
//!   all then. Bounding what it holds is the caller's concern (a test, a demo); the builder warns
//!   at startup that it is not durable.
//!
//! Every clone of a [`MemoryEventStore`] is the same event store: hand one clone to each service
//! instance of a test to make them compete.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, MutexGuard};

use futures::future::BoxFuture;
use tokio::sync::Notify;

use super::browse::{BrowseSettings, EventBrowser};
use super::{
    Capability, Delivery, EventStore, EventStoreError, OutgoingEvent, ReplayRange, ResetTarget,
    Subscription, SubscriptionAdmin, SubscriptionSpec, segment_order,
};
use crate::saga::Clock;

mod browser;
mod subscription;

/// The event store's name ([`EventStore::name`]), reported to the hub.
pub const NAME: &str = "memory";
/// Segments per source unless configured otherwise.
pub const DEFAULT_SEGMENTS: usize = 4;
/// The most segments per source.
pub const MAX_SEGMENTS: usize = 1024;

const CAPABILITIES: [Capability; 3] = [Capability::Replay, Capability::Browse, Capability::Lag];

/// The settings of a [`MemoryEventStore`].
#[derive(Clone, Debug)]
pub struct MemoryEventStoreConfig {
    /// Segments per source, 1 to [`MAX_SEGMENTS`] ([`DEFAULT_SEGMENTS`]): how many instances of
    /// one service can share a processor's events.
    pub segments: usize,
    /// The browser's sources and bounds (the defaults: source `snodra.events`).
    pub browse: BrowseSettings,
    /// What the events are stamped with (their append time): [`Clock::tokio`] by default, which
    /// follows paused time in tests.
    pub clock: Clock,
}

impl Default for MemoryEventStoreConfig {
    fn default() -> Self {
        MemoryEventStoreConfig {
            segments: DEFAULT_SEGMENTS,
            browse: BrowseSettings::default(),
            clock: Clock::tokio(),
        }
    }
}

/// The in-memory event store: see the module docs.
#[derive(Clone)]
pub struct MemoryEventStore {
    shared: Arc<Shared>,
}

impl Default for MemoryEventStore {
    /// Four segments, the default browser settings, [`Clock::tokio`].
    fn default() -> Self {
        MemoryEventStore::with(MemoryEventStoreConfig::default())
    }
}

impl MemoryEventStore {
    /// The event store of `config`; [`EventStoreError::Invalid`] for a number of segments out of
    /// bounds or browser settings that are not valid.
    pub fn new(config: MemoryEventStoreConfig) -> Result<MemoryEventStore, EventStoreError> {
        if !(1..=MAX_SEGMENTS).contains(&config.segments) {
            return Err(EventStoreError::Invalid(format!(
                "the in-memory event store takes 1 to {MAX_SEGMENTS} segments, not {}",
                config.segments
            )));
        }
        config.browse.validate().map_err(EventStoreError::Invalid)?;
        Ok(MemoryEventStore::with(config))
    }

    /// Every event of `source` so far, as a subscription would deliver it, oldest first (by
    /// append time, then segment and position): what a test reads back. Empty for a source
    /// nothing was published to.
    pub fn events(&self, source: &str) -> Vec<Delivery> {
        let state = self.shared.state();
        let Some(log) = state.sources.get(source) else {
            return Vec::new();
        };
        let mut all: Vec<(i64, usize, usize, &Record)> = log
            .iter()
            .enumerate()
            .flat_map(|(s, records)| {
                records
                    .iter()
                    .enumerate()
                    .map(move |(at, r)| (r.timestamp_ms, s, at, &**r))
            })
            .collect();
        all.sort_by_key(|(time, s, at, _)| (*time, *s, *at));
        all.into_iter()
            .map(|(_, s, at, r)| Delivery {
                segment: s.to_string(),
                position: at.to_string(),
                key: r.key.clone(),
                headers: r.headers.clone(),
                payload: r.payload.clone(),
                timestamp_ms: r.timestamp_ms,
            })
            .collect()
    }

    fn with(config: MemoryEventStoreConfig) -> MemoryEventStore {
        MemoryEventStore {
            shared: Arc::new(Shared {
                state: Mutex::default(),
                changed: Notify::new(),
                segments: config.segments,
                browse: config.browse,
                clock: config.clock,
                tails: AtomicUsize::new(0),
                searches: AtomicUsize::new(0),
            }),
        }
    }
}

/// What every clone and every subscription of one store share.
pub(super) struct Shared {
    state: Mutex<State>,
    /// Woken on every change a subscription or a tail may wait for: an event appended, a member
    /// joining or leaving, a segment let go, positions moved by a reset.
    changed: Notify,
    segments: usize,
    browse: BrowseSettings,
    clock: Clock,
    /// Tails open now.
    tails: AtomicUsize,
    /// Searches, record reads, sources and lag listings running now.
    searches: AtomicUsize,
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn wake(&self) {
        self.changed.notify_waiters();
    }

    /// The index of the segment named `name`.
    fn segment(&self, name: &str) -> Result<usize, EventStoreError> {
        parse_index(name)
            .filter(|s| *s < self.segments)
            .ok_or_else(|| {
                EventStoreError::Invalid(format!(
                    "no segment {} (segments 0 to {})",
                    shown(name),
                    self.segments - 1
                ))
            })
    }
}

/// One appended event.
pub(super) struct Record {
    key: Option<String>,
    headers: HashMap<String, String>,
    payload: Vec<u8>,
    timestamp_ms: i64,
}

/// A source: its segments' events, in order.
type Log = Vec<Vec<Arc<Record>>>;

/// A subscription name's committed positions, its members (in joining order) and which member
/// holds each segment now.
#[derive(Default)]
struct Group {
    committed: BTreeMap<usize, u64>,
    members: Vec<u64>,
    holders: HashMap<usize, u64>,
}

impl Group {
    /// The member a segment goes to: the members share the segments round robin.
    fn target(&self, segment: usize) -> Option<u64> {
        (!self.members.is_empty()).then(|| self.members[segment % self.members.len()])
    }

    /// `member` leaves: its segments are free for the others.
    fn leave(&mut self, member: u64) {
        self.members.retain(|m| *m != member);
        self.holders.retain(|_, holder| *holder != member);
    }
}

#[derive(Default)]
struct State {
    sources: HashMap<String, Log>,
    groups: HashMap<SubscriptionSpec, Group>,
    next_member: u64,
    /// Where the next event without a key goes (round robin from segment 0).
    spread: usize,
}

impl State {
    fn source(&mut self, source: &str, segments: usize) -> &mut Log {
        self.sources
            .entry(source.to_string())
            .or_insert_with(|| vec![Vec::new(); segments])
    }

    fn head(&self, source: &str, segment: usize) -> u64 {
        self.sources
            .get(source)
            .and_then(|log| log.get(segment))
            .map_or(0, |records| records.len() as u64)
    }
}

/// A decimal index with no sign, no leading zero and no overflow.
fn parse_index(text: &str) -> Option<usize> {
    let canonical = !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));
    canonical.then(|| text.parse().ok()).flatten()
}

/// A position as the store reads it: a decimal event index.
fn parse_position(text: &str) -> Result<u64, EventStoreError> {
    parse_index(text)
        .map(|p| p as u64)
        .ok_or_else(|| EventStoreError::Invalid(format!("{} is not a position", shown(text))))
}

/// `text` quoted and cut for an error message.
fn shown(text: &str) -> String {
    super::browse::quoted(text)
}

/// FNV-1a 64 of the key's UTF-8 bytes: the key's segment (this mod the number of segments) is the
/// same in every run, and the same as the Kotlin library's in-memory event store.
fn hash(key: &str) -> u64 {
    key.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

impl EventStore for MemoryEventStore {
    fn name(&self) -> &'static str {
        NAME
    }

    fn capabilities(&self) -> &[Capability] {
        &CAPABILITIES
    }

    /// Appends at once; [`EventStoreError::Rejected`] for an event without a source.
    fn publish<'a>(
        &'a self,
        event: &'a OutgoingEvent,
    ) -> BoxFuture<'a, Result<(), EventStoreError>> {
        if event.source.is_empty() {
            return Box::pin(std::future::ready(Err(EventStoreError::Rejected(
                "an event needs a source".into(),
            ))));
        }
        let shared = &self.shared;
        {
            let mut state = shared.state();
            let at = match &event.key {
                Some(key) => (hash(key) % shared.segments as u64) as usize,
                None => {
                    let at = state.spread;
                    state.spread = (at + 1) % shared.segments;
                    at
                }
            };
            let segment = &mut state.source(&event.source, shared.segments)[at];
            // Times rise within a segment (the browser's time ranges rely on it), even if the
            // clock steps back.
            let now = shared.clock.now_ms();
            let timestamp_ms = segment.last().map_or(now, |r| r.timestamp_ms.max(now));
            segment.push(Arc::new(Record {
                key: event.key.clone(),
                headers: event.headers.clone(),
                payload: event.payload.clone(),
                timestamp_ms,
            }));
        }
        shared.wake();
        Box::pin(std::future::ready(Ok(())))
    }

    /// Joins at once; [`EventStoreError::Invalid`] for an empty name or source.
    fn subscribe(
        &self,
        spec: SubscriptionSpec,
    ) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
        if spec.name.is_empty() || spec.source.is_empty() {
            return Box::pin(std::future::ready(Err(EventStoreError::Invalid(
                "a subscription needs a name and a source".into(),
            ))));
        }
        let member = {
            let mut state = self.shared.state();
            state.source(&spec.source, self.shared.segments);
            state.next_member += 1;
            let member = state.next_member;
            state
                .groups
                .entry(spec.clone())
                .or_default()
                .members
                .push(member);
            member
        };
        self.shared.wake();
        let subscription: Box<dyn Subscription> = Box::new(subscription::MemorySubscription::new(
            Arc::clone(&self.shared),
            spec,
            member,
        ));
        Box::pin(std::future::ready(Ok(subscription)))
    }

    fn admin(
        &self,
        spec: &SubscriptionSpec,
    ) -> Result<Box<dyn SubscriptionAdmin>, EventStoreError> {
        Ok(Box::new(Admin {
            shared: Arc::clone(&self.shared),
            spec: spec.clone(),
        }))
    }

    /// Positions by value (they are decimal indexes); anything else after them, by its bytes.
    fn compare(&self, a: &str, b: &str) -> Ordering {
        segment_order(a, b)
    }

    fn browser(&self) -> Option<Arc<dyn EventBrowser>> {
        Some(Arc::new(browser::MemoryBrowser::new(Arc::clone(
            &self.shared,
        ))))
    }
}

/// The reset side of one subscription name: every call answers at once.
struct Admin {
    shared: Arc<Shared>,
    spec: SubscriptionSpec,
}

impl SubscriptionAdmin for Admin {
    fn members(&self) -> Result<Vec<String>, EventStoreError> {
        let state = self.shared.state();
        Ok(state.groups.get(&self.spec).map_or(Vec::new(), |g| {
            g.members.iter().map(|m| format!("memory-{m}")).collect()
        }))
    }

    fn plan(&self, target: &ResetTarget) -> Result<Vec<ReplayRange>, EventStoreError> {
        let shared = &self.shared;
        let state = shared.state();
        let source = &self.spec.source;
        let committed = |s: usize| {
            state
                .groups
                .get(&self.spec)
                .and_then(|g| g.committed.get(&s))
                .map_or(String::new(), u64::to_string)
        };
        let range = |s: usize, to: u64| ReplayRange {
            source: source.clone(),
            segment: s.to_string(),
            from: to.to_string(),
            until: committed(s),
        };
        let every = 0..shared.segments;
        let mut ranges = match target {
            ResetTarget::Earliest => every.map(|s| range(s, 0)).collect(),
            ResetTarget::Latest => every.map(|s| range(s, state.head(source, s))).collect(),
            ResetTarget::Timestamp(ms) => every
                .map(|s| {
                    let records = state.sources.get(source).map_or(&[][..], |log| &log[s]);
                    let at = records.partition_point(|r| r.timestamp_ms < *ms) as u64;
                    range(s, at)
                })
                .collect(),
            ResetTarget::Positions(positions) => positions
                .iter()
                .map(|(segment, position)| {
                    let s = shared.segment(segment)?;
                    let to = parse_position(position)?;
                    if to > state.head(source, s) {
                        return Err(EventStoreError::Invalid(format!(
                            "position {to} is past the head of segment {s}"
                        )));
                    }
                    Ok(range(s, to))
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        ranges.sort_by(|a, b| segment_order(&a.segment, &b.segment));
        Ok(ranges)
    }

    fn apply(&self, ranges: &[ReplayRange]) -> Result<(), EventStoreError> {
        let shared = &self.shared;
        {
            let mut state = shared.state();
            let mut moves = Vec::with_capacity(ranges.len());
            for r in ranges {
                if r.source != self.spec.source {
                    return Err(EventStoreError::Invalid(format!(
                        "a range of source {}, not of {}",
                        shown(&r.source),
                        shown(&self.spec.source)
                    )));
                }
                let s = shared.segment(&r.segment)?;
                let to = parse_position(&r.from)?;
                if to > state.head(&self.spec.source, s) {
                    return Err(EventStoreError::Invalid(format!(
                        "position {to} is past the head of segment {s}"
                    )));
                }
                moves.push((s, to));
            }
            let group = state.groups.entry(self.spec.clone()).or_default();
            if !group.members.is_empty() {
                let members: Vec<String> = group
                    .members
                    .iter()
                    .map(|m| format!("memory-{m}"))
                    .collect();
                return Err(EventStoreError::SubscribersActive(members.join(", ")));
            }
            group.committed.extend(moves);
        }
        shared.wake();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_are_canonical_decimal_indexes() {
        assert_eq!(parse_index("0"), Some(0));
        assert_eq!(parse_index("42"), Some(42));
        for bad in ["", "007", "+1", "-1", "1.0", "x", "99999999999999999999999"] {
            assert_eq!(parse_index(bad), None, "{bad}");
        }
    }

    /// The reference values both libraries pin.
    #[test]
    fn a_key_goes_to_the_segment_both_libraries_give_it() {
        let segment = |key: &str, n: u64| hash(key) % n;
        let keys = ["k", "order-1", "order-2", "é", ""];
        assert_eq!(keys.map(|k| segment(k, 4)), [2, 1, 0, 1, 1]);
        assert_eq!(keys.map(|k| segment(k, 6)), [2, 3, 0, 1, 5]);
    }

    #[test]
    fn a_key_hashes_the_same_in_every_run() {
        assert_eq!(hash(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(hash("a"), 0xaf63_dc4c_8601_ec8c);
    }
}
