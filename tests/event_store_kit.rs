//! The event-store conformance kit's own test (ADR 0027): the kit runs on a minimal in-process
//! event store of this file, a test double (not the in-memory event store add-on, which comes
//! later), with a message store that only keeps processed-event markers. An event-store add-on
//! proves itself by running the same kit. Needs the feature `testing`.
#![cfg(feature = "testing")]

use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use kraken_messaging::Config;
use kraken_messaging::event_store::conformance::run_event_store_conformance;
use kraken_messaging::event_store::{
    Capability, Delivery, EventStore, EventStoreError, OutgoingEvent, ReplayRange, ResetTarget,
    SegmentStatus, Subscription, SubscriptionAdmin, SubscriptionEvent, SubscriptionSpec,
};
use kraken_messaging::saga::Clock;
use kraken_messaging::store::{
    DeadLetterStore, ExternalCallStore, MessageStore, OutboxStore, ProcessedEventStore,
    ProcessedMarker, ProcessorControlStore, SagaStores, StoreError, Tx, TxProvider,
};

#[tokio::test(flavor = "multi_thread")]
async fn the_kit_passes_on_a_minimal_in_process_event_store() {
    let events = Log::default();
    run_event_store_conformance(
        "conformance.events",
        move || Arc::new(events.clone()) as Arc<dyn EventStore>,
        Arc::new(Markers::default()),
    )
    .await;
}

// ---- the event store ---------------------------------------------------------------------------

/// Segments per source.
const SEGMENTS: usize = 2;

struct Record {
    key: Option<String>,
    headers: HashMap<String, String>,
    payload: Vec<u8>,
    timestamp_ms: i64,
}

/// A subscription name's committed positions and its members, in joining order.
#[derive(Default)]
struct Group {
    committed: HashMap<usize, u64>,
    members: Vec<u64>,
}

#[derive(Default)]
struct State {
    sources: HashMap<String, Vec<Vec<Record>>>,
    groups: HashMap<SubscriptionSpec, Group>,
    next_member: u64,
    spread: usize,
}

impl State {
    fn source(&mut self, source: &str) -> &mut Vec<Vec<Record>> {
        self.sources
            .entry(source.to_string())
            .or_insert_with(|| (0..SEGMENTS).map(|_| Vec::new()).collect())
    }

    fn head(&self, source: &str, segment: usize) -> u64 {
        self.sources
            .get(source)
            .map_or(0, |s| s[segment].len() as u64)
    }

    /// The segments of `member`: round robin over the members.
    fn assignment(&self, spec: &SubscriptionSpec, member: u64) -> BTreeSet<usize> {
        let Some(group) = self.groups.get(spec) else {
            return BTreeSet::new();
        };
        let Some(at) = group.members.iter().position(|m| *m == member) else {
            return BTreeSet::new();
        };
        (0..SEGMENTS)
            .filter(|s| s % group.members.len() == at)
            .collect()
    }
}

/// The event store: one shared state for every "instance".
#[derive(Clone, Default)]
struct Log(Arc<Mutex<State>>);

impl Log {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.0.lock().unwrap()
    }
}

fn position(p: &str) -> u64 {
    p.parse().expect("a decimal position")
}

fn segment(s: &str) -> Result<usize, EventStoreError> {
    s.parse::<usize>()
        .ok()
        .filter(|s| *s < SEGMENTS)
        .ok_or_else(|| EventStoreError::Invalid(format!("no segment {s}")))
}

impl EventStore for Log {
    fn name(&self) -> &'static str {
        "kit-double"
    }

    fn capabilities(&self) -> &[Capability] {
        &Capability::ALL
    }

    fn publish<'a>(
        &'a self,
        event: &'a OutgoingEvent,
    ) -> BoxFuture<'a, Result<(), EventStoreError>> {
        let mut state = self.state();
        let at = match &event.key {
            Some(key) => {
                let mut hash = DefaultHasher::new();
                key.hash(&mut hash);
                hash.finish() as usize % SEGMENTS
            }
            None => {
                state.spread += 1;
                state.spread % SEGMENTS
            }
        };
        state.source(&event.source)[at].push(Record {
            key: event.key.clone(),
            headers: event.headers.clone(),
            payload: event.payload.clone(),
            timestamp_ms: kraken_messaging::time::now_ms(),
        });
        Box::pin(std::future::ready(Ok(())))
    }

    fn subscribe(
        &self,
        spec: SubscriptionSpec,
    ) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
        let mut state = self.state();
        state.next_member += 1;
        let member = state.next_member;
        state
            .groups
            .entry(spec.clone())
            .or_default()
            .members
            .push(member);
        let subscription: Box<dyn Subscription> = Box::new(Member {
            log: self.clone(),
            spec,
            member,
            assigned: BTreeSet::new(),
            cursor: HashMap::new(),
            paused: false,
        });
        Box::pin(std::future::ready(Ok(subscription)))
    }

    fn admin(
        &self,
        spec: &SubscriptionSpec,
    ) -> Result<Box<dyn SubscriptionAdmin>, EventStoreError> {
        Ok(Box::new(Admin {
            log: self.clone(),
            spec: spec.clone(),
        }))
    }

    fn compare(&self, a: &str, b: &str) -> Ordering {
        position(a).cmp(&position(b))
    }
}

struct Member {
    log: Log,
    spec: SubscriptionSpec,
    member: u64,
    assigned: BTreeSet<usize>,
    cursor: HashMap<usize, u64>,
    paused: bool,
}

impl Member {
    /// What `next` gives now, if anything.
    fn step(&mut self) -> Option<SubscriptionEvent> {
        let state = self.log.state();
        let now = state.assignment(&self.spec, self.member);
        let revoked: Vec<usize> = self.assigned.difference(&now).copied().collect();
        if !revoked.is_empty() {
            for s in &revoked {
                self.assigned.remove(s);
                self.cursor.remove(s);
            }
            return Some(SubscriptionEvent::Revoked(names(&revoked)));
        }
        let added: Vec<usize> = now.difference(&self.assigned).copied().collect();
        if !added.is_empty() {
            let committed = &state.groups[&self.spec].committed;
            for &s in &added {
                self.assigned.insert(s);
                self.cursor
                    .insert(s, committed.get(&s).copied().unwrap_or(0));
            }
            return Some(SubscriptionEvent::Assigned(names(&added)));
        }
        if self.paused {
            return None;
        }
        let segments = state.sources.get(&self.spec.source)?;
        for &s in &self.assigned {
            let at = self.cursor[&s];
            if let Some(r) = segments[s].get(at as usize) {
                self.cursor.insert(s, at + 1);
                return Some(SubscriptionEvent::Delivery(Delivery {
                    segment: s.to_string(),
                    position: at.to_string(),
                    key: r.key.clone(),
                    headers: r.headers.clone(),
                    payload: r.payload.clone(),
                    timestamp_ms: r.timestamp_ms,
                }));
            }
        }
        None
    }
}

fn names(segments: &[usize]) -> Vec<String> {
    segments.iter().map(usize::to_string).collect()
}

impl Subscription for Member {
    fn next(&mut self) -> BoxFuture<'_, Result<SubscriptionEvent, EventStoreError>> {
        Box::pin(async move {
            loop {
                if let Some(event) = self.step() {
                    return Ok(event);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    fn ack(&mut self, segment_name: &str, at: &str) -> Result<(), EventStoreError> {
        let s = segment(segment_name)?;
        let mut state = self.log.state();
        let committed = state.groups.get_mut(&self.spec).expect("joined");
        let next = position(at) + 1;
        let entry = committed.committed.entry(s).or_insert(0);
        *entry = (*entry).max(next);
        Ok(())
    }

    fn redeliver(&mut self, segment_name: &str, at: &str) -> Result<(), EventStoreError> {
        self.cursor.insert(segment(segment_name)?, position(at));
        Ok(())
    }

    fn pause(&mut self) {
        self.paused = true;
    }

    fn resume(&mut self) {
        self.paused = false;
    }

    fn segments(&self) -> Vec<SegmentStatus> {
        let state = self.log.state();
        let committed = &state.groups[&self.spec].committed;
        self.assigned
            .iter()
            .map(|&s| {
                let head = state.head(&self.spec.source, s);
                let cursor = self.cursor[&s];
                let done = committed.get(&s).copied();
                SegmentStatus {
                    segment: s.to_string(),
                    position: Some(cursor.to_string()),
                    committed: done.map(|c| c.to_string()),
                    head: Some(head.to_string()),
                    lag: Some(head - done.unwrap_or(cursor)),
                }
            })
            .collect()
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        let mut state = self.log.state();
        if let Some(group) = state.groups.get_mut(&self.spec) {
            group.members.retain(|m| *m != self.member);
        }
        Box::pin(std::future::ready(()))
    }
}

struct Admin {
    log: Log,
    spec: SubscriptionSpec,
}

impl SubscriptionAdmin for Admin {
    fn members(&self) -> Result<Vec<String>, EventStoreError> {
        let state = self.log.state();
        Ok(state.groups.get(&self.spec).map_or(Vec::new(), |g| {
            g.members.iter().map(u64::to_string).collect()
        }))
    }

    fn plan(&self, target: &ResetTarget) -> Result<Vec<ReplayRange>, EventStoreError> {
        let state = self.log.state();
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
        let records = |s: usize| state.sources.get(source).map(|all| &all[s]);
        Ok(match target {
            ResetTarget::Earliest => (0..SEGMENTS).map(|s| range(s, 0)).collect(),
            ResetTarget::Latest => (0..SEGMENTS)
                .map(|s| range(s, state.head(source, s)))
                .collect(),
            ResetTarget::Timestamp(ms) => (0..SEGMENTS)
                .map(|s| {
                    let to = records(s)
                        .and_then(|r| r.iter().position(|r| r.timestamp_ms >= *ms))
                        .map_or(state.head(source, s), |at| at as u64);
                    range(s, to)
                })
                .collect(),
            ResetTarget::Positions(all) => all
                .iter()
                .map(|(name, at)| {
                    let s = segment(name)?;
                    let to = position(at);
                    if to > state.head(source, s) {
                        return Err(EventStoreError::Invalid(format!("{at} past the head")));
                    }
                    Ok(range(s, to))
                })
                .collect::<Result<_, _>>()?,
        })
    }

    fn apply(&self, ranges: &[ReplayRange]) -> Result<(), EventStoreError> {
        let mut state = self.log.state();
        let group = state.groups.entry(self.spec.clone()).or_default();
        if !group.members.is_empty() {
            return Err(EventStoreError::SubscribersActive(format!(
                "{:?}",
                group.members
            )));
        }
        for r in ranges {
            group
                .committed
                .insert(segment(&r.segment)?, position(&r.from));
        }
        Ok(())
    }
}

// ---- the message store: processed-event markers only --------------------------------------------

/// A message store keeping committed markers only (each record commits at once); the event-store
/// kit uses nothing else.
#[derive(Default)]
struct Markers(Arc<Mutex<HashSet<String>>>);

impl MessageStore for Markers {
    fn name(&self) -> &'static str {
        "kit-markers"
    }

    fn init_schema<'a>(&'a self, _config: &'a Config) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn use_clock(&self, _clock: Clock) {}

    fn tx(&self) -> Arc<dyn TxProvider> {
        Arc::new(NoTx)
    }

    fn processed_events(&self) -> Arc<dyn ProcessedEventStore> {
        Arc::new(MarkersRef(Arc::clone(&self.0)))
    }

    fn outbox(&self) -> Arc<dyn OutboxStore> {
        unimplemented!("not used by the event-store kit")
    }

    fn dead_letters(&self) -> Arc<dyn DeadLetterStore> {
        unimplemented!("not used by the event-store kit")
    }

    fn processor_control(&self) -> Arc<dyn ProcessorControlStore> {
        unimplemented!("not used by the event-store kit")
    }

    fn external_calls(&self) -> Arc<dyn ExternalCallStore> {
        unimplemented!("not used by the event-store kit")
    }

    fn sagas(&self) -> SagaStores {
        unimplemented!("not used by the event-store kit")
    }
}

struct NoTx;

impl TxProvider for NoTx {
    fn begin(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(std::future::ready(Ok(Tx::none())))
    }

    fn plain(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(std::future::ready(Ok(Tx::none())))
    }

    fn commit<'a>(&'a self, _tx: &'a mut Tx) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn abort<'a>(&'a self, _tx: &'a mut Tx) -> BoxFuture<'a, ()> {
        Box::pin(std::future::ready(()))
    }
}

struct MarkersRef(Arc<Mutex<HashSet<String>>>);

impl ProcessedEventStore for MarkersRef {
    fn record<'a>(
        &'a self,
        _tx: &'a mut Tx,
        marker: &'a ProcessedMarker,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let fresh = self.0.lock().unwrap().insert(marker.id.clone());
        Box::pin(std::future::ready(Ok(fresh)))
    }

    fn exists<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>> {
        let found = self.0.lock().unwrap().contains(id);
        Box::pin(std::future::ready(Ok(found)))
    }

    fn delete_markers<'a>(
        &'a self,
        _processor: &'a str,
        _ranges: &'a [ReplayRange],
    ) -> BoxFuture<'a, Result<u64, StoreError>> {
        unimplemented!("not used by the event-store kit")
    }

    fn purge(&self, _before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        unimplemented!("not used by the event-store kit")
    }
}
