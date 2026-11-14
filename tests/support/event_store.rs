//! An in-process event store (a test double, not the in-memory event store add-on, which comes
//! later): sources of [`SEGMENTS`] segments, subscriptions sharing them round robin, committed
//! positions per subscription name, an admin and a browser. One [`Log`] (and its clones) is one
//! event store shared by every "instance" of a test. The event-store kit proves it
//! (`tests/event_store_kit.rs`).

use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use kraken_messaging::event_store::browse::{
    BrowseError, BrowseSettings, Chunks, EndReason, EventBrowser, EventRecord, EventSummary,
    Examined, Filters, GetRequest, LagRequest, LagView, Range, RecordView, SearchPage, SearchPlan,
    SearchRequest, SegmentBounds, SegmentLag, SourceView, SourcesRequest, SourcesView,
    SubscriptionLag, TailRequest, TailStart, TailStream, Want, heartbeat_period, merge,
    tail_channel,
};
use kraken_messaging::event_store::{
    Capability, Delivery, EventStore, EventStoreError, OutgoingEvent, ReplayRange, ResetTarget,
    SegmentStatus, Subscription, SubscriptionAdmin, SubscriptionEvent, SubscriptionSpec,
};

/// Segments per source.
pub const SEGMENTS: usize = 2;

struct Record {
    key: Option<String>,
    headers: HashMap<String, String>,
    payload: Vec<u8>,
    timestamp_ms: i64,
}

impl Record {
    /// Calls `f` with the record as the browser reads it.
    fn view<T>(&self, f: impl FnOnce(&RecordView<'_>) -> T) -> T {
        let headers: Vec<(&[u8], Option<&[u8]>)> = self
            .headers
            .iter()
            .map(|(name, value)| (name.as_bytes(), Some(value.as_bytes())))
            .collect();
        f(&RecordView {
            timestamp: Some(self.timestamp_ms),
            key: self.key.as_deref().map(str::as_bytes),
            headers: &headers,
            payload: Some(&self.payload),
        })
    }
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
    /// How many more `next` calls fail (a broken subscription).
    failures: usize,
    /// Every subscription made, in order.
    subscribed: Vec<SubscriptionSpec>,
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

/// A call of the double that never answers.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Stuck {
    Publish,
    Close,
}

/// The event store: one shared state for every "instance".
#[derive(Clone)]
pub struct Log {
    state: Arc<Mutex<State>>,
    capabilities: Vec<Capability>,
    stuck: Option<Stuck>,
    browse: BrowseSettings,
}

impl Log {
    pub fn new(capabilities: Vec<Capability>) -> Log {
        Log {
            state: Arc::default(),
            capabilities,
            stuck: None,
            browse: BrowseSettings::default(),
        }
    }

    /// The browser reads these sources.
    pub fn browsing(self, sources: &[&str]) -> Log {
        Log {
            browse: BrowseSettings {
                sources: sources.iter().map(|s| s.to_string()).collect(),
                ..self.browse
            },
            ..self
        }
    }

    pub fn stuck(self, stuck: Stuck) -> Log {
        Log {
            stuck: Some(stuck),
            ..self
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// The next `n` calls of `next` (of any subscription) fail: the subscription is broken.
    pub fn fail_next(&self, n: usize) {
        self.state().failures = n;
    }

    /// How many subscriptions were made of `name`.
    pub fn subscriptions_of(&self, name: &str) -> usize {
        self.state()
            .subscribed
            .iter()
            .filter(|s| s.name == name)
            .count()
    }

    /// The members reading `spec` now.
    pub fn members(&self, spec: &SubscriptionSpec) -> usize {
        self.state().groups.get(spec).map_or(0, |g| g.members.len())
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
        &self.capabilities
    }

    fn publish<'a>(
        &'a self,
        event: &'a OutgoingEvent,
    ) -> BoxFuture<'a, Result<(), EventStoreError>> {
        if self.stuck == Some(Stuck::Publish) {
            return Box::pin(std::future::pending());
        }
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
        state.subscribed.push(spec.clone());
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
        if !self.supports(Capability::Replay) {
            return Err(EventStoreError::Unsupported(Capability::Replay));
        }
        Ok(Box::new(Admin {
            log: self.clone(),
            spec: spec.clone(),
        }))
    }

    fn compare(&self, a: &str, b: &str) -> Ordering {
        position(a).cmp(&position(b))
    }

    fn browser(&self) -> Option<Arc<dyn EventBrowser>> {
        self.supports(Capability::Browse)
            .then(|| Arc::new(LogBrowser { log: self.clone() }) as Arc<dyn EventBrowser>)
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
                {
                    let mut state = self.log.state();
                    if state.failures > 0 {
                        state.failures -= 1;
                        return Err(EventStoreError::Failed("the double broke".into()));
                    }
                }
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
                    // Known from a committed position only, as a store may report it.
                    lag: done
                        .filter(|_| self.log.supports(Capability::Lag))
                        .map(|committed| head - committed),
                }
            })
            .collect()
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        if self.log.stuck == Some(Stuck::Close) {
            return Box::pin(std::future::pending());
        }
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

// ---- the browser -------------------------------------------------------------------------------

/// The double's browser: reads the shared state, never changes it.
struct LogBrowser {
    log: Log,
}

/// A source's segments read by a search.
struct Slices<'a> {
    source: &'a str,
    segments: &'a [Vec<Record>],
    filters: &'a Filters,
}

impl Chunks for Slices<'_> {
    type Error = BrowseError;

    fn read(
        &mut self,
        wants: &[Want],
        _deadline: Instant,
    ) -> Result<Vec<Option<Vec<Examined>>>, BrowseError> {
        Ok(wants
            .iter()
            .map(|w| {
                let records = &self.segments[segment(&w.segment).expect("a segment it listed")];
                let read = (w.start.max(0)..w.end.min(records.len() as i64))
                    .map(|at| {
                        let r = &records[at as usize];
                        Examined {
                            position: at,
                            timestamp: r.timestamp_ms,
                            hit: r.view(|v| {
                                self.filters.matches(v).then(|| {
                                    EventSummary::of(self.source, &w.segment, &at.to_string(), v)
                                })
                            }),
                        }
                    })
                    .collect();
                Some(read)
            })
            .collect())
    }
}

fn not_found(what: String) -> BrowseError {
    BrowseError::NotFound(what)
}

impl LogBrowser {
    fn settings(&self) -> &BrowseSettings {
        &self.log.browse
    }

    fn search_now(&self, request: &SearchRequest) -> Result<SearchPage, BrowseError> {
        let plan = SearchPlan::new(request, self.settings())?;
        let started = Instant::now();
        let state = self.log.state();
        let Some(segments) = state.sources.get(&plan.source) else {
            return Ok(plan.missing());
        };
        // The double's times rise within a segment: `from` and `to` bound each range.
        let ranges: Vec<Range> = (0..SEGMENTS)
            .map(|s| {
                let records = &segments[s];
                let lower = plan
                    .filters
                    .from
                    .map_or(0, |from| records.partition_point(|r| r.timestamp_ms < from))
                    as i64;
                let upper = plan.filters.to.map_or(records.len(), |to| {
                    records.partition_point(|r| r.timestamp_ms <= to)
                }) as i64;
                let name = s.to_string();
                Range {
                    upper: plan.upper(&name, lower, upper),
                    segment: name,
                    lower,
                }
            })
            .collect();
        let mut reader = Slices {
            source: &plan.source,
            segments,
            filters: &plan.filters,
        };
        Ok(plan.answer(merge(&mut reader, &ranges, plan.budget(started))?))
    }

    fn get_now(&self, request: &GetRequest) -> Result<EventRecord, BrowseError> {
        let source = self.settings().source(request.source.as_deref())?;
        let (segment_name, position) = request.place()?;
        let s = segment(segment_name).map_err(|e| BrowseError::Invalid(e.to_string()))?;
        let at: usize = position
            .parse()
            .map_err(|_| BrowseError::Invalid("not a position of the double".into()))?;
        let state = self.log.state();
        let record = state
            .sources
            .get(&source)
            .and_then(|segments| segments[s].get(at))
            .ok_or_else(|| not_found(format!("no event at {at} of segment {s}")))?;
        let payload = request.payload.then_some(self.settings().max_payload);
        Ok(record
            .view(|v| EventRecord::of(&source, &request.segment, &request.position, v, payload)))
    }

    fn sources_now(&self) -> SourcesView {
        let state = self.log.state();
        let sources = self
            .settings()
            .sources
            .iter()
            .map(|source| match state.sources.get(source) {
                None => SourceView {
                    source: source.clone(),
                    missing: true,
                    segments: Vec::new(),
                },
                Some(segments) => SourceView {
                    source: source.clone(),
                    missing: false,
                    segments: segments
                        .iter()
                        .enumerate()
                        .map(|(s, records)| SegmentBounds {
                            segment: s.to_string(),
                            first: "0".to_string(),
                            head: records.len().to_string(),
                        })
                        .collect(),
                },
            })
            .collect();
        SourcesView { sources }
    }

    fn lag_now(&self, request: &LagRequest) -> Result<LagView, BrowseError> {
        let source = self.settings().source(request.source.as_deref())?;
        let state = self.log.state();
        if !state.sources.contains_key(&source) {
            return Ok(LagView {
                source,
                missing: true,
                subscriptions: Vec::new(),
                truncated: false,
            });
        }
        let mut subscriptions: Vec<SubscriptionLag> = state
            .groups
            .iter()
            .filter(|(spec, group)| spec.source == source && !group.committed.is_empty())
            .map(|(spec, group)| {
                let segments: Vec<SegmentLag> = (0..SEGMENTS)
                    .map(|s| {
                        let head = state.head(&source, s);
                        let committed = group.committed.get(&s).copied();
                        SegmentLag {
                            segment: s.to_string(),
                            committed: committed.map(|c| c.to_string()),
                            head: head.to_string(),
                            lag: committed.map(|c| head.saturating_sub(c)),
                        }
                    })
                    .collect();
                SubscriptionLag {
                    subscription: spec.name.clone(),
                    state: if group.members.is_empty() {
                        "Empty"
                    } else {
                        "Stable"
                    }
                    .to_string(),
                    members: group.members.len() as u64,
                    lag: segments.iter().filter_map(|s| s.lag).sum(),
                    segments,
                }
            })
            .collect();
        subscriptions.sort_by(|a, b| {
            b.lag
                .cmp(&a.lag)
                .then_with(|| a.subscription.cmp(&b.subscription))
        });
        Ok(LagView {
            source,
            missing: false,
            subscriptions,
            truncated: false,
        })
    }

    fn tail_now(&self, request: &TailRequest) -> Result<TailStream, BrowseError> {
        let settings = self.settings();
        let source = settings.source(request.source.as_deref())?;
        let filters = Filters::parse(&request.filters, true)?;
        let duration = settings.tail_duration(request.max_duration_ms)?;
        let mut heads: Vec<usize> = {
            let state = self.log.state();
            let segments = state
                .sources
                .get(&source)
                .ok_or_else(|| not_found(format!("source {source} does not exist")))?;
            segments.iter().map(Vec::len).collect()
        };
        let ends = std::time::SystemTime::now() + duration;
        let start = TailStart {
            source: source.clone(),
            segments: SEGMENTS as u64,
            ends_at_ms: ends
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64),
        };
        let (mut feed, stream) =
            tail_channel(settings.tail_buffer, heartbeat_period(request.heartbeat_ms));
        feed.start(start);
        let log = self.log.clone();
        tokio::spawn(async move {
            let deadline = Instant::now() + duration;
            loop {
                if feed.is_closed() {
                    return;
                }
                if Instant::now() >= deadline {
                    feed.end(EndReason::MaxDuration);
                    return;
                }
                let mut new = Vec::new();
                {
                    let state = log.state();
                    let segments = &state.sources[&source];
                    for (s, head) in heads.iter_mut().enumerate() {
                        for (at, record) in segments[s].iter().enumerate().skip(*head) {
                            if let Some(summary) = record.view(|v| {
                                filters.matches(v).then(|| {
                                    EventSummary::of(&source, &s.to_string(), &at.to_string(), v)
                                })
                            }) {
                                new.push(summary);
                            }
                        }
                        *head = segments[s].len();
                    }
                }
                for summary in new {
                    if !feed.record(summary) {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        Ok(stream)
    }
}

impl EventBrowser for LogBrowser {
    fn search(&self, request: SearchRequest) -> BoxFuture<'_, Result<SearchPage, BrowseError>> {
        Box::pin(std::future::ready(self.search_now(&request)))
    }

    fn get(&self, request: GetRequest) -> BoxFuture<'_, Result<EventRecord, BrowseError>> {
        Box::pin(std::future::ready(self.get_now(&request)))
    }

    fn sources(&self, _request: SourcesRequest) -> BoxFuture<'_, Result<SourcesView, BrowseError>> {
        Box::pin(std::future::ready(Ok(self.sources_now())))
    }

    fn lag(&self, request: LagRequest) -> BoxFuture<'_, Result<LagView, BrowseError>> {
        Box::pin(std::future::ready(self.lag_now(&request)))
    }

    fn tail(&self, request: TailRequest) -> BoxFuture<'_, Result<TailStream, BrowseError>> {
        Box::pin(async move { self.tail_now(&request) })
    }
}
