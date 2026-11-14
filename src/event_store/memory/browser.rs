//! The in-memory event store's browser: reads the store, never changes it (no member, no
//! committed position of its own).

use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;

use super::{Record, Shared, parse_index};
use crate::event_store::browse::{
    BrowseError, Chunks, EndReason, EventBrowser, EventRecord, EventSummary, Examined, Filters,
    GetRequest, LagRequest, LagView, Range, RecordView, SearchPage, SearchPlan, SearchRequest,
    SegmentBounds, SegmentLag, SourceView, SourcesRequest, SourcesView, SubscriptionLag,
    TailRequest, TailStart, TailStream, Want, heartbeat_period, merge, tail_channel,
};

/// How often a tail with nothing new checks that its reader is still there.
const TAIL_CHECK: Duration = Duration::from_millis(250);

pub(super) struct MemoryBrowser {
    shared: Arc<Shared>,
}

impl MemoryBrowser {
    pub(super) fn new(shared: Arc<Shared>) -> MemoryBrowser {
        MemoryBrowser { shared }
    }
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

/// A source's segments read by a search.
struct Slices<'a> {
    source: &'a str,
    log: &'a [Vec<Arc<Record>>],
    filters: &'a Filters,
}

impl Chunks for Slices<'_> {
    type Error = BrowseError;

    fn read(
        &mut self,
        wants: &[Want],
        _deadline: Instant,
    ) -> Result<Vec<Option<Vec<Examined>>>, BrowseError> {
        wants
            .iter()
            .map(|w| {
                let records = parse_index(&w.segment)
                    .and_then(|s| self.log.get(s))
                    .ok_or_else(|| BrowseError::Invalid(format!("no segment {}", w.segment)))?;
                let end = w.end.min(records.len() as i64);
                let read = (w.start.max(0)..end)
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
                Ok(Some(read))
            })
            .collect()
    }
}

impl MemoryBrowser {
    fn search_now(&self, request: &SearchRequest) -> Result<SearchPage, BrowseError> {
        let plan = SearchPlan::new(request, &self.shared.browse)?;
        let started = Instant::now();
        let state = self.shared.state();
        let Some(log) = state.sources.get(&plan.source) else {
            return Ok(plan.missing());
        };
        // Times rise within a segment: `from` and `to` bound each range.
        let ranges: Vec<Range> = log
            .iter()
            .enumerate()
            .map(|(s, records)| {
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
            log,
            filters: &plan.filters,
        };
        Ok(plan.answer(merge(&mut reader, &ranges, plan.budget(started))?))
    }

    fn get_now(&self, request: &GetRequest) -> Result<EventRecord, BrowseError> {
        let settings = &self.shared.browse;
        let source = settings.source(request.source.as_deref())?;
        let (segment, position) = request.place()?;
        let s = self
            .shared
            .segment(segment)
            .map_err(|e| BrowseError::Invalid(e.to_string()))?;
        let at = parse_index(position).ok_or_else(|| {
            BrowseError::Invalid(format!(
                "{} is not a position of the in-memory event store",
                crate::event_store::browse::quoted(position)
            ))
        })?;
        let state = self.shared.state();
        let record = state
            .sources
            .get(&source)
            .and_then(|log| log[s].get(at))
            .ok_or_else(|| BrowseError::NotFound(format!("no event at {at} of segment {s}")))?;
        let payload = request.payload.then_some(settings.max_payload);
        Ok(record.view(|v| EventRecord::of(&source, segment, position, v, payload)))
    }

    fn sources_now(&self) -> SourcesView {
        let state = self.shared.state();
        let sources = self
            .shared
            .browse
            .sources
            .iter()
            .map(|source| match state.sources.get(source) {
                None => SourceView {
                    source: source.clone(),
                    missing: true,
                    segments: Vec::new(),
                },
                Some(log) => SourceView {
                    source: source.clone(),
                    missing: false,
                    segments: log
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
        let source = self.shared.browse.source(request.source.as_deref())?;
        let state = self.shared.state();
        let Some(log) = state.sources.get(&source) else {
            return Ok(LagView {
                source,
                missing: true,
                subscriptions: Vec::new(),
                truncated: false,
            });
        };
        let mut subscriptions: Vec<SubscriptionLag> = state
            .groups
            .iter()
            .filter(|(spec, group)| spec.source == source && !group.committed.is_empty())
            .map(|(spec, group)| {
                let segments: Vec<SegmentLag> = log
                    .iter()
                    .enumerate()
                    .map(|(s, records)| {
                        let head = records.len() as u64;
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
        let settings = &self.shared.browse;
        let source = settings.source(request.source.as_deref())?;
        let filters = Filters::parse(&request.filters, true)?;
        let duration = settings.tail_duration(request.max_duration_ms)?;
        let mut heads: Vec<usize> = {
            let state = self.shared.state();
            let log = state
                .sources
                .get(&source)
                .ok_or_else(|| BrowseError::NotFound(format!("source {source} does not exist")))?;
            log.iter().map(Vec::len).collect()
        };
        let slot = Slot::take(&self.shared).ok_or_else(|| {
            BrowseError::Busy(format!(
                "{} tails are open already; try again shortly",
                settings.max_tails
            ))
        })?;
        let ends = self.shared.clock.now() + duration;
        let (mut feed, stream) =
            tail_channel(settings.tail_buffer, heartbeat_period(request.heartbeat_ms));
        feed.start(TailStart {
            source: source.clone(),
            segments: heads.len() as u64,
            ends_at_ms: crate::time::epoch_ms(ends),
        });
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            let _slot = slot;
            let deadline = tokio::time::Instant::now() + duration;
            loop {
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let new = {
                    let state = shared.state();
                    let log = &state.sources[&source];
                    let mut new = Vec::new();
                    for (s, head) in heads.iter_mut().enumerate() {
                        for (at, record) in log[s].iter().enumerate().skip(*head) {
                            if let Some(summary) = record.view(|v| {
                                filters.matches(v).then(|| {
                                    EventSummary::of(&source, &s.to_string(), &at.to_string(), v)
                                })
                            }) {
                                new.push(summary);
                            }
                        }
                        *head = log[s].len();
                    }
                    new
                };
                for summary in new {
                    if !feed.record(summary) {
                        return;
                    }
                }
                if feed.is_closed() {
                    return;
                }
                tokio::select! {
                    _ = changed => {}
                    _ = tokio::time::sleep(TAIL_CHECK) => {}
                    _ = tokio::time::sleep_until(deadline) => {
                        feed.end(EndReason::MaxDuration);
                        return;
                    }
                }
            }
        });
        Ok(stream)
    }
}

/// One of the `max_tails` slots, given back when dropped.
struct Slot(Arc<Shared>);

impl Slot {
    fn take(shared: &Arc<Shared>) -> Option<Slot> {
        let max = shared.browse.max_tails;
        shared
            .tails
            .try_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |n| {
                (n < max).then_some(n + 1)
            })
            .ok()
            .map(|_| Slot(Arc::clone(shared)))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.tails.fetch_sub(1, AtomicOrdering::AcqRel);
    }
}

impl EventBrowser for MemoryBrowser {
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

    /// Needs a tokio runtime (the tail's task).
    fn tail(&self, request: TailRequest) -> BoxFuture<'_, Result<TailStream, BrowseError>> {
        Box::pin(async move { self.tail_now(&request) })
    }
}
