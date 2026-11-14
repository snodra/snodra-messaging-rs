//! The in-memory event store's browser: reads the store, never changes it (no member, no
//! committed position of its own).

use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;

use super::{Record, Shared, parse_index};
use crate::event_store::browse::CHUNK;
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

/// A source's segments read by a search, a chunk at a time: the store's lock is held only to copy
/// the chunk's records (cheap: they are shared), never while they are examined.
struct Slices<'a> {
    shared: &'a Shared,
    source: &'a str,
    filters: &'a Filters,
}

impl Slices<'_> {
    /// The records of `segment` from `start` up to `end`, copied [`CHUNK`] at a time, the lock
    /// released between copies.
    fn copy(&self, segment: usize, start: usize, end: usize) -> Vec<Arc<Record>> {
        let mut copied = Vec::with_capacity(end.saturating_sub(start));
        let mut at = start;
        while at < end {
            let upto = (at + CHUNK).min(end);
            let state = self.shared.state();
            let Some(records) = state
                .sources
                .get(self.source)
                .and_then(|log| log.get(segment))
            else {
                break;
            };
            let upto = upto.min(records.len());
            if at >= upto {
                break;
            }
            copied.extend_from_slice(&records[at..upto]);
            at = upto;
        }
        copied
    }
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
                let segment = parse_index(&w.segment)
                    .filter(|s| *s < self.shared.segments)
                    .ok_or_else(|| BrowseError::Invalid(format!("no segment {}", w.segment)))?;
                let start = w.start.max(0) as usize;
                let records = self.copy(segment, start, w.end.max(0) as usize);
                let read = records
                    .iter()
                    .enumerate()
                    .map(|(i, r)| {
                        let at = (start + i) as i64;
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

/// A search page: the ranges are bounded under the lock (a binary search per segment), the
/// records read a chunk at a time ([`Slices`]).
fn search_now(shared: &Shared, request: &SearchRequest) -> Result<SearchPage, BrowseError> {
    let plan = SearchPlan::new(request, &shared.browse)?;
    let started = Instant::now();
    let ranges: Vec<Range> = {
        let state = shared.state();
        let Some(log) = state.sources.get(&plan.source) else {
            return Ok(plan.missing());
        };
        // Times rise within a segment: `from` and `to` bound each range.
        log.iter()
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
            .collect()
    };
    let mut reader = Slices {
        shared,
        source: &plan.source,
        filters: &plan.filters,
    };
    Ok(plan.answer(merge(&mut reader, &ranges, plan.budget(started))?))
}

impl MemoryBrowser {
    /// One of the `max_searches` slots (searches, record reads, sources, lag), else busy.
    fn search_slot(&self) -> Result<Slot, BrowseError> {
        Slot::take(&self.shared, Kind::Search).ok_or_else(|| {
            BrowseError::Busy(format!(
                "{} searches are running already; try again shortly",
                self.shared.browse.max_searches
            ))
        })
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
        let slot = Slot::take(&self.shared, Kind::Tail).ok_or_else(|| {
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
        let cap = settings.tail_buffer;
        tokio::spawn(async move {
            let _slot = slot;
            let deadline = tokio::time::Instant::now() + duration;
            loop {
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                // At most a buffer's worth per pass, copied under the lock, examined after it.
                let (taken, more) = {
                    let state = shared.state();
                    let log = &state.sources[&source];
                    let mut taken = Vec::new();
                    for (s, head) in heads.iter_mut().enumerate() {
                        let room = cap - taken.len();
                        let upto = log[s].len().min(*head + room);
                        taken.extend(
                            log[s][*head..upto]
                                .iter()
                                .enumerate()
                                .map(|(i, r)| (s, *head + i, Arc::clone(r))),
                        );
                        *head = upto;
                    }
                    let more = heads.iter().enumerate().any(|(s, h)| *h < log[s].len());
                    (taken, more)
                };
                for (s, at, record) in taken {
                    let summary = record.view(|v| {
                        filters
                            .matches(v)
                            .then(|| EventSummary::of(&source, &s.to_string(), &at.to_string(), v))
                    });
                    if let Some(summary) = summary
                        && !feed.record(summary)
                    {
                        return;
                    }
                }
                if more {
                    tokio::task::yield_now().await;
                    continue;
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

/// What a [`Slot`] counts.
#[derive(Clone, Copy)]
enum Kind {
    Search,
    Tail,
}

/// One of the `max_searches` or `max_tails` slots, given back when dropped.
struct Slot(Arc<Shared>, Kind);

impl Slot {
    fn counter(shared: &Shared, kind: Kind) -> (&std::sync::atomic::AtomicUsize, usize) {
        match kind {
            Kind::Search => (&shared.searches, shared.browse.max_searches),
            Kind::Tail => (&shared.tails, shared.browse.max_tails),
        }
    }

    fn take(shared: &Arc<Shared>, kind: Kind) -> Option<Slot> {
        let (count, max) = Slot::counter(shared, kind);
        count
            .try_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |n| {
                (n < max).then_some(n + 1)
            })
            .ok()
            .map(|_| Slot(Arc::clone(shared), kind))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        Slot::counter(&self.0, self.1)
            .0
            .fetch_sub(1, AtomicOrdering::AcqRel);
    }
}

impl EventBrowser for MemoryBrowser {
    /// Runs on a blocking thread (a page may read many records), holding a search slot: needs a
    /// tokio runtime.
    fn search(&self, request: SearchRequest) -> BoxFuture<'_, Result<SearchPage, BrowseError>> {
        Box::pin(async move {
            let slot = self.search_slot()?;
            let shared = Arc::clone(&self.shared);
            tokio::task::spawn_blocking(move || {
                let _slot = slot;
                search_now(&shared, &request)
            })
            .await
            .map_err(|e| BrowseError::Unavailable(format!("the search stopped: {e}")))?
        })
    }

    fn get(&self, request: GetRequest) -> BoxFuture<'_, Result<EventRecord, BrowseError>> {
        Box::pin(async move {
            let _slot = self.search_slot()?;
            self.get_now(&request)
        })
    }

    fn sources(&self, _request: SourcesRequest) -> BoxFuture<'_, Result<SourcesView, BrowseError>> {
        Box::pin(async move {
            let _slot = self.search_slot()?;
            Ok(self.sources_now())
        })
    }

    fn lag(&self, request: LagRequest) -> BoxFuture<'_, Result<LagView, BrowseError>> {
        Box::pin(async move {
            let _slot = self.search_slot()?;
            self.lag_now(&request)
        })
    }

    /// Needs a tokio runtime (the tail's task).
    fn tail(&self, request: TailRequest) -> BoxFuture<'_, Result<TailStream, BrowseError>> {
        Box::pin(async move { self.tail_now(&request) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_store::EventStore;
    use crate::event_store::browse::BrowseSettings;
    use crate::event_store::memory::{MemoryEventStore, MemoryEventStoreConfig};

    /// At `max_searches` calls running, one more of any read kind is busy; a slot given back
    /// lets the next one in.
    #[tokio::test]
    async fn reads_are_busy_at_the_bound() {
        let events = MemoryEventStore::new(MemoryEventStoreConfig {
            browse: BrowseSettings {
                max_searches: 2,
                ..BrowseSettings::default()
            },
            ..MemoryEventStoreConfig::default()
        })
        .unwrap();
        let browser = events.browser().unwrap();
        let held = [
            Slot::take(&events.shared, Kind::Search).unwrap(),
            Slot::take(&events.shared, Kind::Search).unwrap(),
        ];
        assert!(matches!(
            browser.search(SearchRequest::default()).await,
            Err(BrowseError::Busy(_))
        ));
        assert!(matches!(
            browser.sources(SourcesRequest {}).await,
            Err(BrowseError::Busy(_))
        ));
        assert!(matches!(
            browser.lag(LagRequest::default()).await,
            Err(BrowseError::Busy(_))
        ));
        assert!(matches!(
            browser
                .get(GetRequest {
                    segment: "0".into(),
                    position: "0".into(),
                    ..GetRequest::default()
                })
                .await,
            Err(BrowseError::Busy(_))
        ));
        drop(held);
        assert!(browser.search(SearchRequest::default()).await.is_ok());
        assert!(browser.sources(SourcesRequest {}).await.is_ok());
    }
}
