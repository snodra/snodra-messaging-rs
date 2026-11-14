//! The RabbitMQ Streams event browser (behind the core's [`EventBrowser`]): search, one event,
//! the sources, the subscriptions' lag and the tail, on the sources of
//! [`BrowseSettings::sources`] only. A source is a super stream (or a plain stream), a segment a
//! partition's suffix (`0` for a plain stream), a position an offset in decimal.
//!
//! The browser reads and never writes: it reads with consumers that have no name (nothing is
//! stored, no single active consumer is joined), so no subscription of the browser's exists and
//! no subscription's offsets move; the lag view only queries the offsets stored under each name.
//! Every call runs on a task of its own (a search and a record read on the blocking pool, which
//! [`merge`] needs) that holds the call's slot and its connections and closes them itself, under
//! a deadline (the settings' timeout, or the request's): a caller that leaves frees nothing
//! while RabbitMQ work still runs for it. Calls are bounded in number
//! ([`BrowseSettings::max_searches`], [`BrowseSettings::max_tails`]: one more is
//! [`BrowseError::Busy`]), in records and in time. Nothing connects before the first call.
//!
//! - **Time bounds** (`from`, `to`): the stream's timestamp offset specification finds the first
//!   chunk written at or after a time; a record's time is its chunk's (the leader's clock when it
//!   wrote it).
//! - **Lag**: the stream protocol cannot list the names with a stored offset, so the view lists
//!   the names this process subscribed with, the ones the management API shows reading the
//!   source (when configured; once seen, they are kept), and
//!   [`RabbitConfig::known_subscriptions`](crate::RabbitConfig::known_subscriptions), each
//!   with a stored offset. A lag counts offsets, which also number the streams' offset-tracking
//!   entries: it may overstate.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::future::BoxFuture;
use kraken_messaging::event_store::EventStoreError;
use kraken_messaging::event_store::browse::{
    BrowseError, BrowseSettings, Chunks, EndReason, EventBrowser, EventRecord, EventSummary,
    Examined, Filters, GetRequest, LagRequest, LagView, Range, RecordView, SearchPage, SearchPlan,
    SearchRequest, SegmentBounds, SegmentLag, SourceView, SourcesRequest, SourcesView,
    SubscriptionLag, TailRequest, TailStart, TailStream, Want, heartbeat_period, merge, quoted,
    tail_channel,
};
use rabbitmq_stream_client::types::{Message, OffsetSpecification};
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use crate::config::RabbitConfig;
use crate::conn::{Broker, Conn, Nodes, Part, lock};
use crate::management::Management;
use crate::read::{self, Bounds, ChunkReader, EmptyCache};
use crate::wire;

/// The lag view lists at most this many subscriptions (`truncated` beyond).
pub const MAX_SUBSCRIPTIONS: usize = 500;
/// Time a call gets beyond its deadline to close its connections before the caller gives up on
/// it (the call still ends on its own, holding its slot until then).
pub const CLOSE_GRACE: Duration = Duration::from_secs(5);
/// The connection name the browser's connections carry.
pub const BROWSER_CONNECTION: &str = "kraken-event-browser";

/// The subscription names a store's instances subscribed with, per source: what the lag view
/// lists besides the management API's (the stream protocol cannot list them).
#[derive(Clone, Default)]
pub(crate) struct Registry(Arc<Mutex<BTreeMap<String, BTreeSet<String>>>>);

impl Registry {
    pub fn add(&self, source: &str, name: &str) {
        lock(&self.0)
            .entry(source.to_string())
            .or_default()
            .insert(name.to_string());
    }

    fn names(&self, source: &str) -> BTreeSet<String> {
        lock(&self.0).get(source).cloned().unwrap_or_default()
    }
}

fn unavailable(e: EventStoreError) -> BrowseError {
    match e {
        EventStoreError::Invalid(m) => BrowseError::Invalid(m),
        other => BrowseError::Unavailable(format!("RabbitMQ is unavailable: {other}")),
    }
}

fn late() -> BrowseError {
    BrowseError::Unavailable("RabbitMQ did not answer in time".to_string())
}

/// The offset a position names.
fn offset_of(position: &str) -> Result<u64, BrowseError> {
    position.parse::<u64>().map_err(|_| {
        BrowseError::Invalid(format!(
            "position {} is not an offset (a non-negative number)",
            quoted(position)
        ))
    })
}

/// A tokio deadline for a std one.
fn until(deadline: Instant) -> tokio::time::Instant {
    tokio::time::Instant::from_std(deadline)
}

/// Calls `f` with the [`RecordView`] of `message`, written at `timestamp`.
fn with_view<T>(timestamp: i64, message: &Message, f: impl FnOnce(&RecordView<'_>) -> T) -> T {
    let key = wire::key_of(message);
    let owned = wire::headers_of(message);
    let headers: Vec<(&[u8], Option<&[u8]>)> = owned
        .iter()
        .map(|(n, v)| (n.as_slice(), v.as_deref()))
        .collect();
    f(&RecordView {
        timestamp: Some(timestamp),
        key: key.as_deref().map(str::as_bytes),
        headers: &headers,
        payload: message.data(),
    })
}

/// A source's partition streams with their leaders' connections and bounds.
struct Located {
    part: Part,
    conn: Arc<Conn>,
    bounds: Bounds,
}

/// What a call reads of each partition.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Need {
    /// The first offset and the head.
    Bounds,
    /// The head only (`first` reads 0).
    Head,
}

/// The partitions of `source` (only `segment` when given) with their bounds; `None` when the
/// source does not exist. With `cache`, a partition found empty lately is not read again.
async fn locate(
    nodes: &mut Nodes,
    source: &str,
    deadline: tokio::time::Instant,
    need: Need,
    segment: Option<&str>,
    cache: Option<&EmptyCache>,
) -> Result<Option<Vec<Located>>, EventStoreError> {
    let Some(mut parts) = nodes.layout(source).await? else {
        return Ok(None);
    };
    parts.retain(|p| segment.is_none_or(|s| p.segment == s));
    parts.sort_by(|a, b| kraken_messaging::event_store::segment_order(&a.segment, &b.segment));
    let mut conns = Vec::with_capacity(parts.len());
    for part in &parts {
        let conn = nodes.leader(&part.stream).await?.ok_or_else(|| {
            EventStoreError::Failed(format!("rabbitmq: the stream {} is gone", part.stream))
        })?;
        conns.push(conn);
    }
    let bounds = futures::future::join_all(parts.iter().zip(&conns).map(|(part, conn)| async {
        let read = async {
            match need {
                Need::Bounds => read::bounds(conn, &part.stream, deadline).await,
                Need::Head => read::head(conn, &part.stream, deadline).await,
            }
        };
        match cache {
            Some(cache) => cache.bounds(&part.stream, read).await,
            None => read.await,
        }
    }))
    .await;
    parts
        .into_iter()
        .zip(conns)
        .zip(bounds)
        .map(|((part, conn), bounds)| {
            Ok(Located {
                part,
                conn,
                bounds: bounds?,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Reads the chunks of a search, every segment's at once.
struct Reader<'a> {
    handle: &'a Handle,
    located: &'a [Located],
    source: &'a str,
    filters: &'a Filters,
}

impl Chunks for Reader<'_> {
    type Error = BrowseError;

    fn read(
        &mut self,
        wants: &[Want],
        deadline: Instant,
    ) -> Result<Vec<Option<Vec<Examined>>>, BrowseError> {
        let deadline = until(deadline);
        let (located, source, filters) = (self.located, self.source, self.filters);
        let reads = wants.iter().map(|w| async move {
            let at = located
                .iter()
                .find(|l| l.part.segment == w.segment)
                .ok_or_else(|| BrowseError::Invalid(format!("no segment {}", w.segment)))?;
            let mut examined = Vec::new();
            let complete = read::read_range(
                &at.conn,
                &at.part.stream,
                w.start.max(0) as u64,
                w.end.max(0) as u64,
                deadline,
                |offset, timestamp, message| {
                    let hit = with_view(timestamp, message, |r| {
                        filters
                            .matches(r)
                            .then(|| EventSummary::of(source, &w.segment, &offset.to_string(), r))
                    });
                    examined.push(Examined {
                        position: offset as i64,
                        timestamp,
                        hit,
                    });
                },
            )
            .await
            .map_err(unavailable)?;
            Ok::<_, BrowseError>(complete.then_some(examined))
        });
        self.handle
            .block_on(futures::future::join_all(reads))
            .into_iter()
            .collect()
    }
}

/// The RabbitMQ Streams event store's browser
/// ([`RabbitEventStore::browser`](crate::RabbitEventStore)).
pub struct RabbitBrowser {
    broker: Broker,
    settings: BrowseSettings,
    management: Option<Management>,
    registry: Registry,
    /// Partitions found empty lately (not read again for a few seconds).
    empty: EmptyCache,
    reads: Arc<Semaphore>,
    tails: Arc<Semaphore>,
}

impl RabbitBrowser {
    /// A browser alone, without the rest of the store (no publisher): what a program that only
    /// reads the event store needs. Checks `config` as
    /// [`RabbitEventStore::new`](crate::RabbitEventStore::new) does and connects nothing before
    /// its first call.
    pub fn open(config: &RabbitConfig) -> Result<RabbitBrowser, EventStoreError> {
        config.validate()?;
        let config = Arc::new(config.clone());
        let management = Management::of(&config)?;
        let broker = Broker::new(Arc::clone(&config), BROWSER_CONNECTION)?;
        Ok(RabbitBrowser::new(broker, management, Registry::default()))
    }

    /// A browser of `broker`'s settings sharing the store's `registry`. Connects nothing.
    pub(crate) fn new(
        broker: Broker,
        management: Option<Management>,
        registry: Registry,
    ) -> RabbitBrowser {
        let settings = broker.config().browse.clone();
        for name in &broker.config().known_subscriptions {
            for source in &settings.sources {
                registry.add(source, name);
            }
        }
        RabbitBrowser {
            empty: EmptyCache::default(),
            reads: Arc::new(Semaphore::new(settings.max_searches)),
            tails: Arc::new(Semaphore::new(settings.max_tails)),
            broker,
            settings,
            management,
            registry,
        }
    }

    /// A slot for a search, a record read, or a sources or lag listing.
    fn read_slot(&self) -> Result<OwnedSemaphorePermit, BrowseError> {
        Arc::clone(&self.reads).try_acquire_owned().map_err(|_| {
            BrowseError::Busy(format!(
                "{} event searches are running on this browser; try again shortly",
                self.settings.max_searches
            ))
        })
    }

    /// Runs `f` on the blocking pool with its own connections, holding `slot` until it returns
    /// (the connections closed), and waits for it at most `budget` plus [`CLOSE_GRACE`].
    async fn blocking<T: Send + 'static>(
        &self,
        slot: OwnedSemaphorePermit,
        budget: Duration,
        f: impl FnOnce(&Handle, &mut Nodes) -> Result<T, BrowseError> + Send + 'static,
    ) -> Result<T, BrowseError> {
        let handle = Handle::current();
        let broker = self.broker.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _slot = slot;
            let mut nodes = Nodes::new(broker);
            let result = f(&handle, &mut nodes);
            handle.block_on(nodes.close());
            result
        });
        match tokio::time::timeout(budget + CLOSE_GRACE, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BrowseError::Unavailable(
                "the RabbitMQ read failed".to_string(),
            )),
            Err(_) => Err(late()),
        }
    }

    async fn search_page(&self, request: SearchRequest) -> Result<SearchPage, BrowseError> {
        let plan = SearchPlan::new(&request, &self.settings)?;
        let slot = self.read_slot()?;
        let empty = self.empty.clone();
        self.blocking(slot, plan.timeout, move |handle, nodes| {
            let started = Instant::now();
            let deadline = started + plan.timeout;
            let source = plan.source.as_str();
            let filters = &plan.filters;
            let located = handle.block_on(async {
                let Some(located) = locate(
                    nodes,
                    source,
                    until(deadline),
                    Need::Bounds,
                    None,
                    Some(&empty),
                )
                .await?
                else {
                    return Ok(None);
                };
                let mut ranges = Vec::with_capacity(located.len());
                for l in &located {
                    let (stream, bounds) = (&l.part.stream, &l.bounds);
                    let lower = match filters.from {
                        None => bounds.first,
                        Some(ts) => read::offset_at(&l.conn, stream, ts, bounds, until(deadline))
                            .await?
                            .max(bounds.first),
                    };
                    let upper = match filters.to {
                        None => bounds.head,
                        Some(ts) => read::offset_at(
                            &l.conn,
                            stream,
                            ts.saturating_add(1),
                            bounds,
                            until(deadline),
                        )
                        .await?
                        .min(bounds.head),
                    };
                    let (lower, upper) = (lower as i64, upper as i64);
                    ranges.push(Range {
                        upper: plan.upper(&l.part.segment, lower, upper),
                        segment: l.part.segment.clone(),
                        lower,
                    });
                }
                Ok::<_, EventStoreError>(Some((located, ranges)))
            });
            let Some((located, ranges)) = located.map_err(unavailable)? else {
                return Ok(plan.missing());
            };
            let mut reader = Reader {
                handle,
                located: &located,
                source,
                filters,
            };
            let page = merge(&mut reader, &ranges, plan.budget(started))?;
            Ok(plan.answer(page))
        })
        .await
    }

    async fn record(&self, request: GetRequest) -> Result<EventRecord, BrowseError> {
        let source = self.settings.source(request.source.as_deref())?;
        let (segment, position) = request.place()?;
        let (segment, offset) = (segment.to_string(), offset_of(position)?);
        let payload = request.payload.then_some(self.settings.max_payload);
        let slot = self.read_slot()?;
        let budget = self.settings.timeout;
        let empty = self.empty.clone();
        self.blocking(slot, budget, move |handle, nodes| {
            handle.block_on(async {
                let deadline = until(Instant::now() + budget);
                let Some(located) = locate(
                    nodes,
                    &source,
                    deadline,
                    Need::Head,
                    Some(&segment),
                    Some(&empty),
                )
                .await
                .map_err(unavailable)?
                else {
                    return Err(BrowseError::NotFound(format!("source {source} does not exist")));
                };
                let Some(at) = located.first() else {
                    return Err(BrowseError::NotFound(format!(
                        "source {source} has no segment {}",
                        quoted(&segment)
                    )));
                };
                let head = at.bounds.head;
                if offset >= head {
                    return Err(BrowseError::NotFound(format!(
                        "offset {offset} is past segment {segment} of {source} (its head is {head})"
                    )));
                }
                let mut found = None;
                let complete = read::read_range(
                    &at.conn,
                    &at.part.stream,
                    offset,
                    offset + 1,
                    deadline,
                    |at_offset, timestamp, message| {
                        if at_offset == offset {
                            found = Some(with_view(timestamp, message, |r| {
                                EventRecord::of(&source, &segment, position_text(offset).as_str(), r, payload)
                            }));
                        }
                    },
                )
                .await
                .map_err(unavailable)?;
                match (found, complete) {
                    (Some(record), _) => Ok(record),
                    (None, true) => Err(BrowseError::NotFound(format!(
                        "no event at offset {offset} of segment {segment} of {source} (an offset-tracking entry, or removed by retention)"
                    ))),
                    (None, false) => Err(late()),
                }
            })
        })
        .await
    }

    async fn streams(&self) -> Result<SourcesView, BrowseError> {
        let slot = self.read_slot()?;
        let budget = self.settings.timeout;
        let sources = self.settings.sources.clone();
        let empty = self.empty.clone();
        self.blocking(slot, budget, move |handle, nodes| {
            handle.block_on(async {
                let deadline = until(Instant::now() + budget);
                let mut views = Vec::with_capacity(sources.len());
                for source in sources {
                    let Some(located) =
                        locate(nodes, &source, deadline, Need::Bounds, None, Some(&empty))
                            .await
                            .map_err(unavailable)?
                    else {
                        views.push(SourceView {
                            source,
                            missing: true,
                            segments: Vec::new(),
                        });
                        continue;
                    };
                    views.push(SourceView {
                        source,
                        missing: false,
                        segments: located
                            .iter()
                            .map(|l| SegmentBounds {
                                segment: l.part.segment.clone(),
                                first: l.bounds.first.to_string(),
                                head: l.bounds.head.to_string(),
                            })
                            .collect(),
                    });
                }
                Ok(SourcesView { sources: views })
            })
        })
        .await
    }

    /// Every subscription name the browser knows of (see the module docs) with an offset stored
    /// on the source, by lag descending.
    async fn subscriptions(&self, request: LagRequest) -> Result<LagView, BrowseError> {
        let source = self.settings.source(request.source.as_deref())?;
        let slot = self.read_slot()?;
        let budget = self.settings.timeout;
        let management = self.management.clone();
        let registry = self.registry.clone();
        let empty = self.empty.clone();
        self.blocking(slot, budget, move |handle, nodes| {
            handle.block_on(async {
                let deadline = until(Instant::now() + budget);
                let Some(located) =
                    locate(nodes, &source, deadline, Need::Head, None, Some(&empty))
                        .await
                        .map_err(unavailable)?
                else {
                    return Ok(LagView {
                        source,
                        missing: true,
                        subscriptions: vec![],
                        truncated: false,
                    });
                };
                let streams: Vec<String> = located.iter().map(|l| l.part.stream.clone()).collect();
                // `None`: who reads is not known (no management API, or it failed).
                let live: Option<BTreeMap<String, BTreeSet<String>>> = match &management {
                    None => None,
                    Some(m) => match m.subscriptions(&streams).await {
                        Ok(live) => Some(live),
                        Err(e) => {
                            tracing::warn!("the lag view lists no live subscriptions: {e}");
                            None
                        }
                    },
                };
                let live_names = live.clone().unwrap_or_default();
                for name in live_names.keys() {
                    registry.add(&source, name);
                }
                let names = registry.names(&source);
                let mut truncated = names.len() > MAX_SUBSCRIPTIONS;
                let mut subscriptions = Vec::new();
                for name in names.into_iter().take(MAX_SUBSCRIPTIONS) {
                    if tokio::time::Instant::now() >= deadline {
                        truncated = true;
                        break;
                    }
                    let mut segments = Vec::with_capacity(located.len());
                    for l in &located {
                        let committed = l
                            .conn
                            .stored_offset(&name, &l.part.stream)
                            .await
                            .map_err(unavailable)?;
                        segments.push(SegmentLag {
                            segment: l.part.segment.clone(),
                            committed: committed.map(|c| c.to_string()),
                            head: l.bounds.head.to_string(),
                            lag: committed.map(|c| l.bounds.head.saturating_sub(c)),
                        });
                    }
                    if segments.iter().all(|s| s.committed.is_none()) {
                        continue;
                    }
                    let members = live_names.get(&name).map_or(0, BTreeSet::len) as u64;
                    subscriptions.push(SubscriptionLag {
                        lag: segments.iter().filter_map(|s| s.lag).sum(),
                        state: match (&live, members) {
                            (None, _) => "unknown",
                            (Some(_), 0) => "empty",
                            (Some(_), _) => "active",
                        }
                        .to_string(),
                        subscription: name,
                        members,
                        segments,
                    });
                }
                subscriptions.sort_by(|a, b| {
                    b.lag
                        .cmp(&a.lag)
                        .then_with(|| a.subscription.cmp(&b.subscription))
                });
                Ok(LagView {
                    source,
                    missing: false,
                    subscriptions,
                    truncated,
                })
            })
        })
        .await
    }

    /// Starts a tail at the head of every partition: one task holds the slot and the
    /// connections from the setup to the end, so a caller that leaves (or gives up waiting)
    /// during the setup never frees a slot while RabbitMQ work still runs for it.
    async fn open_tail(&self, request: TailRequest) -> Result<TailStream, BrowseError> {
        let source = self.settings.source(request.source.as_deref())?;
        let filters = Filters::parse(&request.filters, true)?;
        let duration = self.settings.tail_duration(request.max_duration_ms)?;
        let slot = Arc::clone(&self.tails).try_acquire_owned().map_err(|_| {
            BrowseError::Busy(format!(
                "{} event tails are open on this browser; try again later",
                self.settings.max_tails
            ))
        })?;
        let budget = self.settings.timeout;
        let broker = self.broker.clone();
        let (mut feed, stream) = tail_channel(
            self.settings.tail_buffer,
            heartbeat_period(request.heartbeat_ms),
        );
        let (started_tx, started) = oneshot::channel::<Result<(), BrowseError>>();
        tokio::spawn(async move {
            let _slot = slot;
            let mut nodes = Nodes::new(broker);
            let setup = async {
                let deadline = until(Instant::now() + budget);
                // Fresh heads (never the cache): the tail starts after what is there now.
                let Some(located) = locate(&mut nodes, &source, deadline, Need::Head, None, None)
                    .await
                    .map_err(unavailable)?
                else {
                    return Err(BrowseError::NotFound(format!(
                        "source {source} does not exist"
                    )));
                };
                let mut readers = Vec::with_capacity(located.len());
                for l in &located {
                    let reader = ChunkReader::open(
                        &l.conn,
                        &l.part.stream,
                        OffsetSpecification::Offset(l.bounds.head),
                    )
                    .await
                    .map_err(unavailable)?;
                    readers.push((l.part.segment.clone(), l.bounds.head, reader));
                }
                Ok(readers)
            };
            let readers = match setup.await {
                Ok(readers) => readers,
                Err(refusal) => {
                    let _ = started_tx.send(Err(refusal));
                    nodes.close().await;
                    return;
                }
            };
            let ends = std::time::SystemTime::now() + duration;
            feed.start(TailStart {
                source: source.clone(),
                segments: readers.len() as u64,
                ends_at_ms: ends
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as i64),
            });
            if started_tx.send(Ok(())).is_err() {
                // The caller left during the setup.
                nodes.close().await;
                return;
            }
            let end = tokio::time::Instant::now() + duration;
            let mut chunks =
                futures::stream::select_all(readers.into_iter().map(|(segment, start, reader)| {
                    futures::stream::unfold(reader, move |mut reader| async move {
                        let next = reader.next(end).await;
                        Some((next, reader))
                    })
                    .map(move |next| (segment.clone(), start, next))
                    .boxed()
                }));
            let mut poll = tokio::time::interval(Duration::from_millis(100));
            loop {
                if feed.is_closed() {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(end) => {
                        feed.end(EndReason::MaxDuration);
                        break;
                    }
                    _ = poll.tick() => {}
                    next = chunks.next() => match next {
                        None | Some((_, _, Err(_))) => {
                            feed.end(EndReason::Failed);
                            break;
                        }
                        Some((_, _, Ok(None))) => {}
                        Some((segment, start, Ok(Some(chunk)))) => {
                            let mut open = true;
                            for (i, message) in chunk.messages.iter().enumerate() {
                                let offset = chunk.first_offset + i as u64;
                                if offset < start {
                                    continue;
                                }
                                let summary = with_view(chunk.timestamp_ms, message, |r| {
                                    filters.matches(r).then(|| {
                                        EventSummary::of(&source, &segment, &offset.to_string(), r)
                                    })
                                });
                                if let Some(summary) = summary
                                    && !feed.record(summary)
                                {
                                    open = false;
                                    break;
                                }
                            }
                            if !open {
                                break;
                            }
                        }
                    },
                }
            }
            drop(chunks);
            nodes.close().await;
        });
        match tokio::time::timeout(budget + CLOSE_GRACE, started).await {
            Ok(Ok(Ok(()))) => Ok(stream),
            Ok(Ok(Err(refusal))) => Err(refusal),
            Ok(Err(_)) => Err(BrowseError::Unavailable("the RabbitMQ read failed".into())),
            Err(_) => Err(late()),
        }
    }
}

fn position_text(offset: u64) -> String {
    offset.to_string()
}

impl EventBrowser for RabbitBrowser {
    fn search(&self, request: SearchRequest) -> BoxFuture<'_, Result<SearchPage, BrowseError>> {
        Box::pin(self.search_page(request))
    }

    fn get(&self, request: GetRequest) -> BoxFuture<'_, Result<EventRecord, BrowseError>> {
        Box::pin(self.record(request))
    }

    fn sources(&self, _request: SourcesRequest) -> BoxFuture<'_, Result<SourcesView, BrowseError>> {
        Box::pin(self.streams())
    }

    fn lag(&self, request: LagRequest) -> BoxFuture<'_, Result<LagView, BrowseError>> {
        Box::pin(self.subscriptions(request))
    }

    fn tail(&self, request: TailRequest) -> BoxFuture<'_, Result<TailStream, BrowseError>> {
        Box::pin(self.open_tail(request))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use kraken_messaging::event_store::browse::{EventFilter, TimeArg};

    fn browser(settings: BrowseSettings) -> RabbitBrowser {
        RabbitBrowser::open(&RabbitConfig::new("127.0.0.1:1").browse(settings)).unwrap()
    }

    #[test]
    fn read_slots_are_bounded_and_given_back() {
        let browser = browser(BrowseSettings {
            max_searches: 2,
            ..BrowseSettings::default()
        });
        let a = browser.read_slot().unwrap();
        let _b = browser.read_slot().unwrap();
        assert!(matches!(browser.read_slot(), Err(BrowseError::Busy(_))));
        drop(a);
        assert!(browser.read_slot().is_ok());
    }

    #[test]
    fn positions_are_offsets() {
        assert_eq!(offset_of("42"), Ok(42));
        for bad in ["-1", "x", ""] {
            assert!(
                matches!(offset_of(bad), Err(BrowseError::Invalid(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn known_subscriptions_are_listed_for_every_source() {
        let config = RabbitConfig::new("127.0.0.1:1").known_subscriptions(&["svc.p"]);
        let browser = RabbitBrowser::open(&config).unwrap();
        assert_eq!(
            browser.registry.names("kraken.events"),
            BTreeSet::from(["svc.p".to_string()])
        );
    }

    #[tokio::test]
    async fn refusals_come_before_any_rabbitmq_call() {
        let browser = browser(BrowseSettings::default());
        for refused in [
            browser
                .search(SearchRequest {
                    source: Some("elsewhere".into()),
                    ..SearchRequest::default()
                })
                .await
                .err(),
            browser
                .get(GetRequest {
                    segment: "0".into(),
                    position: "x".into(),
                    ..GetRequest::default()
                })
                .await
                .err(),
            browser
                .lag(LagRequest {
                    source: Some("elsewhere".into()),
                })
                .await
                .err(),
            browser
                .tail(TailRequest {
                    filters: EventFilter {
                        to: Some(TimeArg::Millis(1)),
                        ..Default::default()
                    },
                    ..TailRequest::default()
                })
                .await
                .err(),
        ] {
            assert!(
                matches!(refused, Some(BrowseError::Invalid(_))),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn a_view_carries_the_key_headers_payload_and_time() {
        let event = kraken_messaging::event_store::OutgoingEvent {
            source: "kraken.events".into(),
            id: "e-1".into(),
            type_name: "t".into(),
            key: Some("k".into()),
            headers: HashMap::from([("type".to_string(), "t".to_string())]),
            payload: b"{}".to_vec(),
        };
        let message = wire::message(&event, 1);
        with_view(5, &message, |r| {
            assert_eq!(r.timestamp, Some(5));
            assert_eq!(r.key, Some(&b"k"[..]));
            assert_eq!(r.header("type"), Some(&b"t"[..]));
            assert_eq!(r.payload, Some(&b"{}"[..]));
        });
    }
}
