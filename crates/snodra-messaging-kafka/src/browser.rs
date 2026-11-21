//! The Kafka event browser (the reads behind the core's [`EventBrowser`]): search, one
//! record, the topics, the consumer groups' lag and the tail, on the topics of
//! [`BrowseSettings::sources`] only. A source is a topic, a segment a partition, a position an
//! offset (both in decimal); a subscription is a consumer group.
//!
//! The browser reads and never writes: every consumer is short-lived, `assign`s and seeks, never
//! subscribes and never commits (`enable.auto.commit=false`), so no consumer group exists for the
//! browser; its `group.id` (`snodra-event-browser-<random>`) is only what librdkafka requires,
//! and the lag view reads a group's committed offsets with an assign-only consumer of that group
//! that does neither either. Every librdkafka call runs on Tokio's blocking pool under a deadline
//! (the settings' timeout, or the request's), and the slot and the consumer belong to the
//! blocking call, never to the caller's future: a caller that leaves frees nothing while Kafka work
//! still runs for it, and no consumer is dropped on an async worker. Calls are bounded in number
//! ([`BrowseSettings::max_searches`], [`BrowseSettings::max_tails`]: one more is
//! [`BrowseError::Busy`]), in records and in time. Nothing connects before the first call.

use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::{Offset, TopicPartitionList};
use snodra_messaging::event_store::browse::{
    BrowseError, BrowseSettings, CHUNK, Chunks, EndReason, EventBrowser, EventRecord, EventSummary,
    Examined, Filters, GetRequest, LagRequest, LagView, Range, RecordView, SearchPage, SearchPlan,
    SearchRequest, SegmentBounds, SegmentLag, SourceView, SourcesRequest, SourcesView,
    SubscriptionLag, TailRequest, TailStart, TailStream, Want, heartbeat_period, merge, quoted,
    tail_channel,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

/// The prefix of the `group.id` every browser consumer carries (never joined, never committed).
pub const BROWSER_GROUP_PREFIX: &str = "snodra-event-browser-";
/// How far past `to` a search still looks for a record stamped within its range, in ms:
/// producers set `CreateTime`, so a partition's timestamps need not rise.
pub const TO_SKEW: i64 = 5 * 60 * 1000;
/// The lag view lists at most this many consumer groups (`truncated` beyond).
pub const MAX_GROUPS: usize = 500;
/// Time a blocking call gets beyond its deadline to close its consumer before the caller gives
/// up on it (the call still ends on its own, holding its slot until then); also the longest a
/// single Kafka request may wait for its answer.
pub const CLOSE_GRACE: Duration = Duration::from_secs(5);
/// Kafka client warnings are logged at most this often (the others are counted in the next).
const WARN_EVERY: Duration = Duration::from_secs(30);
/// One `poll` waits at most this long, so deadlines and cancellation are seen.
const POLL: Duration = Duration::from_millis(100);

fn unavailable(e: KafkaError) -> BrowseError {
    BrowseError::Unavailable(format!("Kafka is unavailable: {e}"))
}

fn late() -> BrowseError {
    BrowseError::Unavailable("Kafka did not answer in time".to_string())
}

/// What is left until `deadline`; [`late`] when nothing is.
fn left(deadline: Instant) -> Result<Duration, BrowseError> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        Err(late())
    } else {
        Ok(left)
    }
}

/// The partition a segment names.
fn partition_of(segment: &str) -> Result<i32, BrowseError> {
    segment
        .parse::<i32>()
        .ok()
        .filter(|p| *p >= 0)
        .ok_or_else(|| {
            BrowseError::Invalid(format!(
                "segment {} is not a partition (a non-negative number)",
                quoted(segment)
            ))
        })
}

/// The offset a position names.
fn offset_of(position: &str) -> Result<i64, BrowseError> {
    position
        .parse::<i64>()
        .ok()
        .filter(|o| *o >= 0)
        .ok_or_else(|| {
            BrowseError::Invalid(format!(
                "position {} is not an offset (a non-negative number)",
                quoted(position)
            ))
        })
}

/// Kafka client warnings, at most one line per [`WARN_EVERY`].
#[derive(Default)]
pub(crate) struct Warnings {
    /// When the last line was written, and how many were held back since.
    state: Mutex<(Option<Instant>, u64)>,
}

impl Warnings {
    fn warn(&self, facility: &str, detail: &str) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let now = Instant::now();
        if state
            .0
            .is_some_and(|last| now.duration_since(last) < WARN_EVERY)
        {
            state.1 += 1;
            return;
        }
        let suppressed = std::mem::take(&mut state.1);
        state.0 = Some(now);
        drop(state);
        tracing::warn!(
            facility,
            error = detail,
            suppressed,
            "Kafka client error (event browser)"
        );
    }
}

/// The librdkafka context of every browser client: its log lines and errors go through
/// [`Warnings`] (they never carry a payload).
struct Context {
    warnings: Arc<Warnings>,
}

impl ClientContext for Context {
    fn log(&self, level: RDKafkaLogLevel, facility: &str, message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error
            | RDKafkaLogLevel::Warning => self.warnings.warn(facility, message),
            _ => tracing::debug!(facility, message, "librdkafka (event browser)"),
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.warnings.warn("error", &format!("{error}: {reason}"));
    }
}

impl ConsumerContext for Context {}

type Client = BaseConsumer<Context>;

/// How to reach Kafka; cloned into every blocking call.
#[derive(Clone)]
struct Kafka {
    bootstrap: String,
    /// The store's consumer properties (security, for example), under the browser's own.
    properties: HashMap<String, String>,
    group: String,
    warnings: Arc<Warnings>,
}

impl Kafka {
    /// The settings of a consumer of `group` that only ever assigns: no commit, no subscription,
    /// no topic creation; fetches of about a chunk, so a search holds little memory. No request
    /// to Kafka waits longer than [`CLOSE_GRACE`] (librdkafka's default is 60 s), so a consumer
    /// closed while a broker hangs is gone within the grace and its call's slot with it.
    fn settings(&self, group: &str) -> ClientConfig {
        let request_timeout = CLOSE_GRACE.as_millis().to_string();
        let mut settings = crate::consumer_config(&self.bootstrap, group, &self.properties);
        settings
            .set("client.id", &self.group)
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "true")
            .set("allow.auto.create.topics", "false")
            .set("auto.offset.reset", "earliest")
            // A fetch brings a partition about one chunk, and the consumer keeps about one chunk
            // ahead per partition: a search's broker traffic follows the records it examines.
            .set("max.partition.fetch.bytes", "262144")
            .set("queued.max.messages.kbytes", "256")
            .set("queued.min.messages", CHUNK.to_string())
            .set("fetch.max.bytes", "4194304")
            .set("fetch.wait.max.ms", "100")
            .set("socket.timeout.ms", &request_timeout)
            .set("socket.connection.setup.timeout.ms", &request_timeout)
            .set("api.version.request.timeout.ms", &request_timeout)
            .set_log_level(RDKafkaLogLevel::Warning);
        settings
    }

    /// A consumer of `group` with [`Kafka::settings`].
    fn consumer(&self, group: &str) -> Result<Client, BrowseError> {
        self.settings(group)
            .create_with_context(Context {
                warnings: Arc::clone(&self.warnings),
            })
            .map_err(|e| BrowseError::Unavailable(format!("cannot create a Kafka client: {e}")))
    }

    /// The browser's own consumer.
    fn browser(&self) -> Result<Client, BrowseError> {
        self.consumer(&self.group)
    }
}

/// The partitions of `topic`, sorted; `None` when the topic does not exist.
fn partitions(c: &Client, topic: &str, deadline: Instant) -> Result<Option<Vec<i32>>, BrowseError> {
    let metadata = c
        .fetch_metadata(Some(topic), left(deadline)?)
        .map_err(unavailable)?;
    let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) else {
        return Ok(None);
    };
    match t.error().map(RDKafkaErrorCode::from) {
        Some(RDKafkaErrorCode::UnknownTopicOrPartition) => Ok(None),
        Some(code) => Err(BrowseError::Unavailable(format!(
            "Kafka is unavailable: topic {topic}: {code}"
        ))),
        None if t.partitions().is_empty() => Ok(None),
        None => {
            let mut ids: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
            ids.sort_unstable();
            Ok(Some(ids))
        }
    }
}

/// `(low, high)` watermarks of `partition`.
fn watermarks(
    c: &Client,
    topic: &str,
    partition: i32,
    deadline: Instant,
) -> Result<(i64, i64), BrowseError> {
    c.fetch_watermarks(topic, partition, left(deadline)?)
        .map_err(unavailable)
}

/// For every partition, the first offset whose record timestamp is at least `ts` (`None` when
/// there is none).
fn offsets_for(
    c: &Client,
    topic: &str,
    parts: &[i32],
    ts: i64,
    deadline: Instant,
) -> Result<Vec<(i32, Option<i64>)>, BrowseError> {
    let mut tpl = TopicPartitionList::new();
    for p in parts {
        tpl.add_partition_offset(topic, *p, Offset::Offset(ts))
            .map_err(unavailable)?;
    }
    let found = c
        .offsets_for_times(tpl, left(deadline)?)
        .map_err(unavailable)?;
    Ok(parts
        .iter()
        .map(|p| {
            let offset = found
                .find_partition(topic, *p)
                .and_then(|e| match e.offset() {
                    Offset::Offset(o) => Some(o),
                    _ => None,
                });
            (*p, offset)
        })
        .collect())
}

/// Every header of `m`, names as bytes. rdkafka's `Headers` reads a name as `&str` and panics on
/// one that is not UTF-8 (any client may write one), so they are read from librdkafka directly.
fn headers_of<'a>(m: &'a BorrowedMessage<'_>) -> Vec<(&'a [u8], Option<&'a [u8]>)> {
    use rdkafka::bindings as rd;
    use rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR as NO_ERROR;
    let mut out = Vec::new();
    let mut headers: *mut rd::rd_kafka_headers_t = std::ptr::null_mut();
    // SAFETY: `m.ptr()` is the live message `m` borrows. librdkafka owns its headers, and the
    // name and value bytes they point to, until the message is destroyed, which cannot happen
    // while `m` (and so `'a`) lives. Names are NUL-terminated C strings; values are `size` bytes.
    unsafe {
        if rd::rd_kafka_message_headers(m.ptr(), &mut headers) != NO_ERROR || headers.is_null() {
            return out;
        }
        for idx in 0.. {
            let mut name: *const std::ffi::c_char = std::ptr::null();
            let mut value: *const std::ffi::c_void = std::ptr::null();
            let mut size = 0usize;
            if rd::rd_kafka_header_get_all(headers, idx, &mut name, &mut value, &mut size)
                != NO_ERROR
            {
                break;
            }
            let name = CStr::from_ptr(name).to_bytes();
            let value = (!value.is_null()).then(|| std::slice::from_raw_parts(value.cast(), size));
            out.push((name, value));
        }
    }
    out
}

/// Calls `f` with the [`RecordView`] of `m`.
fn with_view<T>(m: &BorrowedMessage<'_>, f: impl FnOnce(&RecordView<'_>) -> T) -> T {
    let headers = headers_of(m);
    f(&RecordView {
        timestamp: m.timestamp().to_millis(),
        key: m.key(),
        headers: &headers,
        payload: m.payload(),
    })
}

/// The summary of `m` when it matches `filters`.
fn summary(topic: &str, filters: &Filters, m: &BorrowedMessage<'_>) -> Option<EventSummary> {
    with_view(m, |r| {
        filters.matches(r).then(|| {
            EventSummary::of(
                topic,
                &m.partition().to_string(),
                &m.offset().to_string(),
                r,
            )
        })
    })
}

/// Assigns `partition` of `topic` from `offset`.
fn assign(c: &Client, topic: &str, partition: i32, offset: i64) -> Result<(), BrowseError> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
        .map_err(unavailable)?;
    c.assign(&tpl).map_err(unavailable)
}

/// Reads the chunks of a search: one `assign` of every chunk's partition at the chunk's start,
/// then the records as the broker sends them, until every chunk reached its end.
struct Reader<'a> {
    consumer: &'a Client,
    topic: &'a str,
    filters: &'a Filters,
    warnings: &'a Warnings,
}

impl Chunks for Reader<'_> {
    type Error = BrowseError;

    fn read(
        &mut self,
        wants: &[Want],
        deadline: Instant,
    ) -> Result<Vec<Option<Vec<Examined>>>, BrowseError> {
        let partitions: Vec<i32> = wants
            .iter()
            .map(|w| partition_of(&w.segment))
            .collect::<Result<_, _>>()?;
        let mut tpl = TopicPartitionList::new();
        for (w, p) in wants.iter().zip(&partitions) {
            tpl.add_partition_offset(self.topic, *p, Offset::Offset(w.start))
                .map_err(unavailable)?;
        }
        self.consumer.assign(&tpl).map_err(unavailable)?;
        let mut chunks: Vec<Vec<Examined>> = wants
            .iter()
            .map(|w| Vec::with_capacity((w.end - w.start).clamp(0, CHUNK as i64) as usize))
            .collect();
        let mut complete = vec![false; wants.len()];
        let mut open = wants.len();
        let at = |partition: i32| partitions.iter().position(|p| *p == partition);
        while open > 0 {
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break;
            }
            let ended = match self.consumer.poll(wait.min(POLL)) {
                None => None,
                Some(Ok(m)) => match at(m.partition()) {
                    Some(i) if !complete[i] && m.offset() >= wants[i].start => {
                        if m.offset() < wants[i].end {
                            chunks[i].push(Examined {
                                position: m.offset(),
                                timestamp: m.timestamp().to_millis().unwrap_or(-1),
                                hit: summary(self.topic, self.filters, &m),
                            });
                        }
                        (m.offset() >= wants[i].end - 1).then_some(i)
                    }
                    _ => None,
                },
                Some(Err(KafkaError::PartitionEOF(p))) => at(p).filter(|i| !complete[*i]),
                Some(Err(e)) => {
                    self.warnings.warn("fetch", &e.to_string());
                    None
                }
            };
            if let Some(i) = ended {
                complete[i] = true;
                open -= 1;
            }
        }
        Ok(chunks
            .into_iter()
            .zip(complete)
            .map(|(chunk, complete)| complete.then_some(chunk))
            .collect())
    }
}

/// The Kafka event store's browser ([`KafkaEventStore::browser`](crate::KafkaEventStore)).
pub struct KafkaBrowser {
    kafka: Kafka,
    settings: BrowseSettings,
    reads: Arc<Semaphore>,
    tails: Arc<Semaphore>,
}

impl KafkaBrowser {
    /// A browser alone, without the rest of the store (no producer): what a program that only
    /// reads the event store needs. Checks `config` as [`KafkaEventStore::new`] does (the
    /// browser reads `config.browse`'s topics with the consumer properties) and connects nothing
    /// before its first call.
    ///
    /// [`KafkaEventStore::new`]: crate::KafkaEventStore::new
    pub fn open(
        config: &crate::KafkaConfig,
    ) -> Result<KafkaBrowser, snodra_messaging::event_store::EventStoreError> {
        config.validate()?;
        Ok(KafkaBrowser::new(
            &config.bootstrap_servers,
            config.consumer(),
            config.browse.clone(),
        ))
    }

    /// A browser of `bootstrap` with the store's consumer `properties`, under `settings`
    /// (checked by the store). Connects nothing.
    pub(crate) fn new(
        bootstrap: &str,
        properties: HashMap<String, String>,
        settings: BrowseSettings,
    ) -> KafkaBrowser {
        KafkaBrowser {
            kafka: Kafka {
                bootstrap: bootstrap.to_string(),
                properties,
                group: format!("{BROWSER_GROUP_PREFIX}{}", uuid::Uuid::new_v4().simple()),
                warnings: Arc::default(),
            },
            reads: Arc::new(Semaphore::new(settings.max_searches)),
            tails: Arc::new(Semaphore::new(settings.max_tails)),
            settings,
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

    /// Runs `f` on the blocking pool, holding `slot` until it returns, and waits for it at most
    /// `budget` plus [`CLOSE_GRACE`].
    async fn blocking<T: Send + 'static>(
        slot: OwnedSemaphorePermit,
        budget: Duration,
        f: impl FnOnce() -> Result<T, BrowseError> + Send + 'static,
    ) -> Result<T, BrowseError> {
        let task = tokio::task::spawn_blocking(move || {
            let _slot = slot;
            f()
        });
        match tokio::time::timeout(budget + CLOSE_GRACE, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BrowseError::Unavailable(
                "the Kafka read failed".to_string(),
            )),
            Err(_) => Err(late()),
        }
    }

    async fn search_page(&self, request: SearchRequest) -> Result<SearchPage, BrowseError> {
        let plan = SearchPlan::new(&request, &self.settings)?;
        let kafka = self.kafka.clone();
        let slot = self.read_slot()?;
        Self::blocking(slot, plan.timeout, move || {
            let started = Instant::now();
            let deadline = started + plan.timeout;
            let c = kafka.browser()?;
            let topic = plan.source.as_str();
            let Some(parts) = partitions(&c, topic, deadline)? else {
                return Ok(plan.missing());
            };
            let filters = &plan.filters;
            let lowest = filters
                .from
                .map(|ts| offsets_for(&c, topic, &parts, ts, deadline))
                .transpose()?;
            // The first record stamped after `to` + TO_SKEW bounds the range; every record is
            // still compared with `to` itself.
            let highest = filters
                .to
                .map(|ts| {
                    let bound = ts.saturating_add(TO_SKEW).saturating_add(1);
                    offsets_for(&c, topic, &parts, bound, deadline)
                })
                .transpose()?;
            let mut ranges = Vec::with_capacity(parts.len());
            for (i, p) in parts.iter().enumerate() {
                let (low, high) = watermarks(&c, topic, *p, deadline)?;
                let lower = match &lowest {
                    None => low,
                    Some(found) => found[i].1.map_or(high, |o| o.max(low)),
                };
                let upper = match &highest {
                    None => high,
                    Some(found) => found[i].1.map_or(high, |o| o.min(high)),
                };
                let segment = p.to_string();
                ranges.push(Range {
                    upper: plan.upper(&segment, lower, upper),
                    segment,
                    lower,
                });
            }
            let mut reader = Reader {
                consumer: &c,
                topic,
                filters,
                warnings: &kafka.warnings,
            };
            let page = merge(&mut reader, &ranges, plan.budget(started))?;
            Ok(plan.answer(page))
        })
        .await
    }

    async fn record(&self, request: GetRequest) -> Result<EventRecord, BrowseError> {
        let topic = self.settings.source(request.source.as_deref())?;
        let (segment, position) = request.place()?;
        let partition = partition_of(segment)?;
        let offset = offset_of(position)?;
        let payload = request.payload.then_some(self.settings.max_payload);
        let kafka = self.kafka.clone();
        let slot = self.read_slot()?;
        let budget = self.settings.timeout;
        Self::blocking(slot, budget, move || {
            let deadline = Instant::now() + budget;
            let c = kafka.browser()?;
            let Some(parts) = partitions(&c, &topic, deadline)? else {
                return Err(BrowseError::NotFound(format!("topic {topic} does not exist")));
            };
            if !parts.contains(&partition) {
                return Err(BrowseError::NotFound(format!(
                    "topic {topic} has no partition {partition}"
                )));
            }
            let (low, high) = watermarks(&c, &topic, partition, deadline)?;
            if !(low..high).contains(&offset) {
                return Err(BrowseError::NotFound(format!(
                    "offset {offset} is outside partition {partition} of {topic} (offsets {low} to {})",
                    high - 1
                )));
            }
            assign(&c, &topic, partition, offset)?;
            let gone = || {
                BrowseError::NotFound(format!(
                    "no record at offset {offset} of partition {partition} of {topic} (compacted or deleted)"
                ))
            };
            loop {
                match c.poll(left(deadline)?.min(POLL)) {
                    None => {}
                    Some(Ok(m)) if m.partition() == partition && m.offset() == offset => {
                        return Ok(with_view(&m, |r| {
                            EventRecord::of(
                                &topic,
                                &partition.to_string(),
                                &offset.to_string(),
                                r,
                                payload,
                            )
                        }));
                    }
                    Some(Ok(m)) if m.partition() == partition && m.offset() > offset => {
                        return Err(gone());
                    }
                    Some(Ok(_)) => {}
                    Some(Err(KafkaError::PartitionEOF(p))) if p == partition => {
                        return Err(gone());
                    }
                    Some(Err(KafkaError::PartitionEOF(_))) => {}
                    Some(Err(e)) => kafka.warnings.warn("fetch", &e.to_string()),
                }
            }
        })
        .await
    }

    async fn topics(&self) -> Result<SourcesView, BrowseError> {
        let kafka = self.kafka.clone();
        let slot = self.read_slot()?;
        let budget = self.settings.timeout;
        let topics = self.settings.sources.clone();
        Self::blocking(slot, budget, move || {
            let deadline = Instant::now() + budget;
            let c = kafka.browser()?;
            let mut sources = Vec::with_capacity(topics.len());
            for topic in topics {
                let Some(parts) = partitions(&c, &topic, deadline)? else {
                    sources.push(SourceView {
                        source: topic,
                        missing: true,
                        segments: Vec::new(),
                    });
                    continue;
                };
                let mut segments = Vec::with_capacity(parts.len());
                for p in parts {
                    let (low, high) = watermarks(&c, &topic, p, deadline)?;
                    segments.push(SegmentBounds {
                        segment: p.to_string(),
                        first: low.to_string(),
                        head: high.to_string(),
                    });
                }
                sources.push(SourceView {
                    source: topic,
                    missing: false,
                    segments,
                });
            }
            Ok(SourcesView { sources })
        })
        .await
    }

    /// Every consumer group with a committed offset on the topic: the broker's group list
    /// (sorted by name, at most [`MAX_GROUPS`]), then per group one assign-only consumer of that
    /// group reading its committed offsets (it never subscribes or commits).
    async fn groups(&self, request: LagRequest) -> Result<LagView, BrowseError> {
        let topic = self.settings.source(request.source.as_deref())?;
        let kafka = self.kafka.clone();
        let slot = self.read_slot()?;
        let budget = self.settings.timeout;
        Self::blocking(slot, budget, move || {
            let deadline = Instant::now() + budget;
            let c = kafka.browser()?;
            let Some(parts) = partitions(&c, &topic, deadline)? else {
                return Ok(LagView {
                    source: topic,
                    missing: true,
                    subscriptions: vec![],
                    truncated: false,
                });
            };
            let mut highs = Vec::with_capacity(parts.len());
            for p in &parts {
                highs.push(watermarks(&c, &topic, *p, deadline)?.1);
            }
            let list = c
                .fetch_group_list(None, left(deadline)?)
                .map_err(unavailable)?;
            let mut known: Vec<(String, String, usize)> = list
                .groups()
                .iter()
                .map(|g| {
                    (
                        g.name().to_string(),
                        g.state().to_string(),
                        g.members().len(),
                    )
                })
                .collect();
            drop(c);
            known.sort();
            let mut truncated = known.len() > MAX_GROUPS;
            known.truncate(MAX_GROUPS);
            let mut subscriptions = Vec::new();
            for (name, state, members) in known {
                let Ok(wait) = left(deadline) else {
                    truncated = true;
                    break;
                };
                let member = kafka.consumer(&name)?;
                let mut tpl = TopicPartitionList::new();
                for p in &parts {
                    tpl.add_partition(&topic, *p);
                }
                let committed = match member.committed_offsets(tpl, wait) {
                    Ok(committed) => committed,
                    Err(e) => {
                        kafka.warnings.warn("offsets", &format!("{name}: {e}"));
                        continue;
                    }
                };
                let segments: Vec<SegmentLag> = parts
                    .iter()
                    .zip(&highs)
                    .map(|(p, high)| {
                        let committed = committed.find_partition(&topic, *p).and_then(|e| match e
                            .offset()
                        {
                            Offset::Offset(o) if o >= 0 => Some(o),
                            _ => None,
                        });
                        SegmentLag {
                            segment: p.to_string(),
                            committed: committed.map(|c| c.to_string()),
                            head: high.to_string(),
                            lag: committed.map(|c| (high - c).max(0) as u64),
                        }
                    })
                    .collect();
                if segments.iter().all(|s| s.committed.is_none()) {
                    continue;
                }
                subscriptions.push(SubscriptionLag {
                    lag: segments.iter().filter_map(|s| s.lag).sum(),
                    subscription: name,
                    state,
                    members: members as u64,
                    segments,
                });
            }
            subscriptions.sort_by(|a, b| {
                b.lag
                    .cmp(&a.lag)
                    .then_with(|| a.subscription.cmp(&b.subscription))
            });
            Ok(LagView {
                source: topic,
                missing: false,
                subscriptions,
                truncated,
            })
        })
        .await
    }

    /// Starts a tail at the high watermark of every partition: one blocking call holds the slot
    /// and the consumer from the setup to the end, so a caller that leaves (or gives up waiting)
    /// during the setup never frees a slot while Kafka work still runs for it.
    async fn open_tail(&self, request: TailRequest) -> Result<TailStream, BrowseError> {
        let topic = self.settings.source(request.source.as_deref())?;
        let filters = Filters::parse(&request.filters, true)?;
        let duration = self.settings.tail_duration(request.max_duration_ms)?;
        let kafka = self.kafka.clone();
        let slot = Arc::clone(&self.tails).try_acquire_owned().map_err(|_| {
            BrowseError::Busy(format!(
                "{} event tails are open on this browser; try again later",
                self.settings.max_tails
            ))
        })?;
        let budget = self.settings.timeout;
        let (mut feed, stream) = tail_channel(
            self.settings.tail_buffer,
            heartbeat_period(request.heartbeat_ms),
        );
        let (started_tx, started) = oneshot::channel::<Result<(), BrowseError>>();
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            let setup = || {
                let deadline = Instant::now() + budget;
                let c = kafka.browser()?;
                let Some(parts) = partitions(&c, &topic, deadline)? else {
                    return Err(BrowseError::NotFound(format!(
                        "topic {topic} does not exist"
                    )));
                };
                let mut tpl = TopicPartitionList::new();
                for p in &parts {
                    let (_, high) = watermarks(&c, &topic, *p, deadline)?;
                    tpl.add_partition_offset(&topic, *p, Offset::Offset(high))
                        .map_err(unavailable)?;
                }
                c.assign(&tpl).map_err(unavailable)?;
                Ok((c, parts.len()))
            };
            let c = match setup() {
                Ok((c, parts)) => {
                    let ends = std::time::SystemTime::now() + duration;
                    feed.start(TailStart {
                        source: topic.clone(),
                        segments: parts as u64,
                        ends_at_ms: ends
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| d.as_millis() as i64),
                    });
                    if started_tx.send(Ok(())).is_err() {
                        // The caller left during the setup.
                        return;
                    }
                    c
                }
                Err(refusal) => {
                    let _ = started_tx.send(Err(refusal));
                    return;
                }
            };
            let deadline = Instant::now() + duration;
            loop {
                if feed.is_closed() {
                    break;
                }
                if Instant::now() >= deadline {
                    feed.end(EndReason::MaxDuration);
                    break;
                }
                match c.poll(POLL) {
                    None | Some(Err(KafkaError::PartitionEOF(_))) => {}
                    Some(Err(e)) => kafka.warnings.warn("fetch", &e.to_string()),
                    Some(Ok(m)) => {
                        if let Some(summary) = summary(&topic, &filters, &m)
                            && !feed.record(summary)
                        {
                            break;
                        }
                    }
                }
            }
            drop(c);
        });
        match tokio::time::timeout(budget + CLOSE_GRACE, started).await {
            Ok(Ok(Ok(()))) => Ok(stream),
            Ok(Ok(Err(refusal))) => Err(refusal),
            Ok(Err(_)) => Err(BrowseError::Unavailable("the Kafka read failed".into())),
            Err(_) => Err(late()),
        }
    }
}

impl EventBrowser for KafkaBrowser {
    fn search(&self, request: SearchRequest) -> BoxFuture<'_, Result<SearchPage, BrowseError>> {
        Box::pin(self.search_page(request))
    }

    fn get(&self, request: GetRequest) -> BoxFuture<'_, Result<EventRecord, BrowseError>> {
        Box::pin(self.record(request))
    }

    fn sources(&self, _request: SourcesRequest) -> BoxFuture<'_, Result<SourcesView, BrowseError>> {
        Box::pin(self.topics())
    }

    fn lag(&self, request: LagRequest) -> BoxFuture<'_, Result<LagView, BrowseError>> {
        Box::pin(self.groups(request))
    }

    fn tail(&self, request: TailRequest) -> BoxFuture<'_, Result<TailStream, BrowseError>> {
        Box::pin(self.open_tail(request))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn browser(settings: BrowseSettings) -> KafkaBrowser {
        KafkaBrowser::new("127.0.0.1:1", HashMap::new(), settings)
    }

    #[test]
    fn kafka_warnings_are_rate_limited_and_count_what_they_held_back() {
        let warnings = Warnings::default();
        for _ in 0..5 {
            warnings.warn("fail", "Connect to ipv4#127.0.0.1:1 failed");
        }
        let state = warnings.state.lock().unwrap();
        assert!(state.0.is_some(), "the first one is written");
        assert_eq!(state.1, 4, "the others are held back and counted");
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

    /// The broker sends a partition about one chunk per fetch, and the consumer keeps about one
    /// chunk ahead per partition, so a search's broker traffic follows the records it examines.
    /// The store's consumer properties apply; the browser's own settings win.
    #[test]
    fn a_consumer_fetches_about_one_chunk_per_partition_and_never_commits() {
        let properties = HashMap::from([
            ("security.protocol".to_string(), "plaintext".to_string()),
            ("enable.partition.eof".to_string(), "false".to_string()),
        ]);
        let browser = KafkaBrowser::new("127.0.0.1:1", properties, BrowseSettings::default());
        let kafka = &browser.kafka;
        assert!(kafka.group.starts_with(BROWSER_GROUP_PREFIX));
        let settings = kafka.settings(&kafka.group);
        assert_eq!(settings.get("max.partition.fetch.bytes"), Some("262144"));
        assert_eq!(settings.get("queued.max.messages.kbytes"), Some("256"));
        assert_eq!(
            settings.get("queued.min.messages"),
            Some(CHUNK.to_string().as_str())
        );
        assert_eq!(settings.get("enable.auto.commit"), Some("false"));
        assert_eq!(settings.get("allow.auto.create.topics"), Some("false"));
        assert_eq!(settings.get("security.protocol"), Some("plaintext"));
        assert_eq!(settings.get("enable.partition.eof"), Some("true"));
        assert!(kafka.browser().is_ok(), "librdkafka takes the settings");
    }

    #[test]
    fn segments_are_partitions_and_positions_offsets() {
        assert_eq!(partition_of("3"), Ok(3));
        assert_eq!(offset_of("42"), Ok(42));
        for bad in ["-1", "x", "", "99999999999"] {
            assert!(
                matches!(partition_of(bad), Err(BrowseError::Invalid(_))),
                "{bad}"
            );
        }
        for bad in ["-1", "x", ""] {
            assert!(
                matches!(offset_of(bad), Err(BrowseError::Invalid(_))),
                "{bad}"
            );
        }
    }

    #[tokio::test]
    async fn refusals_come_before_any_kafka_call() {
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
                    segment: "x".into(),
                    position: "0".into(),
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
                    filters: snodra_messaging::event_store::browse::EventFilter {
                        to: Some(snodra_messaging::event_store::browse::TimeArg::Millis(1)),
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
}
