//! The Kafka event browser (the reads behind the core's `EventBrowser`) against a real
//! Kafka: search by every filter, newest first over the partitions, with a cursor; the budgets
//! and the concurrency bounds; one record; the tail; the consumer groups' lag; and what happens
//! when Kafka is unreachable or silent. Every test keeps its broker in a local, dropped at its end.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, FutureProducer, FutureRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use snodra_messaging::event_store::EventStore;
use snodra_messaging::event_store::browse::{
    BrowseError, BrowseSettings, EndReason, EventBrowser, EventFilter, EventRecord, GetRequest,
    LagRequest, SearchPage, SearchRequest, SourcesRequest, StoppedBy, TailEvent, TailRequest,
    TailStream, TimeArg,
};
use snodra_messaging_kafka::{CLOSE_GRACE, KafkaConfig, KafkaEventStore};
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka};

const T: i64 = 1_700_000_000_000;

// ---- support -------------------------------------------------------------------------------

struct Broker {
    _container: ContainerAsync<Kafka>,
    bootstrap: String,
}

async fn kafka() -> Broker {
    let container = Kafka::default().start().await.expect("kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("kafka port");
    Broker {
        _container: container,
        bootstrap: format!("127.0.0.1:{port}"),
    }
}

/// A topic name no other test uses.
fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(20))),
        )
        .await
        .unwrap();
    for r in results {
        r.unwrap();
    }
}

/// The browser of a Kafka event store at `bootstrap` reading `topics`, under `settings`.
fn browser(bootstrap: &str, topics: &[&str], settings: BrowseSettings) -> Arc<dyn EventBrowser> {
    let config = KafkaConfig::new(bootstrap).browse(BrowseSettings {
        sources: topics.iter().map(|t| t.to_string()).collect(),
        ..settings
    });
    KafkaEventStore::new(config)
        .expect("kafka event store")
        .browser()
        .expect("the Kafka event store browses")
}

fn defaults() -> BrowseSettings {
    BrowseSettings::default()
}

/// One record to write.
#[derive(Clone, Default)]
struct Rec {
    partition: i32,
    key: Option<Vec<u8>>,
    headers: Vec<(String, Vec<u8>)>,
    payload: Option<Vec<u8>>,
    timestamp: Option<i64>,
}

impl Rec {
    fn on(partition: i32) -> Self {
        Self {
            partition,
            ..Self::default()
        }
    }

    fn key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    fn header(mut self, name: &str, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.to_string(), value.into()));
        self
    }

    fn payload(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.payload = Some(payload.into());
        self
    }

    fn at(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

/// A record as the libraries write it, `ts` ms after [`T`].
fn library(partition: i32, ts: i64, key: &str, ty: &str, id: &str) -> Rec {
    Rec::on(partition)
        .at(T + ts)
        .key(key)
        .header("type", ty)
        .header("id", id)
        .header("timestamp", (T + ts).to_string())
        .payload(format!("{{\"id\":\"{id}\"}}"))
}

/// Writes `records` in order.
async fn produce(bootstrap: &str, topic: &str, records: &[Rec]) {
    produce_compressed(bootstrap, topic, records, "none").await
}

/// [`produce`] with the producer's `compression.type`.
async fn produce_compressed(bootstrap: &str, topic: &str, records: &[Rec], compression: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "20000")
        .set("compression.type", compression)
        .create()
        .unwrap();
    for r in records {
        let mut headers = OwnedHeaders::new();
        for (name, value) in &r.headers {
            headers = headers.insert(Header {
                key: name,
                value: Some(value),
            });
        }
        let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic)
            .partition(r.partition)
            .headers(headers);
        if let Some(key) = &r.key {
            record = record.key(key);
        }
        if let Some(payload) = &r.payload {
            record = record.payload(payload);
        }
        if let Some(ts) = r.timestamp {
            record = record.timestamp(ts);
        }
        producer
            .send(record, Duration::from_secs(20))
            .await
            .map_err(|(e, _)| e)
            .unwrap();
    }
}

/// Writes `count` small records to `partition` as fast as the producer batches them; record `i`
/// has key `k-<i>` and timestamp `first_ts + i`.
fn produce_many(bootstrap: &str, topic: &str, partition: i32, count: usize, first_ts: i64) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("linger.ms", "20")
        .set("batch.num.messages", "10000")
        .set("queue.buffering.max.messages", "1000000")
        .create()
        .unwrap();
    for i in 0..count {
        let key = format!("k-{i}");
        let mut record = BaseRecord::<str, [u8]>::to(topic)
            .partition(partition)
            .key(&key)
            .payload(b"{}".as_slice())
            .timestamp(first_ts + i as i64);
        loop {
            match producer.send(record) {
                Ok(()) => break,
                Err((_, back)) => {
                    producer.poll(Duration::from_millis(10));
                    record = back;
                }
            }
        }
    }
    producer.flush(Duration::from_secs(60)).unwrap();
}

/// Every consumer group the broker knows.
fn groups(bootstrap: &str) -> BTreeSet<String> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    c.fetch_group_list(None, Duration::from_secs(10))
        .unwrap()
        .groups()
        .iter()
        .map(|g| g.name().to_string())
        .collect()
}

/// Commits `offsets` of `topic` for `group` without joining it: the group then has committed
/// offsets and no member.
fn commit(bootstrap: &str, group: &str, topic: &str, offsets: &[(i32, i64)]) {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    for (p, o) in offsets {
        tpl.add_partition_offset(topic, *p, Offset::Offset(*o))
            .unwrap();
    }
    commit_retrying(&c, &tpl);
}

/// Commits `tpl`, retrying while a fresh broker is still electing the group coordinator.
fn commit_retrying(c: &BaseConsumer, tpl: &TopicPartitionList) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match c.commit(tpl, CommitMode::Sync) {
            Ok(()) => return,
            Err(e) if Instant::now() < deadline => {
                eprintln!("commit retried: {e}");
                std::thread::sleep(Duration::from_millis(300));
            }
            Err(e) => panic!("commit failed: {e}"),
        }
    }
}

/// A member of `group` subscribed to `topic`, polling on its own thread until dropped; it commits
/// `offsets` once it has its partitions.
struct Member {
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Member {
    fn join(bootstrap: &str, group: &str, topic: &str, offsets: &[(i32, i64)]) -> Self {
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .create()
            .unwrap();
        c.subscribe(&[topic]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while c.assignment().unwrap().count() == 0 {
            assert!(Instant::now() < deadline, "no assignment");
            c.poll(Duration::from_millis(100));
        }
        let mut tpl = TopicPartitionList::new();
        for (p, o) in offsets {
            tpl.add_partition_offset(topic, *p, Offset::Offset(*o))
                .unwrap();
        }
        commit_retrying(&c, &tpl);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let thread = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    c.poll(Duration::from_millis(100));
                }
            })
        };
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Member {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// A bootstrap address nothing listens on.
fn closed_port() -> String {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    format!("127.0.0.1:{port}")
}

/// A bootstrap address that accepts connections and never answers.
fn silent_broker() -> String {
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bootstrap = silent.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in silent.incoming() {
            held.push(stream);
        }
    });
    bootstrap
}

/// The `id` header of every result, in order.
fn ids(page: &SearchPage) -> Vec<String> {
    page.results
        .iter()
        .map(|r| r.headers.get("id").cloned().unwrap_or_default())
        .collect()
}

fn on(topic: &str, filters: EventFilter) -> SearchRequest {
    SearchRequest {
        source: Some(topic.to_string()),
        filters,
        ..SearchRequest::default()
    }
}

async fn search(browser: &dyn EventBrowser, request: SearchRequest) -> SearchPage {
    let what = format!("{request:?}");
    tokio::time::timeout(Duration::from_secs(30), browser.search(request))
        .await
        .expect("an answer in time")
        .unwrap_or_else(|e| panic!("{what}: {e:?}"))
}

async fn get(
    browser: &dyn EventBrowser,
    topic: &str,
    segment: &str,
    position: &str,
    payload: bool,
) -> Result<EventRecord, BrowseError> {
    browser
        .get(GetRequest {
            source: Some(topic.to_string()),
            segment: segment.to_string(),
            position: position.to_string(),
            payload,
        })
        .await
}

async fn next_part(tail: &mut TailStream) -> TailEvent {
    tokio::time::timeout(Duration::from_secs(20), tail.next())
        .await
        .expect("a part in time")
        .expect("a part before the stream closes")
}

/// The next part that is not a heartbeat.
async fn next_event(tail: &mut TailStream) -> TailEvent {
    loop {
        match next_part(tail).await {
            TailEvent::Heartbeat(_) => continue,
            part => return part,
        }
    }
}

// ---- search --------------------------------------------------------------------------------

#[tokio::test]
async fn search_matches_every_filter_newest_first_and_pages_with_a_cursor() {
    let broker = kafka().await;
    let topic = unique("events");
    create_topic(&broker.bootstrap, &topic, 3).await;
    produce(
        &broker.bootstrap,
        &topic,
        &[
            library(0, 10, "k-1", "orders.Placed", "e-1")
                .header("correlationId", "c-1")
                .header("source", "svc-a")
                .header("userId", "u-1"),
            library(0, 40, "k-2", "orders.Shipped", "e-2")
                .header("correlationId", "c-2")
                .header("source", "svc-b"),
            library(1, 20, "k-3", "users.Created", "e-3")
                .header("correlationId", "c-1")
                .header("userId", "u-2"),
            library(1, 50, "k-1", "orders.Placed", "e-4")
                .header("correlationId", "c-3")
                .header("source", "svc-a"),
            library(2, 30, "k-4", "orders.Cancelled", "e-5"),
            library(2, 60, "k-5", "users.Deleted", "e-6").header("correlationId", "c-1"),
        ],
    )
    .await;
    let browser = browser(&broker.bootstrap, &[&topic, "absent"], defaults());
    let browser = &*browser;

    let all = search(browser, SearchRequest::default()).await;
    assert_eq!(all.source, topic, "the first source is the default");
    assert_eq!(ids(&all), ["e-6", "e-4", "e-2", "e-5", "e-3", "e-1"]);
    assert_eq!(all.scanned, 6);
    assert!(all.exhausted && all.stopped_by.is_none() && all.cursor.is_none());
    let first = &all.results[0];
    assert_eq!(
        (
            first.segment.as_str(),
            first.position.as_str(),
            first.timestamp
        ),
        ("2", "1", Some(T + 60))
    );
    assert_eq!(first.key.as_deref(), Some("k-5"));
    assert_eq!(first.headers["type"], "users.Deleted");
    assert_eq!(first.size, "{\"id\":\"e-6\"}".len() as u64);
    let json = serde_json::to_string(&all).unwrap();
    assert!(
        !json.contains("{\\\"id\\\""),
        "no payload in a result: {json}"
    );

    let f = |pairs: &[(&str, &str)]| -> EventFilter {
        let value: serde_json::Map<String, serde_json::Value> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::from_value(serde_json::Value::Object(value)).unwrap()
    };
    let (from, to) = ((T + 20).to_string(), (T + 50).to_string());
    for (filters, want) in [
        (f(&[("key", "k-1")]), vec!["e-4", "e-1"]),
        (f(&[("type", "orders.Placed")]), vec!["e-4", "e-1"]),
        (f(&[("type", "orders.*")]), vec!["e-4", "e-2", "e-5", "e-1"]),
        (f(&[("type", "orders")]), vec![]),
        (f(&[("source", "svc-a")]), vec!["e-4", "e-1"]),
        (f(&[("correlationId", "c-1")]), vec!["e-6", "e-3", "e-1"]),
        (f(&[("userId", "u-2")]), vec!["e-3"]),
        (f(&[("eventId", "e-5")]), vec!["e-5"]),
        (
            f(&[("type", "orders.*"), ("key", "k-1"), ("source", "svc-a")]),
            vec!["e-4", "e-1"],
        ),
        (
            f(&[("from", &from), ("to", &to)]),
            vec!["e-4", "e-2", "e-5", "e-3"],
        ),
        (
            f(&[("from", "2023-11-14T22:13:20.030Z")]),
            vec!["e-6", "e-4", "e-2", "e-5"],
        ),
        (
            f(&[("to", "2023-11-14T23:13:20.020+01:00")]),
            vec!["e-3", "e-1"],
        ),
    ] {
        let what = format!("{filters:?}");
        assert_eq!(
            ids(&search(browser, on(&topic, filters)).await),
            want,
            "{what}"
        );
    }

    // Pages of two: together they are the whole result, in the same order, nothing twice.
    let mut pages = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = search(
            browser,
            SearchRequest {
                limit: Some(2),
                cursor: cursor.clone(),
                ..on(&topic, EventFilter::default())
            },
        )
        .await;
        pages.push(ids(&page));
        if page.exhausted {
            assert!(page.cursor.is_none());
            break;
        }
        assert_eq!(page.stopped_by, Some(StoppedBy::Limit));
        cursor = page.cursor;
        assert!(pages.len() < 10, "{pages:?}");
    }
    assert_eq!(
        pages,
        vec![vec!["e-6", "e-4"], vec!["e-2", "e-5"], vec!["e-3", "e-1"]]
    );

    // A cursor only continues its own search; what cannot be honoured is invalid.
    let page = search(
        browser,
        SearchRequest {
            limit: Some(2),
            ..on(&topic, EventFilter::default())
        },
    )
    .await;
    let refused = browser
        .search(SearchRequest {
            limit: Some(2),
            cursor: page.cursor,
            ..on(&topic, f(&[("type", "x")]))
        })
        .await;
    assert!(
        matches!(&refused, Err(BrowseError::Invalid(m)) if m.contains("another search")),
        "{refused:?}"
    );
    for bad in [
        SearchRequest {
            cursor: Some("%%".into()),
            ..on(&topic, EventFilter::default())
        },
        on("not-configured", EventFilter::default()),
        SearchRequest {
            limit: Some(101),
            ..on(&topic, EventFilter::default())
        },
        on(&topic, f(&[("from", "tomorrow")])),
        on(&topic, f(&[("from", "20"), ("to", "10")])),
    ] {
        let what = format!("{bad:?}");
        assert!(
            matches!(browser.search(bad).await, Err(BrowseError::Invalid(_))),
            "{what}"
        );
    }

    // A browsable source that does not exist is missing, not an error (and is not created).
    for _ in 0..2 {
        let page = search(browser, on("absent", EventFilter::default())).await;
        assert!(
            page.missing && page.results.is_empty() && page.exhausted,
            "{page:?}"
        );
    }
    let sources = browser.sources(SourcesRequest {}).await.unwrap();
    assert_eq!(sources.sources.len(), 2);
    assert_eq!(sources.sources[0].source, topic);
    assert_eq!(
        sources.sources[0]
            .segments
            .iter()
            .map(|s| (s.segment.as_str(), s.first.as_str(), s.head.as_str()))
            .collect::<Vec<_>>(),
        [("0", "0", "2"), ("1", "0", "2"), ("2", "0", "2")]
    );
    assert!(sources.sources[1].missing, "the browser created the topic");
    drop(broker);
}

/// Producers set `CreateTime`, so a partition's timestamps need not rise: `to` still finds a
/// record written after a later-stamped one (within `TO_SKEW`), and `from` and `to` hold for every
/// record.
#[tokio::test]
async fn from_and_to_find_records_with_out_of_order_timestamps() {
    let broker = kafka().await;
    let topic = unique("skew");
    create_topic(&broker.bootstrap, &topic, 1).await;
    produce(
        &broker.bootstrap,
        &topic,
        &[
            library(0, 10, "k", "s.A", "e-10"),
            library(0, 100, "k", "s.A", "e-100"),
            library(0, 20, "k", "s.A", "e-20"),
            library(0, 60_000, "k", "s.A", "e-60000"),
            library(0, 30, "k", "s.A", "e-30"),
            library(0, 5, "k", "s.A", "e-5"),
        ],
    )
    .await;
    let browser = browser(&broker.bootstrap, &[&topic], defaults());
    let window = |from: Option<i64>, to: Option<i64>| EventFilter {
        from: from.map(|t| TimeArg::Millis(T + t)),
        to: to.map(|t| TimeArg::Millis(T + t)),
        ..EventFilter::default()
    };
    for (filters, want) in [
        (window(None, Some(50)), vec!["e-5", "e-30", "e-20", "e-10"]),
        (window(Some(15), Some(50)), vec!["e-30", "e-20"]),
        (window(Some(50), None), vec!["e-60000", "e-100"]),
    ] {
        let what = format!("{filters:?}");
        assert_eq!(
            ids(&search(&*browser, on(&topic, filters)).await),
            want,
            "{what}"
        );
    }
    drop(broker);
}

/// Records compressed by their producer (any codec a Kafka client may use) are found and read.
#[tokio::test]
async fn compressed_records_are_found_and_read() {
    let broker = kafka().await;
    let topic = unique("codecs");
    create_topic(&broker.bootstrap, &topic, 1).await;
    for (i, codec) in ["gzip", "snappy", "lz4", "zstd"].into_iter().enumerate() {
        let record = library(0, i as i64, codec, "c.Packed", &format!("e-{codec}"));
        produce_compressed(&broker.bootstrap, &topic, &[record], codec).await;
    }
    let browser = browser(&broker.bootstrap, &[&topic], defaults());
    let page = search(&*browser, SearchRequest::default()).await;
    assert_eq!(ids(&page), ["e-zstd", "e-lz4", "e-snappy", "e-gzip"]);
    let record = get(&*browser, &topic, "0", "3", true).await.unwrap();
    assert_eq!(record.summary.key.as_deref(), Some("zstd"));
    assert_eq!(record.summary.headers["id"], "e-zstd");
    assert_eq!(record.payload, Some(Some("{\"id\":\"e-zstd\"}".into())));
    drop(broker);
}

/// Many partitions read at once: every page of a search with a cursor continues the last, and
/// together they are the whole list, newest first, nothing twice.
#[tokio::test]
async fn many_partitions_page_newest_first_without_skipping_or_repeating() {
    let broker = kafka().await;
    let topic = unique("wide");
    create_topic(&broker.bootstrap, &topic, 24).await;
    let records: Vec<Rec> = (0..24 * 30)
        .map(|i| {
            let ty = if i % 3 == 0 { "w.Hit" } else { "w.Miss" };
            // Rising within a partition, interleaved across them.
            let ts = (i / 24) * 100 + (i % 24) * 37 % 100;
            library(i % 24, ts as i64, "k", ty, &format!("e-{i}"))
        })
        .collect();
    produce(&broker.bootstrap, &topic, &records).await;
    let browser = browser(
        &broker.bootstrap,
        &[&topic],
        BrowseSettings {
            max_results: 1000,
            ..defaults()
        },
    );
    let hits = EventFilter {
        type_name: Some("w.Hit".into()),
        ..EventFilter::default()
    };
    let all = search(
        &*browser,
        SearchRequest {
            limit: Some(1000),
            ..on(&topic, hits.clone())
        },
    )
    .await;
    assert!(all.exhausted, "{all:?}");
    let whole = ids(&all);
    assert_eq!(whole.len(), 24 * 10);
    let times: Vec<i64> = all.results.iter().map(|r| r.timestamp.unwrap()).collect();
    assert!(times.windows(2).all(|w| w[0] >= w[1]), "{times:?}");

    let mut got = Vec::new();
    let mut cursor: Option<String> = None;
    for pages in 1.. {
        assert!(pages <= 40, "{} of {} found", got.len(), whole.len());
        let page = search(
            &*browser,
            SearchRequest {
                limit: Some(7),
                cursor: cursor.clone(),
                ..on(&topic, hits.clone())
            },
        )
        .await;
        got.extend(ids(&page));
        match page.cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(got, whole);
    drop(broker);
}

#[tokio::test]
async fn budgets_stop_a_search_and_concurrency_is_bounded() {
    let broker = kafka().await;
    let big = unique("big");
    create_topic(&broker.bootstrap, &big, 1).await;
    let bootstrap = broker.bootstrap.clone();
    let topic = big.clone();
    tokio::task::spawn_blocking(move || produce_many(&bootstrap, &topic, 0, 300_000, T))
        .await
        .unwrap();
    let nope = EventFilter {
        key: Some("nope".into()),
        ..EventFilter::default()
    };

    // The scan budget (a setting, and lowered by a request): records read, nothing found, a
    // cursor that moves on.
    let scan = browser(
        &broker.bootstrap,
        &[&big],
        BrowseSettings {
            max_scanned: 100,
            ..defaults()
        },
    );
    let page = search(&*scan, on(&big, nope.clone())).await;
    assert_eq!(page.stopped_by, Some(StoppedBy::ScanBudget));
    assert_eq!(page.scanned, 100);
    assert!(!page.exhausted && page.results.is_empty());
    let next = search(
        &*scan,
        SearchRequest {
            cursor: page.cursor.clone(),
            ..on(&big, nope.clone())
        },
    )
    .await;
    assert_eq!(next.stopped_by, Some(StoppedBy::ScanBudget));
    assert_ne!(next.cursor, page.cursor, "the next page moved on");
    let lowered = search(
        &*scan,
        SearchRequest {
            max_scanned: Some(10),
            ..on(&big, nope.clone())
        },
    )
    .await;
    assert_eq!(lowered.scanned, 10, "the request lowers the budget");
    let raised = search(
        &*scan,
        SearchRequest {
            max_scanned: Some(1_000_000),
            ..on(&big, nope.clone())
        },
    )
    .await;
    assert_eq!(raised.scanned, 100, "but never raises it");
    // The newest records come first: the last ones written.
    let newest = search(
        &*scan,
        SearchRequest {
            limit: Some(3),
            ..on(&big, EventFilter::default())
        },
    )
    .await;
    let keys: Vec<&str> = newest
        .results
        .iter()
        .map(|r| r.key.as_deref().unwrap())
        .collect();
    assert_eq!(keys, ["k-299999", "k-299998", "k-299997"]);

    // The time budget, and one search at a time.
    let slow = browser(
        &broker.bootstrap,
        &[&big],
        BrowseSettings {
            max_scanned: 10_000_000,
            timeout: Duration::from_millis(300),
            max_searches: 1,
            ..defaults()
        },
    );
    let first = {
        let slow = Arc::clone(&slow);
        let nope = nope.clone();
        let big = big.clone();
        tokio::spawn(async move { slow.search(on(&big, nope)).await })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    let busy = slow.search(on(&big, nope.clone())).await;
    assert!(matches!(busy, Err(BrowseError::Busy(_))), "{busy:?}");
    let page = first.await.unwrap().unwrap();
    assert_eq!(page.stopped_by, Some(StoppedBy::TimeBudget));
    assert!(page.scanned < 300_000, "{}", page.scanned);
    assert!(page.cursor.is_some());
    // The slot is given back.
    search(
        &*slow,
        SearchRequest {
            limit: Some(1),
            ..on(&big, EventFilter::default())
        },
    )
    .await;
    drop(broker);
}

// ---- one record ----------------------------------------------------------------------------

#[tokio::test]
async fn a_record_carries_every_header_and_the_payload_only_when_asked() {
    let broker = kafka().await;
    let topic = unique("detail");
    create_topic(&broker.bootstrap, &topic, 1).await;
    produce(
        &broker.bootstrap,
        &topic,
        &[
            Rec::on(0)
                .at(T)
                .key(b"k\xff".to_vec())
                .header("type", "a.Placed")
                .header("id", "e-1")
                .header("bin", b"\x00\xff".to_vec())
                .header("\u{e9}\u{e9}", "named in UTF-8")
                .payload("{\"secret\":\"s3cr3t\"}"),
            Rec::on(0)
                .at(T + 1)
                .header("type", "a.Raw")
                .payload(b"\xff\xfe".to_vec()),
            Rec::on(0).at(T + 2).header("type", "a.Tombstone"),
        ],
    )
    .await;
    let browser = browser(&broker.bootstrap, &[&topic], defaults());
    let browser = &*browser;

    let unasked = get(browser, &topic, "0", "0", false).await.unwrap();
    assert_eq!(unasked.summary.key.as_deref(), Some("k\\xff"));
    assert_eq!(unasked.summary.headers["type"], "a.Placed");
    assert!(unasked.payload.is_none() && unasked.payload_base64.is_none());
    assert!(!serde_json::to_string(&unasked).unwrap().contains("s3cr3t"));
    let bin = unasked
        .all_headers
        .iter()
        .find(|h| h.name == "bin")
        .unwrap();
    assert_eq!(
        (bin.value.as_deref(), bin.value_hex.as_deref()),
        (None, Some("00ff"))
    );
    assert!(unasked.all_headers.iter().any(|h| h.name == "\u{e9}\u{e9}"));

    let asked = get(browser, &topic, "0", "0", true).await.unwrap();
    assert_eq!(asked.payload, Some(Some("{\"secret\":\"s3cr3t\"}".into())));
    assert!(!asked.payload_truncated);
    let raw = get(browser, &topic, "0", "1", true).await.unwrap();
    assert_eq!(raw.payload_base64.as_deref(), Some("//4="));
    assert!(raw.payload.is_none());
    let tombstone = get(browser, &topic, "0", "2", true).await.unwrap();
    assert_eq!(tombstone.payload, Some(None));
    let small = browser_small_payload(&broker.bootstrap, &topic);
    let cut = get(&*small, &topic, "0", "0", true).await.unwrap();
    assert_eq!(
        (cut.payload, cut.payload_truncated),
        (Some(Some("{\"sec".into())), true),
        "cut at the browser's payload bound"
    );

    // Out of range, unknown partition, malformed, unconfigured source.
    for (segment, position, source, not_found) in [
        ("0", "3", topic.as_str(), true),
        ("7", "0", topic.as_str(), true),
        ("0", "-1", topic.as_str(), false),
        ("x", "0", topic.as_str(), false),
        ("0", "0", "elsewhere", false),
    ] {
        let answer = get(browser, source, segment, position, false).await;
        let ok = if not_found {
            matches!(answer, Err(BrowseError::NotFound(_)))
        } else {
            matches!(answer, Err(BrowseError::Invalid(_)))
        };
        assert!(ok, "{source} {segment}/{position}: {answer:?}");
    }
    drop(broker);
}

fn browser_small_payload(bootstrap: &str, topic: &str) -> Arc<dyn EventBrowser> {
    browser(
        bootstrap,
        &[topic],
        BrowseSettings {
            max_payload: 5,
            ..defaults()
        },
    )
}

// ---- tail ----------------------------------------------------------------------------------

#[tokio::test]
async fn a_tail_delivers_new_records_ends_on_time_and_drops_a_slow_reader() {
    let broker = kafka().await;
    let topic = unique("tail");
    create_topic(&broker.bootstrap, &topic, 2).await;
    produce(
        &broker.bootstrap,
        &topic,
        &[library(0, 0, "old", "t.Hit", "before")],
    )
    .await;
    let short = browser(
        &broker.bootstrap,
        &[&topic],
        BrowseSettings {
            tail_duration: Duration::from_secs(2),
            max_tails: 1,
            ..defaults()
        },
    );
    let hits = TailRequest {
        filters: EventFilter {
            type_name: Some("t.Hit".into()),
            ..EventFilter::default()
        },
        ..TailRequest::default()
    };

    let started = Instant::now();
    let mut tail = short.tail(hits.clone()).await.unwrap();
    match next_part(&mut tail).await {
        TailEvent::Start(start) => {
            assert_eq!((start.source.as_str(), start.segments), (topic.as_str(), 2))
        }
        other => panic!("{other:?}"),
    }
    let busy = short.tail(TailRequest::default()).await;
    assert!(
        matches!(busy, Err(BrowseError::Busy(_))),
        "one tail at a time"
    );
    produce(
        &broker.bootstrap,
        &topic,
        &[
            library(1, 1, "k", "t.Miss", "miss"),
            library(1, 2, "k", "t.Hit", "hit"),
        ],
    )
    .await;
    match next_event(&mut tail).await {
        TailEvent::Record(r) => assert_eq!(r.headers["id"], "hit", "{r:?}"),
        other => panic!("{other:?}"),
    }
    match next_event(&mut tail).await {
        TailEvent::End(end) => assert_eq!(end.reason, EndReason::MaxDuration),
        other => panic!("{other:?}"),
    }
    assert!(started.elapsed() >= Duration::from_secs(2));
    assert!(tail.next().await.is_none(), "closed");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut again = short
        .tail(TailRequest::default())
        .await
        .expect("the slot was given back");
    assert!(matches!(next_part(&mut again).await, TailEvent::Start(_)));

    // A reader that drops a live tail frees its slot at the tail's next poll.
    drop(again);
    let dropped = Instant::now();
    let reopened = loop {
        match short.tail(TailRequest::default()).await {
            Ok(tail) => break tail,
            Err(BrowseError::Busy(_)) => {
                assert!(
                    dropped.elapsed() < Duration::from_secs(2),
                    "the slot is free within 2 s of the drop"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("{e:?}"),
        }
    };
    drop(reopened);

    // A reader that does not read: its tail ends, and the slot comes back.
    let long = browser(
        &broker.bootstrap,
        &[&topic],
        BrowseSettings {
            max_tails: 1,
            ..defaults()
        },
    );
    let mut slow = long.tail(TailRequest::default()).await.unwrap();
    assert!(matches!(next_part(&mut slow).await, TailEvent::Start(_)));
    let bootstrap = broker.bootstrap.clone();
    let flood = topic.clone();
    tokio::task::spawn_blocking(move || produce_many(&bootstrap, &flood, 0, 30_000, T))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut records = 0;
    let reason = loop {
        match next_part(&mut slow).await {
            TailEvent::Record(_) => records += 1,
            TailEvent::End(end) => break end.reason,
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_eq!(reason, EndReason::SlowReader);
    assert!(records < 30_000, "{records}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let next = long
        .tail(TailRequest::default())
        .await
        .expect("the slot was given back");
    drop((slow, next));
    drop(broker);
}

// ---- lag -----------------------------------------------------------------------------------

#[tokio::test]
async fn lag_shows_groups_with_and_without_members_and_the_browser_creates_none() {
    let broker = kafka().await;
    let topic = unique("groups");
    let other = unique("other");
    create_topic(&broker.bootstrap, &topic, 2).await;
    create_topic(&broker.bootstrap, &other, 1).await;
    let mut records: Vec<Rec> = (0..10).map(|i| library(0, i, "k", "a.B", "e")).collect();
    records.extend((0..5).map(|i| library(1, i, "k", "a.B", "e")));
    produce(&broker.bootstrap, &topic, &records).await;
    let before = groups(&broker.bootstrap);

    let bootstrap = broker.bootstrap.clone();
    let (t, o) = (topic.clone(), other.clone());
    let live = tokio::task::spawn_blocking(move || {
        commit(&bootstrap, "g-empty", &t, &[(0, 4), (1, 5)]);
        commit(&bootstrap, "g-elsewhere", &o, &[(0, 0)]);
        // Named like the browser's group.id: listed like any other (it has none of its own).
        commit(&bootstrap, "snodra-event-browser-planted", &o, &[(0, 0)]);
        Member::join(&bootstrap, "g-live", &t, &[(0, 10), (1, 2)])
    })
    .await
    .unwrap();

    let browser = browser(&broker.bootstrap, &[&topic, &other], defaults());
    let view = browser.lag(LagRequest::default()).await.unwrap();
    assert_eq!(view.source, topic);
    assert!(!view.truncated && !view.missing);
    let names: Vec<&str> = view
        .subscriptions
        .iter()
        .map(|g| g.subscription.as_str())
        .collect();
    assert_eq!(names, ["g-empty", "g-live"], "by lag, descending: {view:?}");
    let empty = &view.subscriptions[0];
    assert_eq!((empty.members, empty.lag), (0, 6));
    assert_eq!(empty.state, "Empty");
    assert_eq!(
        serde_json::to_value(&empty.segments).unwrap(),
        serde_json::json!([
            {"segment": "0", "committed": "4", "head": "10", "lag": 6},
            {"segment": "1", "committed": "5", "head": "5", "lag": 0},
        ])
    );
    let live_view = &view.subscriptions[1];
    assert_eq!((live_view.members, live_view.lag), (1, 3));
    assert_eq!(live_view.state, "Stable");
    let elsewhere = browser
        .lag(LagRequest {
            source: Some(other.clone()),
        })
        .await
        .unwrap();
    let names: Vec<&str> = elsewhere
        .subscriptions
        .iter()
        .map(|g| g.subscription.as_str())
        .collect();
    assert_eq!(names, ["g-elsewhere", "snodra-event-browser-planted"]);
    assert!(matches!(
        browser
            .lag(LagRequest {
                source: Some("nope".into())
            })
            .await,
        Err(BrowseError::Invalid(_))
    ));

    // Everything the browser does, then the broker's groups: only the test's own are new.
    search(
        &*browser,
        SearchRequest {
            limit: Some(5),
            ..SearchRequest::default()
        },
    )
    .await;
    get(&*browser, &topic, "0", "0", true).await.unwrap();
    browser.sources(SourcesRequest {}).await.unwrap();
    let mut tail = browser.tail(TailRequest::default()).await.unwrap();
    assert!(matches!(next_part(&mut tail).await, TailEvent::Start(_)));
    produce(
        &broker.bootstrap,
        &topic,
        &[library(0, 99, "k", "a.B", "x")],
    )
    .await;
    assert!(matches!(next_event(&mut tail).await, TailEvent::Record(_)));
    drop(tail);
    browser.lag(LagRequest::default()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let after = groups(&broker.bootstrap);
    let new: Vec<&String> = after.difference(&before).collect();
    assert_eq!(
        new,
        [
            "g-elsewhere",
            "g-empty",
            "g-live",
            "snodra-event-browser-planted"
        ],
        "{after:?}"
    );
    drop(live);
    drop(broker);
}

// ---- without Kafka -------------------------------------------------------------------------

/// Every call against an address nothing listens on is `unavailable` within its budget, and the
/// caller's runtime goes on meanwhile (every Kafka call is on the blocking pool).
#[tokio::test(flavor = "current_thread")]
async fn an_unreachable_kafka_is_unavailable_and_never_blocks_the_runtime() {
    let budget = Duration::from_secs(2);
    let browser = browser(
        &closed_port(),
        &["snodra.events"],
        BrowseSettings {
            timeout: budget,
            ..defaults()
        },
    );
    let started = Instant::now();
    let ticker = tokio::spawn(async move {
        let mut ticks = 0u32;
        let mut late = Duration::ZERO;
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        loop {
            let before = Instant::now();
            interval.tick().await;
            late = late.max(before.elapsed().saturating_sub(Duration::from_millis(50)));
            ticks += 1;
            if ticks == 30 {
                return late;
            }
        }
    });
    let (search, record, sources, lag, tail) = tokio::join!(
        browser.search(SearchRequest::default()),
        get(&*browser, "snodra.events", "0", "0", false),
        browser.sources(SourcesRequest {}),
        browser.lag(LagRequest::default()),
        browser.tail(TailRequest::default()),
    );
    let late = ticker.await.unwrap();
    assert!(
        late < Duration::from_millis(500),
        "the runtime stalled {late:?}"
    );
    for (what, answer) in [
        ("search", search.err()),
        ("get", record.err()),
        ("sources", sources.err()),
        ("lag", lag.err()),
        ("tail", tail.err()),
    ] {
        assert!(
            matches!(&answer, Some(BrowseError::Unavailable(m)) if m.contains("Kafka")),
            "{what}: {answer:?}"
        );
    }
    assert!(started.elapsed() < budget + CLOSE_GRACE + Duration::from_secs(1));
}

/// A tail whose caller leaves while its Kafka setup still runs keeps its slot until that setup
/// ends: opening and abandoning tails never runs more setups than the tail bound.
#[tokio::test]
async fn an_abandoned_tail_keeps_its_slot_until_its_setup_ends() {
    let budget = Duration::from_secs(3);
    let browser = browser(
        &closed_port(),
        &["snodra.events"],
        BrowseSettings {
            timeout: budget,
            max_tails: 4,
            ..defaults()
        },
    );
    let opened = Instant::now();
    for _ in 0..4 {
        let browser = Arc::clone(&browser);
        let call = tokio::spawn(async move { browser.tail(TailRequest::default()).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        call.abort();
    }
    let busy = browser.tail(TailRequest::default()).await;
    assert!(
        matches!(busy, Err(BrowseError::Busy(_))),
        "{:?}",
        busy.err()
    );
    assert!(opened.elapsed() < budget, "{:?}", opened.elapsed());
    // The setups end at their budget and give their slots back.
    tokio::time::sleep_until((opened + budget + Duration::from_secs(1)).into()).await;
    let answer = browser.tail(TailRequest::default()).await;
    assert!(
        matches!(answer, Err(BrowseError::Unavailable(_))),
        "{:?}",
        answer.err()
    );
}

/// A broker that accepts connections and never answers: every call is `unavailable` at its
/// budget, and its consumer closes within the close grace, so the slot is free again by the
/// budget plus the grace.
#[tokio::test]
async fn a_silent_broker_frees_every_slot_within_the_budget_and_the_close_grace() {
    let budget = Duration::from_secs(2);
    let browser = browser(
        &silent_broker(),
        &["snodra.events"],
        BrowseSettings {
            timeout: budget,
            max_searches: 1,
            max_tails: 1,
            ..defaults()
        },
    );
    for call in ["search", "get", "sources", "lag", "tail"] {
        let run = || async {
            match call {
                "search" => browser.search(SearchRequest::default()).await.err(),
                "get" => get(&*browser, "snodra.events", "0", "0", false).await.err(),
                "sources" => browser.sources(SourcesRequest {}).await.err(),
                "lag" => browser.lag(LagRequest::default()).await.err(),
                _ => browser.tail(TailRequest::default()).await.err(),
            }
        };
        let started = Instant::now();
        let answer = run().await;
        assert!(
            matches!(answer, Some(BrowseError::Unavailable(_))),
            "{call}: {answer:?}"
        );
        tokio::time::sleep_until((started + budget + CLOSE_GRACE).into()).await;
        let again = run().await;
        assert!(
            matches!(again, Some(BrowseError::Unavailable(_))),
            "{call}: the slot is free again: {again:?}"
        );
    }
}
