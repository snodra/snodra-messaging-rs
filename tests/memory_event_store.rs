//! The in-memory event store (`event_store::memory`, ADR 0027): the conformance kit on it, with
//! no container, and what the kit cannot see (its settings, misuse without panics, a dropped
//! subscription, paused time, its bounds). Needs the feature `testing`.
#![cfg(feature = "testing")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use snodra_messaging::event_store::browse::{BrowseError, BrowseSettings, TailRequest};
use snodra_messaging::event_store::conformance::{
    EventStoreKit, run_event_store_conformance, search_stops_at_bounds,
};
use snodra_messaging::event_store::memory::{
    DEFAULT_SEGMENTS, MemoryEventStore, MemoryEventStoreConfig,
};
use snodra_messaging::event_store::{
    Capability, EventStore, EventStoreError, OutgoingEvent, ResetTarget, SubscriptionEvent,
    SubscriptionSpec,
};
use snodra_messaging::saga::Clock;
use snodra_messaging::store::MessageStore;
use snodra_messaging::store::memory::MemoryStore;

const SOURCE: &str = "kraken.events";

fn browsing(source: &str) -> MemoryEventStoreConfig {
    MemoryEventStoreConfig {
        browse: BrowseSettings {
            sources: vec![source.to_string()],
            ..BrowseSettings::default()
        },
        ..MemoryEventStoreConfig::default()
    }
}

fn store(config: MemoryEventStoreConfig) -> MemoryEventStore {
    MemoryEventStore::new(config).expect("a valid configuration")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_kit_passes_on_the_memory_event_store() {
    let events = store(browsing("conformance.events"));
    let messages: Arc<dyn MessageStore> = Arc::new(MemoryStore::new());
    run_event_store_conformance(
        "conformance.events",
        move || Arc::new(events.clone()) as Arc<dyn EventStore>,
        messages,
    )
    .await;
}

/// The scan-budget check on a source that already holds a few hundred older events.
#[tokio::test(flavor = "multi_thread")]
async fn the_scan_budget_check_passes_on_a_pre_filled_source() {
    let source = "conformance.prefilled";
    let events = store(browsing(source));
    for i in 0..400 {
        events
            .publish(&event(source, &format!("key-{i}"), &format!("old-{i}")))
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    let make = move || Arc::new(events.clone()) as Arc<dyn EventStore>;
    let messages = MemoryStore::new();
    let kit = EventStoreKit {
        source,
        make_events: &make,
        store: &messages,
    };
    search_stops_at_bounds(&kit).await;
}

/// The scan-budget check on a store whose clock is 5 s behind the kit's.
#[tokio::test(flavor = "multi_thread")]
async fn the_scan_budget_check_passes_on_a_store_with_a_lagging_clock() {
    let source = "conformance.lagging";
    let events = store(MemoryEventStoreConfig {
        clock: Clock::new(|| SystemTime::now() - Duration::from_secs(5)),
        ..browsing(source)
    });
    let make = move || Arc::new(events.clone()) as Arc<dyn EventStore>;
    let messages = MemoryStore::new();
    let kit = EventStoreKit {
        source,
        make_events: &make,
        store: &messages,
    };
    search_stops_at_bounds(&kit).await;
}

#[test]
fn it_is_named_memory_and_declares_replay_browse_and_lag() {
    let events = MemoryEventStore::default();
    assert_eq!(events.name(), "memory");
    assert_eq!(
        events.capabilities(),
        [Capability::Replay, Capability::Browse, Capability::Lag]
    );
    assert!(!events.supports(Capability::NativeDeadLetter));
    assert!(events.browser().is_some());
}

#[test]
fn the_number_of_segments_is_a_setting_of_four_by_default() {
    assert_eq!(DEFAULT_SEGMENTS, 4);
    assert_eq!(MemoryEventStoreConfig::default().segments, 4);
    for bad in [0, 100_000] {
        let refused = MemoryEventStore::new(MemoryEventStoreConfig {
            segments: bad,
            ..MemoryEventStoreConfig::default()
        });
        assert!(
            matches!(refused, Err(EventStoreError::Invalid(_))),
            "{bad} segments"
        );
    }
    let refused = MemoryEventStore::new(MemoryEventStoreConfig {
        browse: BrowseSettings {
            sources: Vec::new(),
            ..BrowseSettings::default()
        },
        ..MemoryEventStoreConfig::default()
    });
    assert!(matches!(refused, Err(EventStoreError::Invalid(_))));
}

/// Every key goes to one segment of the configured ones; the segments are `0` to `n-1`.
#[tokio::test]
async fn a_key_is_hashed_to_one_of_the_segments() {
    let events = store(MemoryEventStoreConfig {
        segments: 7,
        ..MemoryEventStoreConfig::default()
    });
    let mut sub = events.subscribe(spec("p")).await.expect("subscribe");
    let assigned = match sub.next().await.expect("next") {
        SubscriptionEvent::Assigned(segments) => segments,
        other => panic!("assigned first: {other:?}"),
    };
    assert_eq!(assigned, ["0", "1", "2", "3", "4", "5", "6"]);
    for i in 0..50 {
        let key = format!("key-{}", i % 5);
        events
            .publish(&event(SOURCE, &key, &format!("e-{i}")))
            .await
            .expect("publish");
    }
    let mut segment_of: HashMap<String, String> = HashMap::new();
    for _ in 0..50 {
        let SubscriptionEvent::Delivery(d) = sub.next().await.expect("next") else {
            panic!("a delivery");
        };
        let key = d.key.clone().expect("a key");
        let seen = segment_of.entry(key.clone()).or_insert(d.segment.clone());
        assert_eq!(*seen, d.segment, "{key} stays on one segment");
    }
    sub.close().await;
}

/// Events without a key go round the segments from `0`; the members are `memory-<n>`.
#[tokio::test]
async fn unkeyed_events_go_round_the_segments_from_the_first() {
    let events = MemoryEventStore::default();
    for i in 0..5 {
        let mut e = event(SOURCE, "", &format!("e-{i}"));
        e.key = None;
        events.publish(&e).await.expect("publish");
    }
    let segments: Vec<String> = events
        .events(SOURCE)
        .into_iter()
        .map(|d| d.segment)
        .collect();
    let mut sorted = segments.clone();
    sorted.sort();
    assert_eq!(sorted, ["0", "0", "1", "2", "3"]);
    let first = events
        .events(SOURCE)
        .into_iter()
        .find(|d| d.headers["id"] == "e-0")
        .unwrap();
    assert_eq!(first.segment, "0");
    let sub = events.subscribe(spec("p")).await.expect("subscribe");
    let members = events.admin(&spec("p")).unwrap().members().unwrap();
    assert_eq!(members.len(), 1);
    assert!(members[0].starts_with("memory-"), "{members:?}");
    sub.close().await;
}

/// Misuse answers with an error, never a panic: an empty source, an unknown segment, a position
/// that is not one.
#[tokio::test]
async fn misuse_is_an_error_not_a_panic() {
    let events = MemoryEventStore::default();
    assert!(matches!(
        events.publish(&event("", "k", "e-1")).await,
        Err(EventStoreError::Rejected(_))
    ));
    assert!(matches!(
        events
            .subscribe(SubscriptionSpec {
                name: "p".into(),
                source: String::new(),
            })
            .await
            .err(),
        Some(EventStoreError::Invalid(_))
    ));
    let mut sub = events.subscribe(spec("p")).await.expect("subscribe");
    sub.next().await.expect("assigned");
    for (segment, position) in [("9", "0"), ("x", "0"), ("0", "x"), ("0", "5")] {
        assert!(
            matches!(sub.ack(segment, position), Err(EventStoreError::Invalid(_))),
            "ack {segment} {position}"
        );
        assert!(
            matches!(
                sub.redeliver(segment, position),
                Err(EventStoreError::Invalid(_))
            ),
            "redeliver {segment} {position}"
        );
    }
    assert_eq!(
        events.compare("x", "10"),
        std::cmp::Ordering::Greater,
        "no panic"
    );
    sub.close().await;
    let admin = events.admin(&spec("p")).expect("admin");
    let elsewhere = snodra_messaging::event_store::ReplayRange {
        source: "another.source".into(),
        segment: "0".into(),
        from: "0".into(),
        until: String::new(),
    };
    assert!(
        matches!(admin.apply(&[elsewhere]), Err(EventStoreError::Invalid(_))),
        "a range of another source"
    );
    for target in [
        ResetTarget::Positions(vec![("9".into(), "0".into())]),
        ResetTarget::Positions(vec![("0".into(), "x".into())]),
        ResetTarget::Positions(vec![("0".into(), "1".into())]),
    ] {
        assert!(
            matches!(admin.plan(&target), Err(EventStoreError::Invalid(_))),
            "{target:?}"
        );
    }
}

/// A subscription dropped without a close (a crashed task) leaves the subscription: the other
/// instance gets its segments and what it did not acknowledge.
#[tokio::test]
async fn a_dropped_subscription_frees_its_segments() {
    let events = store(MemoryEventStoreConfig {
        segments: 2,
        ..MemoryEventStoreConfig::default()
    });
    let mut a = events.subscribe(spec("p")).await.expect("subscribe");
    assert!(matches!(
        a.next().await.expect("next"),
        SubscriptionEvent::Assigned(_)
    ));
    events
        .publish(&event(SOURCE, "k", "e-1"))
        .await
        .expect("publish");
    assert!(matches!(
        a.next().await.expect("next"),
        SubscriptionEvent::Delivery(_)
    ));
    let mut b = events.subscribe(spec("p")).await.expect("subscribe");
    drop(a);
    let mut held = Vec::new();
    let mut delivered = Vec::new();
    while delivered.is_empty() {
        match tokio::time::timeout(Duration::from_secs(5), b.next())
            .await
            .expect("within 5 s")
            .expect("next")
        {
            SubscriptionEvent::Assigned(s) => held.extend(s),
            SubscriptionEvent::Revoked(_) => panic!("nothing to revoke"),
            SubscriptionEvent::Delivery(d) => delivered.push(d),
        }
    }
    assert_eq!(held.len(), 2, "both segments: {held:?}");
    assert_eq!(
        delivered[0].headers["id"], "e-1",
        "the unacknowledged event"
    );
    assert!(
        events
            .admin(&spec("p"))
            .expect("admin")
            .members()
            .expect("members")
            .len()
            == 1,
        "only b is a member"
    );
    b.close().await;
}

/// A segment moves to a new member only once the member holding it saw it revoked (its next
/// `next`): until then the holder may still be handling one of its events, and the new member
/// would break the order of that event's key.
#[tokio::test]
async fn a_segment_moves_only_after_its_holder_let_it_go() {
    let events = store(MemoryEventStoreConfig {
        segments: 2,
        ..MemoryEventStoreConfig::default()
    });
    let mut a = events.subscribe(spec("p")).await.expect("subscribe");
    assert!(matches!(
        a.next().await.expect("next"),
        SubscriptionEvent::Assigned(s) if s.len() == 2
    ));
    let mut b = events.subscribe(spec("p")).await.expect("subscribe");
    assert_eq!(
        b.next().await.expect("next"),
        SubscriptionEvent::Assigned(vec![]),
        "b joined holding nothing yet: its first assignment is empty"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), b.next())
            .await
            .is_err(),
        "nothing more for b while a has not let a segment go"
    );
    let SubscriptionEvent::Revoked(gone) = a.next().await.expect("next") else {
        panic!("a sees a segment revoked");
    };
    let SubscriptionEvent::Assigned(got) = b.next().await.expect("next") else {
        panic!("b gets it");
    };
    assert_eq!(gone, got);
    a.close().await;
    b.close().await;
}

/// The subscription waits on the store, not on a timer: under paused time it delivers as soon as
/// an event is published, 30 s of paused time later.
#[tokio::test(start_paused = true)]
async fn it_runs_on_paused_time() {
    let events = MemoryEventStore::default();
    let mut sub = events.subscribe(spec("p")).await.expect("subscribe");
    sub.next().await.expect("assigned");
    let publisher = events.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        publisher
            .publish(&event(SOURCE, "k", "e-1"))
            .await
            .expect("publish");
    });
    let started = tokio::time::Instant::now();
    let SubscriptionEvent::Delivery(d) = sub.next().await.expect("next") else {
        panic!("a delivery");
    };
    assert_eq!(d.headers["id"], "e-1");
    assert_eq!(started.elapsed(), Duration::from_secs(30));
    sub.close().await;
}

/// A search that reads a large source does not hold the store for its length: a publish made
/// while it runs waits at most a chunk's copy, not the search.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_search_does_not_hold_up_a_publish() {
    const EVENTS: usize = 300_000;
    let events = store(MemoryEventStoreConfig {
        browse: BrowseSettings {
            sources: vec![SOURCE.to_string()],
            max_scanned: 1_000_000,
            ..BrowseSettings::default()
        },
        ..MemoryEventStoreConfig::default()
    });
    for i in 0..EVENTS {
        events
            .publish(&event(SOURCE, &format!("k-{}", i % 64), &format!("e-{i}")))
            .await
            .expect("publish");
    }
    let browser = events.browser().expect("a browser");
    let request = snodra_messaging::event_store::browse::SearchRequest {
        filters: snodra_messaging::event_store::browse::EventFilter {
            q: Some("nowhere in any payload".into()),
            ..Default::default()
        },
        max_scanned: Some(1_000_000),
        ..Default::default()
    };
    let started = std::time::Instant::now();
    let search = tokio::spawn(async move { browser.search(request).await });
    let mut slowest = Duration::ZERO;
    let mut published = 0;
    while !search.is_finished() {
        let before = std::time::Instant::now();
        events
            .publish(&event(SOURCE, "k-late", &format!("late-{published}")))
            .await
            .expect("publish");
        slowest = slowest.max(before.elapsed());
        published += 1;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let took = started.elapsed();
    let page = search.await.expect("the search task").expect("the search");
    assert!(
        page.scanned >= EVENTS as u64,
        "the search read the whole source: {}",
        page.scanned
    );
    assert!(
        took > Duration::from_millis(100) && published > 10,
        "the search is long enough to tell ({took:?}, {published} publishes meanwhile)"
    );
    // Held for the whole search, the store would let one publish through, after it: more than
    // ten got through while it ran. Each waited less than the search (a loose bound, no ratio).
    assert!(
        slowest < took,
        "a publish waited {slowest:?} during a search of {took:?}"
    );
}

/// At most `max_tails` tails at once: one more is busy, and a tail that ended frees its slot.
#[tokio::test]
async fn tails_are_bounded() {
    let events = store(MemoryEventStoreConfig {
        browse: BrowseSettings {
            max_tails: 1,
            ..BrowseSettings::default()
        },
        ..MemoryEventStoreConfig::default()
    });
    events
        .publish(&event(SOURCE, "k", "e-1"))
        .await
        .expect("publish");
    let browser = events.browser().expect("a browser");
    let first = browser
        .tail(TailRequest::default())
        .await
        .expect("the first tail");
    assert!(matches!(
        browser.tail(TailRequest::default()).await.err(),
        Some(BrowseError::Busy(_))
    ));
    drop(first);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match browser.tail(TailRequest::default()).await {
            Ok(_) => break,
            Err(BrowseError::Busy(_)) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("a tail after the first ended: {e}"),
        }
    }
}

fn spec(name: &str) -> SubscriptionSpec {
    SubscriptionSpec {
        name: name.to_string(),
        source: SOURCE.to_string(),
    }
}

fn event(source: &str, key: &str, id: &str) -> OutgoingEvent {
    OutgoingEvent {
        source: source.to_string(),
        id: id.to_string(),
        type_name: "memory.Happened".to_string(),
        key: Some(key.to_string()),
        headers: HashMap::from([
            ("type".to_string(), "memory.Happened".to_string()),
            ("id".to_string(), id.to_string()),
        ]),
        payload: b"{}".to_vec(),
    }
}
