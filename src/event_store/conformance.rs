//! The event-store conformance kit (feature `testing`): the checks every
//! [`EventStore`] add-on passes, a third party's included. An add-on's own test hands the kit a
//! source it prepared, a way to make the add-on (one call per service instance) and a message
//! store (for the at-least-once check):
//!
//! ```ignore
//! #[tokio::test]
//! async fn my_event_store_passes_conformance() {
//!     let store: Arc<dyn MessageStore> = ..; // any message store add-on, its schema in place
//!     run_event_store_conformance("snodra.conformance", || Arc::new(MyEventStore::new(..)), store)
//!         .await;
//! }
//! ```
//!
//! - **The source** has at least two segments (the competing-instances check spreads them over
//!   two instances), and when the event store declares
//!   [`NativeDeadLetter`](Capability::NativeDeadLetter) its [`dead_letter_source`] exists too.
//!   The kit only appends to them, and must be their only publisher while it runs (the lag check
//!   reads the whole source).
//! - **Each call of `make_events`** is one service instance: a store reading the same event store
//!   (an in-process store returns the same one each time).
//! - **When the event store declares [`Browse`](Capability::Browse)**, its browser
//!   ([`EventStore::browser`]) reads `source` (one of its
//!   [`BrowseSettings::sources`](super::browse::BrowseSettings::sources)), with bounds no lower
//!   than the defaults' results (the kit asks for 50 at most). The newest-first checks publish
//!   their events 20 ms apart and assume the event store stamps them in milliseconds of its own
//!   (or the publisher's) clock, so each is newer than the one before.
//! - The checks run one after the other, each on subscription names of its own; a new name starts
//!   at the earliest position, so each check reads what the earlier ones appended and looks only
//!   at its own events (by their `id` header). Every check is public, so a failing one can be run
//!   by itself on an [`EventStoreKit`].
//! - The kit waits at most a minute for what the event store does in the background (an
//!   assignment, a lag refresh), and for every call it awaits: a publish, a subscribe or a close
//!   that does not answer fails the check naming the step, and so does a
//!   [`SubscriptionAdmin`] call (each runs on a thread of its own, so a blocked one is left
//!   behind rather than holding the runtime).
//!
//! # The checks
//!
//! Each check, with the capability it needs. The specification lists them with what each
//! asserts and the conformance ids that require them:
//! <https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-conformance-kit>.
//!
//! - [`headers_round_trip`]
//! - [`same_key_arrives_in_publish_order`]
//! - [`every_processor_gets_every_event`]
//! - [`instances_share_a_processor`]
//! - [`a_joining_instance_announces_its_first_assignment`]
//! - [`an_instance_left_without_segments_closes_its_change`]
//! - [`unacked_delivery_is_redelivered`]
//! - [`redelivered_after_commit_is_skipped`]
//! - [`paused_subscription_receives_nothing_and_resumes_in_place`]
//! - [`replay_from_earliest_redelivers_history`] (with `replay`)
//! - [`admin_without_replay_is_unsupported`] (without `replay`)
//! - [`lag_counts_unacked`] (with `lag`)
//! - [`lag_unknown_without_capability`] (without `lag`)
//! - [`dead_letter_source_receives`] (with `native-dead-letter`)
//! - [`search_finds_by_type_key_and_correlation`] (with `browse`)
//! - [`search_stops_at_bounds`] (with `browse`)
//! - [`tail_streams_new_events`] (with `browse`)
//! - [`browser_never_commits_a_position`] (with `browse`)
//! - [`no_browser_without_capability`] (without `browse`)

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use tokio::time::Instant;

use futures::StreamExt;

use super::browse::{
    BrowseError, EndReason, EventBrowser, EventFilter, EventSummary, GetRequest, LagRequest,
    LagView, SearchPage, SearchRequest, SegmentLag, SourcesRequest, StoppedBy, TailEvent,
    TailRequest, TailStream, TimeArg,
};
use super::{
    Capability, Delivery, EventStore, EventStoreError, FIRST_ASSIGNMENT_WITHIN, MAX_POSITION_BYTES,
    MAX_SEGMENT_BYTES, OutgoingEvent, ReplayRange, ResetTarget, Subscription, SubscriptionAdmin,
    SubscriptionEvent, SubscriptionSpec, dead_letter_source, headers, segment_order,
};
use crate::store::{MessageStore, ProcessedMarker};
use crate::time::{now_ms, rfc3339};

/// The longest a check waits for what the event store does in the background.
const WAIT: Duration = Duration::from_secs(60);
/// How long a check watches a subscription that must deliver nothing.
const QUIET: Duration = Duration::from_secs(3);
/// One turn of reading a subscription.
const SLICE: Duration = Duration::from_millis(250);
/// How long a check waits for the assignment change a joining instance may cause.
const JOIN_WINDOW: Duration = Duration::from_secs(6);
/// The type of the kit's events.
const EVENT_TYPE: &str = "conformance.Event";

/// Runs every check, and each one of a capability the event store declares (without `replay`,
/// `lag` or `browse`, the check that it says so), by name; the first failing check panics naming
/// itself and the event store. See the module docs for `source`, `make_events` and `store`.
pub async fn run_event_store_conformance(
    source: &str,
    make_events: impl Fn() -> Arc<dyn EventStore>,
    store: Arc<dyn MessageStore>,
) {
    let kit = EventStoreKit {
        source,
        make_events: &make_events,
        store: &*store,
    };
    let events = make_events();
    let name = events.name();
    macro_rules! run {
        ($($check:ident),* $(,)?) => {
            $( run_check(name, stringify!($check), $check(&kit)).await; )*
        };
    }
    run!(
        headers_round_trip,
        same_key_arrives_in_publish_order,
        every_processor_gets_every_event,
        instances_share_a_processor,
        a_joining_instance_announces_its_first_assignment,
        an_instance_left_without_segments_closes_its_change,
        unacked_delivery_is_redelivered,
        redelivered_after_commit_is_skipped,
        paused_subscription_receives_nothing_and_resumes_in_place,
    );
    if events.supports(Capability::Replay) {
        run!(replay_from_earliest_redelivers_history);
    } else {
        run!(admin_without_replay_is_unsupported);
    }
    if events.supports(Capability::Lag) {
        run!(lag_counts_unacked);
    } else {
        run!(lag_unknown_without_capability);
    }
    if events.supports(Capability::NativeDeadLetter) {
        run!(dead_letter_source_receives);
    }
    if events.supports(Capability::Browse) {
        run!(
            search_finds_by_type_key_and_correlation,
            search_stops_at_bounds,
            tail_streams_new_events,
            browser_never_commits_a_position,
        );
    } else {
        run!(no_browser_without_capability);
    }
}

/// What every check runs on.
pub struct EventStoreKit<'a> {
    /// The prepared source (module docs).
    pub source: &'a str,
    /// One service instance per call.
    pub make_events: &'a dyn Fn() -> Arc<dyn EventStore>,
    /// The message store of the at-least-once check.
    pub store: &'a dyn MessageStore,
}

impl EventStoreKit<'_> {
    fn instance(&self) -> Arc<dyn EventStore> {
        (self.make_events)()
    }

    /// A subscription name nothing has used.
    fn spec(&self) -> SubscriptionSpec {
        SubscriptionSpec {
            name: format!("conformance.{}", uuid::Uuid::new_v4().simple()),
            source: self.source.to_string(),
        }
    }
}

async fn run_check(events: &str, check: &str, run: impl Future<Output = ()>) {
    if let Err(panic) = AssertUnwindSafe(run).catch_unwind().await {
        let cause = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("(no message)");
        panic!("event store conformance check `{check}` failed on event store `{events}`: {cause}");
    }
}

// ---- the contract ------------------------------------------------------------------------------

/// The seven headers, any other header, the key (or none) and the payload arrive as published.
pub async fn headers_round_trip(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let mut keyed = event(kit.source, Some(fresh("key").as_str()));
    keyed
        .headers
        .insert("x-conformance".into(), "kept as given".into());
    let unkeyed = event(kit.source, None);
    publish_all(&*events, &[keyed.clone(), unkeyed.clone()]).await;

    let mut reader = Reader::join(&*events, &kit.spec()).await;
    let ids = ids(&[keyed.clone(), unkeyed.clone()]);
    reader
        .until("both events delivered", |r| r.has_all(&ids))
        .await;
    for sent in [&keyed, &unkeyed] {
        let got = reader.first(&sent.id);
        for name in headers::ALL {
            assert!(got.headers.contains_key(name), "header {name} delivered");
        }
        for (name, value) in &sent.headers {
            assert_eq!(got.headers.get(name), Some(value), "header {name}");
        }
        assert_eq!(got.key, sent.key, "the key");
        assert_eq!(got.payload, sent.payload, "the payload");
        assert!(got.timestamp_ms > 0, "an append time: {}", got.timestamp_ms);
    }
    reader.close().await;
}

/// Events of one key arrive in publish order, all from one segment, each once; the positions of
/// a segment's deliveries increase by [`EventStore::compare`].
pub async fn same_key_arrives_in_publish_order(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let keys: Vec<String> = (0..4).map(|_| fresh("key")).collect();
    let mut sent = Vec::new();
    for _ in 0..10 {
        for key in &keys {
            sent.push(event(kit.source, Some(key)));
        }
    }
    publish_all(&*events, &sent).await;

    let mut reader = Reader::join(&*events, &kit.spec()).await;
    let all = ids(&sent);
    reader
        .until("every event delivered", |r| r.has_all(&all))
        .await;
    let got = reader.mine(&all);
    assert_eq!(got.len(), sent.len(), "each event once");
    for key in &keys {
        let published: Vec<&str> = sent
            .iter()
            .filter(|e| e.key.as_ref() == Some(key))
            .map(|e| e.id.as_str())
            .collect();
        let delivered: Vec<&Delivery> = got
            .iter()
            .copied()
            .filter(|d| d.key.as_ref() == Some(key))
            .collect();
        assert_eq!(
            delivered.iter().map(|d| id_of(d)).collect::<Vec<_>>(),
            published,
            "key {key}: publish order"
        );
        let segments: BTreeSet<&str> = delivered.iter().map(|d| d.segment.as_str()).collect();
        assert_eq!(
            segments.len(),
            1,
            "key {key}: one segment, got {segments:?}"
        );
    }
    let mut last: HashMap<&str, &str> = HashMap::new();
    for d in &got {
        if let Some(before) = last.insert(&d.segment, &d.position) {
            assert_eq!(
                events.compare(before, &d.position),
                std::cmp::Ordering::Less,
                "segment {}: {before} before {}",
                d.segment,
                d.position
            );
        }
    }
    reader.close().await;
}

/// Two subscription names (two processors) each receive every event.
pub async fn every_processor_gets_every_event(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let sent: Vec<OutgoingEvent> = (0..10)
        .map(|_| event(kit.source, Some(fresh("key").as_str())))
        .collect();
    publish_all(&*events, &sent).await;

    let mut a = Reader::join(&*events, &kit.spec()).await;
    let mut b = Reader::join(&*events, &kit.spec()).await;
    let all = ids(&sent);
    both_until(&mut a, &mut b, "both processors get every event", |a, b| {
        a.has_all(&all) && b.has_all(&all)
    })
    .await;
    a.close().await;
    b.close().await;
}

/// Two instances of one subscription split its segments (adding the second spreads them, every
/// revocation closed by an assignment, possibly empty) and share its events: each event goes to
/// one of them, and both get some.
pub async fn instances_share_a_processor(kit: &EventStoreKit<'_>) {
    let (first, second) = (kit.instance(), kit.instance());
    let spec = kit.spec();
    let mut a = Reader::join(&*first, &spec).await;
    a.until("the first instance holds segments", |r| {
        !r.assigned.is_empty()
    })
    .await;
    let mut b = Reader::join(&*second, &spec).await;
    both_until(
        &mut a,
        &mut b,
        "both instances hold segments, none held by both, every revocation closed by an \
         assignment",
        |a, b| {
            !a.assigned.is_empty()
                && !b.assigned.is_empty()
                && a.assigned.is_disjoint(&b.assigned)
                && !a.changing
                && !b.changing
        },
    )
    .await;

    let sent: Vec<OutgoingEvent> = (0..40)
        .map(|_| event(kit.source, Some(fresh("key").as_str())))
        .collect();
    publish_all(&*first, &sent).await;
    let all = ids(&sent);
    both_until(&mut a, &mut b, "every event delivered", |a, b| {
        all.iter().all(|id| a.has(id) || b.has(id))
    })
    .await;
    let to_a: HashSet<&str> = a.mine(&all).into_iter().map(id_of).collect();
    let to_b: HashSet<&str> = b.mine(&all).into_iter().map(id_of).collect();
    assert!(
        !to_a.is_empty() && !to_b.is_empty(),
        "both instances get events: {} and {}",
        to_a.len(),
        to_b.len()
    );
    let twice: Vec<&&str> = to_a.intersection(&to_b).collect();
    assert!(twice.is_empty(), "delivered to both instances: {twice:?}");
    a.close().await;
    b.close().await;
}

/// Instances join one subscription one at a time, one more than its segments, each reading the
/// whole time: every newcomer's first event is an `Assigned` (an empty one when it holds nothing,
/// as the last one may), within [`FIRST_ASSIGNMENT_WITHIN`] of joining, so an instance that joined
/// and holds nothing is a member, idle, and never reads `STARTING`.
pub async fn a_joining_instance_announces_its_first_assignment(kit: &EventStoreKit<'_>) {
    let spec = kit.spec();
    let first = kit.instance();
    let mut readers = vec![Reader::join(&*first, &spec).await];
    let mut stores = vec![first];
    readers[0]
        .until("the first instance holds segments", |r| {
            !r.assigned.is_empty()
        })
        .await;
    readers[0].pump(QUIET).await;
    let segments = readers[0].assigned.len();
    while readers.len() <= segments {
        let store = kit.instance();
        readers.push(Reader::join(&*store, &spec).await);
        stores.push(store);
        let newcomer = readers.len() - 1;
        let deadline = Instant::now() + FIRST_ASSIGNMENT_WITHIN;
        while readers[newcomer].first.is_none() {
            assert!(
                Instant::now() < deadline,
                "instance {} of {} (assigned {:?}): its first assignment, possibly empty, within \
                 {FIRST_ASSIGNMENT_WITHIN:?} of joining",
                newcomer + 1,
                segments + 1,
                readers.iter().map(|r| &r.assigned).collect::<Vec<_>>()
            );
            futures::future::join_all(readers.iter_mut().map(|r| r.pump(SLICE))).await;
        }
        assert_eq!(
            readers[newcomer].first,
            Some("Assigned"),
            "instance {}: the first event of a new subscription is an Assigned",
            newcomer + 1
        );
    }
    for r in readers {
        r.close().await;
    }
}

/// More instances than segments: instances join one at a time; after each join the segments
/// are spread (none held twice, every one held, and once there are two instances or more no
/// former holder, an instance that held segments before the join, holds every segment) and every
/// instance's last assignment change closed (a
/// `Revoked` followed by an `Assigned`, an empty one for an instance left without segments).
/// How the segments spread beyond that is the event store's (a third party's assignor is not
/// constrained). The instances join until one that held segments holds none (or there are twice
/// as many instances as segments, two more; a run that reaches that cap without such an instance
/// says so on stderr, the check then proved less): that instance saw its revocation closed by
/// `Assigned([])`, so the core reports it running, idle, never starting.
///
/// After each join the check watches for the change the join may cause: until a revocation
/// somewhere, or until the newcomer saw its first change and a slice then passed with no event at
/// any instance, at most 6 s.
pub async fn an_instance_left_without_segments_closes_its_change(kit: &EventStoreKit<'_>) {
    let spec = kit.spec();
    let first = kit.instance();
    let mut readers = vec![Reader::join(&*first, &spec).await];
    readers[0]
        .until("the first instance holds segments", |r| {
            !r.assigned.is_empty()
        })
        .await;
    // Alone, it holds every segment (in time: an assignment may come in parts).
    readers[0].pump(QUIET).await;
    let all: BTreeSet<String> = readers[0].assigned.clone();
    let mut stores = vec![first];
    // Spread: every segment held once, no former holder (an instance that held segments before
    // the join) keeping every segment once there are two, every change closed. A newcomer may
    // take them all: how the segments spread beyond that is the event store's.
    let settled = |readers: &[Reader], former: &[usize]| {
        let held: Vec<&String> = readers.iter().flat_map(|r| &r.assigned).collect();
        let unique: BTreeSet<&String> = held.iter().copied().collect();
        held.len() == unique.len()
            && unique.len() == all.len()
            && (readers.len() < 2
                || former
                    .iter()
                    .all(|&i| readers[i].assigned.len() < all.len()))
            && readers.iter().all(|r| !r.changing)
    };
    let cap = 2 * all.len() + 2;
    let mut emptied = false;
    while !emptied && readers.len() < cap {
        let former: Vec<usize> = (0..readers.len())
            .filter(|&i| !readers[i].assigned.is_empty())
            .collect();
        let store = kit.instance();
        let revoked_before: usize = readers.iter().map(|r| r.revoked).sum();
        readers.push(Reader::join(&*store, &spec).await);
        stores.push(store);
        let newcomer = readers.len() - 1;
        let deadline = Instant::now() + JOIN_WINDOW;
        while readers.iter().map(|r| r.revoked).sum::<usize>() == revoked_before
            && Instant::now() < deadline
        {
            let taken: usize = futures::future::join_all(readers.iter_mut().map(|r| r.pump(SLICE)))
                .await
                .into_iter()
                .sum();
            if taken == 0 && readers[newcomer].changes > 0 {
                break;
            }
        }
        all_until(
            &mut readers,
            "the segments spread, every revocation closed by an assignment",
            |readers| settled(readers, &former),
        )
        .await;
        emptied = readers
            .iter()
            .any(|r| r.assigned.is_empty() && r.revoked > 0);
    }
    if !emptied {
        eprintln!(
            "event store conformance check `an_instance_left_without_segments_closes_its_change` \
             on event store `{}`: {cap} instances and none that held segments was left without \
             any (assigned {:?}): the empty assignment that closes such a change was not seen",
            stores[0].name(),
            readers.iter().map(|r| &r.assigned).collect::<Vec<_>>()
        );
    }
    assert!(
        readers.iter().any(|r| r.assigned.is_empty()),
        "more instances than segments leave one without segments"
    );
    for r in readers {
        r.close().await;
    }
}

/// What a subscription did not acknowledge comes again: to the next subscription of the name after
/// a close, and on a live one after [`Subscription::redeliver`]; what it acknowledged does not.
pub async fn unacked_delivery_is_redelivered(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let spec = kit.spec();
    let key = fresh("key");
    let sent: Vec<OutgoingEvent> = (0..3)
        .map(|_| event(kit.source, Some(key.as_str())))
        .collect();
    let all = ids(&sent);
    let unacked = all[1..].to_vec();
    publish_all(&*events, &sent).await;

    let mut first = Reader::join(&*events, &spec).await;
    first.hold(&unacked);
    first
        .until("the three events delivered", |r| r.has_all(&all))
        .await;
    first.close().await;

    let restarted = kit.instance();
    let mut second = Reader::join(&*restarted, &spec).await;
    second.hold(&unacked);
    second
        .until("the unacknowledged events again", |r| r.has_all(&unacked))
        .await;
    assert_eq!(
        second.ids_in_order(&all),
        unacked,
        "after a close: the unacknowledged ones, in order, not the acknowledged one"
    );

    let again = second.first(&unacked[0]).clone();
    second.redeliver(&again);
    second
        .until("the unacknowledged events once more", |r| {
            r.mine(&unacked).len() == 4
        })
        .await;
    assert_eq!(
        second.ids_in_order(&all),
        [unacked.clone(), unacked.clone()].concat(),
        "after redeliver: from the redelivered one on, in order"
    );
    let last = second.first(&unacked[1]).clone();
    second.ack(&last);
    second.close().await;
}

/// A crash after the handler's transaction committed and before the acknowledgement: the event
/// comes again, its processed-event marker (recorded in that transaction, its id built from the
/// delivered `id` header as the core builds it) makes it a duplicate, and once acknowledged it
/// does not come again. The core's own marker path over an event store is proven by the add-on's
/// and the core's suites; this check pins what the event store owes it.
pub async fn redelivered_after_commit_is_skipped(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let spec = kit.spec();
    let key = fresh("key");
    let handled = event(kit.source, Some(key.as_str()));
    publish_all(&*events, std::slice::from_ref(&handled)).await;
    let only = ids(std::slice::from_ref(&handled));
    let marker = |d: &Delivery| ProcessedMarker {
        id: format!("processor:{}:{}", spec.name, id_of(d)),
        type_name: EVENT_TYPE.into(),
        processed_at_ms: now_ms(),
        processor: Some(spec.name.clone()),
        source: None,
        segment: None,
        position: None,
    };
    let txs = kit.store.tx();
    let processed = kit.store.processed_events();

    let mut first = Reader::join(&*events, &spec).await;
    first.hold(&only);
    first
        .until("the event delivered", |r| r.has_all(&only))
        .await;
    let mut tx = txs.begin().await.expect("begin");
    let first_marker = marker(first.first(&handled.id));
    assert!(
        processed
            .record(&mut tx, &first_marker)
            .await
            .expect("record"),
        "the first handling records its marker"
    );
    txs.commit(&mut tx).await.expect("commit");
    first.close().await; // the crash: committed, never acknowledged

    let restarted = kit.instance();
    let mut second = Reader::join(&*restarted, &spec).await;
    second.hold(&only);
    second
        .until("the event again after the crash", |r| r.has_all(&only))
        .await;
    let again = second.first(&handled.id).clone();
    let mut tx = txs.begin().await.expect("begin");
    let recorded = processed
        .record(&mut tx, &marker(&again))
        .await
        .expect("record");
    txs.abort(&mut tx).await;
    assert!(!recorded, "the redelivered event is a duplicate");
    second.ack(&again);
    second.close().await;

    let fence = event(kit.source, Some(key.as_str()));
    publish_all(&*events, std::slice::from_ref(&fence)).await;
    let after = kit.instance();
    let mut third = Reader::join(&*after, &spec).await;
    third
        .until("the next event of the key", |r| r.has(&fence.id))
        .await;
    assert!(
        !third.has(&handled.id),
        "the skipped event, acknowledged, does not come again"
    );
    third.close().await;
}

/// A paused subscription delivers nothing (paused before its first `next` too) while its
/// assignment goes on; resumed, it delivers from where it stopped, each event once, in order.
pub async fn paused_subscription_receives_nothing_and_resumes_in_place(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let key = fresh("key");
    let before: Vec<OutgoingEvent> = (0..3)
        .map(|_| event(kit.source, Some(key.as_str())))
        .collect();
    let during: Vec<OutgoingEvent> = (0..5)
        .map(|_| event(kit.source, Some(key.as_str())))
        .collect();
    let (before_ids, during_ids) = (ids(&before), ids(&during));
    publish_all(&*events, &before).await;

    let mut reader = Reader::join(&*events, &kit.spec()).await;
    reader.pause();
    reader.pump(QUIET).await;
    assert!(
        reader.seen.is_empty(),
        "paused from the start: nothing delivered, got {}",
        reader.seen.len()
    );
    reader.resume();
    reader
        .until("the events before the pause", |r| r.has_all(&before_ids))
        .await;

    reader.pause();
    let delivered = reader.seen.len();
    publish_all(&*events, &during).await;
    reader.pump(QUIET).await;
    assert_eq!(
        reader.seen.len(),
        delivered,
        "nothing delivered while paused"
    );
    reader.resume();
    reader
        .until("the events published while paused", |r| {
            r.has_all(&during_ids)
        })
        .await;
    let all = [before_ids, during_ids].concat();
    assert_eq!(reader.ids_in_order(&all), all, "each once, in order");
    reader.close().await;
}

// ---- capabilities ------------------------------------------------------------------------------

/// `replay`: while an instance reads the subscription, its positions do not move; with none,
/// a reset to earliest replays what it had acknowledged. The plan of each target moves the
/// segments as its doc says.
pub async fn replay_from_earliest_redelivers_history(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let spec = kit.spec();
    let sent: Vec<OutgoingEvent> = (0..3)
        .map(|_| event(kit.source, Some(fresh("key").as_str())))
        .collect();
    let all = ids(&sent);
    publish_all(&*events, &sent).await;

    let mut first = Reader::join(&*events, &spec).await;
    first
        .until("the events delivered", |r| r.has_all(&all))
        .await;
    let delivered: Vec<Delivery> = first.mine(&all).into_iter().cloned().collect();
    let admin = Admin::new(events.admin(&spec).expect("admin"));
    let deadline = Instant::now() + WAIT;
    while admin
        .call("members", |a| a.members())
        .await
        .expect("members")
        .is_empty()
    {
        assert!(
            Instant::now() < deadline,
            "the reading instance is a member"
        );
        first.pump(SLICE).await;
    }
    let earliest = admin
        .plan(ResetTarget::Earliest)
        .await
        .expect("plan earliest");
    let refused = admin.apply(&earliest).await;
    assert!(
        matches!(refused, Err(EventStoreError::SubscribersActive(_))),
        "no move while an instance reads: {refused:?}"
    );
    first.close().await;
    let deadline = Instant::now() + WAIT;
    loop {
        let members = admin
            .call("members", |a| a.members())
            .await
            .expect("members");
        if members.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "members after close: {members:?}"
        );
        tokio::time::sleep(SLICE).await;
    }

    let before = |a: &str, b: &str| events.compare(a, b) == std::cmp::Ordering::Less;
    let earliest = admin
        .plan(ResetTarget::Earliest)
        .await
        .expect("plan earliest");
    for d in &delivered {
        assert!(
            earliest.iter().any(|r| r.segment == d.segment
                && r.source == kit.source
                && !r.until.is_empty()
                && !before(&d.position, &r.from)
                && before(&d.position, &r.until)),
            "earliest replays {} of segment {}: {earliest:?}",
            d.position,
            d.segment
        );
    }
    let latest = admin.plan(ResetTarget::Latest).await.expect("plan latest");
    for d in &delivered {
        assert!(
            latest.iter().any(|r| r.segment == d.segment),
            "latest moves segment {}: {latest:?}",
            d.segment
        );
    }
    assert!(
        latest
            .iter()
            .all(|r| r.until.is_empty() || !before(&r.from, &r.until)),
        "latest replays nothing: {latest:?}"
    );
    let oldest = &delivered[0];
    let at = admin
        .plan(ResetTarget::Timestamp(oldest.timestamp_ms))
        .await
        .expect("plan timestamp");
    assert!(
        at.iter()
            .any(|r| r.segment == oldest.segment && !before(&oldest.position, &r.from)),
        "a time moves to the first position at or after it: {at:?}"
    );
    let newest = delivered
        .iter()
        .map(|d| d.timestamp_ms)
        .max()
        .expect("delivered");
    let past = admin
        .plan(ResetTarget::Timestamp(newest + 1))
        .await
        .expect("plan timestamp");
    for d in &delivered {
        assert!(
            past.iter()
                .any(|r| r.segment == d.segment && before(&d.position, &r.from)),
            "a time after every event of segment {} moves past {}: {past:?}",
            d.segment,
            d.position
        );
    }
    let target = ResetTarget::Positions(vec![(oldest.segment.clone(), oldest.position.clone())]);
    let positions = admin.plan(target).await.expect("plan positions");
    assert_eq!(
        positions
            .iter()
            .map(|r| (r.segment.as_str(), r.from.as_str()))
            .collect::<Vec<_>>(),
        [(oldest.segment.as_str(), oldest.position.as_str())],
        "positions move the listed segments only"
    );
    // A segment the source lacks, and a position the segment does not have (far past any head of
    // a store with decimal positions, not a position at all for the others).
    for target in [
        ResetTarget::Positions(vec![(
            "conformance-no-such-segment".into(),
            oldest.position.clone(),
        )]),
        ResetTarget::Positions(vec![(oldest.segment.clone(), "9223372036854775807".into())]),
    ] {
        let refused = admin.plan(target.clone()).await;
        assert!(
            matches!(refused, Err(EventStoreError::Invalid(_))),
            "{target:?} is invalid: {refused:?}"
        );
    }

    admin.apply(&earliest).await.expect("apply earliest");
    let restarted = kit.instance();
    let mut second = Reader::join(&*restarted, &spec).await;
    second
        .until("the acknowledged events replayed", |r| r.has_all(&all))
        .await;
    second.close().await;
}

/// Without `replay`: the reset side of a subscription is refused as unsupported.
pub async fn admin_without_replay_is_unsupported(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let admin = events.admin(&kit.spec());
    assert!(
        matches!(admin, Err(EventStoreError::Unsupported(Capability::Replay))),
        "admin without replay: {:?}",
        admin.err()
    );
}

/// Without `lag`: no segment reports a lag.
pub async fn lag_unknown_without_capability(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let sent = event(kit.source, Some(fresh("key").as_str()));
    publish_all(&*events, std::slice::from_ref(&sent)).await;
    let mut reader = Reader::join(&*events, &kit.spec()).await;
    reader
        .until("the event delivered", |r| r.has(&sent.id))
        .await;
    let segments = reader.subscription().segments();
    assert!(!segments.is_empty(), "segments() lists the assignment");
    assert!(
        segments.iter().all(|s| s.lag.is_none()),
        "no lag without the capability: {segments:?}"
    );
    reader.close().await;
}

/// `lag`: each segment's lag counts what the subscription has not acknowledged. Passes on its
/// own on an empty source: it first gives every assigned segment an acknowledged event.
pub async fn lag_counts_unacked(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let mut reader = Reader::join(&*events, &kit.spec()).await;
    reader
        .until("segments assigned", |r| !r.assigned.is_empty())
        .await;
    // An acknowledged event on every assigned segment, so each lag counts from a committed
    // position even on a source that was empty (a segment without one may report no lag).
    let deadline = Instant::now() + WAIT;
    let mut primed: BTreeSet<String> = BTreeSet::new();
    while !reader.assigned.is_subset(&primed) {
        assert!(
            Instant::now() < deadline,
            "an acknowledged event on every segment within {WAIT:?} (on {primed:?} of {:?})",
            reader.assigned
        );
        let prime = event(kit.source, Some(fresh("key").as_str()));
        publish_all(&*events, std::slice::from_ref(&prime)).await;
        reader
            .until("the priming event delivered", |r| r.has(&prime.id))
            .await;
        primed.insert(reader.first(&prime.id).segment.clone());
    }
    let every = |r: &Reader| r.assigned.iter().cloned().collect::<Vec<_>>();
    let deadline = Instant::now() + WAIT;
    loop {
        reader
            .until("everything acknowledged, lag 0", |r| {
                r.known_lag(&every(r)) == Some(0)
            })
            .await;
        let delivered = reader.seen.len();
        reader.pump(QUIET).await;
        if reader.seen.len() == delivered && reader.known_lag(&every(&reader)) == Some(0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the source drained within {WAIT:?}"
        );
    }
    let listed: Vec<String> = reader
        .subscription()
        .segments()
        .into_iter()
        .map(|s| s.segment)
        .collect();
    let mut ordered = listed.clone();
    ordered.sort_by(|a, b| super::segment_order(a, b));
    assert_eq!(listed, ordered, "segments() lists them in segment order");
    let segments: BTreeSet<String> = listed.into_iter().collect();
    assert_eq!(segments, reader.assigned, "segments() lists the assignment");

    let sent: Vec<OutgoingEvent> = (0..5)
        .map(|_| event(kit.source, Some(fresh("key").as_str())))
        .collect();
    let all = ids(&sent);
    reader.hold(&all);
    publish_all(&*events, &sent).await;
    reader
        .until("the events delivered", |r| r.has_all(&all))
        .await;
    let held: Vec<String> = reader
        .mine(&all)
        .iter()
        .map(|d| d.segment.clone())
        .collect();
    reader
        .until("the lag counts the 5 unacknowledged events", |r| {
            r.known_lag(&held) == Some(5)
        })
        .await;
    for d in reader.mine(&all).into_iter().cloned().collect::<Vec<_>>() {
        reader.ack(&d);
    }
    reader
        .until("the lag back to 0 once acknowledged", |r| {
            r.known_lag(&held) == Some(0)
        })
        .await;
    reader.close().await;
}

/// `native-dead-letter`: the dead-letter source takes a letter (its `snodra-dlt-*` headers
/// included) and delivers it as published.
pub async fn dead_letter_source_receives(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let dlt = dead_letter_source(kit.source);
    let mut letter = event(&dlt, Some(fresh("key").as_str()));
    for (name, value) in [
        ("snodra-dlt-processor", "conformance"),
        ("snodra-dlt-reason", "FAILED"),
        ("snodra-dlt-error", "the handler failed"),
    ] {
        letter.headers.insert(name.into(), value.into());
    }
    publish_all(&*events, std::slice::from_ref(&letter)).await;
    let spec = SubscriptionSpec {
        source: dlt.clone(),
        ..kit.spec()
    };
    let mut reader = Reader::join(&*events, &spec).await;
    let only = ids(std::slice::from_ref(&letter));
    reader
        .until("the letter delivered from the dead-letter source", |r| {
            r.has_all(&only)
        })
        .await;
    let got = reader.first(&letter.id);
    for (name, value) in &letter.headers {
        assert_eq!(got.headers.get(name), Some(value), "header {name}");
    }
    assert_eq!(got.key, letter.key, "the key");
    assert_eq!(got.payload, letter.payload, "the payload");
    reader.close().await;
}

// ---- the browser -------------------------------------------------------------------------------

/// `browse`: a search finds events by type (exact and prefix), key, correlation id and event id,
/// newest first; a summary carries the event's place, key and library headers but no payload; a
/// record (`get`) carries every header and the payload only when asked; the sources list the
/// source's segments.
pub async fn search_finds_by_type_key_and_correlation(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let browser = browser_of(&*events);
    let tag = fresh("browse");
    let ty = |name: &str| format!("conformance.{tag}.{name}");
    let (k1, k2) = (fresh("key"), fresh("key"));
    let (c1, c2) = (fresh("correlation"), fresh("correlation"));
    let a = typed(kit.source, &k1, &ty("Placed"), &c1);
    let b = typed(kit.source, &k2, &ty("Placed"), &c2);
    let c = typed(kit.source, &k1, &ty("Shipped"), &c1);
    publish_spaced(&*events, &[a.clone(), b.clone(), c.clone()]).await;

    let filter = |f: fn(&mut EventFilter, String), value: String| {
        let mut filter = EventFilter::default();
        f(&mut filter, value);
        filter
    };
    let all = found(
        &*browser,
        kit,
        filter(|f, v| f.type_name = Some(v), format!("conformance.{tag}.*")),
        &[&c, &b, &a],
        "a type prefix",
    )
    .await;
    for (filters, want, what) in [
        (
            filter(|f, v| f.type_name = Some(v), ty("Placed")),
            vec![&b, &a],
            "an exact type",
        ),
        (
            filter(|f, v| f.key = Some(v), k1.clone()),
            vec![&c, &a],
            "a key",
        ),
        (
            filter(|f, v| f.correlation_id = Some(v), c1.clone()),
            vec![&c, &a],
            "a correlation id",
        ),
        (
            filter(|f, v| f.event_id = Some(v), a.id.clone()),
            vec![&a],
            "an event id",
        ),
        (
            EventFilter {
                type_name: Some(ty("Placed")),
                key: Some(k1.clone()),
                ..EventFilter::default()
            },
            vec![&a],
            "a type and a key",
        ),
    ] {
        found(&*browser, kit, filters, &want, what).await;
    }

    for (summary, sent) in all.iter().zip([&c, &b, &a]) {
        assert_eq!(summary.source, kit.source, "a summary names its source");
        assert!(
            !summary.segment.is_empty() && summary.segment.len() <= MAX_SEGMENT_BYTES,
            "a segment of 1 to {MAX_SEGMENT_BYTES} bytes: {:?}",
            summary.segment
        );
        assert!(
            !summary.position.is_empty() && summary.position.len() <= MAX_POSITION_BYTES,
            "a position of 1 to {MAX_POSITION_BYTES} bytes: {:?}",
            summary.position
        );
        assert_eq!(summary.key, sent.key, "the key");
        for name in headers::ALL {
            assert_eq!(
                summary.headers.get(name),
                sent.headers.get(name),
                "summary header {name}"
            );
        }
        assert_eq!(
            summary.size,
            sent.payload.len() as u64,
            "the payload's size"
        );
        assert!(summary.timestamp.is_some_and(|t| t > 0), "a time");

        let get = |payload: bool| GetRequest {
            source: Some(kit.source.to_string()),
            segment: summary.segment.clone(),
            position: summary.position.clone(),
            payload,
        };
        let record = bounded("get", browser.get(get(true)))
            .await
            .unwrap_or_else(|e| panic!("get {}: {e}", sent.id));
        assert_eq!(
            &record.summary, summary,
            "a record's summary is the search's"
        );
        assert_eq!(
            record.payload,
            Some(Some(String::from_utf8(sent.payload.clone()).expect("JSON"))),
            "the payload, asked for"
        );
        for (name, value) in &sent.headers {
            assert!(
                record
                    .all_headers
                    .iter()
                    .any(|h| &h.name == name && h.value.as_ref() == Some(value)),
                "record header {name}: {:?}",
                record.all_headers
            );
        }
        let unasked = bounded("get", browser.get(get(false)))
            .await
            .unwrap_or_else(|e| panic!("get {}: {e}", sent.id));
        assert!(
            unasked.payload.is_none() && unasked.payload_base64.is_none(),
            "no payload unless asked: {unasked:?}"
        );
    }

    let first = &all[2];
    for (segment, position) in [
        ("conformance-no-such-segment", first.position.as_str()),
        (first.segment.as_str(), "9223372036854775807"),
    ] {
        let missing = bounded(
            "get",
            browser.get(GetRequest {
                source: Some(kit.source.to_string()),
                segment: segment.to_string(),
                position: position.to_string(),
                payload: false,
            }),
        )
        .await;
        assert!(
            matches!(
                missing,
                Err(BrowseError::NotFound(_) | BrowseError::Invalid(_))
            ),
            "no event at {position} of segment {segment}: {missing:?}"
        );
    }

    let long = "9".repeat(MAX_SEGMENT_BYTES + 1);
    for (segment, position) in [
        (long.as_str(), first.position.as_str()),
        (first.segment.as_str(), long.as_str()),
    ] {
        let refused = bounded(
            "get",
            browser.get(GetRequest {
                source: Some(kit.source.to_string()),
                segment: segment.to_string(),
                position: position.to_string(),
                payload: false,
            }),
        )
        .await;
        assert!(
            matches!(&refused, Err(BrowseError::Invalid(m)) if !m.contains(&long)),
            "a segment or position over 128 bytes is invalid, never echoed: {refused:?}"
        );
    }

    let sources = bounded("sources", browser.sources(SourcesRequest {}))
        .await
        .unwrap_or_else(|e| panic!("sources: {e}"));
    let mine = sources
        .sources
        .iter()
        .find(|s| s.source == kit.source)
        .unwrap_or_else(|| panic!("the sources list {}: {sources:?}", kit.source));
    assert!(!mine.missing, "the source exists");
    assert!(
        mine.segments.len() >= 2,
        "its segments: {:?}",
        mine.segments
    );
    let listed: Vec<&str> = mine.segments.iter().map(|s| s.segment.as_str()).collect();
    let mut ordered = listed.clone();
    ordered.sort_by(|a, b| segment_order(a, b));
    assert_eq!(listed, ordered, "segments in segment order");
    let bounds = mine
        .segments
        .iter()
        .find(|s| s.segment == first.segment)
        .unwrap_or_else(|| panic!("segment {} listed: {:?}", first.segment, mine.segments));
    assert!(
        events.compare(&bounds.first, &first.position) != std::cmp::Ordering::Greater
            && events.compare(&first.position, &bounds.head) == std::cmp::Ordering::Less,
        "segment {} holds {} ({bounds:?})",
        first.segment,
        first.position
    );

    let elsewhere = bounded(
        "search",
        browser.search(SearchRequest {
            source: Some(fresh("conformance-not-browsable")),
            ..SearchRequest::default()
        }),
    )
    .await;
    assert!(
        matches!(elsewhere, Err(BrowseError::Invalid(_))),
        "a source the browser does not read: {elsewhere:?}"
    );
}

/// `browse`: a search page stops at its `limit` and continues from its cursor (the pages
/// together are every match, newest first, none twice), and stops at its scan budget having
/// read no more (but at least one record per segment that is not done); pages under that budget,
/// from the check's start time, still find every match; a cursor only continues its own search,
/// and bounds it cannot honour are refused. Holds whatever the source held before.
pub async fn search_stops_at_bounds(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let browser = browser_of(&*events);
    let ty = format!("conformance.{}.Paged", fresh("browse"));
    let sent: Vec<OutgoingEvent> = (0..5)
        .map(|_| typed(kit.source, &fresh("key"), &ty, &fresh("correlation")))
        .collect();
    publish_spaced(&*events, &sent).await;
    let newest_first: Vec<&OutgoingEvent> = sent.iter().rev().collect();
    let by_type = EventFilter {
        type_name: Some(ty.clone()),
        ..EventFilter::default()
    };
    let five = found(&*browser, kit, by_type.clone(), &newest_first, "the five").await;
    // The store's own stamp of the oldest of the five (on its clock, not the kit's): no event of
    // this check is older.
    let since = five
        .last()
        .and_then(|s| s.timestamp)
        .expect("the five carry their times");

    let request = |filters: EventFilter, cursor: Option<String>| SearchRequest {
        source: Some(kit.source.to_string()),
        filters,
        limit: Some(2),
        cursor,
        ..SearchRequest::default()
    };
    let mut pages: Vec<Vec<String>> = Vec::new();
    let mut cursor = None;
    let first_cursor = loop {
        let page = search(&*browser, request(by_type.clone(), cursor.clone())).await;
        pages.push(page.results.iter().map(summary_id).collect());
        assert!(pages.len() <= 3, "three pages of two at most: {pages:?}");
        if page.exhausted {
            assert_eq!(
                (page.stopped_by, page.cursor.as_deref()),
                (None, None),
                "an exhausted page has no cursor"
            );
            break pages.len();
        }
        assert_eq!(page.stopped_by, Some(StoppedBy::Limit), "{page:?}");
        cursor = Some(
            page.cursor
                .expect("a page that is not exhausted has a cursor"),
        );
    };
    let ids: Vec<String> = newest_first.iter().map(|e| e.id.clone()).collect();
    assert_eq!(first_cursor, 3, "three pages: {pages:?}");
    assert_eq!(
        pages,
        [&ids[0..2], &ids[2..4], &ids[4..5]],
        "pages of two, in order"
    );

    let page = search(&*browser, request(by_type.clone(), None)).await;
    let other = EventFilter {
        type_name: Some(format!("{ty}.Other")),
        ..EventFilter::default()
    };
    for (filters, cursor) in [
        (other, page.cursor.clone()),
        (by_type.clone(), Some("%%%".to_string())),
    ] {
        let refused = bounded("search", browser.search(request(filters, cursor))).await;
        assert!(
            matches!(refused, Err(BrowseError::Invalid(_))),
            "a cursor of another search, or none: {refused:?}"
        );
    }

    // A page reads at least one record per segment (else the segments past the budget would
    // never be read): the scan budget here is the number of segments, and the source holds more.
    let segments = bounded("sources", browser.sources(SourcesRequest {}))
        .await
        .unwrap_or_else(|e| panic!("sources: {e}"))
        .sources
        .into_iter()
        .find(|s| s.source == kit.source)
        .map_or(0, |s| s.segments.len() as u64);
    assert!(
        segments >= 2,
        "the source's segments are listed: {segments}"
    );
    let fillers: Vec<OutgoingEvent> = (0..segments)
        .map(|_| typed(kit.source, &fresh("key"), &format!("{ty}.Filler"), "f"))
        .collect();
    publish_all(&*events, &fillers).await;
    let nothing = EventFilter {
        type_name: Some(format!("conformance.{}.Nothing", fresh("browse"))),
        ..EventFilter::default()
    };
    let budget = |filters: EventFilter, cursor: Option<String>| SearchRequest {
        source: Some(kit.source.to_string()),
        filters,
        cursor,
        max_scanned: Some(segments),
        ..SearchRequest::default()
    };
    let first = search(&*browser, budget(nothing.clone(), None)).await;
    assert!(
        first.results.is_empty() && first.scanned <= segments,
        "no match, at most {segments} records read: {first:?}"
    );
    assert_eq!(first.stopped_by, Some(StoppedBy::ScanBudget), "{first:?}");
    assert!(!first.exhausted && first.cursor.is_some(), "{first:?}");

    // Progress: pages of that budget from the check's start (whatever the source held before),
    // each continuing the last, find the five, newest first, none twice.
    let recent = EventFilter {
        from: Some(TimeArg::Millis(since)),
        ..by_type.clone()
    };
    let mut pages = 0;
    let mut cursor = None;
    let mut got: Vec<String> = Vec::new();
    let deadline = Instant::now() + WAIT;
    while got.len() < ids.len() {
        pages += 1;
        assert!(
            Instant::now() < deadline,
            "the five within {WAIT:?} of pages of {segments} records ({pages} pages), got {got:?}"
        );
        let page = search(&*browser, budget(recent.clone(), cursor.clone())).await;
        assert!(
            page.scanned <= segments,
            "at most {segments} records read: {page:?}"
        );
        got.extend(page.results.iter().map(summary_id));
        if page.exhausted {
            break;
        }
        cursor = Some(
            page.cursor
                .expect("a page that is not exhausted has a cursor"),
        );
    }
    assert_eq!(
        got, ids,
        "pages under a scan budget: newest first, none twice"
    );

    for (what, bad) in [
        (
            "limit 0",
            SearchRequest {
                limit: Some(0),
                ..request(by_type.clone(), None)
            },
        ),
        (
            "a limit above every bound",
            SearchRequest {
                limit: Some(usize::MAX),
                ..request(by_type.clone(), None)
            },
        ),
        (
            "maxScanned 0",
            SearchRequest {
                max_scanned: Some(0),
                ..request(by_type.clone(), None)
            },
        ),
        (
            "from after to",
            request(
                EventFilter {
                    from: Some(TimeArg::Millis(2)),
                    to: Some(TimeArg::Millis(1)),
                    ..EventFilter::default()
                },
                None,
            ),
        ),
    ] {
        let refused = bounded("search", browser.search(bad)).await;
        assert!(
            matches!(refused, Err(BrowseError::Invalid(_))),
            "{what} is refused: {refused:?}"
        );
    }
}

/// `browse`: a tail starts at the end of every segment (an event appended before it does not
/// come), then delivers the matching events appended after its `start` (each key in order,
/// summaries only), heartbeats while nothing comes, and ends at its maximum duration with `end`.
pub async fn tail_streams_new_events(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let browser = browser_of(&*events);
    let ty = format!("conformance.{}.Tailed", fresh("browse"));
    let (k1, k2) = (fresh("key"), fresh("key"));
    let before = typed(kit.source, &k1, &ty, &fresh("correlation"));
    publish_all(&*events, std::slice::from_ref(&before)).await;
    let request = TailRequest {
        source: Some(kit.source.to_string()),
        filters: EventFilter {
            type_name: Some(ty.clone()),
            ..EventFilter::default()
        },
        heartbeat_ms: Some(200),
        max_duration_ms: None,
    };
    let mut tail = open_tail(&*browser, request.clone()).await;
    match bounded("tail: the start", tail.next()).await {
        Some(TailEvent::Start(start)) => {
            assert_eq!(start.source, kit.source, "{start:?}");
            assert!(start.segments >= 2, "{start:?}");
        }
        other => panic!("the first part is the start: {other:?}"),
    }
    let mut parts = Parts {
        started: true,
        ..Parts::default()
    };
    let after = [
        typed(kit.source, &k1, &ty, &fresh("correlation")),
        typed(kit.source, &k2, &ty, &fresh("correlation")),
        typed(kit.source, &k1, &ty, &fresh("correlation")),
    ];
    let unmatched = typed(
        kit.source,
        &k2,
        &format!("{ty}.Other"),
        &fresh("correlation"),
    );
    publish_all(&*events, &[after[0].clone(), unmatched.clone()]).await;
    publish_all(&*events, &after[1..]).await;
    let want: Vec<String> = ids(&after);
    parts
        .until(&mut tail, "the three events", |p| {
            want.iter().all(|id| p.has(id))
        })
        .await;
    let seen: Vec<String> = parts.records.iter().map(summary_id).collect();
    assert!(!seen.contains(&before.id), "not the event before the start");
    assert!(
        !seen.contains(&unmatched.id),
        "not an event of another type"
    );
    let of_k1: Vec<&String> = seen
        .iter()
        .filter(|id| **id == after[0].id || **id == after[2].id)
        .collect();
    assert_eq!(of_k1, [&after[0].id, &after[2].id], "one key in order");
    for r in &parts.records {
        assert_eq!(r.source, kit.source, "{r:?}");
    }
    parts
        .until(&mut tail, "a heartbeat", |p| p.heartbeats > 0)
        .await;
    drop(tail);

    let mut short = open_tail(
        &*browser,
        TailRequest {
            max_duration_ms: Some(1_000),
            ..request.clone()
        },
    )
    .await;
    let mut parts = Parts::default();
    let started = Instant::now();
    parts
        .until(&mut short, "the end of a tail of 1 s", |p| p.end.is_some())
        .await;
    assert_eq!(parts.end, Some(EndReason::MaxDuration));
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "it lasted its duration: {:?}",
        started.elapsed()
    );
    assert!(
        bounded("the stream after the end", short.next())
            .await
            .is_none(),
        "nothing after the end"
    );

    let refused = bounded(
        "tail",
        browser.tail(TailRequest {
            filters: EventFilter {
                from: Some(TimeArg::Millis(1)),
                ..EventFilter::default()
            },
            ..request
        }),
    )
    .await;
    assert!(
        matches!(refused, Err(BrowseError::Invalid(_))),
        "a tail takes no from: {:?}",
        refused.err()
    );
}

/// `browse`: browsing never moves a position and never joins a subscription. After a search, a
/// record read, the sources, the lag and a tail, with no instance reading a subscription and then
/// with one: its committed positions are the same, the reading instance keeps its segments (and
/// is the only member), what it had not acknowledged comes again, and the lag lists no
/// subscription that was not there before.
pub async fn browser_never_commits_a_position(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let browser = browser_of(&*events);
    let spec = kit.spec();
    let ty = format!("conformance.{}.Held", fresh("browse"));
    let key = fresh("key");
    let sent: Vec<OutgoingEvent> = (0..4)
        .map(|_| typed(kit.source, &key, &ty, &fresh("correlation")))
        .collect();
    let all = ids(&sent);
    let unacked = all[2..].to_vec();
    publish_all(&*events, &sent).await;
    let mut first = Reader::join(&*events, &spec).await;
    first.hold(&unacked);
    first
        .until("the four events delivered", |r| r.has_all(&all))
        .await;
    first.close().await;

    let deadline = Instant::now() + WAIT;
    let before = loop {
        let view = lag(&*browser, kit).await;
        if committed(&view, &spec.name).is_some_and(|c| c.iter().any(|s| s.committed.is_some())) {
            break view;
        }
        assert!(
            Instant::now() < deadline,
            "the lag lists {} with its committed positions within {WAIT:?}: {view:?}",
            spec.name
        );
        tokio::time::sleep(SLICE).await;
    };
    let names = |view: &LagView| -> BTreeSet<String> {
        view.subscriptions
            .iter()
            .map(|s| s.subscription.clone())
            .collect()
    };

    browse_everything(&*events, &*browser, kit, &ty).await;

    // It acknowledges nothing (the tails publish after the held events): its close commits
    // nothing.
    let mut second = Reader::join(&*events, &spec).await;
    second.hold_everything();
    second
        .until("the unacknowledged events again", |r| r.has_all(&unacked))
        .await;
    assert_eq!(
        second.ids_in_order(&all),
        unacked,
        "from where the subscription was: the unacknowledged events only"
    );
    let (assigned, revoked) = (second.assigned.clone(), second.revoked);
    browse_everything(&*events, &*browser, kit, &ty).await;
    second.pump(QUIET).await;
    assert_eq!(
        (&second.assigned, second.revoked),
        (&assigned, revoked),
        "the reading instance keeps its segments while the browser reads"
    );
    if events.supports(Capability::Replay) {
        let admin = Admin::new(events.admin(&spec).expect("admin"));
        let members = admin
            .call("members", |a| a.members())
            .await
            .expect("members");
        assert_eq!(members.len(), 1, "only the reading instance: {members:?}");
    }
    second.close().await;

    let after = lag(&*browser, kit).await;
    assert_eq!(
        committed(&after, &spec.name),
        committed(&before, &spec.name),
        "the committed positions did not move"
    );
    let new: Vec<String> = names(&after).difference(&names(&before)).cloned().collect();
    assert!(
        new.is_empty(),
        "no subscription of the browser's own: {new:?}"
    );
}

/// Without `browse`: no browser.
pub async fn no_browser_without_capability(kit: &EventStoreKit<'_>) {
    assert!(
        kit.instance().browser().is_none(),
        "a browser without the capability"
    );
}

/// Every call of the browser once: the sources, a search and a record read of `ty`, the lag, and
/// a tail that delivers one new event of `ty`.
async fn browse_everything(
    events: &dyn EventStore,
    browser: &dyn EventBrowser,
    kit: &EventStoreKit<'_>,
    ty: &str,
) {
    bounded("sources", browser.sources(SourcesRequest {}))
        .await
        .unwrap_or_else(|e| panic!("sources: {e}"));
    let page = search(
        browser,
        SearchRequest {
            source: Some(kit.source.to_string()),
            filters: EventFilter {
                type_name: Some(ty.to_string()),
                ..EventFilter::default()
            },
            ..SearchRequest::default()
        },
    )
    .await;
    for r in &page.results {
        bounded(
            "get",
            browser.get(GetRequest {
                source: Some(kit.source.to_string()),
                segment: r.segment.clone(),
                position: r.position.clone(),
                payload: true,
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("get: {e}"));
    }
    lag(browser, kit).await;
    let tailed = format!("{ty}.Tailed");
    let mut tail = open_tail(
        browser,
        TailRequest {
            source: Some(kit.source.to_string()),
            filters: EventFilter {
                type_name: Some(tailed.clone()),
                ..EventFilter::default()
            },
            heartbeat_ms: Some(200),
            max_duration_ms: Some(WAIT.as_millis() as u64 * 2),
        },
    )
    .await;
    let mut parts = Parts::default();
    parts.until(&mut tail, "the start", |p| p.started).await;
    let new = typed(kit.source, &fresh("key"), &tailed, &fresh("correlation"));
    publish_all(events, std::slice::from_ref(&new)).await;
    parts
        .until(&mut tail, "the tailed event", |p| p.has(&new.id))
        .await;
}

// ---- helpers -----------------------------------------------------------------------------------

/// One subscription and what it gave: its assignment and its deliveries, in order. Every
/// delivery is acknowledged as it comes but the held ones.
struct Reader {
    name: String,
    subscription: Option<Box<dyn Subscription>>,
    assigned: BTreeSet<String>,
    seen: Vec<Delivery>,
    held: HashSet<String>,
    /// No delivery is acknowledged as it comes.
    hold_everything: bool,
    /// `Revoked` changes seen.
    revoked: usize,
    /// Assignment changes seen (`Assigned` and `Revoked`).
    changes: usize,
    /// The kind of the first event the subscription gave.
    first: Option<&'static str>,
    /// A `Revoked` came and no `Assigned` after it yet (the contract closes every revocation
    /// with an assignment, possibly empty).
    changing: bool,
}

impl Reader {
    async fn join(events: &dyn EventStore, spec: &SubscriptionSpec) -> Reader {
        let subscription = bounded(
            &format!("subscribe {}", spec.name),
            events.subscribe(spec.clone()),
        )
        .await
        .unwrap_or_else(|e| panic!("subscribe {}: {e}", spec.name));
        Reader {
            name: spec.name.clone(),
            subscription: Some(subscription),
            assigned: BTreeSet::new(),
            seen: Vec::new(),
            held: HashSet::new(),
            hold_everything: false,
            revoked: 0,
            changes: 0,
            first: None,
            changing: false,
        }
    }

    fn subscription(&mut self) -> &mut dyn Subscription {
        self.subscription.as_deref_mut().expect("not closed")
    }

    /// The events with these ids are not acknowledged as they come.
    fn hold(&mut self, ids: &[String]) {
        self.held.extend(ids.iter().cloned());
    }

    /// No delivery is acknowledged as it comes.
    fn hold_everything(&mut self) {
        self.hold_everything = true;
    }

    fn pause(&mut self) {
        self.subscription().pause();
    }

    fn resume(&mut self) {
        self.subscription().resume();
    }

    fn ack(&mut self, d: &Delivery) {
        let name = self.name.clone();
        self.subscription()
            .ack(&d.segment, &d.position)
            .unwrap_or_else(|e| panic!("{name}: ack {} {}: {e}", d.segment, d.position));
    }

    fn redeliver(&mut self, d: &Delivery) {
        let name = self.name.clone();
        self.subscription()
            .redeliver(&d.segment, &d.position)
            .unwrap_or_else(|e| panic!("{name}: redeliver {} {}: {e}", d.segment, d.position));
    }

    /// Takes what the subscription gives for `slice`; how many events it gave.
    async fn pump(&mut self, slice: Duration) -> usize {
        let deadline = Instant::now() + slice;
        let mut taken = 0;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return taken;
            }
            match tokio::time::timeout(left, self.subscription().next()).await {
                Err(_) => return taken,
                Ok(Err(e)) => panic!("{}: next: {e}", self.name),
                Ok(Ok(event)) => {
                    taken += 1;
                    self.take(event);
                }
            }
        }
    }

    fn take(&mut self, event: SubscriptionEvent) {
        self.first.get_or_insert(match &event {
            SubscriptionEvent::Assigned(_) => "Assigned",
            SubscriptionEvent::Revoked(_) => "Revoked",
            SubscriptionEvent::Delivery(_) => "Delivery",
        });
        match event {
            SubscriptionEvent::Assigned(segments) => {
                self.changes += 1;
                self.changing = false;
                for segment in segments {
                    assert!(
                        !segment.is_empty() && segment.len() <= MAX_SEGMENT_BYTES,
                        "{}: a segment name of 1 to {MAX_SEGMENT_BYTES} bytes: {segment:?}",
                        self.name
                    );
                    self.assigned.insert(segment);
                }
            }
            SubscriptionEvent::Revoked(segments) => {
                self.changes += 1;
                self.revoked += 1;
                self.changing = true;
                for segment in segments {
                    self.assigned.remove(&segment);
                }
            }
            SubscriptionEvent::Delivery(d) => {
                assert!(
                    self.assigned.contains(&d.segment),
                    "{}: a delivery of segment {} it does not hold ({:?})",
                    self.name,
                    d.segment,
                    self.assigned
                );
                assert!(
                    !d.position.is_empty() && d.position.len() <= MAX_POSITION_BYTES,
                    "{}: a position of 1 to {MAX_POSITION_BYTES} bytes: {:?}",
                    self.name,
                    d.position
                );
                if !self.hold_everything && !self.held.contains(id_of(&d)) {
                    self.ack(&d);
                }
                self.seen.push(d);
            }
        }
    }

    /// Reads until `done`, at most [`WAIT`].
    async fn until(&mut self, what: &str, done: impl Fn(&Reader) -> bool) {
        let deadline = Instant::now() + WAIT;
        while !done(self) {
            assert!(
                Instant::now() < deadline,
                "{}: {what} within {WAIT:?} (assigned {:?}, {} deliveries)",
                self.name,
                self.assigned,
                self.seen.len()
            );
            self.pump(SLICE).await;
        }
    }

    fn has(&self, id: &str) -> bool {
        self.seen.iter().any(|d| id_of(d) == id)
    }

    fn has_all(&self, ids: &[String]) -> bool {
        ids.iter().all(|id| self.has(id))
    }

    /// The deliveries of the events with these ids, in order.
    fn mine(&self, ids: &[String]) -> Vec<&Delivery> {
        self.seen
            .iter()
            .filter(|d| ids.iter().any(|id| id == id_of(d)))
            .collect()
    }

    fn ids_in_order(&self, ids: &[String]) -> Vec<String> {
        self.mine(ids)
            .into_iter()
            .map(|d| id_of(d).to_string())
            .collect()
    }

    /// The first delivery of the event `id`.
    fn first(&self, id: &str) -> &Delivery {
        self.seen
            .iter()
            .find(|d| id_of(d) == id)
            .unwrap_or_else(|| panic!("{}: {id} delivered", self.name))
    }

    /// The lag of the assigned segments whose lag is known, summed; `None` when none is known or
    /// one of `required` is not.
    fn known_lag(&self, required: &[String]) -> Option<u64> {
        let segments = self.subscription.as_deref().expect("not closed").segments();
        if required.iter().any(|segment| {
            !segments
                .iter()
                .any(|s| &s.segment == segment && s.lag.is_some())
        }) {
            return None;
        }
        let known: Vec<u64> = segments.iter().filter_map(|s| s.lag).collect();
        (!known.is_empty()).then(|| known.iter().sum())
    }

    async fn close(mut self) {
        if let Some(subscription) = self.subscription.take() {
            bounded(&format!("close {}", self.name), subscription.close()).await;
        }
    }
}

/// Reads all of `readers` until `done`, at most [`WAIT`].
async fn all_until(readers: &mut [Reader], what: &str, done: impl Fn(&[Reader]) -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done(readers) {
        assert!(
            Instant::now() < deadline,
            "{what} within {WAIT:?} (assigned {:?}, revocations {:?})",
            readers.iter().map(|r| &r.assigned).collect::<Vec<_>>(),
            readers.iter().map(|r| r.revoked).collect::<Vec<_>>()
        );
        futures::future::join_all(readers.iter_mut().map(|r| r.pump(SLICE))).await;
    }
}

/// Reads both until `done`, at most [`WAIT`].
async fn both_until(
    a: &mut Reader,
    b: &mut Reader,
    what: &str,
    done: impl Fn(&Reader, &Reader) -> bool,
) {
    let deadline = Instant::now() + WAIT;
    while !done(a, b) {
        assert!(
            Instant::now() < deadline,
            "{what} within {WAIT:?} (assigned {:?} and {:?}, {} and {} deliveries)",
            a.assigned,
            b.assigned,
            a.seen.len(),
            b.seen.len()
        );
        tokio::join!(a.pump(SLICE), b.pump(SLICE));
    }
}

/// `call`'s outcome; panics naming `step` when it does not answer within [`WAIT`].
async fn bounded<T>(step: &str, call: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, call)
        .await
        .unwrap_or_else(|_| panic!("{step}: no answer within {WAIT:?}"))
}

/// A [`SubscriptionAdmin`] whose blocking calls each run on a thread of their own, bounded by
/// [`WAIT`]: a call that never returns fails the check and leaves its thread behind (a
/// `spawn_blocking` task would hold the runtime's shutdown instead).
struct Admin(Arc<Mutex<Box<dyn SubscriptionAdmin>>>);

impl Admin {
    fn new(admin: Box<dyn SubscriptionAdmin>) -> Admin {
        Admin(Arc::new(Mutex::new(admin)))
    }

    async fn call<T: Send + 'static>(
        &self,
        step: &str,
        call: impl FnOnce(&dyn SubscriptionAdmin) -> T + Send + 'static,
    ) -> T {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let admin = Arc::clone(&self.0);
        std::thread::spawn(move || {
            let admin = admin.lock().unwrap_or_else(|p| p.into_inner());
            let _ = tx.send(call(&**admin));
        });
        bounded(&format!("admin {step}"), rx)
            .await
            .unwrap_or_else(|_| panic!("admin {step}: the call panicked"))
    }

    async fn plan(&self, target: ResetTarget) -> Result<Vec<ReplayRange>, EventStoreError> {
        let step = format!("plan {target:?}");
        self.call(&step, move |a| a.plan(&target)).await
    }

    async fn apply(&self, ranges: &[ReplayRange]) -> Result<(), EventStoreError> {
        let ranges = ranges.to_vec();
        self.call("apply", move |a| a.apply(&ranges)).await
    }
}

/// An event of the kit to `source`: the seven headers, a fresh id, a JSON object payload.
fn event(source: &str, key: Option<&str>) -> OutgoingEvent {
    let id = uuid::Uuid::new_v4().to_string();
    let trace = uuid::Uuid::new_v4().simple().to_string();
    let span = &uuid::Uuid::new_v4().simple().to_string()[..16];
    let headers = HashMap::from([
        (headers::TYPE.to_string(), EVENT_TYPE.to_string()),
        (headers::ID.to_string(), id.clone()),
        (headers::CORRELATION_ID.to_string(), fresh("correlation")),
        (headers::TIMESTAMP.to_string(), rfc3339(now_ms())),
        (headers::SOURCE.to_string(), "conformance".to_string()),
        (headers::USER_ID.to_string(), "conformance-user".to_string()),
        (
            headers::TRACEPARENT.to_string(),
            format!("00-{trace}-{span}-01"),
        ),
    ]);
    OutgoingEvent {
        source: source.to_string(),
        payload: format!("{{\"id\":\"{id}\",\"amount\":42}}").into_bytes(),
        id,
        type_name: EVENT_TYPE.to_string(),
        key: key.map(str::to_string),
        headers,
    }
}

async fn publish_all(events: &dyn EventStore, sent: &[OutgoingEvent]) {
    for e in sent {
        bounded(&format!("publish {}", e.id), events.publish(e))
            .await
            .unwrap_or_else(|err| panic!("publish {}: {err}", e.id));
    }
}

fn ids(sent: &[OutgoingEvent]) -> Vec<String> {
    sent.iter().map(|e| e.id.clone()).collect()
}

fn id_of(d: &Delivery) -> &str {
    d.headers.get(headers::ID).map_or("", String::as_str)
}

fn fresh(what: &str) -> String {
    format!("{what}-{}", uuid::Uuid::new_v4().simple())
}

/// The browser of an event store that declares `browse`.
fn browser_of(events: &dyn EventStore) -> Arc<dyn EventBrowser> {
    events
        .browser()
        .expect("an event store declaring browse gives a browser")
}

/// An event of the kit to `source` with this key, type and correlation id.
fn typed(source: &str, key: &str, type_name: &str, correlation: &str) -> OutgoingEvent {
    let mut e = event(source, Some(key));
    e.type_name = type_name.to_string();
    e.headers
        .insert(headers::TYPE.to_string(), type_name.to_string());
    e.headers
        .insert(headers::CORRELATION_ID.to_string(), correlation.to_string());
    e
}

/// Publishes `sent` in order, a little apart, so each is newer than the one before.
async fn publish_spaced(events: &dyn EventStore, sent: &[OutgoingEvent]) {
    for e in sent {
        publish_all(events, std::slice::from_ref(e)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn summary_id(s: &EventSummary) -> String {
    s.headers.get(headers::ID).cloned().unwrap_or_default()
}

async fn search(browser: &dyn EventBrowser, request: SearchRequest) -> SearchPage {
    let what = format!("search {:?}", request.filters);
    bounded(&what, browser.search(request))
        .await
        .unwrap_or_else(|e| panic!("{what}: {e}"))
}

/// Searches the kit's source with `filters` until it finds exactly `want` (newest first), at
/// most [`WAIT`]; the summaries.
async fn found(
    browser: &dyn EventBrowser,
    kit: &EventStoreKit<'_>,
    filters: EventFilter,
    want: &[&OutgoingEvent],
    what: &str,
) -> Vec<EventSummary> {
    let want: Vec<&str> = want.iter().map(|e| e.id.as_str()).collect();
    let deadline = Instant::now() + WAIT;
    loop {
        let page = search(
            browser,
            SearchRequest {
                source: Some(kit.source.to_string()),
                filters: filters.clone(),
                ..SearchRequest::default()
            },
        )
        .await;
        let got: Vec<String> = page.results.iter().map(summary_id).collect();
        if got == want {
            return page.results;
        }
        let same_set = got.len() == want.len() && want.iter().all(|id| got.iter().any(|g| g == id));
        assert!(
            !same_set,
            "{what}: newest first, want {want:?}, got {got:?}"
        );
        assert!(
            Instant::now() < deadline,
            "{what}: {want:?} found within {WAIT:?}, got {got:?}"
        );
        tokio::time::sleep(SLICE).await;
    }
}

async fn open_tail(browser: &dyn EventBrowser, request: TailRequest) -> TailStream {
    bounded("tail", browser.tail(request))
        .await
        .unwrap_or_else(|e| panic!("tail: {e}"))
}

/// What a tail gave.
#[derive(Default)]
struct Parts {
    started: bool,
    records: Vec<EventSummary>,
    heartbeats: usize,
    end: Option<EndReason>,
}

impl Parts {
    /// Reads parts until `done` holds, at most [`WAIT`] in all (heartbeats do not extend it). The
    /// first part must be the start, no part may follow the end, the stream may not close before
    /// one, and a tail that ends before `done` fails the check.
    async fn until(&mut self, tail: &mut TailStream, what: &str, done: impl Fn(&Parts) -> bool) {
        let deadline = Instant::now() + WAIT;
        while !done(self) {
            if let Some(end) = self.end {
                panic!("tail: {what}: the tail ended first ({end:?})");
            }
            let part = tokio::time::timeout_at(deadline, tail.next())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "tail: {what} within {WAIT:?} (started {}, {} records, {} heartbeats)",
                        self.started,
                        self.records.len(),
                        self.heartbeats
                    )
                })
                .unwrap_or_else(|| panic!("tail: {what}: the stream closed without an end"));
            match part {
                TailEvent::Start(_) if !self.started => self.started = true,
                ref other if !self.started => {
                    panic!("tail: the first part is the start: {other:?}")
                }
                TailEvent::Start(start) => panic!("tail: a second start: {start:?}"),
                TailEvent::Record(r) => self.records.push(r),
                TailEvent::Heartbeat(_) => self.heartbeats += 1,
                TailEvent::End(end) => self.end = Some(end.reason),
            }
        }
    }

    fn has(&self, id: &str) -> bool {
        self.records.iter().any(|r| summary_id(r) == id)
    }
}

async fn lag(browser: &dyn EventBrowser, kit: &EventStoreKit<'_>) -> LagView {
    bounded(
        "lag",
        browser.lag(LagRequest {
            source: Some(kit.source.to_string()),
        }),
    )
    .await
    .unwrap_or_else(|e| panic!("lag: {e}"))
}

/// The segments of `subscription` in `view`, with their committed positions only.
fn committed(view: &LagView, subscription: &str) -> Option<Vec<SegmentLag>> {
    view.subscriptions
        .iter()
        .find(|s| s.subscription == subscription)
        .map(|s| {
            s.segments
                .iter()
                .map(|segment| SegmentLag {
                    head: String::new(),
                    lag: None,
                    ..segment.clone()
                })
                .collect()
        })
}
