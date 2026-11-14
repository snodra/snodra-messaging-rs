//! The event-store conformance kit (ADR 0027, feature `testing`): the checks every
//! [`EventStore`] add-on passes, a third party's included. An add-on's own test hands the kit a
//! source it prepared, a way to make the add-on (one call per service instance) and a message
//! store (for the at-least-once check):
//!
//! ```ignore
//! #[tokio::test]
//! async fn my_event_store_passes_conformance() {
//!     let store: Arc<dyn MessageStore> = ..; // any message store add-on, its schema in place
//!     run_event_store_conformance("kraken.conformance", || Arc::new(MyEventStore::new(..)), store)
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
//! - The checks run one after the other, each on subscription names of its own; a new name starts
//!   at the earliest position, so each check reads what the earlier ones appended and looks only
//!   at its own events (by their `id` header). Every check is public, so a failing one can be run
//!   by itself on an [`EventStoreKit`].
//! - The kit waits at most a minute for what the event store does in the background (an
//!   assignment, a lag refresh), and for every call it awaits: a publish, a subscribe or a close
//!   that does not answer fails the check naming the step, and so does a
//!   [`SubscriptionAdmin`] call (each runs on a thread of its own, so a blocked one is left
//!   behind rather than holding the runtime).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use tokio::time::Instant;

use super::{
    Capability, Delivery, EventStore, EventStoreError, MAX_POSITION_BYTES, MAX_SEGMENT_BYTES,
    OutgoingEvent, ReplayRange, ResetTarget, Subscription, SubscriptionAdmin, SubscriptionEvent,
    SubscriptionSpec, dead_letter_source, headers,
};
use crate::store::{MessageStore, ProcessedMarker};
use crate::time::{now_ms, rfc3339};

/// The longest a check waits for what the event store does in the background.
const WAIT: Duration = Duration::from_secs(60);
/// How long a check watches a subscription that must deliver nothing.
const QUIET: Duration = Duration::from_secs(3);
/// One turn of reading a subscription.
const SLICE: Duration = Duration::from_millis(250);
/// The type of the kit's events.
const EVENT_TYPE: &str = "conformance.Event";

/// Runs every check, and each one of a capability the event store declares (without `replay`
/// or `lag`, the check that it says so), by name; the first failing check panics naming itself
/// and the event store. See the module docs for `source`,
/// `make_events` and `store`.
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

/// Two instances of one subscription split its segments (adding the second spreads them) and
/// share its events: each event goes to one of them, and both get some.
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
        "both instances hold segments, none held by both",
        |a, b| {
            !a.assigned.is_empty() && !b.assigned.is_empty() && a.assigned.is_disjoint(&b.assigned)
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
/// and the core's suites (E0-R2, E0-R3); this check pins what the event store owes it.
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
        topic: None,
        partition: None,
        offset: None,
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
    let segments: BTreeSet<String> = reader
        .subscription()
        .segments()
        .into_iter()
        .map(|s| s.segment)
        .collect();
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

/// `native-dead-letter`: the dead-letter source takes a letter (its `kraken-dlt-*` headers
/// included) and delivers it as published.
pub async fn dead_letter_source_receives(kit: &EventStoreKit<'_>) {
    let events = kit.instance();
    let dlt = dead_letter_source(kit.source);
    let mut letter = event(&dlt, Some(fresh("key").as_str()));
    for (name, value) in [
        ("kraken-dlt-processor", "conformance"),
        ("kraken-dlt-reason", "FAILED"),
        ("kraken-dlt-error", "the handler failed"),
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

// ---- helpers -----------------------------------------------------------------------------------

/// One subscription and what it gave: its assignment and its deliveries, in order. Every
/// delivery is acknowledged as it comes but the held ones.
struct Reader {
    name: String,
    subscription: Option<Box<dyn Subscription>>,
    assigned: BTreeSet<String>,
    seen: Vec<Delivery>,
    held: HashSet<String>,
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
        }
    }

    fn subscription(&mut self) -> &mut dyn Subscription {
        self.subscription.as_deref_mut().expect("not closed")
    }

    /// The events with these ids are not acknowledged as they come.
    fn hold(&mut self, ids: &[String]) {
        self.held.extend(ids.iter().cloned());
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

    /// Takes what the subscription gives for `slice`.
    async fn pump(&mut self, slice: Duration) {
        let deadline = Instant::now() + slice;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return;
            }
            match tokio::time::timeout(left, self.subscription().next()).await {
                Err(_) => return,
                Ok(Err(e)) => panic!("{}: next: {e}", self.name),
                Ok(Ok(event)) => self.take(event),
            }
        }
    }

    fn take(&mut self, event: SubscriptionEvent) {
        match event {
            SubscriptionEvent::Assigned(segments) => {
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
                if !self.held.contains(id_of(&d)) {
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
