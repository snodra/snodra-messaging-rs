//! The store conformance kit (ADR 0028, feature `testing`): the checks every [`MessageStore`]
//! add-on passes. An add-on's own test hands a ready store (its schema or indexes in place) to
//! [`run_store_conformance`]:
//!
//! ```ignore
//! #[tokio::test]
//! async fn my_store_passes_conformance() {
//!     run_store_conformance(Arc::new(MyStore::new(..))).await;
//! }
//! ```
//!
//! The checks run one after the other on the one store, each on its own ids (random processor
//! names, keys and event ids). The outbox checks claim the whole outbox, so each first relays
//! whatever another check left pending and leaves nothing pending itself: the store must be the
//! kit's alone while it runs. Every check is public, so a failing one can be run by itself.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mongodb::bson::DateTime;

use super::dead_letters::letter_id;
use super::{
    ControlOperation, DeadLetter, DeadLetterError, DeadLetterReason, ExternalCall,
    ExternalCallStatus, Locked, MessageStore, OutboxEntry, OutboxStore, Parked, ProcessedMarker,
    RESET_KIND, ReplayRange,
};
use crate::saga::store::{
    Association, DeadlineDocument, DeadlineStatus, DispatchKind, SagaDispatchDocument,
    SagaDocument, SagaFailureDocument,
};

/// The lease of a claim that must not run out while a check runs.
const LONG_LEASE: Duration = Duration::from_secs(60);

/// Runs every check on `store`, by name; the first failing check panics naming itself and the
/// store.
pub async fn run_store_conformance(store: Arc<dyn MessageStore>) {
    let store = &*store;
    macro_rules! run {
        ($($check:ident),* $(,)?) => {
            $( run_check(store.name(), stringify!($check), $check(store)).await; )*
        };
    }
    run!(
        appended_entry_is_claimed_in_created_seq_order,
        rolled_back_transaction_leaves_no_outbox_entry,
        rolled_back_handler_leaves_no_processed_marker,
        processed_marker_records_once,
        concurrent_relays_publish_each_entry_once,
        expired_lease_is_reclaimed,
        purge_removes_only_sent_before,
        dead_letters_sequence_per_key,
        control_lock_is_exclusive_until_lease_expires,
        external_call_idempotency_returns_first_result,
        saga_ports_round_trip,
    );
}

async fn run_check(store: &str, check: &str, run: impl Future<Output = ()>) {
    if let Err(panic) = AssertUnwindSafe(run).catch_unwind().await {
        let cause = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("(no message)");
        panic!("store conformance check `{check}` failed on store `{store}`: {cause}");
    }
}

// ---- outbox ------------------------------------------------------------------------------------

/// Entries are claimed in `(created_at, seq)` order whichever way they were appended (in a
/// transaction or not); a claim skips what another owner holds, gives an owner back what it holds
/// already, and returns every field as appended. A retried entry keeps its place and counts the
/// attempt.
pub async fn appended_entry_is_claimed_in_created_seq_order(store: &dyn MessageStore) {
    let outbox = store.outbox();
    drain(&*outbox).await;
    let base = now_ms() - 60_000;
    let e1 = entry("conformance.Ordered", base, 2);
    let e2 = entry("conformance.Ordered", base, 1);
    let e3 = entry("conformance.Ordered", base - 1_000, 9);
    let e4 = entry("conformance.Ordered", base + 1, 0);
    let mut tx = store.tx().begin().await.expect("begin");
    outbox.append(&mut tx, e1.clone()).await.expect("append");
    store.tx().commit(&mut tx).await.expect("commit");
    outbox.append_now(e2.clone()).await.expect("append_now");
    let mut tx = store.tx().begin().await.expect("begin");
    outbox.append(&mut tx, e3.clone()).await.expect("append");
    store.tx().commit(&mut tx).await.expect("commit");
    outbox.append_now(e4.clone()).await.expect("append_now");

    let first = outbox.claim("relay-a", LONG_LEASE, 2).await.expect("claim");
    assert_eq!(
        ids(&first),
        ids(&[e3.clone(), e2.clone()]),
        "the oldest two, by (created_at, seq)"
    );
    let second = outbox
        .claim("relay-b", LONG_LEASE, 10)
        .await
        .expect("claim");
    assert_eq!(
        ids(&second),
        ids(&[e1.clone(), e4.clone()]),
        "another owner gets what relay-a does not hold"
    );
    let found = second.iter().find(|e| e.id == e1.id).expect("e1 claimed");
    assert_eq!(found.type_name, e1.type_name);
    assert_eq!(found.key, e1.key);
    assert_eq!(found.headers, e1.headers);
    assert_eq!(
        json(&found.payload),
        json(&e1.payload),
        "the payload as JSON"
    );
    assert_eq!(found.created_at_ms, e1.created_at_ms);
    assert_eq!(found.seq, e1.seq);
    assert_eq!(found.attempts, 0);

    outbox.mark_retry(&e3.id).await.expect("mark_retry");
    let again = outbox
        .claim("relay-a", LONG_LEASE, 10)
        .await
        .expect("claim");
    assert_eq!(
        ids(&again),
        ids(&[e3.clone(), e2.clone()]),
        "relay-a's own entries, again"
    );
    assert_eq!(
        again[0].attempts, 1,
        "a retried entry counts the failed attempt"
    );

    let all: Vec<String> = [e1, e2, e3, e4].into_iter().map(|e| e.id).collect();
    outbox.mark_sent(&all).await.expect("mark_sent");
    assert!(
        outbox
            .claim("relay-c", LONG_LEASE, 10)
            .await
            .expect("claim")
            .is_empty(),
        "a sent entry is never claimed"
    );
}

/// An entry appended in a transaction that is rolled back never reaches the relay; one appended
/// in a committed transaction does.
pub async fn rolled_back_transaction_leaves_no_outbox_entry(store: &dyn MessageStore) {
    let outbox = store.outbox();
    drain(&*outbox).await;
    let rolled_back = entry("conformance.RolledBack", now_ms(), 1);
    let committed = entry("conformance.Committed", now_ms(), 2);
    let mut tx = store.tx().begin().await.expect("begin");
    outbox
        .append(&mut tx, rolled_back.clone())
        .await
        .expect("append");
    store.tx().abort(&mut tx).await;
    let mut tx = store.tx().begin().await.expect("begin");
    outbox
        .append(&mut tx, committed.clone())
        .await
        .expect("append");
    store.tx().commit(&mut tx).await.expect("commit");

    let claimed = outbox.claim("relay", LONG_LEASE, 100).await.expect("claim");
    assert_eq!(
        ids(&claimed),
        vec![committed.id.clone()],
        "only the committed entry"
    );
    outbox.mark_sent(&[committed.id]).await.expect("mark_sent");
}

/// A handler's transaction that is rolled back leaves neither its processed-event marker nor the
/// events it published: a redelivery handles the event again.
pub async fn rolled_back_handler_leaves_no_processed_marker(store: &dyn MessageStore) {
    let outbox = store.outbox();
    let processed = store.processed_events();
    drain(&*outbox).await;
    let marker = marker(now_ms());
    let published = entry("conformance.PublishedByHandler", now_ms(), 1);

    let mut tx = store.tx().begin().await.expect("begin");
    assert!(processed.record(&mut tx, &marker).await.expect("record"));
    outbox
        .append(&mut tx, published.clone())
        .await
        .expect("append");
    store.tx().abort(&mut tx).await;

    assert!(
        outbox
            .claim("relay", LONG_LEASE, 100)
            .await
            .expect("claim")
            .is_empty(),
        "the rolled-back handler's event is gone"
    );
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        processed.record(&mut tx, &marker).await.expect("record"),
        "the rolled-back handler's marker is gone: the redelivery is recorded"
    );
    store.tx().commit(&mut tx).await.expect("commit");
}

/// A marker records once: a second record of its id (in a later transaction) is false, until a
/// purge past its time removes it. A purge keeps the markers recorded after its bound.
pub async fn processed_marker_records_once(store: &dyn MessageStore) {
    let processed = store.processed_events();
    let now = now_ms();
    let first = ProcessedMarker {
        segment: Some("kraken.events:0".into()),
        position: Some("42".into()),
        ..marker(now)
    };
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(processed.record(&mut tx, &first).await.expect("record"));
    store.tx().commit(&mut tx).await.expect("commit");

    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        !processed.record(&mut tx, &first).await.expect("record"),
        "a recorded id is a duplicate"
    );
    store.tx().abort(&mut tx).await;

    let other = marker(now);
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        processed.record(&mut tx, &other).await.expect("record"),
        "another id records"
    );
    store.tx().commit(&mut tx).await.expect("commit");

    let old = marker(now - 3_600_000);
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(processed.record(&mut tx, &old).await.expect("record"));
    store.tx().commit(&mut tx).await.expect("commit");
    let purged = processed.purge(now - 1_800_000).await.expect("purge");
    assert!(purged >= 1, "the old marker is purged (purged {purged})");

    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        processed.record(&mut tx, &old).await.expect("record"),
        "a purged id records again"
    );
    store.tx().commit(&mut tx).await.expect("commit");
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        !processed.record(&mut tx, &first).await.expect("record"),
        "the purge kept the recent marker"
    );
    store.tx().abort(&mut tx).await;
}

/// Two relays claiming one outbox at the same time: every entry is claimed by exactly one of them.
pub async fn concurrent_relays_publish_each_entry_once(store: &dyn MessageStore) {
    const ENTRIES: usize = 200;
    let outbox = store.outbox();
    drain(&*outbox).await;
    let now = now_ms();
    let mut appended = HashSet::new();
    for seq in 0..ENTRIES {
        let e = entry("conformance.Concurrent", now, seq as i64);
        appended.insert(e.id.clone());
        outbox.append_now(e).await.expect("append_now");
    }
    let relay = |owner: &'static str| {
        let outbox = Arc::clone(&outbox);
        tokio::spawn(async move {
            let mut published = Vec::new();
            loop {
                let batch = outbox.claim(owner, LONG_LEASE, 7).await.expect("claim");
                if batch.is_empty() {
                    return published;
                }
                let batch = ids(&batch);
                outbox.mark_sent(&batch).await.expect("mark_sent");
                published.extend(batch);
                tokio::task::yield_now().await;
            }
        })
    };
    let (a, b) = tokio::join!(relay("relay-a"), relay("relay-b"));
    let (a, b) = (a.expect("relay-a"), b.expect("relay-b"));

    let a_set: HashSet<String> = a.iter().cloned().collect();
    let b_set: HashSet<String> = b.iter().cloned().collect();
    assert_eq!(a_set.len(), a.len(), "relay-a published an entry twice");
    assert_eq!(b_set.len(), b.len(), "relay-b published an entry twice");
    let both: Vec<&String> = a_set.intersection(&b_set).collect();
    assert!(both.is_empty(), "published by both relays: {both:?}");
    let union: HashSet<String> = a_set.union(&b_set).cloned().collect();
    assert_eq!(union, appended, "every entry is published");
}

/// A claimer that stops (crashes) holds its entries until its lease runs out; then another relay
/// claims them, in order.
pub async fn expired_lease_is_reclaimed(store: &dyn MessageStore) {
    let outbox = store.outbox();
    drain(&*outbox).await;
    let now = now_ms();
    let entries: Vec<OutboxEntry> = (0..3)
        .map(|seq| entry("conformance.Leased", now, seq))
        .collect();
    for e in &entries {
        outbox.append_now(e.clone()).await.expect("append_now");
    }
    let crashed = outbox
        .claim("crashed", Duration::from_millis(500), 10)
        .await
        .expect("claim");
    assert_eq!(ids(&crashed), ids(&entries));
    assert!(
        outbox
            .claim("survivor", LONG_LEASE, 10)
            .await
            .expect("claim")
            .is_empty(),
        "held while the lease runs"
    );
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let reclaimed = outbox
        .claim("survivor", LONG_LEASE, 10)
        .await
        .expect("claim");
    assert_eq!(
        ids(&reclaimed),
        ids(&entries),
        "reclaimed in order after the lease"
    );
    assert!(
        outbox
            .claim("crashed", LONG_LEASE, 10)
            .await
            .expect("claim")
            .is_empty(),
        "the survivor holds them now"
    );
    outbox.mark_sent(&ids(&reclaimed)).await.expect("mark_sent");
}

/// A purge removes the entries sent before its bound and nothing else: neither later sent ones,
/// nor pending ones, nor failed ones (a failed entry is never claimed again either).
pub async fn purge_removes_only_sent_before(store: &dyn MessageStore) {
    let outbox = store.outbox();
    drain(&*outbox).await;
    outbox.purge(now_ms() + 86_400_000).await.expect("purge");
    let now = now_ms();
    let [early, late, failed, pending] =
        [0, 1, 2, 3].map(|seq| entry("conformance.Purged", now, seq));
    for e in [&early, &late, &failed, &pending] {
        outbox.append_now(e.clone()).await.expect("append_now");
    }
    assert_eq!(
        outbox
            .claim("relay", LONG_LEASE, 10)
            .await
            .expect("claim")
            .len(),
        4
    );
    outbox
        .mark_sent(std::slice::from_ref(&early.id))
        .await
        .expect("mark_sent");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let bound = now_ms();
    tokio::time::sleep(Duration::from_millis(300)).await;
    outbox
        .mark_sent(std::slice::from_ref(&late.id))
        .await
        .expect("mark_sent");
    outbox
        .mark_failed(&failed.id, "conformance: given up")
        .await
        .expect("mark_failed");

    assert_eq!(
        outbox.purge(bound).await.expect("purge"),
        1,
        "only the early sent entry"
    );
    assert_eq!(outbox.purge(bound).await.expect("purge"), 0);
    let left = outbox.claim("relay", LONG_LEASE, 10).await.expect("claim");
    assert_eq!(
        ids(&left),
        vec![pending.id.clone()],
        "the pending entry stays, the failed one is not claimed"
    );
    outbox
        .mark_sent(std::slice::from_ref(&pending.id))
        .await
        .expect("mark_sent");
    assert_eq!(
        outbox.purge(now_ms() + 60_000).await.expect("purge"),
        2,
        "the late and the pending entry once sent; never the failed one"
    );
}

// ---- dead letters ------------------------------------------------------------------------------

/// Dead letters are sequenced per (processor, key): the head is the lowest offset that is not
/// INVALID, an earlier letter blocks a retry, a second park of a record finds the first letter,
/// and a failed retry updates the letter in place.
pub async fn dead_letters_sequence_per_key(store: &dyn MessageStore) {
    let letters = store.dead_letters();
    let processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let other_processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let t0 = now_ms();
    let head = letter(
        &processor,
        0,
        5,
        Some("k"),
        DeadLetterReason::HandlerFailed,
        t0,
    );
    let sequenced = DeadLetter {
        blocked_by: Some(head.id.clone()),
        error: DeadLetterError::sequenced(&head.id),
        ..letter(
            &processor,
            0,
            7,
            Some("k"),
            DeadLetterReason::Sequenced,
            t0 + 1,
        )
    };
    let invalid = DeadLetter {
        payload: vec![0xff, 0xfe, 0x00, 0x01],
        error: DeadLetterError::invalid("not a JSON object"),
        ..letter(
            &processor,
            0,
            3,
            Some("k"),
            DeadLetterReason::Invalid,
            t0 + 2,
        )
    };
    let other_key = letter(
        &processor,
        1,
        6,
        Some("k2"),
        DeadLetterReason::HandlerFailed,
        t0 + 3,
    );
    let no_key = letter(
        &processor,
        0,
        9,
        None,
        DeadLetterReason::HandlerFailed,
        t0 + 4,
    );
    let elsewhere = letter(
        &other_processor,
        0,
        1,
        Some("k"),
        DeadLetterReason::HandlerFailed,
        t0,
    );
    for l in [&head, &sequenced, &invalid, &other_key, &no_key, &elsewhere] {
        assert_eq!(letters.park(l).await.expect("park"), Parked::New);
    }
    assert_eq!(
        letters.park(&head).await.expect("park"),
        Parked::Already,
        "a redelivery"
    );

    assert_eq!(
        letters.head(&processor, "k").await.expect("head"),
        Some(head.id.clone())
    );
    assert_eq!(
        letters.earlier(&processor, "k", 7).await.expect("earlier"),
        Some(head.id.clone())
    );
    assert_eq!(
        letters.earlier(&processor, "k", 5).await.expect("earlier"),
        None,
        "an INVALID letter blocks nothing"
    );
    assert_eq!(
        letters
            .parked_keys(&processor, TOPIC, &[0, 1])
            .await
            .expect("parked_keys"),
        HashSet::from(["k".to_string(), "k2".to_string()])
    );
    assert_eq!(
        letters
            .parked_keys(&processor, TOPIC, &[1])
            .await
            .expect("parked_keys"),
        HashSet::from(["k2".to_string()])
    );
    assert!(
        letters
            .parked_keys(&processor, TOPIC, &[])
            .await
            .expect("parked_keys")
            .is_empty()
    );

    assert_eq!(
        letters.get(&head.id).await.expect("get"),
        Some(head.clone())
    );
    assert_eq!(
        letters
            .get(&invalid.id)
            .await
            .expect("get")
            .map(|l| l.payload),
        Some(invalid.payload.clone()),
        "a payload that is not UTF-8 reads back byte for byte"
    );
    let listed = letters.list(Some(&processor), None).await.expect("list");
    assert_eq!(
        listed.iter().map(|l| l.id.clone()).collect::<Vec<_>>(),
        vec![
            head.id.clone(),
            sequenced.id.clone(),
            invalid.id.clone(),
            other_key.id.clone(),
            no_key.id.clone()
        ],
        "by parked_at"
    );
    assert_eq!(
        letters
            .list(Some(&processor), Some(2))
            .await
            .expect("list")
            .len(),
        2
    );
    assert_eq!(letters.count(Some(&processor)).await.expect("count"), 5);
    let mut summaries = letters.summaries(&processor).await.expect("summaries");
    summaries.sort_by_key(|s| s.offset);
    assert_eq!(
        summaries
            .iter()
            .map(|s| (
                s.id.clone(),
                s.key.clone(),
                s.offset,
                s.reason,
                s.parked_at_ms
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                invalid.id.clone(),
                Some("k".into()),
                3,
                DeadLetterReason::Invalid,
                t0 + 2
            ),
            (
                head.id.clone(),
                Some("k".into()),
                5,
                DeadLetterReason::HandlerFailed,
                t0
            ),
            (
                other_key.id.clone(),
                Some("k2".into()),
                6,
                DeadLetterReason::HandlerFailed,
                t0 + 3
            ),
            (
                sequenced.id.clone(),
                Some("k".into()),
                7,
                DeadLetterReason::Sequenced,
                t0 + 1
            ),
            (
                no_key.id.clone(),
                None,
                9,
                DeadLetterReason::HandlerFailed,
                t0 + 4
            ),
        ]
    );

    assert!(letters.delete(&head.id).await.expect("delete"));
    assert!(
        !letters.delete(&head.id).await.expect("delete"),
        "gone already"
    );
    assert_eq!(
        letters.head(&processor, "k").await.expect("head"),
        Some(sequenced.id.clone()),
        "the next letter of the key is the head"
    );
    let error = DeadLetterError::new("RemoteHandler", "failed again");
    assert!(
        letters
            .record_failure(&sequenced.id, &error, t0 + 60_000)
            .await
            .expect("record_failure")
    );
    let failed = letters
        .get(&sequenced.id)
        .await
        .expect("get")
        .expect("letter");
    assert_eq!(failed.attempts, sequenced.attempts + 1);
    assert_eq!(failed.reason, DeadLetterReason::HandlerFailed);
    assert_eq!(failed.error, error);
    assert_eq!(failed.last_attempt_at_ms, t0 + 60_000);
    assert_eq!(failed.blocked_by, None);
    assert!(
        !letters
            .record_failure(&head.id, &error, t0)
            .await
            .expect("record_failure"),
        "no letter"
    );

    for l in [&sequenced, &invalid, &other_key, &no_key, &elsewhere] {
        letters.delete(&l.id).await.expect("delete");
    }
    assert_eq!(letters.count(Some(&processor)).await.expect("count"), 0);
}

// ---- processor control -------------------------------------------------------------------------

/// A reset lock is held by one operation until its lease passes: another operation is refused
/// with the holder, the holder renews, and once the lease has passed another takes it. Only the
/// holder releases it; an expiry clears it only past the lease.
pub async fn control_lock_is_exclusive_until_lease_expires(store: &dyn MessageStore) {
    let control = store.processor_control();
    let processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let t0 = 1_800_000_000_000_i64;
    let state = control.get(&processor).await.expect("get");
    assert_eq!(state.processor, processor);
    assert!(
        !state.paused && state.operation.is_none() && state.replay.is_empty(),
        "absent: nothing held"
    );

    assert_eq!(
        control
            .lock(&processor, "op-1", t0 + 5_000, t0)
            .await
            .expect("lock"),
        Locked::Taken
    );
    let held = ControlOperation {
        id: "op-1".into(),
        kind: RESET_KIND.into(),
        lease_until_ms: t0 + 5_000,
        prepared_at_ms: t0,
    };
    assert_eq!(
        control
            .lock(&processor, "op-2", t0 + 6_000, t0 + 1_000)
            .await
            .expect("lock"),
        Locked::Held(held.clone()),
        "another operation is refused while the lease runs"
    );
    assert_eq!(
        control
            .lock(&processor, "op-1", t0 + 8_000, t0 + 2_000)
            .await
            .expect("lock"),
        Locked::Taken,
        "the holder renews"
    );
    let renewed = control
        .get(&processor)
        .await
        .expect("get")
        .operation
        .expect("held");
    assert_eq!(renewed.lease_until_ms, t0 + 8_000);
    assert_eq!(
        renewed.prepared_at_ms, t0,
        "a renewal keeps the first prepare's time"
    );
    assert!(
        control
            .extend(&processor, "op-1", t0 + 9_000, t0 + 3_000)
            .await
            .expect("extend")
    );
    assert!(
        !control
            .extend(&processor, "op-2", t0 + 9_000, t0 + 3_000)
            .await
            .expect("extend")
    );
    assert!(
        matches!(
            control
                .lock(&processor, "op-2", t0 + 10_000, t0 + 8_999)
                .await
                .expect("lock"),
            Locked::Held(_)
        ),
        "held until the lease passes"
    );
    assert!(
        !control
            .expire(&processor, "op-1", t0 + 8_999)
            .await
            .expect("expire"),
        "not past the lease"
    );
    assert_eq!(
        control
            .lock(&processor, "op-2", t0 + 15_000, t0 + 9_000)
            .await
            .expect("lock"),
        Locked::Taken,
        "the lease has passed"
    );
    assert!(
        !control
            .release(&processor, "op-1", t0 + 9_500)
            .await
            .expect("release"),
        "not the holder"
    );
    assert!(
        control
            .release(&processor, "op-2", t0 + 9_500)
            .await
            .expect("release")
    );
    assert!(
        control
            .get(&processor)
            .await
            .expect("get")
            .operation
            .is_none()
    );

    assert_eq!(
        control
            .lock(&processor, "op-3", t0 + 20_000, t0 + 10_000)
            .await
            .expect("lock"),
        Locked::Taken
    );
    assert!(
        control
            .expire(&processor, "op-3", t0 + 20_000)
            .await
            .expect("expire"),
        "past the lease"
    );
    assert!(
        control
            .get(&processor)
            .await
            .expect("get")
            .operation
            .is_none()
    );

    // The rest of the document beside the lock: pause and replay ranges.
    control
        .set_paused(&processor, true, "control-1", t0 + 21_000)
        .await
        .expect("pause");
    let range = ReplayRange {
        topic: TOPIC.into(),
        partition: 0,
        from: 10,
        until: 20,
    };
    let other = ReplayRange {
        topic: TOPIC.into(),
        partition: 1,
        from: 0,
        until: 5,
    };
    control
        .set_replay(&processor, &[range.clone(), other.clone()], t0 + 22_000)
        .await
        .expect("replay");
    let state = control.get(&processor).await.expect("get");
    assert!(state.paused);
    assert_eq!(state.paused_at_ms, Some(t0 + 21_000));
    assert_eq!(state.paused_by.as_deref(), Some("control-1"));
    assert_eq!(state.replay, vec![range.clone(), other.clone()]);
    assert_eq!(state.updated_at_ms, Some(t0 + 22_000));
    assert!(
        control
            .remove_range(&processor, &range, t0 + 23_000)
            .await
            .expect("remove_range")
    );
    assert!(
        !control
            .remove_range(&processor, &range, t0 + 23_000)
            .await
            .expect("remove_range")
    );
    control
        .set_paused(&processor, false, "control-2", t0 + 24_000)
        .await
        .expect("resume");
    let state = control.get(&processor).await.expect("get");
    assert!(!state.paused && state.paused_at_ms.is_none() && state.paused_by.is_none());
    assert_eq!(state.replay, vec![other]);
}

// ---- external calls ----------------------------------------------------------------------------

/// The first call of a key wins: a repeat finds its record, and once it succeeded it answers with
/// the first reply. Every write after the insert is the owner's only; a lapsed call is taken over
/// or given up as UNKNOWN only when the record is still the one read.
pub async fn external_call_idempotency_returns_first_result(store: &dyn MessageStore) {
    let calls = store.external_calls();
    let t0 = now_ms();
    let key = format!("conformance:{}", uuid::Uuid::new_v4());
    let first = in_progress(&key, "owner-1", t0, t0 + 60_000);
    assert!(calls.insert(&first).await.expect("insert"));
    assert!(
        !calls
            .insert(&in_progress(&key, "owner-2", t0, t0 + 60_000))
            .await
            .expect("insert"),
        "the key is taken"
    );
    assert_eq!(calls.find(&key).await.expect("find"), Some(first.clone()));
    assert!(
        !calls
            .succeed(&key, "owner-2", "{\"n\":2}", t0 + 1)
            .await
            .expect("succeed"),
        "not the owner"
    );
    assert!(
        calls
            .succeed(&key, "owner-1", "{\"n\":1}", t0 + 2)
            .await
            .expect("succeed")
    );
    assert!(
        !calls
            .succeed(&key, "owner-1", "{\"n\":3}", t0 + 3)
            .await
            .expect("succeed"),
        "settled"
    );
    assert!(
        !calls
            .insert(&in_progress(&key, "owner-3", t0, t0 + 60_000))
            .await
            .expect("insert")
    );
    let settled = calls.find(&key).await.expect("find").expect("record");
    assert_eq!(settled.status, ExternalCallStatus::Succeeded);
    assert_eq!(
        settled.reply.as_deref(),
        Some("{\"n\":1}"),
        "the first result"
    );
    assert_eq!(
        (settled.owner, settled.lease_until_ms),
        (None, None),
        "a settled record has no owner"
    );
    assert_eq!(settled.updated_at_ms, t0 + 2);

    // A lapsed call taken over, then failed by its new owner.
    let lapsed_key = format!("conformance:{}", uuid::Uuid::new_v4());
    let lapsed = in_progress(&lapsed_key, "owner-1", t0, t0 + 10);
    assert!(calls.insert(&lapsed).await.expect("insert"));
    let taken = calls
        .take_over(&lapsed, "owner-2", t0 + 60_000, t0 + 20)
        .await
        .expect("take_over")
        .expect("taken over");
    assert_eq!(
        (taken.attempts, taken.owner.as_deref(), taken.lease_until_ms),
        (2, Some("owner-2"), Some(t0 + 60_000))
    );
    assert!(
        calls
            .take_over(&lapsed, "owner-3", t0 + 60_000, t0 + 21)
            .await
            .expect("take_over")
            .is_none(),
        "the record changed since it was read"
    );
    assert!(
        !calls
            .fail(&lapsed_key, "owner-1", "{\"code\":\"X\"}", t0 + 22)
            .await
            .expect("fail"),
        "not the owner any more"
    );
    assert!(
        calls
            .fail(
                &lapsed_key,
                "owner-2",
                "{\"code\":\"HANDLER_ERROR\",\"message\":\"m\"}",
                t0 + 23
            )
            .await
            .expect("fail")
    );
    let failed = calls
        .find(&lapsed_key)
        .await
        .expect("find")
        .expect("record");
    assert_eq!(failed.status, ExternalCallStatus::Failed);
    assert_eq!(
        failed.error.as_deref(),
        Some("{\"code\":\"HANDLER_ERROR\",\"message\":\"m\"}")
    );
    assert_eq!((failed.owner, failed.lease_until_ms), (None, None));

    // A lapsed call given up as UNKNOWN, only while unchanged.
    let unknown_key = format!("conformance:{}", uuid::Uuid::new_v4());
    let stale = in_progress(&unknown_key, "owner-1", t0, t0 + 10);
    assert!(calls.insert(&stale).await.expect("insert"));
    let changed = ExternalCall {
        owner: Some("someone-else".into()),
        ..stale.clone()
    };
    assert!(
        !calls.expire(&changed, t0 + 20).await.expect("expire"),
        "not the record read"
    );
    assert!(calls.expire(&stale, t0 + 20).await.expect("expire"));
    let unknown = calls
        .find(&unknown_key)
        .await
        .expect("find")
        .expect("record");
    assert_eq!(unknown.status, ExternalCallStatus::Unknown);
    assert_eq!((unknown.owner, unknown.lease_until_ms), (None, None));

    // The owner gives up as UNKNOWN; a retryable failure resets the key.
    let owned_key = format!("conformance:{}", uuid::Uuid::new_v4());
    assert!(
        calls
            .insert(&in_progress(&owned_key, "owner-1", t0, t0 + 60_000))
            .await
            .expect("insert")
    );
    assert!(
        !calls
            .mark_unknown(&owned_key, "owner-2", t0 + 1)
            .await
            .expect("mark_unknown")
    );
    assert!(
        calls
            .mark_unknown(&owned_key, "owner-1", t0 + 1)
            .await
            .expect("mark_unknown")
    );
    assert_eq!(
        calls
            .find(&owned_key)
            .await
            .expect("find")
            .map(|c| c.status),
        Some(ExternalCallStatus::Unknown)
    );
    let reset_key = format!("conformance:{}", uuid::Uuid::new_v4());
    assert!(
        calls
            .insert(&in_progress(&reset_key, "owner-1", t0, t0 + 60_000))
            .await
            .expect("insert")
    );
    assert!(
        !calls.reset(&reset_key, "owner-2").await.expect("reset"),
        "not the owner"
    );
    assert!(calls.reset(&reset_key, "owner-1").await.expect("reset"));
    assert_eq!(calls.find(&reset_key).await.expect("find"), None);
    assert!(
        calls
            .insert(&in_progress(&reset_key, "owner-2", t0, t0 + 60_000))
            .await
            .expect("insert"),
        "begun afresh"
    );
}

// ---- sagas -------------------------------------------------------------------------------------

/// Each saga port writes what it reads back, in a committed transaction; an aborted one leaves
/// nothing.
pub async fn saga_ports_round_trip(store: &dyn MessageStore) {
    let sagas = store.sagas();
    let txs = store.tx();
    let now = DateTime::from_millis(now_ms());
    let type_name = format!("conformance.Saga{}", uuid::Uuid::new_v4().simple());
    let saga = SagaDocument {
        id: uuid::Uuid::new_v4().to_string(),
        type_name: type_name.clone(),
        state: "{\"step\":1}".into(),
        associations: vec![Association::new("orderId", "o-1")],
        start_key: Some(format!("{type_name}|orderId=o-1")),
        ended: false,
        ended_at: None,
        version: 0,
        created_at: now,
        updated_at: now,
    };
    let mut tx = txs.begin().await.expect("begin");
    sagas.sagas.insert(&mut tx, &saga).await.expect("insert");
    txs.commit(&mut tx).await.expect("commit");

    let mut tx = txs.begin().await.expect("begin");
    assert_eq!(
        sagas
            .sagas
            .find_by_id(&mut tx, &saga.id)
            .await
            .expect("find"),
        Some(saga.clone())
    );
    assert_eq!(
        sagas
            .sagas
            .find_live(&mut tx, &type_name, "orderId", "o-1")
            .await
            .expect("find_live"),
        vec![saga.clone()]
    );
    assert!(
        sagas
            .sagas
            .find_live(&mut tx, &type_name, "orderId", "o-2")
            .await
            .expect("find_live")
            .is_empty()
    );
    let updated = SagaDocument {
        state: "{\"step\":2}".into(),
        version: 1,
        ..saga.clone()
    };
    assert!(
        sagas
            .sagas
            .update(&mut tx, &updated, 0)
            .await
            .expect("update")
    );
    txs.commit(&mut tx).await.expect("commit");
    let mut tx = txs.begin().await.expect("begin");
    assert!(
        !sagas
            .sagas
            .update(&mut tx, &updated, 0)
            .await
            .expect("update"),
        "a stale version"
    );
    txs.abort(&mut tx).await;
    let mut tx = txs.begin().await.expect("begin");
    let twin = SagaDocument {
        id: uuid::Uuid::new_v4().to_string(),
        ..saga.clone()
    };
    let conflict = sagas.sagas.insert(&mut tx, &twin).await;
    assert!(
        matches!(&conflict, Err(e) if e.is_conflict()),
        "a live start key is a conflict: {conflict:?}"
    );
    txs.abort(&mut tx).await;

    // An aborted insert leaves nothing.
    let mut tx = txs.begin().await.expect("begin");
    let rolled_back = SagaDocument {
        id: uuid::Uuid::new_v4().to_string(),
        start_key: None,
        ..saga.clone()
    };
    sagas
        .sagas
        .insert(&mut tx, &rolled_back)
        .await
        .expect("insert");
    txs.abort(&mut tx).await;
    let mut tx = txs.plain().await.expect("plain");
    assert_eq!(
        sagas
            .sagas
            .find_by_id(&mut tx, &rolled_back.id)
            .await
            .expect("find"),
        None
    );

    // Dispatch entries, deadlines, failures and markers.
    let dispatch = SagaDispatchDocument {
        id: format!("{}:1000", saga.id),
        saga_id: saga.id.clone(),
        seq: 1000,
        kind: DispatchKind::Command,
        type_name: "conformance.Charge".into(),
        payload: "{\"amount\":42}".into(),
        headers: HashMap::from([("correlation-id".to_string(), "c-1".to_string())]),
        routing_key: Some("o-1".into()),
        created_at: now,
        dispatched_at: None,
        attempts: 0,
        next_attempt_at: None,
        failed_at: None,
        failure_reason: None,
        failure_message: None,
        notice_pending: None,
        notice_attempts: None,
        notice_next_at: None,
    };
    let deadline = |name: &str| DeadlineDocument {
        id: uuid::Uuid::new_v4().to_string(),
        saga_type: type_name.clone(),
        saga_id: saga.id.clone(),
        name: name.to_string(),
        payload: Some("{\"why\":\"timeout\"}".into()),
        payload_type: Some("conformance.Timeout".into()),
        due_at: DateTime::from_millis(now.timestamp_millis() + 60_000),
        status: DeadlineStatus::Scheduled,
        claimed_by: None,
        claimed_until: None,
        finished_at: None,
        correlation_id: Some("c-1".into()),
    };
    let fired = deadline("fired");
    let by_id = deadline("by-id");
    let by_name = deadline("by-name");
    let rest = deadline("rest");
    let mut tx = txs.begin().await.expect("begin");
    sagas
        .dispatch
        .record(&mut tx, std::slice::from_ref(&dispatch))
        .await
        .expect("record");
    sagas
        .dispatch
        .record(&mut tx, &[])
        .await
        .expect("record nothing");
    for d in [&fired, &by_id, &by_name, &rest] {
        sagas.deadlines.insert(&mut tx, d).await.expect("insert");
    }
    txs.commit(&mut tx).await.expect("commit");

    let later = DateTime::from_millis(now.timestamp_millis() + 1_000);
    let mut tx = txs.begin().await.expect("begin");
    assert_eq!(
        sagas
            .deadlines
            .find_scheduled(&mut tx, &fired.id)
            .await
            .expect("find"),
        Some(fired.clone())
    );
    assert!(
        sagas
            .deadlines
            .mark_fired(&mut tx, &fired.id, later)
            .await
            .expect("mark_fired")
    );
    assert!(
        !sagas
            .deadlines
            .mark_fired(&mut tx, &fired.id, later)
            .await
            .expect("mark_fired"),
        "fired once"
    );
    sagas
        .deadlines
        .cancel(&mut tx, &saga.id, &by_id.id, later)
        .await
        .expect("cancel");
    sagas
        .deadlines
        .cancel_by_name(&mut tx, &saga.id, "by-name", later)
        .await
        .expect("cancel_by_name");
    assert_eq!(
        sagas
            .deadlines
            .find_scheduled(&mut tx, &rest.id)
            .await
            .expect("find"),
        Some(rest.clone()),
        "untouched"
    );
    sagas
        .deadlines
        .cancel_all(&mut tx, &saga.id, later)
        .await
        .expect("cancel_all");
    txs.commit(&mut tx).await.expect("commit");
    let mut tx = txs.plain().await.expect("plain");
    for d in [&fired, &by_id, &by_name, &rest] {
        assert_eq!(
            sagas
                .deadlines
                .find_scheduled(&mut tx, &d.id)
                .await
                .expect("find"),
            None,
            "{} is not scheduled",
            d.name
        );
    }

    let failure = SagaFailureDocument {
        id: format!("{type_name}:{}", uuid::Uuid::new_v4()),
        saga_type: type_name.clone(),
        event_id: uuid::Uuid::new_v4().to_string(),
        event_type: "conformance.Happened".into(),
        payload: "{\"x\":1}".into(),
        headers: HashMap::from([("correlationId".to_string(), "c-1".to_string())]),
        error: "boom".into(),
        attempts: 3,
        first_failed_at: now,
        last_failed_at: now,
    };
    let mut tx = txs.begin().await.expect("begin");
    sagas.failures.save(&mut tx, &failure).await.expect("save");
    txs.commit(&mut tx).await.expect("commit");
    let mut tx = txs.plain().await.expect("plain");
    assert_eq!(
        sagas
            .failures
            .find(&mut tx, &failure.id)
            .await
            .expect("find"),
        Some(failure.clone())
    );
    assert!(
        sagas
            .failures
            .failed_again(&mut tx, &failure.id, "boom again", 2, later)
            .await
            .expect("failed_again")
    );
    let again = sagas
        .failures
        .find(&mut tx, &failure.id)
        .await
        .expect("find")
        .expect("failure");
    assert_eq!(
        (again.attempts, again.error.as_str(), again.last_failed_at),
        (5, "boom again", later)
    );
    assert_eq!(
        sagas
            .failures
            .list(&mut tx, Some(&type_name))
            .await
            .expect("list"),
        vec![again.clone()]
    );
    assert!(
        sagas
            .failures
            .delete(&mut tx, &failure.id)
            .await
            .expect("delete")
    );
    assert!(
        !sagas
            .failures
            .delete(&mut tx, &failure.id)
            .await
            .expect("delete")
    );
    assert!(
        !sagas
            .failures
            .failed_again(&mut tx, &failure.id, "x", 1, later)
            .await
            .expect("failed_again"),
        "never inserts"
    );
    assert!(
        sagas
            .failures
            .list(&mut tx, Some(&type_name))
            .await
            .expect("list")
            .is_empty()
    );

    let marker_id = format!("saga:{type_name}:{}", uuid::Uuid::new_v4());
    let mut tx = txs.begin().await.expect("begin");
    assert!(
        !sagas
            .markers
            .exists(&mut tx, &marker_id)
            .await
            .expect("exists")
    );
    sagas
        .markers
        .insert(&mut tx, &marker_id, "conformance.Happened", now)
        .await
        .expect("insert");
    txs.commit(&mut tx).await.expect("commit");
    let mut tx = txs.begin().await.expect("begin");
    assert!(
        sagas
            .markers
            .exists(&mut tx, &marker_id)
            .await
            .expect("exists")
    );
    let duplicate = sagas
        .markers
        .insert(&mut tx, &marker_id, "conformance.Happened", now)
        .await;
    assert!(
        matches!(&duplicate, Err(e) if e.is_conflict()),
        "a marker inserts once: {duplicate:?}"
    );
    txs.abort(&mut tx).await;
}

// ---- helpers -----------------------------------------------------------------------------------

const TOPIC: &str = "kraken.events";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn entry(type_name: &str, created_at_ms: i64, seq: i64) -> OutboxEntry {
    OutboxEntry {
        id: uuid::Uuid::new_v4().to_string(),
        type_name: type_name.to_string(),
        key: Some(format!("key-{seq}")),
        headers: HashMap::from([
            (
                "correlationId".to_string(),
                uuid::Uuid::new_v4().to_string(),
            ),
            ("userId".to_string(), String::new()),
        ]),
        payload: serde_json::to_vec(
            &serde_json::json!({ "seq": seq, "text": "é", "nested": { "ok": true } }),
        )
        .expect("JSON"),
        created_at_ms,
        seq,
        attempts: 0,
    }
}

fn ids(entries: &[OutboxEntry]) -> Vec<String> {
    entries.iter().map(|e| e.id.clone()).collect()
}

fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("the payload is JSON")
}

/// Relays (marks sent) whatever another check left claimable, so the next claim sees only the
/// calling check's entries.
async fn drain(outbox: &dyn OutboxStore) {
    let owner = format!("drain-{}", uuid::Uuid::new_v4());
    loop {
        let left = outbox
            .claim(&owner, LONG_LEASE, 1_000)
            .await
            .expect("claim");
        if left.is_empty() {
            return;
        }
        outbox.mark_sent(&ids(&left)).await.expect("mark_sent");
    }
}

fn marker(at_ms: i64) -> ProcessedMarker {
    ProcessedMarker {
        id: format!("processor:conformance:{}", uuid::Uuid::new_v4()),
        segment: None,
        position: None,
        at_ms,
    }
}

fn letter(
    processor: &str,
    partition: i32,
    offset: i64,
    key: Option<&str>,
    reason: DeadLetterReason,
    parked_at_ms: i64,
) -> DeadLetter {
    DeadLetter {
        id: letter_id(processor, TOPIC, partition, offset),
        processor: processor.to_string(),
        topic: TOPIC.to_string(),
        partition,
        offset,
        key: key.map(str::to_string),
        event_id: Some(uuid::Uuid::new_v4().to_string()),
        event_type: Some("conformance.Happened".into()),
        headers: BTreeMap::from([("type".to_string(), "conformance.Happened".to_string())]),
        payload: b"{\"x\":1}".to_vec(),
        reason,
        error: DeadLetterError::new("RemoteHandler", "boom"),
        blocked_by: None,
        attempts: 3,
        parked_at_ms,
        last_attempt_at_ms: parked_at_ms,
        expire_at_ms: parked_at_ms + 86_400_000,
        replay: false,
    }
}

fn in_progress(key: &str, owner: &str, now_ms: i64, lease_until_ms: i64) -> ExternalCall {
    ExternalCall {
        key: key.to_string(),
        status: ExternalCallStatus::InProgress,
        reply: None,
        error: None,
        attempts: 1,
        updated_at_ms: now_ms,
        lease_until_ms: Some(lease_until_ms),
        owner: Some(owner.to_string()),
    }
}
