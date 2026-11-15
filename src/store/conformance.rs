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
//!
//! Two checks race two live transactions of the store on one saga
//! ([`two_transactions_starting_one_saga_key_leave_one_live_saga`],
//! [`two_transactions_updating_one_saga_version_commit_once`]): the loser's
//! [`StoreError::Conflict`] may surface at its write (a database that locks the record, e.g.
//! MongoDB's write conflict) or at its commit (a store that checks at the end); either passes.
//!
//! The kit must run on a multi-thread runtime (`#[tokio::test(flavor = "multi_thread")]`): two
//! tasks race in [`concurrent_relays_publish_each_entry_once`],
//! [`two_schedulers_claim_each_due_deadline_once`], [`two_relays_never_hold_one_saga_lease`] and
//! [`concurrent_notice_claims_return_each_notice_once`].
//! Its time bounds leave the store a second or more: a store stamps times with the application
//! clock, and the kit never compares the host clock with a stamped time at a finer margin.
//!
//! # The checks and their names in kraken-messaging
//!
//! The Kotlin kit (`MessageStoreConformanceTest` in `kraken-messaging-test`) checks the same
//! contract; each check here, in the order it runs, with the name of its Kotlin twin (the same
//! name in camelCase unless the row says otherwise):
//!
//! | Rust check | Kotlin check |
//! |---|---|
//! | [`appended_entry_is_claimed_in_created_seq_order`] | `appendedEntryIsClaimedInCreatedSeqOrder` |
//! | [`outbox_rejects_a_payload_that_is_not_a_json_object`] | **`outboxPayloadMustBeAJsonObject`** |
//! | [`pending_counts_entries_neither_sent_nor_failed`] | **`oldestPendingIsTheOldestEntryNeitherSentNorFailed`** |
//! | [`rolled_back_transaction_leaves_no_outbox_entry`] | `rolledBackTransactionLeavesNoOutboxEntry` |
//! | [`rolled_back_handler_leaves_no_processed_marker`] | `rolledBackHandlerLeavesNoProcessedMarker` |
//! | [`processed_marker_records_once`] | `processedMarkerRecordsOnce` |
//! | [`delete_markers_removes_only_the_range`] | `deleteMarkersRemovesOnlyTheRange` |
//! | [`concurrent_relays_publish_each_entry_once`] | `concurrentRelaysPublishEachEntryOnce` |
//! | [`expired_lease_is_reclaimed`] | `expiredLeaseIsReclaimed` |
//! | [`purge_removes_only_sent_before`] | `purgeRemovesOnlySentBefore` |
//! | [`dead_letters_sequence_per_key`] | `deadLettersSequencePerKey` |
//! | [`dead_letter_parked_inside_rolled_back_transaction_is_kept`] | `deadLetterParkedInsideRolledBackTransactionIsKept` |
//! | [`dead_letter_deleted_in_a_transaction_goes_with_it`] | **none** (Rust only) |
//! | [`control_lock_is_exclusive_until_lease_expires`] | `controlLockIsExclusiveUntilLeaseExpires` |
//! | [`pause_state_round_trips`] | `pauseStateRoundTrips` |
//! | [`replay_ranges_round_trip`] | `replayRangesRoundTrip` |
//! | [`processor_control_write_inside_rolled_back_transaction_is_kept`] | `processorControlWriteInsideRolledBackTransactionIsKept` |
//! | [`external_call_idempotency_returns_first_result`] | `externalCallIdempotencyReturnsFirstResult` |
//! | [`saga_ports_round_trip`] | `sagaPortsRoundTrip` |
//! | [`two_transactions_starting_one_saga_key_leave_one_live_saga`] | **`concurrentStartsOfOneSagaKeyLetOneWin`** |
//! | [`two_transactions_updating_one_saga_version_commit_once`] | **`concurrentUpdatesOfOneSagaLetOneWin`** |
//! | [`saga_dispatch_store_refuses_an_event_the_outbox_would_refuse`] | **`sagaEventTheOutboxWouldRefuseIsNotRecorded`** |
//! | [`due_deadline_is_claimed_once_until_its_lease_passes`] | **`dueDeadlinesAreClaimedOnceUntilTheirLeaseExpires`** |
//! | [`an_empty_deadline_correlation_id_reads_as_absent`] | **none yet** |
//! | [`two_schedulers_claim_each_due_deadline_once`] | **`concurrentDeadlineClaimsReturnEachDeadlineOnce`** |
//! | [`dispatch_hands_out_each_saga_in_seq_order`] | **`sagaDispatchEntriesAreRelayedInOrderPerSaga`** and **`sagaDispatchLeaseIsExclusiveUntilItExpires`** (one check here) |
//! | [`two_relays_never_hold_one_saga_lease`] | **`concurrentLeaseClaimsGrantOneOwner`** |
//! | [`concurrent_notice_claims_return_each_notice_once`] | `concurrentNoticeClaimsReturnEachNoticeOnce` |

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::Barrier;

use super::dead_letters::letter_id;
use super::{
    ControlOperation, DeadLetter, DeadLetterError, DeadLetterReason, DeadlineStore,
    DispatchFailure, ExternalCall, ExternalCallStatus, Locked, MessageStore, OutboxEntry,
    OutboxStore, Parked, ProcessedMarker, ProcessorControlState, RESET_KIND, SagaDispatchStore,
    StoreError, TxProvider,
};
use crate::error::MessagingError;
use crate::event_store::ReplayRange;
use crate::events::outbox::MAX_PAYLOAD_DEPTH;
use crate::saga::FailureReason;
use crate::saga::store::{
    Association, DeadlineDocument, DeadlineStatus, DispatchKind, SagaDispatchDocument,
    SagaDocument, SagaFailureDocument,
};
use crate::time::now_ms;

/// The lease of a claim that must not run out while a check runs.
const LONG_LEASE: Duration = Duration::from_secs(60);
/// The longest a check waits at a barrier for the other task: a failing claim fails the check
/// rather than hanging it.
const BARRIER_WAIT: Duration = Duration::from_secs(30);

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
        outbox_rejects_a_payload_that_is_not_a_json_object,
        pending_counts_entries_neither_sent_nor_failed,
        rolled_back_transaction_leaves_no_outbox_entry,
        rolled_back_handler_leaves_no_processed_marker,
        processed_marker_records_once,
        delete_markers_removes_only_the_range,
        concurrent_relays_publish_each_entry_once,
        expired_lease_is_reclaimed,
        purge_removes_only_sent_before,
        dead_letters_sequence_per_key,
        dead_letter_parked_inside_rolled_back_transaction_is_kept,
        dead_letter_deleted_in_a_transaction_goes_with_it,
        control_lock_is_exclusive_until_lease_expires,
        pause_state_round_trips,
        replay_ranges_round_trip,
        processor_control_write_inside_rolled_back_transaction_is_kept,
        external_call_idempotency_returns_first_result,
        saga_ports_round_trip,
        two_transactions_starting_one_saga_key_leave_one_live_saga,
        two_transactions_updating_one_saga_version_commit_once,
        saga_dispatch_store_refuses_an_event_the_outbox_would_refuse,
        due_deadline_is_claimed_once_until_its_lease_passes,
        an_empty_deadline_correlation_id_reads_as_absent,
        two_schedulers_claim_each_due_deadline_once,
        dispatch_hands_out_each_saga_in_seq_order,
        two_relays_never_hold_one_saga_lease,
        concurrent_notice_claims_return_each_notice_once,
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

    outbox
        .mark_attempt_failed(&e3.id, "conformance: broker unavailable")
        .await
        .expect("mark_attempt_failed");
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

/// An outbox payload is a JSON object whose integers fit in an `i64`, nested at most
/// [`MAX_PAYLOAD_DEPTH`] levels: anything else (not JSON, an array, a bare value, an integer above
/// `i64::MAX`, a number no double holds, a lone surrogate escape, a leading BOM, bytes that are
/// not UTF-8, one level too deep) is refused with `InvalidMessage`, appended on its own or in a
/// transaction, and nothing is appended. The deepest payload allowed is kept by every store.
pub async fn outbox_rejects_a_payload_that_is_not_a_json_object(store: &dyn MessageStore) {
    let outbox = store.outbox();
    drain(&*outbox).await;
    let too_deep = nested(MAX_PAYLOAD_DEPTH + 1).into_bytes();
    for payload in [
        &b"not json"[..],
        b"{\"n\":1e400}",
        b"{\"n\":-1e400}",
        b"{\"s\":\"\\ud800\"}",
        b"{\"\\udc00\":1}",
        b"\xef\xbb\xbf{}",
        b"{\"s\":\"\xff\"}",
        &too_deep,
        b"[1,2]",
        b"\"text\"",
        b"null",
        b"{\"n\":18446744073709551615}",
        // Read as floats by a JSON parser, so only the text tells them apart.
        b"{\"n\":-9223372036854775809}",
        b"{\"n\":18446744073709551616}",
        b"{\"n\":123456789012345678901234567890}",
        b"{\"a\":[1,{\"b\":[2,-99999999999999999999]}]}",
    ] {
        let shown = String::from_utf8_lossy(payload).into_owned();
        let bad = OutboxEntry {
            payload: payload.to_vec(),
            ..entry("conformance.Invalid", now_ms(), 0)
        };
        let refused = outbox.append_now(bad.clone()).await;
        assert!(
            matches!(
                &refused,
                Err(StoreError::Failed(MessagingError::InvalidMessage(_)))
            ),
            "{shown} on its own: {refused:?}"
        );
        let mut tx = store.tx().begin().await.expect("begin");
        let refused = outbox.append(&mut tx, bad).await;
        store.tx().abort(&mut tx).await;
        assert!(
            matches!(
                &refused,
                Err(StoreError::Failed(MessagingError::InvalidMessage(_)))
            ),
            "{shown} in a transaction: {refused:?}"
        );
    }
    let claimed = outbox
        .claim("relay-a", LONG_LEASE, 100)
        .await
        .expect("claim");
    assert!(claimed.is_empty(), "nothing appended: {:?}", ids(&claimed));
    // The same digits in a string, a decimal, an exponent and the i64 bounds are fine.
    let fine = OutboxEntry {
        payload: br#"{"s":"123456789012345678901234567890","d":1.5,"e":1e30,"max":9223372036854775807,"min":-9223372036854775808}"#.to_vec(),
        ..entry("conformance.Fine", now_ms(), 0)
    };
    outbox
        .append_now(fine.clone())
        .await
        .expect("a payload within the contract");
    let claimed = outbox
        .claim("relay-a", LONG_LEASE, 100)
        .await
        .expect("claim");
    assert_eq!(ids(&claimed), ids(&[fine]));
    outbox.mark_sent(&ids(&claimed)).await.expect("mark_sent");
    // The deepest payload allowed is kept as appended, on every store.
    let deepest = OutboxEntry {
        payload: nested(MAX_PAYLOAD_DEPTH).into_bytes(),
        ..entry("conformance.Deep", now_ms(), 0)
    };
    outbox
        .append_now(deepest.clone())
        .await
        .expect("the deepest payload allowed");
    let claimed = outbox
        .claim("relay-a", LONG_LEASE, 100)
        .await
        .expect("claim");
    assert_eq!(ids(&claimed), ids(std::slice::from_ref(&deepest)));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&claimed[0].payload).expect("JSON"),
        serde_json::from_slice::<serde_json::Value>(&deepest.payload).expect("JSON"),
        "kept as appended"
    );
    outbox.mark_sent(&ids(&claimed)).await.expect("mark_sent");
}

/// A JSON object nested `levels` deep: `{"a":{"a":...{"a":1}...}}`, `{"a":1}` being one level.
fn nested(levels: usize) -> String {
    format!("{}1{}", "{\"a\":".repeat(levels), "}".repeat(levels))
}

/// `pending` counts the entries neither sent nor failed, held by a relay or not, and
/// `oldest_pending` is the `created_at_ms` of the oldest of them.
pub async fn pending_counts_entries_neither_sent_nor_failed(store: &dyn MessageStore) {
    let outbox = store.outbox();
    drain(&*outbox).await;
    let oldest = || async { outbox.oldest_pending().await.expect("oldest_pending") };
    assert_eq!(outbox.pending().await.expect("pending"), 0, "drained");
    assert_eq!(oldest().await, None, "none pending");
    let now = now_ms();
    let (e1, e2, e3) = (
        entry("conformance.Pending", now - 3_000, 1),
        entry("conformance.Pending", now - 2_000, 2),
        entry("conformance.Pending", now - 1_000, 3),
    );
    for e in [&e3, &e1, &e2] {
        outbox.append_now(e.clone()).await.expect("append_now");
    }
    assert_eq!(outbox.pending().await.expect("pending"), 3);
    assert_eq!(oldest().await, Some(now - 3_000));
    let held = outbox.claim("relay-a", LONG_LEASE, 3).await.expect("claim");
    assert_eq!(held.len(), 3);
    assert_eq!(
        outbox.pending().await.expect("pending"),
        3,
        "held is pending"
    );
    assert_eq!(oldest().await, Some(now - 3_000), "held is pending");
    outbox
        .mark_sent(std::slice::from_ref(&e1.id))
        .await
        .expect("mark_sent");
    assert_eq!(outbox.pending().await.expect("pending"), 2, "sent is not");
    assert_eq!(oldest().await, Some(now - 2_000), "sent is not");
    outbox
        .mark_failed(&e2.id, "conformance: rejected")
        .await
        .expect("mark_failed");
    assert_eq!(outbox.pending().await.expect("pending"), 1, "failed is not");
    assert_eq!(oldest().await, Some(now - 1_000), "failed is not");
    outbox
        .mark_attempt_failed(&e3.id, "conformance: retry")
        .await
        .expect("mark_attempt_failed");
    assert_eq!(
        outbox.pending().await.expect("pending"),
        1,
        "retried still is"
    );
    outbox
        .mark_sent(std::slice::from_ref(&e3.id))
        .await
        .expect("mark_sent");
    assert_eq!(outbox.pending().await.expect("pending"), 0);
    assert_eq!(oldest().await, None);
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
    assert!(
        !processed.exists(&marker.id).await.expect("exists"),
        "the marker rolled back with the handler"
    );
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        processed.record(&mut tx, &marker).await.expect("record"),
        "the rolled-back handler's marker is gone: the redelivery is recorded"
    );
    assert!(
        !processed.exists(&marker.id).await.expect("exists"),
        "not committed yet"
    );
    store.tx().commit(&mut tx).await.expect("commit");
    assert!(
        processed.exists(&marker.id).await.expect("exists"),
        "the marker committed with the handler"
    );
}

/// A marker records once: a second record of its id (in a later transaction, whatever else it
/// carries) is false, until a purge past its time removes it. A purge keeps the markers recorded
/// after its bound.
pub async fn processed_marker_records_once(store: &dyn MessageStore) {
    let processed = store.processed_events();
    let now = now_ms();
    let first = stage_one(
        &format!("conformance-{}", uuid::Uuid::new_v4()),
        TOPIC,
        3,
        42,
    );
    assert!(!processed.exists(&first.id).await.expect("exists"));
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(processed.record(&mut tx, &first).await.expect("record"));
    store.tx().commit(&mut tx).await.expect("commit");
    assert!(processed.exists(&first.id).await.expect("exists"));

    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        !processed.record(&mut tx, &first).await.expect("record"),
        "a recorded id is a duplicate"
    );
    store.tx().abort(&mut tx).await;
    let mut tx = store.tx().begin().await.expect("begin");
    let same_id = ProcessedMarker {
        id: first.id.clone(),
        type_name: "conformance.Other".into(),
        ..marker(now - 1)
    };
    assert!(
        !processed.record(&mut tx, &same_id).await.expect("record"),
        "the id alone decides"
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
    assert!(!processed.exists(&old.id).await.expect("exists"));
    assert!(processed.exists(&first.id).await.expect("exists"));

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

/// A reset deletes the stage-1 markers of its processor in its ranges (`[from, until)` of a topic's
/// partition) and nothing else: neither other offsets, partitions, topics or processors, nor
/// markers that carry no position.
pub async fn delete_markers_removes_only_the_range(store: &dyn MessageStore) {
    let processed = store.processed_events();
    let processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let other_processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let in_range: Vec<ProcessedMarker> = (3..6)
        .map(|offset| stage_one(&processor, TOPIC, 0, offset))
        .chain([stage_one(&processor, TOPIC, 2, 7)])
        .collect();
    let kept: Vec<ProcessedMarker> = (0..3)
        .map(|offset| stage_one(&processor, TOPIC, 0, offset))
        .chain([
            stage_one(&processor, TOPIC, 0, 6), // the range is [3, 6)
            stage_one(&processor, TOPIC, 1, 4),
            stage_one(&processor, "conformance.other", 0, 4),
            stage_one(&other_processor, TOPIC, 0, 4),
            ProcessedMarker {
                id: format!("saga:conformance.Order:{}", uuid::Uuid::new_v4()),
                ..marker(now_ms())
            },
            ProcessedMarker {
                id: format!("ingress:conformance:{}", uuid::Uuid::new_v4()),
                ..marker(now_ms())
            },
        ])
        .collect();
    let mut tx = store.tx().begin().await.expect("begin");
    for m in in_range.iter().chain(&kept) {
        assert!(
            processed.record(&mut tx, m).await.expect("record"),
            "recorded {}",
            m.id
        );
    }
    store.tx().commit(&mut tx).await.expect("commit");

    let range = |partition: i32, from: i64, until: i64| ReplayRange {
        source: TOPIC.into(),
        segment: partition.to_string(),
        from: from.to_string(),
        until: until.to_string(),
    };
    assert_eq!(
        processed
            .delete_markers(&processor, &[])
            .await
            .expect("delete_markers"),
        0,
        "no ranges"
    );
    assert_eq!(
        processed
            .delete_markers(&processor, &[range(0, 100, 200)])
            .await
            .expect("delete_markers"),
        0,
        "nothing there"
    );
    assert_eq!(
        processed
            .delete_markers(&processor, &[range(0, 3, 6), range(2, 7, 8)])
            .await
            .expect("delete_markers"),
        4
    );
    for m in &in_range {
        assert!(
            !processed.exists(&m.id).await.expect("exists"),
            "{} is deleted",
            m.id
        );
    }
    for m in &kept {
        assert!(
            processed.exists(&m.id).await.expect("exists"),
            "{} is kept",
            m.id
        );
    }
    assert_eq!(
        processed
            .delete_markers(&processor, &[range(0, 3, 6)])
            .await
            .expect("delete_markers"),
        0,
        "deleted once"
    );
}

/// Two relays claiming one outbox at the same time (started together, on a multi-thread
/// runtime): every entry is claimed by exactly one of them, and each claims some while the other
/// holds its first batch (a barrier after both first claims, before any entry is marked sent).
pub async fn concurrent_relays_publish_each_entry_once(store: &dyn MessageStore) {
    const ENTRIES: usize = 200;
    assert_eq!(
        tokio::runtime::Handle::current().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::MultiThread,
        "run the conformance kit on a multi-thread runtime: #[tokio::test(flavor = \"multi_thread\")]"
    );
    let outbox = store.outbox();
    drain(&*outbox).await;
    let now = now_ms();
    let mut appended = HashSet::new();
    for seq in 0..ENTRIES {
        let e = entry("conformance.Concurrent", now, seq as i64);
        appended.insert(e.id.clone());
        outbox.append_now(e).await.expect("append_now");
    }
    let start = Arc::new(Barrier::new(2));
    let claimed = Arc::new(Barrier::new(2));
    let relay = |owner: &'static str| {
        let outbox = Arc::clone(&outbox);
        let (start, claimed) = (Arc::clone(&start), Arc::clone(&claimed));
        tokio::spawn(async move {
            let mut published = Vec::new();
            // Both relays start claiming together.
            tokio::time::timeout(BARRIER_WAIT, start.wait())
                .await
                .expect("the other relay never started");
            let mut first = true;
            loop {
                let batch = outbox.claim(owner, LONG_LEASE, 7).await.expect("claim");
                if std::mem::take(&mut first) {
                    // Both first claims are made before either relay marks anything sent: each
                    // claimed while the other held its batch.
                    tokio::time::timeout(BARRIER_WAIT, claimed.wait())
                        .await
                        .expect("the other relay never made its first claim");
                }
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
    assert!(
        !a.is_empty() && !b.is_empty(),
        "both relays claimed entries: {} and {}",
        a.len(),
        b.len()
    );
    let both: Vec<&String> = a_set.intersection(&b_set).collect();
    assert!(both.is_empty(), "published by both relays: {both:?}");
    let union: HashSet<String> = a_set.union(&b_set).cloned().collect();
    assert_eq!(union, appended, "every entry is published");
}

/// A claimer that stops (crashes) holds its entries until its lease runs out; then another relay
/// claims them, in order, with the attempts counted and without the entry given up on.
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
    let lease = Duration::from_secs(3);
    let crashed = outbox.claim("crashed", lease, 10).await.expect("claim");
    assert_eq!(ids(&crashed), ids(&entries));
    outbox
        .mark_attempt_failed(&entries[0].id, "conformance: broker unavailable")
        .await
        .expect("mark_attempt_failed");
    outbox
        .mark_failed(&entries[2].id, "conformance: record too large")
        .await
        .expect("mark_failed");
    assert!(
        outbox
            .claim("survivor", LONG_LEASE, 10)
            .await
            .expect("claim")
            .is_empty(),
        "held while the lease runs"
    );
    tokio::time::sleep(lease + Duration::from_secs(1)).await;
    let reclaimed = outbox
        .claim("survivor", LONG_LEASE, 10)
        .await
        .expect("claim");
    assert_eq!(
        ids(&reclaimed),
        ids(&entries[..2]),
        "reclaimed in order after the lease, without the failed entry"
    );
    assert_eq!(reclaimed[0].attempts, 1, "the failed attempt is counted");
    assert_eq!(reclaimed[1].attempts, 0);
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
    tokio::time::sleep(Duration::from_secs(1)).await;
    let bound = now_ms();
    tokio::time::sleep(Duration::from_secs(1)).await;
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
        // Position 10: after the head's 5 by value, before it as text.
        ..letter(
            &processor,
            0,
            10,
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
        letters
            .earlier(&processor, "k", "7")
            .await
            .expect("earlier"),
        Some(head.id.clone())
    );
    assert_eq!(
        letters
            .earlier(&processor, "k", "5")
            .await
            .expect("earlier"),
        None,
        "an INVALID letter blocks nothing"
    );
    assert_eq!(
        letters
            .parked_keys(&processor, TOPIC, &["0".to_string(), "1".to_string()])
            .await
            .expect("parked_keys"),
        HashSet::from(["k".to_string(), "k2".to_string()])
    );
    assert_eq!(
        letters
            .parked_keys(&processor, TOPIC, &["1".to_string()])
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
    let summaries = letters.summaries(&processor).await.expect("summaries");
    assert_eq!(
        summaries
            .iter()
            .map(|s| (
                s.id.clone(),
                s.key.clone(),
                s.position.parse::<i64>().expect("decimal"),
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
                no_key.id.clone(),
                None,
                9,
                DeadLetterReason::HandlerFailed,
                t0 + 4
            ),
            (
                sequenced.id.clone(),
                Some("k".into()),
                10,
                DeadLetterReason::Sequenced,
                t0 + 1
            ),
        ],
        "by position (decimal ones by value), then id"
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

/// A dead letter commits on its own: parked, or its failure recorded, while a transaction is open
/// that is then rolled back, it stays.
pub async fn dead_letter_parked_inside_rolled_back_transaction_is_kept(store: &dyn MessageStore) {
    let letters = store.dead_letters();
    let processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let t0 = now_ms();
    let parked = letter(
        &processor,
        0,
        1,
        Some("k1"),
        DeadLetterReason::HandlerFailed,
        t0,
    );
    let mut tx = store.tx().begin().await.expect("begin");
    assert_eq!(letters.park(&parked).await.expect("park"), Parked::New);
    store.tx().abort(&mut tx).await;
    assert_eq!(
        letters.get(&parked.id).await.expect("get"),
        Some(parked.clone()),
        "a dead letter commits on its own"
    );
    assert_eq!(
        letters.head(&processor, "k1").await.expect("head"),
        Some(parked.id.clone())
    );

    let error = DeadLetterError::new("Boom", "failed again");
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        letters
            .record_failure(&parked.id, &error, t0 + 1_000)
            .await
            .expect("record_failure")
    );
    store.tx().abort(&mut tx).await;
    assert_eq!(
        letters.get(&parked.id).await.expect("get").map(|l| l.error),
        Some(error),
        "a retry's failure is recorded on its own too"
    );
    letters.delete(&parked.id).await.expect("delete");
}

/// A retry deletes its letter in the record's transaction: a rollback keeps the letter, a commit
/// removes it.
pub async fn dead_letter_deleted_in_a_transaction_goes_with_it(store: &dyn MessageStore) {
    let letters = store.dead_letters();
    let processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let parked = letter(
        &processor,
        0,
        7,
        Some("k1"),
        DeadLetterReason::HandlerFailed,
        now_ms(),
    );
    assert_eq!(letters.park(&parked).await.expect("park"), Parked::New);
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        letters
            .delete_in(&mut tx, &parked.id)
            .await
            .expect("delete_in")
    );
    store.tx().abort(&mut tx).await;
    assert_eq!(
        letters.get(&parked.id).await.expect("get"),
        Some(parked.clone()),
        "rolled back: the letter stays"
    );
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        letters
            .delete_in(&mut tx, &parked.id)
            .await
            .expect("delete_in")
    );
    store.tx().commit(&mut tx).await.expect("commit");
    assert_eq!(
        letters.get(&parked.id).await.expect("get"),
        None,
        "committed: gone"
    );
    let mut tx = store.tx().begin().await.expect("begin");
    assert!(
        !letters
            .delete_in(&mut tx, &parked.id)
            .await
            .expect("delete_in"),
        "no letter to delete"
    );
    store.tx().abort(&mut tx).await;
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
}

/// A pause round-trips with who paused and when, and is idempotent; a resume clears both. A pause
/// leaves the reset lock alone and the lock leaves the pause alone.
pub async fn pause_state_round_trips(store: &dyn MessageStore) {
    let control = store.processor_control();
    let processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let t0 = 1_800_000_000_000_i64;
    control
        .set_paused(&processor, true, "control-1", t0)
        .await
        .expect("pause");
    control
        .set_paused(&processor, true, "control-1", t0)
        .await
        .expect("pause again");
    let paused = control.get(&processor).await.expect("get");
    assert_eq!(
        paused,
        ProcessorControlState {
            processor: processor.clone(),
            paused: true,
            paused_at_ms: Some(t0),
            paused_by: Some("control-1".into()),
            operation: None,
            replay: Vec::new(),
            updated_at_ms: Some(t0),
        }
    );

    assert_eq!(
        control
            .lock(&processor, "op-1", t0 + 30_000, t0 + 1_000)
            .await
            .expect("lock"),
        Locked::Taken
    );
    assert!(
        control.get(&processor).await.expect("get").paused,
        "a lock leaves the pause alone"
    );
    control
        .set_paused(&processor, false, "control-2", t0 + 2_000)
        .await
        .expect("resume");
    let resumed = control.get(&processor).await.expect("get");
    assert!(!resumed.paused);
    assert_eq!((resumed.paused_at_ms, resumed.paused_by), (None, None));
    assert_eq!(resumed.updated_at_ms, Some(t0 + 2_000));
    assert_eq!(
        resumed.operation.map(|op| op.id),
        Some("op-1".to_string()),
        "a resume leaves the lock alone"
    );

    let fresh = format!("conformance-{}", uuid::Uuid::new_v4());
    control
        .set_paused(&fresh, false, PAUSED_BY_LOCAL, t0)
        .await
        .expect("resume");
    assert_eq!(
        control.get(&fresh).await.expect("get"),
        ProcessorControlState {
            processor: fresh.clone(),
            updated_at_ms: Some(t0),
            ..ProcessorControlState::default()
        },
        "a resume of a processor the store did not know: not paused"
    );
}

/// The replay ranges round-trip in order; a removal takes exactly that range of that processor,
/// once; a replacement replaces them all. The ranges leave the lock alone.
pub async fn replay_ranges_round_trip(store: &dyn MessageStore) {
    let control = store.processor_control();
    let processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let other_processor = format!("conformance-{}", uuid::Uuid::new_v4());
    let t0 = 1_800_000_000_000_i64;
    let r1 = ReplayRange {
        source: TOPIC.into(),
        segment: "0".into(),
        from: "10".into(),
        until: "20".into(),
    };
    let r2 = ReplayRange {
        source: TOPIC.into(),
        segment: "1".into(),
        from: "0".into(),
        until: "5".into(),
    };
    let r3 = ReplayRange {
        source: "conformance.other".into(),
        segment: "0".into(),
        from: "10".into(),
        until: "20".into(),
    };
    assert_eq!(
        control
            .lock(&processor, "op-1", t0 + 30_000, t0)
            .await
            .expect("lock"),
        Locked::Taken
    );
    control
        .set_replay(
            &processor,
            &[r1.clone(), r2.clone(), r3.clone()],
            t0 + 1_000,
        )
        .await
        .expect("set_replay");
    let state = control.get(&processor).await.expect("get");
    assert_eq!(
        state.replay,
        vec![r1.clone(), r2.clone(), r3.clone()],
        "the ranges, in order"
    );
    assert_eq!(state.updated_at_ms, Some(t0 + 1_000));
    assert_eq!(
        state.operation.map(|op| op.id),
        Some("op-1".to_string()),
        "the lock is untouched"
    );

    assert!(
        control
            .remove_range(&processor, &r2, t0 + 2_000)
            .await
            .expect("remove_range")
    );
    assert!(
        !control
            .remove_range(&processor, &r2, t0 + 2_000)
            .await
            .expect("remove_range"),
        "removed once"
    );
    assert!(
        !control
            .remove_range(
                &processor,
                &ReplayRange {
                    until: "21".into(),
                    ..r1.clone()
                },
                t0 + 2_000
            )
            .await
            .expect("remove_range"),
        "exactly that range"
    );
    assert!(
        !control
            .remove_range(&other_processor, &r1, t0 + 2_000)
            .await
            .expect("remove_range"),
        "another processor's"
    );
    assert_eq!(
        control.get(&processor).await.expect("get").replay,
        vec![r1, r3]
    );

    control
        .set_replay(&processor, std::slice::from_ref(&r2), t0 + 3_000)
        .await
        .expect("set_replay");
    assert_eq!(
        control.get(&processor).await.expect("get").replay,
        vec![r2],
        "set_replay replaces"
    );
    control
        .set_replay(&processor, &[], t0 + 4_000)
        .await
        .expect("set_replay");
    assert!(
        control
            .get(&processor)
            .await
            .expect("get")
            .replay
            .is_empty()
    );
}

/// A processor-control write commits on its own: made while a transaction is open that is then
/// rolled back, it stays.
pub async fn processor_control_write_inside_rolled_back_transaction_is_kept(
    store: &dyn MessageStore,
) {
    let control = store.processor_control();
    let paused = format!("conformance-{}", uuid::Uuid::new_v4());
    let locked = format!("conformance-{}", uuid::Uuid::new_v4());
    let t0 = 1_800_000_000_000_i64;
    let range = ReplayRange {
        source: TOPIC.into(),
        segment: "0".into(),
        from: "1".into(),
        until: "2".into(),
    };
    let mut tx = store.tx().begin().await.expect("begin");
    control
        .set_paused(&paused, true, "control-1", t0)
        .await
        .expect("pause");
    assert_eq!(
        control
            .lock(&locked, "op-1", t0 + 30_000, t0)
            .await
            .expect("lock"),
        Locked::Taken
    );
    control
        .set_replay(&locked, std::slice::from_ref(&range), t0)
        .await
        .expect("set_replay");
    store.tx().abort(&mut tx).await;

    assert!(
        control.get(&paused).await.expect("get").paused,
        "a pause commits on its own"
    );
    let state = control.get(&locked).await.expect("get");
    assert_eq!(
        state.operation.map(|op| op.id),
        Some("op-1".to_string()),
        "a lock commits on its own"
    );
    assert_eq!(
        state.replay,
        vec![range],
        "a replay range commits on its own"
    );
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
    let now = now_ms();
    let type_name = format!("conformance.Saga{}", uuid::Uuid::new_v4().simple());
    let saga = SagaDocument {
        id: uuid::Uuid::new_v4().to_string(),
        type_name: type_name.clone(),
        state: "{\"step\":1}".into(),
        associations: vec![Association::new("orderId", "o-1")],
        start_key: Some(format!("{type_name}|orderId=o-1")),
        ended: false,
        ended_at_ms: None,
        version: 0,
        created_at_ms: now,
        updated_at_ms: now,
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
    assert_eq!(
        sagas
            .sagas
            .count_live_by_type()
            .await
            .expect("count_live_by_type")
            .get(&type_name),
        Some(&1),
        "one live instance of the type"
    );
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
        // The saga runtime keeps the cause's correlation id and traceparent with an entry.
        headers: HashMap::from([
            ("correlation-id".to_string(), "c-1".to_string()),
            (
                "traceparent".to_string(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            ),
        ]),
        routing_key: Some("o-1".into()),
        ..dispatch_entry(&saga.id, 1000, DispatchKind::Command, now)
    };
    let deadline = |name: &str| DeadlineDocument {
        id: uuid::Uuid::new_v4().to_string(),
        saga_type: type_name.clone(),
        saga_id: saga.id.clone(),
        name: name.to_string(),
        payload: Some("{\"why\":\"timeout\"}".into()),
        payload_type: Some("conformance.Timeout".into()),
        due_at_ms: now + 60_000,
        status: DeadlineStatus::Scheduled,
        claimed_by: None,
        claimed_until_ms: None,
        finished_at_ms: None,
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
    assert_eq!(
        sagas
            .dispatch
            .next_pending(&saga.id)
            .await
            .expect("next_pending"),
        Some(dispatch.clone()),
        "the recorded entry reads back"
    );

    let later = now + 1_000;
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
        first_failed_at_ms: now,
        last_failed_at_ms: now,
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
        (
            again.attempts,
            again.error.as_str(),
            again.last_failed_at_ms
        ),
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

    // An ended instance is no longer live (it keeps its end time).
    let ended = SagaDocument {
        ended: true,
        ended_at_ms: Some(later),
        version: 2,
        updated_at_ms: later,
        ..updated.clone()
    };
    let mut tx = txs.begin().await.expect("begin");
    assert!(
        sagas
            .sagas
            .update(&mut tx, &ended, 1)
            .await
            .expect("update")
    );
    txs.commit(&mut tx).await.expect("commit");
    let mut tx = txs.plain().await.expect("plain");
    assert_eq!(
        sagas
            .sagas
            .find_by_id(&mut tx, &saga.id)
            .await
            .expect("find"),
        Some(ended)
    );
    assert_eq!(
        sagas
            .sagas
            .count_live_by_type()
            .await
            .expect("count_live_by_type")
            .get(&type_name),
        None,
        "no live instance of the type"
    );
    let mut tx = txs.begin().await.expect("begin");
    sagas
        .dispatch
        .mark_dispatched(&mut tx, &dispatch.id, later)
        .await
        .expect("mark_dispatched");
    txs.commit(&mut tx).await.expect("commit");
}

/// Two live transactions each start a saga (two ids) holding one start key and both try to
/// commit: exactly one commits, the other conflicts (at its insert or at its commit), and one live
/// saga holds the key.
pub async fn two_transactions_starting_one_saga_key_leave_one_live_saga(store: &dyn MessageStore) {
    let sagas = store.sagas().sagas;
    let txs = store.tx();
    let type_name = format!("conformance.Race{}", uuid::Uuid::new_v4().simple());
    let start_key = format!("{type_name}|orderId=o-1");
    let saga = |id: &str| SagaDocument {
        id: id.to_string(),
        type_name: type_name.clone(),
        state: format!("{{\"by\":\"{id}\"}}"),
        associations: vec![Association::new("orderId", "o-1")],
        start_key: Some(start_key.clone()),
        ended: false,
        ended_at_ms: None,
        version: 0,
        created_at_ms: now_ms(),
        updated_at_ms: now_ms(),
    };
    let ids = [
        format!("race-{}", uuid::Uuid::new_v4()),
        format!("race-{}", uuid::Uuid::new_v4()),
    ];
    let mut first = txs.begin().await.expect("begin");
    let mut second = txs.begin().await.expect("begin");
    let inserted = [
        sagas.insert(&mut first, &saga(&ids[0])).await,
        sagas.insert(&mut second, &saga(&ids[1])).await,
    ];
    let mut outcomes = Vec::new();
    for (tx, insert) in [
        (&mut first, inserted[0].as_ref()),
        (&mut second, inserted[1].as_ref()),
    ] {
        outcomes.push(match insert {
            Ok(()) => txs.commit(tx).await,
            Err(e) => {
                assert!(e.is_conflict(), "the losing insert conflicts: {e}");
                txs.abort(tx).await;
                Err(StoreError::Conflict("at the insert".into()))
            }
        });
    }
    let won: Vec<usize> = (0..2).filter(|i| outcomes[*i].is_ok()).collect();
    assert_eq!(won.len(), 1, "exactly one commits: {outcomes:?}");
    let lost = &outcomes[1 - won[0]];
    assert!(
        lost.as_ref().is_err_and(StoreError::is_conflict),
        "the other conflicts: {lost:?}"
    );
    let mut tx = txs.plain().await.expect("plain");
    let live = sagas
        .find_live(&mut tx, &type_name, "orderId", "o-1")
        .await
        .expect("find_live");
    assert_eq!(
        live.iter().map(|s| s.id.clone()).collect::<Vec<_>>(),
        vec![ids[won[0]].clone()],
        "one live saga holds the key, the winner's"
    );
}

/// Two live transactions update one saga from one version and both try to commit: exactly one
/// commits, the other conflicts (at its update or at its commit), and the stored saga is the
/// winner's.
pub async fn two_transactions_updating_one_saga_version_commit_once(store: &dyn MessageStore) {
    let sagas = store.sagas().sagas;
    let txs = store.tx();
    let type_name = format!("conformance.Race{}", uuid::Uuid::new_v4().simple());
    let saga = SagaDocument {
        id: format!("race-{}", uuid::Uuid::new_v4()),
        type_name,
        state: "{\"step\":0}".into(),
        associations: vec![Association::new("orderId", "o-1")],
        start_key: None,
        ended: false,
        ended_at_ms: None,
        version: 0,
        created_at_ms: now_ms(),
        updated_at_ms: now_ms(),
    };
    let mut tx = txs.begin().await.expect("begin");
    sagas.insert(&mut tx, &saga).await.expect("insert");
    txs.commit(&mut tx).await.expect("commit");

    let next = |step: &str| SagaDocument {
        state: format!("{{\"step\":\"{step}\"}}"),
        version: 1,
        ..saga.clone()
    };
    let (a, b) = (next("a"), next("b"));
    let mut first = txs.begin().await.expect("begin");
    let mut second = txs.begin().await.expect("begin");
    let updated = [
        sagas.update(&mut first, &a, 0).await,
        sagas.update(&mut second, &b, 0).await,
    ];
    let mut outcomes = Vec::new();
    for (tx, update) in [(&mut first, &updated[0]), (&mut second, &updated[1])] {
        outcomes.push(match update {
            Ok(true) => txs.commit(tx).await,
            Ok(false) => panic!("an update at the version both read is not refused by the check"),
            Err(e) => {
                assert!(e.is_conflict(), "the losing update conflicts: {e}");
                txs.abort(tx).await;
                Err(StoreError::Conflict("at the update".into()))
            }
        });
    }
    let won: Vec<usize> = (0..2).filter(|i| outcomes[*i].is_ok()).collect();
    assert_eq!(won.len(), 1, "exactly one commits: {outcomes:?}");
    let lost = &outcomes[1 - won[0]];
    assert!(
        lost.as_ref().is_err_and(StoreError::is_conflict),
        "the other conflicts: {lost:?}"
    );
    let mut tx = txs.plain().await.expect("plain");
    let stored = sagas
        .find_by_id(&mut tx, &saga.id)
        .await
        .expect("find_by_id")
        .expect("the saga");
    assert_eq!(stored, [a, b][won[0]].clone(), "the winner's");
}

/// `SagaDispatchStore::record` refuses an EVENT entry whose payload the outbox would refuse (not a
/// JSON object, an integer above `i64::MAX`) with `InvalidMessage`, inside the saga's transaction:
/// nothing of the attempt commits. A COMMAND entry's payload is not the outbox's.
pub async fn saga_dispatch_store_refuses_an_event_the_outbox_would_refuse(
    store: &dyn MessageStore,
) {
    let sagas = store.sagas();
    let txs = store.tx();
    let now = now_ms();
    let too_deep = nested(MAX_PAYLOAD_DEPTH + 1);
    for (n, payload) in [
        "[1]",
        "null",
        "{\"n\":18446744073709551615}",
        "{\"n\":-9223372036854775809}",
        "{\"n\":123456789012345678901234567890}",
        "{\"n\":1e400}",
        "{\"s\":\"\\ud800\"}",
        too_deep.as_str(),
    ]
    .into_iter()
    .enumerate()
    {
        let saga_id = uuid::Uuid::new_v4().to_string();
        let fine = dispatch_entry(&saga_id, 1000, DispatchKind::Event, now);
        let refused = SagaDispatchDocument {
            payload: payload.to_string(),
            ..dispatch_entry(&saga_id, 1001, DispatchKind::Event, now)
        };
        let mut tx = txs.begin().await.expect("begin");
        let recorded = sagas.dispatch.record(&mut tx, &[fine, refused]).await;
        txs.abort(&mut tx).await;
        assert!(
            matches!(
                &recorded,
                Err(StoreError::Failed(MessagingError::InvalidMessage(_)))
            ),
            "{n}: {payload}: {recorded:?}"
        );
        assert_eq!(
            sagas
                .dispatch
                .next_pending(&saga_id)
                .await
                .expect("next_pending"),
            None,
            "{payload}: nothing recorded"
        );
    }
    // A command's payload is the command's, whatever its JSON.
    let saga_id = uuid::Uuid::new_v4().to_string();
    let command = SagaDispatchDocument {
        payload: "null".into(),
        ..dispatch_entry(&saga_id, 1000, DispatchKind::Command, now)
    };
    let mut tx = txs.begin().await.expect("begin");
    sagas
        .dispatch
        .record(&mut tx, std::slice::from_ref(&command))
        .await
        .expect("a command records");
    txs.commit(&mut tx).await.expect("commit");
    let mut tx = txs.plain().await.expect("plain");
    sagas
        .dispatch
        .mark_dispatched(&mut tx, &command.id, now)
        .await
        .expect("mark_dispatched");
}

/// The deadline scheduler's port: the due deadline is claimed by one scheduler at a time, the
/// earliest first, until its claim passes; one not yet due, fired or cancelled never is. The
/// overdue count counts the due SCHEDULED deadlines per name, claimed or not. Times are the
/// caller's.
pub async fn due_deadline_is_claimed_once_until_its_lease_passes(store: &dyn MessageStore) {
    let sagas = store.sagas();
    let txs = store.tx();
    let now = now_ms();
    let saga_id = uuid::Uuid::new_v4().to_string();
    let first_name = format!("first-{}", uuid::Uuid::new_v4().simple());
    let second_name = format!("second-{}", uuid::Uuid::new_v4().simple());
    let deadline = |name: &str, due_at_ms: i64| DeadlineDocument {
        id: uuid::Uuid::new_v4().to_string(),
        saga_type: "conformance.Deadlines".into(),
        saga_id: saga_id.clone(),
        name: name.to_string(),
        payload: None,
        payload_type: None,
        due_at_ms,
        status: DeadlineStatus::Scheduled,
        claimed_by: None,
        claimed_until_ms: None,
        finished_at_ms: None,
        correlation_id: None,
    };
    let first = deadline(&first_name, now - 2_000);
    let second = deadline(&second_name, now - 1_000);
    let later = deadline(&first_name, now + 60_000);
    let mut tx = txs.begin().await.expect("begin");
    for d in [&second, &later, &first] {
        sagas.deadlines.insert(&mut tx, d).await.expect("insert");
    }
    txs.commit(&mut tx).await.expect("commit");
    let overdue = sagas
        .deadlines
        .count_overdue_by_name(now)
        .await
        .expect("count_overdue_by_name");
    assert_eq!(
        (overdue.get(&first_name), overdue.get(&second_name)),
        (Some(&1), Some(&1)),
        "one due deadline of each name (the later one is not due)"
    );

    let until = now + 30_000;
    let deadlines = &*sagas.deadlines;
    let a = claim_mine(deadlines, "scheduler-a", now, until, &saga_id).await;
    assert_eq!(
        a,
        Some(DeadlineDocument {
            claimed_by: Some("scheduler-a".into()),
            claimed_until_ms: Some(until),
            ..first.clone()
        }),
        "the earliest due deadline, claimed"
    );
    let b = claim_mine(deadlines, "scheduler-b", now, until, &saga_id).await;
    assert_eq!(b.map(|d| d.id), Some(second.id.clone()), "the next one");
    assert_eq!(
        claim_mine(deadlines, "scheduler-a", now, until, &saga_id).await,
        None,
        "both due deadlines are held, the later one is not due"
    );
    let overdue = sagas
        .deadlines
        .count_overdue_by_name(now)
        .await
        .expect("count_overdue_by_name");
    assert_eq!(
        (overdue.get(&first_name), overdue.get(&second_name)),
        (Some(&1), Some(&1)),
        "claimed deadlines are still overdue"
    );

    // Past the claims, another scheduler takes them again, the earliest first.
    let past = until + 1;
    let again = claim_mine(deadlines, "scheduler-b", past, past + 30_000, &saga_id).await;
    assert_eq!(
        again.map(|d| d.id),
        Some(first.id.clone()),
        "a passed claim is claimed again"
    );
    let mut tx = txs.begin().await.expect("begin");
    assert!(
        sagas
            .deadlines
            .mark_fired(&mut tx, &first.id, past)
            .await
            .expect("mark_fired")
    );
    sagas
        .deadlines
        .cancel(&mut tx, &saga_id, &second.id, past)
        .await
        .expect("cancel");
    txs.commit(&mut tx).await.expect("commit");
    let far = now + 3_600_000;
    assert_eq!(
        claim_mine(deadlines, "scheduler-a", far, far + 30_000, &saga_id)
            .await
            .map(|d| d.id),
        Some(later.id.clone()),
        "a fired or cancelled deadline is never claimed; the later one is, once due"
    );
    let overdue = sagas
        .deadlines
        .count_overdue_by_name(far)
        .await
        .expect("count_overdue_by_name");
    assert_eq!(
        (overdue.get(&first_name), overdue.get(&second_name)),
        (Some(&1), None),
        "only the later one is still SCHEDULED"
    );
    let mut tx = txs.begin().await.expect("begin");
    sagas
        .deadlines
        .cancel_all(&mut tx, &saga_id, far)
        .await
        .expect("cancel_all");
    txs.commit(&mut tx).await.expect("commit");
}

/// A deadline written with an empty correlation id reads back without one (as with none), from
/// `find_scheduled` and from `claim_due`: every store keeps the two the same, as the SQL schema
/// (a non-null column, empty for none) must.
pub async fn an_empty_deadline_correlation_id_reads_as_absent(store: &dyn MessageStore) {
    let sagas = store.sagas();
    let txs = store.tx();
    let now = now_ms();
    let saga_id = uuid::Uuid::new_v4().to_string();
    let deadline = DeadlineDocument {
        id: uuid::Uuid::new_v4().to_string(),
        saga_type: "conformance.Deadlines".into(),
        saga_id: saga_id.clone(),
        name: format!("empty-{}", uuid::Uuid::new_v4().simple()),
        payload: None,
        payload_type: None,
        due_at_ms: now - 1_000,
        status: DeadlineStatus::Scheduled,
        claimed_by: None,
        claimed_until_ms: None,
        finished_at_ms: None,
        correlation_id: Some(String::new()),
    };
    let mut tx = txs.begin().await.expect("begin");
    sagas
        .deadlines
        .insert(&mut tx, &deadline)
        .await
        .expect("insert");
    txs.commit(&mut tx).await.expect("commit");
    let mut tx = txs.begin().await.expect("begin");
    let found = sagas
        .deadlines
        .find_scheduled(&mut tx, &deadline.id)
        .await
        .expect("find_scheduled")
        .expect("the deadline is scheduled");
    txs.commit(&mut tx).await.expect("commit");
    assert_eq!(found.correlation_id, None, "find_scheduled: {found:?}");
    let claimed = claim_mine(
        &*sagas.deadlines,
        "scheduler-empty",
        now,
        now + 30_000,
        &saga_id,
    )
    .await
    .expect("the deadline is due");
    assert_eq!(claimed.correlation_id, None, "claim_due: {claimed:?}");
    let mut tx = txs.begin().await.expect("begin");
    sagas
        .deadlines
        .cancel_all(&mut tx, &saga_id, now)
        .await
        .expect("cancel_all");
    txs.commit(&mut tx).await.expect("commit");
}

/// Two schedulers claiming at the same time (started together, on a multi-thread runtime): every
/// due deadline is claimed by exactly one of them, and each claims some while the other holds its
/// first claim.
pub async fn two_schedulers_claim_each_due_deadline_once(store: &dyn MessageStore) {
    const DEADLINES: usize = 40;
    let sagas = store.sagas();
    let txs = store.tx();
    let now = now_ms();
    let saga_id = uuid::Uuid::new_v4().to_string();
    let mut mine = HashSet::new();
    let mut tx = txs.begin().await.expect("begin");
    for n in 0..DEADLINES {
        let d = DeadlineDocument {
            id: uuid::Uuid::new_v4().to_string(),
            saga_type: "conformance.Raced".into(),
            saga_id: saga_id.clone(),
            name: "raced".into(),
            payload: None,
            payload_type: None,
            due_at_ms: now - 10_000 + n as i64,
            status: DeadlineStatus::Scheduled,
            claimed_by: None,
            claimed_until_ms: None,
            finished_at_ms: None,
            correlation_id: None,
        };
        mine.insert(d.id.clone());
        sagas.deadlines.insert(&mut tx, &d).await.expect("insert");
    }
    txs.commit(&mut tx).await.expect("commit");

    let start = Arc::new(Barrier::new(2));
    let claimed = Arc::new(Barrier::new(2));
    let scheduler = |owner: &'static str| {
        let deadlines = Arc::clone(&sagas.deadlines);
        let (start, claimed, saga_id) = (Arc::clone(&start), Arc::clone(&claimed), saga_id.clone());
        tokio::spawn(async move {
            let mut got = Vec::new();
            tokio::time::timeout(BARRIER_WAIT, start.wait())
                .await
                .expect("the other scheduler never started");
            let mut first = true;
            loop {
                let next = deadlines
                    .claim_due(owner, now, now + 60_000)
                    .await
                    .expect("claim_due");
                if std::mem::take(&mut first) {
                    tokio::time::timeout(BARRIER_WAIT, claimed.wait())
                        .await
                        .expect("the other scheduler never made its first claim");
                }
                match next {
                    Some(d) if d.saga_id == saga_id => got.push(d.id),
                    Some(_) => {}
                    None => return got,
                }
                tokio::task::yield_now().await;
            }
        })
    };
    let (a, b) = tokio::join!(scheduler("scheduler-a"), scheduler("scheduler-b"));
    let (a, b) = (a.expect("scheduler-a"), b.expect("scheduler-b"));
    let a_set: HashSet<String> = a.iter().cloned().collect();
    let b_set: HashSet<String> = b.iter().cloned().collect();
    assert_eq!(a_set.len(), a.len(), "scheduler-a claimed a deadline twice");
    assert_eq!(b_set.len(), b.len(), "scheduler-b claimed a deadline twice");
    assert!(
        !a.is_empty() && !b.is_empty(),
        "both schedulers claimed deadlines: {} and {}",
        a.len(),
        b.len()
    );
    let both: Vec<&String> = a_set.intersection(&b_set).collect();
    assert!(both.is_empty(), "claimed by both: {both:?}");
    let union: HashSet<String> = a_set.union(&b_set).cloned().collect();
    assert_eq!(union, mine, "every due deadline is claimed");
    let mut tx = txs.begin().await.expect("begin");
    sagas
        .deadlines
        .cancel_all(&mut tx, &saga_id, now)
        .await
        .expect("cancel_all");
    txs.commit(&mut tx).await.expect("commit");
}

/// Two relays claiming the dispatch leases of the same sagas at the same time (started
/// together, on a multi-thread runtime): each saga's lease goes to exactly one of them.
pub async fn two_relays_never_hold_one_saga_lease(store: &dyn MessageStore) {
    const SAGAS: usize = 40;
    let queue = store.sagas().dispatch;
    let now = now_ms();
    let ids: Arc<Vec<String>> = Arc::new(
        (0..SAGAS)
            .map(|_| uuid::Uuid::new_v4().to_string())
            .collect(),
    );
    let start = Arc::new(Barrier::new(2));
    let relay = |owner: &'static str| {
        let (queue, start, ids) = (Arc::clone(&queue), Arc::clone(&start), Arc::clone(&ids));
        tokio::spawn(async move {
            tokio::time::timeout(BARRIER_WAIT, start.wait())
                .await
                .expect("the other relay never started");
            let mut held = HashSet::new();
            for id in ids.iter() {
                if queue
                    .claim_lease(id, owner, now, now + 60_000)
                    .await
                    .expect("claim_lease")
                {
                    held.insert(id.clone());
                }
                tokio::task::yield_now().await;
            }
            held
        })
    };
    let (a, b) = tokio::join!(relay("relay-a"), relay("relay-b"));
    let (a, b) = (a.expect("relay-a"), b.expect("relay-b"));
    let both: Vec<&String> = a.intersection(&b).collect();
    assert!(both.is_empty(), "held by both relays: {both:?}");
    let all: HashSet<String> = ids.iter().cloned().collect();
    let union: HashSet<String> = a.union(&b).cloned().collect();
    assert_eq!(union, all, "every lease has a holder");
    for id in ids.iter() {
        let owner = if a.contains(id) { "relay-a" } else { "relay-b" };
        queue.release_lease(id, owner).await.expect("release_lease");
    }
}

/// Two relays claiming due failure notices at the same time (started together, on a
/// multi-thread runtime), each until none is left: every notice is claimed by exactly one of them,
/// once.
pub async fn concurrent_notice_claims_return_each_notice_once(store: &dyn MessageStore) {
    const NOTICES: usize = 40;
    let queue = store.sagas().dispatch;
    let txs = store.tx();
    let now = now_ms();
    let entries: Vec<SagaDispatchDocument> = (0..NOTICES)
        .map(|_| {
            dispatch_entry(
                &uuid::Uuid::new_v4().to_string(),
                1000,
                DispatchKind::Command,
                now - 1_000,
            )
        })
        .collect();
    let mut tx = txs.begin().await.expect("begin");
    queue.record(&mut tx, &entries).await.expect("record");
    txs.commit(&mut tx).await.expect("commit");
    for e in &entries {
        queue
            .mark_failed(
                &e.id,
                &DispatchFailure {
                    attempts: Some(1),
                    reason: FailureReason::NoHandler,
                    message: "nobody".into(),
                    failed_at_ms: now,
                    notice_at_ms: Some(now),
                },
            )
            .await
            .expect("mark_failed");
    }
    let mine: HashSet<String> = entries.iter().map(|e| e.id.clone()).collect();

    let start = Arc::new(Barrier::new(2));
    let relay = || {
        let (queue, start, mine) = (Arc::clone(&queue), Arc::clone(&start), mine.clone());
        tokio::spawn(async move {
            tokio::time::timeout(BARRIER_WAIT, start.wait())
                .await
                .expect("the other relay never started");
            let mut got = Vec::new();
            // Another check's notices claimed on the way stay reserved.
            while let Some(e) = queue
                .claim_due_notice(now, now + 60_000)
                .await
                .expect("claim_due_notice")
            {
                if mine.contains(&e.id) {
                    got.push(e.id);
                }
                tokio::task::yield_now().await;
            }
            got
        })
    };
    let (a, b) = tokio::join!(relay(), relay());
    let (a, b) = (a.expect("relay-a"), b.expect("relay-b"));
    let claimed: HashSet<String> = a.iter().chain(&b).cloned().collect();
    assert_eq!(claimed, mine, "every due notice was claimed");
    assert_eq!(
        a.len() + b.len(),
        NOTICES,
        "a notice was claimed twice: {a:?} and {b:?}"
    );
    for id in &mine {
        queue.clear_notice(id).await.expect("clear_notice");
    }
}

/// The dispatch relay's port: a saga's pending entries come in `seq` order, the sagas whose first
/// pending entry is due oldest first; a saga's lease is held by one relay until it passes; an
/// entry dispatched in a rolled-back transaction stays pending; a retried entry waits for its next
/// attempt; a failed one carries its outcome and its failure notice, which is claimed by one
/// relay at a time until it is cleared. Every write reads back.
pub async fn dispatch_hands_out_each_saga_in_seq_order(store: &dyn MessageStore) {
    dispatch_hands_out_each_saga_in_seq_order_on(&*store.sagas().dispatch, &*store.tx()).await;
}

/// [`dispatch_hands_out_each_saga_in_seq_order`] on a dispatch port and its transaction, for the
/// saga ports outside a [`MessageStore`] (the in-memory ports of `saga::testing`).
pub async fn dispatch_hands_out_each_saga_in_seq_order_on(
    queue: &dyn SagaDispatchStore,
    txs: &dyn TxProvider,
) {
    let now = now_ms();
    let pending_before = queue.pending().await.expect("pending");
    let older = uuid::Uuid::new_v4().to_string();
    let newer = uuid::Uuid::new_v4().to_string();
    let command = SagaDispatchDocument {
        routing_key: Some("o-1".into()),
        ..dispatch_entry(&newer, 1000, DispatchKind::Command, now - 1_000)
    };
    let event = dispatch_entry(&newer, 1001, DispatchKind::Event, now - 1_000);
    let old = dispatch_entry(&older, 0, DispatchKind::Event, now - 5_000);
    let mut tx = txs.begin().await.expect("begin");
    queue
        .record(&mut tx, &[event.clone(), command.clone()])
        .await
        .expect("record");
    queue
        .record(&mut tx, std::slice::from_ref(&old))
        .await
        .expect("record");
    txs.commit(&mut tx).await.expect("commit");
    assert_eq!(queue.pending().await.expect("pending"), pending_before + 3);

    let due = queue.due_sagas(now, &[], 10_000).await.expect("due_sagas");
    let at = |id: &str| due.iter().position(|s| s == id);
    assert!(
        matches!((at(&older), at(&newer)), (Some(o), Some(n)) if o < n),
        "both sagas are due, the older work first: {due:?}"
    );
    assert!(
        !queue
            .due_sagas(now, std::slice::from_ref(&older), 10_000)
            .await
            .expect("due_sagas")
            .contains(&older),
        "a saga in flight is left out"
    );
    assert_eq!(
        queue.next_pending(&newer).await.expect("next_pending"),
        Some(command.clone()),
        "the lowest seq first, as recorded"
    );

    // The saga's lease.
    let lease = now + 60_000;
    assert!(
        queue
            .claim_lease(&newer, "relay-a", now, lease)
            .await
            .expect("claim_lease")
    );
    assert!(
        !queue
            .claim_lease(&newer, "relay-b", now, lease)
            .await
            .expect("claim_lease"),
        "held by relay-a"
    );
    assert!(
        !queue
            .renew_lease(&newer, "relay-b", lease)
            .await
            .expect("renew_lease"),
        "only the holder renews"
    );
    assert!(
        queue
            .renew_lease(&newer, "relay-a", now + 120_000)
            .await
            .expect("renew_lease")
    );
    assert!(
        !queue
            .claim_lease(&newer, "relay-b", now + 61_000, now + 121_000)
            .await
            .expect("claim_lease"),
        "the renewed lease still runs"
    );
    queue
        .release_lease(&newer, "relay-b")
        .await
        .expect("release_lease");
    assert!(
        !queue
            .claim_lease(&newer, "relay-b", now, lease)
            .await
            .expect("claim_lease"),
        "only the holder releases"
    );
    queue
        .release_lease(&newer, "relay-a")
        .await
        .expect("release_lease");
    assert!(
        queue
            .claim_lease(&newer, "relay-b", now, lease)
            .await
            .expect("claim_lease"),
        "a released lease is free"
    );
    queue
        .release_lease(&newer, "relay-b")
        .await
        .expect("release_lease");
    assert!(
        queue
            .claim_lease(&older, "relay-a", now, now + 1_000)
            .await
            .expect("claim_lease")
    );
    assert!(
        queue
            .claim_lease(&older, "relay-b", now + 2_000, now + 5_000)
            .await
            .expect("claim_lease"),
        "a passed lease is free"
    );
    queue
        .release_lease(&older, "relay-b")
        .await
        .expect("release_lease");

    // Dispatched: in a transaction, rolled back or committed.
    let mut tx = txs.begin().await.expect("begin");
    queue
        .mark_dispatched(&mut tx, &command.id, now)
        .await
        .expect("mark_dispatched");
    txs.abort(&mut tx).await;
    assert_eq!(
        queue.next_pending(&newer).await.expect("next_pending"),
        Some(command.clone()),
        "rolled back: still pending"
    );
    let mut tx = txs.begin().await.expect("begin");
    queue
        .mark_dispatched(&mut tx, &command.id, now)
        .await
        .expect("mark_dispatched");
    txs.commit(&mut tx).await.expect("commit");
    assert_eq!(
        queue.next_pending(&newer).await.expect("next_pending"),
        Some(event.clone()),
        "the next entry of the saga"
    );

    // A retry later: the saga waits until then.
    queue
        .schedule_retry(&event.id, 1, now + 60_000)
        .await
        .expect("schedule_retry");
    assert_eq!(
        queue.next_pending(&newer).await.expect("next_pending"),
        Some(SagaDispatchDocument {
            attempts: 1,
            next_attempt_at_ms: Some(now + 60_000),
            ..event.clone()
        })
    );
    assert!(
        !queue
            .due_sagas(now, &[], 10_000)
            .await
            .expect("due_sagas")
            .contains(&newer),
        "not due before its next attempt"
    );
    assert!(
        queue
            .due_sagas(now + 60_000, &[], 10_000)
            .await
            .expect("due_sagas")
            .contains(&newer),
        "due at its next attempt"
    );

    // Given up, with a notice due at now + 30 s.
    let failure = DispatchFailure {
        attempts: Some(2),
        reason: FailureReason::Exhausted,
        message: "gave up".into(),
        failed_at_ms: now,
        notice_at_ms: Some(now + 30_000),
    };
    queue
        .mark_failed(&event.id, &failure)
        .await
        .expect("mark_failed");
    assert_eq!(
        queue.next_pending(&newer).await.expect("next_pending"),
        None
    );
    assert_eq!(
        claim_notice(queue, now, now + 90_000, &event.id).await,
        None,
        "the notice is not due yet"
    );
    let failed = SagaDispatchDocument {
        attempts: 2,
        next_attempt_at_ms: Some(now + 60_000),
        failed_at_ms: Some(now),
        failure_reason: Some(FailureReason::Exhausted),
        failure_message: Some("gave up".into()),
        notice_pending: Some(true),
        notice_attempts: Some(0),
        notice_next_at_ms: Some(now + 90_000),
        ..event.clone()
    };
    assert_eq!(
        claim_notice(queue, now + 30_000, now + 90_000, &event.id).await,
        Some(failed.clone()),
        "the due notice, reserved until now + 90 s"
    );
    assert_eq!(
        claim_notice(queue, now + 30_000, now + 90_000, &event.id).await,
        None,
        "a reserved notice is claimed once"
    );
    queue
        .schedule_notice_retry(&event.id, 1, now + 100_000)
        .await
        .expect("schedule_notice_retry");
    assert_eq!(
        claim_notice(queue, now + 100_000, now + 160_000, &event.id).await,
        Some(SagaDispatchDocument {
            notice_attempts: Some(1),
            notice_next_at_ms: Some(now + 160_000),
            ..failed.clone()
        }),
        "a failed delivery is tried again at its next time"
    );
    queue.clear_notice(&event.id).await.expect("clear_notice");
    assert_eq!(
        claim_notice(queue, now + 3_600_000, now + 3_660_000, &event.id).await,
        None,
        "a cleared notice is never claimed"
    );

    // An event given up on has no notice.
    queue
        .mark_failed(
            &old.id,
            &DispatchFailure {
                attempts: None,
                reason: FailureReason::Invalid,
                message: "not a JSON object".into(),
                failed_at_ms: now,
                notice_at_ms: None,
            },
        )
        .await
        .expect("mark_failed");
    assert_eq!(
        queue.next_pending(&older).await.expect("next_pending"),
        None
    );
    assert_eq!(
        claim_notice(queue, now + 3_600_000, now + 3_660_000, &old.id).await,
        None,
        "no notice for an event"
    );
    assert_eq!(
        queue.pending().await.expect("pending"),
        pending_before,
        "neither dispatched nor failed: none left"
    );
}

// ---- helpers -----------------------------------------------------------------------------------

const TOPIC: &str = "kraken.events";
/// `paused_by` of a pause made on the instance itself.
const PAUSED_BY_LOCAL: &str = "local";

/// [`DeadlineStore::claim_due`] until it hands out a deadline of `saga_id` (another check's
/// deadlines it claims on the way are left claimed) or nothing.
async fn claim_mine(
    deadlines: &dyn DeadlineStore,
    owner: &str,
    now_ms: i64,
    until_ms: i64,
    saga_id: &str,
) -> Option<DeadlineDocument> {
    loop {
        match deadlines
            .claim_due(owner, now_ms, until_ms)
            .await
            .expect("claim_due")
        {
            Some(d) if d.saga_id == saga_id => return Some(d),
            Some(_) => continue,
            None => return None,
        }
    }
}

/// [`SagaDispatchStore::claim_due_notice`] until it hands out the notice of `id` (another's it claims
/// on the way stay reserved) or nothing.
async fn claim_notice(
    queue: &dyn SagaDispatchStore,
    now_ms: i64,
    reserve_until_ms: i64,
    id: &str,
) -> Option<SagaDispatchDocument> {
    loop {
        match queue
            .claim_due_notice(now_ms, reserve_until_ms)
            .await
            .expect("claim_due_notice")
        {
            Some(e) if e.id == id => return Some(e),
            Some(_) => continue,
            None => return None,
        }
    }
}

/// A pending entry of `saga_id` with `seq`, recorded at `created_at_ms`.
fn dispatch_entry(
    saga_id: &str,
    seq: i64,
    kind: DispatchKind,
    created_at_ms: i64,
) -> SagaDispatchDocument {
    SagaDispatchDocument {
        id: format!("{saga_id}:{seq}"),
        saga_id: saga_id.to_string(),
        seq,
        kind,
        type_name: "conformance.Dispatched".into(),
        payload: "{\"amount\":42}".into(),
        headers: HashMap::from([("correlation-id".to_string(), "c-1".to_string())]),
        routing_key: None,
        created_at_ms,
        dispatched_at_ms: None,
        attempts: 0,
        next_attempt_at_ms: None,
        failed_at_ms: None,
        failure_reason: None,
        failure_message: None,
        notice_pending: None,
        notice_attempts: None,
        notice_next_at_ms: None,
    }
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

/// A marker that carries no position.
fn marker(processed_at_ms: i64) -> ProcessedMarker {
    ProcessedMarker {
        id: format!("processor:conformance:{}", uuid::Uuid::new_v4()),
        type_name: "conformance.Happened".into(),
        processed_at_ms,
        processor: None,
        source: None,
        segment: None,
        position: None,
    }
}

/// The stage-1 marker of the record at `offset` of `partition` of `topic`, handled by `processor`.
fn stage_one(processor: &str, topic: &str, partition: i32, offset: i64) -> ProcessedMarker {
    ProcessedMarker {
        id: format!("processor:{processor}:{}", uuid::Uuid::new_v4()),
        type_name: "conformance.Happened".into(),
        processed_at_ms: now_ms(),
        processor: Some(processor.to_string()),
        source: Some(topic.to_string()),
        segment: Some(partition.to_string()),
        position: Some(offset.to_string()),
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
        id: letter_id(
            processor,
            TOPIC,
            &partition.to_string(),
            &offset.to_string(),
        ),
        processor: processor.to_string(),
        source: TOPIC.to_string(),
        segment: partition.to_string(),
        position: offset.to_string(),
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
