//! The in-memory message store (`store::memory`, ADR 0028): the store conformance kit on it, with
//! no container, and its transactions beyond what the kit sees (isolation, conflicts, misuse,
//! paused time). Needs the feature `testing`.
#![cfg(feature = "testing")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::saga::store::SagaDocument;
use kraken_messaging::store::conformance::run_store_conformance;
use kraken_messaging::store::memory::MemoryStore;
use kraken_messaging::store::{
    DeadLetter, DeadLetterError, DeadLetterReason, MessageStore, OutboxEntry, Parked,
    ProcessedMarker, SagaPorts, StoreError, Tx, letter_id,
};

#[tokio::test(flavor = "multi_thread")]
async fn the_kit_passes_on_the_memory_store() {
    run_store_conformance(Arc::new(MemoryStore::new())).await;
}

#[test]
fn it_is_named_memory() {
    assert_eq!(MemoryStore::new().name(), "memory");
}

/// Two live transactions writing one record: the second write conflicts at once (and that
/// transaction commits nothing); once the first committed, a retry finds the marker.
#[tokio::test]
async fn a_record_another_live_transaction_writes_is_a_conflict_at_once() {
    let store = MemoryStore::new();
    let (txs, processed, outbox) = (store.tx(), store.processed_events(), store.outbox());
    let m = marker("processor:p:e-1");
    let mut first = txs.begin().await.expect("begin");
    let mut second = txs.begin().await.expect("begin");
    assert!(processed.record(&mut first, &m).await.expect("record"));
    outbox
        .append(&mut second, entry("e-out"))
        .await
        .expect("append");
    assert!(matches!(
        processed.record(&mut second, &m).await,
        Err(StoreError::Conflict(_))
    ));
    txs.commit(&mut first).await.expect("the first commit");
    assert!(matches!(
        txs.commit(&mut second).await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        outbox.pending().await.expect("pending"),
        0,
        "nothing of the conflicting transaction"
    );
    let mut retry = txs.begin().await.expect("begin");
    assert!(
        !processed.record(&mut retry, &m).await.expect("record"),
        "the retry finds the marker"
    );
}

/// A transaction aborted, or dropped without an end, lets go of what it wrote.
#[tokio::test]
async fn an_aborted_or_dropped_transaction_lets_go_of_its_records() {
    let store = MemoryStore::new();
    let (txs, processed) = (store.tx(), store.processed_events());
    let m = marker("processor:p:e-1");
    let mut aborted = txs.begin().await.expect("begin");
    assert!(processed.record(&mut aborted, &m).await.expect("record"));
    txs.abort(&mut aborted).await;
    let mut dropped = txs.begin().await.expect("begin");
    assert!(processed.record(&mut dropped, &m).await.expect("record"));
    drop(dropped);
    let mut next = txs.begin().await.expect("begin");
    assert!(processed.record(&mut next, &m).await.expect("record"));
    txs.commit(&mut next).await.expect("commit");
}

/// A write outside any transaction to a record a transaction changed makes that transaction
/// conflict at its commit, as a database's write conflict.
#[tokio::test]
async fn a_record_changed_since_the_transaction_wrote_it_conflicts() {
    let store = MemoryStore::new();
    let ports = SagaPorts::of(&store);
    let saga = saga("s-1", Some("order-1"));
    let mut plain = Tx::none();
    ports.sagas.insert(&mut plain, &saga).await.expect("insert");
    let mut tx = ports.tx.begin().await.expect("begin");
    let next = SagaDocument {
        version: 2,
        state: "{\"step\":2}".into(),
        ..saga.clone()
    };
    assert!(ports.sagas.update(&mut tx, &next, 1).await.expect("update"));
    let other = SagaDocument {
        version: 2,
        state: "{\"step\":9}".into(),
        ..saga.clone()
    };
    assert!(
        ports
            .sagas
            .update(&mut plain, &other, 1)
            .await
            .expect("update")
    );
    assert!(matches!(
        ports.tx.commit(&mut tx).await,
        Err(StoreError::Conflict(_))
    ));
    let stored = ports
        .sagas
        .find_by_id(&mut plain, "s-1")
        .await
        .expect("find")
        .expect("the saga");
    assert_eq!(stored.state, "{\"step\":9}", "the write that won");
}

/// A transaction reads its own writes; the others see them once it commits; a transaction
/// dropped without a commit leaves nothing.
#[tokio::test]
async fn a_transaction_reads_its_own_writes_and_a_dropped_one_leaves_nothing() {
    let store = MemoryStore::new();
    let ports = SagaPorts::of(&store);
    let mut tx = ports.tx.begin().await.expect("begin");
    ports
        .sagas
        .insert(&mut tx, &saga("s-1", Some("order-1")))
        .await
        .expect("insert");
    let mut plain = Tx::none();
    assert!(
        ports
            .sagas
            .find_by_id(&mut tx, "s-1")
            .await
            .expect("find")
            .is_some(),
        "its own write"
    );
    assert!(
        ports
            .sagas
            .find_by_id(&mut plain, "s-1")
            .await
            .expect("find")
            .is_none(),
        "not committed"
    );
    assert_eq!(
        ports
            .sagas
            .find_live(&mut tx, "t.Order", "orderId", "order-1")
            .await
            .expect("find_live")
            .len(),
        1
    );
    drop(tx);
    assert!(
        ports
            .sagas
            .find_by_id(&mut plain, "s-1")
            .await
            .expect("find")
            .is_none(),
        "dropped: rolled back"
    );
    let mut tx = ports.tx.begin().await.expect("begin");
    ports
        .sagas
        .insert(&mut tx, &saga("s-2", Some("order-2")))
        .await
        .expect("insert");
    ports.tx.commit(&mut tx).await.expect("commit");
    assert!(
        ports
            .sagas
            .find_by_id(&mut plain, "s-2")
            .await
            .expect("find")
            .is_some(),
        "committed"
    );
}

/// Two transactions starting a saga on one start key: the second commit conflicts.
#[tokio::test]
async fn a_start_key_is_held_once_across_transactions() {
    let store = MemoryStore::new();
    let ports = SagaPorts::of(&store);
    let mut first = ports.tx.begin().await.expect("begin");
    let mut second = ports.tx.begin().await.expect("begin");
    ports
        .sagas
        .insert(&mut first, &saga("s-1", Some("order-1")))
        .await
        .expect("insert");
    ports
        .sagas
        .insert(&mut second, &saga("s-2", Some("order-1")))
        .await
        .expect("insert");
    ports.tx.commit(&mut first).await.expect("commit");
    assert!(matches!(
        ports.tx.commit(&mut second).await,
        Err(StoreError::Conflict(_))
    ));
}

/// Another store's transaction is refused, never written through; what is written with a
/// transaction that ended commits on its own (as with none).
#[tokio::test]
async fn a_foreign_transaction_is_refused_and_an_ended_one_counts_as_none() {
    let (store, other) = (MemoryStore::new(), MemoryStore::new());
    let outbox = store.outbox();
    let mut foreign = other.tx().begin().await.expect("begin");
    assert!(matches!(
        outbox.append(&mut foreign, entry("e-1")).await,
        Err(StoreError::Failed(_))
    ));
    let mut alien = Tx::new(String::from("a session of another store"));
    assert!(matches!(
        outbox.append(&mut alien, entry("e-2")).await,
        Err(StoreError::Failed(_))
    ));
    assert_eq!(outbox.pending().await.expect("pending"), 0);
    let mut tx = store.tx().begin().await.expect("begin");
    store.tx().commit(&mut tx).await.expect("commit");
    outbox.append(&mut tx, entry("e-3")).await.expect("append");
    assert_eq!(
        outbox.pending().await.expect("pending"),
        1,
        "committed on its own"
    );
    store
        .tx()
        .commit(&mut tx)
        .await
        .expect("an ended transaction commits nothing");
}

/// `on_append` is called once an append commits: on its own at once, in a transaction at its
/// commit, never for a rolled-back one.
#[tokio::test]
async fn an_append_wakes_the_relay_once_committed() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let store = MemoryStore::new();
    let outbox = store.outbox();
    let woken = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&woken);
    outbox.on_append(Arc::new(move || {
        count.fetch_add(1, Ordering::SeqCst);
    }));
    outbox.append_now(entry("e-1")).await.expect("append_now");
    assert_eq!(woken.load(Ordering::SeqCst), 1);
    let mut tx = store.tx().begin().await.expect("begin");
    outbox.append(&mut tx, entry("e-2")).await.expect("append");
    assert_eq!(woken.load(Ordering::SeqCst), 1, "not before the commit");
    store.tx().commit(&mut tx).await.expect("commit");
    assert_eq!(woken.load(Ordering::SeqCst), 2);
    let mut tx = store.tx().begin().await.expect("begin");
    outbox.append(&mut tx, entry("e-3")).await.expect("append");
    store.tx().abort(&mut tx).await;
    assert_eq!(woken.load(Ordering::SeqCst), 2, "rolled back");
}

/// A claim's lease runs on the store's clock: by default tokio's, so it passes in paused time.
#[tokio::test(start_paused = true)]
async fn a_lease_passes_in_paused_time() {
    let store = MemoryStore::new();
    let outbox = store.outbox();
    outbox.append_now(entry("e-1")).await.expect("append_now");
    let lease = Duration::from_secs(30);
    assert_eq!(outbox.claim("a", lease, 10).await.expect("claim").len(), 1);
    assert!(
        outbox
            .claim("b", lease, 10)
            .await
            .expect("claim")
            .is_empty()
    );
    tokio::time::sleep(lease).await;
    assert_eq!(
        outbox.claim("b", lease, 10).await.expect("claim").len(),
        1,
        "the lease passed"
    );
}

/// A dead letter whose `expire_at_ms` passed disappears, as a database's TTL removes it, and
/// unblocks its key.
#[tokio::test(start_paused = true)]
async fn an_expired_dead_letter_disappears() {
    let store = MemoryStore::new();
    let letters = store.dead_letters();
    let now = kraken_messaging::saga::Clock::tokio().now_ms();
    let letter = DeadLetter {
        id: letter_id("p", "kraken.events", "0", "5"),
        processor: "p".into(),
        source: "kraken.events".into(),
        segment: "0".into(),
        position: "5".into(),
        key: Some("k".into()),
        event_id: Some("e-1".into()),
        event_type: Some("t.Happened".into()),
        headers: Default::default(),
        payload: b"{}".to_vec(),
        reason: DeadLetterReason::HandlerFailed,
        error: DeadLetterError::new("RemoteHandler", "boom"),
        blocked_by: None,
        attempts: 3,
        parked_at_ms: now,
        last_attempt_at_ms: now,
        expire_at_ms: now + 60_000,
        replay: false,
    };
    assert_eq!(letters.park(&letter).await.expect("park"), Parked::New);
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(
        letters.head("p", "k").await.expect("head"),
        Some(letter.id.clone())
    );
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(
        letters.head("p", "k").await.expect("head"),
        None,
        "expired: the key is free"
    );
    assert_eq!(letters.count(None).await.expect("count"), 0);
}

fn marker(id: &str) -> ProcessedMarker {
    ProcessedMarker {
        id: id.to_string(),
        type_name: "t.Happened".into(),
        processed_at_ms: 1,
        processor: None,
        source: None,
        segment: None,
        position: None,
    }
}

fn entry(id: &str) -> OutboxEntry {
    OutboxEntry {
        id: id.to_string(),
        type_name: "t.Happened".into(),
        key: Some("k".into()),
        headers: HashMap::new(),
        payload: b"{}".to_vec(),
        created_at_ms: 1,
        seq: 1,
        attempts: 0,
    }
}

fn saga(id: &str, start_key: Option<&str>) -> SagaDocument {
    SagaDocument {
        id: id.to_string(),
        type_name: "t.Order".into(),
        state: "{}".into(),
        associations: vec![kraken_messaging::saga::store::Association::new(
            "orderId",
            start_key.unwrap_or("none"),
        )],
        start_key: start_key.map(str::to_string),
        ended: false,
        ended_at_ms: None,
        version: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}
