//! The relay wake-up on PostgreSQL: an append notifies `kraken_outbox` in its transaction, and the
//! relay, listening on a connection of its own, publishes at once instead of at its next poll.

#![cfg(all(feature = "postgres", feature = "mysql"))]

mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use kraken_messaging::event_store::memory::MemoryEventStore;
use kraken_messaging::store::{MessageStore, OutboxEntry};
use kraken_messaging::time::now_ms;
use kraken_messaging::{Config, KrakenMessaging};
use kraken_messaging_sqlx::SqlxSettings;
use support::Server;

fn entry(id: &str) -> OutboxEntry {
    OutboxEntry {
        id: id.into(),
        type_name: "t.Woken".into(),
        key: Some(id.into()),
        headers: HashMap::new(),
        payload: br#"{"n":1}"#.to_vec(),
        created_at_ms: now_ms(),
        seq: 1,
        attempts: 0,
    }
}

async fn until(what: &str, within: Duration, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + within;
    while !check() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The relay polls every 10 minutes: an entry appended by another instance without a NOTIFY waits
/// (it is not published within 2 s), one appended with a NOTIFY is published at once, and the
/// relay's claim then takes both. Only the NOTIFY could have woken it: it listens before the
/// appends (`listens`), and its start-up wake-up is spent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn notify_wakes_relay_before_poll() {
    let server = Server::postgres().await;
    let url = server.database().await;
    let (relaying, _) = server.open(&url, SqlxSettings::default()).await;
    relaying.apply_schema().await.unwrap();
    let events = MemoryEventStore::default();
    let mut config = Config::new("notify", "static://127.0.0.1:1");
    config.hub_enabled = false;
    config.outbox_poll_interval = Duration::from_secs(600);
    let app = KrakenMessaging::builder(config.clone())
        .await
        .unwrap()
        .store(Arc::new(relaying.clone()))
        .event_store(Arc::new(events.clone()))
        .start()
        .await
        .unwrap();
    until("LISTEN in place", Duration::from_secs(10), || {
        relaying.listens() >= 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await; // the start-up wake-up's claim
    let published = || events.events(&config.events_source).len();

    let (silent, _) = server
        .open(
            &url,
            SqlxSettings {
                notify: false,
                ..SqlxSettings::default()
            },
        )
        .await;
    silent.outbox().append_now(entry("e-silent")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        published(),
        0,
        "no NOTIFY, no wake-up: it waits for the poll"
    );

    let (other, _) = server.open(&url, SqlxSettings::default()).await;
    other
        .outbox()
        .append_now(entry("e-notified"))
        .await
        .unwrap();
    until("published at once", Duration::from_secs(5), || {
        published() == 2
    })
    .await;
    app.shutdown().await;
}

/// A rolled-back append notifies nobody, a committed one wakes the listener; listening ends with
/// the last listener (its connection closes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_commit_wakes_and_listening_ends_with_the_last_listener() {
    let server = Server::postgres().await;
    let (store, raw) = server.store().await;
    let woken = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&woken);
    let listener = store.outbox().on_append(Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    until("LISTEN in place", Duration::from_secs(10), || {
        store.listens() >= 1
    })
    .await;
    until("the start-up wake-up", Duration::from_secs(5), || {
        woken.load(Ordering::SeqCst) >= 1
    })
    .await;
    let before = woken.load(Ordering::SeqCst);
    let txs = store.tx();
    let mut tx = txs.begin().await.unwrap();
    store
        .outbox()
        .append(&mut tx, entry("e-rolled-back"))
        .await
        .unwrap();
    txs.abort(&mut tx).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        woken.load(Ordering::SeqCst),
        before,
        "a rollback notifies nobody"
    );
    let mut tx = txs.begin().await.unwrap();
    store
        .outbox()
        .append(&mut tx, entry("e-committed"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        woken.load(Ordering::SeqCst),
        before,
        "not before the commit"
    );
    txs.commit(&mut tx).await.unwrap();
    until("woken by the commit", Duration::from_secs(5), || {
        woken.load(Ordering::SeqCst) > before
    })
    .await;

    let listening = async || {
        raw.rows(
            "SELECT CAST(pid AS TEXT) FROM pg_stat_activity WHERE query LIKE 'LISTEN%' OR query = 'SELECT 1'",
        )
        .await
        .len()
    };
    assert_eq!(listening().await, 1, "one listening connection");
    drop(listener);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while listening().await > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "listening did not end"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
