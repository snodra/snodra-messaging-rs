//! The MongoDB store passes the store conformance kit, on a replica set (transactions need one),
//! and stamps its own times with the application's clock.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use kraken_messaging::saga::ManualClock;
use kraken_messaging::store::conformance::run_store_conformance;
use kraken_messaging::store::{MessageStore, OutboxEntry};
use kraken_messaging_mongo::{MongoIndexSettings, MongoStore};
use mongodb::bson::{DateTime, Document, doc};
use testcontainers::ContainerAsync;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mongo::Mongo;

/// A replica set and a client of it.
async fn mongo() -> (ContainerAsync<Mongo>, mongodb::Client) {
    let container = Mongo::repl_set().start().await.expect("mongo container");
    let port = container
        .get_host_port_ipv4(27017.tcp())
        .await
        .expect("mongo port");
    let client =
        mongodb::Client::with_uri_str(format!("mongodb://127.0.0.1:{port}/?directConnection=true"))
            .await
            .expect("client");
    (container, client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mongo_store_passes_conformance() {
    let (_container, client) = mongo().await;
    let db = client.database(&format!("t{}", uuid::Uuid::new_v4().simple()));
    let store = MongoStore::new(client, db);
    store
        .ensure_indexes(&MongoIndexSettings::default())
        .await
        .expect("indexes");
    run_store_conformance(Arc::new(store)).await;
}

/// `sentAt`, `failedAt` and the claim's lease are the clock's given to the store, not the
/// database's nor the host's.
#[tokio::test]
async fn the_outbox_stamps_its_times_with_the_application_clock() {
    let (_container, client) = mongo().await;
    let db = client.database(&format!("t{}", uuid::Uuid::new_v4().simple()));
    let at = UNIX_EPOCH + Duration::from_secs(1_600_000_000);
    let clock = ManualClock::new(at);
    let store = MongoStore::new(client, db.clone()).with_clock(clock.clock());
    let outbox = store.outbox();
    let entry = |id: &str| OutboxEntry {
        id: id.into(),
        type_name: "t.Happened".into(),
        key: None,
        headers: Default::default(),
        payload: br#"{"n":1}"#.to_vec(),
        created_at_ms: 1_600_000_000_000,
        seq: 1,
        attempts: 0,
    };
    outbox.append_now(entry("sent")).await.unwrap();
    outbox.append_now(entry("failed")).await.unwrap();
    let claimed = outbox
        .claim("relay-a", Duration::from_secs(30), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);
    outbox.mark_sent(&["sent".to_string()]).await.unwrap();
    outbox.mark_failed("failed", "rejected").await.unwrap();

    let docs = db.collection::<Document>("outbox");
    let read = |id: &'static str| {
        let docs = docs.clone();
        async move { docs.find_one(doc! { "_id": id }).await.unwrap().unwrap() }
    };
    let now = DateTime::from_system_time(at);
    let sent = read("sent").await;
    assert_eq!(sent.get_datetime("sentAt"), Ok(&now));
    assert_eq!(
        sent.get_datetime("leaseUntil"),
        Ok(&DateTime::from_millis(now.timestamp_millis() + 30_000))
    );
    assert_eq!(read("failed").await.get_datetime("failedAt"), Ok(&now));
}
