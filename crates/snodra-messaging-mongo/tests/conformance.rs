//! The MongoDB store passes the store conformance kit, on a replica set (transactions need one),
//! and stamps its own times with the application's clock.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use mongodb::bson::{DateTime, Document, doc};
use snodra_messaging::saga::ManualClock;
use snodra_messaging::store::conformance::run_store_conformance;
use snodra_messaging::store::{MessageStore, OutboxEntry};
use snodra_messaging_mongo::{MongoIndexSettings, MongoStore};
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
    let store = MongoStore::new(client, db.clone());
    store.use_clock(clock.clock());
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

/// Markers without a position (a local dispatch's, a saga's, an ingress's) still deduplicate and
/// no reset deletes them; a marker written before the event store became an add-on (`partition`
/// an Int32, `offset` an Int64, no string fields) keeps its position: it deduplicates and a replay
/// over its offset deletes it, as before, with no migration. A marker of a non-decimal position is
/// stored as text and deleted by a range of the same kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn markers_without_position_behave_as_before() {
    use snodra_messaging::event_store::ReplayRange;
    use snodra_messaging::store::ProcessedMarker;

    let (_container, client) = mongo().await;
    let db = client.database(&format!("t{}", uuid::Uuid::new_v4().simple()));
    let store = MongoStore::new(client, db.clone());
    store
        .ensure_indexes(&MongoIndexSettings::default())
        .await
        .expect("indexes");
    let markers = db.collection::<Document>("processed_events");
    let at = DateTime::now();
    markers
        .insert_many([
            // Written by an older version of a library, for the record at offset 4 of
            // partition 1.
            doc! { "_id": "processor:default:old-4", "type": "t.E", "processedAt": at,
            "processor": "default", "topic": "snodra.events", "partition": 1_i32,
            "offset": 4_i64 },
            // Without a position: a saga's marker, from before as from now on.
            doc! { "_id": "saga:t.Order:e-1", "type": "t.E", "processedAt": at },
        ])
        .await
        .expect("insert");
    let processed = store.processed_events();
    let txs = store.tx();
    let record = |id: &str, segment: Option<&str>, position: Option<&str>| ProcessedMarker {
        id: id.to_string(),
        type_name: "t.E".into(),
        processed_at_ms: 0,
        processor: segment.map(|_| "default".to_string()),
        source: segment.map(|_| "snodra.events".to_string()),
        segment: segment.map(str::to_string),
        position: position.map(str::to_string),
    };
    for (marker, fresh) in [
        (
            record("processor:default:old-4", Some("1"), Some("4")),
            false,
        ),
        (record("saga:t.Order:e-1", None, None), false),
        (record("ingress:partner:k-1", None, None), true),
        (
            record("processor:default:odd", Some("p-a"), Some("x5")),
            true,
        ),
    ] {
        let mut tx = txs.begin().await.expect("begin");
        let recorded = processed.record(&mut tx, &marker).await.expect("record");
        if recorded {
            txs.commit(&mut tx).await.expect("commit");
        } else {
            txs.abort(&mut tx).await;
        }
        assert_eq!(recorded, fresh, "{}", marker.id);
    }
    let range = |segment: &str, from: &str, until: &str| ReplayRange {
        source: "snodra.events".into(),
        segment: segment.into(),
        from: from.into(),
        until: until.into(),
    };
    assert_eq!(
        processed
            .delete_markers(
                "default",
                &[range("1", "0", "10"), range("p-a", "x0", "x9")]
            )
            .await
            .expect("delete"),
        2,
        "the marker from an older version and the textual one"
    );
    for (id, kept) in [
        ("processor:default:old-4", false),
        ("processor:default:odd", false),
        ("saga:t.Order:e-1", true),
        ("ingress:partner:k-1", true),
    ] {
        assert_eq!(processed.exists(id).await.expect("exists"), kept, "{id}");
    }
}
