//! The MongoDB store passes the store conformance kit, on a replica set (transactions need one).

use std::sync::Arc;

use kraken_messaging::store::conformance::run_store_conformance;
use kraken_messaging_mongo::{MongoIndexSettings, MongoStore};
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mongo::Mongo;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mongo_store_passes_conformance() {
    let container = Mongo::repl_set().start().await.expect("mongo container");
    let port = container
        .get_host_port_ipv4(27017.tcp())
        .await
        .expect("mongo port");
    let client =
        mongodb::Client::with_uri_str(format!("mongodb://127.0.0.1:{port}/?directConnection=true"))
            .await
            .expect("client");
    let db = client.database(&format!("t{}", uuid::Uuid::new_v4().simple()));
    let store = MongoStore::new(client, db);
    store
        .ensure_indexes(&MongoIndexSettings::default())
        .await
        .expect("indexes");
    run_store_conformance(Arc::new(store)).await;
}
