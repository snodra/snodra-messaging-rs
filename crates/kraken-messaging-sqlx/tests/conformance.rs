//! The SQL store passes the store conformance kit, every check, on PostgreSQL and on MySQL, at
//! the image versions kraken-messaging-jdbc's kit runs on.

#![cfg(all(feature = "postgres", feature = "mysql"))]

mod support;

use std::sync::Arc;

use kraken_messaging::store::conformance::run_store_conformance;
use support::Server;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_store_passes_conformance() {
    let server = Server::postgres().await;
    let (store, _) = server.store().await;
    run_store_conformance(Arc::new(store)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_store_passes_conformance() {
    let server = Server::mysql().await;
    let (store, _) = server.store().await;
    run_store_conformance(Arc::new(store)).await;
}
