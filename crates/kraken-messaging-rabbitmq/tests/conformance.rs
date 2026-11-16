//! The RabbitMQ Streams event store passes the event-store conformance kit on a real broker
//! (with the management API, so every capability is declared), the in-memory message store
//! beside it (the at-least-once check records processed events), its browser reading the kit's
//! super stream.

mod support;

use std::sync::Arc;

use kraken_messaging::event_store::browse::BrowseSettings;
use kraken_messaging::event_store::conformance::run_event_store_conformance;
use kraken_messaging::event_store::{Capability, EventStore, dead_letter_source};
use kraken_messaging::store::MessageStore;
use kraken_messaging::store::memory::MemoryStore;
use kraken_messaging_rabbitmq::RabbitEventStore;

/// Segments of the kit's source: enough to spread over two instances.
const PARTITIONS: usize = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rabbitmq_event_store_passes_conformance() {
    let rabbit = support::rabbitmq().await;
    let source = "kraken.conformance";
    rabbit.super_stream(source, PARTITIONS).await;
    rabbit.stream(&dead_letter_source(source)).await;

    let config = rabbit.config().browse(BrowseSettings {
        sources: vec![source.to_string()],
        ..BrowseSettings::default()
    });
    let make = || -> Arc<dyn EventStore> {
        Arc::new(RabbitEventStore::new(config.clone()).expect("rabbitmq event store"))
    };
    assert_eq!(make().capabilities(), Capability::ALL);
    let messages: Arc<dyn MessageStore> = Arc::new(MemoryStore::new());
    run_event_store_conformance(source, make, messages).await;
}

/// The default configuration (no management API): `browse`, `lag` and `native-dead-letter`;
/// the kit checks that the reset side is refused as unsupported instead of the replay checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rabbitmq_event_store_without_the_management_api_passes_conformance() {
    let rabbit = support::rabbitmq().await;
    let source = "kraken.conformance.default";
    rabbit.super_stream(source, PARTITIONS).await;
    rabbit.stream(&dead_letter_source(source)).await;
    let config =
        kraken_messaging_rabbitmq::RabbitConfig::new(&rabbit.stream).browse(BrowseSettings {
            sources: vec![source.to_string()],
            ..BrowseSettings::default()
        });
    let make = || -> Arc<dyn EventStore> {
        Arc::new(RabbitEventStore::new(config.clone()).expect("rabbitmq event store"))
    };
    assert_eq!(
        make().capabilities(),
        [
            Capability::Browse,
            Capability::Lag,
            Capability::NativeDeadLetter
        ]
    );
    let messages: Arc<dyn MessageStore> = Arc::new(MemoryStore::new());
    run_event_store_conformance(source, make, messages).await;
}
