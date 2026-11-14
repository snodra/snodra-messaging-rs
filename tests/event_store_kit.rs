//! The event-store conformance kit's own test (ADR 0027): the kit runs on a minimal in-process
//! event store (`support::event_store`), a test double (not the in-memory event store add-on, which comes
//! later), with a message store that only keeps processed-event markers. An event-store add-on
//! proves itself by running the same kit. Needs the feature `testing`.
#![cfg(feature = "testing")]

mod support;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use kraken_messaging::Config;
use kraken_messaging::event_store::conformance::{
    EventStoreKit, lag_counts_unacked, run_event_store_conformance, search_stops_at_bounds,
};
use kraken_messaging::event_store::{Capability, EventStore, OutgoingEvent, ReplayRange};
use kraken_messaging::saga::Clock;
use kraken_messaging::store::{
    DeadLetterStore, ExternalCallStore, MessageStore, OutboxStore, ProcessedEventStore,
    ProcessedMarker, ProcessorControlStore, SagaStores, StoreError, Tx, TxProvider,
};
use support::event_store::{Log, Stuck};

#[tokio::test(flavor = "multi_thread")]
async fn the_kit_passes_on_a_minimal_in_process_event_store() {
    run_kit(Log::new(Capability::ALL.to_vec())).await;
}

/// Without the optional capabilities, the kit checks that the store says so.
#[tokio::test(flavor = "multi_thread")]
async fn the_kit_passes_without_the_optional_capabilities() {
    run_kit(Log::new(Vec::new())).await;
}

/// A call that never answers fails the check naming the step, after the kit's wait (on paused
/// time: at once), instead of hanging the kit.
#[tokio::test(start_paused = true)]
async fn a_publish_that_never_answers_fails_the_check_naming_it() {
    let failure = kit_failure(Log::new(Capability::ALL.to_vec()).stuck(Stuck::Publish)).await;
    assert!(
        failure.contains("check `headers_round_trip`")
            && failure.contains("publish ")
            && failure.contains("no answer within 60s"),
        "{failure}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_close_that_never_answers_fails_the_check_naming_it() {
    let failure = kit_failure(Log::new(Capability::ALL.to_vec()).stuck(Stuck::Close)).await;
    assert!(
        failure.contains("check `headers_round_trip`")
            && failure.contains("close conformance.")
            && failure.contains("no answer within 60s"),
        "{failure}"
    );
}

/// The lag check alone, on a source nothing was ever published to.
#[tokio::test(flavor = "multi_thread")]
async fn the_lag_check_passes_alone_on_an_empty_source() {
    let events = Log::new(Capability::ALL.to_vec());
    let make = move || Arc::new(events.clone()) as Arc<dyn EventStore>;
    let markers = Markers::default();
    let kit = EventStoreKit {
        source: "conformance.empty",
        make_events: &make,
        store: &markers,
    };
    lag_counts_unacked(&kit).await;
}

/// The scan-budget check on a source that already holds a few hundred older events (a reused
/// source): its paging may not depend on what the source held before it started.
#[tokio::test(flavor = "multi_thread")]
async fn the_scan_budget_check_passes_on_a_pre_filled_source() {
    let source = "conformance.prefilled";
    let events = Log::new(Capability::ALL.to_vec()).browsing(&[source]);
    for i in 0..400 {
        let old = OutgoingEvent {
            source: source.to_string(),
            id: format!("old-{i}"),
            type_name: "conformance.Old".to_string(),
            key: Some(format!("key-{i}")),
            headers: std::collections::HashMap::from([
                ("type".to_string(), "conformance.Old".to_string()),
                ("id".to_string(), format!("old-{i}")),
            ]),
            payload: b"{}".to_vec(),
        };
        events.publish(&old).await.expect("publish");
    }
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let make = move || Arc::new(events.clone()) as Arc<dyn EventStore>;
    let markers = Markers::default();
    let kit = EventStoreKit {
        source,
        make_events: &make,
        store: &markers,
    };
    search_stops_at_bounds(&kit).await;
}

/// The scan-budget check on a store whose clock is 5 s behind the kit's: its paging takes its
/// times from the store's own stamps.
#[tokio::test(flavor = "multi_thread")]
async fn the_scan_budget_check_passes_on_a_store_with_a_lagging_clock() {
    let source = "conformance.lagging";
    let events = Log::new(Capability::ALL.to_vec())
        .browsing(&[source])
        .lagging(5_000);
    let make = move || Arc::new(events.clone()) as Arc<dyn EventStore>;
    let markers = Markers::default();
    let kit = EventStoreKit {
        source,
        make_events: &make,
        store: &markers,
    };
    search_stops_at_bounds(&kit).await;
}

async fn run_kit(events: Log) {
    let events = events.browsing(&["conformance.events"]);
    run_event_store_conformance(
        "conformance.events",
        move || Arc::new(events.clone()) as Arc<dyn EventStore>,
        Arc::new(Markers::default()),
    )
    .await;
}

/// The message the kit fails with on `events`.
async fn kit_failure(events: Log) -> String {
    use futures::FutureExt;
    let panic = std::panic::AssertUnwindSafe(run_kit(events))
        .catch_unwind()
        .await
        .expect_err("the kit fails");
    panic
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "(no message)".into())
}

// ---- the message store: processed-event markers only --------------------------------------------

/// A message store keeping committed markers only (each record commits at once); the event-store
/// kit uses nothing else.
#[derive(Default)]
struct Markers(Arc<Mutex<HashSet<String>>>);

impl MessageStore for Markers {
    fn name(&self) -> &'static str {
        "kit-markers"
    }

    fn init_schema<'a>(&'a self, _config: &'a Config) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn use_clock(&self, _clock: Clock) {}

    fn tx(&self) -> Arc<dyn TxProvider> {
        Arc::new(NoTx)
    }

    fn processed_events(&self) -> Arc<dyn ProcessedEventStore> {
        Arc::new(MarkersRef(Arc::clone(&self.0)))
    }

    fn outbox(&self) -> Arc<dyn OutboxStore> {
        unimplemented!("not used by the event-store kit")
    }

    fn dead_letters(&self) -> Arc<dyn DeadLetterStore> {
        unimplemented!("not used by the event-store kit")
    }

    fn processor_control(&self) -> Arc<dyn ProcessorControlStore> {
        unimplemented!("not used by the event-store kit")
    }

    fn external_calls(&self) -> Arc<dyn ExternalCallStore> {
        unimplemented!("not used by the event-store kit")
    }

    fn sagas(&self) -> SagaStores {
        unimplemented!("not used by the event-store kit")
    }
}

struct NoTx;

impl TxProvider for NoTx {
    fn begin(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(std::future::ready(Ok(Tx::none())))
    }

    fn plain(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        Box::pin(std::future::ready(Ok(Tx::none())))
    }

    fn commit<'a>(&'a self, _tx: &'a mut Tx) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn abort<'a>(&'a self, _tx: &'a mut Tx) -> BoxFuture<'a, ()> {
        Box::pin(std::future::ready(()))
    }
}

struct MarkersRef(Arc<Mutex<HashSet<String>>>);

impl ProcessedEventStore for MarkersRef {
    fn record<'a>(
        &'a self,
        _tx: &'a mut Tx,
        marker: &'a ProcessedMarker,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        let fresh = self.0.lock().unwrap().insert(marker.id.clone());
        Box::pin(std::future::ready(Ok(fresh)))
    }

    fn exists<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, StoreError>> {
        let found = self.0.lock().unwrap().contains(id);
        Box::pin(std::future::ready(Ok(found)))
    }

    fn delete_markers<'a>(
        &'a self,
        _processor: &'a str,
        _ranges: &'a [ReplayRange],
    ) -> BoxFuture<'a, Result<u64, StoreError>> {
        unimplemented!("not used by the event-store kit")
    }

    fn purge(&self, _before_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        unimplemented!("not used by the event-store kit")
    }
}
