//! The event-store conformance kit's own test: the kit runs on the in-memory event
//! store (`event_store::memory`, with its message store), and fails as it should on a store that
//! declares fewer capabilities or never answers (the fault-injecting double of
//! `support::event_store`). An event-store add-on proves itself by running the same kit; the
//! in-memory one does in `tests/memory_event_store.rs`. Needs the feature `testing`.
#![cfg(feature = "testing")]

mod support;

use std::sync::Arc;

use snodra_messaging::event_store::EventStore;
use snodra_messaging::event_store::conformance::{
    EventStoreKit, lag_counts_unacked, run_event_store_conformance,
};
use snodra_messaging::event_store::memory::MemoryEventStore;
use snodra_messaging::store::memory::MemoryStore;
use support::event_store::{Log, Stuck};

/// Without the optional capabilities, the kit checks that the store says so.
#[tokio::test(flavor = "multi_thread")]
async fn the_kit_passes_without_the_optional_capabilities() {
    run_kit(Log::new(Vec::new())).await;
}

/// A call that never answers fails the check naming the step, after the kit's wait (on paused
/// time: at once), instead of hanging the kit.
#[tokio::test(start_paused = true)]
async fn a_publish_that_never_answers_fails_the_check_naming_it() {
    let failure = kit_failure(Log::full().stuck(Stuck::Publish)).await;
    assert!(
        failure.contains("check `headers_round_trip`")
            && failure.contains("publish ")
            && failure.contains("no answer within 60s"),
        "{failure}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_close_that_never_answers_fails_the_check_naming_it() {
    let failure = kit_failure(Log::full().stuck(Stuck::Close)).await;
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
    let events = MemoryEventStore::default();
    let make = move || Arc::new(events.clone()) as Arc<dyn EventStore>;
    let store = MemoryStore::new();
    let kit = EventStoreKit {
        source: "conformance.empty",
        make_events: &make,
        store: &store,
    };
    lag_counts_unacked(&kit).await;
}

async fn run_kit(events: Log) {
    let events = events.browsing(&["conformance.events"]);
    run_event_store_conformance(
        "conformance.events",
        move || Arc::new(events.clone()) as Arc<dyn EventStore>,
        Arc::new(MemoryStore::new()),
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
