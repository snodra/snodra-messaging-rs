//! The zero-infrastructure mode (feature `testing`): a service's messaging on the in-memory
//! event store and message store (`testing::InMemoryStores`), with no container: an event
//! published to it is handled, what its handlers publish is read back, a failing record becomes a
//! dead letter. (The startup warnings are pinned in `tests/store_required.rs`.)
#![cfg(feature = "testing")]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_messaging::store::DeadLetterReason;
use kraken_messaging::testing::InMemoryStores;
use kraken_messaging::{Event, KrakenMessaging, MessagingError, message_type};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct OrderPlaced {
    order: String,
}
message_type!(OrderPlaced, "zero.OrderPlaced");
impl Event for OrderPlaced {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct OrderConfirmed {
    order: String,
}
message_type!(OrderConfirmed, "zero.OrderConfirmed");
impl Event for OrderConfirmed {}

const WITHIN: Duration = Duration::from_secs(10);

/// An event published to the service is handled once; what the handler publishes in its
/// transaction is read back, and so is a record its handler fails on, as a dead letter.
#[tokio::test]
async fn a_service_runs_on_the_in_memory_stores() {
    let stores = InMemoryStores::new();
    let mut config = stores.config("zero");
    config.consumer_max_attempts = 2;
    let builder = stores.install(KrakenMessaging::builder(config).await.unwrap());
    let events = builder.events();
    let handled: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen = Arc::clone(&handled);
    let app = builder
        .event::<OrderPlaced, _, _>(move |e, ctx| {
            let (events, seen) = (events.clone(), Arc::clone(&seen));
            async move {
                if e.order.starts_with("poison") {
                    return Err(MessagingError::RetryableExternal("cannot place it".into()));
                }
                seen.lock().unwrap().push(e.order.clone());
                let mut tx = ctx.tx.lock().await;
                events
                    .publish_in(
                        &OrderConfirmed {
                            order: e.order.clone(),
                        },
                        Some(&e.order),
                        &mut tx,
                    )
                    .await
            }
        })
        .start()
        .await
        .unwrap();

    let placed = stores
        .publish(
            &OrderPlaced {
                order: "o-1".into(),
            },
            Some("o-1"),
        )
        .await
        .unwrap();
    let confirmed = stores
        .wait_published::<OrderConfirmed>(1, WITHIN)
        .await
        .unwrap();
    assert_eq!(
        confirmed,
        [OrderConfirmed {
            order: "o-1".into()
        }]
    );
    assert!(stores.processed(&placed), "the handler's marker");
    assert!(!stores.processed("no-such-event"));
    assert_eq!(
        stores.published::<OrderPlaced>(),
        [],
        "what the test published is not the service's"
    );

    stores
        .publish(
            &OrderPlaced {
                order: "poison-1".into(),
            },
            Some("poison-1"),
        )
        .await
        .unwrap();
    let letters = stores.wait_dead_letters(1, WITHIN).await.unwrap();
    assert_eq!(letters[0].reason, DeadLetterReason::HandlerFailed);
    assert_eq!(letters[0].event_type.as_deref(), Some("zero.OrderPlaced"));
    assert_eq!(letters[0].key.as_deref(), Some("poison-1"));
    assert_eq!(*handled.lock().unwrap(), ["o-1"], "each handled once");
    app.shutdown().await;
}

/// Waiting for what never comes ends with an error naming it, after the time given.
#[tokio::test(start_paused = true)]
async fn waiting_for_what_never_comes_ends() {
    let stores = InMemoryStores::new();
    let missing = stores
        .wait_published::<OrderConfirmed>(1, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(
        missing.to_string().contains("zero.OrderConfirmed"),
        "{missing}"
    );
    assert!(
        stores
            .wait_dead_letters(1, Duration::from_secs(5))
            .await
            .is_err()
    );
}

/// The relay publishes an append at once (the store wakes it), not at its next poll.
#[tokio::test]
async fn an_event_is_relayed_without_waiting_for_the_poll() {
    let stores = InMemoryStores::new();
    let mut config = stores.config("zero");
    config.outbox_poll_interval = Duration::from_secs(600);
    let builder = stores.install(KrakenMessaging::builder(config).await.unwrap());
    let events = builder.events();
    let app = builder.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await; // the relay's first poll is done
    events
        .publish(
            &OrderConfirmed {
                order: "o-9".into(),
            },
            Some("o-9"),
        )
        .await
        .unwrap();
    let relayed = stores
        .wait_published::<OrderConfirmed>(1, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        relayed,
        [OrderConfirmed {
            order: "o-9".into()
        }]
    );
    app.shutdown().await;
}
