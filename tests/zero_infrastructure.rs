//! The zero-infrastructure mode (feature `testing`): a service's messaging on the in-memory
//! event store and message store (`testing::InMemoryStores`), with no container: an event
//! published to it is handled, what its handlers publish is read back, a failing record becomes a
//! dead letter. (The startup warnings are pinned in `tests/store_required.rs`.)
#![cfg(feature = "testing")]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use snodra_messaging::store::DeadLetterReason;
use snodra_messaging::testing::InMemoryStores;
use snodra_messaging::{Event, MessagingError, SendOptions, SnodraMessaging, message_type};

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
    let builder = stores.install(SnodraMessaging::builder(config).await.unwrap());
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

/// The relay publishes an append at once (the store wakes it), not at its next poll; its
/// listener goes with it.
#[tokio::test]
async fn an_event_is_relayed_without_waiting_for_the_poll() {
    let stores = InMemoryStores::new();
    let mut config = stores.config("zero");
    config.outbox_poll_interval = Duration::from_secs(600);
    let builder = stores.install(SnodraMessaging::builder(config).await.unwrap());
    let events = builder.events();
    let app = builder.start().await.unwrap();
    assert_eq!(stores.store().append_listeners(), 1, "the relay listens");
    let confirmed = |order: &str| OrderConfirmed {
        order: order.into(),
    };
    // The first may go out with the relay's first poll; the second only through the wake-up
    // (the next poll is 10 minutes away).
    for (n, order) in ["o-1", "o-2"].into_iter().enumerate() {
        events
            .publish(&confirmed(order), Some(order))
            .await
            .unwrap();
        let relayed = stores
            .wait_published::<OrderConfirmed>(n + 1, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(relayed[n], confirmed(order));
    }
    app.shutdown().await;
    assert_eq!(
        stores.store().append_listeners(),
        0,
        "no listener outlives its relay"
    );
}

/// The application's clock is tokio's: on paused time a saga's deadline an hour away fires when
/// an hour passes, not before.
#[tokio::test(start_paused = true)]
async fn paused_time_moves_the_applications_clock() {
    #[derive(Default, Serialize, Deserialize)]
    struct Timed {
        order: String,
    }
    let stores = InMemoryStores::new();
    let app = stores
        .install(
            SnodraMessaging::builder(stores.config("zero"))
                .await
                .unwrap(),
        )
        .saga::<Timed>("zero.Timed", |saga| {
            saga.start_on::<OrderPlaced>(
                "order",
                |e| Some(e.order.clone()),
                |s, e, ctx| {
                    s.order = e.order.clone();
                    ctx.schedule_after(Duration::from_secs(3600), "timeout", None::<&()>)?;
                    Ok(())
                },
            )
            .on_deadline::<serde::de::IgnoredAny>("timeout", |s, _, ctx| {
                ctx.publish(
                    &OrderConfirmed {
                        order: s.order.clone(),
                    },
                    None,
                )?;
                ctx.end();
                Ok(())
            })
        })
        .start()
        .await
        .unwrap();
    stores
        .publish(
            &OrderPlaced {
                order: "o-1".into(),
            },
            Some("o-1"),
        )
        .await
        .unwrap();
    tokio::time::timeout(WITHIN, async {
        while stores.store().contents().deadlines.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the saga's deadline is scheduled");
    tokio::time::sleep(Duration::from_secs(3000)).await;
    assert!(
        stores.published::<OrderConfirmed>().is_empty(),
        "not before the hour"
    );
    tokio::time::sleep(Duration::from_secs(700)).await;
    let fired = stores
        .wait_published::<OrderConfirmed>(1, WITHIN)
        .await
        .unwrap();
    assert_eq!(
        fired,
        [OrderConfirmed {
            order: "o-1".into()
        }]
    );
    app.shutdown().await;
}

/// With the hub turned off, nothing connects, and a command to another service fails at once,
/// naming the setting.
#[tokio::test]
async fn without_a_hub_a_command_fails_at_once_naming_the_setting() {
    let logs = support::logs::Captured::default();
    let _guard = logs.install();
    let stores = InMemoryStores::new();
    let app = stores
        .install(
            SnodraMessaging::builder(stores.config("zero"))
                .await
                .unwrap(),
        )
        .start()
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let refused = app
        .commands()
        .send_command(
            &PlaceOrder {
                order: "o-1".into(),
            },
            SendOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&refused, MessagingError::HubUnavailable(why) if why.contains("SNODRA_SERVER_ENABLED")),
        "{refused:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(1), "at once");
    assert!(!app.await_hub(Duration::from_millis(300)).await);
    app.shutdown().await;
    assert!(
        !logs.text().contains("127.0.0.1:1"),
        "nothing dials the hub address:\n{}",
        logs.text()
    );
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PlaceOrder {
    order: String,
}
message_type!(PlaceOrder, "zero.PlaceOrder");
impl snodra_messaging::Command for PlaceOrder {
    type Reply = ();
}

/// With the hub turned off, waiting for it answers `false` at once rather than at the timeout.
#[tokio::test]
async fn awaiting_a_hub_that_is_off_answers_false_at_once() {
    let stores = InMemoryStores::new();
    let app = stores
        .install(
            SnodraMessaging::builder(stores.config("zero"))
                .await
                .unwrap(),
        )
        .start()
        .await
        .unwrap();
    let started = std::time::Instant::now();
    assert!(!app.await_hub(Duration::from_secs(30)).await);
    assert!(!app.await_all_hubs(Duration::from_secs(30)).await);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "answered after {:?}",
        started.elapsed()
    );
    app.shutdown().await;
}
