//! End to end with two real hubs (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`, ADR 0017) and the
//! multi-hub configuration (every instance on both hubs): a subscription gets its initial
//! result from one owner instance and the updates of every instance of every owner (exclusive and
//! shared types), a clean completion of the last owner ends it, a cancel (or a drop) mid-way
//! reaches every owner instance, `max_results` ends it from the hub, an owner instance at its
//! subscription limit fails it with `HandlerBusy`, and an owner's shutdown completes its
//! subscriptions with a clean end.
//!
//! The hub's `kraken_hub_subscriptions_open` gauge is an OTLP metric (nothing to scrape in a
//! test), so "the hub released the subscription" is observed where the gauge moves with it: the
//! hub sends `Cancel{<id>#n}` to every target still serving and closes the gauge for each in
//! the same step, so a target that stopped serving (its emitter has no subscription left) is the
//! gauge at 0.
mod support;

use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::BoxStream;
use kraken_messaging::hub::SubscriptionOptions;
use kraken_messaging::{Config, KrakenMessaging, MessagingError, SubscriptionQuery, message_type};
use serde::{Deserialize, Serialize};
use support::containers::{HubContainer, HubOptions, free_port, hub_with, mongo};

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Serialize, Deserialize)]
struct Prices {
    symbol: String,
}
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Quote {
    symbol: String,
    price: u32,
}
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Tick {
    from: String,
    price: u32,
}
message_type!(Prices, "e2e.Prices");
impl SubscriptionQuery for Prices {
    type Initial = Quote;
    type Update = Tick;
}

/// The same, shared by several services.
#[derive(Serialize, Deserialize)]
struct SharedPrices {
    symbol: String,
}
message_type!(SharedPrices, "e2e.SharedPrices");
impl SubscriptionQuery for SharedPrices {
    type Initial = Quote;
    type Update = Tick;
}

const IDS: [&str; 2] = ["e2e-p4c-1", "e2e-p4c-2"];

async fn two_hubs() -> (HubContainer, HubContainer) {
    let start =
        |id: &'static str| hub_with(HubOptions::new().port(free_port()).env("KRAKEN_HUB_ID", id));
    tokio::join!(start(IDS[0]), start(IDS[1]))
}

fn config(service: &str, hubs: &str, mongo: &str) -> Config {
    let mut c = Config::new(service, hubs, "127.0.0.1:1", mongo);
    c.outbox_enabled = false;
    c.consumer_enabled = false;
    c.reconnect_backoff_min = Duration::from_millis(100);
    c.max_concurrent_handlers = 8;
    c
}

/// An instance of `service` owning `e2e.Prices` (exclusive) whose initial quote is `price`.
async fn owner(cfg: Config, price: u32) -> KrakenMessaging {
    KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .subscription_query::<Prices, _, _>(move |q, _| async move {
            Ok(Quote {
                symbol: q.symbol,
                price,
            })
        })
        .start()
        .await
        .unwrap()
}

/// An instance of `service` owning `e2e.SharedPrices` (shared).
async fn shared_owner(cfg: Config, price: u32) -> KrakenMessaging {
    KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .shared_subscription_query::<SharedPrices, _, _>(move |q, _| async move {
            Ok(Quote {
                symbol: q.symbol,
                price,
            })
        })
        .start()
        .await
        .unwrap()
}

async fn caller(cfg: Config) -> KrakenMessaging {
    KrakenMessaging::builder(cfg)
        .await
        .unwrap()
        .start()
        .await
        .unwrap()
}

fn tick(from: &str, price: u32) -> Tick {
    Tick {
        from: from.into(),
        price,
    }
}

/// Polls `check` every 50 ms for at most 10 s.
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

type Updates = BoxStream<'static, Result<Tick, MessagingError>>;

async fn next(updates: &mut Updates) -> Result<Tick, MessagingError> {
    tokio::time::timeout(Duration::from_secs(10), updates.next())
        .await
        .expect("an update within 10 s")
        .expect("the subscription is open")
}

/// The updates until the end or the first error.
async fn rest(updates: &mut Updates) -> (Vec<Tick>, Option<MessagingError>) {
    let mut ticks = vec![];
    loop {
        match tokio::time::timeout(Duration::from_secs(10), updates.next())
            .await
            .expect("the end within 10 s")
        {
            Some(Ok(tick)) => ticks.push(tick),
            Some(Err(e)) => return (ticks, Some(e)),
            None => return (ticks, None),
        }
    }
}

#[tokio::test]
async fn the_initial_result_then_the_updates_of_both_owner_instances_then_completion() {
    require_docker!();
    let ((h1, h2), db) = tokio::join!(two_hubs(), mongo());
    let hubs = format!("{},{}", h1.address, h2.address);
    let o1 = owner(config("e2e-prices", &hubs, &db.uri_for("o1")), 101).await;
    let o2 = owner(config("e2e-prices", &hubs, &db.uri_for("o2")), 102).await;
    let c = caller(config("e2e-caller", &hubs, &db.uri_for("caller"))).await;
    for app in [&o1, &o2, &c] {
        assert!(app.await_all_hubs(Duration::from_secs(15)).await);
    }

    let sub = c
        .sender()
        .subscribe(
            &Prices {
                symbol: "acme".into(),
            },
            SubscriptionOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(sub.initial.symbol, "acme");
    assert!(
        [101, 102].contains(&sub.initial.price),
        "the initial result comes from one owner instance: {:?}",
        sub.initial
    );
    eventually("both owner instances subscribed", async || {
        o1.emitter().open_subscriptions() == 1 && o2.emitter().open_subscriptions() == 1
    })
    .await;
    assert_eq!(c.sender().in_flight(), 1, "pending until the end");

    let mut updates = sub.updates;
    // Each instance emits in order; the subscriber gets every instance's updates, per source
    // in order.
    for price in 1..=3 {
        o1.emitter()
            .emit::<Prices>(|q| q.symbol == "acme", &tick("o1", price));
        o2.emitter()
            .emit::<Prices>(|q| q.symbol == "acme", &tick("o2", 10 + price));
    }
    // Another symbol is nobody's update.
    o1.emitter()
        .emit::<Prices>(|q| q.symbol == "other", &tick("o1", 99));
    let mut got = vec![];
    for _ in 0..6 {
        got.push(next(&mut updates).await.unwrap());
    }
    for source in ["o1", "o2"] {
        let prices: Vec<u32> = got
            .iter()
            .filter(|t| t.from == source)
            .map(|t| t.price)
            .collect();
        let expected: Vec<u32> = if source == "o1" {
            vec![1, 2, 3]
        } else {
            vec![11, 12, 13]
        };
        assert_eq!(prices, expected, "{source}'s updates in order: {got:?}");
    }

    // One owner instance completes: the subscription goes on with the other.
    o1.emitter().complete::<Prices>(|_| true);
    o2.emitter().emit::<Prices>(|_| true, &tick("o2", 14));
    assert_eq!(next(&mut updates).await.unwrap(), tick("o2", 14));
    // The last one completes: a clean end.
    o2.emitter().complete::<Prices>(|_| true);
    let (ticks, error) = rest(&mut updates).await;
    assert!(ticks.is_empty() && error.is_none(), "{ticks:?} {error:?}");
    eventually("nothing pending at the caller", async || {
        c.sender().in_flight() == 0
    })
    .await;
    assert_eq!(
        (
            o1.emitter().open_subscriptions(),
            o2.emitter().open_subscriptions()
        ),
        (0, 0)
    );

    // An error of one owner instance ends it for the subscriber with that error.
    let sub = c
        .sender()
        .subscribe(
            &Prices {
                symbol: "acme".into(),
            },
            SubscriptionOptions::default(),
        )
        .await
        .unwrap();
    eventually("both subscribed again", async || {
        o1.emitter().open_subscriptions() == 1 && o2.emitter().open_subscriptions() == 1
    })
    .await;
    let mut updates = sub.updates;
    o1.emitter()
        .complete_exceptionally::<Prices>(|_| true, &MessagingError::Storage("feed down".into()));
    let (_, error) = rest(&mut updates).await;
    assert!(
        matches!(error, Some(MessagingError::RemoteHandler { ref message, .. }) if message.contains("feed down")),
        "{error:?}"
    );
    eventually(
        "the other owner instance cancelled by the hub",
        async || o2.emitter().open_subscriptions() == 0,
    )
    .await;

    c.shutdown().await;
    o1.shutdown().await;
    o2.shutdown().await;
}

#[tokio::test]
async fn a_cancel_mid_way_reaches_every_owner_instance_and_max_results_ends_it_from_the_hub() {
    require_docker!();
    let ((h1, h2), db) = tokio::join!(two_hubs(), mongo());
    let hubs = format!("{},{}", h1.address, h2.address);
    let o1 = owner(config("e2e-prices", &hubs, &db.uri_for("o1")), 1).await;
    let o2 = owner(config("e2e-prices", &hubs, &db.uri_for("o2")), 2).await;
    let c = caller(config("e2e-caller", &hubs, &db.uri_for("caller"))).await;
    for app in [&o1, &o2, &c] {
        assert!(app.await_all_hubs(Duration::from_secs(15)).await);
    }
    let acme = || Prices {
        symbol: "acme".into(),
    };
    let both_subscribed = async |n: usize| {
        eventually("both owner instances subscribed", async || {
            o1.emitter().open_subscriptions() == n && o2.emitter().open_subscriptions() == n
        })
        .await
    };
    let none_left = async || {
        eventually("every owner instance released it", async || {
            o1.emitter().open_subscriptions() == 0 && o2.emitter().open_subscriptions() == 0
        })
        .await
    };

    // A cancel after one update: every owner instance is told by the hub.
    let sub = c
        .sender()
        .subscribe(&acme(), SubscriptionOptions::default())
        .await
        .unwrap();
    both_subscribed(1).await;
    let mut updates = sub.updates;
    o1.emitter().emit::<Prices>(|_| true, &tick("o1", 1));
    assert_eq!(next(&mut updates).await.unwrap(), tick("o1", 1));
    drop(updates);
    none_left().await;
    // The owners keep running: a new subscription works.
    let sub = c
        .sender()
        .subscribe(&acme(), SubscriptionOptions::default())
        .await
        .unwrap();
    both_subscribed(1).await;
    sub.cancel();
    none_left().await;
    eventually("nothing pending at the caller", async || {
        c.sender().in_flight() == 0
    })
    .await;

    // max_results counts updates (not the initial result): the hub ends it and cancels the owners.
    let sub = c
        .sender()
        .subscribe(
            &acme(),
            SubscriptionOptions {
                max_results: 2,
                ..SubscriptionOptions::default()
            },
        )
        .await
        .unwrap();
    both_subscribed(1).await;
    let mut updates = sub.updates;
    for price in 1..=3 {
        o1.emitter().emit::<Prices>(|_| true, &tick("o1", price));
    }
    let (ticks, error) = rest(&mut updates).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(ticks, [tick("o1", 1), tick("o1", 2)]);
    none_left().await;

    c.shutdown().await;
    o1.shutdown().await;
    o2.shutdown().await;
}

#[tokio::test]
async fn an_owner_at_its_subscription_limit_fails_a_new_subscription_and_a_shutdown_ends_the_rest()
{
    require_docker!();
    let ((h1, h2), db) = tokio::join!(two_hubs(), mongo());
    let hubs = format!("{},{}", h1.address, h2.address);
    let o1 = owner(config("e2e-prices", &hubs, &db.uri_for("o1")), 1).await;
    let mut limited = config("e2e-prices", &hubs, &db.uri_for("o2"));
    limited.hub_max_subscriptions = 1;
    let o2 = owner(limited, 2).await;
    let c = caller(config("e2e-caller", &hubs, &db.uri_for("caller"))).await;
    for app in [&o1, &o2, &c] {
        assert!(app.await_all_hubs(Duration::from_secs(15)).await);
    }
    let acme = Prices {
        symbol: "acme".into(),
    };
    let first = c
        .sender()
        .subscribe(&acme, SubscriptionOptions::default())
        .await
        .unwrap();
    eventually("both subscribed", async || {
        o1.emitter().open_subscriptions() == 1 && o2.emitter().open_subscriptions() == 1
    })
    .await;

    // The second one: o2 refuses it (ReplyEnd{RESOURCE_EXHAUSTED}); the hub ends it for the
    // subscriber (the error is the call's, or comes right after the initial result) and
    // cancels o1's target of it. The first one is untouched.
    let error = match c
        .sender()
        .subscribe(&acme, SubscriptionOptions::default())
        .await
    {
        Err(e) => e,
        Ok(second) => {
            let mut updates = second.updates;
            rest(&mut updates).await.1.expect("the refusal")
        }
    };
    assert!(
        matches!(error, MessagingError::HandlerBusy(ref t) if t == "e2e.Prices"),
        "{error:?}"
    );
    eventually("o1 holds only the first one again", async || {
        o1.emitter().open_subscriptions() == 1 && o2.emitter().open_subscriptions() == 1
    })
    .await;
    let mut updates = first.updates;
    o1.emitter().emit::<Prices>(|_| true, &tick("o1", 7));
    assert_eq!(next(&mut updates).await.unwrap(), tick("o1", 7));

    // Shutting the owners down completes their subscriptions with a clean end (without it the
    // last owner leaving would be HANDLER_DISCONNECTED).
    o1.shutdown().await;
    o2.shutdown().await;
    let (ticks, error) = rest(&mut updates).await;
    assert!(ticks.is_empty(), "{ticks:?}");
    assert!(error.is_none(), "a clean end, not a disconnect: {error:?}");
    eventually("nothing pending at the caller", async || {
        c.sender().in_flight() == 0
    })
    .await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_shared_subscription_type_reaches_every_instance_of_every_owner_service() {
    require_docker!();
    let ((h1, h2), db) = tokio::join!(two_hubs(), mongo());
    let hubs = format!("{},{}", h1.address, h2.address);
    let a = shared_owner(config("e2e-prices-a", &hubs, &db.uri_for("a")), 1).await;
    let b = shared_owner(config("e2e-prices-b", &hubs, &db.uri_for("b")), 2).await;
    let c = caller(config("e2e-caller", &hubs, &db.uri_for("caller"))).await;
    for app in [&a, &b, &c] {
        assert!(app.await_all_hubs(Duration::from_secs(15)).await);
    }
    let sub = c
        .sender()
        .subscribe(
            &SharedPrices {
                symbol: "acme".into(),
            },
            SubscriptionOptions::default(),
        )
        .await
        .unwrap();
    assert!([1, 2].contains(&sub.initial.price), "{:?}", sub.initial);
    eventually("both services subscribed", async || {
        a.emitter().open_subscriptions() == 1 && b.emitter().open_subscriptions() == 1
    })
    .await;
    let mut updates = sub.updates;
    a.emitter().emit::<SharedPrices>(|_| true, &tick("a", 1));
    b.emitter().emit::<SharedPrices>(|_| true, &tick("b", 2));
    let mut got = vec![
        next(&mut updates).await.unwrap().price,
        next(&mut updates).await.unwrap().price,
    ];
    got.sort();
    assert_eq!(got, [1, 2], "an update from each service");
    a.emitter().complete::<SharedPrices>(|_| true);
    b.emitter().complete::<SharedPrices>(|_| true);
    let (ticks, error) = rest(&mut updates).await;
    assert!(ticks.is_empty() && error.is_none(), "{ticks:?} {error:?}");
    c.shutdown().await;
    a.shutdown().await;
    b.shutdown().await;
}
