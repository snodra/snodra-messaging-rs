//! The offset-tracking wire rules on a real broker, which the Kotlin add-on shares:
//!
//! - the value stored under a subscription's name, per partition stream, is the **next offset to
//!   read**: stored N, a new consumer's first delivery is offset N; a reset to 0 replays from 0;
//! - a subscription stores its position periodically (not per acknowledgement: every stored
//!   offset is an entry in the stream), and always at close and when a partition moves away.

mod support;

use std::collections::HashMap;
use std::time::Duration;

use kraken_messaging::event_store::{
    EventStore, OutgoingEvent, ReplayRange, Subscription, SubscriptionEvent, SubscriptionSpec,
};
use kraken_messaging_rabbitmq::{RabbitEventStore, StorePolicy};

fn event(source: &str, key: &str) -> OutgoingEvent {
    let id = uuid::Uuid::new_v4().to_string();
    OutgoingEvent {
        source: source.to_string(),
        headers: HashMap::from([
            ("type".to_string(), "offsets.Event".to_string()),
            ("id".to_string(), id.clone()),
        ]),
        id,
        type_name: "offsets.Event".into(),
        key: Some(key.to_string()),
        payload: b"{}".to_vec(),
    }
}

/// The next delivery (assignment changes skipped), at most 30 s.
async fn delivery(sub: &mut Box<dyn Subscription>) -> kraken_messaging::event_store::Delivery {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let SubscriptionEvent::Delivery(d) = sub.next().await.expect("next") {
                return d;
            }
        }
    })
    .await
    .expect("a delivery within 30 s")
}

/// Nothing delivered for `window` (a negative check, bounded by its window).
async fn nothing_for(sub: &mut Box<dyn Subscription>, window: Duration) -> bool {
    tokio::time::timeout(window, async {
        loop {
            if let SubscriptionEvent::Delivery(d) = sub.next().await.expect("next") {
                return d;
            }
        }
    })
    .await
    .is_err()
}

fn spec(source: &str) -> SubscriptionSpec {
    SubscriptionSpec {
        name: format!("offsets.{}", uuid::Uuid::new_v4().simple()),
        source: source.to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stored_value_is_the_next_offset_to_read() {
    let rabbit = support::rabbitmq().await;
    let source = "offsets.next";
    rabbit.stream(source).await;
    let store = RabbitEventStore::new(rabbit.config()).unwrap();
    for _ in 0..5 {
        store.publish(&event(source, "k")).await.unwrap();
    }
    let spec = spec(source);
    // Stored 3 (by a reset of the subscription, as the Kotlin add-on would read it): the first
    // delivery is offset 3.
    let apply = |from: &'static str| {
        let admin = store.admin(&spec).unwrap();
        let ranges = vec![ReplayRange {
            source: source.to_string(),
            segment: "0".into(),
            from: from.into(),
            until: String::new(),
        }];
        async move {
            tokio::task::spawn_blocking(move || admin.apply(&ranges))
                .await
                .unwrap()
        }
    };
    apply("3").await.unwrap();
    let mut sub = store.subscribe(spec.clone()).await.unwrap();
    assert_eq!(delivery(&mut sub).await.position, "3");
    sub.close().await;
    // A reset to 0 replays from offset 0 (a "last processed" convention could not say it).
    apply("0").await.unwrap();
    let mut sub = store.subscribe(spec.clone()).await.unwrap();
    assert_eq!(delivery(&mut sub).await.position, "0");
    sub.close().await;
    store.close().await;
}

/// Acknowledging every one of K events writes far fewer than K offset-tracking entries (each
/// takes an offset in the stream), and the position is stored at close all the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledgements_are_stored_periodically_and_at_close() {
    const K: u64 = 200;
    let rabbit = support::rabbitmq().await;
    let source = "offsets.periodic";
    rabbit.stream(source).await;
    let store = RabbitEventStore::new(rabbit.config()).unwrap();
    for _ in 0..K {
        store.publish(&event(source, "k")).await.unwrap();
    }
    let spec = spec(source);
    let mut sub = store.subscribe(spec.clone()).await.unwrap();
    for expected in 0..K {
        let d = delivery(&mut sub).await;
        assert_eq!(d.position, expected.to_string());
        sub.ack(&d.segment, &d.position).unwrap();
    }
    sub.close().await;
    // The next event's offset tells how many tracking entries the stream got meanwhile.
    store.publish(&event(source, "k")).await.unwrap();
    let mut again = store.subscribe(spec.clone()).await.unwrap();
    let next = delivery(&mut again).await;
    let tracking = next.position.parse::<u64>().unwrap() - K;
    assert!(
        tracking <= 3,
        "{tracking} tracking entries for {K} acknowledgements (one per acknowledgement would be {K})"
    );
    assert!(
        nothing_for(&mut again, Duration::from_millis(500)).await,
        "the acknowledged events do not come again"
    );
    again.close().await;
    store.close().await;
}

/// A partition moving to a second instance carries the first one's last acknowledged position
/// along, though the periodic store had not run yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partition_moving_away_stores_its_position_first() {
    let rabbit = support::rabbitmq().await;
    let source = "offsets.moving";
    rabbit.super_stream(source, 2).await;
    let config = rabbit.config().offset_store(StorePolicy {
        messages: 1_000_000,
        interval: Duration::from_secs(3600),
    });
    let first = RabbitEventStore::new(config.clone()).unwrap();
    // Two keys of each partition (0: order-1, 2: ... per the routing table of 2 partitions).
    let keys: Vec<String> = (0..40).map(|i| format!("key-{i}")).collect();
    for key in &keys {
        first.publish(&event(source, key)).await.unwrap();
    }
    let spec = spec(source);
    let mut a = first.subscribe(spec.clone()).await.unwrap();
    for _ in 0..keys.len() {
        let d = delivery(&mut a).await;
        a.ack(&d.segment, &d.position).unwrap();
    }
    let second = RabbitEventStore::new(config).unwrap();
    let mut b = second.subscribe(spec.clone()).await.unwrap();
    // B gets one partition and nothing A acknowledged.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut assigned = false;
    while !assigned {
        assert!(
            tokio::time::Instant::now() < deadline,
            "B was never assigned a partition"
        );
        tokio::select! {
            change = b.next() => match change.expect("next") {
                SubscriptionEvent::Assigned(s) if !s.is_empty() => assigned = true,
                SubscriptionEvent::Delivery(d) => panic!("an acknowledged event again: {d:?}"),
                _ => {}
            },
            _ = a.next() => {}
        }
    }
    assert!(
        nothing_for(&mut b, Duration::from_secs(1)).await,
        "the acknowledged events do not come again on the partition that moved"
    );
    a.close().await;
    b.close().await;
    first.close().await;
    second.close().await;
}
