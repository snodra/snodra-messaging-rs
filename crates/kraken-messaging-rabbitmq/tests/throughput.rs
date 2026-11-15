//! Throughput on a real broker, as the Kotlin Kafka add-on measures it: 10 000 events published
//! the relay's way (one at a time, each awaiting its confirm), then read by one subscription that
//! acknowledges each and does nothing else. Consuming must reach 3 000 events/s, publishing
//! 500/s (a regression guard only); both rates are printed.

mod support;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use kraken_messaging::event_store::{
    EventStore, OutgoingEvent, SubscriptionEvent, SubscriptionSpec,
};
use kraken_messaging_rabbitmq::RabbitEventStore;

const EVENTS: usize = 10_000;
/// Events per second a no-op reader reaches at least.
const CONSUME_FLOOR: f64 = 3_000.0;
/// Events per second published one at a time, each confirmed: a floor that only guards against
/// a regression (the rate is reported; it depends on the round trip to the broker).
const PUBLISH_FLOOR: f64 = 500.0;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_no_op_reader_consumes_at_least_3000_events_per_second() {
    let rabbit = support::rabbitmq().await;
    let source = "throughput.events";
    rabbit.super_stream(source, 4).await;
    let store = RabbitEventStore::new(rabbit.config()).unwrap();
    let events: Vec<OutgoingEvent> = (0..EVENTS)
        .map(|i| {
            let id = uuid::Uuid::new_v4().to_string();
            OutgoingEvent {
                source: source.to_string(),
                headers: HashMap::from([
                    ("type".to_string(), "throughput.Event".to_string()),
                    ("id".to_string(), id.clone()),
                ]),
                id,
                type_name: "throughput.Event".into(),
                key: Some(format!("key-{}", i % 64)),
                payload: format!("{{\"n\":{i}}}").into_bytes(),
            }
        })
        .collect();
    let started = Instant::now();
    for e in &events {
        store.publish(e).await.unwrap();
    }
    let publishing = started.elapsed();

    let mut sub = store
        .subscribe(SubscriptionSpec {
            name: format!("throughput.{}", uuid::Uuid::new_v4().simple()),
            source: source.to_string(),
        })
        .await
        .unwrap();
    // From the first delivery on: the subscription's join is not consuming.
    let mut seen = 0;
    let mut first = None;
    let deadline = Instant::now() + Duration::from_secs(120);
    while seen < EVENTS {
        assert!(Instant::now() < deadline, "{seen} of {EVENTS} within 120 s");
        if let SubscriptionEvent::Delivery(d) = sub.next().await.unwrap() {
            first.get_or_insert_with(Instant::now);
            sub.ack(&d.segment, &d.position).unwrap();
            seen += 1;
        }
    }
    let consuming = first.unwrap().elapsed();
    sub.close().await;
    store.close().await;
    let rate = |d: Duration| EVENTS as f64 / d.as_secs_f64();
    eprintln!(
        "throughput: published {EVENTS} in {publishing:?} ({:.0}/s, one at a time, each confirmed); \
         consumed in {consuming:?} ({:.0}/s)",
        rate(publishing),
        rate(consuming)
    );
    assert!(
        rate(publishing) >= PUBLISH_FLOOR,
        "published {:.0} events/s, under {PUBLISH_FLOOR}",
        rate(publishing)
    );
    assert!(
        rate(consuming) >= CONSUME_FLOOR,
        "consumed {:.0} events/s, under {CONSUME_FLOOR}",
        rate(consuming)
    );
}
