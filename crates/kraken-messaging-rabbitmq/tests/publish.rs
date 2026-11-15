//! A publisher connection that confirms nothing in time (the broker paused: as a connection
//! whose read task stopped) is replaced: once the broker answers again the next publish goes
//! through on a new connection, and replacements do not pile connections up.

mod support;

use std::collections::HashMap;
use std::time::Duration;

use kraken_messaging::event_store::{EventStore, EventStoreError, OutgoingEvent};
use kraken_messaging_rabbitmq::{RabbitEventStore, STORE_CONNECTION};

fn event(source: &str) -> OutgoingEvent {
    let id = uuid::Uuid::new_v4().to_string();
    OutgoingEvent {
        source: source.to_string(),
        headers: HashMap::from([("id".to_string(), id.clone())]),
        id,
        type_name: "publish.Event".into(),
        key: Some("k".into()),
        payload: b"{}".to_vec(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publisher_connection_that_confirms_nothing_is_replaced() {
    let rabbit = support::rabbitmq().await;
    let source = "publish.events";
    rabbit.super_stream(source, 2).await;
    let store =
        RabbitEventStore::new(rabbit.config().confirm_timeout(Duration::from_secs(2))).unwrap();
    store.publish(&event(source)).await.unwrap();
    for round in 0..3 {
        rabbit.container.pause().await.unwrap();
        let stalled = store.publish(&event(source)).await;
        assert!(
            matches!(&stalled, Err(EventStoreError::Failed(m)) if m.contains("replaced")),
            "round {round}: {stalled:?}"
        );
        rabbit.container.unpause().await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            match store.publish(&event(source)).await {
                Ok(()) => break,
                Err(e) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "round {round}: no publish after the broker answered again: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }
    // The store holds a bounded number (the seed and one leader), and the broker sees the
    // replaced connections go.
    assert!(
        store.publishing_connections().await <= 2,
        "{} connections held",
        store.publishing_connections().await
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let open = rabbit.connections_named(STORE_CONNECTION).await;
        if open <= 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{open} store connections still open at the broker"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    store.close().await;
}
