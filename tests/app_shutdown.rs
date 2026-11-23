//! The order of a graceful shutdown: the empty `Register` (its acks awaited at most 1 s), then
//! every subscription served here ends, then the handlers in flight get at most 5 s, then each
//! hub stream is half-closed (the hub gets at most 2 s).
mod support;

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use snodra_messaging::proto::{self, ServerMessage, server_message};
use snodra_messaging::{Config, Query, SnodraMessaging, SubscriptionQuery, message_type};
use support::fake_hub::FakeHub;

#[derive(Serialize, Deserialize)]
struct Watch {}
message_type!(Watch, "shutdown.Watch");
impl SubscriptionQuery for Watch {
    type Initial = ();
    type Update = ();
}

#[derive(Serialize, Deserialize)]
struct Hang {}
message_type!(Hang, "shutdown.Hang");
impl Query for Hang {
    type Reply = ();
}

fn request(id: &str, type_name: &str, mode: proto::RequestMode) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Request(proto::Request {
            id: id.into(),
            kind: proto::RequestKind::Query as i32,
            r#type: type_name.into(),
            payload: b"{}".to_vec(),
            mode: mode as i32,
            ..Default::default()
        })),
    }
}

/// A subscriber is told at once that the instance leaves, not after a slow handler; a handler
/// that does not finish is waited for 5 s, not 10.
#[tokio::test]
async fn subscriptions_end_before_the_handler_drain_which_lasts_at_most_five_seconds() {
    let hub = FakeHub::start().await;
    let mut config = Config::new("svc-shutdown", &hub.address());
    config.reconnect_backoff_min = Duration::from_millis(50);
    let app = SnodraMessaging::builder(config)
        .await
        .unwrap()
        .subscription_query::<Watch, _, _>(|_, _| async { Ok(()) })
        .query::<Hang, _, _>(|_, _| async {
            std::future::pending::<()>().await;
            Ok(())
        })
        .start()
        .await
        .unwrap();
    assert!(app.await_hub(Duration::from_secs(5)).await);
    hub.send_to_clients(request(
        "w#1",
        "shutdown.Watch",
        proto::RequestMode::Subscription,
    ))
    .await;
    hub.send_to_clients(request("h", "shutdown.Hang", proto::RequestMode::Unary))
        .await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while app.emitter().open_subscriptions() == 0 {
        assert!(
            Instant::now() < deadline,
            "the subscription was not admitted"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await; // the hanging handler runs
    let started = Instant::now();
    let shutdown = tokio::spawn(app.shutdown());
    while !hub.ends().iter().any(|(_, e)| e.request_id == "w#1") {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the subscription never ended"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let ended = started.elapsed();
    shutdown.await.unwrap();
    let took = started.elapsed();
    assert!(
        ended < Duration::from_secs(2),
        "the subscription ended {ended:?} into the shutdown: after the handler drain"
    );
    assert!(
        took >= Duration::from_secs(4) && took < Duration::from_secs(9),
        "the shutdown took {took:?}: the drain is 5 s"
    );
}
