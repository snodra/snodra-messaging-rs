//! The message store is required by what keeps state (ADR 0028): an event handler, a saga, an
//! idempotent handler and publishing an event need one; a service with commands and queries only
//! runs without.

mod support;

use std::time::Duration;

use kraken_messaging::proto::{self, reply, server_message};
use kraken_messaging::{
    Command, Config, Event, KrakenMessaging, KrakenMessagingBuilder, Query, message_type,
};
use serde::{Deserialize, Serialize};
use support::fake_hub::FakeHub;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Happened {
    n: u32,
}
message_type!(Happened, "sr.Happened");
impl Event for Happened {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Shout {
    text: String,
}
message_type!(Shout, "sr.Shout");
impl Command for Shout {
    type Reply = String;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reverse {
    text: String,
}
message_type!(Reverse, "sr.Reverse");
impl Query for Reverse {
    type Reply = String;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Tally {
    n: u32,
}

/// A builder that has not touched the network, with no store.
async fn builder(hub: &str) -> KrakenMessagingBuilder {
    let mut config = Config::new("store-required", hub, "127.0.0.1:1");
    config.outbox_enabled = false;
    KrakenMessaging::builder(config).await.unwrap()
}

async fn refused(builder: KrakenMessagingBuilder) -> String {
    match builder.start().await {
        Ok(_) => panic!("start() accepted a service that needs a store without one"),
        Err(e) => e.to_string(),
    }
}

#[tokio::test]
async fn event_handler_without_store_fails_at_build() {
    let message = refused(
        builder("127.0.0.1:1")
            .await
            .event::<Happened, _, _>(|_, _| async { Ok(()) }),
    )
    .await;
    assert_eq!(
        message,
        "the event handler for sr.Happened needs a message store: add .store(...)"
    );
    let message = refused(
        builder("127.0.0.1:1")
            .await
            .event_in::<Happened, _, _>("projections", |_, _| async { Ok(()) }),
    )
    .await;
    assert_eq!(
        message,
        "the event handler for sr.Happened in processor 'projections' needs a message store: add .store(...)"
    );
}

#[tokio::test]
async fn saga_without_store_fails_at_build() {
    let message = refused(builder("127.0.0.1:1").await.saga::<Tally>("tally", |d| {
        d.start_on::<Happened>(
            "n",
            |e| Some(e.n.to_string()),
            |s, _, _| {
                s.n += 1;
                Ok(())
            },
        )
    }))
    .await;
    assert_eq!(
        message,
        "saga 'tally' needs a message store: add .store(...)"
    );
}

#[tokio::test]
async fn commands_and_queries_need_no_store() {
    let hub = FakeHub::start().await;
    let app = builder(&hub.address())
        .await
        .command::<Shout, _, _>(|c, _| async move { Ok(c.text.to_uppercase()) })
        .query::<Reverse, _, _>(|q, _| async move { Ok(q.text.chars().rev().collect()) })
        .start()
        .await
        .expect("a service of commands and queries starts without a store");
    assert!(app.await_hub(Duration::from_secs(10)).await);

    for (id, kind, type_name, payload) in [
        (
            "c-1",
            proto::RequestKind::Command,
            "sr.Shout",
            r#"{"text":"hi"}"#,
        ),
        (
            "q-1",
            proto::RequestKind::Query,
            "sr.Reverse",
            r#"{"text":"abc"}"#,
        ),
    ] {
        hub.send_to_clients(proto::ServerMessage {
            message: Some(server_message::Message::Request(proto::Request {
                id: id.into(),
                kind: kind as i32,
                r#type: type_name.into(),
                payload: payload.as_bytes().to_vec(),
                timeout_ms: 5_000,
                ..Default::default()
            })),
        })
        .await;
    }
    let started = std::time::Instant::now();
    let answers = loop {
        let replies = hub.replies().await;
        if replies.len() >= 2 {
            break replies;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "replies: {replies:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let answer = |id: &str| {
        answers
            .iter()
            .find(|r| r.request_id == id)
            .and_then(|r| match &r.result {
                Some(reply::Result::Payload(p)) => Some(String::from_utf8_lossy(p).into_owned()),
                _ => None,
            })
    };
    assert_eq!(answer("c-1").as_deref(), Some(r#""HI""#));
    assert_eq!(answer("q-1").as_deref(), Some(r#""cba""#));

    // Publishing needs the store it does not have.
    let e = app
        .events()
        .publish(&Happened { n: 1 }, None)
        .await
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "publishing an event needs a message store: add .store(...)"
    );
    app.shutdown().await;
    hub.stop();
}
