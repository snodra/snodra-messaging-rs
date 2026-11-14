//! The message store is required by what keeps state (ADR 0028): an event handler, a saga, an
//! idempotent handler and publishing an event need one; and the event store by what reads or
//! writes events (ADR 0027): an event handler, a saga and publishing an event. A service with
//! commands and queries only runs without either.

mod support;

use std::time::Duration;

use kraken_messaging::proto::{self, reply, server_message};
use kraken_messaging::{
    Command, Config, Event, KrakenMessaging, KrakenMessagingBuilder, Query, message_type,
};
use serde::{Deserialize, Serialize};
use support::fake_hub::FakeHub;

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

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
    let mut config = Config::new("store-required", hub);
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

/// `start()` prepares the store's schema (MongoDB: its indexes, which create its collections),
/// unless `store_init_schema` is false: then the library creates nothing.
#[tokio::test]
async fn start_prepares_the_store_schema_unless_turned_off() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let start = |uri: String, init: bool| async move {
        let mut config = Config::new("store-required", "127.0.0.1:1");
        config.outbox_enabled = false;
        config.store_init_schema = init;
        KrakenMessaging::builder(config)
            .await
            .unwrap()
            .store(support::mongo_store_bare(&uri).await)
            .command::<Shout, _, _>(|c, _| async move { Ok(c.text) })
            .start()
            .await
            .unwrap()
    };

    let uri = mongo.uri_for("prepared");
    let app = start(uri.clone(), true).await;
    let db = support::mongo_db(&uri).await;
    let mut names = db.list_collection_names().await.unwrap();
    names.sort();
    for collection in [
        "dead_letters",
        "deadlines",
        "external_calls",
        "outbox",
        "processed_events",
        "saga_dispatch",
        "saga_dispatch_leases",
        "saga_failures",
        "sagas",
    ] {
        assert!(
            names.contains(&collection.to_string()),
            "{collection}: {names:?}"
        );
    }
    let outbox: Vec<String> = db
        .collection::<mongodb::bson::Document>("outbox")
        .list_index_names()
        .await
        .unwrap();
    assert!(
        outbox.contains(&"sentAt_1_createdAt_1_seq_1".to_string()),
        "{outbox:?}"
    );
    app.shutdown().await;

    let uri = mongo.uri_for("unprepared");
    let app = start(uri.clone(), false).await;
    let names = support::mongo_db(&uri)
        .await
        .list_collection_names()
        .await
        .unwrap();
    assert!(names.is_empty(), "{names:?}");
    app.shutdown().await;
}

/// A builder with a MongoDB store that is never reached and no event store.
async fn builder_without_event_store(hub: &str) -> KrakenMessagingBuilder {
    let mut config = Config::new("event-store-required", hub);
    config.outbox_enabled = false;
    config.store_init_schema = false;
    KrakenMessaging::builder(config)
        .await
        .unwrap()
        .store(support::mongo_store_unreached().await)
}

#[tokio::test]
async fn event_handler_without_event_store_fails_at_build() {
    let message = refused(
        builder_without_event_store("127.0.0.1:1")
            .await
            .event::<Happened, _, _>(|_, _| async { Ok(()) }),
    )
    .await;
    assert_eq!(
        message,
        "the event handler for sr.Happened needs an event store: add .event_store(...)"
    );
    let message = refused(
        builder_without_event_store("127.0.0.1:1")
            .await
            .event_in::<Happened, _, _>("projections", |_, _| async { Ok(()) }),
    )
    .await;
    assert_eq!(
        message,
        "the event handler for sr.Happened in processor 'projections' needs an event store: add .event_store(...)"
    );
}

#[tokio::test]
async fn saga_without_event_store_fails_at_build() {
    let message = refused(
        builder_without_event_store("127.0.0.1:1")
            .await
            .saga::<Tally>("tally", |d| {
                d.start_on::<Happened>(
                    "n",
                    |e| Some(e.n.to_string()),
                    |s, _, _| {
                        s.n += 1;
                        Ok(())
                    },
                )
            }),
    )
    .await;
    assert_eq!(
        message,
        "saga 'tally' needs an event store: add .event_store(...)"
    );
}

/// A service with a store but no event store starts (its commands need neither), and publishing
/// an event fails naming the missing event store: nothing would relay its outbox.
#[tokio::test]
async fn publishing_without_event_store_fails() {
    let hub = FakeHub::start().await;
    let app = builder_without_event_store(&hub.address())
        .await
        .command::<Shout, _, _>(|c, _| async move { Ok(c.text) })
        .start()
        .await
        .expect("commands need no event store");
    let e = app
        .events()
        .publish(&Happened { n: 1 }, None)
        .await
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "publishing an event needs an event store: add .event_store(...)"
    );
    app.shutdown().await;
    hub.stop();
}

/// Mode `event-store` needs an event store with a native dead letter.
#[tokio::test]
async fn dead_letter_mode_event_store_needs_the_capability() {
    let mut config = Config::new("event-store-required", "127.0.0.1:1");
    config.store_init_schema = false;
    config.dead_letter_mode = kraken_messaging::config::DeadLetterMode::EventStore;
    let events = std::sync::Arc::new(support::event_store::Log::new(vec![
        kraken_messaging::event_store::Capability::Replay,
    ]));
    let message = refused(
        KrakenMessaging::builder(config)
            .await
            .unwrap()
            .store(support::mongo_store_unreached().await)
            .event_store(events)
            .event::<Happened, _, _>(|_, _| async { Ok(()) }),
    )
    .await;
    assert!(
        message.contains("event-store") && message.contains("native-dead-letter"),
        "{message}"
    );
}
