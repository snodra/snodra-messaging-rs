//! End to end with the core: a service of two instances on the RabbitMQ Streams event store and
//! one in-memory message store (shared, as the instances of a service share their database),
//! against the real hub image (`SNODRA_SERVER_IMAGE`): events published through the outbox are
//! shared by the two instances, the hub pauses and resumes the processor and resets it to
//! `earliest` (everything replayed), and a dead letter lands in the dead-letter stream.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snodra_messaging::config::DeadLetterMode;
use snodra_messaging::event_store::{
    EventStore, SubscriptionEvent, SubscriptionSpec, dead_letter_source,
};
use snodra_messaging::proto::ProcessorState;
use snodra_messaging::store::memory::MemoryStore;
use snodra_messaging::{Config, Event, MessagingError, SnodraMessaging, message_type};
use snodra_messaging_rabbitmq::RabbitEventStore;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Noted {
    note: String,
}
message_type!(Noted, "e2e.Noted");
impl Event for Noted {}

/// At least 16 characters, as the hub requires.
const WRITE_TOKEN: &str = "e2e-write-token-0123456789";
const SERVICE: &str = "svc-e2e-rmq";
const SOURCE: &str = "e2e.rmq.events";

type Seen = Arc<Mutex<Vec<(&'static str, String)>>>;

async fn eventually<F, Fut>(what: &str, within: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = std::time::Instant::now();
    while !check().await {
        assert!(started.elapsed() < within, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// One instance of the service: processor `audit` records what it handled (and fails `bad-*`).
async fn instance(
    label: &'static str,
    hub: &support::Hub,
    rabbit: &support::RabbitMq,
    store: &MemoryStore,
    seen: &Seen,
) -> SnodraMessaging {
    let mut config = Config::new(SERVICE, &hub.address);
    config.events_source = SOURCE.to_string();
    config.consumer_max_attempts = 1;
    config.dead_letter_mode = DeadLetterMode::EventStore;
    config.hub_status_interval = Duration::from_millis(500);
    config.outbox_poll_interval = Duration::from_millis(100);
    let events = RabbitEventStore::new(rabbit.config()).expect("the event store");
    let seen = Arc::clone(seen);
    SnodraMessaging::builder(config)
        .await
        .unwrap()
        .store(Arc::new(store.clone()))
        .event_store(Arc::new(events))
        .event_in::<Noted, _, _>("audit", move |e, _| {
            let seen = Arc::clone(&seen);
            async move {
                if e.note.starts_with("bad-") {
                    return Err(MessagingError::Storage("handler failed: boom".into()));
                }
                seen.lock().unwrap().push((label, e.note));
                Ok(())
            }
        })
        .start()
        .await
        .unwrap()
}

/// `POST` on the processor's control route `path` with the write token.
async fn control(http: &reqwest::Client, hub: &support::Hub, path: &str, body: Value) -> Value {
    let url = format!(
        "{}/api/v1/services/{SERVICE}/processors/audit/{path}",
        hub.http
    );
    let response = http
        .post(url)
        .bearer_auth(WRITE_TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body: Value = response.json().await.unwrap_or(Value::Null);
    assert_eq!(status, 200, "{path}: {body}");
    body
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_service_of_two_instances_runs_on_rabbitmq_streams() {
    let rabbit = support::rabbitmq().await;
    rabbit.super_stream(SOURCE, 4).await;
    rabbit.stream(&dead_letter_source(SOURCE)).await;
    let hub = support::hub(WRITE_TOKEN).await;
    let store = MemoryStore::new();
    let seen: Seen = Arc::default();
    // Both instances must be members of the consumer group before the broker assigns the
    // partitions: joining one after the other can hand every partition to the first one, and
    // a single-active-consumer group does not rebalance on its own.
    let (a, b) = tokio::join!(
        instance("a", &hub, &rabbit, &store, &seen),
        instance("b", &hub, &rabbit, &store, &seen),
    );
    assert!(
        a.await_hub(Duration::from_secs(30)).await && b.await_hub(Duration::from_secs(30)).await
    );
    let count = |note: &str| {
        seen.lock()
            .unwrap()
            .iter()
            .filter(|(_, n)| n == note)
            .count()
    };
    let by = |label: &str| {
        seen.lock()
            .unwrap()
            .iter()
            .filter(|(l, _)| *l == label)
            .count()
    };

    // The two instances share the processor's partitions...
    let holds = |app: &SnodraMessaging| {
        app.processors()
            .into_iter()
            .any(|p| p.name == "audit" && !p.segments.is_empty())
    };
    eventually(
        "both instances hold partitions",
        Duration::from_secs(60),
        || async { holds(&a) && holds(&b) },
    )
    .await;
    // ... and the events published through the outbox.
    let notes: Vec<String> = (0..24).map(|i| format!("n-{i}")).collect();
    for note in &notes {
        a.events()
            .publish(&Noted { note: note.clone() }, Some(&format!("k-{note}")))
            .await
            .unwrap();
    }
    eventually("every event handled", Duration::from_secs(60), || async {
        notes.iter().all(|n| count(n) >= 1)
    })
    .await;
    assert!(
        notes.iter().all(|n| count(n) == 1),
        "each once: {:?}",
        seen.lock().unwrap()
    );
    assert!(
        by("a") > 0 && by("b") > 0,
        "both instances handle some: a {} b {}",
        by("a"),
        by("b")
    );

    // The hub pauses and resumes the processor on both instances.
    let http = reqwest::Client::new();
    let state_at_hub = || async {
        let view: Value = http
            .get(format!("{}/api/v1/processors", hub.http))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        view["services"]
            .as_array()
            .and_then(|all| all.iter().find(|s| s["serviceName"] == SERVICE))
            .and_then(|s| s["processors"].as_array().cloned())
            .and_then(|ps| ps.into_iter().find(|p| p["name"] == "audit"))
            .map(|p| (p["state"].as_str().unwrap_or_default().to_string(), p))
    };
    let raw = || async {
        http.get(format!("{}/api/v1/processors", hub.http))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    };
    let started = std::time::Instant::now();
    while !state_at_hub().await.is_some_and(|(s, _)| s == "RUNNING") {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the hub knows the processor: {}",
            raw().await
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let (_, view) = state_at_hub().await.unwrap();
    assert!(
        view.to_string().contains("rabbitmq-streams"),
        "the event store reported: {view}"
    );
    let paused = control(&http, &hub, "pause", json!({})).await;
    assert!(paused["acknowledged"].as_u64() >= Some(1), "{paused}");
    // The instance the hub did not reach reads the pause from the store on its next poll.
    let all_paused = || {
        a.processors()
            .into_iter()
            .chain(b.processors())
            .filter(|p| p.name == "audit")
            .all(|p| p.state == ProcessorState::ProcessorPaused as i32)
    };
    eventually("both instances paused", Duration::from_secs(20), || async {
        all_paused()
    })
    .await;
    a.events()
        .publish(&Noted { note: "p-1".into() }, Some("k-p-1"))
        .await
        .unwrap();
    // p-1 reached the event store (the outbox relayed it: confirmed by the broker)...
    eventually(
        "p-1 relayed to the event store",
        Duration::from_secs(30),
        || async {
            store.contents().outbox.iter().any(|r| {
                r.sent_at_ms.is_some() && String::from_utf8_lossy(&r.entry.payload).contains("p-1")
            })
        },
    )
    .await;
    // ...and stays unhandled: a negative check over a short window (one second), on both
    // instances, which have reported PAUSED.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(count("p-1"), 0, "held while paused");
    control(&http, &hub, "resume", json!({})).await;
    eventually(
        "p-1 handled after the resume",
        Duration::from_secs(30),
        || async { count("p-1") == 1 },
    )
    .await;

    // A handler failure in mode event-store: the letter lands in the dead-letter stream.
    a.events()
        .publish(
            &Noted {
                note: "bad-1".into(),
            },
            Some("k-bad-1"),
        )
        .await
        .unwrap();
    let reader = RabbitEventStore::new(rabbit.config()).unwrap();
    let mut letters = reader
        .subscribe(SubscriptionSpec {
            name: "e2e.letters".into(),
            source: dead_letter_source(SOURCE),
        })
        .await
        .unwrap();
    let letter = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let SubscriptionEvent::Delivery(d) = letters.next().await.unwrap() {
                return d;
            }
        }
    })
    .await
    .expect("a dead letter in the dead-letter stream");
    assert_eq!(
        letter
            .headers
            .get("snodra-dlt-processor")
            .map(String::as_str),
        Some("audit")
    );
    assert_eq!(
        letter.headers.get("type").map(String::as_str),
        Some("e2e.Noted")
    );
    assert_eq!(letter.key.as_deref(), Some("k-bad-1"));
    assert!(String::from_utf8_lossy(&letter.payload).contains("bad-1"));
    letters.close().await;

    // A reset to earliest through the hub: everything replayed once more.
    let reset = control(&http, &hub, "reset", json!({ "to": "earliest" })).await;
    assert_eq!(reset["outcome"], "ok", "{reset}");
    assert!(
        reset["ranges"].as_array().is_some_and(|r| !r.is_empty()),
        "replay ranges: {reset}"
    );
    let all: Vec<String> = notes.iter().cloned().chain(["p-1".to_string()]).collect();
    eventually("the history replayed", Duration::from_secs(90), || async {
        all.iter().all(|n| count(n) == 2)
    })
    .await;
    eventually(
        "consuming again, not replaying",
        Duration::from_secs(60),
        || async {
            !a.processors()
                .into_iter()
                .chain(b.processors())
                .any(|p| p.replaying)
        },
    )
    .await;
    a.shutdown().await;
    b.shutdown().await;
    reader.close().await;
}
