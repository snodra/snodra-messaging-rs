//! End to end against the real hub image (`SNODRA_SERVER_IMAGE`, default `snodra-server:dev`): the hub's
//! HTTP API, with its write token, pauses and resumes a service's named processor, resets it to
//! `earliest` (the range replayed, the reset handler run once, the handler that disallows replay
//! skipped) and retries its dead letter (ADR 0023).

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snodra_messaging::processors::EventOptions;
use snodra_messaging::proto::ProcessorState;
use snodra_messaging::{Config, Event, MessagingError, SnodraMessaging, message_type};
use support::containers::{HubOptions, hub_with};

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Noted {
    note: String,
}
message_type!(Noted, "e2e.Noted");
impl Event for Noted {}

/// At least 16 characters, as the hub requires.
const WRITE_TOKEN: &str = "e2e-write-token-0123456789";
const SERVICE: &str = "svc-e2e-ctl";

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

/// `method` on the processor's control route `path`, with `token` as the bearer credential.
async fn call(
    http: &reqwest::Client,
    base: &str,
    method: reqwest::Method,
    path: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (u16, Value) {
    let url = format!("{base}/api/v1/services/{SERVICE}/processors/audit/{path}");
    let mut request = http.request(method, url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

#[tokio::test]
async fn the_hub_pauses_resets_and_retries_a_processor_with_its_write_token() {
    require_docker!();
    let hub =
        hub_with(HubOptions::new().env("SNODRA_SERVER_DASHBOARD_WRITE_TOKEN", WRITE_TOKEN)).await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let topic = "e2e.ctl.events";
    let mut config = Config::new(SERVICE, &hub.address);
    config.events_source = topic.to_string();
    config.outbox_enabled = false;
    config.consumer_max_attempts = 1;
    config.hub_status_interval = Duration::from_millis(500);
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let notified: Arc<Mutex<Vec<String>>> = Arc::default();
    let failing = Arc::new(Mutex::new(true));
    let resets = Arc::new(AtomicUsize::new(0));
    let (s, f, n, r) = (
        Arc::clone(&seen),
        Arc::clone(&failing),
        Arc::clone(&notified),
        Arc::clone(&resets),
    );
    let app = SnodraMessaging::builder(config)
        .await
        .unwrap()
        .store(support::mongo_store(&mongo.uri_for("ctl")).await)
        .event_store(support::kafka_events(&kafka.bootstrap))
        .event_in::<Noted, _, _>("audit", move |e, _| {
            let (seen, failing) = (Arc::clone(&s), Arc::clone(&f));
            async move {
                if *failing.lock().unwrap() && e.note == "bad-1" {
                    return Err(MessagingError::Storage("handler failed: boom".into()));
                }
                seen.lock().unwrap().push(e.note);
                Ok(())
            }
        })
        .event_with::<Noted, _, _>(
            EventOptions::new().processor("audit").disallow_replay(),
            move |e, _| {
                n.lock().unwrap().push(e.note);
                async { Ok(()) }
            },
        )
        .reset_handler("audit", move |_| {
            r.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .start()
        .await
        .unwrap();
    assert!(app.await_hub(Duration::from_secs(30)).await);
    let p: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap)
        .create()
        .unwrap();
    let produce = |note: &'static str| {
        let p = p.clone();
        async move {
            let value = serde_json::to_vec(&Noted { note: note.into() }).unwrap();
            let headers = OwnedHeaders::new()
                .insert(Header {
                    key: "type",
                    value: Some("e2e.Noted"),
                })
                .insert(Header {
                    key: "id",
                    value: Some(note),
                });
            p.send(
                FutureRecord::to(topic)
                    .key(note)
                    .payload(&value)
                    .headers(headers),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        }
    };
    let count = |note: &str| seen.lock().unwrap().iter().filter(|n| *n == note).count();
    for note in ["n-1", "n-2"] {
        produce(note).await;
    }
    eventually("n-1 and n-2 handled", Duration::from_secs(60), || async {
        count("n-1") == 1 && count("n-2") == 1
    })
    .await;
    let http = reqwest::Client::new();
    let processors = format!("{}/api/v1/processors", hub.http);
    let state_at_hub = || async {
        let view: Value = http
            .get(&processors)
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
            .map(|p| p["state"].as_str().unwrap_or_default().to_string())
    };
    eventually(
        "the hub knows the processor",
        Duration::from_secs(30),
        || async { state_at_hub().await.as_deref() == Some("RUNNING") },
    )
    .await;

    // No credential, a wrong one: refused. The write token: paused everywhere.
    let post = reqwest::Method::POST;
    let (status, _) = call(&http, &hub.http, post.clone(), "pause", None, None).await;
    assert_eq!(status, 401);
    let (status, _) = call(&http, &hub.http, post.clone(), "pause", Some("wrong"), None).await;
    assert_eq!(status, 401);
    let (status, body) = call(
        &http,
        &hub.http,
        post.clone(),
        "pause",
        Some(WRITE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["acknowledged"], 1, "{body}");
    let local = || {
        ProcessorState::try_from(
            app.processors()
                .into_iter()
                .find(|p| p.name == "audit")
                .unwrap()
                .state,
        )
        .unwrap()
    };
    assert_eq!(local(), ProcessorState::ProcessorPaused);
    eventually(
        "the hub shows it PAUSED",
        Duration::from_secs(10),
        || async { state_at_hub().await.as_deref() == Some("PAUSED") },
    )
    .await;
    produce("n-3").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(count("n-3"), 0, "held while paused");
    let (status, body) = call(
        &http,
        &hub.http,
        post.clone(),
        "resume",
        Some(WRITE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    eventually("n-3 flows", Duration::from_secs(30), || async {
        count("n-3") == 1
    })
    .await;

    // A parked letter, listed and retried through the hub.
    produce("bad-1").await;
    eventually("bad-1 parked", Duration::from_secs(30), || async {
        app.dead_letters().count(Some("audit")).await.unwrap() == 1
    })
    .await;
    let (status, body) = call(
        &http,
        &hub.http,
        reqwest::Method::GET,
        "dead-letters?limit=10",
        Some(WRITE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["total"], 1, "{body}");
    let letter = body["letters"][0]["id"].as_str().unwrap().to_string();
    assert_eq!(letter, format!("audit:{topic}:0:3"));
    assert_eq!(body["letters"][0]["reason"], "HANDLER_FAILED");
    *failing.lock().unwrap() = false;
    let (status, body) = call(
        &http,
        &hub.http,
        post.clone(),
        &format!("dead-letters/{letter}/retry"),
        Some(WRITE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["result"], "Retried", "{body}");

    // A reset to earliest: everything replayed once, the reset handler once, the handler that
    // disallows replay not again.
    eventually("committed past bad-1", Duration::from_secs(30), || async {
        app.processors()
            .into_iter()
            .find(|p| p.name == "audit")
            .is_some_and(|p| p.segments.first().is_some_and(|x| x.committed == "4"))
    })
    .await;
    let (status, body) = call(
        &http,
        &hub.http,
        post.clone(),
        "reset",
        Some(WRITE_TOKEN),
        Some(json!({ "to": "earliest" })),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["outcome"], "ok", "{body}");
    assert_eq!(
        body["ranges"],
        json!([{ "segment": "0", "from": "0", "until": "4" }]),
        "{body}"
    );
    assert_eq!(body["resetHandlers"], 1, "{body}");
    assert_eq!(body["markersDeleted"], 4, "{body}");
    eventually("the range replayed", Duration::from_secs(60), || async {
        ["n-1", "n-2", "n-3", "bad-1"].iter().all(|n| count(n) == 2)
    })
    .await;
    assert_eq!(resets.load(Ordering::SeqCst), 1);
    let notified = notified.lock().unwrap().clone();
    assert_eq!(
        notified,
        ["n-1", "n-2", "n-3", "bad-1"],
        "once each (bad-1 by its retry), never by the replay"
    );
    // Consuming again (FAILING for a minute after bad-1's failure) and the range removed.
    eventually(
        "consuming again, not replaying",
        Duration::from_secs(30),
        || async {
            matches!(state_at_hub().await.as_deref(), Some("RUNNING" | "FAILING"))
                && !app.processors().into_iter().any(|p| p.replaying)
        },
    )
    .await;
    app.shutdown().await;
}
