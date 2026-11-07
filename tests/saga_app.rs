//! Saga, deadline and external wiring on the builder (S2 Task 13): through `KrakenMessaging`
//! alone, an event on Kafka starts a saga, the relay delivers its command to this service's own
//! idempotent adapter, a deadline fires, a webhook event reaches a saga, and a service that
//! registers none of it builds none of it. Mongo + Kafka + the in-process fake hub.

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_messaging::external::{ExternalSource, IdempotencyKey, IngressResult, TranslatedEvent};
use kraken_messaging::saga::store::{ExternalCallDocument, ExternalCallStatus, SagaDocument};
use kraken_messaging::saga::{EventOutcome, ExternalOutcome};
use kraken_messaging::{
    Command, Config, Event, KrakenMessaging, MessageContext, MessagingError, message_type,
};
use mongodb::bson::{Document, doc};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Shouted {
    job_id: String,
    text: String,
}
message_type!(Shouted, "app.Shouted");
impl Event for Shouted {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BannerPrinted {
    job_id: String,
}
message_type!(BannerPrinted, "app.BannerPrinted");
impl Event for BannerPrinted {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BannerFailed {
    job_id: String,
    reason: String,
}
message_type!(BannerFailed, "app.BannerFailed");
impl Event for BannerFailed {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrintBanner {
    job_id: String,
    text: String,
}
message_type!(PrintBanner, "app.PrintBanner");
impl Command for PrintBanner {
    type Reply = ();
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Banner {
    job_id: Option<String>,
    printed: bool,
    timed_out: bool,
}

const PARTNER_TOPIC: &str = "partner.printer.banner";

fn config(hub: &str, kafka: &str, mongo: &str) -> Config {
    let mut c = Config::new("svc-banner", hub, kafka, mongo);
    c.outbox_poll_interval = Duration::from_millis(100);
    c.reconnect_backoff_min = Duration::from_millis(100);
    c.reconnect_backoff_max = Duration::from_secs(2);
    c.sagas_relay_poll_interval = Duration::from_millis(100);
    c.deadlines_poll_interval = Duration::from_millis(100);
    c
}

async fn eventually<F, Fut>(what: &str, within: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = std::time::Instant::now();
    while !check().await {
        assert!(started.elapsed() < within, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn sagas(db: &mongodb::Database) -> Vec<SagaDocument> {
    use futures::TryStreamExt;
    db.collection::<SagaDocument>("sagas")
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn state(saga: &SagaDocument) -> serde_json::Value {
    serde_json::from_str(&saga.state).unwrap()
}

#[tokio::test]
async fn an_event_starts_a_saga_whose_command_reaches_the_idempotent_adapter_and_whose_deadline_fires()
 {
    require_docker!();
    let hub = FakeHub::start().await;
    let kafka = support::containers::kafka().await;
    let mongo = support::containers::mongo().await;
    let calls: Arc<Mutex<Vec<MessageContext>>> = Arc::new(Mutex::new(Vec::new()));

    let meter = support::metrics::TestMeter::new();
    let builder = KrakenMessaging::builder_with_meter(
        config(&hub.address(), &kafka.bootstrap, &mongo.uri_for("banner")),
        meter.meter(),
    )
    .await
    .unwrap();
    let recorded = Arc::clone(&calls);
    let app = builder
        // No ordinary event handler: the consumer runs for the saga alone.
        .saga::<Banner>("app.Banner", |d| {
            d.start_on::<Shouted>(
                "jobId",
                |e| Some(e.job_id.clone()),
                |s, e, ctx| {
                    s.job_id = Some(e.job_id.clone());
                    ctx.send(
                        &PrintBanner {
                            job_id: e.job_id.clone(),
                            text: e.text.to_uppercase(),
                        },
                        None,
                    )?;
                    // The second banner waits long enough for its webhook.
                    let wait = if e.text == "again" { 120 } else { 1 };
                    ctx.schedule_after(Duration::from_secs(wait), "banner-timeout", None::<&()>)?;
                    Ok(())
                },
            )
            .on::<BannerPrinted>(
                "jobId",
                |e| Some(e.job_id.clone()),
                |s, _, ctx| {
                    s.printed = true;
                    ctx.cancel_all("banner-timeout");
                    Ok(())
                },
            )
            .ending()
            .on_deadline::<serde::de::IgnoredAny>("banner-timeout", |s, _, ctx| {
                s.timed_out = true;
                ctx.publish(
                    &BannerFailed {
                        job_id: s.job_id.clone().unwrap_or_default(),
                        reason: "timeout".into(),
                    },
                    None,
                )?;
                ctx.end();
                Ok(())
            })
        })
        // The partner's replies on its own topic end a saga too.
        .external_source(
            "printer-banner",
            ExternalSource {
                properties: HashMap::from([(
                    "topic.metadata.refresh.interval.ms".to_string(),
                    "500".to_string(),
                )]),
                ..ExternalSource::new(PARTNER_TOPIC)
            },
            |record| {
                let reply: serde_json::Value =
                    serde_json::from_slice(record.payload().unwrap_or_default())?;
                let event = BannerPrinted {
                    job_id: reply["jobId"].as_str().unwrap_or_default().to_string(),
                };
                Ok(Some(TranslatedEvent::new(
                    &event,
                    reply["eventId"].as_str().map(str::to_string),
                )?))
            },
        )
        .idempotent_command::<PrintBanner, _, _>(IdempotencyKey::default(), false, move |_, ctx| {
            let recorded = Arc::clone(&recorded);
            async move {
                recorded.lock().unwrap().push(ctx);
                Ok(())
            }
        })
        .start()
        .await
        .unwrap();
    let db = app.database();
    assert!(app.await_hub(Duration::from_secs(10)).await);

    // -- event -> saga -> relayed local command -> idempotent adapter -> deadline
    app.events()
        .publish(
            &Shouted {
                job_id: "j1".into(),
                text: "hello".into(),
            },
            Some("j1"),
        )
        .await
        .unwrap();
    eventually(
        "the adapter to be called",
        Duration::from_secs(60),
        || async { !calls.lock().unwrap().is_empty() },
    )
    .await;
    let saga = sagas(&db).await.remove(0);
    let key = calls.lock().unwrap()[0].headers["idempotency-key"].clone();
    assert!(key.starts_with(&format!("{}:", saga.id)), "{key}");
    assert_eq!(calls.lock().unwrap()[0].headers["saga-id"], saga.id);
    eventually("the deadline to fire", Duration::from_secs(20), || async {
        sagas(&db).await.first().is_some_and(|s| s.ended)
    })
    .await;
    let saga = sagas(&db).await.remove(0);
    assert_eq!(state(&saga)["timedOut"], true);
    assert_eq!(calls.lock().unwrap().len(), 1, "the command went out once");
    let record = db
        .collection::<ExternalCallDocument>("external_calls")
        .find_one(doc! { "_id": &key })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, ExternalCallStatus::Succeeded);
    eventually(
        "BannerFailed to reach the outbox",
        Duration::from_secs(10),
        || async {
            db.collection::<Document>("outbox")
                .count_documents(doc! { "type": "app.BannerFailed" })
                .await
                .unwrap()
                == 1
        },
    )
    .await;

    // -- a webhook event reaches a saga through the ingress (the runtime is its second stage)
    app.events()
        .publish(
            &Shouted {
                job_id: "j2".into(),
                text: "again".into(),
            },
            Some("j2"),
        )
        .await
        .unwrap();
    eventually("the second saga", Duration::from_secs(30), || async {
        sagas(&db).await.len() == 2 && calls.lock().unwrap().len() == 2
    })
    .await;
    let accepted = app
        .ingress()
        .accept(
            "printer-banner",
            "evt-1",
            &BannerPrinted {
                job_id: "j2".into(),
            },
            &HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(accepted, IngressResult::Accepted);
    let j2 = sagas(&db)
        .await
        .into_iter()
        .find(|s| state(s)["jobId"] == "j2")
        .unwrap();
    assert!(j2.ended && state(&j2)["printed"] == true);
    assert_eq!(
        app.ingress()
            .accept(
                "printer-banner",
                "evt-1",
                &BannerPrinted {
                    job_id: "j2".into(),
                },
                &HashMap::new()
            )
            .await
            .unwrap(),
        IngressResult::Duplicate
    );

    // -- a record on the partner's topic reaches a saga through the external source
    app.events()
        .publish(
            &Shouted {
                job_id: "j3".into(),
                text: "again".into(),
            },
            Some("j3"),
        )
        .await
        .unwrap();
    eventually("the third saga", Duration::from_secs(30), || async {
        sagas(&db).await.len() == 3 && calls.lock().unwrap().len() == 3
    })
    .await;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap)
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to(PARTNER_TOPIC)
                .key("j3")
                .payload(r#"{"jobId":"j3","eventId":"p-1"}"#),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap();
    eventually(
        "the partner's reply to end j3",
        Duration::from_secs(60),
        || async {
            sagas(&db)
                .await
                .iter()
                .any(|s| state(s)["jobId"] == "j3" && s.ended && state(s)["printed"] == true)
        },
    )
    .await;
    assert_eq!(
        db.collection::<Document>("processed_events")
            .count_documents(doc! { "_id": "saga:app.Banner:printer-banner:p-1" })
            .await
            .unwrap(),
        1
    );

    // -- the statistics followed
    let stats = app.saga_stats();
    assert_eq!(stats.started("app.Banner"), 3);
    assert_eq!(stats.ended("app.Banner"), 3);
    assert!(stats.events("app.Banner", EventOutcome::Handled) >= 5);
    assert_eq!(stats.deadlines_fired("banner-timeout"), 1);
    assert_eq!(
        stats.external_calls("app.PrintBanner", ExternalOutcome::Succeeded),
        3
    );
    assert!(app.saga_failures().list(None).await.unwrap().is_empty());
    // -- and so did the meter, with the same names and labels
    use kraken_messaging::saga::stats::{
        DEADLINES_FIRED, EXTERNAL_CALLS, SAGA_DISPATCH_PENDING, SAGAS_ACTIVE, SAGAS_ENDED,
        SAGAS_STARTED,
    };
    assert_eq!(
        meter.value(SAGAS_STARTED, &[("type", "app.Banner")]),
        Some(3.0)
    );
    assert_eq!(
        meter.value(SAGAS_ENDED, &[("type", "app.Banner")]),
        Some(3.0)
    );
    assert_eq!(
        meter.value(DEADLINES_FIRED, &[("name", "banner-timeout")]),
        Some(1.0)
    );
    assert_eq!(
        meter.value(
            EXTERNAL_CALLS,
            &[("handler", "app.PrintBanner"), ("outcome", "succeeded")]
        ),
        Some(3.0)
    );
    assert!(
        meter
            .value(SAGAS_ACTIVE, &[("type", "app.Banner")])
            .is_some()
    );
    assert!(meter.value(SAGA_DISPATCH_PENDING, &[]).is_some());

    // -- the adapter's command type is registered with the hub
    let registers = hub.registers().await;
    assert!(
        registers
            .iter()
            .any(|r| r.command_types.contains(&"app.PrintBanner".to_string())),
        "the adapter is an ordinary command handler for the hub"
    );

    let started = std::time::Instant::now();
    app.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the relay, the scheduler and the consumer stop"
    );
}

#[tokio::test]
async fn a_service_without_sagas_adapters_or_sources_builds_none_of_it() {
    require_docker!();
    let hub = FakeHub::start().await;
    let mongo = support::containers::mongo().await;
    let app = KrakenMessaging::builder(config(
        &hub.address(),
        "127.0.0.1:1",
        &mongo.uri_for("plain"),
    ))
    .await
    .unwrap()
    .command::<PrintBanner, _, _>(|_, _| async { Ok(()) })
    .start()
    .await
    .unwrap();

    let names = app.database().list_collection_names().await.unwrap();
    for saga_collection in [
        "sagas",
        "saga_dispatch",
        "saga_dispatch_leases",
        "deadlines",
        "saga_failures",
        "external_calls",
    ] {
        assert!(
            !names.contains(&saga_collection.to_string()),
            "{saga_collection} created: {names:?}"
        );
    }
    assert_eq!(app.saga_stats().snapshot(), vec![]);
    assert!(app.saga_failures().list(None).await.unwrap().is_empty());
    app.shutdown().await;

    // Disabled sagas are not built either, even when registered.
    let mut disabled = config(&hub.address(), "127.0.0.1:1", &mongo.uri_for("disabled"));
    disabled.sagas_enabled = false;
    let app = KrakenMessaging::builder(disabled)
        .await
        .unwrap()
        .saga::<Banner>("app.Banner", |d| {
            d.start_on::<Shouted>("jobId", |e| Some(e.job_id.clone()), |_, _, _| Ok(()))
        })
        .start()
        .await
        .unwrap();
    let names = app.database().list_collection_names().await.unwrap();
    assert!(!names.contains(&"sagas".to_string()), "{names:?}");
    app.shutdown().await;

    // An idempotent handler brings the external_calls index, and nothing of the sagas.
    let app = KrakenMessaging::builder(config(
        &hub.address(),
        "127.0.0.1:1",
        &mongo.uri_for("adapter"),
    ))
    .await
    .unwrap()
    .idempotent_command::<PrintBanner, _, _>(IdempotencyKey::default(), false, |_, _| async {
        Ok::<(), MessagingError>(())
    })
    .start()
    .await
    .unwrap();
    let names = app.database().list_collection_names().await.unwrap();
    assert!(names.contains(&"external_calls".to_string()), "{names:?}");
    assert!(!names.contains(&"sagas".to_string()), "{names:?}");
    app.shutdown().await;
}
