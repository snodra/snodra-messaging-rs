//! Partner Kafka topics become local kraken events (S2 Task 10, ADR 0021 rulings 20, 40): the Rust
//! form of the Kotlin `ExternalKafkaSourcesTest`, against a real Kafka and MongoDB. One broker
//! serves every scenario (each on a topic and source of its own).

mod support;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_messaging::external::kafka::{
    ExternalKafkaSources, ExternalSource, SourceRegistration, TranslatedEvent, Translator,
};
use kraken_messaging::hub::HandlerRegistry;
use kraken_messaging::kafka::LocalEventDispatcher;
use kraken_messaging::saga::{NoopSagaMetrics, SagaDefinition, SagaRetry, SagaRuntime, SagaType};
use kraken_messaging::{Event, MessageContext, MessagingError, RetryPolicy, message_type};
use mongodb::bson::doc;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use support::saga::{Store, state};
use tokio_util::sync::CancellationToken;

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
struct Replied {
    job_id: String,
    status: String,
}
message_type!(Replied, "ek.Replied");
impl Event for Replied {}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Job {
    job_id: Option<String>,
    replies: u32,
}

fn job_saga() -> SagaType {
    SagaDefinition::<Job>::new("ek.Job")
        .start_on::<Replied>(
            "jobId",
            |e| Some(e.job_id.clone()),
            |s, e, _| {
                s.job_id = Some(e.job_id.clone());
                s.replies += 1;
                Ok(())
            },
        )
        .build()
        .unwrap()
}

#[derive(Default)]
struct Handlers {
    seen: Mutex<Vec<String>>,
    contexts: Mutex<Vec<MessageContext>>,
}

/// `{"jobId", "status", "eventId"?}`: status `ignore` is ignored, `boom` fails, `bad` is invalid.
fn translator(calls: Arc<AtomicUsize>) -> Translator {
    Arc::new(move |record| {
        calls.fetch_add(1, Ordering::SeqCst);
        let tree: serde_json::Value = serde_json::from_slice(record.payload().unwrap()).unwrap();
        let status = tree["status"].as_str().unwrap().to_string();
        match status.as_str() {
            "ignore" => return Ok(None),
            "boom" => return Err(MessagingError::Storage("cannot translate".into())),
            "bad" => return Err(MessagingError::InvalidMessage("not ours".into())),
            "panic" => panic!("the translator broke"),
            _ => {}
        }
        let event = Replied {
            job_id: tree["jobId"].as_str().unwrap().to_string(),
            status,
        };
        Ok(Some(TranslatedEvent::new(
            &event,
            tree["eventId"].as_str().map(str::to_string),
        )?))
    })
}

struct Ek {
    store: Store,
    bootstrap: String,
    handlers: Arc<Handlers>,
    dispatcher: Arc<LocalEventDispatcher>,
    producer: FutureProducer,
}

impl Ek {
    async fn start(bootstrap: &str) -> Ek {
        let store = Store::start().await;
        let handlers = Arc::new(Handlers::default());
        let mut registry = HandlerRegistry::new();
        let h = Arc::clone(&handlers);
        registry.event::<Replied, _, _>(move |e, ctx| {
            let h = Arc::clone(&h);
            async move {
                h.seen.lock().unwrap().push(e.job_id);
                h.contexts.lock().unwrap().push(ctx.message);
                Ok(())
            }
        });
        let runtime = Arc::new(SagaRuntime::new(
            vec![job_saga()],
            store.ports(),
            store.clock.clock(),
            SagaRetry::new(RetryPolicy {
                max_attempts: 2,
                initial_backoff: Duration::from_millis(1),
                multiplier: 1.0,
                max_backoff: Duration::from_millis(1),
            }),
            Arc::new(NoopSagaMetrics),
        ));
        let dispatcher = Arc::new(LocalEventDispatcher::new(
            store.client.clone(),
            store.db.clone(),
            Arc::new(registry),
            runtime,
        ));
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("acks", "all")
            .create()
            .unwrap();
        Ek {
            store,
            bootstrap: bootstrap.to_string(),
            handlers,
            dispatcher,
            producer,
        }
    }

    async fn topic(&self) -> String {
        let topic = format!("partner.printer-{}", uuid::Uuid::new_v4().simple());
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap)
            .create()
            .unwrap();
        admin
            .create_topics(
                &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap();
        topic
    }

    fn sources(
        &self,
        name: &str,
        source: ExternalSource,
        calls: &Arc<AtomicUsize>,
        max_attempts: u32,
    ) -> Arc<ExternalKafkaSources> {
        ExternalKafkaSources::new(
            vec![SourceRegistration {
                name: name.into(),
                source,
                translator: translator(Arc::clone(calls)),
            }],
            Arc::clone(&self.dispatcher),
            "service-b",
            &self.bootstrap,
            max_attempts,
            Duration::from_millis(50),
        )
        .unwrap()
    }

    async fn send(&self, topic: &str, json: &str, headers: &[(&str, &str)]) {
        let mut hs = rdkafka::message::OwnedHeaders::new();
        for (k, v) in headers {
            hs = hs.insert(rdkafka::message::Header {
                key: k,
                value: Some(*v),
            });
        }
        self.producer
            .send(
                FutureRecord::to(topic).key("k").payload(json).headers(hs),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();
    }

    async fn replies(&self, job_id: &str) -> Option<u64> {
        self.store
            .sagas("ek.Job")
            .await
            .iter()
            .find(|s| state(s)["jobId"] == job_id)
            .map(|s| state(s)["replies"].as_u64().unwrap())
    }

    async fn await_saga(&self, job_id: &str, replies: u64) {
        let started = std::time::Instant::now();
        while self.replies(job_id).await != Some(replies) {
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "saga {job_id} did not reach {replies} replies: {:?}",
                self.replies(job_id).await
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn seen(&self) -> Vec<String> {
        std::mem::take(&mut *self.handlers.seen.lock().unwrap())
    }

    async fn marker(&self, id: &str) -> bool {
        self.store.marker(id).await
    }
}

#[test]
fn a_properties_entry_for_a_key_the_library_fixes_fails_the_startup_naming_the_source_and_the_key()
{
    for key in [
        "bootstrap.servers",
        "group.id",
        "key.deserializer",
        "value.deserializer",
        "enable.auto.commit",
    ] {
        let source = ExternalSource {
            properties: HashMap::from([(key.to_string(), "x".to_string())]),
            ..ExternalSource::new("partner.printer")
        };
        let e = kraken_messaging::external::kafka::validate_source("printer", &source)
            .unwrap_err()
            .to_string();
        assert!(e.contains("printer") && e.contains(key), "{key}: {e}");
    }
    // The keys the library does not fix pass through.
    let source = ExternalSource {
        properties: HashMap::from([
            ("security.protocol".to_string(), "PLAINTEXT".to_string()),
            ("auto.offset.reset".to_string(), "latest".to_string()),
        ]),
        ..ExternalSource::new("partner.printer")
    };
    kraken_messaging::external::kafka::validate_source("printer", &source).unwrap();
    assert!(
        kraken_messaging::external::kafka::validate_source("printer", &ExternalSource::default())
            .unwrap_err()
            .to_string()
            .contains("topic")
    );
}

/// ADR 0022: the marker families `processor:`, `saga:`, `ingress:` and `failed:` are the
/// library's own; a source of one of those names could collide with them.
#[test]
fn a_reserved_source_name_fails_the_startup_naming_it() {
    for name in ["processor", "saga", "ingress", "failed"] {
        let e = kraken_messaging::external::kafka::validate_source(
            name,
            &ExternalSource::new("partner.printer"),
        )
        .unwrap_err()
        .to_string();
        assert!(
            e.contains(&format!("'{name}'")) && e.contains("reserved"),
            "{name}: {e}"
        );
        assert!(kraken_messaging::external::is_reserved_source_name(name));
    }
    assert!(!kraken_messaging::external::is_reserved_source_name(
        "printer"
    ));
    assert!(!kraken_messaging::external::is_reserved_source_name(
        "processors"
    ));
}

#[tokio::test]
async fn partner_records_become_local_events_deduplicated_never_published_and_retried_then_skipped()
{
    require_docker!();
    let kafka = support::containers::kafka().await;
    let ek = Ek::start(&kafka.bootstrap).await;

    // -- a record on the partner topic becomes a local event a saga handles; the handlers see the
    //    dedupe id, the source and the record's headers; the group defaults to service and source
    let topic = ek.topic().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let sources = ek.sources("printer", ExternalSource::new(&topic), &calls, 3);
    assert_eq!(
        sources.group_id_for("printer"),
        Some("service-b.external.printer")
    );
    let cancel = CancellationToken::new();
    let running = sources.start(cancel.clone());
    ek.send(
        &topic,
        r#"{"jobId":"j1","status":"done"}"#,
        &[("x-partner", "acme"), ("correlationId", "corr-acme")],
    )
    .await;
    ek.await_saga("j1", 1).await;
    assert_eq!(ek.seen(), ["j1"], "the ordinary handler saw it too");
    // The dedupe id is the source, the partition and the offset; both stages' markers carry it.
    assert!(ek.marker("printer:0:0").await);
    assert!(ek.marker("saga:ek.Job:printer:0:0").await);
    let ctx = ek.handlers.contexts.lock().unwrap().remove(0);
    assert_eq!(ctx.message_id, "printer:0:0");
    assert_eq!(ctx.type_name, "ek.Replied");
    assert_eq!(ctx.correlation_id, "corr-acme");
    assert_eq!(ctx.headers["x-partner"], "acme");
    assert_eq!(ctx.headers["id"], "printer:0:0");
    assert_eq!(ctx.headers["type"], "ek.Replied");
    assert_eq!(ctx.headers["source"], "printer");

    // -- null from the translator ignores the record (no marker)
    ek.send(&topic, r#"{"jobId":"skipped","status":"ignore"}"#, &[])
        .await;
    ek.send(&topic, r#"{"jobId":"j2","status":"done"}"#, &[])
        .await;
    ek.await_saga("j2", 1).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(ek.seen(), ["j2"]);
    assert_eq!(ek.replies("skipped").await, None);
    assert!(
        !ek.marker("printer:0:1").await,
        "an ignored record leaves no marker"
    );

    // -- a redelivered record is deduplicated by its dedupe key
    ek.send(
        &topic,
        r#"{"jobId":"j3","status":"done","eventId":"evt-1"}"#,
        &[],
    )
    .await;
    ek.send(
        &topic,
        r#"{"jobId":"j3","status":"done","eventId":"evt-1"}"#,
        &[],
    )
    .await;
    ek.send(
        &topic,
        r#"{"jobId":"j3","status":"again","eventId":"evt-2"}"#,
        &[],
    )
    .await;
    ek.await_saga("j3", 2).await;
    assert_eq!(ek.seen(), ["j3", "j3"], "evt-1 once, evt-2 once");
    assert!(ek.marker("printer:evt-1").await);
    assert!(ek.marker("saga:ek.Job:printer:evt-2").await);

    // -- a translator that fails is retried and then skipped; an invalid record and a panic too
    calls.store(0, Ordering::SeqCst);
    ek.send(&topic, r#"{"jobId":"bad","status":"boom"}"#, &[])
        .await;
    ek.send(&topic, r#"{"jobId":"bad","status":"bad"}"#, &[])
        .await;
    ek.send(&topic, r#"{"jobId":"bad","status":"panic"}"#, &[])
        .await;
    ek.send(&topic, r#"{"jobId":"j5","status":"done"}"#, &[])
        .await;
    ek.await_saga("j5", 1).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3 + 1 + 3 + 1,
        "three attempts for the failing record, one for the invalid one, three for the panic, one for the next"
    );
    assert_eq!(ek.replies("bad").await, None);

    // -- the event is not published to kraken.events
    assert_eq!(
        ek.store.count("outbox", doc! {}).await,
        0,
        "nothing reached the outbox that relays to kraken.events"
    );

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the sources stop")
        .unwrap();

    // -- an explicit group id and the source's own servers are used
    let topic = ek.topic().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let sources = ek.sources(
        "billing",
        ExternalSource {
            bootstrap_servers: Some(kafka.bootstrap.clone()),
            group_id: Some("my-group".into()),
            ..ExternalSource::new(&topic)
        },
        &calls,
        3,
    );
    assert_eq!(sources.group_id_for("billing"), Some("my-group"));
    let cancel = CancellationToken::new();
    let running = sources.start(cancel.clone());
    ek.send(&topic, r#"{"jobId":"j7","status":"done"}"#, &[])
        .await;
    ek.await_saga("j7", 1).await;
    assert!(ek.marker("billing:0:0").await);
    cancel.cancel();
    let _ = running.await;
}

#[test]
fn two_sources_of_one_name_fail_the_startup() {
    // Creating consumers connects to nothing, so no broker is needed.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/x")
            .await
            .unwrap();
        let dispatcher = Arc::new(LocalEventDispatcher::new(
            client.clone(),
            client.database("x"),
            Arc::new(HandlerRegistry::new()),
            Arc::new(kraken_messaging::kafka::NoSagas),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let registration = || SourceRegistration {
            name: "printer".into(),
            source: ExternalSource::new("partner.printer"),
            translator: translator(Arc::clone(&calls)),
        };
        let e = ExternalKafkaSources::new(
            vec![registration(), registration()],
            dispatcher,
            "svc",
            "127.0.0.1:1",
            3,
            Duration::from_millis(1),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(e.contains("printer") && e.contains("twice"), "{e}");
    });
}
