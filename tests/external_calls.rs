//! Idempotent adapter handlers over `external_calls` (S2 Task 9, ADR 0021 rulings 12, 17, 38, 39,
//! 41-43): the Rust form of the Kotlin `IdempotentHandlerTest`, against a real MongoDB.

mod support;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_messaging::external::{
    ExternalCallSettings, ExternalCalls, ExternalCallsHandle, IdempotencyKey,
};
use kraken_messaging::hub::{Dispatcher, HandlerRegistry};
use kraken_messaging::identity::IdentityResolver;
use kraken_messaging::kafka::dispatch::{LocalEventDispatcher, NoSagas};
use kraken_messaging::kafka::{EventBus, SagaStage};
use kraken_messaging::proto::{ErrorCode, Reply, Request, RequestKind, reply};
use kraken_messaging::saga::store::{
    DispatchKind, ExternalCallDocument, ExternalCallStatus, SagaDispatchDocument,
};
use kraken_messaging::saga::{
    ExternalOutcome, FailureReason, LocalRawCommandSender, NoopSagaMetrics, RETRYABLE_MARKER,
    RawCommandSender, RawOutcome, RelaySettings, SagaCommandFailed, SagaDefinition,
    SagaDispatchRelay, SagaMetrics, SagaRetry, SagaRuntime, SagaType, UNKNOWN_OUTCOME_MARKER,
    classify_failure,
};
use kraken_messaging::{Command, Event, MessagingError, RetryPolicy, SendOptions, message_type};
use mongodb::bson::{DateTime, doc};
use serde::{Deserialize, Serialize};
use support::saga::{Store, event_ctx, state, t0};

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Print {
    job_id: String,
    name: String,
}
message_type!(Print, "ex.Print");

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Receipt {
    job_id: String,
    ticket: u32,
}
impl Command for Print {
    type Reply = Receipt;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrintSafe {
    job_id: String,
}
message_type!(PrintSafe, "ex.PrintSafe");
impl Command for PrintSafe {
    type Reply = Receipt;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Poll {
    job_id: String,
    attempt: u32,
}
message_type!(Poll, "ex.Poll");
impl Command for Poll {
    type Reply = ();
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Stamp {
    job_id: String,
}
message_type!(Stamp, "ex.Stamp");
impl Event for Stamp {}

type Behaviour = Box<dyn Fn(&Print, usize) -> Result<Receipt, MessagingError> + Send + Sync>;

/// The adapter under test: `behaviour` is the call to the external system; the counters say how
/// often a body really ran.
struct Adapter {
    bodies: AtomicUsize,
    polls: AtomicUsize,
    stamps: AtomicUsize,
    /// Calls of the ordinary (not idempotent) handler of `Stamp` that fail before it succeeds.
    fail_stamp_watcher: AtomicUsize,
    behaviour: Mutex<Behaviour>,
}

impl Adapter {
    fn new() -> Arc<Adapter> {
        Arc::new(Adapter {
            bodies: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            stamps: AtomicUsize::new(0),
            fail_stamp_watcher: AtomicUsize::new(0),
            behaviour: Mutex::new(Box::new(|p, n| {
                Ok(Receipt {
                    job_id: p.job_id.clone(),
                    ticket: 1000 + n as u32,
                })
            })),
        })
    }

    fn behave(
        &self,
        f: impl Fn(&Print, usize) -> Result<Receipt, MessagingError> + Send + Sync + 'static,
    ) {
        *self.behaviour.lock().unwrap() = Box::new(f);
    }

    fn bodies(&self) -> usize {
        self.bodies.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct ExternalMetrics {
    calls: Mutex<Vec<(String, ExternalOutcome)>>,
}

impl SagaMetrics for ExternalMetrics {
    fn external_call(&self, handler: &str, outcome: ExternalOutcome) {
        self.calls
            .lock()
            .unwrap()
            .push((handler.to_string(), outcome));
    }
}

const LEASE: Duration = Duration::from_secs(300);

fn register(registry: &mut HandlerRegistry, calls: &ExternalCallsHandle, adapter: &Arc<Adapter>) {
    let a = Arc::clone(adapter);
    registry.idempotent_command::<Print, _, _>(
        calls,
        IdempotencyKey::default(),
        false,
        move |p, _| {
            let a = Arc::clone(&a);
            async move {
                let n = a.bodies.fetch_add(1, Ordering::SeqCst);
                let behaviour = a.behaviour.lock().unwrap();
                behaviour(&p, n)
            }
        },
    );
    let a = Arc::clone(adapter);
    registry.idempotent_command::<PrintSafe, _, _>(
        calls,
        IdempotencyKey::Header("idempotency-key"),
        true,
        move |p, _| {
            let a = Arc::clone(&a);
            async move {
                let n = a.bodies.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(Receipt {
                    job_id: p.job_id,
                    ticket: n as u32,
                })
            }
        },
    );
    // The key includes the poll attempt, as the long-polling pattern wants.
    let a = Arc::clone(adapter);
    registry.idempotent_command::<Poll, _, _>(
        calls,
        IdempotencyKey::From(|p: &Poll, _| format!("poll:{}:{}", p.job_id, p.attempt)),
        false,
        move |_, _| {
            let a = Arc::clone(&a);
            async move {
                a.polls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );
    let a = Arc::clone(adapter);
    registry.idempotent_event::<Stamp, _, _>(
        calls,
        IdempotencyKey::From(|s: &Stamp, _| format!("stamp:{}", s.job_id)),
        false,
        move |_, _| {
            let a = Arc::clone(&a);
            async move {
                a.stamps.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );
    // An ordinary handler of the same event, after the adapter: when it fails, stage 1 rolls back.
    let a = Arc::clone(adapter);
    registry.event::<Stamp, _, _>(move |_, _| {
        let a = Arc::clone(&a);
        async move {
            let left = a.fail_stamp_watcher.load(Ordering::SeqCst);
            if left > 0 {
                a.fail_stamp_watcher.store(left - 1, Ordering::SeqCst);
                return Err(MessagingError::Storage("the watcher is broken".into()));
            }
            Ok(())
        }
    });
}

struct Ex {
    store: Store,
    adapter: Arc<Adapter>,
    metrics: Arc<ExternalMetrics>,
    calls: Arc<ExternalCalls>,
    registry: Arc<HandlerRegistry>,
    dispatcher: Dispatcher,
}

impl Ex {
    async fn start() -> Ex {
        let store = Store::start().await;
        let _ = store.db.create_collection("external_calls").await;
        let adapter = Adapter::new();
        let metrics = Arc::new(ExternalMetrics::default());
        let calls = ExternalCalls::new(
            store.db.clone(),
            store.clock.clock(),
            ExternalCallSettings { lease: LEASE },
            metrics.clone(),
        )
        .unwrap();
        let mut registry = HandlerRegistry::new();
        register(&mut registry, &calls.handle(), &adapter);
        let registry = Arc::new(registry);
        let dispatcher = Dispatcher::new(
            Arc::clone(&registry),
            Arc::new(IdentityResolver::new(None)),
            4,
        );
        Ex {
            store,
            adapter,
            metrics,
            calls,
            registry,
            dispatcher,
        }
    }

    async fn call(&self, type_name: &str, payload: &str, key: Option<&str>) -> Reply {
        let mut headers = HashMap::new();
        if let Some(key) = key {
            headers.insert("idempotency-key".to_string(), key.to_string());
        }
        self.dispatcher
            .handle(Request {
                id: uuid::Uuid::new_v4().to_string(),
                kind: RequestKind::Command as i32,
                r#type: type_name.into(),
                payload: payload.as_bytes().to_vec(),
                headers,
                ..Request::default()
            })
            .await
    }

    async fn print(&self, key: Option<&str>) -> Reply {
        self.call("ex.Print", r#"{"jobId":"j1","name":"hi"}"#, key)
            .await
    }

    async fn record(&self, key: &str) -> Option<ExternalCallDocument> {
        self.store
            .find::<ExternalCallDocument>("external_calls", doc! { "_id": key })
            .await
            .pop()
    }

    async fn insert_in_progress(&self, key: &str) {
        let start = DateTime::from_system_time(t0());
        self.store
            .db
            .collection::<ExternalCallDocument>("external_calls")
            .insert_one(ExternalCallDocument {
                id: key.into(),
                status: ExternalCallStatus::InProgress,
                reply: None,
                error: None,
                attempts: 1,
                updated_at: start,
                lease_until: Some(DateTime::from_system_time(t0() + LEASE)),
            })
            .await
            .unwrap();
    }

    fn local_dispatcher(&self) -> LocalEventDispatcher {
        LocalEventDispatcher::new(
            self.store.client.clone(),
            self.store.db.clone(),
            Arc::clone(&self.registry),
            Arc::new(NoSagas),
        )
    }
}

fn receipt(reply: &Reply) -> Receipt {
    match &reply.result {
        Some(reply::Result::Payload(p)) => serde_json::from_slice(p).unwrap(),
        other => panic!("expected a payload, got {other:?}"),
    }
}

/// (code, message, details) of an error reply.
fn failure(reply: &Reply) -> (ErrorCode, String, Vec<u8>) {
    match &reply.result {
        Some(reply::Result::Error(e)) => (
            ErrorCode::try_from(e.code).unwrap(),
            e.message.clone(),
            e.details.clone(),
        ),
        other => panic!("expected an error, got {other:?}"),
    }
}

fn is_retryable(details: &[u8]) -> bool {
    details == RETRYABLE_MARKER
}

fn is_unknown(details: &[u8]) -> bool {
    details == UNKNOWN_OUTCOME_MARKER
}

#[tokio::test]
async fn a_second_call_with_the_same_key_returns_the_stored_reply_without_running_the_body() {
    require_docker!();
    let ex = Ex::start().await;

    let first = receipt(&ex.print(Some("job-1")).await);
    let second = receipt(&ex.print(Some("job-1")).await);

    assert_eq!(ex.adapter.bodies(), 1, "the body ran once");
    assert_eq!(first, second);
    let stored = ex.record("job-1").await.unwrap();
    assert_eq!(stored.status, ExternalCallStatus::Succeeded);
    assert_eq!(stored.attempts, 1);
    assert_eq!(stored.lease_until, None);
    assert_eq!(
        stored.reply.as_deref(),
        Some(r#"{"jobId":"j1","ticket":1000}"#)
    );
    assert_eq!(
        *ex.metrics.calls.lock().unwrap(),
        [
            ("ex.Print".to_string(), ExternalOutcome::Succeeded),
            ("ex.Print".to_string(), ExternalOutcome::Replayed)
        ]
    );
    ex.print(Some("job-2")).await;
    assert_eq!(ex.adapter.bodies(), 2, "another key runs the body");
}

#[tokio::test]
async fn a_failed_call_re_throws_the_stored_error() {
    require_docker!();
    let ex = Ex::start().await;
    ex.adapter.behave(|p, _| {
        Err(MessagingError::RemoteHandler {
            type_name: "ex.Print".into(),
            message: format!("printer refused {}", p.name),
            details: vec![],
        })
    });

    let first = failure(&ex.print(Some("job-1")).await);
    let second = failure(&ex.print(Some("job-1")).await);

    assert_eq!(
        ex.adapter.bodies(),
        1,
        "the permanent failure is not retried"
    );
    assert_eq!(first.0, ErrorCode::HandlerError);
    assert_eq!(first.0, second.0);
    assert_eq!(first.1, second.1);
    assert!(second.1.contains("printer refused hi"), "{}", second.1);
    let stored = ex.record("job-1").await.unwrap();
    assert_eq!(stored.status, ExternalCallStatus::Failed);
    let error: serde_json::Value = serde_json::from_str(stored.error.as_deref().unwrap()).unwrap();
    assert_eq!(error["code"], "HANDLER_ERROR");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("printer refused hi"),
        "{error}"
    );
    assert!(!is_retryable(&second.2) && !is_unknown(&second.2));
    assert_eq!(
        ex.metrics.calls.lock().unwrap()[1],
        ("ex.Print".to_string(), ExternalOutcome::Failed)
    );
}

#[tokio::test]
async fn a_retryable_failure_lets_the_next_call_run_the_body_again() {
    require_docker!();
    let ex = Ex::start().await;
    ex.adapter.behave(|p, n| {
        if n == 0 {
            Err(MessagingError::RetryableExternal("partner busy".into()))
        } else {
            Ok(Receipt {
                job_id: p.job_id.clone(),
                ticket: 7,
            })
        }
    });

    let first = failure(&ex.print(Some("job-1")).await);

    assert!(is_retryable(&first.2), "the relay can tell it is transient");
    assert!(first.1.contains("partner busy"), "{}", first.1);
    assert!(
        ex.record("job-1").await.is_none(),
        "the record is reset: nothing is remembered of the failed attempt"
    );
    assert_eq!(receipt(&ex.print(Some("job-1")).await).ticket, 7);
    assert_eq!(ex.adapter.bodies(), 2);
    ex.print(Some("job-1")).await;
    assert_eq!(ex.adapter.bodies(), 2, "now it is stored");
}

#[tokio::test]
async fn an_adapter_that_reports_an_unknown_outcome_itself_leaves_the_record_unknown() {
    require_docker!();
    let ex = Ex::start().await;
    ex.adapter.behave(|_, _| {
        Err(MessagingError::UnknownOutcome(
            "the connection broke after the request went out".into(),
        ))
    });

    let first = failure(&ex.print(Some("job-1")).await);

    assert!(is_unknown(&first.2), "{}", first.1);
    assert_eq!(
        ex.record("job-1").await.unwrap().status,
        ExternalCallStatus::Unknown,
        "not FAILED: the external system may have acted"
    );
    let again = failure(&ex.print(Some("job-1")).await);
    assert!(is_unknown(&again.2), "the redelivery: {}", again.1);
    assert_eq!(ex.adapter.bodies(), 1, "never called again automatically");
}

#[tokio::test]
async fn a_failure_to_write_succeeded_after_the_body_succeeded_is_an_unknown_outcome() {
    require_docker!();
    let ex = Ex::start().await;
    ex.calls.fail_next_succeeded_write();

    let first = failure(&ex.print(Some("job-1")).await);

    assert!(
        is_unknown(&first.2),
        "the call happened but its record could not say so: {}",
        first.1
    );
    assert_eq!(ex.adapter.bodies(), 1);
    assert_eq!(
        ex.record("job-1").await.unwrap().status,
        ExternalCallStatus::Unknown,
        "never FAILED"
    );
    let again = failure(&ex.print(Some("job-1")).await);
    assert!(is_unknown(&again.2));
    assert_eq!(ex.adapter.bodies(), 1);
}

#[tokio::test]
async fn with_external_idempotent_a_failure_to_write_succeeded_is_retryable() {
    require_docker!();
    let ex = Ex::start().await;
    ex.calls.fail_next_succeeded_write();

    let first = failure(
        &ex.call("ex.PrintSafe", r#"{"jobId":"j1"}"#, Some("job-1"))
            .await,
    );

    assert!(is_retryable(&first.2), "{}", first.1);
    assert_eq!(
        ex.record("job-1").await.unwrap().status,
        ExternalCallStatus::InProgress,
        "left for its lease to settle: never FAILED"
    );
}

#[tokio::test]
async fn an_in_progress_record_past_its_lease_becomes_unknown_and_the_call_fails_with_unknown_outcome()
 {
    require_docker!();
    let ex = Ex::start().await;
    ex.insert_in_progress("job-1").await;
    ex.store.clock.set(t0() + LEASE + Duration::from_secs(1)); // the first call crashed mid-way, a lease ago

    let first = failure(&ex.print(Some("job-1")).await);

    assert!(is_unknown(&first.2), "{}", first.1);
    assert_eq!(ex.adapter.bodies(), 0, "never called again automatically");
    let stored = ex.record("job-1").await.unwrap();
    assert_eq!(stored.status, ExternalCallStatus::Unknown);
    assert_eq!(stored.lease_until, None);
    let again = failure(&ex.print(Some("job-1")).await);
    assert!(is_unknown(&again.2));
    assert_eq!(ex.adapter.bodies(), 0);
    assert!(
        ex.metrics
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(_, o)| *o == ExternalOutcome::Unknown)
    );
}

#[tokio::test]
async fn an_in_progress_record_within_its_lease_asks_the_caller_to_come_back() {
    require_docker!();
    let ex = Ex::start().await;
    ex.insert_in_progress("job-1").await;

    let busy = failure(&ex.print(Some("job-1")).await);

    assert!(
        is_retryable(&busy.2),
        "another call is in flight: transient"
    );
    assert_eq!(ex.adapter.bodies(), 0);
    assert_eq!(
        ex.record("job-1").await.unwrap().status,
        ExternalCallStatus::InProgress
    );
}

#[tokio::test]
async fn with_external_idempotent_the_expired_call_is_retried() {
    require_docker!();
    let ex = Ex::start().await;
    ex.insert_in_progress("job-1").await;
    ex.store.clock.set(t0() + LEASE + Duration::from_secs(1));

    let result = ex
        .call("ex.PrintSafe", r#"{"jobId":"j1"}"#, Some("job-1"))
        .await;

    assert_eq!(receipt(&result).ticket, 1);
    assert_eq!(ex.adapter.bodies(), 1);
    let stored = ex.record("job-1").await.unwrap();
    assert_eq!(stored.status, ExternalCallStatus::Succeeded);
    assert_eq!(stored.attempts, 2, "the second attempt is counted");
}

#[tokio::test]
async fn a_missing_key_is_a_call_time_error_naming_the_handler() {
    require_docker!();
    let ex = Ex::start().await;

    let (code, message, _) = failure(&ex.print(None).await);

    assert_eq!(code, ErrorCode::HandlerError);
    assert!(
        message.contains("ex.Print") && message.contains("idempotency-key"),
        "{message}"
    );
    assert_eq!(ex.adapter.bodies(), 0);
    assert!(ex.record("").await.is_none());
}

#[tokio::test]
async fn the_key_can_be_computed_from_the_message() {
    require_docker!();
    let ex = Ex::start().await;
    let poll = |job: &str, attempt: u32| format!(r#"{{"jobId":"{job}","attempt":{attempt}}}"#);

    ex.call("ex.Poll", &poll("j1", 1), None).await;
    ex.call("ex.Poll", &poll("j1", 1), None).await;
    ex.call("ex.Poll", &poll("j1", 2), None).await;

    assert_eq!(
        ex.adapter.polls.load(Ordering::SeqCst),
        2,
        "same job and attempt once, the next attempt again"
    );
    assert_eq!(
        ex.record("poll:j1:1").await.unwrap().reply.as_deref(),
        Some("{}"),
        "a unit reply is stored as the empty object, as Kotlin writes it"
    );
    assert!(ex.record("poll:j1:2").await.is_some());
}

#[tokio::test]
async fn an_idempotent_event_handler_runs_once_per_key() {
    require_docker!();
    let ex = Ex::start().await;
    let local = ex.local_dispatcher();
    let payload = br#"{"jobId":"j9"}"#;

    for id in ["e-1", "e-2"] {
        local
            .dispatch(id, "ex.Stamp", payload, &event_ctx(id, "ex.Stamp", &[]))
            .await
            .unwrap();
    }

    assert_eq!(
        ex.adapter.stamps.load(Ordering::SeqCst),
        1,
        "two events of one key"
    );
}

#[tokio::test]
async fn the_call_record_is_written_apart_from_the_callers_transaction() {
    require_docker!();
    let ex = Ex::start().await;
    let local = ex.local_dispatcher();
    let payload = br#"{"jobId":"j9"}"#;
    ex.adapter.fail_stamp_watcher.store(1, Ordering::SeqCst);

    // The adapter runs inside stage 1's transaction (ruling 43); the handler after it fails, so
    // that transaction and its marker roll back.
    let failed = local
        .dispatch(
            "e-1",
            "ex.Stamp",
            payload,
            &event_ctx("e-1", "ex.Stamp", &[]),
        )
        .await;
    assert!(failed.is_err());
    assert!(!ex.store.marker("e-1").await, "stage 1 rolled back");
    assert_eq!(
        ex.record("stamp:j9").await.unwrap().status,
        ExternalCallStatus::Succeeded,
        "the external call happened: the record must stay"
    );

    local
        .dispatch(
            "e-1",
            "ex.Stamp",
            payload,
            &event_ctx("e-1", "ex.Stamp", &[]),
        )
        .await
        .unwrap();
    assert!(ex.store.marker("e-1").await);
    assert_eq!(
        ex.adapter.stamps.load(Ordering::SeqCst),
        1,
        "the redelivery replays the stored call"
    );
}

#[tokio::test]
async fn a_handler_whose_external_calls_were_never_bound_fails_loudly() {
    require_docker!();
    let store = Store::start().await;
    let adapter = Adapter::new();
    let mut registry = HandlerRegistry::new();
    register(&mut registry, &ExternalCallsHandle::new(), &adapter);
    assert_eq!(
        registry.idempotent_handlers(),
        ["ex.Poll", "ex.Print", "ex.PrintSafe", "ex.Stamp"]
    );
    let dispatcher = Dispatcher::new(Arc::new(registry), Arc::new(IdentityResolver::new(None)), 4);

    let reply = dispatcher
        .handle(Request {
            id: "r-1".into(),
            kind: RequestKind::Command as i32,
            r#type: "ex.Print".into(),
            payload: br#"{"jobId":"j1","name":"hi"}"#.to_vec(),
            headers: HashMap::from([("idempotency-key".to_string(), "k".to_string())]),
            ..Request::default()
        })
        .await;

    let (_, message, _) = failure(&reply);
    assert!(
        message.contains("ex.Print") && message.contains("external calls"),
        "{message}"
    );
    assert_eq!(adapter.bodies(), 0);
    drop(store);
}

#[test]
fn the_new_error_kinds_carry_their_markers_and_classify_for_the_relay() {
    assert_eq!(
        classify_failure(&MessagingError::RetryableExternal("busy".into())),
        RawOutcome::Transient("busy".into())
    );
    assert_eq!(
        classify_failure(&MessagingError::UnknownOutcome("lost".into())),
        RawOutcome::Permanent(FailureReason::UnknownOutcome, "lost".into())
    );
    assert!(
        ExternalCallSettings {
            lease: Duration::ZERO
        }
        .validate()
        .is_err()
    );
    assert_eq!(ExternalCallSettings::default().lease, LEASE);
}

// ---- with the saga relay -------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderStarted {
    order_id: String,
}
message_type!(OrderStarted, "rl.Start");
impl Event for OrderStarted {}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Charge {
    order_id: String,
}
message_type!(Charge, "rl.Charge");
impl Command for Charge {
    type Reply = ();
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Order {
    order_id: Option<String>,
    failures: Vec<String>,
}

fn order_saga() -> SagaType {
    SagaDefinition::<Order>::new("rl.Order")
        .start_on::<OrderStarted>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, ctx| {
                s.order_id = Some(e.order_id.clone());
                ctx.send(
                    &Charge {
                        order_id: e.order_id.clone(),
                    },
                    None,
                )
            },
        )
        .on_command_failed(|s, e: &SagaCommandFailed, _| {
            let reason = serde_json::to_value(e.reason).unwrap();
            s.failures
                .push(format!("{}:{}", e.command_type, reason.as_str().unwrap()));
            Ok(())
        })
        .build()
        .unwrap()
}

struct Harness {
    ex: Ex,
    relay: Arc<SagaDispatchRelay>,
    charges: Arc<AtomicUsize>,
}

async fn harness() -> Harness {
    let ex = Ex::start().await;
    let charges = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    let c = Arc::clone(&charges);
    registry.idempotent_command::<Charge, _, _>(
        &ex.calls.handle(),
        IdempotencyKey::default(),
        false,
        move |_, _| {
            let c = Arc::clone(&c);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );
    let registry = Arc::new(registry);
    let runtime = Arc::new(SagaRuntime::new(
        vec![order_saga()],
        ex.store.ports(),
        ex.store.clock.clock(),
        SagaRetry::new(RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(1),
            multiplier: 1.0,
            max_backoff: Duration::from_millis(1),
        }),
        Arc::new(NoopSagaMetrics),
    ));
    let local = Arc::new(LocalEventDispatcher::new(
        ex.store.client.clone(),
        ex.store.db.clone(),
        Arc::clone(&registry),
        runtime.clone(),
    ));
    let sender: Arc<dyn RawCommandSender> =
        Arc::new(LocalRawCommandSender::new(Arc::clone(&registry)));
    let relay = SagaDispatchRelay::new(
        ex.store.client.clone(),
        ex.store.db.clone(),
        sender,
        EventBus::new(ex.store.db.clone()),
        local,
        ex.store.clock.clock(),
        RelaySettings {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            ..RelaySettings::default()
        },
        Arc::new(NoopSagaMetrics),
    )
    .unwrap();
    runtime
        .process(
            "rl.Start",
            br#"{"orderId":"o1"}"#,
            &event_ctx("e-1", "rl.Start", &[]),
        )
        .await
        .unwrap();
    Harness { ex, relay, charges }
}

impl Harness {
    async fn entry(&self) -> SagaDispatchDocument {
        self.ex
            .store
            .find::<SagaDispatchDocument>("saga_dispatch", doc! { "kind": "COMMAND" })
            .await
            .remove(0)
    }
}

#[tokio::test]
async fn a_crash_between_the_send_and_the_mark_re_sends_with_the_same_key_and_the_body_runs_once() {
    require_docker!();
    let h = harness().await;
    let entry = h.entry().await;
    assert_eq!(entry.kind, DispatchKind::Command);
    h.relay.fail_next_mark();

    h.relay.poll_once().await.unwrap();
    assert!(h.entry().await.dispatched_at.is_none(), "the mark was lost");
    assert_eq!(h.charges.load(Ordering::SeqCst), 1);
    h.relay.poll_once().await.unwrap();

    assert_eq!(
        h.charges.load(Ordering::SeqCst),
        1,
        "the same key: the stored reply, the body did not run again"
    );
    assert!(h.entry().await.dispatched_at.is_some());
    assert_eq!(
        h.ex.record(&entry.id).await.unwrap().status,
        ExternalCallStatus::Succeeded
    );
    assert_eq!(h.ex.store.count("external_calls", doc! {}).await, 1);
}

#[tokio::test]
async fn the_relay_maps_an_unknown_outcome_to_saga_command_failed_unknown_outcome() {
    require_docker!();
    let h = harness().await;
    let entry = h.entry().await;
    h.ex.insert_in_progress(&entry.id).await;
    h.ex.store.clock.set(t0() + LEASE + Duration::from_secs(1));

    h.relay.poll_once().await.unwrap();

    let saga = h.ex.store.sagas("rl.Order").await.remove(0);
    assert_eq!(
        state(&saga)["failures"],
        serde_json::json!(["rl.Charge:UNKNOWN_OUTCOME"])
    );
    assert_eq!(
        h.charges.load(Ordering::SeqCst),
        0,
        "an unknown outcome is not called again"
    );
    assert!(h.entry().await.failed_at.is_some());
    assert_eq!(
        h.ex.store
            .count(
                "saga_dispatch",
                doc! { "failedAt": null, "dispatchedAt": null }
            )
            .await,
        0,
        "nothing left pending"
    );
}

#[tokio::test]
async fn a_retryable_adapter_failure_through_the_local_sender_is_transient() {
    require_docker!();
    let ex = Ex::start().await;
    ex.adapter
        .behave(|_, _| Err(MessagingError::RetryableExternal("partner busy".into())));
    let sender = LocalRawCommandSender::new(Arc::clone(&ex.registry));
    let options = SendOptions {
        headers: HashMap::from([("idempotency-key".to_string(), "s:1".to_string())]),
        ..SendOptions::default()
    };

    let outcome = sender
        .send_raw(
            "ex.Print",
            br#"{"jobId":"j1","name":"hi"}"#.to_vec(),
            options,
        )
        .await;

    assert!(
        matches!(outcome, RawOutcome::Transient(ref m) if m.contains("partner busy")),
        "{outcome:?}"
    );
}
