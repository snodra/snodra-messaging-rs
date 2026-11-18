//! The saga dispatch relay (S2 Task 6, ADR 0021 rulings 7-11, 28-31, 34): an ordered, after-commit
//! relay of what sagas recorded in `saga_dispatch`, on the in-memory message store. The Rust form of the
//! Kotlin `SagaDispatchRelayTest`.
#![cfg(feature = "testing")]

mod support;

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use snodra_messaging::context::CURRENT;
use snodra_messaging::events::EventBus;
use snodra_messaging::events::dispatch::{LocalEventDispatcher, NoSagas, SagaStage};
use snodra_messaging::hub::HandlerRegistry;
use snodra_messaging::saga::store::{DispatchKind, SagaDispatchDocument, SagaDocument};
use snodra_messaging::saga::{
    FailureReason, LocalRawCommandSender, NoopSagaMetrics, RETRYABLE_MARKER, RawCommandSender,
    RawOutcome, RelaySettings, SagaCommandFailed, SagaDefinition, SagaDispatchRelay, SagaMetrics,
    SagaRetry, SagaRuntime, SagaType,
};
use snodra_messaging::{
    Command, Event, Guard, MessageContext, MessagingError, RetryPolicy, SendOptions, message_type,
};
use support::saga::{Store, event_ctx, state, t0};
use tokio_util::sync::CancellationToken;

/// An outbox entry (the fields these tests read), its payload as JSON.
struct OutboxDocument {
    type_name: String,
    key: Option<String>,
    payload: serde_json::Value,
    headers: HashMap<String, String>,
}

fn outbox_document(record: snodra_messaging::store::memory::OutboxRecord) -> OutboxDocument {
    let e = record.entry;
    OutboxDocument {
        type_name: e.type_name,
        key: e.key,
        payload: serde_json::from_slice(&e.payload).expect("a JSON payload"),
        headers: e.headers,
    }
}

macro_rules! message {
    ($name:ident, $wire:literal, $trait:ident, { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct $name { $($field: $ty),* }
        message_type!($name, $wire);
    };
}

message!(Start, "rl.Start", Event, { order_id: String });
impl Event for Start {}
message!(StartAndEnd, "rl.StartAndEnd", Event, { order_id: String });
impl Event for StartAndEnd {}
message!(Announced, "rl.Announced", Event, { order_id: String });
impl Event for Announced {}
message!(QuietStart, "rl.QuietStart", Event, { order_id: String });
message!(GrumpyStart, "rl.GrumpyStart", Event, { order_id: String });
impl Event for GrumpyStart {}
impl Event for QuietStart {}
message!(DeafStart, "rl.DeafStart", Event, { order_id: String });
impl Event for DeafStart {}
message!(Charge, "rl.Charge", Command, { r#ref: String });
impl Command for Charge {
    type Reply = ();
}
message!(Secured, "rl.Secured", Command, { r#ref: String });
impl Command for Secured {
    type Reply = ();
}
message!(Boom, "rl.Boom", Command, { r#ref: String });
impl Command for Boom {
    type Reply = ();
}
message!(Busy, "rl.Busy", Command, { r#ref: String });
impl Command for Busy {
    type Reply = ();
}

fn reason_text(reason: FailureReason) -> String {
    serde_json::to_value(reason)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Order {
    order_id: Option<String>,
    failures: Vec<String>,
}

fn command_failed(s: &mut Order, e: &SagaCommandFailed) {
    s.failures
        .push(format!("{}:{}", e.command_type, reason_text(e.reason)));
}

/// Sends a charge when an order starts and remembers the command failures it is told about.
fn order_saga() -> SagaType {
    SagaDefinition::<Order>::new("rl.Order")
        .start_on::<Start>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, ctx| {
                s.order_id = Some(e.order_id.clone());
                ctx.send(
                    &Charge {
                        r#ref: e.order_id.clone(),
                    },
                    None,
                )
            },
        )
        // Sends and publishes, then ends in the same handling: its entries must still go out.
        .start_on::<StartAndEnd>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, ctx| {
                s.order_id = Some(e.order_id.clone());
                ctx.send(
                    &Charge {
                        r#ref: format!("ended-{}", e.order_id),
                    },
                    None,
                )?;
                ctx.publish(
                    &Announced {
                        order_id: e.order_id.clone(),
                    },
                    Some(&format!("key-{}", e.order_id)),
                )
            },
        )
        .ending()
        .on_command_failed(|s, e, _| {
            command_failed(s, e);
            Ok(())
        })
        .build()
        .unwrap()
}

/// Handles the failure by its id without ever associating itself with it.
fn quiet_saga() -> SagaType {
    SagaDefinition::<Order>::new("rl.Quiet")
        .start_on::<QuietStart>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, _| {
                s.order_id = Some(e.order_id.clone());
                Ok(())
            },
        )
        .on_command_failed(|s, e, _| {
            command_failed(s, e);
            Ok(())
        })
        .build()
        .unwrap()
}

/// A saga type whose `SagaCommandFailed` handler always fails: the notice is parked.
fn grumpy_saga() -> SagaType {
    SagaDefinition::<Order>::new("rl.Grumpy")
        .start_on::<GrumpyStart>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, _| {
                s.order_id = Some(e.order_id.clone());
                Ok(())
            },
        )
        .on_command_failed(|_, _, _| Err(MessagingError::Storage("the ledger is locked".into())))
        .build()
        .unwrap()
}

/// A saga type with no handler for `SagaCommandFailed`.
fn deaf_saga() -> SagaType {
    SagaDefinition::<Order>::new("rl.Deaf")
        .start_on::<DeafStart>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, _| {
                s.order_id = Some(e.order_id.clone());
                Ok(())
            },
        )
        .build()
        .unwrap()
}

/// Adapters of the relay tests: an open one, one that needs a caller, one that fails and one that
/// asks for a retry.
#[derive(Default)]
struct Adapter {
    charged: Mutex<Vec<(String, MessageContext)>>,
    busy_calls: Mutex<Vec<String>>,
}

/// An ordinary event handler for the library's failure event that fails its first `fail_first`
/// calls, as a handler with a bug or an outage does.
#[derive(Default)]
struct Watcher {
    calls: AtomicUsize,
    seen: Mutex<Vec<String>>,
    fail_first: AtomicUsize,
}

fn registry(adapter: &Arc<Adapter>, watcher: Option<&Arc<Watcher>>) -> Arc<HandlerRegistry> {
    let mut registry = HandlerRegistry::new();
    let a = Arc::clone(adapter);
    registry.command::<Charge, _, _>(move |c, ctx| {
        let a = Arc::clone(&a);
        async move {
            a.charged.lock().unwrap().push((c.r#ref, ctx));
            Ok(())
        }
    });
    registry.command::<Secured, _, _>(|_, _| async { Ok(()) });
    registry.requires::<Secured>(Guard::authenticated());
    registry.command::<Boom, _, _>(|_, _| async {
        Err(MessagingError::Storage("printer on fire".into()))
    });
    let a = Arc::clone(adapter);
    registry.command::<Busy, _, _>(move |c, _| {
        let a = Arc::clone(&a);
        async move {
            a.busy_calls.lock().unwrap().push(c.r#ref);
            Err(MessagingError::RemoteHandler {
                type_name: "rl.Busy".into(),
                message: "partner is busy".into(),
                details: RETRYABLE_MARKER.to_vec(),
            })
        }
    });
    if let Some(watcher) = watcher {
        let w = Arc::clone(watcher);
        registry.event::<SagaCommandFailed, _, _>(move |e, _| {
            let w = Arc::clone(&w);
            async move {
                let call = w.calls.fetch_add(1, Ordering::SeqCst) + 1;
                if call <= w.fail_first.load(Ordering::SeqCst) {
                    return Err(MessagingError::Storage("watcher is broken".into()));
                }
                w.seen.lock().unwrap().push(format!(
                    "{}:{}",
                    e.command_type,
                    reason_text(e.reason)
                ));
                Ok(())
            }
        });
    }
    Arc::new(registry)
}

struct Call {
    type_name: String,
    payload: String,
    headers: HashMap<String, String>,
    correlation_id: Option<String>,
    /// The `traceparent` of the context the send ran in.
    traceparent: Option<String>,
    retry: Option<RetryPolicy>,
    has_authorization: bool,
}

type Script = Arc<dyn Fn(Arc<Call>) -> BoxFuture<'static, RawOutcome> + Send + Sync>;

/// A sender the test scripts: every call is recorded, the outcome comes from the script.
struct ScriptedSender {
    calls: Mutex<Vec<Arc<Call>>>,
    script: Mutex<Script>,
}

impl ScriptedSender {
    fn new() -> Arc<ScriptedSender> {
        Arc::new(ScriptedSender {
            calls: Mutex::new(Vec::new()),
            script: Mutex::new(Arc::new(|_| Box::pin(async { RawOutcome::Success }))),
        })
    }

    fn script<F, Fut>(&self, f: F)
    where
        F: Fn(Arc<Call>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = RawOutcome> + Send + 'static,
    {
        *self.script.lock().unwrap() = Arc::new(move |call| Box::pin(f(call)));
    }

    fn calls(&self) -> Vec<Arc<Call>> {
        self.calls.lock().unwrap().clone()
    }

    fn keys(&self) -> Vec<String> {
        self.calls()
            .iter()
            .map(|c| c.headers["idempotency-key"].clone())
            .collect()
    }
}

impl RawCommandSender for ScriptedSender {
    fn send_raw<'a>(
        &'a self,
        type_name: &'a str,
        payload: Vec<u8>,
        options: SendOptions,
    ) -> BoxFuture<'a, RawOutcome> {
        Box::pin(async move {
            let current = MessageContext::current();
            let call = Arc::new(Call {
                type_name: type_name.to_string(),
                payload: String::from_utf8(payload).unwrap(),
                headers: options.headers,
                correlation_id: current.as_ref().map(|c| c.correlation_id.clone()),
                traceparent: current
                    .as_ref()
                    .and_then(|c| c.headers.get("traceparent").cloned()),
                retry: options.retry,
                has_authorization: current.is_some_and(|c| c.authorization.is_some()),
            });
            self.calls.lock().unwrap().push(Arc::clone(&call));
            let script = self.script.lock().unwrap().clone();
            script(call).await
        })
    }
}

#[derive(Default)]
struct RelayMetrics {
    unhandled: Mutex<Vec<(String, FailureReason)>>,
}

impl SagaMetrics for RelayMetrics {
    fn command_failure_unhandled(&self, command_type: &str, reason: FailureReason) {
        self.unhandled
            .lock()
            .unwrap()
            .push((command_type.to_string(), reason));
    }
}

fn settings() -> RelaySettings {
    RelaySettings {
        poll_interval: Duration::from_millis(50),
        lease: Duration::from_secs(30),
        max_attempts: 3,
        initial_backoff: Duration::from_secs(1),
        multiplier: 2.0,
        max_backoff: Duration::from_secs(300),
        ..RelaySettings::default()
    }
}

struct Rl {
    store: Store,
    adapter: Arc<Adapter>,
    watcher: Arc<Watcher>,
    sender: Arc<ScriptedSender>,
    metrics: Arc<RelayMetrics>,
    runtime: Arc<SagaRuntime>,
    registry: Arc<HandlerRegistry>,
}

impl Rl {
    async fn start() -> Rl {
        Self::start_with(false).await
    }

    async fn start_with(with_watcher: bool) -> Rl {
        let store = Store::start().await;
        let adapter = Arc::new(Adapter::default());
        let watcher = Arc::new(Watcher::default());
        let registry = registry(&adapter, with_watcher.then_some(&watcher));
        let runtime = Arc::new(SagaRuntime::new(
            vec![order_saga(), quiet_saga(), deaf_saga(), grumpy_saga()],
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
        Rl {
            store,
            adapter,
            watcher,
            sender: ScriptedSender::new(),
            metrics: Arc::new(RelayMetrics::default()),
            runtime,
            registry,
        }
    }

    fn relay(&self) -> Arc<SagaDispatchRelay> {
        self.relay_with(self.sender.clone(), self.runtime.clone(), settings())
    }

    fn local_relay(&self) -> Arc<SagaDispatchRelay> {
        self.relay_with(
            Arc::new(LocalRawCommandSender::new(Arc::clone(&self.registry))),
            self.runtime.clone(),
            settings(),
        )
    }

    fn relay_with(
        &self,
        commands: Arc<dyn RawCommandSender>,
        stage: Arc<dyn SagaStage>,
        settings: RelaySettings,
    ) -> Arc<SagaDispatchRelay> {
        let dispatcher = Arc::new(LocalEventDispatcher::new(
            Some(self.store.message_store()),
            Arc::clone(&self.registry),
            stage,
        ));
        SagaDispatchRelay::new(
            self.store.message_store().tx(),
            self.store.message_store().sagas().dispatch,
            commands,
            EventBus::over(self.store.message_store().outbox()),
            dispatcher,
            self.store.clock.clock(),
            settings,
            self.metrics.clone(),
        )
        .unwrap()
    }

    async fn deliver<E: Event>(&self, event: &E, id: &str) {
        self.deliver_with(event, id, &[]).await;
    }

    /// [`deliver`](Self::deliver) with these headers on the event besides `id` and `type`.
    async fn deliver_with<E: Event>(&self, event: &E, id: &str, headers: &[(&str, &str)]) {
        use snodra_messaging::events::SagaStage;
        self.runtime
            .process(
                E::TYPE,
                &serde_json::to_vec(event).unwrap(),
                &event_ctx(id, E::TYPE, headers),
            )
            .await
            .unwrap();
    }

    async fn saga_of(&self, type_name: &str, order_id: &str) -> SagaDocument {
        self.store
            .sagas(type_name)
            .await
            .into_iter()
            .find(|s| state(s)["orderId"] == order_id)
            .unwrap_or_else(|| panic!("no {type_name} saga for {order_id}"))
    }

    async fn entry(&self, id: &str) -> SagaDispatchDocument {
        self.store
            .dispatch()
            .await
            .into_iter()
            .find(|e| e.id == id)
            .unwrap_or_else(|| panic!("no dispatch entry {id}"))
    }

    async fn failures_of(&self, type_name: &str, order_id: &str) -> Vec<String> {
        let saga = self.saga_of(type_name, order_id).await;
        state(&saga)["failures"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    }

    /// Records an entry as a saga transaction would have; `seq` decides its place in the saga's order.
    async fn record(&self, saga_id: &str, seq: i64, type_name: &str, payload: &str) {
        self.record_entry(
            saga_id,
            seq,
            type_name,
            payload,
            DispatchKind::Command,
            None,
        )
        .await;
    }

    async fn record_entry(
        &self,
        saga_id: &str,
        seq: i64,
        type_name: &str,
        payload: &str,
        kind: DispatchKind,
        routing_key: Option<&str>,
    ) {
        let entry = SagaDispatchDocument {
            id: format!("{saga_id}:{seq}"),
            saga_id: saga_id.into(),
            seq,
            kind,
            type_name: type_name.into(),
            payload: payload.into(),
            headers: HashMap::from([("correlation-id".to_string(), format!("corr-{saga_id}"))]),
            routing_key: routing_key.map(str::to_string),
            created_at_ms: self.store.clock.clock().now_ms(),
            dispatched_at_ms: None,
            attempts: 0,
            next_attempt_at_ms: None,
            failed_at_ms: None,
            failure_reason: None,
            failure_message: None,
            notice_pending: None,
            notice_attempts: None,
            notice_next_at_ms: None,
        };
        // Written as stored, without the checks of `record` (an entry from before them).
        self.store.edit(|c| c.dispatch.push(entry));
    }

    async fn record_charge(&self, saga_id: &str, seq: i64) {
        self.record(saga_id, seq, "rl.Charge", &format!(r#"{{"ref":"r{seq}"}}"#))
            .await;
    }
}

async fn await_true<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !check().await {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn entries_of_one_saga_are_dispatched_in_seq_order() {
    let rl = Rl::start().await;
    rl.record_charge("s1", 3000).await;
    rl.record_charge("s1", 1000).await;
    rl.record_charge("s1", 2000).await;

    rl.relay().poll_once().await.unwrap();

    assert_eq!(rl.sender.keys(), ["s1:1000", "s1:2000", "s1:3000"]);
    assert_eq!(
        rl.store
            .dispatch()
            .await
            .iter()
            .filter(|e| e.dispatched_at_ms.is_none())
            .count(),
        0
    );
}

#[tokio::test]
async fn the_oldest_work_is_dispatched_first_when_more_sagas_are_due_than_one_pass_takes() {
    let rl = Rl::start().await;
    // Created in this order: the opposite of their ids.
    for (i, name) in ["h", "g", "f", "e", "d", "c", "b", "a"].iter().enumerate() {
        rl.store.clock.set(t0() + Duration::from_secs(i as u64));
        rl.record_charge(name, 1000).await;
    }
    rl.store.clock.set(t0() + Duration::from_secs(100));
    let one_at_a_time = RelaySettings {
        parallelism: 1,
        batch_size: 3,
        ..settings()
    };
    let relay = rl.relay_with(rl.sender.clone(), rl.runtime.clone(), one_at_a_time);

    relay.poll_once().await.unwrap();
    assert_eq!(
        rl.sender.keys(),
        ["h:1000", "g:1000", "f:1000"],
        "the three oldest, oldest first"
    );

    relay.poll_once().await.unwrap();
    assert_eq!(
        rl.sender.keys(),
        ["h:1000", "g:1000", "f:1000", "e:1000", "d:1000", "c:1000"]
    );
}

#[tokio::test]
async fn the_relay_sends_with_a_no_retry_policy_so_a_bus_s_own_retry_never_stacks_under_the_relays()
{
    let rl = Rl::start().await;
    rl.record_charge("s1", 1000).await;

    rl.relay().poll_once().await.unwrap();

    assert_eq!(rl.sender.calls()[0].retry, Some(RetryPolicy::NONE));
}

#[tokio::test]
async fn entries_of_different_sagas_are_dispatched_in_parallel() {
    let rl = Rl::start().await;
    rl.record_charge("a", 1000).await;
    rl.record_charge("b", 1000).await;
    let both = Arc::new(tokio::sync::Barrier::new(2));
    rl.sender.script(move |_| {
        let both = Arc::clone(&both);
        async move {
            // Each send waits for the other saga's send: only parallel dispatch lets both through.
            match tokio::time::timeout(Duration::from_secs(5), both.wait()).await {
                Ok(_) => RawOutcome::Success,
                Err(_) => {
                    RawOutcome::Transient("the other saga was never dispatched meanwhile".into())
                }
            }
        }
    });

    rl.relay().poll_once().await.unwrap();

    let mut keys = rl.sender.keys();
    keys.sort();
    assert_eq!(keys, ["a:1000", "b:1000"]);
    assert_eq!(
        rl.store
            .dispatch()
            .await
            .iter()
            .filter(|e| e.dispatched_at_ms.is_none())
            .count(),
        0
    );
}

#[tokio::test]
async fn a_saga_whose_adapter_call_hangs_does_not_hold_back_the_dispatch_of_another_saga() {
    let rl = Rl::start().await;
    rl.record_charge("hung", 1000).await;
    let release = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::clone(&release);
    rl.sender.script(move |call| {
        let gate = Arc::clone(&gate);
        async move {
            if call.headers["saga-id"] == "hung" {
                gate.notified().await;
            }
            RawOutcome::Success
        }
    });
    let cancel = CancellationToken::new();
    let task = rl.relay().start(cancel.clone());

    await_true("the hung saga's send to start", || async {
        rl.sender.keys().contains(&"hung:1000".to_string())
    })
    .await;
    rl.record_charge("fine", 1000).await; // becomes due while the first send is still alive

    await_true("the other saga to be dispatched meanwhile", || async {
        rl.entry("fine:1000").await.dispatched_at_ms.is_some()
    })
    .await;
    assert!(rl.entry("hung:1000").await.dispatched_at_ms.is_none());
    assert_eq!(
        rl.sender.keys(),
        ["hung:1000", "fine:1000"],
        "the hung saga was not sent a second time either"
    );

    release.notify_one();
    await_true("the released send to be marked", || async {
        rl.entry("hung:1000").await.dispatched_at_ms.is_some()
    })
    .await;
    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn the_lease_of_a_saga_is_renewed_while_its_send_is_alive_so_another_relay_does_not_take_it_over()
 {
    let rl = Rl::start().await;
    rl.record_charge("slow", 1000).await;
    let short = RelaySettings {
        lease: Duration::from_millis(600),
        ..settings()
    };
    let release = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::clone(&release);
    rl.sender.script(move |_| {
        let gate = Arc::clone(&gate);
        async move {
            gate.notified().await;
            RawOutcome::Success
        }
    });
    let first = rl.relay_with(rl.sender.clone(), rl.runtime.clone(), short.clone());
    let second = rl.relay_with(rl.sender.clone(), rl.runtime.clone(), short);
    let cancel = CancellationToken::new();
    let task = first.start(cancel.clone());

    await_true("the send to start", || async {
        rl.sender.calls().len() == 1
    })
    .await;
    rl.store.clock.advance(Duration::from_secs(3600)); // far past the lease the send started with
    await_true("the lease to be renewed to the new time", || async {
        let lease = rl
            .store
            .contents()
            .dispatch_leases
            .into_iter()
            .find(|(saga, _, _)| saga == "slow")
            .expect("the lease of slow");
        lease.2 > snodra_messaging::time::epoch_ms(rl.store.clock.now())
    })
    .await;

    second.poll_once().await.unwrap();
    assert_eq!(
        rl.sender.calls().len(),
        1,
        "the live send still owns the saga: no second send of the same key"
    );

    release.notify_one();
    await_true("the entry to be marked", || async {
        rl.entry("slow:1000").await.dispatched_at_ms.is_some()
    })
    .await;
    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn a_step_in_retry_holds_back_that_sagas_later_steps_only() {
    let rl = Rl::start().await;
    rl.record("a", 1000, "rl.First", "{}").await;
    rl.record("a", 2000, "rl.Second", "{}").await;
    rl.record("b", 1000, "rl.Other", "{}").await;
    let first_fails = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let fails = Arc::clone(&first_fails);
    rl.sender.script(move |call| {
        let transient = call.type_name == "rl.First" && fails.load(Ordering::SeqCst);
        async move {
            if transient {
                RawOutcome::Transient("hub down".into())
            } else {
                RawOutcome::Success
            }
        }
    });
    let relay = rl.relay();

    relay.poll_once().await.unwrap();

    let mut types: Vec<_> = rl
        .sender
        .calls()
        .iter()
        .map(|c| c.type_name.clone())
        .collect();
    types.sort();
    assert_eq!(
        types,
        ["rl.First", "rl.Other"],
        "the second step of saga a waits, saga b goes on"
    );
    let first = rl.entry("a:1000").await;
    assert_eq!(first.attempts, 1);
    assert!(first.next_attempt_at_ms.unwrap() > snodra_messaging::time::epoch_ms(t0()));
    assert!(rl.entry("a:2000").await.dispatched_at_ms.is_none());
    assert!(rl.entry("b:1000").await.dispatched_at_ms.is_some());

    relay.poll_once().await.unwrap();
    assert_eq!(rl.sender.calls().len(), 2, "still backing off");

    first_fails.store(false, Ordering::SeqCst);
    rl.store.clock.advance(Duration::from_secs(3600));
    relay.poll_once().await.unwrap();
    let of_a: Vec<_> = rl
        .sender
        .calls()
        .iter()
        .filter(|c| c.headers["saga-id"] == "a")
        .map(|c| c.type_name.clone())
        .collect();
    assert_eq!(
        of_a,
        ["rl.First", "rl.First", "rl.Second"],
        "in order, the retried step first"
    );
    assert!(rl.entry("a:2000").await.dispatched_at_ms.is_some());
}

#[tokio::test]
async fn an_event_entry_reaches_the_outbox_exactly_once() {
    let rl = Rl::start().await;
    rl.record_entry(
        "s1",
        1000,
        "rl.Announced",
        r#"{"orderId":"o1"}"#,
        DispatchKind::Event,
        Some("key-o1"),
    )
    .await;
    let relay = rl.relay();
    relay.fail_next_mark(); // the crash between inserting into the outbox and marking: one transaction, so neither survives

    relay.poll_once().await.unwrap();
    assert_eq!(
        rl.store.outbox().await.len(),
        0,
        "the outbox insert rolled back with the failed mark"
    );
    assert!(rl.entry("s1:1000").await.dispatched_at_ms.is_none());

    relay.poll_once().await.unwrap();
    relay.poll_once().await.unwrap();

    let outbox: Vec<OutboxDocument> = rl
        .store
        .outbox()
        .await
        .into_iter()
        .map(outbox_document)
        .collect();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].type_name, "rl.Announced");
    assert_eq!(outbox[0].key.as_deref(), Some("key-o1"));
    assert_eq!(outbox[0].payload["orderId"], "o1");
    assert_eq!(outbox[0].headers["correlationId"], "corr-s1");
    assert!(rl.entry("s1:1000").await.dispatched_at_ms.is_some());
}

#[tokio::test]
async fn an_event_entry_that_cannot_become_an_outbox_document_fails_for_good_and_the_sagas_later_entries_go_out()
 {
    let rl = Rl::start().await;
    // A unit struct (null), an array and a number above i64::MAX: none is a BSON document.
    for (seq, payload) in [
        (1000, "null"),
        (1001, "[1,2]"),
        (1002, r#"{"n":18446744073709551615}"#),
        (1003, r#"{"orderId":"o9"}"#),
    ] {
        rl.record_entry(
            "s9",
            seq,
            "rl.Announced",
            payload,
            DispatchKind::Event,
            None,
        )
        .await;
    }
    let captured = support::logs::Captured::default();
    let _guard = captured.install();
    let relay = rl.relay();

    relay.poll_once().await.unwrap();

    for id in ["s9:1000", "s9:1001", "s9:1002"] {
        let entry = rl.entry(id).await;
        assert!(entry.failed_at_ms.is_some(), "{id} failed for good");
        assert!(entry.dispatched_at_ms.is_none(), "{id} was not dispatched");
        assert_eq!(entry.failure_reason, Some(FailureReason::Invalid), "{id}");
        assert!(
            entry
                .failure_message
                .as_deref()
                .is_some_and(|m| m.contains("rl.Announced")),
            "{id}: {:?}",
            entry.failure_message
        );
        assert_eq!(
            entry.notice_pending, None,
            "{id}: an event has no failure notice"
        );
    }
    assert!(
        rl.entry("s9:1003").await.dispatched_at_ms.is_some(),
        "the saga's later entry went out in the same pass"
    );
    let outbox: Vec<OutboxDocument> = rl
        .store
        .outbox()
        .await
        .into_iter()
        .map(outbox_document)
        .collect();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].payload["orderId"], "o9");
    let errors: Vec<_> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("ERROR"))
        .collect();
    assert_eq!(errors.len(), 3, "{errors:?}");
    assert!(errors[0].contains("s9:1000"), "{}", errors[0]);
    assert_eq!(
        *rl.metrics.unhandled.lock().unwrap(),
        vec![("rl.Announced".to_string(), FailureReason::Invalid); 3],
        "each refused event counts on snodra.saga.command.failures{{reason=INVALID}}"
    );
}

#[tokio::test]
async fn a_command_carries_the_idempotency_key_the_saga_id_and_the_correlation_id_of_its_cause() {
    let rl = Rl::start().await;
    rl.deliver(
        &Start {
            order_id: "o1".into(),
        },
        "cause",
    )
    .await;
    let saga = rl.saga_of("rl.Order", "o1").await;
    let entry = rl.store.dispatch_of(&saga.id).await.remove(0);

    rl.relay().poll_once().await.unwrap();

    let calls = rl.sender.calls();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.type_name, "rl.Charge");
    assert_eq!(call.payload, r#"{"ref":"o1"}"#);
    assert_eq!(
        call.headers["idempotency-key"],
        format!("{}:{}", saga.id, entry.seq)
    );
    assert_eq!(call.headers["saga-id"], saga.id);
    assert_eq!(call.correlation_id.as_deref(), Some("corr-cause"));
    assert!(
        !call.has_authorization,
        "a saga command carries no caller identity"
    );
}

const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// The trace floor: a command a saga dispatches is sent in the trace of the event that caused it
/// (its valid `traceparent`, kept in the dispatch entry's headers), never as a header of its own
/// (`traceparent` is reserved); an invalid one is not kept.
#[tokio::test]
async fn a_saga_command_carries_the_traceparent_of_its_cause_a_valid_one_only() {
    let rl = Rl::start().await;
    rl.deliver_with(
        &Start {
            order_id: "o1".into(),
        },
        "cause-1",
        &[("traceparent", TRACEPARENT)],
    )
    .await;
    rl.deliver_with(
        &Start {
            order_id: "o2".into(),
        },
        "cause-2",
        &[("traceparent", "not-a-traceparent")],
    )
    .await;
    let entry = |order: &str| {
        let rl = &rl;
        let order = order.to_string();
        async move {
            let saga = rl.saga_of("rl.Order", &order).await;
            rl.store.dispatch_of(&saga.id).await.remove(0)
        }
    };
    assert_eq!(
        entry("o1")
            .await
            .headers
            .get("traceparent")
            .map(String::as_str),
        Some(TRACEPARENT),
        "kept with the entry"
    );
    assert_eq!(entry("o2").await.headers.get("traceparent"), None);

    rl.relay().poll_once().await.unwrap();
    rl.relay().poll_once().await.unwrap();

    let calls = rl.sender.calls();
    assert_eq!(calls.len(), 2);
    let of = |r: &str| {
        calls
            .iter()
            .find(|c| c.payload.contains(r))
            .unwrap_or_else(|| panic!("no call for {r}"))
    };
    assert_eq!(of("o1").traceparent.as_deref(), Some(TRACEPARENT));
    assert_eq!(of("o2").traceparent, None);
    assert!(
        calls.iter().all(|c| !c.headers.contains_key("traceparent")),
        "never a header of the send's own (reserved)"
    );
}

/// An event a saga publishes carries the `traceparent` recorded with its dispatch entry.
#[tokio::test]
async fn a_saga_event_carries_the_traceparent_recorded_with_it() {
    let rl = Rl::start().await;
    rl.store.edit(|c| {
        c.dispatch.push(SagaDispatchDocument {
            id: "s1:1000".into(),
            saga_id: "s1".into(),
            seq: 1000,
            kind: DispatchKind::Event,
            type_name: "rl.Announced".into(),
            payload: r#"{"orderId":"o1"}"#.into(),
            headers: HashMap::from([
                ("correlation-id".to_string(), "corr-s1".to_string()),
                ("traceparent".to_string(), TRACEPARENT.to_string()),
            ]),
            routing_key: None,
            created_at_ms: rl.store.clock.clock().now_ms(),
            dispatched_at_ms: None,
            attempts: 0,
            next_attempt_at_ms: None,
            failed_at_ms: None,
            failure_reason: None,
            failure_message: None,
            notice_pending: None,
            notice_attempts: None,
            notice_next_at_ms: None,
        })
    });

    rl.relay().poll_once().await.unwrap();

    let outbox: Vec<OutboxDocument> = rl
        .store
        .outbox()
        .await
        .into_iter()
        .map(outbox_document)
        .collect();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].headers["traceparent"], TRACEPARENT);
    assert_eq!(outbox[0].headers["correlationId"], "corr-s1");
}

#[tokio::test]
async fn a_crash_after_the_send_and_before_marking_re_sends_with_the_same_key() {
    let rl = Rl::start().await;
    rl.record_charge("s1", 1000).await;
    let relay = rl.relay();
    relay.fail_next_mark();

    relay.poll_once().await.unwrap();
    assert!(
        rl.entry("s1:1000").await.dispatched_at_ms.is_none(),
        "the mark failed"
    );

    relay.poll_once().await.unwrap();

    assert_eq!(rl.sender.keys(), ["s1:1000", "s1:1000"]);
    assert!(rl.entry("s1:1000").await.dispatched_at_ms.is_some());
    relay.poll_once().await.unwrap();
    assert_eq!(
        rl.sender.calls().len(),
        2,
        "marked dispatched: never sent again"
    );
}

#[tokio::test]
async fn transient_failures_back_off_and_become_exhausted_after_the_limit() {
    let rl = Rl::start().await;
    rl.deliver(
        &Start {
            order_id: "o1".into(),
        },
        "e-1",
    )
    .await;
    let saga = rl.saga_of("rl.Order", "o1").await;
    let key = format!(
        "{}:{}",
        saga.id,
        rl.store.dispatch_of(&saga.id).await[0].seq
    );
    rl.sender
        .script(|_| async { RawOutcome::Transient("hub down".into()) });
    let relay = rl.relay();

    relay.poll_once().await.unwrap();
    assert_eq!(rl.entry(&key).await.attempts, 1);
    relay.poll_once().await.unwrap();
    assert_eq!(
        rl.sender.calls().len(),
        1,
        "no second attempt before the backoff passed"
    );

    rl.store.clock.advance(Duration::from_secs(3600));
    relay.poll_once().await.unwrap();
    assert_eq!(rl.entry(&key).await.attempts, 2);
    assert!(rl.entry(&key).await.failed_at_ms.is_none());
    rl.store.clock.advance(Duration::from_secs(3600));
    relay.poll_once().await.unwrap();

    assert_eq!(rl.sender.calls().len(), 3);
    assert!(
        rl.entry(&key).await.failed_at_ms.is_some(),
        "the limit of 3 attempts is reached"
    );
    assert_eq!(
        rl.failures_of("rl.Order", "o1").await,
        ["rl.Charge:EXHAUSTED"]
    );
    relay.poll_once().await.unwrap();
    assert_eq!(rl.sender.calls().len(), 3, "given up: never sent again");
}

#[tokio::test]
async fn an_adapter_failure_marked_retryable_backs_off_like_a_transient_one() {
    let rl = Rl::start().await;
    rl.deliver(
        &Start {
            order_id: "o1".into(),
        },
        "e-1",
    )
    .await;
    let saga_id = rl.saga_of("rl.Order", "o1").await.id;
    rl.record(&saga_id, 9000, "rl.Busy", r#"{"ref":"b"}"#).await;

    rl.local_relay().poll_once().await.unwrap();

    assert_eq!(*rl.adapter.busy_calls.lock().unwrap(), ["b"]);
    let busy = rl.entry(&format!("{saga_id}:9000")).await;
    assert_eq!(busy.attempts, 1);
    assert!(busy.failed_at_ms.is_none(), "retried later, not failed");
    assert_eq!(rl.failures_of("rl.Order", "o1").await, Vec::<String>::new());
}

#[tokio::test]
async fn no_handler_denied_invalid_and_failed_become_a_failure_event_at_once_and_a_saga_handling_it_receives_it()
 {
    let rl = Rl::start().await;
    rl.deliver(
        &Start {
            order_id: "o1".into(),
        },
        "e-1",
    )
    .await;
    let saga_id = rl.saga_of("rl.Order", "o1").await.id;
    rl.record(&saga_id, 9001, "rl.Nobody", r#"{"ref":"n"}"#)
        .await;
    rl.record(&saga_id, 9002, "rl.Secured", r#"{"ref":"s"}"#)
        .await;
    rl.record(&saga_id, 9003, "rl.Charge", r#"{"bad":1}"#).await;
    rl.record(&saga_id, 9004, "rl.Boom", r#"{"ref":"b"}"#).await;
    rl.record(&saga_id, 9005, "rl.Charge", r#"{"ref":"after"}"#)
        .await;
    let relay = rl.local_relay();

    relay.poll_once().await.unwrap();

    assert_eq!(
        rl.failures_of("rl.Order", "o1").await,
        [
            "rl.Nobody:NO_HANDLER",
            "rl.Secured:DENIED",
            "rl.Charge:INVALID",
            "rl.Boom:FAILED"
        ]
    );
    for seq in 9001..=9004 {
        assert!(
            rl.entry(&format!("{saga_id}:{seq}"))
                .await
                .failed_at_ms
                .is_some(),
            "{seq}"
        );
    }
    let charged: Vec<_> = rl
        .adapter
        .charged
        .lock()
        .unwrap()
        .iter()
        .map(|(r, _)| r.clone())
        .collect();
    assert_eq!(
        charged,
        ["o1", "after"],
        "a failed step does not hold back the saga's later steps"
    );
    relay.poll_once().await.unwrap();
    assert_eq!(
        rl.adapter.charged.lock().unwrap().len(),
        2,
        "failed steps are not sent again"
    );
    assert_eq!(
        rl.failures_of("rl.Order", "o1").await.len(),
        4,
        "nor their failure events delivered again"
    );
}

#[tokio::test]
async fn a_command_whose_outcome_is_known_is_never_re_sent_while_its_failure_notice_is_retried() {
    let rl = Rl::start_with(true).await;
    rl.deliver(
        &Start {
            order_id: "o1".into(),
        },
        "e-1",
    )
    .await;
    let saga = rl.saga_of("rl.Order", "o1").await;
    let key = format!(
        "{}:{}",
        saga.id,
        rl.store.dispatch_of(&saga.id).await[0].seq
    );
    rl.sender.script(|_| async {
        RawOutcome::Permanent(FailureReason::Failed, "printer on fire".into())
    });
    rl.watcher.fail_first.store(2, Ordering::SeqCst);
    let relay = rl.relay();

    relay.poll_once().await.unwrap();
    assert_eq!(rl.sender.calls().len(), 1);
    assert!(
        rl.entry(&key).await.failed_at_ms.is_some(),
        "the permanent outcome is persisted before anything else can fail"
    );
    assert_eq!(rl.watcher.calls.load(Ordering::SeqCst), 1);

    relay.poll_once().await.unwrap();
    assert_eq!(
        rl.watcher.calls.load(Ordering::SeqCst),
        1,
        "the notice waits for its own backoff"
    );
    rl.store.clock.advance(Duration::from_secs(3600));
    relay.poll_once().await.unwrap();
    assert_eq!(
        rl.watcher.calls.load(Ordering::SeqCst),
        2,
        "the notice is retried"
    );
    rl.store.clock.advance(Duration::from_secs(3600));
    relay.poll_once().await.unwrap();
    assert_eq!(rl.watcher.calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        *rl.watcher.seen.lock().unwrap(),
        ["rl.Charge:FAILED"],
        "the ordinary handler finally got it"
    );
    assert_eq!(
        rl.failures_of("rl.Order", "o1").await,
        ["rl.Charge:FAILED"],
        "the saga got it once, however often the notice was retried"
    );

    rl.store.clock.advance(Duration::from_secs(3600));
    relay.poll_once().await.unwrap();
    assert_eq!(
        rl.sender.calls().len(),
        1,
        "the command ran once: its outcome was known"
    );
    assert_eq!(
        rl.watcher.calls.load(Ordering::SeqCst),
        3,
        "the notice was delivered: nothing is pending"
    );
    assert_eq!(
        rl.store
            .dispatch()
            .await
            .iter()
            .filter(|e| e.notice_pending == Some(true))
            .count(),
        0
    );
}

#[tokio::test]
async fn a_failure_notice_that_keeps_failing_is_given_up_after_the_relays_attempts_logged_at_error_and_counted()
 {
    let rl = Rl::start_with(true).await;
    rl.deliver(
        &Start {
            order_id: "o1".into(),
        },
        "e-1",
    )
    .await;
    let saga = rl.saga_of("rl.Order", "o1").await;
    rl.sender.script(|_| async {
        RawOutcome::Permanent(FailureReason::Failed, "printer on fire".into())
    });
    rl.watcher.fail_first.store(usize::MAX, Ordering::SeqCst);
    let captured = support::logs::Captured::default();
    let _guard = captured.install();
    let relay = rl.relay();

    for _ in 0..5 {
        relay.poll_once().await.unwrap();
        rl.store.clock.advance(Duration::from_secs(3600));
    }

    assert_eq!(
        rl.watcher.calls.load(Ordering::SeqCst),
        3,
        "bounded: 3 attempts in all"
    );
    assert_eq!(rl.sender.calls().len(), 1);
    assert_eq!(
        *rl.metrics.unhandled.lock().unwrap(),
        [("rl.Charge".to_string(), FailureReason::Failed)]
    );
    assert!(
        captured.lines().iter().any(|l| l.contains("ERROR")
            && l.contains(&saga.id)
            && l.contains("the saga may have applied it")),
        "the give-up is logged at error, without claiming the saga never got it: {}",
        captured.text()
    );
    assert!(
        !captured.text().contains("never heard of it"),
        "the sagas run even when an ordinary handler fails: {}",
        captured.text()
    );
    assert_eq!(
        rl.store
            .dispatch()
            .await
            .iter()
            .filter(|e| e.notice_pending == Some(true))
            .count(),
        0
    );
}

#[tokio::test]
async fn an_unhandled_failure_event_is_logged_at_error_and_counted() {
    let rl = Rl::start().await;
    rl.record("lost", 1000, "rl.Nobody", "{}").await;
    let captured = support::logs::Captured::default();
    let _guard = captured.install();
    let relay = rl.relay_with(
        Arc::new(LocalRawCommandSender::new(Arc::clone(&rl.registry))),
        Arc::new(NoSagas),
        settings(),
    );

    relay.poll_once().await.unwrap();

    assert_eq!(
        *rl.metrics.unhandled.lock().unwrap(),
        [("rl.Nobody".to_string(), FailureReason::NoHandler)]
    );
    let errors: Vec<_> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("ERROR"))
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].contains("lost:1000") && errors[0].contains("NO_HANDLER"),
        "{}",
        errors[0]
    );
    assert!(
        errors[0].contains("nothing in this service handles SagaCommandFailed"),
        "no handler at all, not a missing live saga: {}",
        errors[0]
    );
    assert!(rl.entry("lost:1000").await.failed_at_ms.is_some());
}

#[tokio::test]
async fn a_failure_event_reaches_a_saga_that_handles_it_without_associating_itself_with_its_own_id()
{
    let rl = Rl::start().await;
    rl.deliver(
        &QuietStart {
            order_id: "q1".into(),
        },
        "e-q1",
    )
    .await;
    let saga = rl.saga_of("rl.Quiet", "q1").await;
    assert_eq!(
        saga.associations.len(),
        1,
        "the saga never associated with its own id"
    );
    rl.record(&saga.id, 9001, "rl.Nobody", r#"{"ref":"n"}"#)
        .await;

    rl.local_relay().poll_once().await.unwrap();

    assert_eq!(
        rl.failures_of("rl.Quiet", "q1").await,
        ["rl.Nobody:NO_HANDLER"]
    );
    assert!(rl.metrics.unhandled.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_failure_event_for_an_ended_saga_or_one_whose_type_has_no_handler_is_logged_at_error_and_counted()
 {
    let rl = Rl::start().await;
    rl.deliver(
        &StartAndEnd {
            order_id: "o9".into(),
        },
        "e-9",
    )
    .await;
    let ended = rl.saga_of("rl.Order", "o9").await;
    assert!(ended.ended);
    rl.deliver(
        &DeafStart {
            order_id: "d1".into(),
        },
        "e-d1",
    )
    .await;
    let deaf = rl.saga_of("rl.Deaf", "d1").await;
    // The ended saga's own entries are dispatched too: take them out of the way.
    let now = snodra_messaging::time::now_ms();
    rl.store.edit(|c| {
        c.dispatch
            .iter_mut()
            .for_each(|e| e.dispatched_at_ms = Some(now))
    });
    rl.record(&ended.id, 9001, "rl.Nobody", r#"{"ref":"n"}"#)
        .await;
    rl.record(&deaf.id, 9001, "rl.Nobody", r#"{"ref":"n"}"#)
        .await;
    let captured = support::logs::Captured::default();
    let _guard = captured.install();

    rl.local_relay().poll_once().await.unwrap();

    assert_eq!(
        *rl.metrics.unhandled.lock().unwrap(),
        [
            ("rl.Nobody".to_string(), FailureReason::NoHandler),
            ("rl.Nobody".to_string(), FailureReason::NoHandler)
        ]
    );
    let errors: Vec<_> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("ERROR"))
        .collect();
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        errors
            .iter()
            .any(|l| l.contains(&format!("{}:9001", ended.id)))
    );
    assert!(
        errors
            .iter()
            .any(|l| l.contains(&format!("{}:9001", deaf.id)))
    );
    assert!(
        errors
            .iter()
            .all(|l| l.contains("no live saga with a SagaCommandFailed handler took it")),
        "sagas handle the type, but no live instance took it: {errors:?}"
    );
    assert!(
        rl.entry(&format!("{}:9001", ended.id))
            .await
            .failed_at_ms
            .is_some()
    );
}

#[tokio::test]
async fn entries_recorded_before_a_saga_ended_are_still_dispatched() {
    let rl = Rl::start().await;
    rl.deliver(
        &StartAndEnd {
            order_id: "o9".into(),
        },
        "e-9",
    )
    .await;
    let saga = rl.saga_of("rl.Order", "o9").await;
    assert!(saga.ended);

    rl.relay().poll_once().await.unwrap();

    let calls = rl.sender.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].type_name, "rl.Charge");
    assert_eq!(calls[0].payload, r#"{"ref":"ended-o9"}"#);
    let outbox: Vec<OutboxDocument> = rl
        .store
        .outbox()
        .await
        .into_iter()
        .map(outbox_document)
        .collect();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].type_name, "rl.Announced");
    assert!(
        rl.store
            .dispatch_of(&saga.id)
            .await
            .iter()
            .all(|e| e.dispatched_at_ms.is_some())
    );
}

#[tokio::test]
async fn two_relays_never_dispatch_one_sagas_entries_concurrently() {
    let rl = Rl::start().await;
    rl.record_charge("s1", 1000).await;
    rl.record_charge("s1", 2000).await;
    let in_flight = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (entered, gate) = (Arc::clone(&in_flight), Arc::clone(&release));
    rl.sender.script(move |call| {
        let (entered, gate) = (Arc::clone(&entered), Arc::clone(&gate));
        async move {
            if call.headers["idempotency-key"] == "s1:1000" {
                entered.notify_one();
                gate.notified().await;
            }
            RawOutcome::Success
        }
    });
    let first = rl.relay();
    let second = rl.relay();
    let running = {
        let first = Arc::clone(&first);
        tokio::spawn(async move { first.poll_once().await.unwrap() })
    };
    tokio::time::timeout(Duration::from_secs(10), in_flight.notified())
        .await
        .expect("the first relay is inside the first step");

    second.poll_once().await.unwrap();

    assert_eq!(
        rl.sender.keys(),
        ["s1:1000"],
        "the second relay must not send the later step past the first"
    );
    release.notify_one();
    running.await.unwrap();
    assert_eq!(rl.sender.keys(), ["s1:1000", "s1:2000"]);
    assert_eq!(
        rl.store.contents().dispatch_leases.len(),
        0,
        "the lease is released when the saga's turn is done"
    );
}

#[tokio::test]
async fn a_saga_command_reaches_a_handler_without_a_guard_and_is_denied_by_one_that_requires_authentication()
 {
    let rl = Rl::start().await;
    rl.deliver(
        &Start {
            order_id: "o1".into(),
        },
        "e-1",
    )
    .await;
    let saga_id = rl.saga_of("rl.Order", "o1").await.id;
    rl.record(&saga_id, 9002, "rl.Secured", r#"{"ref":"s"}"#)
        .await;
    let relay = rl.local_relay();
    // Whoever's handler context the poll runs in is not the saga's caller.
    let user = MessageContext {
        correlation_id: "user-corr".into(),
        authorization: Some("Bearer user-token".into()),
        ..Default::default()
    };

    CURRENT
        .scope(user, async { relay.poll_once().await.unwrap() })
        .await;

    {
        let charged = rl.adapter.charged.lock().unwrap();
        assert_eq!(
            charged.iter().map(|(r, _)| r.as_str()).collect::<Vec<_>>(),
            ["o1"],
            "the open handler ran"
        );
        let ctx = &charged[0].1;
        assert!(
            ctx.identity.is_none() && ctx.authorization.is_none(),
            "{ctx:?}"
        );
    }
    assert_eq!(
        rl.failures_of("rl.Order", "o1").await,
        ["rl.Secured:DENIED"]
    );
}

#[tokio::test]
async fn the_started_relay_stops_when_cancelled_and_waits_for_the_sends_in_flight() {
    let rl = Rl::start().await;
    rl.record_charge("s1", 1000).await;
    let release = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::clone(&release);
    rl.sender.script(move |_| {
        let gate = Arc::clone(&gate);
        async move {
            gate.notified().await;
            RawOutcome::Success
        }
    });
    let cancel = CancellationToken::new();
    let task = rl.relay().start(cancel.clone());
    await_true("the send to start", || async {
        rl.sender.calls().len() == 1
    })
    .await;

    cancel.cancel();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !task.is_finished(),
        "the relay waits for the send in flight"
    );
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();

    assert!(
        rl.entry("s1:1000").await.dispatched_at_ms.is_some(),
        "the send in flight was marked"
    );
}

#[tokio::test]
async fn a_failure_notice_whose_saga_handler_fails_is_parked_and_not_reported_as_unhandled() {
    let rl = Rl::start().await;
    rl.deliver(
        &GrumpyStart {
            order_id: "g1".into(),
        },
        "e-g1",
    )
    .await;
    let saga = rl.saga_of("rl.Grumpy", "g1").await;
    rl.record(&saga.id, 9001, "rl.Nobody", r#"{"ref":"n"}"#)
        .await;
    let captured = support::logs::Captured::default();
    let _guard = captured.install();

    rl.local_relay().poll_once().await.unwrap();

    assert_eq!(
        rl.store
            .failures()
            .await
            .iter()
            .filter(|f| {
                f.saga_type == "rl.Grumpy"
                    && f.event_type == <SagaCommandFailed as snodra_messaging::MessageType>::TYPE
            })
            .count(),
        1,
        "the saga got the notice and its handler failed: parked"
    );
    assert!(
        rl.metrics.unhandled.lock().unwrap().is_empty(),
        "a parked notice reached its saga: not unhandled"
    );
    assert!(
        !captured.text().contains("took it")
            && !captured
                .text()
                .contains("nothing in this service handles SagaCommandFailed"),
        "{}",
        captured.text()
    );
    assert!(
        captured
            .lines()
            .iter()
            .any(|l| l.contains("ERROR") && l.contains("rl.Grumpy") && l.contains("parked")),
        "the park says what happened: {}",
        captured.text()
    );
    assert_eq!(
        rl.store
            .dispatch()
            .await
            .iter()
            .filter(|e| e.notice_pending == Some(true))
            .count(),
        0
    );
}

#[tokio::test]
async fn a_failure_notice_whose_settling_write_fails_is_retried_on_its_own_and_the_sagas_turn_goes_on()
 {
    let rl = Rl::start().await;
    rl.deliver(
        &Start {
            order_id: "o1".into(),
        },
        "e-1",
    )
    .await;
    let saga = rl.saga_of("rl.Order", "o1").await;
    let first = rl.store.dispatch_of(&saga.id).await[0].clone();
    rl.record_charge(&saga.id, first.seq + 1).await;
    rl.sender.script(|_| async {
        RawOutcome::Permanent(FailureReason::Failed, "printer on fire".into())
    });
    let captured = support::logs::Captured::default();
    let _guard = captured.install();
    let relay = rl.relay();
    relay.fail_next_notice_write();

    relay.poll_once().await.unwrap();

    assert_eq!(
        rl.sender.calls().len(),
        2,
        "the first command failed for good, so the saga's next one went out in the same turn"
    );
    assert!(
        !captured.text().contains("trying again at the next poll"),
        "the failed command is never dispatched again: {}",
        captured.text()
    );
    assert!(
        captured.lines().iter().any(|l| l.contains("WARN")
            && l.contains(&first.id)
            && l.contains("Settling the failure notice")),
        "{}",
        captured.text()
    );
    assert_eq!(
        rl.entry(&first.id).await.notice_pending,
        Some(true),
        "the notice was delivered but could not be settled: it stays pending"
    );
    assert_eq!(
        rl.failures_of("rl.Order", "o1").await,
        ["rl.Charge:FAILED", "rl.Charge:FAILED"]
    );

    rl.store.clock.advance(Duration::from_secs(3600));
    relay.poll_once().await.unwrap();

    assert_eq!(
        rl.store
            .dispatch()
            .await
            .iter()
            .filter(|e| e.notice_pending == Some(true))
            .count(),
        0,
        "the notice came again when its claim expired and was settled"
    );
    assert_eq!(
        rl.failures_of("rl.Order", "o1").await,
        ["rl.Charge:FAILED", "rl.Charge:FAILED"],
        "the repeat was harmless"
    );
    assert_eq!(rl.sender.calls().len(), 2);
}
