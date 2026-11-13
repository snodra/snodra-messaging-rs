//! The failure admin (S2 Task 8, ADR 0021 rulings 3 and 36): parked saga events are listed,
//! retried and discarded, against a real MongoDB. The Rust form of the Kotlin `SagaFailuresTest`.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use kraken_messaging::kafka::SagaStage;
use kraken_messaging::saga::ports::SagaPorts;
use kraken_messaging::saga::store::SagaFailureDocument;
use kraken_messaging::saga::{NoopSagaMetrics, SagaFailures, SagaRetry, SagaRuntime, SagaType};
use kraken_messaging::store::{StoreError, Tx, TxProvider};
use kraken_messaging::time::epoch_ms;
use kraken_messaging::{MessageType, MessagingError, RetryPolicy};
use kraken_messaging_mongo::documents::FailureDoc;
use mongodb::bson::doc;
use support::order_saga::*;
use support::saga::{Store, event_ctx, state, t0};

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

fn retry(max_attempts: u32) -> SagaRetry {
    SagaRetry::new(RetryPolicy {
        max_attempts,
        initial_backoff: Duration::from_millis(5),
        multiplier: 1.0,
        max_backoff: Duration::from_millis(20),
    })
}

struct Fx {
    store: Store,
    broken: Arc<AtomicBool>,
    runtime: Arc<SagaRuntime>,
    failures: SagaFailures,
}

impl Fx {
    async fn start() -> Fx {
        let store = Store::start().await;
        let gate = Gate::new();
        let broken = Arc::new(AtomicBool::new(true));
        let runtime = Arc::new(SagaRuntime::new(
            vec![order_saga(&gate), flaky_saga(&broken)],
            store.ports(),
            store.clock.clock(),
            retry(3),
            Arc::new(NoopSagaMetrics),
        ));
        let failures = SagaFailures::new(Arc::clone(&runtime));
        Fx {
            store,
            broken,
            runtime,
            failures,
        }
    }

    async fn deliver(&self, order_id: &str, id: &str) {
        let event = OrderPlaced {
            order_id: order_id.into(),
        };
        let mut ctx = event_ctx(id, OrderPlaced::TYPE, &[("userId", "u-1")]);
        ctx.headers
            .insert("correlationId".into(), format!("corr-{id}"));
        self.runtime
            .process(
                OrderPlaced::TYPE,
                &serde_json::to_vec(&event).unwrap(),
                &ctx,
            )
            .await
            .unwrap();
    }

    async fn ids(&self, saga_type: Option<&str>) -> Vec<String> {
        self.failures
            .list(saga_type)
            .await
            .unwrap()
            .into_iter()
            .map(|f| f.id)
            .collect()
    }
}

#[tokio::test]
async fn retry_applies_a_parked_event_once_and_removes_the_failure() {
    require_docker!();
    let fx = Fx::start().await;
    fx.deliver("f1", "e-f1").await;
    assert_eq!(fx.ids(Some("rt.Flaky")).await, ["rt.Flaky:e-f1"]);
    assert_eq!(
        state(&fx.store.sagas("rt.Order").await[0])["placed"],
        1,
        "the healthy type handled it at once"
    );
    fx.broken.store(false, Ordering::SeqCst);

    assert!(fx.failures.retry("rt.Flaky:e-f1").await.unwrap());

    let saga = fx.store.sagas("rt.Flaky").await.remove(0);
    assert_eq!(state(&saga)["placed"], 1);
    assert_eq!(saga.start_key.as_deref(), Some("rt.Flaky|orderId=f1"));
    assert!(fx.ids(None).await.is_empty());
    assert_eq!(
        fx.store.dispatch_of(&saga.id).await.len(),
        1,
        "its command was recorded for the relay"
    );
    assert_eq!(
        state(&fx.store.sagas("rt.Order").await[0])["placed"],
        1,
        "the other saga type was not re-applied"
    );
    assert!(
        fx.store.marker("saga:rt.Flaky:e-f1").await,
        "the marker of the parking stays"
    );
    assert!(
        !fx.failures.retry("rt.Flaky:e-f1").await.unwrap(),
        "applied once: a second retry finds nothing"
    );
    assert_eq!(fx.store.sagas("rt.Flaky").await.len(), 1);
}

#[tokio::test]
async fn a_retry_that_fails_again_keeps_it_parked_with_attempts_increased() {
    require_docker!();
    let fx = Fx::start().await;
    fx.deliver("f2", "e-f2").await;
    let before = fx.failures.list(Some("rt.Flaky")).await.unwrap().remove(0);
    assert_eq!(before.attempts, 2);
    fx.store.clock.advance(Duration::from_secs(60));

    assert!(!fx.failures.retry(&before.id).await.unwrap());

    let after = fx.failures.list(Some("rt.Flaky")).await.unwrap().remove(0);
    assert_eq!(after.attempts, 4);
    assert_eq!(
        after.last_failed_at_ms,
        epoch_ms(t0() + Duration::from_secs(60))
    );
    assert_eq!(after.first_failed_at_ms, before.first_failed_at_ms);
    assert!(after.error.contains("flaky on f2"), "{}", after.error);
    assert_eq!(fx.store.sagas("rt.Flaky").await.len(), 0);
    assert_eq!(after.payload, before.payload);
}

/// Plays a `discard` committed by someone else right after the `after`th rolled-back attempt of a
/// retry, i.e. between the last failed attempt and the bookkeeping that records it.
struct DiscardAfterRollbacks {
    inner: Arc<dyn TxProvider>,
    db: mongodb::Database,
    failure_id: String,
    after: usize,
    rollbacks: AtomicUsize,
}

impl TxProvider for DiscardAfterRollbacks {
    fn begin(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        self.inner.begin()
    }

    fn plain(&self) -> BoxFuture<'_, Result<Tx, StoreError>> {
        self.inner.plain()
    }

    fn commit<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, Result<(), StoreError>> {
        self.inner.commit(tx)
    }

    fn abort<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.inner.abort(tx).await;
            if self.rollbacks.fetch_add(1, Ordering::SeqCst) + 1 == self.after {
                self.db
                    .collection::<mongodb::bson::Document>("saga_failures")
                    .delete_one(doc! { "_id": &self.failure_id })
                    .await
                    .unwrap();
            }
        })
    }
}

#[tokio::test]
async fn a_discard_that_lands_during_a_failing_retry_is_not_undone_by_the_retrys_bookkeeping() {
    require_docker!();
    let store = Store::start().await;
    let broken = Arc::new(AtomicBool::new(true));
    let base = store.ports();
    let racing = Arc::new(DiscardAfterRollbacks {
        inner: Arc::clone(&base.tx),
        db: store.db.clone(),
        failure_id: "rt.Flaky:e-f9".into(),
        after: 4, // two attempts of the first delivery roll back, then the two of the retry
        rollbacks: AtomicUsize::new(0),
    });
    let ports = SagaPorts {
        tx: racing.clone(),
        ..base
    };
    let types: Vec<SagaType> = vec![flaky_saga(&broken)];
    let runtime = Arc::new(SagaRuntime::new(
        types,
        ports,
        store.clock.clock(),
        retry(3),
        Arc::new(NoopSagaMetrics),
    ));
    let admin = SagaFailures::new(Arc::clone(&runtime));
    runtime
        .process(
            OrderPlaced::TYPE,
            &serde_json::to_vec(&OrderPlaced {
                order_id: "f9".into(),
            })
            .unwrap(),
            &event_ctx("e-f9", OrderPlaced::TYPE, &[]),
        )
        .await
        .unwrap();
    assert_eq!(admin.list(None).await.unwrap().len(), 1);
    assert_eq!(
        racing.rollbacks.load(Ordering::SeqCst),
        2,
        "the first delivery's two attempts"
    );

    assert!(
        !admin.retry("rt.Flaky:e-f9").await.unwrap(),
        "it failed again"
    );

    assert_eq!(
        racing.rollbacks.load(Ordering::SeqCst),
        4,
        "both attempts of the retry rolled back, the discard landed after the last one"
    );
    assert!(
        admin.list(None).await.unwrap().is_empty(),
        "the discarded failure stays discarded"
    );
}

#[tokio::test]
async fn retry_all_retries_every_failure_of_a_type() {
    require_docker!();
    let fx = Fx::start().await;
    for (order, id) in [("f3", "e-f3"), ("f4", "e-f4"), ("f5", "e-f5")] {
        fx.deliver(order, id).await;
    }
    // Another type's failure.
    let mut other: SagaFailureDocument =
        fx.failures.list(Some("rt.Flaky")).await.unwrap().remove(0);
    other.id = "rt.Other:e-x".into();
    other.saga_type = "rt.Other".into();
    fx.store
        .db
        .collection::<FailureDoc>("saga_failures")
        .insert_one(FailureDoc::from(&other))
        .await
        .unwrap();
    fx.broken.store(false, Ordering::SeqCst);

    assert_eq!(fx.failures.retry_all("rt.Flaky").await.unwrap(), 3);

    assert_eq!(fx.store.sagas("rt.Flaky").await.len(), 3);
    assert_eq!(
        fx.ids(None).await,
        ["rt.Other:e-x"],
        "only the other type's failure is left"
    );
}

#[tokio::test]
async fn discard_removes_it_without_applying() {
    require_docker!();
    let fx = Fx::start().await;
    fx.deliver("f6", "e-f6").await;
    fx.broken.store(false, Ordering::SeqCst);

    assert!(fx.failures.discard("rt.Flaky:e-f6").await.unwrap());

    assert!(fx.ids(None).await.is_empty());
    assert_eq!(fx.store.sagas("rt.Flaky").await.len(), 0);
    assert!(
        fx.store.marker("saga:rt.Flaky:e-f6").await,
        "the event stays marked: a redelivery does not apply it either"
    );
    assert!(!fx.failures.discard("rt.Flaky:e-f6").await.unwrap());
}

#[tokio::test]
async fn retry_of_an_unknown_id_is_false() {
    require_docker!();
    let fx = Fx::start().await;

    assert!(!fx.failures.retry("rt.Flaky:nope").await.unwrap());
    assert!(!fx.failures.retry("rt.Gone:e-1").await.unwrap());
    assert_eq!(fx.failures.retry_all("rt.Flaky").await.unwrap(), 0);
    assert_eq!(
        fx.failures.retry_all("rt.Gone").await.unwrap(),
        0,
        "nothing parked: nothing to refuse, as in Kotlin"
    );
    assert!(fx.failures.list(None).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_failure_of_a_saga_type_this_service_does_not_have_is_refused_and_left_as_it_was() {
    require_docker!();
    let fx = Fx::start().await;
    fx.deliver("f7", "e-f7").await;
    let mut gone = fx.failures.list(Some("rt.Flaky")).await.unwrap().remove(0);
    gone.id = "rt.Gone:e-f7".into();
    gone.saga_type = "rt.Gone".into();
    fx.store
        .db
        .collection::<FailureDoc>("saga_failures")
        .insert_one(FailureDoc::from(&gone))
        .await
        .unwrap();

    let refused = fx.failures.retry("rt.Gone:e-f7").await;
    let refused_all = fx.failures.retry_all("rt.Gone").await;

    for (refusal, text) in [
        (
            refused.map(|_| 0),
            "Saga type 'rt.Gone' does not run in this service: parked event rt.Gone:e-f7 is left as it was",
        ),
        (
            refused_all,
            "Saga type 'rt.Gone' does not run in this service: its parked events (1) are left as they were",
        ),
    ] {
        match refusal {
            Err(MessagingError::InvalidMessage(message)) => assert_eq!(message, text),
            Err(e) => panic!("refused as InvalidMessage, got {e}"),
            Ok(n) => panic!("a retry of a type this service does not run is refused, got {n}"),
        }
    }
    let kept = fx.failures.list(Some("rt.Gone")).await.unwrap().remove(0);
    assert_eq!(
        kept, gone,
        "the parked event keeps its error, attempts and lastFailedAt"
    );
}
