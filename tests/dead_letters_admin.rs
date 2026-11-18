//! The dead-letter admin API (ADR 0022): list, get, count, retry (each result), retry_all and
//! discard against a real MongoDB replica set, with letters inserted as the consumer parks them.
//! A retry runs the processor's stage 1 in the marker transaction, the letter's deletion included.

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use mongodb::bson::{Document, doc};
use mongodb::{Client, Database};
use snodra_messaging::config::DeadLetterMode;
use snodra_messaging::events::{LocalEventDispatcher, NoSagas, SagaStage};
use snodra_messaging::hub::HandlerRegistry;
use snodra_messaging::processors::dead_letters::{DeadLetterRecord, park};
use snodra_messaging::processors::{DeadLetters, RetryAllResult, RetryResult};
use snodra_messaging::store::{DeadLetter, DeadLetterError, DeadLetterReason, MessageStore};
use snodra_messaging::{Event, MessageContext, MessagingError, message_type};
use snodra_messaging_mongo::MongoTxExt;

const DEAD_LETTERS_COLLECTION: &str = "dead_letters";

fn storage(e: mongodb::error::Error) -> MessagingError {
    MessagingError::Storage(e.to_string())
}
use serde::{Deserialize, Serialize};

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Serialize, Deserialize, Clone)]
struct Placed {
    order: String,
}
message_type!(Placed, "t.Placed");
impl Event for Placed {}

/// Handled by a stage-1 handler that records the order (in its transaction, as an `effects`
/// document) and fails for the orders in `failing`.
struct Fixture {
    db: Database,
    store: Arc<dyn MessageStore>,
    failing: Arc<Mutex<Vec<String>>>,
    seen: Arc<Mutex<Vec<String>>>,
    /// Deleted by the handler outside its transaction (another instance's discard).
    vanish: Arc<Mutex<Option<String>>>,
    /// A letter whose payload the handler rewrites outside its transaction (`_id`, new order):
    /// an operator fixing it while a `retry_all` runs.
    rewrite: Arc<Mutex<Option<(String, String)>>>,
    _mongo: support::containers::MongoDb,
}

impl Fixture {
    async fn start() -> Fixture {
        let mongo = support::containers::mongo().await;
        let client = Client::with_uri_str(&mongo.uri).await.unwrap();
        let db = client.default_database().unwrap();
        // The handler's own writes join the store's sessions: one client for both.
        let store = snodra_messaging_mongo::MongoStore::new(client.clone(), db.clone());
        store
            .ensure_indexes(&snodra_messaging_mongo::MongoIndexSettings::default())
            .await
            .unwrap();
        let store: Arc<dyn MessageStore> = Arc::new(store);
        for name in ["processed_events", "effects", DEAD_LETTERS_COLLECTION] {
            let _ = db.create_collection(name).await;
        }
        Fixture {
            db,
            store,
            failing: Arc::default(),
            seen: Arc::default(),
            vanish: Arc::default(),
            rewrite: Arc::default(),
            _mongo: mongo,
        }
    }

    fn registry(&self) -> Arc<HandlerRegistry> {
        let mut registry = HandlerRegistry::new();
        let (failing, seen, vanish, rewrite, db) = (
            Arc::clone(&self.failing),
            Arc::clone(&self.seen),
            Arc::clone(&self.vanish),
            Arc::clone(&self.rewrite),
            self.db.clone(),
        );
        registry.event::<Placed, _, _>(move |e, ctx| {
            let (failing, seen, vanish, rewrite, db) = (
                Arc::clone(&failing),
                Arc::clone(&seen),
                Arc::clone(&vanish),
                Arc::clone(&rewrite),
                db.clone(),
            );
            async move {
                if failing.lock().unwrap().contains(&e.order) {
                    return Err(MessagingError::Storage(format!("cannot place {}", e.order)));
                }
                {
                    let mut tx = ctx.tx.lock().await;
                    db.collection::<Document>("effects")
                        .insert_one(doc! { "order": &e.order })
                        .session(tx.session())
                        .await
                        .map_err(storage)?;
                }
                let gone = vanish.lock().unwrap().take();
                if let Some(id) = gone {
                    db.collection::<Document>(DEAD_LETTERS_COLLECTION)
                        .delete_one(doc! { "_id": id })
                        .await
                        .map_err(storage)?;
                }
                let fixed = rewrite.lock().unwrap().take();
                if let Some((id, order)) = fixed {
                    let payload = serde_json::to_string(&Placed { order }).unwrap();
                    db.collection::<Document>(DEAD_LETTERS_COLLECTION)
                        .update_one(doc! { "_id": id }, doc! { "$set": { "payload": payload } })
                        .await
                        .map_err(storage)?;
                }
                seen.lock().unwrap().push(e.order);
                Ok(())
            }
        });
        Arc::new(registry)
    }

    fn dispatcher(&self, name: &str, stage: Arc<dyn SagaStage>) -> Arc<LocalEventDispatcher> {
        Arc::new(
            LocalEventDispatcher::new(Some(Arc::clone(&self.store)), self.registry(), stage)
                .with_marker_prefix(snodra_messaging::processors::marker_prefix(name)),
        )
    }

    fn admin(&self, mode: DeadLetterMode) -> DeadLetters {
        DeadLetters::new(
            Some(self.store.dead_letters()),
            mode,
            vec![
                (
                    "orders".to_string(),
                    self.dispatcher("orders", Arc::new(NoSagas)),
                ),
                (
                    "default".to_string(),
                    self.dispatcher("default", Arc::new(NoSagas)),
                ),
            ],
        )
    }

    async fn park(&self, letter: &Letter<'_>) -> String {
        let letter = letter.document();
        self.store.dead_letters().park(&letter).await.unwrap();
        letter.id
    }

    async fn letter(&self, id: &str) -> Option<DeadLetter> {
        self.store.dead_letters().get(id).await.unwrap()
    }

    async fn markers(&self) -> Vec<String> {
        let mut all: Vec<String> = self
            .db
            .collection::<Document>("processed_events")
            .distinct("_id", doc! {})
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.as_str().unwrap().to_string())
            .collect();
        all.sort();
        all
    }

    async fn effects(&self) -> u64 {
        self.db
            .collection::<Document>("effects")
            .count_documents(doc! {})
            .await
            .unwrap()
    }
}

struct Letter<'a> {
    processor: &'a str,
    offset: i64,
    key: Option<&'a str>,
    order: &'a str,
    event_id: Option<&'a str>,
    type_name: &'a str,
    reason: DeadLetterReason,
    /// Milliseconds after a fixed base, for `parkedAt`.
    parked: i64,
}

impl<'a> Letter<'a> {
    fn new(processor: &'a str, offset: i64, key: Option<&'a str>, order: &'a str) -> Letter<'a> {
        Letter {
            processor,
            offset,
            key,
            order,
            event_id: Some(order),
            type_name: "t.Placed",
            reason: DeadLetterReason::HandlerFailed,
            parked: offset,
        }
    }

    fn reason(mut self, reason: DeadLetterReason) -> Self {
        self.reason = reason;
        self
    }

    fn parked(mut self, ms: i64) -> Self {
        self.parked = ms;
        self
    }

    fn typed(mut self, type_name: &'a str) -> Self {
        self.type_name = type_name;
        self
    }

    fn without_id(mut self) -> Self {
        self.event_id = None;
        self
    }

    fn document(&self) -> DeadLetter {
        let mut headers = HashMap::from([("type".to_string(), self.type_name.to_string())]);
        if let Some(id) = self.event_id {
            headers.insert("id".to_string(), id.to_string());
        }
        headers.insert("correlationId".to_string(), "corr-1".to_string());
        let error = match self.reason {
            DeadLetterReason::HandlerFailed => DeadLetterError::new("Storage", "boom"),
            DeadLetterReason::Sequenced => DeadLetterError::sequenced("head"),
            DeadLetterReason::Invalid => DeadLetterError::invalid("payload is not a JSON object"),
        };
        park(
            DeadLetterRecord {
                processor: self.processor.to_string(),
                source: "snodra.events".to_string(),
                segment: "1".to_string(),
                position: self.offset.to_string(),
                key: self.key.map(str::to_string),
                headers,
                payload: serde_json::to_vec(&Placed {
                    order: self.order.to_string(),
                })
                .unwrap(),
            },
            self.reason,
            error,
            (self.reason == DeadLetterReason::Sequenced).then(|| "head".to_string()),
            if self.reason == DeadLetterReason::HandlerFailed {
                5
            } else {
                0
            },
            1_762_250_000_000 + self.parked,
            Duration::from_secs(86_400),
        )
    }
}

fn names(letters: &[DeadLetter]) -> Vec<&str> {
    letters.iter().map(|l| l.id.as_str()).collect()
}

#[tokio::test]
async fn outside_mode_collection_every_call_is_an_error_naming_the_mode() {
    // No store is reached.
    let store = support::mongo_store_unreached().await;
    for mode in [DeadLetterMode::EventStore, DeadLetterMode::Log] {
        let admin = DeadLetters::new(Some(store.dead_letters()), mode, vec![]);
        let errors = [
            admin.list(None, 100).await.unwrap_err(),
            admin.get("x").await.unwrap_err(),
            admin.count(None).await.unwrap_err(),
            admin.retry("x").await.unwrap_err(),
            admin.retry_all("orders").await.unwrap_err(),
            admin.discard("x").await.unwrap_err(),
        ];
        for e in errors {
            assert!(e.to_string().contains(mode.as_str()), "{mode:?}: {e}");
        }
    }
}

/// Kotlin's `require(limit >= 1)`: a limit of 0 is an error, never "no limit".
#[tokio::test]
async fn a_list_limit_below_one_is_an_error() {
    // No store is reached.
    let admin = DeadLetters::new(
        Some(support::mongo_store_unreached().await.dead_letters()),
        DeadLetterMode::Collection,
        vec![],
    );
    for processor in [None, Some("orders")] {
        match admin.list(processor, 0).await {
            Err(MessagingError::InvalidMessage(m)) => assert!(m.contains("limit"), "{m}"),
            other => panic!("{processor:?}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn list_get_count_and_the_processor_check() {
    require_docker!();
    let f = Fixture::start().await;
    let admin = f.admin(DeadLetterMode::Collection);
    // Parked in another order than their offsets: list sorts by parkedAt, then _id.
    f.park(&Letter::new("orders", 3, Some("a"), "o-3").parked(10))
        .await;
    f.park(&Letter::new("orders", 1, Some("a"), "o-1").parked(20))
        .await;
    f.park(&Letter::new("orders", 2, None, "o-2").parked(20))
        .await;
    f.park(&Letter::new("default", 4, None, "o-4").parked(5))
        .await;
    f.park(&Letter::new("billing", 5, None, "o-5").parked(1))
        .await;

    let orders = admin.list(Some("orders"), 100).await.unwrap();
    assert_eq!(
        names(&orders),
        [
            "orders:snodra.events:1:3",
            "orders:snodra.events:1:1",
            "orders:snodra.events:1:2"
        ]
    );
    assert_eq!(
        names(&admin.list(Some("orders"), 2).await.unwrap()).len(),
        2
    );
    assert_eq!(
        admin.list(None, 100).await.unwrap().len(),
        5,
        "every processor"
    );
    assert_eq!(admin.count(Some("orders")).await.unwrap(), 3);
    assert_eq!(admin.count(Some("default")).await.unwrap(), 1);
    assert_eq!(admin.count(None).await.unwrap(), 5);
    let got = admin
        .get("orders:snodra.events:1:1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.event_id.as_deref(), Some("o-1"));
    assert!(
        admin
            .get("orders:snodra.events:1:99")
            .await
            .unwrap()
            .is_none()
    );

    // A processor this instance does not run: an error naming it.
    for e in [
        admin.list(Some("billing"), 100).await.unwrap_err(),
        admin.count(Some("billing")).await.unwrap_err(),
        admin.get("billing:snodra.events:1:5").await.unwrap_err(),
        admin.retry("billing:snodra.events:1:5").await.unwrap_err(),
        admin.retry_all("billing").await.unwrap_err(),
        admin
            .discard("billing:snodra.events:1:5")
            .await
            .unwrap_err(),
    ] {
        assert!(e.to_string().contains("'billing'"), "{e}");
    }
    assert!(f.letter("billing:snodra.events:1:5").await.is_some());
}

#[tokio::test]
async fn every_retry_result() {
    require_docker!();
    let f = Fixture::start().await;
    let admin = f.admin(DeadLetterMode::Collection);

    // Retried: the processor's marker family, the effect and the deletion, together.
    let ok = f.park(&Letter::new("orders", 10, Some("k1"), "o-10")).await;
    assert_eq!(admin.retry(&ok).await.unwrap(), RetryResult::Retried);
    assert!(f.letter(&ok).await.is_none());
    assert_eq!(f.markers().await, ["processor:orders:o-10"]);
    assert_eq!(f.effects().await, 1);

    // Duplicate: the marker exists; the letter is deleted, the handler does not run.
    let dup = f.park(&Letter::new("orders", 11, Some("k2"), "o-10")).await;
    assert_eq!(admin.retry(&dup).await.unwrap(), RetryResult::Duplicate);
    assert!(f.letter(&dup).await.is_none());
    assert_eq!(*f.seen.lock().unwrap(), ["o-10"]);

    // Failed: the letter is updated, nothing of the attempt stays.
    f.failing.lock().unwrap().push("o-12".into());
    let failing = f
        .park(&Letter::new("orders", 12, Some("k3"), "o-12").reason(DeadLetterReason::Sequenced))
        .await;
    let before = f.letter(&failing).await.unwrap();
    match admin.retry(&failing).await.unwrap() {
        RetryResult::Failed(message) => assert!(message.contains("cannot place o-12"), "{message}"),
        other => panic!("{other:?}"),
    }
    let after = f.letter(&failing).await.unwrap();
    assert_eq!(after.attempts, before.attempts + 1);
    assert_eq!(after.reason, DeadLetterReason::HandlerFailed);
    assert_eq!(after.error.type_name, "Storage");
    assert!(after.error.message.contains("cannot place o-12"));
    assert!(after.blocked_by.is_none(), "blockedBy is SEQUENCED only");
    assert!(after.last_attempt_at_ms > before.last_attempt_at_ms);
    assert_eq!(after.parked_at_ms, before.parked_at_ms);
    assert_eq!(after.expire_at_ms, before.expire_at_ms);
    assert_eq!(f.markers().await, ["processor:orders:o-10"]);
    assert_eq!(f.effects().await, 1);

    // Blocked: an earlier letter of the key is parked.
    let later = f.park(&Letter::new("orders", 13, Some("k3"), "o-13")).await;
    assert_eq!(
        admin.retry(&later).await.unwrap(),
        RetryResult::Blocked(failing.clone())
    );
    // An INVALID letter never blocks, and cannot be retried.
    let invalid = f
        .park(&Letter::new("orders", 9, Some("k4"), "o-9").reason(DeadLetterReason::Invalid))
        .await;
    let behind_invalid = f.park(&Letter::new("orders", 14, Some("k4"), "o-14")).await;
    assert_eq!(
        admin.retry(&invalid).await.unwrap(),
        RetryResult::NotRetriable
    );
    assert!(f.letter(&invalid).await.is_some());
    assert_eq!(
        admin.retry(&behind_invalid).await.unwrap(),
        RetryResult::Retried
    );

    assert_eq!(
        admin.retry("orders:snodra.events:1:999").await.unwrap(),
        RetryResult::NotFound
    );

    // A letter without an id header: the consumer's fallback id keys the marker.
    let no_id = f
        .park(&Letter::new("orders", 15, None, "o-15").without_id())
        .await;
    assert_eq!(admin.retry(&no_id).await.unwrap(), RetryResult::Retried);
    assert!(
        f.markers()
            .await
            .contains(&"processor:orders:snodra.events-1-15".to_string())
    );
}

/// The deletion is in the handlers' transaction: when it cannot happen (another instance removed
/// the letter while the handler ran) nothing of the attempt stays, so a letter is never both
/// handled and kept, nor handled twice.
#[tokio::test]
async fn a_retry_whose_letter_vanishes_meanwhile_commits_nothing() {
    require_docker!();
    let f = Fixture::start().await;
    let admin = f.admin(DeadLetterMode::Collection);
    let id = f.park(&Letter::new("orders", 20, Some("k"), "o-20")).await;
    *f.vanish.lock().unwrap() = Some(id.clone());
    assert_eq!(admin.retry(&id).await.unwrap(), RetryResult::NotFound);
    assert!(f.markers().await.is_empty(), "the marker rolled back");
    assert_eq!(f.effects().await, 0, "the handler's effect rolled back");
}

/// The unnamed processor's retry uses its `processor:default:<eventId>` marker and does not re-run
/// stage 2.
#[tokio::test]
async fn a_default_letter_retries_stage_one_only_with_its_marker() {
    require_docker!();
    let f = Fixture::start().await;
    let stage = Arc::new(CountingStage::default());
    let admin = DeadLetters::new(
        Some(f.store.dead_letters()),
        DeadLetterMode::Collection,
        vec![(
            "default".to_string(),
            f.dispatcher("default", stage.clone()),
        )],
    );
    let id = f.park(&Letter::new("default", 30, Some("k"), "o-30")).await;
    assert_eq!(admin.retry(&id).await.unwrap(), RetryResult::Retried);
    assert_eq!(f.markers().await, ["processor:default:o-30"]);
    assert_eq!(*stage.calls.lock().unwrap(), 0, "stage 2 is not re-run");
}

#[derive(Default)]
struct CountingStage {
    calls: Mutex<u32>,
}

impl SagaStage for CountingStage {
    fn interested(&self, _type_name: &str) -> bool {
        true
    }

    fn process<'a>(
        &'a self,
        _type_name: &'a str,
        _payload: &'a [u8],
        _ctx: &'a MessageContext,
    ) -> BoxFuture<'a, Result<bool, MessagingError>> {
        *self.calls.lock().unwrap() += 1;
        Box::pin(async { Ok(true) })
    }
}

#[tokio::test]
async fn a_letter_whose_type_has_no_handler_any_more_fails_its_retry() {
    require_docker!();
    let f = Fixture::start().await;
    let empty = Arc::new(LocalEventDispatcher::new(
        Some(Arc::clone(&f.store)),
        Arc::new(HandlerRegistry::new()),
        Arc::new(NoSagas),
    ));
    let admin = DeadLetters::new(
        Some(f.store.dead_letters()),
        DeadLetterMode::Collection,
        vec![("orders".to_string(), empty)],
    );
    let id = f.park(&Letter::new("orders", 40, None, "o-40")).await;
    match admin.retry(&id).await.unwrap() {
        RetryResult::Failed(m) => {
            assert_eq!(m, "no handler for t.Placed in processor orders")
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(f.letter(&id).await.unwrap().attempts, 6);
    assert!(f.markers().await.is_empty());
}

#[tokio::test]
async fn retry_all_goes_per_key_in_offset_order_and_a_key_stops_at_its_first_failure() {
    require_docker!();
    let f = Fixture::start().await;
    let admin = f.admin(DeadLetterMode::Collection);
    // Key a: its head fails, the two behind it are blocked.
    f.failing.lock().unwrap().push("a-1".into());
    f.park(&Letter::new("orders", 1, Some("a"), "a-1")).await;
    f.park(&Letter::new("orders", 4, Some("a"), "a-2").reason(DeadLetterReason::Sequenced))
        .await;
    f.park(&Letter::new("orders", 6, Some("a"), "a-3").reason(DeadLetterReason::Sequenced))
        .await;
    // Key b: parked out of offset order, retried in offset order.
    f.park(&Letter::new("orders", 5, Some("b"), "b-2").parked(1))
        .await;
    f.park(&Letter::new("orders", 2, Some("b"), "b-1").parked(2))
        .await;
    // Key z: its head was parked before key b's, so it goes first (keys in their heads' order).
    f.park(&Letter::new("orders", 11, Some("z"), "z-1").parked(0))
        .await;
    // Keyless: in parkedAt order, a failure stops nothing.
    f.failing.lock().unwrap().push("n-2".into());
    f.park(&Letter::new("orders", 9, None, "n-1").parked(3))
        .await;
    f.park(&Letter::new("orders", 7, None, "n-2").parked(4))
        .await;
    f.park(&Letter::new("orders", 8, None, "n-3").parked(5))
        .await;
    // INVALID: skipped; another processor's letters: untouched.
    f.park(&Letter::new("orders", 3, Some("b"), "x").reason(DeadLetterReason::Invalid))
        .await;
    f.park(&Letter::new("default", 10, Some("a"), "d-1")).await;

    let result = admin.retry_all("orders").await.unwrap();

    assert_eq!(
        result,
        RetryAllResult {
            retried: 5,
            failed: 2,
            blocked: 2,
            skipped: 1
        }
    );
    let seen = f.seen.lock().unwrap().clone();
    let b: Vec<&String> = seen.iter().filter(|s| s.starts_with("b-")).collect();
    assert_eq!(b, ["b-1", "b-2"]);
    let at = |o: &str| seen.iter().position(|s| s == o).unwrap();
    assert!(at("z-1") < at("b-1"), "{seen:?}");
    assert!(at("b-2") < at("n-1"), "keyless last: {seen:?}");
    let n: Vec<&String> = seen.iter().filter(|s| s.starts_with("n-")).collect();
    assert_eq!(n, ["n-1", "n-3"]);
    assert!(!seen.iter().any(|s| s.starts_with("a-") || s == "d-1"));
    assert_eq!(
        admin.count(Some("orders")).await.unwrap(),
        5,
        "a-1..3, n-2, x"
    );
    assert_eq!(admin.count(Some("default")).await.unwrap(), 1);
}

/// A letter removed by someone else before its turn (another instance retried or discarded it)
/// is `NotFound`, counted as skipped, never `Failed`: here on the "no handler" path, whose
/// failure update matched nothing.
#[tokio::test]
async fn a_letter_removed_before_its_turn_in_retry_all_is_skipped_not_failed() {
    require_docker!();
    let f = Fixture::start().await;
    let admin = f.admin(DeadLetterMode::Collection);
    f.park(&Letter::new("orders", 1, None, "o-1").parked(1))
        .await;
    let gone = f
        .park(
            &Letter::new("orders", 2, None, "o-2")
                .typed("t.Unknown")
                .parked(2),
        )
        .await;
    *f.vanish.lock().unwrap() = Some(gone.clone()); // removed while o-1 is retried
    assert_eq!(
        admin.retry_all("orders").await.unwrap(),
        RetryAllResult {
            retried: 1,
            failed: 0,
            blocked: 0,
            skipped: 1
        }
    );
    assert!(f.letter(&gone).await.is_none());
}

/// `retry_all` lists only what orders the run and reads each letter just before retrying it (as
/// Kotlin does): a letter changed meanwhile is retried as it is then, not as it was listed.
#[tokio::test]
async fn retry_all_reads_each_letter_just_before_retrying_it() {
    require_docker!();
    let f = Fixture::start().await;
    let admin = f.admin(DeadLetterMode::Collection);
    f.park(&Letter::new("orders", 1, Some("k"), "o-1")).await;
    let second = f.park(&Letter::new("orders", 2, Some("k"), "o-2")).await;
    *f.rewrite.lock().unwrap() = Some((second, "o-2-fixed".into()));
    assert_eq!(
        admin.retry_all("orders").await.unwrap(),
        RetryAllResult {
            retried: 2,
            ..RetryAllResult::default()
        }
    );
    assert_eq!(*f.seen.lock().unwrap(), ["o-1", "o-2-fixed"]);
}

#[tokio::test]
async fn discarding_a_head_unblocks_the_next_letter_of_its_key() {
    require_docker!();
    let f = Fixture::start().await;
    let admin = f.admin(DeadLetterMode::Collection);
    f.failing.lock().unwrap().push("h".into());
    let head = f.park(&Letter::new("orders", 1, Some("k"), "h")).await;
    let next = f
        .park(&Letter::new("orders", 2, Some("k"), "n").reason(DeadLetterReason::Sequenced))
        .await;
    assert_eq!(
        admin.retry(&next).await.unwrap(),
        RetryResult::Blocked(head.clone())
    );
    assert!(admin.discard(&head).await.unwrap());
    assert!(!admin.discard(&head).await.unwrap(), "already gone");
    assert!(!admin.discard("orders:snodra.events:1:77").await.unwrap());
    assert_eq!(admin.retry(&next).await.unwrap(), RetryResult::Retried);
    assert_eq!(*f.seen.lock().unwrap(), ["n"]);
    let _ = HashMap::<String, String>::new();
}
