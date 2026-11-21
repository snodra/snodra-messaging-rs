//! The webhook ingress and the signature verifier: the Rust
//! form of the Kotlin `WebhookIngressTest`. The ingress runs on the in-memory message store;
//! the verifier needs nothing.
#![cfg(feature = "testing")]

mod support;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use snodra_messaging::events::LocalEventDispatcher;
use snodra_messaging::external::webhook::{
    SECRET_HEADER, SIGNATURE_HEADER, constant_time_eq, hmac_hex, verify, verify_with,
};
use snodra_messaging::external::{ExternalEventIngress, IngressResult};
use snodra_messaging::hub::HandlerRegistry;
use snodra_messaging::saga::{NoopSagaMetrics, SagaDefinition, SagaRetry, SagaRuntime, SagaType};
use snodra_messaging::{Event, MessageContext, MessagingError, RetryPolicy, message_type};
use support::saga::{Store, state, t0};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Hook {
    job_id: String,
    status: String,
}
message_type!(Hook, "wh.Hook");
impl Event for Hook {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Unwanted {}
message_type!(Unwanted, "wh.Unwanted");
impl Event for Unwanted {}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Job {
    job_id: Option<String>,
    hooks: u32,
    tenant: Option<String>,
}

fn job_saga() -> SagaType {
    SagaDefinition::<Job>::new("wh.Job")
        .start_on::<Hook>(
            "jobId",
            |e| Some(e.job_id.clone()),
            |s, e, ctx| {
                s.job_id = Some(e.job_id.clone());
                s.hooks += 1;
                s.tenant = ctx.message().headers.get("x-tenant").cloned();
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
    failures_left: AtomicUsize,
}

struct Wh {
    store: Store,
    handlers: Arc<Handlers>,
    ingress: ExternalEventIngress,
}

impl Wh {
    async fn start() -> Wh {
        let store = Store::start().await;
        let handlers = Arc::new(Handlers::default());
        let mut registry = HandlerRegistry::new();
        let h = Arc::clone(&handlers);
        registry.event::<Hook, _, _>(move |e, ctx| {
            let h = Arc::clone(&h);
            async move {
                let left = h.failures_left.load(Ordering::SeqCst);
                if left > 0 {
                    h.failures_left.store(left - 1, Ordering::SeqCst);
                    return Err(MessagingError::Storage("handler failed on purpose".into()));
                }
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
            Some(store.message_store()),
            Arc::new(registry),
            runtime,
        ));
        let ingress =
            ExternalEventIngress::new(dispatcher, Some(store.message_store()), store.clock.clock());
        Wh {
            store,
            handlers,
            ingress,
        }
    }

    async fn hooks(&self, job_id: &str) -> Option<u64> {
        self.store
            .sagas("wh.Job")
            .await
            .iter()
            .find(|s| state(s)["jobId"] == job_id)
            .map(|s| state(s)["hooks"].as_u64().unwrap())
    }

    async fn accept(&self, source: &str, key: &str, job: &str, status: &str) -> IngressResult {
        self.ingress
            .accept(
                source,
                key,
                &Hook {
                    job_id: job.into(),
                    status: status.into(),
                },
                &HashMap::new(),
            )
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn an_accepted_webhook_event_reaches_a_saga() {
    let wh = Wh::start().await;
    let headers = HashMap::from([
        ("x-tenant".to_string(), "acme".to_string()),
        ("correlationId".to_string(), "corr-acme".to_string()),
    ]);

    let result = wh
        .ingress
        .accept(
            "printer",
            "evt-1",
            &Hook {
                job_id: "j1".into(),
                status: "done".into(),
            },
            &headers,
        )
        .await
        .unwrap();

    // Synchronous: the saga and the ordinary handler have run by the time accept returned.
    assert_eq!(result, IngressResult::Accepted);
    assert_eq!(wh.hooks("j1").await, Some(1));
    assert_eq!(*wh.handlers.seen.lock().unwrap(), ["j1"]);
    assert!(
        wh.store.marker("processor:default:printer:evt-1").await,
        "the dedupe id is <source>:<dedupeKey>"
    );
    assert!(wh.store.marker("saga:wh.Job:printer:evt-1").await);
    let marker = wh
        .store
        .contents()
        .processed
        .into_iter()
        .find(|m| m.id == "ingress:printer:evt-1")
        .expect("the ingress marker");
    assert_eq!(marker.type_name, "wh.Hook");
    assert_eq!(
        marker.processed_at_ms,
        snodra_messaging::time::epoch_ms(t0()),
        "the marker's time is the clock's"
    );
    // The handlers see the dedupe id, the source, the type and the given headers.
    let ctx = wh.handlers.contexts.lock().unwrap().remove(0);
    assert_eq!(ctx.message_id, "printer:evt-1");
    assert_eq!(ctx.correlation_id, "corr-acme");
    assert_eq!(ctx.headers["id"], "printer:evt-1");
    assert_eq!(ctx.headers["type"], "wh.Hook");
    assert_eq!(ctx.headers["source"], "printer");
    let saga = wh.store.sagas("wh.Job").await.remove(0);
    assert_eq!(state(&saga)["tenant"], "acme", "a saga reads the headers");
}

#[tokio::test]
async fn the_same_dedupe_key_twice_is_duplicate_and_applied_once() {
    let wh = Wh::start().await;

    assert_eq!(
        wh.accept("printer", "evt-2", "j2", "done").await,
        IngressResult::Accepted
    );
    assert_eq!(
        wh.accept("printer", "evt-2", "j2", "done").await,
        IngressResult::Duplicate
    );
    assert_eq!(
        wh.accept("printer", "evt-3", "j2", "again").await,
        IngressResult::Accepted
    );
    // The same key of another source is another event.
    assert_eq!(
        wh.accept("carrier", "evt-2", "j2", "other").await,
        IngressResult::Accepted
    );

    assert_eq!(wh.hooks("j2").await, Some(3));
    assert_eq!(*wh.handlers.seen.lock().unwrap(), ["j2", "j2", "j2"]);
}

#[tokio::test]
async fn a_failing_handler_fails_the_call_and_the_retry_of_the_same_key_is_accepted() {
    let wh = Wh::start().await;
    wh.handlers.failures_left.store(1, Ordering::SeqCst);

    let failed = wh
        .ingress
        .accept(
            "printer",
            "evt-4",
            &Hook {
                job_id: "j4".into(),
                status: "done".into(),
            },
            &HashMap::new(),
        )
        .await;
    assert!(failed.is_err());
    assert_eq!(
        wh.hooks("j4").await,
        Some(1),
        "stage 1 failed but the saga stage ran"
    );
    assert!(!wh.store.marker("ingress:printer:evt-4").await);

    assert_eq!(
        wh.accept("printer", "evt-4", "j4", "done").await,
        IngressResult::Accepted,
        "a failed call is not a duplicate"
    );
    assert_eq!(
        wh.hooks("j4").await,
        Some(1),
        "the retry did not apply it to the saga a second time"
    );
    assert_eq!(*wh.handlers.seen.lock().unwrap(), ["j4"]);
}

#[tokio::test]
async fn an_event_nobody_handles_is_accepted_without_a_marker_and_blank_ids_are_refused() {
    let wh = Wh::start().await;

    let result = wh
        .ingress
        .accept("printer", "evt-5", &Unwanted {}, &HashMap::new())
        .await
        .unwrap();

    assert_eq!(result, IngressResult::Accepted);
    assert_eq!(
        wh.store.contents().processed.len(),
        0,
        "no marker: the partner must not retry what the service never uses, and nothing is remembered"
    );
    for (source, key) in [("", "k"), ("printer", " ")] {
        let e = wh
            .ingress
            .accept(source, key, &Unwanted {}, &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(e, MessagingError::InvalidMessage(_)), "{e:?}");
    }
}

// ---- the verifier ------------------------------------------------------------------------------

const BODY: &[u8] = br#"{"jobId":"j1"}"#;

fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn the_hmac_is_rfc_4231_hmac_sha256() {
    // RFC 4231 test cases 1 and 2, and case 6 (a key longer than the block).
    assert_eq!(
        hmac_hex(&[0x0b; 20], b"Hi There"),
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
    assert_eq!(
        hmac_hex(b"Jefe", b"what do ya want for nothing?"),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
    assert_eq!(
        hmac_hex(
            &[0xaa; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First"
        ),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
}

#[test]
fn a_valid_hmac_verifies_with_the_current_and_with_the_previous_secret() {
    let current = headers(&[(
        SIGNATURE_HEADER,
        &format!("sha256={}", hmac_hex(b"new-secret", BODY)),
    )]);
    let previous = headers(&[(
        SIGNATURE_HEADER,
        &format!("sha256={}", hmac_hex(b"old-secret", BODY)),
    )]);

    assert!(verify("new-secret", Some("old-secret"), &current, BODY));
    assert!(verify("new-secret", Some("old-secret"), &previous, BODY));
    assert!(
        !verify("new-secret", None, &previous, BODY),
        "the previous secret counts only when configured"
    );
    // Header names are case-insensitive, the prefix and the hex case too, and the shared-secret
    // header works for both secrets; a bare hex signature is accepted.
    let upper = format!("SHA256={}", hmac_hex(b"new-secret", BODY).to_uppercase());
    assert!(verify(
        "new-secret",
        None,
        &headers(&[("x-snodra-signature", &upper)]),
        BODY
    ));
    assert!(verify(
        "new-secret",
        None,
        &headers(&[("X-Snodra-Signature", &hmac_hex(b"new-secret", BODY))]),
        BODY
    ));
    assert!(verify(
        "new-secret",
        Some("old-secret"),
        &headers(&[("x-snodra-webhook-secret", "new-secret")]),
        BODY
    ));
    assert!(verify(
        "new-secret",
        Some("old-secret"),
        &headers(&[("X-SNODRA-WEBHOOK-SECRET", "old-secret")]),
        BODY
    ));
    // Any header shape that iterates as name/value pairs: a slice of tuples here.
    assert!(verify(
        "new-secret",
        None,
        [(SECRET_HEADER, "new-secret")],
        BODY
    ));
}

#[test]
fn a_wrong_or_missing_signature_does_not_verify() {
    let sig = |secret: &[u8], body: &[u8]| format!("sha256={}", hmac_hex(secret, body));
    assert!(!verify("s", None, &headers(&[]), BODY), "no header");
    assert!(
        !verify(
            "s",
            None,
            &headers(&[(SIGNATURE_HEADER, &sig(b"other", BODY))]),
            BODY
        ),
        "wrong secret"
    );
    assert!(
        !verify(
            "s",
            None,
            &headers(&[(SIGNATURE_HEADER, &sig(b"s", b"tampered"))]),
            BODY
        ),
        "another body"
    );
    assert!(
        !verify(
            "s",
            None,
            &headers(&[(SIGNATURE_HEADER, "sha256=zz")]),
            BODY
        ),
        "not hex"
    );
    assert!(
        !verify("s", None, &headers(&[(SIGNATURE_HEADER, "")]), BODY),
        "empty signature"
    );
    assert!(
        !verify("s", None, &headers(&[(SECRET_HEADER, "nope")]), BODY),
        "wrong shared secret"
    );
    assert!(
        !verify("s", None, &headers(&[(SECRET_HEADER, "")]), BODY),
        "empty shared secret"
    );
    // A service with no secret configured accepts nothing, not even an empty header.
    assert!(!verify("", None, &headers(&[(SECRET_HEADER, "")]), BODY));
    assert!(
        !verify(
            "",
            Some(""),
            &headers(&[(
                SIGNATURE_HEADER,
                "sha256=648662a43b6085062599639fbf589c39bacfd8195b933196b10bd04138275d2a"
            )]),
            BODY
        ),
        "the HMAC under an empty key"
    );
}

#[test]
fn the_comparison_is_constant_time() {
    // By code path, not by timing: every comparison of a secret or a signature goes through the
    // injected byte comparison (constant_time_eq by default).
    let compared: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());
    let recording = |a: &[u8], b: &[u8]| {
        compared.lock().unwrap().push((
            String::from_utf8_lossy(a).to_string(),
            String::from_utf8_lossy(b).to_string(),
        ));
        constant_time_eq(a, b)
    };

    assert!(verify_with(
        &recording,
        "s3cret",
        Some("old"),
        &headers(&[(SECRET_HEADER, "s3cret")]),
        BODY
    ));
    assert_eq!(
        std::mem::take(&mut *compared.lock().unwrap()),
        [("s3cret".to_string(), "s3cret".to_string())]
    );

    let good = hmac_hex(b"old", BODY);
    assert!(verify_with(
        &recording,
        "s3cret",
        Some("old"),
        &headers(&[(SIGNATURE_HEADER, &format!("sha256={good}"))]),
        BODY
    ));
    assert_eq!(
        std::mem::take(&mut *compared.lock().unwrap()),
        [
            (hmac_hex(b"s3cret", BODY), good.clone()),
            (good.clone(), good)
        ],
        "the current secret's signature is compared first, then the previous one's"
    );

    assert!(!verify_with(
        &recording,
        "s3cret",
        None,
        &headers(&[(SIGNATURE_HEADER, "sha256=ab")]),
        BODY
    ));
    assert_eq!(
        compared.lock().unwrap().len(),
        1,
        "a short signature is still compared, not rejected by length first"
    );

    assert!(constant_time_eq(b"abc", b"abc"));
    assert!(!constant_time_eq(b"abc", b"abd"));
    assert!(!constant_time_eq(b"abc", b"abcd"));
    assert!(!constant_time_eq(b"", b"a"));
    assert!(constant_time_eq(b"", b""));
}
