//! service-c's named event processor (P6a, ADR 0022) and the example-only admin edge around it.
//!
//! The `audit` processor is its own Kafka consumer, group `service-c.audit`, markers
//! `processor:audit:<eventId>`, its own dead letters. [`Audit::failing`] is example-only: while it
//! is on, every note whose text starts with `poison` fails (the others are handled), so the sandbox
//! can show a record parked as a dead letter, the later records of its key parked behind it while
//! other keys flow, and the key draining once it is off and the letters are retried. The HTTP side mirrors service-b's `AuditController` (same paths and JSON):
//!
//! - `POST /audit/notes` `{"key","text"}`: publish a [`ShoutAudit`] keyed by `key` (202).
//! - `POST /audit/failing?on=true|false`: the failure switch. `GET /audit/handled`: the notes the
//!   processor handled, in order (in memory, lost on restart).
//! - `GET /audit/dead-letters[?processor=&limit=]`, `GET /audit/dead-letters/count[?processor=]`,
//!   `POST /audit/dead-letters/{id}/retry`, `POST /audit/dead-letters/retry-all[?processor=audit]`,
//!   `DELETE /audit/dead-letters/{id}` (404 for an unknown letter): the library's `DeadLetters`.
//!
//! The sandbox is closed; a real service puts its own authorization in front of such endpoints.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use chrono::Utc;
use kraken_messaging::MessagingError;
use kraken_messaging::kafka::EventBus;
use kraken_messaging::processors::dead_letters::DeadLetterDocument;
use kraken_messaging::processors::{DEFAULT_LIST_LIMIT, DeadLetters, RetryResult};
use serde::{Deserialize, Serialize};

use crate::messages::ShoutAudit;

/// The processor's name: `builder.event_in::<ShoutAudit, _, _>(PROCESSOR, ...)`.
pub const PROCESSOR: &str = "audit";
/// While the switch is on, a note whose text starts with this fails.
const POISON: &str = "poison";

/// What the `audit` processor handled and whether it is failing on demand; cheap to clone.
#[derive(Clone, Default)]
pub struct Audit(Arc<Inner>);

#[derive(Default)]
struct Inner {
    failing: AtomicBool,
    handled: Mutex<Vec<ShoutAudit>>,
}

impl Audit {
    /// The processor's handler: records the note, or fails a poison note while
    /// [`set_failing`](Self::set_failing) is on.
    pub fn handle(&self, note: ShoutAudit) -> Result<(), MessagingError> {
        if self.0.failing.load(Ordering::Relaxed) && note.text.starts_with(POISON) {
            return Err(MessagingError::RetryableExternal(format!(
                "the audit processor is failing on demand (key={} text={})",
                note.key, note.text
            )));
        }
        tracing::info!("C: audited key={} text={}", note.key, note.text);
        self.0
            .handled
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(note);
        Ok(())
    }

    pub fn set_failing(&self, on: bool) {
        self.0.failing.store(on, Ordering::Relaxed);
    }

    /// The notes handled so far, in the order they were handled.
    pub fn handled(&self) -> Vec<ShoutAudit> {
        self.0
            .handled
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// One dead letter as the sandbox shows it, the JSON service-b's `LetterView` has.
#[derive(Serialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LetterView {
    pub id: String,
    pub processor: String,
    pub key: Option<String>,
    pub event_type: Option<String>,
    pub reason: &'static str,
    pub attempts: i32,
    pub error: String,
    pub blocked_by: Option<String>,
}

impl From<DeadLetterDocument> for LetterView {
    fn from(letter: DeadLetterDocument) -> LetterView {
        LetterView {
            id: letter.id,
            processor: letter.processor,
            key: letter.key,
            event_type: letter.event_type,
            reason: letter.reason.as_str(),
            attempts: letter.attempts,
            error: format!("{}: {}", letter.error.type_name, letter.error.message),
            blocked_by: letter.blocked_by,
        }
    }
}

/// What a retry did: the `result` (`Retried`, `Duplicate`, `Failed`, `Blocked`, `NotRetriable`,
/// `NotFound`) and its `detail` (the failure, the blocking letter).
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct RetryView {
    pub result: &'static str,
    pub detail: Option<String>,
}

impl From<RetryResult> for RetryView {
    fn from(result: RetryResult) -> RetryView {
        let (result, detail) = match result {
            RetryResult::Retried => ("Retried", None),
            RetryResult::Duplicate => ("Duplicate", None),
            RetryResult::Failed(message) => ("Failed", Some(message)),
            RetryResult::Blocked(head) => ("Blocked", Some(head)),
            RetryResult::NotRetriable => ("NotRetriable", None),
            RetryResult::NotFound => ("NotFound", None),
        };
        RetryView { result, detail }
    }
}

/// The value of `name` in a raw query string (`on=true&limit=5`), when it has one.
fn param<'a>(query: &'a Option<String>, name: &str) -> Option<&'a str> {
    query
        .as_deref()?
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
        .filter(|v| !v.is_empty())
}

#[derive(Clone)]
pub struct AuditHttp {
    pub audit: Audit,
    pub events: EventBus,
    pub dead_letters: DeadLetters,
}

pub fn router(http: AuditHttp) -> Router {
    Router::new()
        .route("/audit/notes", post(publish))
        .route("/audit/failing", post(failing))
        .route("/audit/handled", get(handled))
        .route("/audit/dead-letters", get(list))
        .route("/audit/dead-letters/count", get(count))
        .route("/audit/dead-letters/retry-all", post(retry_all))
        .route("/audit/dead-letters/{id}/retry", post(retry))
        .route("/audit/dead-letters/{id}", delete(discard))
        .with_state(http)
}

fn json<T: Serialize>(status: StatusCode, body: &T) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(body).unwrap_or_default(),
    )
        .into_response()
}

/// A processor this instance does not run (`InvalidMessage`) is the caller's mistake (400); any
/// other failure of the admin API, such as a mode without letters, is a conflict or a server error.
fn failure(e: MessagingError) -> Response {
    let status = match e {
        MessagingError::InvalidMessage(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, e.to_string()).into_response()
}

#[derive(Deserialize)]
struct AuditRequest {
    key: String,
    text: String,
}

async fn publish(State(http): State<AuditHttp>, body: Bytes) -> Response {
    let Ok(request) = serde_json::from_slice::<AuditRequest>(&body) else {
        return (
            StatusCode::BAD_REQUEST,
            "body must be {\"key\":...,\"text\":...}",
        )
            .into_response();
    };
    if request.key.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "key must not be blank").into_response();
    }
    let note = ShoutAudit {
        key: request.key,
        text: request.text,
        at: Utc::now(),
    };
    match http.events.publish(&note, Some(&note.key)).await {
        Ok(()) => json(StatusCode::ACCEPTED, &note),
        Err(e) => failure(e),
    }
}

async fn failing(State(http): State<AuditHttp>, RawQuery(query): RawQuery) -> Response {
    let Some(on) = param(&query, "on").and_then(|v| v.parse::<bool>().ok()) else {
        return (StatusCode::BAD_REQUEST, "on=true or on=false").into_response();
    };
    http.audit.set_failing(on);
    json(StatusCode::OK, &serde_json::json!({ "failing": on }))
}

async fn handled(State(http): State<AuditHttp>) -> Response {
    json(StatusCode::OK, &http.audit.handled())
}

async fn list(State(http): State<AuditHttp>, RawQuery(query): RawQuery) -> Response {
    let limit = param(&query, "limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_LIST_LIMIT);
    match http
        .dead_letters
        .list(param(&query, "processor"), limit)
        .await
    {
        Ok(letters) => json(
            StatusCode::OK,
            &letters
                .into_iter()
                .map(LetterView::from)
                .collect::<Vec<_>>(),
        ),
        Err(e) => failure(e),
    }
}

async fn count(State(http): State<AuditHttp>, RawQuery(query): RawQuery) -> Response {
    match http.dead_letters.count(param(&query, "processor")).await {
        Ok(count) => json(StatusCode::OK, &serde_json::json!({ "count": count })),
        Err(e) => failure(e),
    }
}

async fn retry(State(http): State<AuditHttp>, Path(id): Path<String>) -> Response {
    match http.dead_letters.retry(&id).await {
        Ok(result) => json(StatusCode::OK, &RetryView::from(result)),
        Err(e) => failure(e),
    }
}

async fn retry_all(State(http): State<AuditHttp>, RawQuery(query): RawQuery) -> Response {
    match http
        .dead_letters
        .retry_all(param(&query, "processor").unwrap_or(PROCESSOR))
        .await
    {
        Ok(r) => json(
            StatusCode::OK,
            &serde_json::json!({
                "retried": r.retried, "failed": r.failed, "blocked": r.blocked, "skipped": r.skipped
            }),
        ),
        Err(e) => failure(e),
    }
}

async fn discard(State(http): State<AuditHttp>, Path(id): Path<String>) -> Response {
    match http.dead_letters.discard(&id).await {
        Ok(true) => StatusCode::OK.into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, format!("no dead letter {id}")).into_response(),
        Err(e) => failure(e),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use kraken_messaging::MessageType;
    use kraken_messaging::MessagingError;
    use kraken_messaging::processors::RetryResult;
    use kraken_messaging::processors::dead_letters::DeadLetterDocument;
    use serde_json::json;

    use super::{Audit, LetterView, RetryView, param};
    use crate::messages::ShoutAudit;

    fn note(key: &str, text: &str) -> ShoutAudit {
        ShoutAudit {
            key: key.into(),
            text: text.into(),
            at: Utc.timestamp_opt(0, 0).unwrap(),
        }
    }

    #[test]
    fn the_note_has_its_own_wire_name_and_fields() {
        assert_eq!(ShoutAudit::TYPE, "examples.ShoutAudit");
        let value = serde_json::to_value(note("k1", "hello")).unwrap();
        assert_eq!(value["key"], "k1");
        assert_eq!(value["text"], "hello");
        assert!(value["at"].is_string());
    }

    #[test]
    fn notes_are_recorded_in_the_order_they_were_handled() {
        let audit = Audit::default();
        audit.handle(note("k1", "first")).unwrap();
        audit.handle(note("k2", "other")).unwrap();
        audit.handle(note("k1", "second")).unwrap();
        let texts: Vec<_> = audit.handled().into_iter().map(|n| n.text).collect();
        assert_eq!(texts, ["first", "other", "second"]);
    }

    #[test]
    fn while_failing_a_poison_note_fails_and_is_not_recorded_then_it_works_again() {
        let audit = Audit::default();
        audit.set_failing(true);

        let error = audit.handle(note("k1", "poison-1")).unwrap_err();
        assert!(matches!(error, MessagingError::RetryableExternal(_)));
        assert_eq!(
            error.to_string(),
            "the audit processor is failing on demand (key=k1 text=poison-1)"
        );
        assert!(audit.handled().is_empty());

        audit.set_failing(false);
        audit.handle(note("k1", "poison-1")).unwrap();
        assert_eq!(audit.handled().len(), 1);
    }

    #[test]
    fn while_failing_the_other_notes_are_handled_whatever_their_key() {
        let audit = Audit::default();
        audit.set_failing(true);
        audit.handle(note("k2", "fine")).unwrap();
        audit.handle(note("k1", "not poisoned")).unwrap();
        assert_eq!(audit.handled().len(), 2);
    }

    #[test]
    fn a_clone_shares_the_switch_and_the_notes() {
        let audit = Audit::default();
        let handler = audit.clone();
        audit.set_failing(true);
        assert!(handler.handle(note("k", "poison")).is_err());
        audit.set_failing(false);
        handler.handle(note("k", "poison")).unwrap();
        assert_eq!(audit.handled().len(), 1);
    }

    #[test]
    fn a_retry_is_shown_by_result_name_with_the_failure_or_the_blocking_letter_as_detail() {
        let view = |r: RetryResult| serde_json::to_value(RetryView::from(r)).unwrap();
        assert_eq!(
            view(RetryResult::Retried),
            json!({"result": "Retried", "detail": null})
        );
        assert_eq!(
            view(RetryResult::Failed("boom".into())),
            json!({"result": "Failed", "detail": "boom"})
        );
        assert_eq!(
            view(RetryResult::Blocked("a".into())),
            json!({"result": "Blocked", "detail": "a"})
        );
        assert_eq!(view(RetryResult::NotFound)["result"], "NotFound");
        assert_eq!(view(RetryResult::Duplicate)["result"], "Duplicate");
        assert_eq!(view(RetryResult::NotRetriable)["result"], "NotRetriable");
    }

    #[test]
    fn a_letter_is_shown_the_way_service_b_shows_it() {
        let letter: DeadLetterDocument = serde_json::from_value(json!({
            "_id": "audit:kraken.events:0:8", "processor": "audit", "topic": "kraken.events",
            "partition": 0, "offset": 8, "key": "k1", "eventType": "examples.ShoutAudit",
            "reason": "SEQUENCED", "error": {"type": "Sequenced", "message": "an earlier letter"},
            "blockedBy": "audit:kraken.events:0:7", "attempts": 0,
            "parkedAt": {"$date": "2026-11-04T00:00:00Z"},
            "lastAttemptAt": {"$date": "2026-11-04T00:00:00Z"},
            "expireAt": {"$date": "2026-12-04T00:00:00Z"},
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(LetterView::from(letter)).unwrap(),
            json!({
                "id": "audit:kraken.events:0:8", "processor": "audit", "key": "k1",
                "eventType": "examples.ShoutAudit", "reason": "SEQUENCED", "attempts": 0,
                "error": "Sequenced: an earlier letter", "blockedBy": "audit:kraken.events:0:7",
            })
        );
    }

    #[test]
    fn a_query_parameter_is_read_by_name() {
        let query = Some("processor=audit&limit=5&on=".to_string());
        assert_eq!(param(&query, "processor"), Some("audit"));
        assert_eq!(param(&query, "limit"), Some("5"));
        assert_eq!(param(&query, "on"), None, "an empty value is no value");
        assert_eq!(param(&query, "other"), None);
        assert_eq!(param(&None, "processor"), None);
    }
}
