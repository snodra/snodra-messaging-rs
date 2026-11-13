//! The storage contract of ADR 0021 for sagas (S2) in the MongoDB message store: the documents of
//! `sagas`, `saga_dispatch`, `saga_dispatch_leases`, `deadlines`, `saga_failures` and
//! `external_calls` (`kraken_messaging_mongo::documents`, read back into the core's records), and
//! their indexes.
//! The fixtures in `tests/fixtures/saga_contract` are written by hand from the ADR's table, in the
//! shape Spring Data writes them (`_class` included); Rust writes no `_class` and no nulls.

mod support;

use std::time::Duration;

use futures::TryStreamExt;
use kraken_messaging::saga::FailureReason;
use kraken_messaging::saga::store::{
    Association, DeadlineDocument, DeadlineStatus, DispatchKind, SagaDispatchDocument,
    SagaDocument, SagaFailureDocument,
};
use kraken_messaging::store::{ExternalCall, ExternalCallStatus};
use kraken_messaging_mongo::documents::{
    DeadlineDoc, DispatchDoc, ExternalCallDocument, FailureDoc, LeaseDoc, SagaDoc,
};
use kraken_messaging_mongo::{MongoIndexSettings, MongoStore};
use mongodb::bson::{Bson, DateTime, Document, doc};
use mongodb::{Database, IndexModel};
use serde::Serialize;
use serde::de::DeserializeOwned;

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

/// A fixture as the BSON document Spring Data would have stored (extended JSON for dates and longs).
fn fixture(name: &str) -> Document {
    let path = format!(
        "{}/tests/fixtures/saga_contract/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    match Bson::try_from(json).unwrap() {
        Bson::Document(d) => d,
        other => panic!("{name} is not a document: {other:?}"),
    }
}

fn read<T: DeserializeOwned>(name: &str) -> T {
    mongodb::bson::from_document(fixture(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
}

/// Epoch milliseconds.
fn at(rfc3339: &str) -> i64 {
    DateTime::parse_rfc3339_str(rfc3339)
        .unwrap()
        .timestamp_millis()
}

/// A fixture read as the store reads it: its document, then the core's record.
fn record<D: DeserializeOwned, T: From<D>>(name: &str) -> T {
    T::from(read::<D>(name))
}

#[test]
fn a_kotlin_shaped_document_of_each_collection_deserialises() {
    let saga: SagaDocument = record::<SagaDoc, _>("sagas");
    assert_eq!(saga.id, "7f6c1f0e-5a53-4f3e-9a0a-1d2f6f0c9b11");
    assert_eq!(saga.type_name, "PaymentSaga");
    // The state is JSON text, kept exactly (a decimal's scale, a key with a dot).
    assert_eq!(
        saga.state,
        r#"{"orderId":"o-1","amount":12.30,"log":{"a.b":"x"}}"#
    );
    assert_eq!(
        saga.associations,
        vec![
            Association::new("orderId", "o-1"),
            Association::new("paymentRef", "pay-o-1")
        ]
    );
    assert_eq!(saga.start_key.as_deref(), Some("PaymentSaga|orderId=o-1"));
    assert!(!saga.ended && saga.ended_at_ms.is_none());
    assert_eq!(saga.version, 3);
    assert_eq!(saga.created_at_ms, at("2026-11-03T10:00:00Z"));
    assert_eq!(saga.updated_at_ms, at("2026-11-03T10:05:00.250Z"));

    let ended: SagaDocument = record::<SagaDoc, _>("sagas_ended");
    assert!(ended.ended && ended.start_key.is_none() && ended.associations.is_empty());
    assert_eq!(ended.ended_at_ms, Some(at("2026-11-03T11:00:00Z")));
    assert_eq!(ended.version, 0);

    let dispatch: SagaDispatchDocument = record::<DispatchDoc, _>("saga_dispatch");
    assert_eq!(dispatch.id, "7f6c1f0e-5a53-4f3e-9a0a-1d2f6f0c9b11:3001");
    assert_eq!(dispatch.saga_id, saga.id);
    assert_eq!(dispatch.seq, 3001);
    assert_eq!(dispatch.kind, DispatchKind::Command);
    assert_eq!(dispatch.type_name, "examples.PrintBanner");
    assert_eq!(dispatch.payload, r#"{"jobId":"j-1","text":"hello"}"#);
    assert_eq!(dispatch.headers["correlation-id"], "corr-1");
    assert_eq!(dispatch.routing_key.as_deref(), Some("j-1"));
    assert_eq!(dispatch.attempts, 10);
    assert!(dispatch.dispatched_at_ms.is_none());
    assert_eq!(
        dispatch.next_attempt_at_ms,
        Some(at("2026-11-03T10:06:00Z"))
    );
    assert_eq!(dispatch.failed_at_ms, Some(at("2026-11-03T10:06:01Z")));
    assert_eq!(dispatch.failure_reason, Some(FailureReason::UnknownOutcome));
    assert_eq!(
        dispatch.failure_message.as_deref(),
        Some("the printer hung up")
    );
    assert_eq!(dispatch.notice_pending, Some(true));
    assert_eq!(dispatch.notice_attempts, Some(2));
    assert_eq!(dispatch.notice_next_at_ms, Some(at("2026-11-03T10:06:30Z")));

    let pending: SagaDispatchDocument = record::<DispatchDoc, _>("saga_dispatch_pending");
    assert_eq!(pending.kind, DispatchKind::Event);
    assert!(pending.routing_key.is_none() && pending.failed_at_ms.is_none());
    assert!(pending.failure_reason.is_none() && pending.notice_pending.is_none());

    let lease: LeaseDoc = read("saga_dispatch_leases");
    assert_eq!(lease.id, saga.id);
    assert_eq!(lease.owner, "relay-instance-1");
    assert_eq!(lease.until.timestamp_millis(), at("2026-11-03T10:06:30Z"));

    let deadline: DeadlineDocument = record::<DeadlineDoc, _>("deadlines");
    assert_eq!(deadline.saga_type, "PaymentSaga");
    assert_eq!(deadline.saga_id, saga.id);
    assert_eq!(deadline.name, "charge-timeout");
    assert_eq!(deadline.payload.as_deref(), Some(r#"{"attempt":2}"#));
    assert_eq!(deadline.payload_type.as_deref(), Some("io.example.Retry"));
    assert_eq!(deadline.due_at_ms, at("2026-11-03T10:35:00Z"));
    assert_eq!(deadline.status, DeadlineStatus::Fired);
    assert_eq!(deadline.claimed_by.as_deref(), Some("scheduler-instance-1"));
    assert_eq!(deadline.claimed_until_ms, Some(at("2026-11-03T10:35:30Z")));
    assert_eq!(deadline.finished_at_ms, Some(at("2026-11-03T10:35:01Z")));
    assert_eq!(deadline.correlation_id.as_deref(), Some("corr-1"));

    let failure: SagaFailureDocument = record::<FailureDoc, _>("saga_failures");
    assert_eq!(failure.id, "PaymentSaga:evt-9");
    assert_eq!(
        (failure.saga_type.as_str(), failure.event_id.as_str()),
        ("PaymentSaga", "evt-9")
    );
    assert_eq!(failure.event_type, "examples.OrderPlaced");
    assert_eq!(failure.payload, r#"{"orderId":"o-9"}"#);
    assert_eq!(failure.headers["userId"], "u-1");
    assert_eq!(failure.attempts, 2);
    assert_eq!(failure.first_failed_at_ms, at("2026-11-03T10:10:00Z"));
    assert_eq!(failure.last_failed_at_ms, at("2026-11-03T10:10:00.100Z"));

    let call: ExternalCall = record::<ExternalCallDocument, _>("external_calls");
    assert_eq!(call.status, ExternalCallStatus::InProgress);
    assert_eq!(call.reply.as_deref(), Some(r#"{"printed":true}"#));
    assert_eq!(
        call.error.as_deref(),
        Some(r#"{"code":"HANDLER_ERROR","message":"no paper"}"#)
    );
    assert_eq!(call.attempts, 1);
    assert_eq!(call.updated_at_ms, at("2026-11-03T10:05:01Z"));
    assert_eq!(call.lease_until_ms, Some(at("2026-11-03T10:10:01Z")));
    assert_eq!(
        call.owner.as_deref(),
        Some("0b9e2c7a-3d41-4f7e-8c55-6a1f2e9d4b70")
    );
}

/// The names and BSON types of `doc` against the contract: `expected` lists `(field, type)` for
/// every field that must be there; nothing else may be (no `_class`, no nulls).
fn assert_shape(doc: &Document, expected: &[(&str, &str)]) {
    let mut got: Vec<(String, String)> = doc
        .iter()
        .map(|(k, v)| (k.clone(), bson_type(v).to_string()))
        .collect();
    got.sort();
    let mut want: Vec<(String, String)> = expected
        .iter()
        .map(|(k, t)| (k.to_string(), t.to_string()))
        .collect();
    want.sort();
    assert_eq!(got, want, "{doc:?}");
    assert!(!doc.values().any(|v| matches!(v, Bson::Null)), "{doc:?}");
}

fn bson_type(v: &Bson) -> &'static str {
    match v {
        Bson::String(_) => "string",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Array(_) => "array",
        Bson::Document(_) => "document",
        Bson::Null => "null",
        _ => "other",
    }
}

fn to_doc<T: Serialize>(value: &T) -> Document {
    mongodb::bson::to_document(value).unwrap()
}

#[test]
fn a_document_rust_writes_has_exactly_the_contracts_field_names_and_bson_types_and_no_nulls() {
    // Full: every optional field set, from the Kotlin fixtures (which also proves the round trip).
    let saga: SagaDocument = record::<SagaDoc, _>("sagas");
    let ended: SagaDocument = record::<SagaDoc, _>("sagas_ended");
    let written = to_doc(&SagaDoc::from(&saga));
    assert_shape(
        &written,
        &[
            ("_id", "string"),
            ("type", "string"),
            ("state", "string"),
            ("associations", "array"),
            ("startKey", "string"),
            ("ended", "bool"),
            ("version", "long"),
            ("createdAt", "date"),
            ("updatedAt", "date"),
        ],
    );
    let association = written.get_array("associations").unwrap()[0]
        .as_document()
        .unwrap();
    assert_shape(association, &[("k", "string"), ("v", "string")]);
    assert_shape(
        &to_doc(&SagaDoc::from(&ended)),
        &[
            ("_id", "string"),
            ("type", "string"),
            ("state", "string"),
            ("associations", "array"),
            ("ended", "bool"),
            ("endedAt", "date"),
            ("version", "long"),
            ("createdAt", "date"),
            ("updatedAt", "date"),
        ],
    );

    let dispatch = DispatchDoc::from(&record::<DispatchDoc, SagaDispatchDocument>(
        "saga_dispatch",
    ));
    assert_shape(
        &to_doc(&dispatch),
        &[
            ("_id", "string"),
            ("sagaId", "string"),
            ("seq", "long"),
            ("kind", "string"),
            ("type", "string"),
            ("payload", "string"),
            ("headers", "document"),
            ("routingKey", "string"),
            ("createdAt", "date"),
            ("attempts", "int"),
            ("nextAttemptAt", "date"),
            ("failedAt", "date"),
            ("failureReason", "string"),
            ("failureMessage", "string"),
            ("noticePending", "bool"),
            ("noticeAttempts", "int"),
            ("noticeNextAt", "date"),
        ],
    );
    assert_eq!(to_doc(&dispatch).get_str("kind"), Ok("COMMAND"));
    assert_eq!(
        to_doc(&dispatch).get_str("failureReason"),
        Ok("UNKNOWN_OUTCOME")
    );
    let pending = DispatchDoc::from(&record::<DispatchDoc, SagaDispatchDocument>(
        "saga_dispatch_pending",
    ));
    assert_shape(
        &to_doc(&pending),
        &[
            ("_id", "string"),
            ("sagaId", "string"),
            ("seq", "long"),
            ("kind", "string"),
            ("type", "string"),
            ("payload", "string"),
            ("headers", "document"),
            ("createdAt", "date"),
            ("attempts", "int"),
        ],
    );
    assert_eq!(to_doc(&pending).get_str("kind"), Ok("EVENT"));

    let lease: LeaseDoc = read("saga_dispatch_leases");
    assert_shape(
        &to_doc(&lease),
        &[("_id", "string"), ("owner", "string"), ("until", "date")],
    );

    let deadline = DeadlineDoc::from(&record::<DeadlineDoc, DeadlineDocument>("deadlines"));
    assert_shape(
        &to_doc(&deadline),
        &[
            ("_id", "string"),
            ("sagaType", "string"),
            ("sagaId", "string"),
            ("name", "string"),
            ("payload", "string"),
            ("payloadType", "string"),
            ("dueAt", "date"),
            ("status", "string"),
            ("claimedBy", "string"),
            ("claimedUntil", "date"),
            ("finishedAt", "date"),
            ("correlationId", "string"),
        ],
    );
    assert_eq!(to_doc(&deadline).get_str("status"), Ok("FIRED"));
    let scheduled = DeadlineDocument {
        id: "d-1".into(),
        saga_type: "PaymentSaga".into(),
        saga_id: "s-1".into(),
        name: "timeout".into(),
        payload: None,
        payload_type: None,
        due_at_ms: at("2026-11-03T12:00:00Z"),
        status: DeadlineStatus::Scheduled,
        claimed_by: None,
        claimed_until_ms: None,
        finished_at_ms: None,
        correlation_id: None,
    };
    let scheduled = DeadlineDoc::from(&scheduled);
    assert_shape(
        &to_doc(&scheduled),
        &[
            ("_id", "string"),
            ("sagaType", "string"),
            ("sagaId", "string"),
            ("name", "string"),
            ("dueAt", "date"),
            ("status", "string"),
        ],
    );
    assert_eq!(to_doc(&scheduled).get_str("status"), Ok("SCHEDULED"));

    let failure = FailureDoc::from(&record::<FailureDoc, SagaFailureDocument>("saga_failures"));
    assert_shape(
        &to_doc(&failure),
        &[
            ("_id", "string"),
            ("sagaType", "string"),
            ("eventId", "string"),
            ("eventType", "string"),
            ("payload", "string"),
            ("headers", "document"),
            ("error", "string"),
            ("attempts", "int"),
            ("firstFailedAt", "date"),
            ("lastFailedAt", "date"),
        ],
    );

    let call = ExternalCallDocument::from(&record::<ExternalCallDocument, ExternalCall>(
        "external_calls",
    ));
    assert_shape(
        &to_doc(&call),
        &[
            ("_id", "string"),
            ("status", "string"),
            ("reply", "string"),
            ("error", "string"),
            ("attempts", "int"),
            ("updatedAt", "date"),
            ("leaseUntil", "date"),
            ("owner", "string"),
        ],
    );
    assert_eq!(to_doc(&call).get_str("status"), Ok("IN_PROGRESS"));
    let started = ExternalCallDocument::from(&ExternalCall {
        key: "k-1".into(),
        status: ExternalCallStatus::Unknown,
        reply: None,
        error: None,
        attempts: 1,
        updated_at_ms: at("2026-11-03T12:00:00Z"),
        lease_until_ms: None,
        owner: None,
    });
    assert_shape(
        &to_doc(&started),
        &[
            ("_id", "string"),
            ("status", "string"),
            ("attempts", "int"),
            ("updatedAt", "date"),
        ],
    );
    assert_eq!(to_doc(&started).get_str("status"), Ok("UNKNOWN"));
}

// ---- Mongo

async fn db(uri: &str) -> Database {
    let client = mongodb::Client::with_uri_str(uri).await.unwrap();
    client.default_database().expect("database in uri")
}

const DAY: Duration = Duration::from_secs(86_400);

/// Every index of the store (the saga and external-call ones among them), default retentions.
async fn ensure_all(db: &Database) {
    MongoStore::new(db.client().clone(), db.clone())
        .ensure_indexes(&MongoIndexSettings::default())
        .await
        .unwrap();
}

async fn indexes(db: &Database, collection: &str) -> Vec<IndexModel> {
    db.collection::<Document>(collection)
        .list_indexes()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn key_names(model: &IndexModel) -> Vec<&str> {
    model.keys.keys().map(String::as_str).collect()
}

/// The one index of `collection` on exactly `keys` (ascending, in this order).
async fn index(db: &Database, collection: &str, keys: &[&str]) -> IndexModel {
    let all = indexes(db, collection).await;
    let found: Vec<&IndexModel> = all.iter().filter(|m| key_names(m) == keys).collect();
    assert_eq!(
        found.len(),
        1,
        "no single index on {collection}{keys:?} among {:?}",
        all.iter().map(key_names).collect::<Vec<_>>()
    );
    for (_, direction) in &found[0].keys {
        assert!(
            matches!(
                direction,
                Bson::Int32(1) | Bson::Int64(1) | Bson::Double(1.0)
            ),
            "{direction:?}"
        );
    }
    found[0].clone()
}

fn options(model: &IndexModel) -> mongodb::options::IndexOptions {
    model.options.clone().unwrap_or_default()
}

#[tokio::test]
async fn every_index_of_the_contract_exists_with_its_keys_partial_filters_and_ttls() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let db = db(&mongo.uri).await;
    ensure_all(&db).await;

    let associations = index(&db, "sagas", &["type", "associations.k", "associations.v"]).await;
    assert_eq!(
        options(&associations).partial_filter_expression,
        Some(doc! { "ended": false })
    );
    assert_ne!(options(&associations).unique, Some(true));

    let start_key = index(&db, "sagas", &["startKey"]).await;
    assert_eq!(options(&start_key).unique, Some(true));
    assert_eq!(
        options(&start_key).partial_filter_expression,
        Some(doc! { "ended": false, "startKey": { "$exists": true } })
    );
    assert_eq!(
        options(&index(&db, "sagas", &["endedAt"]).await).expire_after,
        Some(30 * DAY)
    );

    index(&db, "saga_dispatch", &["sagaId", "seq"]).await;
    index(
        &db,
        "saga_dispatch",
        &["dispatchedAt", "failedAt", "sagaId", "seq"],
    )
    .await;
    let notice = index(&db, "saga_dispatch", &["noticeNextAt"]).await;
    assert_eq!(
        options(&notice).partial_filter_expression,
        Some(doc! { "noticePending": true })
    );
    assert_eq!(
        options(&index(&db, "saga_dispatch", &["dispatchedAt"]).await).expire_after,
        Some(7 * DAY)
    );
    let failed = index(&db, "saga_dispatch", &["failedAt"]).await;
    assert_eq!(options(&failed).name.as_deref(), Some("failedAt_ttl"));
    assert_eq!(options(&failed).expire_after, Some(30 * DAY));
    assert_eq!(
        options(&index(&db, "saga_dispatch_leases", &["until"]).await).expire_after,
        Some(Duration::ZERO)
    );

    index(&db, "deadlines", &["status", "dueAt"]).await;
    index(&db, "deadlines", &["sagaId", "status"]).await;
    assert_eq!(
        options(&index(&db, "deadlines", &["finishedAt"]).await).expire_after,
        Some(7 * DAY)
    );

    index(&db, "saga_failures", &["sagaType", "lastFailedAt"]).await;
    let parked = index(&db, "saga_failures", &["lastFailedAt"]).await;
    assert_eq!(options(&parked).name.as_deref(), Some("lastFailedAt_ttl"));
    assert_eq!(options(&parked).expire_after, Some(30 * DAY));

    assert_eq!(
        options(&index(&db, "external_calls", &["updatedAt"]).await).expire_after,
        Some(30 * DAY)
    );
}

#[tokio::test]
async fn creating_them_twice_is_idempotent() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let db = db(&mongo.uri).await;
    ensure_all(&db).await;
    let names = |db: &Database| {
        let db = db.clone();
        async move {
            let mut all = Vec::new();
            for c in [
                "sagas",
                "saga_dispatch",
                "saga_dispatch_leases",
                "deadlines",
                "saga_failures",
                "external_calls",
            ] {
                for m in indexes(&db, c).await {
                    all.push((c, m.options.and_then(|o| o.name).unwrap_or_default()));
                }
            }
            all.sort();
            all
        }
    };
    let before = names(&db).await;
    assert!(before.len() >= 13, "{before:?}");

    ensure_all(&db).await;

    assert_eq!(before, names(&db).await);
}

fn saga(id: &str, start_key: Option<&str>, ended: bool) -> SagaDocument {
    let now = at("2026-11-03T10:00:00Z");
    SagaDocument {
        id: id.into(),
        type_name: "PaymentSaga".into(),
        state: "{}".into(),
        associations: vec![],
        start_key: start_key.map(str::to_string),
        ended,
        ended_at_ms: ended.then_some(now),
        version: 0,
        created_at_ms: now,
        updated_at_ms: now,
    }
}

fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    use mongodb::error::{ErrorKind, WriteFailure};
    matches!(&*e.kind, ErrorKind::Write(WriteFailure::WriteError(w)) if w.code == 11000)
}

#[tokio::test]
async fn two_live_instances_with_one_start_key_are_refused() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let db = db(&mongo.uri).await;
    ensure_all(&db).await;
    let sagas = db.collection::<SagaDoc>("sagas");
    let key = Some("PaymentSaga|orderId=o-1");
    let doc = |id, key, ended| SagaDoc::from(&saga(id, key, ended));

    // Ended instances do not hold the key.
    sagas.insert_one(doc("s-1", key, true)).await.unwrap();
    sagas.insert_one(doc("s-2", key, true)).await.unwrap();
    sagas.insert_one(doc("s-3", key, false)).await.unwrap();

    let refused = sagas.insert_one(doc("s-4", key, false)).await.unwrap_err();
    assert!(is_duplicate_key(&refused), "{refused:?}");

    // forceNew instances carry no start key: any number may be live; other keys are independent.
    sagas.insert_one(doc("s-5", None, false)).await.unwrap();
    sagas.insert_one(doc("s-6", None, false)).await.unwrap();
    sagas
        .insert_one(doc("s-7", Some("PaymentSaga|orderId=o-2"), false))
        .await
        .unwrap();
    // Ending the live one frees the key.
    sagas
        .update_one(doc! { "_id": "s-3" }, doc! { "$set": { "ended": true } })
        .await
        .unwrap();
    sagas.insert_one(doc("s-8", key, false)).await.unwrap();
}

#[tokio::test]
async fn kotlin_shaped_documents_are_read_back_through_the_driver_and_rewritten_without_class() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let db = db(&mongo.uri).await;
    ensure_all(&db).await;
    let raw = db.collection::<Document>("sagas");
    raw.insert_one(fixture("sagas")).await.unwrap();

    let typed = db.collection::<SagaDoc>("sagas");
    let mut saga = SagaDocument::from(
        typed
            .find_one(doc! { "_id": "7f6c1f0e-5a53-4f3e-9a0a-1d2f6f0c9b11" })
            .await
            .unwrap()
            .expect("the Kotlin document"),
    );
    assert_eq!(saga.version, 3);
    assert_eq!(saga.associations.len(), 2);

    // Rust replaces it: its own shape, no `_class`, state text untouched.
    saga.version += 1;
    typed
        .replace_one(doc! { "_id": &saga.id }, SagaDoc::from(&saga))
        .await
        .unwrap();
    let stored = raw
        .find_one(doc! { "_id": &saga.id })
        .await
        .unwrap()
        .unwrap();
    assert!(!stored.contains_key("_class"), "{stored:?}");
    assert_eq!(stored.get_i64("version"), Ok(4));
    assert_eq!(
        stored.get_str("state"),
        Ok(r#"{"orderId":"o-1","amount":12.30,"log":{"a.b":"x"}}"#)
    );
}
