//! The storage contract of ADR 0022 for dead letters: the `dead_letters` document and its indexes.
//! The fixtures in `tests/fixtures/dead_letters_contract` are written by hand from the ADR's table
//! (the Kotlin library writes its own from the same table); neither library writes `_class` or a
//! null, and an absent field stays absent.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use futures::TryStreamExt;
use kraken_messaging::MessagingError;
use kraken_messaging::processors::dead_letters::{
    DEAD_LETTERS_COLLECTION, DeadLetterDocument, DeadLetterError, DeadLetterReason,
    DeadLetterRecord, ensure_dead_letter_indexes,
};
use mongodb::bson::{Bson, DateTime, Document, spec::BinarySubtype};
use mongodb::{Database, IndexModel};

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

fn fixture(name: &str) -> Document {
    let path = format!(
        "{}/tests/fixtures/dead_letters_contract/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    match Bson::try_from(json).unwrap() {
        Bson::Document(d) => d,
        other => panic!("{name} is not a document: {other:?}"),
    }
}

fn read(name: &str) -> DeadLetterDocument {
    mongodb::bson::from_document(fixture(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
}

fn at(rfc3339: &str) -> DateTime {
    DateTime::parse_rfc3339_str(rfc3339).unwrap()
}

fn to_doc(letter: &DeadLetterDocument) -> Document {
    mongodb::bson::to_document(letter).unwrap()
}

fn sorted(doc: &Document) -> Vec<(String, Bson)> {
    let mut all: Vec<(String, Bson)> = doc.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    all.sort_by(|a, b| a.0.cmp(&b.0));
    all
}

fn bson_type(v: &Bson) -> &'static str {
    match v {
        Bson::String(_) => "string",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::DateTime(_) => "date",
        Bson::Document(_) => "document",
        Bson::Binary(_) => "binData",
        Bson::Null => "null",
        _ => "other",
    }
}

/// The names and BSON types of `doc`: exactly `expected`, nothing else (no `_class`, no nulls).
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
}

const COMMON: [(&str, &str); 11] = [
    ("_id", "string"),
    ("processor", "string"),
    ("topic", "string"),
    ("partition", "int"),
    ("offset", "long"),
    ("headers", "document"),
    ("reason", "string"),
    ("error", "document"),
    ("attempts", "int"),
    ("parkedAt", "date"),
    ("lastAttemptAt", "date"),
];

fn shape(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
    let mut all: Vec<_> = COMMON.to_vec();
    all.push(("expireAt", "date"));
    all.extend_from_slice(extra);
    all
}

#[test]
fn the_fixtures_of_each_reason_deserialise() {
    let failed = read("handler_failed");
    assert_eq!(failed.id, "orders:kraken.events:1:42");
    assert_eq!(failed.processor, "orders");
    assert_eq!(failed.topic, "kraken.events");
    assert_eq!((failed.partition, failed.offset), (1, 42));
    assert_eq!(failed.key.as_deref(), Some("order-7"));
    assert_eq!(failed.event_id.as_deref(), Some("evt-7"));
    assert_eq!(failed.event_type.as_deref(), Some("examples.OrderPlaced"));
    assert_eq!(failed.headers["correlationId"], "corr-7");
    assert_eq!(failed.headers["userId"], "");
    assert_eq!(
        failed.payload.as_deref(),
        Some(r#"{"orderId":"o-7","amount":12.30}"#)
    );
    assert!(failed.payload_binary.is_none());
    assert_eq!(failed.reason, DeadLetterReason::HandlerFailed);
    assert_eq!(
        failed.error,
        DeadLetterError {
            type_name: "IllegalStateException".into(),
            message: "boom".into()
        }
    );
    assert!(failed.blocked_by.is_none());
    assert_eq!(failed.attempts, 5);
    assert_eq!(failed.parked_at, at("2026-11-04T10:00:05Z"));
    assert_eq!(failed.last_attempt_at, at("2026-11-04T10:00:05Z"));
    assert_eq!(failed.expire_at, at("2026-12-04T10:00:05Z"));
    assert_eq!(
        failed.payload_bytes(),
        br#"{"orderId":"o-7","amount":12.30}"#.to_vec()
    );

    let sequenced = read("sequenced");
    assert_eq!(sequenced.reason, DeadLetterReason::Sequenced);
    assert_eq!(
        sequenced.blocked_by.as_deref(),
        Some("orders:kraken.events:1:42")
    );
    assert_eq!(
        sequenced.error,
        DeadLetterError::sequenced("orders:kraken.events:1:42")
    );
    assert_eq!(sequenced.attempts, 0);
    assert_eq!(sequenced.parked_at, at("2026-11-04T10:00:06.250Z"));

    let binary = read("invalid_binary");
    assert_eq!(binary.reason, DeadLetterReason::Invalid);
    assert!(binary.key.is_none() && binary.event_id.is_none() && binary.event_type.is_none());
    assert!(binary.payload.is_none());
    assert_eq!(binary.payload_bytes(), vec![0xff, 0x00, 0xfe]);
    assert_eq!(
        binary.error,
        DeadLetterError::invalid("record without type header")
    );

    let empty = read("invalid_empty");
    assert!(empty.payload.is_none() && empty.payload_binary.is_none());
    assert!(empty.payload_bytes().is_empty());
    assert!(empty.event_id.is_none());
}

#[test]
fn rust_writes_each_fixture_back_field_for_field() {
    for name in [
        "handler_failed",
        "sequenced",
        "invalid_binary",
        "invalid_empty",
    ] {
        assert_eq!(
            sorted(&to_doc(&read(name))),
            sorted(&fixture(name)),
            "{name}"
        );
    }
}

fn record(key: Option<&[u8]>, headers: &[(&str, &[u8])], payload: &[u8]) -> DeadLetterRecord {
    DeadLetterRecord {
        processor: "orders".into(),
        topic: "kraken.events".into(),
        partition: 1,
        offset: 42,
        key: key.map(<[u8]>::to_vec),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_vec()))
            .collect(),
        payload: payload.to_vec(),
    }
}

#[test]
fn a_letter_rust_parks_has_exactly_the_contracts_fields_and_types() {
    let now = at("2026-11-04T10:00:05Z");
    let retention = Duration::from_secs(30 * 86_400);
    let failed = DeadLetterDocument::park(
        record(
            Some(b"order-7"),
            &[
                ("type", b"examples.OrderPlaced"),
                ("id", b"evt-7"),
                ("correlationId", b"corr-7"),
                ("userId", b""),
            ],
            br#"{"orderId":"o-7","amount":12.30}"#,
        ),
        DeadLetterReason::HandlerFailed,
        DeadLetterError {
            type_name: "IllegalStateException".into(),
            message: "boom".into(),
        },
        None,
        5,
        now,
        retention,
    );
    assert_eq!(
        sorted(&to_doc(&failed)),
        sorted(&fixture("handler_failed")),
        "the fixture, built from the record"
    );
    assert_shape(
        &to_doc(&failed),
        &shape(&[
            ("key", "string"),
            ("eventId", "string"),
            ("eventType", "string"),
            ("payload", "string"),
        ]),
    );

    let sequenced = DeadLetterDocument::park(
        record(Some(b"order-7"), &[("type", b"t.E"), ("id", b"e-2")], b"{}"),
        DeadLetterReason::Sequenced,
        DeadLetterError::sequenced("orders:kraken.events:1:41"),
        Some("orders:kraken.events:1:41".into()),
        0,
        now,
        retention,
    );
    let doc = to_doc(&sequenced);
    assert_shape(
        &doc,
        &shape(&[
            ("key", "string"),
            ("eventId", "string"),
            ("eventType", "string"),
            ("payload", "string"),
            ("blockedBy", "string"),
        ]),
    );
    assert_eq!(doc.get_str("reason"), Ok("SEQUENCED"));
    assert_eq!(
        doc.get_document("error").unwrap(),
        &mongodb::bson::doc! { "type": "Sequenced", "message": "an earlier letter for this key is parked: orders:kraken.events:1:41" }
    );

    // No key, no type or id header, a value that is not UTF-8 (and a header that is not either).
    let binary = DeadLetterDocument::park(
        record(None, &[("source", b"legacy-\xfftool")], &[0xff, 0x00, 0xfe]),
        DeadLetterReason::Invalid,
        DeadLetterError::invalid("record without type header"),
        None,
        0,
        now,
        retention,
    );
    let doc = to_doc(&binary);
    assert_shape(&doc, &shape(&[("payloadBinary", "binData")]));
    match doc.get("payloadBinary") {
        Some(Bson::Binary(b)) => {
            assert_eq!(b.subtype, BinarySubtype::Generic);
            assert_eq!(b.bytes, vec![0xff, 0x00, 0xfe]);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(doc.get_str("reason"), Ok("INVALID"));
    assert_eq!(
        doc.get_document("headers").unwrap().get_str("source"),
        Ok("legacy-\u{fffd}tool"),
        "lossy UTF-8"
    );

    // An empty value: neither payload field.
    let empty = DeadLetterDocument::park(
        record(Some(b"k\xff"), &[("type", b"t.E")], b""),
        DeadLetterReason::Invalid,
        DeadLetterError::invalid("payload is not a JSON object"),
        None,
        0,
        now,
        retention,
    );
    let doc = to_doc(&empty);
    assert_shape(&doc, &shape(&[("key", "string"), ("eventType", "string")]));
    assert_eq!(doc.get_str("key"), Ok("k\u{fffd}"), "lossy UTF-8");
    assert_eq!(doc.get_str("_id"), Ok("orders:kraken.events:1:42"));
}

#[test]
fn the_error_message_is_cut_at_2000_characters_and_a_repeated_header_keeps_its_last_value() {
    let long = "é".repeat(2500);
    let e = DeadLetterError::from_error(&MessagingError::Storage(long.clone()));
    assert_eq!(e.type_name, "Storage");
    assert_eq!(e.message.chars().count(), 2000);
    assert!(
        long.starts_with(&e.message[..])
            || format!("storage error: {long}").starts_with(&e.message[..])
    );
    let letter = DeadLetterDocument::park(
        record(None, &[("type", b"a"), ("type", b"b")], b"{}"),
        DeadLetterReason::HandlerFailed,
        e,
        None,
        1,
        DateTime::now(),
        Duration::from_secs(1),
    );
    assert_eq!(
        letter.headers,
        BTreeMap::from([("type".to_string(), "b".to_string())])
    );
    assert_eq!(letter.event_type.as_deref(), Some("b"));
    assert_eq!(
        letter.expire_at.timestamp_millis() - letter.parked_at.timestamp_millis(),
        1000
    );
}

async fn indexes(db: &Database) -> Vec<IndexModel> {
    db.collection::<Document>(DEAD_LETTERS_COLLECTION)
        .list_indexes()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn the_three_indexes_of_the_contract_exist_with_their_names_keys_and_ttl() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let client = mongodb::Client::with_uri_str(&mongo.uri).await.unwrap();
    let db = client.default_database().unwrap();
    ensure_dead_letter_indexes(&db).await.unwrap();
    ensure_dead_letter_indexes(&db).await.unwrap(); // idempotent
    let all = indexes(&db).await;
    let by_name = |name: &str| {
        all.iter()
            .find(|m| m.options.as_ref().and_then(|o| o.name.as_deref()) == Some(name))
            .unwrap_or_else(|| panic!("no index {name}"))
            .clone()
    };
    let keys = |m: &IndexModel| m.keys.keys().cloned().collect::<Vec<_>>();
    assert_eq!(
        keys(&by_name("dead_letters_processor_key_offset")),
        ["processor", "key", "offset"]
    );
    assert_eq!(
        keys(&by_name("dead_letters_processor_parked")),
        ["processor", "parkedAt"]
    );
    let expire = by_name("dead_letters_expire");
    assert_eq!(keys(&expire), ["expireAt"]);
    assert_eq!(
        expire.options.unwrap().expire_after,
        Some(Duration::ZERO),
        "expireAfterSeconds: 0"
    );
    assert_eq!(all.len(), 4, "the three and _id");
}
