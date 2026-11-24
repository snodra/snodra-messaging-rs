//! The storage contract for dead letters: the `dead_letters` document and its indexes.
//! The fixtures in `tests/fixtures/dead_letters_contract` are written by hand from the contract's
//! table (the specification's MongoDB layout): this library writes each of them from its record
//! and reads each of them back through its store and admin API, so a letter parked by any Snodra
//! library is one this library can list, retry and discard. No `_class` or null is written, an
//! absent field stays absent, and a blank `id` or
//! `type` header is an absent `eventId` or `eventType`. `replay` (parked while its processor
//! replayed the record) is written only when true.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use futures::TryStreamExt;
use mongodb::bson::{Bson, DateTime, Document, spec::BinarySubtype};
use mongodb::{Database, IndexModel};
use snodra_messaging::MessagingError;
use snodra_messaging::config::DeadLetterMode;
use snodra_messaging::processors::DeadLetters;
use snodra_messaging::processors::dead_letters::{DeadLetterRecord, park};
use snodra_messaging::store::{DeadLetter, DeadLetterError, DeadLetterReason, MessageStore};
use snodra_messaging_mongo::documents::DeadLetterDocument;
use snodra_messaging_mongo::{MongoIndexSettings, MongoStore};

const DEAD_LETTERS_COLLECTION: &str = "dead_letters";

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

/// Every shared fixture.
const FIXTURES: [&str; 6] = [
    "handler_failed",
    "sequenced",
    "invalid_binary",
    "invalid_empty",
    "invalid_blank_headers",
    "replayed",
];

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

/// A fixture read as the MongoDB store reads it.
fn read(name: &str) -> DeadLetter {
    mongodb::bson::from_document::<DeadLetterDocument>(fixture(name))
        .unwrap_or_else(|e| panic!("{name}: {e}"))
        .into()
}

/// Epoch milliseconds.
fn at(rfc3339: &str) -> i64 {
    DateTime::parse_rfc3339_str(rfc3339)
        .unwrap()
        .timestamp_millis()
}

/// A letter as the MongoDB store writes it.
fn to_doc(letter: &DeadLetter) -> Document {
    mongodb::bson::to_document(&DeadLetterDocument::from(letter)).unwrap()
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
        Bson::Boolean(_) => "bool",
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
    assert_eq!(failed.id, "orders:snodra.events:1:42");
    assert_eq!(failed.processor, "orders");
    assert_eq!(failed.source, "snodra.events");
    assert_eq!(
        (failed.segment.as_str(), failed.position.as_str()),
        ("1", "42")
    );
    assert_eq!(failed.key.as_deref(), Some("order-7"));
    assert_eq!(failed.event_id.as_deref(), Some("evt-7"));
    assert_eq!(failed.event_type.as_deref(), Some("examples.OrderPlaced"));
    assert_eq!(failed.headers["correlationId"], "corr-7");
    assert_eq!(failed.headers["userId"], "");
    assert_eq!(
        to_doc(&failed).get_str("payload"),
        Ok(r#"{"orderId":"o-7","amount":12.30}"#),
        "the value as text"
    );
    assert!(!to_doc(&failed).contains_key("payloadBinary"));
    assert_eq!(failed.reason, DeadLetterReason::HandlerFailed);
    assert_eq!(
        failed.error,
        DeadLetterError {
            type_name: "IllegalStateException".into(),
            message: "boom".into()
        }
    );
    assert!(failed.blocked_by.is_none());
    assert!(!failed.replay, "absent is false");
    assert_eq!(failed.attempts, 5);
    assert_eq!(failed.parked_at_ms, at("2026-11-04T10:00:05Z"));
    assert_eq!(failed.last_attempt_at_ms, at("2026-11-04T10:00:05Z"));
    assert_eq!(failed.expire_at_ms, at("2026-12-04T10:00:05Z"));
    assert_eq!(
        failed.payload,
        br#"{"orderId":"o-7","amount":12.30}"#.to_vec()
    );

    let sequenced = read("sequenced");
    assert_eq!(sequenced.reason, DeadLetterReason::Sequenced);
    assert_eq!(
        sequenced.blocked_by.as_deref(),
        Some("orders:snodra.events:1:42")
    );
    assert_eq!(
        sequenced.error,
        DeadLetterError::sequenced("orders:snodra.events:1:42")
    );
    assert_eq!(sequenced.attempts, 0);
    assert_eq!(sequenced.parked_at_ms, at("2026-11-04T10:00:06.250Z"));

    let binary = read("invalid_binary");
    assert_eq!(binary.reason, DeadLetterReason::Invalid);
    assert!(binary.key.is_none() && binary.event_id.is_none() && binary.event_type.is_none());
    assert_eq!(binary.headers["source"], "legacy-\u{fffd}tool");
    assert!(!to_doc(&binary).contains_key("payload"));
    assert_eq!(binary.payload, vec![0xff, 0x00, 0xfe]);
    assert_eq!(
        binary.error,
        DeadLetterError::invalid("record without type header")
    );

    let empty = read("invalid_empty");
    let doc = to_doc(&empty);
    assert!(!doc.contains_key("payload") && !doc.contains_key("payloadBinary"));
    assert!(empty.payload.is_empty());
    assert!(empty.event_id.is_none());

    let blank = read("invalid_blank_headers");
    assert!(blank.event_id.is_none() && blank.event_type.is_none());
    assert_eq!(blank.headers["type"], "  ");
    assert_eq!(blank.dedupe_id(), "snodra.events-0-8");

    let replayed = read("replayed");
    assert_eq!(replayed.reason, DeadLetterReason::HandlerFailed);
    assert!(replayed.replay);
    assert_eq!(replayed.parked_at_ms, at("2026-11-04T10:04:00Z"));
}

#[test]
fn rust_writes_each_fixture_back_field_for_field() {
    for name in FIXTURES {
        assert_eq!(
            sorted(&to_doc(&read(name))),
            sorted(&fixture(name)),
            "{name}"
        );
    }
}

fn record(key: Option<&[u8]>, headers: &[(&str, &[u8])], payload: &[u8]) -> DeadLetterRecord {
    record_at("orders", 1, 42, key, headers, payload)
}

fn record_at(
    processor: &str,
    partition: i32,
    offset: i64,
    key: Option<&[u8]>,
    headers: &[(&str, &[u8])],
    payload: &[u8],
) -> DeadLetterRecord {
    DeadLetterRecord {
        processor: processor.into(),
        source: "snodra.events".into(),
        segment: partition.to_string(),
        position: offset.to_string(),
        key: key.map(|k| String::from_utf8_lossy(k).into_owned()),
        // As a delivery carries them: text, lossy UTF-8.
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), String::from_utf8_lossy(v).into_owned()))
            .collect(),
        payload: payload.to_vec(),
    }
}

#[test]
fn a_letter_rust_parks_has_exactly_the_contracts_fields_and_types() {
    let now = at("2026-11-04T10:00:05Z");
    let retention = Duration::from_secs(30 * 86_400);
    let failed = park(
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

    let sequenced = park(
        record(Some(b"order-7"), &[("type", b"t.E"), ("id", b"e-2")], b"{}"),
        DeadLetterReason::Sequenced,
        DeadLetterError::sequenced("orders:snodra.events:1:41"),
        Some("orders:snodra.events:1:41".into()),
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
        &mongodb::bson::doc! { "type": "Sequenced", "message": "an earlier letter for this key is parked: orders:snodra.events:1:41" }
    );

    // No key, no type or id header, a value that is not UTF-8 (and a header that is not either).
    let binary = park(
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
    let empty = park(
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
    assert_eq!(doc.get_str("_id"), Ok("orders:snodra.events:1:42"));
}

/// Every fixture but `handler_failed` (above), written by this library from its record.
#[test]
fn rust_parks_each_shared_fixture_from_its_record() {
    let retention = Duration::from_secs(30 * 86_400);
    let sequenced = park(
        record_at(
            "orders",
            1,
            43,
            Some(b"order-7"),
            &[("type", b"examples.OrderShipped"), ("id", b"evt-8")],
            br#"{"orderId":"o-7"}"#,
        ),
        DeadLetterReason::Sequenced,
        DeadLetterError::sequenced("orders:snodra.events:1:42"),
        Some("orders:snodra.events:1:42".into()),
        0,
        at("2026-11-04T10:00:06.250Z"),
        retention,
    );
    let binary = park(
        record_at(
            "default",
            0,
            7,
            None,
            &[("source", b"legacy-\xfftool")],
            &[0xff, 0x00, 0xfe],
        ),
        DeadLetterReason::Invalid,
        DeadLetterError::invalid("record without type header"),
        None,
        0,
        at("2026-11-04T10:01:00Z"),
        retention,
    );
    let empty = park(
        record_at(
            "orders",
            2,
            9,
            Some(b"order-9"),
            &[("type", b"examples.OrderPlaced")],
            b"",
        ),
        DeadLetterReason::Invalid,
        DeadLetterError::invalid("payload is not a JSON object"),
        None,
        0,
        at("2026-11-04T10:02:00Z"),
        retention,
    );
    // Blank `type` and `id` headers: kept in `headers`, absent as `eventType` and `eventId`.
    let blank = park(
        record_at(
            "default",
            0,
            8,
            None,
            &[("type", b"  "), ("id", b"")],
            b"{}",
        ),
        DeadLetterReason::Invalid,
        DeadLetterError::invalid("record without type header"),
        None,
        0,
        at("2026-11-04T10:03:00Z"),
        retention,
    );
    // Parked while its processor replayed the record.
    let replayed = DeadLetter {
        replay: true,
        ..park(
            record_at(
                "orders",
                1,
                44,
                Some(b"order-8"),
                &[("type", b"examples.OrderPlaced"), ("id", b"evt-9")],
                br#"{"orderId":"o-8"}"#,
            ),
            DeadLetterReason::HandlerFailed,
            DeadLetterError {
                type_name: "IllegalStateException".into(),
                message: "boom".into(),
            },
            None,
            5,
            at("2026-11-04T10:04:00Z"),
            retention,
        )
    };
    assert_shape(
        &to_doc(&replayed),
        &shape(&[
            ("key", "string"),
            ("eventId", "string"),
            ("eventType", "string"),
            ("payload", "string"),
            ("replay", "bool"),
        ]),
    );
    for (name, letter) in [
        ("sequenced", sequenced),
        ("invalid_binary", binary),
        ("invalid_empty", empty),
        ("invalid_blank_headers", blank),
        ("replayed", replayed),
    ] {
        assert_eq!(sorted(&to_doc(&letter)), sorted(&fixture(name)), "{name}");
    }
}

/// A blank `eventId` (a document from before the blank rule, or written by hand) is no id: the
/// retry's marker uses the consumer's fallback, never a shared `""`.
#[test]
fn a_blank_event_id_dedupes_by_the_fallback_id() {
    let mut letter = read("handler_failed");
    for blank in ["", "   "] {
        letter.event_id = Some(blank.to_string());
        assert_eq!(letter.dedupe_id(), "snodra.events-1-42", "{blank:?}");
    }
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
    let letter = park(
        record(None, &[("type", b"a"), ("type", b"b")], b"{}"),
        DeadLetterReason::HandlerFailed,
        e,
        None,
        1,
        snodra_messaging::time::now_ms(),
        Duration::from_secs(1),
    );
    assert_eq!(
        letter.headers,
        BTreeMap::from([("type".to_string(), "b".to_string())])
    );
    assert_eq!(letter.event_type.as_deref(), Some("b"));
    assert_eq!(letter.expire_at_ms - letter.parked_at_ms, 1000);
}

/// A document another service wrote (each shared fixture, inserted as is) reads back through
/// this library's store and admin API with every field the admin API returns.
#[tokio::test]
async fn every_shared_fixture_reads_back_through_the_store_and_the_admin_api() {
    require_docker!();
    let mongo = support::containers::mongo().await;
    let client = mongodb::Client::with_uri_str(&mongo.uri).await.unwrap();
    let db = client.default_database().unwrap();
    let raw = db.collection::<Document>(DEAD_LETTERS_COLLECTION);
    for name in FIXTURES {
        raw.insert_one(fixture(name)).await.unwrap();
    }
    let store = MongoStore::new(client.clone(), db.clone()).dead_letters();
    for name in FIXTURES {
        let id = fixture(name).get_str("_id").unwrap().to_string();
        let letter = store
            .get(&id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{name}"));
        assert_eq!(letter, read(name), "{name}");
        assert_eq!(
            sorted(&to_doc(&letter)),
            sorted(&fixture(name)),
            "{name}: every field"
        );
    }
    let failed = store
        .get("orders:snodra.events:1:42")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.dedupe_id(), "evt-7");
    assert_eq!(failed.error.type_name, "IllegalStateException");
    let binary = store
        .get("default:snodra.events:0:7")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binary.payload, vec![0xff, 0x00, 0xfe]);
    let blank = store
        .get("default:snodra.events:0:8")
        .await
        .unwrap()
        .unwrap();
    assert!(blank.event_id.is_none() && blank.event_type.is_none());

    // The admin API's reads (list and count need no processor of this instance).
    let admin = DeadLetters::new(Some(store), DeadLetterMode::Collection, vec![]);
    let listed = admin.list(None, 100).await.unwrap();
    assert_eq!(
        listed.iter().map(|l| l.id.as_str()).collect::<Vec<_>>(),
        [
            "orders:snodra.events:1:42",
            "orders:snodra.events:1:43",
            "default:snodra.events:0:7",
            "orders:snodra.events:2:9",
            "default:snodra.events:0:8",
            "orders:snodra.events:1:44",
        ],
        "by parkedAt"
    );
    // FIXTURES is in parkedAt order too.
    for (letter, name) in listed.iter().zip(FIXTURES) {
        assert_eq!(sorted(&to_doc(letter)), sorted(&fixture(name)), "{name}");
    }
    assert_eq!(admin.count(None).await.unwrap(), 6);
    let store = MongoStore::new(client, db).dead_letters();
    assert_eq!(
        store.head("orders", "order-7").await.unwrap().as_deref(),
        Some("orders:snodra.events:1:42")
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
    let store = MongoStore::new(client, db.clone());
    store
        .ensure_indexes(&MongoIndexSettings::default())
        .await
        .unwrap();
    store
        .ensure_indexes(&MongoIndexSettings::default())
        .await
        .unwrap(); // idempotent
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
