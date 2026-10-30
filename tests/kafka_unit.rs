mod support;

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use kraken_messaging::hub::HandlerRegistry;
use kraken_messaging::kafka::{RecordDisposition, classify, headers, new_outbox_document};
use kraken_messaging::{Event, Identity, MessageContext, message_type};
use mongodb::bson::Bson;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Happened {
    id: String,
    at: DateTime<Utc>,
}
message_type!(Happened, "t.Happened");
impl Event for Happened {}

fn event() -> Happened {
    Happened {
        id: "e-1".into(),
        at: "2026-10-30T00:49:45.227152891Z".parse().unwrap(),
    }
}

#[test]
fn document_matches_kotlin_shape() {
    let doc = new_outbox_document(&event(), Some("k1"), None, 42).unwrap();
    let bson = mongodb::bson::to_document(&doc).unwrap();
    let mut keys: Vec<&str> = bson.keys().map(String::as_str).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "_id",
            "attempts",
            "createdAt",
            "failedAt",
            "headers",
            "key",
            "payload",
            "sentAt",
            "seq",
            "type"
        ]
    );
    assert!(
        matches!(bson.get("createdAt"), Some(Bson::DateTime(_))),
        "createdAt is a BSON date"
    );
    assert!(
        matches!(bson.get("payload"), Some(Bson::Document(p)) if p.get_str("id") == Ok("e-1") && p.get_str("at").is_ok())
    );
    assert_eq!(bson.get_str("type").unwrap(), "t.Happened");
    assert_eq!(bson.get_str("key").unwrap(), "k1");
    assert_eq!(bson.get_i64("seq").unwrap(), 42);
    assert_eq!(bson.get_i32("attempts").unwrap(), 0);
    assert_eq!(bson.get("sentAt"), Some(&Bson::Null));
    assert_eq!(bson.get("failedAt"), Some(&Bson::Null));
    let headers = bson.get_document("headers").unwrap();
    assert_eq!(
        headers.get_str("userId").unwrap(),
        "",
        "anonymous publisher writes an empty userId like Kotlin"
    );
    assert!(!headers.get_str("correlationId").unwrap().is_empty());
    assert!(uuid::Uuid::parse_str(bson.get_str("_id").unwrap()).is_ok());
}

#[test]
fn document_takes_correlation_and_user_from_context() {
    let ctx = MessageContext {
        correlation_id: "c-7".into(),
        identity: Some(Identity {
            user_id: Some("u-7".into()),
            roles: vec![],
            verified: true,
        }),
        headers: HashMap::from([("traceparent".to_string(), "00-abc-def-01".to_string())]),
        ..Default::default()
    };
    let doc = new_outbox_document(&event(), None, Some(&ctx), 1).unwrap();
    assert_eq!(doc.headers.get("correlationId").unwrap(), "c-7");
    assert_eq!(doc.headers.get("userId").unwrap(), "u-7");
    assert_eq!(doc.headers.get("traceparent").unwrap(), "00-abc-def-01");
    assert_eq!(doc.key, None);
}

fn registry_with_happened() -> HandlerRegistry {
    let mut r = HandlerRegistry::new();
    r.event::<Happened, _, _>(|_, _| async move { Ok(()) });
    r
}

fn hdrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn classify_skips_missing_type() {
    let d = classify(
        &hdrs(&[(headers::ID, "x")]),
        b"{}",
        "kraken.events",
        0,
        1,
        &registry_with_happened(),
    );
    assert!(matches!(d, RecordDisposition::Skip(reason) if reason.contains("type")));
}

#[test]
fn classify_skips_unhandled_type() {
    let d = classify(
        &hdrs(&[(headers::TYPE, "t.Other"), (headers::ID, "x")]),
        b"{}",
        "kraken.events",
        0,
        1,
        &registry_with_happened(),
    );
    assert!(matches!(d, RecordDisposition::Skip(_)));
}

#[test]
fn classify_skips_non_object_payload() {
    let d = classify(
        &hdrs(&[(headers::TYPE, "t.Happened"), (headers::ID, "x")]),
        b"[1,2]",
        "kraken.events",
        0,
        1,
        &registry_with_happened(),
    );
    assert!(matches!(d, RecordDisposition::Skip(reason) if reason.contains("object")));
}

#[test]
fn classify_uses_topic_partition_offset_when_id_missing() {
    let d = classify(
        &hdrs(&[(headers::TYPE, "t.Happened")]),
        b"{}",
        "kraken.events",
        3,
        17,
        &registry_with_happened(),
    );
    assert!(
        matches!(d, RecordDisposition::Handle { ref type_name, ref id } if type_name == "t.Happened" && id == "kraken.events-3-17")
    );
}

#[test]
fn classify_handles_known_type() {
    let d = classify(
        &hdrs(&[(headers::TYPE, "t.Happened"), (headers::ID, "e-9")]),
        br#"{"id":"e-9"}"#,
        "kraken.events",
        0,
        1,
        &registry_with_happened(),
    );
    assert!(
        matches!(d, RecordDisposition::Handle { ref type_name, ref id } if type_name == "t.Happened" && id == "e-9")
    );
}
