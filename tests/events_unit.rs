mod support;

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snodra_messaging::event_store::headers;
use snodra_messaging::events::{RecordDisposition, classify, new_outbox_entry};
use snodra_messaging::hub::HandlerRegistry;
use snodra_messaging::{Event, Identity, MessageContext, message_type};

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
fn entry_matches_the_outbox_shape() {
    let before = snodra_messaging::time::now_ms();
    let entry = new_outbox_entry(&event(), Some("k1"), None, 42).unwrap();
    assert_eq!(entry.type_name, "t.Happened");
    assert_eq!(entry.key.as_deref(), Some("k1"));
    assert_eq!(entry.seq, 42);
    assert_eq!(entry.attempts, 0);
    assert!(entry.created_at_ms >= before);
    let payload: serde_json::Value = serde_json::from_slice(&entry.payload).unwrap();
    assert!(
        payload["id"] == "e-1" && payload["at"].is_string(),
        "the event as a JSON object: {payload}"
    );
    assert_eq!(
        entry.headers.get("userId").map(String::as_str),
        Some(""),
        "anonymous publisher writes an empty userId"
    );
    assert!(!entry.headers["correlationId"].is_empty());
    assert!(uuid::Uuid::parse_str(&entry.id).is_ok());
}

#[test]
fn an_event_that_is_not_a_json_object_is_invalid() {
    #[derive(Serialize)]
    struct Unit;
    message_type!(Unit, "t.Unit");
    impl Event for Unit {}
    impl<'de> Deserialize<'de> for Unit {
        fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
            Ok(Unit)
        }
    }
    let e = new_outbox_entry(&Unit, None, None, 1).unwrap_err();
    assert_eq!(
        e.to_string(),
        "the payload of event t.Unit is null, not a JSON object, so it cannot be an outbox entry"
    );
}

#[test]
fn an_event_with_an_integer_above_i64_max_is_invalid() {
    #[derive(Serialize, Deserialize)]
    struct Big {
        n: u64,
    }
    message_type!(Big, "t.Big");
    impl Event for Big {}
    let e = new_outbox_entry(&Big { n: u64::MAX }, None, None, 1).unwrap_err();
    assert_eq!(
        e.to_string(),
        "the payload of event t.Big holds the integer 18446744073709551615, above i64::MAX, so it cannot be an outbox entry"
    );
    assert!(new_outbox_entry(&Big { n: i64::MAX as u64 }, None, None, 1).is_ok());
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
        headers: HashMap::from([("traceparent".to_string(), TRACEPARENT.to_string())]),
        ..Default::default()
    };
    let doc = new_outbox_entry(&event(), None, Some(&ctx), 1).unwrap();
    assert_eq!(doc.headers.get("correlationId").unwrap(), "c-7");
    assert_eq!(doc.headers.get("userId").unwrap(), "u-7");
    assert_eq!(doc.headers.get("traceparent").unwrap(), TRACEPARENT);
    assert_eq!(doc.key, None);
}

const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// The trace floor forwards only a valid W3C `traceparent` (version `00`, lower-case hex,
/// neither id all zeros), as snodra-messaging does: anything else is dropped.
#[test]
fn only_a_valid_w3c_traceparent_is_forwarded() {
    let invalid = [
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01", // upper case
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01", // trace id all zeros
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // parent id all zeros
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // another version
        "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",  // short trace id
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g", // not hex
        "garbage",
        "",
    ];
    for value in invalid {
        let ctx = MessageContext {
            headers: HashMap::from([("traceparent".to_string(), value.to_string())]),
            ..Default::default()
        };
        let doc = new_outbox_entry(&event(), None, Some(&ctx), 1).unwrap();
        assert_eq!(doc.headers.get("traceparent"), None, "{value:?}");
    }
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
        "snodra.events",
        "0",
        "1",
        &registry_with_happened(),
    );
    assert!(matches!(d, RecordDisposition::Skip(reason) if reason.contains("type")));
}

/// A blank (empty or whitespace-only) `type` header is no type: INVALID.
#[test]
fn classify_skips_a_blank_type() {
    for blank in ["", "  ", "\t"] {
        let d = classify(
            &hdrs(&[(headers::TYPE, blank), (headers::ID, "x")]),
            b"{}",
            "snodra.events",
            "0",
            "1",
            &registry_with_happened(),
        );
        assert!(
            matches!(d, RecordDisposition::Skip(reason) if reason == "record without type header"),
            "{blank:?}: {d:?}"
        );
    }
}

#[test]
fn classify_skips_unhandled_type() {
    let d = classify(
        &hdrs(&[(headers::TYPE, "t.Other"), (headers::ID, "x")]),
        b"{}",
        "snodra.events",
        "0",
        "1",
        &registry_with_happened(),
    );
    assert!(matches!(d, RecordDisposition::Skip(_)));
}

#[test]
fn classify_skips_non_object_payload() {
    let d = classify(
        &hdrs(&[(headers::TYPE, "t.Happened"), (headers::ID, "x")]),
        b"[1,2]",
        "snodra.events",
        "0",
        "1",
        &registry_with_happened(),
    );
    assert!(matches!(d, RecordDisposition::Skip(reason) if reason.contains("object")));
}

#[test]
fn classify_uses_source_segment_position_when_id_missing() {
    let d = classify(
        &hdrs(&[(headers::TYPE, "t.Happened")]),
        b"{}",
        "snodra.events",
        "3",
        "17",
        &registry_with_happened(),
    );
    assert!(
        matches!(d, RecordDisposition::Handle { ref type_name, ref id } if type_name == "t.Happened" && id == "snodra.events-3-17")
    );
}

/// A blank `id` header is no id: the fallback (as the letter's absent `eventId`
/// makes the retry use).
#[test]
fn classify_uses_source_segment_position_when_id_blank() {
    for blank in ["", "   "] {
        let d = classify(
            &hdrs(&[(headers::TYPE, "t.Happened"), (headers::ID, blank)]),
            b"{}",
            "snodra.events",
            "3",
            "17",
            &registry_with_happened(),
        );
        assert!(
            matches!(d, RecordDisposition::Handle { ref id, .. } if id == "snodra.events-3-17"),
            "{blank:?}: {d:?}"
        );
    }
}

#[test]
fn classify_handles_known_type() {
    let d = classify(
        &hdrs(&[(headers::TYPE, "t.Happened"), (headers::ID, "e-9")]),
        br#"{"id":"e-9"}"#,
        "snodra.events",
        "0",
        "1",
        &registry_with_happened(),
    );
    assert!(
        matches!(d, RecordDisposition::Handle { ref type_name, ref id } if type_name == "t.Happened" && id == "e-9")
    );
}
