use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snodra_messaging::{MessageContext, MessageType, codec, message_type};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Ping {
    text: String,
}
message_type!(Ping, "test.Ping");

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Stamped {
    at: DateTime<Utc>,
}

#[test]
fn message_type_macro_sets_const() {
    assert_eq!(Ping::TYPE, "test.Ping");
}

#[test]
fn decodes_jackson_instant_variants() {
    let nanos: Stamped = codec::decode(br#"{"at":"2026-10-30T00:49:45.227152891Z"}"#).unwrap();
    assert_eq!(nanos.at.timestamp_subsec_nanos(), 227_152_891);
    let plain: Stamped = codec::decode(br#"{"at":"2026-10-30T00:49:45Z"}"#).unwrap();
    assert_eq!(plain.at.timestamp(), nanos.at.timestamp());
    let offset: Stamped = codec::decode(br#"{"at":"2026-10-30T00:49:45.227+00:00"}"#).unwrap();
    assert_eq!(offset.at.timestamp_subsec_millis(), 227);
}

#[test]
fn encodes_instant_as_rfc3339_utc() {
    let v: Stamped = codec::decode(br#"{"at":"2026-10-30T00:49:45.227152891Z"}"#).unwrap();
    let out = String::from_utf8(codec::encode(&v).unwrap()).unwrap();
    assert_eq!(out, r#"{"at":"2026-10-30T00:49:45.227152891Z"}"#);
}

#[test]
fn is_json_object_rejects_arrays_and_scalars() {
    assert!(codec::is_json_object(br#"{"a":1}"#));
    assert!(codec::is_json_object(b" {} "));
    assert!(!codec::is_json_object(b"[1]"));
    assert!(!codec::is_json_object(b"42"));
    assert!(!codec::is_json_object(b"not json"));
}

#[test]
fn debug_output_redacts_bearer_tokens() {
    let ctx = MessageContext {
        authorization: Some("Bearer super-secret-token".into()),
        headers: std::collections::HashMap::from([
            (
                "authorization".to_string(),
                "Bearer super-secret-token".to_string(),
            ),
            ("correlationId".to_string(), "c-1".to_string()),
        ]),
        ..Default::default()
    };
    let printed = format!("{ctx:?}");
    assert!(!printed.contains("super-secret-token"), "{printed}");
    assert!(printed.contains("c-1"));
}
