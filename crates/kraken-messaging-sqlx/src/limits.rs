//! The bounds of the store's columns (schema version 1), the same on both databases and in both
//! libraries (kraken-messaging-jdbc's `Limits`): a value over its bound is refused before anything
//! is written, with an `InvalidMessage` naming the field and its length (never the value: it may
//! be personal data); the database never truncates. Lengths are in characters (Unicode code
//! points), as `VARCHAR(n)` counts them on both databases.
//!
//! Values without a bound (marker ids, idempotency keys, saga start keys, record keys,
//! associations) are found through the SHA-256 of their UTF-8 bytes ([`sha256`]) and kept whole in
//! a text column. Free text (error messages) is cut to [`FREE_TEXT_BYTES`] bytes of UTF-8
//! ([`cut`]).

use std::collections::{BTreeMap, HashMap};

use kraken_messaging::error::MessagingError;
use kraken_messaging::store::StoreError;
use sha2::{Digest, Sha256};

/// An event processor's name.
pub(crate) const PROCESSOR: usize = 64;
/// A source (a Kafka topic name is at most 249).
pub(crate) const SOURCE: usize = 255;
/// A segment of a source (a Kafka partition number).
pub(crate) const SEGMENT: usize = 128;
/// A position in a segment (a Kafka offset is at most 19 digits).
pub(crate) const POSITION: usize = 128;
/// A type name: an event or message type, a saga type, a deadline name.
pub(crate) const NAME: usize = 255;
/// An id the library builds: an outbox entry's, a saga's, a saga-dispatch entry's, a deadline's.
pub(crate) const ID: usize = 255;
/// A dead letter's id `<processor>:<source>:<segment>:<position>`: the longest those bounds build.
pub(crate) const DEAD_LETTER_ID: usize = PROCESSOR + SOURCE + SEGMENT + POSITION + 3;
/// Free text (an error message, a send's last error) is cut to this many bytes of UTF-8.
pub(crate) const FREE_TEXT_BYTES: usize = 16 * 1024;

fn invalid(message: String) -> StoreError {
    StoreError::Failed(MessagingError::InvalidMessage(message))
}

/// Refuses `value` longer than `max` characters, or holding U+0000, naming `field`.
pub(crate) fn check(field: &str, value: &str, max: usize) -> Result<(), StoreError> {
    let length = value.chars().count();
    if length > max {
        return Err(invalid(format!(
            "The message store keeps {field} of at most {max} characters; this one has {length}"
        )));
    }
    refuse_nul(field, value)
}

/// [`check`] of a value that may be absent.
pub(crate) fn check_opt(field: &str, value: Option<&str>, max: usize) -> Result<(), StoreError> {
    value.map_or(Ok(()), |v| check(field, v, max))
}

/// Refuses the character U+0000 in `value`, naming `field`: PostgreSQL cannot store it in text
/// or JSON, so neither database is given one (the same refusal on both).
pub(crate) fn refuse_nul(field: &str, value: &str) -> Result<(), StoreError> {
    if value.contains('\0') {
        return Err(invalid(format!(
            "The message store refuses the character U+0000 in {field}"
        )));
    }
    Ok(())
}

/// `text` cut to at most [`FREE_TEXT_BYTES`] bytes of UTF-8, never inside a character.
pub(crate) fn cut(text: &str) -> String {
    if text.len() <= FREE_TEXT_BYTES {
        return text.to_string();
    }
    let mut end = FREE_TEXT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

/// The lowercase hex SHA-256 of `value`'s UTF-8 bytes.
pub(crate) fn sha256(value: &str) -> String {
    hex(&Sha256::digest(value.as_bytes()))
}

/// The hash of an association: SHA-256 of `key`'s UTF-8 bytes, one zero byte, `value`'s.
pub(crate) fn association_hash(key: &str, value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(key.as_bytes());
    digest.update([0u8]);
    digest.update(value.as_bytes());
    hex(&digest.finalize())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A position (or segment) as the number it stands for when it is a canonical decimal one (the
/// `position_num` column), else `None`: ordered by value.
pub(crate) fn position_number(position: &str) -> Option<i64> {
    position
        .parse::<i64>()
        .ok()
        .filter(|n| n.to_string() == position)
}

/// A header map as the JSON object of strings the `headers` columns hold; U+0000 in a key or a
/// value refused, naming `field`.
pub(crate) fn headers_json<'a>(
    field: &str,
    headers: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> Result<String, StoreError> {
    let map: BTreeMap<&String, &String> = headers.into_iter().collect();
    for (k, v) in &map {
        refuse_nul(field, k)?;
        refuse_nul(field, v)?;
    }
    Ok(serde_json::to_string(&map)?)
}

/// A JSON object of the `headers` columns as strings (a value that is not a string as its JSON
/// text); nothing for no text.
pub(crate) fn strings(text: Option<&str>) -> Result<HashMap<String, String>, StoreError> {
    let Some(text) = text else {
        return Ok(HashMap::new());
    };
    let json: serde_json::Value = serde_json::from_str(text)?;
    Ok(match json {
        serde_json::Value::Object(fields) => fields
            .into_iter()
            .map(|(k, v)| match v {
                serde_json::Value::String(s) => (k, s),
                other => (k, other.to_string()),
            })
            .collect(),
        _ => HashMap::new(),
    })
}

/// Refuses `payload` with `InvalidMessage` naming the event `type_name` unless it keeps the outbox
/// contract ([`kraken_messaging::store::check_outbox_payload`]: a JSON object whose integers fit
/// in a signed 64-bit integer, read from the text), with no U+0000 in its strings or keys (written
/// as an escape, which PostgreSQL's `jsonb` refuses).
pub(crate) fn require_object(type_name: &str, payload: &[u8]) -> Result<(), StoreError> {
    kraken_messaging::store::check_outbox_payload(type_name, payload)?;
    let json: serde_json::Value = serde_json::from_slice(payload)?;
    if has_nul(&json) {
        return Err(invalid(format!(
            "The message store refuses the character U+0000 in the payload of event {type_name}"
        )));
    }
    Ok(())
}

fn has_nul(json: &serde_json::Value) -> bool {
    match json {
        serde_json::Value::String(s) => s.contains('\0'),
        serde_json::Value::Array(items) => items.iter().any(has_nul),
        serde_json::Value::Object(fields) => {
            fields.iter().any(|(k, v)| k.contains('\0') || has_nul(v))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The golden values both libraries compute (kraken-messaging-jdbc's `Limits.sha256` and
    /// `Limits.associationHash`), taken from an independent SHA-256 (`printf '...' | shasum -a
    /// 256`): a marker id, an association (key, a zero byte, value), a start key with the
    /// runtime's escaping, and multi-byte text.
    #[test]
    fn hashes_are_the_lowercase_hex_sha256_of_the_utf8_bytes() {
        assert_eq!(
            sha256(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256("processor:default:e-1"),
            "24ca69a5b6e5e53de85d1bd8f13651ed593283ecfe1638c5eb04d81bafade4ff"
        );
        assert_eq!(
            association_hash("orderId", "o-1"),
            "97939626adbe4b238abf09acf51868ad89b5022cd2617dfad599d472374fc598"
        );
        assert_eq!(
            sha256("conformance.Order|orderId=o-1"),
            "83dc8dac44387920c04b298f2fa8a5685646f6b823ef12b3aefc8265260eb350"
        );
        assert_eq!(
            sha256("rt.Escape%7Ca%25b|k%3Dx=1"),
            "e8d25fa2667a9f810c301d584faf8b3289e1404248387d74afb11a61f65c7bce"
        );
        assert_eq!(
            sha256("é😀\0x"),
            "e8d673369c826a11be0cdbdaa662a86fc7edc0320aa1ccdd09b852aad8b400ca"
        );
    }

    #[test]
    fn positions_are_numbers_only_when_canonical_decimals() {
        assert_eq!(position_number("41"), Some(41));
        assert_eq!(position_number("-3"), Some(-3));
        for odd in ["007", "+1", "", "x7", "99999999999999999999", " 1"] {
            assert_eq!(position_number(odd), None, "{odd}");
        }
    }

    #[test]
    fn bounds_count_characters_and_name_the_field_not_the_value() {
        assert!(check("a processor's name", &"é".repeat(64), PROCESSOR).is_ok());
        assert!(check("a processor's name", &"😀".repeat(64), PROCESSOR).is_ok());
        let refused = check("a processor's name", &"😀".repeat(65), PROCESSOR).unwrap_err();
        let StoreError::Failed(MessagingError::InvalidMessage(message)) = refused else {
            panic!("{refused:?}");
        };
        assert_eq!(
            message,
            "The message store keeps a processor's name of at most 64 characters; this one has 65"
        );
        assert!(check("an id", "a\0b", ID).is_err());
        assert_eq!(DEAD_LETTER_ID, 578);
    }

    #[test]
    fn free_text_is_cut_to_16_kib_on_a_character_boundary() {
        assert_eq!(cut("short"), "short");
        let ascii = "a".repeat(FREE_TEXT_BYTES + 10);
        assert_eq!(cut(&ascii).len(), FREE_TEXT_BYTES);
        // é is 2 bytes: 16 384 bytes hold 8 192 of them exactly; a leading "a" pushes the last
        // one over the edge, so it goes whole.
        let accents = format!("a{}", "é".repeat(FREE_TEXT_BYTES / 2));
        let cut_text = cut(&accents);
        assert_eq!(cut_text.len(), FREE_TEXT_BYTES - 1);
        assert!(cut_text.ends_with('é'));
        let emoji = "😀".repeat(FREE_TEXT_BYTES / 4 + 1);
        assert_eq!(cut(&emoji).len(), FREE_TEXT_BYTES);
    }

    #[test]
    fn outbox_payloads_must_be_json_objects_of_i64_integers_without_nul() {
        assert!(require_object("t.E", br#"{"n":9223372036854775807}"#).is_ok());
        assert!(
            require_object("t.E", br#"{"s":"123456789012345678901234567890","d":1.5}"#).is_ok()
        );
        for bad in [
            &b"not json"[..],
            b"[1]",
            b"null",
            br#"{"n":18446744073709551615}"#,
            br#"{"n":-9223372036854775809}"#,
            br#"{"n":18446744073709551616}"#,
            br#"{"a":[{"n":123456789012345678901234567890}]}"#,
            br#"{"s":"a\u0000b"}"#,
            br#"{"a\u0000":1}"#,
        ] {
            assert!(
                matches!(
                    require_object("t.E", bad),
                    Err(StoreError::Failed(MessagingError::InvalidMessage(_)))
                ),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn headers_are_a_json_object_of_strings_read_back_as_written() {
        let headers = HashMap::from([
            ("traceparent".to_string(), "00-x".to_string()),
            ("userId".to_string(), String::new()),
        ]);
        let json = headers_json("headers", &headers).unwrap();
        assert_eq!(json, r#"{"traceparent":"00-x","userId":""}"#);
        assert_eq!(strings(Some(&json)).unwrap(), headers);
        assert_eq!(
            strings(Some(r#"{"n":1,"s":"x"}"#)).unwrap(),
            HashMap::from([("n".into(), "1".into()), ("s".into(), "x".into())])
        );
        let nul = HashMap::from([("k".to_string(), "a\0".to_string())]);
        assert!(headers_json("headers", &nul).is_err());
    }
}
