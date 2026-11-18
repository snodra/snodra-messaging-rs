//! What a record looks like to the event browser: a summary in a search result or a tail (never
//! a payload), the full record of `snodra.events.Get` (every header, the payload when asked), and
//! the bounds of both.

use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};

use crate::event_store::headers;

/// A summary's key is cut to this many bytes (at a character boundary).
pub const MAX_KEY: usize = 128;
/// A summary's header values are cut to this many bytes.
pub const MAX_SUMMARY_HEADER: usize = 256;
/// A record lists at most this many headers...
pub const MAX_HEADERS: usize = 64;
/// ...names cut to this many bytes...
pub const MAX_HEADER_NAME: usize = 128;
/// ...and values to this many.
pub const MAX_HEADER_VALUE: usize = 1024;

/// `bytes` as text: valid UTF-8 as it is, every byte that is not part of valid UTF-8 as `\xNN`.
pub fn escaped(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for chunk in bytes.utf8_chunks() {
        out.push_str(chunk.valid());
        for b in chunk.invalid() {
            out.push_str(&format!("\\x{b:02x}"));
        }
    }
    out
}

/// The longest prefix of `s` of at most `max` bytes that ends at a character boundary, and
/// whether anything was cut.
pub fn cut(s: &str, max: usize) -> (&str, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

/// Lowercase hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// One stored event as the browser reads it, borrowed from the event store's client: the bytes
/// as stored, which need not be what the libraries write (any client may write to a source).
#[derive(Debug, Clone, Copy)]
pub struct RecordView<'a> {
    /// When the event store appended it (or the producer stamped it), epoch milliseconds; `None`
    /// when it has no time.
    pub timestamp: Option<i64>,
    pub key: Option<&'a [u8]>,
    /// Every header in stored order: name and value (`None`: a null value).
    pub headers: &'a [(&'a [u8], Option<&'a [u8]>)],
    pub payload: Option<&'a [u8]>,
}

impl<'a> RecordView<'a> {
    /// The value of the last header named `name`, when it has one.
    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        self.headers
            .iter()
            .rev()
            .find(|(n, _)| *n == name.as_bytes())
            .and_then(|(_, v)| *v)
    }
}

/// An event in a search result or a tail: where it is, its key, the library headers and the
/// payload's size. Never the payload.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventSummary {
    /// The source it is in.
    pub source: String,
    /// Its segment and its position there (opaque, at most 128 bytes each).
    pub segment: String,
    pub position: String,
    /// Epoch milliseconds; `null` when the event has no time.
    pub timestamp: Option<i64>,
    /// The key as text ([`escaped`]), cut to [`MAX_KEY`] bytes; `null` without a key.
    pub key: Option<String>,
    /// The key was cut (absent when not).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub key_truncated: bool,
    /// The library headers the event carries ([`headers::ALL`]), as text ([`escaped`]) cut to
    /// [`MAX_SUMMARY_HEADER`] bytes.
    pub headers: BTreeMap<String, String>,
    /// Payload bytes (0 without a payload).
    pub size: u64,
}

impl EventSummary {
    /// The summary of `r`, at `position` of `segment` of `source`.
    pub fn of(source: &str, segment: &str, position: &str, r: &RecordView<'_>) -> Self {
        let (key, key_truncated) = match r.key {
            Some(k) => {
                let text = escaped(k);
                let (shown, cut_off) = cut(&text, MAX_KEY);
                (Some(shown.to_string()), cut_off)
            }
            None => (None, false),
        };
        let headers = headers::ALL
            .iter()
            .filter_map(|name| {
                let text = escaped(r.header(name)?);
                Some((
                    (*name).to_string(),
                    cut(&text, MAX_SUMMARY_HEADER).0.to_string(),
                ))
            })
            .collect();
        Self {
            source: source.to_string(),
            segment: segment.to_string(),
            position: position.to_string(),
            timestamp: r.timestamp,
            key,
            key_truncated,
            headers,
            size: r.payload.map_or(0, |p| p.len() as u64),
        }
    }
}

/// One header of a record.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HeaderView {
    /// The name as text ([`escaped`]), cut to [`MAX_HEADER_NAME`] bytes.
    pub name: String,
    /// The value when it is UTF-8 (cut to [`MAX_HEADER_VALUE`] bytes); `null` for a null value or
    /// one that is not UTF-8.
    pub value: Option<String>,
    /// The value in hex when it is not UTF-8 (its first [`MAX_HEADER_VALUE`] bytes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_hex: Option<String>,
    /// The name or the value was cut (absent when not).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

/// One event as `snodra.events.Get` answers it: the summary, every header, and the payload when
/// it was asked for.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventRecord {
    #[serde(flatten)]
    pub summary: EventSummary,
    /// Every header in stored order, at most [`MAX_HEADERS`].
    pub all_headers: Vec<HeaderView>,
    /// Headers beyond [`MAX_HEADERS`], not listed (absent when none).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub headers_omitted: u64,
    /// Only when the payload was asked for: its text (UTF-8, cut to the browser's payload bound),
    /// or `null` for an event without a payload (a tombstone). Absent when it was not asked for,
    /// and when it is not UTF-8 (then `payload_base64` carries it).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "present_or_null"
    )]
    pub payload: Option<Option<String>>,
    /// The payload in standard base64 (its first bytes up to the payload bound) when it was asked
    /// for and is not UTF-8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,
    /// The payload was cut at the payload bound (absent when not).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub payload_truncated: bool,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// `Some(None)` as `null`, `Some(Some(text))` as the text; `None` is left out by the field's
/// `skip_serializing_if` (and read back by `default`).
mod present_or_null {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Option<String>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(inner) => inner.serialize(s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<Option<String>>, D::Error> {
        Option::<String>::deserialize(d).map(Some)
    }
}

impl EventRecord {
    /// The record of `r` at `position` of `segment` of `source`; with `payload` set, its payload
    /// cut to that many bytes, else none.
    pub fn of(
        source: &str,
        segment: &str,
        position: &str,
        r: &RecordView<'_>,
        payload: Option<usize>,
    ) -> Self {
        let all_headers = r
            .headers
            .iter()
            .take(MAX_HEADERS)
            .map(|(name, value)| {
                let name = escaped(name);
                let (name, name_cut) = cut(&name, MAX_HEADER_NAME);
                let (value, value_hex, value_cut) = match value {
                    None => (None, None, false),
                    Some(v) => match std::str::from_utf8(v) {
                        Ok(text) => {
                            let (shown, cut_off) = cut(text, MAX_HEADER_VALUE);
                            (Some(shown.to_string()), None, cut_off)
                        }
                        Err(_) => {
                            let shown = &v[..v.len().min(MAX_HEADER_VALUE)];
                            (None, Some(hex(shown)), v.len() > MAX_HEADER_VALUE)
                        }
                    },
                };
                HeaderView {
                    name: name.to_string(),
                    value,
                    value_hex,
                    truncated: name_cut || value_cut,
                }
            })
            .collect();
        let mut record = Self {
            summary: EventSummary::of(source, segment, position, r),
            all_headers,
            headers_omitted: r.headers.len().saturating_sub(MAX_HEADERS) as u64,
            payload: None,
            payload_base64: None,
            payload_truncated: false,
        };
        let Some(max) = payload else {
            return record;
        };
        match r.payload {
            None => record.payload = Some(None),
            Some(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => {
                    let (shown, cut_off) = cut(text, max);
                    record.payload = Some(Some(shown.to_string()));
                    record.payload_truncated = cut_off;
                }
                Err(_) => {
                    record.payload_base64 = Some(STANDARD.encode(&bytes[..bytes.len().min(max)]));
                    record.payload_truncated = bytes.len() > max;
                }
            },
        }
        record
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn view<'a>(
        key: Option<&'a [u8]>,
        headers: &'a [(&'a [u8], Option<&'a [u8]>)],
        payload: Option<&'a [u8]>,
    ) -> RecordView<'a> {
        RecordView {
            timestamp: Some(1_000),
            key,
            headers,
            payload,
        }
    }

    const MAX_PAYLOAD: usize = 256 * 1024;

    #[test]
    fn escaped_keeps_utf8_and_escapes_every_other_byte() {
        assert_eq!(escaped(b"order-1"), "order-1");
        assert_eq!(escaped("caf\u{e9}".as_bytes()), "caf\u{e9}");
        assert_eq!(escaped(b"a\xff\xfeb"), "a\\xff\\xfeb");
        assert_eq!(escaped(b"\xc3"), "\\xc3", "a truncated sequence");
    }

    #[test]
    fn cut_stops_at_a_character_boundary() {
        assert_eq!(cut("abc", 3), ("abc", false));
        assert_eq!(cut("abcd", 3), ("abc", true));
        // "é" is two bytes: a cut in its middle keeps it out.
        assert_eq!(cut("a\u{e9}", 2), ("a", true));
        assert_eq!(cut("", 0), ("", false));
    }

    #[test]
    fn a_summary_cuts_the_key_and_headers_and_never_carries_the_payload() {
        let long_type = "t".repeat(300);
        let key = vec![b'k'; 200];
        let headers: &[(&[u8], Option<&[u8]>)] = &[
            (b"type", Some(long_type.as_bytes())),
            (b"x-custom", Some(b"not listed")),
            (b"source", Some(b"svc")),
        ];
        let s = EventSummary::of(
            "snodra.events",
            "3",
            "42",
            &view(Some(&key), headers, Some(b"secret")),
        );
        assert_eq!((s.segment.as_str(), s.position.as_str()), ("3", "42"));
        assert_eq!(s.key.as_deref().map(str::len), Some(MAX_KEY));
        assert!(s.key_truncated);
        assert_eq!(s.headers["type"].len(), MAX_SUMMARY_HEADER);
        assert_eq!(s.headers["source"], "svc");
        assert!(!s.headers.contains_key("x-custom"));
        assert_eq!(s.size, 6);
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("secret"), "{json}");
        assert!(json.contains("\"keyTruncated\":true"), "{json}");
        assert_eq!(serde_json::from_str::<EventSummary>(&json).unwrap(), s);
        let short = EventSummary::of("t", "0", "0", &view(Some(b"k"), &[], None));
        let json = serde_json::to_string(&short).unwrap();
        assert!(!json.contains("keyTruncated"), "{json}");
        assert_eq!(short.size, 0);
        assert_eq!(serde_json::from_str::<EventSummary>(&json).unwrap(), short);
    }

    #[test]
    fn a_record_bounds_every_header_and_carries_the_payload_only_when_asked() {
        let long_name = "n".repeat(200);
        let long_value = "v".repeat(2000);
        let mut headers: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"type", Some(b"a.B")),
            (long_name.as_bytes(), Some(long_value.as_bytes())),
            (b"bin", Some(b"\x00\xff")),
            (b"\xffname", None),
        ];
        for _ in 0..70 {
            headers.push((b"filler", Some(b"x")));
        }
        let r = view(None, &headers, Some(b"{\"a\":1}"));
        let d = EventRecord::of("snodra.events", "1", "2", &r, Some(MAX_PAYLOAD));
        assert_eq!(d.all_headers.len(), MAX_HEADERS);
        assert_eq!(d.headers_omitted as usize, headers.len() - MAX_HEADERS);
        let long = &d.all_headers[1];
        assert_eq!(
            (
                long.name.len(),
                long.value.as_ref().unwrap().len(),
                long.truncated
            ),
            (MAX_HEADER_NAME, MAX_HEADER_VALUE, true)
        );
        assert_eq!(
            (
                &d.all_headers[2].value,
                d.all_headers[2].value_hex.as_deref()
            ),
            (&None, Some("00ff"))
        );
        assert_eq!(d.all_headers[3].name, "\\xffname");
        assert_eq!(d.payload, Some(Some("{\"a\":1}".into())));
        let json = serde_json::to_value(&d).unwrap();
        assert_eq!(json["payload"], "{\"a\":1}");
        assert_eq!(json["headers"]["type"], "a.B");
        assert_eq!(json["segment"], "1");
        assert_eq!(serde_json::from_value::<EventRecord>(json).unwrap(), d);

        let unasked = EventRecord::of("snodra.events", "1", "2", &r, None);
        let json = serde_json::to_value(&unasked).unwrap();
        assert!(
            json.get("payload").is_none() && json.get("payloadBase64").is_none(),
            "{json}"
        );
        assert!(!json.to_string().contains("\\\"a\\\":1"), "{json}");
        assert_eq!(
            serde_json::from_value::<EventRecord>(json).unwrap(),
            unasked
        );

        let big = "p".repeat(MAX_PAYLOAD + 10);
        let d = EventRecord::of(
            "t",
            "0",
            "0",
            &view(None, &[], Some(big.as_bytes())),
            Some(MAX_PAYLOAD),
        );
        assert!(d.payload_truncated);
        assert_eq!(d.payload, Some(Some("p".repeat(MAX_PAYLOAD))));
        let small = EventRecord::of("t", "0", "0", &view(None, &[], Some(b"abcdef")), Some(4));
        assert_eq!(
            (small.payload, small.payload_truncated),
            (Some(Some("abcd".into())), true),
            "the bound is the caller's"
        );
        let d = EventRecord::of(
            "t",
            "0",
            "0",
            &view(None, &[], Some(b"\xff\x00")),
            Some(MAX_PAYLOAD),
        );
        let json = serde_json::to_value(&d).unwrap();
        assert!(json.get("payload").is_none(), "{json}");
        assert_eq!(json["payloadBase64"], "/wA=");
        let d = EventRecord::of("t", "0", "0", &view(None, &[], None), Some(MAX_PAYLOAD));
        let json = serde_json::to_value(&d).unwrap();
        assert!(
            json["payload"].is_null() && json.get("payload").is_some(),
            "{json}"
        );
        assert_eq!(serde_json::from_value::<EventRecord>(json).unwrap(), d);
    }
}
