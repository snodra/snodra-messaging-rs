//! What an event must match to be found: the filters of a search and of a tail, as sent
//! ([`EventFilter`]) and as checked ([`Filters`]).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::records::{RecordView, escaped};
use super::{BrowseError, quoted};

/// A filter value (and `q`) is at most this many bytes.
pub const MAX_FILTER: usize = 1024;

/// A time of `from` or `to`: epoch milliseconds as a number, or as text either epoch
/// milliseconds (digits only) or an RFC 3339 date-time (`2026-11-05T10:00:00Z`,
/// `2026-11-05T10:00:00.250+01:00`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum TimeArg {
    Millis(i64),
    Text(String),
}

/// The filters of a search or a tail, as sent: every field optional, an empty text the same as
/// an absent one (a form sends every field). Every set filter must hold.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventFilter {
    /// The event's time is at least this...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<TimeArg>,
    /// ...and at most this. A tail takes neither.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<TimeArg>,
    /// Header `type`: exact, or a prefix when it ends with `*` (`orders.*`; `*` alone: any event
    /// with a type).
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    /// The key, exactly as a summary shows it untruncated ([`escaped`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Header `source` (the publishing service), exactly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Header `correlationId`, exactly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    /// Header `userId`, exactly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    /// Header `id` (the event's id), exactly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// A case-sensitive byte substring of the payload. The browser searches payloads when asked;
    /// whether the caller may is the hub's rule (ADR 0025).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
}

/// The `type` filter: exact, or a prefix when it ends with `*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeFilter {
    Exact(String),
    Prefix(String),
}

/// What an event must match ([`EventFilter`] checked); every set field must hold.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filters {
    /// The event's time is at least this (epoch ms)...
    pub from: Option<i64>,
    /// ...and at most this.
    pub to: Option<i64>,
    pub type_name: Option<TypeFilter>,
    /// The key as [`escaped`] shows it, untruncated: the bytes for a UTF-8 key.
    pub key: Option<String>,
    /// Headers `source`, `correlationId`, `userId`, `id`: the value as [`escaped`] shows it.
    pub source: Option<String>,
    pub correlation_id: Option<String>,
    pub user_id: Option<String>,
    pub event_id: Option<String>,
    /// A substring of the payload bytes (case-sensitive).
    pub q: Option<String>,
}

fn present(v: &Option<String>) -> Option<&str> {
    v.as_deref().filter(|s| !s.is_empty())
}

impl Filters {
    /// The filters of `filter`; [`BrowseError::Invalid`] with the reason. `tail` refuses `from`
    /// and `to` (a tail starts at the end).
    pub fn parse(filter: &EventFilter, tail: bool) -> Result<Self, BrowseError> {
        let time = |name: &str, v: &Option<TimeArg>| -> Result<Option<i64>, BrowseError> {
            match v {
                None => Ok(None),
                Some(TimeArg::Millis(ms)) => Ok(Some(*ms)),
                Some(TimeArg::Text(s)) if s.is_empty() => Ok(None),
                Some(TimeArg::Text(s)) if s.len() > MAX_FILTER => Err(BrowseError::Invalid(
                    format!("{name} is longer than {MAX_FILTER} bytes"),
                )),
                Some(TimeArg::Text(s)) => parse_time(s)
                    .map(Some)
                    .map_err(|e| BrowseError::Invalid(format!("{name}: {e}"))),
            }
        };
        let filters_from = time("from", &filter.from)?;
        let filters_to = time("to", &filter.to)?;
        if tail {
            for (name, v) in [("from", filters_from), ("to", filters_to)] {
                if v.is_some() {
                    return Err(BrowseError::Invalid(format!(
                        "a tail takes no {name}: it starts at the end"
                    )));
                }
            }
        }
        let text = |name: &str, v: &Option<String>| -> Result<Option<String>, BrowseError> {
            match present(v) {
                Some(s) if s.len() > MAX_FILTER => Err(BrowseError::Invalid(format!(
                    "{name} is longer than {MAX_FILTER} bytes"
                ))),
                other => Ok(other.map(str::to_string)),
            }
        };
        let filters = Self {
            from: filters_from,
            to: filters_to,
            type_name: text("type", &filter.type_name)?.map(|t| match t.strip_suffix('*') {
                Some(prefix) => TypeFilter::Prefix(prefix.to_string()),
                None => TypeFilter::Exact(t),
            }),
            key: text("key", &filter.key)?,
            source: text("source", &filter.source)?,
            correlation_id: text("correlationId", &filter.correlation_id)?,
            user_id: text("userId", &filter.user_id)?,
            event_id: text("eventId", &filter.event_id)?,
            q: text("q", &filter.q)?,
        };
        if let (Some(from), Some(to)) = (filters.from, filters.to)
            && from > to
        {
            return Err(BrowseError::Invalid("from is after to".to_string()));
        }
        Ok(filters)
    }

    /// True when `r` matches every filter. A header is matched by its last value; an absent or
    /// null header matches no value; an event without a time counts as -1 (before any `from`).
    pub fn matches(&self, r: &RecordView<'_>) -> bool {
        let ts = r.timestamp.unwrap_or(-1);
        if self.from.is_some_and(|from| ts < from) || self.to.is_some_and(|to| ts > to) {
            return false;
        }
        let equal = |want: &Option<String>, got: Option<&[u8]>| match want {
            None => true,
            Some(want) => got.is_some_and(|g| {
                g == want.as_bytes() || (std::str::from_utf8(g).is_err() && escaped(g) == *want)
            }),
        };
        if !equal(&self.key, r.key)
            || !equal(&self.source, r.header("source"))
            || !equal(&self.correlation_id, r.header("correlationId"))
            || !equal(&self.user_id, r.header("userId"))
            || !equal(&self.event_id, r.header("id"))
        {
            return false;
        }
        if let Some(t) = &self.type_name {
            let Some(got) = r.header("type") else {
                return false;
            };
            let ok = match t {
                TypeFilter::Exact(want) => got == want.as_bytes(),
                TypeFilter::Prefix(prefix) => got.starts_with(prefix.as_bytes()),
            };
            if !ok {
                return false;
            }
        }
        match &self.q {
            None => true,
            // Never empty: an empty `q` is no filter.
            Some(q) => r
                .payload
                .is_some_and(|p| p.windows(q.len()).any(|w| w == q.as_bytes())),
        }
    }

    /// A digest of `source` and the filters (16 hex digits), which a cursor carries: a cursor
    /// only continues the search it came from, on whichever browser instance reads it. Every
    /// value is framed by its length, so no value can pass for another field.
    pub fn digest(&self, source: &str) -> String {
        let mut text = String::new();
        let mut put = |name: &str, v: Option<&str>| {
            text.push_str(name);
            match v {
                Some(v) => text.push_str(&format!("={}:{v};", v.len())),
                None => text.push_str("-;"),
            }
        };
        put("source", Some(source));
        put("from", self.from.map(|v| v.to_string()).as_deref());
        put("to", self.to.map(|v| v.to_string()).as_deref());
        let type_name = self.type_name.as_ref().map(|t| match t {
            TypeFilter::Exact(t) => format!("exact:{t}"),
            TypeFilter::Prefix(p) => format!("prefix:{p}"),
        });
        put("type", type_name.as_deref());
        put("key", self.key.as_deref());
        put("sourceHeader", self.source.as_deref());
        put("correlationId", self.correlation_id.as_deref());
        put("userId", self.user_id.as_deref());
        put("eventId", self.event_id.as_deref());
        put("q", self.q.as_deref());
        let digest = Sha256::digest(text.as_bytes());
        digest[..8].iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Epoch milliseconds (digits only) or an RFC 3339 date-time (`2026-11-05T10:00:00Z`,
/// `2026-11-05T10:00:00.250+01:00`).
pub fn parse_time(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        return s
            .parse::<i64>()
            .map_err(|_| format!("{} is too large", quoted(s)));
    }
    rfc3339(s).ok_or_else(|| {
        format!(
            "{} is neither epoch milliseconds nor an RFC 3339 time",
            quoted(s)
        )
    })
}

fn rfc3339(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let digits = |from: usize, len: usize| -> Option<i64> {
        let part = b.get(from..from + len)?;
        part.iter()
            .all(u8::is_ascii_digit)
            .then(|| std::str::from_utf8(part).ok()?.parse().ok())?
    };
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't' | b' ') {
        return None;
    }
    if b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (year, month, day) = (digits(0, 4)?, digits(5, 2)?, digits(8, 2)?);
    let (hour, minute, second) = (digits(11, 2)?, digits(14, 2)?, digits(17, 2)?);
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&month)
        || day < 1
        || day > month_days[(month - 1) as usize]
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let mut i = 19;
    let mut millis = 0;
    if b[i] == b'.' {
        let start = i + 1;
        i = start;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return None;
        }
        let frac = &s[start..i.min(start + 3)];
        millis = frac.parse::<i64>().ok()? * 10i64.pow(3 - frac.len() as u32);
    }
    let offset_minutes = match &b[i..] {
        [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), _, _, b':', _, _] => {
            let (h, m) = (digits(i + 1, 2)?, digits(i + 4, 2)?);
            if h > 23 || m > 59 {
                return None;
            }
            let minutes = h * 60 + m;
            if *sign == b'-' { -minutes } else { minutes }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3600 + minute * 60 + second.min(59) - offset_minutes * 60;
    Some(seconds * 1000 + millis)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant's algorithm).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view<'a>(
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

    /// The filter of these JSON fields (all text, as a form sends them).
    fn filter(pairs: &[(&str, &str)]) -> EventFilter {
        let value: serde_json::Map<String, serde_json::Value> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::from_value(serde_json::Value::Object(value)).unwrap()
    }

    #[test]
    fn times_are_epoch_millis_or_rfc3339() {
        assert_eq!(parse_time("1730000000000"), Ok(1_730_000_000_000));
        assert_eq!(parse_time("1970-01-01T00:00:00Z"), Ok(0));
        assert_eq!(parse_time("2026-11-05T10:00:00Z"), Ok(1_793_872_800_000));
        assert_eq!(
            parse_time("2026-11-05T11:00:00.25+01:00"),
            Ok(1_793_872_800_250)
        );
        assert_eq!(
            parse_time("2026-11-05t09:30:00.123456-00:30"),
            Ok(1_793_872_800_123)
        );
        assert_eq!(parse_time("2024-02-29T00:00:00Z"), Ok(1_709_164_800_000));
        for bad in [
            "",
            "yesterday",
            "2026-11-05",
            "2026-11-05T10:00:00",
            "2026-13-01T00:00:00Z",
            "2026-02-29T00:00:00Z",
            "2026-11-05T24:00:00Z",
            "2026-11-05T10:00:00.Z",
            "2026-11-05T10:00:00+1:00",
            "-5",
            "99999999999999999999",
        ] {
            assert!(parse_time(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn times_come_as_numbers_or_text() {
        let f: EventFilter =
            serde_json::from_str(r#"{"from": 1000, "to": "1970-01-01T00:00:02Z"}"#).unwrap();
        assert_eq!(f.from, Some(TimeArg::Millis(1000)));
        let parsed = Filters::parse(&f, false).unwrap();
        assert_eq!((parsed.from, parsed.to), (Some(1000), Some(2000)));
        assert_eq!(
            serde_json::to_value(&f).unwrap(),
            serde_json::json!({"from": 1000, "to": "1970-01-01T00:00:02Z"})
        );
        assert_eq!(
            serde_json::to_value(EventFilter::default()).unwrap(),
            serde_json::json!({}),
            "no filter, no field"
        );
    }

    #[test]
    fn filters_parse_types_as_exact_or_prefix_and_refuse_what_they_cannot_honour() {
        let f = Filters::parse(
            &filter(&[
                ("type", "orders.*"),
                ("key", "k-1"),
                ("from", "1000"),
                ("to", "1970-01-01T00:00:02Z"),
                ("correlationId", ""),
            ]),
            false,
        )
        .unwrap();
        assert_eq!(f.type_name, Some(TypeFilter::Prefix("orders.".into())));
        assert_eq!((f.from, f.to), (Some(1000), Some(2000)));
        assert_eq!(f.key.as_deref(), Some("k-1"));
        assert_eq!(f.correlation_id, None, "an empty value is no filter");
        let exact = Filters::parse(&filter(&[("type", "orders.Placed")]), false).unwrap();
        assert_eq!(
            exact.type_name,
            Some(TypeFilter::Exact("orders.Placed".into()))
        );

        let long = "x".repeat(MAX_FILTER + 1);
        for bad in [
            filter(&[("from", "2000"), ("to", "1000")]),
            filter(&[("from", "soon")]),
            filter(&[("key", &long)]),
            filter(&[("q", &long)]),
        ] {
            assert!(
                matches!(Filters::parse(&bad, false), Err(BrowseError::Invalid(_))),
                "{bad:?}"
            );
        }
        let err = Filters::parse(&filter(&[("from", &long)]), false).unwrap_err();
        assert!(err.message().contains("longer than"), "{err}");
        let not_a_time = format!("soon{}", "x".repeat(MAX_FILTER - 10));
        let err = Filters::parse(&filter(&[("to", &not_a_time)]), false).unwrap_err();
        assert!(err.message().len() < 200, "the value is cut: {err}");
        for name in ["from", "to"] {
            let err = Filters::parse(&filter(&[(name, "1")]), true).unwrap_err();
            assert!(err.message().contains(name), "{err}");
        }
        assert!(Filters::parse(&filter(&[("type", "a.*"), ("from", "")]), true).is_ok());
    }

    #[test]
    fn filters_match_the_key_and_the_library_headers_exactly_and_types_by_prefix() {
        let headers: &[(&[u8], Option<&[u8]>)] = &[
            (b"type", Some(b"orders.OrderPlaced")),
            (b"id", Some(b"e-1")),
            (b"correlationId", Some(b"c-old")),
            (b"correlationId", Some(b"c-1")),
            (b"source", Some(b"service-b")),
            (b"userId", Some(b"u\xff")),
            (b"traceparent", None),
        ];
        let r = view(Some(b"k\xfe"), headers, Some(b"{\"total\":42}"));
        let matches =
            |pairs: &[(&str, &str)]| Filters::parse(&filter(pairs), false).unwrap().matches(&r);
        assert!(matches(&[]));
        assert!(matches(&[("type", "orders.OrderPlaced")]));
        assert!(matches(&[("type", "orders.*")]));
        assert!(matches(&[("type", "*")]));
        assert!(
            !matches(&[("type", "orders")]),
            "exact unless it ends with *"
        );
        assert!(!matches(&[("type", "orders.OrderPlaced.*")]));
        assert!(matches(&[("eventId", "e-1")]));
        assert!(matches(&[("correlationId", "c-1")]), "the last header wins");
        assert!(!matches(&[("correlationId", "c-old")]));
        assert!(matches(&[("source", "service-b")]));
        assert!(!matches(&[("source", "service")]), "exact, not a substring");
        assert!(
            matches(&[("userId", "u\\xff")]),
            "a value that is not UTF-8 matches as shown"
        );
        assert!(matches(&[("key", "k\\xfe")]));
        assert!(!matches(&[("key", "k")]));
        assert!(matches(&[("q", "\"total\":42")]));
        assert!(!matches(&[("q", "TOTAL")]), "case-sensitive");
        assert!(matches(&[("from", "1000"), ("to", "1000")]));
        assert!(!matches(&[("from", "1001")]));
        assert!(!matches(&[("to", "999")]));

        // A header that is absent (or null) matches no value; an event without a key no key.
        let bare = view(None, &[], None);
        let f = |pairs: &[(&str, &str)]| Filters::parse(&filter(pairs), false).unwrap();
        assert!(!f(&[("key", "k")]).matches(&bare));
        assert!(!f(&[("type", "*")]).matches(&bare));
        assert!(!f(&[("q", "x")]).matches(&bare));
        let null_type: &[(&[u8], Option<&[u8]>)] = &[(b"type", None)];
        assert!(!f(&[("type", "*")]).matches(&view(None, null_type, None)));
    }

    #[test]
    fn the_digest_names_the_source_and_every_filter() {
        let f = Filters::parse(&filter(&[("type", "a.*")]), false).unwrap();
        let digest = f.digest("kraken.events");
        assert_eq!(digest.len(), 16);
        assert_eq!(digest, f.digest("kraken.events"), "stable");
        assert_ne!(f.digest("kraken.events.dlt"), digest, "the source counts");
        let other = Filters::parse(&filter(&[("type", "a.Placed")]), false).unwrap();
        assert_ne!(other.digest("kraken.events"), digest);
        let header = Filters::parse(&filter(&[("source", "kraken.events")]), false).unwrap();
        assert_ne!(
            header.digest("x"),
            Filters::default().digest("kraken.events"),
            "the source header is not the source"
        );
        // A value holding what would separate fields in a plain text cannot pass for two.
        let one = Filters {
            key: Some("a\u{0}sourceHeader=b".into()),
            ..Filters::default()
        };
        let two = Filters {
            key: Some("a".into()),
            source: Some("b\u{0}sourceHeader-".into()),
            ..Filters::default()
        };
        assert_ne!(one.digest("s"), two.digest("s"));
        let framed_one = Filters {
            key: Some("a;sourceHeader=1:b".into()),
            ..Filters::default()
        };
        let framed_two = Filters {
            key: Some("a".into()),
            source: Some("b".into()),
            ..Filters::default()
        };
        assert_ne!(framed_one.digest("s"), framed_two.digest("s"));
    }
}
