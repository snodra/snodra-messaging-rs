//! The five calls of the event browser, as JSON: the payloads of the hub query types
//! `kraken.events.*` (see the module docs of [`browse`](super) for the shapes), the error every
//! call may answer, and the settings that bound them.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::filters::EventFilter;
use super::records::{EventRecord, EventSummary};
use crate::error::MessagingError;
use crate::event_store::{MAX_POSITION_BYTES, MAX_SEGMENT_BYTES};
use crate::types::{Query, StreamingQuery};

/// The query type of [`SearchRequest`].
pub const SEARCH: &str = "kraken.events.Search";
/// The query type of [`GetRequest`].
pub const GET: &str = "kraken.events.Get";
/// The query type of [`SourcesRequest`].
pub const SOURCES: &str = "kraken.events.Sources";
/// The query type of [`LagRequest`].
pub const LAG: &str = "kraken.events.Lag";
/// The streaming query type of [`TailRequest`].
pub const TAIL: &str = "kraken.events.Tail";

/// Results a search page returns when the request names no `limit`.
pub const DEFAULT_LIMIT: usize = 50;
/// How often a tail sends a heartbeat when the request names no `heartbeatMs`.
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(15);
/// The shortest heartbeat period a tail takes...
pub const MIN_HEARTBEAT: Duration = Duration::from_millis(100);
/// ...and the longest.
pub const MAX_HEARTBEAT: Duration = Duration::from_secs(3600);
/// The most results a browser may be set to return per page (ADR 0025 ruling 21).
pub const MAX_RESULTS_SETTING: usize = 1000;
/// The longest time a browser may be set to give a call.
pub const MAX_TIMEOUT_SETTING: Duration = Duration::from_secs(5 * 60);
/// The shortest and the longest a browser may be set to keep a tail open.
pub const MIN_TAIL_DURATION_SETTING: Duration = Duration::from_secs(1);
pub const MAX_TAIL_DURATION_SETTING: Duration = Duration::from_secs(24 * 3600);
/// The most events a browser may be set to buffer for a tail's reader.
pub const MAX_TAIL_BUFFER_SETTING: usize = 65_536;
/// The most payload bytes a browser may be set to carry in a record.
pub const MAX_PAYLOAD_SETTING: usize = 16 * 1024 * 1024;

/// What every call of the browser may fail with, as JSON `{"error": "invalid" | "not-found" |
/// "busy" | "unavailable", "message": "<text>"}`.
///
/// Over the hub, a handler answers it as a handler error (`HANDLER_ERROR`) whose message is the
/// text and whose `details` are this JSON ([`MessagingError::from`],
/// [`BrowseError::from_details`]).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "error", content = "message", rename_all = "kebab-case")]
pub enum BrowseError {
    /// The request cannot be honoured as sent: a source not browsable here, a filter, limit or
    /// cursor out of bounds, a malformed segment or position (HTTP 400).
    Invalid(String),
    /// The source, the segment or the event does not exist (HTTP 404).
    NotFound(String),
    /// Every slot of the call's kind is taken; try again shortly (HTTP 429).
    Busy(String),
    /// The event store did not answer in time, or answered an error (HTTP 503).
    Unavailable(String),
}

impl BrowseError {
    /// The text.
    pub fn message(&self) -> &str {
        match self {
            BrowseError::Invalid(m)
            | BrowseError::NotFound(m)
            | BrowseError::Busy(m)
            | BrowseError::Unavailable(m) => m,
        }
    }

    /// The code: `invalid`, `not-found`, `busy` or `unavailable`.
    pub fn code(&self) -> &'static str {
        match self {
            BrowseError::Invalid(_) => "invalid",
            BrowseError::NotFound(_) => "not-found",
            BrowseError::Busy(_) => "busy",
            BrowseError::Unavailable(_) => "unavailable",
        }
    }

    /// The error a handler error's `details` carry, when they are one.
    pub fn from_details(details: &[u8]) -> Option<BrowseError> {
        serde_json::from_slice(details).ok()
    }
}

impl std::fmt::Display for BrowseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for BrowseError {}

/// A handler error (`HANDLER_ERROR`) with the text as its message and the JSON as its details.
impl From<BrowseError> for MessagingError {
    fn from(e: BrowseError) -> Self {
        MessagingError::RemoteHandler {
            type_name: String::new(),
            message: e.message().to_string(),
            details: serde_json::to_vec(&e).expect("a browse error serializes"),
        }
    }
}

/// The bounds of a browser (ADR 0025's, as defaults) and the sources it reads: settings of the
/// add-on's browser, which the calls' own bounds (`limit`, `maxScanned`, `timeoutMs`,
/// `maxDurationMs`) may only lower.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseSettings {
    /// The only sources the browser reads; the first is the default of a call that names none.
    pub sources: Vec<String>,
    /// Records a search page reads at most (50 000).
    pub max_scanned: u64,
    /// Results a search page returns at most (100).
    pub max_results: usize,
    /// The time a search page, a record read, a sources or lag listing, or a tail's setup takes
    /// at most (10 s).
    pub timeout: Duration,
    /// Searches, record reads, sources and lag listings at once (4); one more is
    /// [`BrowseError::Busy`].
    pub max_searches: usize,
    /// Tails open at once (4); one more is [`BrowseError::Busy`].
    pub max_tails: usize,
    /// How long a tail lasts at most (15 min).
    pub tail_duration: Duration,
    /// Events a tail holds for a reader that is behind (256); one more ends the tail
    /// (`slowReader`).
    pub tail_buffer: usize,
    /// Payload bytes a record carries at most (256 KiB).
    pub max_payload: usize,
}

impl Default for BrowseSettings {
    fn default() -> Self {
        BrowseSettings {
            sources: vec!["kraken.events".to_string()],
            max_scanned: 50_000,
            max_results: 100,
            timeout: Duration::from_secs(10),
            max_searches: 4,
            max_tails: 4,
            tail_duration: Duration::from_secs(15 * 60),
            tail_buffer: 256,
            max_payload: 256 * 1024,
        }
    }
}

impl BrowseSettings {
    /// The settings' mistakes: no source, an empty one, a bound of zero, or a bound past its
    /// maximum: `max_results` at most [`MAX_RESULTS_SETTING`] (1000), `timeout` at most
    /// [`MAX_TIMEOUT_SETTING`] (5 min), `tail_duration` from [`MIN_TAIL_DURATION_SETTING`]
    /// (1 s) to [`MAX_TAIL_DURATION_SETTING`] (24 h), `tail_buffer` at most
    /// [`MAX_TAIL_BUFFER_SETTING`] (65 536), `max_payload` at most [`MAX_PAYLOAD_SETTING`]
    /// (16 MiB). Within these, no deadline a browser computes from them can overflow.
    pub fn validate(&self) -> Result<(), String> {
        if self.sources.is_empty() || self.sources.iter().any(|s| s.trim().is_empty()) {
            return Err("the browser needs at least one source, none of them empty".into());
        }
        for (name, zero) in [
            ("max_scanned", self.max_scanned == 0),
            ("max_results", self.max_results == 0),
            ("timeout", self.timeout.is_zero()),
            ("max_searches", self.max_searches == 0),
            ("max_tails", self.max_tails == 0),
            ("tail_duration", self.tail_duration.is_zero()),
            ("tail_buffer", self.tail_buffer == 0),
            ("max_payload", self.max_payload == 0),
        ] {
            if zero {
                return Err(format!("the browser's {name} must be positive"));
            }
        }
        for (name, over, max) in [
            (
                "max_results",
                self.max_results > MAX_RESULTS_SETTING,
                MAX_RESULTS_SETTING.to_string(),
            ),
            (
                "timeout",
                self.timeout > MAX_TIMEOUT_SETTING,
                format!("{MAX_TIMEOUT_SETTING:?}"),
            ),
            (
                "tail_duration",
                self.tail_duration > MAX_TAIL_DURATION_SETTING,
                format!("{MAX_TAIL_DURATION_SETTING:?}"),
            ),
            (
                "tail_buffer",
                self.tail_buffer > MAX_TAIL_BUFFER_SETTING,
                MAX_TAIL_BUFFER_SETTING.to_string(),
            ),
            (
                "max_payload",
                self.max_payload > MAX_PAYLOAD_SETTING,
                MAX_PAYLOAD_SETTING.to_string(),
            ),
        ] {
            if over {
                return Err(format!("the browser's {name} is at most {max}"));
            }
        }
        if self.tail_duration < MIN_TAIL_DURATION_SETTING {
            return Err("the browser's tail_duration is at least 1 s".into());
        }
        Ok(())
    }

    /// The source a call reads: `requested` when it is one of [`sources`](Self::sources), the
    /// first of them when it is absent or empty; [`BrowseError::Invalid`] otherwise.
    pub fn source(&self, requested: Option<&str>) -> Result<String, BrowseError> {
        match requested.filter(|s| !s.is_empty()) {
            None => self
                .sources
                .first()
                .cloned()
                .ok_or_else(|| BrowseError::Invalid("no source is browsable here".into())),
            Some(s) if self.sources.iter().any(|c| c == s) => Ok(s.to_string()),
            Some(s) => Err(BrowseError::Invalid(format!(
                "source {} is not browsable here; the browser reads {}",
                quoted(s),
                self.sources.join(", ")
            ))),
        }
    }

    /// The time a tail lasts: `requested` (ms) when set, at most
    /// [`tail_duration`](Self::tail_duration).
    pub fn tail_duration(&self, requested: Option<u64>) -> Result<Duration, BrowseError> {
        match requested {
            None => Ok(self.tail_duration),
            Some(0) => Err(BrowseError::Invalid(
                "maxDurationMs must be at least 1".into(),
            )),
            Some(ms) => Ok(Duration::from_millis(ms).min(self.tail_duration)),
        }
    }
}

/// The heartbeat period of a tail: `requested` (ms) when set, within [`MIN_HEARTBEAT`] ..
/// [`MAX_HEARTBEAT`].
pub fn heartbeat_period(requested: Option<u64>) -> Duration {
    requested.map_or(DEFAULT_HEARTBEAT, |ms| {
        Duration::from_millis(ms).clamp(MIN_HEARTBEAT, MAX_HEARTBEAT)
    })
}

/// The most bytes of a value an error message quotes.
const MAX_QUOTED: usize = 64;

/// `s` quoted for an error message, cut to [`MAX_QUOTED`] bytes (`...` when cut): an error
/// never echoes unbounded input.
pub fn quoted(s: &str) -> String {
    let (shown, cut_off) = super::records::cut(s, MAX_QUOTED);
    if cut_off {
        format!("{shown:?}...")
    } else {
        format!("{shown:?}")
    }
}

// ---- kraken.events.Search -----------------------------------------------------------------------

/// `kraken.events.Search`: one page of a search, newest first.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SearchRequest {
    /// The source searched; absent or empty: the browser's first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// What an event must match.
    #[serde(default)]
    pub filters: EventFilter,
    /// Results at most: 1 to the browser's maximum (100), default 50.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// The `cursor` of the previous page, sent with the same source and filters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Records read at most (lowers the browser's budget, 50 000); a page still reads one record
    /// per segment at least.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_scanned: Option<u64>,
    /// Milliseconds the page may take (lowers the browser's budget, 10 s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// Why a search page ended before every segment reached its lower bound.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StoppedBy {
    Limit,
    ScanBudget,
    TimeBudget,
}

/// The answer of `kraken.events.Search`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SearchPage {
    pub source: String,
    /// The source is browsable but does not exist (absent when it does).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub missing: bool,
    /// The matching events, newest first.
    pub results: Vec<EventSummary>,
    /// Records read for this page.
    pub scanned: u64,
    /// Nothing is left to search: there is no next page.
    pub exhausted: bool,
    /// Why the page ended early; `null` when exhausted.
    pub stopped_by: Option<StoppedBy>,
    /// Continues this search (send it with the same source and filters); `null` when exhausted.
    pub cursor: Option<String>,
}

crate::message_type!(SearchRequest, "kraken.events.Search");
impl Query for SearchRequest {
    type Reply = SearchPage;
}

// ---- kraken.events.Get --------------------------------------------------------------------------

/// `kraken.events.Get`: one event, every header, and its payload when asked for. Check
/// [`place`](Self::place) before reading.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GetRequest {
    /// Absent or empty: the browser's first source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub segment: String,
    pub position: String,
    /// Carry the payload (the hub asks only under the payload rules of ADR 0025).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub payload: bool,
}

impl GetRequest {
    /// The segment and the position, each 1 to 128 bytes ([`MAX_SEGMENT_BYTES`],
    /// [`MAX_POSITION_BYTES`]); [`BrowseError::Invalid`] otherwise. An add-on checks them with
    /// this before reading.
    pub fn place(&self) -> Result<(&str, &str), BrowseError> {
        for (name, value, max) in [
            ("segment", &self.segment, MAX_SEGMENT_BYTES),
            ("position", &self.position, MAX_POSITION_BYTES),
        ] {
            if value.is_empty() || value.len() > max {
                return Err(BrowseError::Invalid(format!(
                    "the {name} is 1 to {max} bytes, not {}",
                    value.len()
                )));
            }
        }
        Ok((&self.segment, &self.position))
    }
}

crate::message_type!(GetRequest, "kraken.events.Get");
impl Query for GetRequest {
    type Reply = EventRecord;
}

// ---- kraken.events.Sources ----------------------------------------------------------------------

/// `kraken.events.Sources`: the sources the browser reads, `{}`.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct SourcesRequest {}

/// The answer of `kraken.events.Sources`: every source the browser reads, the default first.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SourcesView {
    pub sources: Vec<SourceView>,
}

/// One source and its segments.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceView {
    pub source: String,
    /// It does not exist (absent when it does; it then has no segments).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub missing: bool,
    /// In segment order.
    pub segments: Vec<SegmentBounds>,
}

/// The positions a segment holds now.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SegmentBounds {
    pub segment: String,
    /// The first position it still holds.
    pub first: String,
    /// The position of the next event appended (`first == head`: empty).
    pub head: String,
}

crate::message_type!(SourcesRequest, "kraken.events.Sources");
impl Query for SourcesRequest {
    type Reply = SourcesView;
}

// ---- kraken.events.Lag --------------------------------------------------------------------------

/// `kraken.events.Lag`: every subscription with a committed position on a source, live or not.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LagRequest {
    /// Absent or empty: the browser's first source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// The answer of `kraken.events.Lag`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LagView {
    pub source: String,
    /// The source does not exist (absent when it does).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub missing: bool,
    /// By lag, descending, then name.
    pub subscriptions: Vec<SubscriptionLag>,
    /// Some subscriptions are not listed (too many, or the time ran out).
    pub truncated: bool,
}

/// One subscription (`<service>.<processor>`) of the source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionLag {
    pub subscription: String,
    /// As the event store reports it, in its own names.
    pub state: String,
    /// Instances reading it now.
    pub members: u64,
    /// Every segment of the source, in segment order.
    pub segments: Vec<SegmentLag>,
    /// The sum of the known lags.
    pub lag: u64,
}

/// One segment of a subscription.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SegmentLag {
    pub segment: String,
    /// The position after its last acknowledgement; `null` when it committed none here.
    pub committed: Option<String>,
    /// The position of the next event appended.
    pub head: String,
    /// Events between `committed` and `head`; `null` without a committed position.
    pub lag: Option<u64>,
}

crate::message_type!(LagRequest, "kraken.events.Lag");
impl Query for LagRequest {
    type Reply = LagView;
}

// ---- kraken.events.Tail -------------------------------------------------------------------------

/// `kraken.events.Tail` (a streaming query): the events appended from now on, as they come.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TailRequest {
    /// Absent or empty: the browser's first source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// What an event must match; no `from` or `to`.
    #[serde(default)]
    pub filters: EventFilter,
    /// Milliseconds between heartbeats (default 15 000, at least 100).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_ms: Option<u64>,
    /// Milliseconds the tail lasts (lowers the browser's bound, 15 min).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_duration_ms: Option<u64>,
}

/// One part of a tail: `start` first, then `record`s and `heartbeat`s, `end` last.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TailEvent {
    /// The tail reads from the head of every segment: an event appended after this part is
    /// delivered.
    Start(TailStart),
    /// A matching event (a summary, never a payload).
    Record(EventSummary),
    /// Nothing new; the tail is alive.
    Heartbeat(Heartbeat),
    /// The tail is over; nothing follows.
    End(TailEnd),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TailStart {
    pub source: String,
    /// The source's segments.
    pub segments: u64,
    /// When the tail ends at the latest, epoch milliseconds.
    pub ends_at_ms: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Heartbeat {
    pub now_ms: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TailEnd {
    pub reason: EndReason,
}

/// Why a tail ended.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum EndReason {
    /// It lasted its maximum duration.
    MaxDuration,
    /// Its reader fell the browser's buffer behind (sent before what was still buffered).
    SlowReader,
    /// The browser is stopping (sent by the browser's process, not by an add-on).
    Shutdown,
    /// The browser failed while reading.
    Failed,
}

crate::message_type!(TailRequest, "kraken.events.Tail");
impl StreamingQuery for TailRequest {
    type Item = TailEvent;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MessageType;
    use serde_json::json;

    #[test]
    fn the_query_types_are_the_kraken_events_namespace() {
        assert_eq!(SearchRequest::TYPE, SEARCH);
        assert_eq!(GetRequest::TYPE, GET);
        assert_eq!(SourcesRequest::TYPE, SOURCES);
        assert_eq!(LagRequest::TYPE, LAG);
        assert_eq!(TailRequest::TYPE, TAIL);
    }

    #[test]
    fn errors_are_a_code_and_a_message_and_travel_as_handler_details() {
        let e = BrowseError::NotFound("no event at 7".into());
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            json!({"error": "not-found", "message": "no event at 7"})
        );
        assert_eq!(
            (e.code(), e.to_string().as_str()),
            ("not-found", "no event at 7")
        );
        let m = MessagingError::from(e.clone());
        assert_eq!(m.to_string(), "no event at 7");
        assert_eq!(BrowseError::from_details(&m.reply_details()), Some(e));
        for (e, code) in [
            (BrowseError::Invalid(String::new()), "invalid"),
            (BrowseError::Busy(String::new()), "busy"),
            (BrowseError::Unavailable(String::new()), "unavailable"),
        ] {
            assert_eq!(serde_json::to_value(&e).unwrap()["error"], code);
            assert_eq!(e.code(), code);
        }
        assert_eq!(BrowseError::from_details(b"retryable"), None);
    }

    #[test]
    fn requests_read_the_documented_json() {
        let search: SearchRequest = serde_json::from_value(json!({
            "source": "kraken.events",
            "filters": {"type": "orders.*", "from": 1700000000000i64, "to": "2026-11-05T10:00:00Z"},
            "limit": 20,
            "cursor": "abc",
            "maxScanned": 1000,
            "timeoutMs": 2000
        }))
        .unwrap();
        assert_eq!(search.limit, Some(20));
        assert_eq!(search.filters.type_name.as_deref(), Some("orders.*"));
        assert_eq!(
            serde_json::from_value::<SearchRequest>(json!({})).unwrap(),
            SearchRequest::default()
        );
        assert_eq!(
            serde_json::to_value(SearchRequest::default()).unwrap(),
            json!({"filters": {}})
        );
        let get: GetRequest =
            serde_json::from_value(json!({"segment": "3", "position": "42", "payload": true}))
                .unwrap();
        assert!(get.payload && get.source.is_none());
        assert_eq!(
            serde_json::from_value::<SourcesRequest>(json!({})).unwrap(),
            SourcesRequest {}
        );
        let tail: TailRequest =
            serde_json::from_value(json!({"filters": {"key": "k"}, "heartbeatMs": 1000})).unwrap();
        assert_eq!(tail.heartbeat_ms, Some(1000));
    }

    #[test]
    fn a_get_names_a_segment_and_a_position_of_1_to_128_bytes() {
        let get = |segment: &str, position: &str| GetRequest {
            segment: segment.into(),
            position: position.into(),
            ..GetRequest::default()
        };
        assert_eq!(get("3", "42").place(), Ok(("3", "42")));
        let long = "x".repeat(129);
        for bad in [get("", "1"), get("1", ""), get(&long, "1"), get("1", &long)] {
            let err = bad.place().unwrap_err();
            assert!(matches!(err, BrowseError::Invalid(_)), "{bad:?}");
            assert!(!err.message().contains(&long), "never echoed: {err}");
        }
        assert!(get(&"x".repeat(128), &"9".repeat(128)).place().is_ok());
    }

    #[test]
    fn tail_parts_are_tagged_by_kind() {
        let parts = [
            (
                TailEvent::Start(TailStart {
                    source: "s".into(),
                    segments: 6,
                    ends_at_ms: 9,
                }),
                json!({"start": {"source": "s", "segments": 6, "endsAtMs": 9}}),
            ),
            (
                TailEvent::Heartbeat(Heartbeat { now_ms: 5 }),
                json!({"heartbeat": {"nowMs": 5}}),
            ),
            (
                TailEvent::End(TailEnd {
                    reason: EndReason::SlowReader,
                }),
                json!({"end": {"reason": "slowReader"}}),
            ),
        ];
        for (part, want) in parts {
            assert_eq!(serde_json::to_value(&part).unwrap(), want);
            assert_eq!(serde_json::from_value::<TailEvent>(want).unwrap(), part);
        }
        for (reason, name) in [
            (EndReason::MaxDuration, "maxDuration"),
            (EndReason::Shutdown, "shutdown"),
            (EndReason::Failed, "failed"),
        ] {
            assert_eq!(serde_json::to_value(reason).unwrap(), name);
        }
    }

    #[test]
    fn settings_default_to_adr_0025_and_refuse_zero_bounds() {
        let s = BrowseSettings::default();
        assert_eq!(s.sources, ["kraken.events"]);
        assert_eq!((s.max_scanned, s.max_results), (50_000, 100));
        assert_eq!(s.timeout, Duration::from_secs(10));
        assert_eq!((s.max_searches, s.max_tails, s.tail_buffer), (4, 4, 256));
        assert_eq!(s.tail_duration, Duration::from_secs(900));
        assert_eq!(s.max_payload, 256 * 1024);
        assert_eq!(s.validate(), Ok(()));
        for bad in [
            BrowseSettings {
                sources: vec![],
                ..BrowseSettings::default()
            },
            BrowseSettings {
                sources: vec![" ".into()],
                ..BrowseSettings::default()
            },
            BrowseSettings {
                max_tails: 0,
                ..BrowseSettings::default()
            },
            BrowseSettings {
                max_results: 1001,
                ..BrowseSettings::default()
            },
            BrowseSettings {
                timeout: Duration::from_secs(301),
                ..BrowseSettings::default()
            },
            BrowseSettings {
                tail_duration: Duration::from_millis(999),
                ..BrowseSettings::default()
            },
            BrowseSettings {
                tail_duration: Duration::from_secs(24 * 3600 + 1),
                ..BrowseSettings::default()
            },
            BrowseSettings {
                tail_buffer: 65_537,
                ..BrowseSettings::default()
            },
            BrowseSettings {
                max_payload: 16 * 1024 * 1024 + 1,
                ..BrowseSettings::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        let widest = BrowseSettings {
            max_results: 1000,
            timeout: Duration::from_secs(300),
            tail_duration: Duration::from_secs(24 * 3600),
            tail_buffer: 65_536,
            max_payload: 16 * 1024 * 1024,
            ..BrowseSettings::default()
        };
        assert_eq!(widest.validate(), Ok(()));
        assert_eq!(s.source(None).as_deref(), Ok("kraken.events"));
        assert_eq!(s.source(Some("")).as_deref(), Ok("kraken.events"));
        assert!(matches!(s.source(Some("b")), Err(BrowseError::Invalid(_))));
        assert_eq!(s.tail_duration(Some(2_000)), Ok(Duration::from_secs(2)));
        assert_eq!(s.tail_duration(Some(u64::MAX)), Ok(s.tail_duration));
        assert!(s.tail_duration(Some(0)).is_err());
        assert_eq!(heartbeat_period(None), DEFAULT_HEARTBEAT);
        assert_eq!(heartbeat_period(Some(1)), MIN_HEARTBEAT);
        assert_eq!(heartbeat_period(Some(2_000)), Duration::from_secs(2));
        assert_eq!(heartbeat_period(Some(u64::MAX)), MAX_HEARTBEAT);
        let long = "s".repeat(10_000);
        let err = s.source(Some(&long)).unwrap_err();
        assert!(err.message().len() < 200, "the source is cut: {err}");
    }
}
