//! The event browser (capability [`Browse`](super::Capability::Browse)): reading an
//! event store's sources the way an operator does, never as a subscription. An event store that
//! declares `browse` gives an [`EventBrowser`]
//! ([`EventStore::browser`](super::EventStore::browser)).
//!
//! The browser binary (`snodra-event-browser`) answers five hub query types with it; the hub
//! turns its dashboard API into them. Each request and answer below is the JSON of the type
//! named, which implements the library's [`Query`](crate::Query) (or
//! [`StreamingQuery`](crate::StreamingQuery)) with that type name. Field names are camelCase; a
//! field marked optional may be absent; positions and segments are the event store's opaque
//! strings (at most 128 bytes).
//!
//! # `snodra.events.Search` ([`SearchRequest`] → [`SearchPage`])
//!
//! ```json
//! {"source": "snodra.events",                      // optional: the browser's first source
//!  "filters": {"from": 1730000000000,              // optional, each; number (epoch ms) or text
//!              "to": "2026-11-05T10:00:00Z",       //   (epoch ms or RFC 3339)
//!              "type": "orders.*",                 // exact, or a prefix ending with *
//!              "key": "order-42",                  // exact, as a summary shows it
//!              "source": "service-b",              // header `source` (the publisher), exact
//!              "correlationId": "c-1", "userId": "u-1",
//!              "eventId": "e-1",                   // header `id`, exact
//!              "q": "\"total\":42"},               // payload substring (the hub's rules)
//!  "limit": 50,                                    // optional: 1 to max (100), default 50
//!  "cursor": "eyJmIjoi...",                        // optional: the previous page's
//!  "maxScanned": 50000, "timeoutMs": 10000}        // optional: lower the browser's budgets
//!                                                  // (a page reads 1 record per segment at least)
//! ```
//!
//! An empty text counts as absent; each filter value (`from` and `to` as text too) is at most
//! 1 KiB.
//!
//! ```json
//! {"source": "snodra.events",
//!  "missing": true,                                // only when the source does not exist
//!  "results": [<summary>, ...],                    // newest first
//!  "scanned": 1234,                                // records read for this page
//!  "exhausted": false,
//!  "stoppedBy": "limit" | "scanBudget" | "timeBudget" | null,
//!  "cursor": "eyJmIjoi..." | null}                 // null when exhausted
//! ```
//!
//! A summary ([`EventSummary`]) is never a payload:
//!
//! ```json
//! {"source": "snodra.events", "segment": "3", "position": "42",
//!  "timestamp": 1730000000000 | null,              // epoch ms
//!  "key": "order-42" | null,                       // \xNN for bytes not UTF-8, cut to 128 bytes
//!  "keyTruncated": true,                           // only when cut
//!  "headers": {"type": "...", "id": "...", "correlationId": "...", "timestamp": "...",
//!              "source": "...", "userId": "...", "traceparent": "..."},
//!                                                  // those present, each cut to 256 bytes
//!  "size": 17}                                     // payload bytes
//! ```
//!
//! # `snodra.events.Get` ([`GetRequest`] → [`EventRecord`])
//!
//! ```json
//! {"source": "snodra.events", "segment": "3", "position": "42",  // source optional; 1-128 bytes
//!  "payload": true}                                // optional, default false
//! ```
//!
//! ```json
//! {<summary fields>,
//!  "allHeaders": [{"name": "type", "value": "orders.Placed"},
//!                 {"name": "bin", "value": null, "valueHex": "00ff", "truncated": true}],
//!  "headersOmitted": 3,                            // only when more than 64 headers
//!  "payload": "{...}" | null,                      // only when asked: text, null for none
//!  "payloadBase64": "/wA=",                        // only when asked and not UTF-8
//!  "payloadTruncated": true}                       // only when cut (256 KiB)
//! ```
//!
//! # `snodra.events.Sources` ([`SourcesRequest`] → [`SourcesView`])
//!
//! `{}` →
//!
//! ```json
//! {"sources": [{"source": "snodra.events",
//!               "missing": true,                   // only when it does not exist
//!               "segments": [{"segment": "0", "first": "0", "head": "120"}]}]}
//! ```
//!
//! # `snodra.events.Lag` ([`LagRequest`] → [`LagView`])
//!
//! `{"source": "snodra.events"}` (optional) →
//!
//! ```json
//! {"source": "snodra.events", "missing": true,     // missing only when it does not exist
//!  "subscriptions": [{"subscription": "orders.projector", "state": "<the store's>", "members": 0,
//!                     "segments": [{"segment": "0", "committed": "4" | null, "head": "10",
//!                                   "lag": 6 | null}],
//!                     "lag": 6}],                  // by lag descending, then name
//!  "truncated": false}
//! ```
//!
//! # `snodra.events.Tail` ([`TailRequest`] → [`TailEvent`] parts, streaming)
//!
//! ```json
//! {"source": "snodra.events", "filters": {<no from, no to>},   // both optional
//!  "heartbeatMs": 15000, "maxDurationMs": 900000}             // optional
//!                                // heartbeat: 100 ms to 1 h, sent when no part came for that long
//! ```
//!
//! Each part is one of
//!
//! ```json
//! {"start": {"source": "snodra.events", "segments": 6, "endsAtMs": 1730000900000}}  // first
//! {"record": <summary>}
//! {"heartbeat": {"nowMs": 1730000015000}}
//! {"end": {"reason": "maxDuration" | "slowReader" | "shutdown" | "failed"}}       // last
//! ```
//!
//! # Errors ([`BrowseError`])
//!
//! Every call may fail with `{"error": "invalid" | "not-found" | "busy" | "unavailable",
//! "message": "<text>"}`, carried over the hub as a handler error (`HANDLER_ERROR`) whose message
//! is the text and whose `details` are that JSON. A tail fails this way before its `start` only.
//! A request that does not decode (a wrong JSON type, a negative number, `1.5` for an integer)
//! never reaches the browser: the library answers it `INVALID_MESSAGE`, without these details.
//!
//! # What every browser does
//!
//! - **It never moves a position.** Browsing never commits, acknowledges or resets anything, and
//!   never joins a subscription: no subscription's members or committed positions change, and the
//!   browser has no subscription of its own (the rule for every event store).
//! - **It is bounded** by its [`BrowseSettings`] (the sources it reads, records scanned, results,
//!   time, calls at once, tail duration and buffer, payload size) and by the bounds a request
//!   adds, which may only lower them.
//! - **It never blocks the caller's runtime** and every call is cancel-safe: dropping a call's
//!   future or a tail's stream leaves nothing behind beyond the call's own bounded time.

use futures::future::BoxFuture;

mod filters;
mod queries;
mod records;
mod search;
mod tail;

pub use filters::{EventFilter, Filters, MAX_FILTER, TimeArg, TypeFilter, parse_time};
pub use queries::{
    BrowseError, BrowseSettings, DEFAULT_HEARTBEAT, DEFAULT_LIMIT, EndReason, GET, GetRequest,
    Heartbeat, LAG, LagRequest, LagView, MAX_CALLS_SETTING, MAX_HEARTBEAT, MAX_PAYLOAD_SETTING,
    MAX_RESULTS_SETTING, MAX_TAIL_BUFFER_SETTING, MAX_TAIL_DURATION_SETTING, MAX_TIMEOUT_SETTING,
    MIN_HEARTBEAT, MIN_TAIL_DURATION_SETTING, SEARCH, SOURCES, SearchPage, SearchRequest,
    SegmentBounds, SegmentLag, SourceView, SourcesRequest, SourcesView, StoppedBy, SubscriptionLag,
    TAIL, TailEnd, TailEvent, TailRequest, TailStart, heartbeat_period, quoted,
};
pub use records::{
    EventRecord, EventSummary, HeaderView, MAX_HEADER_NAME, MAX_HEADER_VALUE, MAX_HEADERS, MAX_KEY,
    MAX_SUMMARY_HEADER, RecordView, cut, escaped,
};
pub use search::{
    Budget, CHUNK, Chunks, Cursor, Examined, FIRST_CHUNK, MAX_CURSOR, Page, Range, SearchPlan,
    Want, merge,
};
pub use tail::{TailFeed, TailStream, tail_channel};

/// Reads an event store's sources for an operator: search, one event, the sources, the
/// subscriptions' lag and the live tail. See the module docs for the JSON of each call and what
/// every implementation guarantees.
pub trait EventBrowser: Send + Sync {
    /// One page of a search of a source, newest first.
    fn search(&self, request: SearchRequest) -> BoxFuture<'_, Result<SearchPage, BrowseError>>;

    /// One event, every header, and its payload when asked for; [`BrowseError::NotFound`] for a
    /// segment the source lacks or a position it does not hold.
    fn get(&self, request: GetRequest) -> BoxFuture<'_, Result<EventRecord, BrowseError>>;

    /// The sources the browser reads and their segments.
    fn sources(&self, request: SourcesRequest) -> BoxFuture<'_, Result<SourcesView, BrowseError>>;

    /// Every subscription with a committed position on a source and its lag, whether an
    /// instance reads it now or not.
    fn lag(&self, request: LagRequest) -> BoxFuture<'_, Result<LagView, BrowseError>>;

    /// A tail of a source from the head of every segment. The future returns once the tail
    /// started (its stream's first part is `start`); its refusals come before.
    fn tail(&self, request: TailRequest) -> BoxFuture<'_, Result<TailStream, BrowseError>>;
}
