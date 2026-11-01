//! Streaming queries (ADR 0017, P4b) against in-process fake hubs: the handler side (parts under
//! credit, the end, error items, the hub's `Cancel` for streams and for unary handlers, the
//! permit held for the whole stream) and the sender side (`Sender::stream`: items in order,
//! credit refills, `max_results`, the idle timeout, errors, the drop guard's `Cancel`, retries
//! until the first part, the capability gates, shared streaming types).
mod support;

use kraken_messaging::hub::{HandlerRegistry, RegisteredTypes};
use kraken_messaging::{MessagingError, Query, StreamingQuery, message_type};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

/// A unary query, next to the streaming ones.
#[derive(Serialize, Deserialize)]
struct Lookup {}
message_type!(Lookup, "t.Lookup");
impl Query for Lookup {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct SharedLookup {}
message_type!(SharedLookup, "t.SharedLookup");
impl Query for SharedLookup {
    type Reply = ();
}

/// `n` rows, each after `delay_ms`; the handler fails at row `fail_at` (an error item).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Export {
    n: u32,
    delay_ms: u64,
    fail_at: Option<u32>,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Row {
    i: u32,
}
message_type!(Export, "t.Export");
impl StreamingQuery for Export {
    type Item = Row;
}

#[derive(Serialize, Deserialize)]
struct SharedExport {}
message_type!(SharedExport, "t.SharedExport");
impl StreamingQuery for SharedExport {
    type Item = Row;
}

fn rows(export: Export) -> impl futures::Stream<Item = Result<Row, MessagingError>> + Send {
    let Export {
        n,
        delay_ms,
        fail_at,
    } = export;
    tokio_stream::iter(0..n).then(move |i| async move {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        if fail_at == Some(i) {
            Err(MessagingError::Storage("disk gone".into()))
        } else {
            Ok(Row { i })
        }
    })
}

#[test]
fn streaming_handlers_are_query_types_exclusive_or_shared() {
    let mut r = HandlerRegistry::new();
    r.query::<Lookup, _, _>(|_, _| async move { Ok(()) });
    r.shared_query::<SharedLookup, _, _>(|_, _| async move { Ok(()) });
    r.streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
    r.shared_streaming_query::<SharedExport, _, _, _>(|_, _| async move {
        Ok(tokio_stream::empty::<Result<Row, MessagingError>>())
    });
    assert_eq!(
        r.registered_types(),
        RegisteredTypes {
            commands: vec![],
            queries: vec!["t.Lookup".into()],
            shared_queries: vec!["t.SharedExport".into(), "t.SharedLookup".into()],
            streaming_queries: vec!["t.Export".into()],
        }
    );
    assert_eq!(
        r.query_types(),
        ["t.Export", "t.Lookup", "t.SharedExport", "t.SharedLookup"]
    );
    assert_eq!(r.exclusive_query_types(), ["t.Lookup"]);
    assert_eq!(r.streaming_query_types(), ["t.Export", "t.SharedExport"]);
    assert_eq!(r.shared_query_types(), ["t.SharedExport", "t.SharedLookup"]);
}

#[test]
#[should_panic(expected = "duplicate handler for t.Export")]
fn a_type_has_one_handler_whatever_its_kind() {
    let mut r = HandlerRegistry::new();
    r.streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
    r.streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
}

#[test]
#[should_panic(expected = "registered both as an exclusive and as a shared query")]
fn a_streaming_type_cannot_be_exclusive_and_shared() {
    let mut r = HandlerRegistry::new();
    r.streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
    r.shared_streaming_query::<Export, _, _, _>(|q, _| async move { Ok(rows(q)) });
}
