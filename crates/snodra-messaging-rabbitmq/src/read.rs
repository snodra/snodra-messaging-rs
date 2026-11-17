//! Reading a stream without a consumer name (nothing stored, no single active consumer): a
//! stream's bounds, the offset of a time, a range of offsets. The browser, the admin and the
//! subscriptions' lag refresh read this way.
//!
//! The stream protocol has no command for a stream's offsets in the client this crate is built
//! on (`StreamStats` is missing from `rabbitmq-stream-client` 0.11), so they are read: the first
//! chunk gives the first offset, the last chunk the head. An empty stream sends no chunk at all:
//! a stream is taken as empty only when neither read gets a chunk within [`QUIET`]; one read
//! answering and the other not is a failure, never an empty or shortened stream. (Neither the
//! client nor its protocol crate 0.11 has the `StreamStats` command.)

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use rabbitmq_stream_client::types::{Message, OffsetSpecification};
use snodra_messaging::event_store::EventStoreError;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::conn::{Chunk, Conn, Frame, failed};

/// How long a stream may stay silent before it is taken as empty.
pub const QUIET: Duration = Duration::from_millis(1500);

/// A subscription without a name, one chunk in flight: [`next`](Self::next) gives the chunks in
/// order and asks for the next one.
pub(crate) struct ChunkReader {
    conn: Arc<Conn>,
    sub: u8,
    frames: mpsc::UnboundedReceiver<Frame>,
    stream: String,
}

impl ChunkReader {
    pub async fn open(
        conn: &Arc<Conn>,
        stream: &str,
        from: OffsetSpecification,
    ) -> Result<ChunkReader, EventStoreError> {
        let (sub, frames) = conn.route(stream)?;
        let reader = ChunkReader {
            conn: Arc::clone(conn),
            sub,
            frames,
            stream: stream.to_string(),
        };
        match conn
            .timed(conn.client.subscribe(sub, stream, from, 1, HashMap::new()))
            .await
        {
            Ok(response) if response.is_ok() => Ok(reader),
            Ok(response) => {
                conn.unroute(sub);
                Err(failed(format!(
                    "cannot read {stream}: {:?}",
                    response.code()
                )))
            }
            Err(e) => {
                conn.unroute(sub);
                Err(failed(format!("cannot read {stream}: {e}")))
            }
        }
    }

    /// The next chunk; `None` when none came before `until`.
    pub async fn next(&mut self, until: Instant) -> Result<Option<Chunk>, EventStoreError> {
        match tokio::time::timeout_at(until, self.frames.recv()).await {
            Err(_) => Ok(None),
            Ok(Some(Frame::Chunk(chunk))) => {
                self.conn
                    .client
                    .credit(self.sub, 1)
                    .await
                    .map_err(|e| failed(format!("reading {}: {e}", self.stream)))?;
                Ok(Some(chunk))
            }
            Ok(Some(Frame::Update { .. })) => Ok(None),
            Ok(Some(Frame::Closed(why))) => Err(failed(format!("reading {}: {why}", self.stream))),
            Ok(None) => Err(failed(format!(
                "reading {}: the connection closed",
                self.stream
            ))),
        }
    }

    /// Ends the subscription (the frames still on their way are dropped).
    pub async fn close(self) {
        self.conn.unroute(self.sub);
        if !self.conn.is_closed() {
            let _ = tokio::time::timeout(
                crate::conn::CONNECT_TIMEOUT,
                self.conn.client.unsubscribe(self.sub),
            )
            .await;
        }
    }
}

/// A stream's offsets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Bounds {
    /// Its first offset (0 when empty).
    pub first: u64,
    /// The offset the next message appended gets (0 when empty).
    pub head: u64,
    /// When its last chunk was written, epoch ms (`None` when empty).
    pub last_timestamp: Option<i64>,
}

/// What one read of a chunk at a stream's start or end found: its first offset, its messages,
/// its time; `None` when the stream sent nothing within [`QUIET`].
pub(crate) type Seen = Option<(u64, u64, i64)>;

/// The first chunk read from `from` (first offset, messages, time), or `None` when the stream
/// sends none within [`QUIET`] (bounded by `deadline`).
async fn one_chunk(
    conn: &Arc<Conn>,
    stream: &str,
    from: OffsetSpecification,
    deadline: Instant,
) -> Result<Seen, EventStoreError> {
    let mut reader = ChunkReader::open(conn, stream, from).await?;
    let chunk = reader.next(deadline.min(Instant::now() + QUIET)).await;
    reader.close().await;
    Ok(chunk?.map(|c| (c.first_offset, c.messages.len() as u64, c.timestamp_ms)))
}

/// The bounds from what the first and the last chunk reads found. Empty only when both were
/// silent: one silent read beside one that answered is no answer (a slow or stalled read, never
/// an empty or shortened stream), so it fails.
pub(crate) fn bounds_of(stream: &str, first: Seen, last: Seen) -> Result<Bounds, EventStoreError> {
    match (first, last) {
        (None, None) => Ok(Bounds::default()),
        (Some((first, _, _)), Some((last, count, timestamp))) => {
            let head = last + count;
            Ok(Bounds {
                first: first.min(head),
                head,
                last_timestamp: Some(timestamp),
            })
        }
        _ => Err(failed(format!(
            "the bounds of {stream} were not read: one read answered, the other did not within {QUIET:?}"
        ))),
    }
}

/// The bounds of `stream` on its leader `conn`.
pub(crate) async fn bounds(
    conn: &Arc<Conn>,
    stream: &str,
    deadline: Instant,
) -> Result<Bounds, EventStoreError> {
    let (first, last) = tokio::join!(
        one_chunk(conn, stream, OffsetSpecification::First, deadline),
        one_chunk(conn, stream, OffsetSpecification::Last, deadline),
    );
    bounds_of(stream, first?, last?)
}

/// The head of `stream` (and its last chunk's time): both reads at once, so an empty stream
/// costs one [`QUIET`] wait, not two in a row.
pub(crate) async fn head(
    conn: &Arc<Conn>,
    stream: &str,
    deadline: Instant,
) -> Result<Bounds, EventStoreError> {
    let bounds = bounds(conn, stream, deadline).await?;
    Ok(Bounds { first: 0, ..bounds })
}

/// The head of a stream known to hold events (its last chunk only); `None` when the read is
/// silent (stalled: the stream is not empty).
pub(crate) async fn head_of_known(
    conn: &Arc<Conn>,
    stream: &str,
    deadline: Instant,
) -> Result<Option<u64>, EventStoreError> {
    Ok(one_chunk(conn, stream, OffsetSpecification::Last, deadline)
        .await?
        .map(|(first, count, _)| first + count))
}

/// The streams found empty lately, so a browser call does not wait [`QUIET`] on each of them
/// every time: an empty answer is kept for [`EMPTY_FOR`]. Answers with data are never kept.
#[derive(Clone, Default)]
pub(crate) struct EmptyCache(Arc<std::sync::Mutex<HashMap<String, std::time::Instant>>>);

/// How long an empty answer is kept.
pub(crate) const EMPTY_FOR: Duration = Duration::from_secs(5);

impl EmptyCache {
    fn fresh(&self, stream: &str) -> bool {
        crate::conn::lock(&self.0)
            .get(stream)
            .is_some_and(|until| std::time::Instant::now() < *until)
    }

    /// `read`'s answer, or an empty one kept from an earlier call.
    pub async fn bounds(
        &self,
        stream: &str,
        read: impl Future<Output = Result<Bounds, EventStoreError>>,
    ) -> Result<Bounds, EventStoreError> {
        if self.fresh(stream) {
            return Ok(Bounds::default());
        }
        let bounds = read.await?;
        let mut kept = crate::conn::lock(&self.0);
        if bounds.last_timestamp.is_none() {
            kept.insert(stream.to_string(), std::time::Instant::now() + EMPTY_FOR);
        } else {
            kept.remove(stream);
        }
        Ok(bounds)
    }
}

/// The first offset of `stream` written at or after `timestamp_ms` (the time of its chunk),
/// else its head.
pub(crate) async fn offset_at(
    conn: &Arc<Conn>,
    stream: &str,
    timestamp_ms: i64,
    bounds: &Bounds,
    deadline: Instant,
) -> Result<u64, EventStoreError> {
    match bounds.last_timestamp {
        None => return Ok(bounds.head),
        Some(last) if last < timestamp_ms => return Ok(bounds.head),
        _ => {}
    }
    let mut reader =
        ChunkReader::open(conn, stream, OffsetSpecification::Timestamp(timestamp_ms)).await?;
    let found = loop {
        match reader.next(deadline).await {
            Err(e) => break Err(e),
            Ok(None) => break Err(failed(format!("reading {stream}: no answer in time"))),
            Ok(Some(chunk)) if chunk.timestamp_ms >= timestamp_ms => {
                break Ok(chunk.first_offset.max(bounds.first));
            }
            Ok(Some(chunk)) if chunk.first_offset + chunk.messages.len() as u64 >= bounds.head => {
                break Ok(bounds.head);
            }
            Ok(Some(_)) => {}
        }
    };
    reader.close().await;
    found
}

/// Reads the messages of `stream` at offsets `start..end` in order, handing each to `visit`
/// (offset, chunk time, message); `true` when it reached `end` before `deadline`.
pub(crate) async fn read_range(
    conn: &Arc<Conn>,
    stream: &str,
    start: u64,
    end: u64,
    deadline: Instant,
    mut visit: impl FnMut(u64, i64, &Message),
) -> Result<bool, EventStoreError> {
    if start >= end {
        return Ok(true);
    }
    let mut reader = ChunkReader::open(conn, stream, OffsetSpecification::Offset(start)).await?;
    let complete = loop {
        match reader.next(deadline).await {
            Err(e) => break Err(e),
            Ok(None) => break Ok(false),
            Ok(Some(chunk)) => {
                let mut done = false;
                for (i, message) in chunk.messages.iter().enumerate() {
                    let offset = chunk.first_offset + i as u64;
                    if offset >= end {
                        done = true;
                        break;
                    }
                    if offset >= start {
                        visit(offset, chunk.timestamp_ms, message);
                    }
                }
                if done || chunk.first_offset + chunk.messages.len() as u64 >= end {
                    break Ok(true);
                }
            }
        }
    };
    reader.close().await;
    complete
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A read double: what the first and last chunk reads found (`None`: silent, as a stalled
    /// or slow read is).
    #[test]
    fn a_stream_is_empty_only_when_both_reads_are_silent() {
        assert_eq!(bounds_of("s", None, None).unwrap(), Bounds::default());
        assert_eq!(
            bounds_of("s", Some((3, 2, 7)), Some((40, 5, 9))).unwrap(),
            Bounds {
                first: 3,
                head: 45,
                last_timestamp: Some(9)
            }
        );
        // A stalled first read never shortens the stream to its last chunk, and a stalled last
        // read never makes it empty (a reset to latest replaying it all).
        for (first, last) in [(None, Some((40, 5, 9))), (Some((3, 2, 7)), None)] {
            assert!(matches!(
                bounds_of("s", first, last),
                Err(EventStoreError::Failed(m)) if m.contains("not read")
            ));
        }
    }

    #[tokio::test]
    async fn an_empty_answer_is_kept_briefly_and_a_full_one_never() {
        let cache = EmptyCache::default();
        let reads = std::sync::atomic::AtomicUsize::new(0);
        // Counts the reads that run (a future built but not awaited is no read).
        let read = |b: Bounds| {
            let reads = &reads;
            async move {
                reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(b)
            }
        };
        let full = Bounds {
            first: 0,
            head: 3,
            last_timestamp: Some(1),
        };
        let count = || reads.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(cache.bounds("a", read(full)).await.unwrap(), full);
        assert_eq!(cache.bounds("a", read(full)).await.unwrap(), full);
        assert_eq!(count(), 2, "a full answer is read every time");
        cache.bounds("b", read(Bounds::default())).await.unwrap();
        assert_eq!(count(), 3);
        assert_eq!(
            cache.bounds("b", read(full)).await.unwrap(),
            Bounds::default(),
            "kept empty"
        );
        assert_eq!(count(), 3, "not read again while the empty answer is kept");
    }
}
