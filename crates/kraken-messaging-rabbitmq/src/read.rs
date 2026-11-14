//! Reading a stream without a consumer name (nothing stored, no single active consumer): a
//! stream's bounds, the offset of a time, a range of offsets. The browser, the admin and the
//! subscriptions' lag refresh read this way.
//!
//! The stream protocol has no command for a stream's offsets in the client this crate is built
//! on (`StreamStats` is missing from `rabbitmq-stream-client` 0.11), so they are read: the first
//! chunk gives the first offset, the last chunk the head. An empty stream sends no chunk at all;
//! one that sends none within [`QUIET`] is taken as empty.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::event_store::EventStoreError;
use rabbitmq_stream_client::types::{Message, OffsetSpecification};
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
            .client
            .subscribe(sub, stream, from, 1, HashMap::new())
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

/// The first chunk read from `from`, or `None` when the stream sends none within [`QUIET`]
/// (bounded by `deadline`).
async fn one_chunk(
    conn: &Arc<Conn>,
    stream: &str,
    from: OffsetSpecification,
    deadline: Instant,
) -> Result<Option<Chunk>, EventStoreError> {
    let mut reader = ChunkReader::open(conn, stream, from).await?;
    let chunk = reader.next(deadline.min(Instant::now() + QUIET)).await;
    reader.close().await;
    chunk
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
    let (first, last) = (first?, last?);
    Ok(match (first, last) {
        (_, None) => Bounds::default(),
        (first, Some(last)) => {
            let head = last.first_offset + last.messages.len() as u64;
            Bounds {
                first: first
                    .map_or(last.first_offset, |c| c.first_offset)
                    .min(head),
                head,
                last_timestamp: Some(last.timestamp_ms),
            }
        }
    })
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
