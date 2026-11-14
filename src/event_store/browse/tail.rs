//! A tail's two ends: the [`TailFeed`] an add-on hands the start and the events to (from a
//! blocking thread or a task), and the [`TailStream`] the caller reads: `start`, the events, a
//! heartbeat whenever nothing came for a heartbeat period, `end`.

use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::{mpsc, oneshot};

use super::queries::{
    EndReason, Heartbeat, MAX_HEARTBEAT, MIN_HEARTBEAT, TailEnd, TailEvent, TailStart,
};
use super::records::EventSummary;
use crate::time::now_ms;

/// What [`EventBrowser::tail`](super::EventBrowser::tail) gives: [`TailEvent`]s, `start` first
/// and `end` last. Dropping it ends the tail (the add-on stops at its next read).
pub type TailStream = BoxStream<'static, TailEvent>;

/// The add-on's end of a tail.
pub struct TailFeed {
    start: Option<oneshot::Sender<TailStart>>,
    records: mpsc::Sender<EventSummary>,
    end: Option<oneshot::Sender<EndReason>>,
}

impl TailFeed {
    /// The tail started (it reads from the head of every segment): the stream's first part. The
    /// first call counts; call it before the first [`record`](Self::record).
    pub fn start(&mut self, start: TailStart) {
        if let Some(tx) = self.start.take() {
            let _ = tx.send(start);
        }
    }

    /// Hands `summary` to the reader. `false` when the tail is over: the reader left, or it
    /// already holds a whole buffer of events it has not taken, which ends the tail
    /// (`slowReader`) at once. Never waits.
    pub fn record(&mut self, summary: EventSummary) -> bool {
        match self.records.try_send(summary) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.end(EndReason::SlowReader);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// The reader left (dropped the stream): stop reading.
    pub fn is_closed(&self) -> bool {
        self.records.is_closed()
    }

    /// Ends the tail with `reason` (the first reason given counts). A feed dropped without one
    /// ends it `failed`.
    pub fn end(&mut self, reason: EndReason) {
        if let Some(end) = self.end.take() {
            let _ = end.send(reason);
        }
    }
}

/// The reader's side, unfolded into the stream.
struct Reader {
    start: Option<oneshot::Receiver<TailStart>>,
    records: mpsc::Receiver<EventSummary>,
    records_closed: bool,
    end: oneshot::Receiver<EndReason>,
    heartbeat: tokio::time::Interval,
    done: bool,
}

/// A tail's channel. The feed holds at most `buffer` (at least 1) events the reader has not
/// taken; the start has a place of its own. The stream gives the feed's start first, then the
/// events, and a heartbeat whenever no part came for `heartbeat` (clamped to [`MIN_HEARTBEAT`]
/// .. [`MAX_HEARTBEAT`]). Its `end` is sent as soon as the feed gives it, before the events still
/// buffered; a feed dropped before its start ends the stream `failed`. Call it inside a Tokio
/// runtime.
pub fn tail_channel(buffer: usize, heartbeat: Duration) -> (TailFeed, TailStream) {
    let heartbeat = heartbeat.clamp(MIN_HEARTBEAT, MAX_HEARTBEAT);
    let (start_tx, start) = oneshot::channel();
    let (records_tx, records) = mpsc::channel(buffer.max(1));
    let (end_tx, end) = oneshot::channel();
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + heartbeat, heartbeat);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let reader = Reader {
        start: Some(start),
        records,
        records_closed: false,
        end,
        heartbeat: ticks,
        done: false,
    };
    let stream = futures::stream::unfold(reader, |mut r| async move {
        if r.done {
            return None;
        }
        if let Some(start) = r.start.take() {
            return match start.await {
                Ok(start) => {
                    r.heartbeat.reset();
                    Some((TailEvent::Start(start), r))
                }
                // No start: the feed failed.
                Err(_) => {
                    r.done = true;
                    let end = TailEnd {
                        reason: EndReason::Failed,
                    };
                    Some((TailEvent::End(end), r))
                }
            };
        }
        loop {
            tokio::select! {
                biased;
                reason = &mut r.end => {
                    r.done = true;
                    let reason = reason.unwrap_or(EndReason::Failed);
                    return Some((TailEvent::End(TailEnd { reason }), r));
                }
                record = r.records.recv(), if !r.records_closed => match record {
                    Some(summary) => {
                        r.heartbeat.reset();
                        return Some((TailEvent::Record(summary), r));
                    }
                    // The feed is done; its end follows.
                    None => r.records_closed = true,
                },
                _ = r.heartbeat.tick() => {
                    return Some((TailEvent::Heartbeat(Heartbeat { now_ms: now_ms() }), r));
                }
            }
        }
    });
    let feed = TailFeed {
        start: Some(start_tx),
        records: records_tx,
        end: Some(end_tx),
    };
    (feed, stream.boxed())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn start() -> TailStart {
        TailStart {
            source: "s".into(),
            segments: 2,
            ends_at_ms: 0,
        }
    }

    fn summary(position: u64) -> EventSummary {
        EventSummary {
            source: "s".into(),
            segment: "0".into(),
            position: position.to_string(),
            timestamp: None,
            key: None,
            key_truncated: false,
            headers: BTreeMap::new(),
            size: 0,
        }
    }

    fn kinds(parts: &[TailEvent]) -> Vec<String> {
        parts
            .iter()
            .map(|p| match p {
                TailEvent::Start(_) => "start".to_string(),
                TailEvent::Record(s) => format!("record {}", s.position),
                TailEvent::Heartbeat(_) => "heartbeat".to_string(),
                TailEvent::End(e) => format!("end {:?}", e.reason),
            })
            .collect()
    }

    #[tokio::test]
    async fn a_tail_starts_delivers_and_ends_with_its_reason() {
        let (mut feed, stream) = tail_channel(8, Duration::from_secs(3600));
        feed.start(start());
        assert!(feed.record(summary(1)) && feed.record(summary(2)));
        feed.end(EndReason::MaxDuration);
        feed.end(EndReason::Failed);
        drop(feed);
        let parts: Vec<TailEvent> = stream.collect().await;
        assert_eq!(
            kinds(&parts),
            ["start", "end MaxDuration"],
            "the end comes first"
        );
    }

    #[tokio::test]
    async fn records_come_in_order_while_the_tail_runs() {
        let (mut feed, mut stream) = tail_channel(8, Duration::from_secs(3600));
        feed.start(start());
        assert_eq!(kinds(&[stream.next().await.unwrap()]), ["start"]);
        feed.record(summary(1));
        feed.record(summary(2));
        let two = vec![stream.next().await.unwrap(), stream.next().await.unwrap()];
        assert_eq!(kinds(&two), ["record 1", "record 2"]);
        drop(feed);
        assert_eq!(kinds(&[stream.next().await.unwrap()]), ["end Failed"]);
        assert!(stream.next().await.is_none(), "nothing after the end");
    }

    /// The buffer is exact: the start has its own place, so a reader that took the start and
    /// nothing else is ended when it holds `buffer` events and one more comes.
    #[tokio::test]
    async fn a_tail_holds_exactly_its_buffer() {
        let (mut feed, mut stream) = tail_channel(3, Duration::from_secs(3600));
        feed.start(start());
        assert_eq!(kinds(&[stream.next().await.unwrap()]), ["start"]);
        for p in 1..=3 {
            assert!(feed.record(summary(p)), "event {p} of a buffer of 3");
        }
        assert!(!feed.record(summary(4)), "the fourth is one too many");
        assert_eq!(kinds(&[stream.next().await.unwrap()]), ["end SlowReader"]);
        let (mut unread, _stream) = tail_channel(3, Duration::from_secs(3600));
        unread.start(start());
        for p in 1..=3 {
            assert!(unread.record(summary(p)), "the unread start takes no place");
        }
        assert!(!unread.record(summary(4)));
    }

    #[tokio::test]
    async fn a_reader_a_buffer_behind_ends_the_tail_slow_reader() {
        let (mut feed, stream) = tail_channel(2, Duration::from_secs(3600));
        feed.start(start());
        assert!(feed.record(summary(1)));
        assert!(feed.record(summary(2)));
        assert!(
            !feed.record(summary(3)),
            "the buffer is full: the tail is over"
        );
        let parts: Vec<TailEvent> = stream.collect().await;
        assert_eq!(kinds(&parts), ["start", "end SlowReader"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_quiet_tail_sends_heartbeats() {
        let (mut feed, mut stream) = tail_channel(2, Duration::from_millis(500));
        feed.start(start());
        stream.next().await.unwrap();
        let before = tokio::time::Instant::now();
        assert_eq!(kinds(&[stream.next().await.unwrap()]), ["heartbeat"]);
        assert_eq!(before.elapsed(), Duration::from_millis(500));
        assert_eq!(kinds(&[stream.next().await.unwrap()]), ["heartbeat"]);
    }

    /// Heartbeats come only while nothing else does: an event starts the period again.
    #[tokio::test(start_paused = true)]
    async fn an_event_puts_the_next_heartbeat_off() {
        let (mut feed, mut stream) = tail_channel(4, Duration::from_millis(500));
        feed.start(start());
        stream.next().await.unwrap();
        let started = tokio::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(300)).await;
        feed.record(summary(1));
        assert_eq!(kinds(&[stream.next().await.unwrap()]), ["record 1"]);
        assert_eq!(kinds(&[stream.next().await.unwrap()]), ["heartbeat"]);
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(800),
            "500 ms after the event, not after the start"
        );
    }

    /// A zero (or huge) period is clamped instead of panicking.
    #[tokio::test(start_paused = true)]
    async fn a_heartbeat_period_is_clamped() {
        let (mut feed, mut stream) = tail_channel(1, Duration::ZERO);
        feed.start(start());
        stream.next().await.unwrap();
        let before = tokio::time::Instant::now();
        assert_eq!(kinds(&[stream.next().await.unwrap()]), ["heartbeat"]);
        assert_eq!(before.elapsed(), MIN_HEARTBEAT);
        let (mut feed, _stream) = tail_channel(1, Duration::MAX);
        feed.start(start());
    }

    #[tokio::test]
    async fn a_feed_dropped_before_its_start_ends_the_tail_failed() {
        let (feed, stream) = tail_channel(2, Duration::from_secs(3600));
        drop(feed);
        let parts: Vec<TailEvent> = stream.collect().await;
        assert_eq!(kinds(&parts), ["end Failed"]);
    }

    #[tokio::test]
    async fn a_reader_that_leaves_closes_the_feed() {
        let (mut feed, stream) = tail_channel(2, Duration::from_secs(3600));
        feed.start(start());
        assert!(!feed.is_closed());
        drop(stream);
        assert!(feed.is_closed());
        assert!(!feed.record(summary(1)));
    }
}
