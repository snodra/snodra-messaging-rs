//! Keeping a subscription (the `Subscription` contract): a `Failed` from `next` ends it, so the
//! reader closes it and subscribes again after a backoff ([`RESUBSCRIBE_MIN`] doubling to
//! [`RESUBSCRIBE_MAX`]). The backoff starts over once a subscription delivered a record or lasted
//! [`RESUBSCRIBE_MAX`]; one that is only assigned and then fails each time keeps backing off. Shared
//! by the processors and the external sources.

use std::time::Duration;

use futures::future::BoxFuture;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::event_store::{EventStoreError, Subscription};

/// The first wait before subscribing again after a subscription failed.
pub const RESUBSCRIBE_MIN: Duration = Duration::from_secs(1);
/// The longest wait before subscribing again.
pub const RESUBSCRIBE_MAX: Duration = Duration::from_secs(30);

/// How a read of a subscription ended.
pub(crate) enum Ended {
    /// The reader stops (its cancel, or a shutdown in the middle of a record).
    Stopped,
    /// The subscription failed: subscribe again. `delivered`: it handed out a record first.
    Failed {
        error: EventStoreError,
        delivered: bool,
    },
}

/// A reader of one subscription that [`keep_subscribed`] keeps subscribed.
pub(crate) trait Resubscribing: Sync {
    /// Joins the subscription once more.
    fn subscribe(&self) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>>;

    /// Reads `subscription` until `cancel` (or a shutdown mid-record) or its failure.
    fn read<'a>(
        &'a self,
        subscription: &'a mut dyn Subscription,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Ended>;

    /// The subscription failed with `error`: the next one is made after `backoff`.
    fn failed(&self, error: &EventStoreError, backoff: Duration);
}

/// Subscribes, reads until the read ends, closes the subscription, and on a failure (of the
/// subscribe or of the read) tells the reader, waits and subscribes again; returns at `cancel` or
/// when a read stops.
pub(crate) async fn keep_subscribed(reader: &impl Resubscribing, cancel: &CancellationToken) {
    let mut backoff = RESUBSCRIBE_MIN;
    loop {
        let subscribed = tokio::select! {
            _ = cancel.cancelled() => return,
            subscribed = reader.subscribe() => subscribed,
        };
        let failure = match subscribed {
            Err(e) => e,
            Ok(mut subscription) => {
                let started = Instant::now();
                let ended = reader.read(&mut *subscription, cancel).await;
                subscription.close().await;
                match ended {
                    Ended::Stopped => return,
                    Ended::Failed { error, delivered } => {
                        if delivered || started.elapsed() >= RESUBSCRIBE_MAX {
                            backoff = RESUBSCRIBE_MIN;
                        }
                        error
                    }
                }
            }
        };
        reader.failed(&failure, backoff);
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(RESUBSCRIBE_MAX);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::event_store::{SegmentStatus, SubscriptionEvent};

    struct Nothing;

    impl Subscription for Nothing {
        fn next(&mut self) -> BoxFuture<'_, Result<SubscriptionEvent, EventStoreError>> {
            Box::pin(std::future::pending())
        }
        fn ack(&mut self, _: &str, _: &str) -> Result<(), EventStoreError> {
            Ok(())
        }
        fn redeliver(&mut self, _: &str, _: &str) -> Result<(), EventStoreError> {
            Ok(())
        }
        fn pause(&mut self) {}
        fn resume(&mut self) {}
        fn segments(&self) -> Vec<SegmentStatus> {
            Vec::new()
        }
        fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    /// Reads that end as `script` says (`true`: it delivered a record first), then a stop.
    struct Scripted {
        script: Mutex<std::vec::IntoIter<bool>>,
        waits: Mutex<Vec<Duration>>,
    }

    impl Resubscribing for Scripted {
        fn subscribe(&self) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
            Box::pin(async { Ok(Box::new(Nothing) as Box<dyn Subscription>) })
        }

        fn read<'a>(
            &'a self,
            _: &'a mut dyn Subscription,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Ended> {
            let ended = match self.script.lock().unwrap().next() {
                Some(delivered) => Ended::Failed {
                    error: EventStoreError::Failed("broke".into()),
                    delivered,
                },
                None => Ended::Stopped,
            };
            Box::pin(async move { ended })
        }

        fn failed(&self, _: &EventStoreError, backoff: Duration) {
            self.waits.lock().unwrap().push(backoff);
        }
    }

    /// The waits `keep_subscribed` announced for `script`.
    async fn waits(script: Vec<bool>) -> Vec<Duration> {
        let reader = Scripted {
            script: Mutex::new(script.into_iter()),
            waits: Mutex::new(Vec::new()),
        };
        keep_subscribed(&reader, &CancellationToken::new()).await;
        reader.waits.into_inner().unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn the_backoff_doubles_until_a_record_is_delivered() {
        let s = Duration::from_secs;
        assert_eq!(
            waits(vec![false; 7]).await,
            [s(1), s(2), s(4), s(8), s(16), s(30), s(30)],
            "assigned and failing each time: it keeps backing off"
        );
        assert_eq!(
            waits(vec![false, false, true, false]).await,
            [s(1), s(2), s(1), s(2)],
            "a delivery starts it over"
        );
    }
}
