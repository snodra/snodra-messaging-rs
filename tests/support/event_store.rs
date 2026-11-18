//! A fault-injecting event store for the tests that need one: the in-memory event store
//! (`event_store::memory`, two segments per source) behind a wrapper that declares fewer
//! capabilities, breaks `next` on demand, never answers a publish or a close, and counts the
//! subscriptions made. A test that needs none of that uses `MemoryEventStore` itself.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use snodra_messaging::event_store::browse::{BrowseSettings, EventBrowser};
use snodra_messaging::event_store::memory::{MemoryEventStore, MemoryEventStoreConfig};
use snodra_messaging::event_store::{
    Capability, EventStore, EventStoreError, OutgoingEvent, SegmentStatus, Subscription,
    SubscriptionAdmin, SubscriptionEvent, SubscriptionSpec,
};

/// Segments per source.
pub const SEGMENTS: usize = 2;

/// A call of the double that never answers.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Stuck {
    Publish,
    Close,
}

/// The event store: every clone is the same one, shared by every "instance" of a test.
#[derive(Clone)]
pub struct Log {
    inner: MemoryEventStore,
    capabilities: Vec<Capability>,
    stuck: Option<Stuck>,
    /// How many more `next` calls fail (a broken subscription).
    failures: Arc<AtomicUsize>,
    /// Every subscription made, in order.
    subscribed: Arc<Mutex<Vec<SubscriptionSpec>>>,
}

impl Log {
    /// An event store declaring `capabilities` (of the in-memory store's `replay`, `browse` and
    /// `lag`).
    pub fn new(capabilities: Vec<Capability>) -> Log {
        Log::with(capabilities, BrowseSettings::default())
    }

    fn with(capabilities: Vec<Capability>, browse: BrowseSettings) -> Log {
        let inner = MemoryEventStore::new(MemoryEventStoreConfig {
            segments: SEGMENTS,
            browse,
            ..MemoryEventStoreConfig::default()
        })
        .expect("a valid configuration");
        assert!(
            capabilities.iter().all(|c| inner.supports(*c)),
            "the double declares only what the in-memory store has: {capabilities:?}"
        );
        Log {
            inner,
            capabilities,
            stuck: None,
            failures: Arc::default(),
            subscribed: Arc::default(),
        }
    }

    /// An event store declaring every capability the in-memory store has.
    pub fn full() -> Log {
        Log::new(vec![
            Capability::Replay,
            Capability::Browse,
            Capability::Lag,
        ])
    }

    /// The browser reads these sources (a new store: call it before using the double).
    pub fn browsing(self, sources: &[&str]) -> Log {
        let browse = BrowseSettings {
            sources: sources.iter().map(|s| s.to_string()).collect(),
            ..BrowseSettings::default()
        };
        Log {
            stuck: self.stuck,
            ..Log::with(self.capabilities, browse)
        }
    }

    pub fn stuck(self, stuck: Stuck) -> Log {
        Log {
            stuck: Some(stuck),
            ..self
        }
    }

    /// The next `n` calls of `next` (of any subscription) fail: the subscription is broken.
    pub fn fail_next(&self, n: usize) {
        self.failures.store(n, AtomicOrdering::SeqCst);
    }

    /// How many subscriptions were made of `name`.
    pub fn subscriptions_of(&self, name: &str) -> usize {
        self.subscribed
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.name == name)
            .count()
    }

    /// The members reading `spec` now.
    pub fn members(&self, spec: &SubscriptionSpec) -> usize {
        self.inner
            .admin(spec)
            .and_then(|admin| admin.members())
            .map_or(0, |members| members.len())
    }
}

impl EventStore for Log {
    fn name(&self) -> &'static str {
        "kit-double"
    }

    fn capabilities(&self) -> &[Capability] {
        &self.capabilities
    }

    fn publish<'a>(
        &'a self,
        event: &'a OutgoingEvent,
    ) -> BoxFuture<'a, Result<(), EventStoreError>> {
        if self.stuck == Some(Stuck::Publish) {
            return Box::pin(std::future::pending());
        }
        self.inner.publish(event)
    }

    fn subscribe(
        &self,
        spec: SubscriptionSpec,
    ) -> BoxFuture<'_, Result<Box<dyn Subscription>, EventStoreError>> {
        self.subscribed.lock().unwrap().push(spec.clone());
        Box::pin(async move {
            let inner = self.inner.subscribe(spec).await?;
            Ok(Box::new(Member {
                inner,
                failures: Arc::clone(&self.failures),
                lag: self.supports(Capability::Lag),
                stuck_close: self.stuck == Some(Stuck::Close),
            }) as Box<dyn Subscription>)
        })
    }

    fn admin(
        &self,
        spec: &SubscriptionSpec,
    ) -> Result<Box<dyn SubscriptionAdmin>, EventStoreError> {
        if !self.supports(Capability::Replay) {
            return Err(EventStoreError::Unsupported(Capability::Replay));
        }
        self.inner.admin(spec)
    }

    fn compare(&self, a: &str, b: &str) -> Ordering {
        self.inner.compare(a, b)
    }

    fn browser(&self) -> Option<Arc<dyn EventBrowser>> {
        self.supports(Capability::Browse)
            .then(|| self.inner.browser())
            .flatten()
    }
}

struct Member {
    inner: Box<dyn Subscription>,
    failures: Arc<AtomicUsize>,
    lag: bool,
    stuck_close: bool,
}

impl Subscription for Member {
    fn next(&mut self) -> BoxFuture<'_, Result<SubscriptionEvent, EventStoreError>> {
        let broken = self
            .failures
            .try_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |n| {
                n.checked_sub(1)
            })
            .is_ok();
        if broken {
            return Box::pin(std::future::ready(Err(EventStoreError::Failed(
                "the double broke".into(),
            ))));
        }
        self.inner.next()
    }

    fn ack(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError> {
        self.inner.ack(segment, position)
    }

    fn redeliver(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError> {
        self.inner.redeliver(segment, position)
    }

    fn pause(&mut self) {
        self.inner.pause();
    }

    fn resume(&mut self) {
        self.inner.resume();
    }

    /// Without `lag`, no lag (as the contract wants).
    fn segments(&self) -> Vec<SegmentStatus> {
        let mut segments = self.inner.segments();
        if !self.lag {
            segments.iter_mut().for_each(|s| s.lag = None);
        }
        segments
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        if self.stuck_close {
            // Never answers, and never leaves either.
            return Box::pin(async move {
                let _held = self;
                std::future::pending::<()>().await
            });
        }
        self.inner.close()
    }
}
