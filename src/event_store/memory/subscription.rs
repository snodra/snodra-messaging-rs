//! One instance's subscription of the in-memory event store.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use futures::future::BoxFuture;

use super::{Shared, parse_position};
use crate::event_store::{
    Delivery, EventStoreError, SegmentStatus, Subscription, SubscriptionEvent, SubscriptionSpec,
};

pub(super) struct MemorySubscription {
    shared: Arc<Shared>,
    spec: SubscriptionSpec,
    member: u64,
    /// The segments this subscription holds, with the next position it delivers of each.
    cursors: HashMap<usize, u64>,
    paused: bool,
    /// Where the next delivery is looked for first, so a busy segment cannot starve the others.
    turn: usize,
    left: bool,
    /// A `Revoked` was given and no `Assigned` since: the next step closes the change.
    changing: bool,
}

impl MemorySubscription {
    pub(super) fn new(shared: Arc<Shared>, spec: SubscriptionSpec, member: u64) -> Self {
        MemorySubscription {
            shared,
            spec,
            member,
            cursors: HashMap::new(),
            paused: false,
            turn: 0,
            left: false,
            changing: false,
        }
    }

    fn held(&self) -> BTreeSet<usize> {
        self.cursors.keys().copied().collect()
    }

    /// What `next` gives now, if anything; never waits.
    fn step(&mut self) -> Option<SubscriptionEvent> {
        let shared = Arc::clone(&self.shared);
        let mut state = shared.state();
        let state = &mut *state;
        let group = state.groups.get_mut(&self.spec)?;
        let revoked: Vec<usize> = self
            .held()
            .into_iter()
            .filter(|s| group.target(*s) != Some(self.member))
            .collect();
        if !revoked.is_empty() {
            for s in &revoked {
                self.cursors.remove(s);
                if group.holders.get(s) == Some(&self.member) {
                    group.holders.remove(s);
                }
            }
            shared.wake();
            self.changing = true;
            return Some(SubscriptionEvent::Revoked(names(&revoked)));
        }
        let added: Vec<usize> = (0..shared.segments)
            .filter(|s| {
                group.target(*s) == Some(self.member)
                    && !self.cursors.contains_key(s)
                    && group.holders.get(s).is_none_or(|h| *h == self.member)
            })
            .collect();
        if !added.is_empty() {
            for &s in &added {
                group.holders.insert(s, self.member);
                self.cursors
                    .insert(s, group.committed.get(&s).copied().unwrap_or(0));
            }
            self.changing = false;
            return Some(SubscriptionEvent::Assigned(names(&added)));
        }
        if std::mem::take(&mut self.changing) {
            // Every revocation is closed by an assignment, an empty one when nothing new came.
            return Some(SubscriptionEvent::Assigned(Vec::new()));
        }
        if self.paused || self.cursors.is_empty() {
            return None;
        }
        let log = state.sources.get(&self.spec.source)?;
        let held: Vec<usize> = self.held().into_iter().collect();
        for i in 0..held.len() {
            let s = held[(self.turn + i) % held.len()];
            let at = self.cursors[&s];
            if let Some(r) = log[s].get(at as usize) {
                self.cursors.insert(s, at + 1);
                self.turn = (self.turn + i + 1) % held.len();
                return Some(SubscriptionEvent::Delivery(Delivery {
                    segment: s.to_string(),
                    position: at.to_string(),
                    key: r.key.clone(),
                    headers: r.headers.clone(),
                    payload: r.payload.clone(),
                    timestamp_ms: r.timestamp_ms,
                }));
            }
        }
        None
    }

    /// The held segment and the position named, checked against its head. `None` for a segment
    /// of the source this subscription does not hold (revoked meanwhile).
    fn place(
        &self,
        segment: &str,
        position: &str,
    ) -> Result<Option<(usize, u64)>, EventStoreError> {
        let s = self.shared.segment(segment)?;
        let at = parse_position(position)?;
        let head = self.shared.state().head(&self.spec.source, s);
        if at >= head {
            return Err(EventStoreError::Invalid(format!(
                "segment {s} holds no event at {at} (head {head})"
            )));
        }
        Ok(self.cursors.contains_key(&s).then_some((s, at)))
    }

    fn leave(&mut self) {
        if std::mem::replace(&mut self.left, true) {
            return;
        }
        {
            let mut state = self.shared.state();
            if let Some(group) = state.groups.get_mut(&self.spec) {
                group.leave(self.member);
            }
        }
        self.shared.wake();
    }
}

fn names(segments: &[usize]) -> Vec<String> {
    segments.iter().map(usize::to_string).collect()
}

impl Subscription for MemorySubscription {
    /// Waits on the store itself (no polling): an append, a member joining or leaving, a segment
    /// let go. Cancel-safe: a step that gives something gives it at once.
    fn next(&mut self) -> BoxFuture<'_, Result<SubscriptionEvent, EventStoreError>> {
        Box::pin(async move {
            loop {
                let shared = Arc::clone(&self.shared);
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(event) = self.step() {
                    return Ok(event);
                }
                changed.await;
            }
        })
    }

    /// Commits at once. An acknowledgement of a segment this subscription no longer holds is
    /// dropped, as the segment's new holder resumes from the committed position.
    fn ack(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError> {
        let Some((s, at)) = self.place(segment, position)? else {
            return Ok(());
        };
        let mut state = self.shared.state();
        if let Some(group) = state.groups.get_mut(&self.spec) {
            let committed = group.committed.entry(s).or_insert(0);
            *committed = (*committed).max(at + 1);
        }
        Ok(())
    }

    /// Ignored for a segment this subscription no longer holds.
    fn redeliver(&mut self, segment: &str, position: &str) -> Result<(), EventStoreError> {
        if let Some((s, at)) = self.place(segment, position)? {
            self.cursors.insert(s, at);
        }
        Ok(())
    }

    fn pause(&mut self) {
        self.paused = true;
    }

    fn resume(&mut self) {
        self.paused = false;
    }

    fn segments(&self) -> Vec<SegmentStatus> {
        let state = self.shared.state();
        let committed = state.groups.get(&self.spec).map(|g| &g.committed);
        self.held()
            .into_iter()
            .map(|s| {
                let head = state.head(&self.spec.source, s);
                let position = self.cursors[&s];
                let done = committed.and_then(|c| c.get(&s)).copied();
                SegmentStatus {
                    segment: s.to_string(),
                    position: Some(position.to_string()),
                    committed: done.map(|c| c.to_string()),
                    head: Some(head.to_string()),
                    lag: Some(head.saturating_sub(done.unwrap_or(position))),
                }
            })
            .collect()
    }

    fn close(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        self.leave();
        Box::pin(std::future::ready(()))
    }
}

impl Drop for MemorySubscription {
    /// A subscription dropped without a close (a task that ended or panicked) leaves too.
    fn drop(&mut self) {
        self.leave();
    }
}
