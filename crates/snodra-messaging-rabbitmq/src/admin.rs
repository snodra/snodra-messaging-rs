//! The reset side of a subscription (capability `replay`): the offsets stored under its name,
//! moved while no consumer of that name reads the source. Who reads it comes from the
//! management API, which the stream protocol has no command for: without the API the store does
//! not declare `replay` and gives no admin.

use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;

use snodra_messaging::event_store::{
    EventStoreError, ReplayRange, ResetTarget, SubscriptionAdmin, SubscriptionSpec, segment_order,
};
use tokio::time::Instant;

use crate::conn::{Broker, Nodes, Part};
use crate::management::Management;
use crate::read::{self, Bounds};
use crate::subscription::Live;

/// The longest one admin call may read the streams for.
const CALL_BUDGET: Duration = Duration::from_secs(20);
/// How long a moved offset may take to be read back.
const STORE_WAIT: Duration = Duration::from_secs(5);

/// Runs `f` to completion on this thread: on the runtime of a blocking thread (a
/// `spawn_blocking` task), else on a runtime of its own (a plain thread).
pub(crate) fn block<F: Future>(f: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle.block_on(f),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime for an admin call")
            .block_on(f),
    }
}

/// The admin of one subscription ([`RabbitEventStore::admin`](crate::RabbitEventStore)). Its
/// calls block: run them on a blocking thread.
pub struct RabbitAdmin {
    broker: Broker,
    management: Management,
    live: Live,
    spec: SubscriptionSpec,
}

/// One partition with its offsets as they are now.
struct Now {
    part: Part,
    stored: Option<u64>,
    bounds: Bounds,
}

impl RabbitAdmin {
    pub(crate) fn new(
        broker: Broker,
        management: Management,
        live: Live,
        spec: &SubscriptionSpec,
    ) -> RabbitAdmin {
        RabbitAdmin {
            broker,
            management,
            live,
            spec: spec.clone(),
        }
    }

    /// Runs `call` with connections of its own, closed after.
    fn with_nodes<T>(
        &self,
        call: impl AsyncFnOnce(&mut Nodes) -> Result<T, EventStoreError>,
    ) -> Result<T, EventStoreError> {
        block(async {
            let mut nodes = Nodes::new(self.broker.clone());
            let result = tokio::time::timeout(CALL_BUDGET, call(&mut nodes))
                .await
                .unwrap_or_else(|_| {
                    Err(EventStoreError::Failed(format!(
                        "rabbitmq: the reset of {} took over {CALL_BUDGET:?}",
                        self.spec.name
                    )))
                });
            nodes.close().await;
            result
        })
    }

    async fn layout(&self, nodes: &mut Nodes) -> Result<Vec<Part>, EventStoreError> {
        let mut parts = nodes.layout(&self.spec.source).await?.ok_or_else(|| {
            EventStoreError::Invalid(format!("the source {} does not exist", self.spec.source))
        })?;
        parts.sort_by(|a, b| segment_order(&a.segment, &b.segment));
        Ok(parts)
    }

    async fn now(&self, nodes: &mut Nodes) -> Result<Vec<Now>, EventStoreError> {
        let deadline = Instant::now() + CALL_BUDGET;
        let mut all = Vec::new();
        for part in self.layout(nodes).await? {
            let conn = nodes.require_leader(&part.stream).await?;
            let stored = conn.stored_offset(&self.spec.name, &part.stream).await?;
            let bounds = read::bounds(&conn, &part.stream, deadline).await?;
            if let Some(stored) = stored.filter(|s| *s > bounds.head) {
                // The stored offset past the head read: one of the two is wrong, so nothing is
                // planned from them.
                return Err(EventStoreError::Failed(format!(
                    "rabbitmq: the offset stored for {} on {} ({stored}) is past the head read ({})",
                    self.spec.name, part.stream, bounds.head
                )));
            }
            all.push(Now {
                part,
                stored,
                bounds,
            });
        }
        Ok(all)
    }
}

fn position_of(position: &str) -> Result<u64, EventStoreError> {
    position
        .parse()
        .map_err(|_| EventStoreError::Invalid(format!("position {position:?} is not an offset")))
}

/// A position the segment does not have: past its head.
fn check_position(now: &Now, position: u64) -> Result<(), EventStoreError> {
    if position > now.bounds.head {
        return Err(EventStoreError::Invalid(format!(
            "position {position} is past the head ({}) of segment {}",
            now.bounds.head, now.part.segment
        )));
    }
    Ok(())
}

impl SubscriptionAdmin for RabbitAdmin {
    /// The members the management API shows (an error, never "none", when it cannot tell),
    /// plus this process's own open subscriptions of the name. The API lists a consumer only
    /// after its next metrics collection (about every 5 s): one another process opened within the
    /// last seconds can be missing.
    fn members(&self) -> Result<Vec<String>, EventStoreError> {
        let streams = self.with_nodes(async |nodes| {
            Ok(self
                .layout(nodes)
                .await?
                .into_iter()
                .map(|p| p.stream)
                .collect::<Vec<_>>())
        })?;
        let mut members: std::collections::BTreeSet<String> =
            block(self.management.members(&self.spec.name, &streams))?
                .into_iter()
                .collect();
        members.extend(self.live.members(&self.spec));
        Ok(members.into_iter().collect())
    }

    fn plan(&self, target: &ResetTarget) -> Result<Vec<ReplayRange>, EventStoreError> {
        self.with_nodes(async |nodes| {
            let now = self.now(nodes).await?;
            let mut ranges = Vec::new();
            let range = |n: &Now, from: u64| ReplayRange {
                source: self.spec.source.clone(),
                segment: n.part.segment.clone(),
                from: from.to_string(),
                until: n.stored.map(|s| s.to_string()).unwrap_or_default(),
            };
            match target {
                ResetTarget::Earliest => {
                    ranges.extend(now.iter().map(|n| range(n, n.bounds.first)))
                }
                ResetTarget::Latest => ranges.extend(now.iter().map(|n| range(n, n.bounds.head))),
                ResetTarget::Timestamp(ts) => {
                    let deadline = Instant::now() + CALL_BUDGET;
                    for n in &now {
                        let conn = nodes.require_leader(&n.part.stream).await?;
                        let from = read::offset_at(&conn, &n.part.stream, *ts, &n.bounds, deadline)
                            .await?;
                        ranges.push(range(n, from));
                    }
                }
                ResetTarget::Positions(positions) => {
                    let mut seen = HashSet::new();
                    for (segment, position) in positions {
                        if !seen.insert(segment.as_str()) {
                            return Err(EventStoreError::Invalid(format!(
                                "segment {segment} is listed twice"
                            )));
                        }
                        let n =
                            now.iter()
                                .find(|n| &n.part.segment == segment)
                                .ok_or_else(|| {
                                    EventStoreError::Invalid(format!(
                                        "the source {} has no segment {segment}",
                                        self.spec.source
                                    ))
                                })?;
                        let from = position_of(position)?;
                        check_position(n, from)?;
                        ranges.push(range(n, from));
                    }
                    ranges.sort_by(|a, b| segment_order(&a.segment, &b.segment));
                }
            }
            Ok(ranges)
        })
    }

    fn apply(&self, ranges: &[ReplayRange]) -> Result<(), EventStoreError> {
        let members = self.members()?;
        if !members.is_empty() {
            return Err(EventStoreError::SubscribersActive(members.join(", ")));
        }
        self.with_nodes(async |nodes| {
            let now = self.now(nodes).await?;
            let mut moves = Vec::with_capacity(ranges.len());
            for r in ranges {
                let n = now
                    .iter()
                    .find(|n| n.part.segment == r.segment && r.source == self.spec.source)
                    .ok_or_else(|| {
                        EventStoreError::Invalid(format!(
                            "the source {} has no segment {}",
                            r.source, r.segment
                        ))
                    })?;
                let from = position_of(&r.from)?;
                check_position(n, from)?;
                moves.push((n.part.stream.clone(), from));
            }
            for (stream, from) in &moves {
                let conn = nodes.require_leader(stream).await?;
                conn.store_offset(&self.spec.name, stream, *from, STORE_WAIT)
                    .await?;
            }
            // Read back once all are stored: an instance that joined meanwhile and stored its
            // own position undoes the reset, which then fails. This rarely catches one (an
            // instance stores periodically, not at once): the guard is the members check before
            // (the management API and this process's own subscriptions).
            for (stream, from) in &moves {
                let conn = nodes.require_leader(stream).await?;
                let now = conn.stored_offset(&self.spec.name, stream).await?;
                if now != Some(*from) {
                    return Err(EventStoreError::SubscribersActive(format!(
                        "the offset of {} on {stream} is {now:?} after the reset stored {from}: an \
                         instance read and stored meanwhile",
                        self.spec.name
                    )));
                }
            }
            Ok(())
        })
    }
}
