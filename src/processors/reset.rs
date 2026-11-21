//! The reset of a processor: its target, the replay ranges it makes and the reset
//! handlers' context. The event store moves the positions
//! ([`SubscriptionAdmin`](crate::event_store::SubscriptionAdmin), capability `replay`); positions
//! are ordered by its [`compare`](crate::event_store::EventStore::compare) only.

use std::cmp::Ordering;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::error::MessagingError;
use crate::event_store::{ReplayRange, ResetTarget, segment_order};
use crate::proto;

impl ResetTarget {
    /// The request's target; `None` when it names none.
    pub fn from_proto(target: Option<&proto::ResetTarget>) -> Option<ResetTarget> {
        use proto::reset_target::To;
        Some(match target?.to.as_ref()? {
            To::Earliest(_) => ResetTarget::Earliest,
            To::Latest(_) => ResetTarget::Latest,
            To::TimestampMs(ms) => ResetTarget::Timestamp(*ms),
            To::Positions(p) => ResetTarget::Positions(
                p.positions
                    .iter()
                    .map(|p| (p.segment.clone(), p.position.clone()))
                    .collect(),
            ),
        })
    }

    /// For the log: `earliest`, `latest`, `timestamp 1700000000000`, `positions 0@12,1@40`.
    pub fn describe(&self) -> String {
        match self {
            ResetTarget::Earliest => "earliest".into(),
            ResetTarget::Latest => "latest".into(),
            ResetTarget::Timestamp(ms) => format!("timestamp {ms}"),
            ResetTarget::Positions(all) => format!(
                "positions {}",
                all.iter()
                    .map(|(p, o)| format!("{p}@{o}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        }
    }
}

/// What a reset handler ([`reset_handler`](crate::SnodraMessagingBuilder::reset_handler)) is
/// told: the processor, the operation, the target and the ranges that will be replayed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResetContext {
    pub processor: String,
    pub operation_id: String,
    pub target: ResetTarget,
    /// Per segment of the processor's source, in segment order; never empty (a reset that
    /// replays nothing runs no reset handler).
    pub ranges: Vec<ReplayRange>,
}

/// A registered reset handler.
pub(crate) type ResetHandler =
    Arc<dyn Fn(ResetContext) -> BoxFuture<'static, Result<(), MessagingError>> + Send + Sync>;

/// The order of two positions of one segment (the event store's).
pub type Compare<'a> = &'a dyn Fn(&str, &str) -> Ordering;

/// The replay ranges of a reset whose plan is `moves` (one per segment it moves:
/// [`SubscriptionAdmin::plan`](crate::event_store::SubscriptionAdmin::plan)) over `open` (the
/// ranges still open): only the moves that replay, `[from, until)` with `until` set and `from`
/// before it, never for `latest`; a segment that still has an open range gets
/// `[from, max(until, open until))` (that range's records past `until` were not replayed yet and
/// their markers are gone). In segment order.
pub fn replaying(
    moves: &[ReplayRange],
    target: &ResetTarget,
    open: &[ReplayRange],
    compare: Compare<'_>,
) -> Vec<ReplayRange> {
    if *target == ResetTarget::Latest {
        return Vec::new();
    }
    let mut ranges: Vec<ReplayRange> = moves
        .iter()
        .filter(|m| !m.until.is_empty() && compare(&m.from, &m.until) == Ordering::Less)
        .map(|m| {
            let until = open
                .iter()
                .filter(|r| r.source == m.source && r.segment == m.segment)
                .map(|r| r.until.as_str())
                .fold(m.until.as_str(), |a, b| {
                    if compare(a, b) == Ordering::Less {
                        b
                    } else {
                        a
                    }
                });
            ReplayRange {
                until: until.to_string(),
                ..m.clone()
            }
        })
        .collect();
    ranges.sort_by(|a, b| segment_order(&a.segment, &b.segment));
    ranges
}

/// The replay ranges after the reset: `open` without the ranges of the segments that got a new
/// one, then the new ones; sorted by source, segment (in segment order) and `from`.
pub fn merged(open: &[ReplayRange], new: &[ReplayRange], compare: Compare<'_>) -> Vec<ReplayRange> {
    let mut all: Vec<ReplayRange> = open
        .iter()
        .filter(|r| {
            !new.iter()
                .any(|n| n.source == r.source && n.segment == r.segment)
        })
        .cloned()
        .chain(new.iter().cloned())
        .collect();
    all.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then_with(|| segment_order(&a.segment, &b.segment))
            .then_with(|| compare(&a.from, &b.from))
    });
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(a: &str, b: &str) -> Ordering {
        a.parse::<i64>().unwrap().cmp(&b.parse::<i64>().unwrap())
    }

    fn range(segment: i32, from: i64, until: &str) -> ReplayRange {
        ReplayRange {
            source: "t".into(),
            segment: segment.to_string(),
            from: from.to_string(),
            until: until.to_string(),
        }
    }

    #[test]
    fn a_range_is_a_move_whose_target_is_before_the_committed_position() {
        let moves = [
            range(10, 0, "10"),
            range(1, 5, "5"),
            range(2, 8, "3"),
            range(3, 0, ""),
            range(0, 9, "10"),
        ];
        assert_eq!(
            replaying(&moves, &ResetTarget::Earliest, &[], &decimal),
            [range(0, 9, "10"), range(10, 0, "10")],
            "not at or past the committed position, not without one; by position, not text; in segment order"
        );
        assert!(
            replaying(&[range(0, 0, "10")], &ResetTarget::Latest, &[], &decimal).is_empty(),
            "latest never makes a range"
        );
    }

    #[test]
    fn an_open_range_of_the_segment_extends_the_new_one_and_others_stay() {
        let open = [
            range(0, 2, "20"),
            range(1, 0, "7"),
            ReplayRange {
                source: "other".into(),
                ..range(0, 0, "99")
            },
        ];
        let moves = [range(0, 4, "12"), range(1, 9, "9")];
        let new = replaying(&moves, &ResetTarget::Timestamp(1), &open, &decimal);
        assert_eq!(new, [range(0, 4, "20")]);
        assert_eq!(
            merged(&open, &new, &decimal),
            [
                ReplayRange {
                    source: "other".into(),
                    ..range(0, 0, "99")
                },
                range(0, 4, "20"),
                range(1, 0, "7")
            ]
        );
    }

    #[test]
    fn targets_from_the_request() {
        use proto::reset_target::To;
        let t = |to| proto::ResetTarget { to: Some(to) };
        assert_eq!(
            ResetTarget::from_proto(Some(&t(To::Earliest(true)))),
            Some(ResetTarget::Earliest)
        );
        assert_eq!(
            ResetTarget::from_proto(Some(&t(To::TimestampMs(7)))),
            Some(ResetTarget::Timestamp(7))
        );
        let positions = To::Positions(proto::SegmentPositions {
            positions: vec![proto::SegmentPosition {
                segment: "1".into(),
                position: "4".into(),
            }],
        });
        assert_eq!(
            ResetTarget::from_proto(Some(&t(positions))).map(|t| t.describe()),
            Some("positions 1@4".to_string())
        );
        assert_eq!(ResetTarget::from_proto(None), None);
        assert_eq!(
            ResetTarget::from_proto(Some(&proto::ResetTarget { to: None })),
            None
        );
    }
}
