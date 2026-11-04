//! The reset of a processor (ADR 0023): its target, the replay ranges it makes, the reset
//! handlers' context, the markers it deletes and the Kafka calls of the executing instance.
//!
//! rdkafka has no admin call to alter a group's offsets: the executor commits them with a
//! consumer of the group that never subscribes (an assign-mode consumer, generation -1), which
//! the broker refuses while the group has members (`UNKNOWN_MEMBER_ID`, `ILLEGAL_GENERATION` or
//! `REBALANCE_IN_PROGRESS`: [`ResetFailure::GroupNotEmpty`]). The group is checked empty before
//! with `fetch_group_list`; the watermarks, `offsets_for_times`, the committed offsets and the
//! topic's partitions come from the same consumer. Every call blocks: run them on a blocking
//! thread.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use mongodb::bson::{Document, Regex, doc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::{Offset, TopicPartitionList};

use super::DEFAULT_PROCESSOR;
use super::control_store::ReplayRange;
use crate::error::MessagingError;
use crate::proto;

/// How long one Kafka call of a reset may take.
const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);

/// Where a reset moves the processor's consumer group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResetTarget {
    /// Every partition's low watermark: everything Kafka still has is a replay.
    Earliest,
    /// Every partition's high watermark: nothing is replayed.
    Latest,
    /// The first offset at or after this time (ms since the epoch) per partition, else its high
    /// watermark.
    Timestamp(i64),
    /// These partitions to these offsets; a partition not listed keeps its committed offset.
    Offsets(Vec<(i32, i64)>),
}

impl ResetTarget {
    /// The request's target; `None` when it names none.
    pub fn from_proto(target: Option<&proto::ResetTarget>) -> Option<ResetTarget> {
        use proto::reset_target::To;
        Some(match target?.to.as_ref()? {
            To::Earliest(_) => ResetTarget::Earliest,
            To::Latest(_) => ResetTarget::Latest,
            To::TimestampMs(ms) => ResetTarget::Timestamp(*ms),
            To::Offsets(o) => {
                ResetTarget::Offsets(o.offsets.iter().map(|p| (p.partition, p.offset)).collect())
            }
        })
    }

    /// For the log: `earliest`, `latest`, `timestamp 1700000000000`, `offsets 0@12,1@40`.
    pub fn describe(&self) -> String {
        match self {
            ResetTarget::Earliest => "earliest".into(),
            ResetTarget::Latest => "latest".into(),
            ResetTarget::Timestamp(ms) => format!("timestamp {ms}"),
            ResetTarget::Offsets(all) => format!(
                "offsets {}",
                all.iter()
                    .map(|(p, o)| format!("{p}@{o}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        }
    }
}

/// What a reset handler ([`reset_handler`](crate::KrakenMessagingBuilder::reset_handler)) is
/// told: the processor, the operation, the target and the ranges that will be replayed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResetContext {
    pub processor: String,
    pub operation_id: String,
    pub target: ResetTarget,
    /// Per partition of the processor's topic, sorted by partition; never empty (a reset that
    /// replays nothing runs no reset handler).
    pub ranges: Vec<ReplayRange>,
}

/// A registered reset handler.
pub(crate) type ResetHandler =
    Arc<dyn Fn(ResetContext) -> BoxFuture<'static, Result<(), MessagingError>> + Send + Sync>;

/// Why a reset's Kafka work refused or failed; nothing changed.
#[derive(Debug, PartialEq, Eq)]
pub enum ResetFailure {
    /// A bad target: an unknown partition, a negative offset or one past the high watermark.
    Invalid(String),
    /// The group has members (their client ids), or the broker refused the commit for them.
    GroupNotEmpty(String),
    /// A Kafka error.
    Failed(String),
}

/// What step 1 and 2 of an execute found: the target offset of each partition it moves, the
/// committed offset before (-1 none) of each.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    /// Partition → (target, committed before).
    pub partitions: BTreeMap<i32, (i64, i64)>,
}

impl Plan {
    /// The replay ranges of this reset over `open` (the ranges still open): `[target, before)`
    /// where `target < before`, never for `latest` and never for a partition without a committed
    /// offset; a partition that still has an open range gets `[target, max(before, open until))`
    /// (that range's records past `before` were not replayed yet and their markers are gone).
    /// Sorted by partition.
    pub fn ranges(
        &self,
        topic: &str,
        target: &ResetTarget,
        open: &[ReplayRange],
    ) -> Vec<ReplayRange> {
        if *target == ResetTarget::Latest {
            return Vec::new();
        }
        self.partitions
            .iter()
            .filter(|(_, (to, before))| *before >= 0 && to < before)
            .map(|(&partition, &(from, before))| {
                let until = open
                    .iter()
                    .filter(|r| r.topic == topic && r.partition == partition)
                    .map(|r| r.until)
                    .fold(before, i64::max);
                ReplayRange {
                    topic: topic.to_string(),
                    partition,
                    from,
                    until,
                }
            })
            .collect()
    }

    /// The replay ranges after the reset: `open` without the ranges of the partitions that got a
    /// new one, then the new ones (sorted by topic and partition).
    pub fn merged(open: &[ReplayRange], new: &[ReplayRange]) -> Vec<ReplayRange> {
        let mut all: Vec<ReplayRange> = open
            .iter()
            .filter(|r| {
                !new.iter()
                    .any(|n| n.topic == r.topic && n.partition == r.partition)
            })
            .cloned()
            .chain(new.iter().cloned())
            .collect();
        all.sort_by(|a, b| (&a.topic, a.partition, a.from).cmp(&(&b.topic, b.partition, b.from)));
        all
    }
}

/// The markers of `processor` in `range`: `{processor, topic, partition, offset: [from, until)}`.
pub fn range_filter(processor: &str, range: &ReplayRange) -> Document {
    doc! {
        "processor": processor,
        "topic": &range.topic,
        "partition": range.partition,
        "offset": { "$gte": range.from, "$lt": range.until },
    }
}

/// The markers of `processor` written before P6b (no `processor` field), deleted by a reset to
/// `earliest` only: for `default`, an `_id` without a `:` (every other marker family has one:
/// `processor:`, `saga:`, `ingress:`, `failed:`, a partner topic's `<source>:...`; a Kafka event id
/// is a UUID or `<topic>-<partition>-<offset>`, and a topic name has no `:`); for a named
/// processor `P`, `processor:P:` followed by such an id.
pub fn legacy_filter(processor: &str) -> Document {
    let pattern = if processor == DEFAULT_PROCESSOR {
        "^[^:]+$".to_string()
    } else {
        // A processor name is `[a-z0-9-]`: nothing to escape.
        format!("^processor:{processor}:[^:]+$")
    };
    doc! {
        "processor": { "$exists": false },
        "_id": Regex { pattern, options: String::new() },
    }
}

/// The Kafka side of a reset: a consumer of the processor's group that never subscribes.
pub struct GroupAdmin {
    consumer: BaseConsumer,
    group: String,
    topic: String,
}

impl GroupAdmin {
    pub fn new(bootstrap: &str, group: &str, topic: &str) -> Result<GroupAdmin, ResetFailure> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .create()
            .map_err(|e| ResetFailure::Failed(format!("kafka: {e}")))?;
        Ok(GroupAdmin {
            consumer,
            group: group.to_string(),
            topic: topic.to_string(),
        })
    }

    /// Steps 1 and 2 of an execute: the target offset of every partition it moves and the
    /// group's committed offset there.
    pub fn plan(&self, target: &ResetTarget) -> Result<Plan, ResetFailure> {
        let partitions = self.partitions()?;
        let mut targets: BTreeMap<i32, i64> = BTreeMap::new();
        match target {
            ResetTarget::Earliest | ResetTarget::Latest => {
                for &p in &partitions {
                    let (low, high) = self.watermarks(p)?;
                    targets.insert(
                        p,
                        if *target == ResetTarget::Earliest {
                            low
                        } else {
                            high
                        },
                    );
                }
            }
            ResetTarget::Timestamp(ms) => {
                let mut tpl = TopicPartitionList::new();
                for &p in &partitions {
                    tpl.add_partition_offset(&self.topic, p, Offset::Offset(*ms))
                        .map_err(kafka)?;
                }
                let found = self
                    .consumer
                    .offsets_for_times(tpl, KAFKA_TIMEOUT)
                    .map_err(kafka)?;
                for &p in &partitions {
                    let offset = match found.find_partition(&self.topic, p).map(|e| e.offset()) {
                        Some(Offset::Offset(o)) if o >= 0 => o,
                        // No record that late: the end.
                        _ => self.watermarks(p)?.1,
                    };
                    targets.insert(p, offset);
                }
            }
            ResetTarget::Offsets(given) => {
                if given.is_empty() {
                    return Err(ResetFailure::Invalid("no partition offsets given".into()));
                }
                for &(p, offset) in given {
                    if !partitions.contains(&p) {
                        return Err(ResetFailure::Invalid(format!(
                            "topic {} has no partition {p}",
                            self.topic
                        )));
                    }
                    if offset < 0 {
                        return Err(ResetFailure::Invalid(format!(
                            "offset {offset} of partition {p} is negative"
                        )));
                    }
                    let high = self.watermarks(p)?.1;
                    if offset > high {
                        return Err(ResetFailure::Invalid(format!(
                            "offset {offset} of partition {p} is past its end ({high})"
                        )));
                    }
                    if targets.insert(p, offset).is_some() {
                        return Err(ResetFailure::Invalid(format!(
                            "partition {p} is given twice"
                        )));
                    }
                }
            }
        }
        let committed = self.committed(&targets.keys().copied().collect::<Vec<_>>())?;
        Ok(Plan {
            partitions: targets
                .into_iter()
                .map(|(p, to)| (p, (to, committed.get(&p).copied().unwrap_or(-1))))
                .collect(),
        })
    }

    /// Step 3: the client ids of the group's members; empty when the group is empty.
    pub fn members(&self) -> Result<Vec<String>, ResetFailure> {
        let groups = self
            .consumer
            .fetch_group_list(Some(&self.group), KAFKA_TIMEOUT)
            .map_err(kafka)?;
        Ok(groups
            .groups()
            .iter()
            .filter(|g| g.name() == self.group)
            .flat_map(|g| g.members().iter().map(|m| m.client_id().to_string()))
            .collect())
    }

    /// The last step: commits `offsets` (partition, offset) for the group.
    pub fn commit(&self, offsets: &[(i32, i64)]) -> Result<(), ResetFailure> {
        let mut tpl = TopicPartitionList::new();
        for &(p, o) in offsets {
            tpl.add_partition_offset(&self.topic, p, Offset::Offset(o))
                .map_err(kafka)?;
        }
        match self.consumer.commit(&tpl, CommitMode::Sync) {
            Ok(()) => Ok(()),
            Err(KafkaError::ConsumerCommit(
                code @ (RDKafkaErrorCode::UnknownMemberId
                | RDKafkaErrorCode::IllegalGeneration
                | RDKafkaErrorCode::RebalanceInProgress),
            )) => Err(ResetFailure::GroupNotEmpty(format!(
                "Kafka refused the offsets of group {}: {code} (the group has members)",
                self.group
            ))),
            Err(e) => Err(kafka(e)),
        }
    }

    fn partitions(&self) -> Result<Vec<i32>, ResetFailure> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), KAFKA_TIMEOUT)
            .map_err(kafka)?;
        let topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic)
            .filter(|t| t.error().is_none())
            .ok_or_else(|| ResetFailure::Failed(format!("topic {} not found", self.topic)))?;
        let mut all: Vec<i32> = topic.partitions().iter().map(|p| p.id()).collect();
        all.sort();
        Ok(all)
    }

    fn watermarks(&self, partition: i32) -> Result<(i64, i64), ResetFailure> {
        self.consumer
            .fetch_watermarks(&self.topic, partition, KAFKA_TIMEOUT)
            .map_err(kafka)
    }

    fn committed(&self, partitions: &[i32]) -> Result<HashMap<i32, i64>, ResetFailure> {
        let mut tpl = TopicPartitionList::new();
        for &p in partitions {
            tpl.add_partition(&self.topic, p);
        }
        let committed = self
            .consumer
            .committed_offsets(tpl, KAFKA_TIMEOUT)
            .map_err(kafka)?;
        Ok(partitions
            .iter()
            .map(|&p| {
                let offset = match committed.find_partition(&self.topic, p).map(|e| e.offset()) {
                    Some(Offset::Offset(o)) => o,
                    _ => -1,
                };
                (p, offset)
            })
            .collect())
    }
}

fn kafka(e: KafkaError) -> ResetFailure {
    ResetFailure::Failed(format!("kafka: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(partition: i32, from: i64, until: i64) -> ReplayRange {
        ReplayRange {
            topic: "t".into(),
            partition,
            from,
            until,
        }
    }

    fn plan(all: &[(i32, i64, i64)]) -> Plan {
        Plan {
            partitions: all
                .iter()
                .map(|&(p, to, before)| (p, (to, before)))
                .collect(),
        }
    }

    #[test]
    fn a_range_is_target_to_before_when_the_target_is_lower() {
        let p = plan(&[(0, 0, 10), (1, 5, 5), (2, 8, 3), (3, 0, -1)]);
        assert_eq!(
            p.ranges("t", &ResetTarget::Earliest, &[]),
            [range(0, 0, 10)],
            "not at or past before, and not without a commit"
        );
        assert!(
            plan(&[(0, 0, 10)])
                .ranges("t", &ResetTarget::Latest, &[])
                .is_empty(),
            "latest never makes a range"
        );
    }

    #[test]
    fn an_open_range_of_the_partition_extends_the_new_one_and_others_stay() {
        let open = [
            range(0, 2, 20),
            range(1, 0, 7),
            ReplayRange {
                topic: "other".into(),
                ..range(0, 0, 99)
            },
        ];
        let p = plan(&[(0, 4, 12), (1, 9, 9)]);
        let new = p.ranges("t", &ResetTarget::Timestamp(1), &open);
        assert_eq!(new, [range(0, 4, 20)]);
        let merged = Plan::merged(&open, &new);
        assert_eq!(
            merged,
            [
                ReplayRange {
                    topic: "other".into(),
                    ..range(0, 0, 99)
                },
                range(0, 4, 20),
                range(1, 0, 7)
            ]
        );
    }

    #[test]
    fn the_legacy_filters_match_only_kafka_markers_without_a_position() {
        let default = legacy_filter("default");
        assert_eq!(
            default.get_document("processor").unwrap(),
            &doc! { "$exists": false }
        );
        let pattern = |d: &Document| match d.get("_id") {
            Some(mongodb::bson::Bson::RegularExpression(r)) => r.pattern.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(pattern(&default), "^[^:]+$");
        assert_eq!(pattern(&legacy_filter("audit")), "^processor:audit:[^:]+$");
        assert_eq!(
            range_filter("audit", &range(3, 5, 9)),
            doc! { "processor": "audit", "topic": "t", "partition": 3, "offset": { "$gte": 5i64, "$lt": 9i64 } }
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
        let offsets = To::Offsets(proto::PartitionOffsets {
            offsets: vec![proto::PartitionOffset {
                partition: 1,
                offset: 4,
            }],
        });
        assert_eq!(
            ResetTarget::from_proto(Some(&t(offsets))).map(|t| t.describe()),
            Some("offsets 1@4".to_string())
        );
        assert_eq!(ResetTarget::from_proto(None), None);
        assert_eq!(
            ResetTarget::from_proto(Some(&proto::ResetTarget { to: None })),
            None
        );
    }
}
