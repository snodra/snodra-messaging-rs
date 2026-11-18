//! The reset side of a subscription (ADR 0023, capability `replay`): rdkafka has no admin call to
//! alter a group's offsets, so the admin commits them with a consumer of the group that never
//! subscribes (an assign-mode consumer, generation -1), which the broker refuses while the group
//! has members (`UNKNOWN_MEMBER_ID`, `ILLEGAL_GENERATION` or `REBALANCE_IN_PROGRESS`:
//! [`EventStoreError::SubscribersActive`]). The group is checked empty before with
//! `fetch_group_list`; the watermarks, `offsets_for_times`, the committed offsets and the topic's
//! partitions come from the same consumer. Every call blocks.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::{Offset, TopicPartitionList};
use snodra_messaging::event_store::{EventStoreError, ReplayRange, ResetTarget, SubscriptionAdmin};

use crate::external::reject_fixed_keys;
use crate::{consumer_config, failed};

/// How long one Kafka call of a reset may take.
const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);

/// The Kafka side of a reset of one subscription's group.
pub struct KafkaAdmin {
    consumer: BaseConsumer,
    group: String,
    topic: String,
}

impl KafkaAdmin {
    pub(crate) fn new(
        bootstrap_servers: &str,
        group: &str,
        topic: &str,
        properties: &HashMap<String, String>,
    ) -> Result<KafkaAdmin, EventStoreError> {
        reject_fixed_keys("the Kafka properties", properties).map_err(EventStoreError::Invalid)?;
        let consumer: BaseConsumer = consumer_config(bootstrap_servers, group, properties)
            .create()
            .map_err(failed)?;
        Ok(KafkaAdmin {
            consumer,
            group: group.to_string(),
            topic: topic.to_string(),
        })
    }

    fn partitions(&self) -> Result<Vec<i32>, EventStoreError> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), KAFKA_TIMEOUT)
            .map_err(failed)?;
        let topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic)
            .filter(|t| t.error().is_none())
            .ok_or_else(|| EventStoreError::Invalid(format!("topic {} not found", self.topic)))?;
        let mut all: Vec<i32> = topic.partitions().iter().map(|p| p.id()).collect();
        all.sort();
        Ok(all)
    }

    fn watermarks(&self, partition: i32) -> Result<(i64, i64), EventStoreError> {
        self.consumer
            .fetch_watermarks(&self.topic, partition, KAFKA_TIMEOUT)
            .map_err(failed)
    }

    fn committed(&self, partitions: &[i32]) -> Result<HashMap<i32, i64>, EventStoreError> {
        let mut tpl = TopicPartitionList::new();
        for &p in partitions {
            tpl.add_partition(&self.topic, p);
        }
        let committed = self
            .consumer
            .committed_offsets(tpl, KAFKA_TIMEOUT)
            .map_err(failed)?;
        Ok(partitions
            .iter()
            .filter_map(
                |&p| match committed.find_partition(&self.topic, p)?.offset() {
                    Offset::Offset(o) => Some((p, o)),
                    _ => None,
                },
            )
            .collect())
    }

    /// Where `target` moves each partition it addresses.
    fn targets(&self, target: &ResetTarget) -> Result<BTreeMap<i32, i64>, EventStoreError> {
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
                        .map_err(failed)?;
                }
                let found = self
                    .consumer
                    .offsets_for_times(tpl, KAFKA_TIMEOUT)
                    .map_err(failed)?;
                for &p in &partitions {
                    let offset = match found.find_partition(&self.topic, p).map(|e| e.offset()) {
                        Some(Offset::Offset(o)) if o >= 0 => o,
                        // No record that late: the end.
                        _ => self.watermarks(p)?.1,
                    };
                    targets.insert(p, offset);
                }
            }
            ResetTarget::Positions(given) => {
                if given.is_empty() {
                    return Err(EventStoreError::Invalid("no positions given".into()));
                }
                for (segment, position) in given {
                    let invalid = |why: String| Err(EventStoreError::Invalid(why));
                    let Ok(p) = segment.parse::<i32>() else {
                        return invalid(format!("topic {} has no partition {segment}", self.topic));
                    };
                    let Ok(offset) = position.parse::<i64>() else {
                        return invalid(format!(
                            "position {position} of partition {p} is not an offset"
                        ));
                    };
                    if !partitions.contains(&p) {
                        return invalid(format!("topic {} has no partition {p}", self.topic));
                    }
                    if offset < 0 {
                        return invalid(format!("offset {offset} of partition {p} is negative"));
                    }
                    let high = self.watermarks(p)?.1;
                    if offset > high {
                        return invalid(format!(
                            "offset {offset} of partition {p} is past its end ({high})"
                        ));
                    }
                    if targets.insert(p, offset).is_some() {
                        return invalid(format!("partition {p} is given twice"));
                    }
                }
            }
        }
        Ok(targets)
    }
}

impl SubscriptionAdmin for KafkaAdmin {
    fn members(&self) -> Result<Vec<String>, EventStoreError> {
        let groups = self
            .consumer
            .fetch_group_list(Some(&self.group), KAFKA_TIMEOUT)
            .map_err(failed)?;
        Ok(groups
            .groups()
            .iter()
            .filter(|g| g.name() == self.group)
            .flat_map(|g| g.members().iter().map(|m| m.client_id().to_string()))
            .collect())
    }

    fn plan(&self, target: &ResetTarget) -> Result<Vec<ReplayRange>, EventStoreError> {
        let targets = self.targets(target)?;
        let committed = self.committed(&targets.keys().copied().collect::<Vec<_>>())?;
        Ok(targets
            .into_iter()
            .map(|(p, to)| ReplayRange {
                source: self.topic.clone(),
                segment: p.to_string(),
                from: to.to_string(),
                until: committed.get(&p).map(i64::to_string).unwrap_or_default(),
            })
            .collect())
    }

    fn apply(&self, ranges: &[ReplayRange]) -> Result<(), EventStoreError> {
        let mut tpl = TopicPartitionList::new();
        for r in ranges {
            let invalid = |why: String| EventStoreError::Invalid(why);
            if r.source != self.topic {
                return Err(invalid(format!(
                    "a range of {} for the subscription of {}",
                    r.source, self.topic
                )));
            }
            let partition: i32 = r
                .segment
                .parse()
                .map_err(|_| invalid(format!("segment {} is not a partition", r.segment)))?;
            let offset: i64 = r
                .from
                .parse()
                .map_err(|_| invalid(format!("position {} is not an offset", r.from)))?;
            if offset < 0 {
                return Err(invalid(format!(
                    "offset {offset} of partition {partition} is negative"
                )));
            }
            tpl.add_partition_offset(&self.topic, partition, Offset::Offset(offset))
                .map_err(failed)?;
        }
        if tpl.count() == 0 {
            return Ok(());
        }
        let members = self.members()?;
        if !members.is_empty() {
            return Err(EventStoreError::SubscribersActive(members.join(", ")));
        }
        match self.consumer.commit(&tpl, CommitMode::Sync) {
            Ok(()) => Ok(()),
            Err(KafkaError::ConsumerCommit(
                code @ (RDKafkaErrorCode::UnknownMemberId
                | RDKafkaErrorCode::IllegalGeneration
                | RDKafkaErrorCode::RebalanceInProgress),
            )) => Err(EventStoreError::SubscribersActive(format!(
                "Kafka refused the offsets of group {}: {code} (the group has members)",
                self.group
            ))),
            Err(e) => Err(failed(e)),
        }
    }
}
