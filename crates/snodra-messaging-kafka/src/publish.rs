//! Publishing: one record per event to the topic named by its source.

use std::time::Duration;

use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use snodra_messaging::event_store::{EventStoreError, OutgoingEvent, headers};

use crate::{KafkaConfig, failed};

/// The most one send takes: the producer's delivery budget (as Kotlin's `max.block.ms` 10 s +
/// `delivery.timeout.ms` 30 s). A relay's lease must be longer.
pub const SEND_TIMEOUT: Duration = Duration::from_secs(40);

/// The partitioner the producer uses unless a `partitioner` property names another: the Java
/// client's default (murmur2 of the key, positive, modulo the partitions; a record without a key
/// on a random partition), so a key lands on the partition a Kotlin publisher writes it to.
/// librdkafka's own default (`consistent_random`) would put it elsewhere.
pub const DEFAULT_PARTITIONER: &str = "murmur2_random";

/// The idempotent producer, with the budget above: a broker restart must not exhaust a relay's
/// attempts. Partitions keys as the Java client does ([`DEFAULT_PARTITIONER`]) unless the
/// configuration sets `partitioner`.
pub(crate) fn producer(config: &KafkaConfig) -> Result<FutureProducer, EventStoreError> {
    let mut client = rdkafka::config::ClientConfig::new();
    client.set("partitioner", DEFAULT_PARTITIONER);
    for (k, v) in &config.producer() {
        client.set(k, v);
    }
    client
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("message.timeout.ms", "30000")
        .set("request.timeout.ms", "20000")
        .set("enable.idempotence", "true")
        .create()
        .map_err(failed)
}

/// The headers in the order the records have always had (byte-identical): the others sorted by
/// name, then `type`, `id`, `timestamp` and `source`.
fn ordered_headers(headers: &std::collections::HashMap<String, String>) -> Vec<(&str, &str)> {
    const TRAILING: [&str; 4] = [
        headers::TYPE,
        headers::ID,
        headers::TIMESTAMP,
        headers::SOURCE,
    ];
    let mut others: Vec<(&str, &str)> = headers
        .iter()
        .filter(|(k, _)| !TRAILING.contains(&k.as_str()))
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    others.sort();
    others.extend(
        TRAILING
            .iter()
            .filter_map(|k| headers.get(*k).map(|v| (*k, v.as_str()))),
    );
    others
}

/// `event` to its source's topic, the headers (see [`ordered_headers`]) as the record's headers; returns
/// once the broker acknowledged it, within [`SEND_TIMEOUT`].
pub(crate) async fn send(
    producer: &FutureProducer,
    event: &OutgoingEvent,
) -> Result<(), EventStoreError> {
    if event.source.trim().is_empty() {
        return Err(EventStoreError::Invalid("the event has no source".into()));
    }
    let mut headers = OwnedHeaders::new();
    for (name, value) in ordered_headers(&event.headers) {
        headers = headers.insert(Header {
            key: name,
            value: Some(value),
        });
    }
    let mut record = FutureRecord::to(&event.source)
        .payload(&event.payload)
        .headers(headers);
    if let Some(key) = &event.key {
        record = record.key(key);
    }
    producer
        .send(record, Timeout::After(SEND_TIMEOUT))
        .await
        .map(|_| ())
        .map_err(|(e, _)| publish_error(e))
}

/// A failure no retry can fix (oversized or invalid records, unknown or unauthorized topics: the
/// Kotlin starter's NON_RETRIABLE set) is [`EventStoreError::Rejected`]; anything else `Failed`.
fn publish_error(e: KafkaError) -> EventStoreError {
    use rdkafka::types::RDKafkaErrorCode as Code;
    match e.rdkafka_error_code() {
        Some(
            Code::MessageSizeTooLarge
            | Code::InvalidMessage
            | Code::InvalidMessageSize
            | Code::InvalidTopic
            | Code::TopicAuthorizationFailed
            | Code::ClusterAuthorizationFailed
            | Code::InvalidRecord,
        ) => EventStoreError::Rejected(format!("kafka: {e}")),
        _ => failed(e),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn headers_are_the_others_sorted_then_type_id_timestamp_source() {
        let all: HashMap<String, String> = [
            ("source", "s"),
            ("userId", "u"),
            ("timestamp", "t"),
            ("correlationId", "c"),
            ("id", "i"),
            ("type", "y"),
            ("traceparent", "p"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let names: Vec<&str> = ordered_headers(&all).into_iter().map(|(k, _)| k).collect();
        assert_eq!(
            names,
            [
                "correlationId",
                "traceparent",
                "userId",
                "type",
                "id",
                "timestamp",
                "source"
            ]
        );
    }

    #[test]
    fn a_failure_no_retry_can_fix_is_rejected_others_failed() {
        use rdkafka::types::RDKafkaErrorCode as Code;
        for code in [
            Code::MessageSizeTooLarge,
            Code::InvalidMessage,
            Code::InvalidMessageSize,
            Code::InvalidTopic,
            Code::TopicAuthorizationFailed,
            Code::ClusterAuthorizationFailed,
            Code::InvalidRecord,
        ] {
            assert!(
                matches!(
                    publish_error(KafkaError::MessageProduction(code)),
                    EventStoreError::Rejected(_)
                ),
                "{code:?}"
            );
        }
        for code in [
            Code::MessageTimedOut,
            Code::BrokerTransportFailure,
            Code::NotEnoughReplicas,
        ] {
            assert!(
                matches!(
                    publish_error(KafkaError::MessageProduction(code)),
                    EventStoreError::Failed(_)
                ),
                "{code:?}"
            );
        }
    }

    #[tokio::test]
    async fn an_event_without_a_source_is_refused() {
        let producer = producer(&KafkaConfig::new("127.0.0.1:1")).unwrap();
        let event = OutgoingEvent {
            source: " ".into(),
            id: "1".into(),
            type_name: "t".into(),
            key: None,
            headers: HashMap::new(),
            payload: b"{}".to_vec(),
        };
        assert!(matches!(
            send(&producer, &event).await,
            Err(EventStoreError::Invalid(_))
        ));
    }
}
