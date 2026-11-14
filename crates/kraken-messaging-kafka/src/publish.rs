//! Publishing: one record per event to the topic named by its source.

use std::time::Duration;

use kraken_messaging::event_store::{EventStoreError, OutgoingEvent, headers};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::{KafkaConfig, failed};

/// The most one send takes: the producer's delivery budget (as Kotlin's `max.block.ms` 10 s +
/// `delivery.timeout.ms` 30 s). A relay's lease must be longer.
pub const SEND_TIMEOUT: Duration = Duration::from_secs(40);

/// The idempotent producer, with the budget above: a broker restart must not exhaust a relay's
/// attempts.
pub(crate) fn producer(config: &KafkaConfig) -> Result<FutureProducer, EventStoreError> {
    let mut client = rdkafka::config::ClientConfig::new();
    for (k, v) in &config.properties {
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
        .map_err(|(e, _)| failed(e))
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
