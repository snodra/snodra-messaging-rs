//! Publishing: one record per event to the topic named by its source.

use std::time::Duration;

use kraken_messaging::event_store::{EventStoreError, OutgoingEvent};
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

/// `event` to its source's topic, the headers (sorted by name) as the record's headers; returns
/// once the broker acknowledged it, within [`SEND_TIMEOUT`].
pub(crate) async fn send(
    producer: &FutureProducer,
    event: &OutgoingEvent,
) -> Result<(), EventStoreError> {
    if event.source.trim().is_empty() {
        return Err(EventStoreError::Invalid("the event has no source".into()));
    }
    let mut sorted: Vec<(&String, &String)> = event.headers.iter().collect();
    sorted.sort();
    let mut headers = OwnedHeaders::new();
    for (name, value) in sorted {
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
