//! Kafka transport for events: publish to `kraken.events`, consume with a per-service group and
//! dispatch to registered event handlers. Skeleton.

use std::sync::Arc;

use rdkafka::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;

use crate::envelope::Envelope;
use crate::error::MessagingError;
use crate::handlers::{Event, HandlerRegistry};

pub const EVENTS_TOPIC: &str = "kraken.events";

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Env: `KRAKEN_KAFKA_BOOTSTRAP`, for example `kafka:19092`.
    pub bootstrap_servers: String,
    pub topic: String,
    /// Consumer group; by convention the service name.
    pub group_id: String,
}

impl KafkaConfig {
    pub fn from_env(service_name: &str) -> Self {
        Self {
            bootstrap_servers: std::env::var("KRAKEN_KAFKA_BOOTSTRAP")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            topic: EVENTS_TOPIC.to_string(),
            group_id: service_name.to_string(),
        }
    }
}

pub struct EventPublisher {
    config: KafkaConfig,
    source: String,
    producer: Option<FutureProducer>,
}

impl EventPublisher {
    pub fn new(config: KafkaConfig, source: impl Into<String>) -> Result<Self, MessagingError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .create()?;
        Ok(Self { config, source: source.into(), producer: Some(producer) })
    }

    /// Publishes `event` keyed by `aggregate_id`, with envelope metadata in record headers.
    pub async fn publish<E: Event>(
        &self,
        _aggregate_id: &str,
        event: &E,
        user_id: Option<String>,
    ) -> Result<Envelope, MessagingError> {
        let envelope = Envelope::new(E::TYPE_NAME, self.source.clone(), user_id, serde_json::to_value(event)?);
        let _ = (&self.config, &self.producer);
        // TODO: FutureRecord::to(&self.config.topic).key(aggregate_id).payload(json).headers(...)
        Err(MessagingError::NotImplemented("EventPublisher::publish"))
    }
}

pub struct EventConsumer {
    config: KafkaConfig,
    registry: Arc<HandlerRegistry>,
    consumer: Option<StreamConsumer>,
}

impl EventConsumer {
    pub fn new(config: KafkaConfig, registry: Arc<HandlerRegistry>) -> Result<Self, MessagingError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        Ok(Self { config, registry, consumer: Some(consumer) })
    }

    /// Runs until cancelled: reads records, skips types without a handler, dispatches the rest,
    /// commits after the handler succeeds (at-least-once; handlers must be idempotent).
    pub async fn run(self) -> Result<(), MessagingError> {
        let _ = (&self.config, &self.registry, &self.consumer);
        Err(MessagingError::NotImplemented("EventConsumer::run"))
    }
}
