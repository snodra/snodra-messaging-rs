//! The Kafka event store passes the event-store conformance kit on a real broker, with the
//! MongoDB message store beside it (the at-least-once check records processed events), its
//! browser reading the kit's topic.

use std::sync::Arc;
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use snodra_messaging::event_store::browse::BrowseSettings;
use snodra_messaging::event_store::conformance::run_event_store_conformance;
use snodra_messaging::event_store::{Capability, EventStore, dead_letter_source};
use snodra_messaging::store::MessageStore;
use snodra_messaging_kafka::{KafkaConfig, KafkaEventStore};
use snodra_messaging_mongo::{MongoIndexSettings, MongoStore};
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka};
use testcontainers_modules::mongo::Mongo;

/// Segments of the kit's source: enough to spread over two instances.
const PARTITIONS: i32 = 4;

async fn create_topics(bootstrap: &str, topics: &[&str]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client");
    let new: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|t| NewTopic::new(t, PARTITIONS, TopicReplication::Fixed(1)))
        .collect();
    let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(30)));
    for result in admin
        .create_topics(&new, &options)
        .await
        .expect("create topics")
    {
        result.unwrap_or_else(|(topic, code)| panic!("create topic {topic}: {code}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_event_store_passes_conformance() {
    let kafka = Kafka::default().start().await.expect("kafka container");
    let port = kafka
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("kafka port");
    let bootstrap = format!("127.0.0.1:{port}");
    let source = "snodra.conformance";
    create_topics(&bootstrap, &[source, &dead_letter_source(source)]).await;

    let mongo = Mongo::repl_set().start().await.expect("mongo container");
    let mongo_port = mongo
        .get_host_port_ipv4(27017.tcp())
        .await
        .expect("mongo port");
    let client = mongodb::Client::with_uri_str(format!(
        "mongodb://127.0.0.1:{mongo_port}/?directConnection=true"
    ))
    .await
    .expect("mongo client");
    let db = client.database(&format!("t{}", uuid::Uuid::new_v4().simple()));
    let messages = MongoStore::new(client, db);
    messages
        .ensure_indexes(&MongoIndexSettings::default())
        .await
        .expect("indexes");
    let messages: Arc<dyn MessageStore> = Arc::new(messages);

    let config = KafkaConfig::new(&bootstrap).browse(BrowseSettings {
        sources: vec![source.to_string()],
        ..BrowseSettings::default()
    });
    let make = || -> Arc<dyn EventStore> {
        Arc::new(KafkaEventStore::new(config.clone()).expect("kafka event store"))
    };
    assert_eq!(make().capabilities(), Capability::ALL);
    run_event_store_conformance(source, make, messages).await;
}
