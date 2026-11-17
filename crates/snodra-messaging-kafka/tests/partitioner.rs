//! A key lands on the partition the Java client's default partitioner gives it, so a Kotlin and a
//! Rust publisher of one key write to one partition (and its events keep their order across
//! languages). Against a real Kafka.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{Message, Offset, TopicPartitionList};
use snodra_messaging::event_store::{EventStore, OutgoingEvent, headers};
use snodra_messaging_kafka::{KafkaConfig, KafkaEventStore};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka};

const PARTITIONS: i32 = 6;

/// The partition of each key over 6 partitions under the Java client's default partitioner
/// (`BuiltInPartitioner.partitionForKey`: `Utils.toPositive(Utils.murmur2(key)) % partitions`,
/// murmur2 with seed `0x9747b28c`, the positive mask `& 0x7fffffff`). Computed with a transcript
/// of `Utils.murmur2` that reproduces the vectors of Kafka's `UtilsTest.testMurmur2` (`"21"` →
/// -973932308, `"foobar"` → -790332482, `"abc"` → 479470107, ...).
const JAVA_PARTITIONS: [(&str, i32); 9] = [
    ("order-1", 4),
    ("order-2", 3),
    ("order-3", 3),
    ("order-42", 0),
    ("customer-7", 1),
    ("o-1001", 1),
    ("a", 4),
    ("snodra", 5),
    // An empty key is a key (not none): hashed like any other, as Java hashes an empty array.
    ("", 3),
];

async fn create_topic(bootstrap: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    for r in admin
        .create_topics(
            &[NewTopic::new(topic, PARTITIONS, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(20))),
        )
        .await
        .unwrap()
    {
        r.unwrap();
    }
}

fn event(topic: &str, key: &str) -> OutgoingEvent {
    let id = uuid::Uuid::new_v4().to_string();
    OutgoingEvent {
        source: topic.to_string(),
        id: id.clone(),
        type_name: "p.Keyed".into(),
        key: Some(key.to_string()),
        headers: HashMap::from([
            (headers::ID.to_string(), id),
            (headers::TYPE.to_string(), "p.Keyed".to_string()),
        ]),
        payload: b"{}".to_vec(),
    }
}

/// The partition each key's record was written to.
fn partitions_of_keys(bootstrap: &str, topic: &str, expected: usize) -> BTreeMap<String, i32> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("partitioner-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut assignment = TopicPartitionList::new();
    for p in 0..PARTITIONS {
        assignment
            .add_partition_offset(topic, p, Offset::Beginning)
            .unwrap();
    }
    consumer.assign(&assignment).unwrap();
    let mut found = BTreeMap::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while found.len() < expected && Instant::now() < deadline {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(200)) {
            let key = String::from_utf8(m.key().unwrap_or_default().to_vec()).unwrap();
            found.insert(key, m.partition());
        }
    }
    found
}

async fn published_partitions(config: KafkaConfig, bootstrap: &str) -> BTreeMap<String, i32> {
    let topic = format!("partitioner-{}", uuid::Uuid::new_v4().simple());
    create_topic(bootstrap, &topic).await;
    let store = KafkaEventStore::new(config).unwrap();
    for (key, _) in JAVA_PARTITIONS {
        store.publish(&event(&topic, key)).await.unwrap();
    }
    let bootstrap = bootstrap.to_string();
    tokio::task::spawn_blocking(move || {
        partitions_of_keys(&bootstrap, &topic, JAVA_PARTITIONS.len())
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn a_key_lands_on_the_partition_the_java_default_partitioner_gives_it_unless_overridden() {
    let container = Kafka::default().start().await.expect("kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .unwrap();
    let bootstrap = format!("127.0.0.1:{port}");

    let java: BTreeMap<String, i32> = JAVA_PARTITIONS
        .iter()
        .map(|(k, p)| (k.to_string(), *p))
        .collect();
    let by_default = published_partitions(KafkaConfig::new(&bootstrap), &bootstrap).await;
    assert_eq!(by_default, java, "the Java client's partitions");

    // SNODRA_KAFKA_PROPERTY_PARTITIONER still chooses another one.
    let config = KafkaConfig::from_vars([
        ("SNODRA_KAFKA_BOOTSTRAP".to_string(), bootstrap.clone()),
        (
            "SNODRA_KAFKA_PROPERTY_PARTITIONER".to_string(),
            "fnv1a".to_string(),
        ),
    ]);
    assert_eq!(config.producer_properties["partitioner"], "fnv1a");
    let overridden = published_partitions(config, &bootstrap).await;
    assert_eq!(overridden.len(), JAVA_PARTITIONS.len());
    assert_ne!(overridden, java, "another partitioner, other partitions");
}
