//! Testcontainers for the integration tests: Kafka (apache image), MongoDB replica set, kraken-hub.
#![allow(dead_code)]

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::kafka::apache::{self, Kafka};
use testcontainers_modules::mongo::Mongo;

pub struct KafkaBroker {
    pub container: ContainerAsync<Kafka>,
    pub bootstrap: String,
}

pub async fn kafka() -> KafkaBroker {
    let container = Kafka::default().start().await.expect("kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("kafka port");
    KafkaBroker {
        container,
        bootstrap: format!("127.0.0.1:{port}"),
    }
}

pub struct MongoDb {
    pub container: ContainerAsync<Mongo>,
    /// Connection string with a fresh database name per call and `directConnection=true`.
    pub uri: String,
}

pub async fn mongo() -> MongoDb {
    let container = Mongo::repl_set().start().await.expect("mongo container");
    let port = container
        .get_host_port_ipv4(27017.tcp())
        .await
        .expect("mongo port");
    let db = format!("t{}", uuid::Uuid::new_v4().simple());
    MongoDb {
        container,
        uri: format!("mongodb://127.0.0.1:{port}/{db}?directConnection=true"),
    }
}

pub fn hub_image() -> (String, String) {
    let full = std::env::var("KRAKEN_HUB_IMAGE").unwrap_or_else(|_| "kraken-hub:dev".to_string());
    match full.rsplit_once(':') {
        Some((name, tag)) => (name.to_string(), tag.to_string()),
        None => (full, "latest".to_string()),
    }
}

/// Panics with the build command when the hub image is missing: an absent image must never skip a test silently.
pub fn hub_image_or_fail() -> (String, String) {
    let (name, tag) = hub_image();
    let ok = std::process::Command::new("docker")
        .args(["image", "inspect", &format!("{name}:{tag}")])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    assert!(
        ok,
        "hub image {name}:{tag} not found: run `docker build -t {name}:{tag} .` in ../kraken-hub (or set KRAKEN_HUB_IMAGE)"
    );
    (name, tag)
}

pub struct HubContainer {
    pub container: ContainerAsync<GenericImage>,
    pub address: String,
    pub http: String,
}

pub async fn hub() -> HubContainer {
    let (name, tag) = hub_image_or_fail();
    let container = GenericImage::new(name, tag)
        .with_exposed_port(7000.tcp())
        .with_exposed_port(7001.tcp())
        .with_wait_for(WaitFor::message_on_stdout("hub listening"))
        .with_env_var("KRAKEN_HUB_HTTP_LISTEN", "0.0.0.0:7001")
        .with_env_var("KRAKEN_HUB_OTLP_ENDPOINT", "")
        .with_env_var("KRAKEN_HUB_LOG", "info")
        .start()
        .await
        .expect("hub container");
    let grpc = container
        .get_host_port_ipv4(7000.tcp())
        .await
        .expect("hub grpc port");
    let http = container
        .get_host_port_ipv4(7001.tcp())
        .await
        .expect("hub http port");
    HubContainer {
        container,
        address: format!("static://127.0.0.1:{grpc}"),
        http: format!("http://127.0.0.1:{http}"),
    }
}
