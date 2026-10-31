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

/// Which hub build a test runs against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HubImage {
    /// The hub under development: `KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`.
    Current,
    /// The previous hub release, for compatibility tests: `KRAKEN_HUB_BASELINE_IMAGE`, default
    /// `kraken-hub:v1-baseline` (the hub before protocol v2).
    Baseline,
}

impl HubImage {
    fn reference(self) -> String {
        match self {
            HubImage::Current => {
                std::env::var("KRAKEN_HUB_IMAGE").unwrap_or_else(|_| "kraken-hub:dev".to_string())
            }
            HubImage::Baseline => std::env::var("KRAKEN_HUB_BASELINE_IMAGE")
                .unwrap_or_else(|_| "kraken-hub:v1-baseline".to_string()),
        }
    }

    fn hint(self) -> &'static str {
        match self {
            HubImage::Current => {
                "run `docker build -t kraken-hub:dev .` in ../kraken-hub (or set KRAKEN_HUB_IMAGE)"
            }
            HubImage::Baseline => {
                "tag the previous hub release, e.g. `git worktree add /tmp/hub-v1 <tag> && docker build -t kraken-hub:v1-baseline /tmp/hub-v1` (or set KRAKEN_HUB_BASELINE_IMAGE)"
            }
        }
    }
}

fn split_image(full: String) -> (String, String) {
    match full.rsplit_once(':') {
        Some((name, tag)) => (name.to_string(), tag.to_string()),
        None => (full, "latest".to_string()),
    }
}

pub fn hub_image() -> (String, String) {
    split_image(HubImage::Current.reference())
}

/// Panics with the build command when the hub image is missing: an absent image must never skip a test silently.
pub fn hub_image_or_fail() -> (String, String) {
    image_or_fail(HubImage::Current)
}

pub fn image_or_fail(image: HubImage) -> (String, String) {
    let (name, tag) = split_image(image.reference());
    let ok = std::process::Command::new("docker")
        .args(["image", "inspect", &format!("{name}:{tag}")])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    assert!(ok, "hub image {name}:{tag} not found: {}", image.hint());
    (name, tag)
}

pub struct HubContainer {
    pub container: ContainerAsync<GenericImage>,
    pub address: String,
    pub http: String,
}

/// How to start a hub container.
#[derive(Clone, Debug)]
pub struct HubOptions {
    pub image: HubImage,
    /// Fixed host port for gRPC, so `stop`/`start` keeps the address the clients use.
    pub port: Option<u16>,
    /// Extra `KRAKEN_HUB_*` variables, for example `KRAKEN_HUB_TIMEOUT_OVERRIDES`.
    pub env: Vec<(String, String)>,
}

impl HubOptions {
    pub fn new(image: HubImage) -> HubOptions {
        HubOptions {
            image,
            port: None,
            env: vec![],
        }
    }

    pub fn port(mut self, port: u16) -> HubOptions {
        self.port = Some(port);
        self
    }

    pub fn env(mut self, name: &str, value: &str) -> HubOptions {
        self.env.push((name.to_string(), value.to_string()));
        self
    }
}

pub async fn hub_with(options: HubOptions) -> HubContainer {
    let (name, tag) = image_or_fail(options.image);
    let mut image = GenericImage::new(name, tag)
        .with_exposed_port(7000.tcp())
        .with_exposed_port(7001.tcp())
        .with_wait_for(WaitFor::message_on_stdout("hub listening"))
        .with_env_var("KRAKEN_HUB_HTTP_LISTEN", "0.0.0.0:7001")
        .with_env_var("KRAKEN_HUB_OTLP_ENDPOINT", "")
        .with_env_var("KRAKEN_HUB_LOG", "info");
    for (k, v) in &options.env {
        image = image.with_env_var(k.clone(), v.clone());
    }
    let container = match options.port {
        Some(port) => {
            let http_port = free_port();
            let container = image
                .with_mapped_port(port, 7000.tcp())
                .with_mapped_port(http_port, 7001.tcp())
                .start()
                .await
                .expect("hub container");
            return HubContainer {
                container,
                address: format!("static://127.0.0.1:{port}"),
                http: format!("http://127.0.0.1:{http_port}"),
            };
        }
        None => image.start().await.expect("hub container"),
    };
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

pub async fn hub() -> HubContainer {
    hub_with(HubOptions::new(HubImage::Current)).await
}

/// The previous hub release (`KRAKEN_HUB_BASELINE_IMAGE`).
pub async fn hub_baseline() -> HubContainer {
    hub_with(HubOptions::new(HubImage::Baseline)).await
}

impl MongoDb {
    /// Connection string for another database on the same container.
    pub fn uri_for(&self, db: &str) -> String {
        let base = self.uri.split('/').take(3).collect::<Vec<_>>().join("/");
        format!("{base}/{db}?directConnection=true")
    }
}

/// A free TCP port on the host, for containers that must keep their mapping across restarts.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The hub published on a fixed host port, so `stop`/`start` keeps the address the clients use.
pub async fn hub_on(port: u16) -> HubContainer {
    hub_with(HubOptions::new(HubImage::Current).port(port)).await
}
