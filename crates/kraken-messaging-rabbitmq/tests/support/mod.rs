//! A RabbitMQ container for the add-on's tests: `rabbitmq:4.4.0-management` with the stream and
//! stream-management plugins, the stream port published on a fixed free host port that the node
//! advertises (the store connects to each stream's leader as the broker advertises it), and the
//! super streams and streams a test needs created through the stream protocol.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use kraken_messaging_rabbitmq::RabbitConfig;
use rabbitmq_stream_client::{Client, ClientOptions};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// The image every test runs: the current RabbitMQ 4 with the management plugin.
pub const IMAGE: (&str, &str) = ("rabbitmq", "4.4.0-management");

pub struct RabbitMq {
    _container: ContainerAsync<GenericImage>,
    /// `127.0.0.1:<port>` of the stream protocol.
    pub stream: String,
    /// `http://127.0.0.1:<port>` of the management API.
    pub management: String,
}

impl RabbitMq {
    /// The store's settings for this broker, with the management API.
    pub fn config(&self) -> RabbitConfig {
        RabbitConfig::new(&self.stream).management(&self.management)
    }

    async fn client(&self) -> Client {
        let (host, port) = self.stream.split_once(':').unwrap();
        Client::connect(
            ClientOptions::builder()
                .host(host)
                .port(port.parse().unwrap())
                .build(),
        )
        .await
        .expect("a stream connection")
    }

    /// The super stream `name` with `partitions` partitions (`<name>-0` ..), as
    /// `rabbitmq-streams add_super_stream` creates it.
    pub async fn super_stream(&self, name: &str, partitions: usize) {
        let client = self.client().await;
        let streams: Vec<String> = (0..partitions).map(|i| format!("{name}-{i}")).collect();
        let keys: Vec<String> = (0..partitions).map(|i| i.to_string()).collect();
        let response = client
            .create_super_stream(name, streams, keys, HashMap::new())
            .await
            .expect("create the super stream");
        assert!(response.is_ok(), "create {name}: {:?}", response.code());
        client.close().await.unwrap();
    }

    /// The plain stream `name`.
    pub async fn stream(&self, name: &str) {
        let client = self.client().await;
        let response = client
            .create_stream(name, HashMap::new())
            .await
            .expect("create the stream");
        assert!(response.is_ok(), "create {name}: {:?}", response.code());
        client.close().await.unwrap();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Starts the broker (removed when the value is dropped).
pub async fn rabbitmq() -> RabbitMq {
    // The stream port is published on a host port the node advertises, so it is picked here;
    // the management port is Docker's. A picked port taken meanwhile by another process ("failed
    // to bind port") is picked again.
    let mut tries = 0;
    let (container, port) = loop {
        tries += 1;
        let port = free_port();
        let conf = format!("stream.advertised_host = 127.0.0.1\nstream.advertised_port = {port}\n");
        let started = GenericImage::new(IMAGE.0, IMAGE.1)
            .with_exposed_port(5552.tcp())
            .with_exposed_port(15672.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
            .with_copy_to(
                "/etc/rabbitmq/enabled_plugins",
                b"[rabbitmq_management,rabbitmq_stream,rabbitmq_stream_management].\n".to_vec(),
            )
            .with_copy_to("/etc/rabbitmq/conf.d/20-stream.conf", conf.into_bytes())
            .with_mapped_port(port, 5552.tcp())
            .with_mapped_port(0, 15672.tcp())
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await;
        match started {
            Ok(container) => break (container, port),
            Err(e) if tries < 3 && e.to_string().contains("bind") => {
                eprintln!("rabbitmq container: {e}; trying again");
            }
            Err(e) => panic!("rabbitmq container: {e}"),
        }
    };
    let http = container
        .get_host_port_ipv4(15672.tcp())
        .await
        .expect("the management port");
    let broker = RabbitMq {
        _container: container,
        stream: format!("127.0.0.1:{port}"),
        management: format!("http://127.0.0.1:{http}"),
    };
    // The stream listener and the management API answer a moment after the startup line.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (host, p) = broker.stream.split_once(':').unwrap();
        let ok = Client::connect(
            ClientOptions::builder()
                .host(host)
                .port(p.parse().unwrap())
                .build(),
        )
        .await;
        let api = reqwest::Client::new()
            .get(format!("{}/api/stream/consumers/%2F", broker.management))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        if let Ok(client) = ok {
            let _ = client.close().await;
            if api {
                return broker;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the broker never answered"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The hub image under test: `KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`.
fn hub_image() -> (String, String) {
    let full = std::env::var("KRAKEN_HUB_IMAGE").unwrap_or_else(|_| "kraken-hub:dev".to_string());
    match full.rsplit_once(':') {
        Some((name, tag)) => (name.to_string(), tag.to_string()),
        None => (full, "latest".to_string()),
    }
}

pub struct Hub {
    _container: ContainerAsync<GenericImage>,
    /// `static://127.0.0.1:<port>` of the hub's gRPC.
    pub address: String,
    /// `http://127.0.0.1:<port>` of its HTTP API.
    pub http: String,
}

/// A hub whose HTTP API takes `write_token` for processor control.
pub async fn hub(write_token: &str) -> Hub {
    let (name, tag) = hub_image();
    let container = GenericImage::new(name, tag)
        .with_exposed_port(7000.tcp())
        .with_exposed_port(7001.tcp())
        .with_wait_for(WaitFor::message_on_stdout("hub listening"))
        .with_env_var("KRAKEN_HUB_HTTP_LISTEN", "0.0.0.0:7001")
        .with_env_var("KRAKEN_HUB_OTLP_ENDPOINT", "")
        .with_env_var("KRAKEN_HUB_DASHBOARD_WRITE_TOKEN", write_token)
        .start()
        .await
        .expect("hub container (KRAKEN_HUB_IMAGE)");
    let grpc = container.get_host_port_ipv4(7000.tcp()).await.unwrap();
    let http = container.get_host_port_ipv4(7001.tcp()).await.unwrap();
    Hub {
        _container: container,
        address: format!("static://127.0.0.1:{grpc}"),
        http: format!("http://127.0.0.1:{http}"),
    }
}
