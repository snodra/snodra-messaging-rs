//! Testcontainers for the integration tests: Kafka (apache image), MongoDB replica set, kraken-hub.
#![allow(dead_code)]

use testcontainers::core::{AccessMode, IntoContainerPort, Mount, WaitFor};
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
    /// The merged P0 hub (protocol v2 without flow control): `KRAKEN_HUB_P0_IMAGE`, default
    /// `kraken-hub:p0`.
    P0,
    /// The merged P2 hub (flow control, no `instance-status`): `KRAKEN_HUB_P2_IMAGE`, default
    /// `kraken-hub:p2`.
    P2,
    /// The P3 hub (multi-hub, `instance-status`): `KRAKEN_HUB_P3_IMAGE`, default
    /// `kraken-hub:p3-hub`.
    P3,
    /// The P4a hub (scatter-gather, shared query types): `KRAKEN_HUB_P4A_IMAGE`, default
    /// `kraken-hub:p4a-hub`.
    P4a,
    /// The P4b hub (streaming queries): `KRAKEN_HUB_P4B_IMAGE`, default `kraken-hub:p4b-hub`.
    P4b,
    /// The P4c hub (subscription queries): `KRAKEN_HUB_P4C_IMAGE`, default `kraken-hub:p4c-hub`.
    P4c,
    /// The P5a hub (access control: static tokens, ACL, TLS): `KRAKEN_HUB_P5A_IMAGE`, default
    /// `kraken-hub:p5a-hub`.
    P5a,
}

impl HubImage {
    fn reference(self) -> String {
        match self {
            HubImage::Current => {
                std::env::var("KRAKEN_HUB_IMAGE").unwrap_or_else(|_| "kraken-hub:dev".to_string())
            }
            HubImage::Baseline => std::env::var("KRAKEN_HUB_BASELINE_IMAGE")
                .unwrap_or_else(|_| "kraken-hub:v1-baseline".to_string()),
            HubImage::P0 => {
                std::env::var("KRAKEN_HUB_P0_IMAGE").unwrap_or_else(|_| "kraken-hub:p0".to_string())
            }
            HubImage::P2 => {
                std::env::var("KRAKEN_HUB_P2_IMAGE").unwrap_or_else(|_| "kraken-hub:p2".to_string())
            }
            HubImage::P3 => std::env::var("KRAKEN_HUB_P3_IMAGE")
                .unwrap_or_else(|_| "kraken-hub:p3-hub".to_string()),
            HubImage::P4a => std::env::var("KRAKEN_HUB_P4A_IMAGE")
                .unwrap_or_else(|_| "kraken-hub:p4a-hub".to_string()),
            HubImage::P4b => std::env::var("KRAKEN_HUB_P4B_IMAGE")
                .unwrap_or_else(|_| "kraken-hub:p4b-hub".to_string()),
            HubImage::P4c => std::env::var("KRAKEN_HUB_P4C_IMAGE")
                .unwrap_or_else(|_| "kraken-hub:p4c-hub".to_string()),
            HubImage::P5a => std::env::var("KRAKEN_HUB_P5A_IMAGE")
                .unwrap_or_else(|_| "kraken-hub:p5a-hub".to_string()),
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
            HubImage::P0 => {
                "build the merged P0 hub and tag it `kraken-hub:p0` (or set KRAKEN_HUB_P0_IMAGE)"
            }
            HubImage::P2 => {
                "build the merged P2 hub and tag it `kraken-hub:p2` (or set KRAKEN_HUB_P2_IMAGE)"
            }
            HubImage::P3 => {
                "build the P3 hub and tag it `kraken-hub:p3-hub` (or set KRAKEN_HUB_P3_IMAGE)"
            }
            HubImage::P4a => {
                "build the P4a hub and tag it `kraken-hub:p4a-hub` (or set KRAKEN_HUB_P4A_IMAGE)"
            }
            HubImage::P4b => {
                "build the P4b hub and tag it `kraken-hub:p4b-hub` (or set KRAKEN_HUB_P4B_IMAGE)"
            }
            HubImage::P4c => {
                "build the P4c hub and tag it `kraken-hub:p4c-hub` (or set KRAKEN_HUB_P4C_IMAGE)"
            }
            HubImage::P5a => {
                "build the P5a hub and tag it `kraken-hub:p5a-hub` (or set KRAKEN_HUB_P5A_IMAGE)"
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
    /// Host directories bind-mounted read-only: (host path, container path).
    pub mounts: Vec<(String, String)>,
    /// Extra `/etc/hosts` entries of the container: (name, address).
    pub host_names: Vec<(String, std::net::IpAddr)>,
}

impl HubOptions {
    pub fn new(image: HubImage) -> HubOptions {
        HubOptions {
            image,
            port: None,
            env: vec![],
            mounts: vec![],
            host_names: vec![],
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

    /// Makes `name` resolve inside the container to `address` (a single-label name, so the hub
    /// does not warn about plain http to it).
    pub fn host_name(mut self, name: &str, address: std::net::IpAddr) -> HubOptions {
        self.host_names.push((name.to_string(), address));
        self
    }

    /// Bind-mounts the host directory `host` at `container` (read-only), for an ACL file or a
    /// certificate and key.
    pub fn mount(mut self, host: &std::path::Path, container: &str) -> HubOptions {
        self.mounts
            .push((host.display().to_string(), container.to_string()));
        self
    }
}

/// Retries a TCP connect to `addr` (`host:port`) until it is accepted or `within` has passed;
/// `true` when it was. The hub prints "hub listening" before the runtime's host-mapped port
/// forwards (Rancher Desktop), so a client that connects right away can be refused.
pub async fn wait_for_tcp(addr: &str, within: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + within;
    loop {
        let attempt = tokio::net::TcpStream::connect(addr);
        if let Ok(Ok(_)) =
            tokio::time::timeout(std::time::Duration::from_millis(500), attempt).await
        {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn probe_or_fail(addr: &str) {
    assert!(
        wait_for_tcp(addr, std::time::Duration::from_secs(30)).await,
        "the hub's mapped port {addr} never accepted a connection"
    );
}

pub async fn hub_with(options: HubOptions) -> HubContainer {
    let (name, tag) = image_or_fail(options.image);
    let mut image = GenericImage::new(name, tag)
        .with_exposed_port(7000.tcp())
        .with_exposed_port(7001.tcp())
        .with_wait_for(WaitFor::message_on_stdout("hub listening"))
        .with_env_var("KRAKEN_HUB_HTTP_LISTEN", "0.0.0.0:7001")
        .with_env_var("KRAKEN_HUB_OTLP_ENDPOINT", "")
        .with_env_var(
            "KRAKEN_HUB_LOG",
            std::env::var("KRAKEN_TEST_HUB_LOG").unwrap_or_else(|_| "info".to_string()),
        );
    for (k, v) in &options.env {
        image = image.with_env_var(k.clone(), v.clone());
    }
    let mut image = image;
    for (host, container) in &options.mounts {
        image = image.with_mount(
            Mount::bind_mount(host.clone(), container.clone())
                .with_access_mode(AccessMode::ReadOnly),
        );
    }
    for (name, address) in &options.host_names {
        image = image.with_host(name.clone(), testcontainers::core::Host::Addr(*address));
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
            probe_or_fail(&format!("127.0.0.1:{port}")).await;
            probe_or_fail(&format!("127.0.0.1:{http_port}")).await;
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
    probe_or_fail(&format!("127.0.0.1:{grpc}")).await;
    probe_or_fail(&format!("127.0.0.1:{http}")).await;
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

/// Returns once a `Topology` RPC on `address` succeeds (bounded by 30 s). After `start` the host
/// port forwarding can take seconds to work again (Rancher Desktop).
pub async fn wait_until_reachable(address: &str) {
    let target = address.replace("static://", "http://");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let attempt = async {
            let channel = tonic::transport::Endpoint::from_shared(target.clone())
                .ok()?
                .connect()
                .await
                .ok()?;
            kraken_messaging::proto::hub_client::HubClient::new(channel)
                .topology(kraken_messaging::proto::TopologyRequest {})
                .await
                .ok()
        };
        if tokio::time::timeout(std::time::Duration::from_millis(500), attempt)
            .await
            .ok()
            .flatten()
            .is_some()
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "hub at {address} never became reachable"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A container serving one static file over plain HTTP (busybox `httpd`, from the official `busybox` image), for what a hub fetches
/// from a server of the test: the JWKS. Reached by other containers on the default bridge at
/// [`ip`](Self::ip) port 8080, so it needs nothing from the host's network.
pub struct StaticServer {
    pub container: ContainerAsync<GenericImage>,
    pub ip: std::net::IpAddr,
    _dir: std::path::PathBuf,
}

pub const STATIC_SERVER_PORT: u16 = 8080;

/// Serves `content` at `/<name>`. The directory is created under `root` (inside the checkout, where
/// a VM-based container runtime can mount it).
pub async fn static_server(root: &std::path::Path, name: &str, content: &str) -> StaticServer {
    use std::os::unix::fs::PermissionsExt;
    let dir = root.join(format!("www-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let file = dir.join(name);
    std::fs::write(&file, content).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    let image = GenericImage::new("busybox", "1.36.1")
        .with_exposed_port(STATIC_SERVER_PORT.tcp())
        .with_wait_for(WaitFor::seconds(1))
        .with_mount(
            Mount::bind_mount(dir.display().to_string(), "/www")
                .with_access_mode(AccessMode::ReadOnly),
        )
        .with_cmd([
            "httpd",
            "-f",
            "-p",
            &STATIC_SERVER_PORT.to_string(),
            "-h",
            "/www",
        ]);
    let container = image.start().await.expect("static server container");
    let port = match container.get_host_port_ipv4(STATIC_SERVER_PORT.tcp()).await {
        Ok(port) => port,
        Err(e) => {
            let out = container.stdout_to_vec().await.unwrap_or_default();
            let err = container.stderr_to_vec().await.unwrap_or_default();
            panic!(
                "static server port: {e}\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&out),
                String::from_utf8_lossy(&err)
            );
        }
    };
    probe_or_fail(&format!("127.0.0.1:{port}")).await;
    let ip = container
        .get_bridge_ip_address()
        .await
        .expect("static server bridge address");
    StaticServer {
        container,
        ip,
        _dir: dir,
    }
}

impl Drop for StaticServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self._dir);
    }
}
