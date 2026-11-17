//! Testcontainers for the integration tests: Kafka (apache image), MongoDB replica set, snodra-server.
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

/// The hub image under test: `SNODRA_SERVER_IMAGE`, default `snodra-server:dev`.
pub fn hub_image() -> (String, String) {
    let full =
        std::env::var("SNODRA_SERVER_IMAGE").unwrap_or_else(|_| "snodra-server:dev".to_string());
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
        "hub image {name}:{tag} not found: run `docker build -t snodra-server:dev .` in ../snodra-server (or set SNODRA_SERVER_IMAGE)"
    );
    (name, tag)
}

pub struct HubContainer {
    pub container: ContainerAsync<GenericImage>,
    pub address: String,
    pub http: String,
}

/// How to start a hub container (the image under test, [`hub_image`]).
#[derive(Clone, Debug, Default)]
pub struct HubOptions {
    /// Fixed host port for gRPC, so `stop`/`start` keeps the address the clients use.
    pub port: Option<u16>,
    /// Extra `SNODRA_SERVER_*` variables, for example `SNODRA_SERVER_TIMEOUT_OVERRIDES`.
    pub env: Vec<(String, String)>,
    /// Host directories bind-mounted read-only: (host path, container path).
    pub mounts: Vec<(String, String)>,
    /// Extra `/etc/hosts` entries of the container: (name, address).
    pub host_names: Vec<(String, std::net::IpAddr)>,
}

impl HubOptions {
    pub fn new() -> HubOptions {
        HubOptions::default()
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
    let (name, tag) = hub_image_or_fail();
    // Built anew for every start (a request is used up by it).
    let build = || {
        let mut image = GenericImage::new(name.clone(), tag.clone())
            .with_exposed_port(7000.tcp())
            .with_exposed_port(7001.tcp())
            .with_wait_for(WaitFor::message_on_stdout("hub listening"))
            .with_env_var("SNODRA_SERVER_HTTP_LISTEN", "0.0.0.0:7001")
            .with_env_var("SNODRA_SERVER_OTLP_ENDPOINT", "")
            .with_env_var(
                "SNODRA_SERVER_LOG",
                std::env::var("SNODRA_TEST_HUB_LOG").unwrap_or_else(|_| "info".to_string()),
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
        image
    };
    let container = match options.port {
        Some(port) => {
            // A fixed port is free when picked, not when Docker binds it (another process may
            // take it meanwhile: "failed to bind port", "port is already allocated"): both
            // ports are picked again then. The caller reads the hub's address back.
            let mut tries = 0;
            let mut port = port;
            let (container, port, http_port) = loop {
                tries += 1;
                let http_port = free_port();
                match build()
                    .with_mapped_port(port, 7000.tcp())
                    .with_mapped_port(http_port, 7001.tcp())
                    .start()
                    .await
                {
                    Ok(container) => break (container, port, http_port),
                    Err(e) if tries < 3 && bind_failure(&e.to_string()) => {
                        eprintln!("hub container: {e}; trying again on other ports");
                        port = free_port();
                    }
                    Err(e) => panic!("hub container: {e}"),
                }
            };
            probe_or_fail(&format!("127.0.0.1:{port}")).await;
            probe_or_fail(&format!("127.0.0.1:{http_port}")).await;
            return HubContainer {
                container,
                address: format!("static://127.0.0.1:{port}"),
                http: format!("http://127.0.0.1:{http_port}"),
            };
        }
        None => build().start().await.expect("hub container"),
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
    hub_with(HubOptions::new()).await
}

impl MongoDb {
    /// Connection string for another database on the same container.
    pub fn uri_for(&self, db: &str) -> String {
        let base = self.uri.split('/').take(3).collect::<Vec<_>>().join("/");
        format!("{base}/{db}?directConnection=true")
    }
}

/// Whether a container start failed because a host port was taken (Docker words it several
/// ways: "failed to bind port", "Bind for 0.0.0.0:N failed: port is already allocated").
pub fn bind_failure(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("bind")
        || error.contains("already allocated")
        || error.contains("address already in use")
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
    hub_with(HubOptions::new().port(port)).await
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
            snodra_messaging::proto::hub_client::HubClient::new(channel)
                .topology(snodra_messaging::proto::TopologyRequest {})
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
