//! End to end against the real hub image (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`, the P5c
//! hub) running `KRAKEN_HUB_AUTH_MODE=enforce` with `KRAKEN_HUB_TLS_CLIENT_AUTH=required`: the
//! services authenticate with client certificates (`kraken://service/<name>` URI SAN) that a CA
//! generated at test time signed, and their rights come from the ACL's `[[service]]` tables. The hub's
//! own server certificate is issued from the same CA with the client usage too, so the image's
//! `--health-check` (which presents the hub's own pair under `required`) stays green. No Kafka or
//! MongoDB: the participants are a `HubConnection` or a `HubPool` and a `Sender`.
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use kraken_messaging::hub::{
    HubConnection, HubPool, HubSettings, Inbound, RegisteredTypes, SendOptions, Sender,
    SenderSettings,
};
use kraken_messaging::proto::{self, ClientMessage, client_message, reply};
use kraken_messaging::{HubTls, MessagingError, Query, message_type};
use serde::{Deserialize, Serialize};
use support::containers::{HubContainer, HubImage, HubOptions, free_port, hub_with};
use support::logs::Captured;
use support::pki::{ClientSpec, Pki, TempDir, fingerprint_of_pem};
use tokio::sync::mpsc;

macro_rules! require_docker {
    () => {
        if !support::docker_available() {
            eprintln!("docker not available, skipping");
            return;
        }
    };
}

#[derive(Serialize, Deserialize)]
struct GetInvoice {
    id: u32,
}
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Invoice {
    id: u32,
    owner: String,
}
message_type!(GetInvoice, "billing.GetInvoice");
impl Query for GetInvoice {
    type Reply = Invoice;
}

#[derive(Serialize, Deserialize)]
struct ListFiles {}
message_type!(ListFiles, "files.ListFiles");
impl Query for ListFiles {
    type Reply = ();
}

/// The CA, the directory the hubs mount (their pair, the client CA, the ACL) and the directory of
/// the services' pairs.
struct Fixture {
    pki: Pki,
    hub_dir: TempDir,
    clients: TempDir,
}

/// `billing` (a `[[service]]`: handles `billing.*`, sends nothing) and `orders` (handles
/// `orders.*`, sends `billing.*`).
fn fixture() -> Fixture {
    let (pki, hub_dir, clients) = (Pki::new("kraken test CA"), TempDir::new(), TempDir::new());
    let (cert, key) = pki.server_and_client(&[]);
    hub_dir.write("hub-cert.pem", &cert);
    hub_dir.write("hub-key.pem", &key);
    hub_dir.write("client-ca.pem", &pki.ca_pem);
    hub_dir.write(
        "acl.toml",
        "[[service]]\nname = \"billing\"\nhandle = [\"billing.*\"]\nsend = []\n\n\
         [[service]]\nname = \"orders\"\nhandle = [\"orders.*\"]\nsend = [\"billing.*\"]\n",
    );
    Fixture {
        pki,
        hub_dir,
        clients,
    }
}

impl Fixture {
    /// A hub container; `client_auth` is `required` or `optional`; `port` and `id` for a pair.
    async fn hub(&self, client_auth: &str, port: Option<u16>, id: Option<&str>) -> HubContainer {
        let mut options = HubOptions::new(HubImage::Current)
            .env("KRAKEN_HUB_AUTH_MODE", "enforce")
            .env("KRAKEN_HUB_ACL_FILE", "/etc/kraken/acl.toml")
            .env("KRAKEN_HUB_TLS_CERT", "/etc/kraken/hub-cert.pem")
            .env("KRAKEN_HUB_TLS_KEY", "/etc/kraken/hub-key.pem")
            .env("KRAKEN_HUB_TLS_CLIENT_CA", "/etc/kraken/client-ca.pem")
            .env("KRAKEN_HUB_TLS_CLIENT_AUTH", client_auth)
            .mount(self.hub_dir.dir(), "/etc/kraken");
        if let Some(port) = port {
            options = options.port(port);
        }
        if let Some(id) = id {
            options = options.env("KRAKEN_HUB_ID", id);
        }
        hub_with(options).await
    }

    /// `service`'s pair in the clients' directory, as `HubTls` (the CA trusted for the hub is the
    /// same one); its fingerprint.
    fn tls(&self, service: &str, spec: &ClientSpec) -> (HubTls, String) {
        let (cert, key) = self.pki.client(spec);
        let tls = HubTls {
            ca_file: Some(self.clients.write("ca.pem", &self.pki.ca_pem)),
            client_cert_file: Some(self.clients.write(&format!("{service}-cert.pem"), &cert)),
            client_key_file: Some(self.clients.write(&format!("{service}-key.pem"), &key)),
            ..HubTls::default()
        };
        (tls, fingerprint_of_pem(&cert))
    }

    fn renew(&self, tls: &HubTls, spec: &ClientSpec) -> String {
        let (cert, key) = self.pki.client(spec);
        std::fs::write(tls.client_cert_file.as_ref().unwrap(), &cert).unwrap();
        std::fs::write(tls.client_key_file.as_ref().unwrap(), &key).unwrap();
        fingerprint_of_pem(&cert)
    }
}

/// Answers `billing.GetInvoice` with an `Invoice` naming `service`; everything else goes to the
/// sender.
fn serve(service: &str, mut rx: mpsc::Receiver<Inbound>, forward: Arc<Sender>) {
    let name = service.to_string();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { request, reply_to } => {
                    let id = serde_json::from_slice::<serde_json::Value>(&request.payload)
                        .ok()
                        .and_then(|v| v["id"].as_u64())
                        .unwrap_or(0) as u32;
                    let payload = serde_json::to_vec(&Invoice {
                        id,
                        owner: name.clone(),
                    })
                    .unwrap();
                    let _ = reply_to
                        .send(ClientMessage {
                            message: Some(client_message::Message::Reply(proto::Reply {
                                request_id: request.id,
                                result: Some(reply::Result::Payload(payload)),
                            })),
                        })
                        .await;
                    reply_to.grant().await;
                }
                other => forward.on_inbound(other),
            }
        }
    });
}

struct Peer {
    conn: Arc<HubConnection>,
    sender: Arc<Sender>,
}

fn peer(
    address: &str,
    service: &str,
    tls: Option<HubTls>,
    types: RegisteredTypes,
    backoff_max: Duration,
) -> Peer {
    let (tx, rx) = mpsc::channel::<Inbound>(64);
    let conn = HubConnection::new(
        HubSettings {
            address: address.to_string(),
            backoff_min: Duration::from_millis(100),
            backoff_max,
            tls,
            ..HubSettings::default()
        },
        service.to_string(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    );
    conn.set_registered_types(types);
    let sender = Sender::new(Arc::clone(&conn), Some(Duration::from_secs(5)), false);
    serve(service, rx, Arc::clone(&sender));
    conn.start();
    Peer { conn, sender }
}

fn billing_types() -> RegisteredTypes {
    RegisteredTypes {
        queries: vec!["billing.GetInvoice".into()],
        ..RegisteredTypes::default()
    }
}

#[tokio::test]
async fn services_with_certificates_register_and_one_serves_a_query_from_the_other() {
    require_docker!();
    let f = fixture();
    let hub = f.hub("required", None, None).await;
    let (billing_tls, _) = f.tls("billing", &ClientSpec::service("billing"));
    let (orders_tls, _) = f.tls("orders", &ClientSpec::service("orders"));
    let billing = peer(
        &hub.address,
        "billing",
        Some(billing_tls),
        billing_types(),
        Duration::from_secs(1),
    );
    let orders = peer(
        &hub.address,
        "orders",
        Some(orders_tls),
        RegisteredTypes::default(),
        Duration::from_secs(1),
    );
    assert!(billing.conn.await_registered(Duration::from_secs(15)).await);
    assert!(orders.conn.await_registered(Duration::from_secs(15)).await);
    let invoice = orders
        .sender
        .send_query(&GetInvoice { id: 7 }, SendOptions::default())
        .await
        .expect(
            "orders may send billing.* to the billing service, authenticated by its certificate",
        );
    assert_eq!(
        invoice,
        Invoice {
            id: 7,
            owner: "billing".into()
        }
    );
    billing.conn.shutdown().await;
    orders.conn.shutdown().await;
}

#[tokio::test]
async fn a_send_outside_the_certificates_service_entry_is_access_denied() {
    require_docker!();
    let f = fixture();
    let hub = f.hub("required", None, None).await;
    let (orders_tls, _) = f.tls("orders", &ClientSpec::service("orders"));
    let orders = peer(
        &hub.address,
        "orders",
        Some(orders_tls),
        RegisteredTypes::default(),
        Duration::from_secs(1),
    );
    assert!(orders.conn.await_registered(Duration::from_secs(15)).await);
    let denied = orders
        .sender
        .send_query(&ListFiles {}, SendOptions::default())
        .await
        .expect_err("files.* is outside orders' send list");
    assert!(
        matches!(denied, MessagingError::AccessDenied(ref m) if m.contains("files.ListFiles")),
        "{denied:?}"
    );
    orders.conn.shutdown().await;
}

#[tokio::test]
async fn a_client_without_a_certificate_fails_at_the_transport_and_retries_without_error_spam() {
    require_docker!();
    let f = fixture();
    let hub = f.hub("required", None, None).await;
    let (mut tls, _) = f.tls("billing", &ClientSpec::service("billing"));
    tls.client_cert_file = None;
    tls.client_key_file = None;
    let captured = Captured::default();
    let _logs = captured.install();
    let p = peer(
        &hub.address,
        "billing",
        Some(tls),
        billing_types(),
        Duration::from_millis(300),
    );
    // The hint (the first failure can be a refused connect that carries none).
    let started = Instant::now();
    while !captured.text().contains("client certificate") {
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "{}",
            captured.text()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // It keeps trying: several attempts, none of them a registration rejection.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(p.conn.handshake().is_none());
    assert_eq!(p.conn.last_rejection(), None);
    let log = captured.text();
    assert!(!log.contains("rejected the registration"), "{log}");
    let errors = log.lines().filter(|l| l.contains(" ERROR ")).count();
    assert_eq!(errors, 0, "{log}");
    let attempts = log.matches("hub stream closed").count();
    assert!(attempts >= 2, "{attempts} attempts: {log}");
    p.conn.shutdown().await;
}

#[tokio::test]
async fn a_certificate_naming_another_service_is_refused() {
    require_docker!();
    let f = fixture();
    let hub = f.hub("required", None, None).await;
    // The library checks it before opening a stream.
    let (orders_tls, _) = f.tls("orders", &ClientSpec::service("orders"));
    let p = peer(
        &hub.address,
        "billing",
        Some(orders_tls.clone()),
        billing_types(),
        Duration::from_secs(1),
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    while p.conn.last_rejection().is_none() {
        assert!(Instant::now() < deadline, "never refused");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let rejection = p.conn.last_rejection().unwrap();
    assert!(
        rejection.contains("orders") && rejection.contains("billing"),
        "{rejection}"
    );
    p.conn.shutdown().await;

    // The hub refuses it too: a raw client registers as billing with orders' certificate.
    let address = hub.address.replace("static://", "https://");
    let channel = tonic::transport::Endpoint::from_shared(address)
        .unwrap()
        .tls_config(
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(&f.pki.ca_pem))
                .identity(tonic::transport::Identity::from_pem(
                    std::fs::read(orders_tls.client_cert_file.as_ref().unwrap()).unwrap(),
                    std::fs::read(orders_tls.client_key_file.as_ref().unwrap()).unwrap(),
                )),
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = kraken_messaging::proto::hub_client::HubClient::new(channel);
    let (tx, rx) = mpsc::channel(4);
    tx.send(ClientMessage {
        message: Some(client_message::Message::Register(proto::Register {
            service_name: "billing".into(),
            instance_id: uuid::Uuid::new_v4().to_string(),
            protocol_version: 2,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut stream = client
        .connect(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .expect("the stream opens; the Register is judged on it")
        .into_inner();
    let status = stream.message().await.expect_err("refused");
    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
}

#[tokio::test]
async fn the_hub_closing_at_not_after_leads_to_a_reconnect_with_the_renewed_file() {
    require_docker!();
    let f = fixture();
    let hub = f.hub("required", None, None).await;
    // About 8 s to live, from an hour ago.
    let (tls, short) = f.tls(
        "orders",
        &ClientSpec::service("orders").living(Duration::from_secs(8)),
    );
    let captured = Captured::default();
    let _logs = captured.install();
    let p = peer(
        &hub.address,
        "orders",
        Some(tls.clone()),
        RegisteredTypes::default(),
        Duration::from_secs(1),
    );
    assert!(p.conn.await_registered(Duration::from_secs(15)).await);
    assert_eq!(p.conn.auth().certificate.unwrap().fingerprint, short);
    // The hub closes the session at notAfter: UNAUTHENTICATED "certificate expired".
    let deadline = Instant::now() + Duration::from_secs(20);
    while !captured.text().contains("certificate expired") {
        assert!(
            Instant::now() < deadline,
            "the hub never closed the session: {}",
            captured.text()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!p.conn.is_connected() || p.conn.handshake().is_none());
    // The file still holds the expired certificate: nothing is offered, the error is logged once.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let expired_errors = captured
        .lines()
        .iter()
        .filter(|l| l.contains(" ERROR ") && l.contains(&short) && l.contains("expired at"))
        .count();
    assert_eq!(expired_errors, 1, "{}", captured.text());
    // The renewed file is picked up by the next attempt.
    let renewed = f.renew(&tls, &ClientSpec::service("orders"));
    assert!(p.conn.await_registered(Duration::from_secs(15)).await);
    let cert = p.conn.auth().certificate.unwrap();
    assert_eq!((cert.fingerprint, cert.rotations), (renewed, 1));
    p.conn.shutdown().await;
}

#[tokio::test]
async fn rotating_the_files_moves_the_session_to_the_new_certificate_with_no_failed_request() {
    require_docker!();
    rotation(Rotated::Sender).await;
}

/// The single owner instance of `billing.GetInvoice` is the one that rotates: while its hub is
/// rolled over it is deregistered there, the hub answers `NO_HANDLER`, and the sender moves the
/// request to the other hub, where the owner still is.
#[tokio::test]
async fn rotating_the_only_owners_files_leaves_no_request_without_a_handler() {
    require_docker!();
    rotation(Rotated::Owner).await;
}

#[derive(Clone, Copy)]
enum Rotated {
    /// `orders`, which sends through the pool.
    Sender,
    /// `billing`, the one instance that handles the query.
    Owner,
}

async fn rotation(rotated: Rotated) {
    let f = fixture();
    let (a, b) = tokio::join!(
        f.hub("required", Some(free_port()), Some("mtls-hub-1")),
        f.hub("required", Some(free_port()), Some("mtls-hub-2")),
    );
    let both = format!("{},{}", a.address, b.address);
    let (billing_tls, billing_first) = f.tls("billing", &ClientSpec::service("billing"));
    let (orders_tls, orders_first) = f.tls("orders", &ClientSpec::service("orders"));
    let (rotated_name, rotated_tls, first) = match rotated {
        Rotated::Sender => ("orders", orders_tls.clone(), orders_first),
        Rotated::Owner => ("billing", billing_tls.clone(), billing_first),
    };

    // billing serves on both hubs (its own pool); the files checked every 300 ms.
    let (btx, brx) = mpsc::channel::<Inbound>(64);
    let billing = HubPool::new(
        HubSettings {
            address: both.clone(),
            heartbeat_interval: Duration::from_millis(300),
            backoff_min: Duration::from_millis(100),
            backoff_max: Duration::from_secs(1),
            tls: Some(billing_tls),
            ..HubSettings::default()
        },
        "billing".into(),
        uuid::Uuid::new_v4().to_string(),
        btx,
    )
    .unwrap();
    billing.set_types(vec![], vec!["billing.GetInvoice".into()]);
    let billing_sender = Sender::with_pool(Arc::clone(&billing), SenderSettings::default());
    serve("billing", brx, billing_sender);
    billing.start();

    // orders sends through its own pool.
    let (otx, orx) = mpsc::channel::<Inbound>(64);
    let orders = HubPool::new(
        HubSettings {
            address: both,
            heartbeat_interval: Duration::from_millis(300),
            backoff_min: Duration::from_millis(100),
            backoff_max: Duration::from_secs(1),
            tls: Some(orders_tls.clone()),
            ..HubSettings::default()
        },
        "orders".into(),
        uuid::Uuid::new_v4().to_string(),
        otx,
    )
    .unwrap();
    let sender = Sender::with_pool(Arc::clone(&orders), SenderSettings::default());
    serve("orders", orx, Arc::clone(&sender));
    orders.start();
    assert!(billing.await_all_registered(Duration::from_secs(20)).await);
    assert!(orders.await_all_registered(Duration::from_secs(20)).await);
    let rotating = match rotated {
        Rotated::Sender => &orders,
        Rotated::Owner => &billing,
    };
    assert_eq!(rotating.auth().certificate.unwrap().fingerprint, first);

    let (sent, failed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let first_failure = Arc::new(std::sync::Mutex::new(None::<String>));
    let running = Arc::new(AtomicBool::new(true));
    let trickle = {
        let (sender, sent, failed, running, first_failure) = (
            Arc::clone(&sender),
            Arc::clone(&sent),
            Arc::clone(&failed),
            Arc::clone(&running),
            Arc::clone(&first_failure),
        );
        tokio::spawn(async move {
            let mut id = 0;
            while running.load(Ordering::SeqCst) {
                id += 1;
                match sender
                    .send_query(&GetInvoice { id }, SendOptions::default())
                    .await
                {
                    Ok(invoice) if invoice.id == id => {}
                    other => {
                        failed.fetch_add(1, Ordering::SeqCst);
                        first_failure
                            .lock()
                            .unwrap()
                            .get_or_insert(format!("{other:?}"));
                    }
                }
                sent.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    let second = f.renew(&rotated_tls, &ClientSpec::service(rotated_name));
    assert_ne!(first, second);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = rotating.auth().certificate.unwrap();
        if status.fingerprint == second
            && rotating
                .await_all_registered(Duration::from_millis(50))
                .await
        {
            assert_eq!(status.rotations, 1);
            break;
        }
        assert!(Instant::now() < deadline, "still presenting {status:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    running.store(false, Ordering::SeqCst);
    trickle.await.unwrap();
    assert!(
        sent.load(Ordering::SeqCst) > 100,
        "{} sent",
        sent.load(Ordering::SeqCst)
    );
    assert_eq!(
        failed.load(Ordering::SeqCst),
        0,
        "{} of {} failed, the first: {:?}",
        failed.load(Ordering::SeqCst),
        sent.load(Ordering::SeqCst),
        first_failure.lock().unwrap()
    );
    orders.shutdown().await;
    billing.shutdown().await;
}
