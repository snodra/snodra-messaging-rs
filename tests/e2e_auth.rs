//! End to end against the real hub image (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`) running
//! `KRAKEN_HUB_AUTH_MODE=enforce` with an ACL file and a TLS certificate, both generated at test
//! time and mounted in (nothing is checked in): a service with the right token registers over TLS
//! and serves a query from a second service, a wrong token is refused with `UNAUTHENTICATED`, a
//! token used for the wrong service or type is refused with `PERMISSION_DENIED`, a send outside the
//! ACL is `AccessDenied`, and plain text against the TLS hub fails cleanly. No Kafka or MongoDB:
//! the participants are a `HubConnection` and a `Sender`.
mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use kraken_messaging::hub::{
    HubConnection, HubSettings, Inbound, RegisteredTypes, SendOptions, Sender,
};
use kraken_messaging::proto::{self, ClientMessage, client_message, reply};
use kraken_messaging::{HubTls, MessagingError, Query, SecretString, message_type};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use support::containers::{HubContainer, HubOptions, hub_with};
use support::logs::Captured;
use support::pki::{Pki, TempDir};
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

/// A random token, as an operator would generate one (`openssl rand -hex 32`).
fn random_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// One `[[token]]` entry: the hub keeps the SHA-256 of the token only.
fn acl_entry(token: &str, service: &str, handle: &[&str], send: &[&str]) -> String {
    let list = |items: &[&str]| {
        items
            .iter()
            .map(|i| format!("\"{i}\""))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let digest: String = Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!(
        "[[token]]\ntoken_sha256 = \"{digest}\"\nservice = \"{service}\"\nhandle = [{}]\nsend = [{}]\n\n",
        list(handle),
        list(send)
    )
}

/// The enforcing TLS hub, its CA, and the two tokens of the ACL.
struct Fixture {
    hub: HubContainer,
    ca: std::path::PathBuf,
    billing_token: String,
    orders_token: String,
    _dir: TempDir,
}

impl Fixture {
    fn tls(&self) -> Option<HubTls> {
        Some(HubTls {
            ca_file: Some(self.ca.clone()),
            server_name: None,
            ..HubTls::default()
        })
    }
}

/// `billing` handles `billing.*` and sends nothing; `orders` handles `orders.*` and may send
/// `billing.*` only.
async fn fixture() -> Fixture {
    let dir = TempDir::new();
    let pki = Pki::new("kraken test CA");
    let (cert, key) = pki.server(&[]);
    let ca = dir.write("ca.pem", &pki.ca_pem);
    dir.write("hub-cert.pem", &cert);
    dir.write("hub-key.pem", &key);
    let (billing_token, orders_token) = (random_token(), random_token());
    dir.write(
        "acl.toml",
        &(acl_entry(&billing_token, "billing", &["billing.*"], &[])
            + &acl_entry(&orders_token, "orders", &["orders.*"], &["billing.*"])),
    );
    let hub = hub_with(
        HubOptions::new()
            .env("KRAKEN_HUB_AUTH_MODE", "enforce")
            .env("KRAKEN_HUB_ACL_FILE", "/etc/kraken/acl.toml")
            .env("KRAKEN_HUB_TLS_CERT", "/etc/kraken/hub-cert.pem")
            .env("KRAKEN_HUB_TLS_KEY", "/etc/kraken/hub-key.pem")
            .mount(dir.dir(), "/etc/kraken"),
    )
    .await;
    Fixture {
        hub,
        ca,
        billing_token,
        orders_token,
        _dir: dir,
    }
}

struct Peer {
    conn: Arc<HubConnection>,
    sender: Arc<Sender>,
}

impl Peer {
    async fn shutdown(self) {
        self.conn.shutdown().await;
    }
}

/// A participant of `service` with `token` and `tls`, registering `types`; it answers
/// `billing.GetInvoice` with an `Invoice` naming the service.
fn peer(
    f: &Fixture,
    service: &str,
    token: &str,
    tls: Option<HubTls>,
    types: RegisteredTypes,
) -> Peer {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let conn = HubConnection::new(
        HubSettings {
            address: f.hub.address.clone(),
            backoff_min: Duration::from_millis(100),
            backoff_max: Duration::from_secs(1),
            token: Some(SecretString::new(token)),
            tls,
            ..HubSettings::default()
        },
        service.to_string(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .expect("valid hub settings");
    conn.set_registered_types(types);
    let sender = Sender::new(Arc::clone(&conn), Some(Duration::from_secs(5)), false);
    let forward = Arc::clone(&sender);
    let name = service.to_string();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { request, reply_to } => {
                    let invoice = serde_json::from_slice::<serde_json::Value>(&request.payload)
                        .ok()
                        .and_then(|v| v["id"].as_u64())
                        .unwrap_or(0) as u32;
                    let payload = serde_json::to_vec(&Invoice {
                        id: invoice,
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
async fn a_service_with_the_right_token_registers_over_tls_and_serves_a_query() {
    require_docker!();
    let f = fixture().await;
    let billing = peer(&f, "billing", &f.billing_token, f.tls(), billing_types());
    let orders = peer(
        &f,
        "orders",
        &f.orders_token,
        f.tls(),
        RegisteredTypes::default(),
    );
    assert!(billing.conn.await_registered(Duration::from_secs(15)).await);
    assert!(orders.conn.await_registered(Duration::from_secs(15)).await);

    let invoice = orders
        .sender
        .send_query(&GetInvoice { id: 7 }, SendOptions::default())
        .await
        .expect("orders may send billing.* to the billing service over TLS");
    assert_eq!(
        invoice,
        Invoice {
            id: 7,
            owner: "billing".into()
        }
    );
    billing.shutdown().await;
    orders.shutdown().await;
}

#[tokio::test]
async fn a_wrong_token_is_rejected_with_unauthenticated() {
    require_docker!();
    let f = fixture().await;
    let captured = Captured::default();
    let _logs = captured.install();
    let intruder = peer(&f, "billing", &random_token(), f.tls(), billing_types());
    let deadline = Instant::now() + Duration::from_secs(15);
    while intruder.conn.last_rejection().is_none() {
        assert!(
            Instant::now() < deadline,
            "never rejected: {}",
            captured.text()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!intruder.conn.is_connected() || intruder.conn.handshake().is_none());
    assert!(
        captured.text().contains("code=Unauthenticated"),
        "{}",
        captured.text()
    );
    let rejection = intruder.conn.last_rejection().unwrap();
    assert!(rejection.contains("token"), "{rejection}");
    intruder.shutdown().await;
}

#[tokio::test]
async fn a_token_used_for_the_wrong_service_or_type_is_rejected_with_permission_denied() {
    require_docker!();
    let f = fixture().await;
    for (service, types, text) in [
        ("orders", RegisteredTypes::default(), "orders"),
        (
            "billing",
            RegisteredTypes {
                queries: vec!["orders.Secret".into()],
                ..RegisteredTypes::default()
            },
            "orders.Secret",
        ),
    ] {
        let captured = Captured::default();
        let _logs = captured.install();
        // The billing token, presented as another service or for a type it may not handle.
        let p = peer(&f, service, &f.billing_token, f.tls(), types);
        let deadline = Instant::now() + Duration::from_secs(15);
        while p.conn.last_rejection().is_none() {
            assert!(
                Instant::now() < deadline,
                "never rejected: {}",
                captured.text()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            captured.text().contains("code=PermissionDenied"),
            "{}",
            captured.text()
        );
        let rejection = p.conn.last_rejection().unwrap();
        assert!(rejection.contains(text), "{rejection}");
        p.shutdown().await;
    }
}

#[tokio::test]
async fn a_send_outside_the_acl_is_access_denied() {
    require_docker!();
    let f = fixture().await;
    let orders = peer(
        &f,
        "orders",
        &f.orders_token,
        f.tls(),
        RegisteredTypes::default(),
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
    orders.shutdown().await;
}

#[tokio::test]
async fn plain_text_against_the_tls_hub_fails_cleanly() {
    require_docker!();
    let f = fixture().await;
    let captured = Captured::default();
    let _logs = captured.install();
    let plain = peer(&f, "billing", &f.billing_token, None, billing_types());
    let started = Instant::now();
    // Not a hang: the hint is logged within a few seconds. The first failure can be a refused
    // connect (the host-mapped port not forwarding yet) that carries no hint, so wait for the
    // hint itself, not for the first "hub stream closed".
    while !captured.text().contains("KRAKEN_HUB_TLS=true") {
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "no hint logged: {}",
            captured.text()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(captured.text().contains("hub stream closed"));
    assert!(plain.conn.handshake().is_none());
    assert!(!captured.text().contains(&f.billing_token));
    plain.shutdown().await;
}
