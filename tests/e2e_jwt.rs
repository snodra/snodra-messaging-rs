//! End to end against the real hub image (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`) running
//! `KRAKEN_HUB_AUTH_MODE=enforce` with JWT authentication: the test identity provider
//! (`support::idp`: a token endpoint on the host and ES256 keys generated at test time) issues the
//! tokens; its JWKS is served to the hub by a small container on the same bridge network (the hub
//! reaches it as `keycloak`; reaching the test host from a container is runtime-specific). A
//! `[[service]]` ACL entry gives a JWT service its rights; a static-token service (`orders`) sits
//! beside it. No Kafka or MongoDB: the participants are a `HubConnection` and a `Sender`.
mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use kraken_messaging::hub::{
    HubConnection, HubSettings, Inbound, RegisteredTypes, SendOptions, Sender, TokenProvider,
};
use kraken_messaging::proto::{self, ClientMessage, client_message, reply};
use kraken_messaging::{HubOAuth2, MessagingError, Query, SecretString, message_type};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use support::containers::{
    HubContainer, HubOptions, STATIC_SERVER_PORT, StaticServer, hub_with, static_server,
};
use support::idp::Idp;
use support::logs::Captured;
use support::pki::TempDir;
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

const SECRET: &str = "e2e-client-secret";

fn random_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// The hub in `enforce` with JWT on: `billing` (a `[[service]]`: handles `billing.*`, sends
/// nothing) and `orders` (a `[[token]]`: sends `billing.*`).
struct Fixture {
    hub: HubContainer,
    idp: Idp,
    orders_token: String,
    _jwks: StaticServer,
    _dir: TempDir,
}

/// `lifetime`: seconds the test identity provider's tokens live.
async fn fixture(lifetime: u64) -> Fixture {
    let idp = Idp::start("billing", SECRET).await;
    idp.set_lifetime(lifetime);
    let dir = TempDir::new();
    let jwks = static_server(dir.dir(), "certs", &idp.jwks()).await;
    let orders_token = random_token();
    let digest: String = Sha256::digest(orders_token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    dir.write(
        "acl.toml",
        &format!(
            "[[service]]\nname = \"billing\"\nhandle = [\"billing.*\"]\nsend = []\n\n\
             [[token]]\ntoken_sha256 = \"{digest}\"\nservice = \"orders\"\nhandle = []\nsend = [\"billing.*\"]\n"
        ),
    );
    let hub = hub_with(
        HubOptions::new()
            .env("KRAKEN_HUB_AUTH_MODE", "enforce")
            .env("KRAKEN_HUB_ACL_FILE", "/etc/kraken/acl.toml")
            .env("KRAKEN_HUB_JWT_ISSUER", &idp.issuer())
            .env(
                "KRAKEN_HUB_JWT_JWKS_URI",
                &format!("http://keycloak:{STATIC_SERVER_PORT}/certs"),
            )
            .env("KRAKEN_HUB_JWT_LEEWAY", "0s")
            .env("KRAKEN_HUB_JWT_EXPIRY_GRACE", "2s")
            .mount(dir.dir(), "/etc/kraken")
            .host_name("keycloak", jwks.ip),
    )
    .await;
    Fixture {
        hub,
        idp,
        orders_token,
        _jwks: jwks,
        _dir: dir,
    }
}

struct Peer {
    conn: Arc<HubConnection>,
    sender: Arc<Sender>,
}

/// A participant of `service` that answers `billing.GetInvoice` with an `Invoice` naming it;
/// `settings` has everything but the address and the backoff.
fn peer(f: &Fixture, service: &str, settings: HubSettings, types: RegisteredTypes) -> Peer {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let conn = HubConnection::new(
        HubSettings {
            address: f.hub.address.clone(),
            backoff_min: Duration::from_millis(100),
            backoff_max: Duration::from_secs(1),
            ..settings
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
    conn.start();
    Peer { conn, sender }
}

fn provider(f: &Fixture, client_id: &str) -> Arc<TokenProvider> {
    TokenProvider::new(HubOAuth2 {
        token_uri: f.idp.token_uri(),
        client_id: client_id.into(),
        client_secret: Some(SecretString::new(SECRET)),
        ..HubOAuth2::default()
    })
    .unwrap()
}

fn jwt_peer(f: &Fixture, service: &str) -> Peer {
    peer(
        f,
        service,
        HubSettings {
            token_provider: Some(provider(f, "billing")),
            ..HubSettings::default()
        },
        RegisteredTypes {
            queries: vec!["billing.GetInvoice".into()],
            ..RegisteredTypes::default()
        },
    )
}

fn token_peer(f: &Fixture, service: &str) -> Peer {
    peer(
        f,
        service,
        HubSettings {
            token: Some(SecretString::new(&f.orders_token)),
            ..HubSettings::default()
        },
        RegisteredTypes::default(),
    )
}

/// The peer is registered, else the test fails with what the peer and the hub say.
async fn registered(f: &Fixture, p: &Peer) {
    if p.conn.await_registered(REGISTER_WITHIN).await {
        return;
    }
    let logs = f.hub.container.stdout_to_vec().await.unwrap_or_default();
    panic!(
        "not registered: rejection {:?}, token error {:?}\nhub log:\n{}",
        p.conn.last_rejection(),
        p.conn.auth().last_error,
        String::from_utf8_lossy(&logs)
    );
}

// A JWT Register can be refused with UNAVAILABLE "jwks not loaded yet" for a moment after the hub
// starts (it fetches the keys in the background): the client retries, so allow for it.
const REGISTER_WITHIN: Duration = Duration::from_secs(20);

#[tokio::test]
async fn a_jwt_service_registers_and_serves_a_command_from_a_token_service() {
    require_docker!();
    let f = fixture(300).await;
    let billing = jwt_peer(&f, "billing");
    let orders = token_peer(&f, "orders");
    registered(&f, &billing).await;
    registered(&f, &orders).await;

    let invoice = orders
        .sender
        .send_query(&GetInvoice { id: 7 }, SendOptions::default())
        .await
        .expect("orders may send billing.* to the JWT service");
    assert_eq!(
        invoice,
        Invoice {
            id: 7,
            owner: "billing".into()
        }
    );
    // The hub saw a JWT session: the provider's token was the credential.
    assert_eq!(f.idp.issued(), 1);
    assert_eq!(
        billing.conn.auth().kind,
        kraken_messaging::hub::AuthKind::Jwt
    );
    billing.conn.shutdown().await;
    orders.conn.shutdown().await;
}

#[tokio::test]
async fn a_send_outside_the_service_entry_is_access_denied() {
    require_docker!();
    let f = fixture(300).await;
    let billing = jwt_peer(&f, "billing");
    registered(&f, &billing).await;
    let denied = billing
        .sender
        .send_query(&ListFiles {}, SendOptions::default())
        .await
        .expect_err("the billing entry sends nothing");
    assert!(
        matches!(denied, MessagingError::AccessDenied(ref m) if m.contains("files.ListFiles")),
        "{denied:?}"
    );
    billing.conn.shutdown().await;
}

#[tokio::test]
async fn a_token_for_another_service_is_refused() {
    require_docker!();
    let f = fixture(300).await;
    let captured = Captured::default();
    let _logs = captured.install();
    // The identity provider issues the token with azp "orders", registered as "billing".
    f.idp.set_azp(Some("orders"));
    let wrong = jwt_peer(&f, "billing");
    let deadline = Instant::now() + REGISTER_WITHIN;
    while !wrong
        .conn
        .last_rejection()
        .is_some_and(|r| r.contains("azp"))
    {
        assert!(
            Instant::now() < deadline,
            "never refused for its azp: {:?} {}",
            wrong.conn.last_rejection(),
            captured.text()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(wrong.conn.handshake().is_none());
    assert!(
        captured.text().contains("code=PermissionDenied"),
        "{}",
        captured.text()
    );
    assert!(!captured.text().contains(SECRET));
    wrong.conn.shutdown().await;
}

/// The token lives 6 s and the hub closes a JWT session 2 s after it expires (no leeway): the
/// session is still the same stream after 10 s because the provider refreshed the token in the
/// background and the connection sent `ClientMessage.credentials`.
#[tokio::test]
async fn the_session_outlives_the_token_because_the_provider_refreshes_it() {
    require_docker!();
    let f = fixture(6).await;
    let billing = jwt_peer(&f, "billing");
    let orders = token_peer(&f, "orders");
    registered(&f, &billing).await;
    registered(&f, &orders).await;
    let stream = billing.conn.current_stream().expect("a live stream");
    let started = Instant::now();

    // Past exp + grace (8 s after the first token), with margin.
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(started.elapsed() > Duration::from_secs(9));
    assert_eq!(
        billing.conn.current_stream(),
        Some(stream),
        "no reconnect: the hub accepted the refresh"
    );
    assert!(f.idp.issued() >= 2, "issued {}", f.idp.issued());
    assert!(
        billing.conn.credentials_refreshes_sent() >= 1,
        "{}",
        billing.conn.credentials_refreshes_sent()
    );
    let invoice = orders
        .sender
        .send_query(&GetInvoice { id: 9 }, SendOptions::default())
        .await
        .expect("still serving");
    assert_eq!(invoice.owner, "billing");
    billing.conn.shutdown().await;
    orders.conn.shutdown().await;
}
