//! JWT authentication, client side (roadmap P5b, ADR 0019) against the in-process fake hub and a
//! test OAuth2 token endpoint: the client-credentials token goes in `Register.credentials.jwt`,
//! one provider serves every hub connection, a refreshed token reaches the hubs that offer
//! `credentials-refresh` as `ClientMessage.credentials`, and nothing secret is ever logged.
mod support;

use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::hub::{HubConnection, HubSettings, Inbound};
use kraken_messaging::proto::{self, ClientMessage, client_message};
use support::fake_hub::FakeHub;
use tokio::sync::mpsc;

fn connection(settings: HubSettings, service: &str) -> Arc<HubConnection> {
    let (tx, rx) = mpsc::channel::<Inbound>(64);
    // Nobody reads inbound traffic in these tests.
    tokio::spawn(async move {
        let mut rx = rx;
        while rx.recv().await.is_some() {}
    });
    HubConnection::new(
        settings,
        service.into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
}

#[tokio::test]
async fn a_credentials_message_reaches_the_hub_and_is_recorded() {
    let hub = FakeHub::start().await;
    let conn = connection(
        HubSettings {
            address: hub.address(),
            heartbeat_interval: Duration::from_secs(1),
            ..HubSettings::default()
        },
        "svc",
    );
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    conn.send(ClientMessage {
        message: Some(client_message::Message::Credentials(proto::Credentials {
            kind: Some(proto::credentials::Kind::Jwt("a.b.c".into())),
        })),
    })
    .await
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while hub.refreshes().is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "no refresh seen");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let refreshes = hub.refreshes();
    assert_eq!(refreshes.len(), 1);
    assert_eq!(
        refreshes[0].1.kind,
        Some(proto::credentials::Kind::Jwt("a.b.c".into()))
    );
    conn.shutdown().await;
    hub.stop();
}
