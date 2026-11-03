//! Mutual TLS to the hub (roadmap P5c, hub ADR 0020) against the in-process fake hub, which
//! verifies client certificates against a CA generated at test time: the connection presents its
//! certificate and sends no credentials, a client without one fails at the transport, a pair that
//! names another service or has expired is refused before a stream is opened, the files are read
//! again on every connect, and the auth status shows the certificate.
mod support;

use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::hub::{AuthKind, HubConnection, HubSettings, Inbound};
use kraken_messaging::{HubTls, SecretString};
use support::fake_hub::FakeHub;
use support::logs::Captured;
use support::pki::{ClientSpec, Pki, TempDir, fingerprint_of_pem};
use tokio::sync::mpsc;

/// The hub's CA, the client CA and a hub that requires (or accepts) client certificates.
struct World {
    dir: TempDir,
    server_ca: Pki,
    client_ca: Pki,
    hub: FakeHub,
}

async fn world(required: bool) -> World {
    let (dir, server_ca, client_ca) = (TempDir::new(), Pki::new("hub CA"), Pki::new("client CA"));
    let (cert, key) = server_ca.server(&["kraken-hub"]);
    let hub = FakeHub::start_mtls(&cert, &key, &client_ca.ca_pem, required).await;
    World {
        dir,
        server_ca,
        client_ca,
        hub,
    }
}

impl World {
    /// The service's pair written to `<name>-cert.pem` / `<name>-key.pem`; its fingerprint.
    fn issue(&self, file: &str, spec: &ClientSpec) -> (HubTls, String) {
        let (cert, key) = self.client_ca.client(spec);
        let tls = HubTls {
            ca_file: Some(self.dir.write("hub-ca.pem", &self.server_ca.ca_pem)),
            client_cert_file: Some(self.dir.write(&format!("{file}-cert.pem"), &cert)),
            client_key_file: Some(self.dir.write(&format!("{file}-key.pem"), &key)),
            ..HubTls::default()
        };
        let fingerprint = fingerprint_of_pem(&cert);
        (tls, fingerprint)
    }

    /// Replaces the pair behind `tls` with a new one (a renewal) and returns its fingerprint.
    fn renew(&self, tls: &HubTls, spec: &ClientSpec) -> String {
        let (cert, key) = self.client_ca.client(spec);
        std::fs::write(tls.client_cert_file.as_ref().unwrap(), &cert).unwrap();
        std::fs::write(tls.client_key_file.as_ref().unwrap(), &key).unwrap();
        fingerprint_of_pem(&cert)
    }
}

fn settings(address: String, tls: HubTls) -> HubSettings {
    HubSettings {
        address,
        heartbeat_interval: Duration::from_millis(200),
        backoff_min: Duration::from_millis(20),
        backoff_max: Duration::from_millis(200),
        handshake_timeout: Duration::from_secs(3),
        tls: Some(tls),
        ..HubSettings::default()
    }
}

fn connection(settings: HubSettings, service: &str) -> Arc<HubConnection> {
    let (tx, mut rx) = mpsc::channel::<Inbound>(16);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    HubConnection::new(
        settings,
        service.into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
}

#[tokio::test]
async fn registers_presenting_the_certificate_and_sends_no_credentials() {
    support::init_tracing();
    let w = world(true).await;
    let (tls, fingerprint) = w.issue("svc", &ClientSpec::service("svc"));
    let conn = connection(
        HubSettings {
            // A token next to a certificate is a stale one: the hub would refuse it.
            token: Some(SecretString::new("stale-token")),
            ..settings(w.hub.address(), tls)
        },
        "svc",
    );
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let registers = w.hub.registers().await;
    assert_eq!(registers.len(), 1);
    assert!(
        registers[0].credentials.is_none(),
        "{:?}",
        registers[0].credentials
    );
    assert_eq!(w.hub.peer_fingerprints(), [(1, fingerprint)]);
    conn.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn a_client_without_a_certificate_fails_at_the_transport_not_as_a_rejection() {
    let w = world(true).await;
    let (mut tls, _) = w.issue("svc", &ClientSpec::service("svc"));
    tls.client_cert_file = None;
    tls.client_key_file = None;
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(w.hub.address(), tls), "svc");
    conn.start();
    assert!(!conn.await_registered(Duration::from_millis(1200)).await);
    assert!(w.hub.registers().await.is_empty());
    assert_eq!(
        conn.last_rejection(),
        None,
        "a handshake failure is not a rejection"
    );
    let log = captured.text();
    assert!(!log.contains("rejected the registration"), "{log}");
    assert!(
        log.contains("client certificate"),
        "the log does not say that the hub may require a client certificate: {log}"
    );
    conn.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn a_certificate_from_another_ca_fails_the_handshake() {
    let w = world(true).await;
    let rogue = Pki::new("rogue CA");
    let (mut tls, _) = w.issue("svc", &ClientSpec::service("svc"));
    let (cert, key) = rogue.client_for("svc");
    std::fs::write(tls.client_cert_file.as_ref().unwrap(), cert).unwrap();
    std::fs::write(tls.client_key_file.as_ref().unwrap(), key).unwrap();
    tls.ca_file = Some(w.dir.write("hub-ca.pem", &w.server_ca.ca_pem));
    let conn = connection(settings(w.hub.address(), tls), "svc");
    conn.start();
    assert!(!conn.await_registered(Duration::from_millis(1200)).await);
    assert!(w.hub.registers().await.is_empty());
    conn.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn a_pair_that_names_another_service_is_refused_before_a_stream_opens() {
    let w = world(true).await;
    let (tls, _) = w.issue("orders", &ClientSpec::service("orders"));
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(w.hub.address(), tls), "billing");
    conn.start();
    assert!(!conn.await_registered(Duration::from_millis(800)).await);
    let rejection = conn.last_rejection().expect("a rejection");
    assert!(
        rejection.contains("orders") && rejection.contains("billing"),
        "{rejection}"
    );
    assert_eq!(
        w.hub.streams(),
        0,
        "a stream was opened with a certificate for another service"
    );
    let errors = captured
        .lines()
        .iter()
        .filter(|l| l.contains("ERROR") && l.contains("orders"))
        .count();
    assert_eq!(errors, 1, "{}", captured.text());
    conn.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn an_expired_certificate_is_not_offered_and_the_error_is_logged_once() {
    let w = world(true).await;
    let now = time::OffsetDateTime::now_utc();
    let (tls, _) = w.issue(
        "svc",
        &ClientSpec {
            not_before: now - time::Duration::hours(2),
            not_after: now - time::Duration::seconds(30),
            ..ClientSpec::service("svc")
        },
    );
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(w.hub.address(), tls), "svc");
    conn.start();
    assert!(!conn.await_registered(Duration::from_millis(1000)).await);
    assert!(conn.last_rejection().is_some_and(|r| r.contains("expired")));
    assert_eq!(w.hub.streams(), 0);
    let errors = captured
        .lines()
        .iter()
        .filter(|l| l.contains("ERROR") && l.contains("expired"))
        .count();
    assert_eq!(errors, 1, "{}", captured.text());
    conn.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn the_files_are_read_again_on_every_connect() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let conn = connection(settings(w.hub.address(), tls.clone()), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    assert_ne!(first, second);
    w.hub.drop_streams();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while w.hub.peer_fingerprints().len() < 2 {
        assert!(std::time::Instant::now() < deadline, "no reconnect");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        w.hub
            .peer_fingerprints()
            .into_iter()
            .map(|(_, f)| f)
            .collect::<Vec<_>>(),
        [first, second]
    );
    conn.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn the_auth_status_shows_the_certificate_and_counts_its_rotations() {
    let w = world(true).await;
    let spec = ClientSpec::service("svc");
    let (tls, first) = w.issue("svc", &spec);
    let conn = connection(settings(w.hub.address(), tls.clone()), "svc");
    let before = conn.auth();
    assert_eq!(before.kind, AuthKind::Certificate);
    assert!(before.certificate.is_none(), "nothing presented yet");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let status = conn.auth();
    assert_eq!(status.kind, AuthKind::Certificate);
    assert_eq!(AuthKind::Certificate.label(), "certificate");
    let cert = status.certificate.expect("a certificate");
    assert_eq!(cert.fingerprint, first);
    assert_eq!(cert.service, "svc");
    assert_eq!(cert.rotations, 0);
    let expected = spec.not_after.unix_timestamp() as u64;
    let got = cert
        .not_after
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert_eq!(got, expected);
    assert_eq!(status.token_expires_at, None);
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    w.hub.drop_streams();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(c) = conn.auth().certificate
            && c.fingerprint == second
        {
            assert_eq!(c.rotations, 1);
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{:?}", conn.auth());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    conn.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn the_hub_closing_at_not_after_is_a_reconnect_with_the_renewed_file() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let conn = connection(settings(w.hub.address(), tls.clone()), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    w.hub
        .close_streams_with(tonic::Code::Unauthenticated, "certificate expired");
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while w.hub.peer_fingerprints().len() < 2 {
        assert!(std::time::Instant::now() < deadline, "no reconnect");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(w.hub.peer_fingerprints()[1].1, second);
    assert_ne!(first, second);
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    conn.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn files_caught_half_way_through_a_rotation_keep_the_last_good_pair() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(w.hub.address(), tls.clone()), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    // The certificate file is caught empty (replaced non-atomically).
    std::fs::write(tls.client_cert_file.as_ref().unwrap(), "").unwrap();
    w.hub.drop_streams();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while w.hub.peer_fingerprints().len() < 2 {
        assert!(std::time::Instant::now() < deadline, "no reconnect");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(w.hub.peer_fingerprints()[1].1, first);
    let log = captured.text();
    assert!(log.contains("keeping the last good pair"), "{log}");
    conn.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn without_a_good_pair_to_fall_back_on_the_attempt_fails_and_nothing_is_offered() {
    let w = world(true).await;
    let (tls, _) = w.issue("svc", &ClientSpec::service("svc"));
    std::fs::write(tls.client_cert_file.as_ref().unwrap(), "").unwrap();
    let conn = connection(settings(w.hub.address(), tls), "svc");
    conn.start();
    assert!(!conn.await_registered(Duration::from_millis(600)).await);
    assert_eq!(w.hub.streams(), 0);
    assert_eq!(conn.last_rejection(), None);
    conn.shutdown().await;
    w.hub.stop();
}
