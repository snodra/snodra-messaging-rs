//! TLS to the hub (roadmap P5a, ADR 0018) against the in-process fake hub serving TLS with a
//! certificate generated at test time: the configured CA is trusted (and re-read on reconnect),
//! another CA is not, `server_name` is what the certificate is checked against, every address form
//! (a static list, `dns:///`, `https://`) uses TLS, and a plain-text client against a TLS hub, or
//! the reverse, fails with a log line that says what to check, not with a hang.
mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use kraken_messaging::HubTls;
use kraken_messaging::hub::{HubConnection, HubPool, HubSettings, Inbound};
use support::fake_hub::FakeHub;
use support::logs::Captured;
use support::pki::{Pki, TempDir};
use tokio::sync::mpsc;

fn settings(address: String, tls: Option<HubTls>) -> HubSettings {
    HubSettings {
        address,
        heartbeat_interval: Duration::from_secs(1),
        backoff_min: Duration::from_millis(20),
        backoff_max: Duration::from_millis(200),
        handshake_timeout: Duration::from_secs(3),
        tls,
        ..HubSettings::default()
    }
}

fn connection(settings: HubSettings) -> Arc<HubConnection> {
    let (tx, mut rx) = mpsc::channel::<Inbound>(16);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    HubConnection::new(settings, "svc".into(), uuid::Uuid::new_v4().to_string(), tx)
}

fn ca(dir: &TempDir, pki: &Pki) -> HubTls {
    HubTls {
        ca_file: Some(dir.write("ca.pem", &pki.ca_pem)),
        server_name: None,
    }
}

async fn tls_hub(pki: &Pki) -> FakeHub {
    let (cert, key) = pki.server(&["kraken-hub"]);
    FakeHub::start_tls(&cert, &key).await
}

/// `static://127.0.0.1:port` as the TLS hub's address.
fn address(hub: &FakeHub) -> String {
    hub.address()
}

#[tokio::test]
async fn registers_over_tls_trusting_the_configured_ca() {
    support::init_tracing();
    let (dir, pki) = (TempDir::new(), Pki::new("test CA"));
    let hub = tls_hub(&pki).await;
    let conn = connection(settings(address(&hub), Some(ca(&dir, &pki))));
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert_eq!(hub.registers().await.len(), 1);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_certificate_from_another_ca_is_refused_and_the_log_says_why() {
    let (dir, pki) = (TempDir::new(), Pki::new("test CA"));
    let other = Pki::new("other CA");
    let hub = tls_hub(&pki).await;
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(address(&hub), Some(ca(&dir, &other))));
    conn.start();
    assert!(!conn.await_registered(Duration::from_millis(800)).await);
    assert!(hub.registers().await.is_empty());
    let log = captured.text();
    assert!(log.contains("TLS to the hub failed"), "{log}");
    assert!(
        log.to_lowercase().contains("certificate") || log.contains("UnknownIssuer"),
        "{log}"
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn the_ca_file_is_reread_on_reconnect() {
    let (dir, pki) = (TempDir::new(), Pki::new("test CA"));
    let other = Pki::new("other CA");
    let hub = tls_hub(&pki).await;
    let tls = ca(&dir, &other);
    let path = tls.ca_file.clone().unwrap();
    let conn = connection(settings(address(&hub), Some(tls)));
    conn.start();
    assert!(!conn.await_registered(Duration::from_millis(500)).await);
    std::fs::write(&path, &pki.ca_pem).unwrap();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn server_name_is_what_the_certificate_is_checked_against() {
    let (dir, pki) = (TempDir::new(), Pki::new("test CA"));
    let hub = tls_hub(&pki).await; // the certificate names localhost, 127.0.0.1 and kraken-hub
    let wrong = connection(settings(
        address(&hub),
        Some(HubTls {
            server_name: Some("not-the-hub".into()),
            ..ca(&dir, &pki)
        }),
    ));
    wrong.start();
    assert!(!wrong.await_registered(Duration::from_millis(600)).await);
    wrong.shutdown().await;

    let right = connection(settings(
        address(&hub),
        Some(HubTls {
            server_name: Some("kraken-hub".into()),
            ..ca(&dir, &pki)
        }),
    ));
    right.start();
    assert!(right.await_registered(Duration::from_secs(5)).await);
    right.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_dns_target_and_a_static_list_both_use_tls() {
    let (dir, pki) = (TempDir::new(), Pki::new("test CA"));
    let (a, b) = (tls_hub(&pki).await, tls_hub(&pki).await);

    let (tx, _rx) = mpsc::channel::<Inbound>(16);
    let list = format!("{},{}", a.address(), b.address());
    let pool = HubPool::new(
        settings(list, Some(ca(&dir, &pki))),
        "svc".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    pool.start();
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    assert_eq!(
        (a.registers().await.len(), b.registers().await.len()),
        (1, 1)
    );
    pool.shutdown().await;

    let (tx, _rx) = mpsc::channel::<Inbound>(16);
    let dns = format!("dns:///localhost:{}", a.addr.port());
    let pool = HubPool::new(
        settings(dns, Some(ca(&dir, &pki))),
        "svc".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    pool.start();
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    assert_eq!(a.registers().await.len(), 2);
    pool.shutdown().await;
    a.stop();
    b.stop();
}

#[tokio::test]
async fn plain_text_against_a_tls_hub_fails_fast_and_says_to_enable_tls() {
    let pki = Pki::new("test CA");
    let hub = tls_hub(&pki).await;
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(address(&hub), None));
    let started = Instant::now();
    conn.start();
    // Not a hang: the first failure is logged well inside the handshake timeout.
    let deadline = Instant::now() + Duration::from_secs(2);
    while !captured.text().contains("hub stream closed") {
        assert!(
            Instant::now() < deadline,
            "no failure within 2 s: {}",
            captured.text()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(!conn.is_connected() || conn.handshake().is_none());
    assert!(hub.registers().await.is_empty());
    let log = captured.text();
    assert!(log.contains("KRAKEN_HUB_TLS=true"), "{log}");
    conn.shutdown().await;
    hub.stop();
}

/// A port nothing listens on.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn the_tls_hint_is_logged_at_warn_once_whatever_the_attempt_number() {
    let pki = Pki::new("test CA");
    let (cert, key) = pki.server(&["kraken-hub"]);
    let port = free_port();
    let captured = Captured::default();
    let _logs = captured.install();
    // Attempt 1 is a refused connect (nothing listens yet, so no hint applies) ...
    let conn = connection(settings(format!("static://127.0.0.1:{port}"), None));
    conn.start();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !captured.text().contains("hub stream closed") {
        assert!(Instant::now() < deadline, "no first failure");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // ... then a TLS-only hub comes up on that port: the first failure it causes is a later attempt.
    let hub = FakeHub::start_tls_on(port, &cert, &key).await;
    let hint = |l: &String| l.contains("KRAKEN_HUB_TLS=true");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !captured
        .lines()
        .iter()
        .any(|l| hint(l) && l.contains(" WARN "))
    {
        assert!(
            Instant::now() < deadline,
            "no hint at warn: {}",
            captured.text()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Further attempts keep failing the same way: the repeats are debug, not warn.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let lines = captured.lines();
    let warns = lines
        .iter()
        .filter(|l| hint(l) && l.contains(" WARN "))
        .count();
    let debugs = lines
        .iter()
        .filter(|l| hint(l) && l.contains(" DEBUG "))
        .count();
    assert_eq!(warns, 1, "{lines:#?}");
    assert!(debugs >= 1, "{lines:#?}");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn tls_against_a_plain_text_hub_fails_and_says_to_check_tls() {
    let (dir, pki) = (TempDir::new(), Pki::new("test CA"));
    let hub = FakeHub::start().await;
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(hub.address(), Some(ca(&dir, &pki))));
    conn.start();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !captured.text().contains("hub stream closed") {
        assert!(
            Instant::now() < deadline,
            "no failure within 3 s: {}",
            captured.text()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        captured.text().contains("does it serve TLS"),
        "{}",
        captured.text()
    );
    assert!(hub.registers().await.is_empty());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn an_https_address_turns_tls_on_without_hub_tls() {
    let pki = Pki::new("test CA");
    let hub = tls_hub(&pki).await;
    let captured = Captured::default();
    let _logs = captured.install();
    // System roots do not hold the test CA: the handshake is attempted and refused.
    let conn = connection(settings(
        format!("https://127.0.0.1:{}", hub.addr.port()),
        None,
    ));
    conn.start();
    // Loading the platform roots can take a moment the first time.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !captured.text().contains("TLS to the hub failed") {
        assert!(Instant::now() < deadline, "{}", captured.text());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!conn.is_connected());
    conn.shutdown().await;
    hub.stop();
}
