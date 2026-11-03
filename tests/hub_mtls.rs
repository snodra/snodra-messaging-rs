//! Mutual TLS to the hub (roadmap P5c, hub ADR 0020) against the in-process fake hub, which
//! verifies client certificates against a CA generated at test time: the connection presents its
//! certificate and sends no credentials, a client without one fails at the transport, a pair that
//! names another service or has expired is refused before a stream is opened, the files are read
//! again on every connect, and the auth status shows the certificate.
mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kraken_messaging::hub::{
    AuthKind, Dispatcher, HandlerRegistry, HubConnection, HubPool, HubSettings, Inbound, ReplyTo,
    SendOptions, Sender, SenderSettings, SubscriptionOptions,
};
use kraken_messaging::identity::IdentityResolver;
use kraken_messaging::proto::{self, ServerMessage, reply, server_message};
use kraken_messaging::{Command, HubTls, SecretString, SubscriptionQuery, message_type};
use serde::{Deserialize, Serialize};
use support::fake_hub::{Event, FakeHub, default_ack};
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
        // No delay before a roll, unless a test is about it.
        certificate_roll_delay_max: Duration::ZERO,
        certificate_check_interval: Duration::from_millis(200),
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
async fn the_hubs_certificate_expired_close_is_a_normal_reconnect_not_a_rejection() {
    let w = world(true).await;
    let (tls, _) = w.issue("svc", &ClientSpec::service("svc"));
    let captured = Captured::default();
    let _logs = captured.install();
    // The default backoff_max (5 s): a rejection would wait that long, a normal reconnect starts
    // from backoff_min.
    let conn = connection(
        HubSettings {
            backoff_max: HubSettings::default().backoff_max,
            ..settings(w.hub.address(), tls.clone())
        },
        "svc",
    );
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    w.hub
        .close_streams_with(tonic::Code::Unauthenticated, "certificate expired");
    let closed = Instant::now();
    let deadline = closed + Duration::from_secs(8);
    while w.hub.peer_fingerprints().len() < 2 {
        assert!(Instant::now() < deadline, "no reconnect");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        closed.elapsed() < Duration::from_secs(2),
        "reconnected after {:?}: waited like for a rejection",
        closed.elapsed()
    );
    assert_eq!(w.hub.peer_fingerprints()[1].1, second);
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert_eq!(conn.last_rejection(), None);
    let log = captured.text();
    assert!(!log.contains("hub rejected the registration"), "{log}");
    assert!(!log.contains(" ERROR "), "{log}");
    let info: Vec<_> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("INFO") && l.contains("certificate expired"))
        .collect();
    assert_eq!(info.len(), 1, "{log}");
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
async fn at_connect_every_problem_with_the_files_falls_back_to_the_last_good_pair() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(w.hub.address(), tls.clone()), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let now = time::OffsetDateTime::now_utc();
    let problems = [
        // Another service's pair (a wrong one copied over).
        ClientSpec::service("orders"),
        // Expired.
        ClientSpec {
            not_before: now - time::Duration::hours(2),
            not_after: now - time::Duration::seconds(30),
            ..ClientSpec::service("svc")
        },
        // Not valid yet.
        ClientSpec::service("svc").starting_in(Duration::from_secs(3600)),
    ];
    let mut streams = 1;
    for (n, problem) in problems.iter().enumerate() {
        w.renew(&tls, problem);
        // The same problem on two reconnects: one warning.
        for _ in 0..2 {
            w.hub.drop_streams();
            streams += 1;
            let deadline = Instant::now() + Duration::from_secs(5);
            while w.hub.peer_fingerprints().len() < streams {
                assert!(Instant::now() < deadline, "problem {n}: no reconnect");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(conn.await_registered(Duration::from_secs(5)).await);
            assert_eq!(
                w.hub.peer_fingerprints().last().unwrap().1,
                first,
                "problem {n}: not the last good pair"
            );
        }
    }
    assert_eq!(conn.last_rejection(), None);
    let warnings: Vec<String> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("WARN") && l.contains("keeping the last good pair"))
        .collect();
    assert_eq!(warnings.len(), 3, "one per problem: {warnings:#?}");
    assert!(warnings[0].contains("orders"), "{}", warnings[0]);
    assert!(warnings[1].contains("expired"), "{}", warnings[1]);
    assert!(warnings[2].contains("not valid before"), "{}", warnings[2]);
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

// Rotation: a renewed pair rolls the pool's connections over, one hub at a time (ADR 0020, the
// client slice of P5c).

#[derive(Serialize, Deserialize)]
struct Ping {}
message_type!(Ping, "t.Ping");
impl Command for Ping {
    type Reply = ();
}

fn ok_reply(req: &proto::Request) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Reply(proto::Reply {
            request_id: req.id.clone(),
            result: Some(reply::Result::Payload(b"{}".to_vec())),
        })),
    }
}

struct Client {
    pool: Arc<HubPool>,
    sender: Arc<Sender>,
    /// Requests pushed by the hubs, held until the test finishes them.
    held: Arc<Mutex<Vec<ReplyTo>>>,
}

impl Client {
    async fn shutdown(self) {
        self.pool.shutdown().await;
    }

    /// Waits (bounded) until every hub is registered and presents the certificate `fingerprint`.
    async fn presenting(&self, fingerprint: &str) {
        self.presenting_within(fingerprint, Duration::from_secs(10))
            .await;
    }

    async fn presenting_within(&self, fingerprint: &str, limit: Duration) {
        let deadline = Instant::now() + limit;
        loop {
            let now = self.pool.auth().certificate.map(|c| c.fingerprint);
            if now.as_deref() == Some(fingerprint)
                && self
                    .pool
                    .await_all_registered(Duration::from_millis(50))
                    .await
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "still presenting {now:?}, not {fingerprint}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

async fn client(settings: HubSettings) -> Client {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let pool = HubPool::new(settings, "svc".into(), uuid::Uuid::new_v4().to_string(), tx)
        .expect("valid hub address");
    pool.set_types(vec!["t.Handled".into()], vec![]);
    let sender = Sender::with_pool(Arc::clone(&pool), SenderSettings::default());
    let held = Arc::new(Mutex::new(Vec::new()));
    let (forward, keep) = (Arc::clone(&sender), Arc::clone(&held));
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { reply_to, .. } => keep.lock().unwrap().push(reply_to),
                other => forward.on_inbound(other),
            }
        }
    });
    pool.start();
    Client { pool, sender, held }
}

/// A fake mTLS hub that answers requests and names itself `id`.
async fn hub_named(w: &World, id: &str) -> FakeHub {
    let (cert, key) = w.server_ca.server(&["kraken-hub"]);
    let hub = FakeHub::start_mtls(&cert, &key, &w.client_ca.ca_pem, true).await;
    hub.set_ack(Some(proto::RegisterAck {
        hub_id: id.to_string(),
        ..default_ack()
    }));
    hub.set_responder(|req| Some(ok_reply(&req))).await;
    hub
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check() {
        assert!(Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_renewed_pair_rolls_the_connection_over_deregistering_first() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(w.hub.address(), tls.clone())).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    c.presenting(&second).await;
    let events: Vec<Event> = w.hub.events_timed().into_iter().map(|(e, _)| e).collect();
    assert_eq!(
        events,
        [
            Event::Register {
                stream: 1,
                types: 1
            },
            // The hub is told the instance is stopping, then the stream is closed, then the new
            // stream registers with the types again.
            Event::Register {
                stream: 1,
                types: 0
            },
            Event::Closed { stream: 1 },
            Event::Register {
                stream: 2,
                types: 1
            },
        ]
    );
    let fingerprints: Vec<String> = w
        .hub
        .peer_fingerprints()
        .into_iter()
        .map(|(_, f)| f)
        .collect();
    assert_eq!(fingerprints, [first, second.clone()]);
    let status = c.pool.auth();
    assert_eq!(status.kind, AuthKind::Certificate);
    let cert = status.certificate.unwrap();
    assert_eq!((cert.fingerprint, cert.rotations), (second, 1));
    // It stays put while nothing changes.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(w.hub.peer_fingerprints().len(), 2);
    c.shutdown().await;
    w.hub.stop();
}

fn push_request(id: &str) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Request(proto::Request {
            id: id.into(),
            kind: proto::RequestKind::Command as i32,
            r#type: "t.Handled".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        })),
    }
}

fn when(hub: &FakeHub, wanted: Event) -> Instant {
    hub.events_timed()
        .into_iter()
        .find(|(e, _)| *e == wanted)
        .unwrap_or_else(|| panic!("{wanted:?} not in {:?}", hub.events_timed()))
        .1
}

#[tokio::test]
async fn with_several_hubs_each_is_deregistered_closed_and_back_before_the_next() {
    let w = world(true).await;
    let (a, b) = (hub_named(&w, "hub-a").await, hub_named(&w, "hub-b").await);
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(
        format!("{},{}", a.address(), b.address()),
        tls.clone(),
    ))
    .await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    c.presenting(&second).await;
    for hub in [&a, &b] {
        let fingerprints: Vec<String> = hub
            .peer_fingerprints()
            .into_iter()
            .map(|(_, f)| f)
            .collect();
        assert_eq!(fingerprints, [first.clone(), second.clone()]);
    }
    let a_deregistered = when(
        &a,
        Event::Register {
            stream: 1,
            types: 0,
        },
    );
    let a_back = when(
        &a,
        Event::Register {
            stream: 2,
            types: 1,
        },
    );
    let b_deregistered = when(
        &b,
        Event::Register {
            stream: 1,
            types: 0,
        },
    );
    let b_back = when(
        &b,
        Event::Register {
            stream: 2,
            types: 1,
        },
    );
    assert!(
        a_deregistered < a_back && a_back < b_deregistered && b_deregistered < b_back,
        "not one hub at a time: a {a_deregistered:?} .. {a_back:?}, b {b_deregistered:?} .. {b_back:?}"
    );
    assert!(when(&a, Event::Closed { stream: 1 }) > a_deregistered);
    c.shutdown().await;
    a.stop();
    b.stop();
}

#[tokio::test]
async fn a_trickle_of_requests_sees_no_failure_while_the_hubs_roll_over() {
    let w = world(true).await;
    let (a, b) = (hub_named(&w, "hub-a").await, hub_named(&w, "hub-b").await);
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(
        format!("{},{}", a.address(), b.address()),
        tls.clone(),
    ))
    .await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let sender = Arc::clone(&c.sender);
    let failures = Arc::new(Mutex::new(Vec::<String>::new()));
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let last_ok = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let trickle = {
        let (failures, sent, running, last_ok) = (
            Arc::clone(&failures),
            Arc::clone(&sent),
            Arc::clone(&running),
            Arc::clone(&last_ok),
        );
        tokio::spawn(async move {
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                // A reply from the hub, decoded: the answer itself, not only no error.
                let answer = sender.send_command(&Ping {}, SendOptions::default()).await;
                last_ok.store(answer.is_ok(), std::sync::atomic::Ordering::SeqCst);
                if let Err(e) = answer {
                    failures.lock().unwrap().push(e.to_string());
                }
                sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    c.presenting(&second).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    running.store(false, std::sync::atomic::Ordering::SeqCst);
    trickle.await.unwrap();
    assert!(sent.load(std::sync::atomic::Ordering::SeqCst) > 100);
    assert_eq!(*failures.lock().unwrap(), Vec::<String>::new());
    assert!(
        last_ok.load(std::sync::atomic::Ordering::SeqCst),
        "the last answer is a reply"
    );
    assert_eq!(c.pool.auth().certificate.unwrap().rotations, 1);
    c.shutdown().await;
    a.stop();
    b.stop();
}

#[tokio::test]
async fn the_roll_waits_for_a_running_handler_before_closing_the_stream() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(w.hub.address(), tls.clone())).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    w.hub.send_to_clients(push_request("in-1")).await;
    eventually("the handler runs", || c.held.lock().unwrap().len() == 1).await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    eventually("the hub is told the instance stops", || {
        w.hub.events().contains(&Event::Register {
            stream: 1,
            types: 0,
        })
    })
    .await;
    // The handler still runs: the stream stays open, nothing new reaches it.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        !w.hub.events().contains(&Event::Closed { stream: 1 }),
        "{:?}",
        w.hub.events()
    );
    assert_eq!(w.hub.peer_fingerprints().len(), 1);
    let reply_to = c.held.lock().unwrap().pop().unwrap();
    reply_to.grant().await;
    c.presenting(&second).await;
    assert!(w.hub.events().contains(&Event::Closed { stream: 1 }));
    c.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn the_roll_stops_waiting_for_a_handler_after_ten_seconds() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(w.hub.address(), tls.clone())).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    w.hub.send_to_clients(push_request("in-1")).await;
    eventually("the handler runs", || c.held.lock().unwrap().len() == 1).await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    let deregistered = {
        eventually("the hub is told the instance stops", || {
            w.hub.events().contains(&Event::Register {
                stream: 1,
                types: 0,
            })
        })
        .await;
        Instant::now()
    };
    c.presenting_within(&second, Duration::from_secs(20)).await;
    let waited = deregistered.elapsed();
    assert!(
        waited >= Duration::from_secs(9) && waited < Duration::from_secs(14),
        "waited {waited:?} for a handler that never finishes"
    );
    c.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn an_invalid_new_pair_keeps_the_connection_and_is_logged_once_per_problem() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(w.hub.address(), tls.clone())).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let captured = Captured::default();
    let _logs = captured.install();
    // A renewal that names another service, then the key of another certificate, then garbage.
    w.renew(&tls, &ClientSpec::service("orders"));
    tokio::time::sleep(Duration::from_millis(900)).await;
    let (_, other_key) = w.client_ca.client_for("svc");
    std::fs::write(tls.client_key_file.as_ref().unwrap(), other_key).unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    std::fs::write(tls.client_cert_file.as_ref().unwrap(), "garbage").unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    // The current connection was never touched.
    assert_eq!(w.hub.events().len(), 1, "{:?}", w.hub.events());
    assert_eq!(c.pool.auth().certificate.unwrap().fingerprint, first);
    let errors: Vec<String> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("ERROR"))
        .collect();
    assert_eq!(errors.len(), 3, "once per distinct problem: {errors:#?}");
    assert!(errors[0].contains("orders"), "{}", errors[0]);
    assert!(errors[1].contains("does not match"), "{}", errors[1]);
    assert!(errors[2].contains("no PEM certificate"), "{}", errors[2]);
    // Fixed: the next tick rolls it over.
    let fixed = w.renew(&tls, &ClientSpec::service("svc"));
    c.presenting(&fixed).await;
    c.shutdown().await;
    w.hub.stop();
}

fn warnings(captured: &Captured) -> Vec<String> {
    captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("WARN") && l.contains("expires"))
        .collect()
}

#[tokio::test]
async fn a_certificate_near_its_end_is_warned_about_once_when_no_renewal_appears() {
    let w = world(true).await;
    // Valid for an hour and 3 s: well inside the last tenth of its life.
    let (tls, first) = w.issue(
        "svc",
        &ClientSpec::service("svc").living(Duration::from_secs(3)),
    );
    let captured = Captured::default();
    let _logs = captured.install();
    let c = client(settings(w.hub.address(), tls.clone())).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    tokio::time::sleep(Duration::from_millis(1000)).await; // five ticks
    let lines = warnings(&captured);
    assert_eq!(lines.len(), 1, "{}", captured.text());
    assert!(lines[0].contains(&first), "{}", lines[0]);
    // A renewal rolls the connection over, and the new certificate is not near its end.
    w.renew(&tls, &ClientSpec::service("svc"));
    let second = fingerprint_of_pem(
        &std::fs::read_to_string(tls.client_cert_file.as_ref().unwrap()).unwrap(),
    );
    c.presenting(&second).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(warnings(&captured).len(), 1, "{}", captured.text());
    // No key material in anything logged.
    let key = std::fs::read_to_string(tls.client_key_file.as_ref().unwrap()).unwrap();
    let body: String = key.lines().filter(|l| !l.starts_with("-----")).collect();
    assert!(!captured.text().contains(&body[10..30]));
    c.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn no_warning_while_a_renewed_file_is_waiting_to_be_picked_up() {
    let w = world(true).await;
    let (tls, _) = w.issue(
        "svc",
        &ClientSpec::service("svc").living(Duration::from_secs(3)),
    );
    let captured = Captured::default();
    let _logs = captured.install();
    // A slow tick: the renewal is on disk before the first one looks.
    let c = client(HubSettings {
        certificate_check_interval: Duration::from_secs(1),
        ..settings(w.hub.address(), tls.clone())
    })
    .await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    w.renew(&tls, &ClientSpec::service("svc"));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(warnings(&captured).is_empty(), "{}", captured.text());
    c.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn shutdown_stops_watching_the_files() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(w.hub.address(), tls.clone())).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let pool = Arc::clone(&c.pool);
    c.shutdown().await;
    let streams = w.hub.streams();
    w.renew(&tls, &ClientSpec::service("svc"));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        w.hub.streams(),
        streams,
        "a stream was opened after the shutdown"
    );
    assert_eq!(pool.background_tasks(), 0);
    w.hub.stop();
}

#[tokio::test]
async fn with_one_hub_the_roll_has_a_short_gap_that_a_retry_policy_covers() {
    let w = world(true).await;
    w.hub.set_responder(|req| Some(ok_reply(&req))).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let pool = HubPool::new(
        settings(w.hub.address(), tls.clone()),
        "svc".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    let sender = Sender::with_pool(
        Arc::clone(&pool),
        SenderSettings {
            retry: Some(kraken_messaging::RetryPolicy {
                max_attempts: 5,
                initial_backoff: Duration::from_millis(30),
                ..kraken_messaging::RetryPolicy::default()
            }),
            ..SenderSettings::default()
        },
    );
    let forward = Arc::clone(&sender);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            forward.on_inbound(msg);
        }
    });
    pool.start();
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let failures = Arc::new(Mutex::new(Vec::<String>::new()));
    let last_ok = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let trickle = {
        let (sender, running, failures, last_ok) = (
            Arc::clone(&sender),
            Arc::clone(&running),
            Arc::clone(&failures),
            Arc::clone(&last_ok),
        );
        tokio::spawn(async move {
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                let answer = sender.send_command(&Ping {}, SendOptions::default()).await;
                last_ok.store(answer.is_ok(), std::sync::atomic::Ordering::SeqCst);
                if let Err(e) = answer {
                    failures.lock().unwrap().push(e.to_string());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    let deadline = Instant::now() + Duration::from_secs(10);
    while pool.auth().certificate.map(|c| c.fingerprint) != Some(second.clone()) {
        assert!(Instant::now() < deadline, "not rolled over from {first}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    running.store(false, std::sync::atomic::Ordering::SeqCst);
    trickle.await.unwrap();
    assert_eq!(*failures.lock().unwrap(), Vec::<String>::new());
    assert!(
        last_ok.load(std::sync::atomic::Ordering::SeqCst),
        "the last answer is a reply"
    );
    pool.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn a_renewed_pair_is_not_rolled_to_before_its_not_before() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(w.hub.address(), tls.clone())).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    // A renewal whose validity starts in 3 s (the issuer's clock runs ahead, or a pre-issued pair).
    let second = w.renew(
        &tls,
        &ClientSpec::service("svc").starting_in(Duration::from_secs(3)),
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // Still the first pair on the first stream: nothing closed, nothing re-registered.
    assert_eq!(w.hub.events().len(), 1, "{:?}", w.hub.events());
    assert_eq!(c.pool.auth().certificate.unwrap().fingerprint, first);
    // Valid now: it is rolled to.
    c.presenting_within(&second, Duration::from_secs(10)).await;
    assert_eq!(c.pool.auth().certificate.unwrap().rotations, 1);
    c.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn a_reply_that_arrives_while_the_stream_is_closing_for_the_roll_reaches_its_caller() {
    let w = world(true).await;
    // The hub keeps the request unanswered; once the instance half-closes the stream (the drain
    // ran out) it sends the late reply, and then takes its time to end the stream.
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = Arc::clone(&seen);
    w.hub
        .set_responder(move |req| {
            recorded.lock().unwrap().push(req.id);
            None
        })
        .await;
    let late = Arc::clone(&seen);
    w.hub.set_half_close(Duration::from_millis(500), move || {
        late.lock()
            .unwrap()
            .iter()
            .map(|id| {
                ok_reply(&proto::Request {
                    id: id.clone(),
                    ..Default::default()
                })
            })
            .collect()
    });
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(w.hub.address(), tls.clone())).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let sender = Arc::clone(&c.sender);
    let call = tokio::spawn(async move {
        sender
            .send_command(
                &Ping {},
                SendOptions {
                    timeout: Some(Duration::from_secs(30)),
                    ..SendOptions::default()
                },
            )
            .await
    });
    eventually("the hub has the request", || {
        !seen.lock().unwrap().is_empty()
    })
    .await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    // The roll waits 10 s for the request, then closes the stream; the late reply arrives then.
    c.presenting_within(&second, Duration::from_secs(20)).await;
    let outcome = call.await.unwrap();
    assert!(
        outcome.is_ok(),
        "the late reply was lost with the close: {outcome:?}"
    );
    c.shutdown().await;
    w.hub.stop();
}

/// One hub, no retry policy; the hub keeps its side of a closed stream open for `linger`, so the
/// roll's close (and the reconnect after it) takes at least that long.
async fn single_hub_client(w: &World, tls: &HubTls, linger: Duration) -> Client {
    w.hub.set_responder(|req| Some(ok_reply(&req))).await;
    w.hub.set_half_close(linger, Vec::new);
    client(settings(w.hub.address(), tls.clone())).await
}

#[tokio::test]
async fn a_send_during_a_single_hub_roll_waits_for_the_new_stream_instead_of_failing() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = single_hub_client(&w, &tls, Duration::from_millis(600)).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    // The old stream is closing (the hub lingers 600 ms): nothing is routable, only because of the roll.
    eventually("the stream is closed for the roll", || {
        w.hub.events().contains(&Event::Register {
            stream: 1,
            types: 0,
        })
    })
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let started = Instant::now();
    // The default retry policy (none): the call itself waits for the new stream.
    c.sender
        .send_command(
            &Ping {},
            SendOptions {
                timeout: Some(Duration::from_secs(10)),
                ..SendOptions::default()
            },
        )
        .await
        .expect("a send during the roll waits for the new stream");
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "it did wait"
    );
    c.presenting(&second).await;
    c.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn a_send_stops_waiting_for_the_roll_at_its_own_deadline() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = single_hub_client(&w, &tls, Duration::from_millis(1500)).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    w.renew(&tls, &ClientSpec::service("svc"));
    eventually("the stream is closed for the roll", || {
        w.hub.events().contains(&Event::Register {
            stream: 1,
            types: 0,
        })
    })
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let started = Instant::now();
    let err = c
        .sender
        .send_command(
            &Ping {},
            SendOptions {
                timeout: Some(Duration::from_millis(300)),
                ..SendOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, kraken_messaging::MessagingError::HubUnavailable(_)),
        "{err:?}"
    );
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(250) && waited < Duration::from_millis(900),
        "waited {waited:?}"
    );
    c.shutdown().await;
    w.hub.stop();
}

/// A subscription query this instance consumes (another service owns it).
#[derive(Serialize, Deserialize)]
struct Prices {}
message_type!(Prices, "t.Prices");
#[derive(Serialize, Deserialize, Debug)]
struct Quote {}
impl SubscriptionQuery for Prices {
    type Initial = Quote;
    type Update = Quote;
}

/// The instance of service `owner` that handles `t.Prices`, with its own certificate.
async fn price_owner(w: &World) -> Arc<HubPool> {
    let (tls, _) = w.issue("owner", &ClientSpec::service("owner"));
    let mut registry = HandlerRegistry::new();
    registry.subscription_query::<Prices, _, _>(|_, _| async move { Ok(Quote {}) });
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let pool = HubPool::new(
        settings(w.hub.address(), tls),
        "owner".into(),
        "o1".into(),
        tx,
    )
    .unwrap();
    pool.set_registered_types(registry.registered_types());
    let dispatcher = Arc::new(Dispatcher::new(
        Arc::new(registry),
        Arc::new(IdentityResolver::new(None)),
        4,
    ));
    pool.start();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Inbound::Request { request, reply_to } => {
                    tokio::spawn(dispatcher.serve(request, reply_to));
                }
                Inbound::Flow(f) => dispatcher.flow(&f.request_id, f.credits),
                Inbound::Cancel(c) => dispatcher.cancel(&c.request_id, &c.reason),
                _ => {}
            }
        }
    });
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    pool
}

#[tokio::test]
async fn the_drain_does_not_wait_for_the_subscriptions_this_instance_consumes() {
    let w = world(true).await;
    w.hub.set_subscription(true);
    let owner = price_owner(&w).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let c = client(settings(w.hub.address(), tls.clone())).await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    // An open subscription: it ends only when its owner completes it, which is not going to happen.
    let subscription = c
        .sender
        .subscribe(&Prices {}, SubscriptionOptions::default())
        .await
        .expect("subscribed");
    assert_eq!(c.sender.in_flight(), 1);
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    let started = Instant::now();
    c.presenting_within(&second, Duration::from_secs(20)).await;
    let took = started.elapsed();
    assert!(
        took < Duration::from_secs(5),
        "the roll waited {took:?} for the subscription this instance itself consumes"
    );
    // The roll cut it, as always: the consumer sees the loss.
    let mut updates = subscription.updates;
    let last = tokio_stream::StreamExt::next(&mut updates).await;
    assert!(matches!(last, Some(Err(_))), "{last:?}");
    c.shutdown().await;
    owner.shutdown().await;
    w.hub.stop();
}

/// A pool on the hub(s) of `address` whose rollover delay is picked by `source` from its bound.
async fn client_delayed(
    settings: HubSettings,
    source: impl Fn(Duration) -> Duration + Send + Sync + 'static,
) -> Client {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let pool = HubPool::new(settings, "svc".into(), uuid::Uuid::new_v4().to_string(), tx)
        .expect("valid hub address");
    pool.set_types(vec!["t.Handled".into()], vec![]);
    pool.set_roll_delay_source(Arc::new(source));
    let sender = Sender::with_pool(Arc::clone(&pool), SenderSettings::default());
    let forward = Arc::clone(&sender);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            forward.on_inbound(msg);
        }
    });
    pool.start();
    Client {
        pool,
        sender,
        held: Arc::new(Mutex::new(Vec::new())),
    }
}

#[tokio::test]
async fn replicas_that_see_the_same_renewal_do_not_deregister_at_the_same_instant() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let bounded = HubSettings {
        certificate_roll_delay_max: Duration::from_secs(2),
        ..settings(w.hub.address(), tls.clone())
    };
    // Two replicas sharing the files; their delays are 0 and half the bound.
    let a = client_delayed(bounded.clone(), |_| Duration::ZERO).await;
    let b = client_delayed(bounded, |bound| bound / 2).await;
    for c in [&a, &b] {
        assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
        c.presenting(&first).await;
    }
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    for c in [&a, &b] {
        c.presenting(&second).await;
    }
    let deregistered: Vec<Instant> = w
        .hub
        .events_timed()
        .into_iter()
        .filter(|(e, _)| matches!(e, Event::Register { types: 0, .. }))
        .map(|(_, at)| at)
        .collect();
    assert_eq!(deregistered.len(), 2, "{:?}", w.hub.events());
    let apart = deregistered[1].duration_since(deregistered[0]);
    assert!(
        apart >= Duration::from_millis(800),
        "both deregistered within {apart:?}"
    );
    a.shutdown().await;
    b.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn the_delay_never_exceeds_its_bound_and_is_capped_by_a_tenth_of_the_remaining_validity() {
    let w = world(true).await;
    // 100 s left: a tenth is 10 s, below the 30 s setting.
    let (tls, first) = w.issue(
        "svc",
        &ClientSpec::service("svc").living(Duration::from_secs(100)),
    );
    let bounds = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let seen = Arc::clone(&bounds);
    let c = client_delayed(
        HubSettings {
            certificate_roll_delay_max: Duration::from_secs(30),
            ..settings(w.hub.address(), tls.clone())
        },
        move |bound| {
            seen.lock().unwrap().push(bound);
            Duration::ZERO
        },
    )
    .await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    c.presenting(&second).await;
    let bounds = bounds.lock().unwrap().clone();
    assert_eq!(bounds.len(), 1, "one delay per renewed pair: {bounds:?}");
    assert!(
        bounds[0] <= Duration::from_secs(10) && bounds[0] > Duration::from_millis(9_000),
        "{:?}",
        bounds[0]
    );
    c.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn the_pair_is_checked_again_after_the_delay() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let draws = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&draws);
    let c = client_delayed(
        HubSettings {
            certificate_roll_delay_max: Duration::from_secs(1),
            ..settings(w.hub.address(), tls.clone())
        },
        move |bound| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            bound
        },
    )
    .await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let _superseded = w.renew(&tls, &ClientSpec::service("svc"));
    // Within the delay the files change again: the roll goes to what is there after it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let third = w.renew(&tls, &ClientSpec::service("svc"));
    c.presenting(&third).await;
    let presented: Vec<String> = w
        .hub
        .peer_fingerprints()
        .into_iter()
        .map(|(_, f)| f)
        .collect();
    assert_eq!(
        presented,
        [first, third],
        "the superseded pair was presented"
    );
    assert_eq!(
        draws.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "one delay for the renewal, none more for the pair that replaced it meanwhile"
    );
    c.shutdown().await;
    w.hub.stop();
}

#[test]
fn the_files_are_looked_at_every_three_seconds_by_default_like_kotlin() {
    assert_eq!(
        HubSettings::default().certificate_check_interval,
        Duration::from_secs(3)
    );
}

#[tokio::test]
async fn the_files_are_checked_on_their_own_interval_not_the_heartbeat_period() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    // A heartbeat period of a minute: a check tied to it would not see the renewal in time.
    let c = client(HubSettings {
        heartbeat_interval: Duration::from_secs(60),
        certificate_check_interval: Duration::from_millis(200),
        ..settings(w.hub.address(), tls.clone())
    })
    .await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    let second = w.renew(&tls, &ClientSpec::service("svc"));
    c.presenting_within(&second, Duration::from_secs(3)).await;
    c.shutdown().await;
    w.hub.stop();
}

#[tokio::test]
async fn a_failure_in_the_rotation_tick_is_logged_once_and_the_watch_goes_on() {
    let w = world(true).await;
    let (tls, first) = w.issue("svc", &ClientSpec::service("svc"));
    let captured = Captured::default();
    let _logs = captured.install();
    // The delay source panics on its first draws (an unexpected failure inside the tick).
    let draws = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&draws);
    let c = client_delayed(
        HubSettings {
            certificate_roll_delay_max: Duration::from_secs(1),
            ..settings(w.hub.address(), tls.clone())
        },
        move |_| {
            if counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 1 {
                panic!("boom in the tick");
            }
            Duration::ZERO
        },
    )
    .await;
    assert!(c.pool.await_all_registered(Duration::from_secs(5)).await);
    c.presenting(&first).await;
    w.renew(&tls, &ClientSpec::service("svc"));
    tokio::time::sleep(Duration::from_millis(900)).await;
    let errors: Vec<String> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains(" ERROR ") && l.contains("boom in the tick"))
        .collect();
    assert_eq!(
        errors.len(),
        1,
        "once per distinct failure: {}",
        captured.text()
    );
    // The watch is alive: a later renewal is rolled to.
    let third = w.renew(&tls, &ClientSpec::service("svc"));
    c.presenting(&third).await;
    c.shutdown().await;
    w.hub.stop();
}
