//! Hub access control, client side (roadmap P5a, ADR 0018) against the in-process fake hub, which
//! answers like the real hub in `enforce` and `permissive` mode: the token goes in every
//! `Register`, a refusal closed with `UNAUTHENTICATED` or `PERMISSION_DENIED` is a registration
//! rejection retried at the backoff cap and logged once, the token file is re-read on reconnect,
//! and a request outside the ACL's `send` is `AccessDenied`, never retried.
mod support;

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_messaging::hub::{
    HubConnection, HubPool, HubSettings, Inbound, RegisteredTypes, ScatterOptions, SendOptions,
    Sender, SenderSettings, StreamOptions, SubscriptionOptions,
};
use kraken_messaging::proto::{self, ErrorCode};
use kraken_messaging::{
    Command, Config, MessagingError, Query, RetryPolicy, SecretString, StreamingQuery,
    SubscriptionQuery, message_type,
};
use serde::{Deserialize, Serialize};
use support::fake_hub::{Acl, AuthMode, FakeHub};
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize)]
struct Do {}
message_type!(Do, "orders.Do");
impl Command for Do {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Ask {}
message_type!(Ask, "orders.Ask");
impl Query for Ask {
    type Reply = ();
}

#[derive(Serialize, Deserialize)]
struct Rows {}
message_type!(Rows, "orders.Rows");
impl StreamingQuery for Rows {
    type Item = u32;
}

#[derive(Serialize, Deserialize)]
struct Watch {}
message_type!(Watch, "orders.Watch");
impl SubscriptionQuery for Watch {
    type Initial = ();
    type Update = u32;
}

/// A random token, as an operator would generate one (`openssl rand -hex 32`).
fn random_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

fn settings(address: String, token: Option<&str>) -> HubSettings {
    HubSettings {
        address,
        heartbeat_interval: Duration::from_secs(1),
        backoff_min: Duration::from_millis(20),
        backoff_max: Duration::from_millis(300),
        token: token.map(SecretString::new),
        ..HubSettings::default()
    }
}

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

/// What `tracing` printed while the guard lives (no ANSI); current-thread tests only.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        let sink = self.clone();
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_ansi(false)
                .with_writer(move || sink.clone())
                .finish(),
        )
    }

    fn lines(&self) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .map(str::to_string)
            .collect()
    }
}

fn temp_file(content: &str) -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!("kraken-auth-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("hub-token");
    std::fs::write(&path, content).unwrap();
    (dir, path)
}

#[tokio::test]
async fn the_token_is_sent_in_the_register_of_every_stream() {
    let token = random_token();
    let hub = FakeHub::start().await;
    hub.set_auth(
        AuthMode::Enforce,
        &[(&token, Acl::new("svc", &["orders.*"], &[]))],
    );
    let conn = connection(settings(hub.address(), Some(&token)), "svc");
    conn.set_types(vec!["orders.Do".into()], vec![]);
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);

    // A second stream after the first one is lost carries it too.
    hub.drop_streams();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while hub.registers().await.len() < 2 {
        assert!(tokio::time::Instant::now() < deadline, "no second stream");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for register in hub.registers().await {
        assert_eq!(
            register.credentials.and_then(|c| c.kind),
            Some(proto::credentials::Kind::Token(token.clone()))
        );
    }
    assert!(hub.rejections().is_empty());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn every_hub_of_a_pool_gets_the_token() {
    let token = random_token();
    let (a, b) = (FakeHub::start().await, FakeHub::start().await);
    for hub in [&a, &b] {
        hub.set_auth(
            AuthMode::Enforce,
            &[(&token, Acl::new("svc", &["*"], &["*"]))],
        );
    }
    let (tx, _rx) = mpsc::channel::<Inbound>(16);
    let address = format!("{},{}", a.address(), b.address());
    let pool = HubPool::new(
        settings(address, Some(&token)),
        "svc".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    pool.start();
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    for hub in [&a, &b] {
        let registers = hub.registers().await;
        assert_eq!(registers.len(), 1);
        assert_eq!(
            registers[0].credentials.clone().and_then(|c| c.kind),
            Some(proto::credentials::Kind::Token(token.clone()))
        );
    }
    pool.shutdown().await;
}

#[tokio::test]
async fn an_empty_token_is_no_token() {
    let hub = FakeHub::start().await;
    let conn = connection(settings(hub.address(), Some("")), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert!(hub.registers().await[0].credentials.is_none());
    conn.shutdown().await;
    hub.stop();
}

#[test]
fn debug_output_never_shows_the_token() {
    let token = random_token();
    let mut config = Config::new("svc", "static://hub:7000", "kafka:19092", "mongodb://m/db");
    config.hub_token = Some(SecretString::new(token.clone()));
    let settings = settings("static://hub:7000".into(), Some(&token));
    for printed in [
        format!("{config:?}"),
        format!("{config:#?}"),
        format!("{settings:?}"),
        format!("{:?}", config.hub_token),
    ] {
        assert!(!printed.contains(&token), "token leaked: {printed}");
        assert!(printed.contains("\"***\""), "{printed}");
    }
}

#[tokio::test]
async fn the_token_file_is_reread_on_reconnect_and_a_fixed_token_registers() {
    support::init_tracing();
    let good = random_token();
    let wrong = random_token();
    let hub = FakeHub::start().await;
    hub.set_auth(
        AuthMode::Enforce,
        &[(&good, Acl::new("svc", &["*"], &["*"]))],
    );
    let (dir, file) = temp_file(&format!("{wrong}\n"));
    let conn = connection(
        HubSettings {
            token_file: Some(file.clone()),
            ..settings(hub.address(), None)
        },
        "svc",
    );
    conn.start();
    // The first attempt carries the file's (wrong) token and is refused.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while hub.rejections().is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "no attempt");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(hub.rejections()[0].token.as_deref(), Some(wrong.as_str()));
    assert_eq!(hub.rejections()[0].code, tonic::Code::Unauthenticated);
    assert!(conn.last_rejection().is_some());

    // The secret is rotated; the next attempt reads it.
    std::fs::write(&file, format!("{good}\n")).unwrap();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert_eq!(
        hub.registers().await[0]
            .credentials
            .clone()
            .and_then(|c| c.kind),
        Some(proto::credentials::Kind::Token(good.clone())),
        "the trailing newline is trimmed"
    );
    assert_eq!(
        conn.last_rejection(),
        None,
        "a registration clears the rejection"
    );
    conn.shutdown().await;
    hub.stop();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn the_token_file_wins_over_the_configured_token() {
    let good = random_token();
    let hub = FakeHub::start().await;
    hub.set_auth(
        AuthMode::Enforce,
        &[(&good, Acl::new("svc", &["*"], &["*"]))],
    );
    let (dir, file) = temp_file(&good);
    let conn = connection(
        HubSettings {
            token_file: Some(file),
            ..settings(hub.address(), Some(&random_token()))
        },
        "svc",
    );
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert!(hub.rejections().is_empty());
    conn.shutdown().await;
    hub.stop();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn an_unreadable_token_file_keeps_the_last_token() {
    let good = random_token();
    let hub = FakeHub::start().await;
    hub.set_auth(
        AuthMode::Enforce,
        &[(&good, Acl::new("svc", &["*"], &["*"]))],
    );
    let (dir, file) = temp_file(&good);
    let conn = connection(
        HubSettings {
            token_file: Some(file.clone()),
            ..settings(hub.address(), None)
        },
        "svc",
    );
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    // The file disappears mid-swap while the stream drops: the reconnect still has a token.
    std::fs::remove_file(&file).unwrap();
    hub.drop_streams();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while hub.registers().await.len() < 2 {
        assert!(tokio::time::Instant::now() < deadline, "no second stream");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(hub.rejections().is_empty());
    conn.shutdown().await;
    hub.stop();
    std::fs::remove_dir_all(dir).unwrap();
}

/// A refusal, as a rejection: reported, retried at the backoff cap (80-100 % of 300 ms), logged at
/// `error` once per distinct message and then at `debug`, never with the token.
async fn rejected(
    token: Option<&str>,
    service: &str,
    types: &[&str],
    code: tonic::Code,
    text: &str,
) {
    let known = random_token();
    let hub = FakeHub::start().await;
    hub.set_auth(
        AuthMode::Enforce,
        &[(&known, Acl::new("billing", &["billing.*"], &[]))],
    );
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(hub.address(), token), service);
    conn.set_types(types.iter().map(|t| t.to_string()).collect(), vec![]);
    conn.start();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while hub.rejections().len() < 3 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "fewer than 3 attempts"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!conn.is_connected());
    assert!(
        conn.await_registered(Duration::from_millis(50))
            .await
            .eq(&false),
        "a rejected hub is not registered"
    );
    let rejection = conn.last_rejection().expect("a rejection is kept");
    assert!(rejection.contains(text), "{rejection}");
    let attempts = hub.rejections();
    assert!(attempts.iter().all(|r| r.code == code), "{attempts:?}");
    for pair in attempts.windows(2) {
        let gap = pair[1].at - pair[0].at;
        assert!(gap >= Duration::from_millis(230), "retried early: {gap:?}");
        assert!(gap <= Duration::from_millis(900), "retried late: {gap:?}");
    }
    assert!(hub.registers().await.is_empty());

    let lines: Vec<String> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("hub rejected the registration"))
        .collect();
    let errors = lines.iter().filter(|l| l.contains(" ERROR ")).count();
    let debugs = lines.iter().filter(|l| l.contains(" DEBUG ")).count();
    assert_eq!(errors, 1, "one error per distinct message: {lines:#?}");
    assert!(debugs >= 1, "the repeats are debug: {lines:#?}");
    for line in captured.lines() {
        assert!(!line.contains(&known), "token leaked: {line}");
        if let Some(t) = token {
            assert!(!line.contains(t), "token leaked: {line}");
        }
    }
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn an_unknown_token_is_an_unauthenticated_rejection() {
    rejected(
        Some(&random_token()),
        "billing",
        &[],
        tonic::Code::Unauthenticated,
        "unknown token",
    )
    .await;
}

#[tokio::test]
async fn no_token_is_an_unauthenticated_rejection() {
    rejected(
        None,
        "billing",
        &[],
        tonic::Code::Unauthenticated,
        "missing token",
    )
    .await;
}

#[tokio::test]
async fn a_token_for_another_service_is_a_permission_denied_rejection() {
    // The hub's table has the token below; the client registers as another service.
    let token = random_token();
    let hub = FakeHub::start().await;
    hub.set_auth(
        AuthMode::Enforce,
        &[(&token, Acl::new("billing", &["billing.*"], &[]))],
    );
    let conn = connection(settings(hub.address(), Some(&token)), "orders");
    conn.start();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while hub.rejections().len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "fewer than 2 attempts"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        conn.last_rejection()
            .is_some_and(|r| r.contains("not valid for service 'orders'"))
    );
    let gap = hub.rejections()[1].at - hub.rejections()[0].at;
    assert!(gap >= Duration::from_millis(230), "retried early: {gap:?}");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_handled_type_outside_the_acl_is_a_permission_denied_rejection() {
    let token = random_token();
    let hub = FakeHub::start().await;
    hub.set_auth(
        AuthMode::Enforce,
        &[(&token, Acl::new("billing", &["billing.*"], &[]))],
    );
    let captured = Captured::default();
    let _logs = captured.install();
    let conn = connection(settings(hub.address(), Some(&token)), "billing");
    conn.set_types(vec!["billing.Charge".into(), "orders.Do".into()], vec![]);
    conn.start();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while hub.rejections().len() < 3 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "fewer than 3 attempts"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let rejection = conn.last_rejection().unwrap();
    assert!(rejection.contains("orders.Do"), "{rejection}");
    assert!(
        hub.rejections()
            .iter()
            .all(|r| r.code == tonic::Code::PermissionDenied)
    );
    let errors = captured
        .lines()
        .iter()
        .filter(|l| l.contains(" ERROR ") && l.contains("hub rejected the registration"))
        .count();
    assert_eq!(errors, 1);
    // The ACL is fixed (the operator widened it): the next attempt registers and logs again if
    // it is ever refused for another reason.
    hub.set_auth(
        AuthMode::Enforce,
        &[(&token, Acl::new("billing", &["billing.*", "orders.*"], &[]))],
    );
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_pool_reports_a_rejected_hub_as_not_registered() {
    let hub = FakeHub::start().await;
    hub.set_auth(AuthMode::Enforce, &[]);
    let (tx, _rx) = mpsc::channel::<Inbound>(16);
    let pool = HubPool::new(
        settings(hub.address(), Some(&random_token())),
        "svc".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    pool.start();
    assert!(!pool.await_registered(Duration::from_millis(700)).await);
    assert!(!pool.await_all_registered(Duration::from_millis(100)).await);
    let conns = pool.connections();
    assert_eq!(conns.len(), 1);
    assert!(
        conns[0]
            .last_rejection()
            .is_some_and(|r| r.contains("unknown token"))
    );
    pool.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_permissive_hub_allows_a_service_without_a_token() {
    let hub = FakeHub::start().await;
    hub.set_auth(AuthMode::Permissive, &[]);
    let conn = connection(settings(hub.address(), None), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert!(hub.rejections().is_empty());
    assert_eq!(hub.violations().len(), 1, "{:?}", hub.violations());
    conn.shutdown().await;
    hub.stop();
}

// ---- Request-level PERMISSION_DENIED ----

async fn caller(
    hub: &FakeHub,
    token: &str,
    retry: Option<RetryPolicy>,
) -> (Arc<Sender>, Arc<HubConnection>) {
    let (tx, mut rx) = mpsc::channel::<Inbound>(64);
    let conn = HubConnection::new(
        settings(hub.address(), Some(token)),
        "caller".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    );
    conn.set_registered_types(RegisteredTypes::default());
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let sender = Sender::with_settings(
        Arc::clone(&conn),
        SenderSettings {
            default_timeout: Some(Duration::from_secs(2)),
            retry,
            ..SenderSettings::default()
        },
    );
    let forward = Arc::clone(&sender);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            forward.on_inbound(msg);
        }
    });
    (sender, conn)
}

fn retrying() -> Option<RetryPolicy> {
    Some(RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(10),
        multiplier: 2.0,
        max_backoff: Duration::from_millis(50),
    })
}

#[tokio::test]
async fn a_request_outside_the_acl_is_access_denied_in_every_mode_and_never_retried() {
    let token = random_token();
    let hub = FakeHub::start().await;
    // May send nothing under orders.*.
    hub.set_auth(
        AuthMode::Enforce,
        &[(&token, Acl::new("caller", &[], &["billing.*"]))],
    );
    let (sender, conn) = caller(&hub, &token, retrying()).await;
    let denied = |e: MessagingError| {
        assert!(
            matches!(e, MessagingError::AccessDenied(ref m) if m.contains("orders.")),
            "{e:?}"
        );
    };

    denied(
        sender
            .send_command(&Do {}, SendOptions::default())
            .await
            .unwrap_err(),
    );
    assert_eq!(hub.requests().await.len(), 1, "command retried");
    denied(
        sender
            .send_query(&Ask {}, SendOptions::default())
            .await
            .unwrap_err(),
    );
    assert_eq!(hub.requests().await.len(), 2, "query retried");
    denied(
        sender
            .scatter_gather(&Ask {}, ScatterOptions::default())
            .await
            .unwrap_err(),
    );
    assert_eq!(hub.requests().await.len(), 3, "scatter retried");
    denied(
        sender
            .stream(&Rows {}, StreamOptions::default())
            .await
            .err()
            .expect("a refused stream"),
    );
    assert_eq!(hub.requests().await.len(), 4, "stream retried");
    denied(
        sender
            .subscribe(&Watch {}, SubscriptionOptions::default())
            .await
            .err()
            .expect("a refused subscription"),
    );
    assert_eq!(hub.requests().await.len(), 5, "subscription retried");
    assert_eq!(sender.in_flight(), 0);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_permissive_hub_routes_a_request_outside_the_acl() {
    let token = random_token();
    let hub = FakeHub::start().await;
    hub.set_auth(
        AuthMode::Permissive,
        &[(&token, Acl::new("caller", &[], &["billing.*"]))],
    );
    hub.set_responder(|req| {
        Some(proto::ServerMessage {
            message: Some(proto::server_message::Message::Reply(proto::Reply {
                request_id: req.id,
                result: Some(proto::reply::Result::Payload(b"{}".to_vec())),
            })),
        })
    })
    .await;
    let (sender, conn) = caller(&hub, &token, None).await;
    sender
        .send_command(&Do {}, SendOptions::default())
        .await
        .expect("permissive allows it");
    assert!(
        hub.violations().iter().any(|v| v.contains("orders.Do")),
        "{:?}",
        hub.violations()
    );
    conn.shutdown().await;
    hub.stop();
}

#[test]
fn permission_denied_maps_to_access_denied_and_is_not_retryable() {
    let e = MessagingError::from_hub(
        "orders.Do",
        ErrorCode::PermissionDenied as i32,
        "type 'orders.Do' is not allowed to be sent",
        b"",
    );
    assert!(matches!(e, MessagingError::AccessDenied(_)), "{e:?}");
    for kind in [proto::RequestKind::Command, proto::RequestKind::Query] {
        assert!(!kraken_messaging::retry::is_retryable(kind, &e));
    }
}
