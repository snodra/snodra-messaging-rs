//! JWT authentication, client side (roadmap P5b, ADR 0019) against the in-process fake hub and a
//! test OAuth2 token endpoint: the client-credentials token goes in `Register.credentials.jwt`,
//! one provider serves every hub connection, a refreshed token reaches the hubs that offer
//! `credentials-refresh` as `ClientMessage.credentials`, and nothing secret is ever logged.
mod support;

use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::hub::{
    AuthKind, FetchResult, HubConnection, HubPool, HubSettings, Inbound, TokenProvider, TokenStats,
};
use kraken_messaging::proto::{self, ClientMessage, client_message};
use kraken_messaging::{HubOAuth2, SecretString};
use support::fake_hub::{Acl, AuthMode, FakeHub};
use support::idp::{self, Idp, Mode};
use support::logs::Captured;
use tokio::sync::mpsc;

const SECRET: &str = "s3cr3t-client-secret-value";

fn oauth2(idp: &Idp, client_id: &str, secret: &str) -> HubOAuth2 {
    HubOAuth2 {
        token_uri: idp.token_uri(),
        client_id: client_id.into(),
        client_secret: Some(SecretString::new(secret)),
        ..HubOAuth2::default()
    }
}

fn provider(idp: &Idp) -> Arc<TokenProvider> {
    TokenProvider::new(oauth2(idp, "svc", SECRET)).unwrap()
}

fn temp_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("kraken-jwt-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn until(what: &str, within: Duration, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + within;
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
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

// ---- the token provider against a test OAuth2 token endpoint ----

#[tokio::test]
async fn the_grant_is_posted_as_client_credentials_with_basic_authentication() {
    let idp = Idp::start("svc", SECRET).await;
    let token = provider(&idp).token().await.unwrap();
    let requests = idp.token_requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].body, "grant_type=client_credentials");
    assert_eq!(
        requests[0].content_type.as_deref(),
        Some("application/x-www-form-urlencoded")
    );
    assert_eq!(requests[0].basic, Some(("svc".into(), SECRET.into())));
    // The token is the endpoint's: its azp is the client id.
    let claims = idp::claims(token.value().expose());
    assert_eq!(claims["azp"], "svc");
}

#[tokio::test]
async fn the_scope_is_sent_as_configured() {
    let idp = Idp::start("svc", SECRET).await;
    let provider = TokenProvider::new(HubOAuth2 {
        scope: Some("hub orders:read".into()),
        ..oauth2(&idp, "svc", SECRET)
    })
    .unwrap();
    provider.token().await.unwrap();
    assert_eq!(
        idp.token_requests().await[0].body,
        "grant_type=client_credentials&scope=hub+orders%3Aread"
    );
}

#[tokio::test]
async fn special_characters_in_the_client_id_and_secret_are_form_encoded_before_base64() {
    // RFC 6749 2.3.1: both are form-urlencoded, then joined with ':' and base64-encoded.
    let (id, secret) = ("my svc:1", "p@ss w%rd&é+/=");
    let idp = Idp::start(id, secret).await;
    TokenProvider::new(oauth2(&idp, id, secret))
        .unwrap()
        .token()
        .await
        .expect("the endpoint decoded the credentials back to the originals");
    let header = idp.token_requests().await[0].authorization.clone().unwrap();
    use base64::Engine;
    let expected = base64::engine::general_purpose::STANDARD
        .encode("my+svc%3A1:p%40ss+w%25rd%26%C3%A9%2B%2F%3D");
    assert_eq!(header, format!("Basic {expected}"));
}

#[tokio::test]
async fn a_token_is_cached_while_it_is_fresh() {
    let idp = Idp::start("svc", SECRET).await;
    let provider = provider(&idp);
    let (a, b) = (
        provider.token().await.unwrap(),
        provider.token().await.unwrap(),
    );
    assert_eq!(a.value().expose(), b.value().expose());
    assert_eq!(idp.token_request_count().await, 1);
}

#[tokio::test]
async fn concurrent_callers_share_one_fetch() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_delay(Duration::from_millis(300));
    let provider = provider(&idp);
    let tokens = futures::future::join_all((0..8).map(|_| {
        let p = Arc::clone(&provider);
        async move { p.token().await.unwrap() }
    }))
    .await;
    assert_eq!(idp.token_request_count().await, 1, "single-flight");
    assert!(
        tokens
            .iter()
            .all(|t| t.value().expose() == tokens[0].value().expose())
    );
}

#[tokio::test]
async fn endpoint_failures_are_classified_and_say_what_the_endpoint_said() {
    let idp = Idp::start("svc", SECRET).await;
    let provider = provider(&idp);
    let long = "x".repeat(500);
    let long: &'static str = Box::leak(long.into_boxed_str());
    let cases: Vec<(Mode, FetchResult, Vec<&str>)> = vec![
        (
            Mode::OAuthError(401, "invalid_client", "Invalid client credentials"),
            FetchResult::OauthError,
            vec!["invalid_client", "Invalid client credentials"],
        ),
        (Mode::Status(503), FetchResult::HttpError, vec!["503"]),
        (
            Mode::Garbage,
            FetchResult::BadResponse,
            vec!["not valid JSON"],
        ),
        (
            Mode::NoAccessToken,
            FetchResult::BadResponse,
            vec!["access_token"],
        ),
        (
            Mode::TokenType("mac"),
            FetchResult::BadResponse,
            vec!["token_type", "mac"],
        ),
        (
            Mode::OAuthError(400, "invalid_scope", long),
            FetchResult::OauthError,
            vec!["invalid_scope"],
        ),
    ];
    for (mode, result, texts) in cases {
        idp.set_mode(mode.clone());
        let err = provider.refresh().await.expect_err("the endpoint refuses");
        assert_eq!(err.result(), result, "{mode:?}: {err}");
        for text in texts {
            assert!(err.to_string().contains(text), "{mode:?}: {err}");
        }
        assert!(err.to_string().chars().count() <= 200, "clipped: {err}");
        assert!(!err.to_string().contains(SECRET), "{err}");
    }
    let stats = provider.stats();
    assert_eq!(
        (
            stats.ok,
            stats.http_error,
            stats.oauth_error,
            stats.io_error,
            stats.bad_response
        ),
        (0, 1, 2, 0, 3)
    );
    let status = provider.status();
    assert!(status.last_error.is_some());
    assert_eq!(status.token_expires_at, None, "no token was ever fetched");
}

#[tokio::test]
async fn an_unreachable_endpoint_is_an_io_error() {
    let port = support::containers::free_port();
    let provider = TokenProvider::new(HubOAuth2 {
        token_uri: format!("http://127.0.0.1:{port}/token"),
        client_id: "svc".into(),
        client_secret: Some(SecretString::new(SECRET)),
        ..HubOAuth2::default()
    })
    .unwrap();
    let err = provider.token().await.expect_err("nothing listens");
    assert_eq!(err.result(), FetchResult::IoError);
    assert_eq!(provider.stats().io_error, 1);
}

#[tokio::test]
async fn a_missing_expires_in_means_60_seconds_and_warns_once() {
    let captured = Captured::default();
    let _logs = captured.install();
    let idp = Idp::start("svc", SECRET).await;
    idp.set_mode(Mode::NoExpiresIn);
    let provider = provider(&idp);
    let token = provider.token().await.unwrap();
    assert_eq!(token.lifetime(), Duration::from_secs(60));
    provider.refresh().await.unwrap();
    let warnings: Vec<_> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains(" WARN ") && l.contains("expires_in"))
        .collect();
    assert_eq!(warnings.len(), 1, "{warnings:#?}");
}

#[tokio::test]
async fn the_token_type_is_compared_without_case() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_mode(Mode::TokenType("bearer"));
    provider(&idp).token().await.expect("bearer, any case");
}

#[tokio::test]
async fn an_endpoint_that_echoes_the_credentials_back_does_not_leak_them_in_the_error() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let secret = "p@ss w/rd:42";
    let encoded_secret = "p%40ss+w%2Frd%3A42";
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(move |request: &wiremock::Request| {
            let authorization = request.headers["authorization"].to_str().unwrap().to_string();
            let basic = authorization.trim_start_matches("Basic ").to_string();
            ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": "invalid_client",
                "error_description": format!(
                    "rejected '{authorization}' (basic {basic}); secret {secret} / {encoded_secret}; credentials svc:{encoded_secret}"
                ),
            }))
        })
        .mount(&server)
        .await;
    let provider = TokenProvider::new(HubOAuth2 {
        token_uri: format!("{}/token", server.uri()),
        client_id: "svc".into(),
        client_secret: Some(SecretString::new(secret)),
        ..HubOAuth2::default()
    })
    .unwrap();
    let basic = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(format!("svc:{encoded_secret}"))
    };

    let error = provider.refresh().await.expect_err("401").to_string();
    assert_eq!(
        provider.status().last_error.as_deref(),
        Some(error.as_str())
    );
    assert!(error.contains("invalid_client"), "{error}");
    assert!(error.contains("***"), "masked, not just cut: {error}");
    for leaked in [secret, encoded_secret, basic.as_str(), "Basic "] {
        assert!(!error.contains(leaked), "'{leaked}' leaked: {error}");
    }
}

#[tokio::test]
async fn the_secret_file_is_reread_on_every_fetch_and_wins_over_the_inline_secret() {
    let dir = temp_dir();
    let file = dir.join("client-secret");
    std::fs::write(&file, "first-secret\n").unwrap();
    let idp = Idp::start("svc", "first-secret").await;
    let provider = TokenProvider::new(HubOAuth2 {
        client_secret_file: Some(file.clone()),
        ..oauth2(&idp, "svc", "the-inline-secret-loses")
    })
    .unwrap();
    provider.token().await.expect("the file's secret is used");

    // The secret is rotated at the endpoint and in the file: the next fetch sends the new one.
    idp.set_secret("second-secret");
    std::fs::write(&file, "second-secret\n").unwrap();
    provider
        .refresh()
        .await
        .expect("a rotated secret applies without a restart");
    let sent: Vec<_> = idp
        .token_requests()
        .await
        .into_iter()
        .map(|r| r.basic.unwrap().1)
        .collect();
    assert_eq!(sent, ["first-secret", "second-secret"]);

    // A file caught empty mid-rotation or gone fails the fetch with a message, never a panic.
    std::fs::write(&file, "").unwrap();
    let err = provider.refresh().await.expect_err("no secret");
    assert_eq!(err.result(), FetchResult::IoError, "{err}");
    assert!(err.to_string().contains("client secret"), "{err}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn no_secret_and_no_token_ever_reach_a_log_a_debug_print_or_an_error() {
    let captured = Captured::default();
    let _logs = captured.install();
    let idp = Idp::start("svc", SECRET).await;
    let provider = provider(&idp);
    let token = provider.token().await.unwrap();
    idp.set_mode(Mode::OAuthError(401, "invalid_client", "nope"));
    let err = provider.refresh().await.expect_err("refused");
    let jwt = token.value().expose().to_string();
    let mut printed = vec![
        format!("{provider:?}"),
        format!("{token:?}"),
        format!("{token:#?}"),
        format!("{err:?}"),
        format!("{err}"),
        format!("{:?}", oauth2(&idp, "svc", SECRET)),
        format!("{:?}", provider.status()),
        format!("{:?}", jwt_settings("static://hub:7000".into(), &provider)),
        format!(
            "{:?}",
            kraken_messaging::Config {
                hub_oauth2: Some(oauth2(&idp, "svc", SECRET)),
                ..kraken_messaging::Config::new("svc", "hub:7000", "kafka:19092", "mongodb://m/db")
            }
        ),
    ];
    printed.extend(captured.lines());
    for text in printed {
        assert!(!text.contains(SECRET), "client secret leaked: {text}");
        assert!(!text.contains(&jwt), "access token leaked: {text}");
        assert!(
            !text.contains(jwt.split('.').nth(1).unwrap()),
            "the token's payload leaked: {text}"
        );
    }
}

#[tokio::test]
async fn the_provider_refreshes_a_token_before_it_expires_and_publishes_it() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(6); // margin 3 s: replaced after ~3 s
    let provider = provider(&idp);
    let mut tokens = provider.subscribe();
    assert!(tokens.borrow().is_none(), "nothing before the first fetch");
    let first = provider.token().await.unwrap();
    tokens.changed().await.unwrap();
    assert_eq!(tokens.borrow_and_update().as_ref().unwrap().generation(), 1);

    let second = tokio::time::timeout(Duration::from_secs(6), async {
        tokens.changed().await.unwrap();
        tokens.borrow_and_update().clone().unwrap()
    })
    .await
    .expect("refreshed in the background, not on demand");
    assert_ne!(second.value().expose(), first.value().expose());
    assert!(second.generation() > first.generation());
    assert_eq!(idp.issued(), 2);
    // Callers get the new token without another request.
    assert_eq!(
        provider.token().await.unwrap().value().expose(),
        second.value().expose()
    );
    assert_eq!(idp.token_request_count().await, 2);
    assert_eq!(provider.status().last_error, None);
}

#[tokio::test]
async fn a_failed_refresh_keeps_the_token_retries_and_recovers() {
    let captured = Captured::default();
    let _logs = captured.install();
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(6);
    let provider = provider(&idp);
    let first = provider.token().await.unwrap();
    idp.set_mode(Mode::Status(503));

    // The refresh at ~3 s fails; the failure is recorded and the refresh retried.
    until("the failed refresh", Duration::from_secs(6), || {
        provider.status().last_error.is_some()
    })
    .await;
    assert!(provider.status().last_error.unwrap().contains("503"));
    assert_eq!(
        provider.status().token_expires_at,
        Some(first.expires_at()),
        "the current token is kept"
    );
    // It retries (backoff 1 s, 2 s, ...) and recovers once the endpoint does.
    let failed = idp.token_request_count().await;
    idp.set_mode(Mode::Ok);
    until("a new token", Duration::from_secs(5), || idp.issued() >= 2).await;
    assert!(idp.token_request_count().await > failed);
    until("the error cleared", Duration::from_secs(1), || {
        provider.status().last_error.is_none()
    })
    .await;
    assert_ne!(
        provider.token().await.unwrap().value().expose(),
        first.value().expose()
    );
    let warnings = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains(" WARN ") && l.contains("refresh failed"))
        .count();
    assert_eq!(
        warnings,
        1,
        "one warning per distinct error: {:?}",
        captured.lines()
    );
}

#[tokio::test]
async fn a_token_past_its_margin_is_not_served_when_the_endpoint_is_down() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(1); // margin 0.5 s
    let provider = provider(&idp);
    provider.token().await.unwrap();
    idp.set_mode(Mode::Status(503));
    tokio::time::sleep(Duration::from_millis(600)).await;
    let err = provider
        .token()
        .await
        .expect_err("valid for another 0.4 s, but past its margin: the stream is not opened");
    assert_eq!(err.result(), FetchResult::HttpError);
}

#[tokio::test]
async fn a_token_fetched_during_a_failure_streak_is_not_fetched_again_by_the_retry() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(4); // margin 2 s: the proactive refresh is due at ~2 s
    let provider = provider(&idp);
    provider.token().await.unwrap();
    idp.set_mode(Mode::Status(503));
    until("the failed refresh", Duration::from_secs(5), || {
        provider.status().last_error.is_some()
    })
    .await;
    // A stream's own fetch succeeds while the loop waits for its 1 s retry: the token is new and
    // fresh for ~2 s more, so the retry has nothing to do.
    idp.set_mode(Mode::Ok);
    provider.refresh().await.unwrap();
    assert_eq!(idp.issued(), 2);
    tokio::time::sleep(Duration::from_millis(1400)).await; // past the retry, before the next margin
    assert_eq!(
        idp.issued(),
        2,
        "the retry saw the replaced token and did not fetch another"
    );
}

#[tokio::test]
async fn the_default_wake_interval_renews_within_a_few_seconds_of_a_wake_from_suspend() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(300);
    let (provider, skew) = skewed_provider(&idp, kraken_messaging::hub::WAKE_INTERVAL);
    let mut tokens = provider.subscribe();
    provider.token().await.unwrap();
    tokens.changed().await.unwrap();
    // The loop is now asleep towards a refresh point that is minutes away.
    tokio::time::sleep(Duration::from_millis(300)).await;
    skew.store(400, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(7), tokens.changed())
        .await
        .expect("renewed within one wake slice (5 s) of the wall clock jump")
        .unwrap();
}

/// A provider whose wall clock runs `skew` seconds ahead of the real one (what a machine that
/// slept sees: the monotonic clocks stood still, the wall clock moved).
fn skewed_provider(
    idp: &Idp,
    wake: Duration,
) -> (Arc<TokenProvider>, Arc<std::sync::atomic::AtomicU64>) {
    let skew = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let clock_skew = Arc::clone(&skew);
    let clock: kraken_messaging::hub::WallClock = Arc::new(move || {
        std::time::SystemTime::now()
            + Duration::from_secs(clock_skew.load(std::sync::atomic::Ordering::SeqCst))
    });
    let provider = TokenProvider::with_timing(oauth2(idp, "svc", SECRET), clock, wake).unwrap();
    (provider, skew)
}

#[tokio::test]
async fn a_token_past_its_wall_clock_expiry_is_refetched_although_the_monotonic_clock_says_fresh() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(300);
    let (provider, skew) = skewed_provider(&idp, Duration::from_secs(30));
    let first = provider.token().await.unwrap();
    assert_eq!(
        provider.token().await.unwrap().generation(),
        first.generation()
    );
    assert_eq!(idp.issued(), 1);

    // The machine slept for 400 s: `Instant` saw a few milliseconds, the wall clock 400 s.
    skew.store(400, std::sync::atomic::Ordering::SeqCst);
    let second = provider.token().await.unwrap();
    assert_ne!(second.value().expose(), first.value().expose());
    assert_eq!(idp.issued(), 2, "the expired token was not handed out");
}

#[tokio::test]
async fn a_token_inside_its_margin_by_the_wall_clock_is_refetched_too() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(300); // margin 60 s
    let (provider, skew) = skewed_provider(&idp, Duration::from_secs(30));
    provider.token().await.unwrap();
    skew.store(200, std::sync::atomic::Ordering::SeqCst); // 100 s left: fresh
    provider.token().await.unwrap();
    assert_eq!(idp.issued(), 1);
    skew.store(250, std::sync::atomic::Ordering::SeqCst); // 50 s left: inside the margin
    provider.token().await.unwrap();
    assert_eq!(idp.issued(), 2);
}

#[tokio::test]
async fn the_refresh_loop_notices_a_wall_clock_jump_without_waiting_for_the_monotonic_deadline() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(300); // the monotonic refresh point is 240 s away
    let (provider, skew) = skewed_provider(&idp, Duration::from_millis(100));
    let mut tokens = provider.subscribe();
    provider.token().await.unwrap();
    tokens.changed().await.unwrap();
    // The loop is now asleep towards a refresh point that is minutes away.
    tokio::time::sleep(Duration::from_millis(300)).await;
    skew.store(400, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), async {
        tokens.changed().await.unwrap();
    })
    .await
    .expect("the loop re-checked against the wall clock and refreshed");
    assert_eq!(idp.issued(), 2);
}

// ---- the provider on the hub connections ----

fn jwt_settings(address: String, provider: &Arc<TokenProvider>) -> HubSettings {
    HubSettings {
        address,
        heartbeat_interval: Duration::from_secs(1),
        backoff_min: Duration::from_millis(20),
        backoff_max: Duration::from_millis(300),
        token_provider: Some(Arc::clone(provider)),
        ..HubSettings::default()
    }
}

fn jwt_of(register: &proto::Register) -> Option<String> {
    match register.credentials.clone().and_then(|c| c.kind) {
        Some(proto::credentials::Kind::Jwt(j)) => Some(j),
        _ => None,
    }
}

fn jwt_hub_acl(hub: &FakeHub) {
    hub.set_auth(AuthMode::Enforce, &[]);
    hub.set_services(&[Acl::new("svc", &["*"], &["*"])]);
}

async fn wait_for_registers(hub: &FakeHub, n: usize) {
    until("registers", Duration::from_secs(5), || {
        futures::executor::block_on(hub.registers()).len() >= n
    })
    .await;
}

#[tokio::test]
async fn the_register_carries_the_fetched_jwt_and_declares_credentials_refresh() {
    let idp = Idp::start("svc", SECRET).await;
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);

    let register = hub.registers().await.remove(0);
    let jwt = jwt_of(&register).expect("a jwt credential, not a token");
    assert_eq!(jwt, provider.token().await.unwrap().value().expose());
    assert_eq!(idp::claims(&jwt)["azp"], "svc");
    assert!(
        register
            .capabilities
            .iter()
            .any(|c| c == "credentials-refresh")
    );
    assert!(hub.rejections().is_empty());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_static_token_session_declares_credentials_refresh_too() {
    let hub = FakeHub::start().await;
    let conn = connection(
        HubSettings {
            address: hub.address(),
            token: Some(SecretString::new("a-static-token")),
            ..HubSettings::default()
        },
        "svc",
    );
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert!(
        hub.registers().await[0]
            .capabilities
            .iter()
            .any(|c| c == "credentials-refresh")
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn two_hubs_share_one_token_and_one_fetch() {
    let idp = Idp::start("svc", SECRET).await;
    let (a, b) = (FakeHub::start().await, FakeHub::start().await);
    for hub in [&a, &b] {
        jwt_hub_acl(hub);
    }
    let provider = provider(&idp);
    let (tx, _rx) = mpsc::channel::<Inbound>(16);
    let pool = HubPool::new(
        jwt_settings(format!("{},{}", a.address(), b.address()), &provider),
        "svc".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    pool.start();
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);

    let jwts: Vec<_> = [&a, &b]
        .into_iter()
        .map(|h| jwt_of(&futures::executor::block_on(h.registers())[0]).expect("jwt"))
        .collect();
    assert_eq!(jwts[0], jwts[1], "one token for every hub");
    assert_eq!(idp.token_request_count().await, 1, "one fetch for N hubs");
    pool.shutdown().await;
    a.stop();
    b.stop();
}

#[tokio::test]
async fn after_the_pool_shuts_down_the_provider_asks_the_idp_for_nothing_more() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(3); // margin 1.5 s: a refresh is due soon
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    // A retained handle (a `Sender` in the application) keeps the provider alive.
    let provider = provider(&idp);
    let (tx, _rx) = mpsc::channel::<Inbound>(16);
    let pool = HubPool::new(
        jwt_settings(hub.address(), &provider),
        "svc".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    pool.start();
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    pool.shutdown().await;

    let requests = idp.token_request_count().await;
    tokio::time::sleep(Duration::from_millis(3200)).await; // past two refresh points
    assert_eq!(
        idp.token_request_count().await,
        requests,
        "no token request after the shutdown"
    );
    let err = provider.token().await.expect_err("a closed provider");
    assert!(err.to_string().contains("closed"), "{err}");
    assert_eq!(idp.token_request_count().await, requests);
    hub.stop();
}

#[tokio::test]
async fn closing_the_provider_is_idempotent_and_stops_refreshes() {
    let idp = Idp::start("svc", SECRET).await;
    let provider = provider(&idp);
    provider.token().await.unwrap();
    provider.close();
    provider.close();
    assert!(provider.token().await.is_err());
    assert!(provider.refresh().await.is_err());
    assert_eq!(idp.token_request_count().await, 1);
}

#[tokio::test]
async fn connections_that_start_together_fetch_one_token() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_delay(Duration::from_millis(300));
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    let provider = provider(&idp);
    let conns: Vec<_> = (0..5)
        .map(|_| connection(jwt_settings(hub.address(), &provider), "svc"))
        .collect();
    for c in &conns {
        c.start();
    }
    for c in &conns {
        assert!(c.await_registered(Duration::from_secs(5)).await);
    }
    assert_eq!(idp.token_request_count().await, 1, "single-flight");
    for c in &conns {
        c.shutdown().await;
    }
    hub.stop();
}

#[tokio::test]
async fn a_refreshed_token_goes_to_the_hubs_that_offer_credentials_refresh_and_to_no_other() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(6); // refreshed after ~3 s
    // Distinct hub ids: a pool keeps one connection per hub.
    let (capable, plain) = (
        FakeHub::start_with_id("capable").await,
        FakeHub::start_with_id("plain").await,
    );
    for hub in [&capable, &plain] {
        jwt_hub_acl(hub);
    }
    capable.set_credentials_refresh(true);
    let provider = provider(&idp);
    let (tx, _rx) = mpsc::channel::<Inbound>(16);
    let pool = HubPool::new(
        jwt_settings(
            format!("{},{}", capable.address(), plain.address()),
            &provider,
        ),
        "svc".into(),
        uuid::Uuid::new_v4().to_string(),
        tx,
    )
    .unwrap();
    pool.start();
    assert!(pool.await_all_registered(Duration::from_secs(5)).await);
    let registered = jwt_of(&capable.registers().await[0]).unwrap();

    until("the refresh", Duration::from_secs(8), || {
        !capable.refreshes().is_empty()
    })
    .await;
    let refreshes = capable.refreshes();
    assert_eq!(
        refreshes.len(),
        1,
        "one refresh per new token: {refreshes:?}"
    );
    let Some(proto::credentials::Kind::Jwt(sent)) = refreshes[0].1.kind.clone() else {
        panic!("a jwt refresh: {:?}", refreshes[0]);
    };
    assert_ne!(sent, registered, "the new token, not the registered one");
    assert_eq!(idp::claims(&sent)["azp"], "svc");
    assert_eq!(
        sent,
        provider.token().await.unwrap().value().expose(),
        "the provider's current token"
    );
    // The same token reached both hubs' provider-side at once: give a (wrongly) sent refresh time to
    // arrive before asserting that none did.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(plain.refreshes().is_empty(), "no capability, no refresh");
    assert_eq!(
        pool.credentials_refreshes_sent(),
        1,
        "the metric's counterpart"
    );
    assert_eq!(pool.auth().kind, AuthKind::Jwt);
    assert_eq!(
        pool.auth().token_expires_at,
        Some(provider.token().await.unwrap().expires_at())
    );
    // Nobody reconnected: one Register per hub.
    assert_eq!(capable.registers().await.len(), 1);
    assert_eq!(plain.registers().await.len(), 1);
    pool.shutdown().await;
    capable.stop();
    plain.stop();
}

#[tokio::test]
async fn a_renewal_published_before_the_register_ack_is_sent_right_after_the_ack() {
    let idp = Idp::start("svc", SECRET).await;
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    hub.set_ack(Some(support::fake_hub::default_ack()));
    hub.set_credentials_refresh(true);
    hub.set_ack_delay(Duration::from_millis(1200));
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    wait_for_registers(&hub, 1).await;
    let registered_with = jwt_of(&hub.registers().await[0]).unwrap();

    // The Register is out, the ack is not: the provider publishes a new token now. The stream
    // cannot send it yet (it does not know that the hub takes refreshes).
    let renewed = provider.refresh().await.unwrap();
    assert_ne!(renewed.value().expose(), registered_with);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(conn.handshake().is_none(), "the ack is still held back");
    assert!(hub.refreshes().is_empty());

    assert!(conn.await_registered(Duration::from_secs(5)).await);
    until("the renewal after the ack", Duration::from_secs(3), || {
        !hub.refreshes().is_empty()
    })
    .await;
    let refreshes = hub.refreshes();
    assert_eq!(refreshes.len(), 1);
    assert_eq!(
        refreshes[0].1.kind,
        Some(proto::credentials::Kind::Jwt(
            renewed.value().expose().to_string()
        )),
        "the token published during the handshake, not the one the stream registered with"
    );
    assert_eq!(hub.registers().await.len(), 1, "no reconnect");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_new_stream_after_a_refresh_registers_with_the_new_token() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(6);
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    hub.set_ack(Some(support::fake_hub::default_ack()));
    hub.set_credentials_refresh(true);
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let first = jwt_of(&hub.registers().await[0]).unwrap();
    until("the refresh", Duration::from_secs(8), || {
        !hub.refreshes().is_empty()
    })
    .await;
    hub.drop_streams();
    wait_for_registers(&hub, 2).await;
    let second = jwt_of(&hub.registers().await[1]).unwrap();
    assert_ne!(first, second);
    assert_eq!(second, provider.token().await.unwrap().value().expose());
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_token_endpoint_that_refuses_opens_no_stream_logs_one_error_and_recovers() {
    let captured = Captured::default();
    let _logs = captured.install();
    let idp = Idp::start("svc", "the-real-secret").await;
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    // The client has the wrong secret: the endpoint answers 401 invalid_client.
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    until("three attempts", Duration::from_secs(5), || {
        futures::executor::block_on(idp.token_request_count()) >= 3
    })
    .await;
    assert_eq!(hub.streams(), 0, "no stream was opened");
    assert!(!conn.is_connected());
    let rejection = conn.last_rejection().expect("surfaced like a rejection");
    assert!(
        rejection.starts_with("no access token for svc ("),
        "distinguishable from a hub refusal: {rejection}"
    );
    assert!(rejection.contains("invalid_client"), "{rejection}");
    assert!(
        rejection.contains("Invalid client credentials"),
        "{rejection}"
    );

    let lines: Vec<String> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("access token"))
        .collect();
    assert_eq!(
        lines.iter().filter(|l| l.contains(" ERROR ")).count(),
        1,
        "one ERROR per distinct error: {lines:#?}"
    );
    assert!(lines.iter().any(|l| l.contains(" DEBUG ")), "{lines:#?}");

    // The endpoint (here: the secret it expects) recovers: the next attempt registers.
    idp.set_secret(SECRET);
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert_eq!(conn.last_rejection(), None, "cleared by the registration");
    assert!(!hub.registers().await.is_empty());
    for line in captured.lines() {
        assert!(
            !line.contains(SECRET) && !line.contains("the-real-secret"),
            "{line}"
        );
    }
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn an_unauthenticated_refusal_of_the_register_makes_the_next_stream_fetch_a_new_token() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(300); // fresh by every clock: only the refusal can retire it
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    hub.refuse_jwts_with(Some((tonic::Code::Unauthenticated, "token expired")));
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    until("a refusal", Duration::from_secs(5), || {
        !hub.rejections().is_empty()
    })
    .await;
    hub.refuse_jwts_with(None);
    assert!(conn.await_registered(Duration::from_secs(5)).await);

    let refused = hub.rejections();
    let accepted = jwt_of(&hub.registers().await[0]).unwrap();
    assert!(
        refused
            .iter()
            .all(|r| r.token.as_deref() != Some(&accepted)),
        "the token the hub refused as unauthenticated was not offered again"
    );
    assert_eq!(
        idp.issued() as usize,
        refused.len() + 1,
        "one token per refused attempt, one for the accepted one"
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn an_unauthenticated_close_of_a_live_stream_makes_the_next_stream_fetch_a_new_token() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(300);
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    let first = jwt_of(&hub.registers().await[0]).unwrap();
    assert_eq!(idp.issued(), 1);

    hub.close_streams_with(tonic::Code::Unauthenticated, "credentials expired");
    wait_for_registers(&hub, 2).await;
    let second = jwt_of(&hub.registers().await[1]).unwrap();
    assert_ne!(first, second, "a new token, not the one the hub gave up on");
    assert_eq!(idp.issued(), 2);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_stream_refused_on_an_old_token_does_not_discard_a_newer_one_fetched_meanwhile() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(300);
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    hub.set_ack(Some(support::fake_hub::default_ack()));
    // The hub does not offer credentials-refresh: the stream never sends the newer token.
    hub.set_ack_delay(Duration::from_millis(1200));
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    wait_for_registers(&hub, 1).await;
    let presented = jwt_of(&hub.registers().await[0]).unwrap();

    // While the ack is held back a newer token is fetched (published, not presented), and the hub
    // then closes the stream as unauthenticated.
    let newer = provider.refresh().await.unwrap();
    hub.close_streams_with(tonic::Code::Unauthenticated, "token expired");
    wait_for_registers(&hub, 2).await;
    let next = jwt_of(&hub.registers().await[1]).unwrap();
    assert_ne!(next, presented);
    assert_eq!(
        next,
        newer.value().expose(),
        "the newer token survived: only the one the stream presented was retired"
    );
    assert_eq!(idp.issued(), 2, "no third fetch");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_permission_denied_refusal_keeps_the_token_because_a_new_one_would_not_help() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(300);
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    hub.refuse_jwts_with(Some((
        tonic::Code::PermissionDenied,
        "no ACL entry for service",
    )));
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    until("two refusals", Duration::from_secs(5), || {
        hub.rejections().len() >= 2
    })
    .await;
    hub.refuse_jwts_with(None);
    assert!(conn.await_registered(Duration::from_secs(5)).await);

    let tokens: Vec<_> = hub
        .rejections()
        .into_iter()
        .filter_map(|r| r.token)
        .collect();
    let accepted = jwt_of(&hub.registers().await[0]).unwrap();
    assert!(
        tokens.iter().all(|t| *t == accepted),
        "the same token every time"
    );
    assert_eq!(idp.issued(), 1, "the IdP was not asked again");
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn jwks_not_loaded_yet_is_a_retry_not_a_logged_rejection() {
    let captured = Captured::default();
    let _logs = captured.install();
    let idp = Idp::start("svc", SECRET).await;
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    hub.set_jwks_loaded(false);
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    until("three attempts", Duration::from_secs(5), || {
        hub.streams() >= 3
    })
    .await;
    assert_eq!(
        conn.last_rejection(),
        None,
        "UNAVAILABLE is not a rejection"
    );
    assert!(
        !captured.lines().iter().any(|l| l.contains(" ERROR ")),
        "{:#?}",
        captured.lines()
    );
    hub.set_jwks_loaded(true);
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_token_for_another_service_is_a_permission_denied_rejection() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_azp(Some("somebody-else"));
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    until("a rejection", Duration::from_secs(5), || {
        conn.last_rejection().is_some()
    })
    .await;
    let rejection = conn.last_rejection().unwrap();
    assert!(rejection.contains("azp"), "{rejection}");
    assert_eq!(hub.rejections()[0].code, tonic::Code::PermissionDenied);
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_refused_refresh_is_a_registration_rejection() {
    let captured = Captured::default();
    let _logs = captured.install();
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(6);
    let hub = FakeHub::start().await;
    jwt_hub_acl(&hub);
    hub.set_ack(Some(support::fake_hub::default_ack()));
    hub.set_credentials_refresh(true);
    let provider = provider(&idp);
    let conn = connection(jwt_settings(hub.address(), &provider), "svc");
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    // The ACL entry goes away: the refresh (and the Register after it) is refused.
    hub.set_services(&[]);
    until("the refresh is refused", Duration::from_secs(5), || {
        conn.last_rejection().is_some()
    })
    .await;
    assert!(conn.last_rejection().unwrap().contains("no ACL entry"));
    assert_eq!(hub.rejections()[0].code, tonic::Code::PermissionDenied);
    assert!(
        captured
            .lines()
            .iter()
            .any(|l| l.contains(" ERROR ") && l.contains("rejected the registration")),
        "{:#?}",
        captured.lines()
    );
    conn.shutdown().await;
    hub.stop();
}

#[tokio::test]
async fn a_rotated_token_file_is_sent_as_a_refresh_without_reconnecting() {
    let dir = temp_dir();
    let file = dir.join("hub-token");
    std::fs::write(&file, "first-token\n").unwrap();
    let hub = FakeHub::start().await;
    hub.set_auth(
        AuthMode::Enforce,
        &[
            ("first-token", Acl::new("svc", &["*"], &["*"])),
            ("second-token", Acl::new("svc", &["*"], &["*"])),
        ],
    );
    hub.set_ack(Some(support::fake_hub::default_ack()));
    hub.set_credentials_refresh(true);
    let conn = connection(
        HubSettings {
            address: hub.address(),
            heartbeat_interval: Duration::from_millis(200),
            token_file: Some(file.clone()),
            ..HubSettings::default()
        },
        "svc",
    );
    conn.start();
    assert!(conn.await_registered(Duration::from_secs(5)).await);
    assert!(hub.refreshes().is_empty(), "nothing changed yet");
    std::fs::write(&file, "second-token\n").unwrap();
    until("the refresh", Duration::from_secs(8), || {
        !hub.refreshes().is_empty()
    })
    .await;
    let refreshes = hub.refreshes();
    assert_eq!(
        refreshes[0].1.kind,
        Some(proto::credentials::Kind::Token("second-token".into()))
    );
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(hub.refreshes().len(), 1, "one refresh per change");
    assert_eq!(hub.registers().await.len(), 1, "no reconnect");
    conn.shutdown().await;
    hub.stop();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn the_auth_status_names_the_kind_the_expiry_and_the_last_fetch_error() {
    let hub = FakeHub::start().await;
    let none = connection(
        HubSettings {
            address: hub.address(),
            ..HubSettings::default()
        },
        "svc",
    );
    assert_eq!(none.auth().kind, AuthKind::None);
    let token = connection(
        HubSettings {
            address: hub.address(),
            token: Some(SecretString::new("t")),
            ..HubSettings::default()
        },
        "svc",
    );
    assert_eq!(token.auth().kind, AuthKind::Token);
    assert_eq!(AuthKind::Jwt.label(), "jwt");

    let idp = Idp::start("svc", SECRET).await;
    let provider = provider(&idp);
    let jwt = connection(jwt_settings(hub.address(), &provider), "svc");
    assert_eq!(jwt.auth().kind, AuthKind::Jwt);
    assert_eq!(jwt.auth().token_expires_at, None, "before the first fetch");
    let fetched = provider.token().await.unwrap();
    assert_eq!(jwt.auth().token_expires_at, Some(fetched.expires_at()));
    idp.set_mode(Mode::Status(503));
    provider.refresh().await.expect_err("503");
    assert!(jwt.auth().last_error.unwrap().contains("503"));
    hub.stop();
}

#[tokio::test]
async fn the_auth_status_carries_the_token_fetch_counts() {
    let hub = FakeHub::start().await;
    let token = connection(
        HubSettings {
            address: hub.address(),
            token: Some(SecretString::new("t")),
            ..HubSettings::default()
        },
        "svc",
    );
    assert_eq!(token.auth().token_fetches, TokenStats::default());

    let idp = Idp::start("svc", SECRET).await;
    let provider = provider(&idp);
    let pool_side = connection(jwt_settings(hub.address(), &provider), "svc");
    assert_eq!(pool_side.auth().token_fetches, TokenStats::default());
    provider.token().await.unwrap();
    idp.set_mode(Mode::Status(503));
    provider.refresh().await.expect_err("503");
    idp.set_mode(Mode::OAuthError(401, "invalid_client", "nope"));
    provider.refresh().await.expect_err("401");
    let counts = pool_side.auth().token_fetches;
    assert_eq!(
        counts,
        TokenStats {
            ok: 1,
            http_error: 1,
            oauth_error: 1,
            ..TokenStats::default()
        }
    );
    assert_eq!(counts, provider.stats());
    hub.stop();
}
