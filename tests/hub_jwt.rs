//! JWT authentication, client side (roadmap P5b, ADR 0019) against the in-process fake hub and a
//! test OAuth2 token endpoint: the client-credentials token goes in `Register.credentials.jwt`,
//! one provider serves every hub connection, a refreshed token reaches the hubs that offer
//! `credentials-refresh` as `ClientMessage.credentials`, and nothing secret is ever logged.
mod support;

use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::hub::{FetchResult, HubConnection, HubSettings, Inbound, TokenProvider};
use kraken_messaging::proto::{self, ClientMessage, client_message};
use kraken_messaging::{HubOAuth2, SecretString};
use support::fake_hub::FakeHub;
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
    idp.set_lifetime(6); // margin 4.8 s: replaced after ~1.2 s
    let provider = provider(&idp);
    let mut tokens = provider.subscribe();
    assert!(tokens.borrow().is_none(), "nothing before the first fetch");
    let first = provider.token().await.unwrap();
    tokens.changed().await.unwrap();
    assert_eq!(tokens.borrow_and_update().as_ref().unwrap().generation(), 1);

    let second = tokio::time::timeout(Duration::from_secs(4), async {
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

    // The refresh at ~1.2 s fails; the token that is still valid keeps being served.
    until("the failed refresh", Duration::from_secs(4), || {
        provider.status().last_error.is_some()
    })
    .await;
    assert!(provider.status().last_error.unwrap().contains("503"));
    assert_eq!(
        provider.token().await.unwrap().value().expose(),
        first.value().expose(),
        "the endpoint is down but the token is valid: new streams get it"
    );
    // It retries (backoff 1 s, 2 s, ...) and recovers once the endpoint does.
    let failed = idp.token_request_count().await;
    idp.set_mode(Mode::Ok);
    until("a new token", Duration::from_secs(4), || idp.issued() >= 2).await;
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
async fn an_expired_token_is_not_served_when_the_endpoint_is_down() {
    let idp = Idp::start("svc", SECRET).await;
    idp.set_lifetime(1);
    let provider = provider(&idp);
    provider.token().await.unwrap();
    idp.set_mode(Mode::Status(503));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let err = provider.token().await.expect_err("expired and no new one");
    assert_eq!(err.result(), FetchResult::HttpError);
}
