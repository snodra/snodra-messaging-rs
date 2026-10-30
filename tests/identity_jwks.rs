mod support;

use kraken_messaging::MessagingError;
use kraken_messaging::identity::{IdentityResolver, JwksVerifier};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn server_with(expected_calls: Option<u64>) -> MockServer {
    let server = MockServer::start().await;
    let mut mock = Mock::given(method("GET"))
        .and(path("/certs"))
        .respond_with(ResponseTemplate::new(200).set_body_string(support::JWKS_JSON));
    if let Some(n) = expected_calls {
        mock = mock.expect(n);
    }
    mock.mount(&server).await;
    server
}

fn resolver(server: &MockServer) -> IdentityResolver {
    IdentityResolver::new(Some(JwksVerifier::new(
        format!("{}/certs", server.uri()),
        support::ISSUER.to_string(),
    )))
}

#[tokio::test]
async fn valid_token_is_verified_identity() {
    let server = server_with(None).await;
    let token = support::token(
        "u-1",
        &["kraken-user"],
        support::ISSUER,
        support::now() + 300,
    );
    let id = resolver(&server)
        .resolve(Some(&format!("Bearer {token}")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id.user_id.as_deref(), Some("u-1"));
    assert_eq!(id.roles, vec!["kraken-user"]);
    assert!(id.verified);
}

#[tokio::test]
async fn roles_empty_when_realm_access_missing_in_verified_token() {
    let server = server_with(None).await;
    let token = support::token_without_realm_access("u-2");
    let id = resolver(&server)
        .resolve(Some(&format!("Bearer {token}")))
        .await
        .unwrap()
        .unwrap();
    assert!(id.roles.is_empty());
    assert!(id.verified);
}

#[tokio::test]
async fn expired_token_is_unauthenticated() {
    let server = server_with(None).await;
    let token = support::token("u-1", &[], support::ISSUER, support::now() - 120);
    assert!(matches!(
        resolver(&server)
            .resolve(Some(&format!("Bearer {token}")))
            .await,
        Err(MessagingError::Unauthenticated(_))
    ));
}

#[tokio::test]
async fn wrong_issuer_is_unauthenticated() {
    let server = server_with(None).await;
    let token = support::token(
        "u-1",
        &[],
        "https://other.test/realms/kraken",
        support::now() + 300,
    );
    assert!(matches!(
        resolver(&server)
            .resolve(Some(&format!("Bearer {token}")))
            .await,
        Err(MessagingError::Unauthenticated(_))
    ));
}

#[tokio::test]
async fn token_signed_by_other_key_is_unauthenticated() {
    let server = server_with(None).await;
    let token = support::sign(
        support::OTHER_KEY_PEM,
        Some("k1"),
        "u-1",
        &[],
        support::ISSUER,
        support::now() + 300,
    );
    assert!(matches!(
        resolver(&server)
            .resolve(Some(&format!("Bearer {token}")))
            .await,
        Err(MessagingError::Unauthenticated(_))
    ));
}

#[tokio::test]
async fn unknown_kid_refreshes_once_then_fails() {
    // First call primes the cache (valid token), the unknown kid forces exactly one refresh, then fails.
    let server = server_with(Some(2)).await;
    let r = resolver(&server);
    let good = support::token("u-1", &[], support::ISSUER, support::now() + 300);
    r.resolve(Some(&format!("Bearer {good}"))).await.unwrap();
    let unknown = support::sign(
        support::TEST_KEY_PEM,
        Some("k-unknown"),
        "u-1",
        &[],
        support::ISSUER,
        support::now() + 300,
    );
    assert!(matches!(
        r.resolve(Some(&format!("Bearer {unknown}"))).await,
        Err(MessagingError::Unauthenticated(_))
    ));
    // A second unknown kid within the refresh window must not hit the server again (expect(2) verifies on drop).
    assert!(matches!(
        r.resolve(Some(&format!("Bearer {unknown}"))).await,
        Err(MessagingError::Unauthenticated(_))
    ));
}
