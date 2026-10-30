mod support;

use kraken_messaging::identity::{Guard, IdentityResolver, realm_roles};
use kraken_messaging::{Identity, MessageContext, MessagingError};
use serde_json::json;

fn ctx() -> MessageContext {
    MessageContext::default()
}

fn resolver() -> IdentityResolver {
    IdentityResolver::new(None)
}

#[test]
fn realm_roles_read_from_claims() {
    let claims = json!({"realm_access": {"roles": ["kraken-user", "kraken-admin"]}});
    assert_eq!(
        realm_roles(claims.as_object().unwrap()),
        vec!["kraken-user", "kraken-admin"]
    );
}

#[test]
fn roles_empty_when_realm_access_missing() {
    let claims = json!({"sub": "u-1"});
    assert!(realm_roles(claims.as_object().unwrap()).is_empty());
}

#[test]
fn guard_role_denies_missing_role() {
    let id = Identity {
        user_id: Some("u".into()),
        roles: vec!["kraken-user".into()],
        verified: true,
    };
    assert!(matches!(
        Guard::role("kraken-admin").check(Some(&id), &ctx()),
        Err(MessagingError::AccessDenied(_))
    ));
    assert!(Guard::role("kraken-user").check(Some(&id), &ctx()).is_ok());
    assert!(
        Guard::any_role(["x", "kraken-user"])
            .check(Some(&id), &ctx())
            .is_ok()
    );
    assert!(Guard::authenticated().check(Some(&id), &ctx()).is_ok());
    assert!(
        Guard::custom(|i, _| i.verified)
            .check(Some(&id), &ctx())
            .is_ok()
    );
}

#[test]
fn guard_anonymous_is_unauthenticated() {
    assert!(matches!(
        Guard::authenticated().check(None, &ctx()),
        Err(MessagingError::Unauthenticated(_))
    ));
    assert!(matches!(
        Guard::role("kraken-user").check(None, &ctx()),
        Err(MessagingError::Unauthenticated(_))
    ));
}

#[tokio::test]
async fn absent_header_is_anonymous() {
    assert_eq!(resolver().resolve(None).await.unwrap(), None);
    assert_eq!(resolver().resolve(Some("  ")).await.unwrap(), None);
}

#[tokio::test]
async fn malformed_header_is_unauthenticated() {
    assert!(matches!(
        resolver().resolve(Some("Basic abc")).await,
        Err(MessagingError::Unauthenticated(_))
    ));
    assert!(matches!(
        resolver().resolve(Some("Bearer not-a-jwt")).await,
        Err(MessagingError::Unauthenticated(_))
    ));
}

#[tokio::test]
async fn unverified_mode_reads_signed_token_without_checking_signature() {
    // Signed by a key this process does not know: accepted, but marked unverified.
    let token = support::sign(
        support::OTHER_KEY_PEM,
        None,
        "u-9",
        &["kraken-admin"],
        "https://other.test",
        support::now() + 60,
    );
    let id = resolver()
        .resolve(Some(&format!("Bearer {token}")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id.user_id.as_deref(), Some("u-9"));
    assert_eq!(id.roles, vec!["kraken-admin"]);
    assert!(!id.verified);
}

#[tokio::test]
async fn unverified_mode_rejects_alg_none() {
    let token = support::unsigned_token("u-1");
    assert!(matches!(
        resolver().resolve(Some(&format!("Bearer {token}"))).await,
        Err(MessagingError::Unauthenticated(_))
    ));
}

#[tokio::test]
async fn unverified_mode_rejects_five_segment_token() {
    assert!(matches!(
        resolver().resolve(Some("Bearer a.b.c.d.e")).await,
        Err(MessagingError::Unauthenticated(_))
    ));
}
