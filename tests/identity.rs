mod support;

use serde_json::json;
use snodra_messaging::identity::{Guard, IdentityResolver, RolesClaim};
use snodra_messaging::{Identity, MessageContext, MessagingError};

fn ctx() -> MessageContext {
    MessageContext::default()
}

fn resolver() -> IdentityResolver {
    IdentityResolver::new(None)
}

#[test]
fn the_default_claim_is_roles() {
    let claims = json!({"roles": ["snodra-user", "snodra-admin"]});
    assert_eq!(
        RolesClaim::default().roles(claims.as_object().unwrap()),
        vec!["snodra-user", "snodra-admin"]
    );
}

#[test]
fn a_dotted_path_reads_a_nested_claim() {
    let claims = json!({"realm_access": {"roles": ["snodra-user"]}, "roles": ["ignored"]});
    assert_eq!(
        RolesClaim::parse("realm_access.roles").roles(claims.as_object().unwrap()),
        vec!["snodra-user"]
    );
}

#[test]
fn scope_is_space_separated() {
    let claims = json!({"scope": "orders.read orders.write"});
    assert_eq!(
        RolesClaim::parse("scope").roles(claims.as_object().unwrap()),
        vec!["orders.read", "orders.write"]
    );
}

#[test]
fn roles_empty_when_the_claim_is_missing() {
    let claims = json!({"sub": "u-1"});
    assert!(
        RolesClaim::default()
            .roles(claims.as_object().unwrap())
            .is_empty()
    );
}

#[test]
fn guard_role_denies_missing_role() {
    let id = Identity {
        user_id: Some("u".into()),
        roles: vec!["snodra-user".into()],
        verified: true,
    };
    assert!(matches!(
        Guard::role("snodra-admin").check(Some(&id), &ctx()),
        Err(MessagingError::AccessDenied(_))
    ));
    assert!(Guard::role("snodra-user").check(Some(&id), &ctx()).is_ok());
    assert!(
        Guard::any_role(["x", "snodra-user"])
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
        Guard::role("snodra-user").check(None, &ctx()),
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
        &["snodra-admin"],
        "https://other.test",
        support::now() + 60,
    );
    let id = resolver()
        .resolve(Some(&format!("Bearer {token}")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id.user_id.as_deref(), Some("u-9"));
    assert_eq!(id.roles, vec!["snodra-admin"]);
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
