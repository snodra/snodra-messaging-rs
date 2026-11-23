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
async fn a_header_that_is_not_a_bearer_token_is_unauthenticated() {
    assert!(matches!(
        resolver().resolve(Some("Basic abc")).await,
        Err(MessagingError::Unauthenticated(_))
    ));
    assert!(matches!(
        resolver().resolve(Some("Bearer ")).await,
        Err(MessagingError::Unauthenticated(_))
    ));
}

/// Without a JWKS a forwarded token is never read: whatever it claims (signed by any key,
/// unsigned, not even a JWT), the caller is anonymous. The token itself is still forwarded by
/// the handler's own sends (`MessageContext.authorization`).
#[tokio::test]
async fn without_a_jwks_a_token_is_never_an_identity() {
    let signed = support::sign(
        support::OTHER_KEY_PEM,
        None,
        "u-9",
        &["snodra-admin"],
        "https://other.test",
        support::now() + 60,
    );
    let fixture = support::token("u-1", &["admin"], support::ISSUER, support::now() + 60);
    for token in [
        signed,
        fixture,
        support::unsigned_token("u-1"),
        "not-a-jwt".to_string(),
        "a.b.c.d.e".to_string(),
    ] {
        assert_eq!(
            resolver()
                .resolve(Some(&format!("Bearer {token}")))
                .await
                .unwrap(),
            None,
            "{token}"
        );
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Guarded {}
snodra_messaging::message_type!(Guarded, "t.Guarded");
impl snodra_messaging::Command for Guarded {
    type Reply = ();
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Open {}
snodra_messaging::message_type!(Open, "t.Open");
impl snodra_messaging::Command for Open {
    type Reply = ();
}

fn app_config() -> snodra_messaging::Config {
    let mut c = snodra_messaging::Config::new("svc-guard", "static://127.0.0.1:1");
    c.hub_enabled = false;
    c
}

async fn guarded(
    config: snodra_messaging::Config,
) -> Result<snodra_messaging::SnodraMessaging, MessagingError> {
    snodra_messaging::SnodraMessaging::builder(config)
        .await
        .unwrap()
        .command::<Open, _, _>(|_, _| async { Ok(()) })
        .command::<Guarded, _, _>(|_, _| async { Ok(()) })
        .requires::<Guarded>(Guard::authenticated())
        .start()
        .await
}

/// A handler with a guard needs a JWKS to verify the callers it judges: without one the service
/// does not start, and the error names the handler and the settings.
#[tokio::test]
async fn a_guarded_handler_without_a_jwks_is_a_startup_error() {
    let refused = guarded(app_config()).await.err().expect("refused");
    assert!(
        matches!(&refused, MessagingError::InvalidMessage(m) if m.contains("t.Guarded") && m.contains("SNODRA_JWKS_URL") && !m.contains("t.Open")),
        "{refused:?}"
    );
    let mut with_jwks = app_config();
    with_jwks.jwks_url = Some("http://127.0.0.1:1/jwks".into());
    with_jwks.issuer = Some(support::ISSUER.into());
    guarded(with_jwks).await.unwrap().shutdown().await;
    // Without a guard, no JWKS is needed.
    snodra_messaging::SnodraMessaging::builder(app_config())
        .await
        .unwrap()
        .command::<Open, _, _>(|_, _| async { Ok(()) })
        .start()
        .await
        .unwrap()
        .shutdown()
        .await;
}
