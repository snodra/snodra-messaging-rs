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

/// Without a JWKS a well-formed signed token is never an identity: whatever it claims (signed by
/// any key), the caller is anonymous. The token itself is still forwarded by the handler's own
/// sends (`MessageContext.authorization`).
#[tokio::test]
async fn without_a_jwks_a_signed_token_is_never_an_identity() {
    let signed = support::sign(
        support::OTHER_KEY_PEM,
        None,
        "u-9",
        &["snodra-admin"],
        "https://other.test",
        support::now() + 60,
    );
    let fixture = support::token("u-1", &["admin"], support::ISSUER, support::now() + 60);
    for token in [signed, fixture] {
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

/// Without a JWKS a token that is not a well-formed signed JWS is still refused
/// (`UNAUTHENTICATED`), by its shape alone: not three segments, encrypted (five), a header or
/// claims segment that is not a base64url JSON object, `alg: none`, no signature.
#[tokio::test]
async fn without_a_jwks_a_malformed_token_is_unauthenticated() {
    use base64::Engine;
    let b64 = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s);
    let header = b64(r#"{"alg":"RS256","kid":"k1"}"#);
    let claims = b64(r#"{"sub":"u-1"}"#);
    let cases = [
        ("one segment", "not-a-jwt".to_string()),
        ("two segments", format!("{header}.{claims}")),
        ("four segments", format!("{header}.{claims}.sig.x")),
        ("encrypted", "a.b.c.d.e".to_string()),
        ("unreadable header", format!("!!.{claims}.sig")),
        (
            "header not an object",
            format!("{}.{claims}.sig", b64("[1]")),
        ),
        (
            "claims not an object",
            format!("{header}.{}.sig", b64("42")),
        ),
        ("unreadable claims", format!("{header}.%%.sig")),
        ("alg none", support::unsigned_token("u-1")),
        (
            "alg none, signed",
            format!("{}.{claims}.sig", b64(r#"{"alg":"none"}"#)),
        ),
        ("no signature", format!("{header}.{claims}.")),
        (
            "alg not a string",
            format!("{}.{claims}.sig", b64(r#"{"alg":5}"#)),
        ),
    ];
    for (what, token) in cases {
        let resolved = resolver().resolve(Some(&format!("Bearer {token}"))).await;
        assert!(
            matches!(resolved, Err(MessagingError::Unauthenticated(_))),
            "{what}: {resolved:?}"
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

/// The authorization header as the reference reads it: the scheme `Bearer ` in any letter case
/// at the very start of the value, the token what follows it, trimmed. A blank header is an
/// anonymous caller; leading whitespace, another scheme or no token is a malformed header.
#[tokio::test]
async fn the_bearer_scheme_is_read_at_the_start_in_any_case() {
    let token = support::sign(
        support::OTHER_KEY_PEM,
        None,
        "u-1",
        &[],
        "https://other.test",
        support::now() + 60,
    );
    for header in [
        format!("Bearer {token}"),
        format!("bearer {token}"),
        format!("BEARER {token}"),
        format!("bEaReR   {token}  "),
    ] {
        assert_eq!(
            resolver().resolve(Some(&header)).await.unwrap(),
            None,
            "accepted (anonymous without a JWKS): {header:?}"
        );
    }
    for header in ["", "   ", "\t"] {
        assert_eq!(
            resolver().resolve(Some(header)).await.unwrap(),
            None,
            "blank is anonymous: {header:?}"
        );
    }
    for header in [
        format!(" Bearer {token}"),
        format!("Bearer\t{token}"),
        format!("Token {token}"),
        "Bearer".to_string(),
        "Bearer    ".to_string(),
    ] {
        let resolved = resolver().resolve(Some(&header)).await;
        assert!(
            matches!(&resolved, Err(MessagingError::Unauthenticated(m)) if m == "malformed authorization header"),
            "{header:?}: {resolved:?}"
        );
    }
}
