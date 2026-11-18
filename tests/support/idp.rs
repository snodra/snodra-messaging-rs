//! A test identity provider for the hub JWT tests (P5b): an OAuth2 token endpoint that answers the
//! client-credentials grant like Keycloak (HTTP Basic client authentication, `invalid_client` as a
//! 401 JSON body) with ES256 access tokens, and the JWKS that verifies them. The signing key is
//! generated at test time (rcgen); nothing is checked in.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::json;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// What the token endpoint answers.
#[derive(Clone, Debug)]
pub enum Mode {
    /// A token with the configured lifetime.
    Ok,
    /// A bare HTTP status (an overloaded proxy, say).
    Status(u16),
    /// `{"error": .., "error_description": ..}` with the given status (Keycloak: 401 `invalid_client`).
    OAuthError(u16, &'static str, &'static str),
    /// A 200 whose body is not JSON.
    Garbage,
    /// A 200 without `access_token`.
    NoAccessToken,
    /// A token with this `token_type`.
    TokenType(&'static str),
    /// A token without `expires_in`.
    NoExpiresIn,
}

pub struct Config {
    pub client_id: String,
    pub secret: String,
    /// Seconds.
    pub lifetime: u64,
    pub issuer: String,
    pub mode: Mode,
    pub delay: Duration,
    /// The `azp` of the tokens; `None`: the authenticated client id.
    pub azp: Option<String>,
}

pub struct TokenRequest {
    /// The decoded Basic credentials (form-decoded id and secret), if the header was one.
    pub basic: Option<(String, String)>,
    pub authorization: Option<String>,
    pub content_type: Option<String>,
    pub body: String,
}

struct Shared {
    key: EncodingKey,
    jwks: String,
    config: Mutex<Config>,
    issued: AtomicU64,
}

pub struct Idp {
    server: MockServer,
    shared: Arc<Shared>,
}

struct Token(Arc<Shared>);

impl Respond for Token {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let cfg = self.0.config.lock().unwrap();
        let basic = parse_basic(request);
        let authenticated = basic
            .as_ref()
            .is_some_and(|(id, secret)| *id == cfg.client_id && *secret == cfg.secret);
        let template = if !authenticated {
            ResponseTemplate::new(401).set_body_json(json!({
                "error": "invalid_client",
                "error_description": "Invalid client or Invalid client credentials",
            }))
        } else {
            match &cfg.mode {
                Mode::Status(code) => ResponseTemplate::new(*code),
                Mode::OAuthError(code, error, description) => ResponseTemplate::new(*code)
                    .set_body_json(json!({"error": error, "error_description": description})),
                Mode::Garbage => {
                    ResponseTemplate::new(200).set_body_string("<html>not json</html>")
                }
                Mode::NoAccessToken => ResponseTemplate::new(200)
                    .set_body_json(json!({"token_type": "Bearer", "expires_in": 60})),
                Mode::Ok | Mode::TokenType(_) | Mode::NoExpiresIn => {
                    let n = self.0.issued.fetch_add(1, Ordering::SeqCst) + 1;
                    let now = jsonwebtoken::get_current_timestamp();
                    let mut header = Header::new(Algorithm::ES256);
                    header.kid = Some("k1".into());
                    let claims = json!({
                        "iss": cfg.issuer,
                        "azp": cfg.azp.clone().unwrap_or_else(|| cfg.client_id.clone()),
                        "typ": "Bearer",
                        "sub": format!("service-account-{}", cfg.client_id),
                        // A few seconds back: the hub of an e2e test runs with no leeway, in a VM
                        // whose clock may trail the host's by a moment.
                        "iat": now.saturating_sub(10),
                        "exp": now + cfg.lifetime,
                        "jti": format!("token-{n}"),
                    });
                    let jwt = jsonwebtoken::encode(&header, &claims, &self.0.key).unwrap();
                    let mut body = json!({
                        "access_token": jwt,
                        "token_type": "Bearer",
                        "expires_in": cfg.lifetime,
                    });
                    match &cfg.mode {
                        Mode::TokenType(t) => body["token_type"] = json!(t),
                        Mode::NoExpiresIn => {
                            body.as_object_mut().unwrap().remove("expires_in");
                        }
                        _ => {}
                    }
                    ResponseTemplate::new(200).set_body_json(body)
                }
            }
        };
        template.set_delay(cfg.delay)
    }
}

fn header(request: &Request, name: &str) -> Option<String> {
    request
        .headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

fn parse_basic(request: &Request) -> Option<(String, String)> {
    let value = header(request, "authorization")?;
    let encoded = value.strip_prefix("Basic ")?;
    let decoded = String::from_utf8(STANDARD.decode(encoded).ok()?).ok()?;
    let (id, secret) = decoded.split_once(':')?;
    Some((form_decode(id), form_decode(secret)))
}

/// `application/x-www-form-urlencoded` decoding (`+` is a space).
fn form_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out).unwrap()
}

impl Idp {
    /// On 127.0.0.1, for tests with the in-process fake hub.
    pub async fn start(client_id: &str, secret: &str) -> Idp {
        Self::start_on(client_id, secret, MockServer::builder()).await
    }

    /// On every interface (for a hub container that reaches the host through a host-gateway alias).
    pub async fn start_public(client_id: &str, secret: &str) -> Idp {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        Self::start_on(client_id, secret, MockServer::builder().listener(listener)).await
    }

    async fn start_on(client_id: &str, secret: &str, builder: wiremock::MockServerBuilder) -> Idp {
        let pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let raw = pair.public_key_raw();
        assert_eq!(raw.len(), 65, "an uncompressed P-256 point");
        let jwks = json!({"keys": [{
            "kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": "k1",
            "x": URL_SAFE_NO_PAD.encode(&raw[1..33]),
            "y": URL_SAFE_NO_PAD.encode(&raw[33..65]),
        }]})
        .to_string();
        let key = EncodingKey::from_ec_pem(pair.serialize_pem().as_bytes()).unwrap();
        let server = builder.start().await;
        let shared = Arc::new(Shared {
            key,
            jwks: jwks.clone(),
            config: Mutex::new(Config {
                client_id: client_id.to_string(),
                secret: secret.to_string(),
                lifetime: 300,
                issuer: format!("{}/realms/snodra", server.uri()),
                mode: Mode::Ok,
                delay: Duration::ZERO,
                azp: None,
            }),
            issued: AtomicU64::new(0),
        });
        Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/token"))
            .respond_with(Token(Arc::clone(&shared)))
            .mount(&server)
            .await;
        Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/certs"))
            .respond_with(ResponseTemplate::new(200).set_body_string(jwks))
            .mount(&server)
            .await;
        Idp { server, shared }
    }

    pub fn token_uri(&self) -> String {
        format!("{}/token", self.server.uri())
    }

    pub fn port(&self) -> u16 {
        self.server.address().port()
    }

    pub fn issuer(&self) -> String {
        self.shared.config.lock().unwrap().issuer.clone()
    }

    pub fn set_issuer(&self, issuer: &str) {
        self.shared.config.lock().unwrap().issuer = issuer.to_string();
    }

    pub fn jwks(&self) -> String {
        self.shared.jwks.clone()
    }

    /// Token lifetime in seconds: `expires_in`, and `exp` is that long after issue. `iat` is set
    /// 10 s before issue (see the claims above), so `exp - iat` is this plus 10.
    pub fn set_lifetime(&self, seconds: u64) {
        self.shared.config.lock().unwrap().lifetime = seconds;
    }

    pub fn set_mode(&self, mode: Mode) {
        self.shared.config.lock().unwrap().mode = mode;
    }

    pub fn set_delay(&self, delay: Duration) {
        self.shared.config.lock().unwrap().delay = delay;
    }

    /// The secret the endpoint now expects (a rotation).
    pub fn set_secret(&self, secret: &str) {
        self.shared.config.lock().unwrap().secret = secret.to_string();
    }

    pub fn set_azp(&self, azp: Option<&str>) {
        self.shared.config.lock().unwrap().azp = azp.map(str::to_string);
    }

    /// Tokens issued so far.
    pub fn issued(&self) -> u64 {
        self.shared.issued.load(Ordering::SeqCst)
    }

    /// The requests the token endpoint received, oldest first.
    pub async fn token_requests(&self) -> Vec<TokenRequest> {
        self.server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| r.url.path() == "/token")
            .map(|r| TokenRequest {
                basic: parse_basic(r),
                authorization: header(r, "authorization"),
                content_type: header(r, "content-type"),
                body: String::from_utf8_lossy(&r.body).into_owned(),
            })
            .collect()
    }

    pub async fn token_request_count(&self) -> usize {
        self.token_requests().await.len()
    }
}

/// The claims of a JWT, read without verifying it.
pub fn claims(jwt: &str) -> serde_json::Value {
    let payload = jwt.split('.').nth(1).expect("a JWT has three segments");
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap()
}
