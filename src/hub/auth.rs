//! JWT authentication, client side: the OAuth2 client-credentials token
//! provider. One [`TokenProvider`] per process serves every hub connection:
//!
//! - `POST <token_uri>` with `grant_type=client_credentials` (+ `scope`), client authentication
//!   HTTP Basic (`client_secret_basic`: id and secret form-urlencoded before base64, RFC 6749
//!   2.3.1), 10 s timeout, response body capped at 1 MiB. `access_token` is required; `expires_in`
//!   missing means 60 s (one warning); `token_type`, when present, must be `Bearer`.
//! - The token is cached while more than its refresh margin remains (20 % of its lifetime, at least
//!   5 s, at most half of it) by the monotonic clock **and** by the wall clock (a suspended machine
//!   stops the monotonic clocks, not the wall clock); concurrent callers share one fetch ([`TokenProvider::token`],
//!   [`TokenProvider::refresh`]).
//! - A background task refreshes the token at that point and publishes it on a watch channel
//!   ([`TokenProvider::subscribe`]), which every live hub stream follows to send
//!   `ClientMessage.credentials`. A failed proactive refresh keeps the current token and retries
//!   after 1 s, 2 s ... 30 s. The loop wakes at least every 5 s ([`WAKE_INTERVAL`]) and looks at the
//!   wall clock again, so a wake from suspend refreshes at once. A new stream never gets a token that is past its margin: when the
//!   endpoint cannot give a new one the stream is not opened.
//!
//! The client secret (re-read from `client_secret_file` on every fetch) and the access tokens are
//! never logged and never in an error or a `Debug` print.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::error::MessagingError;
use crate::security::{HubOAuth2, SecretString};

/// How long the token endpoint may take to answer.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// The most of a response body that is read.
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// The lifetime assumed when the endpoint sends no `expires_in`.
pub const DEFAULT_LIFETIME: Duration = Duration::from_secs(60);
/// A token is refreshed at least this long before it expires...
pub const MIN_REFRESH_MARGIN: Duration = Duration::from_secs(5);
/// The wait after the first failed proactive refresh; it doubles up to [`RETRY_MAX`].
pub const RETRY_MIN: Duration = Duration::from_secs(1);
pub const RETRY_MAX: Duration = Duration::from_secs(30);
/// The refresh loop wakes at least this often and looks at the wall clock: the monotonic clocks
/// (`Instant`, tokio's timers) stand still while a laptop sleeps, the wall clock does not.
pub const WAKE_INTERVAL: Duration = Duration::from_secs(5);
/// An error text is clipped to this many characters.
pub const ERROR_CLIP: usize = 200;

/// The outcome of one token fetch, as the `result` label of
/// `snodra_hub_client_token_fetches_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchResult {
    Ok,
    /// A non-success HTTP status without an OAuth2 error body.
    HttpError,
    /// The endpoint answered with an OAuth2 `error` (`invalid_client`, ...).
    OauthError,
    /// The request did not complete (connect, TLS, timeout, read) or the client secret could not be
    /// read.
    IoError,
    /// A 200 that is not a usable token response.
    BadResponse,
}

impl FetchResult {
    pub fn label(self) -> &'static str {
        match self {
            FetchResult::Ok => "ok",
            FetchResult::HttpError => "http_error",
            FetchResult::OauthError => "oauth_error",
            FetchResult::IoError => "io_error",
            FetchResult::BadResponse => "bad_response",
        }
    }
}

/// Fetch counts per [`FetchResult`]: what `snodra_hub_client_token_fetches_total{result}` reads
/// ([`HubPool::observe`](super::HubPool::observe)).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TokenStats {
    pub ok: u64,
    pub http_error: u64,
    pub oauth_error: u64,
    pub io_error: u64,
    pub bad_response: u64,
}

#[derive(Default)]
struct Counters {
    ok: AtomicU64,
    http_error: AtomicU64,
    oauth_error: AtomicU64,
    io_error: AtomicU64,
    bad_response: AtomicU64,
}

impl Counters {
    fn of(&self, result: FetchResult) -> &AtomicU64 {
        match result {
            FetchResult::Ok => &self.ok,
            FetchResult::HttpError => &self.http_error,
            FetchResult::OauthError => &self.oauth_error,
            FetchResult::IoError => &self.io_error,
            FetchResult::BadResponse => &self.bad_response,
        }
    }
}

/// Why a token could not be fetched: what the endpoint said (HTTP status, OAuth2 `error` and
/// `error_description`), clipped to [`ERROR_CLIP`] characters. Never carries the secret or a token.
#[derive(Clone, Debug)]
pub struct TokenError {
    result: FetchResult,
    message: String,
}

impl TokenError {
    fn new(result: FetchResult, message: impl AsRef<str>) -> TokenError {
        TokenError {
            result,
            message: clip(message.as_ref()),
        }
    }

    pub fn result(&self) -> FetchResult {
        self.result
    }
}

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TokenError {}

/// Control characters become spaces (a server-supplied text must not forge a log line); longer
/// than [`ERROR_CLIP`] characters ends with an ellipsis.
fn clip(text: &str) -> String {
    let clean: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if clean.chars().count() <= ERROR_CLIP {
        clean
    } else {
        let mut out: String = clean.chars().take(ERROR_CLIP - 1).collect();
        out.push('…');
        out
    }
}

/// An access token and when it dies. `Debug` never shows it.
#[derive(Debug)]
pub struct AccessToken {
    value: SecretString,
    issued: Instant,
    lifetime: Duration,
    expires_at: SystemTime,
    generation: u64,
}

impl AccessToken {
    /// The JWT; [`SecretString::expose`] it only to put it on the wire.
    pub fn value(&self) -> &SecretString {
        &self.value
    }

    /// `expires_in` of the response (60 s when it had none).
    pub fn lifetime(&self) -> Duration {
        self.lifetime
    }

    pub fn expires_at(&self) -> SystemTime {
        self.expires_at
    }

    /// 1, 2, ... in the order the provider fetched them: two streams compare it to tell whether a
    /// token is the one they already sent.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// How long the token still lives: the lesser of what the monotonic clock says (immune to the
    /// wall clock being set) and what the wall clock says (immune to a suspended machine, whose
    /// monotonic clocks stand still: macOS, a paused VM).
    fn remaining(&self, now: SystemTime) -> Duration {
        let monotonic = self.lifetime.saturating_sub(self.issued.elapsed());
        let wall = self
            .expires_at
            .duration_since(now)
            .unwrap_or(Duration::ZERO);
        monotonic.min(wall)
    }

    /// More than the refresh margin remains.
    fn fresh(&self, now: SystemTime) -> bool {
        self.remaining(now) > refresh_margin(self.lifetime)
    }

    /// How long until the margin starts.
    fn until_refresh(&self, now: SystemTime) -> Duration {
        self.remaining(now)
            .saturating_sub(refresh_margin(self.lifetime))
    }
}

/// How long before its expiry a token of `lifetime` is replaced: 20 % of it, at least 5 s, at
/// most half of it. The cap only matters below 10 s: without it a 6 s token would be renewed every
/// second, at the hub's refresh throttle.
pub fn refresh_margin(lifetime: Duration) -> Duration {
    (lifetime / 5).max(MIN_REFRESH_MARGIN).min(lifetime / 2)
}

/// What kind of credential the hub connections present.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthKind {
    None,
    /// A static token (`hub_token` / `hub_token_file`).
    Token,
    /// An OAuth2 client-credentials access token (`hub_oauth2`).
    Jwt,
    /// A client certificate (`hub_tls.client_cert_file`, mutual TLS).
    Certificate,
}

impl AuthKind {
    pub fn label(self) -> &'static str {
        match self {
            AuthKind::None => "none",
            AuthKind::Token => "token",
            AuthKind::Jwt => "jwt",
            AuthKind::Certificate => "certificate",
        }
    }
}

/// The authentication of the hub connections at one moment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthStatus {
    pub kind: AuthKind,
    /// When the current access token expires (`Jwt` only; `None` until the first fetch).
    pub token_expires_at: Option<SystemTime>,
    /// The last token fetch error (clipped), cleared by the next success (`Jwt` only).
    pub last_error: Option<String>,
    /// Token fetches so far by result (`Jwt` only; all zero otherwise), as
    /// `snodra_hub_client_token_fetches_total{result}` exports them.
    pub token_fetches: TokenStats,
    /// The client certificate the hub streams present (`Certificate` only; `None` until a stream
    /// registered with it).
    pub certificate: Option<CertificateStatus>,
}

/// The client certificate in use. The certificate itself is never shown, only the
/// first 8 hex characters of the SHA-256 of its DER, as the hub shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CertificateStatus {
    /// The service the leaf names (`snodra://service/<name>`).
    pub service: String,
    /// The first 8 hex characters of the SHA-256 fingerprint of the leaf presented.
    pub fingerprint: String,
    /// The leaf's `notAfter`: the hub closes the session then.
    pub not_after: SystemTime,
    /// How many times the presented certificate changed after the first one (a renewed pair
    /// rolled over, or picked up by a reconnect).
    pub rotations: u64,
}

/// The wall clock the provider reads (replaceable in tests).
pub type WallClock = Arc<dyn Fn() -> SystemTime + Send + Sync>;

type Flight = Shared<BoxFuture<'static, Result<Arc<AccessToken>, TokenError>>>;

/// The token provider of the process: see the module docs.
pub struct TokenProvider {
    settings: HubOAuth2,
    http: reqwest::Client,
    current: Mutex<Option<Arc<AccessToken>>>,
    published: watch::Sender<Option<Arc<AccessToken>>>,
    inflight: Mutex<Option<Flight>>,
    generation: AtomicU64,
    counters: Counters,
    last_error: Mutex<Option<String>>,
    expires_in_warned: AtomicBool,
    refresher_started: AtomicBool,
    closed: AtomicBool,
    cancel: CancellationToken,
    clock: WallClock,
    wake: Duration,
}

impl fmt::Debug for TokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenProvider")
            .field("token_uri", &self.settings.token_uri)
            .field("client_id", &self.settings.client_id)
            .field("scope", &self.settings.scope)
            .finish_non_exhaustive()
    }
}

impl Drop for TokenProvider {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl TokenProvider {
    /// Builds the provider; nothing is fetched yet. `InvalidMessage` for a token URI that is not
    /// `http(s)://` and for a CA file that cannot be read ([`crate::Config::validate`] checks the
    /// same). A plain `http://` URI logs one warning unless its host is loopback or a single-label
    /// name (a compose service): the client secret then crosses the network unencrypted.
    pub fn new(settings: HubOAuth2) -> Result<Arc<TokenProvider>, MessagingError> {
        Self::with_timing(settings, Arc::new(SystemTime::now), WAKE_INTERVAL)
    }

    /// [`new`](Self::new) with the wall clock and the refresh loop's wake interval given (tests).
    #[doc(hidden)]
    pub fn with_timing(
        settings: HubOAuth2,
        clock: WallClock,
        wake: Duration,
    ) -> Result<Arc<TokenProvider>, MessagingError> {
        let url = reqwest::Url::parse(settings.token_uri.trim()).map_err(|e| {
            MessagingError::InvalidMessage(format!(
                "hub_oauth2.token_uri (SNODRA_SERVER_OAUTH2_TOKEN_URI) is not a URL: {e}"
            ))
        })?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(MessagingError::InvalidMessage(
                "hub_oauth2.token_uri (SNODRA_SERVER_OAUTH2_TOKEN_URI) must be an http:// or https:// URL"
                    .into(),
            ));
        }
        if url.scheme() == "http" && !local_host(&url) {
            tracing::warn!(token_uri = %display_uri(&url), "the OAuth2 token endpoint is plain http over a routed network: the client secret and the tokens travel unencrypted; use https");
        }
        let mut http = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            // The Basic credentials must go to the configured endpoint only.
            .redirect(reqwest::redirect::Policy::none());
        if let Some(path) = &settings.ca_file {
            let pem = std::fs::read(path).map_err(|e| {
                MessagingError::InvalidMessage(format!(
                    "hub_oauth2.ca_file (SNODRA_SERVER_OAUTH2_CA) '{}' cannot be read: {e}",
                    path.display()
                ))
            })?;
            let certs = reqwest::Certificate::from_pem_bundle(&pem).map_err(|e| {
                MessagingError::InvalidMessage(format!(
                    "hub_oauth2.ca_file (SNODRA_SERVER_OAUTH2_CA) '{}' holds no usable certificate: {e}",
                    path.display()
                ))
            })?;
            for cert in certs {
                http = http.add_root_certificate(cert);
            }
        }
        let http = http.build().map_err(|e| {
            MessagingError::InvalidMessage(format!("OAuth2 HTTP client: {}", e.without_url()))
        })?;
        let (published, _) = watch::channel(None);
        Ok(Arc::new(TokenProvider {
            settings,
            http,
            current: Mutex::new(None),
            published,
            inflight: Mutex::new(None),
            generation: AtomicU64::new(0),
            counters: Counters::default(),
            last_error: Mutex::new(None),
            expires_in_warned: AtomicBool::new(false),
            refresher_started: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            clock,
            wake,
        }))
    }

    /// A token to register with: the cached one while more than its refresh margin remains, else a
    /// new one (concurrent callers wait for one fetch). A token past its margin is never returned
    /// when the fetch fails: the error is.
    pub async fn token(self: &Arc<Self>) -> Result<Arc<AccessToken>, TokenError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::closed_error());
        }
        if let Some(token) = self.cached().filter(|t| t.fresh((self.clock)())) {
            return Ok(token);
        }
        self.refresh().await
    }

    /// Fetches a new token now (joining a fetch already under way), whatever the cache holds.
    pub async fn refresh(self: &Arc<Self>) -> Result<Arc<AccessToken>, TokenError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::closed_error());
        }
        let flight = {
            let mut inflight = self.inflight.lock().unwrap();
            match inflight.as_ref() {
                Some(flight) => flight.clone(),
                None => {
                    let me = Arc::clone(self);
                    let flight: Flight = async move {
                        let outcome = me.fetch().await;
                        me.inflight.lock().unwrap().take();
                        outcome
                    }
                    .boxed()
                    .shared();
                    *inflight = Some(flight.clone());
                    flight
                }
            }
        };
        flight.await
    }

    /// Stops the provider for good (idempotent): the background refresh ends and no request goes
    /// to the token endpoint any more; [`token`](Self::token) and [`refresh`](Self::refresh)
    /// return an error. Without it the loop runs until the last `Arc` is dropped, which a retained
    /// handle can delay arbitrarily. [`HubPool::shutdown`](super::HubPool::shutdown) calls it.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.cancel.cancel();
    }

    fn closed_error() -> TokenError {
        TokenError::new(FetchResult::IoError, "the OAuth2 token provider is closed")
    }

    /// The hub turned `token` down as unauthenticated (expired, unknown signing key, ...): it is not
    /// handed out again, so the next stream fetches a new one. A token that has been replaced since
    /// is left alone. Not for `PERMISSION_DENIED`: that is about the service, and a new token
    /// would not help (nor should the IdP be hit for it).
    pub fn invalidate(&self, token: &SecretString) {
        let mut current = self.current.lock().unwrap();
        if current.as_ref().is_some_and(|t| t.value() == token) {
            *current = None;
        }
    }

    /// The tokens as they are fetched: every live hub stream follows this to send
    /// `ClientMessage.credentials`. Holds `None` until the first fetch.
    pub fn subscribe(&self) -> watch::Receiver<Option<Arc<AccessToken>>> {
        self.published.subscribe()
    }

    /// The expiry of the current token and the last fetch error.
    pub fn status(&self) -> AuthStatus {
        AuthStatus {
            kind: AuthKind::Jwt,
            token_expires_at: self.cached().map(|t| t.expires_at),
            last_error: self.last_error.lock().unwrap().clone(),
            token_fetches: self.stats(),
            certificate: None,
        }
    }

    /// Fetches so far, by result.
    pub fn stats(&self) -> TokenStats {
        let c = &self.counters;
        let get = |n: &AtomicU64| n.load(Ordering::Relaxed);
        TokenStats {
            ok: get(&c.ok),
            http_error: get(&c.http_error),
            oauth_error: get(&c.oauth_error),
            io_error: get(&c.io_error),
            bad_response: get(&c.bad_response),
        }
    }

    fn cached(&self) -> Option<Arc<AccessToken>> {
        self.current.lock().unwrap().clone()
    }

    /// One request to the token endpoint; records the outcome and starts the background refresh
    /// after the first success.
    async fn fetch(self: &Arc<Self>) -> Result<Arc<AccessToken>, TokenError> {
        let outcome = self.request().await;
        let result = outcome
            .as_ref()
            .map_or_else(TokenError::result, |_| FetchResult::Ok);
        self.counters.of(result).fetch_add(1, Ordering::Relaxed);
        match &outcome {
            Ok(token) => {
                *self.last_error.lock().unwrap() = None;
                *self.current.lock().unwrap() = Some(Arc::clone(token));
                self.published.send_replace(Some(Arc::clone(token)));
                self.start_refresher();
            }
            Err(e) => *self.last_error.lock().unwrap() = Some(e.message.clone()),
        }
        outcome
    }

    fn client_secret(&self) -> Result<SecretString, TokenError> {
        let secret = match &self.settings.client_secret_file {
            Some(path) => crate::security::read_token_file(path).map_err(|e| {
                TokenError::new(
                    FetchResult::IoError,
                    format!(
                        "OAuth2 client secret file '{}' cannot be read: {e}",
                        path.display()
                    ),
                )
            })?,
            None => self
                .settings
                .client_secret
                .clone()
                .filter(|s| !s.is_blank()),
        };
        secret.ok_or_else(|| {
            TokenError::new(
                FetchResult::IoError,
                "no OAuth2 client secret (the client secret file is empty)",
            )
        })
    }

    async fn request(&self) -> Result<Arc<AccessToken>, TokenError> {
        let secret = self.client_secret()?;
        let credentials = format!(
            "{}:{}",
            form_encode(&self.settings.client_id),
            form_encode(secret.expose())
        );
        let basic = STANDARD.encode(&credentials);
        // What an endpoint might echo back in an error text: masked before the text is clipped
        // (a clipped secret could no longer be recognised).
        let masker = Masker(vec![
            format!("Basic {basic}"),
            basic.clone(),
            credentials.clone(),
            form_encode(secret.expose()),
            secret.expose().to_string(),
        ]);
        let mut authorization = HeaderValue::from_str(&format!("Basic {basic}")).map_err(|_| {
            TokenError::new(
                FetchResult::IoError,
                "the client credentials cannot be sent",
            )
        })?;
        authorization.set_sensitive(true);
        let mut body = String::from("grant_type=client_credentials");
        if let Some(scope) = self
            .settings
            .scope
            .as_deref()
            .filter(|s| !s.trim().is_empty())
        {
            body.push_str("&scope=");
            body.push_str(&form_encode(scope));
        }
        let io = |e: reqwest::Error| {
            let e = e.without_url();
            masker.error(
                FetchResult::IoError,
                format!("OAuth2 token endpoint: {}", super::tls::chain(&e)),
            )
        };
        let mut response = self
            .http
            .post(self.settings.token_uri.trim())
            .header(AUTHORIZATION, authorization)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(ACCEPT, "application/json")
            .body(body)
            .send()
            .await
            .map_err(io)?;
        let status = response.status();
        let mut bytes: Vec<u8> = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(io)? {
            if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(masker.error(
                    FetchResult::BadResponse,
                    format!(
                        "OAuth2 token endpoint answered with more than {MAX_RESPONSE_BYTES} bytes"
                    ),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let json: Option<serde_json::Value> = serde_json::from_slice(&bytes).ok();
        if !status.is_success() {
            return Err(refusal(status, json.as_ref(), &masker));
        }
        let bad = |what: &str| {
            masker.error(
                FetchResult::BadResponse,
                format!("OAuth2 token endpoint response {what}"),
            )
        };
        let json = json.ok_or_else(|| bad("is not valid JSON"))?;
        let access_token = json
            .get("access_token")
            .and_then(|v| v.as_str())
            .filter(|t| !t.is_empty())
            .ok_or_else(|| bad("has no access_token"))?;
        if let Some(kind) = json.get("token_type") {
            let kind = kind.as_str().unwrap_or("");
            if !kind.eq_ignore_ascii_case("bearer") {
                return Err(bad(&format!("has token_type '{kind}', not Bearer")));
            }
        }
        let lifetime = match json.get("expires_in") {
            None | Some(serde_json::Value::Null) => {
                if !self.expires_in_warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!(default = ?DEFAULT_LIFETIME, "the OAuth2 token response has no expires_in; assuming the default lifetime");
                }
                DEFAULT_LIFETIME
            }
            Some(v) => v
                .as_f64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
                .filter(|s| s.is_finite() && *s >= 1.0)
                .map(|s| Duration::from_secs_f64(s.min(10.0 * 365.0 * 86_400.0)))
                .ok_or_else(|| bad("has an expires_in that is not a positive number of seconds"))?,
        };
        Ok(Arc::new(AccessToken {
            value: SecretString::new(access_token),
            issued: Instant::now(),
            lifetime,
            expires_at: (self.clock)() + lifetime,
            generation: self.generation.fetch_add(1, Ordering::SeqCst) + 1,
        }))
    }

    fn start_refresher(self: &Arc<Self>) {
        if !self.refresher_started.swap(true, Ordering::SeqCst) {
            tokio::spawn(refresh_loop(Arc::downgrade(self), self.cancel.clone()));
        }
    }
}

/// Hides the client credentials in a text an endpoint sent back (some echo the request).
struct Masker(Vec<String>);

impl Masker {
    /// `text` with every secret form replaced by `***`, longest first.
    fn apply(&self, text: &str) -> String {
        let mut out = text.to_string();
        for needle in &self.0 {
            // A secret shorter than this would turn any text into asterisks.
            if needle.len() >= 3 {
                out = out.replace(needle.as_str(), "***");
            }
        }
        out
    }

    fn error(&self, result: FetchResult, text: impl AsRef<str>) -> TokenError {
        TokenError::new(result, self.apply(text.as_ref()))
    }
}

/// A non-success answer: the OAuth2 error body when there is one, else the status.
fn refusal(
    status: reqwest::StatusCode,
    body: Option<&serde_json::Value>,
    masker: &Masker,
) -> TokenError {
    let field = |name: &str| {
        body.and_then(|b| b.get(name))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    };
    match (field("error"), field("error_description")) {
        (Some(error), Some(description)) => masker.error(
            FetchResult::OauthError,
            format!(
                "OAuth2 token endpoint refused (HTTP {}): {error}: {description}",
                status.as_u16()
            ),
        ),
        (Some(error), None) => masker.error(
            FetchResult::OauthError,
            format!(
                "OAuth2 token endpoint refused (HTTP {}): {error}",
                status.as_u16()
            ),
        ),
        _ => masker.error(
            FetchResult::HttpError,
            format!("OAuth2 token endpoint answered HTTP {status}"),
        ),
    }
}

/// The proactive refresh: waits for the current token's margin, fetches, and after a failure
/// retries after [`RETRY_MIN`], doubling up to [`RETRY_MAX`], keeping the token it has. Ends when
/// the provider is dropped.
async fn refresh_loop(provider: Weak<TokenProvider>, cancel: CancellationToken) {
    let mut failures: u32 = 0;
    // The error logged at `warn` last, so a repeat goes to `debug`.
    let mut logged: Option<String> = None;
    loop {
        let delay = {
            let Some(p) = provider.upgrade() else { return };
            let wait = if failures == 0 {
                p.cached()
                    .map_or(RETRY_MIN, |t| t.until_refresh((p.clock)()))
                    .max(RETRY_MIN)
            } else {
                retry_delay(failures)
            };
            // Never sleep through more than the wake interval: a suspended machine does not advance
            // the timer, so the deadline is re-checked against the wall clock after every wake.
            wait.min(p.wake)
        };
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = cancel.cancelled() => return,
        }
        let Some(p) = provider.upgrade() else { return };
        // Not due yet (an early wake), or replaced meanwhile (a stream's own fetch): wait for the
        // current token's margin instead.
        if p.cached().is_some_and(|t| t.fresh((p.clock)())) {
            failures = 0;
            logged = None;
            continue;
        }
        match p.refresh().await {
            Ok(token) => {
                if failures > 0 {
                    tracing::info!(client_id = %p.settings.client_id, expires_in = ?token.lifetime, "OAuth2 token refreshed again");
                }
                failures = 0;
                logged = None;
            }
            Err(e) => {
                failures += 1;
                let retry_in = retry_delay(failures);
                if logged.as_deref() == Some(e.message.as_str()) {
                    tracing::debug!(client_id = %p.settings.client_id, reason = %e, ?retry_in, "OAuth2 token refresh failed; keeping the current token");
                } else {
                    tracing::warn!(client_id = %p.settings.client_id, reason = %e, ?retry_in, "OAuth2 token refresh failed; keeping the current token");
                    logged = Some(e.message);
                }
            }
        }
    }
}

/// 1 s, 2 s, 4 s ... up to 30 s for the 1st, 2nd, 3rd ... consecutive failure.
fn retry_delay(failures: u32) -> Duration {
    RETRY_MIN
        .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1).min(8)))
        .min(RETRY_MAX)
}

/// `application/x-www-form-urlencoded` encoding of one value: alphanumerics and `*-._` as they
/// are, space as `+`, every other byte as `%XX`.
fn form_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// A loopback address, `localhost` or a single-label name (a compose service).
fn local_host(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    match host.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => {
            host.eq_ignore_ascii_case("localhost") || !host.trim_end_matches('.').contains('.')
        }
    }
}

/// The URI without credentials, query or fragment (for a log line).
fn display_uri(url: &reqwest::Url) -> String {
    format!(
        "{}://{}{}",
        url.scheme(),
        url.host_str().unwrap_or(""),
        url.port().map(|p| format!(":{p}")).unwrap_or_default()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_refresh_margin_is_a_fifth_of_the_lifetime_at_least_5_seconds_at_most_half() {
        let s = Duration::from_secs;
        assert_eq!(refresh_margin(s(300)), s(60));
        assert_eq!(refresh_margin(s(100)), s(20));
        assert_eq!(refresh_margin(s(25)), s(5));
        assert_eq!(refresh_margin(s(10)), s(5));
        assert_eq!(refresh_margin(s(6)), s(3));
        assert_eq!(refresh_margin(s(1)), Duration::from_millis(500));
    }

    #[test]
    fn the_retry_delay_doubles_from_1_to_30_seconds() {
        let delays: Vec<u64> = (1..=8).map(|n| retry_delay(n).as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30, 30]);
        assert_eq!(retry_delay(u32::MAX), RETRY_MAX);
    }

    #[test]
    fn form_encoding_follows_the_urlencoded_set() {
        assert_eq!(form_encode("abcXYZ019*-._"), "abcXYZ019*-._");
        assert_eq!(form_encode("a b"), "a+b");
        assert_eq!(
            form_encode("a:b@c/d=e&f+g%h"),
            "a%3Ab%40c%2Fd%3De%26f%2Bg%25h"
        );
        assert_eq!(form_encode("é"), "%C3%A9");
    }

    #[test]
    fn errors_are_clipped_and_cannot_forge_log_lines() {
        let long = "x".repeat(500);
        assert_eq!(clip(&long).chars().count(), ERROR_CLIP);
        assert!(clip(&long).ends_with('…'));
        assert_eq!(clip("a\nb\r\tc"), "a b  c");
        assert_eq!(clip("short"), "short");
    }

    #[test]
    fn plain_http_is_local_for_loopback_and_single_label_hosts() {
        let local = |u: &str| local_host(&reqwest::Url::parse(u).unwrap());
        assert!(local("http://localhost:8180/token"));
        assert!(local("http://127.0.0.1:8180/token"));
        assert!(local("http://[::1]:8180/token"));
        assert!(local("http://auth:8080/token"));
        assert!(!local("http://auth.example:8080/token"));
        assert!(!local("http://auth.internal.example:8080/token"));
        assert!(!local("http://10.1.2.3/token"));
    }
}
