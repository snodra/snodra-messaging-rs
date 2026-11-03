//! Hub access control, client side (roadmap P5a, ADR 0018): the static token a service presents in
//! `Register.credentials.token`, where it comes from, and the TLS settings of the hub channel.

use std::fmt;
use std::path::{Path, PathBuf};

/// A secret that never prints: `Debug` shows `"***"`, there is no `Display`. Read it with
/// [`expose`](Self::expose) at the one place that must send it.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> SecretString {
        SecretString(value.into())
    }

    /// The secret itself; call it only to put it on the wire.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// True for an empty or blank secret, which counts as no secret at all.
    pub fn is_blank(&self) -> bool {
        self.0.trim().is_empty()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"***\"")
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        SecretString(value)
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        SecretString(value.to_string())
    }
}

/// TLS towards the hubs (`KRAKEN_HUB_TLS=true`), rustls with the platform's root certificates
/// unless `ca_file` names the CA to trust instead. Applies to every address form (a static list,
/// `dns:///`). With `client_cert_file` and `client_key_file` the service also presents a client
/// certificate (mutual TLS, roadmap P5c, hub ADR 0020). Build `HubTls` with `..HubTls::default()`
/// so the fields added later do not break you.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HubTls {
    /// PEM file with the CA certificate(s) that sign the hub's certificate (`KRAKEN_HUB_TLS_CA`).
    /// `None`: the platform's root store. Re-read on every (re)connect.
    pub ca_file: Option<PathBuf>,
    /// The name the hub's certificate must carry and the SNI sent (`KRAKEN_HUB_TLS_SERVER_NAME`).
    /// `None`: the host of the address (for `dns:///name:port`, `name`, not the resolved IP).
    pub server_name: Option<String>,
    /// PEM file with the client certificate chain, leaf first (`KRAKEN_HUB_TLS_CLIENT_CERT`). The
    /// leaf carries exactly one URI SAN `kraken://service/<name>`, `<name>` being the service name.
    /// Both this and `client_key_file`, or neither. Re-read on every (re)connect and checked for
    /// a renewal once per heartbeat period, see [`crate::hub::HubPool`]. Excludes a static hub
    /// token and OAuth2 ([`crate::Config::validate`]).
    pub client_cert_file: Option<PathBuf>,
    /// PEM file with the private key of the client certificate (`KRAKEN_HUB_TLS_CLIENT_KEY`),
    /// PKCS#8 (`BEGIN PRIVATE KEY`); EC and RSA PEM keys work as well without a promise.
    pub client_key_file: Option<PathBuf>,
}

impl HubTls {
    /// True when a client certificate is configured (either file; [`crate::Config::validate`]
    /// insists on both).
    pub fn has_client_certificate(&self) -> bool {
        self.client_cert_file.is_some() || self.client_key_file.is_some()
    }
}

/// The OAuth2 client-credentials setup that authenticates this service to the hubs with a Keycloak
/// access token (JWT; roadmap P5b, hub ADR 0019). `Config.hub_oauth2`; mutually exclusive with a
/// static hub token. Build it with `..HubOAuth2::default()` so fields added later do not break you.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HubOAuth2 {
    /// The realm's token endpoint (`KRAKEN_HUB_OAUTH2_TOKEN_URI`), e.g.
    /// `https://keycloak/realms/kraken/protocol/openid-connect/token`. `http://` is allowed (a
    /// compose network) and logs one warning unless the host is loopback or a single-label name.
    pub token_uri: String,
    /// The Keycloak client id (`KRAKEN_HUB_OAUTH2_CLIENT_ID`); it must be the service name, because
    /// the hub checks the token's `azp` against `Register.service_name`. `from_env` defaults it to
    /// `KRAKEN_SERVICE_NAME`.
    pub client_id: String,
    /// The client secret (`KRAKEN_HUB_OAUTH2_CLIENT_SECRET`). Never logged (`Debug` prints `"***"`).
    /// Replaced by `client_secret_file` when that is set.
    pub client_secret: Option<SecretString>,
    /// A file holding the client secret (`KRAKEN_HUB_OAUTH2_CLIENT_SECRET_FILE`), read at startup
    /// and again on every token fetch, so a rotated secret applies without a restart. **It wins
    /// over `client_secret`.** Surrounding whitespace is trimmed.
    pub client_secret_file: Option<PathBuf>,
    /// The `scope` sent with the grant (`KRAKEN_HUB_OAUTH2_SCOPE`, space-separated as sent), if any.
    pub scope: Option<String>,
    /// PEM file with extra root certificates for an `https://` token endpoint
    /// (`KRAKEN_HUB_OAUTH2_CA`), added to the platform roots. Not the hub CA (`HubTls.ca_file`).
    pub ca_file: Option<PathBuf>,
}

/// The token in `path`, surrounding whitespace (the trailing newline of a secret file) removed;
/// `None` when the file is empty. The error never carries file content.
pub(crate) fn read_token_file(path: &Path) -> std::io::Result<Option<SecretString>> {
    let raw = std::fs::read_to_string(path)?;
    let token = raw.trim();
    Ok((!token.is_empty()).then(|| SecretString::new(token)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_shows_the_secret() {
        let s = SecretString::new("hunter2-very-secret");
        assert_eq!(format!("{s:?}"), "\"***\"");
        assert_eq!(format!("{:#?}", Some(&s)), "Some(\n    \"***\",\n)");
        assert_eq!(s.expose(), "hunter2-very-secret");
    }

    #[test]
    fn blank_secrets_count_as_none() {
        assert!(SecretString::new("").is_blank());
        assert!(SecretString::new(" \n").is_blank());
        assert!(!SecretString::new("x").is_blank());
    }

    #[test]
    fn a_token_file_is_trimmed_and_an_empty_one_is_no_token() {
        let dir = std::env::temp_dir().join(format!("kraken-sec-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        std::fs::write(&path, "abc123\n").unwrap();
        assert_eq!(read_token_file(&path).unwrap().unwrap().expose(), "abc123");
        std::fs::write(&path, " \n").unwrap();
        assert!(read_token_file(&path).unwrap().is_none());
        assert!(read_token_file(&dir.join("missing")).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
