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
/// `dns:///`). Client certificates (mTLS) are roadmap P5c: build `HubTls` with
/// `..HubTls::default()` so the fields added then do not break you.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HubTls {
    /// PEM file with the CA certificate(s) that sign the hub's certificate (`KRAKEN_HUB_TLS_CA`).
    /// `None`: the platform's root store. Re-read on every (re)connect.
    pub ca_file: Option<PathBuf>,
    /// The name the hub's certificate must carry and the SNI sent (`KRAKEN_HUB_TLS_SERVER_NAME`).
    /// `None`: the host of the address.
    pub server_name: Option<String>,
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
