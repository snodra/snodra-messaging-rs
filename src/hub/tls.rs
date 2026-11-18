//! TLS on the hub channel (roadmap P5a, ADR 0018): tonic's rustls with the `ring` provider (the one
//! the hub uses; selected by tonic's `tls-ring` feature, which applies unless the application has
//! installed a process-wide rustls provider of its own), the platform's root certificates unless a
//! CA file is configured. The same channel setup serves every address form (a static list,
//! `dns:///`).

use std::path::Path;

use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

use super::mtls::ClientIdentity;
use crate::security::HubTls;

/// The PEM bytes of a CA file. An error (as text, for the config message and the log) when it
/// cannot be read or holds no `CERTIFICATE` block.
pub fn read_ca_file(path: &Path) -> Result<Vec<u8>, String> {
    let pem = std::fs::read(path).map_err(|e| format!("cannot be read: {e}"))?;
    if !String::from_utf8_lossy(&pem).contains("-----BEGIN CERTIFICATE-----") {
        return Err("holds no PEM certificate".into());
    }
    Ok(pem)
}

/// The TLS settings that apply to `address`: the configured ones, else (for an `https://` address)
/// the defaults, else `None` (plain text).
pub(crate) fn effective(configured: Option<&HubTls>, address: &str) -> Option<HubTls> {
    let https = address
        .strip_prefix("static://")
        .unwrap_or(address)
        .starts_with("https://");
    match configured {
        Some(tls) => Some(tls.clone()),
        None if https => Some(HubTls::default()),
        None => None,
    }
}

/// Adds TLS to `endpoint`, presenting `identity` (mutual TLS, P5c) when there is one. The CA file
/// is read now, so a rotated CA applies at the next connect.
pub(crate) fn configure(
    endpoint: Endpoint,
    tls: &HubTls,
    identity: Option<&ClientIdentity>,
) -> Result<Endpoint, String> {
    let mut config = ClientTlsConfig::new();
    match &tls.ca_file {
        Some(path) => {
            let pem =
                read_ca_file(path).map_err(|e| format!("hub CA file '{}' {e}", path.display()))?;
            config = config.ca_certificate(Certificate::from_pem(pem));
        }
        None => config = config.with_native_roots(),
    }
    if let Some(name) = tls
        .server_name
        .as_deref()
        .map(str::trim)
        .filter(|n| !n.is_empty())
    {
        config = config.domain_name(name);
    }
    if let Some(identity) = identity {
        config = config.identity(identity.tonic_identity());
    }
    endpoint
        .tls_config(config)
        .map_err(|e| format!("invalid TLS configuration: {e}"))
}

/// The `Display` of an error and of every error it wraps (`a: b: c`): tonic's connect error
/// prints only "transport error", the cause is what tells a handshake failure from a refusal.
pub(crate) fn chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        let part = s.to_string();
        if !out.contains(&part) {
            out.push_str(": ");
            out.push_str(&part);
        }
        source = s.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn https_addresses_get_tls_even_when_none_is_configured() {
        assert_eq!(effective(None, "static://hub:7000"), None);
        assert_eq!(effective(None, "hub:7000"), None);
        assert_eq!(effective(None, "https://hub:7000"), Some(HubTls::default()));
        assert_eq!(
            effective(None, "static://https://hub:7000"),
            Some(HubTls::default())
        );
        let tls = HubTls {
            server_name: Some("hub".into()),
            ..HubTls::default()
        };
        assert_eq!(effective(Some(&tls), "hub:7000"), Some(tls.clone()));
        assert_eq!(effective(Some(&tls), "http://hub:7000"), Some(tls));
    }

    #[test]
    fn a_ca_file_must_hold_a_certificate() {
        let dir = std::env::temp_dir().join(format!("snodra-tls-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let junk = dir.join("junk.pem");
        std::fs::write(&junk, "nope").unwrap();
        assert!(
            read_ca_file(&junk)
                .unwrap_err()
                .contains("no PEM certificate")
        );
        assert!(
            read_ca_file(&dir.join("absent"))
                .unwrap_err()
                .contains("cannot be read")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
