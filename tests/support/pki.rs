//! Test certificates, generated at test time with rcgen (nothing is checked in): a throwaway CA
//! that signs server certificates, and a temporary directory that holds the PEM files.
#![allow(dead_code)]

use std::path::PathBuf;

/// A throwaway CA that signs server certificates.
pub struct Pki {
    ca_params: rcgen::CertificateParams,
    ca_key: rcgen::KeyPair,
    pub ca_pem: String,
}

impl Pki {
    pub fn new(name: &str) -> Self {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let ca = ca_params.self_signed(&ca_key).unwrap();
        Self {
            ca_pem: ca.pem(),
            ca_params,
            ca_key,
        }
    }

    /// `(certificate PEM, private key PEM)` for a server reachable as `localhost`, `127.0.0.1` and
    /// each of `names`.
    pub fn server(&self, names: &[&str]) -> (String, String) {
        self.sign(names, true)
    }

    /// Like [`server`](Self::server) but the certificate names exactly `names` (DNS only): no
    /// `localhost`, no IP address, as a certificate issued for a service name.
    pub fn server_names_only(&self, names: &[&str]) -> (String, String) {
        self.sign(names, false)
    }

    fn sign(&self, names: &[&str], with_local: bool) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut sans = if with_local {
            vec!["localhost".to_string()]
        } else {
            vec![]
        };
        sans.extend(names.iter().map(|n| n.to_string()));
        let mut params = rcgen::CertificateParams::new(sans).unwrap();
        if with_local {
            params.subject_alt_names.push(rcgen::SanType::IpAddress(
                std::net::Ipv4Addr::LOCALHOST.into(),
            ));
        }
        let issuer = rcgen::Issuer::from_params(&self.ca_params, &self.ca_key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

/// What a client certificate carries: its SANs and its validity period.
pub struct ClientSpec {
    pub uris: Vec<String>,
    pub dns: Vec<String>,
    pub not_before: time::OffsetDateTime,
    pub not_after: time::OffsetDateTime,
}

impl ClientSpec {
    /// The certificate of `name`: one `snodra://service/<name>` URI SAN, valid for a week.
    pub fn service(name: &str) -> Self {
        let now = time::OffsetDateTime::now_utc();
        ClientSpec {
            uris: vec![format!("snodra://service/{name}")],
            dns: vec![],
            not_before: now - time::Duration::hours(1),
            not_after: now + time::Duration::days(7),
        }
    }

    /// Not valid yet: valid from `delay` from now.
    pub fn starting_in(mut self, delay: std::time::Duration) -> Self {
        self.not_before = time::OffsetDateTime::now_utc() + delay;
        self
    }

    /// Valid from an hour ago for `lifetime` from now.
    pub fn living(mut self, lifetime: std::time::Duration) -> Self {
        self.not_after = time::OffsetDateTime::now_utc() + lifetime;
        self
    }
}

impl Pki {
    /// `(certificate PEM, private key PEM)` of a client with the `clientAuth` usage.
    pub fn client(&self, spec: &ClientSpec) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(spec.dns.clone()).unwrap();
        for uri in &spec.uris {
            params
                .subject_alt_names
                .push(rcgen::SanType::URI(uri.clone().try_into().unwrap()));
        }
        params.not_before = spec.not_before;
        params.not_after = spec.not_after;
        params
            .extended_key_usages
            .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);
        let issuer = rcgen::Issuer::from_params(&self.ca_params, &self.ca_key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// The client certificate of service `name`.
    pub fn client_for(&self, name: &str) -> (String, String) {
        self.client(&ClientSpec::service(name))
    }

    /// A server certificate that also carries the `clientAuth` usage, as a platform CA issues the
    /// hub's own pair when it must pass the hub's own `required` health check.
    pub fn server_and_client(&self, names: &[&str]) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut sans = vec!["localhost".to_string()];
        sans.extend(names.iter().map(|n| n.to_string()));
        let mut params = rcgen::CertificateParams::new(sans).unwrap();
        params.subject_alt_names.push(rcgen::SanType::IpAddress(
            std::net::Ipv4Addr::LOCALHOST.into(),
        ));
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let issuer = rcgen::Issuer::from_params(&self.ca_params, &self.ca_key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

/// The first 8 hex characters of the SHA-256 of the first certificate in `pem`: how the library and
/// the hub show a certificate.
pub fn fingerprint_of_pem(pem: &str) -> String {
    let der = pem
        .lines()
        .skip_while(|l| !l.starts_with("-----BEGIN CERTIFICATE"))
        .skip(1)
        .take_while(|l| !l.starts_with("-----END"))
        .collect::<String>();
    use base64::Engine;
    let der = base64::engine::general_purpose::STANDARD
        .decode(der)
        .unwrap();
    fingerprint_of_der(&der)
}

pub fn fingerprint_of_der(der: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(der)[..4]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A temporary directory removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new() -> Self {
        // Under `target/` (inside the checkout) rather than the system temp dir: container runtimes
        // that run in a VM share the home directory, not necessarily `/var/folders`.
        let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("snodra-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    /// Writes `content` to `name` (world-readable, so a container user can read it) and returns
    /// the path.
    pub fn write(&self, name: &str, content: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = self.path(name);
        std::fs::write(&path, content).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        path
    }

    pub fn dir(&self) -> &std::path::Path {
        &self.0
    }
}

impl Default for TempDir {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
