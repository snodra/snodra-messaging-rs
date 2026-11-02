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
        let key = rcgen::KeyPair::generate().unwrap();
        let mut sans = vec!["localhost".to_string()];
        sans.extend(names.iter().map(|n| n.to_string()));
        let mut params = rcgen::CertificateParams::new(sans).unwrap();
        params.subject_alt_names.push(rcgen::SanType::IpAddress(
            std::net::Ipv4Addr::LOCALHOST.into(),
        ));
        let issuer = rcgen::Issuer::from_params(&self.ca_params, &self.ca_key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

/// A temporary directory removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("kraken-test-{}", uuid::Uuid::new_v4()));
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
