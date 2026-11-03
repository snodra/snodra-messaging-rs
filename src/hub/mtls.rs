//! The client certificate of a mutual-TLS setup (roadmap P5c, hub ADR 0020): reading and checking
//! the certificate and key files, the identity the hub will read from the leaf (exactly one URI SAN
//! `kraken://service/<name>`), and when to warn that it is about to expire.
//!
//! Parsing is pure Rust (`x509-parser`, rustls' own PEM and key parsing), so the crate still needs
//! no C toolchain. Nothing here logs or prints key material: errors name the file and what is wrong
//! with it, never a line of its content; a certificate is shown as the first 8 hex characters of
//! the SHA-256 of its DER.

use std::path::Path;
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};

/// The URI SAN prefix that names a service.
pub const SERVICE_SAN_PREFIX: &str = "kraken://service/";

/// The warning threshold: the smaller of a tenth of the certificate's lifetime and this.
pub const EXPIRY_WARNING_CAP: Duration = Duration::from_secs(24 * 60 * 60);

/// A client certificate pair that was read and checked: it parses, the key belongs to the leaf and
/// the leaf names the service.
#[derive(Clone)]
pub(crate) struct ClientIdentity {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    /// The service the leaf names.
    pub service: String,
    /// First 8 hex characters of the SHA-256 of the leaf's DER: the only form of the certificate
    /// that is logged or shown.
    pub fingerprint: String,
    /// SHA-256 over both files' content: tells a renewed pair from the one in use.
    pub digest: [u8; 32],
    pub not_before: SystemTime,
    pub not_after: SystemTime,
}

impl std::fmt::Debug for ClientIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientIdentity")
            .field("service", &self.service)
            .field("fingerprint", &self.fingerprint)
            .field("not_after", &self.not_after)
            .finish_non_exhaustive()
    }
}

impl ClientIdentity {
    /// The identity tonic presents in the TLS handshake: the very bytes that were checked.
    pub fn tonic_identity(&self) -> tonic::transport::Identity {
        tonic::transport::Identity::from_pem(&self.cert_pem, &self.key_pem)
    }
}

/// Why a pair cannot be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LoadError {
    /// A file cannot be read or parsed, the key is not the certificate's, or the leaf names no
    /// (or more than one) service.
    Unusable(String),
    /// The leaf names another service than the configured one: the hub refuses the `Register`.
    WrongService(String),
    /// The leaf's validity has ended.
    Expired(String),
    /// The leaf's validity has not begun.
    NotYetValid(String),
}

impl LoadError {
    pub fn message(&self) -> &str {
        match self {
            LoadError::Unusable(m)
            | LoadError::WrongService(m)
            | LoadError::Expired(m)
            | LoadError::NotYetValid(m) => m,
        }
    }
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

/// The longest service name a certificate may carry (the hub's rule).
const MAX_SERVICE_NAME: usize = 128;

/// A service name a certificate may carry: 1 to 128 characters of `[A-Za-z0-9._-]` (the hub's rule).
fn valid_service_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_SERVICE_NAME
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn read(path: &Path, what: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("{what} '{}' cannot be read: {e}", path.display()))
}

/// Reads and checks the pair in `cert_file` and `key_file` for `service`: both files read, the
/// certificate chain (leaf first) and the key parse, the key belongs to the leaf, and the leaf
/// names `service` with exactly one `kraken://service/<name>` URI SAN. Expiry is not judged here
/// (see [`ClientIdentity::not_after`]).
pub(crate) fn load(
    cert_file: &Path,
    key_file: &Path,
    service: &str,
) -> Result<ClientIdentity, LoadError> {
    let identity = load_pair(cert_file, key_file).map_err(LoadError::Unusable)?;
    if identity.service != service {
        return Err(LoadError::WrongService(format!(
            "the client certificate in '{}' names service '{}', but this service is '{service}': the hub requires the same name",
            cert_file.display(),
            identity.service
        )));
    }
    Ok(identity)
}

fn load_pair(cert_file: &Path, key_file: &Path) -> Result<ClientIdentity, String> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use x509_parser::extensions::GeneralName;

    let cert_pem = read(cert_file, "client certificate file")?;
    let key_pem = read(key_file, "client key file")?;
    let chain = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            format!(
                "client certificate file '{}' is not valid PEM",
                cert_file.display()
            )
        })?;
    let Some(leaf) = chain.first() else {
        return Err(format!(
            "client certificate file '{}' holds no PEM certificate",
            cert_file.display()
        ));
    };
    // Neither the parser's error nor the key's content goes into a message.
    let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|e| match e {
        rustls::pki_types::pem::Error::NoItemsFound => format!(
            "client key file '{}' holds no PEM private key",
            key_file.display()
        ),
        _ => format!("client key file '{}' is not valid PEM", key_file.display()),
    })?;
    let signing = rustls::crypto::ring::sign::any_supported_type(&key).map_err(|_| {
        format!(
            "client key file '{}' holds a private key this client cannot use: it must be an RSA, ECDSA (P-256 or P-384) or Ed25519 key in PKCS#8 PEM, and an EC key must carry its public key (OpenSSL writes it; some tools leave it out). Convert it with `openssl pkcs8 -topk8 -nocrypt -in <key> -out <key>.pkcs8.pem` (a key that still fails: generate a new one with openssl, e.g. `openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256`)",
            key_file.display()
        )
    })?;

    let (rest, cert) = x509_parser::parse_x509_certificate(leaf.as_ref()).map_err(|_| {
        format!(
            "the first certificate in '{}' cannot be parsed",
            cert_file.display()
        )
    })?;
    if !rest.is_empty() {
        return Err(format!(
            "the first certificate in '{}' cannot be parsed",
            cert_file.display()
        ));
    }
    let belongs = signing
        .public_key()
        .is_some_and(|spki| spki.as_ref() == cert.tbs_certificate.subject_pki.raw);
    if !belongs {
        return Err(format!(
            "client key '{}' does not match the first certificate in '{}' (the leaf goes first)",
            key_file.display(),
            cert_file.display()
        ));
    }

    let sans = cert.subject_alternative_name().map_err(|_| {
        format!(
            "the subject alternative names of the certificate in '{}' cannot be parsed",
            cert_file.display()
        )
    })?;
    let mut claims = sans
        .iter()
        .flat_map(|san| &san.value.general_names)
        .filter_map(|name| match name {
            GeneralName::URI(uri) => uri.strip_prefix(SERVICE_SAN_PREFIX),
            _ => None,
        });
    let named = claims.next().ok_or_else(|| {
        format!(
            "the client certificate in '{}' carries no {SERVICE_SAN_PREFIX}<name> URI SAN, so the hub cannot tell which service it is",
            cert_file.display()
        )
    })?;
    if claims.next().is_some() {
        return Err(format!(
            "the client certificate in '{}' carries more than one {SERVICE_SAN_PREFIX}<name> URI SAN; the hub refuses it",
            cert_file.display()
        ));
    }
    if !valid_service_name(named) {
        return Err(format!(
            "the client certificate in '{}' names a service the hub would refuse (1 to {MAX_SERVICE_NAME} characters of A-Z a-z 0-9 . _ -)",
            cert_file.display()
        ));
    }
    let time = |t: x509_parser::time::ASN1Time| {
        u64::try_from(t.timestamp())
            .map(|secs| SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
            .unwrap_or(SystemTime::UNIX_EPOCH)
    };
    let validity = cert.validity();
    let fingerprint: String = Sha256::digest(leaf.as_ref())[..4]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let digest = Sha256::new()
        .chain_update((cert_pem.len() as u64).to_be_bytes())
        .chain_update(&cert_pem)
        .chain_update(&key_pem)
        .finalize()
        .into();
    Ok(ClientIdentity {
        service: named.to_string(),
        fingerprint,
        digest,
        not_before: time(validity.not_before),
        not_after: time(validity.not_after),
        cert_pem,
        key_pem,
    })
}

/// [`LoadError::Expired`] when the leaf of `identity` has expired at `now`,
/// [`LoadError::NotYetValid`] when its validity has not begun.
pub(crate) fn check_valid_at(identity: &ClientIdentity, now: SystemTime) -> Result<(), LoadError> {
    if now < identity.not_before {
        let begins: chrono::DateTime<chrono::Utc> = identity.not_before.into();
        return Err(LoadError::NotYetValid(format!(
            "the client certificate {} ({}) is not valid before {}",
            identity.fingerprint,
            identity.service,
            begins.format("%Y-%m-%dT%H:%M:%SZ")
        )));
    }
    if now < identity.not_after {
        return Ok(());
    }
    let ended: chrono::DateTime<chrono::Utc> = identity.not_after.into();
    Err(LoadError::Expired(format!(
        "the client certificate {} ({}) expired at {}",
        identity.fingerprint,
        identity.service,
        ended.format("%Y-%m-%dT%H:%M:%SZ")
    )))
}

/// True when a certificate valid from `not_before` to `not_after` has no more than a tenth of its
/// lifetime (or [`EXPIRY_WARNING_CAP`], whichever is smaller) left at `now`.
pub(crate) fn expiry_warning_due(
    not_before: SystemTime,
    not_after: SystemTime,
    now: SystemTime,
) -> bool {
    let lifetime = not_after.duration_since(not_before).unwrap_or_default();
    let threshold = (lifetime / 10).min(EXPIRY_WARNING_CAP);
    not_after
        .duration_since(now)
        .map_or(true, |left| left <= threshold)
}

#[cfg(test)]
pub(crate) mod test_pki {
    //! Certificates for the unit tests, generated with rcgen.
    use std::time::Duration;

    pub struct Spec {
        pub uris: Vec<String>,
        pub dns: Vec<String>,
        pub not_before: time::OffsetDateTime,
        pub not_after: time::OffsetDateTime,
    }

    impl Spec {
        pub fn service(name: &str) -> Spec {
            let now = time::OffsetDateTime::now_utc();
            Spec {
                uris: vec![format!("kraken://service/{name}")],
                dns: vec![],
                not_before: now - Duration::from_secs(3600),
                not_after: now + Duration::from_secs(7 * 24 * 3600),
            }
        }
    }

    pub struct Pair {
        pub cert_pem: String,
        pub key_pem: String,
        pub ca_pem: String,
    }

    pub fn issue(spec: &Spec) -> Pair {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test CA");
        let ca = ca_params.self_signed(&ca_key).unwrap();
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
        let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        Pair {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            ca_pem: ca.pem(),
        }
    }

    /// A temporary directory with `cert.pem` and `key.pem`, removed on drop.
    pub struct Dir(pub std::path::PathBuf);

    impl Dir {
        pub fn new() -> Dir {
            let dir = std::env::temp_dir().join(format!("kraken-mtls-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Dir(dir)
        }

        pub fn write(&self, name: &str, content: &str) -> std::path::PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, content).unwrap();
            path
        }

        pub fn pair(&self, pair: &Pair) -> (std::path::PathBuf, std::path::PathBuf) {
            (
                self.write("cert.pem", &pair.cert_pem),
                self.write("key.pem", &pair.key_pem),
            )
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_pki::{Dir, Spec, issue};
    use super::*;

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn a_valid_pair_loads_with_the_service_the_fingerprint_and_the_validity() {
        let (dir, spec) = (Dir::new(), Spec::service("billing"));
        let pair = issue(&spec);
        let (cert, key) = dir.pair(&pair);
        let id = load(&cert, &key, "billing").unwrap();
        assert_eq!(id.service, "billing");
        assert_eq!(id.fingerprint.len(), 8);
        assert!(id.fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
        let expected = spec.not_after.unix_timestamp() as u64;
        let got = id
            .not_after
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(got, expected);
        assert!(id.not_before < id.not_after);
        let again = load(&cert, &key, "billing").unwrap();
        assert_eq!(again.digest, id.digest);
        assert_eq!(again.fingerprint, id.fingerprint);
    }

    #[test]
    fn a_chain_is_accepted_and_the_leaf_is_the_first_certificate() {
        let (dir, spec) = (Dir::new(), Spec::service("billing"));
        let pair = issue(&spec);
        let leaf_only = dir.pair(&pair);
        let alone = load(&leaf_only.0, &leaf_only.1, "billing").unwrap();
        let chain = dir.write("chain.pem", &format!("{}{}", pair.cert_pem, pair.ca_pem));
        let id = load(&chain, &leaf_only.1, "billing").unwrap();
        assert_eq!(id.fingerprint, alone.fingerprint);
        // A chain in the wrong order puts a certificate that is not the key's first.
        let reversed = dir.write("rev.pem", &format!("{}{}", pair.ca_pem, pair.cert_pem));
        let err = load(&reversed, &leaf_only.1, "billing")
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not match"), "{err}");
    }

    #[test]
    fn a_renewed_pair_has_another_digest_and_fingerprint() {
        let dir = Dir::new();
        let (c1, k1) = dir.pair(&issue(&Spec::service("billing")));
        let first = load(&c1, &k1, "billing").unwrap();
        let (c2, k2) = dir.pair(&issue(&Spec::service("billing")));
        let second = load(&c2, &k2, "billing").unwrap();
        assert_ne!(first.digest, second.digest);
        assert_ne!(first.fingerprint, second.fingerprint);
    }

    #[test]
    fn unreadable_or_empty_files_are_errors_naming_the_file() {
        let dir = Dir::new();
        let (cert, key) = dir.pair(&issue(&Spec::service("billing")));
        let missing = dir.0.join("absent.pem");
        let err = load(&missing, &key, "billing").unwrap_err().to_string();
        assert!(
            err.contains("absent.pem") && err.contains("cannot be read"),
            "{err}"
        );
        let err = load(&cert, &missing, "billing").unwrap_err().to_string();
        assert!(
            err.contains("absent.pem") && err.contains("cannot be read"),
            "{err}"
        );
        let empty = dir.write("empty.pem", "");
        let err = load(&empty, &key, "billing").unwrap_err().to_string();
        assert!(
            err.contains("empty.pem") && err.contains("no PEM certificate"),
            "{err}"
        );
        let err = load(&cert, &empty, "billing").unwrap_err().to_string();
        assert!(
            err.contains("empty.pem") && err.contains("no PEM private key"),
            "{err}"
        );
        // The key file where the certificate belongs, and the other way round.
        let err = load(&key, &key, "billing").unwrap_err().to_string();
        assert!(err.contains("no PEM certificate"), "{err}");
        let err = load(&cert, &cert, "billing").unwrap_err().to_string();
        assert!(err.contains("no PEM private key"), "{err}");
    }

    #[test]
    fn a_key_that_does_not_belong_to_the_certificate_is_an_error() {
        let dir = Dir::new();
        let (cert, _) = dir.pair(&issue(&Spec::service("billing")));
        let other = dir.write("other-key.pem", &issue(&Spec::service("billing")).key_pem);
        let err = load(&cert, &other, "billing").unwrap_err().to_string();
        assert!(err.contains("does not match"), "{err}");
    }

    #[test]
    fn the_leaf_must_name_exactly_the_configured_service() {
        let dir = Dir::new();
        let pair = issue(&Spec::service("orders"));
        let (cert, key) = dir.pair(&pair);
        let err = load(&cert, &key, "billing").unwrap_err();
        assert!(matches!(err, LoadError::WrongService(_)), "{err:?}");
        let err = err.to_string();
        assert!(err.contains("orders") && err.contains("billing"), "{err}");

        let none = issue(&Spec {
            uris: vec![],
            dns: vec!["billing".into()],
            ..Spec::service("x")
        });
        let (cert, key) = dir.pair(&none);
        let err = load(&cert, &key, "billing").unwrap_err().to_string();
        assert!(err.contains("kraken://service/"), "{err}");

        let other_uri = issue(&Spec {
            uris: vec!["spiffe://example.org/billing".into()],
            ..Spec::service("x")
        });
        let (cert, key) = dir.pair(&other_uri);
        assert!(load(&cert, &key, "billing").is_err());

        // Two service names, even equal ones, are refused by the hub.
        let two = issue(&Spec {
            uris: vec![
                "kraken://service/billing".into(),
                "kraken://service/billing".into(),
            ],
            ..Spec::service("x")
        });
        let (cert, key) = dir.pair(&two);
        let err = load(&cert, &key, "billing").unwrap_err().to_string();
        assert!(err.contains("more than one"), "{err}");

        // One service SAN among other URIs and DNS names is fine.
        let mixed = issue(&Spec {
            uris: vec![
                "spiffe://example.org/billing".into(),
                "kraken://service/billing".into(),
            ],
            dns: vec!["billing.example".into()],
            ..Spec::service("x")
        });
        let (cert, key) = dir.pair(&mixed);
        assert!(load(&cert, &key, "billing").is_ok());

        // A name the hub would refuse (outside [A-Za-z0-9._-]).
        let odd = issue(&Spec {
            uris: vec!["kraken://service/bill ing".into()],
            ..Spec::service("x")
        });
        let (cert, key) = dir.pair(&odd);
        assert!(load(&cert, &key, "bill ing").is_err());
    }

    #[test]
    fn errors_and_debug_never_carry_key_material() {
        let dir = Dir::new();
        let pair = issue(&Spec::service("orders"));
        let (cert, key) = dir.pair(&pair);
        let body: String = pair
            .key_pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect();
        let probe = &body[10..30];
        for err in [
            load(&cert, &key, "billing").unwrap_err().to_string(),
            load(&key, &key, "billing").unwrap_err().to_string(),
            load(&cert, &cert, "billing").unwrap_err().to_string(),
        ] {
            assert!(!err.contains(probe), "{err}");
        }
        let id = load(&cert, &key, "orders").unwrap();
        assert!(!format!("{id:?}").contains(probe));
        assert!(!format!("{id:#?}").contains(probe));
    }

    /// A PKCS#8 P-256 key whose `ECPrivateKey` carries no public key: ring refuses it.
    fn ec_key_without_public_key_pem() -> String {
        use base64::Engine;
        let mut der = vec![0x30, 0x4d, 0x02, 0x01, 0x00];
        der.extend([
            0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01,
        ]);
        der.extend([0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07]);
        der.extend([0x04, 0x33, 0x30, 0x31, 0x02, 0x01, 0x01, 0x04, 0x20]);
        der.extend((1..=32u8).collect::<Vec<_>>());
        der.extend([
            0xa0, 0x0a, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07,
        ]);
        let body = base64::engine::general_purpose::STANDARD.encode(der);
        format!("-----BEGIN PRIVATE KEY-----\n{body}\n-----END PRIVATE KEY-----\n")
    }

    #[test]
    fn a_key_that_ring_refuses_says_what_is_wrong_and_how_to_convert_it() {
        let dir = Dir::new();
        let (cert, _) = dir.pair(&issue(&Spec::service("billing")));
        let key = dir.write("odd-key.pem", &ec_key_without_public_key_pem());
        let err = load(&cert, &key, "billing").unwrap_err().to_string();
        assert!(err.contains("odd-key.pem"), "{err}");
        assert!(!err.contains("not supported"), "{err}");
        assert!(err.contains("public key"), "{err}");
        assert!(err.contains("openssl pkcs8 -topk8 -nocrypt"), "{err}");
        assert!(err.contains("PKCS#8"), "{err}");
    }

    #[test]
    fn the_warning_is_due_below_a_tenth_of_the_lifetime_or_a_day() {
        let day = 24 * 3600;
        // 10 days: a tenth is exactly a day.
        let (from, to) = (at(0), at(10 * day));
        assert!(!expiry_warning_due(from, to, at(8 * day)));
        assert!(!expiry_warning_due(from, to, at(9 * day - 1)));
        assert!(expiry_warning_due(from, to, at(9 * day)));
        // 100 days: a tenth is 10 days, capped at one day.
        let (from, to) = (at(0), at(100 * day));
        assert!(!expiry_warning_due(from, to, at(98 * day)));
        assert!(expiry_warning_due(from, to, at(99 * day)));
        // 8 s: 0.8 s.
        let (from, to) = (at(1000), at(1008));
        assert!(!expiry_warning_due(from, to, at(1007)));
        assert!(expiry_warning_due(
            from,
            to,
            at(1007) + Duration::from_millis(250)
        ));
        // Already past: due.
        assert!(expiry_warning_due(from, to, at(2000)));
    }
}
